'use strict';

// Compares a Purchase Order (structured DB data) against a supplier-issued
// Proforma/Commercial Invoice (PDF in S3) and returns a structured discrepancy
// report. The actual line-item / total / supplier-identity diff is delegated
// to Gemini via the structured-output API; this module is the glue: PDF
// fetch, PO data loading, prompt assembly, JSON schema, error handling.
//
// Gemini reads the PDF natively (no text extraction step) — image-only /
// scanned invoices work transparently.
//
// Consumers: thin HTTP handlers in src/handlers/orders.js — keep all
// comparison logic here so future tweaks (rubric, model, output shape)
// don't ripple into the handlers.

const { GoogleGenAI } = require('@google/genai');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const log = require('../lib/logger');

const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const REGION = process.env.AWS_REGION || 'eu-north-1';
const s3 = new S3Client({ region: REGION });

// Default model. Newer Pro/Flash preview models are available
// (`gemini-3.1-pro-preview`, `gemini-3-flash-preview`); 2.5-pro is the
// stable Pro tier. Flash 3 is "frontier-class at a fraction of the cost"
// per Google's docs and is right-sized for this task — it has to compare a
// few dozen line items against extracted PDF content, not write code.
// Callers can override per-call via the `model` option on compare().
const GEMINI_MODEL_DEFAULT = 'gemini-3-flash-preview';

// ── Prompt + schema ──────────────────────────────────────────────────────
const SYSTEM_INSTRUCTION = `You are an accounts-payable verifier for a UK medical-supplies importer. You receive a Purchase Order (PO) as structured JSON and a supplier-issued Proforma/Commercial Invoice (PI) as a PDF. Compare them and return a structured discrepancy report.

Compare these dimensions:

1. Line items — match each PI line to a PO line by SKU/jf_code first, then by product description if SKU is absent on the PI. For each pair, compare quantity, unit price, and line total. Flag mismatches even if small (these are commercial contracts). Currency amounts are equal when they round to the same 2dp value.

2. Header totals — subtotal, shipping, grand total. Compute the PO grand total yourself from line_total values plus shipping. Flag any delta > 0.50 currency units between PO and PI.

3. Supplier identity — does the invoice come from the same supplier named on the PO? Does it reference the PO number anywhere?

4. Currency and payment terms — does the PI currency match the PO currency? Do payment terms differ?

Separately, EXTRACT the payment instructions from the PI so a downstream payment job can arrange the transfer. Populate the \`paymentTerms\` object:
- paymentType — is the amount payable on THIS invoice a "deposit" (an upfront/down-payment, e.g. "30% deposit"), the "balance" (final/remaining payment before or after shipment), the "full" amount, or "other"/"unknown" if unclear.
- amountDueNow — the exact amount the buyer must actually pay against this invoice now, as a number (the deposit amount if a deposit is due, otherwise the payable total). Null if the invoice states no payable amount.
- currency — ISO code of that amount (e.g. "USD", "EUR", "CNY", "GBP").
- depositPercentage — if the terms express a percentage (e.g. 30 for "30% deposit"), the number; else null.
- invoiceTotal — the invoice grand total as a number, for context.
- dueDate — a concrete payment due date as YYYY-MM-DD if the invoice states one (or one clearly derivable, e.g. "due within 7 days" from an invoice date); else null.
- dueTerms — the payment-timing terms verbatim/short (e.g. "30% deposit on order confirmation, 70% balance before shipment"); else null.
- beneficiaryName, bankName, bankAddress, accountNumber, iban, swiftBic, intermediaryBank, paymentReference — the supplier's remittance/bank details exactly as printed (do not invent or reformat account numbers; copy digits/letters verbatim). Null any field not present.
- rawText — the full payment-terms + bank-details section copied verbatim from the PI, so a human can sanity-check the parsed fields.
Extraction is best-effort and independent of the verdict: a clean invoice with clear bank details still fills paymentTerms; never fail the comparison because a payment field is missing — just null it.

For every discrepancy:
- Give the exact PO value and the exact PI value so the operator can verify quickly.
- Use the SKU/jf_code where available, otherwise the product description.

Verdict rules:
- "pass" — no discrepancies, or only cosmetic differences (e.g. supplier name punctuation).
- "minor_discrepancies" — one or two small numeric mismatches OR missing PO-number reference, but nothing that would block payment.
- "major_discrepancies" — material differences that need supplier action: missing lines, wrong quantities, totals more than 1% apart, wrong supplier, wrong currency.

Be strict on numbers — even a 0.5% unit-price drift across a 16-line PO is material.`;

const RESPONSE_SCHEMA = {
    type: 'object',
    additionalProperties: false,
    properties: {
        summary: {
            type: 'object',
            additionalProperties: false,
            properties: {
                matchedLines: { type: 'integer' },
                mismatches: { type: 'integer' },
                missingOnInvoice: { type: 'integer' },
                extraOnInvoice: { type: 'integer' },
            },
            required: ['matchedLines', 'mismatches', 'missingOnInvoice', 'extraOnInvoice'],
        },
        header: {
            type: 'object',
            additionalProperties: false,
            properties: {
                supplierMatches: { type: 'boolean' },
                supplierNotes: { type: 'string' },
                poNumberReferenced: { type: 'boolean' },
                currencyMatches: { type: 'boolean' },
                paymentTermsMatch: { type: 'boolean' },
                paymentTermsNotes: { type: 'string' },
            },
            required: [
                'supplierMatches', 'supplierNotes',
                'poNumberReferenced',
                'currencyMatches',
                'paymentTermsMatch', 'paymentTermsNotes',
            ],
        },
        totals: {
            type: 'object',
            additionalProperties: false,
            properties: {
                poGrandTotal: { type: ['number', 'null'] },
                piGrandTotal: { type: ['number', 'null'] },
                delta: { type: ['number', 'null'] },
                notes: { type: 'string' },
            },
            required: ['poGrandTotal', 'piGrandTotal', 'delta', 'notes'],
        },
        discrepancies: {
            type: 'array',
            items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                    type: {
                        type: 'string',
                        enum: [
                            'unit_price_mismatch',
                            'quantity_mismatch',
                            'line_total_mismatch',
                            'missing_on_invoice',
                            'extra_on_invoice',
                            'supplier_mismatch',
                            'currency_mismatch',
                            'payment_terms_mismatch',
                            'header_total_mismatch',
                            'other',
                        ],
                    },
                    sku: { type: ['string', 'null'] },
                    description: { type: 'string' },
                    po: { type: ['string', 'number', 'null'] },
                    pi: { type: ['string', 'number', 'null'] },
                },
                required: ['type', 'sku', 'description', 'po', 'pi'],
            },
        },
        paymentTerms: {
            type: 'object',
            additionalProperties: false,
            properties: {
                paymentType: { type: 'string', enum: ['deposit', 'balance', 'full', 'other', 'unknown'] },
                amountDueNow: { type: ['number', 'null'] },
                currency: { type: ['string', 'null'] },
                depositPercentage: { type: ['number', 'null'] },
                invoiceTotal: { type: ['number', 'null'] },
                dueDate: { type: ['string', 'null'] },
                dueTerms: { type: ['string', 'null'] },
                beneficiaryName: { type: ['string', 'null'] },
                bankName: { type: ['string', 'null'] },
                bankAddress: { type: ['string', 'null'] },
                accountNumber: { type: ['string', 'null'] },
                iban: { type: ['string', 'null'] },
                swiftBic: { type: ['string', 'null'] },
                intermediaryBank: { type: ['string', 'null'] },
                paymentReference: { type: ['string', 'null'] },
                rawText: { type: ['string', 'null'] },
            },
            required: [
                'paymentType', 'amountDueNow', 'currency', 'depositPercentage', 'invoiceTotal',
                'dueDate', 'dueTerms', 'beneficiaryName', 'bankName', 'bankAddress',
                'accountNumber', 'iban', 'swiftBic', 'intermediaryBank', 'paymentReference', 'rawText',
            ],
        },
        overallVerdict: {
            type: 'string',
            enum: ['pass', 'minor_discrepancies', 'major_discrepancies'],
        },
        verdictExplanation: { type: 'string' },
    },
    required: ['summary', 'header', 'totals', 'discrepancies', 'paymentTerms', 'overallVerdict', 'verdictExplanation'],
};

// ── DB loading ───────────────────────────────────────────────────────────
// Reads the PO header + line items in the shape the model expects. Mirrors
// the aggregation that loadPoForPdf in orders.js uses so "the PO as the
// model sees it" matches the PDF we actually issued.
async function loadPoForCheck(conn, poId) {
    const [poRows] = await conn.query(
        `SELECT po.id, po.po_number, po.supplier, po.notes, po.currency,
                po.shipping_total, po.created_at,
                c.name AS company_name
           FROM purchase_orders po
           LEFT JOIN companies c ON c.id = po.company_id
          WHERE po.id = ? AND po.deleted_at IS NULL`,
        [poId]
    );
    if (!poRows.length) return null;
    const po = poRows[0];

    const [lineRows] = await conn.query(
        `SELECT jf_code, asin, product_name,
                SUM(quantity) AS quantity,
                unit_price
           FROM orders
          WHERE purchase_order_id = ? AND deleted_at IS NULL
          GROUP BY jf_code, asin, product_name, unit_price
          ORDER BY product_name`,
        [poId]
    );

    const lines = lineRows.map(r => {
        const qty = Number(r.quantity || 0);
        const unit = r.unit_price != null ? Number(r.unit_price) : 0;
        return {
            sku: r.jf_code || r.asin || null,
            description: r.product_name || '',
            quantity: qty,
            unitPrice: unit,
            lineTotal: Number((qty * unit).toFixed(4)),
        };
    });

    const subtotal = lines.reduce((sum, l) => sum + l.lineTotal, 0);
    const shipping = po.shipping_total != null ? Number(po.shipping_total) : 0;
    return {
        poNumber: po.po_number,
        supplier: po.supplier || null,
        buyer: po.company_name || null,
        currency: po.currency || 'USD',
        orderDate: po.created_at,
        notes: po.notes || null,
        lines,
        subtotal: Number(subtotal.toFixed(2)),
        shippingTotal: shipping,
        grandTotal: Number((subtotal + shipping).toFixed(2)),
    };
}

// ── PDF fetch ────────────────────────────────────────────────────────────
async function fetchInvoicePdf(s3Key) {
    const obj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: s3Key }));
    return Buffer.from(await obj.Body.transformToByteArray());
}

// ── Compare ──────────────────────────────────────────────────────────────
// Loads the PO and the invoice PDF, runs Gemini, returns the structured
// result plus usage metadata so the caller can surface cost.
async function compare(conn, { purchaseOrderId, invoiceId, model }) {
    const modelId = model || GEMINI_MODEL_DEFAULT;
    if (!process.env.GEMINI_API_KEY) {
        const err = new Error('GEMINI_API_KEY is not configured.');
        err.code = 'NOT_CONFIGURED';
        throw err;
    }

    const po = await loadPoForCheck(conn, purchaseOrderId);
    if (!po) {
        const err = new Error(`Purchase order ${purchaseOrderId} not found.`);
        err.code = 'PO_NOT_FOUND';
        throw err;
    }
    if (!po.lines.length) {
        const err = new Error(`Purchase order ${purchaseOrderId} has no line items.`);
        err.code = 'PO_NO_LINES';
        throw err;
    }

    const [invRows] = await conn.query(
        `SELECT id, purchase_order_id, filename, s3_key, content_type, file_size
           FROM purchase_order_invoices
          WHERE id = ? AND deleted_at IS NULL`,
        [invoiceId]
    );
    if (!invRows.length) {
        const err = new Error(`Invoice ${invoiceId} not found.`);
        err.code = 'INVOICE_NOT_FOUND';
        throw err;
    }
    const invoice = invRows[0];
    if (Number(invoice.purchase_order_id) !== Number(purchaseOrderId)) {
        const err = new Error(`Invoice ${invoiceId} does not belong to PO ${purchaseOrderId}.`);
        err.code = 'INVOICE_PO_MISMATCH';
        throw err;
    }

    let pdfBytes;
    try { pdfBytes = await fetchInvoicePdf(invoice.s3_key); }
    catch (e) {
        const err = new Error(`Failed to fetch invoice PDF from S3: ${e.message}`);
        err.code = 'PDF_FETCH_FAILED';
        err.cause = e;
        throw err;
    }

    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    const promptText = [
        SYSTEM_INSTRUCTION,
        '',
        '## Purchase Order (structured)',
        '```json',
        JSON.stringify(po, null, 2),
        '```',
        '',
        '## Proforma/Commercial Invoice',
        'The attached PDF is the supplier invoice. Read it and compare against the PO above. Return the structured discrepancy report per the JSON schema.',
    ].join('\n');

    let response;
    try {
        response = await ai.models.generateContent({
            model: modelId,
            contents: [
                { text: promptText },
                {
                    inlineData: {
                        mimeType: invoice.content_type || 'application/pdf',
                        data: pdfBytes.toString('base64'),
                    },
                },
            ],
            config: {
                responseMimeType: 'application/json',
                responseSchema: RESPONSE_SCHEMA,
            },
        });
    } catch (e) {
        log.error('[po-invoice-check] Gemini call failed', { message: e.message });
        const err = new Error(`Gemini call failed: ${e.message}`);
        err.code = 'MODEL_CALL_FAILED';
        err.cause = e;
        throw err;
    }

    const raw = response?.text || '';
    let parsed;
    try { parsed = JSON.parse(raw); }
    catch {
        log.error('[po-invoice-check] non-JSON response from Gemini', { preview: raw.slice(0, 300) });
        const err = new Error('Model did not return parseable JSON output.');
        err.code = 'MODEL_NO_JSON';
        throw err;
    }

    return {
        invoiceId: invoice.id,
        purchaseOrderId: po.poNumber,
        invoiceFilename: invoice.filename,
        check: parsed,
        po: {
            poNumber: po.poNumber,
            supplier: po.supplier,
            currency: po.currency,
            lineCount: po.lines.length,
            subtotal: po.subtotal,
            shippingTotal: po.shippingTotal,
            grandTotal: po.grandTotal,
        },
        modelUsed: modelId,
        usage: response?.usageMetadata ?? null,
    };
}

// ── Persistence of the extracted payment instructions ─────────────────────
// Coerce the model's free-form dueDate into a DATE column value, or null.
function toDateOnlyOrNull(v) {
    if (!v || typeof v !== 'string') return null;
    if (/^\d{4}-\d{2}-\d{2}/.test(v)) return v.slice(0, 10);
    const dt = new Date(v);
    return Number.isNaN(dt.getTime()) ? null : dt.toISOString().slice(0, 10);
}

// Persist the payment instructions Gemini extracted from a PI into
// purchase_order_invoice_payments. One row per invoice (UNIQUE), so a re-check
// refreshes the same row. payment_status is intentionally left out of the
// UPDATE clause: it's owned by the operator / payment job and must survive a
// re-extraction. Best-effort — a storage failure here must not fail the check.
async function upsertInvoicePaymentTerms(conn, { invoiceId, purchaseOrderId, checkId, paymentTerms, modelUsed }) {
    if (!paymentTerms || typeof paymentTerms !== 'object') return;
    const pt = paymentTerms;
    const s = (v, n) => (v == null ? null : String(v).slice(0, n));
    const num = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));
    await conn.query(
        `INSERT INTO purchase_order_invoice_payments
            (purchase_order_invoice_id, purchase_order_id, payment_type, amount_due, currency,
             deposit_percentage, invoice_total, due_date, due_terms,
             beneficiary_name, bank_name, bank_address, account_number, iban, swift_bic,
             intermediary_bank, payment_reference, raw_terms_text, extracted_from_check_id, model_used)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            payment_type = VALUES(payment_type), amount_due = VALUES(amount_due),
            currency = VALUES(currency), deposit_percentage = VALUES(deposit_percentage),
            invoice_total = VALUES(invoice_total), due_date = VALUES(due_date),
            due_terms = VALUES(due_terms), beneficiary_name = VALUES(beneficiary_name),
            bank_name = VALUES(bank_name), bank_address = VALUES(bank_address),
            account_number = VALUES(account_number), iban = VALUES(iban),
            swift_bic = VALUES(swift_bic), intermediary_bank = VALUES(intermediary_bank),
            payment_reference = VALUES(payment_reference), raw_terms_text = VALUES(raw_terms_text),
            extracted_from_check_id = VALUES(extracted_from_check_id), model_used = VALUES(model_used)`,
        [
            invoiceId, purchaseOrderId, s(pt.paymentType, 32), num(pt.amountDueNow),
            s(pt.currency, 3), num(pt.depositPercentage), num(pt.invoiceTotal),
            toDateOnlyOrNull(pt.dueDate), s(pt.dueTerms, 500),
            s(pt.beneficiaryName, 255), s(pt.bankName, 255), s(pt.bankAddress, 500),
            s(pt.accountNumber, 100), s(pt.iban, 64), s(pt.swiftBic, 32),
            s(pt.intermediaryBank, 255), s(pt.paymentReference, 255),
            pt.rawText != null ? String(pt.rawText).slice(0, 8000) : null, checkId, modelUsed || null,
        ]
    );
}

module.exports = {
    compare, loadPoForCheck, fetchInvoicePdf,
    toDateOnlyOrNull, upsertInvoicePaymentTerms,
};
