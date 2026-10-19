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
// don't ripple into the handlers. The supplier-portal check
// (src/services/pi-check.js) calls checkDocument() directly with the bytes it
// fetched from the portal's bucket, the same prompt and schema, plus
// SUPPLIER_PORTAL_ADDENDUM and a deadline.

const { GoogleGenAI } = require('@google/genai');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const log = require('../lib/logger');
const { withGeminiRetry } = require('../lib/gemini-retry');
const bank = require('../lib/bank-verification');
const { recordAuditStrict } = require('../lib/audit');
// Charges a PI bills that are not goods (mould, handling, …) come back as
// otherCharges. The page shows them as already inside the PI's own figures.
const { EXTRA_KINDS, OTHER_CHARGES_SCHEMA } = require('../lib/payment-extras');

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

2. Header totals — subtotal, shipping, grand total. Compute the PO grand total yourself from line_total values plus shipping. Flag any delta > 0.50 currency units between PO and PI. Charges the PI bills that are not goods and not the shipping the PO carries — mould or tooling, handling, freight, samples, testing, packaging, bank charges — and any discount or credit are NOT discrepancies: list them in otherCharges and compare the totals without them.

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

Also EXTRACT \`otherCharges\` — every amount the PI bills that is not a PO line item and not the shipping the PO carries: mould or tooling, handling, freight, samples, testing, packaging, bank charges, surcharges — and any discount or credit, as a NEGATIVE amount. One entry each: description (as printed), amount, kind (one of: ${EXTRA_KINDS.join(', ')}). Empty when there are none.

For every discrepancy:
- Give the exact PO value and the exact PI value so the operator can verify quickly.
- Use the SKU/jf_code where available, otherwise the product description.

Verdict rules:
- "pass" — no discrepancies, or only cosmetic differences (e.g. supplier name punctuation).
- "minor_discrepancies" — one or two small numeric mismatches OR missing PO-number reference, but nothing that would block payment.
- "major_discrepancies" — material differences that need supplier action: missing lines, wrong quantities, totals more than 1% apart, wrong supplier, wrong currency.

Be strict on numbers — even a 0.5% unit-price drift across a 16-line PO is material.`;

// Appended for a PI a supplier uploaded through the supplier portal, from the
// page of one PO: the PO is known before the upload, so a PI quoting another
// PO number is the wrong document (or the wrong page), not a typo.
const SUPPLIER_PORTAL_ADDENDUM = 'The supplier uploaded this for PO {poNumber}. If the document quotes a different PO number, add a discrepancy of type `other` and set overallVerdict to `major_discrepancies`.';

function supplierPortalInstructions(poNumber) {
    return SUPPLIER_PORTAL_ADDENDUM.replace('{poNumber}', String(poNumber == null ? '' : poNumber));
}

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
        otherCharges: OTHER_CHARGES_SCHEMA,
        overallVerdict: {
            type: 'string',
            enum: ['pass', 'minor_discrepancies', 'major_discrepancies'],
        },
        verdictExplanation: { type: 'string' },
    },
    required: ['summary', 'header', 'totals', 'discrepancies', 'paymentTerms', 'otherCharges', 'overallVerdict', 'verdictExplanation'],
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
// Loads the PO and the invoice PDF, runs Gemini (checkDocument, below),
// returns the structured result plus usage metadata so the caller can surface cost.
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

    const { result: parsed, usage } = await checkDocument({
        po, bytes: pdfBytes, mimeType: invoice.content_type || 'application/pdf', modelId,
    });

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
        usage,
    };
}

// ── Check one document ───────────────────────────────────────────────────
// The Gemini call itself, on a PO already loaded (loadPoForCheck) and the
// document's bytes already fetched — no database, no S3. compare() is a thin
// wrapper around it; the supplier-portal check (src/services/pi-check.js)
// calls it directly.
//   extraInstructions: added after the system instruction (e.g.
//     supplierPortalInstructions(po.poNumber)); omitted, the prompt is exactly
//     the one compare() has always sent.
//   deadline: epoch ms. Without it there is one attempt, as compare() always
//     made. With it, transient errors are retried (src/lib/gemini-retry.js)
//     while enough time remains, and each attempt is cut off before it.
// → { result, usage, model }. Throws coded errors as before: NOT_CONFIGURED,
// MODEL_CALL_FAILED (the SDK error as .cause), MODEL_NO_JSON.
async function checkDocument({ po, bytes, mimeType, modelId, extraInstructions = null, deadline = null }) {
    const model = modelId || GEMINI_MODEL_DEFAULT;
    if (!process.env.GEMINI_API_KEY) {
        const err = new Error('GEMINI_API_KEY is not configured.');
        err.code = 'NOT_CONFIGURED';
        throw err;
    }

    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    const promptText = [
        SYSTEM_INSTRUCTION,
        ...(extraInstructions ? ['', '## Additional instructions', String(extraInstructions)] : []),
        '',
        '## Purchase Order (structured)',
        '```json',
        JSON.stringify(po, null, 2),
        '```',
        '',
        '## Proforma/Commercial Invoice',
        'The attached PDF is the supplier invoice. Read it and compare against the PO above. Return the structured discrepancy report per the JSON schema.',
    ].join('\n');

    const request = () => {
        const config = {
            responseMimeType: 'application/json',
            responseSchema: RESPONSE_SCHEMA,
        };
        if (deadline != null) {
            // Leave a minute after the call for the caller to write its result.
            const left = deadline - Date.now() - 60 * 1000;
            config.httpOptions = { timeout: Math.max(15 * 1000, Math.min(180 * 1000, left)) };
        }
        return ai.models.generateContent({
            model,
            contents: [
                { text: promptText },
                {
                    inlineData: {
                        mimeType: mimeType || 'application/pdf',
                        data: Buffer.from(bytes).toString('base64'),
                    },
                },
            ],
            config,
        });
    };

    let response;
    try {
        response = deadline != null
            ? await withGeminiRetry(`po-invoice-check(${model})`, request, 5, { deadline })
            : await request();
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
    return { result: parsed, usage: response?.usageMetadata ?? null, model };
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
// re-extraction. Best-effort — a storage failure here must not fail the check
// (recordInvoiceCheck in orders.js catches it; the PI import calls it inside
// its transaction and lets errors, deadlocks included, propagate).
//
// A PI that came through the supplier portal (purchase_order_invoices
// .pi_submission_id set) also carries bank verification
// (src/lib/bank-verification.js): see upsertPortalPaymentTerms below. Every
// other PI is written exactly as before, and its bank_* verification columns
// stay NULL (not applicable).
async function upsertInvoicePaymentTerms(conn, opts) {
    const [inv] = await conn.query(
        `SELECT pi_submission_id FROM purchase_order_invoices WHERE id = ?`,
        [opts.invoiceId]
    );
    if (inv.length && inv[0].pi_submission_id != null) return upsertPortalPaymentTerms(conn, opts);
    return upsertLegacyPaymentTerms(conn, opts);
}

const clip = (v, n) => (v == null ? null : String(v).slice(0, n));
const numOrNull = (v) => (v == null || v === '' || Number.isNaN(Number(v)) ? null : Number(v));

async function upsertLegacyPaymentTerms(conn, { invoiceId, purchaseOrderId, checkId, paymentTerms, modelUsed }) {
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

// A portal PI's payment row. Always written, so the PI's bank state is explicit
// from the moment it is imported:
//   - paymentTerms null (the supplier's check was unusable): creates the row
//     with empty terms, an empty fingerprint and 'needs_verification'; an
//     existing row is left alone. The later re-check fills it in.
//   - otherwise the extracted terms and bank fields are written with
//     bank_fingerprint, as for any PI.
//   - verification { fingerprint, details, method, note, by } (the import's
//     bank gate passed, or staff verified): the verified fields are written too.
// bank_verification_status is computed in the same statement —
//   fingerprint non-empty AND = bank_verified_fingerprint ? verified : needs_verification
// — so there is no transient needs_verification and no false audit row. A
// move from verified to needs_verification (a re-check read different
// details) is audited as bank_details_unverified on the purchase order.
// → { status, previousStatus, fingerprint, created }
async function upsertPortalPaymentTerms(conn, { invoiceId, purchaseOrderId, checkId, paymentTerms, modelUsed, verification = null, userEmail = null }) {
    const pt = paymentTerms && typeof paymentTerms === 'object' ? paymentTerms : null;
    // A plain read: a locking read of a row that does not exist yet would take
    // a gap lock that two imports could deadlock on.
    const [prevRows] = await conn.query(
        `SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`,
        [invoiceId]
    );
    const prev = prevRows[0] || null;
    if (!pt && prev) {
        return { status: prev.bank_verification_status || null, previousStatus: prev.bank_verification_status || null, fingerprint: prev.bank_fingerprint || '', created: false };
    }

    const details = bank.bankDetailsOf(pt);
    const fingerprint = bank.bankFingerprint(details);
    const p = pt || {};
    const cols = [
        'purchase_order_invoice_id', 'purchase_order_id', 'payment_type', 'amount_due', 'currency',
        'deposit_percentage', 'invoice_total', 'due_date', 'due_terms',
        'beneficiary_name', 'bank_name', 'bank_address', 'account_number', 'iban', 'swift_bic',
        'intermediary_bank', 'payment_reference', 'raw_terms_text', 'extracted_from_check_id', 'model_used',
        'bank_fingerprint',
    ];
    const vals = [
        invoiceId, purchaseOrderId, clip(p.paymentType, 32), numOrNull(p.amountDueNow),
        clip(p.currency, 3), numOrNull(p.depositPercentage), numOrNull(p.invoiceTotal),
        toDateOnlyOrNull(p.dueDate), clip(p.dueTerms, 500),
        details.beneficiaryName, details.bankName, details.bankAddress,
        details.accountNumber, details.iban, details.swiftBic,
        details.intermediaryBank, clip(p.paymentReference, 255),
        p.rawText != null ? String(p.rawText).slice(0, 8000) : null, checkId || null, modelUsed || null,
        fingerprint,
    ];
    const updates = cols.slice(2).map(c => `${c} = VALUES(${c})`);
    if (verification) {
        cols.push('bank_verified_fingerprint', 'bank_verified_json', 'bank_verified_by',
            'bank_verification_method', 'bank_verification_note');
        vals.push(verification.fingerprint || '', JSON.stringify(bank.bankDetailsOf(verification.details || details)),
            clip(verification.by, 255), clip(verification.method, 32), clip(verification.note, 1000));
        updates.push('bank_verified_fingerprint = VALUES(bank_verified_fingerprint)',
            'bank_verified_json = VALUES(bank_verified_json)', 'bank_verified_by = VALUES(bank_verified_by)',
            'bank_verified_at = NOW()', 'bank_verification_method = VALUES(bank_verification_method)',
            'bank_verification_note = VALUES(bank_verification_note)');
    }
    // A new row's status, from what this statement writes; an existing row's
    // is recomputed in the UPDATE clause from its (possibly just updated)
    // verified fingerprint — assignments there are applied left to right.
    const insertStatus = bank.statusFor(fingerprint, verification ? verification.fingerprint : null);
    cols.push('bank_verification_status');
    vals.push(insertStatus);
    updates.push(`bank_verification_status = IF(VALUES(bank_fingerprint) <> '' AND VALUES(bank_fingerprint) = bank_verified_fingerprint, '${bank.VERIFIED}', '${bank.NEEDS_VERIFICATION}')`);
    const placeholders = cols.map(() => '?');
    let colSql = cols.join(', ');
    let valSql = placeholders.join(', ');
    if (verification) { colSql += ', bank_verified_at'; valSql += ', NOW()'; }

    await conn.query(
        `INSERT INTO purchase_order_invoice_payments (${colSql})
         VALUES (${valSql})
         ON DUPLICATE KEY UPDATE ${updates.join(', ')}`,
        vals
    );
    const [after] = await conn.query(
        `SELECT bank_verification_status FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`,
        [invoiceId]
    );
    const status = after[0] ? after[0].bank_verification_status : null;
    const previousStatus = prev ? prev.bank_verification_status || null : null;
    if (previousStatus === bank.VERIFIED && status === bank.NEEDS_VERIFICATION) {
        await recordAuditStrict(conn, {
            entityType: 'purchase_order', entityId: Number(purchaseOrderId), action: 'bank_details_unverified',
            before: { invoiceId: Number(invoiceId), bankVerificationStatus: previousStatus, bank: bank.bankDetailsOf(prev, { fromRow: true }) },
            after: { invoiceId: Number(invoiceId), bankVerificationStatus: status, bank: details, checkId: checkId || null },
            userEmail,
        });
    }
    return { status, previousStatus, fingerprint, created: !prev };
}

module.exports = {
    compare, checkDocument, loadPoForCheck, fetchInvoicePdf,
    toDateOnlyOrNull, upsertInvoicePaymentTerms,
    RESPONSE_SCHEMA, SUPPLIER_PORTAL_ADDENDUM, supplierPortalInstructions,
    GEMINI_MODEL_DEFAULT,
};
