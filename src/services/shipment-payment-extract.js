'use strict';

// Reads a supplier's BALANCE invoice (or remittance advice) for one shipment
// and turns it into a payment record with a per-PO split.
//
// The PO-vs-PI check (po-invoice-check.js) answers "does this invoice match
// the purchase order?". This answers a different question: "this document
// bills us for the goods in container 311 — how much, by when, and which of
// the purchase orders on board does it cover?". So the model is handed the
// shipment and every PO travelling in it, and asked to map the document's own
// references onto our PO numbers.
//
// What it will never do is mark anything paid. An invoice is a request for
// money; the record lands 'pending' and an operator flips it once the money
// has actually gone. A re-run refreshes the figures of a record that is still
// pending and machine-written, and leaves anything an operator has touched
// exactly as they left it.
//
// Called in the background by the upload and /extract routes. The pool has a
// single connection, so the model call happens with no connection held: load,
// release, call Gemini, reacquire, write.

const { GoogleGenAI } = require('@google/genai');
const log = require('../lib/logger');
const { fetchInvoicePdf, toDateOnlyOrNull } = require('./po-invoice-check');
const { withGeminiRetry } = require('./supplier-email-check');
const { recordAudit } = require('../lib/audit');
const {
    supplierKey, loadShipmentContext, loadMemberPurchaseOrders, shareAllocations,
} = require('./shipment-payments');

// Same tiering as the QC reader: Flash does this comfortably, Pro is the
// fallback when Flash refuses a scan.
const GEMINI_MODEL_DEFAULT = 'gemini-3-flash-preview';
const FALLBACK_MODEL = 'gemini-3.1-pro-preview';

const SYSTEM_INSTRUCTION = `You read supplier payment documents for a UK medical-supplies importer and return structured JSON.

You are given ONE document (a PDF or image) and the shipment it was filed against, including every purchase order travelling in that shipment with its PO number, the supplier's own spelling of its name, its line items and the value it has on board.

Decide first what the document IS:
- "balance_invoice" — the supplier billing for goods being shipped (often "balance", "final payment", "70% against B/L"). This is the common case.
- "deposit_invoice" — an upfront/proforma asking for a deposit before production.
- "remittance" — evidence that a payment was MADE (a bank transfer advice, SWIFT copy, payment receipt).
- "other" — anything else.

Then extract:
- supplierName — the issuing supplier exactly as printed (for a remittance: the beneficiary who was paid).
- invoiceNumber, invoiceDate (YYYY-MM-DD), dueDate (YYYY-MM-DD if stated or clearly derivable, e.g. "30 days from B/L" with the B/L date on the document).
- paymentDate — for a remittance only: the date the money was sent (YYYY-MM-DD). Null on anything else.
- currency — ISO code of the amounts.
- totalAmount — the document's grand total.
- amountDueNow — the amount actually payable against THIS document. For a balance invoice that is the balance, not the whole order value. Null if it states no payable amount.
- depositDeducted — any deposit or advance the document nets off (often "less 30% deposit"), as a positive number; null if none.
- dueTerms — the payment-timing wording, verbatim and short.
- containerRefs, blRefs — container numbers and bill-of-lading numbers printed on the document.
- poRefs — every purchase order reference printed anywhere on it.
- lines — one entry per billed line: poRef (mapped to one of OUR PO numbers when you can), jfCode (OUR product code for that line, chosen from the purchase orders you were given, when the product clearly matches; else null), piRef (the supplier's own invoice/PI number if the line cites one), sku (the supplier's own code as printed), description, qty, unitPrice, amount.
- bank — the beneficiary and remittance details exactly as printed. Never reformat or invent account numbers.
- rawText — the payment-terms and bank-details section copied verbatim, so a human can check the parsed figures.
- confidence — "high" when the figures and the purchase orders are unambiguous, "low" when you are guessing.

Rules:
- Map references to the PO numbers you were given whenever the digits match, even if the document writes them differently (PO00299, PO_00299J, 299). If you cannot map a reference, return it verbatim in poRef and leave the mapping to a human.
- NEVER invent an amount. If a figure is not on the document, return null.
- The sum of the line amounts should reconcile to totalAmount; if it does not, still report both faithfully.
- Amounts are numbers, not strings, with no currency symbols or thousands separators.`;

const RESPONSE_SCHEMA = {
    type: 'object',
    additionalProperties: false,
    properties: {
        documentKind: { type: 'string', enum: ['balance_invoice', 'deposit_invoice', 'remittance', 'other'] },
        supplierName: { type: ['string', 'null'] },
        invoiceNumber: { type: ['string', 'null'] },
        invoiceDate: { type: ['string', 'null'] },
        dueDate: { type: ['string', 'null'] },
        paymentDate: { type: ['string', 'null'] },
        currency: { type: ['string', 'null'] },
        totalAmount: { type: ['number', 'null'] },
        amountDueNow: { type: ['number', 'null'] },
        depositDeducted: { type: ['number', 'null'] },
        dueTerms: { type: ['string', 'null'] },
        containerRefs: { type: 'array', items: { type: 'string' } },
        blRefs: { type: 'array', items: { type: 'string' } },
        poRefs: { type: 'array', items: { type: 'string' } },
        lines: {
            type: 'array',
            items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                    poRef: { type: ['string', 'null'] },
                    jfCode: { type: ['string', 'null'] },
                    piRef: { type: ['string', 'null'] },
                    sku: { type: ['string', 'null'] },
                    description: { type: ['string', 'null'] },
                    qty: { type: ['number', 'null'] },
                    unitPrice: { type: ['number', 'null'] },
                    amount: { type: ['number', 'null'] },
                },
                required: ['poRef', 'jfCode', 'piRef', 'sku', 'description', 'qty', 'unitPrice', 'amount'],
            },
        },
        bank: {
            type: 'object',
            additionalProperties: false,
            properties: {
                beneficiaryName: { type: ['string', 'null'] },
                bankName: { type: ['string', 'null'] },
                accountNumber: { type: ['string', 'null'] },
                iban: { type: ['string', 'null'] },
                swiftBic: { type: ['string', 'null'] },
                intermediaryBank: { type: ['string', 'null'] },
                paymentReference: { type: ['string', 'null'] },
            },
            required: ['beneficiaryName', 'bankName', 'accountNumber', 'iban', 'swiftBic', 'intermediaryBank', 'paymentReference'],
        },
        rawText: { type: ['string', 'null'] },
        confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
    },
    required: [
        'documentKind', 'supplierName', 'invoiceNumber', 'invoiceDate', 'dueDate', 'paymentDate', 'currency',
        'totalAmount', 'amountDueNow', 'depositDeducted', 'dueTerms', 'containerRefs', 'blRefs',
        'poRefs', 'lines', 'bank', 'rawText', 'confidence',
    ],
};

// ── Reference matching ───────────────────────────────────────────────────
// Suppliers write our PO numbers every way imaginable: "PO_00299J", "PO 299",
// "299", "po00299j". Compare on letters+digits, then on the digit core with
// leading zeros stripped, and only when that core identifies exactly one PO on
// the shipment — a two-way guess is worse than no guess.
const norm = v => String(v ?? '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const digitCore = v => {
    const m = norm(v).match(/\d+/g);
    return m ? m.join('').replace(/^0+/, '') : '';
};

function buildRefIndex(memberPos) {
    const byExact = new Map();
    const byCore = new Map();
    for (const po of memberPos) {
        byExact.set(norm(po.poNumber), po);
        const core = digitCore(po.poNumber);
        if (!core) continue;
        if (!byCore.has(core)) byCore.set(core, []);
        byCore.get(core).push(po);
    }
    return { byExact, byCore };
}

function matchPoRef(ref, index) {
    const exact = index.byExact.get(norm(ref));
    if (exact) return exact;
    const core = digitCore(ref);
    if (!core) return null;
    const hits = index.byCore.get(core);
    return hits && hits.length === 1 ? hits[0] : null;
}

/** Allocations read off line values, scaled so they cover `amount` in the same
 *  proportion the lines covered `lineTotal` — in whole cents, remainder to the
 *  largest, so a full set of lines sums to `amount` exactly. */
function scaleToAmount(allocations, lineTotal, amount) {
    const weights = allocations.map(a => Math.round(a.amount * 100));
    const weightSum = weights.reduce((a, b) => a + b, 0);
    if (!(weightSum > 0) || !(lineTotal > 0)) return allocations;
    const target = Math.round(amount * 100 * (weightSum / 100 / lineTotal));
    const parts = weights.map(w => Math.floor((target * w) / weightSum));
    let left = target - parts.reduce((a, b) => a + b, 0);
    const order = weights.map((_, i) => i).sort((a, b) => weights[b] - weights[a] || a - b);
    for (let i = 0; left > 0; i = (i + 1) % order.length) { parts[order[i]] += 1; left -= 1; }
    return allocations.map((a, i) => ({ ...a, amount: parts[i] / 100 }));
}

/** Extracted lines/refs → allocations. Lines win; a document that names only
 *  PO references splits by what each has on board; one that names nothing is
 *  left for a human (needsAllocation). */
function matchLinesToPos(extract, memberPos, amount) {
    const index = buildRefIndex(memberPos);
    const unmatched = [];
    const byPo = new Map();
    let lineTotal = 0;

    for (const line of extract.lines || []) {
        const value = Number(line.amount);
        if (!Number.isFinite(value) || value <= 0) continue;
        lineTotal += value;
        const po = line.poRef ? matchPoRef(line.poRef, index) : null;
        if (!po) { unmatched.push({ poRef: line.poRef ?? null, amount: value }); continue; }
        byPo.set(po.id, { po, amount: (byPo.get(po.id)?.amount ?? 0) + value });
    }

    if (byPo.size || unmatched.length) {
        let allocations = [...byPo.values()].map(x => ({
            purchaseOrderId: x.po.id, poRef: x.po.poNumber, amount: Math.round(x.amount * 100) / 100, source: 'extracted',
        }));
        for (const u of unmatched) {
            if (!u.poRef) continue;
            allocations.push({ purchaseOrderId: null, poRef: String(u.poRef).slice(0, 100), amount: Math.round(u.amount * 100) / 100, source: 'extracted' });
        }
        // The lines bill the goods; the payable is often the goods less the
        // deposit ("LESS 30% DEPOSIT"). A split taken straight off the lines
        // would then exceed the balance and could never be marked paid, so it
        // is scaled to the amount due, each PO keeping its share of the lines.
        // Lines that add up to LESS are left as read: the gap stays unallocated
        // and is flagged, rather than stretched over POs the document may not
        // cover.
        const scaled = amount > 0 && lineTotal > amount + 0.005 && allocations.length > 0;
        if (scaled) allocations = scaleToAmount(allocations, lineTotal, amount);
        return {
            allocations, matchedLines: byPo.size, unmatchedLines: unmatched.length,
            lineTotal: Math.round(lineTotal * 100) / 100, scaledToAmount: scaled,
        };
    }

    // No usable lines: fall back to the PO references on the document.
    const named = [...new Set((extract.poRefs || []).map(r => matchPoRef(r, index)).filter(Boolean))];
    if (named.length && amount > 0) {
        return {
            allocations: shareAllocations(amount, named, { source: 'extracted' }),
            matchedLines: 0, unmatchedLines: 0, lineTotal: 0, splitByShare: true,
        };
    }
    return { allocations: [], matchedLines: 0, unmatchedLines: 0, lineTotal: 0, needsAllocation: true };
}

// ── Does this document belong here? ──────────────────────────────────────
// What the reader found is compared with what we hold for the shipment and
// the supplier it was uploaded against. Each check is true (it ties the
// document here), false (it points somewhere else) or null (the document, or
// our records, say nothing). One false makes the document a mismatch; a
// container or PO reference that ties it here, with no false, makes it a
// match; anything else is unconfirmed — normal for a bank remittance, which
// names the payee and nothing else, and worth a second look on an invoice.
const LEGAL_WORDS = new Set([
    'CO', 'COMPANY', 'CORP', 'CORPORATION', 'INC', 'LTD', 'LIMITED', 'LLC', 'PLC', 'GMBH', 'AG', 'SA',
    'SAS', 'SRL', 'SPA', 'BV', 'NV', 'AB', 'AS', 'OY', 'KG', 'THE', 'AND', 'OF', 'GROUP', 'TRADING',
    'INTERNATIONAL', 'INTL', 'IMPORT', 'EXPORT', 'SANAYI', 'TICARET', 'VE',
]);
const nameTokens = v => String(v ?? '').toUpperCase().split(/[^A-Z0-9]+/).filter(t => t.length >= 3 && !LEGAL_WORDS.has(t));

function sameSupplier(a, b) {
    const na = norm(a);
    const nb = norm(b);
    if (!na || !nb) return false;
    if (na === nb) return true;
    const [short, long] = na.length <= nb.length ? [na, nb] : [nb, na];
    if (short.length >= 5 && long.includes(short)) return true;
    // Every distinctive word of the shorter name must be in the longer one:
    // "SUNMED" is "Suzhou Sunmed Co., Ltd", but two suppliers in the same
    // city share "Suzhou" and are not each other.
    const ta = nameTokens(a);
    const tb = nameTokens(b);
    if (!ta.length || !tb.length) return false;
    const [fewer, more] = ta.length <= tb.length ? [ta, new Set(tb)] : [tb, new Set(ta)];
    return fewer.every(t => more.has(t));
}

// Container numbers are 11 characters and documents decorate them ("CSGU
// 220587-0", "CSGU2205870/40HQ"); our internal references are three digits and
// must match whole, or "312" would be found inside every other box number.
function sameRef(printed, ours) {
    const a = norm(printed);
    const b = norm(ours);
    if (!a || !b) return false;
    return a === b || (b.length >= 7 && a.includes(b));
}

const fmtAmount = (n, currency) =>
    `${currency ? `${currency} ` : ''}${Number(n).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

/** { verdict: 'match' | 'unconfirmed' | 'mismatch', checks: [{ key, ok, text }], onBoard, currency } */
function assessFit({ extract, shipment, supplierName, memberPos, amount }) {
    const checks = [];
    const add = (key, ok, text) => checks.push({ key, ok, text });
    const pos = memberPos || [];
    const mine = supplierName
        ? pos.filter(p => p.supplierKey === supplierKey(supplierName) || sameSupplier(p.supplier, supplierName))
        : pos;

    // Who issued it (for a remittance, who was paid).
    const printed = String(extract.supplierName || extract.bank?.beneficiaryName || '').trim().replace(/[.,\s]+$/, '');
    const names = [...new Set([supplierName, ...mine.map(p => p.supplier)].filter(Boolean))];
    const by = extract.documentKind === 'remittance' ? 'Paid to' : 'Issued by';
    if (!printed) add('supplier', null, 'No supplier name on it.');
    else if (!names.length) add('supplier', null, `${by} ${printed}.`);
    else if (names.some(n => sameSupplier(printed, n))) add('supplier', true, `${by} ${printed}.`);
    else add('supplier', false, `${by} ${printed}, not ${supplierName || names[0]}.`);

    // Which box. A container we hold no number for cannot be contradicted.
    const docRefs = [...new Set([...(extract.containerRefs || []), ...(extract.blRefs || [])].map(r => String(r).trim()).filter(Boolean))];
    const s = shipment || {};
    const ours = [s.reference, s.tracking_ref, s.bl_number, s.booking_ref].filter(v => norm(v));
    const heldNumber = [s.tracking_ref, s.bl_number, s.booking_ref].some(v => norm(v));
    const label = s.tracking_ref ? `${s.tracking_ref} (${s.reference})` : String(s.reference ?? '');
    if (!docRefs.length) {
        add('container', null, 'Names no container or bill of lading.');
    } else {
        const hits = docRefs.filter(r => ours.some(o => sameRef(r, o)));
        const others = docRefs.length - hits.length;
        if (hits.length) add('container', true, `Names ${hits[0]}${others ? ` and ${others} other container${others === 1 ? '' : 's'}` : ''}.`);
        else if (heldNumber) add('container', false, `Names ${docRefs.slice(0, 3).join(', ')} — this shipment is ${label}.`);
        else add('container', null, `Names ${docRefs.slice(0, 3).join(', ')}; ${s.reference} has no container number on file to compare.`);
    }

    // Which purchase orders.
    const index = buildRefIndex(pos);
    const refs = [...new Set([...(extract.poRefs || []), ...(extract.lines || []).map(l => l && l.poRef)]
        .map(r => String(r ?? '').trim()).filter(Boolean))];
    if (!refs.length) {
        add('pos', null, 'Names none of our purchase orders.');
    } else {
        const hits = new Map();
        const misses = [];
        for (const r of refs) {
            const po = matchPoRef(r, index);
            if (po) hits.set(po.id, po);
            else misses.push(r);
        }
        const found = [...hits.values()];
        const ownFound = found.filter(p => mine.includes(p));
        const missNote = misses.length ? `; ${misses.slice(0, 3).join(', ')}${misses.length > 3 ? '…' : ''} ${misses.length === 1 ? 'is' : 'are'} not on this shipment` : '';
        if (ownFound.length) add('pos', true, `Names ${ownFound.map(p => p.poNumber).join(', ')}${missNote}.`);
        else if (found.length) add('pos', false, `Names ${found.map(p => p.poNumber).join(', ')}, filed under ${found[0].supplier || 'another supplier'}.`);
        else add('pos', false, `Names ${misses.slice(0, 3).join(', ')}${misses.length > 3 ? '…' : ''} — not on this shipment.`);
    }

    // Money: the currency, then the amount against what is on board.
    const currencies = [...new Set(mine.map(p => p.currency).filter(Boolean))];
    const currency = extract.currency ? String(extract.currency).toUpperCase().slice(0, 3) : null;
    const onBoard = Math.round(mine.reduce((a, p) => a + (Number(p.valueInShipment) || 0), 0) * 100) / 100;
    if (currency && currencies.length && !currencies.includes(currency)) {
        add('currency', false, `In ${currency}; the purchase orders are in ${currencies.join('/')}.`);
    } else if (amount != null && amount > 0 && onBoard > 0) {
        const cur = currency || currencies[0] || null;
        if (amount > onBoard * 1.05 + 1) {
            add('amount', false, `${fmtAmount(amount, cur)} is more than the ${fmtAmount(onBoard, cur)} of goods this supplier has on board.`);
        } else {
            add('amount', null, `${Math.round((amount / onBoard) * 100)}% of the ${fmtAmount(onBoard, cur)} this supplier has on board.`);
        }
    }

    const verdict = checks.some(c => c.ok === false)
        ? 'mismatch'
        : checks.some(c => c.ok === true && (c.key === 'container' || c.key === 'pos')) ? 'match' : 'unconfirmed';
    return { verdict, checks, onBoard, currency: currencies[0] ?? null };
}

// ── Invoice vs container ─────────────────────────────────────────────────
// The PO-vs-PI check asks "does this invoice match the order?". This asks
// "does this invoice match what is actually in the box?": every line the
// supplier bills against the lines they have on board (quantity, unit price,
// line total), and the amount they ask for against what the terms say the
// balance should be — the goods on board less the deposit already invoiced.
const LINE_PRICE_EPS = 0.005;
const LINE_TOTAL_EPS = 0.5;
const descKey = v => String(v ?? '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();

function compareInvoiceToContainer({ extract, context, supplierName }) {
    const key = supplierKey(supplierName);
    const mine = (context.purchaseOrders || []).filter(p => p.supplierKey === key || sameSupplier(p.supplier, supplierName));
    const index = buildRefIndex(mine);
    const ours = [];
    for (const po of mine) {
        for (const l of po.lines || []) {
            ours.push({
                poId: po.id, poNumber: po.poNumber, jfCode: l.jfCode || null, productName: l.productName || null,
                qty: l.quantity != null ? Number(l.quantity) : null, unitPrice: l.unitPrice != null ? Number(l.unitPrice) : null,
                total: l.quantity != null && l.unitPrice != null ? Math.round(Number(l.quantity) * Number(l.unitPrice) * 100) / 100 : null,
                matched: false,
            });
        }
    }
    const goodsOnBoard = Math.round(ours.reduce((a, l) => a + (l.total || 0), 0) * 100) / 100;

    // Match each invoice line: our code first, then the PO plus the product
    // name, then a PO that has only one line left.
    const lines = [];
    const extra = [];
    for (const raw of extract.lines || []) {
        const po = raw.poRef ? matchPoRef(raw.poRef, index) : null;
        const code = norm(raw.jfCode || raw.sku);
        const desc = descKey(raw.description);
        const candidates = ours.filter(l => !l.matched && (!po || l.poId === po.id));
        let hit = code ? candidates.find(l => norm(l.jfCode) === code) : null;
        if (!hit && desc) hit = candidates.find(l => descKey(l.productName) === desc);
        if (!hit && po && candidates.length === 1) hit = candidates[0];
        if (!hit) {
            extra.push({ poRef: raw.poRef ?? null, jfCode: raw.jfCode ?? null, sku: raw.sku ?? null, description: raw.description ?? null, qty: raw.qty ?? null, unitPrice: raw.unitPrice ?? null, amount: raw.amount ?? null });
            continue;
        }
        hit.matched = true;
        const issues = [];
        const invQty = raw.qty != null ? Number(raw.qty) : null;
        const invPrice = raw.unitPrice != null ? Number(raw.unitPrice) : null;
        const invTotal = raw.amount != null ? Number(raw.amount) : null;
        if (invQty != null && hit.qty != null && invQty !== hit.qty) issues.push(`quantity ${invQty.toLocaleString('en-GB')} on the invoice, ${hit.qty.toLocaleString('en-GB')} in the container`);
        if (invPrice != null && hit.unitPrice != null && Math.abs(invPrice - hit.unitPrice) > LINE_PRICE_EPS) issues.push(`unit price ${invPrice} on the invoice, ${hit.unitPrice} on the PO`);
        if (invTotal != null && hit.total != null && Math.abs(invTotal - hit.total) > LINE_TOTAL_EPS) issues.push(`line total ${invTotal.toFixed(2)} on the invoice, ${hit.total.toFixed(2)} in the container`);
        lines.push({
            poNumber: hit.poNumber, jfCode: hit.jfCode, productName: hit.productName,
            ourQty: hit.qty, invoiceQty: invQty, ourUnitPrice: hit.unitPrice, invoiceUnitPrice: invPrice, ourTotal: hit.total, invoiceTotal: invTotal,
            issues,
        });
    }
    const missing = ours.filter(l => !l.matched).map(l => ({ poNumber: l.poNumber, jfCode: l.jfCode, productName: l.productName, qty: l.qty, total: l.total }));

    // What the balance should be. The deposit on file (a deposit PI's
    // percentage) beats what the invoice says it deducted; with neither, the
    // whole goods value is expected.
    const pcts = mine.map(p => p.deposit && p.deposit.depositPercentage != null ? Number(p.deposit.depositPercentage) : null).filter(v => v != null && v > 0);
    let depositBasis = 'none';
    let depositExpected = 0;
    if (pcts.length) {
        depositBasis = 'pi_percentage';
        // Each PO's own percentage on its own goods.
        depositExpected = Math.round(mine.reduce((a, p) => {
            const pct = p.deposit && p.deposit.depositPercentage != null ? Number(p.deposit.depositPercentage) : 0;
            const goods = ours.filter(l => l.poId === p.id).reduce((s, l) => s + (l.total || 0), 0);
            return a + goods * pct / 100;
        }, 0) * 100) / 100;
    } else if (extract.depositDeducted != null && Number(extract.depositDeducted) > 0) {
        depositBasis = 'invoice_deduction';
        depositExpected = Math.round(Number(extract.depositDeducted) * 100) / 100;
    }
    const expectedBalance = Math.round((goodsOnBoard - depositExpected) * 100) / 100;
    const invoiceBalance = extract.amountDueNow != null ? Number(extract.amountDueNow) : extract.totalAmount != null ? Number(extract.totalAmount) : null;
    const balanceDelta = invoiceBalance == null ? 0 : Math.round((invoiceBalance - expectedBalance) * 100) / 100;
    const balanceTolerance = Math.max(1, expectedBalance * 0.005);

    const lineTrouble = lines.some(l => l.issues.length) || missing.length > 0 || extra.length > 0;
    const balanceTrouble = invoiceBalance != null && Math.abs(balanceDelta) > balanceTolerance;
    const verdict = !ours.length || (!lines.length && !extra.length && invoiceBalance == null)
        ? 'unverified'
        : !lines.length && !extra.length
            ? (balanceTrouble ? 'differs' : 'unverified')
            : (lineTrouble || balanceTrouble ? 'differs' : 'match');
    return {
        verdict,
        matchedLines: lines.length, lines, missingOurLines: missing, extraInvoiceLines: extra,
        goodsOnBoard, invoiceGoods: extract.totalAmount != null ? Number(extract.totalAmount) : null,
        depositBasis, depositExpected, expectedBalance, invoiceBalance, balanceDelta, balanceTolerance,
    };
}

// ── The model call ───────────────────────────────────────────────────────
function promptFor(context) {
    const s = context.shipment;
    if (!s) {
        return [
            '## Context',
            'This document was uploaded against a supplier, not a shipment — most likely a bank payment confirmation or remittance advice. Read it as it is.',
        ].join('\n');
    }
    const lines = [
        `## Shipment`,
        `reference: ${s.reference ?? '(none)'}`,
        `mode: ${s.mode ?? 'unknown'}`,
        s.tracking_ref ? `carrier reference: ${s.tracking_ref}` : null,
        s.bl_number ? `bill of lading: ${s.bl_number}` : null,
        s.booking_ref ? `booking reference: ${s.booking_ref}` : null,
        s.vessel_name ? `vessel: ${s.vessel_name}` : null,
        s.etd ? `ETD: ${s.etd}` : null,
        s.eta ? `ETA: ${s.eta}` : null,
        '',
        `## Purchase orders on this shipment`,
        'Map every reference on the document to one of these PO numbers when the digits allow it.',
        '',
    ].filter(Boolean);
    for (const po of context.purchaseOrders) {
        lines.push(`### ${po.poNumber}`);
        lines.push(`supplier as filed: ${po.supplier ?? '(none)'} · currency ${po.currency} · value on board ${po.valueInShipment}`);
        if (po.deposit) {
            lines.push(`deposit already recorded: ${po.deposit.amountDue ?? '?'} ${po.deposit.currency ?? ''} (${po.deposit.paymentStatus ?? 'unknown'})`);
        }
        for (const l of po.lines.slice(0, 40)) {
            lines.push(`- ${l.jfCode ?? ''} ${l.productName ?? ''} · qty ${l.quantity ?? '?'} @ ${l.unitPrice ?? '?'}`);
        }
        lines.push('');
    }
    return lines.join('\n');
}

/** Runs the model over one document. Returns { parsed, modelUsed, usage }. */
async function readDocument({ s3Key, contentType, context, model }) {
    if (!process.env.GEMINI_API_KEY) {
        const err = new Error('GEMINI_API_KEY is not configured.');
        err.code = 'NOT_CONFIGURED';
        throw err;
    }
    let bytes;
    try {
        bytes = await fetchInvoicePdf(s3Key);
    } catch (e) {
        const err = new Error(`Could not read the uploaded document: ${e.message}`);
        err.code = 'PDF_FETCH_FAILED';
        err.cause = e;
        throw err;
    }
    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    const promptText = [SYSTEM_INSTRUCTION, '', promptFor(context)].join('\n');
    const models = [...new Set([model || GEMINI_MODEL_DEFAULT, FALLBACK_MODEL])];

    let lastErr;
    for (const modelId of models) {
        try {
            const response = await withGeminiRetry(`shipment-payment-extract(${modelId})`, () => ai.models.generateContent({
                model: modelId,
                contents: [
                    { text: promptText },
                    { inlineData: { mimeType: contentType || 'application/pdf', data: bytes.toString('base64') } },
                ],
                config: { responseMimeType: 'application/json', responseSchema: RESPONSE_SCHEMA },
            }));
            const raw = response?.text || '';
            let parsed;
            try { parsed = JSON.parse(raw); }
            catch {
                const err = new Error('The model did not return parseable JSON.');
                err.code = 'MODEL_NO_JSON';
                throw err;
            }
            return { parsed, modelUsed: modelId, usage: response?.usageMetadata ?? null };
        } catch (e) {
            lastErr = e;
            if (e.code === 'MODEL_NO_JSON') break;
            log.warn('[shipment-payment-extract] model failed, trying the next', { modelId, error: String(e.message || '').slice(0, 160) });
        }
    }
    const err = new Error(`Gemini call failed: ${lastErr?.message ?? 'unknown error'}`);
    err.code = lastErr?.code === 'MODEL_NO_JSON' ? 'MODEL_NO_JSON' : 'MODEL_CALL_FAILED';
    err.cause = lastErr;
    throw err;
}

// ── Orchestration ────────────────────────────────────────────────────────
const money = v => {
    const n = Number(v);
    return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
};

/** Read one uploaded document and write what it says. Owns its own
 *  connections: the pool has a single one, so none is held across the model
 *  call. Never throws — a failure is recorded on the document row. */
async function runShipmentPaymentExtraction(pool, { documentId, userEmail = null, model = null } = {}) {
    // (a) Load the document and its shipment, then let go of the connection.
    let doc;
    let context;
    let memberPos;
    {
        const conn = await pool.getConnection();
        try {
            const [rows] = await conn.query(`SELECT * FROM shipment_payment_documents WHERE id = ?`, [documentId]);
            doc = rows[0];
            if (!doc || doc.deleted_at) return { skipped: 'not_found' };
            if (doc.shipment_id == null) {
                // A proof of payment uploaded against the supplier alone: there
                // is no box to map it to, only the transfer it will be applied with.
                context = { shipment: null, shipmentCurrency: null, purchaseOrders: [] };
                memberPos = [];
            } else {
                context = await loadShipmentContext(conn, doc.shipment_id);
                memberPos = await loadMemberPurchaseOrders(conn, doc.shipment_id);
            }
        } finally {
            conn.release();
        }
    }
    if (!context) {
        await finishFailed(pool, documentId, 'SHIPMENT_MISSING', 'The shipment this document belongs to no longer resolves.', userEmail);
        return { failed: 'SHIPMENT_MISSING' };
    }

    // (b) The model call, holding nothing.
    let read;
    try {
        read = await readDocument({ s3Key: doc.s3_key, contentType: doc.content_type, context, model });
    } catch (e) {
        await finishFailed(pool, documentId, e.code || 'MODEL_CALL_FAILED', e.message, userEmail);
        return { failed: e.code || 'MODEL_CALL_FAILED' };
    }

    // (c) Write what it found.
    const conn = await pool.getConnection();
    try {
        const extract = read.parsed;
        const amount = money(extract.amountDueNow) ?? money(extract.totalAmount);
        const matched = matchLinesToPos(extract, memberPos, amount ?? 0);
        const currency = (extract.currency || context.shipmentCurrency || 'USD').toUpperCase().slice(0, 3);
        // The record is filed under the supplier the operator uploaded it
        // against — the name the page groups by — not the spelling printed on
        // the letterhead ("SUZHOU SUNMED CO.,LTD." would open a row of its own).
        const supplierName = (doc.supplier_name || extract.supplierName || '').trim().slice(0, 255);
        // Only a balance invoice becomes a balance. A deposit invoice belongs
        // on its purchase order; a remittance proves a payment and is applied
        // by an operator (mark-paid), never by the reader.
        const payable = extract.documentKind === 'balance_invoice';
        const fit = assessFit({ extract, shipment: context.shipment, supplierName: doc.supplier_name || null, memberPos, amount });
        // Line by line against what is in the box, for an invoice with a box.
        const check = payable && doc.shipment_id != null
            ? compareInvoiceToContainer({ extract, context, supplierName: supplierName || doc.supplier_name || '' })
            : null;

        const stored = {
            ...extract,
            fit,
            check,
            allocations: matched.allocations,
            matchedLines: matched.matchedLines,
            unmatchedLines: matched.unmatchedLines,
            lineTotal: matched.lineTotal,
            scaledToAmount: !!matched.scaledToAmount,
            splitByShare: !!matched.splitByShare,
            needsAllocation: !!matched.needsAllocation,
        };

        await conn.beginTransaction();
        try {
            let paymentId = doc.payment_id ?? null;

            if (paymentId) {
                // Refresh a record this reader wrote and nobody has touched.
                const [existing] = await conn.query(`SELECT * FROM shipment_payments WHERE id = ? AND deleted_at IS NULL`, [paymentId]);
                const rec = existing[0];
                if (rec && rec.status === 'pending' && rec.source === 'extracted' && payable && amount != null) {
                    await writeExtractedPayment(conn, paymentId, {
                        amount, currency, extract, supplierName, allocations: matched.allocations,
                    });
                } else {
                    stored.skippedUpdate = rec ? 'operator_owned' : 'record_missing';
                }
            } else if (payable && amount != null && amount > 0 && doc.shipment_id == null) {
                // An invoice with no shipment to bill: the page says to upload
                // it on the container.
                stored.noPaymentCreated = 'no_shipment';
            } else if (payable && amount != null && amount > 0 && fit.verdict === 'mismatch') {
                // It names another box, another supplier or another currency:
                // putting its figure on this balance would be wrong without a
                // person saying so. The page offers "record it anyway".
                stored.noPaymentCreated = 'mismatch';
            } else if (payable && amount != null && amount > 0) {
                // A document the supplier has issued before: do not create a
                // second record for the same invoice number.
                const [dupes] = await conn.query(
                    `SELECT id FROM shipment_payments
                      WHERE deleted_at IS NULL AND supplier_key = ? AND invoice_number = ? AND invoice_number IS NOT NULL
                      LIMIT 1`,
                    [supplierKey(supplierName), extract.invoiceNumber ? String(extract.invoiceNumber).trim().slice(0, 100) : null]
                );
                if (dupes.length) {
                    stored.duplicateOfPaymentId = dupes[0].id;
                    paymentId = dupes[0].id;
                } else {
                    const [ins] = await conn.query(
                        `INSERT INTO shipment_payments
                            (shipment_id, shipment_reference, supplier_name, supplier_key, kind, amount, currency,
                             invoice_number, invoice_date, due_date, invoice_total, deposit_deducted,
                             status, note, source, created_by_email)
                         VALUES (?, ?, ?, ?, 'balance', ?, ?, ?, ?, ?, ?, ?, 'pending', ?, 'extracted', ?)`,
                        [
                            doc.shipment_id, doc.shipment_reference,
                            supplierName || doc.supplier_name || 'Unknown supplier', supplierKey(supplierName || doc.supplier_name || ''),
                            amount, currency,
                            extract.invoiceNumber ? String(extract.invoiceNumber).trim().slice(0, 100) : null,
                            toDateOnlyOrNull(extract.invoiceDate), toDateOnlyOrNull(extract.dueDate),
                            money(extract.totalAmount), money(extract.depositDeducted),
                            extract.dueTerms ? String(extract.dueTerms).slice(0, 2000) : null,
                            userEmail,
                        ]
                    );
                    paymentId = ins.insertId;
                    for (const a of matched.allocations) {
                        await conn.query(
                            `INSERT INTO shipment_payment_allocations (payment_id, purchase_order_id, po_ref, amount, source)
                             VALUES (?, ?, ?, ?, 'extracted')`,
                            [paymentId, a.purchaseOrderId, a.poRef || '', a.amount]
                        );
                    }
                    await recordAudit(conn, {
                        entityType: 'shipment_payment', entityId: paymentId, action: 'create',
                        before: null,
                        after: { source: 'extracted', amount, currency, documentId, status: 'pending' },
                        userEmail,
                    });
                }
            } else {
                // A remittance advice is evidence of payment, not a new claim:
                // its details are stored for the operator, nothing is created.
                stored.noPaymentCreated = payable ? 'no_amount' : extract.documentKind;
            }

            await conn.query(
                `UPDATE shipment_payment_documents
                    SET extract_status = 'succeeded', extract_json = ?, model_used = ?, extract_error = NULL,
                        extracted_at = NOW(), doc_kind = ?, supplier_name = COALESCE(supplier_name, ?),
                        supplier_key = COALESCE(supplier_key, ?), payment_id = COALESCE(payment_id, ?)
                  WHERE id = ?`,
                [
                    JSON.stringify(stored), read.modelUsed,
                    extract.documentKind === 'remittance' ? 'remittance' : doc.doc_kind,
                    supplierName || null, supplierName ? supplierKey(supplierName) : null,
                    paymentId, documentId,
                ]
            );
            await conn.commit();
            return { ok: true, paymentId, modelUsed: read.modelUsed };
        } catch (e) {
            await conn.rollback().catch(() => {});
            throw e;
        }
    } catch (e) {
        log.error('[shipment-payment-extract] write failed', { documentId, error: e.message });
        await finishFailed(pool, documentId, 'WRITE_FAILED', e.message, userEmail);
        return { failed: 'WRITE_FAILED' };
    } finally {
        conn.release();
    }
}

async function writeExtractedPayment(conn, paymentId, { amount, currency, extract, supplierName, allocations }) {
    await conn.query(
        `UPDATE shipment_payments
            SET amount = ?, currency = ?, invoice_number = ?, invoice_date = ?, due_date = ?,
                invoice_total = ?, deposit_deducted = ?, note = COALESCE(note, ?),
                supplier_name = COALESCE(NULLIF(?, ''), supplier_name)
          WHERE id = ?`,
        [
            amount, currency,
            extract.invoiceNumber ? String(extract.invoiceNumber).trim().slice(0, 100) : null,
            toDateOnlyOrNull(extract.invoiceDate), toDateOnlyOrNull(extract.dueDate),
            money(extract.totalAmount), money(extract.depositDeducted),
            extract.dueTerms ? String(extract.dueTerms).slice(0, 2000) : null,
            supplierName || '', paymentId,
        ]
    );
    await conn.query(`DELETE FROM shipment_payment_allocations WHERE payment_id = ?`, [paymentId]);
    for (const a of allocations) {
        await conn.query(
            `INSERT INTO shipment_payment_allocations (payment_id, purchase_order_id, po_ref, amount, source)
             VALUES (?, ?, ?, ?, 'extracted')`,
            [paymentId, a.purchaseOrderId, a.poRef || '', a.amount]
        );
    }
}

async function finishFailed(pool, documentId, code, message, userEmail) {
    const conn = await pool.getConnection();
    try {
        await conn.query(
            `UPDATE shipment_payment_documents
                SET extract_status = 'failed', extract_error = ?, extracted_at = NOW()
              WHERE id = ?`,
            [`${code}: ${String(message || '').slice(0, 900)}`, documentId]
        );
        await recordAudit(conn, {
            entityType: 'shipment_payment_document', entityId: documentId, action: 'document_extract_failed',
            before: null, after: { code, message: String(message || '').slice(0, 300) }, userEmail,
        });
    } catch (e) {
        log.error('[shipment-payment-extract] could not record the failure', { documentId, error: e.message });
    } finally {
        conn.release();
    }
}

module.exports = {
    GEMINI_MODEL_DEFAULT,
    FALLBACK_MODEL,
    SYSTEM_INSTRUCTION,
    RESPONSE_SCHEMA,
    norm,
    digitCore,
    buildRefIndex,
    matchPoRef,
    matchLinesToPos,
    sameSupplier,
    assessFit,
    compareInvoiceToContainer,
    promptFor,
    readDocument,
    runShipmentPaymentExtraction,
};
