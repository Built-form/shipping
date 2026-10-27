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
    supplierKey, loadShipmentContext, loadMemberPurchaseOrders, loadQcLines, shareAllocations,
} = require('./shipment-payments');
const { EXTRA_KINDS, OTHER_CHARGES_SCHEMA } = require('../lib/payment-extras');

// Flash reads the figures; the judging (lines vs the box, invoice vs what is
// owed) is ours, not the model's. gemini-3.8-flash (not a preview) read the
// real uploads field-for-field the same as 3-flash-preview and 3.1-pro, in
// about half the time (2026-09-28). Pro is the fallback when Flash refuses a
// scan. NB the PO/PI check (po-invoice-check.js) stays on 3-flash-preview:
// there the MODEL judges, and 3.8-flash passed three PIs that preview and pro
// both flagged (one a _FQC code difference).
const GEMINI_MODEL_DEFAULT = 'gemini-3.8-flash';
const FALLBACK_MODEL = 'gemini-3.1-pro-preview';

const SYSTEM_INSTRUCTION = `You read supplier payment documents for a UK medical-supplies importer and return structured JSON.

You are given ONE document (a PDF or image) and the shipment it was filed against, including every purchase order travelling in that shipment with its PO number, the supplier's own spelling of its name, its line items and the value it has on board.

Decide first what the document IS:
- "balance_invoice" — the supplier billing for goods being shipped (often "balance", "final payment", "70% against B/L"). This is the common case. A supplier's invoice for QC sample units (our codes ending "_FQC"; often "FQC" or "QC samples"), alone or beside the goods, is one too.
- "deposit_invoice" — an upfront/proforma asking for a deposit before production.
- "remittance" — evidence that a payment was MADE (a bank transfer advice, SWIFT copy, payment receipt).
- "other" — anything else.

Then extract:
- supplierName — the issuing supplier exactly as printed (for a remittance: the beneficiary who was paid).
- invoiceNumber, invoiceDate (YYYY-MM-DD), dueDate (YYYY-MM-DD if stated or clearly derivable, e.g. "30 days from B/L" with the B/L date on the document).
- paymentDate — for a remittance only: the date the money was sent (YYYY-MM-DD). Null on anything else.
- currency — ISO code of the amounts.
- totalAmount — the document's grand total.
- amountDueNow — the amount actually payable against THIS document. For a balance invoice that is the balance, not the whole order value. Null if it states no payable amount. A commercial invoice that only prices the goods (line values and a grand total, no "amount due", "balance" or deposit wording) is a balance_invoice with amountDueNow null — never copy its grand total into amountDueNow.
- depositDeducted — any deposit or advance the document nets off (often "less 30% deposit"), as a positive number; null if none.
- dueTerms — the payment-timing wording, verbatim and short.
- containerRefs, blRefs — container numbers and bill-of-lading numbers printed on the document.
- poRefs — every purchase order reference printed anywhere on it.
- lines — the GOODS only, one entry per product billed: poRef (mapped to one of OUR PO numbers when you can), jfCode (OUR product code for that line, chosen from the purchase orders you were given, when the product clearly matches; else null), piRef (the supplier's own invoice/PI number if the line cites one), sku (the supplier's own code as printed), description, qty, unitPrice, amount. QC sample units are lines too, with their _FQC code as jfCode. Never put a charge in lines.
- otherCharges — every amount billed that is NOT goods: mould or tooling, handling, freight, samples, testing, packaging, bank charges, surcharges — and any discount or credit, as a NEGATIVE amount. One entry each: description (as printed), amount, kind (one of: ${EXTRA_KINDS.join(', ')}). A deposit the document nets off is depositDeducted, not a charge. Empty when there are none.
- bank — the beneficiary and remittance details exactly as printed. Never reformat or invent account numbers.
- rawText — the payment-terms and bank-details section copied verbatim, so a human can check the parsed figures.
- confidence — "high" when the figures and the purchase orders are unambiguous, "low" when you are guessing.

Rules:
- Map references to the PO numbers you were given whenever the digits match, even if the document writes them differently (PO00299, PO_00299J, 299). If you cannot map a reference, return it verbatim in poRef and leave the mapping to a human.
- NEVER invent an amount. If a figure is not on the document, return null.
- The line amounts plus otherCharges should reconcile to totalAmount; if they do not, still report them faithfully.
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
        otherCharges: OTHER_CHARGES_SCHEMA,
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
        'poRefs', 'lines', 'otherCharges', 'bank', 'rawText', 'confidence',
    ],
};

// Charges read off a document, cleaned: numbers only, known kinds. Reads made
// before the field existed have none.
function chargesOf(extract) {
    return (Array.isArray(extract && extract.otherCharges) ? extract.otherCharges : [])
        .filter(c => c && typeof c === 'object' && Number.isFinite(Number(c.amount)) && c.amount !== null && Number(c.amount) !== 0)
        .map(c => ({
            description: typeof c.description === 'string' && c.description.trim() ? c.description.trim() : null,
            amount: Math.round(Number(c.amount) * 100) / 100,
            kind: EXTRA_KINDS.includes(c.kind) ? c.kind : 'other',
        }));
}
const chargesTotalOf = charges => Math.round(charges.reduce((a, c) => a + c.amount, 0) * 100) / 100;

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

// A PO number printed inside free text (a line's description): a token that
// is one of ours, spelt out. Only whole tokens with letters and digits, and
// only an exact spelling — a lot number or a pack size is never a PO.
function poRefInText(text, index) {
    for (const token of String(text ?? '').split(/[\s,;:()（）]+/)) {
        const n = norm(token);
        if (!/[A-Z]/.test(n) || !/\d/.test(n)) continue;
        if (index.byExact.has(n)) return token;
    }
    return null;
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
function assessFit({ extract, shipment, supplierName, memberPos, qcPos = [], amount }) {
    const checks = [];
    const add = (key, ok, text) => checks.push({ key, ok, text });
    const pos = memberPos || [];
    const isMine = p => p.supplierKey === supplierKey(supplierName) || sameSupplier(p.supplier, supplierName);
    const mine = supplierName ? pos.filter(isMine) : pos;
    // This supplier's other POs whose QC units a document here may bill.
    const qcOnly = supplierName ? (qcPos || []).filter(p => isMine(p) && !pos.some(m => m.id === p.id)) : [];

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

    // Which purchase orders. One of this supplier's other POs counts too when
    // the document bills its QC units (they are paid with this box).
    const index = buildRefIndex(pos);
    const qcIndex = buildRefIndex(qcOnly);
    const refs = [...new Set([...(extract.poRefs || []), ...(extract.lines || []).map(l => l && l.poRef)]
        .map(r => String(r ?? '').trim()).filter(Boolean))];
    if (!refs.length) {
        add('pos', null, 'Names none of our purchase orders.');
    } else {
        const hits = new Map();
        const qcHits = new Map();
        const misses = [];
        for (const r of refs) {
            const po = matchPoRef(r, index);
            const qc = po ? null : matchPoRef(r, qcIndex);
            if (po) hits.set(po.id, po);
            else if (qc) qcHits.set(qc.id, qc);
            else misses.push(r);
        }
        const found = [...hits.values()];
        const ownFound = found.filter(p => mine.includes(p));
        const qcFound = [...qcHits.values()];
        const named = [...ownFound.map(p => p.poNumber), ...qcFound.map(p => `${p.poNumber} (QC units)`)];
        const missNote = misses.length ? `; ${misses.slice(0, 3).join(', ')}${misses.length > 3 ? '…' : ''} ${misses.length === 1 ? 'is' : 'are'} not on this shipment` : '';
        if (named.length) add('pos', true, `Names ${named.join(', ')}${missNote}.`);
        else if (found.length) add('pos', false, `Names ${found.map(p => p.poNumber).join(', ')}, filed under ${found[0].supplier || 'another supplier'}.`);
        else add('pos', false, `Names ${misses.slice(0, 3).join(', ')}${misses.length > 3 ? '…' : ''} — not on this shipment.`);

        // Suppliers often invoice through another company (the factory for
        // the trading house, or the reverse). An invoice that cites this
        // supplier's own PO is theirs whatever the letterhead says — a
        // warning, not a mismatch. Never for a remittance: money paid to
        // another beneficiary is exactly what must stop someone.
        const supplierCheck = checks.find(c => c.key === 'supplier' && c.ok === false);
        if (supplierCheck && named.length && extract.documentKind !== 'remittance') {
            supplierCheck.ok = null;
            supplierCheck.text = `${by} ${printed} — not the name on file (${supplierName || names[0]}), but it names ${[...ownFound, ...qcFound].map(p => p.poNumber).join(', ')}.`;
        }
    }

    // Money: the currency, then the amount against what is on board — the
    // goods part of it: a mould fee or a handling charge is not goods.
    const currencies = [...new Set(mine.map(p => p.currency).filter(Boolean))];
    const currency = extract.currency ? String(extract.currency).toUpperCase().slice(0, 3) : null;
    const onBoard = Math.round(mine.reduce((a, p) => a + (Number(p.valueInShipment) || 0), 0) * 100) / 100;
    const charges = chargesTotalOf(chargesOf(extract));
    const goods = amount == null ? null : Math.round((amount - charges) * 100) / 100;
    if (currency && currencies.length && !currencies.includes(currency)) {
        add('currency', false, `In ${currency}; the purchase orders are in ${currencies.join('/')}.`);
    } else if (goods != null && goods > 0 && onBoard > 0) {
        const cur = currency || currencies[0] || null;
        const less = charges ? ` (after ${fmtAmount(charges, cur)} of other charges)` : '';
        if (goods > onBoard * 1.05 + 1) {
            add('amount', false, `${fmtAmount(goods, cur)}${less} is more than the ${fmtAmount(onBoard, cur)} of goods this supplier has on board.`);
        } else {
            add('amount', null, `${Math.round((goods / onBoard) * 100)}% of the ${fmtAmount(onBoard, cur)} this supplier has on board${less}.`);
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
const numOrNull = v => (v == null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v));
// A figure for several lines: their sum, or null when any of them is unknown.
const sumOrNull = vals => (vals.length && vals.every(v => v != null) ? Math.round(vals.reduce((a, v) => a + v, 0) * 100) / 100 : null);
// The price several lines share, or null when they differ or one is unknown.
const sameOrNull = vals => (vals.length && vals.every(v => v != null && Math.abs(v - vals[0]) <= LINE_PRICE_EPS) ? vals[0] : null);
const fmtQty = n => n.toLocaleString('en-GB');

// QC sample units — our codes ending _FQC — never travel. The supplier bills
// them on their own invoice or beside the goods, often for several POs at once
// (user, 2026-09-29: they are paid with the box the invoice is uploaded on).
const isQcCode = v => /FQC$/.test(norm(v));
// How suppliers word a line that bills QC units ("BY INSPECTION …", "QC
// samples"). ShipLine's qcUnits.ts QC_WORDING reads the same: keep them in step.
const QC_WORDING = /\bby\s+insp|\binspection\b|\bQC\b|\bsamples?\b/i;
const lineAmountOf = raw => {
    const amount = Number(raw.amount);
    if (raw.amount != null && Number.isFinite(amount)) return amount;
    const q = Number(raw.qty);
    const p = Number(raw.unitPrice);
    return raw.qty != null && raw.unitPrice != null && Number.isFinite(q) && Number.isFinite(p) ? Math.round(q * p * 100) / 100 : 0;
};
const poOfLine = l => ({ id: l.poId, poNumber: l.poNumber, supplier: l.supplier || null, supplierKey: l.supplierKey || supplierKey(l.supplier) });
const uniquePos = pos => [...new Map(pos.map(p => [p.id, p])).values()];

function compareInvoiceToContainer({ extract, context, supplierName }) {
    const key = supplierKey(supplierName);
    const mine = (context.purchaseOrders || []).filter(p => p.supplierKey === key || sameSupplier(p.supplier, supplierName));
    const index = buildRefIndex(mine);
    const onBoardPoIds = new Set(mine.map(p => p.id));
    // The supplier's QC units on any of its POs, never another supplier's.
    const qcOurs = (context.qcLines || [])
        .filter(l => (l.supplierKey || supplierKey(l.supplier)) === key || sameSupplier(l.supplier, supplierName))
        .map(l => ({
            poId: l.poId, poNumber: l.poNumber, jfCode: l.jfCode || null, productName: l.productName || null,
            qty: l.quantity != null ? Number(l.quantity) : null, unitPrice: l.unitPrice != null ? Number(l.unitPrice) : null,
            total: l.quantity != null && l.unitPrice != null ? Math.round(Number(l.quantity) * Number(l.unitPrice) * 100) / 100 : null,
            matched: false,
        }));
    const qcIndex = buildRefIndex(uniquePos(qcOurs.map(poOfLine)));
    const ours = [];
    for (const po of mine) {
        for (const l of po.lines || []) {
            ours.push({
                poId: po.id, poNumber: po.poNumber, jfCode: l.jfCode || null, productName: l.productName || null,
                qty: l.quantity != null ? Number(l.quantity) : null, unitPrice: l.unitPrice != null ? Number(l.unitPrice) : null,
                total: l.quantity != null && l.unitPrice != null ? Math.round(Number(l.quantity) * Number(l.unitPrice) * 100) / 100 : null,
            });
        }
    }
    const goodsOnBoard = Math.round(ours.reduce((a, l) => a + (l.total || 0), 0) * 100) / 100;

    // Our goods, one group per product per PO, in the order the box lists
    // them: packing part of a quantity splits the order row, so one product
    // can sit on several rows of the box. Keyed by our code; a row with no
    // code by its name.
    const groups = [];
    const groupAt = new Map();
    ours.forEach((l, i) => {
        const code = norm(l.jfCode);
        const groupKey = `${l.poId}|${code || `name:${descKey(l.productName) || i}`}`;
        let g = groupAt.get(groupKey);
        if (!g) {
            g = { index: groups.length, poId: l.poId, poNumber: l.poNumber, code, jfCode: l.jfCode, productName: l.productName, desc: descKey(l.productName), rows: [], invoiceRows: [] };
            groupAt.set(groupKey, g);
            groups.push(g);
        }
        g.rows.push(l);
    });
    const groupsByCode = new Map();
    for (const g of groups) {
        if (!g.code) continue;
        if (!groupsByCode.has(g.code)) groupsByCode.set(g.code, []);
        groupsByCode.get(g.code).push(g);
    }

    const lines = [];
    const qcLines = [];
    const extra = [];
    // A QC line's quantity, unit price and line total against ours, in the
    // words the page shows (goods lines are added up first: see below).
    const issuesOf = (raw, hit, where) => {
        const issues = [];
        const invQty = raw.qty != null ? Number(raw.qty) : null;
        const invPrice = raw.unitPrice != null ? Number(raw.unitPrice) : null;
        const invTotal = raw.amount != null ? Number(raw.amount) : null;
        if (invQty != null && hit.qty != null && invQty !== hit.qty) issues.push(`quantity ${invQty.toLocaleString('en-GB')} on the invoice, ${hit.qty.toLocaleString('en-GB')} ${where}`);
        if (invPrice != null && hit.unitPrice != null && Math.abs(invPrice - hit.unitPrice) > LINE_PRICE_EPS) issues.push(`unit price ${invPrice} on the invoice, ${hit.unitPrice} on the PO`);
        if (invTotal != null && hit.total != null && Math.abs(invTotal - hit.total) > LINE_TOTAL_EPS) issues.push(`line total ${invTotal.toFixed(2)} on the invoice, ${hit.total.toFixed(2)} ${where}`);
        return { issues, invQty, invPrice, invTotal };
    };
    // A QC unit: on the PO printed on the line — the document's own word,
    // which outranks the model's poRef (2026-10-01: the model set every line
    // of invoice 26/89132 to PO_00333J while the lines print SUNMED-93 and
    // SUNMED-96) — else the model's poRef; else, with the same code on several
    // of this supplier's POs, the one whose quantity is the invoice's, else
    // the one with goods in this box, else the newest. A model-named PO whose
    // quantity differs gives way to the one other PO carrying exactly the
    // invoice's quantity. False when none of the supplier's QC lines fits.
    const matchQcUnit = (raw, code) => {
        const printed = poRefInText(`${raw.description ?? ''} ${raw.sku ?? ''}`, qcIndex);
        const named = printed ? matchPoRef(printed, qcIndex) : raw.poRef ? matchPoRef(raw.poRef, qcIndex) : null;
        const withCode = qcOurs.filter(l => !l.matched && norm(l.jfCode) === code);
        const invQty = raw.qty != null ? Number(raw.qty) : null;
        let found = withCode;
        if (printed || raw.poRef) {
            const onNamed = named ? withCode.filter(l => l.poId === named.id) : [];
            const exact = invQty != null ? withCode.filter(l => l.qty === invQty) : [];
            found = !printed && onNamed.length && !onNamed.some(l => l.qty === invQty) && exact.length === 1 ? exact : onNamed;
        }
        const byQty = invQty != null ? found.filter(l => l.qty === invQty) : [];
        const pool = byQty.length ? byQty : found;
        const qhit = pool.find(l => onBoardPoIds.has(l.poId)) ?? pool.sort((a, b) => b.poId - a.poId)[0];
        if (!qhit) return false;
        qhit.matched = true;
        const { issues, invPrice, invTotal } = issuesOf(raw, qhit, 'on the PO');
        qcLines.push({
            poId: qhit.poId, poNumber: qhit.poNumber, jfCode: qhit.jfCode, productName: qhit.productName,
            ourQty: qhit.qty, invoiceQty: invQty, ourUnitPrice: qhit.unitPrice, invoiceUnitPrice: invPrice, ourTotal: qhit.total, invoiceTotal: invTotal,
            issues,
        });
        return true;
    };

    // Each goods line on the invoice is tied to the group(s) it bills. A
    // supplier bills the full cartons and the part carton of one product on
    // two lines, and we hold one product on two rows: the lines tied to a
    // product are added up and compared with all of its rows. Every step reads
    // only the line and what the box holds, never what an earlier line took,
    // so the order the lines are printed in changes nothing.
    const link = (row, targets, matchedBy) => {
        row.groups = targets;
        row.matchedBy = matchedBy;
        for (const g of targets) g.invoiceRows.push(row);
    };
    const unplaced = [];
    const byCodeElsewhere = [];
    const heldQc = [];
    (extract.lines || []).forEach((raw, at) => {
        const row = { raw, at, po: raw.poRef ? matchPoRef(raw.poRef, index) : null, groups: [], matchedBy: null };
        // An amount with no quantity and no price is a charge the reader put
        // among the goods ("printing plate charges JF0880"), not goods: never
        // added to a product, even one whose code it prints.
        if (numOrNull(raw.qty) == null && numOrNull(raw.unitPrice) == null) {
            row.charge = true;
            unplaced.push(row);
            return;
        }
        // 1. Our code, when the box holds it: on the PO the line names, else
        //    (below) wherever this supplier has it. A line carrying a code the
        //    box holds is never checked against another product.
        const known = [norm(raw.jfCode), norm(raw.sku)].find(c => c && groupsByCode.has(c));
        if (known) {
            const onPo = row.po ? groupsByCode.get(known).find(g => g.poId === row.po.id) : null;
            if (onPo) link(row, [onPo], 'code');
            else byCodeElsewhere.push({ row, code: known });
            return;
        }
        // A QC unit (an _FQC code not on board) is matched to the QC lines
        // only — never taken for the goods it samples by name. Matched below,
        // in the order printed: one whose product IS in the box may turn out
        // to be its goods, once every line coded as that product is tied to it.
        const code = norm(raw.jfCode || raw.sku);
        if (isQcCode(code)) {
            const base = code.replace(/FQC$/, '');
            heldQc.push({ row, code, base: groupsByCode.has(base) ? base : null });
            return;
        }
        unplaced.push(row);
    });
    // The line names another PO, or none: the groups with that code the
    // lines naming them leave free (all of them if none is free). Several
    // groups are checked together, as one.
    const claimedByCode = new Set(groups.filter(g => g.invoiceRows.length));
    for (const { row, code } of byCodeElsewhere) {
        const all = groupsByCode.get(code);
        const free = all.filter(g => !claimedByCode.has(g));
        link(row, free.length ? free : all, row.po ? 'code_other_po' : 'code');
    }

    // Lines coded as QC units whose product is in the box (user, 2026-10-27:
    // INV-26_89131 bills JF1402 as 4,992 + 24, the box holds 5,016, and the
    // reader — shown JF1402_FQC × 24 on the PO — coded the 24 as QC units).
    // Goods when nothing on them says QC and the box adds up only with them:
    // what is already tied to the product, plus these lines, is exactly the
    // box's quantity. Else QC units, as before, in the order printed.
    const heldFor = new Map();
    for (const h of heldQc) {
        if (!h.base) continue;
        const all = groupsByCode.get(h.base);
        const g = (h.row.po ? all.find(x => x.poId === h.row.po.id) : null) ?? (all.length === 1 ? all[0] : null);
        if (!g) continue;
        if (!heldFor.has(g)) heldFor.set(g, []);
        heldFor.get(g).push(h);
    }
    const qcAsGoods = new Set();
    for (const [g, held] of heldFor) {
        const tiedQtys = g.invoiceRows.map(r => numOrNull(r.raw.qty));
        const tied = tiedQtys.length ? sumOrNull(tiedQtys) : 0;
        const heldQty = sumOrNull(held.map(h => numOrNull(h.row.raw.qty)));
        const box = sumOrNull(g.rows.map(l => l.qty));
        const worded = held.some(h => QC_WORDING.test(`${h.row.raw.description ?? ''} ${h.row.raw.sku ?? ''}`));
        if (worded || tied == null || heldQty == null || box == null || Math.abs(tied + heldQty - box) > 1e-9) continue;
        for (const h of held) { link(h.row, [g], 'qc_code_goods'); qcAsGoods.add(h.row.at); }
    }
    for (const h of [...heldQc].sort((a, b) => a.row.at - b.row.at)) {
        if (qcAsGoods.has(h.row.at)) continue;
        if (!matchQcUnit(h.row.raw, h.code)) unplaced.push(h.row);
    }

    // 2. Goods lines with no code of ours, bunched by what identifies them on
    //    the invoice (their code, else the supplier's, else the description)
    //    on the PO they name, so a product split over two lines is placed as one.
    const bunches = new Map();
    for (const row of unplaced) {
        if (row.charge || isQcCode(norm(row.raw.jfCode || row.raw.sku))) continue;
        const id = norm(row.raw.jfCode) || norm(row.raw.sku) || descKey(row.raw.description) || `line:${row.at}`;
        const bunchKey = `${row.po ? row.po.id : '-'}|${id}`;
        if (!bunches.has(bunchKey)) bunches.set(bunchKey, { po: row.po, rows: [] });
        bunches.get(bunchKey).rows.push(row);
    }
    const scopeOf = b => (b.po ? groups.filter(g => g.poId === b.po.id) : groups);
    const placeAll = (b, targets, matchedBy) => { for (const row of b.rows) link(row, targets, matchedBy); b.placed = true; };
    const open = () => [...bunches.values()].filter(b => !b.placed);
    // 2a. Our product name.
    for (const b of open()) {
        const descs = new Set(b.rows.map(r => descKey(r.raw.description)).filter(Boolean));
        const hits = descs.size ? scopeOf(b).filter(g => g.desc && descs.has(g.desc)) : [];
        if (hits.length === 1) placeAll(b, hits, 'description');
    }
    // 2b. The PO's one product nothing bills, for the PO's one bunch left.
    let billed = new Set(groups.filter(g => g.invoiceRows.length));
    const left = open();
    for (const b of left) {
        if (!b.po) continue;
        const free = groups.filter(g => g.poId === b.po.id && !billed.has(g));
        if (free.length === 1 && left.filter(o => o.po && o.po.id === b.po.id).length === 1) placeAll(b, free, 'only_line');
    }
    // 2c. The one product nothing bills whose quantity is the bunch's — taken
    //     only when no other bunch points at it too.
    billed = new Set(groups.filter(g => g.invoiceRows.length));
    const byQty = new Map();
    for (const b of open()) {
        const qty = sumOrNull(b.rows.map(r => numOrNull(r.raw.qty)));
        if (!(qty > 0)) continue;
        const hits = scopeOf(b).filter(g => !billed.has(g) && sumOrNull(g.rows.map(l => l.qty)) === qty);
        if (hits.length !== 1) continue;
        if (!byQty.has(hits[0])) byQty.set(hits[0], []);
        byQty.get(hits[0]).push(b);
    }
    for (const [g, bs] of byQty) if (bs.length === 1) placeAll(bs[0], [g], 'quantity');

    // Not placed: on the invoice, not in the box.
    for (const row of unplaced.filter(r => !r.groups.length).sort((a, b) => a.at - b.at)) {
        const raw = row.raw;
        extra.push({ poRef: raw.poRef ?? null, jfCode: raw.jfCode ?? null, sku: raw.sku ?? null, description: raw.description ?? null, qty: raw.qty ?? null, unitPrice: raw.unitPrice ?? null, amount: raw.amount ?? null });
    }

    // Groups tied together by one line are checked as one; in box order.
    const parent = groups.map((_, i) => i);
    const root = i => (parent[i] === i ? i : (parent[i] = root(parent[i])));
    for (const g of groups) for (const row of g.invoiceRows) parent[root(row.groups[0].index)] = root(g.index);
    const units = new Map();
    for (const g of groups) {
        if (!g.invoiceRows.length) continue;
        const r = root(g.index);
        if (!units.has(r)) units.set(r, { groups: [], rows: new Set() });
        units.get(r).groups.push(g);
        for (const row of g.invoiceRows) units.get(r).rows.add(row);
    }
    let invoiceLinesMatched = 0;
    for (const unit of units.values()) {
        const rows = [...unit.rows].sort((a, b) => a.at - b.at);
        invoiceLinesMatched += rows.length;
        const ourRows = unit.groups.flatMap(g => g.rows);
        const poNumber = [...new Set(unit.groups.map(g => g.poNumber))].join(', ');
        const ourQty = sumOrNull(ourRows.map(l => l.qty));
        const ourTotal = sumOrNull(ourRows.map(l => l.total));
        const ourUnitPrice = sameOrNull(ourRows.map(l => l.unitPrice));
        const invQtys = rows.map(r => numOrNull(r.raw.qty));
        const invPrices = rows.map(r => numOrNull(r.raw.unitPrice));
        const invoiceQty = sumOrNull(invQtys);
        const invoiceTotal = sumOrNull(rows.map(r => numOrNull(r.raw.amount)));
        const issues = [];
        if (invoiceQty != null && ourQty != null && invoiceQty !== ourQty) {
            const parts = invQtys.length > 1 ? `${[...invQtys].sort((a, b) => b - a).map(fmtQty).join(' + ')} = ${fmtQty(invoiceQty)}` : fmtQty(invoiceQty);
            issues.push(`quantity ${parts} on the invoice, ${fmtQty(ourQty)} in the container`);
        }
        if (ourUnitPrice != null) {
            for (const p of [...new Set(invPrices.filter(p => p != null && Math.abs(p - ourUnitPrice) > LINE_PRICE_EPS))].sort((a, b) => a - b)) {
                issues.push(`unit price ${p} on the invoice, ${ourUnitPrice} on the PO`);
            }
        }
        if (invoiceTotal != null && ourTotal != null && Math.abs(invoiceTotal - ourTotal) > LINE_TOTAL_EPS) {
            issues.push(`line total ${invoiceTotal.toFixed(2)} on the invoice, ${ourTotal.toFixed(2)} in the container`);
        }
        for (const ref of new Set(rows.filter(r => r.matchedBy === 'code_other_po').map(r => String(r.raw.poRef)))) {
            issues.push(`billed under ${ref} on the invoice, in the container under ${poNumber}`);
        }
        lines.push({
            poNumber, jfCode: unit.groups[0].jfCode, productName: unit.groups[0].productName,
            ourQty, invoiceQty, ourUnitPrice, invoiceUnitPrice: sameOrNull(invPrices), ourTotal, invoiceTotal,
            issues,
            // How many rows of the box, and the invoice's own lines as printed.
            ourRows: ourRows.length,
            invoiceRows: rows.map(r => ({
                poRef: r.raw.poRef ?? null, jfCode: r.raw.jfCode ?? null, sku: r.raw.sku ?? null, description: r.raw.description ?? null,
                qty: numOrNull(r.raw.qty), unitPrice: numOrNull(r.raw.unitPrice), amount: numOrNull(r.raw.amount), matchedBy: r.matchedBy,
            })),
        });
    }
    // What it bills: goods (matched, or strangers that are not QC units), QC units, or both.
    const extraQc = extra.filter(l => isQcCode(l.jfCode || l.sku));
    const billsGoods = lines.length > 0 || extra.length > extraQc.length;
    const billsQc = qcLines.length > 0 || extraQc.length > 0;
    const invoiceKind = billsGoods && billsQc ? 'goods_and_qc' : billsQc ? 'qc' : 'goods';
    const qcOurTotal = Math.round(qcLines.reduce((a, l) => a + (l.ourTotal || 0), 0) * 100) / 100;
    const qcInvoiceTotal = Math.round(qcLines.reduce((a, l) => a + (l.invoiceTotal ?? 0), 0) * 100) / 100;
    // Lines coded as QC units but taken as the box's goods (above) are goods money.
    const qcBilled = Math.round([...(extract.lines || [])].filter((raw, at) => isQcCode(raw.jfCode || raw.sku) && !qcAsGoods.has(at)).reduce((a, raw) => a + lineAmountOf(raw), 0) * 100) / 100;
    // A QC units invoice does not bill the goods: none of them is missing.
    const missing = billsGoods
        ? groups.filter(g => !g.invoiceRows.length).map(g => ({
            poNumber: g.poNumber, jfCode: g.jfCode, productName: g.productName,
            qty: sumOrNull(g.rows.map(l => l.qty)), total: sumOrNull(g.rows.map(l => l.total)),
        }))
        : [];

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
    const invoiceTotal = money(extract.totalAmount);
    const statedDue = money(extract.amountDueNow);
    // Charges that are not goods (handling, mould, bank charges, credits) are
    // the page's to add as extras; the goods are checked without them.
    const otherCharges = chargesOf(extract);
    const chargesTotal = chargesTotalOf(otherCharges);
    const net = v => (v == null ? null : Math.round((v - chargesTotal) * 100) / 100);
    // A commercial invoice prices the goods at full value and states nothing
    // payable: its total is the GOODS, not what the supplier asks for now, and
    // it is checked against the goods on board.
    const invoiceBasis = statedDue == null && !(money(extract.depositDeducted) > 0) && invoiceTotal != null ? 'goods' : 'balance';
    const goodsTolerance = Math.max(1, goodsOnBoard * 0.005);
    // The goods it prices: its total less the charges and the QC units it bills.
    const invoiceGoods = invoiceTotal == null ? null : Math.round((net(invoiceTotal) - qcBilled) * 100) / 100;
    const goodsDelta = invoiceBasis === 'goods' && billsGoods ? Math.round((invoiceGoods - goodsOnBoard) * 100) / 100 : 0;
    // What it asks for, QC units included (qcBilled says how much of it they are).
    const invoiceBalance = invoiceBasis === 'goods' ? null : net(statedDue ?? invoiceTotal);
    const balanceDelta = invoiceBalance == null ? 0 : Math.round((invoiceBalance - expectedBalance) * 100) / 100;
    const balanceTolerance = Math.max(1, expectedBalance * 0.005);
    const lineTrouble = lines.some(l => l.issues.length) || qcLines.some(l => l.issues.length) || missing.length > 0 || extra.length > 0;
    // The goods on a goods invoice are judged here — they do not depend on the
    // terms. What an invoice ASKS is not: the balance owed comes from the
    // supplier's terms, which only the page knows (JFPRO), so the page compares
    // it (expectedBalance/balanceDelta here use deposit PIs only — information).
    const balanceTrouble = invoiceBasis === 'goods' && billsGoods && Math.abs(goodsDelta) > goodsTolerance;
    const nothingToWeigh = invoiceBasis === 'balance' && invoiceBalance == null;
    const noLines = !lines.length && !qcLines.length && !extra.length;
    const verdict = (!ours.length && !qcLines.length) || (noLines && nothingToWeigh)
        ? 'unverified'
        : noLines
            ? (balanceTrouble ? 'differs' : 'unverified')
            : (lineTrouble || balanceTrouble ? 'differs' : 'match');
    return {
        verdict,
        invoiceKind,
        matchedLines: lines.length, invoiceLinesMatched, lines, missingOurLines: missing, extraInvoiceLines: extra,
        qcLines, qcOurTotal, qcInvoiceTotal, qcBilled,
        goodsOnBoard, invoiceGoods,
        depositBasis, depositExpected, expectedBalance, invoiceBalance, balanceDelta, balanceTolerance,
        invoiceBasis, goodsDelta, goodsTolerance,
        otherCharges, chargesTotal,
    };
}

// ── The model call ───────────────────────────────────────────────────────
// The supplier's QC sample units — never on board, often billed on their own
// invoice for several POs. Newest POs first, capped.
function qcSectionOf(context) {
    const qc = [...(context.qcLines || [])].sort((a, b) => b.poId - a.poId).slice(0, 80);
    if (!qc.length) return [];
    const lines = [
        '## QC sample units on this supplier\'s purchase orders',
        'They never travel in a container. If the document bills them, list each as a line with its _FQC code as jfCode and its PO as poRef.',
        'Use an _FQC code only when the document says the line bills QC or inspection units (e.g. "BY INSPECTION", "QC"). A small second line of a product (a part carton) is goods, with the product\'s own code, even when its quantity equals a QC unit listed below.',
        'poRef is the purchase order number printed on that line (often inside its description, e.g. "SUNMED-93" or "PO_00299J"), copied exactly as printed when it is one of the purchase orders below — never a different one, even if another lists the same product.',
    ];
    const byPo = new Map();
    for (const l of qc) {
        if (!byPo.has(l.poNumber)) byPo.set(l.poNumber, []);
        byPo.get(l.poNumber).push(l);
    }
    for (const [poNumber, list] of byPo) {
        lines.push(`- ${poNumber}: ${list.map(l => `${l.jfCode} ${l.productName ?? ''} × ${l.quantity ?? '?'} @ ${l.unitPrice ?? '?'}`).join('; ')}`);
    }
    lines.push('');
    return lines;
}

function promptFor(context) {
    const s = context.shipment;
    if (!s && context.portalUpload) {
        // Sent by the supplier through the supplier portal with no shipment
        // (src/services/ci-check.js): what it prints is how we find the box.
        return [
            '## Context',
            'The supplier uploaded this document through our supplier portal as their commercial invoice — the invoice for goods being shipped to us, usually a balance invoice. We do not know yet which shipment it is for, and no purchase orders are listed here: copy every container number, bill-of-lading number and purchase-order reference exactly as printed, so we can find the shipment. Read it as it is.',
            '',
            ...qcSectionOf(context),
        ].join('\n');
    }
    if (!s) {
        // An invoice on the supplier alone bills QC units (they belong to no
        // container, user 2026-09-29); anything else there is a proof of payment.
        if ((context.qcLines || []).length) {
            return [
                '## Context',
                'This document was uploaded against a supplier as an invoice for QC sample units — extra units bought for QC, our codes ending _FQC. Read it as it is.',
                '',
                ...qcSectionOf(context),
            ].join('\n');
        }
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
    lines.push(...qcSectionOf(context));
    return lines.join('\n');
}

/** Whether a read is checked line by line: any invoice on a container; on
 *  the supplier alone, a balance invoice when the supplier has QC units (a QC
 *  invoice — they belong to no container). A proof of payment never is. */
function shouldCheck({ documentKind, hasShipment, hasQcLines }) {
    if (documentKind !== 'balance_invoice' && documentKind !== 'deposit_invoice') return false;
    return hasShipment || (documentKind === 'balance_invoice' && hasQcLines);
}

/** What a read says it made (it never makes a balance): a balance invoice with
 *  no container is "no_shipment" — unless it bills QC units, which need none. */
function noPaymentReason({ documentKind, hasShipment, invoiceKind }) {
    if (documentKind !== 'balance_invoice') return documentKind;
    if (!hasShipment && invoiceKind !== 'qc') return 'no_shipment';
    return 'checked';
}

/** Runs the model over one document. Returns { parsed, modelUsed, usage }.
 *  `bytes` (a Buffer), when given, is read instead of fetching `s3Key` from
 *  the PO bucket: the supplier portal's uploads live in the portal's bucket. */
async function readDocument({ s3Key, contentType, context, model, bytes: given = null }) {
    if (!process.env.GEMINI_API_KEY) {
        const err = new Error('GEMINI_API_KEY is not configured.');
        err.code = 'NOT_CONFIGURED';
        throw err;
    }
    let bytes = given;
    if (!bytes) {
        try {
            bytes = await fetchInvoicePdf(s3Key);
        } catch (e) {
            const err = new Error(`Could not read the uploaded document: ${e.message}`);
            err.code = 'PDF_FETCH_FAILED';
            err.cause = e;
            throw err;
        }
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
// Nothing stays nothing: Number(null) is 0, which would turn "the document
// states no amount" into "the amount is 0".
const money = v => {
    if (v == null || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
};

// The figure a document shows: what it states payable, else its total. It is
// never what is owed — that is the terms × the goods on board (the page's).
function documentAmountOf(extract) {
    return money(extract.amountDueNow) ?? money(extract.totalAmount);
}

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
                // Uploaded against the supplier alone: a proof of payment (no
                // box to map it to, only the transfer it will be applied with) —
                // or, uploaded as an invoice, one billing QC units, which belong
                // to no container: read with the supplier's QC units.
                context = { shipment: null, shipmentCurrency: null, purchaseOrders: [] };
                memberPos = [];
                if (doc.doc_kind === 'balance_invoice' && doc.supplier_name) {
                    const key = supplierKey(doc.supplier_name);
                    context.qcLines = (await loadQcLines(conn)).filter(l => l.supplierKey === key || sameSupplier(l.supplier, doc.supplier_name));
                }
            } else {
                context = await loadShipmentContext(conn, doc.shipment_id);
                memberPos = await loadMemberPurchaseOrders(conn, doc.shipment_id);
                // The supplier's QC units on any of its POs: an invoice here may bill them.
                if (context && doc.supplier_name) {
                    const key = supplierKey(doc.supplier_name);
                    context.qcLines = (await loadQcLines(conn)).filter(l => l.supplierKey === key || sameSupplier(l.supplier, doc.supplier_name));
                }
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
        const currency = (extract.currency || context.shipmentCurrency || 'USD').toUpperCase().slice(0, 3);
        // The record is filed under the supplier the operator uploaded it
        // against — the name the page groups by — not the spelling printed on
        // the letterhead ("SUZHOU SUNMED CO.,LTD." would open a row of its own).
        const supplierName = (doc.supplier_name || extract.supplierName || '').trim().slice(0, 255);
        // Line by line against what is in the box, for an invoice with a box —
        // a proforma (read as a deposit invoice: "PI & PL", "30 % deposit, 70 %
        // balance") uploaded on a container prices the same goods, so it is
        // checked too — and against the supplier's QC units for a QC invoice
        // on the supplier alone. A remittance proves a payment and is applied
        // by an operator (Record payment), never by the reader.
        const check = shouldCheck({ documentKind: extract.documentKind, hasShipment: doc.shipment_id != null, hasQcLines: (context.qcLines || []).length > 0 })
            ? compareInvoiceToContainer({ extract, context, supplierName: supplierName || doc.supplier_name || '' })
            : null;
        // The figure it shows (what it states payable, else its total) — for the
        // line split (goods only: charges are not split across POs) and the
        // "does it belong here" check only.
        const amount = documentAmountOf(extract);
        const goodsAmount = amount == null ? 0 : Math.round((amount - chargesTotalOf(chargesOf(extract))) * 100) / 100;
        const matched = matchLinesToPos(extract, memberPos, goodsAmount);
        const qcPos = [...new Map((context.qcLines || []).map(l => [l.poId, { id: l.poId, poNumber: l.poNumber, supplier: l.supplier, supplierKey: l.supplierKey }])).values()];
        const fit = assessFit({ extract, shipment: context.shipment, supplierName: doc.supplier_name || null, memberPos, qcPos, amount });

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
            // The reader makes no balance and never changes one (user rule,
            // 2026-09-28): what is owed is the terms × the goods on board, and
            // this document is only checked against it. A balance is recorded
            // when it is paid (Record payment). A link an older read made stays.
            const paymentId = doc.payment_id ?? null;
            stored.noPaymentCreated = noPaymentReason({ documentKind: extract.documentKind, hasShipment: doc.shipment_id != null, invoiceKind: check?.invoiceKind ?? null });

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
    documentAmountOf,
    chargesOf,
    money,
    promptFor,
    shouldCheck,
    noPaymentReason,
    readDocument,
    runShipmentPaymentExtraction,
};
