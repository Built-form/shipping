'use strict';

// Extra charges and credits on payments — pure rules behind
// /api/v1/payment-extras (routes: src/services/payment-extra-routes.js) and
// the `extra` line kind of /api/v1/supplier-payments.
//
// ShipLine's Payments flow page works out what is owed from the terms and the
// goods on board, and an uploaded invoice never changes that by itself (user
// rule, 2026-09-28). Money a supplier bills that is not goods — mould,
// handling, samples — or a credit they give is added here, by a person, with
// a reason. It rides with a payment the page already has, a PO's deposit or
// a container's balance for that supplier, and is signed off and paid with
// it; never part of the PO value, never split by the deposit %.
//
// Rules (user, 2026-09-29): a credit is a negative extra; admin, standard and
// accountant users add, change and mark extras paid by hand; a charge whose
// payment is already paid gets its own due date. A transfer settles an extra
// the way it settles a balance or a PI: once the money applied covers it
// within a bank charge (max of 1 and 1 %), on the absolute amounts.

const EXTRA_KINDS = ['mould', 'tooling', 'handling', 'samples', 'testing', 'freight', 'packaging', 'bank_charge', 'discount', 'customs', 'duty', 'delivery', 'credit_note', 'other'];
const EXTRA_LABEL = {
    mould: 'Mould cost', tooling: 'Tooling', handling: 'Handling fee', samples: 'Samples', testing: 'Testing',
    freight: 'Freight', packaging: 'Packaging', bank_charge: 'Bank charge', discount: 'Discount',
    customs: 'Customs clearance', duty: 'Import duty', delivery: 'Delivery', credit_note: 'Credit note', other: 'Other charge',
};
// 'shipment' (user, 2026-09-30): a cost of the shipment itself — freight,
// customs, delivery — paid to its own payee (the forwarder, in supplierName),
// with the container and in its own currency; it names no PO.
// 'account' (user, 2026-10-01): a supplier credit note — a credit held with
// the supplier, tied to no PO and no container. It is in no payment's figure
// until a person uses it in a transfer (Record payment), in whole or in part.
const RIDES_WITH = ['deposit', 'balance', 'shipment', 'account'];
const EDITOR_ROLES = ['admin', 'standard', 'accountant'];
// Where a suggested extra was read from: a supplier's invoice on a container.
const SOURCE_KINDS = ['shipment_document'];
// How both invoice readers (the PI check and the container-invoice reader)
// return the charges on a document that are not goods — for the page to
// suggest as extras. A discount or credit comes back negative.
const OTHER_CHARGES_SCHEMA = {
    type: 'array',
    items: {
        type: 'object',
        additionalProperties: false,
        properties: {
            description: { type: ['string', 'null'] },
            amount: { type: ['number', 'null'] },
            kind: { type: 'string', enum: EXTRA_KINDS },
        },
        required: ['description', 'amount', 'kind'],
    },
};
const EPS = 0.005;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

const canEdit = userType => EDITOR_ROLES.includes(String(userType));
const money = n => Math.round(Number(n) * 100) / 100;
const tolerance = amount => Math.max(1, Math.abs(amount) * 0.01);
const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));
const ymd = v => (v == null ? null : v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10));
const text = (v, max) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, max) : null);
const positiveId = v => (v == null || v === '' ? null : Number.isInteger(Number(v)) && Number(v) > 0 ? Number(v) : NaN);

// { value } or { error } — an extra as the page sends it (POST and PUT alike).
function parseExtraBody(body) {
    const b = body && typeof body === 'object' ? body : {};
    const supplierName = text(b.supplierName, 255);
    if (!supplierName) return { error: 'supplierName is required.' };
    const currency = typeof b.currency === 'string' ? b.currency.trim().toUpperCase() : '';
    if (!/^[A-Z]{3}$/.test(currency)) return { error: 'currency must be a three-letter code.' };
    const raw = Number(b.amount);
    const amount = money(raw);
    if (b.amount == null || b.amount === '' || !Number.isFinite(raw) || amount === 0 || Math.abs(amount) >= 1e10) {
        return { error: 'amount must be a number other than 0 — negative for a credit.' };
    }
    const kind = String(b.kind || '');
    if (!EXTRA_KINDS.includes(kind)) return { error: `kind must be one of: ${EXTRA_KINDS.join(', ')}.` };
    if (kind === 'discount' && amount > 0) return { error: 'A discount is money off — enter it as a negative amount (a credit).' };
    if (b.description != null && typeof b.description === 'string' && b.description.trim().length > 255) {
        return { error: 'description cannot exceed 255 characters.' };
    }
    const ridesWith = String(b.ridesWith || '');
    if (!RIDES_WITH.includes(ridesWith)) return { error: 'ridesWith must be "deposit", "balance", "shipment" or "account".' };
    const purchaseOrderId = positiveId(b.purchaseOrderId);
    const shipmentId = positiveId(b.shipmentId);
    if (Number.isNaN(purchaseOrderId)) return { error: 'purchaseOrderId must be an id.' };
    if (Number.isNaN(shipmentId)) return { error: 'shipmentId must be an id.' };
    const shipmentReference = text(b.shipmentReference, 64);
    if (ridesWith === 'deposit') {
        if (purchaseOrderId == null) return { error: 'An extra riding with a deposit names its purchase order (purchaseOrderId).' };
        if (shipmentId != null || shipmentReference) return { error: 'An extra riding with a deposit belongs to the PO, not a container — leave the shipment out.' };
    } else if (ridesWith === 'account') {
        if (amount > 0) return { error: 'A credit note is a credit — enter it as a negative amount.' };
        if (purchaseOrderId != null || shipmentId != null || shipmentReference) {
            return { error: 'A credit note belongs to the supplier alone — leave the purchase order and the container out.' };
        }
    } else if (ridesWith === 'shipment') {
        if (purchaseOrderId != null) return { error: 'A shipment cost belongs to the container, not a purchase order — leave the PO out.' };
        if (shipmentId == null && !shipmentReference) return { error: 'A shipment cost names its container (shipmentId or shipmentReference).' };
    } else if (shipmentId == null && !shipmentReference) {
        return { error: 'An extra riding with a balance names its container (shipmentId or shipmentReference).' };
    }
    let dueDate = null;
    if (b.dueDate != null && b.dueDate !== '') {
        if (typeof b.dueDate !== 'string' || !DATE_RE.test(b.dueDate)) return { error: 'dueDate must be YYYY-MM-DD.' };
        dueDate = b.dueDate;
    }
    const hasSource = b.sourceKind != null || b.sourceId != null;
    const sourceId = positiveId(b.sourceId);
    if (hasSource && (!SOURCE_KINDS.includes(String(b.sourceKind)) || sourceId == null || Number.isNaN(sourceId))) {
        return { error: `source must be sourceKind (${SOURCE_KINDS.join(', ')}) with sourceId, or neither.` };
    }
    return {
        value: {
            supplierName, currency, amount, kind, description: text(b.description, 255),
            ridesWith, purchaseOrderId, shipmentId, shipmentReference, dueDate,
            sourceKind: hasSource ? String(b.sourceKind) : null, sourceId: hasSource ? sourceId : null,
            note: text(b.note, 2000),
        },
    };
}

// A transfer line applied to an extra: the same sign (a credit is used as a
// negative amount), and never more than is left on it. null, or { code, error }.
function checkLine({ lineAmount, extraAmount, appliedBefore }) {
    if (Math.sign(lineAmount) !== Math.sign(extraAmount)) {
        return extraAmount < 0
            ? { code: 'SIGN_MISMATCH', error: 'a credit is used as a negative amount.' }
            : { code: 'SIGN_MISMATCH', error: 'a charge is paid as a positive amount.' };
    }
    const remaining = money(extraAmount - appliedBefore);
    if (Math.abs(lineAmount) > Math.abs(remaining) + EPS) {
        return { code: 'OVER_APPLIED', error: `${Math.abs(lineAmount)} applied, but only ${Math.abs(remaining)} is left on it.`, remaining };
    }
    return null;
}

// Credits in one transfer, taken together: a credit is used against something
// being paid, never for more than it — and credits may cover everything paid,
// in which case no money is sent (user, 2026-10-01: "4,000 deposit, 4,500 of
// credit: 0 due"). null, or { code, error }. `lines`: [{ amount }], credits negative.
function checkCreditUse({ lines, amount }) {
    const total = money(lines.reduce((a, l) => a + l.amount, 0));
    const credits = lines.some(l => l.amount < 0);
    const paid = lines.some(l => l.amount > 0);
    const nothingSent = !(Number(amount) > EPS);
    if (!credits) {
        return nothingSent ? { code: 'NOTHING_SENT', error: 'amount must be above 0 — only credits can cover a payment with no money sent.' } : null;
    }
    if (!paid) return { code: 'CREDIT_ALONE', error: 'A credit is used against something being paid — tick what the transfer paid as well.' };
    if (total < -EPS) return { code: 'CREDIT_EXCEEDS', error: 'The credits used come to more than what is being paid — use less of the credit.' };
    if (!nothingSent && total <= EPS) {
        return { code: 'CREDIT_ALONE', error: 'The credits already cover everything ticked — record it with nothing sent, or tick what the money paid as well.' };
    }
    return null;
}

// Whether the money applied to an extra (signed) settles it.
function settles(applied, amount) {
    if (Math.sign(applied) !== Math.sign(amount)) return false;
    return Math.abs(applied) >= Math.abs(amount) - tolerance(amount);
}

const hasPayments = error => ({ status: 409, code: 'EXTRA_HAS_PAYMENTS', error });

// Editing an extra some transfer has already applied money to: it must stay
// the thing that money paid. null, or a refusal.
function decideEdit({ current, next, applied }) {
    if (Math.abs(applied) <= EPS) return null;
    if (next.currency !== current.currency) return hasPayments('Money has been applied to it — its currency cannot change.');
    if (next.supplierKey !== current.supplierKey) return hasPayments('Money has been applied to it — its supplier cannot change.');
    if (Math.sign(next.amount) !== Math.sign(current.amount)) return hasPayments('Money has been applied to it — a charge cannot become a credit, or the other way round.');
    if (Math.abs(next.amount) < Math.abs(applied) - EPS) return hasPayments(`Money has been applied to it — it cannot go below the ${Math.abs(money(applied))} already applied.`);
    return null;
}

function decideDelete({ applied }) {
    if (Math.abs(applied) <= EPS) return null;
    return hasPayments('Money has been applied to it — edit or delete that transfer first.');
}

// Marking paid by hand, or open again. { action: 'set' | 'none' } or a refusal.
function decideStatus({ status, settledByPaymentId, want }) {
    if (want !== 'paid' && want !== 'open') return { status: 400, code: 'BAD_FIELD', error: 'status must be "paid" or "open".' };
    if (want === status) return { action: 'none' };
    if (want === 'open' && settledByPaymentId != null) {
        return { status: 409, code: 'SETTLED_BY_TRANSFER', error: 'A recorded transfer paid it — edit or delete that transfer instead.' };
    }
    return { action: 'set' };
}

// "PO_00395J mould cost", "301 handling fee".
function lineLabel(r) {
    const what = EXTRA_LABEL[r.kind] || 'Extra charge';
    const where = r.po_number || r.shipment_reference || null;
    return where ? `${where} ${what.toLowerCase()}` : what;
}

function extraRowToJson(r, { applied = 0, settlements = [] } = {}) {
    const amount = Number(r.amount);
    return {
        id: r.id,
        supplierName: r.supplier_name,
        supplierKey: r.supplier_key,
        currency: r.currency,
        amount,
        kind: r.kind,
        label: lineLabel(r),
        description: r.description || null,
        ridesWith: r.rides_with,
        purchaseOrderId: r.purchase_order_id ?? null,
        poNumber: r.po_number || null,
        shipmentId: r.shipment_id ?? null,
        shipmentReference: r.shipment_reference || null,
        dueDate: ymd(r.due_date),
        sourceKind: r.source_kind || null,
        sourceId: r.source_id ?? null,
        status: r.status,
        paidOn: ymd(r.paid_on),
        settledByPaymentId: r.settled_by_payment_id ?? null,
        applied: money(applied),
        remaining: r.status === 'paid' ? 0 : money(amount - applied),
        note: r.note || null,
        createdByEmail: r.created_by_email,
        createdAt: iso(r.created_at),
        updatedByEmail: r.updated_by_email || null,
        updatedAt: iso(r.updated_at),
        settlements,
    };
}

module.exports = {
    EXTRA_KINDS,
    EXTRA_LABEL,
    RIDES_WITH,
    EDITOR_ROLES,
    SOURCE_KINDS,
    OTHER_CHARGES_SCHEMA,
    canEdit,
    parseExtraBody,
    checkLine,
    checkCreditUse,
    settles,
    decideEdit,
    decideDelete,
    decideStatus,
    lineLabel,
    extraRowToJson,
};
