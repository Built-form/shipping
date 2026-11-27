'use strict';

// ── Sample orders ───────────────────────────────────────────────────────────
// A purchase order has a kind: 'stock' (every PO before 2026-11) or 'sample'.
// A sample PO buys a few units of a product to look at, not to sell. Its lines
// are rows of `orders` like any other, marked order_kind = 'sample', and they
// live by different rules:
//
//   · no container, no AWB, no shipment: a sample comes by courier, alone;
//   · a short path: SCHEDULED → PO_SENT → ON_AIR → ARRIVED_AT_WAREHOUSE →
//     RECEIVED, one step at a time, with DESTROYED for a parcel that was lost;
//   · never booked into Mintsoft: RECEIVED is a hand-over, recorded by its own
//     route; the cupboard count lives on the sample's page in JFPRO;
//   · its lot is the sample's own code from JFPRO's register (a lot of type
//     'sample'), fixed for good once the line has been sent;
//   · its money is the whole PO, owed upfront; a line with no price is a free
//     sample and is stored with a price of 0, so "free" is a stated value.
//
// The order routes enforce no status rules for stock lines (ShipLine's UI does
// that). For a sample they do, HERE: this lane skips the production gates, so
// it must not be usable to slip real stock past them. Every write to a sample
// row goes through validateSampleWrite, which looks at the complete resulting
// row, not at the fields the caller happened to send.
//
// Pure: no database, no clock. The callers read and lock the rows, work out
// `ctx` (who is asking, whether the PO is committed, whether the lot is an
// eligible sample lot) and write what comes back in `set`.

const KIND_STOCK = 'stock';
const KIND_SAMPLE = 'sample';
// A third kind, 'local' (docs/local-orders.md): stock a supplier delivers to the
// warehouse. Its rules are src/lib/local-orders.js; here it is only a word a PO
// may be made with.
const { KIND_LOCAL } = require('./local-orders');
const KINDS = [KIND_STOCK, KIND_SAMPLE, KIND_LOCAL];

// A typo guard, not an abuse control: nobody orders 500 samples of one thing.
const QTY_MAX = 50;
const COURIER_MAX = 64;

const SAMPLE_STATUSES = ['SCHEDULED', 'PO_SENT', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'DESTROYED'];
// A line is born a draft, or already sent (the PO view creates its lines at PO_SENT).
const CREATE_STATUSES = ['SCHEDULED', 'PO_SENT'];

const FORWARD = {
    SCHEDULED: 'PO_SENT',
    PO_SENT: 'ON_AIR',
    ON_AIR: 'ARRIVED_AT_WAREHOUSE',
    ARRIVED_AT_WAREHOUSE: 'RECEIVED',
};
// Lost or written off: only once the parcel has left the supplier.
const DESTROY_FROM = new Set(['ON_AIR', 'ARRIVED_AT_WAREHOUSE']);
// An admin's correction: one step back, never to SCHEDULED (an unsent mistake
// is deleted instead).
const CORRECTIONS = {
    RECEIVED: 'ARRIVED_AT_WAREHOUSE',
    ARRIVED_AT_WAREHOUSE: 'ON_AIR',
    ON_AIR: 'PO_SENT',
    DESTROYED: 'ON_AIR',
};
// What each correction clears: exactly what the step it undoes had set.
const CORRECTION_CLEARS = {
    'RECEIVED>ARRIVED_AT_WAREHOUSE': ['sample_received_qty'],
    'ARRIVED_AT_WAREHOUSE>ON_AIR': ['arrived_date'],
    'ON_AIR>PO_SENT': ['shipped_date', 'courier_name', 'courier_tracking'],
    'DESTROYED>ON_AIR': [],
};

// The only route that may set RECEIVED.
const VIA_RECEIVE = 'sample-received';

// How far along each state is; a state needs everything the earlier ones did.
// DESTROYED happens to a parcel that was sent, so it needs what ON_AIR needed.
const LEVEL = { SCHEDULED: 0, PO_SENT: 1, ON_AIR: 2, ARRIVED_AT_WAREHOUSE: 3, RECEIVED: 4, DESTROYED: 2 };
const REQUIRED_AT = [
    [],
    [['poNumber', 'po_number'], ['poDate', 'po_date'], ['supplier', 'supplier']],
    [['shippedDate', 'shipped_date']],
    [['arrivedDate', 'arrived_date']],
    [['receivedQuantity', 'sample_received_qty']],
];

const NO_CONTAINER_COLUMNS = ['container_number', 'awb_number', 'external_container_number', 'shipment_id'];

const ARMING_PREFIX = 'sample_orders_on:';

function isBlank(v) {
    return v === null || v === undefined || (typeof v === 'string' && v.trim() === '');
}

// A refusal, in the shape every route sends back: { error, code, ... }.
function refuse(code, message, extra = {}, http = 409) {
    return { ok: false, http, code, message, ...extra };
}

// 'stock' when nothing is sent (every caller from before kinds existed), the
// kind when it is one we know, null for anything else: an unknown kind is
// refused, never guessed.
function normalizeKind(raw) {
    if (raw === undefined || raw === null) return KIND_STOCK;
    if (typeof raw !== 'string') return null;
    const v = raw.trim().toLowerCase();
    if (v === '') return KIND_STOCK;
    return KINDS.includes(v) ? v : null;
}

function isSample(row) {
    if (!row) return false;
    return (row.order_kind ?? row.orderKind) === KIND_SAMPLE;
}

function canMove(from, to, { isAdmin = false, via = null } = {}) {
    if (from === to) return true;
    if (!SAMPLE_STATUSES.includes(from) || !SAMPLE_STATUSES.includes(to)) return false;
    if (FORWARD[from] === to) return to === 'RECEIVED' ? via === VIA_RECEIVE : true;
    if (to === 'DESTROYED') return isAdmin && DESTROY_FROM.has(from);
    return isAdmin && CORRECTIONS[from] === to;
}

// What may follow `from`, in the order a person would reach for it: the next
// step, then (an admin) writing it off, then the correction. RECEIVED is listed
// for an arrived line although only the receive route can set it: the list
// says where the line can go, the route says how.
function allowedNext(from, { isAdmin = false } = {}) {
    if (!SAMPLE_STATUSES.includes(from)) return [];
    const out = [];
    if (FORWARD[from]) out.push(FORWARD[from]);
    if (isAdmin && DESTROY_FROM.has(from)) out.push('DESTROYED');
    if (isAdmin && CORRECTIONS[from]) out.push(CORRECTIONS[from]);
    return out;
}

// Every required field `row` lacks to sit in `status`, earliest state first.
function missingFor(status, row) {
    const level = LEVEL[status];
    if (level === undefined) return [];
    const missing = [];
    for (let l = 0; l <= level; l++) {
        for (const [field, col] of REQUIRED_AT[l]) {
            if (isBlank(row ? row[col] : null)) missing.push(field);
        }
    }
    return missing;
}

// A whole number as a number or as digits; anything else is not one.
function toInt(v) {
    if (typeof v === 'number') return Number.isInteger(v) ? v : NaN;
    if (typeof v === 'string' && /^\d+$/.test(v.trim())) return Number(v.trim());
    return NaN;
}

function validQuantity(v) {
    const n = toInt(v);
    return Number.isInteger(n) && n >= 1 && n <= QTY_MAX ? n : null;
}

// No price is a free sample: 0. Otherwise a number that is not negative.
function normalizePrice(v) {
    if (isBlank(v)) return 0;
    const n = typeof v === 'number' ? v : Number(String(v).trim());
    return Number.isFinite(n) && n >= 0 ? n : null;
}

const codeKey = (v) => String(v ?? '').trim().toUpperCase();
const lotKey = (v) => String(v ?? '').trim();

// A change to what the PO costs, on a PO the supplier already has (or that has
// money on it), is an admin's edit and says why.
function billRefusal(ctx) {
    if (!ctx.po || !ctx.po.committed) return null;
    if (!ctx.isAdmin) {
        return refuse('SAMPLE_BILL_LOCKED',
            'This sample order has been sent to the supplier or has a payment on it. Only an admin can change what it costs.');
    }
    if (isBlank(ctx.reason)) {
        return refuse('SAMPLE_REASON_REQUIRED', 'Say why the order is being changed.');
    }
    return null;
}

/**
 * May `after` be written, given `before` (the stored row, or null when the line
 * is being created)? Both are orders rows, snake_case, COMPLETE: `after` is the
 * row as it would stand once the write is applied.
 *
 * ctx:
 *   via         'create' | 'update' | 'status' | 'sample-received' | 'change-request'
 *   isAdmin     the caller is an admin
 *   reason      why, for an edit that needs one
 *   po          { id, kind, committed } of the line's purchase order, or null
 *   lotChecked  true: the lot is a sample lot of this product in JFPRO's
 *               register; false: it is not; null: nobody looked. null is only
 *               accepted when neither the product nor the lot is changing.
 *
 * Returns { ok: true, set, statusChanged, supersedePending, billChanged,
 * clearReceivedStamp } where `set` holds columns the caller must write on top
 * of its own (normalised values, what a correction clears, the ever-sent
 * mark), or a refusal { ok: false, http, code, message, allowedNext?, missing? }.
 */
function validateSampleWrite(before, after, ctx = {}) {
    const creating = !before;
    const isAdmin = ctx.isAdmin === true;
    const via = ctx.via || (creating ? 'create' : 'update');
    const row = { ...after };
    const set = {};

    // ── kind and purchase order ──────────────────────────────────────────
    if (!isSample(row) || (before && !isSample(before))) {
        return refuse('SAMPLE_KIND_FIXED', 'The kind of an order never changes.');
    }
    if (before && Number(before.purchase_order_id) !== Number(row.purchase_order_id)) {
        return refuse('SAMPLE_PO_FIXED', 'A sample line cannot move to another purchase order.');
    }
    const po = ctx.po || null;
    if (!po || po.kind !== KIND_SAMPLE || row.purchase_order_id == null || Number(po.id) !== Number(row.purchase_order_id)) {
        return refuse('SAMPLE_PO_REQUIRED', 'A sample line belongs to a sample purchase order.');
    }

    // ── never in a container ─────────────────────────────────────────────
    for (const col of NO_CONTAINER_COLUMNS) {
        if (!isBlank(row[col])) {
            return refuse('SAMPLE_NO_CONTAINER', 'A sample order travels alone: it has no container, AWB or shipment.', { field: col });
        }
    }
    // No ASIN either: stock figures summed by ASIN must never meet a sample.
    if (!isBlank(row.asin)) { set.asin = null; row.asin = null; }

    // ── product and lot ──────────────────────────────────────────────────
    if (isBlank(row.jf_code)) return refuse('SAMPLE_PRODUCT_REQUIRED', 'A sample order needs its product.', {}, 400);
    if (isBlank(row.lot_number)) return refuse('SAMPLE_LOT_REQUIRED', 'A sample order needs the sample code as its lot.', {}, 400);
    const identityChanged = creating
        || codeKey(before.jf_code) !== codeKey(row.jf_code)
        || lotKey(before.lot_number) !== lotKey(row.lot_number);
    if (identityChanged) {
        if (before && Number(before.sample_ever_sent) === 1) {
            return refuse('SAMPLE_IDENTITY_FIXED', 'This sample has been sent: its product and sample code can no longer change.');
        }
        if (ctx.lotChecked === false) {
            return refuse('SAMPLE_LOT_NOT_ELIGIBLE', 'That lot is not a sample of this product in JFPRO.');
        }
        if (ctx.lotChecked !== true) {
            return refuse('SAMPLE_LOT_UNCHECKED', 'The sample code could not be checked against JFPRO. Nothing was saved.', {}, 503);
        }
    }

    // ── quantity and price ───────────────────────────────────────────────
    const quantity = validQuantity(row.quantity);
    if (quantity === null) {
        return refuse('SAMPLE_QUANTITY', `A sample line holds a whole number of units, 1 to ${QTY_MAX}.`, {}, 400);
    }
    if (creating || row.quantity !== quantity) set.quantity = quantity;
    row.quantity = quantity;

    const price = normalizePrice(row.unit_price);
    if (price === null) return refuse('SAMPLE_PRICE', 'The unit price must be a number that is not negative, or left empty for a free sample.', {}, 400);
    if (creating || row.unit_price !== price) set.unit_price = price;
    row.unit_price = price;

    for (const col of ['courier_name', 'courier_tracking']) {
        if (!isBlank(row[col]) && String(row[col]).trim().length > COURIER_MAX) {
            return refuse('SAMPLE_COURIER', `The courier and the tracking number hold ${COURIER_MAX} characters each.`, { field: col }, 400);
        }
    }

    // ── status ───────────────────────────────────────────────────────────
    const from = before ? before.status : null;
    const to = row.status;
    if (creating) {
        if (!CREATE_STATUSES.includes(to)) {
            return refuse('SAMPLE_STATUS', 'A sample line starts as a draft or as sent to the supplier.', { allowedNext: [...CREATE_STATUSES] });
        }
    } else if (from !== to) {
        if (!canMove(from, to, { isAdmin, via })) {
            return refuse('SAMPLE_STATUS', `A sample order cannot move from ${from} to ${to}.`, { allowedNext: allowedNext(from, { isAdmin }) });
        }
        // Writing a sample off is a statement about a parcel: it says why.
        if (to === 'DESTROYED' && isBlank(ctx.reason)) {
            return refuse('SAMPLE_REASON_REQUIRED', 'Say what happened to the sample.');
        }
        // A correction clears what the step it undoes had set.
        const clears = CORRECTION_CLEARS[`${from}>${to}`];
        if (clears) {
            for (const col of clears) { set[col] = null; row[col] = null; }
        }
    }
    const statusChanged = creating || from !== to;
    const clearReceivedStamp = !creating && from === 'RECEIVED' && to === 'ARRIVED_AT_WAREHOUSE';

    const missing = missingFor(to, row);
    if (missing.length) {
        return refuse('SAMPLE_MISSING_FIELDS', `Missing before the order can be ${to}: ${missing.join(', ')}.`, { missing });
    }

    // Sent once is sent for good: no correction and no delete frees the code.
    if (LEVEL[to] >= LEVEL.ON_AIR && to !== 'DESTROYED' && Number(row.sample_ever_sent) !== 1) set.sample_ever_sent = 1;

    // ── the bill ─────────────────────────────────────────────────────────
    const billChanged = creating
        || Number(before.quantity) !== quantity
        || normalizePrice(before.unit_price) !== price;
    if (billChanged) {
        const refusal = billRefusal({ ...ctx, isAdmin, po });
        if (refusal) return refusal;
    }

    const supersedePending = !creating && (
        from !== to
        || codeKey(before.jf_code) !== codeKey(row.jf_code)
        || lotKey(before.lot_number) !== lotKey(row.lot_number)
        || Number(before.quantity) !== quantity
    );

    return { ok: true, set, statusChanged, supersedePending, billChanged, clearReceivedStamp };
}

// A sample line that was ever sent stays: its exits are RECEIVED and
// DESTROYED, and its bill changes by a credit, not by removing the row.
function validateSampleDelete(before, ctx = {}) {
    if (before && Number(before.sample_ever_sent) === 1) {
        return refuse('SAMPLE_SENT_NO_DELETE', 'This sample has been sent and cannot be deleted. Mark it received, or written off.');
    }
    const refusal = billRefusal({ ...ctx, isAdmin: ctx.isAdmin === true });
    if (refusal) return refusal;
    return { ok: true, billChanged: true };
}

/**
 * A change to a sample purchase order's own fields.
 *   po     { kind, committed, supplier, currency, shippingTotal } as stored
 *   patch  the fields the caller wants to change (absent = untouched)
 */
function validateSamplePoEdit(po, patch = {}, ctx = {}) {
    const has = (k) => Object.prototype.hasOwnProperty.call(patch, k) && patch[k] !== undefined;
    if (has('kind') && normalizeKind(patch.kind) !== po.kind) {
        return refuse('SAMPLE_KIND_FIXED', 'The kind of a purchase order never changes.');
    }
    const supplierChanged = has('supplier') && String(patch.supplier ?? '').trim() !== String(po.supplier ?? '').trim();
    const currencyChanged = has('currency') && codeKey(patch.currency) !== codeKey(po.currency);
    if (po.committed && (supplierChanged || currencyChanged)) {
        return refuse('SAMPLE_PO_FROZEN',
            'This sample order has been sent to the supplier or has a payment on it: its supplier and currency are fixed.');
    }
    const billChanged = has('shippingTotal') && Number(patch.shippingTotal ?? 0) !== Number(po.shippingTotal ?? 0);
    if (billChanged) {
        const refusal = billRefusal({ ...ctx, isAdmin: ctx.isAdmin === true, po });
        if (refusal) return refusal;
    }
    return { ok: true, billChanged };
}

// A sample PO goes only when nothing hangs on it, and then only as a soft
// delete: its deleted lines still point at a sample PO.
function validateSamplePoDelete({ liveLines = 0, moneyRecorded = false } = {}) {
    if (Number(liveLines) > 0) return refuse('SAMPLE_PO_HAS_LINES', 'Delete or finish the lines of this sample order first.');
    if (moneyRecorded) return refuse('SAMPLE_PO_HAS_MONEY', 'This sample order has a payment recorded on it and cannot be deleted.');
    return { ok: true };
}

// The body of POST /orders/:id/sample-received. What arrived can differ from
// what was ordered; the order (and so the bill) is not touched, but a
// difference is explained.
function parseSampleReceive(body, line) {
    const quantity = validQuantity(body ? body.quantity : null);
    if (quantity === null) {
        return refuse('SAMPLE_QUANTITY', `Say how many arrived: a whole number, 1 to ${QTY_MAX}.`, {}, 400);
    }
    const note = isBlank(body.note) ? null : String(body.note).trim();
    if (quantity !== Number(line ? line.quantity : NaN) && !note) {
        return refuse('SAMPLE_NOTE_REQUIRED', 'Fewer or more arrived than were ordered: add a note saying what happened.', {}, 400);
    }
    return { ok: true, quantity, note };
}

// A stock line may not TAKE a sample lot (that would walk the sample into
// Mintsoft through the ordinary receive). One it already carries is left alone:
// some orders from before sample orders existed were written against sticker codes.
function stockLotRefusal({ lotIsSample = false, lotChanged = false } = {}) {
    if (lotIsSample && lotChanged) {
        return refuse('SAMPLE_LOT_ON_STOCK', 'That lot is a sample. Order it as a sample order; a stock order cannot carry it.');
    }
    return null;
}

// The feature is armed by a row in app_migrations named for ONE stage, so the
// row a TEST rebuild copies from production never arms TEST.
function armingRowName(stage) {
    const s = String(stage ?? '').trim().toLowerCase();
    return s ? `${ARMING_PREFIX}${s}` : null;
}

function isArmed(rowNames, stage) {
    const name = armingRowName(stage);
    return name !== null && Array.isArray(rowNames) && rowNames.includes(name);
}

module.exports = {
    KIND_STOCK,
    KIND_SAMPLE,
    KIND_LOCAL,
    KINDS,
    QTY_MAX,
    COURIER_MAX,
    SAMPLE_STATUSES,
    CREATE_STATUSES,
    VIA_RECEIVE,
    NO_CONTAINER_COLUMNS,
    ARMING_PREFIX,
    normalizeKind,
    isSample,
    canMove,
    allowedNext,
    missingFor,
    normalizePrice,
    validateSampleWrite,
    validateSampleDelete,
    validateSamplePoEdit,
    validateSamplePoDelete,
    parseSampleReceive,
    stockLotRefusal,
    armingRowName,
    isArmed,
};
