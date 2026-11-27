'use strict';

// Local orders: the rules, pure. Read docs/local-orders.md first.
//
// A purchase order of kind 'local' buys stock from a supplier who delivers it
// to the warehouse themselves. It is stock (it is booked into Mintsoft, counts
// in Cashboard's figures, and is paid like any PO), but it is made, sent,
// delivered and booked in, and nothing else: no production, QC, port,
// container, air shipment or shipment. Its lines are rows of `orders` marked
// order_kind = 'local', stamped by the server from the line's PO.
//
// ShipLine keeps the record; the supplier portal is not changed for it.
//
// Pure: no database, no clock. src/services/sample-orders.js (guardWrite)
// reads the PO and calls validateLocalWrite on every write of a local line.

const KIND_LOCAL = 'local';

// Every status a local line may hold. ON_AIR is ShipLine's On Air/Road column:
// for a local line, the supplier's van on its way (the user, 2026-11-27), in no
// container and with no AWB; the step is optional. PARTIALLY_RECEIVED is
// written by the Mintsoft receive and the shortfall record, never chosen.
const LOCAL_STATUSES = ['SCHEDULED', 'PO_SENT', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'PARTIALLY_RECEIVED', 'RECEIVED'];
const CREATE_STATUSES = ['SCHEDULED', 'PO_SENT'];

// What a shipment writes onto a line. A local line holds none of them.
const SHIPPING_FIELDS = ['container_number', 'external_container_number', 'awb_number', 'shipment_id'];

const NEXT = {
    SCHEDULED: ['PO_SENT'],
    PO_SENT: ['ON_AIR', 'ARRIVED_AT_WAREHOUSE'],
    ON_AIR: ['ARRIVED_AT_WAREHOUSE'],
    // Received is reached by booking the stock into Mintsoft.
    ARRIVED_AT_WAREHOUSE: ['RECEIVED'],
    PARTIALLY_RECEIVED: ['RECEIVED'],
    RECEIVED: [],
};

function refuse(code, message, extra = {}, http = 409) {
    return { ok: false, http, code, message, ...extra };
}

function isLocal(row) {
    if (!row) return false;
    return (row.order_kind ?? row.orderKind) === KIND_LOCAL;
}

// The stages a local line can take next; for a line not yet made, the stages it
// can be made at.
function allowedNext(from) {
    if (from == null) return [...CREATE_STATUSES];
    return [...(NEXT[from] || [])];
}

const present = v => v !== null && v !== undefined && String(v).trim() !== '';

/**
 * May this write of a local line go ahead?
 *   before  the stored row (snake_case), or null when creating
 *   after   the row as it would stand after the write (snake_case, complete)
 *   ctx.po  { id, kind } of the PO the row would be on, or null
 * Returns { ok: true, set } (set: the columns to write on top of the caller's
 * own, the kind on a create) or a refusal { ok: false, http, code, message, ... }.
 */
function validateLocalWrite(before, after, { po } = {}) {
    if (!po || po.kind !== KIND_LOCAL || after.purchase_order_id == null
        || Number(po.id) !== Number(after.purchase_order_id)) {
        return refuse('LOCAL_PO_REQUIRED', 'A local order line stays on its local purchase order.');
    }
    const allowed = before ? LOCAL_STATUSES : CREATE_STATUSES;
    if (!allowed.includes(after.status)) {
        return refuse('LOCAL_NOT_SHIPPED',
            `A local order is delivered to the warehouse by the supplier: it goes PO Sent, On Air/Road (optional), Arrived at Warehouse, Received, and never ${after.status}.`,
            { allowedNext: allowedNext(before ? before.status : null) });
    }
    for (const field of SHIPPING_FIELDS) {
        if (present(after[field])) {
            return refuse('LOCAL_NOT_SHIPPED',
                'A local order travels in no container, air shipment or shipment.',
                { field, allowedNext: allowedNext(before ? before.status : null) });
        }
    }
    return { ok: true, set: before ? {} : { order_kind: KIND_LOCAL } };
}

// What a booking into Mintsoft still needs from the line. A stock line lands
// with a batch and an expiry or not at all. A local line is booked in with
// whatever it has: the receive dialog asks, in case the product carries them
// (the user, 2026-11-27).
function missingForBookIn({ kind, lot, expiry }) {
    if (kind === KIND_LOCAL) return [];
    const missing = [];
    if (!present(lot)) missing.push('lot_number');
    if (!present(expiry)) missing.push('exp_date');
    return missing;
}

// "Not for a local order": the routes that pack, split, book a shipment or move
// a line from a supplier's email.
function refuseLocal(row, what) {
    if (!isLocal(row)) return null;
    return refuse('LOCAL_NOT_SHIPPED', `A local order cannot be ${what}: the supplier delivers it to the warehouse.`,
        { allowedNext: allowedNext(row.status) });
}

module.exports = {
    KIND_LOCAL,
    LOCAL_STATUSES,
    SHIPPING_FIELDS,
    isLocal,
    allowedNext,
    validateLocalWrite,
    missingForBookIn,
    refuseLocal,
};
