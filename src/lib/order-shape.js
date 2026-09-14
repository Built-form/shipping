'use strict';

// Shared order-shaping helpers + the canonical order SELECT.
//
// These were originally defined inline in src/handlers/orders.js. They moved
// here so the receive/lookup service (src/services/order-receive.js) and the
// public carton-scan Lambda (src/handlers/carton-scan.js) can project orders
// and receipts in EXACTLY the same shape as the authed API — without either
// importing the 7000-line orders.js handler (which would pull in its Express
// app + every route and risk a require cycle). Pure functions only: no DB, no
// pool, no logging.

// Always project live `received_quantity` (physically checked-in stock pushed
// to Mintsoft) and `not_received_quantity` (recorded shortfalls, no Mintsoft
// side effect) from order_receipts. Outstanding = qty - (received +
// not_received). Soft-deleted rows are filtered out — every caller must append
// further conditions with AND.
const ORDER_SELECT = `
    SELECT orders.*,
           COALESCE((SELECT SUM(quantity) FROM order_receipts WHERE order_id = orders.id AND type = 'received'), 0) AS received_quantity,
           COALESCE((SELECT SUM(quantity) FROM order_receipts WHERE order_id = orders.id AND type = 'not_received'), 0) AS not_received_quantity,
           (SELECT JSON_ARRAYAGG(draft_container_name)
              FROM draft_container_allocations
             WHERE order_id = orders.id) AS draft_container_names,
           (SELECT JSON_ARRAYAGG(planned_container_name)
              FROM planned_container_allocations
             WHERE order_id = orders.id) AS planned_container_names
    FROM orders
    WHERE orders.deleted_at IS NULL
`;

function parseDates(raw) {
    if (typeof raw === 'string') return JSON.parse(raw) || {};
    return raw || {};
}

// MySQL's zero date ('0000-00-00 00:00:00') comes back from mysql2 as a JS
// Date whose time value is NaN, and toISOString() on one throws RangeError —
// which, from inside a .map() over the result set, fails the WHOLE request. A
// single unrepresentable cell in one row 500s every order in the list, so these
// helpers report an unusable date as "no date" rather than throwing.
function isUnrepresentableDate(val) {
    return val instanceof Date && Number.isNaN(val.getTime());
}

function formatDate(val) {
    if (!val || isUnrepresentableDate(val)) return null;
    return val.toISOString?.().slice(0, 10) ?? val;
}

// Like formatDate but preserves the time component — for DATETIME columns
// (e.g. delivery_date) where the client needs the wall-clock time, not just
// the calendar day. Returns a full ISO string for Date inputs, passes strings
// through untouched.
function formatDateTime(val) {
    if (!val || isUnrepresentableDate(val)) return null;
    return val.toISOString?.() ?? val;
}

// Normalize an expiry value (string, Date, or null) to YYYY-MM-DD or null.
// Mintsoft sometimes ships a far-future sentinel (e.g. 9999-12-31) when no
// expiry is set — treat anything past year 9000 as "no expiry".
// Returns the sentinel `false` for input that looks like a date but cannot be
// parsed, so callers can distinguish "missing" from "invalid".
function normalizeExpiry(val) {
    if (val === null || val === undefined || val === '') return null;
    if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(val)) {
        const year = Number(val.slice(0, 4));
        if (year >= 9000) return null;
        return val;
    }
    const d = val instanceof Date ? val : new Date(val);
    if (isNaN(d.getTime())) return false;
    if (d.getFullYear() >= 9000) return null;
    return d.toISOString().slice(0, 10);
}

function receiptToJson(row) {
    return {
        id: row.id,
        orderId: row.order_id,
        type: row.type || 'received',
        jfCode: row.jf_code || null,
        quantity: Number(row.quantity),
        locationId: row.location_id,
        warehouseId: row.warehouse_id,
        asnId: row.asn_id,
        asnItemId: row.asn_item_id,
        idempotencyKey: row.idempotency_key || null,
        batchNo: row.batch_no || null,
        expiryDate: formatDate(row.expiry_date),
        quarantined: !!row.quarantined,
        receivedAt: formatDateTime(row.received_at),
    };
}

function rowToOrder(row) {
    const quantity = Number(row.quantity || 0);
    const receivedQuantity = Number(row.received_quantity || 0);
    const notReceivedQuantity = Number(row.not_received_quantity || 0);
    // JSON_ARRAYAGG returns either a JSON string, an already-parsed array
    // (mysql2 unwraps JSON columns automatically), or NULL when the order
    // has no allocations.
    let partDraftContainer = [];
    if (row.draft_container_names) {
        partDraftContainer = typeof row.draft_container_names === 'string'
            ? JSON.parse(row.draft_container_names)
            : row.draft_container_names;
    }
    // Same treatment for the independent planned-container stream.
    let partPlannedContainer = [];
    if (row.planned_container_names) {
        partPlannedContainer = typeof row.planned_container_names === 'string'
            ? JSON.parse(row.planned_container_names)
            : row.planned_container_names;
    }
    return {
        id: row.id,
        jfCode: row.jf_code || null,
        asin: row.asin || null,
        productName: row.product_name || null,
        quantity,
        receivedQuantity,
        notReceivedQuantity,
        outstandingQuantity: Math.max(quantity - receivedQuantity - notReceivedQuantity, 0),
        partDraftContainer,
        partPlannedContainer,
        status: row.status,
        poNumber: row.po_number || null,
        supplier: row.supplier || null,
        containerNumber: row.container_number || null,
        vesselName: row.vessel_name || null,
        eta: formatDate(row.eta),
        cbmPerUnit: row.cbm_per_unit != null ? Number(row.cbm_per_unit) : null,
        orderCbm: row.order_cbm != null ? Number(row.order_cbm) : null,
        notes: row.notes || null,
        port: row.port || null,
        cartonCbm: row.carton_cbm != null ? Number(row.carton_cbm) : null,
        unitsPerCarton: row.units_per_carton != null ? Number(row.units_per_carton) : null,
        cartonWeight: row.carton_weight != null ? Number(row.carton_weight) : null,
        cartonHeight: row.carton_height != null ? Number(row.carton_height) : null,
        cartonWidth: row.carton_width != null ? Number(row.carton_width) : null,
        cartonDepth: row.carton_depth != null ? Number(row.carton_depth) : null,
        packSize: row.pack_size || null,
        scheduledDate: formatDate(row.scheduled_date),
        poDate: formatDate(row.po_date),
        qcStatus: row.qc_status || null,
        qcDate: formatDate(row.qc_date),
        qcInvoiceNumber: row.qc_invoice_number || null,
        dates: parseDates(row.dates),
        deliveryDate: formatDateTime(row.delivery_date),
        lotNumber: row.lot_number || null,
        mfgDate: formatDate(row.mfg_date),
        expDate: formatDate(row.exp_date),
        deliveryTime: row.delivery_time || null,
        containerStatus: row.container_status || null,
        bookingStatus: row.booking_status || null,
        arrivedDate: formatDate(row.arrived_date),
        externalContainerNumber: row.external_container_number || null,
        awbNumber: row.awb_number || null,
        purchaseOrderId: row.purchase_order_id ?? null,
        unitPrice: row.unit_price != null ? Number(row.unit_price) : null,
        actualReadyDate: formatDate(row.actual_ready_date),
        estimatedDepartureDate: formatDate(row.estimated_departure_date),
        shippedDate: formatDate(row.shipped_date),
        orderedDate: formatDate(row.ordered_date),
        estimatedReadyDate: formatDate(row.estimated_ready_date),
        artworkConfirmedDate: formatDate(row.artwork_confirmed_date),
    };
}

module.exports = {
    ORDER_SELECT,
    parseDates,
    formatDate,
    formatDateTime,
    normalizeExpiry,
    receiptToJson,
    rowToOrder,
};
