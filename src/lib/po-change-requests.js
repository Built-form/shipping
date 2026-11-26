'use strict';

// ── Supplier change requests from jportal ───────────────────────────────────
// The supplier portal (the jportal repo) never writes to this schema. A
// supplier's change to a PO line is STAGED there as a `pending` row in
// jportal.po_change_requests; staff see it here with the line's live values
// beside the requested ones, and apply it. Applying makes exactly the writes
// the old public portal (src/handlers/supplier-portal.js) made straight away:
//   estimatedReadyDate → orders.estimated_ready_date   (date, or null to clear)
//   actualReadyDate    → orders.actual_ready_date      (date, or null to clear)
//   markReadyForQc     → orders.status IN_PRODUCTION → READY_FOR_QC, only with
//                        an actual ready date (stored, or set by the same row)
// and only while the line is still at the factory (EDITABLE_STATUSES), and its
// purchase order is live and still belongs to the row's supplier. jportal checks
// that when the supplier submits, but shipping does not take the row's word for
// it: the routes re-check it against purchase_orders and suppliers (a supplier
// row with id = supplier_id whose name is exactly purchase_orders.supplier, as
// jportal's own access rule) and pass the answer in as `poOwned`.
//
// Pure: no database, no clock. The routes (src/services/po-change-request-routes.js)
// read and lock the rows, call planApply, and write what it returns.

// Same set as supplier-portal.js and jportal's src/lib/poChanges.js.
const EDITABLE_STATUSES = new Set(['PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY']);
const READY_FOR_QC_FROM = 'IN_PRODUCTION';
const READY_FOR_QC_STATUS = 'READY_FOR_QC';

const DATE_FIELDS = {
    estimatedReadyDate: 'estimated_ready_date',
    actualReadyDate: 'actual_ready_date',
};

const REQUEST_STATUSES = new Set(['pending', 'applied', 'rejected', 'superseded', 'withdrawn']);

// Why a row cannot be applied as it stands. The codes are API surface.
const BLOCKERS = {
    line_missing: 'The line has been deleted or moved to another PO.',
    po_supplier_changed: 'The purchase order no longer belongs to this supplier, or has been deleted.',
    line_locked: 'The line has moved past the factory stages; its ready dates are locked.',
    bad_date: 'The requested date is not a valid YYYY-MM-DD date.',
    qc_from: 'The line can only be marked ready for QC while it is under production.',
    qc_no_date: 'Marking ready for QC needs an actual ready date.',
    nothing_requested: 'The request holds no change this API knows how to apply.',
    // A sample order (docs/sample-orders.md): one request, "mark as sent".
    sample_only: 'Only a sample order is marked as sent this way.',
    not_for_sample: 'A sample order has no ready dates: its supplier marks it as sent.',
    sent_from: 'The line is not waiting to be sent any more.',
    sent_future: 'The sent date is in the future.',
    sent_before_order: 'The sent date is before the order was placed.',
    bad_courier: 'The courier and the tracking number are text of at most 64 characters each.',
    stale: 'The line changed in ShipLine after the supplier opened it. Reject this; they can mark it as sent again.',
};

// "Mark as sent" moves a sample line from waiting to on its way, and nothing else.
const SAMPLE_KIND = 'sample';
const SENT_FROM = 'PO_SENT';
const SENT_STATUS = 'ON_AIR';
const COURIER_MAX = 64;

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// The schema jportal lives in: moved to src/lib/jportal.js (shared with the
// proforma-invoice routes) and re-exported from here unchanged.
const { jportalSchema } = require('./jportal');

// A JSON column as mysql2 returns it (object, or a string on some servers).
function parseJsonObject(value) {
    let v = value;
    if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch { return {}; }
    }
    return v && typeof v === 'object' && !Array.isArray(v) ? v : {};
}

// 'YYYY-MM-DD', or null for "clear". undefined when the value is not a date.
function normalizeDate(raw) {
    if (raw === null || raw === undefined || raw === '') return null;
    if (typeof raw !== 'string' || !DATE_RE.test(raw)) return undefined;
    const d = new Date(`${raw}T00:00:00Z`);
    if (Number.isNaN(d.getTime()) || d.toISOString().slice(0, 10) !== raw) return undefined;
    const year = d.getUTCFullYear();
    if (year < 2020 || year > 2100) return undefined;
    return raw;
}

function toDateOnly(value) {
    if (value === null || value === undefined || value === '') return null;
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
    return String(value).slice(0, 10);
}

// The live line as the rules need it, from an orders row (snake_case), or null.
function lineFromRow(row) {
    if (!row) return null;
    return {
        id: Number(row.id),
        purchaseOrderId: row.purchase_order_id == null ? null : Number(row.purchase_order_id),
        status: row.status || null,
        estimatedReadyDate: toDateOnly(row.estimated_ready_date),
        actualReadyDate: toDateOnly(row.actual_ready_date),
        // What "mark as sent" needs. A row read without these (a database from
        // before sample orders) is a stock line.
        orderKind: row.order_kind === SAMPLE_KIND ? SAMPLE_KIND : 'stock',
        jfCode: row.jf_code || null,
        quantity: row.quantity == null ? null : Number(row.quantity),
        shippedDate: toDateOnly(row.shipped_date),
        courierName: row.courier_name || null,
        courierTracking: row.courier_tracking || null,
        // UTC, as text ('...Z'): compared with the moment the supplier's page was loaded.
        statusChangedAt: row.sample_status_changed_at_iso || null,
        poDate: toDateOnly(row.po_date),
    };
}

// A courier name or tracking number as the supplier typed it: trimmed text, null
// when empty or absent, undefined when it is not text or too long.
function courierText(raw) {
    if (raw === undefined || raw === null) return null;
    if (typeof raw !== 'string') return undefined;
    const text = raw.trim();
    if (text.length > COURIER_MAX) return undefined;
    return text || null;
}

// Is the line still what the supplier was looking at? `base` is what the portal
// stored with the request: the line's status, product and quantity as shown,
// and when the page was loaded. No record of that is not a pass. The time
// catches what the three values cannot: a line sent and corrected back in
// between looks the same and is not.
function isStale(base, line) {
    if (!base || typeof base !== 'object') return true;
    if (base.status !== line.status) return true;
    if (String(base.jfCode ?? '').trim().toUpperCase() !== String(line.jfCode ?? '').trim().toUpperCase()) return true;
    if (Number(base.quantity) !== Number(line.quantity)) return true;
    const seen = Date.parse(base.seenAt);
    if (!Number.isFinite(seen)) return true;
    const changed = line.statusChangedAt ? Date.parse(line.statusChangedAt) : NaN;
    return Number.isFinite(changed) && seen < changed;
}

// What applying a sample line's "mark as sent" would write.
//   ms    requested.markSent: { sentOn, courier?, tracking? }
//   opts  { base, today, useSupplierCourier }: `today` is the latest date it is
//         anywhere (a supplier ahead of UTC is not "in the future"); courier
//         details staff entered first are kept unless useSupplierCourier.
function planMarkSent(ms, line, opts, empty) {
    if (line.status !== SENT_FROM) return { blockers: ['sent_from'], ...empty };
    const asked = ms && typeof ms === 'object' && !Array.isArray(ms) ? ms : {};
    const blockers = [];

    const sentOn = normalizeDate(asked.sentOn);
    if (!sentOn) {
        blockers.push('bad_date');
    } else {
        if (opts.today && sentOn > opts.today) blockers.push('sent_future');
        if (line.poDate && sentOn < line.poDate) blockers.push('sent_before_order');
    }
    const courier = courierText(asked.courier);
    const tracking = courierText(asked.tracking);
    if (courier === undefined || tracking === undefined) blockers.push('bad_courier');
    if (isStale(opts.base, line)) blockers.push('stale');
    if (blockers.length) return { blockers: [...new Set(blockers)], ...empty };

    const cols = { shipped_date: sentOn };
    const before = { status: line.status, shippedDate: line.shippedDate };
    const after = { status: SENT_STATUS, shippedDate: sentOn };
    for (const [field, col, value] of [['courierName', 'courier_name', courier], ['courierTracking', 'courier_tracking', tracking]]) {
        if (value === null || value === line[field]) continue;
        if (line[field] !== null && !opts.useSupplierCourier) continue;
        cols[col] = value;
        before[field] = line[field];
        after[field] = value;
    }
    return { blockers: [], cols, before, after, moveToQc: false, moveToSent: true };
}

/**
 * What applying `requested` to `line` would write.
 *   requested: the row's requested_json, parsed.
 *   line: lineFromRow(...) of the live line, or null when it is gone.
 *   purchaseOrderId: the row's purchase_order_id; a line now on another PO is
 *     gone, and so is one when this is missing (fails closed).
 *   poOwned: true only when that purchase order is not deleted and the row's
 *     supplier still owns it (worked out by the caller); anything else blocks.
 * Returns { blockers: string[], cols: { column: value }, before, after, moveToQc, moveToSent }.
 * `cols` holds only columns whose value actually changes; with blockers it is empty.
 *
 * A SAMPLE line (line.orderKind 'sample') takes one request and no other:
 * markSent, which moves it from PO_SENT to ON_AIR (moveToSent). `opts` is for
 * that: { base, today, useSupplierCourier }, see planMarkSent.
 */
function planApply(requested, line, purchaseOrderId, poOwned, opts = {}) {
    const empty = { cols: {}, before: {}, after: {}, moveToQc: false, moveToSent: false };
    if (!line || purchaseOrderId == null || line.purchaseOrderId !== Number(purchaseOrderId)) {
        return { blockers: ['line_missing'], ...empty };
    }
    if (poOwned !== true) return { blockers: ['po_supplier_changed'], ...empty };

    const asksSent = requested.markSent !== undefined && requested.markSent !== null && requested.markSent !== false;
    if (line.orderKind === SAMPLE_KIND) {
        const asksStock = requested.markReadyForQc === true
            || Object.keys(DATE_FIELDS).some(f => Object.prototype.hasOwnProperty.call(requested, f));
        if (asksStock) return { blockers: ['not_for_sample'], ...empty };
        if (!asksSent) return { blockers: ['nothing_requested'], ...empty };
        return planMarkSent(requested.markSent, line, opts, empty);
    }
    if (asksSent) return { blockers: ['sample_only'], ...empty };

    if (!EDITABLE_STATUSES.has(line.status)) return { blockers: ['line_locked'], ...empty };

    const blockers = [];
    const cols = {};
    const before = {};
    const after = {};
    let known = false;

    const next = { ...line };
    for (const [field, col] of Object.entries(DATE_FIELDS)) {
        if (!Object.prototype.hasOwnProperty.call(requested, field)) continue;
        known = true;
        const value = normalizeDate(requested[field]);
        if (value === undefined) { blockers.push('bad_date'); continue; }
        next[field] = value;
        if (value !== line[field]) {
            cols[col] = value;
            before[field] = line[field];
            after[field] = value;
        }
    }

    let moveToQc = false;
    if (requested.markReadyForQc === true) {
        known = true;
        if (line.status === READY_FOR_QC_FROM) {
            if (!next.actualReadyDate) blockers.push('qc_no_date');
            else moveToQc = true;
        } else if (line.status !== READY_FOR_QC_STATUS) {
            // Already ready for QC is a no-op, as in supplier-portal.js.
            blockers.push('qc_from');
        }
    }

    if (!known) blockers.push('nothing_requested');
    if (blockers.length) return { blockers: [...new Set(blockers)], ...empty };
    if (moveToQc) {
        before.status = line.status;
        after.status = READY_FOR_QC_STATUS;
    }
    return { blockers: [], cols, before, after, moveToQc, moveToSent: false };
}

/**
 * One change-request row for the API: the line's live (old) value and the
 * requested (new) value for every field the supplier asked to change, plus
 * whether it can be applied right now. `row` is a po_change_requests row
 * joined with the live line (live_* columns) and po_owned (1 when its purchase
 * order is live and still the supplier's); see the routes' SELECT.
 */
function requestToJson(row, opts = {}) {
    const base = parseJsonObject(row.base_json);
    const requested = parseJsonObject(row.requested_json);
    const line = row.live_id == null ? null : lineFromRow({
        id: row.live_id,
        purchase_order_id: row.live_purchase_order_id,
        status: row.live_status,
        estimated_ready_date: row.live_estimated_ready_date,
        actual_ready_date: row.live_actual_ready_date,
        order_kind: row.live_order_kind,
        jf_code: row.live_jf_code,
        quantity: row.live_quantity,
        shipped_date: row.live_shipped_date,
        courier_name: row.live_courier_name,
        courier_tracking: row.live_courier_tracking,
        sample_status_changed_at_iso: row.live_sample_status_changed_at_iso,
        po_date: row.live_po_date,
    });

    // Pending: old is the line NOW, which is what applying overwrites. Decided
    // (applied, superseded…): old is the line when the supplier sent it, so the
    // history shows what the request changed, not today's value twice.
    const pending = row.status === 'pending';
    const change = (field, submittedOld, live, newValue) => {
        if (!pending) {
            return { field, oldValue: submittedOld, newValue, submittedOldValue: submittedOld, changedSinceSubmit: false, alreadyApplied: false };
        }
        return {
            field,
            oldValue: line ? live : null,
            newValue,
            submittedOldValue: submittedOld,
            changedSinceSubmit: line ? live !== submittedOld : true,
            alreadyApplied: line ? live === newValue : false,
        };
    };

    const changes = [];
    for (const field of Object.keys(DATE_FIELDS)) {
        if (!Object.prototype.hasOwnProperty.call(requested, field)) continue;
        const submittedOld = Object.prototype.hasOwnProperty.call(base, field) ? (base[field] ?? null) : null;
        changes.push(change(field, submittedOld, line ? line[field] : null, requested[field] ?? null));
    }
    if (requested.markReadyForQc === true) {
        changes.push(change('status', base.status ?? null, line ? line.status : null, READY_FOR_QC_STATUS));
    }
    // A sample's "mark as sent": the move, the date, and the courier where given.
    if (requested.markSent && typeof requested.markSent === 'object') {
        const ms = requested.markSent;
        changes.push(change('status', base.status ?? null, line ? line.status : null, SENT_STATUS));
        changes.push(change('shippedDate', null, line ? line.shippedDate : null, normalizeDate(ms.sentOn) ?? null));
        const courier = courierText(ms.courier);
        if (courier) changes.push(change('courierName', null, line ? line.courierName : null, courier));
        const tracking = courierText(ms.tracking);
        if (tracking) changes.push(change('courierTracking', null, line ? line.courierTracking : null, tracking));
    }

    const plan = pending
        ? planApply(requested, line, row.purchase_order_id, Number(row.po_owned) === 1, { base, today: opts.today || null })
        : null;

    return {
        id: Number(row.id),
        status: row.status,
        rowVersion: Number(row.row_version),
        supplierId: Number(row.supplier_id),
        supplierName: row.supplier_name,
        purchaseOrderId: Number(row.purchase_order_id),
        poNumber: row.po_number || null,
        orderId: Number(row.order_id),
        jfCode: row.jf_code || null,
        productName: row.product_name || null,
        lineStatus: line ? line.status : null,
        // 'sample' for a line of a sample order (its one request is "mark as sent").
        orderKind: line ? line.orderKind : 'stock',
        changes,
        canApply: pending && plan.blockers.length === 0,
        blockers: plan ? plan.blockers.map(code => ({ code, message: BLOCKERS[code] })) : [],
        submittedBy: row.submitted_by_email,
        submittedAt: row.submitted_at_iso || null,
        decidedBy: row.decided_by || null,
        decidedAt: row.decided_at_iso || null,
        decidedNote: row.decided_note || null,
        appliedAt: row.applied_at_iso || null,
    };
}

// The optional note on an apply. Returns { value } or { error }.
function parseNote(raw) {
    if (raw === undefined || raw === null || raw === '') return { value: null };
    if (typeof raw !== 'string') return { error: 'note must be a string.' };
    const note = raw.trim();
    if (note.length > 500) return { error: 'note must be at most 500 characters.' };
    return { value: note || null };
}

// A positive id from a query string or a JSON body, or null. Past
// MAX_SAFE_INTEGER the number is no longer the id that was sent.
function positiveInt(raw) {
    const n = typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : raw;
    return Number.isSafeInteger(n) && n > 0 ? n : null;
}

module.exports = {
    EDITABLE_STATUSES,
    READY_FOR_QC_STATUS,
    SENT_STATUS,
    courierText,
    DATE_FIELDS,
    REQUEST_STATUSES,
    BLOCKERS,
    jportalSchema,
    parseJsonObject,
    normalizeDate,
    lineFromRow,
    planApply,
    requestToJson,
    parseNote,
    positiveInt,
};
