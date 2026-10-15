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
// and only while the line is still at the factory (EDITABLE_STATUSES).
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
    line_locked: 'The line has moved past the factory stages; its ready dates are locked.',
    bad_date: 'The requested date is not a valid YYYY-MM-DD date.',
    qc_from: 'The line can only be marked ready for QC while it is under production.',
    qc_no_date: 'Marking ready for QC needs an actual ready date.',
    nothing_requested: 'The request holds no change this API knows how to apply.',
};

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// The schema jportal lives in, backtick-quoted for interpolation. No
// placeholder can carry an identifier, so the name is whitelisted.
function jportalSchema(env = process.env) {
    const name = env.JPORTAL_DB_SCHEMA || 'jportal';
    if (!/^[A-Za-z0-9_]+$/.test(name)) throw new Error('JPORTAL_DB_SCHEMA is not a plain identifier');
    return `\`${name}\``;
}

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
    };
}

/**
 * What applying `requested` to `line` would write.
 *   requested: the row's requested_json, parsed.
 *   line: lineFromRow(...) of the live line, or null when it is gone.
 *   purchaseOrderId: the row's purchase_order_id; a line now on another PO is gone.
 * Returns { blockers: string[], cols: { column: value }, before, after, moveToQc }.
 * `cols` holds only columns whose value actually changes; with blockers it is empty.
 */
function planApply(requested, line, purchaseOrderId) {
    const empty = { cols: {}, before: {}, after: {}, moveToQc: false };
    if (!line || (purchaseOrderId != null && line.purchaseOrderId !== Number(purchaseOrderId))) {
        return { blockers: ['line_missing'], ...empty };
    }
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
    return { blockers: [], cols, before, after, moveToQc };
}

/**
 * One change-request row for the API: the line's live (old) value and the
 * requested (new) value for every field the supplier asked to change, plus
 * whether it can be applied right now. `row` is a po_change_requests row
 * joined with the live line (live_* columns); see the routes' SELECT.
 */
function requestToJson(row) {
    const base = parseJsonObject(row.base_json);
    const requested = parseJsonObject(row.requested_json);
    const line = row.live_id == null ? null : lineFromRow({
        id: row.live_id,
        purchase_order_id: row.live_purchase_order_id,
        status: row.live_status,
        estimated_ready_date: row.live_estimated_ready_date,
        actual_ready_date: row.live_actual_ready_date,
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

    const plan = pending ? planApply(requested, line, row.purchase_order_id) : null;

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

function positiveInt(raw) {
    const n = typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : raw;
    return Number.isInteger(n) && n > 0 ? n : null;
}

module.exports = {
    EDITABLE_STATUSES,
    READY_FOR_QC_STATUS,
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
