'use strict';

// /api/v1/qc-visits, /api/v1/qc-factories, /api/v1/qc-inspectors — arranging
// a factory QC visit in China (plan: ShipLine docs/plans/qc-visits.md; build
// contract: docs/plans/qc-visits-contract.md §4.1).
//
// A visit books an inspector at a factory on a China day for a set of order
// lines (how many units of each), records goods moved in from other
// suppliers, and logs the factory's and the inspector's confirmations of the
// slot. DRAFT → PROPOSED → CONFIRMED → COMPLETED, or CANCELLED from any open
// state. The rules (validation, readiness warnings, the state machine, the
// clearing rule for edits after confirmation) live in src/lib/qc-visits.js;
// this module is the SQL and HTTP around them.
//
// Concurrency: qc_visits.updated_at has millisecond precision and is the
// optimistic token. Every write that changes a visit carries the client's
// updatedAt; a mismatch is a 409 STALE with the current visit so the modal
// can reload. The row is locked (SELECT … FOR UPDATE) for the check and the
// write, inside one transaction.
//
// Confirmation state: qc_visits.factory_confirmed_at / inspector_confirmed_at
// are authoritative. An edit that clears a party's confirmation (plan §6)
// nulls its column; the old 'confirmed' row stays in the log, so state is
// never re-derived from the whole log without honouring a cleared column
// (effectiveConfirmations below; the portal handler reads the columns too).
//
// Inspection kits are JFPRO documents (listed by qc-kit-routes.js; the
// kit_missing warning reads the same view), emails and ICS are in
// qc-visit-comms-routes.js. Registered from orders.js; authed through the
// greedy /api/v1/{proxy+} route (req.userEmail, req.userType).

const { PutObjectCommand, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const Q = require('../lib/qc-visits');
const KD = require('../lib/qc-kit-docs');
const { contentDisposition, viewResponseParams, isUploadType } = require('../lib/qc-file-types');
// Open availability links follow status / visit_date changes (updateVisit).
const { syncAvailabilityLinks } = require('./qc-availability-routes');

const {
    VISIT_STATUSES, OPEN_STATUSES, TERMINAL_STATUSES, CANDIDATE_ORDER_STATUSES,
    canEdit, isYmd, isHm, normSupplier, normJfCode, isFqcCode, formatRef, chinaToday,
    validateVisitFields, validateLines, validateConfirmation, validateFactory, validateInspector,
    confirmationTimestamps, clearingFor, computeWarnings, transitionBlockers, applyTransition,
    ymdOf, hmOf,
} = Q;

const MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024;
const UPLOAD_URL_SECONDS = 600;
const VIEW_URL_SECONDS = 3600;
const SAFE_FILENAME_RE = /[^A-Za-z0-9._-]+/g;
const ATTACHMENT_KEY_RE = /^qc-visits\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/[A-Za-z0-9._-]+$/;

// Statuses in which an edit applies the clearing rule's confirmation clears
// (plan §6). The plan lists CONFIRMED; PROPOSED is included because a party
// that confirmed the old date or factory has not confirmed the new one, and
// keeping its tick would let the other party's confirmation auto-confirm a
// slot the first never agreed (accept-counter clears in PROPOSED for the same
// reason). The status only moves (CONFIRMED → PROPOSED) when it was CONFIRMED.
const CLEAR_IN_STATUSES = ['PROPOSED', 'CONFIRMED'];

const FACTORY_SNAPSHOT_COLS = {
    label: 'factory_label', address: 'factory_address', city: 'factory_city', province: 'factory_province',
    contact_name: 'factory_contact_name', contact_title: 'factory_contact_title', contact_email: 'factory_contact_email',
    contact_wechat: 'factory_contact_wechat', contact_phone: 'factory_contact_phone',
};
const INSPECTOR_SNAPSHOT_COLS = {
    name: 'inspector_name', company: 'inspector_company', email: 'inspector_email',
    wechat: 'inspector_wechat', phone: 'inspector_phone',
};
const DATE_COLS = new Set(['visit_date', 'move_by', 'on_date', 'counter_date']);
const TIME_COLS = new Set(['window_start', 'window_end', 'counter_window_start', 'counter_window_end']);
const TS_COLS = new Set(['factory_confirmed_at', 'inspector_confirmed_at', 'proposed_at', 'confirmed_at', 'completed_at', 'cancelled_at']);
const COL_RE = /^[a-z_]+$/;

// ── small helpers ───────────────────────────────────────────────────────────

class HttpFail extends Error {
    constructor(status, code, error, payload) {
        super(error);
        this.status = status;
        this.code = code;
        this.error = error;
        this.payload = payload || null;
    }
}
const badField = (field, error) => new HttpFail(400, 'BAD_FIELD', error, { field });
const notFound = error => new HttpFail(404, 'NOT_FOUND', error);
const terminalFail = status => new HttpFail(409, 'TERMINAL', `This visit is ${String(status).toLowerCase()}: it can no longer change.`, { status });

const unique = list => [...new Set(list)];
const num = v => (v == null || v === '' ? null : Number(v));
const bool = v => v === true || v === 1 || v === '1' || (Buffer.isBuffer(v) && v[0] === 1);
const clip = (v, n) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, n) : null);

// A TIMESTAMP as ISO 8601 with milliseconds (Date from mysql2, or a
// 'YYYY-MM-DD HH:MM:SS(.mmm)' string, which is UTC in this app).
function isoTs(v) {
    if (v == null || v === '') return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
    const s = String(v);
    const t = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(s) ? `${s.replace(' ', 'T')}Z` : s);
    return Number.isNaN(t) ? null : new Date(t).toISOString();
}
const msOf = v => {
    const s = isoTs(v);
    return s ? Date.parse(s) : NaN;
};

// Column value normalised for change detection (dates and times as text).
function normCol(col, v) {
    if (v == null || v === '') return null;
    if (DATE_COLS.has(col)) return ymdOf(v);
    if (TIME_COLS.has(col)) return hmOf(v);
    if (TS_COLS.has(col) || v instanceof Date) return isoTs(v);
    if (typeof v === 'number') return v;
    if (col.endsWith('_id') || col === 'report_deadline_hours' || col === 'wasted_trip') return Number(v);
    return String(v);
}

// `col = ?` pairs for an object of columns (names come from this module and
// the lib's validators; checked anyway).
function setClause(obj) {
    const cols = Object.keys(obj);
    for (const c of cols) if (!COL_RE.test(c)) throw new Error(`bad column ${c}`);
    return { sql: cols.map(c => `${c} = ?`).join(', '), params: cols.map(c => obj[c]) };
}

function requireUpdatedAt(body) {
    const v = body && body.updatedAt;
    const t = typeof v === 'string' ? Date.parse(v) : NaN;
    if (Number.isNaN(t)) throw badField('updatedAt', 'updatedAt (the visit\'s updatedAt as you last saw it) is required.');
    return t;
}

const ackOf = v => (Array.isArray(v) ? v.filter(k => typeof k === 'string') : []);

function fromValidation(r) {
    if (r && r.error) throw badField(r.field, r.error);
    return r;
}

async function inTx(conn, fn) {
    await conn.beginTransaction();
    try {
        const out = await fn();
        await conn.commit();
        return out;
    } catch (e) {
        try { await conn.rollback(); } catch { /* the original error matters */ }
        throw e;
    }
}

function blockedFail(blockers) {
    const msg = blockers.length === 1 ? blockers[0].message : `${blockers.length} problems block this: ${blockers[0].message}`;
    return new HttpFail(409, 'BLOCKED', msg, { blockers });
}

// The 'confirmed' rows of a party whose column was cleared no longer count
// (see the header): the lib's latestByParty / confirmationTimestamps then see
// that party as unconfirmed until it confirms again. `keepIds`: rows written
// by the current request, which always count.
function effectiveConfirmations(visitRow, rows, keepIds = []) {
    const keep = new Set(keepIds.map(Number));
    return (rows || []).filter(r => keep.has(Number(r.id)) || !(r.kind === 'confirmed'
        && (r.party === 'factory' || r.party === 'inspector')
        && visitRow[`${r.party}_confirmed_at`] == null));
}

const factoryDeleted = r => r.factory_id != null && (r.f_live_id == null || r.f_deleted_at != null);
const inspectorDeleted = r => r.inspector_id != null && (r.i_live_id == null || !bool(r.i_active));

// ── JSON shapes (contract §6.1) ─────────────────────────────────────────────

function contactJson(r, prefix) {
    return {
        name: r[`${prefix}name`] || null,
        title: r[`${prefix}title`] || null,
        email: r[`${prefix}email`] || null,
        wechat: r[`${prefix}wechat`] || null,
        phone: r[`${prefix}phone`] || null,
    };
}

function visitToJson(r, { lines = [], warnings = [], includeLines = false } = {}) {
    const out = {
        id: Number(r.id),
        ref: r.ref || formatRef(r.id),
        status: r.status,
        supplierName: r.supplier_name,
        factory: {
            id: num(r.factory_id),
            label: r.factory_label || null,
            address: r.factory_address || null,
            city: r.factory_city || null,
            province: r.factory_province || null,
            contact: contactJson(r, 'factory_contact_'),
            deleted: factoryDeleted(r),
        },
        inspector: {
            id: num(r.inspector_id),
            name: r.inspector_name || null,
            company: r.inspector_company || null,
            email: r.inspector_email || null,
            wechat: r.inspector_wechat || null,
            phone: r.inspector_phone || null,
            deleted: inspectorDeleted(r),
        },
        visitDate: ymdOf(r.visit_date),
        windowStart: hmOf(r.window_start),
        windowEnd: hmOf(r.window_end),
        tz: r.tz || 'Asia/Shanghai',
        expectedProductionState: r.expected_production_state || null,
        reportDeadlineHours: num(r.report_deadline_hours),
        escalationNote: r.escalation_note || null,
        notes: r.notes || null,
        factoryConfirmedAt: isoTs(r.factory_confirmed_at),
        inspectorConfirmedAt: isoTs(r.inspector_confirmed_at),
        proposedAt: isoTs(r.proposed_at),
        confirmedAt: isoTs(r.confirmed_at),
        completedAt: isoTs(r.completed_at),
        cancelledAt: isoTs(r.cancelled_at),
        cancelCategory: r.cancel_category || null,
        wastedTrip: bool(r.wasted_trip),
        cancelReason: r.cancel_reason || null,
        clonedFromVisitId: num(r.cloned_from_visit_id),
        lineCount: lines.length,
        unitCount: lines.reduce((s, l) => s + (Number(l.quantity_to_inspect) || 0), 0),
        warnings,
        createdByEmail: r.created_by_email || null,
        createdAt: isoTs(r.created_at),
        updatedByEmail: r.updated_by_email || null,
        updatedAt: isoTs(r.updated_at),
    };
    if (includeLines) out.lines = lines.map(lineToJson);
    return out;
}

function lineToJson(l) {
    const aqlBlank = l.aql_level == null && l.aql_critical == null && l.aql_major == null && l.aql_minor == null;
    return {
        id: Number(l.id),
        orderId: Number(l.order_id),
        jfCode: l.o_jf_code || null,
        productName: l.o_product_name || null,
        poNumber: l.o_po_number || null,
        lotNumber: l.o_lot_number || null,
        supplier: l.o_supplier || null,
        orderStatus: l.o_status || null, // server dialect; the frontend maps it
        quantity: num(l.o_quantity),
        poQuantitySnapshot: Number(l.po_quantity_snapshot) || 0,
        quantityToInspect: Number(l.quantity_to_inspect) || 0,
        sampleMethod: l.sample_method || 'full',
        aql: aqlBlank ? null : {
            level: l.aql_level || null,
            critical: num(l.aql_critical),
            major: num(l.aql_major),
            minor: num(l.aql_minor),
        },
        note: l.note || null,
        orderDeleted: l.o_id == null || l.o_deleted_at != null,
    };
}

const moveToJson = m => ({
    id: Number(m.id),
    orderId: Number(m.order_id),
    fromSupplierName: m.from_supplier_name || '',
    toFactoryId: num(m.to_factory_id),
    moveBy: ymdOf(m.move_by),
    arrangedBy: m.arranged_by || null,
    note: m.note || null,
    confirmedAt: isoTs(m.confirmed_at),
});

const confirmationToJson = c => ({
    id: Number(c.id),
    party: c.party,
    kind: c.kind,
    channel: c.channel,
    onDate: ymdOf(c.on_date),
    counterDate: ymdOf(c.counter_date),
    counterWindowStart: hmOf(c.counter_window_start),
    counterWindowEnd: hmOf(c.counter_window_end),
    byName: c.by_name || null,
    note: c.note || null,
    recordedByEmail: c.recorded_by_email || null,
    createdAt: isoTs(c.created_at),
});

const factoryToJson = f => ({
    id: Number(f.id),
    supplierName: f.supplier_name,
    supplierCode: f.supplier_code || null,
    label: f.label,
    address: f.address || null,
    city: f.city || null,
    province: f.province || null,
    contact: contactJson(f, 'contact_'),
    isDefault: bool(f.is_default),
    notes: f.notes || null,
    deleted: f.deleted_at != null,
    createdAt: isoTs(f.created_at),
    updatedAt: isoTs(f.updated_at),
});

const inspectorToJson = i => ({
    id: Number(i.id),
    name: i.name,
    kind: i.kind || 'agency',
    company: i.company || null,
    email: i.email || null,
    wechat: i.wechat || null,
    phone: i.phone || null,
    baseCity: i.base_city || null,
    notes: i.notes || null,
    active: bool(i.active),
    createdAt: isoTs(i.created_at),
    updatedAt: isoTs(i.updated_at),
});

// Audit snapshot of a visit row: the columns people care about.
function visitAuditSnapshot(r) {
    return {
        status: r.status,
        supplierName: r.supplier_name,
        factoryId: num(r.factory_id),
        factoryLabel: r.factory_label || null,
        inspectorId: num(r.inspector_id),
        inspectorName: r.inspector_name || null,
        visitDate: ymdOf(r.visit_date),
        windowStart: hmOf(r.window_start),
        windowEnd: hmOf(r.window_end),
        factoryConfirmedAt: isoTs(r.factory_confirmed_at),
        inspectorConfirmedAt: isoTs(r.inspector_confirmed_at),
    };
}

// ── routes ──────────────────────────────────────────────────────────────────

function registerQcVisitRoutes(app, deps) {
    const { withConnection, s3, poBucket, recordAudit, auditLogSchemaReady, log } = deps;

    const sendFail = (res, f) => res.status(f.status).json({ error: f.error, code: f.code, ...(f.payload || {}) });

    // Wrap a handler: HttpFail → its JSON, anything else → 500.
    const route = (label, fn) => async (req, res) => {
        try {
            await fn(req, res);
        } catch (e) {
            if (e instanceof HttpFail) {
                if (!res.headersSent) sendFail(res, e);
                return;
            }
            log.error(`[${label}]`, e);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    };

    function refuseReadOnly(req) {
        if (!canEdit(req.userType)) throw new HttpFail(403, 'READ_ONLY', 'Read only.');
    }

    const audit = (conn, entityType, entityId, action, before, after, userEmail) =>
        recordAudit(conn, { entityType, entityId, action, before, after, userEmail });

    // ── loaders ─────────────────────────────────────────────────────────────

    // Visits with the live factory / inspector joined (LEFT: a deleted or
    // missing one yields deleted:true, never a crash).
    async function loadVisitRows(conn, where, params) {
        const [rows] = await conn.query(
            `SELECT v.*,
                    f.id AS f_live_id, f.deleted_at AS f_deleted_at,
                    i.id AS i_live_id, i.active AS i_active
               FROM qc_visits v
               LEFT JOIN qc_factories f ON f.id = v.factory_id
               LEFT JOIN qc_inspectors i ON i.id = v.inspector_id
              WHERE ${where}
              ORDER BY v.visit_date IS NULL, v.visit_date, v.id DESC`,
            params
        );
        return rows;
    }

    async function loadLineRows(conn, visitIds) {
        if (!visitIds.length) return [];
        const [rows] = await conn.query(
            `SELECT l.*,
                    o.id AS o_id, o.jf_code AS o_jf_code, o.product_name AS o_product_name,
                    o.po_number AS o_po_number, o.lot_number AS o_lot_number, o.supplier AS o_supplier,
                    o.status AS o_status, o.quantity AS o_quantity, o.deleted_at AS o_deleted_at
               FROM qc_visit_lines l
               LEFT JOIN orders o ON o.id = l.order_id
              WHERE l.visit_id IN (?)
              ORDER BY l.visit_id, l.id`,
            [visitIds]
        );
        return rows;
    }

    async function loadMoveRows(conn, visitIds) {
        if (!visitIds.length) return [];
        const [rows] = await conn.query(
            `SELECT * FROM qc_visit_moves WHERE visit_id IN (?) ORDER BY visit_id, id`,
            [visitIds]
        );
        return rows;
    }

    async function loadConfirmationRows(conn, visitId) {
        const [rows] = await conn.query(
            `SELECT * FROM qc_visit_confirmations WHERE visit_id = ? ORDER BY created_at, id`,
            [visitId]
        );
        return rows;
    }

    async function loadAttachmentRows(conn, visitId) {
        const [rows] = await conn.query(
            `SELECT * FROM qc_visit_attachments WHERE visit_id = ? AND deleted_at IS NULL ORDER BY created_at, id`,
            [visitId]
        );
        return rows;
    }

    // orders by id, as computeWarnings' ctx.orders (deleted rows included and
    // flagged; ids with no row are simply absent).
    async function loadOrderCtx(conn, orderIds) {
        const out = {};
        if (!orderIds.length) return out;
        const [rows] = await conn.query(
            `SELECT id, jf_code, product_name, po_number, lot_number, supplier, status, quantity,
                    estimated_ready_date, actual_ready_date, purchase_order_id, deleted_at
               FROM orders WHERE id IN (?)`,
            [orderIds]
        );
        for (const o of rows) {
            out[Number(o.id)] = {
                id: Number(o.id),
                quantity: o.quantity == null ? null : Number(o.quantity),
                status: o.status || null,
                supplier: o.supplier || null,
                estimatedReadyDate: ymdOf(o.estimated_ready_date),
                actualReadyDate: ymdOf(o.actual_ready_date),
                jfCode: o.jf_code || null,
                poNumber: o.po_number || null,
                purchaseOrderId: o.purchase_order_id == null ? null : Number(o.purchase_order_id),
                deleted: o.deleted_at != null,
            };
        }
        return out;
    }

    // Every line on any visit for these orders (computeWarnings drops the
    // visit's own lines by id).
    async function loadOtherLines(conn, orderIds) {
        if (!orderIds.length) return [];
        const [rows] = await conn.query(
            `SELECT l.visit_id, l.order_id, l.quantity_to_inspect, v.ref, v.status, v.visit_date
               FROM qc_visit_lines l
               JOIN qc_visits v ON v.id = l.visit_id
              WHERE l.order_id IN (?)`,
            [orderIds]
        );
        return rows.map(r => ({
            visitId: Number(r.visit_id),
            ref: r.ref || formatRef(r.visit_id),
            status: r.status,
            visitDate: ymdOf(r.visit_date),
            orderId: Number(r.order_id),
            quantityToInspect: Number(r.quantity_to_inspect) || 0,
        }));
    }

    // { [normJfCode]: 'docs' | 'none' }: whether the JF code has a China
    // factory QC document in JFPRO (view qc_kit_documents). Best effort:
    // if the view cannot be read (not created yet, privileges), every code is
    // unknown and kit_missing is not raised.
    async function loadKitStatus(conn, jfCodes) {
        const codes = unique(jfCodes.map(normJfCode).filter(Boolean));
        if (!codes.length) return {};
        try {
            const [rows] = await conn.query(KD.KIT_CODES_WITH_DOCUMENTS_SQL, [codes]);
            return KD.kitStatusFromRows(codes, rows);
        } catch (e) {
            log.warn('[qc-visits] qc_kit_documents lookup failed; kit_missing skipped', { error: e.message, code: e.code });
            return {};
        }
    }

    // { [normJfCode]: true | false | null } from jfpro.products, best effort:
    // any error means "unknown" (fqc_missing is then skipped).
    async function loadRepackable(conn, jfCodes) {
        const codes = unique(jfCodes.map(normJfCode).filter(Boolean));
        if (!codes.length) return {};
        try {
            const [rows] = await conn.query(`SELECT jfcode, repackable FROM jfpro.products WHERE jfcode IN (?)`, [codes]);
            const out = {};
            for (const r of rows) {
                const k = normJfCode(r.jfcode);
                const raw = Buffer.isBuffer(r.repackable) ? r.repackable[0] : r.repackable;
                const s = raw == null ? '' : String(raw).trim().toLowerCase();
                const v = ['1', 'true', 'yes', 'y'].includes(s) ? true : (['0', 'false', 'no', 'n'].includes(s) ? false : null);
                if (v === true) out[k] = true;
                else if (v === false && out[k] !== true) out[k] = false;
                else if (out[k] === undefined) out[k] = null;
            }
            return out;
        } catch (e) {
            log.warn('[qc-visits] repackable lookup failed; fqc_missing skipped', { error: e.message });
            return {};
        }
    }

    // Order ids (non-_FQC) whose PO carries a live _FQC line of the same product.
    async function loadFqcSiblingParents(conn, orders) {
        const parents = Object.values(orders).filter(o => !o.deleted && o.jfCode && !isFqcCode(o.jfCode));
        if (!parents.length) return [];
        const poIds = unique(parents.map(o => o.purchaseOrderId).filter(x => x != null));
        const poNums = unique(parents.map(o => o.poNumber).filter(Boolean));
        const conds = [];
        const params = [];
        if (poIds.length) { conds.push('purchase_order_id IN (?)'); params.push(poIds); }
        if (poNums.length) { conds.push('po_number IN (?)'); params.push(poNums); }
        if (!conds.length) return [];
        const [rows] = await conn.query(
            `SELECT id, jf_code, po_number, purchase_order_id FROM orders
              WHERE deleted_at IS NULL AND jf_code LIKE '%\\_FQC' AND (${conds.join(' OR ')})`,
            params
        );
        const fqcs = rows.filter(r => isFqcCode(r.jf_code));
        const samePo = (o, r) => (o.purchaseOrderId != null && r.purchase_order_id != null && Number(r.purchase_order_id) === o.purchaseOrderId)
            || (o.poNumber && r.po_number && String(r.po_number).trim().toLowerCase() === String(o.poNumber).trim().toLowerCase());
        return parents
            .filter(o => fqcs.some(r => samePo(o, r) && normJfCode(r.jf_code) === normJfCode(o.jfCode)))
            .map(o => o.id);
    }

    // Other open visits of the same inspectors on the same days.
    async function loadInspectorDays(conn, items) {
        const keyed = items.filter(it => it.visit.inspector && it.visit.inspector.id != null && it.visit.visitDate);
        if (!keyed.length) return [];
        const [rows] = await conn.query(
            `SELECT id, ref, inspector_id, visit_date, factory_city FROM qc_visits
              WHERE status IN (?) AND inspector_id IN (?) AND visit_date IN (?)`,
            [OPEN_STATUSES, unique(keyed.map(it => Number(it.visit.inspector.id))), unique(keyed.map(it => it.visit.visitDate))]
        );
        return rows;
    }

    // Readiness warnings for several visits (or would-be visits) at once.
    // items: [{ visit: ctx.visit, lines: [{orderId, quantityToInspect, poQuantitySnapshot, sampleMethod}],
    //           moves: [{orderId}], acknowledge? }]  →  Warning[][] in the same order.
    async function warningsFor(conn, items, stage) {
        if (!items.length) return [];
        const orderIds = unique(items.flatMap(it => it.lines.map(l => Number(l.orderId))));
        const orders = await loadOrderCtx(conn, orderIds);
        const others = await loadOtherLines(conn, orderIds);
        const othersByOrder = new Map();
        for (const r of others) {
            if (!othersByOrder.has(r.orderId)) othersByOrder.set(r.orderId, []);
            othersByOrder.get(r.orderId).push(r);
        }
        const jfCodes = Object.values(orders).map(o => o.jfCode).filter(Boolean);
        const kitStatus = await loadKitStatus(conn, jfCodes);
        let repackable = {};
        let fqcSiblingParents = [];
        if (stage !== 'view') { // fqc_missing is not raised at 'view'
            repackable = await loadRepackable(conn, jfCodes);
            fqcSiblingParents = await loadFqcSiblingParents(conn, orders);
        }
        const days = await loadInspectorDays(conn, items);

        return items.map(it => {
            const v = it.visit;
            const sameDay = (v.inspector && v.inspector.id != null && v.visitDate)
                ? days.filter(d => Number(d.inspector_id) === Number(v.inspector.id)
                    && ymdOf(d.visit_date) === v.visitDate
                    && (v.id == null || Number(d.id) !== Number(v.id)))
                    .map(d => ({ visitId: Number(d.id), ref: d.ref || formatRef(d.id), city: d.factory_city || null }))
                : [];
            const ctx = {
                visit: v,
                lines: it.lines,
                moves: it.moves || [],
                orders,
                otherLines: unique(it.lines.map(l => Number(l.orderId))).flatMap(id => othersByOrder.get(id) || []),
                fqcSiblingParents,
                repackable,
                kitStatus,
                inspectorSameDay: sameDay,
                acknowledge: it.acknowledge || [],
            };
            return computeWarnings(ctx, stage);
        });
    }

    function visitCtx(r) {
        return {
            id: r.id == null ? null : Number(r.id),
            status: r.status,
            visitDate: ymdOf(r.visit_date),
            windowStart: hmOf(r.window_start),
            windowEnd: hmOf(r.window_end),
            supplierName: r.supplier_name,
            factory: (r.factory_id != null || r.factory_label)
                ? { id: num(r.factory_id), deleted: factoryDeleted(r), city: r.factory_city || null }
                : null,
            inspector: (r.inspector_id != null || r.inspector_name)
                ? { id: num(r.inspector_id), deleted: inspectorDeleted(r) }
                : null,
        };
    }

    const lineCtx = rows => rows.map(l => ({
        orderId: Number(l.order_id),
        quantityToInspect: Number(l.quantity_to_inspect) || 0,
        poQuantitySnapshot: l.po_quantity_snapshot == null ? null : Number(l.po_quantity_snapshot),
        sampleMethod: l.sample_method || 'full',
    }));

    const groupBy = (rows, key) => {
        const m = new Map();
        for (const r of rows) {
            const k = Number(r[key]);
            if (!m.has(k)) m.set(k, []);
            m.get(k).push(r);
        }
        return m;
    };

    // Warnings at 'view' for visit rows; terminal visits carry none (their
    // lines have moved on by design, and nothing can be done about it).
    async function viewWarnings(conn, rows, linesByVisit, movesByVisit) {
        const open = rows.filter(r => OPEN_STATUSES.includes(r.status));
        const lists = await warningsFor(conn, open.map(r => ({
            visit: visitCtx(r),
            lines: lineCtx(linesByVisit.get(Number(r.id)) || []),
            moves: (movesByVisit.get(Number(r.id)) || []).map(m => ({ orderId: Number(m.order_id) })),
        })), 'view');
        const out = new Map();
        open.forEach((r, i) => out.set(Number(r.id), lists[i]));
        return out;
    }

    async function attachmentToJson(a) {
        const name = a.filename || `attachment-${a.id}`;
        const [url, downloadUrl] = await Promise.all([
            getSignedUrl(s3, new GetObjectCommand({
                Bucket: poBucket, Key: a.s3_key,
                ...viewResponseParams(name, a.content_type),
            }), { expiresIn: VIEW_URL_SECONDS }),
            getSignedUrl(s3, new GetObjectCommand({
                Bucket: poBucket, Key: a.s3_key,
                ResponseContentDisposition: contentDisposition('attachment', name),
            }), { expiresIn: VIEW_URL_SECONDS }),
        ]);
        return {
            id: Number(a.id),
            filename: a.filename,
            contentType: a.content_type || null,
            fileSize: num(a.file_size),
            note: a.note || null,
            uploadedByEmail: a.uploaded_by_email || null,
            createdAt: isoTs(a.created_at),
            url,
            downloadUrl,
        };
    }

    // QcVisitDetail, or null.
    async function loadDetail(conn, id, extra) {
        const rows = await loadVisitRows(conn, 'v.id = ?', [id]);
        if (!rows.length) return null;
        const r = rows[0];
        const lines = await loadLineRows(conn, [id]);
        const moves = await loadMoveRows(conn, [id]);
        const confirmations = await loadConfirmationRows(conn, id);
        const attachments = await loadAttachmentRows(conn, id);
        const warnings = (await viewWarnings(conn, rows, new Map([[Number(id), lines]]), new Map([[Number(id), moves]]))).get(Number(id)) || [];
        const out = visitToJson(r, { lines, warnings, includeLines: true });
        out.moves = moves.map(moveToJson);
        out.confirmations = confirmations.map(confirmationToJson);
        out.attachments = await Promise.all(attachments.map(attachmentToJson));
        if (extra && typeof extra === 'object') Object.assign(out, extra);
        return out;
    }

    async function loadDetailOr404(conn, id) {
        const d = await loadDetail(conn, id);
        if (!d) throw notFound(`Visit ${id} not found.`);
        return d;
    }

    // One visit write: lock, STALE check (when updatedAtMs is given), run fn
    // in the transaction, then answer with the fresh QcVisitDetail. fn returns
    // extra fields for the response (or nothing).
    async function writeVisit(res, id, updatedAtMs, fn, { status = 200 } = {}) {
        const detail = await withConnection(async (conn) => {
            let extra;
            try {
                extra = await inTx(conn, async () => {
                    const [rows] = await conn.query(`SELECT * FROM qc_visits WHERE id = ? FOR UPDATE`, [id]);
                    if (!rows.length) throw notFound(`Visit ${id} not found.`);
                    const row = rows[0];
                    if (updatedAtMs !== undefined && msOf(row.updated_at) !== updatedAtMs) {
                        throw new HttpFail(409, 'STALE', 'Someone else changed this visit.');
                    }
                    return fn(conn, row);
                });
            } catch (e) {
                if (e instanceof HttpFail && e.code === 'STALE') e.payload = { visit: await loadDetail(conn, id) };
                throw e;
            }
            return loadDetail(conn, id, extra);
        });
        res.status(status).json(detail);
    }

    // UPDATE qc_visits with a column patch, bumping the STALE token. A patch
    // touching status or visit_date also brings the visit's open availability
    // links along (docs/qc-availability.md), on this connection and inside
    // this transaction; that hook never fails the write. `before`: the row as
    // locked by writeVisit (read here when not given).
    async function updateVisit(conn, id, patch, userEmail, before) {
        const follows = 'status' in patch || 'visit_date' in patch;
        let prev = before;
        if (follows && !prev) {
            const [rows] = await conn.query(`SELECT status, visit_date FROM qc_visits WHERE id = ?`, [id]);
            prev = rows[0] || null;
        }
        const { sql, params } = setClause({ ...patch, updated_by_email: userEmail || null });
        await conn.query(`UPDATE qc_visits SET ${sql}, updated_at = CURRENT_TIMESTAMP(3) WHERE id = ?`, [...params, id]);
        if (follows && prev) {
            await syncAvailabilityLinks(conn, { visitId: id, before: prev, after: patch, userEmail, recordAudit, log });
        }
    }

    async function loadLiveFactory(conn, id, field) {
        const [rows] = await conn.query(`SELECT * FROM qc_factories WHERE id = ? AND deleted_at IS NULL`, [id]);
        if (!rows.length) throw badField(field, `Factory ${id} not found (or deleted).`);
        return rows[0];
    }

    async function loadActiveInspector(conn, id, field) {
        const [rows] = await conn.query(`SELECT * FROM qc_inspectors WHERE id = ?`, [id]);
        if (!rows.length || !bool(rows[0].active)) throw badField(field, `Inspector ${id} not found (or inactive).`);
        return rows[0];
    }

    const factorySnapshot = f => Object.fromEntries(Object.entries(FACTORY_SNAPSHOT_COLS).map(([src, dst]) => [dst, f[src] == null ? null : f[src]]));
    const inspectorSnapshot = i => Object.fromEntries(Object.entries(INSPECTOR_SNAPSHOT_COLS).map(([src, dst]) => [dst, i[src] == null ? null : i[src]]));

    async function checkMoveFactories(conn, moves) {
        const ids = unique(moves.map(m => m.toFactoryId).filter(x => x != null));
        if (!ids.length) return;
        const [rows] = await conn.query(`SELECT id FROM qc_factories WHERE id IN (?) AND deleted_at IS NULL`, [ids]);
        const live = new Set(rows.map(r => Number(r.id)));
        moves.forEach((m, i) => {
            if (m.toFactoryId != null && !live.has(Number(m.toFactoryId))) {
                throw badField(`moves[${i}].toFactoryId`, `Factory ${m.toFactoryId} not found (or deleted).`);
            }
        });
    }

    const lineCols = (l, orders) => ({
        quantity_to_inspect: l.quantityToInspect,
        po_quantity_snapshot: orders[l.orderId] && orders[l.orderId].quantity != null ? orders[l.orderId].quantity : 0,
        sample_method: l.sampleMethod || 'full',
        aql_level: l.aql ? l.aql.level : null,
        aql_critical: l.aql ? l.aql.critical : null,
        aql_major: l.aql ? l.aql.major : null,
        aql_minor: l.aql ? l.aql.minor : null,
        note: l.note,
    });
    const moveCols = (m, orders) => ({
        from_supplier_name: (orders[m.orderId] && orders[m.orderId].supplier) || '',
        to_factory_id: m.toFactoryId,
        move_by: m.moveBy,
        arranged_by: m.arrangedBy,
        note: m.note,
    });

    // Bring a visit's lines and moves to exactly `lines` / `moves` (validated
    // camelCase from validateLines). Kept lines keep their id and created_at;
    // every line re-snapshots po_quantity_snapshot.
    async function syncLines(conn, visitId, lines, moves, orders, existingLines, existingMoves) {
        const byOrder = new Map(existingLines.map(l => [Number(l.order_id), l]));
        const keep = new Set(lines.map(l => l.orderId));
        const drop = existingLines.filter(l => !keep.has(Number(l.order_id))).map(l => l.id);
        if (drop.length) await conn.query(`DELETE FROM qc_visit_lines WHERE id IN (?)`, [drop]);
        for (const l of lines) {
            const cols = lineCols(l, orders);
            const prev = byOrder.get(l.orderId);
            if (prev) {
                const { sql, params } = setClause(cols);
                await conn.query(`UPDATE qc_visit_lines SET ${sql} WHERE id = ?`, [...params, prev.id]);
            } else {
                const { sql, params } = setClause({ visit_id: visitId, order_id: l.orderId, ...cols });
                await conn.query(`INSERT INTO qc_visit_lines SET ${sql}`, params);
            }
        }
        const movesByOrder = new Map(existingMoves.map(m => [Number(m.order_id), m]));
        const keepMoves = new Set(moves.map(m => m.orderId));
        const dropMoves = existingMoves.filter(m => !keepMoves.has(Number(m.order_id))).map(m => m.id);
        if (dropMoves.length) await conn.query(`DELETE FROM qc_visit_moves WHERE id IN (?)`, [dropMoves]);
        for (const m of moves) {
            const cols = moveCols(m, orders);
            const prev = movesByOrder.get(m.orderId);
            if (prev) {
                const { sql, params } = setClause(cols);
                await conn.query(`UPDATE qc_visit_moves SET ${sql} WHERE id = ?`, [...params, prev.id]);
            } else {
                const { sql, params } = setClause({ visit_id: visitId, order_id: m.orderId, ...cols });
                await conn.query(`INSERT INTO qc_visit_moves SET ${sql}`, params);
            }
        }
    }

    // Did the inspection scope change (orders, quantities, method, AQL,
    // moves)? Line notes do not reopen a confirmed slot.
    function scopeChanged(existingLines, existingMoves, lines, moves) {
        const n = v => (v == null || v === '' ? null : Number(v));
        const lineKey = (orderId, qty, method, level, c, ma, mi) => JSON.stringify([Number(orderId), Number(qty), method || 'full', level || null, n(c), n(ma), n(mi)]);
        const a = existingLines.map(l => lineKey(l.order_id, l.quantity_to_inspect, l.sample_method, l.aql_level, l.aql_critical, l.aql_major, l.aql_minor)).sort();
        const b = lines.map(l => lineKey(l.orderId, l.quantityToInspect, l.sampleMethod, l.aql && l.aql.level, l.aql && l.aql.critical, l.aql && l.aql.major, l.aql && l.aql.minor)).sort();
        const moveKey = (orderId, to, by, arranged, note) => JSON.stringify([Number(orderId), n(to), ymdOf(by), arranged || null, note || null]);
        const c = existingMoves.map(m => moveKey(m.order_id, m.to_factory_id, m.move_by, m.arranged_by, m.note)).sort();
        const d = moves.map(m => moveKey(m.orderId, m.toFactoryId, m.moveBy, m.arrangedBy, m.note)).sort();
        return JSON.stringify(a) !== JSON.stringify(b) || JSON.stringify(c) !== JSON.stringify(d);
    }

    // The column patch the clearing rule asks for, given the changed fields
    // (camelCase or column names) and the visit as stored.
    function clearingPatch(row, changedFields) {
        if (!CLEAR_IN_STATUSES.includes(row.status)) return { patch: {}, reopened: false, cleared: [] };
        const c = clearingFor(changedFields);
        const patch = {};
        const cleared = [];
        if (c.clearFactory && row.factory_confirmed_at != null) { patch.factory_confirmed_at = null; cleared.push('factory'); }
        if (c.clearInspector && row.inspector_confirmed_at != null) { patch.inspector_confirmed_at = null; cleared.push('inspector'); }
        let reopened = false;
        if (c.reopen && row.status === 'CONFIRMED') {
            // The "reopen": applyTransition has no CONFIRMED → PROPOSED; the
            // slot was proposed already, so proposed_at stays.
            patch.status = 'PROPOSED';
            patch.confirmed_at = null;
            reopened = true;
        }
        return { patch, reopened, cleared };
    }

    // After a confirmation row is written: the stored timestamps and the
    // PROPOSED ↔ CONFIRMED move they imply.
    function confirmationPatch(row, rowsAfter, newId, now) {
        const ts = confirmationTimestamps(effectiveConfirmations(row, rowsAfter, [newId]));
        const patch = {
            factory_confirmed_at: ts.factory_confirmed_at || null,
            inspector_confirmed_at: ts.inspector_confirmed_at || null,
        };
        const both = patch.factory_confirmed_at != null && patch.inspector_confirmed_at != null;
        let moved = null;
        if (row.status === 'PROPOSED' && both) {
            Object.assign(patch, applyTransition(row, 'CONFIRMED', { now }));
            moved = 'confirmed';
        } else if (row.status === 'CONFIRMED' && !both) {
            patch.status = 'PROPOSED';
            patch.confirmed_at = null;
            moved = 'reproposed';
        }
        return { patch, moved };
    }

    // ── GET /api/v1/qc-visits ───────────────────────────────────────────────
    // ?status=CSV&supplier=&inspectorId=&from=&to=&includeLines=1&wasted=1
    app.get('/api/v1/qc-visits', route('GET /qc-visits', async (req, res) => {
        const q = req.query || {};
        const where = ['1 = 1'];
        const params = [];
        if (q.status != null && String(q.status).trim()) {
            const list = unique(String(q.status).split(',').map(s => s.trim().toUpperCase()).filter(Boolean));
            const badOne = list.find(s => !VISIT_STATUSES.includes(s));
            if (badOne || !list.length) throw badField('status', `status must be a comma-separated list of ${VISIT_STATUSES.join(', ')}.`);
            where.push('v.status IN (?)');
            params.push(list);
        }
        const supplier = clip(q.supplier, 255);
        if (supplier) {
            // Folded like normSupplier (case, runs of whitespace) so a name
            // stored with a double space still matches; JS re-checks below.
            where.push("TRIM(REGEXP_REPLACE(LOWER(v.supplier_name), '[[:space:]]+', ' ')) = ?");
            params.push(normSupplier(supplier));
        }
        if (q.inspectorId != null && q.inspectorId !== '') {
            if (!/^\d+$/.test(String(q.inspectorId))) throw badField('inspectorId', 'inspectorId must be a whole number.');
            where.push('v.inspector_id = ?');
            params.push(Number(q.inspectorId));
        }
        for (const [k, op] of [['from', '>='], ['to', '<=']]) {
            if (q[k] == null || q[k] === '') continue;
            if (!isYmd(String(q[k]))) throw badField(k, `${k} must be a date, YYYY-MM-DD.`);
            where.push(`v.visit_date ${op} ?`);
            params.push(String(q[k]));
        }
        if (String(q.wasted || '') === '1' || String(q.wasted || '') === 'true') where.push('v.wasted_trip = 1');
        const includeLines = String(q.includeLines || '') === '1' || String(q.includeLines || '') === 'true';

        const data = await withConnection(async (conn) => {
            let rows = await loadVisitRows(conn, where.join(' AND '), params);
            if (supplier) rows = rows.filter(r => normSupplier(r.supplier_name) === normSupplier(supplier));
            const ids = rows.map(r => Number(r.id));
            const lines = await loadLineRows(conn, ids);
            const moves = await loadMoveRows(conn, ids);
            const linesBy = groupBy(lines, 'visit_id');
            const movesBy = groupBy(moves, 'visit_id');
            const warnings = await viewWarnings(conn, rows, linesBy, movesBy);
            return rows.map(r => visitToJson(r, {
                lines: linesBy.get(Number(r.id)) || [],
                warnings: warnings.get(Number(r.id)) || [],
                includeLines,
            }));
        });
        res.json({ data });
    }));

    // ── POST /api/v1/qc-visits/warnings ─────────────────────────────────────
    // Readiness checks for a would-be visit (the arrange dialog). Read-only
    // users may ask: it changes nothing.
    app.post('/api/v1/qc-visits/warnings', route('POST /qc-visits/warnings', async (req, res) => {
        const b = req.body || {};
        const stage = b.stage == null ? 'create' : b.stage;
        if (!['create', 'propose', 'view'].includes(stage)) throw badField('stage', 'stage must be create, propose or view.');
        let lines = [];
        let moves = [];
        if (!(Array.isArray(b.lines) && b.lines.length === 0)) {
            const v = fromValidation(validateLines(b.lines, b.moves));
            lines = v.lines;
            moves = v.moves;
        }
        // The window is checked for format only: a reversed window is a
        // warning here (window_invalid), not a 400.
        const f = fromValidation(validateVisitFields({
            visitDate: b.visitDate, factoryId: b.factoryId, inspectorId: b.inspectorId,
        }, { partial: true })).value;
        for (const k of ['windowStart', 'windowEnd']) {
            if (b[k] != null && b[k] !== '' && !isHm(b[k])) throw badField(k, `${k} must be a time, HH:MM (00:00 to 23:59).`);
        }
        let excludeId = null;
        if (b.excludeVisitId != null && b.excludeVisitId !== '') {
            excludeId = Number(b.excludeVisitId);
            if (!Number.isInteger(excludeId) || excludeId <= 0) throw badField('excludeVisitId', 'excludeVisitId must be a visit id.');
        }
        const data = await withConnection(async (conn) => {
            let status = 'DRAFT';
            if (excludeId != null) {
                const [rows] = await conn.query(`SELECT status FROM qc_visits WHERE id = ?`, [excludeId]);
                if (rows.length) status = rows[0].status;
            }
            let factory = null;
            if (f.factory_id != null) {
                const [rows] = await conn.query(`SELECT id, city, deleted_at FROM qc_factories WHERE id = ?`, [f.factory_id]);
                factory = { id: f.factory_id, deleted: !rows.length || rows[0].deleted_at != null, city: rows.length ? rows[0].city || null : null };
            }
            let inspector = null;
            if (f.inspector_id != null) {
                const [rows] = await conn.query(`SELECT id, active FROM qc_inspectors WHERE id = ?`, [f.inspector_id]);
                inspector = { id: f.inspector_id, deleted: !rows.length || !bool(rows[0].active) };
            }
            const [w] = await warningsFor(conn, [{
                visit: {
                    id: excludeId,
                    status,
                    visitDate: f.visit_date || null,
                    windowStart: isHm(b.windowStart) ? b.windowStart : null,
                    windowEnd: isHm(b.windowEnd) ? b.windowEnd : null,
                    supplierName: typeof b.supplierName === 'string' && b.supplierName.trim() ? b.supplierName.trim() : null,
                    factory,
                    inspector,
                },
                lines: lines.map(l => ({ orderId: l.orderId, quantityToInspect: l.quantityToInspect, poQuantitySnapshot: null, sampleMethod: l.sampleMethod })),
                moves: moves.map(m => ({ orderId: m.orderId })),
                acknowledge: ackOf(b.acknowledge),
            }], stage);
            return w;
        });
        res.json({ data });
    }));

    // ── POST /api/v1/qc-visits ──────────────────────────────────────────────
    app.post('/api/v1/qc-visits', route('POST /qc-visits', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const b = req.body || {};
        const fields = fromValidation(validateVisitFields(b, { partial: false })).value;
        const { lines, moves } = fromValidation(validateLines(b.lines, b.moves));
        const acknowledge = ackOf(b.acknowledge);

        const detail = await withConnection(async (conn) => {
            const id = await inTx(conn, async () => {
                const cols = { ...fields };
                let factory = null;
                let inspector = null;
                if (cols.factory_id != null) {
                    factory = await loadLiveFactory(conn, cols.factory_id, 'factoryId');
                    // Snapshot first; snapshot fields sent explicitly win.
                    for (const [k, v] of Object.entries(factorySnapshot(factory))) if (!(k in fields)) cols[k] = v;
                }
                if (cols.inspector_id != null) {
                    inspector = await loadActiveInspector(conn, cols.inspector_id, 'inspectorId');
                    for (const [k, v] of Object.entries(inspectorSnapshot(inspector))) if (!(k in fields)) cols[k] = v;
                }
                await checkMoveFactories(conn, moves);

                const [warnings] = await warningsFor(conn, [{
                    visit: {
                        id: null,
                        status: 'DRAFT',
                        visitDate: cols.visit_date || null,
                        windowStart: cols.window_start || null,
                        windowEnd: cols.window_end || null,
                        supplierName: cols.supplier_name,
                        factory: factory ? { id: Number(factory.id), deleted: false, city: cols.factory_city || null } : null,
                        inspector: inspector ? { id: Number(inspector.id), deleted: false } : null,
                    },
                    lines: lines.map(l => ({ orderId: l.orderId, quantityToInspect: l.quantityToInspect, poQuantitySnapshot: null, sampleMethod: l.sampleMethod })),
                    moves: moves.map(m => ({ orderId: m.orderId })),
                    acknowledge,
                }], 'create');
                const blockers = warnings.filter(w => w.level === 'block');
                if (blockers.length) throw blockedFail(blockers);

                const orders = await loadOrderCtx(conn, lines.map(l => l.orderId));
                cols.status = 'DRAFT';
                cols.created_by_email = req.userEmail || null;
                cols.updated_by_email = req.userEmail || null;
                const { sql, params } = setClause(cols);
                const [ins] = await conn.query(`INSERT INTO qc_visits SET ${sql}`, params);
                const visitId = ins.insertId;
                await conn.query(`UPDATE qc_visits SET ref = ? WHERE id = ?`, [formatRef(visitId), visitId]);
                await syncLines(conn, visitId, lines, moves, orders, [], []);
                await audit(conn, 'qc_visit', visitId, 'created', null, {
                    ref: formatRef(visitId),
                    supplierName: cols.supplier_name,
                    factoryId: cols.factory_id == null ? null : cols.factory_id,
                    inspectorId: cols.inspector_id == null ? null : cols.inspector_id,
                    visitDate: cols.visit_date || null,
                    lines: lines.map(l => ({ orderId: l.orderId, quantityToInspect: l.quantityToInspect, sampleMethod: l.sampleMethod })),
                    moves: moves.map(m => ({ orderId: m.orderId, toFactoryId: m.toFactoryId })),
                    warnings: warnings.map(w => w.key),
                }, req.userEmail);
                return visitId;
            });
            return loadDetail(conn, id);
        });
        res.status(201).json(detail);
    }));

    // ── GET /api/v1/qc-visits/:id ───────────────────────────────────────────
    app.get('/api/v1/qc-visits/:id(\\d+)', route('GET /qc-visits/:id', async (req, res) => {
        const id = Number(req.params.id);
        const detail = await withConnection(conn => loadDetailOr404(conn, id));
        res.json(detail);
    }));

    // ── PATCH /api/v1/qc-visits/:id ─────────────────────────────────────────
    // Header and snapshot fields. supplierName only in DRAFT. Setting a new
    // factoryId / inspectorId copies that row into the snapshot columns
    // (fields sent alongside win). Applies the clearing rule (plan §6).
    app.patch('/api/v1/qc-visits/:id(\\d+)', route('PATCH /qc-visits/:id', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const updatedAtMs = requireUpdatedAt(b);
        await writeVisit(res, id, updatedAtMs, async (conn, row) => {
            if (TERMINAL_STATUSES.includes(row.status)) throw terminalFail(row.status);
            const value = fromValidation(validateVisitFields(b, { partial: true, current: row })).value;
            if ('supplier_name' in value && row.status !== 'DRAFT'
                && normSupplier(value.supplier_name) !== normSupplier(row.supplier_name)) {
                throw badField('supplierName', 'The supplier can only be changed while the visit is a draft.');
            }
            const cols = { ...value };
            if ('factory_id' in value && value.factory_id != null && Number(value.factory_id) !== num(row.factory_id)) {
                const f = await loadLiveFactory(conn, value.factory_id, 'factoryId');
                for (const [k, v] of Object.entries(factorySnapshot(f))) if (!(k in value)) cols[k] = v;
            }
            if ('inspector_id' in value && value.inspector_id != null && Number(value.inspector_id) !== num(row.inspector_id)) {
                const i = await loadActiveInspector(conn, value.inspector_id, 'inspectorId');
                for (const [k, v] of Object.entries(inspectorSnapshot(i))) if (!(k in value)) cols[k] = v;
            }
            const changed = Object.keys(cols).filter(c => normCol(c, cols[c]) !== normCol(c, row[c]));
            if (!changed.length) return undefined;
            const patch = Object.fromEntries(changed.map(c => [c, cols[c]]));
            const clear = clearingPatch(row, changed);
            await updateVisit(conn, id, { ...patch, ...clear.patch }, req.userEmail, row);
            const before = Object.fromEntries(changed.map(c => [c, normCol(c, row[c])]));
            const after = Object.fromEntries(changed.map(c => [c, normCol(c, patch[c])]));
            if (clear.cleared.length) after.confirmationsCleared = clear.cleared;
            await audit(conn, 'qc_visit', id, 'updated', before, after, req.userEmail);
            if (clear.reopened) {
                await audit(conn, 'qc_visit', id, 'reproposed', { status: row.status }, { status: 'PROPOSED', changedFields: changed, confirmationsCleared: clear.cleared }, req.userEmail);
            }
            return undefined;
        });
    }));

    // ── PUT /api/v1/qc-visits/:id/lines ─────────────────────────────────────
    // Replaces the line and move set. Checked at stage 'create'. A scope
    // change in PROPOSED/CONFIRMED clears both confirmations; CONFIRMED drops
    // to PROPOSED.
    app.put('/api/v1/qc-visits/:id(\\d+)/lines', route('PUT /qc-visits/:id/lines', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const updatedAtMs = requireUpdatedAt(b);
        const { lines, moves } = fromValidation(validateLines(b.lines, b.moves));
        const acknowledge = ackOf(b.acknowledge);
        await writeVisit(res, id, updatedAtMs, async (conn, row) => {
            if (TERMINAL_STATUSES.includes(row.status)) throw terminalFail(row.status);
            await checkMoveFactories(conn, moves);
            const joined = (await loadVisitRows(conn, 'v.id = ?', [id]))[0];
            const [warnings] = await warningsFor(conn, [{
                visit: visitCtx(joined),
                lines: lines.map(l => ({ orderId: l.orderId, quantityToInspect: l.quantityToInspect, poQuantitySnapshot: null, sampleMethod: l.sampleMethod })),
                moves: moves.map(m => ({ orderId: m.orderId })),
                acknowledge,
            }], 'create');
            const blockers = warnings.filter(w => w.level === 'block');
            if (blockers.length) throw blockedFail(blockers);

            const [existingLines] = await conn.query(`SELECT * FROM qc_visit_lines WHERE visit_id = ?`, [id]);
            const [existingMoves] = await conn.query(`SELECT * FROM qc_visit_moves WHERE visit_id = ?`, [id]);
            const orders = await loadOrderCtx(conn, lines.map(l => l.orderId));
            const changedScope = scopeChanged(existingLines, existingMoves, lines, moves);
            await syncLines(conn, id, lines, moves, orders, existingLines, existingMoves);
            const clear = changedScope ? clearingPatch(row, ['lines']) : { patch: {}, reopened: false, cleared: [] };
            await updateVisit(conn, id, clear.patch, req.userEmail, row);
            await audit(conn, 'qc_visit', id, 'lines_updated', {
                lines: existingLines.map(l => ({ orderId: Number(l.order_id), quantityToInspect: Number(l.quantity_to_inspect), sampleMethod: l.sample_method })),
                moves: existingMoves.map(m => ({ orderId: Number(m.order_id), toFactoryId: num(m.to_factory_id) })),
            }, {
                lines: lines.map(l => ({ orderId: l.orderId, quantityToInspect: l.quantityToInspect, sampleMethod: l.sampleMethod })),
                moves: moves.map(m => ({ orderId: m.orderId, toFactoryId: m.toFactoryId })),
                ...(clear.cleared.length ? { confirmationsCleared: clear.cleared } : {}),
            }, req.userEmail);
            if (clear.reopened) {
                await audit(conn, 'qc_visit', id, 'reproposed', { status: row.status }, { status: 'PROPOSED', changedFields: ['lines'], confirmationsCleared: clear.cleared }, req.userEmail);
            }
            return undefined;
        });
    }));

    // ── POST /api/v1/qc-visits/:id/transition ───────────────────────────────
    // { updatedAt, to, cancelCategory?, reason?, acknowledge? }
    app.post('/api/v1/qc-visits/:id(\\d+)/transition', route('POST /qc-visits/:id/transition', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const updatedAtMs = requireUpdatedAt(b);
        const to = typeof b.to === 'string' ? b.to.trim().toUpperCase() : null;
        if (!VISIT_STATUSES.includes(to)) throw badField('to', `to must be one of ${VISIT_STATUSES.join(', ')}.`);
        const acknowledge = ackOf(b.acknowledge);
        const cancelCategory = b.cancelCategory == null || b.cancelCategory === '' ? null : b.cancelCategory;
        const reason = typeof b.reason === 'string' ? b.reason : null;
        await writeVisit(res, id, updatedAtMs, async (conn, row) => {
            if (TERMINAL_STATUSES.includes(row.status)) throw terminalFail(row.status);
            const joined = (await loadVisitRows(conn, 'v.id = ?', [id]))[0];
            let warnings = [];
            if (to === 'PROPOSED') {
                const lines = await loadLineRows(conn, [id]);
                const moves = await loadMoveRows(conn, [id]);
                [warnings] = await warningsFor(conn, [{
                    visit: visitCtx(joined),
                    lines: lineCtx(lines),
                    moves: moves.map(m => ({ orderId: Number(m.order_id) })),
                    acknowledge,
                }], 'propose');
            }
            const confirmations = to === 'CONFIRMED' ? effectiveConfirmations(row, await loadConfirmationRows(conn, id)) : [];
            const blockers = transitionBlockers(joined, to, { warnings, confirmations, cancelCategory, reason, today: chinaToday() });
            if (blockers.length) throw blockedFail(blockers);

            const patch = applyTransition(row, to, { cancelCategory, reason, now: new Date() });
            if (to === 'PROPOSED') {
                // Fill any empty snapshot column from the live rows (a visit
                // created before the factory had an address, say).
                if (row.factory_id != null && !factoryDeleted(joined)) {
                    const [fr] = await conn.query(`SELECT * FROM qc_factories WHERE id = ?`, [row.factory_id]);
                    if (fr.length) for (const [k, v] of Object.entries(factorySnapshot(fr[0]))) if (row[k] == null && v != null) patch[k] = v;
                }
                if (row.inspector_id != null && !inspectorDeleted(joined)) {
                    const [ir] = await conn.query(`SELECT * FROM qc_inspectors WHERE id = ?`, [row.inspector_id]);
                    if (ir.length) for (const [k, v] of Object.entries(inspectorSnapshot(ir[0]))) if (row[k] == null && v != null) patch[k] = v;
                }
            }
            await updateVisit(conn, id, patch, req.userEmail, row);
            const action = { PROPOSED: 'proposed', CONFIRMED: 'confirmed', COMPLETED: 'completed', CANCELLED: 'cancelled' }[to];
            const after = { status: to };
            if (to === 'CANCELLED') Object.assign(after, { cancelCategory: patch.cancel_category, wastedTrip: patch.wasted_trip === 1, cancelReason: patch.cancel_reason });
            if (to === 'PROPOSED') {
                const warned = warnings.filter(w => w.level === 'warn').map(w => w.key);
                if (warned.length) after.warnings = unique(warned);
                if (acknowledge.length) after.acknowledged = acknowledge;
            }
            await audit(conn, 'qc_visit', id, action, { status: row.status }, after, req.userEmail);
            return undefined;
        });
    }));

    // ── POST /api/v1/qc-visits/:id/confirmations ────────────────────────────
    app.post('/api/v1/qc-visits/:id(\\d+)/confirmations', route('POST /qc-visits/:id/confirmations', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const updatedAtMs = requireUpdatedAt(b);
        const value = fromValidation(validateConfirmation(b)).value;
        await writeVisit(res, id, updatedAtMs, async (conn, row) => {
            if (TERMINAL_STATUSES.includes(row.status)) throw terminalFail(row.status);
            if (row.status === 'DRAFT') throw new HttpFail(409, 'NOT_PROPOSED', 'Propose the visit before recording confirmations.');
            const { sql, params } = setClause({ visit_id: id, ...value, recorded_by_email: req.userEmail || null });
            const [ins] = await conn.query(`INSERT INTO qc_visit_confirmations SET ${sql}`, params);
            const rowsAfter = await loadConfirmationRows(conn, id);
            const { patch, moved } = confirmationPatch(row, rowsAfter, ins.insertId, new Date());
            await updateVisit(conn, id, patch, req.userEmail, row);
            await audit(conn, 'qc_visit', id, 'confirmation_recorded', visitAuditSnapshot(row), {
                status: patch.status || row.status,
                factoryConfirmedAt: isoTs(patch.factory_confirmed_at),
                inspectorConfirmedAt: isoTs(patch.inspector_confirmed_at),
                confirmation: { id: ins.insertId, party: value.party, kind: value.kind, channel: value.channel, counterDate: value.counter_date, byName: value.by_name },
            }, req.userEmail);
            if (moved) await audit(conn, 'qc_visit', id, moved, { status: row.status }, { status: patch.status, via: 'confirmation', confirmationId: ins.insertId }, req.userEmail);
            return undefined;
        });
    }));

    // ── POST /api/v1/qc-visits/:id/accept-counter ───────────────────────────
    // { updatedAt, confirmationId } — the party's latest row must be that
    // counter. Copies its date (and window, when it gave one) onto the visit,
    // records a 'confirmed' row for the party and, when the slot changed,
    // clears the other party's confirmation.
    app.post('/api/v1/qc-visits/:id(\\d+)/accept-counter', route('POST /qc-visits/:id/accept-counter', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const updatedAtMs = requireUpdatedAt(b);
        const confirmationId = Number(b.confirmationId);
        if (!Number.isInteger(confirmationId) || confirmationId <= 0) throw badField('confirmationId', 'confirmationId is required.');
        await writeVisit(res, id, updatedAtMs, async (conn, row) => {
            if (TERMINAL_STATUSES.includes(row.status)) throw terminalFail(row.status);
            if (row.status === 'DRAFT') throw new HttpFail(409, 'NOT_PROPOSED', 'Propose the visit before recording confirmations.');
            const rows = await loadConfirmationRows(conn, id);
            const counter = rows.find(r => Number(r.id) === confirmationId);
            if (!counter) throw notFound(`Confirmation ${confirmationId} not found on this visit.`);
            if (counter.kind !== 'counter' || !ymdOf(counter.counter_date)) throw badField('confirmationId', 'That row is not a counter-proposal.');
            const latest = rows.filter(r => r.party === counter.party && r.kind !== 'proposed').pop();
            if (!latest || Number(latest.id) !== confirmationId) {
                throw badField('confirmationId', `The ${counter.party} has replied since that counter-proposal; only the latest one can be accepted.`);
            }
            const party = counter.party;
            const other = party === 'factory' ? 'inspector' : 'factory';

            const slot = { visit_date: ymdOf(counter.counter_date) };
            if (counter.counter_window_start != null || counter.counter_window_end != null) {
                slot.window_start = hmOf(counter.counter_window_start);
                slot.window_end = hmOf(counter.counter_window_end);
            }
            const slotChanged = Object.keys(slot).some(c => normCol(c, slot[c]) !== normCol(c, row[c]));

            const [ins] = await conn.query(
                `INSERT INTO qc_visit_confirmations
                    (visit_id, party, kind, channel, on_date, by_name, note, recorded_by_email)
                 VALUES (?, ?, 'confirmed', ?, ?, ?, 'Accepted counter-proposal', ?)`,
                [id, party, counter.channel, chinaToday(), counter.by_name || null, req.userEmail || null]
            );
            const rowsAfter = await loadConfirmationRows(conn, id);
            // The other party agreed to the old slot, not this one.
            const rowForCalc = slotChanged ? { ...row, [`${other}_confirmed_at`]: null } : row;
            const { patch } = confirmationPatch(rowForCalc, rowsAfter, ins.insertId, new Date());
            if (slotChanged) patch[`${other}_confirmed_at`] = null;
            // A CONFIRMED visit whose other party is cleared reopens; status
            // only moves forward when both are confirmed (confirmationPatch).
            if (row.status === 'CONFIRMED' && (patch.factory_confirmed_at == null || patch.inspector_confirmed_at == null)) {
                patch.status = 'PROPOSED';
                patch.confirmed_at = null;
            }
            await updateVisit(conn, id, { ...slot, ...patch }, req.userEmail, row);
            await audit(conn, 'qc_visit', id, 'counter_accepted', visitAuditSnapshot(row), {
                status: patch.status || row.status,
                visitDate: slot.visit_date,
                windowStart: 'window_start' in slot ? slot.window_start : hmOf(row.window_start),
                windowEnd: 'window_end' in slot ? slot.window_end : hmOf(row.window_end),
                party,
                counterConfirmationId: confirmationId,
                confirmationId: ins.insertId,
                ...(slotChanged && row[`${other}_confirmed_at`] != null ? { confirmationsCleared: [other] } : {}),
            }, req.userEmail);
            return undefined;
        });
    }));

    // ── POST /api/v1/qc-visits/:id/clone ────────────────────────────────────
    // COMPLETED or CANCELLED visits only (409 NOT_CLONEABLE otherwise).
    // { visitDate? } → a new DRAFT with the same supplier, factory, inspector,
    // lines (quantities kept) and moves. Lines whose order was deleted or
    // left IN_PRODUCTION / READY_FOR_QC are dropped (droppedOrderIds).
    app.post('/api/v1/qc-visits/:id(\\d+)/clone', route('POST /qc-visits/:id/clone', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const b = req.body || {};
        const visitDate = b.visitDate == null || b.visitDate === '' ? null : b.visitDate;
        if (visitDate != null && !isYmd(visitDate)) throw badField('visitDate', 'visitDate must be a real date, YYYY-MM-DD.');

        const detail = await withConnection(async (conn) => {
            const out = await inTx(conn, async () => {
                const [srcRows] = await conn.query(`SELECT * FROM qc_visits WHERE id = ?`, [id]);
                if (!srcRows.length) throw notFound(`Visit ${id} not found.`);
                const src = srcRows[0];
                // Plan §6: only a finished visit is cloned. Cloning an open one
                // would copy its quantities past the qty_exceeds check and
                // double-book the units.
                if (!TERMINAL_STATUSES.includes(src.status)) {
                    throw new HttpFail(409, 'NOT_CLONEABLE', 'Only a completed or cancelled visit can be cloned.');
                }
                const srcLines = await loadLineRows(conn, [id]);
                const srcMoves = await loadMoveRows(conn, [id]);
                const keepLines = srcLines.filter(l => l.o_id != null && l.o_deleted_at == null && CANDIDATE_ORDER_STATUSES.includes(l.o_status));
                const droppedOrderIds = srcLines.filter(l => !keepLines.includes(l)).map(l => Number(l.order_id));
                if (!keepLines.length) {
                    throw new HttpFail(409, 'NOTHING_TO_CLONE', 'None of this visit\'s lines is still in production or ready for QC.', { droppedOrderIds });
                }
                const keptIds = new Set(keepLines.map(l => Number(l.order_id)));

                const copy = [
                    'supplier_name', 'factory_id', ...Object.values(FACTORY_SNAPSHOT_COLS),
                    'inspector_id', ...Object.values(INSPECTOR_SNAPSHOT_COLS),
                    'window_start', 'window_end', 'tz', 'expected_production_state', 'report_deadline_hours',
                    'escalation_note', 'notes',
                ];
                const cols = Object.fromEntries(copy.map(c => [c, src[c] === undefined ? null : src[c]]));
                cols.window_start = hmOf(cols.window_start);
                cols.window_end = hmOf(cols.window_end);
                if (!cols.tz) cols.tz = 'Asia/Shanghai';
                Object.assign(cols, {
                    status: 'DRAFT',
                    visit_date: visitDate,
                    cloned_from_visit_id: id,
                    created_by_email: req.userEmail || null,
                    updated_by_email: req.userEmail || null,
                });
                const { sql, params } = setClause(cols);
                const [ins] = await conn.query(`INSERT INTO qc_visits SET ${sql}`, params);
                const newId = ins.insertId;
                await conn.query(`UPDATE qc_visits SET ref = ? WHERE id = ?`, [formatRef(newId), newId]);

                const orders = await loadOrderCtx(conn, [...keptIds]);
                const lines = keepLines.map(l => ({
                    orderId: Number(l.order_id),
                    quantityToInspect: Number(l.quantity_to_inspect),
                    sampleMethod: l.sample_method || 'full',
                    aql: (l.aql_level == null && l.aql_critical == null && l.aql_major == null && l.aql_minor == null) ? null
                        : { level: l.aql_level, critical: num(l.aql_critical), major: num(l.aql_major), minor: num(l.aql_minor) },
                    note: l.note || null,
                }));
                const moves = srcMoves.filter(m => keptIds.has(Number(m.order_id))).map(m => ({
                    orderId: Number(m.order_id),
                    toFactoryId: num(m.to_factory_id),
                    moveBy: ymdOf(m.move_by),
                    arrangedBy: m.arranged_by || null,
                    note: m.note || null,
                }));
                await syncLines(conn, newId, lines, moves, orders, [], []);
                await audit(conn, 'qc_visit', newId, 'cloned', null, {
                    ref: formatRef(newId),
                    clonedFromVisitId: id,
                    clonedFromRef: src.ref || formatRef(id),
                    visitDate,
                    lines: lines.map(l => ({ orderId: l.orderId, quantityToInspect: l.quantityToInspect })),
                    droppedOrderIds,
                }, req.userEmail);
                return { newId, droppedOrderIds };
            });
            return loadDetail(conn, out.newId, { droppedOrderIds: out.droppedOrderIds });
        });
        res.status(201).json(detail);
    }));

    // ── Attachments ─────────────────────────────────────────────────────────

    // POST /api/v1/qc-visits/:id/attachments/upload-url  { filename, contentType }
    app.post('/api/v1/qc-visits/:id(\\d+)/attachments/upload-url', route('POST /qc-visits/:id/attachments/upload-url', async (req, res) => {
        refuseReadOnly(req);
        if (!poBucket) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        const id = Number(req.params.id);
        const filename = clip(req.body && req.body.filename, 200);
        if (!filename) throw badField('filename', 'filename is required.');
        const contentType = clip(req.body && req.body.contentType, 100) || 'application/octet-stream';
        if (!isUploadType(contentType)) {
            throw badField('contentType', 'That file type is not allowed. Upload an image, PDF, video, Office document, CSV, text or zip file.');
        }
        const exists = await withConnection(async (conn) => {
            const [rows] = await conn.query(`SELECT id FROM qc_visits WHERE id = ?`, [id]);
            return rows.length > 0;
        });
        if (!exists) throw notFound(`Visit ${id} not found.`);
        const s3Key = `qc-visits/${uuidv4()}/${filename.replace(SAFE_FILENAME_RE, '_')}`;
        const uploadUrl = await getSignedUrl(
            s3, new PutObjectCommand({ Bucket: poBucket, Key: s3Key, ContentType: contentType }), { expiresIn: UPLOAD_URL_SECONDS }
        );
        res.status(201).json({
            uploadUrl, s3Key, uploadMethod: 'PUT', uploadHeaders: { 'Content-Type': contentType },
            expiresInSeconds: UPLOAD_URL_SECONDS, maxBytes: MAX_ATTACHMENT_BYTES,
        });
    }));

    // POST /api/v1/qc-visits/:id/attachments  { s3Key, filename, contentType, note? }
    // Attachments do not bump the visit's updated_at: they are not part of
    // the agreed slot and must not make another user's open copy STALE.
    app.post('/api/v1/qc-visits/:id(\\d+)/attachments', route('POST /qc-visits/:id/attachments', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        if (!poBucket) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        const id = Number(req.params.id);
        const b = req.body || {};
        const filename = clip(b.filename, 200);
        if (!filename) throw badField('filename', 'filename is required.');
        if (typeof b.s3Key !== 'string' || !ATTACHMENT_KEY_RE.test(b.s3Key)) {
            throw badField('s3Key', 'An s3Key from /qc-visits/:id/attachments/upload-url is required.');
        }
        const note = clip(b.note, 255);
        const exists = await withConnection(async (conn) => {
            const [rows] = await conn.query(`SELECT id FROM qc_visits WHERE id = ?`, [id]);
            return rows.length > 0;
        });
        if (!exists) throw notFound(`Visit ${id} not found.`);
        let head;
        try { head = await s3.send(new HeadObjectCommand({ Bucket: poBucket, Key: b.s3Key })); }
        catch { throw new HttpFail(409, 'NOT_UPLOADED', 'File not found in storage — PUT it to the uploadUrl first.'); }
        const size = Number(head.ContentLength) || 0;
        if (!size) throw badField('s3Key', 'The uploaded file is empty.');
        if (size > MAX_ATTACHMENT_BYTES) throw new HttpFail(413, 'TOO_LARGE', `File too large (max ${MAX_ATTACHMENT_BYTES} bytes).`);
        const contentType = head.ContentType || clip(b.contentType, 100) || 'application/octet-stream';

        const row = await withConnection(async (conn) => {
            const [taken] = await conn.query(`SELECT id FROM qc_visit_attachments WHERE s3_key = ? LIMIT 1`, [b.s3Key]);
            if (taken.length) throw new HttpFail(409, 'DUPLICATE_UPLOAD', 'That file is already registered.', { existingId: taken[0].id });
            const [ins] = await conn.query(
                `INSERT INTO qc_visit_attachments (visit_id, filename, s3_key, content_type, file_size, note, uploaded_by_email)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [id, filename, b.s3Key, contentType, size, note, req.userEmail || null]
            );
            await audit(conn, 'qc_visit', id, 'attachment_added', null, { attachmentId: ins.insertId, filename, fileSize: size, note }, req.userEmail);
            const [rows] = await conn.query(`SELECT * FROM qc_visit_attachments WHERE id = ?`, [ins.insertId]);
            return rows[0];
        });
        res.status(201).json(await attachmentToJson(row));
    }));

    // DELETE /api/v1/qc-visits/:id/attachments/:attId — soft; the file stays.
    app.delete('/api/v1/qc-visits/:id(\\d+)/attachments/:attId(\\d+)', route('DELETE /qc-visits/:id/attachments/:attId', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const attId = Number(req.params.attId);
        await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT * FROM qc_visit_attachments WHERE id = ? AND visit_id = ? AND deleted_at IS NULL`, [attId, id]
            );
            if (!rows.length) throw notFound(`Attachment ${attId} not found on this visit.`);
            await conn.query(
                `UPDATE qc_visit_attachments SET deleted_at = CURRENT_TIMESTAMP, deleted_by_email = ? WHERE id = ?`,
                [req.userEmail || null, attId]
            );
            await audit(conn, 'qc_visit', id, 'attachment_deleted', { attachmentId: attId, filename: rows[0].filename }, null, req.userEmail);
        });
        res.status(204).end();
    }));

    // ── Factories ───────────────────────────────────────────────────────────

    // A factory whose supplier_code now belongs to a supplier with another
    // name follows the rename: returned (and stored) under the new name.
    async function rehomeFactories(conn, rows) {
        const codes = unique(rows.filter(r => r.supplier_code).map(r => String(r.supplier_code)));
        if (!codes.length) return rows;
        let sup;
        try {
            [sup] = await conn.query(`SELECT code, name FROM suppliers WHERE code IN (?) AND deleted_at IS NULL`, [codes]);
        } catch (e) {
            log.warn('[qc-factories] supplier lookup for re-homing failed', { error: e.message });
            return rows;
        }
        const nameByCode = new Map();
        for (const s of sup) if (s.code && s.name && !nameByCode.has(String(s.code).toLowerCase())) nameByCode.set(String(s.code).toLowerCase(), s.name);
        for (const r of rows) {
            const name = r.supplier_code ? nameByCode.get(String(r.supplier_code).toLowerCase()) : null;
            if (!name || normSupplier(name) === normSupplier(r.supplier_name)) continue;
            const old = r.supplier_name;
            r.supplier_name = name;
            try {
                await conn.query(`UPDATE qc_factories SET supplier_name = ? WHERE id = ?`, [name, r.id]);
                await audit(conn, 'qc_factory', Number(r.id), 'updated', { supplierName: old }, { supplierName: name, via: 'supplier_code re-home' }, 'system:supplier-rehome');
            } catch (e) {
                log.warn('[qc-factories] re-home update failed', { id: r.id, error: e.message });
            }
        }
        return rows;
    }

    async function liveFactoriesOf(conn, supplierName, exceptId) {
        const [rows] = await conn.query(`SELECT id, supplier_name, label, is_default FROM qc_factories WHERE deleted_at IS NULL`);
        return rows.filter(r => normSupplier(r.supplier_name) === normSupplier(supplierName) && Number(r.id) !== Number(exceptId || 0));
    }

    const labelKey = s => String(s || '').trim().replace(/\s+/g, ' ').toLowerCase();

    // The JFPRO code of a supplier, matched by normalised name (null when
    // none, or when the lookup fails), so a later rename re-homes the factory.
    async function lookupSupplierCode(conn, supplierName) {
        try {
            const [s] = await conn.query(
                `SELECT code FROM suppliers
                  WHERE deleted_at IS NULL AND code IS NOT NULL AND code <> ''
                    AND TRIM(REGEXP_REPLACE(LOWER(name), '[[:space:]]+', ' ')) = ?
                  ORDER BY id LIMIT 1`,
                [normSupplier(supplierName)]
            );
            return s.length ? String(s[0].code).slice(0, 64) : null;
        } catch (e) {
            log.warn('[qc-factories] supplier code lookup failed', { error: e.message });
            return null;
        }
    }

    // GET /api/v1/qc-factories?supplier=&includeDeleted=1
    app.get('/api/v1/qc-factories', route('GET /qc-factories', async (req, res) => {
        const q = req.query || {};
        const includeDeleted = String(q.includeDeleted || '') === '1' || String(q.includeDeleted || '') === 'true';
        const supplier = clip(q.supplier, 255);
        const data = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT * FROM qc_factories ${includeDeleted ? '' : 'WHERE deleted_at IS NULL'}
                  ORDER BY supplier_name, is_default DESC, label`
            );
            await rehomeFactories(conn, rows);
            return rows.filter(r => !supplier || normSupplier(r.supplier_name) === normSupplier(supplier));
        });
        res.json({ data: data.map(factoryToJson) });
    }));

    // POST /api/v1/qc-factories  QcFactoryInput
    app.post('/api/v1/qc-factories', route('POST /qc-factories', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const value = fromValidation(validateFactory(req.body, { partial: false })).value;
        const row = await withConnection(async (conn) => inTx(conn, async () => {
            const peers = await liveFactoriesOf(conn, value.supplier_name, null);
            if (peers.some(p => labelKey(p.label) === labelKey(value.label))) {
                throw new HttpFail(409, 'DUPLICATE', `${value.supplier_name} already has a factory called "${value.label}".`, { field: 'label' });
            }
            const cols = { ...value };
            // Snapshot the JFPRO code so a later rename re-homes it. Only an
            // explicit non-empty supplierCode overrides the lookup (the UI
            // always sends supplierCode: null).
            if (cols.supplier_code == null) cols.supplier_code = await lookupSupplierCode(conn, value.supplier_name);
            cols.created_by_email = req.userEmail || null;
            cols.updated_by_email = req.userEmail || null;
            const { sql, params } = setClause(cols);
            const [ins] = await conn.query(`INSERT INTO qc_factories SET ${sql}`, params);
            if (cols.is_default === 1) {
                const others = peers.filter(p => bool(p.is_default)).map(p => p.id);
                if (others.length) await conn.query(`UPDATE qc_factories SET is_default = 0 WHERE id IN (?)`, [others]);
            }
            const [rows] = await conn.query(`SELECT * FROM qc_factories WHERE id = ?`, [ins.insertId]);
            await audit(conn, 'qc_factory', ins.insertId, 'created', null, factoryToJson(rows[0]), req.userEmail);
            return rows[0];
        }));
        res.status(201).json(factoryToJson(row));
    }));

    // PATCH /api/v1/qc-factories/:id  partial QcFactoryInput
    app.patch('/api/v1/qc-factories/:id(\\d+)', route('PATCH /qc-factories/:id', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const value = fromValidation(validateFactory(req.body, { partial: true })).value;
        const row = await withConnection(async (conn) => inTx(conn, async () => {
            const [cur] = await conn.query(`SELECT * FROM qc_factories WHERE id = ? AND deleted_at IS NULL FOR UPDATE`, [id]);
            if (!cur.length) throw notFound(`Factory ${id} not found.`);
            const before = cur[0];
            // supplierCode null / empty means "no override": it never wipes the
            // stored code. A supplier change looks the code up again (the old
            // one would re-home the factory back to the old supplier).
            if ('supplier_code' in value && value.supplier_code == null) delete value.supplier_code;
            if ('supplier_name' in value && !('supplier_code' in value)
                && normSupplier(value.supplier_name) !== normSupplier(before.supplier_name)) {
                value.supplier_code = await lookupSupplierCode(conn, value.supplier_name);
            }
            const supplierName = 'supplier_name' in value ? value.supplier_name : before.supplier_name;
            const label = 'label' in value ? value.label : before.label;
            const peers = await liveFactoriesOf(conn, supplierName, id);
            if (('supplier_name' in value || 'label' in value) && peers.some(p => labelKey(p.label) === labelKey(label))) {
                throw new HttpFail(409, 'DUPLICATE', `${supplierName} already has a factory called "${label}".`, { field: 'label' });
            }
            if (Object.keys(value).length) {
                const { sql, params } = setClause({ ...value, updated_by_email: req.userEmail || null });
                await conn.query(`UPDATE qc_factories SET ${sql} WHERE id = ?`, [...params, id]);
            }
            const isDefault = 'is_default' in value ? value.is_default : (bool(before.is_default) ? 1 : 0);
            if (isDefault === 1) {
                const others = peers.filter(p => bool(p.is_default)).map(p => p.id);
                if (others.length) await conn.query(`UPDATE qc_factories SET is_default = 0 WHERE id IN (?)`, [others]);
            }
            const [rows] = await conn.query(`SELECT * FROM qc_factories WHERE id = ?`, [id]);
            await audit(conn, 'qc_factory', id, 'updated', factoryToJson(before), factoryToJson(rows[0]), req.userEmail);
            return rows[0];
        }));
        res.json(factoryToJson(row));
    }));

    // DELETE /api/v1/qc-factories/:id — soft; refused while an open visit
    // (as host, or as a move's destination) uses it.
    app.delete('/api/v1/qc-factories/:id(\\d+)', route('DELETE /qc-factories/:id', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        await withConnection(async (conn) => inTx(conn, async () => {
            const [cur] = await conn.query(`SELECT * FROM qc_factories WHERE id = ? AND deleted_at IS NULL FOR UPDATE`, [id]);
            if (!cur.length) throw notFound(`Factory ${id} not found.`);
            const [used] = await conn.query(
                `SELECT DISTINCT v.id, v.ref FROM qc_visits v
                  WHERE v.status IN (?)
                    AND (v.factory_id = ? OR v.id IN (SELECT m.visit_id FROM qc_visit_moves m WHERE m.to_factory_id = ?))
                  ORDER BY v.id`,
                [OPEN_STATUSES, id, id]
            );
            if (used.length) {
                const visits = used.map(v => v.ref || formatRef(v.id));
                throw new HttpFail(409, 'IN_USE', `Factory is used by open visit${visits.length > 1 ? 's' : ''} ${visits.join(', ')}.`, { visits });
            }
            await conn.query(
                `UPDATE qc_factories SET deleted_at = CURRENT_TIMESTAMP, deleted_by_email = ?, is_default = 0 WHERE id = ?`,
                [req.userEmail || null, id]
            );
            await audit(conn, 'qc_factory', id, 'deleted', factoryToJson(cur[0]), null, req.userEmail);
        }));
        res.status(204).end();
    }));

    // ── Inspectors ──────────────────────────────────────────────────────────

    // GET /api/v1/qc-inspectors?includeInactive=1
    app.get('/api/v1/qc-inspectors', route('GET /qc-inspectors', async (req, res) => {
        const q = req.query || {};
        const includeInactive = String(q.includeInactive || '') === '1' || String(q.includeInactive || '') === 'true';
        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT * FROM qc_inspectors ${includeInactive ? '' : 'WHERE active = 1'} ORDER BY active DESC, name`
            );
            return r;
        });
        res.json({ data: rows.map(inspectorToJson) });
    }));

    async function openVisitsOfInspector(conn, id) {
        const [rows] = await conn.query(
            `SELECT id, ref FROM qc_visits WHERE inspector_id = ? AND status IN (?) ORDER BY id`, [id, OPEN_STATUSES]
        );
        return rows.map(v => v.ref || formatRef(v.id));
    }

    const dupInspector = (e, name) => (e && e.code === 'ER_DUP_ENTRY'
        ? new HttpFail(409, 'DUPLICATE', `An inspector called "${name}" already exists.`, { field: 'name' })
        : e);

    // POST /api/v1/qc-inspectors  QcInspectorInput
    app.post('/api/v1/qc-inspectors', route('POST /qc-inspectors', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const value = fromValidation(validateInspector(req.body, { partial: false })).value;
        const row = await withConnection(async (conn) => {
            const cols = { ...value, created_by_email: req.userEmail || null, updated_by_email: req.userEmail || null };
            const { sql, params } = setClause(cols);
            let ins;
            try { [ins] = await conn.query(`INSERT INTO qc_inspectors SET ${sql}`, params); }
            catch (e) { throw dupInspector(e, value.name); }
            const [rows] = await conn.query(`SELECT * FROM qc_inspectors WHERE id = ?`, [ins.insertId]);
            await audit(conn, 'qc_inspector', ins.insertId, 'created', null, inspectorToJson(rows[0]), req.userEmail);
            return rows[0];
        });
        res.status(201).json(inspectorToJson(row));
    }));

    // PATCH /api/v1/qc-inspectors/:id  partial QcInspectorInput; active:false
    // is refused while open visits use the inspector.
    app.patch('/api/v1/qc-inspectors/:id(\\d+)', route('PATCH /qc-inspectors/:id', async (req, res) => {
        refuseReadOnly(req);
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        const value = fromValidation(validateInspector(req.body, { partial: true })).value;
        const row = await withConnection(async (conn) => inTx(conn, async () => {
            const [cur] = await conn.query(`SELECT * FROM qc_inspectors WHERE id = ? FOR UPDATE`, [id]);
            if (!cur.length) throw notFound(`Inspector ${id} not found.`);
            const before = cur[0];
            const deactivating = value.active === 0 && bool(before.active);
            if (deactivating) {
                const visits = await openVisitsOfInspector(conn, id);
                if (visits.length) {
                    throw new HttpFail(409, 'IN_USE', `Inspector is on open visit${visits.length > 1 ? 's' : ''} ${visits.join(', ')}.`, { visits });
                }
            }
            if (Object.keys(value).length) {
                const { sql, params } = setClause({ ...value, updated_by_email: req.userEmail || null });
                try { await conn.query(`UPDATE qc_inspectors SET ${sql} WHERE id = ?`, [...params, id]); }
                catch (e) { throw dupInspector(e, value.name); }
            }
            const [rows] = await conn.query(`SELECT * FROM qc_inspectors WHERE id = ?`, [id]);
            await audit(conn, 'qc_inspector', id, deactivating ? 'deactivated' : 'updated', inspectorToJson(before), inspectorToJson(rows[0]), req.userEmail);
            return rows[0];
        }));
        res.json(inspectorToJson(row));
    }));
}

module.exports = {
    registerQcVisitRoutes,
    // for tests
    _internal: { visitToJson, lineToJson, moveToJson, confirmationToJson, factoryToJson, inspectorToJson, effectiveConfirmations, isoTs, normCol },
};
