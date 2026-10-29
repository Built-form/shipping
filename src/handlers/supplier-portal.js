// ── Supplier self-service "ready date" portal — PUBLIC API ──────────────────
// A small, deliberately isolated Lambda that external suppliers (no Google
// login) hit to set the estimated ready date on the lines of one of their POs.
//
// It is a SEPARATE function/file from the authed orders.js on purpose: the
// public attack surface is just the two routes below, and none of the authed
// routes are reachable through it. The ONLY fields a supplier can write are
// orders.estimated_ready_date and orders.actual_ready_date, and every request
// is gated by a per-supplier access code (suppliers.portal_code) checked
// server-side in constant time.
//
// A supplier only needs their PO number + their access code — no login and no
// supplier picker. The PO number resolves to the PO, the PO names its supplier,
// and the code must match that supplier's portal_code. Suppliers are never
// enumerated to the public.
//
// Routes (all unauthenticated — no JWT authorizer in serverless.yml):
//   POST /api/v1/portal/po-lookup       → { poNumber, accessCode }
//   POST /api/v1/portal/po-ready-date   → + { updates: [{ orderId, estimatedReadyDate }] }
//
// The one exception to "dates only" is the opt-in markReadyForQc flag on an
// update, which moves a line IN_PRODUCTION → READY_FOR_QC (and nothing else).
//
// QC visits (docs/qc-visit-portal.md):
//   POST /api/v1/portal/qc-visit-lookup → { poNumber, portalCode }
//   POST /api/v1/portal/qc-visit        → + { visitRef, action:'confirm'|'counter', … }
// The factory confirms (or counter-proposes) a QC visit slot. The only write is
// a qc_visit_confirmations row (party 'factory', channel 'portal') plus the
// visit's denormalised factory_confirmed_at / status, by the same rule the
// authed app applies.
const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { getPool } = require('../db');
const log = require('../lib/logger');
const { ensureSupplierPortalCodes, safeCompareCode, normalizeCode } = require('../lib/portal-code');
const {
    normSupplier, chinaToday, isYmd, isHm, confirmationTimestamps, applyTransition,
} = require('../lib/qc-visits');
// Open availability links follow the visit (docs/qc-availability.md): a
// portal confirm that completes the pair revokes them ('confirmed').
const { syncAvailabilityLinks } = require('../services/qc-availability-routes');

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

const pool = getPool();

// Cold-start setup: make sure every live JFPro supplier has a portal access
// code (a data top-up on jfpro.suppliers, idempotent). Schema, audit_log
// included, is applied at deploy time from src/db/migrate/.
const schemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await ensureSupplierPortalCodes(conn);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[portal] supplier portal-code setup failed', err));

// ── Helpers ─────────────────────────────────────────────────────────────────
async function withConnection(fn) {
    const conn = await pool.getConnection();
    try {
        return await fn(conn);
    } finally {
        conn.release();
    }
}

function formatDate(val) {
    if (!val) return null;
    return val.toISOString?.().slice(0, 10) ?? val;
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
// Accept a calendar date string (YYYY-MM-DD) within a sane window, or null/''
// meaning "clear the date". Anything else is rejected.
function normalizeReadyDate(raw) {
    if (raw === null || raw === undefined || raw === '') return { ok: true, value: null };
    if (typeof raw !== 'string' || !DATE_RE.test(raw)) return { ok: false };
    const d = new Date(`${raw}T00:00:00Z`);
    if (Number.isNaN(d.getTime())) return { ok: false };
    const year = d.getUTCFullYear();
    if (year < 2020 || year > 2100) return { ok: false };
    return { ok: true, value: raw };
}

// Single generic failure for every "we won't show you this PO" case (bad code,
// no such PO, PO's supplier has no code). Never reveal which part failed — that
// would turn the endpoint into an enumeration oracle.
const GENERIC_AUTH_ERROR = 'PO not found, or the access code is incorrect.';

// Resolve { supplier, po } from a PO number + access code, or null on ANY
// mismatch. The PO number finds the PO; the PO names its supplier; the code
// must match that supplier's portal_code. If two non-deleted POs share a
// number (rare), the code also disambiguates — we accept the one whose
// supplier's code matches.
async function authenticate(conn, { poNumber, accessCode }) {
    if (typeof poNumber !== 'string' || !poNumber.trim()) return null;
    if (typeof accessCode !== 'string') return null;
    // Case-insensitive, space/dash-tolerant — so the supplier can type loosely.
    const code = normalizeCode(accessCode);
    if (!code) return null;

    const [poRows] = await conn.query(
        `SELECT id, po_number, supplier FROM purchase_orders
          WHERE po_number = ? AND deleted_at IS NULL
          ORDER BY created_at DESC`,
        [poNumber.trim()]
    );

    for (const po of poRows) {
        if (!po.supplier) continue;
        const [supRows] = await conn.query(
            'SELECT id, name, portal_code FROM suppliers WHERE name = ? AND deleted_at IS NULL',
            [po.supplier]
        );
        // suppliers is now a view over jfpro.suppliers, whose `name` is not
        // unique-constrained, so one name can resolve to several rows. Accept the
        // first whose code verifies rather than blindly trusting supRows[0].
        for (const supplier of supRows) {
            if (supplier.portal_code && safeCompareCode(code, normalizeCode(supplier.portal_code))) {
                return { supplier, po };
            }
        }
    }
    return null;
}

// A supplier may only change the ready date while a line is still in the
// production phase — the kanban stages "Purchase Order Sent" (PO_SENT) →
// "Under Production" (IN_PRODUCTION) → "Ready for QC" (READY_FOR_QC) → "Ready
// at Factory" (READY). Once a line is consolidated / on the water / received,
// the ready date is locked. Status is per line, so a part-shipped PO can have
// some editable lines and some locked.
const EDITABLE_STATUSES = new Set(['PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY']);

// ── The one status move a supplier may make ─────────────────────────────────
// "goods finished, come and inspect": IN_PRODUCTION (doc name UNDER_PRODUCTION)
// → READY_FOR_QC, opted into per line with `markReadyForQc: true`. It is the
// only write here that touches orders.status, it is one step forward on the
// ShipLine pipeline (src/lib/order-transitions.js), and it is refused from any
// other status — a supplier can never advance a line past QC, move it
// backwards, or skip a stage.
//
// Same gate as the authed app (gateFieldsFor('READY_FOR_QC') === ['actualReadyDate']):
// the line must have an actual ready date — either already stored or supplied
// in the same request, which is the normal case (the supplier fills the date
// and ticks "ready for QC" together).
const READY_FOR_QC_FROM = 'IN_PRODUCTION';
const READY_FOR_QC_STATUS = 'READY_FOR_QC';
// Mirrors STATUS_DATE_KEY in orders.js — the milestone timestamp key stamped
// into orders.dates so this move looks identical to one made in ShipLine.
const READY_FOR_QC_DATE_KEY = 'ready_for_qc';

function parseDates(raw) {
    if (!raw) return {};
    if (typeof raw === 'string') {
        try { return JSON.parse(raw) || {}; } catch { return {}; }
    }
    return raw;
}

async function fetchLineItems(conn, poId) {
    const [rows] = await conn.query(
        `SELECT id, jf_code, product_name, quantity, status,
                estimated_ready_date, actual_ready_date
           FROM orders
          WHERE purchase_order_id = ? AND deleted_at IS NULL
          ORDER BY jf_code ASC, id ASC`,
        [poId]
    );
    return rows.map(r => ({
        orderId: r.id,
        jfCode: r.jf_code || null,
        productName: r.product_name || null,
        quantity: Number(r.quantity || 0),
        status: r.status || null,
        // Frontend should disable the date input when false; the write path
        // enforces the same rule server-side.
        editable: EDITABLE_STATUSES.has(r.status),
        // Whether `markReadyForQc` would be accepted for this line right now.
        // The actual-ready-date part can also be satisfied by the same request,
        // so the frontend should offer the tick-box whenever the status is
        // right and just require the date alongside it.
        canMarkReadyForQc: r.status === READY_FOR_QC_FROM,
        estimatedReadyDate: formatDate(r.estimated_ready_date),
        actualReadyDate: formatDate(r.actual_ready_date),
    }));
}

// Minimal audit insert. `before`/`after` are objects holding only the changed
// fields (e.g. { estimatedReadyDate, actualReadyDate }). We compute them
// explicitly and skip no-ops, so no diffing is needed here. Never throws — a
// missing audit row must not fail the supplier's update.
async function recordReadyDateAudit(conn, { orderId, before, after, supplierName }) {
    try {
        await conn.query(
            `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
             VALUES ('order', ?, 'update', ?, ?, ?)`,
            [
                orderId,
                JSON.stringify(before),
                JSON.stringify(after),
                `supplier-portal:${supplierName}`,
            ]
        );
    } catch (err) {
        log.warn('[portal audit] insert failed', err.message);
    }
}

// The date columns a supplier may set, keyed by the request field name. Each
// is YYYY-MM-DD, or empty/null to clear. A field is only touched when its key
// is present in the update object — so sending one date never wipes the other.
const WRITABLE_DATE_FIELDS = {
    estimatedReadyDate: 'estimated_ready_date',
    actualReadyDate: 'actual_ready_date',
};

// ── POST /api/v1/portal/po-lookup ───────────────────────────────────────────
// { poNumber, accessCode } → the PO's line items (read).
app.post('/api/v1/portal/po-lookup', async (req, res) => {
    try {
        await schemaReady;
        const { poNumber, accessCode } = req.body || {};
        const result = await withConnection(async (conn) => {
            const auth = await authenticate(conn, { poNumber, accessCode });
            if (!auth) return null;
            const items = await fetchLineItems(conn, auth.po.id);
            return { auth, items };
        });
        if (!result) return res.status(401).json({ error: GENERIC_AUTH_ERROR });
        res.json({
            data: {
                poNumber: result.auth.po.po_number,
                supplier: result.auth.supplier.name,
                items: result.items,
            },
        });
    } catch (error) {
        log.error('[POST /portal/po-lookup]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── POST /api/v1/portal/po-ready-date ───────────────────────────────────────
// { poNumber, accessCode, updates: [{ orderId, estimatedReadyDate?, actualReadyDate?, markReadyForQc? }] }
// Re-validates the code + PO on every write (stateless — the client re-sends
// the code it holds). Writes only estimated_ready_date / actual_ready_date
// (whichever keys are present), only on lines that belong to this PO AND are
// still in an editable status (EDITABLE_STATUSES), and audits every change. A
// locked line (status moved past the factory) → 409.
//
// `markReadyForQc: true` additionally moves that line IN_PRODUCTION →
// READY_FOR_QC (see READY_FOR_QC_FROM above): 409 from any other status, 422
// without an actual ready date (stored or set in the same update).
app.post('/api/v1/portal/po-ready-date', async (req, res) => {
    try {
        await schemaReady;
        const { poNumber, accessCode, updates } = req.body || {};
        if (!Array.isArray(updates) || updates.length === 0) {
            return res.status(400).json({ error: 'updates must be a non-empty array.' });
        }
        if (updates.length > 500) {
            return res.status(400).json({ error: 'Too many updates in a single request.' });
        }

        const outcome = await withConnection(async (conn) => {
            const auth = await authenticate(conn, { poNumber, accessCode });
            if (!auth) return { error: 'auth' };

            // Lines that actually belong to this PO — the ownership allow-list
            // plus status (for the editable gate) and the before-state for
            // auditing.
            const [ownRows] = await conn.query(
                `SELECT id, status, estimated_ready_date, actual_ready_date, dates FROM orders
                  WHERE purchase_order_id = ? AND deleted_at IS NULL`,
                [auth.po.id]
            );
            const byId = new Map(ownRows.map(r => [r.id, r]));

            // Validate everything before applying anything. For each line,
            // collect just the columns whose key is present in the update.
            const planned = [];
            for (const u of updates) {
                const oid = Number(u?.orderId);
                if (!Number.isInteger(oid) || !byId.has(oid)) return { error: 'order' };
                const row = byId.get(oid);
                // Locked once the line moves past the factory stages.
                if (!EDITABLE_STATUSES.has(row.status)) return { error: 'status' };

                const cols = {};
                for (const [field, col] of Object.entries(WRITABLE_DATE_FIELDS)) {
                    if (!u || !Object.prototype.hasOwnProperty.call(u, field)) continue;
                    const norm = normalizeReadyDate(u[field]);
                    if (!norm.ok) return { error: 'date' };
                    cols[col] = norm.value;
                }

                // Opt-in status move. Only IN_PRODUCTION → READY_FOR_QC, only
                // with an actual ready date (stored, or set by this same update).
                const markReadyForQc = u?.markReadyForQc === true;
                if (u && 'markReadyForQc' in u && typeof u.markReadyForQc !== 'boolean') {
                    return { error: 'qcFlag' };
                }
                if (markReadyForQc) {
                    // Already there (a double-submit / a re-send of the whole
                    // form) is a no-op success, not an error — only a genuinely
                    // wrong starting status is refused.
                    if (row.status !== READY_FOR_QC_FROM && row.status !== READY_FOR_QC_STATUS) {
                        return { error: 'qcFrom' };
                    }
                    if (row.status === READY_FOR_QC_FROM) {
                        const ard = ('actual_ready_date' in cols)
                            ? cols.actual_ready_date
                            : formatDate(row.actual_ready_date);
                        if (!ard) return { error: 'qcNoDate' };
                    }
                }

                if (Object.keys(cols).length || markReadyForQc) planned.push({ oid, cols, markReadyForQc });
            }

            for (const { oid, cols, markReadyForQc } of planned) {
                const row = byId.get(oid);
                const sets = [];
                const vals = [];
                const before = {};
                const after = {};
                for (const [field, col] of Object.entries(WRITABLE_DATE_FIELDS)) {
                    if (!(col in cols)) continue;
                    const prev = formatDate(row[col]);
                    if (prev === cols[col]) continue; // unchanged, skip
                    sets.push(`${col} = ?`);
                    vals.push(cols[col]);
                    before[field] = prev;
                    after[field] = cols[col];
                }
                // The status move rides along on the same UPDATE, stamping the
                // milestone into orders.dates exactly as ShipLine's own status
                // patch does. Guarded on the live status so a duplicate submit
                // is a no-op rather than a second write.
                if (markReadyForQc && row.status === READY_FOR_QC_FROM) {
                    const dates = parseDates(row.dates);
                    dates[READY_FOR_QC_DATE_KEY] = new Date().toISOString();
                    sets.push('status = ?', 'dates = ?');
                    vals.push(READY_FOR_QC_STATUS, JSON.stringify(dates));
                    before.status = row.status;
                    after.status = READY_FOR_QC_STATUS;
                }
                if (!sets.length) continue; // all no-ops for this line
                vals.push(oid);
                await conn.query(`UPDATE orders SET ${sets.join(', ')} WHERE id = ?`, vals);
                await recordReadyDateAudit(conn, {
                    orderId: oid,
                    before,
                    after,
                    supplierName: auth.supplier.name,
                });
            }

            const items = await fetchLineItems(conn, auth.po.id);
            return { items };
        });

        if (outcome.error === 'auth') return res.status(401).json({ error: GENERIC_AUTH_ERROR });
        if (outcome.error === 'order') {
            return res.status(400).json({ error: 'One or more items do not belong to this PO.' });
        }
        if (outcome.error === 'status') {
            return res.status(409).json({ error: 'This item can no longer be updated — production has moved past the factory stage.' });
        }
        if (outcome.error === 'date') {
            return res.status(400).json({ error: 'Invalid ready date — use YYYY-MM-DD (or empty to clear).' });
        }
        if (outcome.error === 'qcFlag') {
            return res.status(400).json({ error: 'markReadyForQc must be true or false.' });
        }
        if (outcome.error === 'qcFrom') {
            return res.status(409).json({ error: 'This item can only be marked ready for QC while it is under production.' });
        }
        if (outcome.error === 'qcNoDate') {
            return res.status(422).json({ error: 'An actual ready date is required before an item can be marked ready for QC.' });
        }
        res.json({ data: { items: outcome.items } });
    } catch (error) {
        log.error('[POST /portal/po-ready-date]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── QC visits: the factory confirms or counter-proposes a slot ──────────────
// Contract: docs/qc-visit-portal.md. Same auth as above (PO number → supplier →
// portal code). The visit must (a) carry at least one live line of that PO,
// (b) be hosted by that PO's supplier (qc_visits.supplier_name, case-folded —
// a line moved in from another supplier does not let that supplier's code
// confirm this factory's slot) and (c) be PROPOSED or CONFIRMED. Any visit
// mismatch is a 404, never a 403, so the endpoint does not reveal which
// visits exist.
const QC_PORTAL_STATUSES = ['PROPOSED', 'CONFIRMED'];
const QC_ACTIONS = new Set(['confirm', 'counter']);
const QC_NOT_FOUND = { error: 'QC visit not found.', code: 'NOT_FOUND' };
const QC_NOTE_MAX = 500;
// Refs are QCV_<digits> (formatRef); anything else is refused before the DB.
const QC_REF_RE = /^[A-Za-z0-9_-]{1,16}$/;
// A counter-proposal more than this far ahead is a typo or abuse, not a slot.
const QC_COUNTER_MAX_DAYS = 366;
// Portal answers a single visit accepts per rolling 24 h. A real factory
// answers a handful of times; past this the code is being replayed (log
// flooding, or flipping a CONFIRMED visit back to PROPOSED over and over).
// The gateway throttle is global per route, so it cannot cap one visit.
const QC_PORTAL_ROWS_PER_DAY = 10;

// Control characters other than tab / newline are dropped from free text.
// eslint-disable-next-line no-control-regex
const stripControls = str => str.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '');

function addDaysYmd(ymd, days) {
    const d = new Date(`${ymd}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().slice(0, 10);
}

// The existing portal routes call the code `accessCode`; the QC visit contract
// calls it `portalCode`. Accept either so jportal can reuse its form state.
// CONTRACT-GAP: `accessCode` alias accepted alongside `portalCode`.
function portalCodeOf(body) {
    return typeof body.portalCode === 'string' ? body.portalCode : body.accessCode;
}

function hmOf(val) {
    return typeof val === 'string' && val.length >= 5 ? val.slice(0, 5) : null;
}

function isoOf(val) {
    if (!val) return null;
    return val.toISOString?.() ?? val;
}

function badField(res, field, error) {
    return res.status(400).json({ error, code: 'BAD_FIELD', field });
}

// The PO numbers on a visit that belong to the host supplier (another
// supplier's PO numbers, on moved-in lines, are never shown to this one).
async function hostPoNumbers(conn, visitIds, hostSupplier) {
    if (!visitIds.length) return new Map();
    const [rows] = await conn.query(
        `SELECT DISTINCT l.visit_id, o.po_number, o.supplier
           FROM qc_visit_lines l
           JOIN orders o ON o.id = l.order_id
          WHERE l.visit_id IN (${visitIds.map(() => '?').join(', ')})
            AND o.deleted_at IS NULL`,
        visitIds
    );
    const host = normSupplier(hostSupplier);
    const out = new Map(visitIds.map(id => [id, new Set()]));
    for (const r of rows) {
        if (!r.po_number || normSupplier(r.supplier || '') !== host) continue;
        out.get(r.visit_id)?.add(r.po_number);
    }
    return new Map([...out].map(([id, set]) => [id, [...set].sort()]));
}

// ── POST /api/v1/portal/qc-visit-lookup ─────────────────────────────────────
// { poNumber, portalCode } → { data: [{ ref, status, visitDate, windowStart,
// windowEnd, factoryLabel, factoryCity, poNumbers, factoryConfirmedAt }] }
app.post('/api/v1/portal/qc-visit-lookup', async (req, res) => {
    try {
        await schemaReady;
        const body = req.body || {};
        const result = await withConnection(async (conn) => {
            const auth = await authenticate(conn, { poNumber: body.poNumber, accessCode: portalCodeOf(body) });
            if (!auth) return null;
            const [rows] = await conn.query(
                `SELECT DISTINCT v.id, v.ref, v.status, v.supplier_name, v.visit_date,
                        v.window_start, v.window_end, v.factory_label, v.factory_city,
                        v.factory_confirmed_at
                   FROM qc_visits v
                   JOIN qc_visit_lines l ON l.visit_id = v.id
                   JOIN orders o ON o.id = l.order_id
                  WHERE o.purchase_order_id = ?
                    AND o.deleted_at IS NULL
                    AND v.status IN (${QC_PORTAL_STATUSES.map(() => '?').join(', ')})
                  ORDER BY v.visit_date IS NULL, v.visit_date, v.id`,
                [auth.po.id, ...QC_PORTAL_STATUSES]
            );
            const host = normSupplier(auth.po.supplier);
            const visits = rows.filter(v => normSupplier(v.supplier_name || '') === host);
            const poNumbers = await hostPoNumbers(conn, visits.map(v => v.id), auth.po.supplier);
            return visits.map(v => ({
                ref: v.ref,
                status: v.status,
                visitDate: formatDate(v.visit_date),
                windowStart: hmOf(v.window_start),
                windowEnd: hmOf(v.window_end),
                factoryLabel: v.factory_label || null,
                factoryCity: v.factory_city || null,
                poNumbers: poNumbers.get(v.id) || [],
                // CONTRACT-GAP: additive — lets the portal show "you confirmed on …".
                factoryConfirmedAt: isoOf(v.factory_confirmed_at),
            }));
        });
        if (!result) return res.status(401).json({ error: GENERIC_AUTH_ERROR });
        res.json({ data: result });
    } catch (error) {
        log.error('[POST /portal/qc-visit-lookup]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── POST /api/v1/portal/qc-visit ────────────────────────────────────────────
// { poNumber, portalCode, visitRef, action:'confirm'|'counter', counterDate?,
//   counterWindowStart?, counterWindowEnd?, note?, visitDate? } → { ok:true, status }
// `confirm` records a 'confirmed' row; with the inspector already confirmed a
// PROPOSED visit becomes CONFIRMED. `counter` records a 'counter' row (needs
// counterDate, today or later in China), clears the factory confirmation and
// drops a CONFIRMED visit back to PROPOSED. Optional `visitDate` is the date the
// supplier was shown: if staff moved the visit since, 409 STALE instead of
// confirming a date the supplier never saw.
app.post('/api/v1/portal/qc-visit', async (req, res) => {
    try {
        await schemaReady;
        const body = req.body || {};
        const { visitRef, action } = body;

        // Shape checks first (cheap, reveal nothing about any visit).
        if (typeof visitRef !== 'string' || !QC_REF_RE.test(visitRef.trim())) {
            return badField(res, 'visitRef', 'visitRef is required.');
        }
        if (!QC_ACTIONS.has(action)) return badField(res, 'action', "action must be 'confirm' or 'counter'.");
        let note = null;
        if (body.note != null && body.note !== '') {
            if (typeof body.note !== 'string') return badField(res, 'note', 'note must be text.');
            note = stripControls(body.note).trim() || null;
            if (note && note.length > QC_NOTE_MAX) return badField(res, 'note', `note cannot exceed ${QC_NOTE_MAX} characters.`);
        }
        let shownDate = null;
        if (body.visitDate != null && body.visitDate !== '') {
            if (!isYmd(body.visitDate)) return badField(res, 'visitDate', 'visitDate must be YYYY-MM-DD.');
            shownDate = body.visitDate;
        }
        let counter = { date: null, start: null, end: null };
        if (action === 'counter') {
            const { counterDate, counterWindowStart, counterWindowEnd } = body;
            if (!isYmd(counterDate)) return badField(res, 'counterDate', 'counterDate is required (YYYY-MM-DD).');
            if (counterDate < chinaToday()) return badField(res, 'counterDate', 'counterDate cannot be in the past.');
            // CONTRACT-GAP: upper bound added in the security review.
            if (counterDate > addDaysYmd(chinaToday(), QC_COUNTER_MAX_DAYS)) {
                return badField(res, 'counterDate', 'counterDate is too far ahead.');
            }
            const start = counterWindowStart == null || counterWindowStart === '' ? null : counterWindowStart;
            const end = counterWindowEnd == null || counterWindowEnd === '' ? null : counterWindowEnd;
            if (start !== null && !isHm(start)) return badField(res, 'counterWindowStart', 'counterWindowStart must be HH:MM.');
            if (end !== null && !isHm(end)) return badField(res, 'counterWindowEnd', 'counterWindowEnd must be HH:MM.');
            if (start && end && end <= start) return badField(res, 'counterWindowEnd', 'counterWindowEnd must be after counterWindowStart.');
            counter = { date: counterDate, start, end };
        }

        const outcome = await withConnection(async (conn) => {
            const auth = await authenticate(conn, { poNumber: body.poNumber, accessCode: portalCodeOf(body) });
            if (!auth) return { error: 'auth' };
            const who = `supplier-portal:${auth.supplier.name}`;

            await conn.beginTransaction();
            try {
                const [vRows] = await conn.query(
                    `SELECT id, ref, status, supplier_name, visit_date, window_start, window_end,
                            factory_confirmed_at, inspector_confirmed_at, proposed_at, confirmed_at
                       FROM qc_visits WHERE ref = ? FOR UPDATE`,
                    [visitRef.trim().toUpperCase()]
                );
                const visit = vRows[0];
                const fail = async (error) => { await conn.rollback(); return { error }; };
                if (!visit) return await fail('notFound');
                if (!QC_PORTAL_STATUSES.includes(visit.status)) return await fail('notFound');
                if (normSupplier(visit.supplier_name || '') !== normSupplier(auth.po.supplier)) return await fail('notFound');
                const [onVisit] = await conn.query(
                    `SELECT 1 FROM qc_visit_lines l
                       JOIN orders o ON o.id = l.order_id
                      WHERE l.visit_id = ? AND o.purchase_order_id = ? AND o.deleted_at IS NULL
                      LIMIT 1`,
                    [visit.id, auth.po.id]
                );
                if (!onVisit.length) return await fail('notFound');
                if (shownDate && formatDate(visit.visit_date) !== shownDate) return await fail('stale');

                const kind = action === 'confirm' ? 'confirmed' : 'counter';
                // A repeat confirm (double-submit, page re-opened) is a no-op
                // success rather than a second row in the visit's history.
                if (kind === 'confirmed' && visit.factory_confirmed_at
                    && (visit.status === 'CONFIRMED' || !visit.inspector_confirmed_at)) {
                    await conn.commit();
                    return { status: visit.status };
                }
                // Per-visit cap on portal answers (QC_PORTAL_ROWS_PER_DAY),
                // counted under the visit-row lock so concurrent calls cannot
                // all slip under it.
                const [[recent]] = await conn.query(
                    `SELECT COUNT(*) AS n FROM qc_visit_confirmations c
                      WHERE c.visit_id = ? AND c.party = 'factory' AND c.channel = 'portal'
                        AND c.created_at > (CURRENT_TIMESTAMP - INTERVAL 1 DAY)`,
                    [visit.id]
                );
                if (recent && Number(recent.n) >= QC_PORTAL_ROWS_PER_DAY) return await fail('tooMany');
                // on_date = the China day the factory answered.
                const [ins] = await conn.query(
                    `INSERT INTO qc_visit_confirmations
                        (visit_id, party, kind, channel, on_date, counter_date, counter_window_start,
                         counter_window_end, by_name, note, recorded_by_email)
                     VALUES (?, 'factory', ?, 'portal', ?, ?, ?, ?, ?, ?, ?)`,
                    [visit.id, kind, chinaToday(), counter.date, counter.start, counter.end,
                        auth.supplier.name, note, who.slice(0, 255)]
                );
                const [confs] = await conn.query(
                    `SELECT id, visit_id, party, kind, channel, created_at
                       FROM qc_visit_confirmations WHERE visit_id = ?
                      ORDER BY created_at ASC, id ASC`,
                    [visit.id]
                );
                const factoryConfirmedAt = confirmationTimestamps(confs).factory_confirmed_at || null;

                // Same rule as POST /qc-visits/:id/confirmations: PROPOSED →
                // CONFIRMED once both parties are confirmed; a counter on a
                // CONFIRMED visit drops it back to PROPOSED. The inspector side
                // is read from the denormalised column, which already honours
                // any clearing the authed app applied.
                let to = null;
                if (kind === 'confirmed' && visit.status === 'PROPOSED' && visit.inspector_confirmed_at) to = 'CONFIRMED';
                if (kind === 'counter' && visit.status === 'CONFIRMED') to = 'PROPOSED';

                const sets = ['factory_confirmed_at = ?', 'updated_by_email = ?', 'updated_at = CURRENT_TIMESTAMP(3)'];
                const vals = [factoryConfirmedAt, who.slice(0, 255)];
                if (to === 'CONFIRMED') {
                    // { status, confirmed_at }
                    for (const [col, val] of Object.entries(applyTransition(visit, to, { now: new Date() }))) {
                        sets.push(`${col} = ?`);
                        vals.push(val);
                    }
                } else if (to === 'PROPOSED') {
                    // The "reopen" of the clearing rule: applyTransition only
                    // covers forward moves (CONFIRMED -> PROPOSED throws there).
                    // CONTRACT-GAP: no helper for the reopen patch; proposed_at is
                    // kept (the slot was proposed then), confirmed_at cleared.
                    sets.push('status = ?', 'confirmed_at = NULL');
                    vals.push('PROPOSED');
                }
                vals.push(visit.id);
                await conn.query(`UPDATE qc_visits SET ${sets.join(', ')} WHERE id = ?`, vals);
                // Same transaction; never fails the confirm (it rolls back to
                // its own savepoint and logs). The portal never moves the date.
                if (to) {
                    await syncAvailabilityLinks(conn, {
                        visitId: visit.id, before: visit, after: { status: to },
                        userEmail: who.slice(0, 255), recordAudit: recordPortalAudit, log,
                    });
                }

                const status = to || visit.status;
                const before = { status: visit.status, factoryConfirmedAt: isoOf(visit.factory_confirmed_at) };
                const after = {
                    status,
                    factoryConfirmedAt: isoOf(factoryConfirmedAt),
                    confirmation: {
                        id: ins.insertId, party: 'factory', kind, channel: 'portal',
                        counterDate: counter.date, counterWindowStart: counter.start,
                        counterWindowEnd: counter.end, byName: auth.supplier.name, note,
                    },
                };
                await recordPortalAudit(conn, { entityType: 'qc_visit', entityId: visit.id, action: 'confirmation_recorded', before, after, userEmail: who });
                if (to) {
                    await recordPortalAudit(conn, {
                        entityType: 'qc_visit', entityId: visit.id,
                        action: to === 'CONFIRMED' ? 'confirmed' : 'reproposed',
                        before: { status: visit.status }, after: { status: to, reason: `factory ${kind} via supplier portal` },
                        userEmail: who,
                    });
                }
                await conn.commit();
                return { status };
            } catch (err) {
                try { await conn.rollback(); } catch { /* connection already gone */ }
                throw err;
            }
        });

        if (outcome.error === 'auth') return res.status(401).json({ error: GENERIC_AUTH_ERROR });
        if (outcome.error === 'notFound') return res.status(404).json(QC_NOT_FOUND);
        if (outcome.error === 'tooMany') {
            return res.status(429).json({ error: 'Too many answers for this visit today. Please contact us directly.', code: 'TOO_MANY' });
        }
        if (outcome.error === 'stale') {
            return res.status(409).json({ error: 'The visit date has changed. Please reload and check the new date.', code: 'STALE' });
        }
        res.json({ ok: true, status: outcome.status });
    } catch (error) {
        log.error('[POST /portal/qc-visit]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Body-parser failures (malformed JSON, body over the limit) on the QC routes
// answer JSON. Without this, Express's default handler replies with an HTML
// page carrying the stack trace (NODE_ENV is unset on Lambda, so Express runs
// in 'development' mode). Scoped to the QC paths; the same handler without
// the path list would cover the older portal routes too.
app.use(['/api/v1/portal/qc-visit', '/api/v1/portal/qc-visit-lookup'], (err, req, res, next) => {
    if (res.headersSent) return next(err);
    const status = err && (err.status || err.statusCode);
    if (status === 413) return res.status(413).json({ error: 'Request body too large.', code: 'TOO_LARGE' });
    if (status && status >= 400 && status < 500) return res.status(400).json({ error: 'Invalid request body.', code: 'BAD_BODY' });
    log.error('[portal qc] unhandled', err);
    return res.status(500).json({ error: 'An internal error occurred.' });
});

// Raw audit insert, like recordReadyDateAudit but for any entity. Never throws.
async function recordPortalAudit(conn, { entityType, entityId, action, before, after, userEmail }) {
    try {
        await conn.query(
            `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [entityType, entityId, action, JSON.stringify(before), JSON.stringify(after), userEmail]
        );
    } catch (err) {
        log.warn('[portal audit] insert failed', err.message);
    }
}

// ── Serverless export ───────────────────────────────────────────────────────
const serverlessApp = serverless(app);

module.exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.PORTAL_PORT || 3002;
    app.listen(PORT, () => {
        log.info(`Supplier portal API running on http://localhost:${PORT}`);
    });
}
