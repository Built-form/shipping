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
const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { getPool } = require('../db');
const log = require('../lib/logger');
const { ensureSupplierPortalCodes, safeCompareCode, normalizeCode } = require('../lib/portal-code');

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

const pool = getPool();

// Cold-start setup. Self-contained: ensures the suppliers.portal_code column +
// codes exist, and that audit_log exists (normally created by orders.js, but we
// don't want to depend on which Lambda warmed first). Both are idempotent.
const schemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await ensureSupplierPortalCodes(conn);
        await conn.query(`
            CREATE TABLE IF NOT EXISTS audit_log (
                id BIGINT NOT NULL AUTO_INCREMENT,
                entity_type VARCHAR(32) NOT NULL,
                entity_id INT NOT NULL,
                action VARCHAR(16) NOT NULL,
                before_json JSON NULL,
                after_json JSON NULL,
                user_email VARCHAR(255) NULL,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_entity (entity_type, entity_id, created_at)
            )
        `);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[portal] schema migration failed', err));

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
// { poNumber, accessCode, updates: [{ orderId, estimatedReadyDate?, actualReadyDate? }] }
// Re-validates the code + PO on every write (stateless — the client re-sends
// the code it holds). Writes only estimated_ready_date / actual_ready_date
// (whichever keys are present), only on lines that belong to this PO AND are
// still in an editable status (EDITABLE_STATUSES), and audits every change. A
// locked line (status moved past the factory) → 409.
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
                `SELECT id, status, estimated_ready_date, actual_ready_date FROM orders
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
                // Locked once the line moves past the factory stages.
                if (!EDITABLE_STATUSES.has(byId.get(oid).status)) return { error: 'status' };

                const cols = {};
                for (const [field, col] of Object.entries(WRITABLE_DATE_FIELDS)) {
                    if (!u || !Object.prototype.hasOwnProperty.call(u, field)) continue;
                    const norm = normalizeReadyDate(u[field]);
                    if (!norm.ok) return { error: 'date' };
                    cols[col] = norm.value;
                }
                if (Object.keys(cols).length) planned.push({ oid, cols });
            }

            for (const { oid, cols } of planned) {
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
        res.json({ data: { items: outcome.items } });
    } catch (error) {
        log.error('[POST /portal/po-ready-date]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

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
