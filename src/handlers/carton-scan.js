// ── Carton-scan receiving — PUBLIC API (no Google login) ────────────────────
// Warehouse staff receive stock straight from a carton scan. A scan yields a
// jf_code (SKU), a lot/batch number, and optionally an expiry date; the app
// looks up the matching order(s), the operator picks one + a received quantity,
// and the stock is allocated into Mintsoft. The scanned lot OVERWRITES whatever
// was on the order (the lot lives on the carton, not always on the order), but
// the EXPIRY DATE sent to Mintsoft is taken from the order row in our DB — the
// order's exp_date is authoritative, so the scanned/OCR'd expiry is NOT used for
// the receive.
//
// SEPARATE function/file from the authed orders.js on purpose: the public
// surface is just the routes below, and none of the authed routes are reachable
// through it. Every request is gated by a single shared SIX-DIGIT code
// (RECEIVING_QUICK_AUTH_CODE), checked server-side in constant time and sent on
// the `X-Quick-Auth` header.
//
// Routes (all gated by X-Quick-Auth; no JWT authorizer in serverless.yml):
//   GET  /api/v1/scan/warehouses                          → warehouses + locations
//   GET  /api/v1/scan/warehouses/:warehouseId/locations   → one warehouse's locations
//   POST /api/v1/scan/lookup    { jfCode, lotNumber, expiryDate? } → candidate orders
//   POST /api/v1/scan/receive   { orderId, quantity, locationId, warehouseId,
//                                 lotNumber, expiryDate, goodsInType?,
//                                 idempotencyKey?, operator? }      → receive into Mintsoft
//   GET  /api/v1/scan/gemini-key                          → { apiKey } for on-device OCR
const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { getPool } = require('../db');
const log = require('../lib/logger');
const { safeCompareCode, normalizeCode } = require('../lib/portal-code');
const { receiveOrderStock, findReceivableOrders, ReceiveError } = require('../services/order-receive');
const { snapshotJfCode } = require('./mintsoft-snapshot');
const { listWarehousesWithLocations, listLocationsForWarehouse } = require('../services/mintsoft-locations');

const app = express();
app.use(cors());
app.use(express.json({ limit: '256kb' }));

const pool = getPool();

// The shared six-digit access code. Normalised once here; requests are compared
// against it in constant time. Empty when unset → every request gets a 503.
const QUICK_AUTH_CODE = process.env.RECEIVING_QUICK_AUTH_CODE || '';
const QUICK_AUTH_NORM = normalizeCode(QUICK_AUTH_CODE);

// Cold-start setup. Self-contained so this Lambda works even if it warms before
// ordersApi: ensure order_receipts has the idempotency columns/index the
// receive path needs, and that audit_log exists. All idempotent.
const schemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        const migrations = [
            `ALTER TABLE order_receipts ADD COLUMN idempotency_key VARCHAR(64) NULL AFTER asn_item_id`,
            `ALTER TABLE order_receipts ADD UNIQUE KEY uk_order_idempotency (order_id, idempotency_key)`,
            `ALTER TABLE order_receipts ADD COLUMN type VARCHAR(16) NOT NULL DEFAULT 'received'`,
        ];
        for (const sql of migrations) {
            try { await conn.query(sql); } catch (e) {
                const msg = e.message || '';
                if (!msg.includes('Duplicate column') && !msg.includes('Duplicate key name')) throw e;
            }
        }
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
})().catch(err => log.error('[carton-scan] schema migration failed', err));

async function withConnection(fn) {
    const conn = await pool.getConnection();
    try {
        return await fn(conn);
    } finally {
        conn.release();
    }
}

// ── Quick-auth gate ─────────────────────────────────────────────────────────
// Runs on every route. The code is read from the X-Quick-Auth header (preferred,
// works for GET too) or a `code` field in the JSON body. A single generic 401
// for any failure so the endpoint can't be probed. Constant-time compare.
app.use((req, res, next) => {
    if (!QUICK_AUTH_NORM) {
        return res.status(503).json({ error: 'Receiving is not configured.' });
    }
    const supplied = normalizeCode(req.get('X-Quick-Auth') || req.body?.code || '');
    if (!supplied || !safeCompareCode(supplied, QUICK_AUTH_NORM)) {
        return res.status(401).json({ error: 'Invalid or missing access code.' });
    }
    next();
});

// ── GET /api/v1/scan/warehouses ───────────────────────────────────────────
app.get('/api/v1/scan/warehouses', async (req, res) => {
    try {
        const data = await withConnection((conn) => listWarehousesWithLocations(conn));
        res.json({ data });
    } catch (error) {
        log.error('[GET /scan/warehouses]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── GET /api/v1/scan/warehouses/:warehouseId/locations ─────────────────────
app.get('/api/v1/scan/warehouses/:warehouseId/locations', async (req, res) => {
    try {
        const warehouseId = Number(req.params.warehouseId);
        if (!warehouseId) return res.status(400).json({ error: 'warehouseId must be a number.' });
        const data = await withConnection((conn) => listLocationsForWarehouse(conn, warehouseId));
        res.json({ data });
    } catch (error) {
        log.error('[GET /scan/warehouses/:warehouseId/locations]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── POST /api/v1/scan/lookup ──────────────────────────────────────────────
// { jfCode, lotNumber?, expiryDate? } → { data: { candidates: [...] } }.
// Returns every receivable order matching the scanned jf_code, ranked by how
// well the carton's lot (then expiry) line up. More than one match → the
// operator disambiguates by picking an order.
app.post('/api/v1/scan/lookup', async (req, res) => {
    try {
        await schemaReady;
        const { jfCode, lotNumber, expiryDate } = req.body || {};
        const result = await withConnection((conn) => findReceivableOrders(conn, { jfCode, lotNumber, expiryDate }));
        res.json({ data: result });
    } catch (error) {
        if (error instanceof ReceiveError) {
            return res.status(error.status).json({ error: error.message, ...(error.payload || {}) });
        }
        log.error('[POST /scan/lookup]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── POST /api/v1/scan/receive ─────────────────────────────────────────────
// Allocate a scanned carton's quantity into Mintsoft against a chosen order.
// Same shared transaction the authed POST /orders/:id/receive uses. The scanned
// lotNumber overwrites the order's lot. The EXPIRY DATE, however, comes from the
// order row in our DB — the order's exp_date is authoritative, so the scanned
// expiry is deliberately NOT forwarded to the receive (it can still feed the
// /lookup ranking). The receive fails with MISSING_LOT_EXPIRY if the order has
// no exp_date set. `operator` (optional) is recorded as the audit actor. Send a
// stable per-carton `idempotencyKey` so a double-tap doesn't double-receive.
app.post('/api/v1/scan/receive', async (req, res) => {
    try {
        await schemaReady;
        const {
            orderId, quantity, locationId, warehouseId,
            goodsInType, lotNumber, idempotencyKey, operator,
        } = req.body || {};
        if (!orderId) return res.status(400).json({ error: 'orderId is required.' });

        const actor = `carton-scan:${(operator && String(operator).trim()) || 'unknown'}`;

        const conn = await pool.getConnection();
        let result;
        try {
            result = await receiveOrderStock(conn, {
                orderId,
                quantity, locationId, warehouseId, goodsInType,
                lotNumber, idempotencyKey,
                actorEmail: actor,
                // expiryDate intentionally omitted — the order's exp_date is what's
                // sent to Mintsoft as ExpiryDate, not the scanned/OCR'd value.
            });
        } finally {
            conn.release();
        }

        res.json({
            order: result.order,
            asnId: result.asnId,
            receipts: result.receipts,
            ...(result.idempotent ? { idempotent: true } : {}),
        });

        // Refresh stock_snapshots for this JF code so the stock-sum views reflect
        // the receive quickly (and can net it out) instead of waiting up to an
        // hour for the scheduled snapshot. Mirrors POST /orders/:id/receive:
        // fire-and-forget after a short delay for Mintsoft's aggregates to settle;
        // skipped on an idempotent replay (result.jfCode is null).
        if (result.jfCode) {
            const { jfCode, asin } = result;
            setTimeout(async () => {
                const c = await pool.getConnection();
                try {
                    await snapshotJfCode(c, jfCode, asin || '');
                } catch (err) {
                    log.warn('[POST /scan/receive] post-receive snapshot failed', { jfCode, error: err.message });
                } finally {
                    c.release();
                }
            }, 10000);
        }
    } catch (error) {
        if (error instanceof ReceiveError) {
            return res.status(error.status).json({ error: error.message, ...(error.payload || {}) });
        }
        log.error('[POST /scan/receive]', error);
        if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── GET /api/v1/scan/gemini-key ───────────────────────────────────────────
// Hands the scan app the Gemini API key so it can run image recognition (OCR
// of carton labels) on-device. Gated by the same quick-auth code as everything
// else here. 503 if the key isn't configured.
app.get('/api/v1/scan/gemini-key', (req, res) => {
    const apiKey = process.env.GEMINI_API_KEY || '';
    if (!apiKey) return res.status(503).json({ error: 'Gemini is not configured.' });
    res.json({ apiKey });
});

// ── Serverless export ───────────────────────────────────────────────────────
const serverlessApp = serverless(app);

module.exports.handler = async (event, context) => {
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.CARTON_SCAN_PORT || 3003;
    app.listen(PORT, () => {
        log.info(`Carton-scan API running on http://localhost:${PORT}`);
    });
}
