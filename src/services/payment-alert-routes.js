'use strict';

// /api/v1/payment-alert-dismissals — "Needs attention" lines on ShipLine's
// Payments flow page that an admin has dismissed. The page works its alerts
// out itself and keys each by what it says (see src/lib/payment-alerts.js);
// this keeps who dismissed which key, when, and what it said then. Schema:
// src/db/migrate/2026-10-02_10_payment_alert_dismissals.sql (no cold-start DDL).
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const L = require('../lib/payment-alerts');
const { TABLE: USERS_TABLE } = require('../lib/allowed-emails');

const SELECT = `SELECT d.*, u.display_name AS dismisser_name
                  FROM payment_alert_dismissals d
                  LEFT JOIN ${USERS_TABLE} u ON u.email = d.dismissed_by_email`;

function registerPaymentAlertRoutes(app, { withConnection, recordAudit, auditLogSchemaReady, log }) {
    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code });
    const cannot = req => ({ status: 403, code: 'CANNOT_DISMISS', error: `Only an admin can dismiss or restore an alert (your role: ${req.userType || 'none'}).` });
    const byId = async (conn, id) => {
        const [rows] = await conn.query(`${SELECT} WHERE d.id = ?`, [id]);
        return rows[0] || null;
    };

    // GET /api/v1/payment-alert-dismissals — every dismissal still standing.
    app.get('/api/v1/payment-alert-dismissals', async (req, res) => {
        try {
            const rows = await withConnection(async (conn) => {
                const [r] = await conn.query(`${SELECT} WHERE d.restored_at IS NULL ORDER BY d.dismissed_at, d.id`);
                return r;
            });
            res.json({ data: rows.map(L.dismissalRowToJson) });
        } catch (error) {
            log.error('[GET /payment-alert-dismissals]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/payment-alert-dismissals — admin only.
    // Body: { alertKey, kind, currency?, poNumber?, shipmentReference?, supplierName?, detail?, amount?, note? }
    // 201 { dismissal }; 200 { dismissal } when that key is already dismissed.
    app.post('/api/v1/payment-alert-dismissals', async (req, res) => {
        try {
            if (!L.canDismiss(req.userType)) return refuse(res, cannot(req));
            const parsed = L.parseDismissBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const v = parsed.value;
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                const [have] = await conn.query(`${SELECT} WHERE d.alert_key = ? AND d.restored_at IS NULL ORDER BY d.id LIMIT 1`, [v.alertKey]);
                if (have[0]) return { status: 200, dismissal: L.dismissalRowToJson(have[0]) };
                const [ins] = await conn.query(
                    `INSERT INTO payment_alert_dismissals (alert_key, kind, currency, po_number, shipment_reference, supplier_name, detail, amount, note, dismissed_by_email)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [v.alertKey, v.kind, v.currency, v.poNumber, v.shipmentReference, v.supplierName, v.detail, v.amount, v.note, req.userEmail || 'unknown']
                );
                const dismissal = L.dismissalRowToJson(await byId(conn, ins.insertId));
                await recordAudit(conn, {
                    entityType: 'payment_alert_dismissal', entityId: ins.insertId, action: 'create',
                    before: null, after: dismissal, userEmail: req.userEmail,
                });
                return { status: 201, dismissal };
            });
            res.status(out.status).json({ dismissal: out.dismissal });
        } catch (error) {
            log.error('[POST /payment-alert-dismissals]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/payment-alert-dismissals/:id — restore the alert (admin
    // only). The row stays, marked restored. 200 { dismissal }.
    app.delete('/api/v1/payment-alert-dismissals/:id(\\d+)', async (req, res) => {
        try {
            if (!L.canDismiss(req.userType)) return refuse(res, cannot(req));
            const id = Number(req.params.id);
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                const row = await byId(conn, id);
                if (!row) return { fail: { status: 404, code: 'NOT_FOUND', error: `Dismissal ${id} not found.` } };
                if (row.restored_at) return { fail: { status: 409, code: 'NOT_ACTIVE', error: 'That alert was already restored.' } };
                const before = L.dismissalRowToJson(row);
                await conn.query(
                    `UPDATE payment_alert_dismissals SET restored_at = NOW(), restored_by_email = ? WHERE id = ? AND restored_at IS NULL`,
                    [req.userEmail || null, id]
                );
                const after = L.dismissalRowToJson(await byId(conn, id));
                await recordAudit(conn, { entityType: 'payment_alert_dismissal', entityId: id, action: 'update', before, after, userEmail: req.userEmail });
                return { dismissal: after };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[DELETE /payment-alert-dismissals/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPaymentAlertRoutes };
