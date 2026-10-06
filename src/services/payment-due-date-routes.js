'use strict';

// /api/v1/payment-due-dates — due dates set by hand on ShipLine's Payments
// flow page, in place of the derived ones. Keyed like sign-offs (a payment
// key, or an item key for one row of a payment); one row per key. See
// src/lib/payment-due-dates.js for the key shapes and rules. Schema:
// src/db/migrate/2026-10-06_10_payment_due_dates.sql (no cold-start DDL).
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const L = require('../lib/payment-due-dates');
const { TABLE: USERS_TABLE } = require('../lib/allowed-emails');

const SELECT = `SELECT d.*, u.display_name AS setter_name
                  FROM payment_due_dates d
                  LEFT JOIN ${USERS_TABLE} u ON u.email = d.set_by_email`;

function registerPaymentDueDateRoutes(app, { withConnection, recordAudit, auditLogSchemaReady, log }) {
    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code });
    const byKey = async (conn, key, { lock = false } = {}) => {
        const [rows] = await conn.query(`${SELECT} WHERE d.target_key = ?${lock ? ' FOR UPDATE' : ''}`, [key]);
        return rows[0] || null;
    };
    const byId = async (conn, id, { lock = false } = {}) => {
        const [rows] = await conn.query(`${SELECT} WHERE d.id = ?${lock ? ' FOR UPDATE' : ''}`, [id]);
        return rows[0] || null;
    };

    // GET /api/v1/payment-due-dates — every date set by hand. 200 { data }
    app.get('/api/v1/payment-due-dates', async (req, res) => {
        try {
            const rows = await withConnection(async (conn) => {
                const [r] = await conn.query(`${SELECT} ORDER BY d.id`);
                return r;
            });
            res.json({ data: rows.map(L.rowToJson) });
        } catch (error) {
            log.error('[GET /payment-due-dates]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // PUT /api/v1/payment-due-dates  Body: { key, dueDate, note? }
    // Sets (or replaces) the date for that key. 200 { dueDate }
    app.put('/api/v1/payment-due-dates', async (req, res) => {
        try {
            if (!L.canSet(req.userType)) {
                return refuse(res, { status: 403, code: 'CANNOT_SET_DUE_DATE', error: `Your role (${req.userType || 'none'}) cannot set due dates — admin or standard users do.` });
            }
            const parsed = L.parseBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const v = parsed.value;
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const current = await byKey(conn, v.key, { lock: true });
                    const before = current ? L.rowToJson(current) : null;
                    if (current && L.rowToJson(current).dueDate === v.dueDate && (current.note || null) === v.note) {
                        await conn.commit();
                        return { dueDate: before, unchanged: true };
                    }
                    await conn.query(
                        `INSERT INTO payment_due_dates (target_key, due_date, note, set_by_email) VALUES (?, ?, ?, ?)
                         ON DUPLICATE KEY UPDATE due_date = VALUES(due_date), note = VALUES(note), set_by_email = VALUES(set_by_email)`,
                        [v.key, v.dueDate, v.note, req.userEmail || 'unknown']
                    );
                    const row = await byKey(conn, v.key);
                    const after = L.rowToJson(row);
                    await recordAudit(conn, { entityType: 'payment_due_date', entityId: row.id, action: current ? 'update' : 'create', before, after, userEmail: req.userEmail });
                    await conn.commit();
                    return { dueDate: after };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            res.json(out);
        } catch (error) {
            log.error('[PUT /payment-due-dates]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/payment-due-dates/:id — back to the derived date. 200 { removed }
    app.delete('/api/v1/payment-due-dates/:id(\\d+)', async (req, res) => {
        try {
            if (!L.canSet(req.userType)) {
                return refuse(res, { status: 403, code: 'CANNOT_SET_DUE_DATE', error: `Your role (${req.userType || 'none'}) cannot change due dates — admin or standard users do.` });
            }
            const id = Number(req.params.id);
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const row = await byId(conn, id, { lock: true });
                    if (!row) { await conn.rollback(); return { fail: { status: 404, code: 'NOT_FOUND', error: `Due date ${id} not found — it may already be back to the derived date.` } }; }
                    const before = L.rowToJson(row);
                    await conn.query(`DELETE FROM payment_due_dates WHERE id = ?`, [id]);
                    await recordAudit(conn, { entityType: 'payment_due_date', entityId: id, action: 'delete', before, after: null, userEmail: req.userEmail });
                    await conn.commit();
                    return { removed: before };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[DELETE /payment-due-dates/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPaymentDueDateRoutes };
