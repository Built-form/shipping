'use strict';

// /api/v1/payment-reviews + /api/v1/payment-assignees — sign-off for the
// payments on ShipLine's Payments flow page. Two different people sign each
// payment off; one accountant is its assignee and is never one of the two.
// Accountants assign (and record payments elsewhere) but do not review.
//
// A payment has no row of its own (the page works it out from the terms and
// the goods on board), so everything is keyed by the key the page builds and
// a review records the figure the reviewer saw — see src/lib/payment-reviews.js
// for the key shapes and the rules. Schema:
// src/db/migrate/2026-09-28_20_payment_reviews.sql (no cold-start DDL).
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const L = require('../lib/payment-reviews');
const { TABLE: USERS_TABLE } = require('../lib/allowed-emails');

const REVIEW_SELECT = `SELECT r.*, u.display_name AS reviewer_name
                         FROM payment_reviews r
                         LEFT JOIN ${USERS_TABLE} u ON u.email = r.reviewed_by_email`;
const ASSIGNEE_SELECT = `SELECT a.*, u.display_name AS assignee_name
                           FROM payment_assignees a
                           LEFT JOIN ${USERS_TABLE} u ON u.email = a.assignee_email`;

function registerPaymentReviewRoutes(app, { withConnection, recordAudit, auditLogSchemaReady, log }) {
    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code });

    const activeFor = async (conn, key, { lock = false } = {}) => {
        const [rows] = await conn.query(
            `${REVIEW_SELECT} WHERE r.payment_key = ? AND r.revoked_at IS NULL ORDER BY r.reviewed_at, r.id${lock ? ' FOR UPDATE' : ''}`, [key]
        );
        return rows;
    };
    const assigneeFor = async (conn, key, { lock = false } = {}) => {
        const [rows] = await conn.query(`${ASSIGNEE_SELECT} WHERE a.payment_key = ?${lock ? ' FOR UPDATE' : ''}`, [key]);
        return rows[0] || null;
    };
    const reviewById = async (conn, id) => {
        const [rows] = await conn.query(`${REVIEW_SELECT} WHERE r.id = ?`, [id]);
        return rows[0] || null;
    };

    // GET /api/v1/payment-reviews — every current review and every assignee,
    // for the page's rows. ?paymentKey=… narrows to one payment and includes
    // its withdrawn and replaced reviews (the pop-up's history).
    app.get('/api/v1/payment-reviews', async (req, res) => {
        try {
            const out = await withConnection(async (conn) => {
                if (req.query.paymentKey != null) {
                    const k = L.parsePaymentKey(String(req.query.paymentKey));
                    if (k.error) return { fail: { status: 400, code: 'BAD_FIELD', error: k.error } };
                    const [rows] = await conn.query(`${REVIEW_SELECT} WHERE r.payment_key = ? ORDER BY r.reviewed_at, r.id`, [k.key]);
                    const a = await assigneeFor(conn, k.key);
                    return { data: rows.map(L.reviewRowToJson), assignees: a ? [L.assigneeRowToJson(a)] : [] };
                }
                const [rows] = await conn.query(`${REVIEW_SELECT} WHERE r.revoked_at IS NULL ORDER BY r.reviewed_at, r.id`);
                const [assignees] = await conn.query(`${ASSIGNEE_SELECT} ORDER BY a.id`);
                return { data: rows.map(L.reviewRowToJson), assignees: assignees.map(L.assigneeRowToJson) };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json({ ...out, required: L.REQUIRED_REVIEWS });
        } catch (error) {
            log.error('[GET /payment-reviews]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/payment-reviews
    // Body: { paymentKey, currency, amount, supplierName?, poNumbers?, dueDate? }
    // Signs the payment off as the caller, at the figure on their screen.
    // 201 { review, reviews } — reviews = the payment's current ones.
    app.post('/api/v1/payment-reviews', async (req, res) => {
        try {
            if (!L.canReview(req.userType)) {
                return refuse(res, { status: 403, code: 'CANNOT_REVIEW', error: `Your role (${req.userType || 'none'}) cannot sign payments off — two admin or standard users do.` });
            }
            const parsed = L.parseReviewBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const v = parsed.value;
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const active = await activeFor(conn, v.paymentKey, { lock: true });
                    const assignee = await assigneeFor(conn, v.paymentKey);
                    const d = L.decideReview({
                        active, userEmail: req.userEmail, assigneeEmail: assignee ? assignee.assignee_email : null,
                        amount: v.amount, currency: v.currency,
                    });
                    if (d.refuse) { await conn.rollback(); return { fail: d.refuse }; }
                    for (const id of d.supersede) {
                        const before = L.reviewRowToJson(active.find(r => r.id === id));
                        await conn.query(
                            `UPDATE payment_reviews SET revoked_at = NOW(), revoked_by_email = ?, revoked_reason = 'superseded' WHERE id = ? AND revoked_at IS NULL`,
                            [req.userEmail || null, id]
                        );
                        await recordAudit(conn, {
                            entityType: 'payment_review', entityId: id, action: 'update',
                            before, after: L.reviewRowToJson(await reviewById(conn, id)), userEmail: req.userEmail,
                        });
                    }
                    const [ins] = await conn.query(
                        `INSERT INTO payment_reviews (payment_key, kind, supplier_name, po_numbers, container_ref, currency, amount, due_date, reviewed_by_email)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [v.paymentKey, v.kind, v.supplierName, v.poNumbers, v.containerRef, v.currency, v.amount, v.dueDate, req.userEmail]
                    );
                    const review = L.reviewRowToJson(await reviewById(conn, ins.insertId));
                    await recordAudit(conn, { entityType: 'payment_review', entityId: ins.insertId, action: 'create', before: null, after: review, userEmail: req.userEmail });
                    await conn.commit();
                    return { review, reviews: (await activeFor(conn, v.paymentKey)).map(L.reviewRowToJson) };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.status(201).json(out);
        } catch (error) {
            log.error('[POST /payment-reviews]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/payment-reviews/:id — withdraw a sign-off: your own, or
    // anyone's as an admin. The row stays, marked withdrawn.
    // 200 { reviews } — the payment's current ones.
    app.delete('/api/v1/payment-reviews/:id(\\d+)', async (req, res) => {
        try {
            const id = Number(req.params.id);
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                const row = await reviewById(conn, id);
                if (!row) return { fail: { status: 404, code: 'NOT_FOUND', error: `Review ${id} not found.` } };
                if (row.revoked_at) return { fail: { status: 409, code: 'NOT_ACTIVE', error: 'That review was already withdrawn or replaced.' } };
                if (!L.canWithdraw({ row, userEmail: req.userEmail, userType: req.userType })) {
                    return { fail: { status: 403, code: 'NOT_YOURS', error: 'Only the reviewer, or an admin, can withdraw a review.' } };
                }
                const before = L.reviewRowToJson(row);
                await conn.query(
                    `UPDATE payment_reviews SET revoked_at = NOW(), revoked_by_email = ?, revoked_reason = 'withdrawn' WHERE id = ? AND revoked_at IS NULL`,
                    [req.userEmail || null, id]
                );
                await recordAudit(conn, {
                    entityType: 'payment_review', entityId: id, action: 'update',
                    before, after: L.reviewRowToJson(await reviewById(conn, id)), userEmail: req.userEmail,
                });
                return { reviews: (await activeFor(conn, row.payment_key)).map(L.reviewRowToJson) };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[DELETE /payment-reviews/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/payment-assignees/candidates — the accountants a payment can
    // be assigned to. Open to every allowlisted user: /api/v1/users is admin-only.
    app.get('/api/v1/payment-assignees/candidates', async (req, res) => {
        try {
            const rows = await withConnection(async (conn) => {
                const [r] = await conn.query(
                    `SELECT email, display_name FROM ${USERS_TABLE} WHERE type = ? ORDER BY COALESCE(NULLIF(display_name, ''), email)`, [L.ASSIGNEE_ROLE]
                );
                return r;
            });
            res.json({ data: rows.map(r => ({ email: r.email, displayName: r.display_name || null })) });
        } catch (error) {
            log.error('[GET /payment-assignees/candidates]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // PUT /api/v1/payment-assignees  Body: { paymentKey, assigneeEmail | null }
    // One assignee per payment, an accountant; null unassigns.
    // 200 { assignee } (null once unassigned).
    app.put('/api/v1/payment-assignees', async (req, res) => {
        try {
            if (!L.canAssign(req.userType)) {
                return refuse(res, { status: 403, code: 'CANNOT_ASSIGN', error: `Your role (${req.userType || 'none'}) cannot assign payments.` });
            }
            const parsed = L.parseAssigneeBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const { paymentKey, email } = parsed.value;
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const current = await assigneeFor(conn, paymentKey, { lock: true });
                    let candidate = null;
                    if (email) {
                        const [u] = await conn.query(`SELECT email, type FROM ${USERS_TABLE} WHERE email = ?`, [email]);
                        candidate = u[0] || null;
                    }
                    const d = L.decideAssignment({ email, candidate, activeReviews: await activeFor(conn, paymentKey) });
                    if (d.refuse) { await conn.rollback(); return { fail: d.refuse }; }
                    const before = current ? L.assigneeRowToJson(current) : null;
                    if (d.action === 'clear') {
                        if (current) {
                            await conn.query(`DELETE FROM payment_assignees WHERE id = ?`, [current.id]);
                            await recordAudit(conn, { entityType: 'payment_assignee', entityId: current.id, action: 'delete', before, after: null, userEmail: req.userEmail });
                        }
                        await conn.commit();
                        return { assignee: null };
                    }
                    if (current && current.assignee_email === d.email) { await conn.commit(); return { assignee: before }; }
                    await conn.query(
                        `INSERT INTO payment_assignees (payment_key, assignee_email, assigned_by_email) VALUES (?, ?, ?)
                         ON DUPLICATE KEY UPDATE assignee_email = VALUES(assignee_email), assigned_by_email = VALUES(assigned_by_email)`,
                        [paymentKey, d.email, req.userEmail || null]
                    );
                    const row = await assigneeFor(conn, paymentKey);
                    const after = L.assigneeRowToJson(row);
                    await recordAudit(conn, { entityType: 'payment_assignee', entityId: row.id, action: current ? 'update' : 'create', before, after, userEmail: req.userEmail });
                    await conn.commit();
                    return { assignee: after };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[PUT /payment-assignees]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPaymentReviewRoutes };
