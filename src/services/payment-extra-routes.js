'use strict';

// /api/v1/payment-extras — extra charges and credits on the payments of
// ShipLine's Payments flow page: money a supplier bills that is not goods
// (mould, handling, samples, …) or a credit they give. Each rides with a
// payment the page already works out — a PO's deposit, or one supplier's
// balance in one container — and is signed off and paid with it. Rules and
// shapes: src/lib/payment-extras.js. Schema:
// src/db/migrate/2026-09-29_10_payment_extras.sql (no cold-start DDL).
//
// A transfer pays an extra through a supplier-payments line of kind 'extra'
// (orders.js), which settles it; "mark paid" here is for money that went out
// with no transfer recorded in ShipLine.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const L = require('../lib/payment-extras');
const { supplierKey, resolveShipment } = require('./shipment-payments');

const EXTRA_SELECT = `SELECT e.*, po.po_number
                        FROM payment_extras e
                        LEFT JOIN purchase_orders po ON po.id = e.purchase_order_id`;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function registerPaymentExtraRoutes(app, {
    withConnection, recordAudit, auditLogSchemaReady, log,
    loadAppliedByTarget, loadSettlementsByTarget, sameSupplierName,
}) {
    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.payload || {}) });
    const cannotEdit = req => ({ status: 403, code: 'CANNOT_EDIT_EXTRAS', error: `Your role (${req.userType || 'none'}) cannot change extra charges.` });
    const notFound = id => ({ status: 404, code: 'NOT_FOUND', error: `Extra charge ${id} not found.` });

    const hydrate = async (conn, rows) => {
        const ids = rows.map(r => r.id);
        const applied = await loadAppliedByTarget(conn, 'extra', ids);
        const settlements = await loadSettlementsByTarget(conn, 'extra', ids);
        return rows.map(r => L.extraRowToJson(r, { applied: applied.get(r.id) || 0, settlements: settlements.get(r.id) || [] }));
    };
    const rowById = async (conn, id, { lock = false } = {}) => {
        const [rows] = await conn.query(`${EXTRA_SELECT} WHERE e.id = ? AND e.deleted_at IS NULL${lock ? ' FOR UPDATE' : ''}`, [id]);
        return rows[0] || null;
    };
    const jsonById = async (conn, id) => {
        const r = await rowById(conn, id);
        return r ? (await hydrate(conn, [r]))[0] : null;
    };

    // What the extra rides with, checked against what exists: a live PO in the
    // same currency, and the container. { fail } or { shipmentId, shipmentReference, warnings }.
    const resolveTarget = async (conn, v) => {
        const warnings = [];
        if (v.purchaseOrderId != null) {
            const [rows] = await conn.query(
                `SELECT id, po_number, supplier, currency FROM purchase_orders WHERE id = ? AND deleted_at IS NULL`, [v.purchaseOrderId]
            );
            const po = rows[0];
            if (!po) return { fail: { status: 404, code: 'NOT_FOUND', error: `Purchase order ${v.purchaseOrderId} not found.` } };
            const poCurrency = String(po.currency || 'USD').trim().toUpperCase();
            if (poCurrency !== v.currency) {
                return { fail: { status: 422, code: 'CURRENCY_MISMATCH', error: `${po.po_number} is in ${poCurrency}; the extra is in ${v.currency}.` } };
            }
            // The page names the supplier as JFPRO does, the PO as it was typed.
            if (po.supplier && supplierKey(po.supplier) !== supplierKey(v.supplierName) && !sameSupplierName(po.supplier, v.supplierName)) {
                warnings.push(`${po.po_number} is filed under "${po.supplier}".`);
            }
        }
        let shipmentId = null;
        let shipmentReference = v.shipmentReference;
        if (v.ridesWith === 'balance' && v.shipmentId != null) {
            const resolved = await resolveShipment(conn, { id: v.shipmentId });
            if (resolved.notFound || !resolved.row) return { fail: { status: 404, code: 'NOT_FOUND', error: `Shipment ${v.shipmentId} not found.` } };
            shipmentId = resolved.row.id;
            shipmentReference = shipmentReference || resolved.row.reference || String(resolved.row.id);
        }
        return { shipmentId, shipmentReference, warnings };
    };

    // An extra suggested from an invoice is added once.
    const duplicateOf = async (conn, v, { excludeId = null } = {}) => {
        if (!v.sourceKind) return null;
        const [rows] = await conn.query(
            `SELECT id FROM payment_extras
              WHERE deleted_at IS NULL AND source_kind = ? AND source_id = ? AND amount = ? ${excludeId ? 'AND id <> ?' : ''} LIMIT 1`,
            [v.sourceKind, v.sourceId, v.amount, ...(excludeId ? [excludeId] : [])]
        );
        return rows[0] ? rows[0].id : null;
    };

    // After an edit: a transfer that settled the old figure may not cover the
    // new one (open it again), or money already applied may now cover it.
    const reconcile = async (conn, id, userEmail) => {
        const row = await rowById(conn, id);
        const applied = (await loadAppliedByTarget(conn, 'extra', [id])).get(id) || 0;
        const covered = L.settles(applied, Number(row.amount));
        if (row.status === 'paid' && row.settled_by_payment_id != null && !covered) {
            await conn.query(`UPDATE payment_extras SET status = 'open', paid_on = NULL, settled_by_payment_id = NULL, updated_by_email = ? WHERE id = ?`, [userEmail || null, id]);
        } else if (row.status === 'open' && covered) {
            const last = ((await loadSettlementsByTarget(conn, 'extra', [id])).get(id) || [])[0];
            if (last) await conn.query(`UPDATE payment_extras SET status = 'paid', paid_on = ?, settled_by_payment_id = ?, updated_by_email = ? WHERE id = ?`, [last.paidOn, last.paymentId, userEmail || null, id]);
        }
    };

    const writeValues = (v, t) => [
        v.supplierName, supplierKey(v.supplierName), v.currency, v.amount, v.kind, v.description, v.ridesWith,
        v.purchaseOrderId, t.shipmentId, t.shipmentReference, v.dueDate, v.sourceKind, v.sourceId, v.note,
    ];

    // GET /api/v1/payment-extras?supplier=&currency=&purchaseOrderId=&shipmentId=
    // Every live extra (open and paid) with what transfers have applied to it.
    app.get('/api/v1/payment-extras', async (req, res) => {
        try {
            const where = ['e.deleted_at IS NULL'];
            const params = [];
            if (req.query.currency) { where.push('e.currency = ?'); params.push(String(req.query.currency).trim().toUpperCase()); }
            if (req.query.purchaseOrderId) { where.push('e.purchase_order_id = ?'); params.push(Number(req.query.purchaseOrderId)); }
            if (req.query.shipmentId) { where.push('e.shipment_id = ?'); params.push(Number(req.query.shipmentId)); }
            const supplier = typeof req.query.supplier === 'string' ? req.query.supplier.trim() : '';
            const data = await withConnection(async (conn) => {
                const [rows] = await conn.query(`${EXTRA_SELECT} WHERE ${where.join(' AND ')} ORDER BY e.id`, params);
                const key = supplier ? supplierKey(supplier) : null;
                return hydrate(conn, key ? rows.filter(r => r.supplier_key === key || sameSupplierName(r.supplier_name, supplier)) : rows);
            });
            res.json({ data });
        } catch (error) {
            log.error('[GET /payment-extras]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/payment-extras — add a charge or credit. 201 { ...extra, warnings }.
    app.post('/api/v1/payment-extras', async (req, res) => {
        try {
            if (!L.canEdit(req.userType)) return refuse(res, cannotEdit(req));
            const parsed = L.parseExtraBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const v = parsed.value;
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                const t = await resolveTarget(conn, v);
                if (t.fail) return { fail: t.fail };
                await conn.beginTransaction();
                try {
                    const dup = await duplicateOf(conn, v);
                    if (dup) {
                        await conn.rollback();
                        return { fail: { status: 409, code: 'ALREADY_ADDED', error: 'That charge from this invoice has already been added.', payload: { extraId: dup } } };
                    }
                    const [ins] = await conn.query(
                        `INSERT INTO payment_extras (supplier_name, supplier_key, currency, amount, kind, description, rides_with,
                                                     purchase_order_id, shipment_id, shipment_reference, due_date, source_kind, source_id, note, created_by_email)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [...writeValues(v, t), req.userEmail || null]
                    );
                    const json = await jsonById(conn, ins.insertId);
                    await recordAudit(conn, { entityType: 'payment_extra', entityId: ins.insertId, action: 'create', before: null, after: json, userEmail: req.userEmail });
                    await conn.commit();
                    return { json, warnings: t.warnings };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.status(201).json({ ...out.json, warnings: out.warnings });
        } catch (error) {
            log.error('[POST /payment-extras]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // PUT /api/v1/payment-extras/:id — replace its details (same body as POST).
    // Once a transfer has applied money to it, it must stay what that money paid.
    app.put('/api/v1/payment-extras/:id(\\d+)', async (req, res) => {
        try {
            if (!L.canEdit(req.userType)) return refuse(res, cannotEdit(req));
            const id = Number(req.params.id);
            const parsed = L.parseExtraBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const v = parsed.value;
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                if (!(await rowById(conn, id))) return { fail: notFound(id) };
                const t = await resolveTarget(conn, v);
                if (t.fail) return { fail: t.fail };
                await conn.beginTransaction();
                try {
                    const current = await rowById(conn, id, { lock: true });
                    if (!current) { await conn.rollback(); return { fail: notFound(id) }; }
                    const applied = (await loadAppliedByTarget(conn, 'extra', [id])).get(id) || 0;
                    const refusal = L.decideEdit({
                        current: { amount: Number(current.amount), currency: current.currency, supplierKey: current.supplier_key },
                        next: { amount: v.amount, currency: v.currency, supplierKey: supplierKey(v.supplierName) },
                        applied,
                    });
                    if (refusal) { await conn.rollback(); return { fail: refusal }; }
                    const dup = await duplicateOf(conn, v, { excludeId: id });
                    if (dup) {
                        await conn.rollback();
                        return { fail: { status: 409, code: 'ALREADY_ADDED', error: 'That charge from this invoice has already been added.', payload: { extraId: dup } } };
                    }
                    const before = (await hydrate(conn, [current]))[0];
                    await conn.query(
                        `UPDATE payment_extras
                            SET supplier_name = ?, supplier_key = ?, currency = ?, amount = ?, kind = ?, description = ?, rides_with = ?,
                                purchase_order_id = ?, shipment_id = ?, shipment_reference = ?, due_date = ?, source_kind = ?, source_id = ?, note = ?,
                                updated_by_email = ?
                          WHERE id = ?`,
                        [...writeValues(v, t), req.userEmail || null, id]
                    );
                    await reconcile(conn, id, req.userEmail);
                    const json = await jsonById(conn, id);
                    await recordAudit(conn, { entityType: 'payment_extra', entityId: id, action: 'update', before, after: json, userEmail: req.userEmail });
                    await conn.commit();
                    return { json, warnings: t.warnings };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.json({ ...out.json, warnings: out.warnings });
        } catch (error) {
            log.error('[PUT /payment-extras/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/payment-extras/:id — soft; refused while money is applied to it.
    app.delete('/api/v1/payment-extras/:id(\\d+)', async (req, res) => {
        try {
            if (!L.canEdit(req.userType)) return refuse(res, cannotEdit(req));
            const id = Number(req.params.id);
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const current = await rowById(conn, id, { lock: true });
                    if (!current) { await conn.rollback(); return { fail: notFound(id) }; }
                    const applied = (await loadAppliedByTarget(conn, 'extra', [id])).get(id) || 0;
                    const refusal = L.decideDelete({ applied });
                    if (refusal) { await conn.rollback(); return { fail: refusal }; }
                    const before = (await hydrate(conn, [current]))[0];
                    await conn.query(`UPDATE payment_extras SET deleted_at = NOW(), deleted_by_email = ? WHERE id = ?`, [req.userEmail || null, id]);
                    await recordAudit(conn, { entityType: 'payment_extra', entityId: id, action: 'delete', before, after: null, userEmail: req.userEmail });
                    await conn.commit();
                    return { ok: true };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.status(204).end();
        } catch (error) {
            log.error('[DELETE /payment-extras/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/payment-extras/:id/status  Body: { status: 'paid' | 'open', paidOn? }
    // Paid by hand — money that went out with no transfer recorded here — or
    // open again. One a recorded transfer settled is undone through that transfer.
    app.post('/api/v1/payment-extras/:id(\\d+)/status', async (req, res) => {
        try {
            if (!L.canEdit(req.userType)) return refuse(res, cannotEdit(req));
            const id = Number(req.params.id);
            const b = req.body || {};
            const want = String(b.status || '');
            let paidOn = null;
            if (b.paidOn != null && b.paidOn !== '') {
                if (typeof b.paidOn !== 'string' || !DATE_RE.test(b.paidOn)) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'paidOn must be YYYY-MM-DD.' });
                paidOn = b.paidOn;
            }
            await auditLogSchemaReady;
            const out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const current = await rowById(conn, id, { lock: true });
                    if (!current) { await conn.rollback(); return { fail: notFound(id) }; }
                    const d = L.decideStatus({ status: current.status, settledByPaymentId: current.settled_by_payment_id ?? null, want });
                    if (d.status) { await conn.rollback(); return { fail: d }; }
                    if (d.action === 'none') { await conn.commit(); return { json: await jsonById(conn, id) }; }
                    if (want === 'paid') {
                        await conn.query(`UPDATE payment_extras SET status = 'paid', paid_on = COALESCE(?, CURDATE()), settled_by_payment_id = NULL, updated_by_email = ? WHERE id = ?`, [paidOn, req.userEmail || null, id]);
                    } else {
                        await conn.query(`UPDATE payment_extras SET status = 'open', paid_on = NULL, updated_by_email = ? WHERE id = ?`, [req.userEmail || null, id]);
                    }
                    const json = await jsonById(conn, id);
                    await recordAudit(conn, {
                        entityType: 'payment_extra', entityId: id, action: 'status',
                        before: { status: current.status, paidOn: current.paid_on || null },
                        after: { status: json.status, paidOn: json.paidOn, via: 'hand' },
                        userEmail: req.userEmail,
                    });
                    await conn.commit();
                    return { json };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out.json);
        } catch (error) {
            log.error('[POST /payment-extras/:id/status]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPaymentExtraRoutes };
