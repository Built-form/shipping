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
const FL = require('../lib/forwarder-links');
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
        if ((v.ridesWith === 'balance' || v.ridesWith === 'shipment') && v.shipmentId != null) {
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

    // `link`: { forwarderId, supplierName } — the payee as resolved below.
    const writeValues = (v, t, link) => [
        link.supplierName, supplierKey(link.supplierName), v.currency, v.amount, v.kind, v.description, v.ridesWith,
        v.purchaseOrderId, t.shipmentId, t.shipmentReference, v.dueDate, v.sourceKind, v.sourceId, v.note, link.forwarderId,
    ];

    // ── Forwarder links (2026-11-20, src/lib/payment-extras.js decidePayeeLink) ──
    const LIVE_BOOKING = ['requested', 'confirmed'];
    const badForwarder = id => ({ status: 400, code: 'BAD_FIELD', error: `Forwarder ${id} is not on the Forwarders list.`, payload: { field: 'forwarderId' } });
    const forwarderById = async (conn, id) => {
        const [rows] = await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE id = ?', [id]);
        return rows[0] || null;
    };
    // The payee a forwarder payment names: { forwarderId, supplierName } or { fail }.
    // Only a forwarder payment carries a forwarder.
    const resolveLink = async (conn, current, v) => {
        if (v.ridesWith !== 'shipment') return { forwarderId: null, supplierName: v.supplierName };
        const d = L.decidePayeeLink({
            current: current ? { forwarderId: current.forwarder_id ?? null, supplierName: current.supplier_name, supplierKey: current.supplier_key } : null,
            next: { ...v, supplierKey: supplierKey(v.supplierName) },
        });
        if (d.action === 'keep') return { forwarderId: Number(current.forwarder_id), supplierName: current.supplier_name };
        if (d.action === 'unlink') return { forwarderId: null, supplierName: d.name };
        if (d.action === 'byId') {
            const f = await forwarderById(conn, d.id);
            if (!f || f.deleted_at) return { fail: badForwarder(d.id) };
            return { forwarderId: Number(f.id), supplierName: f.name };
        }
        // byName: linked when exactly one live forwarder carries that name; the name stays as sent.
        const [list] = await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE deleted_at IS NULL');
        return { forwarderId: FL.pairFromText(d.name, list).id, supplierName: d.name };
    };

    // A forwarder payment priced from a freight booking or one stock movement
    // (POST only): inside the caller's transaction. Discovers the ids without
    // locks, then locks booking -> request -> movement (the order every freight
    // route takes), and checks everything again under the locks.
    // { bookingId, movementId, forwarderId, forwarderName } or { fail }.
    const claimFreightSource = async (conn, v, t) => {
        const { kind, id } = v.freightSource;
        const stale = { fail: { status: 409, code: 'STALE', error: 'The booking changed while this was being added — read it again.' } };
        let requestId;
        let bookingId;
        if (kind === 'booking') {
            const [[b]] = await conn.query('SELECT id, request_id FROM freight_bookings WHERE id = ?', [id]);
            if (!b) return { fail: { status: 404, code: 'NOT_FOUND', error: `Freight booking ${id} not found.` } };
            bookingId = Number(b.id);
            requestId = Number(b.request_id);
        } else {
            const [[m]] = await conn.query('SELECT id, request_id FROM freight_stock_movements WHERE id = ?', [id]);
            if (!m) return { fail: { status: 404, code: 'NOT_FOUND', error: `Stock movement ${id} not found.` } };
            requestId = Number(m.request_id);
            const [[b]] = await conn.query(
                'SELECT id FROM freight_bookings WHERE request_id = ? AND status IN (?) ORDER BY id DESC LIMIT 1', [requestId, LIVE_BOOKING]
            );
            if (!b) return { fail: { status: 409, code: 'NOT_LIVE', error: 'That stock movement has no live booking: price it once a booking is requested or confirmed.' } };
            bookingId = Number(b.id);
        }
        const [[bk]] = await conn.query('SELECT id, request_id, status, shipment_id FROM freight_bookings WHERE id = ? FOR UPDATE', [bookingId]);
        const [[rq]] = await conn.query('SELECT id, forwarder_id FROM freight_quote_requests WHERE id = ? FOR UPDATE', [requestId]);
        let mv = null;
        if (kind === 'movement') {
            [[mv]] = await conn.query('SELECT id, request_id, deleted_at, price FROM freight_stock_movements WHERE id = ? FOR UPDATE', [id]);
        }
        if (!bk || !rq || Number(bk.request_id) !== requestId) return stale;
        if (!LIVE_BOOKING.includes(bk.status)) {
            return { fail: { status: 409, code: 'NOT_LIVE', error: `That booking is ${bk.status}: its price can no longer be added.` } };
        }
        if (kind === 'movement') {
            if (!mv || Number(mv.request_id) !== requestId) return stale;
            if (mv.deleted_at) return { fail: { status: 409, code: 'STALE', error: 'That stock movement was removed.' } };
            if (mv.price == null) return { fail: { status: 409, code: 'UNPRICED', error: 'That stock movement has no price yet.' } };
        }
        if (t.shipmentId == null || bk.shipment_id == null || Number(bk.shipment_id) !== Number(t.shipmentId)) {
            return { fail: { status: 422, code: 'SOURCE_MISMATCH', error: 'That booking is not on this container.' } };
        }
        const f = rq.forwarder_id != null ? await forwarderById(conn, rq.forwarder_id) : null;
        if (!f || f.deleted_at) {
            return { fail: { status: 409, code: 'FORWARDER_REMOVED', error: "The booking's forwarder was removed from the Forwarders list: restore it, or add the payment by hand." } };
        }
        return { bookingId: kind === 'booking' ? bookingId : null, movementId: kind === 'movement' ? Number(id) : null, forwarderId: Number(f.id), forwarderName: f.name };
    };
    const freightDuplicate = async (conn, e, v) => {
        if (!(e && e.code === 'ER_DUP_ENTRY' && /uq_payment_extras_freight_source/.test(e.message || ''))) return null;
        const key = `${v.freightSource.kind === 'booking' ? 'b' : 'm'}:${v.freightSource.id}`;
        const [[row]] = await conn.query('SELECT id FROM payment_extras WHERE freight_source_key = ?', [key]);
        const what = v.freightSource.kind === 'booking' ? 'this booking' : 'this stock movement';
        return { status: 409, code: 'ALREADY_ADDED', error: `That price from ${what} has already been added.`, payload: { extraId: row ? row.id : null } };
    };
    // InnoDB picked this transaction to break a deadlock: once more, then give up.
    const DEADLOCK = 1213;
    const withDeadlockRetry = async (fn) => {
        try { return await fn(); } catch (e) {
            if (e && e.errno === DEADLOCK) {
                try { return await fn(); } catch (e2) {
                    if (e2 && e2.errno === DEADLOCK) return { fail: { status: 503, code: 'TRY_AGAIN', error: 'Busy — try again.' } };
                    throw e2;
                }
            }
            throw e;
        }
    };

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
            const out = await withConnection(async (conn) => withDeadlockRetry(async () => {
                const t = await resolveTarget(conn, v);
                if (t.fail) return { fail: t.fail };
                await conn.beginTransaction();
                try {
                    const dup = await duplicateOf(conn, v);
                    if (dup) {
                        await conn.rollback();
                        return { fail: { status: 409, code: 'ALREADY_ADDED', error: 'That charge from this invoice has already been added.', payload: { extraId: dup } } };
                    }
                    let link;
                    let freight = { bookingId: null, movementId: null };
                    if (v.freightSource) {
                        // Priced from the Freight page: the payee is the booking's forwarder.
                        const f = await claimFreightSource(conn, v, t);
                        if (f.fail) { await conn.rollback(); return { fail: f.fail }; }
                        freight = f;
                        link = { forwarderId: f.forwarderId, supplierName: f.forwarderName };
                    } else {
                        link = await resolveLink(conn, null, v);
                        if (link.fail) { await conn.rollback(); return { fail: link.fail }; }
                    }
                    let ins;
                    try {
                        [ins] = await conn.query(
                            `INSERT INTO payment_extras (supplier_name, supplier_key, currency, amount, kind, description, rides_with,
                                                         purchase_order_id, shipment_id, shipment_reference, due_date, source_kind, source_id, note,
                                                         forwarder_id, freight_booking_id, freight_movement_id, created_by_email)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                            [...writeValues(v, t, link), freight.bookingId, freight.movementId, req.userEmail || null]
                        );
                    } catch (e) {
                        // Two adds of one booking price at once: the unique key lets one in.
                        const taken = v.freightSource ? await freightDuplicate(conn, e, v) : null;
                        if (taken) { await conn.rollback(); return { fail: taken }; }
                        throw e;
                    }
                    const json = await jsonById(conn, ins.insertId);
                    await recordAudit(conn, { entityType: 'payment_extra', entityId: ins.insertId, action: 'create', before: null, after: json, userEmail: req.userEmail });
                    await conn.commit();
                    return { json, warnings: t.warnings };
                } catch (e) {
                    await conn.rollback().catch(() => {});
                    throw e;
                }
            }));
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
                    // Priced from a freight booking or stock movement (2026-11-20): what
                    // it came from, its container, its forwarder and its kind stay as
                    // made. An older client sends back neither freightSource nor the
                    // forwarder, and source_kind for nothing: both are written as stored.
                    const fromFreight = current.freight_booking_id != null || current.freight_movement_id != null;
                    if (!fromFreight && v.freightSource) {
                        await conn.rollback();
                        return { fail: { status: 422, code: 'SOURCE_LOCKED', error: 'Where a payment came from is set when it is added.' } };
                    }
                    let w = v;
                    let target = t;
                    let link;
                    if (fromFreight) {
                        const locked = L.decideFreightLock({
                            current: {
                                freightBookingId: current.freight_booking_id ?? null, freightMovementId: current.freight_movement_id ?? null,
                                ridesWith: current.rides_with, shipmentId: current.shipment_id ?? null, shipmentReference: current.shipment_reference,
                                forwarderId: current.forwarder_id ?? null, supplierKey: current.supplier_key,
                            },
                            next: { ...v, supplierKey: supplierKey(v.supplierName) },
                        });
                        if (locked) { await conn.rollback(); return { fail: locked }; }
                        w = { ...v, ridesWith: current.rides_with, purchaseOrderId: null, sourceKind: null, sourceId: null };
                        target = { ...t, shipmentId: current.shipment_id ?? null, shipmentReference: current.shipment_reference };
                        link = { forwarderId: current.forwarder_id ?? null, supplierName: current.supplier_name };
                    } else {
                        link = await resolveLink(conn, current, v);
                        if (link.fail) { await conn.rollback(); return { fail: link.fail }; }
                    }
                    const applied = (await loadAppliedByTarget(conn, 'extra', [id])).get(id) || 0;
                    const refusal = L.decideEdit({
                        current: { amount: Number(current.amount), currency: current.currency, supplierKey: current.supplier_key, forwarderId: current.forwarder_id ?? null },
                        next: { amount: w.amount, currency: w.currency, supplierKey: supplierKey(link.supplierName), forwarderId: link.forwarderId },
                        applied,
                    });
                    if (refusal) { await conn.rollback(); return { fail: refusal }; }
                    const dup = await duplicateOf(conn, w, { excludeId: id });
                    if (dup) {
                        await conn.rollback();
                        return { fail: { status: 409, code: 'ALREADY_ADDED', error: 'That charge from this invoice has already been added.', payload: { extraId: dup } } };
                    }
                    const before = (await hydrate(conn, [current]))[0];
                    await conn.query(
                        `UPDATE payment_extras
                            SET supplier_name = ?, supplier_key = ?, currency = ?, amount = ?, kind = ?, description = ?, rides_with = ?,
                                purchase_order_id = ?, shipment_id = ?, shipment_reference = ?, due_date = ?, source_kind = ?, source_id = ?, note = ?,
                                forwarder_id = ?, updated_by_email = ?
                          WHERE id = ?`,
                        [...writeValues(w, target, link), req.userEmail || null, id]
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
