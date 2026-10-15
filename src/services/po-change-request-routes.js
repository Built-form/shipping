'use strict';

// /api/v1/po-change-requests — supplier changes staged by the supplier portal
// (jportal) in jportal.po_change_requests, shown with the line's live (old)
// value beside the requested (new) one, and applied to orders here: one at a
// time, or every pending one from a supplier at once. The rules are in
// src/lib/po-change-requests.js; the frontend guide is
// docs/po-change-requests-frontend.md.
//
// jportal is another schema on the same MySQL server (JPORTAL_DB_SCHEMA,
// default `jportal`). This app's login needs SELECT and UPDATE on
// <jportal>.po_change_requests; nothing here creates or deletes its rows, and
// no DDL runs against it.
//
// Lock order, in every transaction: the change-request rows, then the orders
// rows (ascending id). The bulk apply locks its requests through
// uniq_pending_order in ascending pending_order_id (= order_id), the index and
// order jportal's submit locks them in; locking by supplier_id instead would
// scan idx_supplier_status in id order and deadlock against a submit. Its
// candidates are found by a plain read BEFORE the transaction opens, so the
// transaction's first read is a locking one and its snapshot reads (the audit
// before/after) are taken after the locks. A deadlock is retried once, then
// answered 409 CONFLICT.
//
// Audit rows are written with recordAuditStrict: inside these transactions a
// failed audit insert must fail the apply. recordAudit swallows errors, and
// after a deadlock InnoDB has already rolled the transaction back, so the
// statements after it would run in autocommit and mark the request applied
// with the order change lost.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const L = require('../lib/po-change-requests');
const { diffSnapshots } = require('../lib/audit');

const LIST_LIMIT = 500;
const BULK_LIMIT = 500;

const isDeadlock = err => Boolean(err) && (err.code === 'ER_LOCK_DEADLOCK' || err.errno === 1213);
const isLockTimeout = err => Boolean(err) && (err.code === 'ER_LOCK_WAIT_TIMEOUT' || err.errno === 1205);
const CONFLICT = { status: 409, code: 'CONFLICT', error: 'Someone changed these requests at the same moment; reload and try again.' };

// recordAudit's diff and no-op skip, but errors propagate.
async function recordAuditStrict(conn, { entityType, entityId, action, before, after, userEmail }) {
    const diffed = diffSnapshots(before, after);
    if (action === 'update' && diffed.before && Object.keys(diffed.before).length === 0) return;
    await conn.query(
        `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [
            entityType, entityId, action,
            diffed.before ? JSON.stringify(diffed.before) : null,
            diffed.after ? JSON.stringify(diffed.after) : null,
            userEmail || null,
        ]
    );
}

function registerPoChangeRequestRoutes(app, deps) {
    const {
        withConnection, ready, ORDER_SELECT, rowToOrder, parseDates, setDateKey,
        shipmentSync, membershipKey, patchShipmentIds, log,
    } = deps;

    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.extra || {}) });

    // Every column the API returns, with the live line joined in. Read on every
    // call: an invalid JPORTAL_DB_SCHEMA is a 500 on these routes, not a failed
    // import that would take the whole Lambda down.
    const selectSql = () => {
        const jp = L.jportalSchema();
        return `SELECT r.id, r.supplier_id, r.supplier_name, r.purchase_order_id, r.po_number, r.order_id,
                       r.jf_code, r.product_name, r.base_json, r.requested_json, r.status, r.row_version,
                       r.submitted_by_email, r.decided_by, r.decided_note,
                       DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso,
                       DATE_FORMAT(r.decided_at, '%Y-%m-%dT%H:%i:%sZ') AS decided_at_iso,
                       DATE_FORMAT(r.applied_at, '%Y-%m-%dT%H:%i:%sZ') AS applied_at_iso,
                       o.id AS live_id, o.purchase_order_id AS live_purchase_order_id, o.status AS live_status,
                       o.estimated_ready_date AS live_estimated_ready_date, o.actual_ready_date AS live_actual_ready_date
                  FROM ${jp}.po_change_requests r
                  LEFT JOIN orders o ON o.id = r.order_id AND o.deleted_at IS NULL`;
    };

    const readByIds = async (conn, ids) => {
        if (!ids.length) return [];
        const [rows] = await conn.query(`${selectSql()} WHERE r.id IN (${ids.map(() => '?').join(', ')}) ORDER BY r.order_id, r.id`, ids);
        return rows.map(L.requestToJson);
    };

    /**
     * Apply locked, pending change-request rows inside the caller's transaction.
     * Rows with blockers are skipped and reported, nothing written for them.
     * Returns { applied: [{ id, orderId }], skipped: [{ id, orderId, blockers }], orders }.
     */
    const applyLocked = async (conn, rows, { userEmail, note }) => {
        const jp = L.jportalSchema();
        const orderIds = [...new Set(rows.map(r => Number(r.order_id)))].sort((a, b) => a - b);
        const liveById = new Map();
        if (orderIds.length) {
            const [live] = await conn.query(
                `SELECT id, purchase_order_id, status, estimated_ready_date, actual_ready_date, dates
                   FROM orders WHERE id IN (${orderIds.map(() => '?').join(', ')}) AND deleted_at IS NULL
                  ORDER BY id FOR UPDATE`,
                orderIds
            );
            for (const row of live) liveById.set(Number(row.id), row);
        }

        const applied = [];
        const skipped = [];
        const touched = [];
        for (const row of rows) {
            const orderId = Number(row.order_id);
            const liveRow = liveById.get(orderId) || null;
            const requested = L.parseJsonObject(row.requested_json);
            const plan = L.planApply(requested, L.lineFromRow(liveRow), row.purchase_order_id);
            if (plan.blockers.length) {
                skipped.push({
                    id: Number(row.id), orderId,
                    blockers: plan.blockers.map(code => ({ code, message: L.BLOCKERS[code] })),
                });
                continue;
            }

            const sets = [];
            const vals = [];
            for (const [col, value] of Object.entries(plan.cols)) {
                sets.push(`${col} = ?`);
                vals.push(value);
            }
            if (plan.moveToQc) {
                // Stamps the milestone into orders.dates exactly as
                // PATCH /orders/:id/status does.
                const dates = setDateKey(parseDates(liveRow.dates), L.READY_FOR_QC_STATUS);
                sets.push('status = ?', 'dates = ?');
                vals.push(L.READY_FOR_QC_STATUS, JSON.stringify(dates));
            }

            if (sets.length) {
                const [beforeRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [orderId]);
                const beforeOrder = rowToOrder(beforeRows[0]);
                await conn.query(`UPDATE orders SET ${sets.join(', ')} WHERE id = ?`, [...vals, orderId]);
                const [afterRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [orderId]);
                const afterOrder = rowToOrder(afterRows[0]);
                await recordAuditStrict(conn, {
                    entityType: 'order', entityId: orderId, action: 'update',
                    before: beforeOrder, after: afterOrder, userEmail,
                });
                // A line can have only one pending row, but keep the last copy if not.
                const at = touched.findIndex(o => o.id === afterOrder.id);
                if (at >= 0) touched[at] = afterOrder; else touched.push(afterOrder);
                // Later rows in this batch see this write.
                for (const [col, value] of Object.entries(plan.cols)) liveRow[col] = value;
                if (plan.moveToQc) liveRow.status = L.READY_FOR_QC_STATUS;
            }

            const [done] = await conn.query(
                `UPDATE ${jp}.po_change_requests
                    SET status = 'applied', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                        applied_at = UTC_TIMESTAMP(), row_version = row_version + 1
                  WHERE id = ? AND status = 'pending'`,
                [userEmail || 'unknown', note, row.id]
            );
            // The row is locked, so this cannot miss; if it does, nothing is committed.
            if (done.affectedRows !== 1) throw new Error(`po_change_requests ${row.id} was not pending at apply`);
            await recordAuditStrict(conn, {
                entityType: 'po_change_request', entityId: Number(row.id), action: 'apply',
                before: { status: 'pending' },
                after: { status: 'applied', orderId, changed: plan.after, note },
                userEmail,
            });
            applied.push({ id: Number(row.id), orderId });
        }
        return { applied, skipped, orders: touched };
    };

    // Runs prepare(conn) outside any transaction (plain reads only), then
    // fn(conn, prepared) in a transaction, then the shipment-membership hook for
    // the orders it changed, as the other order-writing routes do. The whole
    // thing is retried once on a deadlock; a second deadlock or a lock-wait
    // timeout is returned as { fail: CONFLICT }.
    const inTransaction = async (site, fn, prepare = async () => null) => {
        const once = () => withConnection(async (conn) => {
            const prepared = await prepare(conn);
            await conn.beginTransaction();
            try {
                const out = await fn(conn, prepared);
                let linked = null;
                if (!out.fail && out.orders && out.orders.length) {
                    const ids = out.orders.map(o => o.id);
                    linked = await shipmentSync.shadow(conn, {
                        site, inTx: true, ...membershipKey(out.orders),
                    }, c => shipmentSync.syncOrderMembership(c, ids, { userEmail: out.userEmail }));
                }
                await conn.commit();
                return { ...out, linked };
            } catch (e) {
                try {
                    await conn.rollback();
                } catch (rollbackError) {
                    log.warn(`[${site}] rollback failed`, rollbackError.message);
                }
                throw e;
            }
        });
        try {
            return await once();
        } catch (e) {
            if (isLockTimeout(e)) return { fail: CONFLICT };
            if (!isDeadlock(e)) throw e;
        }
        try {
            return await once();
        } catch (e) {
            if (isDeadlock(e) || isLockTimeout(e)) return { fail: CONFLICT };
            throw e;
        }
    };

    // GET /api/v1/po-change-requests?status=pending&supplierId=&purchaseOrderId=
    // status defaults to pending; `all` lists every status. Newest first for
    // decided rows, oldest first for pending (the order they were sent in).
    app.get('/api/v1/po-change-requests', async (req, res) => {
        try {
            const q = req.query || {};
            const status = q.status ? String(q.status) : 'pending';
            if (status !== 'all' && !L.REQUEST_STATUSES.has(status)) {
                return refuse(res, { status: 400, code: 'BAD_FIELD', error: `status must be one of ${[...L.REQUEST_STATUSES].join(', ')} or all.` });
            }
            const where = [];
            const params = [];
            if (status !== 'all') { where.push('r.status = ?'); params.push(status); }
            for (const [key, col] of [['supplierId', 'r.supplier_id'], ['purchaseOrderId', 'r.purchase_order_id'], ['orderId', 'r.order_id']]) {
                if (q[key] === undefined || q[key] === '') continue;
                const id = L.positiveInt(q[key]);
                if (!id) return refuse(res, { status: 400, code: 'BAD_FIELD', error: `${key} must be a positive integer.` });
                where.push(`${col} = ?`);
                params.push(id);
            }
            const order = status === 'pending'
                ? 'r.supplier_name, r.purchase_order_id, r.submitted_at, r.id'
                : 'r.updated_at DESC, r.id DESC';
            const data = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `${selectSql()} ${where.length ? `WHERE ${where.join(' AND ')}` : ''} ORDER BY ${order} LIMIT ${LIST_LIMIT}`,
                    params
                );
                return rows.map(L.requestToJson);
            });
            res.json({ data });
        } catch (error) {
            log.error('[GET /po-change-requests]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/po-change-requests/suppliers — every supplier with pending
    // requests: how many, and how many can be applied right now.
    app.get('/api/v1/po-change-requests/suppliers', async (req, res) => {
        try {
            const rows = await withConnection(async (conn) => {
                const [r] = await conn.query(`${selectSql()} WHERE r.status = 'pending' ORDER BY r.supplier_name, r.id`);
                return r.map(L.requestToJson);
            });
            const bySupplier = new Map();
            for (const r of rows) {
                const s = bySupplier.get(r.supplierId) || {
                    supplierId: r.supplierId, supplierName: r.supplierName, pending: 0, applicable: 0,
                    purchaseOrders: new Set(), oldestSubmittedAt: r.submittedAt,
                };
                s.pending += 1;
                if (r.canApply) s.applicable += 1;
                s.purchaseOrders.add(r.purchaseOrderId);
                if (r.submittedAt && (!s.oldestSubmittedAt || r.submittedAt < s.oldestSubmittedAt)) s.oldestSubmittedAt = r.submittedAt;
                bySupplier.set(r.supplierId, s);
            }
            const data = [...bySupplier.values()].map(s => ({ ...s, purchaseOrders: s.purchaseOrders.size }));
            res.json({ data });
        } catch (error) {
            log.error('[GET /po-change-requests/suppliers]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/po-change-requests/:id/apply — body { rowVersion?, note? }.
    // rowVersion, when sent, must match the row the user was looking at.
    // 200 { request, order } · 404 NOT_FOUND · 409 NOT_PENDING | STALE · 422 NOT_APPLICABLE
    app.post('/api/v1/po-change-requests/:id(\\d+)/apply', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const b = req.body || {};
            const note = L.parseNote(b.note);
            if (note.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: note.error });
            let rowVersion = null;
            if (b.rowVersion !== undefined && b.rowVersion !== null) {
                rowVersion = Number(b.rowVersion);
                if (!Number.isInteger(rowVersion) || rowVersion < 0) {
                    return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'rowVersion must be a non-negative integer.' });
                }
            }
            await ready();

            const out = await inTransaction('POST /po-change-requests/:id/apply', async (conn) => {
                const [rows] = await conn.query(
                    `SELECT * FROM ${L.jportalSchema()}.po_change_requests WHERE id = ? FOR UPDATE`, [id]
                );
                const row = rows[0];
                if (!row) return { fail: { status: 404, code: 'NOT_FOUND', error: `Change request ${id} not found.` } };
                if (row.status !== 'pending') {
                    return { fail: { status: 409, code: 'NOT_PENDING', error: `Change request ${id} is already ${row.status}.` } };
                }
                if (rowVersion !== null && Number(row.row_version) !== rowVersion) {
                    return { fail: { status: 409, code: 'STALE', error: 'The supplier changed this request since it was loaded; reload and check it again.' } };
                }
                const result = await applyLocked(conn, [row], { userEmail: req.userEmail, note: note.value });
                if (result.skipped.length) {
                    return { fail: {
                        status: 422, code: 'NOT_APPLICABLE', error: 'This change cannot be applied as it stands.',
                        extra: { blockers: result.skipped[0].blockers },
                    } };
                }
                return { ...result, userEmail: req.userEmail };
            });
            if (out.fail) return refuse(res, out.fail);

            const [request] = await withConnection(conn => readByIds(conn, [id]));
            patchShipmentIds(out.orders, out.linked);
            res.json({ request, order: out.orders[0] || null });
        } catch (error) {
            log.error('[POST /po-change-requests/:id/apply]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/po-change-requests/:id/reject — body { rowVersion?, note? }.
    // Drops the request: it leaves the pending list and the order is not
    // touched. The row is kept, marked `rejected` (jportal's audit_logs point at
    // it); the supplier can send a new request for the line.
    // 200 { request } · 404 NOT_FOUND · 409 NOT_PENDING | STALE | CONFLICT
    app.post('/api/v1/po-change-requests/:id(\\d+)/reject', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const b = req.body || {};
            const note = L.parseNote(b.note);
            if (note.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: note.error });
            let rowVersion = null;
            if (b.rowVersion !== undefined && b.rowVersion !== null) {
                rowVersion = Number(b.rowVersion);
                if (!Number.isInteger(rowVersion) || rowVersion < 0) {
                    return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'rowVersion must be a non-negative integer.' });
                }
            }
            await ready();

            const out = await inTransaction('POST /po-change-requests/:id/reject', async (conn) => {
                const jp = L.jportalSchema();
                const [rows] = await conn.query(`SELECT * FROM ${jp}.po_change_requests WHERE id = ? FOR UPDATE`, [id]);
                const row = rows[0];
                if (!row) return { fail: { status: 404, code: 'NOT_FOUND', error: `Change request ${id} not found.` } };
                if (row.status !== 'pending') {
                    return { fail: { status: 409, code: 'NOT_PENDING', error: `Change request ${id} is already ${row.status}.` } };
                }
                if (rowVersion !== null && Number(row.row_version) !== rowVersion) {
                    return { fail: { status: 409, code: 'STALE', error: 'The supplier changed this request since it was loaded; reload and check it again.' } };
                }
                await conn.query(
                    `UPDATE ${jp}.po_change_requests
                        SET status = 'rejected', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            row_version = row_version + 1
                      WHERE id = ? AND status = 'pending'`,
                    [req.userEmail || 'unknown', note.value, id]
                );
                await recordAuditStrict(conn, {
                    entityType: 'po_change_request', entityId: id, action: 'reject',
                    before: { status: 'pending' },
                    after: { status: 'rejected', orderId: Number(row.order_id), requested: L.parseJsonObject(row.requested_json), note: note.value },
                    userEmail: req.userEmail,
                });
                return { orders: [] };
            });
            if (out.fail) return refuse(res, out.fail);

            const [request] = await withConnection(conn => readByIds(conn, [id]));
            res.json({ request });
        } catch (error) {
            log.error('[POST /po-change-requests/:id/reject]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/po-change-requests/apply-supplier — body { supplierId, ids?, note? }.
    // Applies every pending request from that supplier, or only `ids` (the
    // ones the user was shown; recommended, so a request that arrives while
    // they look is not applied unseen). One transaction. Rows that cannot be
    // applied are skipped and listed; the rest are applied.
    // 200 { applied: Request[], skipped: [{ id, orderId, blockers }], orders }
    app.post('/api/v1/po-change-requests/apply-supplier', async (req, res) => {
        try {
            const b = req.body || {};
            const supplierId = L.positiveInt(b.supplierId);
            if (!supplierId) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'supplierId must be a positive integer.' });
            const note = L.parseNote(b.note);
            if (note.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: note.error });
            let ids = null;
            if (b.ids !== undefined && b.ids !== null) {
                if (!Array.isArray(b.ids) || b.ids.length === 0) {
                    return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'ids must be a non-empty list when sent.' });
                }
                ids = [...new Set(b.ids.map(L.positiveInt))];
                if (ids.includes(null)) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'ids must be positive integers.' });
                if (ids.length > BULK_LIMIT) return refuse(res, { status: 400, code: 'TOO_MANY', error: `At most ${BULK_LIMIT} requests at once.` });
            }
            await ready();

            // Before the transaction: which lines have a pending request from
            // this supplier (unlocked). Inside it: lock exactly those through
            // uniq_pending_order, ascending, and re-check what was locked.
            const findCandidates = async (conn) => {
                const params = [supplierId];
                let idFilter = '';
                if (ids) {
                    idFilter = ` AND id IN (${ids.map(() => '?').join(', ')})`;
                    params.push(...ids);
                }
                const [rows] = await conn.query(
                    `SELECT order_id FROM ${L.jportalSchema()}.po_change_requests
                      WHERE supplier_id = ? AND status = 'pending'${idFilter}
                      ORDER BY order_id LIMIT ${BULK_LIMIT + 1}`,
                    params
                );
                return rows.map(r => Number(r.order_id));
            };

            const out = await inTransaction('POST /po-change-requests/apply-supplier', async (conn, orderIds) => {
                if (orderIds.length > BULK_LIMIT) {
                    return { fail: { status: 400, code: 'TOO_MANY', error: `More than ${BULK_LIMIT} pending requests; send ids to apply them in parts.` } };
                }
                let rows = [];
                if (orderIds.length) {
                    const [locked] = await conn.query(
                        `SELECT * FROM ${L.jportalSchema()}.po_change_requests
                          WHERE pending_order_id IN (${orderIds.map(() => '?').join(', ')})
                          ORDER BY pending_order_id FOR UPDATE`,
                        orderIds
                    );
                    // A line's pending row may have been superseded by a new one
                    // (new id) since the candidate read: with `ids`, only the ids
                    // the user saw are applied; a new one is reported not_pending.
                    const wanted = ids ? new Set(ids) : null;
                    rows = locked.filter(r => Number(r.supplier_id) === supplierId && r.status === 'pending'
                        && (!wanted || wanted.has(Number(r.id))));
                }
                const result = await applyLocked(conn, rows, { userEmail: req.userEmail, note: note.value });
                // Asked-for ids that were not pending for this supplier.
                if (ids) {
                    const found = new Set(rows.map(r => Number(r.id)));
                    for (const id of ids) {
                        if (!found.has(id)) {
                            result.skipped.push({ id, orderId: null, blockers: [{ code: 'not_pending', message: 'Not a pending request from this supplier.' }] });
                        }
                    }
                }
                return { ...result, userEmail: req.userEmail };
            }, findCandidates);
            if (out.fail) return refuse(res, out.fail);

            const applied = await withConnection(conn => readByIds(conn, out.applied.map(a => a.id)));
            patchShipmentIds(out.orders, out.linked);
            res.json({ applied, skipped: out.skipped, orders: out.orders });
        } catch (error) {
            log.error('[POST /po-change-requests/apply-supplier]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPoChangeRequestRoutes };
