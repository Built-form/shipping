'use strict';

// Order receiving + receivable-order lookup, shared by:
//   • the authed API  — POST /api/v1/orders/:id/receive   (src/handlers/orders.js)
//   • the public scan  — POST /api/v1/scan/receive|lookup  (src/handlers/carton-scan.js)
//
// The transaction logic (idempotency replay, FOR UPDATE lock, capacity check,
// Mintsoft ASN create/confirm/receive, receipt persistence, lot/expiry
// override, status transition + audit) was lifted verbatim out of the orders
// handler so there is ONE Mintsoft receiving path. Callers own the connection;
// this module owns the transaction on it (begins/commits/rolls back) but never
// releases. Domain errors are thrown as `ReceiveError` carrying an HTTP status
// + payload so each handler maps them to a response identically.

const { getProductsByJfCode, createAsn, receiveAsnItems, quarantineStock } = require('./mintsoft');
const { norm, normLot } = require('./qc-report-check');
const { ORDER_SELECT, rowToOrder, receiptToJson, parseDates, normalizeExpiry } = require('../lib/order-shape');
const { recordAudit } = require('../lib/audit');
const log = require('../lib/logger');

// Typed domain error. `status` is the HTTP code the handler should return;
// `payload` is merged into the JSON body alongside `{ error: message }`.
class ReceiveError extends Error {
    constructor(code, status, message, payload = null) {
        super(message);
        this.name = 'ReceiveError';
        this.code = code;
        this.status = status;
        this.payload = payload;
    }
}

// Validate + normalise the request inputs. Throws ReceiveError(400) on bad
// input. Returns the cleaned values used by receiveOrderStock. Centralised here
// so the authed API and the scan API enforce identical rules.
function parseReceiveInput({ quantity, locationId, warehouseId, goodsInType, idempotencyKey, lotNumber, expiryDate, quarantine }) {
    if (!locationId) throw new ReceiveError('INVALID_LOCATION', 400, 'locationId is required.');
    if (!warehouseId) throw new ReceiveError('INVALID_WAREHOUSE', 400, 'warehouseId is required.');
    if (!quantity || Number(quantity) <= 0) {
        throw new ReceiveError('INVALID_QUANTITY', 400, 'quantity must be a positive number.');
    }
    const qty = Number(quantity);

    // Optional client-supplied batch + expiry. If sent, they overwrite the
    // order row and are forwarded to Mintsoft; otherwise the existing values
    // on the order are used.
    let suppliedLot = null;
    if (lotNumber !== undefined && lotNumber !== null && String(lotNumber).trim() !== '') {
        suppliedLot = String(lotNumber).trim();
    }
    let suppliedExpiry = null;
    if (expiryDate !== undefined && expiryDate !== null && expiryDate !== '') {
        const normalized = normalizeExpiry(expiryDate);
        if (normalized === false) {
            throw new ReceiveError('INVALID_EXPIRY', 400, 'expiryDate is not a valid date.');
        }
        suppliedExpiry = normalized;
    }

    // GoodsInType: 0=TwentyFtContainer, 1=FortyFtContainer, 2=Pallet, 3=Carton,
    // 4=FortyFtContainerHC, 5=FortyFiveFtContainer, 6=FortyFiveFtContainerHC
    let goodsInTypeId = 3;
    if (goodsInType !== undefined && goodsInType !== null && goodsInType !== '') {
        goodsInTypeId = Number(goodsInType);
        if (!Number.isInteger(goodsInTypeId) || goodsInTypeId < 0 || goodsInTypeId > 6) {
            throw new ReceiveError('INVALID_GOODS_IN_TYPE', 400, 'goodsInType must be an integer between 0 and 6.');
        }
    }

    let idemKey = null;
    if (idempotencyKey !== undefined && idempotencyKey !== null && idempotencyKey !== '') {
        if (typeof idempotencyKey !== 'string' || idempotencyKey.length > 64) {
            throw new ReceiveError('INVALID_IDEMPOTENCY_KEY', 400, 'idempotencyKey must be a string of ≤64 characters.');
        }
        idemKey = idempotencyKey;
    }

    // Optional: book the units straight into quarantine instead of leaving them
    // as normal sellable stock. Strict boolean — a typo'd string must not
    // silently quarantine a receipt, so only true/false (and the string forms a
    // form post sends) are accepted.
    let quarantineFlag = false;
    if (quarantine !== undefined && quarantine !== null && quarantine !== '') {
        if (quarantine === true || quarantine === 'true' || quarantine === 1 || quarantine === '1') {
            quarantineFlag = true;
        } else if (!(quarantine === false || quarantine === 'false' || quarantine === 0 || quarantine === '0')) {
            throw new ReceiveError('INVALID_QUARANTINE', 400, 'quarantine must be a boolean.');
        }
    }

    return { qty, suppliedLot, suppliedExpiry, goodsInTypeId, idemKey, quarantineFlag };
}

// Receives a portion of an order into a Mintsoft location. Mirrors the original
// POST /api/v1/orders/:id/receive handler exactly:
//   • Each call creates one ASN, ships it into one Location, appends a row to
//     order_receipts. Status only flips to RECEIVED/PARTIALLY_RECEIVED when the
//     running sum of receipts settles the order. On Mintsoft failure nothing is
//     persisted.
//   • Concurrency: the orders row is locked FOR UPDATE for the duration, so two
//     concurrent receives can't both pass the "remaining" check and over-receive.
//   • Idempotency: if a receipt with the same (order_id, idempotency_key) already
//     exists we skip Mintsoft entirely and return the prior result.
//
// Returns { order, asnId, receipts, idempotent, jfCode, asin }. `jfCode`/`asin`
// are for the caller's post-commit stock-snapshot refresh; on an idempotent
// replay `jfCode` is null (no snapshot needed), matching the original.
async function receiveOrderStock(conn, params) {
    const { orderId, actorEmail } = params;
    const { qty, suppliedLot, suppliedExpiry, goodsInTypeId, idemKey, quarantineFlag } = parseReceiveInput(params);
    const id = orderId;

    let committed = false;
    let asn = null;
    let jfCode = null;
    let orderRow = null;
    // Set by the post-put-away quarantine movement; surfaced on the response so
    // the caller can tell "quarantined" from "booked in as normal stock".
    let quarantineApplied = false;
    let quarantineError = null;
    try {
        await conn.beginTransaction();

        // Idempotent replay: if we've already processed this (order, key),
        // return the prior state without a second Mintsoft call.
        if (idemKey) {
            const [dup] = await conn.query(
                `SELECT * FROM order_receipts WHERE order_id = ? AND idempotency_key = ? LIMIT 1`,
                [id, idemKey]
            );
            if (dup.length) {
                const [orderRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
                const [allReceipts] = await conn.query(
                    'SELECT * FROM order_receipts WHERE order_id = ? ORDER BY received_at DESC',
                    [id]
                );
                await conn.commit();
                committed = true;
                return {
                    order: rowToOrder(orderRows[0]),
                    asnId: dup[0].asn_id,
                    receipts: allReceipts.map(receiptToJson),
                    idempotent: true,
                    jfCode: null,
                    asin: null,
                };
            }
        }

        // Lock the order row so concurrent receives serialize.
        const [lockRows] = await conn.query(
            `SELECT * FROM orders WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
            [id]
        );
        if (!lockRows.length) {
            await conn.rollback();
            committed = true;
            throw new ReceiveError('ORDER_NOT_FOUND', 404, `Order ${id} not found.`);
        }
        orderRow = lockRows[0];

        // Live sums under the same lock — never trust a cached counter.
        // `received` = physically checked-in (Mintsoft); `settled` adds in
        // not_received shortfalls so remaining capacity is bounded by the
        // total reconciled qty, not just received.
        const [sumRows] = await conn.query(
            `SELECT
                COALESCE(SUM(CASE WHEN type = 'received' THEN quantity END), 0) AS received,
                COALESCE(SUM(CASE WHEN type = 'not_received' THEN quantity END), 0) AS not_received,
                COALESCE(SUM(quantity), 0) AS settled
             FROM order_receipts WHERE order_id = ?`,
            [id]
        );
        const receivedQty = Number(sumRows[0].received);
        const notReceivedQty = Number(sumRows[0].not_received);
        const settledQty = Number(sumRows[0].settled);
        const orderQty = Number(orderRow.quantity);
        const remaining = orderQty - settledQty;
        const beforeOrder = rowToOrder({
            ...orderRow,
            received_quantity: receivedQty,
            not_received_quantity: notReceivedQty,
        });

        if (remaining <= 0) {
            const [orderRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            await conn.rollback();
            committed = true;
            throw new ReceiveError('ALREADY_RECEIVED', 409, 'Order is already fully received.', {
                order: rowToOrder(orderRows[0]),
            });
        }
        if (qty > remaining) {
            await conn.rollback();
            committed = true;
            throw new ReceiveError('QUANTITY_EXCEEDS_REMAINING', 400, `quantity (${qty}) exceeds remaining (${remaining}).`);
        }
        if (!orderRow.jf_code) {
            await conn.rollback();
            committed = true;
            throw new ReceiveError('NO_JF_CODE', 400, 'Order has no jf_code; cannot resolve Mintsoft product.');
        }
        jfCode = orderRow.jf_code;

        // BatchNo + ExpiryDate sent to Mintsoft. Client-supplied values win and
        // are also written back to the order; otherwise fall back to whatever's
        // already on the order row. Refuse if neither source has a value so
        // stock never lands without batch/expiry.
        const existingLot = (orderRow.lot_number ?? '').toString().trim();
        const existingExpiry = normalizeExpiry(orderRow.exp_date) || null;
        const orderLot = suppliedLot ?? existingLot;
        const orderExpiry = suppliedExpiry ?? existingExpiry;
        const missingFields = [];
        if (!orderLot) missingFields.push('lot_number');
        if (!orderExpiry) missingFields.push('exp_date');
        if (missingFields.length) {
            await conn.rollback();
            committed = true;
            throw new ReceiveError(
                'MISSING_LOT_EXPIRY', 400,
                `Order is missing ${missingFields.join(' and ')}; set these on the order before receiving into Mintsoft.`,
                { missing: missingFields }
            );
        }
        const lotChanged = suppliedLot !== null && suppliedLot !== existingLot;
        const expiryChanged = suppliedExpiry !== null && suppliedExpiry !== existingExpiry;

        // Resolve Mintsoft product — exact SKU match only.
        // /Product/Search is a substring match so 'HW1122' returns both
        // 'HW1122' and any variant ('HW1122_QC', 'HW1122_READY', ...).
        // Receive must target the base SKU; otherwise stock lands on the wrong
        // product (observed: HW1122 receipts allocated to HW1122_QC).
        const products = await getProductsByJfCode(jfCode);
        const exact = products.find(p => p.sku === jfCode);
        if (!exact) {
            await conn.rollback();
            committed = true;
            throw new ReceiveError('MINTSOFT_PRODUCT_NOT_FOUND', 404, `No Mintsoft product with exact SKU "${jfCode}" found.`, {
                candidates: products.map(p => p.sku),
            });
        }
        const { productId, sku } = exact;

        // Mintsoft side effects (still under lock so a concurrent retry waits)
        let asnItemId;
        try {
            const today = new Date().toISOString().slice(0, 10);
            const poRefParts = [orderRow.po_number, orderRow.asin, jfCode, today, `r${Date.now()}`].filter(Boolean);
            const poReference = poRefParts.join('-');

            asn = await createAsn({
                warehouseId: params.warehouseId,
                poReference,
                supplier: orderRow.supplier || '',
                quantity: qty,
                items: [{ productId, sku, quantity: qty }],
                goodsInType: goodsInTypeId,
            });

            const asnItem = (asn.Items || [])[0];
            if (!asn.ID || !asnItem?.ID) {
                throw new Error('Mintsoft ASN response missing ID or Items[0].ID');
            }
            asnItemId = asnItem.ID;

            await receiveAsnItems(asn.ID, [{
                asnItemId,
                productId,
                quantity: qty,
                locationId: params.locationId,
                batchNo: orderLot,
                expiryDate: orderExpiry,
            }]);
        } catch (err) {
            await conn.rollback();
            committed = true;
            log.error('[order-receive] Mintsoft call failed', err?.response?.data || err);
            throw new ReceiveError('MINTSOFT_CALL_FAILED', 502, 'Mintsoft request failed; please retry.', {
                details: err?.response?.data || err.message,
            });
        }

        // Optional quarantine (POST /Warehouse/StockMovement?Action=7). Mintsoft
        // has no quarantine flag on goods-in, so this is a second call against
        // the stock the put-away just created.
        //
        // Deliberately NOT inside the try above: the units are physically booked
        // in by this point, so failing the whole receive (and rolling back our
        // receipt row) would leave Mintsoft holding stock we have no record of —
        // strictly worse than stock that's in the wrong state. Instead the
        // receipt persists with quarantined = 0, which is the truth, and the
        // caller gets `quarantine.error` to act on.
        if (quarantineFlag) {
            try {
                await quarantineStock({
                    productId,
                    warehouseId: params.warehouseId,
                    locationId: params.locationId,
                    quantity: qty,
                    // Must be the SAME batch + expiry the ASN allocation used —
                    // that pair is how Mintsoft finds the units to move.
                    batchNo: orderLot,
                    expiryDate: orderExpiry,
                    comment: `Order ${id} receipt${idemKey ? ` (${idemKey})` : ''}`,
                });
                quarantineApplied = true;
            } catch (err) {
                quarantineError = err?.response?.data?.Message || err.message;
                log.error('[order-receive] quarantine failed AFTER stock was booked in', {
                    orderId: id, jfCode, productId, quantity: qty,
                    locationId: params.locationId, asnId: asn?.ID, error: quarantineError,
                });
            }
        }

        // Persist receipt + (maybe) flip status
        await conn.query(
            `INSERT INTO order_receipts
                (order_id, jf_code, quantity, location_id, warehouse_id,
                 asn_id, asn_item_id, idempotency_key, batch_no, expiry_date, type, quarantined)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received', ?)`,
            [
                id, jfCode, qty, params.locationId, params.warehouseId,
                asn.ID, asnItemId, idemKey,
                orderLot,
                orderExpiry,
                // Records what Mintsoft actually did, not what was asked for —
                // a failed quarantine leaves this 0 because the stock is normal.
                quarantineApplied ? 1 : 0,
            ]
        );

        const newSettled = settledQty + qty;
        const dates = parseDates(orderRow.dates);
        if (!dates.received_by_warehouse) {
            dates.received_by_warehouse = new Date().toISOString();
        }

        // Flip status when the order is fully reconciled (received +
        // not_received = quantity). Partial receipts must NOT clobber the
        // existing status (e.g. IN_WAREHOUSE set by the Sea import) — that's
        // what was hiding partials from the warehouse view. Outstanding=0 + any
        // not_received → PARTIALLY_RECEIVED (this is a /receive call, so the
        // not_received total is unchanged).
        let nextStatus = orderRow.status;
        if (newSettled >= orderQty) {
            nextStatus = notReceivedQty > 0 ? 'PARTIALLY_RECEIVED' : 'RECEIVED';
            dates.received = new Date().toISOString();
        }

        const updateCols = ['status = ?', 'dates = ?'];
        const updateVals = [nextStatus, JSON.stringify(dates)];
        if (lotChanged) {
            updateCols.push('lot_number = ?');
            updateVals.push(orderLot);
        }
        if (expiryChanged) {
            updateCols.push('exp_date = ?');
            updateVals.push(orderExpiry);
        }
        updateVals.push(id);
        await conn.query(
            `UPDATE orders SET ${updateCols.join(', ')} WHERE id = ?`,
            updateVals
        );

        const [finalRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
        const updatedOrder = rowToOrder(finalRows[0]);
        await recordAudit(conn, {
            entityType: 'order', entityId: updatedOrder.id, action: 'update',
            before: beforeOrder, after: updatedOrder, userEmail: actorEmail,
        });

        await conn.commit();
        committed = true;

        const [receipts] = await conn.query(
            'SELECT * FROM order_receipts WHERE order_id = ? ORDER BY received_at DESC',
            [id]
        );

        return {
            order: updatedOrder,
            asnId: asn.ID,
            receipts: receipts.map(receiptToJson),
            idempotent: false,
            jfCode,
            asin: orderRow.asin || '',
            quarantine: {
                requested: quarantineFlag,
                applied: quarantineApplied,
                error: quarantineError,
            },
        };
    } catch (err) {
        if (!committed) {
            try { await conn.rollback(); } catch (_) { /* ignore */ }
        }
        throw err;
    }
}

// ── Receivable-order lookup (the /scan/lookup logic) ───────────────────────
// Finds live orders whose jf_code matches the scanned SKU, that have arrived at
// the warehouse (IN_WAREHOUSE or PARTIALLY_RECEIVED), and that still have
// outstanding quantity to receive, ranked by how well the carton's lot (and
// optional expiry) line up. Orders that haven't landed yet (ON_SEA/ON_AIR/etc.)
// are never surfaced as scan targets.
// Unlike matchOrders (QC report attachment, which
// requires BOTH jf_code and lot), receiving must surface orders whose
// lot_number isn't set yet — the lot is on the carton, not the order.
//
// jf_code matching reuses the same normalisation as matchOrders (case- and
// space/dash/underscore-insensitive; leading zeros are SIGNIFICANT on a
// jf_code). Lot comparison uses normLot (leading-zero-tolerant). Returns ALL
// candidates so the operator can disambiguate when more than one matches.
const LOT_RANK = { exact: 0, normalised: 1, unset: 2, none: 3 };

// Statuses an order can be in to be a carton-scan receive target — it must have
// physically ARRIVED at the warehouse. ARRIVED_AT_WAREHOUSE is the live arrived
// status (also covers part-received-in-progress: a partial receive leaves the
// status untouched). IN_WAREHOUSE is the legacy/alternate name from the status
// constant in orders.js, kept for tolerance. PARTIALLY_RECEIVED is the
// fully-reconciled-with-shortfall state — outstanding is 0 there so the
// outstanding > 0 check filters it anyway, but it's listed for intent.
// In-transit statuses (ON_SEA/ON_AIR/CONSOLIDATED/IN_PRODUCTION/…) are excluded.
const RECEIVABLE_STATUSES = ['ARRIVED_AT_WAREHOUSE', 'IN_WAREHOUSE', 'PARTIALLY_RECEIVED'];

function classifyLot(orderLot, scannedLot) {
    const ol = (orderLot ?? '').toString().trim();
    const sl = (scannedLot ?? '').toString().trim();
    if (!ol) return 'unset';
    if (!sl) return 'none';
    if (ol === sl) return 'exact';
    if (normLot(ol) && normLot(ol) === normLot(sl)) return 'normalised';
    return 'none';
}

async function findReceivableOrders(conn, { jfCode, lotNumber, expiryDate }) {
    if (!jfCode || !String(jfCode).trim()) {
        throw new ReceiveError('INVALID_JF_CODE', 400, 'jfCode is required.');
    }
    const scannedExpiry = expiryDate ? normalizeExpiry(expiryDate) : null;
    if (scannedExpiry === false) {
        throw new ReceiveError('INVALID_EXPIRY', 400, 'expiryDate is not a valid date.');
    }

    // Match jf_code with the same space/dash/underscore-insensitive expression
    // matchOrders uses (leading zeros preserved — JF0134 ≠ JF134), and compute
    // outstanding from order_receipts so we only surface receivable lines.
    //
    // Status gate: only orders that have physically ARRIVED at the warehouse are
    // receivable from a carton scan (see RECEIVABLE_STATUSES). An ON_SEA/ON_AIR/
    // IN_PRODUCTION line hasn't landed yet, so it must never be a scan target
    // even if its jf_code matches. The outstanding > 0 check below still gates
    // each row, so a fully-settled order never appears.
    const statusPlaceholders = RECEIVABLE_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `SELECT orders.id, orders.jf_code, orders.product_name, orders.supplier, orders.status,
                orders.po_number, orders.quantity, orders.lot_number, orders.exp_date,
                COALESCE((SELECT SUM(quantity) FROM order_receipts WHERE order_id = orders.id AND type = 'received'), 0) AS received_quantity,
                COALESCE((SELECT SUM(quantity) FROM order_receipts WHERE order_id = orders.id AND type = 'not_received'), 0) AS not_received_quantity
           FROM orders
          WHERE orders.deleted_at IS NULL
            AND orders.status IN (${statusPlaceholders})
            AND UPPER(REPLACE(REPLACE(REPLACE(jf_code,' ',''),'-',''),'_','')) = ?
          ORDER BY orders.id`,
        [...RECEIVABLE_STATUSES, norm(jfCode)]
    );

    const candidates = [];
    for (const r of rows) {
        const quantity = Number(r.quantity || 0);
        const received = Number(r.received_quantity || 0);
        const notReceived = Number(r.not_received_quantity || 0);
        const outstanding = Math.max(quantity - received - notReceived, 0);
        if (outstanding <= 0) continue; // already settled — nothing left to receive

        const lotMatch = classifyLot(r.lot_number, lotNumber);
        const expDate = normalizeExpiry(r.exp_date) || null;
        const expiryMatch = scannedExpiry == null ? null : (expDate && expDate === scannedExpiry ? 'exact' : 'none');

        candidates.push({
            orderId: r.id,
            poNumber: r.po_number || null,
            jfCode: r.jf_code || null,
            productName: r.product_name || null,
            supplier: r.supplier || null,
            status: r.status || null,
            quantity,
            receivedQuantity: received,
            outstandingQuantity: outstanding,
            lotNumber: r.lot_number || null,
            expDate,
            lotMatch,
            expiryMatch,
        });
    }

    // Rank: best lot match first; an expiry match breaks ties; then most
    // outstanding; then id for stability.
    candidates.sort((a, b) => {
        const lr = LOT_RANK[a.lotMatch] - LOT_RANK[b.lotMatch];
        if (lr !== 0) return lr;
        const ae = a.expiryMatch === 'exact' ? 0 : 1;
        const be = b.expiryMatch === 'exact' ? 0 : 1;
        if (ae !== be) return ae - be;
        if (b.outstandingQuantity !== a.outstandingQuantity) return b.outstandingQuantity - a.outstandingQuantity;
        return a.orderId - b.orderId;
    });

    return { candidates };
}

module.exports = { receiveOrderStock, findReceivableOrders, parseReceiveInput, ReceiveError };
