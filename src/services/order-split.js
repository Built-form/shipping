'use strict';

// Split one order row in two: the original keeps `quantity - splitQuantity`,
// and a clone takes `splitQuantity` with the given column overrides and status.
//
// Extracted from POST /orders/:id/split (whose inline copy was also pasted into
// the partial branch of POST /containers/pack), keeping that route's semantics
// exactly:
//   - splitQuantity must be > 0 and < the order's quantity;
//   - an order with receipts of any type is never split, because order_receipts
//     would keep pointing at the shrunken original;
//   - the clone copies every UPDATABLE_FIELDS column of the raw row
//     (orderInsertValues), takes `status`, and gets that status's dates key
//     stamped on a copy of the original's dates JSON.
//
// Dependency-injected so UPDATABLE_FIELDS / ORDER_INSERT_COLS / setDateKey stay
// in orders.js. The caller owns the transaction and writes the audit rows
// (both call sites audit after all of their writes). Pass `receiptsByOrderId`
// when the caller has already summed receipts under lock (POST
// /shipments/:id/book reads them FOR SHARE after locking the orders); otherwise
// they are summed here.
//
// Known inherited limitation, preserved: the clone inherits no order_qc_reports
// or order_emails links.

class SplitError extends Error {
    constructor(code, message, status, payload = {}) {
        super(message);
        this.code = code;
        this.status = status;
        this.payload = payload;
    }
}

function makeSplitOrder({ ORDER_INSERT_COLS, ORDER_INSERT_PLACEHOLDERS, orderInsertValues, setDateKey, parseDates }) {
    // order: the raw orders row (SELECT * / ORDER_SELECT shape, snake_case).
    return async function splitOrder(conn, { order, splitQuantity, overrides = {}, status = 'CONSOLIDATED', receiptsByOrderId = null }) {
        const qty = Number(splitQuantity);
        const orderQty = Number(order.quantity);
        if (!Number.isFinite(qty) || qty <= 0 || qty >= orderQty) {
            throw new SplitError('BAD_SPLIT_QUANTITY',
                `split quantity (${splitQuantity}) must be positive and less than order ${order.id}'s quantity (${orderQty}).`, 400,
                { orderId: order.id, splitQuantity, orderQuantity: orderQty });
        }
        let received;
        if (receiptsByOrderId && receiptsByOrderId[order.id] != null) {
            received = Number(receiptsByOrderId[order.id]);
        } else {
            const [sumRows] = await conn.query(
                `SELECT COALESCE(SUM(quantity), 0) AS received FROM order_receipts WHERE order_id = ?`,
                [order.id]
            );
            received = Number(sumRows[0].received);
        }
        if (received > 0) {
            throw new SplitError('SPLIT_BLOCKED_BY_RECEIPTS',
                `Cannot split order ${order.id}: it has ${received} unit(s) already received.`, 409,
                { orderId: order.id, received });
        }

        await conn.query('UPDATE orders SET quantity = ? WHERE id = ?', [orderQty - qty, order.id]);
        const newDates = setDateKey({ ...parseDates(order.dates) }, status);
        const [insertResult] = await conn.query(
            `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
            orderInsertValues({ ...order, ...overrides }, status, newDates, qty)
        );
        return {
            originalId: order.id,
            childId: insertResult.insertId,
            originalQuantity: orderQty - qty,
            childQuantity: qty,
        };
    };
}

module.exports = { makeSplitOrder, SplitError };
