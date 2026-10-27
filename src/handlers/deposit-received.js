'use strict';

// Lambda jportal-deposit-received-<env>: a supplier confirmed on the portal that the
// deposit payment for a purchase order arrived (po_deposit_receipts in this schema,
// written by the portal; received_on NOT NULL). Invoked directly, async, by jportal
// with { purchaseOrderId }, fire-and-forget after the portal's own commit.
//
// It moves the PO's live lines that have not started production — status SCHEDULED or
// PO_SENT — into IN_PRODUCTION, exactly as an operator's move in ShipLine is written:
// status only (setDateKey stamps nothing for IN_PRODUCTION) and an audit_log row per
// line, under the supplier user's email. Lines at IN_PRODUCTION or beyond, deleted
// lines and DESTROYED samples are left alone, so a re-run is a no-op. The receipt is
// re-read here rather than trusted from the payload: an unticked receipt moves nothing.
//
// A failure is only logged on the portal side and staff can move lines in ShipLine, so
// this Lambda never needs to answer anything to anyone; its return value is for the log.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { recordAudit } = require('../lib/audit');

// The statuses before production (order-transitions levels 0 and 1).
const BEFORE_PRODUCTION = ['SCHEDULED', 'PO_SENT'];

async function handler(event) {
    const purchaseOrderId = Number(event && event.purchaseOrderId);
    if (!Number.isSafeInteger(purchaseOrderId) || purchaseOrderId <= 0) {
        log.warn('[deposit-received] bad payload', { event });
        return { outcome: 'bad_payload' };
    }

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [receipts] = await conn.query(
            `SELECT received_on, confirmed_by_email FROM po_deposit_receipts
              WHERE purchase_order_id = ? AND received_on IS NOT NULL`,
            [purchaseOrderId],
        );
        if (!receipts.length) {
            log.info('[deposit-received] no confirmed receipt; nothing moved', { purchaseOrderId });
            return { outcome: 'skipped', purchaseOrderId, reason: 'no_receipt' };
        }
        const userEmail = receipts[0].confirmed_by_email || null;

        await conn.beginTransaction();
        const ph = BEFORE_PRODUCTION.map(() => '?').join(', ');
        const [lines] = await conn.query(
            `SELECT id, status FROM orders
              WHERE purchase_order_id = ? AND deleted_at IS NULL AND status IN (${ph})
              FOR UPDATE`,
            [purchaseOrderId, ...BEFORE_PRODUCTION],
        );
        if (!lines.length) {
            await conn.rollback();
            log.info('[deposit-received] already in production or later', { purchaseOrderId });
            return { outcome: 'noop', purchaseOrderId, moved: 0 };
        }
        await conn.query(
            `UPDATE orders SET status = 'IN_PRODUCTION'
              WHERE id IN (${lines.map(() => '?').join(', ')})`,
            lines.map((l) => l.id),
        );
        for (const line of lines) {
            await recordAudit(conn, {
                entityType: 'order', entityId: Number(line.id), action: 'update',
                before: { status: line.status },
                after: { status: 'IN_PRODUCTION', note: 'deposit received (supplier portal)' },
                userEmail,
            });
        }
        await conn.commit();
        log.info('[deposit-received] done', { purchaseOrderId, moved: lines.length });
        return { outcome: 'moved', purchaseOrderId, moved: lines.length };
    } catch (err) {
        try { await conn.rollback(); } catch { /* not in a transaction */ }
        log.error('[deposit-received] failed', { purchaseOrderId, error: err.message });
        throw err;
    } finally {
        conn.release();
    }
}

exports.handler = handler;
