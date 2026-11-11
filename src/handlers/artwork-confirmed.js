'use strict';

// Lambda jportal-artwork-confirmed-<env>: a supplier ticked items on the portal as
// "artwork confirmed" (artwork_confirmations in this schema, written by the portal
// from the email link's page or the signed-in order page; docs/artwork-confirmations.md).
// Invoked directly, async, by jportal with { purchaseOrderId }, fire-and-forget after
// the portal's own commit, as jportal-deposit-received-<env> is.
//
// For each confirmation not applied yet it writes the day of the tick to the line's
// artwork_confirmed_by_supplier — a field of its own; artwork_confirmed_date, which
// staff record, is never touched. A line still at PO_SENT also moves to IN_PRODUCTION,
// but ONLY when the supplier requires no deposit or the deposit has been paid
// (src/services/deposit-gate.js); otherwise its status stays and the audit note says
// why. When a line does move, its _FQC sample line on the same PO (JF0787_FQC for
// JF0787) moves from PO_SENT with it; the sample gets no date, only the status.
// Each line written gets one audit_log row under "supplier-portal:<who>". A line
// the supplier had already confirmed, or that is deleted, is left alone. Every
// confirmation handled is stamped applied_at, so a re-run is a no-op and a
// confirmation staff clear later is not written back.
//
// The rows are re-read here rather than trusted from the payload. A failure is only
// logged on the portal side; the next tick on the PO applies whatever is still waiting.

require('dotenv').config();
const log = require('../lib/logger');
const { getPool } = require('../db');
const { recordAuditStrict } = require('../lib/audit');
const A = require('../lib/artwork-confirmations');
const { loadDepositGate: defaultLoadGate } = require('../services/deposit-gate');

const sentToOf = (value) => {
    if (Array.isArray(value)) return value;
    try { const parsed = JSON.parse(value); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
};

// The gate is only needed when a line could move. If it cannot be worked out, the
// move is held (the confirmation itself is still recorded): never move on a guess.
async function gateFor(conn, purchaseOrderId, orders, loadGate) {
    const held = { clear: false, reason: 'terms_unknown', depositPct: null, depositPaid: false };
    if (!orders.some(o => String(o.status) === A.MOVES_FROM && !A.isConfirmed(o))) return held;
    try {
        const [pos] = await conn.query('SELECT id, supplier FROM purchase_orders WHERE id = ?', [purchaseOrderId]);
        if (!pos.length) return held;
        return await loadGate(conn, pos[0]);
    } catch (err) {
        log.warn('[artwork-confirmed] deposit gate could not be read; lines stay where they are', { purchaseOrderId, error: err.message, errno: err.errno });
        return held;
    }
}

/**
 * Apply the PO's waiting confirmations on `conn`, in one transaction.
 * → { outcome: 'noop' | 'applied', updated, moved, applied, gate? }
 */
async function applyArtworkConfirmations(conn, purchaseOrderId, { loadGate = defaultLoadGate } = {}) {
    await conn.beginTransaction();
    try {
        const [confirmations] = await conn.query(
            `SELECT id, order_id, request_id, confirmed_by_email, confirmed_by_name, confirmed_on
               FROM artwork_confirmations
              WHERE purchase_order_id = ? AND applied_at IS NULL
              ORDER BY id ASC
                FOR UPDATE`,
            [purchaseOrderId],
        );
        if (!confirmations.length) {
            await conn.rollback();
            return { outcome: 'noop', updated: 0, moved: 0, applied: 0 };
        }

        const orderIds = [...new Set(confirmations.map(c => Number(c.order_id)))];
        const [orders] = await conn.query(
            `SELECT id, status, artwork_confirmed_by_supplier, jf_code, asin FROM orders
              WHERE purchase_order_id = ? AND deleted_at IS NULL
                AND id IN (${orderIds.map(() => '?').join(', ')})
                FOR UPDATE`,
            [purchaseOrderId, ...orderIds],
        );

        const requestIds = [...new Set(confirmations.map(c => Number(c.request_id)).filter(Boolean))];
        const sentToByRequest = new Map();
        if (requestIds.length) {
            const [requests] = await conn.query(
                `SELECT id, sent_to FROM artwork_confirmation_requests
                  WHERE id IN (${requestIds.map(() => '?').join(', ')})`,
                requestIds,
            );
            for (const r of requests) sentToByRequest.set(Number(r.id), sentToOf(r.sent_to));
        }

        const gate = await gateFor(conn, purchaseOrderId, orders, loadGate);
        const plan = A.planApply(confirmations, orders, sentToByRequest, gate);
        for (const u of plan.updates) {
            if (u.status) {
                await conn.query(
                    'UPDATE orders SET artwork_confirmed_by_supplier = ?, status = ? WHERE id = ?',
                    [u.confirmedOn, u.status, u.orderId],
                );
            } else {
                await conn.query(
                    'UPDATE orders SET artwork_confirmed_by_supplier = ? WHERE id = ?',
                    [u.confirmedOn, u.orderId],
                );
            }
            // Strict: the confirmation must not land without its audit row.
            await recordAuditStrict(conn, {
                entityType: 'order', entityId: u.orderId, action: 'update',
                before: u.before, after: u.after, userEmail: u.userEmail,
            });
        }

        // A line that moved takes its _FQC sample line with it (same PO, still at
        // PO_SENT). Read only when something moved; the status is checked again in
        // the UPDATE, and a sample that did not change gets no audit row.
        let samplesMoved = 0;
        if (plan.updates.some(u => u.status)) {
            const [samples] = await conn.query(
                `SELECT id, jf_code, asin, status FROM orders
                  WHERE purchase_order_id = ? AND deleted_at IS NULL AND status = ?
                    AND (UPPER(jf_code) LIKE '%\\_FQC' OR UPPER(asin) LIKE '%\\_FQC')
                    FOR UPDATE`,
                [purchaseOrderId, A.MOVES_FROM],
            );
            for (const m of A.planSampleMoves(plan.updates, orders, samples)) {
                const [upd] = await conn.query(
                    'UPDATE orders SET status = ? WHERE id = ? AND status = ?',
                    [m.status, m.orderId, A.MOVES_FROM],
                );
                if (!upd.affectedRows) continue;
                samplesMoved += 1;
                await recordAuditStrict(conn, {
                    entityType: 'order', entityId: m.orderId, action: 'update',
                    before: m.before, after: m.after, userEmail: m.userEmail,
                });
            }
        }

        await conn.query(
            `UPDATE artwork_confirmations SET applied_at = UTC_TIMESTAMP()
              WHERE id IN (${plan.appliedIds.map(() => '?').join(', ')})`,
            plan.appliedIds,
        );
        await conn.commit();
        return {
            outcome: 'applied',
            updated: plan.updates.length,
            moved: plan.updates.filter(u => u.status).length,
            samplesMoved,
            applied: plan.appliedIds.length,
            gate: gate.reason,
        };
    } catch (err) {
        try { await conn.rollback(); } catch { /* not in a transaction */ }
        throw err;
    }
}

async function handler(event) {
    const purchaseOrderId = Number(event && event.purchaseOrderId);
    if (!Number.isSafeInteger(purchaseOrderId) || purchaseOrderId <= 0) {
        log.warn('[artwork-confirmed] bad payload', { event });
        return { outcome: 'bad_payload' };
    }

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const result = await applyArtworkConfirmations(conn, purchaseOrderId);
        log.info('[artwork-confirmed] done', { purchaseOrderId, ...result });
        return { purchaseOrderId, ...result };
    } catch (err) {
        log.error('[artwork-confirmed] failed', { purchaseOrderId, error: err.message });
        throw err;
    } finally {
        conn.release();
    }
}

exports.handler = handler;
exports.applyArtworkConfirmations = applyArtworkConfirmations;
