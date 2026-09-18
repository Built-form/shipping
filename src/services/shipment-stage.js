'use strict';

// POST /shipments/:id/transition: move a shipment through its lifecycle and
// fan the move out to its member orders.
//
//   BOOKED -> IN_TRANSIT -> ARRIVED -> CLOSED      (forward, stages may be skipped)
//   PLANNED | DRAFT -> CANCELLED                    (writes through to legacy)
//   any booked stage -> an earlier one              (admin `force` only)
//
// The guard runs on the EFFECTIVE stage, recomputed from the member rows after
// locking them (shipments row first, then orders). A successful move writes the
// stored stage plus its milestone (departed_at / arrived_at / closed_at). The
// fan-out moves members forward only: an order moves iff its level is known, it
// has been packed (level >= 5), it is below the target, and it is not an FQC
// sample; any other order is skipped with a reason and never blocks the rest.
//   IN_TRANSIT -> ON_SEA / ON_AIR by mode; ROAD moves no order (ON_ROAD is
//                 deferred): it stamps departed_at and leaves them CONSOLIDATED
//   ARRIVED    -> ARRIVED_AT_WAREHOUSE (makes orders receivable on the public
//                 /scan Lambda, so only this human-initiated route writes it)
//   CLOSED     -> needs every member in a terminal status; moves nothing
// orders.dates behaves exactly as PATCH /orders/:id/status (setDateKey); the
// transit timestamps live on the shipment. A forced backward move rewrites the
// stored stage, clears the later milestones and touches no orders, so the
// response reports both the stored and the effective stage.
// The caller owns the transaction.

const S = require('../lib/shipments');
const sync = require('./shipment-sync');
const { ShipmentError } = S;

const LATER_MILESTONES = {
    BOOKED: ['departed_at', 'arrived_at', 'closed_at'],
    IN_TRANSIT: ['arrived_at', 'closed_at'],
    ARRIVED: ['closed_at'],
    CLOSED: [],
};

function makeTransitionShipment(deps) {
    const { ORDER_SELECT, rowToOrder, setDateKey, parseDates, recordAudit, draftAudit } = deps;

    async function cancelOpen(conn, ship, { userEmail, reason }) {
        if (ship.stage === 'DRAFT') {
            // Legacy half first: the registry row closes 'deleted' and its
            // allocations go, exactly as POST /draft-containers/close does.
            await draftAudit.closeDraft(conn, { name: ship.name, reason: 'deleted', userEmail });
        } else if (ship.stage === 'PLANNED') {
            await conn.query(`DELETE FROM planned_container_allocations WHERE planned_container_name = ?`, [ship.name]);
        }
        await sync.cancelShipment(conn, ship, { reason, userEmail });
    }

    async function transitionShipment(conn, { shipmentId, stage: rawStage, note = null, force = false, isAdmin = false, userEmail = null }) {
        const target = String(rawStage || '').trim().toUpperCase();
        if (!S.isStage(target)) throw new ShipmentError(400, 'BAD_STAGE', `stage must be one of ${S.STAGES.join(', ')}.`);
        const ship = await sync.lockShipment(conn, shipmentId);
        if (!ship || ship.deleted_at) throw new ShipmentError(404, 'NOT_FOUND', `Shipment ${shipmentId} not found.`);
        if (ship.merged_into_id) {
            throw new ShipmentError(409, 'MERGED', `Shipment ${shipmentId} was merged into ${ship.merged_into_id}.`,
                { mergedIntoId: ship.merged_into_id });
        }
        const noteText = S.clean(note) ? S.clean(note).slice(0, 500) : null;

        if (target === 'CANCELLED') {
            if (!S.isOpenStage(ship.stage)) {
                throw new ShipmentError(409, 'NOT_CANCELLABLE',
                    `Only a PLANNED or DRAFT shipment can be cancelled (this one is ${ship.stage}).`);
            }
            await cancelOpen(conn, ship, { userEmail, reason: 'cancelled' });
            await recordAudit(conn, {
                entityType: 'shipment', entityId: ship.id, action: 'stage_changed',
                before: { stage: ship.stage }, after: { stage: 'CANCELLED', note: noteText }, userEmail,
            });
            return { moved: [], skipped: [], stage: { stored: 'CANCELLED', effective: 'CANCELLED' } };
        }
        if (!S.isBookedStage(target)) {
            throw new ShipmentError(422, 'UNSUPPORTED_TARGET',
                target === 'DRAFT' || target === 'PLANNED'
                    ? 'A shipment is not moved back to PLANNED / DRAFT; reuse the name to reopen it.'
                    : `Cannot transition to ${target}.`);
        }
        if (!S.isBookedStage(ship.stage)) {
            throw new ShipmentError(409, 'NOT_BOOKED',
                ship.stage === 'DRAFT' ? 'Book the draft first (POST /shipments/:id/book).' : `A ${ship.stage} shipment cannot move to ${target}.`);
        }

        const [members] = await conn.query(
            `SELECT * FROM orders WHERE shipment_id = ? AND deleted_at IS NULL ORDER BY id FOR UPDATE`,
            [ship.id]
        );
        const derived = S.deriveStage(members.map(m => m.status));
        const effective = S.effectiveStage(ship.stage, derived);
        const rTarget = S.stageRank(target);
        const rStored = S.stageRank(ship.stage);
        const rEffective = S.stageRank(effective);

        if (rTarget < rStored) {
            if (!force) {
                throw new ShipmentError(409, 'BACKWARD_TRANSITION',
                    `The shipment is ${ship.stage}; moving back to ${target} needs force (admin).`, { stored: ship.stage, effective });
            }
            if (!isAdmin) throw new ShipmentError(403, 'ADMIN_ONLY', 'A forced backward move is admin-only.');
            const clears = LATER_MILESTONES[target].map(c => `${c} = NULL`);
            await conn.query(
                `UPDATE shipments SET stage = ?${clears.length ? ', ' + clears.join(', ') : ''} WHERE id = ?`,
                [target, ship.id]
            );
            const eff = S.effectiveStage(target, derived);
            await recordAudit(conn, {
                entityType: 'shipment', entityId: ship.id, action: 'stage_changed',
                before: { stage: ship.stage, effective },
                after: { stage: target, effective: eff, forced: true, note: noteText },
                userEmail,
            });
            return {
                moved: [], skipped: [], forced: true,
                stage: { stored: target, effective: eff },
                ...(S.stageRank(eff) > rTarget ? { warning: `Member orders are still at ${eff}; only the stored stage moved.` } : {}),
            };
        }
        if (rTarget === rStored) {
            return { moved: [], skipped: [], unchanged: true, stage: { stored: ship.stage, effective } };
        }
        if (rTarget < rEffective) {
            throw new ShipmentError(409, 'BEHIND_MEMBERS',
                `The member orders are already at ${effective}; ${target} would be a step back.`, { stored: ship.stage, effective });
        }
        if (target === 'CLOSED') {
            const open = members.filter(m => !S.TERMINAL_STATUSES.has(m.status));
            if (open.length) {
                throw new ShipmentError(409, 'MEMBERS_NOT_TERMINAL',
                    `${open.length} member order(s) are not received, partially received or destroyed yet.`,
                    { orders: open.map(m => ({ orderId: m.id, status: m.status })) });
            }
        }
        if (target === 'IN_TRANSIT' && !ship.mode) {
            throw new ShipmentError(422, 'MODE_REQUIRED', 'Set the shipment mode (PATCH) before it departs.');
        }

        // Fan-out.
        const targetStatus = S.statusForStage(target, ship.mode);
        const moved = [];
        const skipped = [];
        if (targetStatus) {
            const ids = members.map(m => m.id);
            const beforeById = new Map();
            if (ids.length) {
                const [rows] = await conn.query(`${ORDER_SELECT} AND orders.id IN (${ids.map(() => '?').join(',')})`, ids);
                for (const r of rows) beforeById.set(r.id, rowToOrder(r));
            }
            for (const m of members) {
                const d = S.fanOutDecision(m, targetStatus);
                if (!d.move) { skipped.push({ orderId: m.id, status: m.status, reason: d.reason }); continue; }
                const dates = setDateKey(parseDates(m.dates), targetStatus);
                await conn.query('UPDATE orders SET status = ?, dates = ? WHERE id = ?', [targetStatus, JSON.stringify(dates), m.id]);
                moved.push({ orderId: m.id, from: m.status, to: targetStatus });
            }
            if (moved.length) {
                const movedIds = moved.map(x => x.orderId);
                const [rows] = await conn.query(`${ORDER_SELECT} AND orders.id IN (${movedIds.map(() => '?').join(',')})`, movedIds);
                for (const r of rows) {
                    await recordAudit(conn, {
                        entityType: 'order', entityId: r.id, action: 'update',
                        before: S.auditSnapshot(beforeById.get(r.id)), after: S.auditSnapshot(rowToOrder(r)), userEmail,
                    });
                }
            }
        } else if (target === 'IN_TRANSIT') {
            for (const m of members) skipped.push({ orderId: m.id, status: m.status, reason: 'no_order_status_for_mode' });
        }

        const milestone = S.MILESTONE_BY_STAGE[target];
        await conn.query(
            `UPDATE shipments SET stage = ?, ${milestone} = COALESCE(${milestone}, NOW()) WHERE id = ?`,
            [target, ship.id]
        );
        const statusesAfter = members.map(m => {
            const mv = moved.find(x => x.orderId === m.id);
            return mv ? mv.to : m.status;
        });
        const effectiveAfter = S.effectiveStage(target, S.deriveStage(statusesAfter));
        await recordAudit(conn, {
            entityType: 'shipment', entityId: ship.id, action: 'stage_changed',
            before: { stage: ship.stage, effective },
            after: { stage: target, effective: effectiveAfter, moved: moved.length, skipped: skipped.length, note: noteText },
            userEmail,
        });
        return { moved, skipped, stage: { stored: target, effective: effectiveAfter } };
    }

    return { transitionShipment, cancelOpen };
}

module.exports = { makeTransitionShipment };
