'use strict';

// POST /shipments/:id/book: the atomic replacement for the legacy two-call
// conversion (POST /containers/pack, then POST /draft-containers/close), where
// the carrier ref, AWB, ETD and port typed on the conversion form survived only
// inside an audit JSON blob.
//
// One transaction on one connection; the caller owns begin / commit / rollback
// (repo convention). In order:
//   1. lock the shipment; it must be a DRAFT (a replay of a completed booking
//      returns alreadyBooked);
//   2. resolve the reference: explicit, else the draft's own name hint
//      ('... - 328') when free, else the next number in the mode's sequence.
//      uk_reference is the guard: a taken candidate raises ER_DUP_ENTRY and the
//      allocator increments locally (re-reading its snapshot would compute the
//      same number again);
//   3. lock the orders in id order;
//   4. validate everything from the locked rows before any order is written:
//      quantities, "already carries another number", shippable status, and
//      receipts, read FOR SHARE after the order locks (a receive on the public
//      /scan Lambda can commit during the lock wait, and a plain SUM would read
//      the older snapshot). A partial pack of an order with receipts refuses the
//      whole booking;
//   5. pack: a full pack is exactly the UPDATE legacy pack issues, plus the
//      carrier ref and estimated_departure_date that legacy loses; a partial
//      pack is splitOrder;
//   6. close the legacy draft ('converted', with the form details) through
//      draftAudit.closeDraft, realise the manifest (one line per packed order
//      row), move the shipment to BOOKED, audit.
// Legacy stays the source of truth: every legacy column and table ends up
// exactly as pack + close would leave it.

const S = require('../lib/shipments');
const sync = require('./shipment-sync');
const { ShipmentError } = S;

const SHIPPABLE_MAX_LEVEL = 5; // CONSOLIDATED: never regress an order that has already left

function makeBookShipment(deps) {
    const {
        ORDER_SELECT, rowToOrder, setDateKey, parseDates, recordAudit, recordPoAttachmentChange,
        draftAudit, splitOrder, statusLevel, isFqcOrder,
    } = deps;

    async function takeReference(conn, shipmentId, reference) {
        const parsed = S.parseReference(reference);
        try {
            await conn.query(
                `UPDATE shipments SET reference = ?, reference_seq = ? WHERE id = ?`,
                [parsed.reference.slice(0, 100), parsed.seq, shipmentId]
            );
            return true;
        } catch (e) {
            if (e.code === 'ER_DUP_ENTRY') return false;
            throw e;
        }
    }

    async function resolveReference(conn, ship, mode, explicit) {
        if (explicit) {
            const parsed = S.parseReference(explicit);
            if (parsed.mode && parsed.mode !== mode) {
                throw new ShipmentError(422, 'REFERENCE_MODE_MISMATCH',
                    `Reference "${parsed.reference}" belongs to the ${parsed.mode} sequence, but this is a ${mode} shipment.`);
            }
            if (!(await sync.referenceIsFree(conn, parsed.reference, { exceptShipmentId: ship.id }))
                || !(await takeReference(conn, ship.id, parsed.reference))) {
                throw new ShipmentError(409, 'REFERENCE_IN_USE', `Reference "${parsed.reference}" is already in use.`,
                    { reference: parsed.reference });
            }
            return { reference: parsed.reference, source: 'explicit' };
        }
        const hint = S.parseNameHint(ship.name);
        if (hint && hint.mode === mode
            && await sync.referenceIsFree(conn, hint.reference, { exceptShipmentId: ship.id })
            && await takeReference(conn, ship.id, hint.reference)) {
            return { reference: hint.reference, source: 'name_hint' };
        }
        if (mode === 'ROAD') {
            throw new ShipmentError(422, 'REFERENCE_REQUIRED', 'ROAD shipments have no reference sequence: send an explicit reference.');
        }
        const next = await sync.nextReference(conn, mode);
        let seq = next.seq;
        for (let attempt = 0; attempt < 25; attempt++, seq++) {
            const candidate = S.formatReference(mode, seq);
            if (await sync.referenceIsFree(conn, candidate, { exceptShipmentId: ship.id })
                && await takeReference(conn, ship.id, candidate)) {
                return { reference: candidate, source: 'allocated' };
            }
        }
        throw new ShipmentError(409, 'REFERENCE_ALLOCATION_FAILED', `Could not allocate a free ${mode} reference after ${seq - next.seq} attempts.`);
    }

    return async function bookShipment(conn, { shipmentId, body = {}, userEmail = null }) {
        const b = body || {};
        const ship = await sync.lockShipment(conn, shipmentId);
        if (!ship || ship.deleted_at) throw new ShipmentError(404, 'NOT_FOUND', `Shipment ${shipmentId} not found.`);
        if (ship.merged_into_id) {
            throw new ShipmentError(409, 'MERGED', `Shipment ${shipmentId} was merged into ${ship.merged_into_id}.`,
                { mergedIntoId: ship.merged_into_id });
        }
        if (S.isBookedStage(ship.stage)) return { alreadyBooked: true, shipmentId: ship.id, reference: ship.reference };
        if (ship.stage !== 'DRAFT') {
            throw new ShipmentError(409, 'NOT_A_DRAFT', `Only a DRAFT shipment can be booked (this one is ${ship.stage}).`);
        }

        // Inputs.
        let mode = ship.mode;
        let modeFromBody = false;
        if (b.mode !== undefined && b.mode !== null && b.mode !== '') {
            const m = String(b.mode).trim().toUpperCase();
            if (!S.isMode(m)) throw new ShipmentError(400, 'BAD_MODE', 'mode must be SEA, AIR or ROAD.');
            if (mode && m !== mode) throw new ShipmentError(409, 'MODE_CONFLICT', `This shipment is ${mode}; change it with PATCH first.`);
            if (!mode) { mode = m; modeFromBody = true; }
        }
        if (!mode) throw new ShipmentError(422, 'MODE_REQUIRED', 'The shipment has no mode: send mode (SEA, AIR or ROAD).');
        const dateField = (key) => {
            if (b[key] === undefined || b[key] === null || b[key] === '') return null;
            const d = sync.validDate(b[key]);
            if (!d) throw new ShipmentError(400, 'BAD_DATE', `${key} must be a YYYY-MM-DD date.`);
            return d;
        };
        const eta = dateField('eta');
        const etd = dateField('etd');
        const trackingRef = S.clean(b.trackingRef) ? S.clean(b.trackingRef).slice(0, 255) : null;
        const vesselName = S.clean(b.vesselName) ? S.clean(b.vesselName).slice(0, 255) : null;
        const originPort = S.clean(b.originPort) ? S.clean(b.originPort).slice(0, 255) : null;
        const bookingRef = S.clean(b.bookingRef) ? S.clean(b.bookingRef).slice(0, 100) : null;
        const blNumber = S.clean(b.blNumber) ? S.clean(b.blNumber).slice(0, 100) : null;
        const forwarder = S.clean(b.forwarder) ? S.clean(b.forwarder).slice(0, 255) : null;
        const carrierCol = trackingRef ? S.carrierColumnFor(trackingRef) : null;

        // What to pack: the draft's allocation rows (legacy is the source of
        // truth), or an explicit subset of them.
        const [allocs] = await conn.query(
            `SELECT order_id, allocated FROM draft_container_allocations
              WHERE draft_container_name = ? ORDER BY order_id FOR SHARE`,
            [ship.name]
        );
        const allocated = new Map(allocs.map(a => [Number(a.order_id), Number(a.allocated) || 0]));
        let packs;
        const explicitPacks = Array.isArray(b.packs) && b.packs.length > 0;
        if (explicitPacks) {
            packs = b.packs.map(p => ({ orderId: Number(p && p.orderId), qty: Number(p && p.qty) }));
            const seen = new Set();
            for (const p of packs) {
                if (!Number.isInteger(p.orderId) || p.orderId <= 0 || !Number.isInteger(p.qty) || p.qty <= 0) {
                    throw new ShipmentError(400, 'BAD_PACK', 'Each pack needs an integer orderId and an integer qty > 0.');
                }
                if (seen.has(p.orderId)) throw new ShipmentError(400, 'DUPLICATE_PACK', `Order ${p.orderId} is packed twice.`);
                seen.add(p.orderId);
                if (!allocated.has(p.orderId)) {
                    throw new ShipmentError(400, 'NOT_IN_DRAFT', `Order ${p.orderId} is not a line of this draft.`, { orderId: p.orderId });
                }
            }
        } else {
            packs = allocs.map(a => ({ orderId: Number(a.order_id), qty: Number(a.allocated) || 0 }));
        }

        const { reference, source: referenceSource } = await resolveReference(conn, ship, mode, S.clean(b.reference));

        // Lock the orders, then read everything the validation needs from the
        // locked rows.
        const orderIds = sync.intIds(packs.map(p => p.orderId));
        const [locked] = orderIds.length
            ? await conn.query(`SELECT * FROM orders WHERE id IN (${orderIds.map(() => '?').join(',')}) ORDER BY id FOR UPDATE`, orderIds)
            : [[]];
        const byId = new Map(locked.filter(o => !o.deleted_at).map(o => [o.id, o]));
        const received = {};
        if (orderIds.length) {
            const [receipts] = await conn.query(
                `SELECT order_id, quantity FROM order_receipts WHERE order_id IN (${orderIds.map(() => '?').join(',')}) FOR SHARE`,
                orderIds
            );
            for (const r of receipts) received[r.order_id] = (received[r.order_id] || 0) + Number(r.quantity || 0);
        }

        const plan = [];
        for (const p of packs.sort((x, y) => x.orderId - y.orderId)) {
            const order = byId.get(p.orderId);
            if (!order) {
                // A line on a soft-deleted order is invisible in the Draft tab
                // and is dropped here the same way; an explicit pack is an error.
                if (explicitPacks) throw new ShipmentError(404, 'ORDER_NOT_FOUND', `Order ${p.orderId} not found.`, { orderId: p.orderId });
                continue;
            }
            const orderQty = Number(order.quantity);
            if (p.qty <= 0) throw new ShipmentError(400, 'BAD_PACK', `Order ${p.orderId}: nothing allocated to pack.`, { orderId: p.orderId });
            if (p.qty > orderQty) {
                throw new ShipmentError(400, 'QTY_EXCEEDS', `qty (${p.qty}) exceeds order ${p.orderId} quantity (${orderQty}).`,
                    { orderId: p.orderId, qty: p.qty, orderQuantity: orderQty });
            }
            const current = S.clean(order.container_number);
            if (current && current.toLowerCase() !== reference.toLowerCase()) {
                throw new ShipmentError(409, 'ORDER_IN_OTHER_SHIPMENT', `Order ${p.orderId} already travels in ${current}.`,
                    { orderId: p.orderId, containerNumber: current });
            }
            const level = statusLevel(order.status);
            if (level == null || level > SHIPPABLE_MAX_LEVEL) {
                throw new ShipmentError(409, 'ORDER_NOT_SHIPPABLE', `Order ${p.orderId} is ${order.status}; it cannot be packed again.`,
                    { orderId: p.orderId, status: order.status });
            }
            if (isFqcOrder(order)) {
                throw new ShipmentError(409, 'FQC_NOT_SHIPPABLE', `Order ${p.orderId} is an FQC sample; it cannot be shipped.`, { orderId: p.orderId });
            }
            const partial = p.qty < orderQty;
            if (partial && (received[p.orderId] || 0) > 0) {
                throw new ShipmentError(409, 'SPLIT_BLOCKED_BY_RECEIPTS',
                    `Cannot pack part of order ${p.orderId}: it has ${received[p.orderId]} unit(s) already received.`,
                    { orderId: p.orderId, received: received[p.orderId] });
            }
            plan.push({ order, qty: p.qty, partial });
        }
        if (!plan.length) throw new ShipmentError(422, 'NOTHING_TO_BOOK', 'The draft has no lines on live orders to pack.');

        // Snapshots for the audit rows, taken before any order is written.
        const beforeIds = plan.map(x => x.order.id);
        const [beforeRows] = await conn.query(
            `${ORDER_SELECT} AND orders.id IN (${beforeIds.map(() => '?').join(',')})`, beforeIds
        );
        const beforeById = new Map(beforeRows.map(r => [r.id, rowToOrder(r)]));

        // Pack.
        const audits = [];
        const packedRowIds = [];
        for (const { order, qty, partial } of plan) {
            if (!partial) {
                const dates = setDateKey(parseDates(order.dates), 'CONSOLIDATED');
                const sets = [`status = 'CONSOLIDATED'`, 'container_number = ?', 'vessel_name = ?', 'eta = ?', 'dates = ?', 'shipment_id = ?'];
                const vals = [reference, vesselName, eta, JSON.stringify(dates), ship.id];
                if (carrierCol) { sets.push(`${carrierCol} = ?`); vals.push(trackingRef); }
                if (etd) { sets.push('estimated_departure_date = ?'); vals.push(etd); }
                await conn.query(`UPDATE orders SET ${sets.join(', ')} WHERE id = ?`, [...vals, order.id]);
                packedRowIds.push(order.id);
                audits.push({ id: order.id, before: beforeById.get(order.id), action: 'update' });
            } else {
                const overrides = { container_number: reference, vessel_name: vesselName, eta };
                if (carrierCol) overrides[carrierCol] = trackingRef;
                if (etd) overrides.estimated_departure_date = etd;
                const r = await splitOrder(conn, {
                    order, splitQuantity: qty, overrides, status: 'CONSOLIDATED', receiptsByOrderId: received,
                });
                await conn.query(
                    `UPDATE orders SET shipment_id = ?, last_updated = last_updated WHERE id = ?`,
                    [ship.id, r.childId]
                );
                packedRowIds.push(r.childId);
                audits.push({ id: order.id, before: beforeById.get(order.id), action: 'update' });
                audits.push({ id: r.childId, before: null, action: 'create' });
            }
        }

        // Legacy half: the registry row closes 'converted' with the form
        // details, and every allocation of the draft goes, exactly as the
        // legacy close route would do it.
        const close = await draftAudit.closeDraft(conn, {
            name: ship.name, reason: 'converted', userEmail,
            details: {
                containerNumber: reference,
                externalContainerNumber: carrierCol === 'external_container_number' ? trackingRef : null,
                awbNumber: carrierCol === 'awb_number' ? trackingRef : null,
                vesselName, eta, etd, freightType: mode, port: originPort,
                packs: plan.map(x => ({ orderId: x.order.id, qty: x.qty })),
            },
        });

        // The shipment: booked, keys settled, header from the form.
        const sets = [
            `stage = 'BOOKED'`, 'open_key = NULL', 'booked_at = COALESCE(booked_at, NOW())',
            'tracking_ref = COALESCE(?, tracking_ref)', 'vessel_name = COALESCE(?, vessel_name)',
            'eta = COALESCE(?, eta)', 'etd = COALESCE(?, etd)', 'origin_port = COALESCE(?, origin_port)',
            'booking_ref = COALESCE(?, booking_ref)', 'bl_number = COALESCE(?, bl_number)', 'forwarder = COALESCE(?, forwarder)',
        ];
        const vals = [trackingRef, vesselName, eta, etd, originPort, bookingRef, blNumber, forwarder];
        if (modeFromBody) { sets.push('mode = ?', `mode_source = 'user'`); vals.push(mode); }
        await conn.query(`UPDATE shipments SET ${sets.join(', ')} WHERE id = ?`, [...vals, ship.id]);
        await sync.leaveBookedManifests(conn, packedRowIds, ship.id);
        await sync.rebuildBookedLines(conn, ship.id);
        if (close && close.id) {
            await conn.query(`UPDATE draft_containers SET shipment_id = ? WHERE id = ?`, [ship.id, close.id]);
        }
        await sync.stampDraftDocuments(conn, ship.name, ship.id);

        // Audit, after every write: per order (like pack), then the shipment.
        const afterIds = sync.intIds([...beforeIds, ...packedRowIds]);
        const [afterRows] = await conn.query(
            `${ORDER_SELECT} AND orders.id IN (${afterIds.map(() => '?').join(',')}) ORDER BY orders.id`, afterIds
        );
        const afterById = new Map(afterRows.map(r => [r.id, rowToOrder(r)]));
        for (const a of audits) {
            const after = afterById.get(a.id) || null;
            await recordAudit(conn, {
                entityType: 'order', entityId: a.id, action: a.action,
                before: S.auditSnapshot(a.before), after: S.auditSnapshot(after), userEmail,
            });
            await recordPoAttachmentChange(conn, { before: a.before, after, userEmail });
        }
        await recordAudit(conn, {
            entityType: 'shipment', entityId: ship.id, action: 'booked',
            before: { stage: ship.stage, reference: null },
            after: {
                stage: 'BOOKED', reference, referenceSource, mode, trackingRef, vesselName, eta, etd, originPort,
                packs: plan.map(x => ({ orderId: x.order.id, qty: x.qty, partial: x.partial })),
                orders: packedRowIds,
            },
            userEmail,
        });

        return {
            alreadyBooked: false,
            shipmentId: ship.id,
            reference,
            referenceSource,
            orders: afterRows.map(rowToOrder),
            packedOrderIds: packedRowIds,
            draft: close && !close.notFound ? { id: close.id, name: close.name, deletedAllocations: close.deleted } : null,
        };
    };
}

module.exports = { makeBookShipment };
