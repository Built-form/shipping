'use strict';

// Dual-write suite: every legacy route that writes container / draft / planned
// state keeps the shipments shadow in step, and the verify gate stays clean.
//
//   npm run dev:local                                  # server on :3031, TEST db
//   TEST_BASE_URL=http://localhost:3031 node tools/test-shipments-dualwrite.js
//
// Needs the shadow ARMED (tools/backfill-shipments.js --apply has run on this
// database). Creates its own fixture orders and removes them at the end.

const H = require('./shipments-test-helpers');
const { api, check, section } = H;
const { getPool } = require('../src/db');
const { upsertSuggestionAlert, londonToday } = require('../src/services/daily-alerts');

async function run() {
    await H.guard();
    const armed = await H.sql(`SELECT name FROM app_migrations WHERE name = 'shipments_backfill_v1'`);
    if (!armed.length) { console.error('The shadow is not armed here: run tools/backfill-shipments.js --apply first.'); process.exit(2); }
    const [failBefore] = await H.sql(`SELECT COUNT(*) AS n FROM shipment_sync_failures WHERE resolved_at IS NULL`);

    section('0. Fixtures (POST /orders hook)');
    const A = await H.createOrder();
    const B = await H.createOrder();
    const C = await H.createOrder();
    const D = await H.createOrder();
    check('POST /orders: a READY order without a number has no shipment', A.shipmentId === null, A.shipmentId);

    section('1. Draft lines: POST / PUT / DELETE /draft-containers');
    const D1 = H.draftName('lines');
    const la = await H.addDraftLine(A.id, D1, 50);
    let s1 = await H.shipmentByName(D1);
    check('a DRAFT shipment mirrors the new draft (mode from the stamp)',
        s1 && s1.stage === 'DRAFT' && s1.mode === 'SEA' && s1.lineCount === 1 && s1.totalUnits === 50, s1);
    await api.put(`/api/v1/draft-containers/${la.id}`, { allocated: 60 });
    s1 = await H.shipment(s1.id);
    check('a quantity change is mirrored', s1.lines.length === 1 && s1.lines[0].quantity === 60, s1.lines);
    const lb = await H.addDraftLine(B.id, D1, 30);
    const D2 = H.draftName('moved');
    let r = await api.put(`/api/v1/draft-containers/${lb.id}`, { draftContainerName: D2 });
    check('per-line move 200', r.status === 200, r.data);
    let s2 = await H.shipmentByName(D2);
    s1 = await H.shipment(s1.id);
    check('a per-line name change moves the line between shipments',
        s2 && s2.lineCount === 1 && s1.lines.length === 1 && s1.lines[0].orderId === A.id, { s2, s1: s1.lines });
    r = await api.delete(`/api/v1/draft-containers/${lb.id}`);
    s2 = await H.shipment(s2.id);
    check('a deleted line leaves an empty, still-open draft (legacy parity)', s2.stage === 'DRAFT' && s2.lines.length === 0, s2);

    section('2. Whole-draft rename');
    const D1R = H.draftName('renamed');
    r = await api.post('/api/v1/draft-containers/rename', { from: D1, to: D1R });
    check('rename 200', r.status === 200, r.data);
    const s1r = await H.shipmentByName(D1R);
    check('the shipment follows the rename (same id)', s1r && s1r.id === s1.id, s1r);
    check('the old name has no shipment left', !(await H.shipmentByName(D1)));

    section('3. Planned lines: POST / PUT (move) / DELETE');
    const P1 = H.draftName('planned', { kind: 'PLANNED' });
    r = await api.post('/api/v1/planned-containers', { orderId: C.id, plannedContainerName: P1, allocated: 10 });
    check('planned line 201', r.status === 201, r.data);
    const pl = r.data;
    const p1 = await H.shipmentByName(P1);
    check('a PLANNED shipment mirrors it', p1 && p1.stage === 'PLANNED' && p1.lineCount === 1, p1);
    const P2 = H.draftName('planned-moved', { kind: 'PLANNED' });
    r = await api.put(`/api/v1/planned-containers/${pl.id}`, { plannedContainerName: P2 });
    const p2 = await H.shipmentByName(P2);
    const p1After = await H.shipment(p1.id);
    check('moving the only line opens the target and cancels the emptied source',
        p2 && p2.lineCount === 1 && p1After.stage === 'CANCELLED', { p2, p1: p1After && p1After.stage });
    r = await api.delete(`/api/v1/planned-containers/${pl.id}`);
    check('deleting the last line cancels the planned shipment', (await H.shipment(p2.id)).stage === 'CANCELLED');

    section('4. Pack, then close (partial pack): the draft\'s shipment survives');
    const N1 = H.reference('N1');
    r = await api.post('/api/v1/containers/pack', { containerNumber: N1, packs: [{ orderId: A.id, qty: 60 }] });
    check('pack 200', r.status === 200, r.data);
    const childA = (r.data.data || []).find(o => o.id !== A.id);
    const origA = (r.data.data || []).find(o => o.id === A.id);
    check('the pack response carries the new shipmentId on the packed row', childA && childA.shipmentId != null && origA.shipmentId === null,
        { child: childA && childA.shipmentId, orig: origA && origA.shipmentId });
    const packHolder = await H.shipmentByReference(N1);
    check('the pack created a BOOKED shipment for the new number', packHolder && packHolder.stage === 'BOOKED' && packHolder.memberCount === 1, packHolder);
    r = await api.post('/api/v1/draft-containers/close', {
        name: D1R, reason: 'converted', containerNumber: N1, packs: [{ orderId: A.id, qty: 60 }],
        vesselName: 'SHIPTEST VESSEL', etd: '2026-10-01', port: 'Ningbo', externalContainerNumber: 'SHPU1234567',
    });
    check('close 200', r.status === 200 && r.data.deleted === 1, r.data);
    const conv = await H.shipmentByReference(N1);
    check('the draft\'s shipment took the number (one row through the lifecycle)', conv && conv.id === s1.id, { conv: conv && conv.id, draft: s1.id });
    const merged = await H.shipment(packHolder.id);
    check('the pack-created shipment was merged into it', merged && merged.mergedIntoId === s1.id && merged.deletedAt, merged);
    check('BOOKED with the packed row and the form details kept',
        conv.stage === 'BOOKED' && conv.memberCount === 1 && conv.etd === '2026-10-01' && conv.originPort === 'Ningbo'
        && conv.trackingRef === 'SHPU1234567', conv);

    section('5. Close, then pack (full pack): the pack joins the draft\'s shipment');
    const D3 = H.draftName('close-then-pack');
    await H.addDraftLine(D.id, D3, 100);
    const s3 = await H.shipmentByName(D3);
    const N2 = H.reference('N2');
    r = await api.post('/api/v1/draft-containers/close', { name: D3, reason: 'converted', containerNumber: N2, packs: [{ orderId: D.id, qty: 100 }] });
    let n2 = await H.shipmentByReference(N2);
    check('closing first: the draft\'s shipment takes the number, no members yet', n2 && n2.id === s3.id && n2.memberCount === 0, n2);
    r = await api.post('/api/v1/containers/pack', { containerNumber: N2, packs: [{ orderId: D.id, qty: 100 }] });
    n2 = await H.shipmentByReference(N2);
    check('then the pack joins that same shipment', n2 && n2.id === s3.id && n2.memberCount === 1, n2);

    section('6. Top-up into an existing number keeps that number\'s id');
    const D4 = H.draftName('top-up');
    await H.addDraftLine(C.id, D4, 100);
    const s4 = await H.shipmentByName(D4);
    await api.post('/api/v1/containers/pack', { containerNumber: N2, packs: [{ orderId: C.id, qty: 100 }] });
    r = await api.post('/api/v1/draft-containers/close', { name: D4, reason: 'converted', containerNumber: N2, packs: [{ orderId: C.id, qty: 100 }] });
    n2 = await H.shipmentByReference(N2);
    check('the existing holder survives with both members', n2 && n2.id === s3.id && n2.memberCount === 2, n2);
    check('the top-up draft\'s shipment was merged into it', (await H.shipment(s4.id)).mergedIntoId === s3.id);

    section('7. A repeat close with a different reason is a no-op');
    r = await api.post('/api/v1/draft-containers/close', { name: D4, reason: 'deleted' });
    check('legacy says alreadyClosed', r.status === 200 && r.data.alreadyClosed === true, r.data);
    n2 = await H.shipment(s3.id);
    check('the shipment is untouched (still BOOKED, 2 members)', n2.stage === 'BOOKED' && n2.memberCount === 2, n2);

    section('8. Reusing a converted draft name opens a new generation');
    await H.addDraftLine(B.id, D1R, 10);
    const gen2 = (await H.shipmentsWhere({ name: D1R, stage: 'DRAFT' }))[0];
    check('a fresh DRAFT shipment for the name', gen2 && gen2.id !== s1.id && gen2.lineCount === 1, gen2);
    check('the converted shipment keeps its number', (await H.shipment(s1.id)).reference === N1);
    const [reg] = await H.sql(`SELECT shipment_id FROM draft_containers WHERE name = ?`, [D1R]);
    check('the registry points at the new generation', reg && gen2 && reg.shipment_id === gen2.id, reg);

    section('9. QA sheet raised for a converted draft');
    r = await api.post('/api/v1/quality-assurance/generate', {
        orderIds: [D.id], qcUnits: { [D.id]: 5 }, comments: 'SHIPTEST — please disregard', draftContainerName: D3,
    });
    check('QA generate 201', r.status === 201, r.data);
    if (r.status === 201) {
        const [qa] = await H.sql(`SELECT shipment_id FROM quality_assurance_documents WHERE id = ?`, [r.data.documentId]);
        check('stamped onto the booked shipment', qa && qa.shipment_id === s3.id, qa);
        check('no new draft generation opened for it', (await H.shipmentsWhere({ name: D3, stage: 'DRAFT' })).length === 0);
    }

    section('10. Deleting an order that sits in an open draft');
    r = await api.delete(`/api/v1/orders/${B.id}`);
    check('order delete 200', r.status === 200, r.data);
    const g = await H.shipment(gen2.id);
    check('the line disappears from the draft\'s visible lines', g.lines.length === 0, g.lines);
    const [kept] = await H.sql(`SELECT COUNT(*) AS n FROM shipment_lines WHERE shipment_id = ? AND order_id = ?`, [gen2.id, B.id]);
    check('but stays mirrored, like the legacy allocation row', Number(kept.n) === 1);

    section('11. Stage follows order status for a legacy-flow shipment');
    r = await api.patch(`/api/v1/orders/${D.id}/status`, { status: 'ON_SEA' });
    check('PATCH status carries the shipmentId', r.data && r.data.shipmentId === s3.id, r.data && r.data.shipmentId);
    let x = await H.shipment(s3.id);
    check('effective IN_TRANSIT, stored still BOOKED', x.stage === 'IN_TRANSIT' && x.storedStage === 'BOOKED', { stage: x.stage, stored: x.storedStage });
    await api.patch(`/api/v1/orders/${D.id}/status`, { status: 'CONSOLIDATED' });
    x = await H.shipment(s3.id);
    check('and back to BOOKED when a human corrects the order', x.stage === 'BOOKED', x.stage);

    section('12. PATCH /containers/:cn/status');
    r = await api.patch(`/api/v1/containers/${encodeURIComponent(N2)}/status`, { status: 'ARRIVED_AT_WAREHOUSE' });
    check('container status 200, rows carry shipmentId', r.status === 200 && r.data.data.every(o => o.shipmentId === s3.id), r.data);
    check('effective ARRIVED', (await H.shipment(s3.id)).stage === 'ARRIVED');
    await api.patch(`/api/v1/containers/${encodeURIComponent(N2)}/status`, { status: 'CONSOLIDATED' });

    section('13. A hand-edited container number (PUT /orders/:id)');
    const N3 = H.reference('N3');
    r = await api.put(`/api/v1/orders/${C.id}`, { containerNumber: N3 });
    const n3 = await H.shipmentByReference(N3);
    check('the order moves to a new, needs-review shipment', n3 && n3.memberCount === 1 && n3.needsReview === true && r.data.shipmentId === n3.id, { n3, id: r.data.shipmentId });
    check('and leaves the old one', (await H.shipment(s3.id)).memberCount === 1);

    section('14. Alerts approve (status suggestion)');
    const conn = await getPool().getConnection();
    let alertId = null;
    try {
        const a = await upsertSuggestionAlert(conn, {
            dedupKey: `shiptest|${H.STAMP}|approve`, eventDate: londonToday(), title: 'SHIPTEST suggestion',
            entityType: 'order', entityId: C.id, meta: { orderId: C.id, suggestedStatus: 'ON_SEA', category: 'forward' },
        });
        alertId = a.id;
    } finally {
        conn.release();
    }
    r = await api.patch(`/api/v1/alerts/${alertId}/approve`, {});
    check('approve 200 and the order carries its shipmentId', r.status === 200 && r.data.order.shipmentId === n3.id, r.data);
    check('the shipment follows: IN_TRANSIT', (await H.shipment(n3.id)).stage === 'IN_TRANSIT');
    await H.sql(`DELETE FROM daily_alerts WHERE id = ?`, [alertId]);

    section('15. PO delete cascade');
    const [company] = await H.sql(`SELECT id FROM companies ORDER BY id LIMIT 1`);
    r = await api.post('/api/v1/purchase-orders', { companyId: company.id, supplier: 'Shiptest Supplier', notes: `SHIPTEST ${H.STAMP}` });
    check('PO created', r.status === 201, r.data);
    const po = r.data;
    const E = await H.createOrder({ purchaseOrderId: po.id, poNumber: po.poNumber });
    const N4 = H.reference('N4');
    await api.post('/api/v1/containers/pack', { containerNumber: N4, packs: [{ orderId: E.id, qty: 100 }] });
    const n4 = await H.shipmentByReference(N4);
    check('the PO line is a member of its container', n4 && n4.memberCount === 1, n4);
    r = await api.delete(`/api/v1/purchase-orders/${po.id}`);
    const docsHaveDeletedAt = (await H.sql(`SHOW COLUMNS FROM purchase_order_documents LIKE 'deleted_at'`)).length > 0;
    if (r.status === 500 && !docsHaveDeletedAt) {
        // Pre-existing bug (not the shadow's): the route soft-deletes the PO and
        // its orders, then updates purchase_order_documents.deleted_at, a column
        // that does not exist, and 500s before its audit rows and its hook.
        console.log('  SKIP  PO delete cascade: blocked by a pre-existing bug (purchase_order_documents has no deleted_at column;'
            + ' the route 500s after soft-deleting the PO and its orders, before its hook runs)');
        const [ghost] = await H.sql(
            `SELECT COUNT(*) AS n FROM shipment_lines WHERE shipment_id = ? AND order_id = ?`, [n4.id, E.id]);
        check('verify-style drift is visible: the dead order still has its manifest line', Number(ghost.n) === 1);
        await H.resyncOrders([E.id]);
        const [after] = await H.sql(
            `SELECT COUNT(*) AS n FROM shipment_lines WHERE shipment_id = ? AND order_id = ?`, [n4.id, E.id]);
        check('a membership resync (what verify --fix runs) repairs it', Number(after.n) === 0);
    } else {
        check('PO delete 200', r.status === 200, r.data);
        const n4After = await H.shipment(n4.id);
        check('the cascaded order left the manifest', n4After.memberCount === 0 && n4After.lines.length === 0, n4After);
        const [ghost] = await H.sql(
            `SELECT COUNT(*) AS n FROM shipment_lines WHERE shipment_id = ? AND order_id = ?`, [n4.id, E.id]);
        check('its manifest line is gone', Number(ghost.n) === 0);
    }

    section('16. Invariants');
    const v = await H.verify();
    check('verify: no hard drift', v.ok, H.verifySummary(v));
    const [failAfter] = await H.sql(`SELECT COUNT(*) AS n FROM shipment_sync_failures WHERE resolved_at IS NULL`);
    check('no hook failed during the run', Number(failAfter.n) === Number(failBefore.n), `${failBefore.n} -> ${failAfter.n}`);
    const [aud] = await H.sql(`
        SELECT COUNT(*) AS n FROM audit_log WHERE entity_type = 'order'
           AND (JSON_CONTAINS_PATH(COALESCE(before_json, JSON_OBJECT()), 'one', '$.shipmentId')
                OR JSON_CONTAINS_PATH(COALESCE(after_json, JSON_OBJECT()), 'one', '$.shipmentId'))`);
    check('no order audit row carries shipmentId', Number(aud.n) === 0, aud.n);
}

run().catch(err => H.fail('suite crashed', err)).finally(() => H.finish());
