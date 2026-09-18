'use strict';

// POST /shipments/:id/transition and the effective-stage rule (the later of
// the stored stage and the stage the member orders justify).
//
//   npm run dev:local
//   TEST_BASE_URL=http://localhost:3031 node tools/test-shipments-transition.js
//
// Includes the no-downgrade cases: a ROAD shipment stays IN_TRANSIT while its
// orders stay CONSOLIDATED; a shipment moved to ARRIVED stays ARRIVED after the
// real orders-from-shipsgo job runs against this (TEST) database; a legacy-flow
// shipment follows its orders back when a human corrects them; an admin force
// backward reports stored vs effective. Needs the shadow armed.

const H = require('./shipments-test-helpers');
const { api, check, section } = H;

async function booked(mode, orders, body = {}) {
    const r = await api.post('/api/v1/shipments', {
        mode, name: H.draftName('transition', { mode }), lines: orders.map(o => ({ orderId: o.id, quantity: o.quantity })),
    });
    if (r.status !== 201) throw new Error(`draft failed ${r.status} ${JSON.stringify(r.data)}`);
    H.track(r.data.id);
    const b = await api.post(`/api/v1/shipments/${r.data.id}/book`, body);
    if (b.status !== 200) throw new Error(`book failed ${b.status} ${JSON.stringify(b.data)}`);
    return b.data;
}

const move = (id, stage, extra = {}) => api.post(`/api/v1/shipments/${id}/transition`, { stage, ...extra });

async function run() {
    await H.guard();
    const [cache] = await H.sql(`SELECT container_number FROM containers
                                  WHERE UPPER(delay_status) IN ('SAILING', 'ARRIVED', 'DISCHARGED') ORDER BY fetched_at DESC LIMIT 1`);

    section('1. SEA: BOOKED -> IN_TRANSIT -> ARRIVED fans out to the orders');
    const A = await H.createOrder();
    const B = await H.createOrder();
    const sea = await booked('SEA', [A, B], { reference: H.reference('SEA'), trackingRef: cache ? cache.container_number : 'SHPU1111118' });
    let r = await move(sea.id, 'IN_TRANSIT', { note: 'sailed' });
    check('IN_TRANSIT: both orders ON_SEA, stored + effective IN_TRANSIT',
        r.status === 200 && r.data.moved.length === 2 && r.data.moved.every(m => m.to === 'ON_SEA') && r.data.stage.stored === 'IN_TRANSIT', r.data);
    check('departedAt stamped', r.data.shipment && r.data.shipment.departedAt, r.data.shipment);
    r = await move(sea.id, 'IN_TRANSIT');
    check('the same stage again is an idempotent no-op', r.status === 200 && r.data.unchanged === true, r.data);
    r = await move(sea.id, 'ARRIVED');
    check('ARRIVED: orders ARRIVED_AT_WAREHOUSE', r.status === 200 && r.data.moved.every(m => m.to === 'ARRIVED_AT_WAREHOUSE') && r.data.stage.effective === 'ARRIVED', r.data);
    r = await move(sea.id, 'CLOSED');
    check('CLOSED needs every member terminal: 409', r.status === 409 && r.data.code === 'MEMBERS_NOT_TERMINAL', r.data);

    section('2. orders-from-shipsgo cannot downgrade an ARRIVED shipment');
    const job = require('../src/handlers/orders-from-shipsgo');
    const out = await job.handler();
    check('the job ran against TEST', out.statusCode === 200, out);
    const afterJob = await H.shipment(sea.id);
    check('still ARRIVED (stored and effective)', afterJob.stage === 'ARRIVED' && afterJob.storedStage === 'ARRIVED', afterJob);
    check('its orders are still ARRIVED_AT_WAREHOUSE', afterJob.orders.every(o => o.status === 'ARRIVED_AT_WAREHOUSE'), afterJob.orders.map(o => o.status));

    section('3. Admin force backward reports stored vs effective');
    r = await move(sea.id, 'BOOKED');
    check('backward without force: 409', r.status === 409 && r.data.code === 'BACKWARD_TRANSITION', r.data);
    r = await move(sea.id, 'BOOKED', { force: true });
    check('forced: stored BOOKED, effective still ARRIVED (orders ahead), with a warning',
        r.status === 200 && r.data.stage.stored === 'BOOKED' && r.data.stage.effective === 'ARRIVED' && r.data.warning, r.data);
    check('later milestones cleared', r.data.shipment && !r.data.shipment.departedAt && !r.data.shipment.arrivedAt, r.data.shipment);
    r = await move(sea.id, 'IN_TRANSIT');
    check('a move behind the members: 409 BEHIND_MEMBERS', r.status === 409 && r.data.code === 'BEHIND_MEMBERS', r.data);

    section('4. ROAD: IN_TRANSIT leaves the orders CONSOLIDATED and is not downgraded');
    const C = await H.createOrder();
    check('ROAD needs an explicit reference',
        (await (async () => {
            const d = await api.post('/api/v1/shipments', { mode: 'ROAD', name: H.draftName('road-noref', { mode: 'ROAD' }), lines: [{ orderId: C.id, quantity: 100 }] });
            H.track(d.data.id);
            const b = await api.post(`/api/v1/shipments/${d.data.id}/book`, {});
            await api.delete(`/api/v1/shipments/${d.data.id}`);
            return b.status === 422 && b.data.code === 'REFERENCE_REQUIRED';
        })()));
    const road = await booked('ROAD', [C], { reference: H.reference('ROAD') });
    r = await move(road.id, 'IN_TRANSIT');
    check('IN_TRANSIT: no order moves (no ON_ROAD yet)', r.status === 200 && r.data.moved.length === 0
        && r.data.skipped.every(x => x.reason === 'no_order_status_for_mode'), r.data);
    const roadNow = await H.shipment(road.id);
    check('stays IN_TRANSIT though its orders are CONSOLIDATED', roadNow.stage === 'IN_TRANSIT' && roadNow.derivedStage === 'BOOKED'
        && roadNow.orders.every(o => o.status === 'CONSOLIDATED'), roadNow);
    r = await move(road.id, 'ARRIVED');
    check('ARRIVED moves them to ARRIVED_AT_WAREHOUSE', r.status === 200 && r.data.moved.length === 1, r.data);

    section('5. A legacy-flow shipment follows its orders, both ways');
    const D = await H.createOrder();
    const LEG = H.reference('LEG');
    await api.post('/api/v1/containers/pack', { containerNumber: LEG, packs: [{ orderId: D.id, qty: 100 }] });
    const leg = await H.shipmentByReference(LEG);
    await api.patch(`/api/v1/orders/${D.id}/status`, { status: 'ON_SEA' });
    let legNow = await H.shipment(leg.id);
    check('orders ON_SEA: effective IN_TRANSIT over a stored BOOKED', legNow.stage === 'IN_TRANSIT' && legNow.storedStage === 'BOOKED', legNow);
    await api.patch(`/api/v1/orders/${D.id}/status`, { status: 'CONSOLIDATED' });
    legNow = await H.shipment(leg.id);
    check('a human corrects them back: effective BOOKED again', legNow.stage === 'BOOKED', legNow);

    section('6. Cancel');
    const E = await H.createOrder();
    const dr = await api.post('/api/v1/shipments', { mode: 'SEA', name: H.draftName('cancel'), lines: [{ orderId: E.id, quantity: 10 }] });
    H.track(dr.data.id);
    r = await move(dr.data.id, 'CANCELLED', { note: 'not needed' });
    const reg = await api.get('/api/v1/draft-container-registry', { params: { name: dr.data.name } });
    check('DRAFT -> CANCELLED writes through: the legacy draft closes as deleted',
        r.status === 200 && reg.data.data[0].status === 'deleted', { r: r.data, reg: reg.data });
    r = await move(road.id, 'CANCELLED');
    check('a booked shipment cannot be cancelled: 409', r.status === 409 && r.data.code === 'NOT_CANCELLABLE', r.data);
    r = await move(dr.data.id, 'SAILING');
    check('an unknown stage: 400', r.status === 400, r.data);

    section('7. Stage filter uses the effective stage');
    const arrived = await H.shipmentsWhere({ stage: 'ARRIVED', q: String(H.STAMP) });
    check('?stage=ARRIVED finds the SEA shipment forced back to a stored BOOKED (its orders keep it ARRIVED)',
        arrived.some(x => x.id === sea.id), arrived.map(x => x.id));
    const bookedOnly = await H.shipmentsWhere({ stage: 'BOOKED', q: String(H.STAMP) });
    check('?stage=BOOKED does not list it', !bookedOnly.some(x => x.id === sea.id), bookedOnly.map(x => x.id));
}

run().catch(err => H.fail('suite crashed', err)).finally(() => H.finish());
