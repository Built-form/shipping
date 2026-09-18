'use strict';

// Characterisation of today's POST /orders/:id/split, and proof that the
// extracted splitOrder (src/services/order-split.js, used by POST
// /shipments/:id/book for partial packs) produces the same rows. This must be
// green before D3d re-points the legacy split route at splitOrder, and after.
//
//   npm run dev:local
//   TEST_BASE_URL=http://localhost:3031 node tools/test-order-split.js

const H = require('./shipments-test-helpers');
const { api, check, section } = H;

// Every settable column (UPDATABLE_FIELDS in orders.js), snake_case.
const COPIED = [
    'jf_code', 'asin', 'product_name', 'po_number', 'supplier', 'cbm_per_unit', 'order_cbm', 'carton_cbm',
    'units_per_carton', 'carton_weight', 'carton_height', 'carton_width', 'carton_depth', 'pack_size',
    'scheduled_date', 'po_date', 'qc_status', 'qc_date', 'qc_invoice_number', 'notes', 'port', 'delivery_date',
    'lot_number', 'mfg_date', 'exp_date', 'delivery_time', 'container_status', 'booking_status', 'arrived_date',
    'external_container_number', 'awb_number', 'purchase_order_id', 'unit_price', 'actual_ready_date',
    'estimated_departure_date', 'shipped_date', 'ordered_date', 'estimated_ready_date', 'artwork_confirmed_date',
];

const same = (a, b) => (a instanceof Date || b instanceof Date)
    ? String(a && a.toISOString ? a.toISOString() : a) === String(b && b.toISOString ? b.toISOString() : b)
    : (a == null ? null : String(a)) === (b == null ? null : String(b));

function fixtureBody() {
    return {
        quantity: 100, lotNumber: 'SPLITLOT', unitPrice: 2.5, port: 'Ningbo', notes: 'split fixture',
        cbmPerUnit: 0.001, orderCbm: 0.1, unitsPerCarton: 10, cartonCbm: 0.05, cartonWeight: 5,
        cartonHeight: 30, cartonWidth: 30, cartonDepth: 30, mfgDate: '2026-08-01', expDate: '2029-08-01',
        estimatedReadyDate: '2026-09-01', artworkConfirmedDate: '2026-07-15', packSize: '10s',
    };
}

async function run() {
    await H.guard();

    section('1. Today\'s route');
    const S = await H.createOrder(fixtureBody());
    const R = H.reference('SPLIT');
    let r = await api.post(`/api/v1/orders/${S.id}/split`, { splitQuantity: 30, containerNumber: R });
    check('201 with both rows', r.status === 201 && r.data.originalOrder && r.data.newOrder, r.data);
    const { originalOrder, newOrder } = r.data;
    H.created.orders.push(newOrder.id);
    check('the original keeps 70 and its status / number', originalOrder.quantity === 70 && originalOrder.status === 'READY' && originalOrder.containerNumber === null, originalOrder);
    check('the clone takes 30, CONSOLIDATED, in the given number', newOrder.quantity === 30 && newOrder.status === 'CONSOLIDATED' && newOrder.containerNumber === R, newOrder);
    check('the clone gets the consolidated date key on a copy of the original dates',
        newOrder.dates && newOrder.dates.consolidated && newOrder.dates.ready === S.dates.ready, newOrder.dates);
    const orig = await H.getOrder(S.id);
    const child = await H.getOrder(newOrder.id);
    const differs = COPIED.filter(c => !same(orig[c], child[c]));
    check('the clone copies every settable column', differs.length === 0, differs);
    check('the clone travels in its number\'s shipment (hook)', newOrder.shipmentId != null && originalOrder.shipmentId === null, { child: newOrder.shipmentId, orig: originalOrder.shipmentId });
    const audit = await H.sql(`SELECT entity_id, action, before_json, after_json FROM audit_log
                                WHERE entity_type = 'order' AND entity_id IN (?, ?) ORDER BY id`, [S.id, newOrder.id]);
    const upd = audit.find(a => a.entity_id === S.id && a.action === 'update');
    const cre = audit.find(a => a.entity_id === newOrder.id && a.action === 'create');
    const js = v => (typeof v === 'string' ? JSON.parse(v) : v);
    check('audit: an update of the original (quantity 100 -> 70)', upd && js(upd.before_json).quantity === 100 && js(upd.after_json).quantity === 70, upd);
    check('audit: a create of the clone, without shipmentId', cre && js(cre.after_json).quantity === 30 && !('shipmentId' in js(cre.after_json)), cre);

    section('2. Refusals');
    check('splitQuantity >= quantity: 400', (await api.post(`/api/v1/orders/${S.id}/split`, { splitQuantity: 70, containerNumber: R })).status === 400);
    check('splitQuantity 0: 400', (await api.post(`/api/v1/orders/${S.id}/split`, { splitQuantity: 0, containerNumber: R })).status === 400);
    check('no containerNumber: 400', (await api.post(`/api/v1/orders/${S.id}/split`, { splitQuantity: 5 })).status === 400);
    check('unknown order: 404', (await api.post(`/api/v1/orders/999999999/split`, { splitQuantity: 5, containerNumber: R })).status === 404);
    await api.post(`/api/v1/orders/${S.id}/not-received`, { quantity: 1 });
    r = await api.post(`/api/v1/orders/${S.id}/split`, { splitQuantity: 5, containerNumber: R });
    check('an order with receipts: 409', r.status === 409, r.data);

    section('3. The extracted splitOrder (via book) makes the same rows');
    const T = await H.createOrder(fixtureBody());
    const d = await api.post('/api/v1/shipments', { mode: 'SEA', name: H.draftName('split-eq'), lines: [{ orderId: T.id, quantity: 30 }] });
    H.track(d.data.id);
    r = await api.post(`/api/v1/shipments/${d.data.id}/book`, { reference: H.reference('SPLIT2') });
    check('book 200', r.status === 200, r.data);
    const origT = await H.getOrder(T.id);
    const [childT] = await H.sql(`SELECT * FROM orders WHERE container_number = ? AND deleted_at IS NULL`, [H.reference('SPLIT2')]);
    if (childT) H.created.orders.push(childT.id);
    check('originals alike (quantity, status, number)', origT.quantity === 70 && origT.status === orig.status && origT.container_number === null, origT);
    // vessel_name / eta are pack overrides (both empty here); everything else must match.
    const COMPARE = COPIED.filter(c => !['jf_code', 'product_name'].includes(c));
    const diff = COMPARE.filter(c => !same(child[c], childT && childT[c]));
    check('clones alike on every copied column', childT && diff.length === 0, diff.map(c => `${c}: ${child[c]} vs ${childT && childT[c]}`));
    check('same quantity and status', childT && childT.quantity === child.quantity && childT.status === child.status, childT);
    const kc = Object.keys(js(child.dates) || {}).sort().join(',');
    const kt = childT ? Object.keys(js(childT.dates) || {}).sort().join(',') : '';
    check('same dates keys', kc === kt, { route: kc, book: kt });
}

run().catch(err => H.fail('suite crashed', err)).finally(() => H.finish());
