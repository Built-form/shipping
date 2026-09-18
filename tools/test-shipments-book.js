'use strict';

// POST /shipments/:id/book: the atomic replacement for pack + close.
//
//   npm run dev:local
//   TEST_BASE_URL=http://localhost:3031 node tools/test-shipments-book.js
//
// Full + partial packs, replay, reference collision / allocation / name hint,
// a bad second line leaves NOTHING changed, the receipts 409, and a receipt
// committed while book waits on the order lock. Needs the shadow armed.

const H = require('./shipments-test-helpers');
const { api, check, section } = H;
const { getPool } = require('../src/db');

async function draft(mode, lines, opts = {}) {
    const r = await api.post('/api/v1/shipments', { mode, name: opts.name || H.draftName(opts.label || 'book', { mode, hint: opts.hint }), lines });
    if (r.status !== 201) throw new Error(`draft failed: ${r.status} ${JSON.stringify(r.data)}`);
    H.track(r.data.id);
    return r.data;
}

async function unchanged(order, label) {
    const now = await H.getOrder(order.id);
    check(`${label}: order ${order.id} untouched`,
        now.quantity === order.quantity && now.status === order.status && (now.container_number || null) === (order.containerNumber || null)
        && now.shipment_id === null, { quantity: now.quantity, status: now.status, container: now.container_number, shipment: now.shipment_id });
}

async function run() {
    await H.guard();

    section('1. Full + partial pack in one booking');
    const A = await H.createOrder();
    const B = await H.createOrder({ lotNumber: 'SHIPLOT', unitPrice: 1.25 });
    const s = await draft('SEA', [{ orderId: A.id, quantity: 100 }, { orderId: B.id, quantity: 40 }], { label: 'full-partial' });
    const R1 = H.reference('R1');
    let r = await api.post(`/api/v1/shipments/${s.id}/book`, {
        reference: R1, trackingRef: 'SHPU7654321', vesselName: 'SHIPTEST VESSEL', eta: '2026-11-01', etd: '2026-10-15', originPort: 'Ningbo',
    });
    check('200 BOOKED under the explicit reference', r.status === 200 && r.data.stage === 'BOOKED' && r.data.reference === R1, r.data);
    const booked = r.data;
    check('header from the form: tracking ref, etd, origin port', booked.trackingRef === 'SHPU7654321' && booked.etd === '2026-10-15' && booked.originPort === 'Ningbo', booked);
    const a = await H.getOrder(A.id);
    check('full pack: A is CONSOLIDATED in R1 with the carrier ref, ETD and port Create Real used to write afterwards',
        a.status === 'CONSOLIDATED' && a.container_number === R1 && a.external_container_number === 'SHPU7654321'
        && a.estimated_departure_date === '2026-10-15' && a.vessel_name === 'SHIPTEST VESSEL' && a.port === 'Ningbo'
        && a.shipment_id === s.id, a);
    const b = await H.getOrder(B.id);
    const [child] = await H.sql(`SELECT * FROM orders WHERE container_number = ? AND id <> ? AND deleted_at IS NULL`, [R1, A.id]);
    check('partial pack: B keeps 60, untouched otherwise', b.quantity === 60 && b.status === 'READY' && b.container_number === null && b.shipment_id === null, b);
    check('partial pack: the split child carries 40 in R1 and copies the order',
        child && child.quantity === 40 && child.status === 'CONSOLIDATED' && child.shipment_id === s.id
        && child.lot_number === 'SHIPLOT' && Number(child.unit_price) === 1.25 && child.jf_code === B.jfCode
        && child.port === 'Ningbo', child);
    check('partial pack: the remaining order keeps its own port', b.port === B.port || (b.port == null && B.port == null), { was: B.port, now: b.port });
    if (child) H.created.orders.push(child.id);
    const d = await H.shipment(s.id);
    check('the manifest is the packed rows', d.lines.length === 2 && d.lines.some(l => l.orderId === A.id && l.quantity === 100)
        && d.lines.some(l => l.orderId === (child && child.id) && l.quantity === 40), d.lines);
    const regRows = await api.get('/api/v1/draft-container-registry', { params: { name: s.name } });
    check('legacy: the registry row closed as converted into R1',
        regRows.data.data[0] && regRows.data.data[0].status === 'converted' && regRows.data.data[0].containerNumber === R1, regRows.data);
    check('legacy: no allocations left', (await api.get('/api/v1/draft-containers', { params: { name: s.name } })).data.data.length === 0);
    const [conv] = await H.sql(`SELECT after_json FROM audit_log WHERE entity_type = 'draft_container' AND action = 'converted'
                                  AND entity_id = (SELECT id FROM draft_containers WHERE name = ?) ORDER BY id DESC LIMIT 1`, [s.name]);
    const after = conv && (typeof conv.after_json === 'string' ? JSON.parse(conv.after_json) : conv.after_json);
    check('legacy converted event carries the form details', after && after.externalContainerNumber === 'SHPU7654321' && after.etd === '2026-10-15' && after.port === 'Ningbo', after);

    section('2. Replay');
    r = await api.post(`/api/v1/shipments/${s.id}/book`, { reference: R1 });
    check('a replayed booking answers 200 alreadyBooked', r.status === 200 && r.data.alreadyBooked === true, r.data);

    section('3. Reference collision');
    const C = await H.createOrder();
    const s2 = await draft('SEA', [{ orderId: C.id, quantity: 100 }], { label: 'collision' });
    r = await api.post(`/api/v1/shipments/${s2.id}/book`, { reference: R1 });
    check('an explicit reference already in use: 409', r.status === 409 && r.data.code === 'REFERENCE_IN_USE', r.data);
    await unchanged(C, 'collision');
    check('the draft is still a DRAFT with its line', (await H.shipment(s2.id)).stage === 'DRAFT'
        && (await api.get('/api/v1/draft-containers', { params: { name: s2.name } })).data.data.length === 1);
    r = await api.post(`/api/v1/shipments/${s2.id}/book`, { reference: '9321. Air Freight' });
    check('a reference from the other sequence: 422', r.status === 422 && r.data.code === 'REFERENCE_MODE_MISMATCH', r.data);

    section('4. Allocated reference + an AWB carrier ref (AIR)');
    const E = await H.createOrder();
    const s3 = await draft('AIR', [{ orderId: E.id, quantity: 100 }], { label: 'air' });
    const next = (await api.get('/api/v1/shipments/next-reference', { params: { mode: 'AIR' } })).data;
    r = await api.post(`/api/v1/shipments/${s3.id}/book`, { trackingRef: '112-12345675' });
    check('no reference given: the next AIR number is allocated', r.status === 200 && r.data.reference === next.reference, { got: r.data.reference, expected: next.reference });
    check('booking.referenceSource = allocated', r.data.booking && r.data.booking.referenceSource === 'allocated', r.data.booking);
    const e = await H.getOrder(E.id);
    check('the AWB went to awb_number (by shape), not external_container_number', e.awb_number === '112-12345675' && !e.external_container_number, e);

    section('5. The draft\'s own name hint');
    const F = await H.createOrder();
    const hint = 9000 + Math.floor(Math.random() * 900);
    const s4 = await draft('SEA', [{ orderId: F.id, quantity: 100 }], { label: 'hint', hint });
    r = await api.post(`/api/v1/shipments/${s4.id}/book`, {});
    const free = r.data && r.data.booking && r.data.booking.referenceSource === 'name_hint';
    check('books under the number its name reserves', r.status === 200 && (free ? r.data.reference === String(hint) : true), r.data);

    section('6. A bad second pack line leaves NOTHING changed');
    const G1 = await H.createOrder();
    const G2 = await H.createOrder();
    const s5 = await draft('SEA', [{ orderId: G1.id, quantity: 10 }, { orderId: G2.id, quantity: 100 }], { label: 'bad-line' });
    r = await api.post(`/api/v1/shipments/${s5.id}/book`, {
        reference: H.reference('BAD'), packs: [{ orderId: G1.id, qty: 10 }, { orderId: G2.id, qty: 999 }],
    });
    check('400 QTY_EXCEEDS', r.status === 400 && r.data.code === 'QTY_EXCEEDS', r.data);
    await unchanged(G1, 'bad line');
    await unchanged(G2, 'bad line');
    const s5after = await H.shipment(s5.id);
    check('the draft keeps its lines and gets no reference', s5after.stage === 'DRAFT' && s5after.reference === null && s5after.lines.length === 2, s5after);
    check('legacy allocations intact', (await api.get('/api/v1/draft-containers', { params: { name: s5.name } })).data.data.length === 2);
    r = await api.post(`/api/v1/shipments/${s5.id}/book`, { reference: H.reference('BAD2'), packs: [{ orderId: A.id, qty: 1 }] });
    check('a pack of an order that is not a line: 400', r.status === 400 && r.data.code === 'NOT_IN_DRAFT', r.data);

    section('7. Receipts block a partial pack (whole booking refused)');
    const K = await H.createOrder();
    r = await api.post(`/api/v1/orders/${K.id}/not-received`, { quantity: 1 });
    check('shortfall recorded', r.status === 200, r.data);
    const s6 = await draft('SEA', [{ orderId: K.id, quantity: 50 }], { label: 'receipts' });
    r = await api.post(`/api/v1/shipments/${s6.id}/book`, { reference: H.reference('RC') });
    check('409 SPLIT_BLOCKED_BY_RECEIPTS', r.status === 409 && r.data.code === 'SPLIT_BLOCKED_BY_RECEIPTS', r.data);
    const k = await H.getOrder(K.id);
    check('nothing written', k.quantity === 100 && k.container_number === null && (await H.shipment(s6.id)).reference === null, k);

    section('8. A receipt committed while book waits on the order lock');
    const L = await H.createOrder();
    const s7 = await draft('SEA', [{ orderId: L.id, quantity: 50 }], { label: 'race' });
    const conn = await getPool().getConnection();
    try {
        await conn.beginTransaction();
        await conn.query(`SELECT id FROM orders WHERE id = ? FOR UPDATE`, [L.id]);
        const pending = api.post(`/api/v1/shipments/${s7.id}/book`, { reference: H.reference('RACE') });
        await new Promise(res => setTimeout(res, 1500));
        await conn.query(`INSERT INTO order_receipts (order_id, jf_code, quantity, type) VALUES (?, ?, 1, 'not_received')`, [L.id, L.jfCode]);
        await conn.commit();
        r = await pending;
    } finally {
        conn.release();
    }
    check('book saw the receipt that committed during its lock wait: 409', r.status === 409 && r.data.code === 'SPLIT_BLOCKED_BY_RECEIPTS', r.data);
    const l = await H.getOrder(L.id);
    check('nothing written', l.quantity === 100 && l.container_number === null, l);

    section('9. An order that already travels elsewhere');
    const s8 = await draft('SEA', [{ orderId: A.id, quantity: 100 }], { label: 'elsewhere' });
    r = await api.post(`/api/v1/shipments/${s8.id}/book`, { reference: H.reference('ELSE') });
    check('409 ORDER_IN_OTHER_SHIPMENT', r.status === 409 && r.data.code === 'ORDER_IN_OTHER_SHIPMENT', r.data);

    section('10. Audit');
    const [aud] = await H.sql(`SELECT COUNT(*) AS n FROM audit_log WHERE entity_type = 'order' AND entity_id IN (?, ?)
                                  AND JSON_CONTAINS_PATH(COALESCE(after_json, JSON_OBJECT()), 'one', '$.shipmentId')`, [A.id, child ? child.id : 0]);
    check('order audit rows written by book carry no shipmentId', Number(aud.n) === 0);
    const hist = await api.get(`/api/v1/shipments/${s.id}/history`);
    check('the shipment history has the booking', hist.data.data.some(e => e.entityType === 'shipment' && e.action === 'booked'));
}

run().catch(err => H.fail('suite crashed', err)).finally(() => H.finish());
