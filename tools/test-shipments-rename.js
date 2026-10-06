'use strict';

// PATCH /shipments/:id { reference } on a booked shipment: the rename carries
// every record keyed to the old number as text, in the same transaction, and
// only after the caller confirms it has seen what will move.
//
//   npm run dev:local
//   TEST_BASE_URL=http://localhost:3031 node tools/test-shipments-rename.js
//
// The fixtures behind the container (a balance, an invoice document, extras,
// sign-offs, an assignee, packing rows, a photo, an ETA alert) are inserted
// directly and removed by id at the end. Needs the shadow armed.

const H = require('./shipments-test-helpers');
const { api, check, section } = H;

const fixtures = [];   // [table, id]
async function insert(table, row) {
    const cols = Object.keys(row);
    const res = await H.sql(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`, cols.map(c => row[c]));
    fixtures.push([table, res.insertId]);
    return res.insertId;
}
const one = async (table, id) => (await H.sql(`SELECT * FROM ${table} WHERE id = ?`, [id]))[0];

async function booked(orders, reference, label) {
    const r = await api.post('/api/v1/shipments', {
        mode: 'SEA', name: H.draftName(label), lines: orders.map(o => ({ orderId: o.id, quantity: o.quantity })),
    });
    if (r.status !== 201) throw new Error(`draft failed ${r.status} ${JSON.stringify(r.data)}`);
    H.track(r.data.id);
    const b = await api.post(`/api/v1/shipments/${r.data.id}/book`, { reference });
    if (b.status !== 200) throw new Error(`book failed ${b.status} ${JSON.stringify(b.data)}`);
    return b.data;
}

async function run() {
    await H.guard();
    const SUP = 'Shiptest Supplier';
    const KEY = 'shiptest supplier';
    const OLD = H.reference('RN-OLD');
    const NEW = H.reference('RN-NEW');
    const A = await H.createOrder();
    const B = await H.createOrder();
    const s = await booked([A, B], OLD, 'rename');

    const payId = await insert('shipment_payments', { shipment_id: s.id, shipment_reference: OLD, supplier_name: SUP, supplier_key: KEY, amount: 100, currency: 'USD' });
    const docId = await insert('shipment_payment_documents', { shipment_id: s.id, shipment_reference: OLD, filename: 'shiptest.pdf', s3_key: `shiptest/${H.STAMP}/invoice.pdf` });
    const extraId = await insert('payment_extras', { supplier_name: SUP, supplier_key: KEY, currency: 'USD', amount: 10, kind: 'other', rides_with: 'balance', shipment_id: s.id, shipment_reference: OLD });
    const looseExtraId = await insert('payment_extras', { supplier_name: SUP, supplier_key: KEY, currency: 'USD', amount: 5, kind: 'freight', rides_with: 'shipment', shipment_id: null, shipment_reference: OLD });
    const oldKey = `balance:USD:${OLD.toUpperCase()}|${KEY}`;
    const newKey = `balance:USD:${NEW.toUpperCase()}|${KEY}`;
    const neighbourKey = `balance:USD:${OLD.toUpperCase()}0|${KEY}`;
    const reviewId = await insert('payment_reviews', { payment_key: oldKey, kind: 'balance', supplier_name: SUP, container_ref: OLD.toUpperCase(), currency: 'USD', amount: 100, reviewed_by_email: 'shiptest-a@example.com' });
    const neighbourReviewId = await insert('payment_reviews', { payment_key: neighbourKey, kind: 'balance', supplier_name: SUP, container_ref: `${OLD.toUpperCase()}0`, currency: 'USD', amount: 100, reviewed_by_email: 'shiptest-a@example.com' });
    const assigneeId = await insert('payment_assignees', { payment_key: oldKey, assignee_email: 'shiptest-acc@example.com' });
    // Due dates set by hand: on the balance payment, on one row of it, and one on the …0 neighbour.
    const dueId = await insert('payment_due_dates', { target_key: oldKey, due_date: '2026-11-20', set_by_email: 'shiptest-a@example.com' });
    const dueRowKey = `item:derived:bal:${A.purchase_order_id ?? 1}:${OLD}`;
    const dueRowId = await insert('payment_due_dates', { target_key: dueRowKey, due_date: '2026-11-21', set_by_email: 'shiptest-a@example.com' });
    const neighbourDueId = await insert('payment_due_dates', { target_key: `item:derived:bal:1:${OLD}0`, due_date: '2026-11-22', set_by_email: 'shiptest-a@example.com' });
    const listId = await insert('packing_lists', { container_kind: 'booked', container_number: OLD, supplier_key: KEY, filename: 'shiptest.xlsx', s3_key: `shiptest/${H.STAMP}/packing.xlsx` });
    const signOffId = await insert('packing_list_sign_offs', { container_kind: 'booked', container_number: OLD, supplier_key: KEY, line_key: 'shiptest', fingerprint: 'f'.repeat(40) });
    const approvalId = await insert('packing_approvals', { container_kind: 'booked', container_number: OLD });
    const photoId = await insert('container_photos', { container_kind: 'booked', container_number: OLD, filename: 'shiptest.jpg', s3_key: `shiptest/${H.STAMP}/photo.jpg` });
    const alertId = await insert('daily_alerts', { dedup_key: `container_eta:c:${OLD}`, type: 'container_eta', event_date: '2026-10-05', title: `Container ${OLD} — ETA`, entity_type: 'container', entity_id: OLD });
    const [registry] = await H.sql(`SELECT id, container_number FROM draft_containers WHERE name = ?`, [s.name]);

    section('1. Without a confirm nothing moves');
    let r = await api.patch(`/api/v1/shipments/${s.id}`, { reference: NEW });
    check('409 RENAME_TOUCHES_RECORDS', r.status === 409 && r.data.code === 'RENAME_TOUCHES_RECORDS', r.data);
    check('it says what would move: 1 balance, 1 invoice document, 2 extras, 1 sign-off, 1 assignee, 2 due dates, 1 packing list, 1 packing sign-off, 1 packing approval, 1 photo',
        r.data.records && r.data.records.balances === 1 && r.data.records.paymentDocuments === 1 && r.data.records.extras === 2
        && r.data.records.signOffs === 1 && r.data.records.assignees === 1 && r.data.records.dueDates === 2 && r.data.records.packingLists === 1
        && r.data.records.packingSignOffs === 1 && r.data.records.packingApprovals === 1 && r.data.records.photos === 1, r.data.records);
    check('the orders keep the old number', (await H.getOrder(A.id)).container_number === OLD && (await H.getOrder(B.id)).container_number === OLD);
    check('the balance keeps the old number', (await one('shipment_payments', payId)).shipment_reference === OLD);
    check('the shipment keeps the old number', (await H.shipment(s.id)).reference === OLD);

    section('2. Confirmed: everything moves together');
    r = await api.patch(`/api/v1/shipments/${s.id}`, { reference: NEW, confirmRecords: true });
    check('200 under the new number', r.status === 200 && r.data.reference === NEW, r.data);
    check('the response counts what moved', r.data.recordsMoved && r.data.recordsMoved.balances === 1 && r.data.recordsMoved.extras === 2 && r.data.recordsMoved.photos === 1, r.data.recordsMoved);
    check('both orders carry the new number and stay in the same shipment',
        (await H.getOrder(A.id)).container_number === NEW && (await H.getOrder(B.id)).container_number === NEW
        && (await H.getOrder(A.id)).shipment_id === s.id && (await H.getOrder(B.id)).shipment_id === s.id);
    check('balance record', (await one('shipment_payments', payId)).shipment_reference === NEW);
    check('invoice document', (await one('shipment_payment_documents', docId)).shipment_reference === NEW);
    check('extra linked by id', (await one('payment_extras', extraId)).shipment_reference === NEW);
    check('extra linked by number only', (await one('payment_extras', looseExtraId)).shipment_reference === NEW);
    const review = await one('payment_reviews', reviewId);
    check('sign-off re-keyed, container part only', review.payment_key === newKey && review.container_ref === NEW.toUpperCase(), review);
    const neighbour = await one('payment_reviews', neighbourReviewId);
    check('a sign-off of the container numbered …0 is left alone', neighbour.payment_key === neighbourKey && neighbour.container_ref === `${OLD.toUpperCase()}0`, neighbour);
    check('assignee re-keyed', (await one('payment_assignees', assigneeId)).payment_key === newKey);
    check('due date on the payment re-keyed', (await one('payment_due_dates', dueId)).target_key === newKey);
    check('due date on one row re-keyed, the number as given', (await one('payment_due_dates', dueRowId)).target_key === dueRowKey.replace(OLD, NEW));
    check('a due date of the container numbered …0 is left alone', (await one('payment_due_dates', neighbourDueId)).target_key === `item:derived:bal:1:${OLD}0`);
    check('the moved due dates keep their dates and who set them', (await one('payment_due_dates', dueId)).due_date.toISOString().slice(0, 10) === '2026-11-20'
        && (await one('payment_due_dates', dueRowId)).set_by_email === 'shiptest-a@example.com');
    check('packing list', (await one('packing_lists', listId)).container_number === NEW);
    check('packing sign-off', (await one('packing_list_sign_offs', signOffId)).container_number === NEW);
    check('packing approval', (await one('packing_approvals', approvalId)).container_number === NEW);
    check('photo', (await one('container_photos', photoId)).container_number === NEW);
    check('the draft it came from now points at the new number', registry && (await one('draft_containers', registry.id)).container_number === NEW);
    const alert = await one('daily_alerts', alertId);
    check('the ETA alert follows', alert.dedup_key === `container_eta:c:${NEW}` && alert.entity_id === NEW, alert);
    check('amounts and statuses are untouched', Number((await one('shipment_payments', payId)).amount) === 100 && (await one('shipment_payments', payId)).status === 'pending'
        && Number((await one('payment_extras', extraId)).amount) === 10 && (await one('payment_extras', extraId)).status === 'open');

    section('3. Audit');
    const audits = async (type, id) => (await H.sql(
        `SELECT after_json FROM audit_log WHERE entity_type = ? AND entity_id = ? AND action = 'update' ORDER BY id DESC LIMIT 1`, [type, id]
    )).map(x => (typeof x.after_json === 'string' ? JSON.parse(x.after_json) : x.after_json))[0];
    check('the balance has an audit row naming the new number', (await audits('shipment_payment', payId) || {}).shipmentReference === NEW, await audits('shipment_payment', payId));
    check('so does the invoice document', (await audits('shipment_payment_document', docId) || {}).shipmentReference === NEW);
    check('and each extra', (await audits('payment_extra', extraId) || {}).shipmentReference === NEW && (await audits('payment_extra', looseExtraId) || {}).shipmentReference === NEW);
    check('and the sign-off', (await audits('payment_review', reviewId) || {}).paymentKey === newKey, await audits('payment_review', reviewId));
    check('and each due date', (await audits('payment_due_date', dueId) || {}).key === newKey && (await audits('payment_due_date', dueRowId) || {}).key === dueRowKey.replace(OLD, NEW));
    const ship = await audits('shipment', s.id);
    check('the shipment audit row carries the counts', ship && ship.reference === NEW && ship.recordsMoved && ship.recordsMoved.balances === 1, ship);

    section('4. A number that already has records of its own is refused');
    const C = await H.createOrder();
    const OTHER = H.reference('RN-OTHER');
    const TAKEN = H.reference('RN-TAKEN');
    const s2 = await booked([C], OTHER, 'rename-other');
    await insert('container_photos', { container_kind: 'booked', container_number: TAKEN, filename: 'shiptest-taken.jpg', s3_key: `shiptest/${H.STAMP}/taken.jpg` });
    r = await api.patch(`/api/v1/shipments/${s2.id}`, { reference: TAKEN, confirmRecords: true });
    check('409 REFERENCE_HAS_RECORDS, even with the confirm', r.status === 409 && r.data.code === 'REFERENCE_HAS_RECORDS' && r.data.records && r.data.records.photos === 1, r.data);
    check('nothing moved', (await H.getOrder(C.id)).container_number === OTHER && (await H.shipment(s2.id)).reference === OTHER);

    section('5. A container with nothing attached renames without a confirm');
    const CLEAN = H.reference('RN-CLEAN');
    r = await api.patch(`/api/v1/shipments/${s2.id}`, { reference: CLEAN });
    check('200', r.status === 200 && r.data.reference === CLEAN && (await H.getOrder(C.id)).container_number === CLEAN, r.data);

    section('6. A number too long for the extras column');
    r = await api.patch(`/api/v1/shipments/${s.id}`, { reference: `SHIPTEST-${H.STAMP}-${'L'.repeat(60)}`, confirmRecords: true });
    check('400 BAD_REFERENCE, nothing moved', r.status === 400 && r.data.code === 'BAD_REFERENCE' && (await H.shipment(s.id)).reference === NEW, r.data);

    section('7. A number an open draft holds');
    const holder = await api.post('/api/v1/shipments', { mode: 'SEA', reserve: true, label: `SHIPTEST rename-holder ${H.STAMP}` });
    if (holder.status === 201) H.track(holder.data.id);
    r = await api.patch(`/api/v1/shipments/${s2.id}`, { reference: String(holder.data.reservedSeq) });
    check('409 REFERENCE_IN_USE naming the draft', r.status === 409 && r.data.code === 'REFERENCE_IN_USE' && r.data.reservedBy === holder.data.name, r.data);
    check('nothing moved', (await H.shipment(s2.id)).reference === CLEAN && (await H.getOrder(C.id)).container_number === CLEAN);
    await api.delete(`/api/v1/shipments/${holder.data.id}`);
}

async function removeFixtures() {
    for (const [table, id] of fixtures.reverse()) {
        try { await H.sql(`DELETE FROM ${table} WHERE id = ?`, [id]); } catch (e) { console.log(`  (fixture ${table} ${id} not removed: ${e.message})`); }
    }
}

run().catch(err => H.fail('suite crashed', err)).finally(async () => { await removeFixtures(); await H.finish(); });
