'use strict';

// HTTP suite for the shipment balance payments ledger
// (/api/v1/shipment-payments). Read-mostly: it creates and then deletes its
// own records against a real booked shipment, and touches nothing else — no
// orders, no shipments, no purchase orders.
//
// Needs the local server on a TEST database:
//   npm run dev:local
//   node tools/test-shipment-payments.js
//   TEST_BASE_URL=http://localhost:3031 node tools/test-shipment-payments.js
//
// The admin-gated routes (DELETE, relink) are exercised only when the server
// reports X-User-Type: admin, which tools/dev-server.js does by default; under
// LOCAL_USER_TYPE=standard the suite asserts the 403s instead.

const { api, check, section, guard, sql, finish, counts } = require('./shipments-test-helpers');

const INVOICE_TAG = `SPTEST-${Date.now()}`;
const createdIds = [];

async function post(body) { return api.post('/api/v1/shipment-payments', body); }
async function put(id, body) { return api.put(`/api/v1/shipment-payments/${id}`, body); }

// A booked shipment with at least two purchase orders from one supplier, so
// the share split has something to divide. Keyed on purchase_orders.supplier,
// which is what membership and the context route report: orders.supplier is
// routinely spelled differently for the same company on the same shipment
// ("SUNMED" vs "Suzhou Sunmed Co., Ltd"), which is exactly why a supplier
// mismatch on an allocation is a warning and not a refusal.
async function pickFixture() {
    const rows = await sql(`
        SELECT s.id, s.reference, po.supplier, COUNT(DISTINCT po.id) AS pos
          FROM shipments s
          JOIN orders o ON o.shipment_id = s.id AND o.deleted_at IS NULL
          JOIN purchase_orders po ON po.id = o.purchase_order_id AND po.deleted_at IS NULL
         WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
           AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED')
           AND s.reference IS NOT NULL
         GROUP BY s.id, po.supplier
        HAVING pos >= 2
         ORDER BY pos DESC
         LIMIT 1
    `);
    return rows[0] || null;
}

(async () => {
    await guard();
    const isAdmin = (await api.get('/api/v1/shipment-payments')).headers['x-user-type'] === 'admin';
    console.log(`Role: ${isAdmin ? 'admin' : 'standard'}`);

    const fixture = await pickFixture();
    if (!fixture) {
        console.error('No booked shipment with two purchase orders from one supplier — nothing to test against.');
        process.exit(2);
    }

    section(`GET /shipment-payments/context (shipment ${fixture.id} · ${fixture.reference})`);
    const ctx = await api.get(`/api/v1/shipment-payments/context?shipmentReference=${encodeURIComponent(fixture.reference)}`);
    check('context 200', ctx.status === 200, ctx.data);
    const pos = (ctx.data.purchaseOrders || []).filter(p => p.supplier === fixture.supplier);
    check('context lists the supplier\'s member POs', pos.length >= 2, pos.length);
    check('context carries the share basis', pos.every(p => typeof p.valueInShipment === 'number'), pos[0]);
    check('context reports the shipment', ctx.data.shipment && ctx.data.shipment.id === fixture.id, ctx.data.shipment);
    const missingCtx = await api.get('/api/v1/shipment-payments/context?shipmentId=999999');
    check('context for an unknown shipment → 404', missingCtx.status === 404, missingCtx.data);

    section('POST /shipment-payments — share split');
    const AMOUNT = 34400.02;
    const created = await post({
        shipmentId: fixture.id, supplierName: fixture.supplier, amount: AMOUNT, currency: 'USD',
        invoiceNumber: `${INVOICE_TAG}-A`, dueDate: '2026-10-05',
        allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) },
    });
    check('create → 201', created.status === 201, created.data);
    const rec = created.data;
    if (rec && rec.id) createdIds.push(rec.id);
    const allocated = (rec.allocations || []).reduce((a, x) => a + x.amount, 0);
    check('the split sums back to the invoice exactly', Math.round(allocated * 100) === Math.round(AMOUNT * 100), { allocated, AMOUNT });
    check('fullyAllocated', rec.fullyAllocated === true, rec.unallocated);
    check('lands pending, never paid', rec.status === 'pending', rec.status);
    check('link resolves', rec.link === 'ok', rec.link);
    check('allocations name their PO', (rec.allocations || []).every(a => a.purchaseOrderId && a.poNumber), rec.allocations && rec.allocations[0]);

    section('validation');
    const dupe = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 10, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-A` });
    check('same invoice number → 409 DUPLICATE_INVOICE', dupe.status === 409 && dupe.data.code === 'DUPLICATE_INVOICE', dupe.data);
    const forced = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 500, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-A`, force: true });
    check('force overrides it → 201', forced.status === 201, forced.data);
    if (forced.data && forced.data.id) createdIds.push(forced.data.id);
    check('a second open balance is warned, not blocked', (forced.data.warnings || []).some(w => /open balance/i.test(w)), forced.data.warnings);
    const stranger = await post({
        shipmentId: fixture.id, supplierName: fixture.supplier, amount: 10, currency: 'USD',
        allocations: [{ purchaseOrderId: 999999, amount: 10 }],
    });
    check('a PO not on the shipment → 422 PO_NOT_IN_SHIPMENT', stranger.status === 422 && stranger.data.code === 'PO_NOT_IN_SHIPMENT', stranger.data);
    const over = await post({
        shipmentId: fixture.id, supplierName: fixture.supplier, amount: 10, currency: 'USD',
        allocations: [{ purchaseOrderId: pos[0].id, amount: 99 }],
    });
    check('split above the invoice → 422 OVER_ALLOCATED', over.status === 422 && over.data.code === 'OVER_ALLOCATED', over.data);
    const badCurrency = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 10, currency: 'dollars' });
    check('bad currency → 400', badCurrency.status === 400, badCurrency.data);
    const badDate = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 10, currency: 'USD', dueDate: 'soon' });
    check('bad date → 400', badDate.status === 400, badDate.data);
    const noAmount = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, currency: 'USD' });
    check('no amount → 400', noAmount.status === 400, noAmount.data);
    const draft = await sql(`SELECT id FROM shipments WHERE stage IN ('DRAFT','PLANNED') AND deleted_at IS NULL AND merged_into_id IS NULL LIMIT 1`);
    if (draft[0]) {
        const notBooked = await post({ shipmentId: draft[0].id, supplierName: fixture.supplier, amount: 10, currency: 'USD' });
        check('an unbooked shipment → 422 NOT_BOOKED', notBooked.status === 422 && notBooked.data.code === 'NOT_BOOKED', notBooked.data);
    }

    section('PUT /shipment-payments/:id — status and re-split');
    const partialPay = await put(forced.data.id, { status: 'paid' });
    check('paid with an unallocated remainder → 422 NOT_FULLY_ALLOCATED', partialPay.status === 422 && partialPay.data.code === 'NOT_FULLY_ALLOCATED', partialPay.data);
    const arranged = await put(rec.id, { status: 'arranged' });
    check('pending → arranged', arranged.status === 200 && arranged.data.status === 'arranged', arranged.data);
    const paid = await put(rec.id, { status: 'paid' });
    check('arranged → paid', paid.status === 200 && paid.data.status === 'paid', paid.data);
    check('paying stamps paidOn', !!paid.data.paidOn, paid.data.paidOn);
    const unpaid = await put(rec.id, { status: 'pending' });
    check('un-paying clears paidOn', unpaid.data.paidOn === null, unpaid.data.paidOn);
    const reSplit = await put(rec.id, { amount: 1000, allocations: [{ purchaseOrderId: pos[0].id, amount: 400 }] });
    check('re-split → 200', reSplit.status === 200, reSplit.data);
    check('the remainder is reported', Math.abs(reSplit.data.unallocated - 600) < 0.01, reSplit.data.unallocated);
    check('and blocks paid', reSplit.data.fullyAllocated === false, reSplit.data.fullyAllocated);
    const missing = await put(999999, { status: 'paid' });
    check('unknown id → 404', missing.status === 404, missing.data);

    section('GET /shipment-payments — feed and filters');
    const feed = await api.get('/api/v1/shipment-payments');
    check('feed carries our records', createdIds.every(id => feed.data.data.some(d => d.id === id)), feed.data.data.length);
    check('feed carries the shipments side-map', !!feed.data.shipments[String(fixture.id)], Object.keys(feed.data.shipments));
    check('feed carries a documents array', Array.isArray(feed.data.documents), typeof feed.data.documents);
    const byPo = await api.get(`/api/v1/shipment-payments?purchaseOrderId=${pos[0].id}`);
    check('filter by purchaseOrderId', byPo.data.data.some(d => d.id === rec.id), byPo.data.data.length);
    const byShipment = await api.get(`/api/v1/shipment-payments?shipmentId=${fixture.id}`);
    check('filter by shipmentId', byShipment.data.data.every(d => d.shipmentId === fixture.id), byShipment.data.data.length);
    const open = await api.get('/api/v1/shipment-payments?status=open');
    check('status=open is pending + arranged only', open.data.data.every(d => d.status === 'pending' || d.status === 'arranged'), open.data.data.map(d => d.status));
    const badStatus = await api.get('/api/v1/shipment-payments?status=nonsense');
    check('unknown status → 400', badStatus.status === 400, badStatus.data);

    section('audit trail');
    const audit = await sql(`SELECT action FROM audit_log WHERE entity_type = 'shipment_payment' AND entity_id = ? ORDER BY id`, [rec.id]);
    const actions = audit.map(a => a.action);
    check('create and status changes are audited', actions.includes('create') && actions.includes('status'), actions);

    section('relink after a broken pointer');
    await sql(`UPDATE shipment_payments SET shipment_id = 999999 WHERE id = ?`, [rec.id]);
    const brokenFeed = await api.get(`/api/v1/shipment-payments?purchaseOrderId=${pos[0].id}`);
    const broken = brokenFeed.data.data.find(d => d.id === rec.id);
    check('a re-seeded pointer reads as link:missing', broken && broken.link === 'missing', broken && broken.link);
    const dryRun = await api.post('/api/v1/shipment-payments/relink', { dryRun: true });
    if (isAdmin) {
        check('relink dry-run finds it', dryRun.data.relinked.some(r => r.id === rec.id), dryRun.data);
        const untouched = await sql(`SELECT shipment_id FROM shipment_payments WHERE id = ?`, [rec.id]);
        check('dry-run writes nothing', untouched[0].shipment_id === 999999, untouched[0]);
        const applied = await api.post('/api/v1/shipment-payments/relink', { dryRun: false });
        check('relink --apply repairs it', applied.data.relinked.some(r => r.id === rec.id), applied.data);
        const repaired = await sql(`SELECT shipment_id FROM shipment_payments WHERE id = ?`, [rec.id]);
        check('the pointer is restored', repaired[0].shipment_id === fixture.id, repaired[0]);
    } else {
        check('relink is admin-only → 403', dryRun.status === 403, dryRun.data);
        await sql(`UPDATE shipment_payments SET shipment_id = ? WHERE id = ?`, [fixture.id, rec.id]);
    }

    section('DELETE /shipment-payments/:id');
    const del = await api.delete(`/api/v1/shipment-payments/${rec.id}`);
    if (isAdmin) {
        check('delete → 204', del.status === 204, del.status);
        const gone = await api.get('/api/v1/shipment-payments');
        check('a deleted record leaves the feed', !gone.data.data.some(d => d.id === rec.id), gone.data.data.map(d => d.id));
        check('deleting twice → 404', (await api.delete(`/api/v1/shipment-payments/${rec.id}`)).status === 404);
    } else {
        check('delete is admin-only → 403', del.status === 403, del.data);
    }

    // Always leave the database as we found it, whatever the role allowed.
    if (createdIds.length) {
        await sql(`DELETE FROM shipment_payment_allocations WHERE payment_id IN (${createdIds.map(() => '?').join(',')})`, createdIds);
        await sql(`DELETE FROM shipment_payments WHERE id IN (${createdIds.map(() => '?').join(',')})`, createdIds);
    }
    const leftovers = await sql(`SELECT COUNT(*) AS n FROM shipment_payments WHERE invoice_number LIKE ?`, [`${INVOICE_TAG}%`]);
    check('cleanup left nothing behind', Number(leftovers[0].n) === 0, leftovers[0]);

    await finish();
    process.exit(counts().fail ? 1 : 0);
})().catch(async (err) => {
    console.error(err);
    try {
        if (createdIds.length) {
            await sql(`DELETE FROM shipment_payment_allocations WHERE payment_id IN (${createdIds.map(() => '?').join(',')})`, createdIds);
            await sql(`DELETE FROM shipment_payments WHERE id IN (${createdIds.map(() => '?').join(',')})`, createdIds);
        }
        await finish();
    } catch { /* already failing */ }
    process.exit(1);
});
