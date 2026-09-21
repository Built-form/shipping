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
const { assessFit } = require('../src/services/shipment-payment-extract');

const INVOICE_TAG = `SPTEST-${Date.now()}`;
const createdIds = [];
const docIds = [];

async function post(body) { return api.post('/api/v1/shipment-payments', body); }
async function put(id, body) { return api.put(`/api/v1/shipment-payments/${id}`, body); }

// A document row as the reader would have left it. Inserted directly: the
// upload route needs S3, and everything after the upload reads only the row.
async function insertDocument({ shipmentId, reference, supplier, kind = 'remittance', status = 'succeeded', extracted = null }) {
    const name = `${INVOICE_TAG}-doc-${docIds.length}.pdf`;
    const r = await sql(
        `INSERT INTO shipment_payment_documents
            (shipment_id, shipment_reference, supplier_name, supplier_key, doc_kind, filename, s3_key,
             content_type, file_size, extract_status, extract_json, uploaded_by_email)
         VALUES (?, ?, ?, ?, ?, ?, ?, 'application/pdf', 1, ?, ?, 'test@local')`,
        [
            shipmentId, reference, supplier, supplier ? supplier.trim().replace(/\s+/g, ' ').toLowerCase() : null,
            kind, name, `stub/${name}`, status, extracted ? JSON.stringify(extracted) : null,
        ]
    );
    docIds.push(r.insertId);
    return r.insertId;
}

async function cleanup() {
    if (docIds.length) {
        await sql(`DELETE FROM shipment_payment_documents WHERE id IN (${docIds.map(() => '?').join(',')})`, docIds);
    }
    if (createdIds.length) {
        await sql(`DELETE FROM shipment_payment_allocations WHERE payment_id IN (${createdIds.map(() => '?').join(',')})`, createdIds);
        await sql(`DELETE FROM shipment_payments WHERE id IN (${createdIds.map(() => '?').join(',')})`, createdIds);
    }
}

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

    section('does a document belong here? (pure)');
    {
        const shipment = { reference: '312', tracking_ref: 'CSGU2205870', bl_number: null, booking_ref: null };
        const memberPos = [
            { id: 1, poNumber: 'MEDOFFICE-14', supplier: 'MEDOFFICE', supplierKey: 'medoffice', currency: 'USD', valueInShipment: 31246.5 },
            { id: 2, poNumber: 'PO_00299J', supplier: 'Suzhou Sunmed Co., Ltd', supplierKey: 'suzhou sunmed co., ltd', currency: 'USD', valueInShipment: 9758.62 },
        ];
        const fit = (extract, { onShipment = shipment, supplierName = 'MEDOFFICE' } = {}) => (typeof assessFit === 'function'
            ? assessFit({
                extract: { supplierName: null, currency: null, containerRefs: [], blRefs: [], poRefs: [], lines: [], bank: null, ...extract },
                shipment: onShipment, supplierName, memberPos, amount: extract.amountDueNow ?? null,
            })
            : { verdict: 'assessFit is not exported', checks: [] });

        const remittance = fit({ documentKind: 'remittance', supplierName: 'MEDOFFICE SAGLIK ENDUSTRI', currency: 'USD', amountDueNow: 25958.25 });
        check('a remittance naming only the supplier is unconfirmed', remittance.verdict === 'unconfirmed', remittance);
        check('the supplier is recognised through the legal-name noise', remittance.checks.some(c => c.key === 'supplier' && c.ok === true), remittance.checks);
        check('the container number ties it', fit({ containerRefs: ['CSGU 220587-0'] }).verdict === 'match');
        check('a container number with a size suffix still ties it', fit({ containerRefs: ['CSGU2205870/40HQ'] }).verdict === 'match');
        check('our internal reference ties it', fit({ containerRefs: ['312'] }).verdict === 'match');
        check('another container is a mismatch', fit({ containerRefs: ['TGHU1234567'] }).verdict === 'mismatch');
        check('a short reference is never found inside a container number', fit({ containerRefs: ['ABCU3123456'] }).verdict === 'mismatch');
        check('a container cannot be contradicted when we hold no number for it',
            fit({ containerRefs: ['TEMU9930041'] }, { onShipment: { reference: '324' } }).verdict === 'unconfirmed');
        check('our PO number ties it, however written', fit({ poRefs: ['MEDOFFICE 14'] }).verdict === 'match');
        check('a PO not on the shipment is a mismatch', fit({ poRefs: ['PO_77777Q'] }).verdict === 'mismatch');
        check('another supplier\'s PO is a mismatch', fit({ poRefs: ['PO_00299J'] }).verdict === 'mismatch');
        check('another supplier is a mismatch', fit({ supplierName: 'Acme Widgets Ltd' }).verdict === 'mismatch');
        check('another currency is a mismatch', fit({ currency: 'GBP', amountDueNow: 100 }).verdict === 'mismatch');
        check('more than the goods on board is a mismatch', fit({ currency: 'USD', amountDueNow: 40000 }).verdict === 'mismatch');
        const reasons = fit({ containerRefs: ['TGHU1234567'] }).checks.find(c => c.key === 'container');
        check('a mismatch says what it saw and what we hold', reasons && /TGHU1234567/.test(reasons.text) && /CSGU2205870/.test(reasons.text), reasons);
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

    section('uploads reach the feed without a record');
    const [other] = await sql(
        `SELECT id, reference FROM shipments
          WHERE deleted_at IS NULL AND merged_into_id IS NULL AND reference IS NOT NULL
            AND stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED') AND id <> ?
          ORDER BY id LIMIT 1`,
        [fixture.id]
    );
    const looseId = await insertDocument({ shipmentId: other.id, reference: other.reference, supplier: 'Loose Test Supplier', kind: 'balance_invoice', status: 'failed' });
    const whole = await api.get('/api/v1/shipment-payments');
    check('an upload with no record is in the feed while other shipments have records',
        whole.data.documents.some(d => d.id === looseId), whole.data.documents.map(d => d.id));
    const narrowed = await api.get(`/api/v1/shipment-payments?shipmentId=${fixture.id}`);
    check('a shipment filter keeps other shipments\' uploads out',
        !narrowed.data.documents.some(d => d.id === looseId), narrowed.data.documents.map(d => d.shipmentId));
    const own = await api.get(`/api/v1/shipment-payments?shipmentId=${other.id}`);
    check('its own shipment lists it', own.data.documents.some(d => d.id === looseId), own.data.documents.map(d => d.id));
    check('and only its own shipment\'s uploads', own.data.documents.every(d => d.shipmentId === other.id), own.data.documents.map(d => d.shipmentId));

    section('a remittance marks a balance paid');
    const remittance = (amount, extra = {}) => ({
        documentKind: 'remittance', supplierName: fixture.supplier, currency: 'USD',
        amountDueNow: amount, totalAmount: amount, paymentDate: '2026-09-18', invoiceDate: null, dueDate: null,
        invoiceNumber: null, containerRefs: [], blRefs: [], poRefs: [], lines: [],
        bank: { paymentReference: `${INVOICE_TAG}-FX` }, ...extra,
    });
    const onFixture = { shipmentId: fixture.id, reference: fixture.reference, supplier: fixture.supplier };
    const markPaid = (docId, body = {}) => api.post(`/api/v1/shipment-payment-documents/${docId}/mark-paid`, body);

    const proof1 = await insertDocument({ ...onFixture, extracted: remittance(1234.56) });
    const made = await markPaid(proof1, { purchaseOrderIds: pos.map(p => p.id) });
    check('with no balance on file, the proof records one → 201', made.status === 201, made.data);
    if (made.data && made.data.id) createdIds.push(made.data.id);
    check('recorded paid', made.data.status === 'paid', made.data.status);
    check('paid on the date the bank sent it', made.data.paidOn === '2026-09-18', made.data.paidOn);
    check('the bank reference is kept', made.data.bankRef === `${INVOICE_TAG}-FX`, made.data.bankRef);
    check('for the amount sent', Math.abs(made.data.amount - 1234.56) < 0.005, made.data.amount);
    check('split across the supplier\'s POs', made.data.fullyAllocated === true, made.data.unallocated);
    check('the proof is attached to it', (made.data.documents || []).some(d => d.id === proof1), (made.data.documents || []).map(d => d.id));
    const twice = await markPaid(proof1);
    check('applying the same proof again creates nothing', twice.status === 200 && twice.data.id === made.data.id, twice.data);

    const open1 = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 5000, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-R`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    if (open1.data && open1.data.id) createdIds.push(open1.data.id);
    const proof2 = await insertDocument({ ...onFixture, extracted: remittance(4000) });
    const differs = await markPaid(proof2, { paymentId: open1.data.id });
    check('a proof for a different amount → 409 AMOUNT_DIFFERS', differs.status === 409 && differs.data.code === 'AMOUNT_DIFFERS', differs.data);
    check('naming both figures', differs.data.remittance === 4000 && differs.data.balance === 5000, differs.data);
    const stillOpen = await api.get(`/api/v1/shipment-payments?shipmentId=${fixture.id}`);
    check('the balance stays pending', (stillOpen.data.data.find(d => d.id === open1.data.id) || {}).status === 'pending');
    const forcedPaid = await markPaid(proof2, { paymentId: open1.data.id, force: true });
    check('force marks it paid', forcedPaid.status === 200 && forcedPaid.data.status === 'paid', forcedPaid.data);
    check('keeping the invoiced amount', forcedPaid.data.amount === 5000, forcedPaid.data.amount);

    const open2 = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 800, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-S`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    if (open2.data && open2.data.id) createdIds.push(open2.data.id);
    const proof3 = await insertDocument({ ...onFixture, extracted: remittance(799.2) });
    const close = await markPaid(proof3, { paymentId: open2.data.id });
    check('a proof within a bank charge of the balance needs no force', close.status === 200 && close.data.status === 'paid', close.data);

    const invoiceDoc = await insertDocument({ ...onFixture, kind: 'balance_invoice', extracted: remittance(10, { documentKind: 'balance_invoice' }) });
    const notProof = await markPaid(invoiceDoc);
    check('an invoice cannot mark anything paid → 422 NOT_A_REMITTANCE', notProof.status === 422 && notProof.data.code === 'NOT_A_REMITTANCE', notProof.data);
    const unread = await insertDocument({ ...onFixture, status: 'processing' });
    const notRead = await markPaid(unread);
    check('an unread document → 422 NOT_READ', notRead.status === 422 && notRead.data.code === 'NOT_READ', notRead.data);
    const elsewhere = await insertDocument({ shipmentId: other.id, reference: other.reference, supplier: fixture.supplier, extracted: remittance(800) });
    const wrongBox = await markPaid(elsewhere, { paymentId: open2.data.id });
    check('a proof for another shipment → 422 PAYMENT_NOT_ON_SHIPMENT', wrongBox.status === 422 && wrongBox.data.code === 'PAYMENT_NOT_ON_SHIPMENT', wrongBox.data);
    if (wrongBox.status === 201 && wrongBox.data.id) createdIds.push(wrongBox.data.id);

    section('recording a balance from an upload attaches it');
    const unmatched = await insertDocument({ ...onFixture, kind: 'balance_invoice', extracted: remittance(321, { documentKind: 'balance_invoice', noPaymentCreated: 'mismatch' }) });
    const fromDoc = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 321, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-D`, documentId: unmatched });
    check('create with documentId → 201', fromDoc.status === 201, fromDoc.data);
    if (fromDoc.data && fromDoc.data.id) createdIds.push(fromDoc.data.id);
    check('the upload is attached to the new balance', (fromDoc.data.documents || []).some(d => d.id === unmatched), fromDoc.data.documents);
    const foreign = await post({ shipmentId: fixture.id, supplierName: fixture.supplier, amount: 5, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-E`, documentId: looseId });
    check('an upload from another shipment → 422 DOCUMENT_NOT_ON_SHIPMENT', foreign.status === 422 && foreign.data.code === 'DOCUMENT_NOT_ON_SHIPMENT', foreign.data);
    if (foreign.status === 201 && foreign.data.id) createdIds.push(foreign.data.id);

    section('DELETE /shipment-payment-documents/:id');
    const delDoc = await api.delete(`/api/v1/shipment-payment-documents/${looseId}`);
    if (isAdmin) {
        check('delete an upload → 204', delDoc.status === 204, delDoc.status);
        const afterDel = await api.get(`/api/v1/shipment-payments?shipmentId=${other.id}`);
        check('a deleted upload leaves the feed', !afterDel.data.documents.some(d => d.id === looseId), afterDel.data.documents.map(d => d.id));
    } else {
        check('deleting an upload is admin-only → 403', delDoc.status === 403, delDoc.data);
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
    await cleanup();
    const leftovers = await sql(
        `SELECT COUNT(*) AS n FROM shipment_payments WHERE invoice_number LIKE ? OR bank_ref LIKE ?`,
        [`${INVOICE_TAG}%`, `${INVOICE_TAG}%`]
    );
    check('cleanup left no records behind', Number(leftovers[0].n) === 0, leftovers[0]);
    const leftoverDocs = await sql(`SELECT COUNT(*) AS n FROM shipment_payment_documents WHERE filename LIKE ?`, [`${INVOICE_TAG}%`]);
    check('cleanup left no uploads behind', Number(leftoverDocs[0].n) === 0, leftoverDocs[0]);

    await finish();
    process.exit(counts().fail ? 1 : 0);
})().catch(async (err) => {
    console.error(err);
    try {
        await cleanup();
        await finish();
    } catch { /* already failing */ }
    process.exit(1);
});
