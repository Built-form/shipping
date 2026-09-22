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
const { assessFit, matchLinesToPos, compareInvoiceToContainer } = require('../src/services/shipment-payment-extract');

const INVOICE_TAG = `SPTEST-${Date.now()}`;
const createdIds = [];
const docIds = [];
const paymentIds = [];
let piFixture = null;

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
    // Transfers first: their lines may have settled rows below.
    if (paymentIds.length) {
        await sql(`DELETE FROM supplier_payment_lines WHERE payment_id IN (${paymentIds.map(() => '?').join(',')})`, paymentIds);
        await sql(`DELETE FROM supplier_payments WHERE id IN (${paymentIds.map(() => '?').join(',')})`, paymentIds);
    }
    if (piFixture) {
        await sql(`UPDATE purchase_order_invoice_payments SET payment_status = 'pending', settled_by_payment_id = NULL WHERE id = ?`, [piFixture.id]);
    }
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
        // Two suppliers in the same city share a word; that word is not a match.
        const { sameSupplier } = require('../src/services/shipment-payment-extract');
        check('a shared city name does not make two suppliers one', sameSupplier('Suzhou Sunmed Co., Ltd', 'Suzhou Quanjuda Purification Tech Co., Ltd') === false);
        check('a short name inside a long one still does', sameSupplier('SUNMED', 'Suzhou Sunmed Co.,Ltd.') === true);
        check('the same name with legal-form noise does', sameSupplier('MEDOFFICE SAGLIK ENDUSTRI', 'Medoffice Saglik Endustri A.S.') === true);
        check('a name with extra trading words does', sameSupplier('Suzhou Sunmed Co., Ltd', 'SUZHOU SUNMED MEDICAL PRODUCTS CO., LTD.') === true);
        check('two Ningbo suppliers do not', sameSupplier('Ningbo Likesgreen Industrial co., Ltd', 'Ningbo Tape Industrial Co., Ltd.') === false);
    }

    section('a deposit netted off the invoice (pure)');
    {
        // Suppliers bill the goods line by line, then take the deposit off the
        // total: the lines add up to the goods, the payable is the balance.
        const pos = [
            { id: 1, poNumber: 'PO_00333J', valueInShipment: 23147.2, lineCount: 1 },
            { id: 2, poNumber: 'PO_00297J', valueInShipment: 11010.8, lineCount: 1 },
        ];
        const lines = [{ poRef: 'PO00333J', amount: 23147.2 }, { poRef: 'PO 00297J', amount: 11010.8 }];
        const balance = 23910.6; // 70 % of 34,158.00
        const netted = matchLinesToPos({ lines, poRefs: [] }, pos, balance);
        const sum = netted.allocations.reduce((a, x) => a + x.amount, 0);
        check('a split read off line values adds up to the balance, not the goods', Math.round(sum * 100) === Math.round(balance * 100), { sum, balance });
        const first = netted.allocations.find(a => a.purchaseOrderId === 1);
        check('each PO keeps its share of the lines', first && Math.abs(first.amount - 23147.2 * 0.7) < 0.02, netted.allocations);
        const whole = matchLinesToPos({ lines, poRefs: [] }, pos, 34158);
        check('lines that already add up to the amount are left as read', whole.allocations.find(a => a.purchaseOrderId === 1).amount === 23147.2, whole.allocations);
        const partial = matchLinesToPos({ lines: [lines[0]], poRefs: [] }, pos, 34158);
        check('lines short of the amount are not stretched — the rest stays unallocated', partial.allocations[0].amount === 23147.2, partial.allocations);
    }

    section('an invoice checked line by line against the container (pure)');
    {
        const context = {
            shipment: { reference: '324' }, shipmentCurrency: 'USD',
            purchaseOrders: [
                { id: 1, poNumber: 'PO_00333J', supplier: 'Suzhou Sunmed Co., Ltd', supplierKey: 'suzhou sunmed co., ltd', currency: 'USD', valueInShipment: 23147.2, deposit: { amountDue: 6944.16, depositPercentage: 30, paymentStatus: 'paid' },
                  lines: [{ jfCode: 'JF-GLV-M', productName: 'Nitrile gloves M', quantity: 18400, unitPrice: 1.258 }] },
                { id: 2, poNumber: 'PO_00297J', supplier: 'Suzhou Sunmed Co., Ltd', supplierKey: 'suzhou sunmed co., ltd', currency: 'USD', valueInShipment: 11011.08, deposit: { amountDue: 3303.32, depositPercentage: 30, paymentStatus: 'paid' },
                  lines: [{ jfCode: 'JF-GLV-L', productName: 'Nitrile gloves L', quantity: 8900, unitPrice: 1.2372 }] },
                { id: 3, poNumber: 'PO_00999Z', supplier: 'Someone Else', supplierKey: 'someone else', currency: 'USD', valueInShipment: 500, deposit: null,
                  lines: [{ jfCode: 'JF-OTHER', productName: 'Other', quantity: 10, unitPrice: 50 }] },
            ],
        };
        const compare = (lines, extra = {}) => (typeof compareInvoiceToContainer === 'function'
            ? compareInvoiceToContainer({ extract: { lines, totalAmount: 34158.28, amountDueNow: 23910.8, depositDeducted: 10247.48, currency: 'USD', ...extra }, context, supplierName: 'Suzhou Sunmed Co., Ltd' })
            : { verdict: 'compareInvoiceToContainer is not exported' });
        const good = compare([
            { poRef: 'PO00333J', jfCode: 'JF-GLV-M', description: 'Nitrile gloves M', qty: 18400, unitPrice: 1.258, amount: 23147.2 },
            { poRef: 'PO 00297J', jfCode: 'JF-GLV-L', description: 'Nitrile gloves L', qty: 8900, unitPrice: 1.2372, amount: 11011.08 },
        ]);
        check('lines that agree with the container → match', good.verdict === 'match', good);
        check('every container line for this supplier was found on the invoice', good.matchedLines === 2 && good.missingOurLines.length === 0 && good.extraInvoiceLines.length === 0, good);
        check('goods on board are this supplier\'s only', good.goodsOnBoard === 34158.28, good.goodsOnBoard);
        check('expected balance = goods less the deposit % on file', Math.abs(good.expectedBalance - 23910.8) < 0.01 && good.depositBasis === 'pi_percentage', { e: good.expectedBalance, b: good.depositBasis });
        check('and the invoice asks for that', good.balanceDelta === 0, good.balanceDelta);
        const off = compare([
            { poRef: 'PO00333J', jfCode: 'JF-GLV-M', description: 'Nitrile gloves M', qty: 18000, unitPrice: 1.3, amount: 23400 },
            { poRef: 'PO 00297J', jfCode: 'JF-GLV-L', description: 'Nitrile gloves L', qty: 8900, unitPrice: 1.2372, amount: 11011.08 },
        ], { totalAmount: 34411.08, amountDueNow: 24087.76 });
        check('a line with a different qty and price → differs', off.verdict === 'differs', off.verdict);
        const disc = off.lines.find(l => l.jfCode === 'JF-GLV-M');
        check('naming ours and theirs', disc && disc.ourQty === 18400 && disc.invoiceQty === 18000 && disc.ourUnitPrice === 1.258 && disc.invoiceUnitPrice === 1.3 && disc.issues.length >= 2, disc);
        check('the balance is out by the difference', Math.abs(off.balanceDelta - (24087.76 - 23910.8)) < 0.01, off.balanceDelta);
        const missing = compare([{ poRef: 'PO00333J', jfCode: 'JF-GLV-M', description: 'Nitrile gloves M', qty: 18400, unitPrice: 1.258, amount: 23147.2 }]);
        check('a container line the invoice does not bill is reported', missing.missingOurLines.length === 1 && missing.missingOurLines[0].jfCode === 'JF-GLV-L' && missing.verdict === 'differs', missing.missingOurLines);
        const stranger = compare([{ poRef: null, jfCode: 'JF-NOPE', description: 'Something', qty: 1, unitPrice: 5, amount: 5 }]);
        check('an invoice line not in the container is reported', stranger.extraInvoiceLines.length === 1, stranger.extraInvoiceLines);
        const byDesc = compare([{ poRef: 'PO00333J', jfCode: null, description: 'nitrile gloves m', qty: 18400, unitPrice: 1.258, amount: 23147.2 }, { poRef: 'PO 00297J', jfCode: null, description: 'Nitrile Gloves L', qty: 8900, unitPrice: 1.2372, amount: 11011.08 }]);
        check('a line with no code still matches on PO + description', byDesc.matchedLines === 2 && byDesc.verdict === 'match', byDesc);
        const noLines = compare([]);
        check('no lines to compare → unverified, totals only', noLines.verdict === 'unverified' && noLines.balanceDelta === 0, noLines);
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

    section('supplier payments — one transfer applied to several obligations');
    const remittance = (amount, extra = {}) => ({
        documentKind: 'remittance', supplierName: fixture.supplier, currency: 'USD',
        amountDueNow: amount, totalAmount: amount, paymentDate: '2026-09-18', invoiceDate: null, dueDate: null,
        invoiceNumber: null, containerRefs: [], blRefs: [], poRefs: [], lines: [],
        bank: { paymentReference: `${INVOICE_TAG}-FX` }, ...extra,
    });
    const onFixture = { shipmentId: fixture.id, reference: fixture.reference, supplier: fixture.supplier };
    const pay = (body) => api.post('/api/v1/supplier-payments', body);
    const openItems = async (supplier, currency = 'USD') =>
        api.get(`/api/v1/supplier-payments/open-items?supplier=${encodeURIComponent(supplier)}&currency=${currency}`);
    const balanceRow = async (id) => (await api.get(`/api/v1/shipment-payments?shipmentId=${fixture.id}`)).data.data.find(d => d.id === id) || {};

    check('the old one-balance mark-paid route is gone → 404',
        (await api.post('/api/v1/shipment-payment-documents/1/mark-paid', {})).status === 404);

    // Two open balances on the fixture shipment, as two containers would give.
    const balA = await post({ ...onFixture, supplierName: fixture.supplier, amount: 5000, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-A1`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    const balB = await post({ ...onFixture, supplierName: fixture.supplier, amount: 800, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-B1`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    for (const r of [balA, balB]) if (r.data && r.data.id) createdIds.push(r.data.id);
    check('two open balances to pay', balA.status === 201 && balB.status === 201, { a: balA.data, b: balB.data });

    const items = await openItems(fixture.supplier);
    check('open items → 200', items.status === 200, items.data);
    const itemA = (items.data.items || []).find(i => i.kind === 'balance' && i.id === balA.data.id);
    check('lists the open balances with what is left to pay', itemA && itemA.remaining === 5000 && itemA.applied === 0, itemA);
    check('each item is labelled for the checklist', itemA && /\d/.test(itemA.label || '') && itemA.currency === 'USD', itemA);
    check('a pending balance with no split yet is still an open item', (items.data.items || []).some(i => i.kind === 'balance' && i.id === rec.id), items.data.items && items.data.items.map(i => i.id));
    await put(rec.id, { status: 'skipped' });
    const skippedItems = await openItems(fixture.supplier);
    check('a skipped balance is not', !(skippedItems.data.items || []).some(i => i.kind === 'balance' && i.id === rec.id), skippedItems.data.items && skippedItems.data.items.map(i => i.id));
    await put(rec.id, { status: 'pending' });

    const both = await pay({
        supplierName: fixture.supplier, amount: 5800, currency: 'USD', paidOn: '2026-09-19', bankRef: `${INVOICE_TAG}-TT1`,
        lines: [{ kind: 'balance', id: balA.data.id, amount: 5000 }, { kind: 'balance', id: balB.data.id, amount: 800 }],
    });
    check('one transfer covering two balances → 201', both.status === 201, both.data);
    if (both.data && both.data.id) paymentIds.push(both.data.id);
    check('it carries its lines, labelled', both.data.lines && both.data.lines.length === 2 && both.data.lines.every(l => l.label), both.data.lines);
    check('nothing unexplained', both.data.unexplained === 0, both.data.unexplained);
    const paidA = await balanceRow(balA.data.id);
    check('the first balance is paid', paidA.status === 'paid', paidA.status);
    check('on the date the money went', paidA.paidOn === '2026-09-19', paidA.paidOn);
    check('with the bank reference', paidA.bankRef === `${INVOICE_TAG}-TT1`, paidA.bankRef);
    check('and knows which transfer settled it', paidA.settledByPaymentId === both.data.id && paidA.appliedTotal === 5000, { s: paidA.settledByPaymentId, a: paidA.appliedTotal });
    check('the second balance is paid too', (await balanceRow(balB.data.id)).status === 'paid');
    const feed1 = await api.get(`/api/v1/supplier-payments?supplier=${encodeURIComponent(fixture.supplier)}`);
    check('the payments feed lists it', feed1.status === 200 && feed1.data.data.some(p => p.id === both.data.id), feed1.data);
    check('the balance knows the transfer that settled it, and what else it covered',
        Array.isArray(paidA.settlements) && paidA.settlements.length === 1 && paidA.settlements[0].paymentId === both.data.id
        && paidA.settlements[0].amount === 5000 && paidA.settlements[0].paidOn === '2026-09-19'
        && paidA.settlements[0].also.some(x => x.kind === 'balance' && x.label === fixture.reference && x.amount === 800),
        paidA.settlements);
    const paidAudit = await sql(`SELECT action, after_json FROM audit_log WHERE entity_type = 'shipment_payment' AND entity_id = ? ORDER BY id DESC LIMIT 1`, [balA.data.id]);
    check('settling a balance is audited as a status change by payment', paidAudit[0] && paidAudit[0].action === 'status' && /"via":"payment"/.test(JSON.stringify(paidAudit[0].after_json)), paidAudit[0]);

    section('partial payments');
    const balC = await post({ ...onFixture, supplierName: fixture.supplier, amount: 1000, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-C1`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    if (balC.data && balC.data.id) createdIds.push(balC.data.id);
    const part = await pay({ supplierName: fixture.supplier, amount: 400, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: balC.data.id, amount: 400 }] });
    check('a part payment → 201', part.status === 201, part.data);
    if (part.data && part.data.id) paymentIds.push(part.data.id);
    const partly = await balanceRow(balC.data.id);
    check('the balance stays pending', partly.status === 'pending', partly.status);
    check('but shows what has been applied', partly.appliedTotal === 400 && partly.remaining === 600, { a: partly.appliedTotal, r: partly.remaining });
    const itemC = ((await openItems(fixture.supplier)).data.items || []).find(i => i.kind === 'balance' && i.id === balC.data.id);
    check('open items show the remainder', itemC && itemC.remaining === 600 && itemC.applied === 400, itemC);
    const tooMuch = await pay({ supplierName: fixture.supplier, amount: 700, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: balC.data.id, amount: 700 }] });
    check('applying more than is left → 422 OVER_APPLIED', tooMuch.status === 422 && tooMuch.data.code === 'OVER_APPLIED', tooMuch.data);
    if (tooMuch.status === 201 && tooMuch.data.id) paymentIds.push(tooMuch.data.id);
    const rest = await pay({ supplierName: fixture.supplier, amount: 600, currency: 'USD', paidOn: '2026-09-20', lines: [{ kind: 'balance', id: balC.data.id, amount: 600 }] });
    check('the rest → 201', rest.status === 201, rest.data);
    if (rest.data && rest.data.id) paymentIds.push(rest.data.id);
    const done = await balanceRow(balC.data.id);
    check('now paid, by the transfer that completed it', done.status === 'paid' && done.settledByPaymentId === rest.data.id && done.paidOn === '2026-09-20', { s: done.status, by: done.settledByPaymentId, on: done.paidOn });
    const within = await post({ ...onFixture, supplierName: fixture.supplier, amount: 300, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-D1`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    if (within.data && within.data.id) createdIds.push(within.data.id);
    const charge = await pay({ supplierName: fixture.supplier, amount: 299.5, currency: 'USD', paidOn: '2026-09-20', lines: [{ kind: 'balance', id: within.data.id, amount: 299.5 }] });
    if (charge.data && charge.data.id) paymentIds.push(charge.data.id);
    check('a bank charge short of the balance still settles it', charge.status === 201 && (await balanceRow(within.data.id)).status === 'paid', charge.data);

    section('validation');
    const noLines = await pay({ supplierName: fixture.supplier, amount: 10, currency: 'USD', paidOn: '2026-09-19', lines: [] });
    check('no lines → 400', noLines.status === 400, noLines.data);
    const badKind = await pay({ supplierName: fixture.supplier, amount: 10, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'gift', id: 1, amount: 10 }] });
    check('unknown line kind → 400', badKind.status === 400, badKind.data);
    const noDate = await pay({ supplierName: fixture.supplier, amount: 10, currency: 'USD', lines: [{ kind: 'balance', id: balA.data.id, amount: 10 }] });
    check('no date sent → 400', noDate.status === 400, noDate.data);
    const ghost = await pay({ supplierName: fixture.supplier, amount: 10, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: 999999, amount: 10 }] });
    check('a balance that does not exist → 404 TARGET_NOT_FOUND', ghost.status === 404 && ghost.data.code === 'TARGET_NOT_FOUND', ghost.data);
    const balE = await post({ ...onFixture, supplierName: fixture.supplier, amount: 50, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-E1`, allocate: { mode: 'share', purchaseOrderIds: pos.map(p => p.id) } });
    if (balE.data && balE.data.id) createdIds.push(balE.data.id);
    const gbp = await pay({ supplierName: fixture.supplier, amount: 50, currency: 'GBP', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: balE.data.id, amount: 50 }] });
    check('a transfer in another currency → 422 CURRENCY_MISMATCH', gbp.status === 422 && gbp.data.code === 'CURRENCY_MISMATCH', gbp.data);
    if (gbp.status === 201 && gbp.data.id) paymentIds.push(gbp.data.id);
    const overAlloc = await pay({ supplierName: fixture.supplier, amount: 20, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: balE.data.id, amount: 50 }] });
    check('lines adding up to more than was sent → 422 OVER_ALLOCATED', overAlloc.status === 422 && overAlloc.data.code === 'OVER_ALLOCATED', overAlloc.data);
    if (overAlloc.status === 201 && overAlloc.data.id) paymentIds.push(overAlloc.data.id);
    const dupLine = await pay({ supplierName: fixture.supplier, amount: 50, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: balE.data.id, amount: 25 }, { kind: 'balance', id: balE.data.id, amount: 25 }] });
    check('the same obligation twice → 400', dupLine.status === 400, dupLine.data);
    if (dupLine.status === 201 && dupLine.data.id) paymentIds.push(dupLine.data.id);
    const unexplained = await pay({ supplierName: fixture.supplier, amount: 80, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: balE.data.id, amount: 50 }] });
    check('sending more than the lines explain is allowed and reported', unexplained.status === 201 && unexplained.data.unexplained === 30, unexplained.data);
    if (unexplained.data && unexplained.data.id) paymentIds.push(unexplained.data.id);
    const unsplit = await post({ ...onFixture, supplierName: fixture.supplier, amount: 90, currency: 'USD', invoiceNumber: `${INVOICE_TAG}-F1` });
    if (unsplit.data && unsplit.data.id) createdIds.push(unsplit.data.id);
    const cannotSettle = await pay({ supplierName: fixture.supplier, amount: 90, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'balance', id: unsplit.data.id, amount: 90 }] });
    check('settling a balance not split across POs → 422 NOT_FULLY_ALLOCATED', cannotSettle.status === 422 && cannotSettle.data.code === 'NOT_FULLY_ALLOCATED', cannotSettle.data);
    if (cannotSettle.status === 201 && cannotSettle.data.id) paymentIds.push(cannotSettle.data.id);

    section('open items say what each is for, and what is already paid');
    const ctxItems = (await openItems(fixture.supplier)).data.items || [];
    const withSplit = ctxItems.find(i => i.kind === 'balance' && i.id === within.data.id) || ctxItems.find(i => i.kind === 'balance' && i.allocations && i.allocations.length);
    const openC = ctxItems.find(i => i.kind === 'balance' && i.id === unsplit.data.id);
    check('an open balance carries its per-PO split', openC && Array.isArray(openC.allocations), openC);
    const paidItem = ctxItems.find(i => i.kind === 'balance' && i.id === balA.data.id);
    check('a paid balance is listed for context, not to tick', paidItem && paidItem.open === false && paidItem.status === 'paid' && paidItem.paidOn === '2026-09-19', paidItem);
    check('an open balance is open', openC && openC.open === true, openC && openC.open);
    const splitItem = ctxItems.find(i => i.kind === 'balance' && i.allocations && i.allocations.length > 0);
    check('a split names the PO and how much of it is on board', splitItem && splitItem.allocations.every(a => a.poNumber && a.linesOnBoard >= 1 && a.linesTotal >= a.linesOnBoard), splitItem && splitItem.allocations);
    check('and whether that PO\'s deposit is settled', splitItem && splitItem.allocations.every(a => 'deposit' in a), splitItem && splitItem.allocations && splitItem.allocations[0]);

    section('paying for what is in a container before any balance is recorded');
    const [noBalanceShipment] = await sql(
        `SELECT s.id, s.reference, po.supplier, COUNT(DISTINCT po.id) AS pos
           FROM shipments s
           JOIN orders o ON o.shipment_id = s.id AND o.deleted_at IS NULL
           JOIN purchase_orders po ON po.id = o.purchase_order_id AND po.deleted_at IS NULL
          WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL AND s.reference IS NOT NULL
            AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED') AND s.id <> ?
            AND NOT EXISTS (SELECT 1 FROM shipment_payments b WHERE b.shipment_id = s.id AND b.deleted_at IS NULL)
          GROUP BY s.id, po.supplier
         HAVING pos >= 1
          ORDER BY pos DESC, s.id LIMIT 1`,
        [fixture.id]
    );
    if (noBalanceShipment) {
        const cb = await pay({
            supplierName: noBalanceShipment.supplier, amount: 700, currency: 'USD', paidOn: '2026-09-19',
            lines: [{ kind: 'container_balance', id: noBalanceShipment.id, amount: 700, balanceAmount: 1000 }],
        });
        check('a projected container balance can be paid → 201', cb.status === 201, cb.data);
        if (cb.data && cb.data.id) paymentIds.push(cb.data.id);
        const madeLine = cb.data.lines && cb.data.lines[0];
        check('the line became a balance record', madeLine && madeLine.kind === 'balance' && madeLine.shipmentId === noBalanceShipment.id, madeLine);
        if (madeLine) createdIds.push(madeLine.targetId);
        const made = madeLine ? (await api.get(`/api/v1/shipment-payments?shipmentId=${noBalanceShipment.id}`)).data.data.find(b => b.id === madeLine.targetId) : null;
        check('recorded for the projected amount, split across the supplier\'s POs on board', made && made.amount === 1000 && made.fullyAllocated, made && { a: made.amount, f: made.fullyAllocated, s: made.allocations && made.allocations.map(x => x.poNumber) });
        check('part paid: pending with the rest still owed', made && made.status === 'pending' && made.appliedTotal === 700 && made.remaining === 300, made && { s: made.status, a: made.appliedTotal, r: made.remaining });
        const again = await pay({
            supplierName: noBalanceShipment.supplier, amount: 300, currency: 'USD', paidOn: '2026-09-20',
            lines: [{ kind: 'container_balance', id: noBalanceShipment.id, amount: 300, balanceAmount: 1000 }],
        });
        check('a second projected balance for a box that has an open one → 409 BALANCE_EXISTS naming it', again.status === 409 && again.data.code === 'BALANCE_EXISTS' && again.data.balanceId === madeLine.targetId, again.data);
        if (again.status === 201 && again.data.id) paymentIds.push(again.data.id);
        const restCb = await pay({
            supplierName: noBalanceShipment.supplier, amount: 300, currency: 'USD', paidOn: '2026-09-20',
            lines: [{ kind: 'balance', id: madeLine.targetId, amount: 300 }],
        });
        check('paying the rest against that balance settles it', restCb.status === 201 && (await api.get(`/api/v1/shipment-payments?shipmentId=${noBalanceShipment.id}`)).data.data.find(b => b.id === madeLine.targetId).status === 'paid', restCb.data);
        if (restCb.data && restCb.data.id) paymentIds.push(restCb.data.id);
        const full = await pay({
            supplierName: noBalanceShipment.supplier, amount: 50, currency: 'USD', paidOn: '2026-09-20',
            lines: [{ kind: 'container_balance', id: noBalanceShipment.id, amount: 50 }],
        });
        check('with no projected amount the balance is what was paid, and paid', full.status === 201 && full.data.lines[0].targetAmount === 50 && full.data.lines[0].targetStatus === 'paid', full.data && full.data.lines);
        if (full.data && full.data.id) { paymentIds.push(full.data.id); if (full.data.lines[0]) createdIds.push(full.data.lines[0].targetId); }
    } else {
        console.log('  (no booked shipment without a balance — container_balance not exercised)');
    }
    const draftForCb = await sql(`SELECT id FROM shipments WHERE stage IN ('DRAFT','PLANNED') AND deleted_at IS NULL AND merged_into_id IS NULL LIMIT 1`);
    if (draftForCb[0]) {
        const notBooked = await pay({ supplierName: fixture.supplier, amount: 5, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'container_balance', id: draftForCb[0].id, amount: 5 }] });
        check('a container not yet booked → 422 NOT_BOOKED', notBooked.status === 422 && notBooked.data.code === 'NOT_BOOKED', notBooked.data);
        if (notBooked.status === 201 && notBooked.data.id) paymentIds.push(notBooked.data.id);
    }

    section('a deposit with no PI, and a deposit PI');
    const dep = await pay({ supplierName: fixture.supplier, amount: 250, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'po_deposit', id: pos[0].id, amount: 250 }] });
    check('a deposit on a PO with no PI → 201', dep.status === 201, dep.data);
    if (dep.data && dep.data.id) paymentIds.push(dep.data.id);
    check('its line names the PO', dep.data.lines && dep.data.lines[0].label === pos[0].poNumber && dep.data.lines[0].poNumber === pos[0].poNumber, dep.data.lines);
    const poPayments = await api.get(`/api/v1/purchase-orders/${pos[0].id}/payments`);
    check('the PO page sees the deposit paid by transfer', poPayments.status === 200 && Array.isArray(poPayments.data.transfers)
        && poPayments.data.transfers.some(t => t.paymentId === dep.data.id && t.kind === 'po_deposit' && t.amount === 250 && t.paidOn === '2026-09-19'), poPayments.data.transfers);
    const strangerPo = await pay({ supplierName: fixture.supplier, amount: 5, currency: 'USD', paidOn: '2026-09-19', lines: [{ kind: 'po_deposit', id: 999999, amount: 5 }] });
    check('a PO that does not exist → 404', strangerPo.status === 404, strangerPo.data);
    const [piRow] = await sql(`
        SELECT p.id, p.purchase_order_id, p.amount_due, p.currency, po.supplier, po.po_number
          FROM purchase_order_invoice_payments p
          JOIN purchase_orders po ON po.id = p.purchase_order_id AND po.deleted_at IS NULL
          JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
         WHERE p.payment_status = 'pending' AND p.amount_due > 1 AND p.settled_by_payment_id IS NULL
         ORDER BY p.id DESC LIMIT 1`);
    if (piRow) {
        piFixture = piRow;
        const due = Number(piRow.amount_due);
        const half = Math.round(due * 50) / 100;
        const piItems = await openItems(piRow.supplier, piRow.currency);
        const piItem = (piItems.data.items || []).find(i => i.kind === 'pi' && i.id === piRow.id);
        check('a pending PI is an open item for its supplier', piItem && piItem.remaining === due && piItem.poNumber === piRow.po_number, piItem);
        const piPart = await pay({ supplierName: piRow.supplier, amount: half, currency: piRow.currency, paidOn: '2026-09-19', lines: [{ kind: 'pi', id: piRow.id, amount: half }] });
        check('half the PI → 201', piPart.status === 201, piPart.data);
        if (piPart.data && piPart.data.id) paymentIds.push(piPart.data.id);
        const [stillPending] = await sql(`SELECT payment_status FROM purchase_order_invoice_payments WHERE id = ?`, [piRow.id]);
        check('the PI stays pending', stillPending.payment_status === 'pending', stillPending);
        const piRest = await pay({ supplierName: piRow.supplier, amount: Math.round((due - half) * 100) / 100, currency: piRow.currency, paidOn: '2026-09-20', lines: [{ kind: 'pi', id: piRow.id, amount: Math.round((due - half) * 100) / 100 }] });
        check('the other half → 201', piRest.status === 201, piRest.data);
        if (piRest.data && piRest.data.id) paymentIds.push(piRest.data.id);
        const [nowPaid] = await sql(`SELECT payment_status, settled_by_payment_id FROM purchase_order_invoice_payments WHERE id = ?`, [piRow.id]);
        check('the PI is paid — the same chip Purchase Orders shows', nowPaid.payment_status === 'paid' && nowPaid.settled_by_payment_id === piRest.data.id, nowPaid);
        const piAudit = await sql(`SELECT action FROM audit_log WHERE entity_type = 'purchase_order' AND entity_id = ? ORDER BY id DESC LIMIT 1`, [piRow.purchase_order_id]);
        check('audited on the PO like a chip flip', piAudit[0] && piAudit[0].action === 'invoice_payment_status', piAudit[0]);
        const poInvoices = await api.get(`/api/v1/purchase-orders/${piRow.purchase_order_id}/invoices`);
        const inv = (poInvoices.data.data || []).find(i => i.payment && i.payment.id === piRow.id);
        check('the PO page sees both transfers on the PI, newest first', inv && inv.payment.settlements && inv.payment.settlements.length === 2
            && inv.payment.settlements[0].paymentId === piRest.data.id && inv.payment.settlements[1].paymentId === piPart.data.id, inv && inv.payment && inv.payment.settlements);
        const poPays = await api.get(`/api/v1/purchase-orders/${piRow.purchase_order_id}/payments`);
        check('and under the PO\'s payments', (poPays.data.transfers || []).filter(t => t.kind === 'pi').length === 2, poPays.data.transfers);
        if (isAdmin) {
            const undo = await api.delete(`/api/v1/supplier-payments/${piRest.data.id}`);
            const [reverted] = await sql(`SELECT payment_status, settled_by_payment_id FROM purchase_order_invoice_payments WHERE id = ?`, [piRow.id]);
            check('deleting the transfer puts the PI back to pending', undo.status === 204 && reverted.payment_status === 'pending' && reverted.settled_by_payment_id == null, reverted);
        }
    } else {
        console.log('  (no pending PI on this database — PI application not exercised)');
    }

    section('a proof of payment attached to the transfer');
    const proof = await insertDocument({ shipmentId: null, reference: null, supplier: fixture.supplier, extracted: remittance(5800) });
    const loose = await api.get(`/api/v1/supplier-payments?supplier=${encodeURIComponent(fixture.supplier)}`);
    check('an unapplied proof is listed for its supplier, shipment or not', (loose.data.documents || []).some(d => d.id === proof), loose.data.documents && loose.data.documents.map(d => d.id));
    const withProof = await pay({ supplierName: fixture.supplier, amount: 1, currency: 'USD', paidOn: '2026-09-19', documentId: proof, lines: [{ kind: 'po_deposit', id: pos[0].id, amount: 1 }] });
    check('recording with the proof → 201', withProof.status === 201, withProof.data);
    if (withProof.data && withProof.data.id) paymentIds.push(withProof.data.id);
    check('the proof is on the transfer', (withProof.data.documents || []).some(d => d.id === proof), withProof.data.documents);
    const proofOnPo = await api.get(`/api/v1/purchase-orders/${pos[0].id}/payments`);
    check('the proof follows the transfer to the PO it covered',
        (proofOnPo.data.transfers || []).some(t => t.paymentId === withProof.data.id && (t.documents || []).some(d => d.id === proof)), proofOnPo.data.transfers);
    const again = await pay({ supplierName: fixture.supplier, amount: 1, currency: 'USD', paidOn: '2026-09-19', documentId: proof, lines: [{ kind: 'po_deposit', id: pos[0].id, amount: 1 }] });
    check('the same proof on a second transfer → 409 DOCUMENT_LINKED', again.status === 409 && again.data.code === 'DOCUMENT_LINKED', again.data);
    if (again.status === 201 && again.data.id) paymentIds.push(again.data.id);
    const loose2 = await api.get(`/api/v1/supplier-payments?supplier=${encodeURIComponent(fixture.supplier)}`);
    check('an applied proof is no longer loose', !(loose2.data.documents || []).some(d => d.id === proof), loose2.data.documents && loose2.data.documents.map(d => d.id));

    section('editing and deleting a transfer');
    const edited = await api.put(`/api/v1/supplier-payments/${both.data.id}`, { amount: 5000, lines: [{ kind: 'balance', id: balA.data.id, amount: 5000 }] });
    check('dropping a line → 200', edited.status === 200 && edited.data.lines.length === 1, edited.data);
    check('the balance it no longer covers goes back to pending', (await balanceRow(balB.data.id)).status === 'pending');
    check('the one it still covers stays paid', (await balanceRow(balA.data.id)).status === 'paid');
    const del2 = await api.delete(`/api/v1/supplier-payments/${both.data.id}`);
    if (isAdmin) {
        check('delete → 204', del2.status === 204, del2.status);
        const backA = await balanceRow(balA.data.id);
        check('deleting the transfer reverts the balance it settled', backA.status === 'pending' && backA.paidOn === null && backA.settledByPaymentId === null && backA.appliedTotal === 0, backA);
        check('deleting twice → 404', (await api.delete(`/api/v1/supplier-payments/${both.data.id}`)).status === 404);
    } else {
        check('delete is admin-only → 403', del2.status === 403, del2.data);
    }


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
    const leftoverPay = await sql(`SELECT COUNT(*) AS n FROM supplier_payments WHERE bank_ref LIKE ? OR id IN (${paymentIds.length ? paymentIds.map(() => '?').join(',') : '0'})`, [`${INVOICE_TAG}%`, ...paymentIds]);
    check('cleanup left no transfers behind', Number(leftoverPay[0].n) === 0, leftoverPay[0]);

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
