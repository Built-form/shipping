'use strict';

// Unit tests for the payment proof rules (src/lib/payment-notices.js;
// docs/payment-proof-send.md): which email a line of a transfer belongs to,
// the amounts, what a supplier may be told about a line, which proofs go out,
// when a transfer cannot be sent, the state of a notice, the links and the
// fixed blocks. Pure: no database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const N = require('../src/lib/payment-notices');

const SUPPLIER = 'Acme Medical Co., Ltd';

// A line as the notice loader hands it over (src/services/payment-notices.js).
const line = (kind, amount, target = {}, extra = {}) => ({
    id: extra.id ?? 1, kind, targetId: extra.targetId ?? 10, amount, paidWith: extra.paidWith ?? null,
    target: target === null ? null : {
        paymentType: null, poId: null, poNumber: null, poSupplier: null,
        shipmentId: null, shipmentReference: null, shipmentSupplier: null,
        invoiceNumber: null, topUp: false, jfCode: null, extraKind: null, ridesWith: null, extraSupplier: null,
        ...target,
    },
});
const po = (n, over = {}) => ({ poId: 100 + n, poNumber: `PO_00${n}J`, poSupplier: SUPPLIER, ...over });
const box = (ref, over = {}) => ({ shipmentId: Number(String(ref).replace(/\D.*$/, '')) + 5000, shipmentReference: ref, shipmentSupplier: SUPPLIER, ...over });
const proof = (over = {}) => ({
    id: 77, docKind: 'remittance', supplierName: SUPPLIER, filename: 'proof.pdf',
    url: 'https://shipping-purchase-orders-test.s3.eu-north-1.amazonaws.com/shipment-payments/u1/proof.pdf',
    s3Key: 'shipment-payments/u1/proof.pdf', contentType: 'application/pdf', fileSize: 120000, ...over,
});
const source = (lines, over = {}) => ({
    payment: { id: 45, supplierName: SUPPLIER, supplierKey: 'acme medical co., ltd', amount: lines.reduce((a, l) => a + l.amount, 0), currency: 'USD', paidOn: '2026-11-14', ...(over.payment || {}) },
    lines,
    documents: over.documents ?? [proof()],
});

// ── Which email a line belongs to ─────────────────────────────────────────

test('the notice of a line: every kind, as the plan\'s table has it', () => {
    const k = (l, both = false) => N.noticeKindOfLine(l, { both });
    assert.equal(k({ kind: 'po_deposit' }), 'deposit');
    assert.equal(k({ kind: 'pi', paymentType: 'deposit' }), 'deposit');
    assert.equal(k({ kind: 'pi', paymentType: 'balance' }), 'balance');
    assert.equal(k({ kind: 'balance' }), 'balance');
    assert.equal(k({ kind: 'balance', targetSource: 'top_up' }), 'balance');
    assert.equal(k({ kind: 'qc' }), 'balance');
    assert.equal(k({ kind: 'repackaged' }), 'balance');
    assert.equal(k({ kind: 'extra', ridesWith: 'deposit' }), 'deposit');
    assert.equal(k({ kind: 'extra', ridesWith: 'balance' }), 'balance');
    // A forwarder's charge is not a supplier's payment at all.
    assert.equal(k({ kind: 'extra', ridesWith: 'shipment' }), 'forwarder');
});

test('a 100% PI is a balance when its money was ticked against containers, a deposit otherwise', () => {
    const full = paidWith => N.noticeKindOfLine({ kind: 'pi', paymentType: 'full', paidWith });
    assert.equal(full([{ shipmentReference: '333', amount: 100 }]), 'balance');
    // null and an empty list both mean "against no container".
    assert.equal(full(null), 'deposit');
    assert.equal(full([]), 'deposit');
    assert.equal(full(undefined), 'deposit');
});

test('a credit (an extra on the account) rides with the transfer\'s only kind, and with the balance when it has both', () => {
    const credit = line('extra', -50, { ridesWith: 'account', extraKind: 'credit', extraSupplier: SUPPLIER });
    const dep = line('po_deposit', 1000, po(1), { id: 2, targetId: 101 });
    const bal = line('balance', 2000, box('333'), { id: 3, targetId: 9 });
    const only = lines => N.buildNotices(source(lines)).notices.map(n => [n.kind, n.amount, n.lines.length]);
    assert.deepEqual(only([dep, credit]), [['deposit', 950, 2]]);
    assert.deepEqual(only([bal, credit]), [['balance', 1950, 2]]);
    assert.deepEqual(only([dep, bal, credit]), [['deposit', 1000, 1], ['balance', 1950, 2]]);
});

// ── One or two emails, and their figures ──────────────────────────────────

test('deposit only is one email, balance and QC is one, both together are two: deposit first', () => {
    const dep = line('po_deposit', 3000, po(1), { id: 1, targetId: 101 });
    const piDep = line('pi', 1500.5, { ...po(2), paymentType: 'deposit' }, { id: 2, targetId: 55 });
    const bal = line('balance', 8122.4, { ...box('122. Air Freight'), invoiceNumber: '26/89132' }, { id: 3, targetId: 9 });
    const qc = line('qc', 124.1, { ...po(3), jfCode: 'JF0938_FQC' }, { id: 4, targetId: 7001 });

    const d = N.buildNotices(source([dep, piDep]));
    assert.deepEqual(d.notices.map(n => [n.kind, n.amount]), [['deposit', 4500.5]]);
    const b = N.buildNotices(source([bal, qc]));
    assert.deepEqual(b.notices.map(n => [n.kind, n.amount]), [['balance', 8246.5]]);
    const both = N.buildNotices(source([qc, bal, piDep, dep]));
    assert.deepEqual(both.notices.map(n => [n.kind, n.amount]), [['deposit', 4500.5], ['balance', 8246.5]]);
    assert.equal(both.sendable, true);
    assert.equal(both.blocked, null);
    assert.deepEqual(both.transfer, { amount: 12747, currency: 'USD', paidOn: '2026-11-14', linesTotal: 12747, notItemised: 0, roundingDifference: 0 });
});

test('amounts are added in minor units: no float drift, and the parts add up to the transfer', () => {
    const lines = [0.1, 0.2, 0.3, 1000.07, 2000.05].map((a, i) => line('balance', a, box(String(300 + i)), { id: i + 1, targetId: i + 1 }));
    const b = N.buildNotices(source(lines));
    assert.equal(b.notices[0].amount, 3000.72);
    assert.equal(b.transfer.linesTotal, 3000.72);
});

test('money no line explains is never spread: it is said as its own figure', () => {
    const lines = [line('po_deposit', 1000, po(1)), line('balance', 2000, box('333'), { id: 2, targetId: 9 })];
    const b = N.buildNotices(source(lines, { payment: { amount: 3004.66 } }));
    assert.deepEqual(b.notices.map(n => n.amount), [1000, 2000]);
    assert.equal(b.transfer.notItemised, 4.66);
    assert.equal(b.transfer.roundingDifference, 0);
    assert.equal(b.sendable, true);
});

test('lines one cent above the transfer are a rounding difference; more than that cannot be sent', () => {
    const lines = [line('balance', 1000.01, box('333'))];
    const cent = N.buildNotices(source(lines, { payment: { amount: 1000 } }));
    assert.equal(cent.sendable, true);
    assert.deepEqual([cent.transfer.notItemised, cent.transfer.roundingDifference], [0, -0.01]);
    const two = N.buildNotices(source(lines, { payment: { amount: 999.99 } }));
    assert.equal(two.sendable, false);
    assert.equal(two.blocked.code, 'LINES_EXCEED_TRANSFER');
});

// ── What cannot be sent ───────────────────────────────────────────────────

test('nothing sent, a forwarder, a deleted target, no lines: the transfer cannot be sent, and says why', () => {
    const dep = line('po_deposit', 1000, po(1));
    const code = (lines, over) => { const b = N.buildNotices(source(lines, over)); return [b.sendable, b.blocked && b.blocked.code]; };
    // Paid by credit: no money went, so there is no proof to send.
    assert.deepEqual(code([dep, line('extra', -1000, { ridesWith: 'account', extraSupplier: SUPPLIER }, { id: 2 })], { payment: { amount: 0 } }), [false, 'NOTHING_SENT']);
    // A forwarder's charge whose payee was typed, not picked from the Forwarders list
    // (2026-11-20: a forwarder's payment is sent to the forwarder; tools/test-forwarder-payment-notices-lib.js).
    assert.deepEqual(code([line('extra', 900, { ridesWith: 'shipment', extraKind: 'freight', extraSupplier: 'DCG' })]), [false, 'FORWARDER_NOT_LINKED']);
    assert.deepEqual(code([dep, line('balance', 500, null, { id: 2, targetId: 9 })]), [false, 'LINE_TARGET_MISSING']);
    assert.deepEqual(code([], { payment: { amount: 100 } }), [false, 'NO_LINES']);
    assert.deepEqual(code([dep]), [true, null]);
});

test('a notice at zero or below is not sent; the other one still is', () => {
    const dep = line('po_deposit', 1000, po(1));
    const bal = line('balance', 40, box('333'), { id: 2, targetId: 9 });
    const credit = line('extra', -40, { ridesWith: 'account', extraSupplier: SUPPLIER }, { id: 3, targetId: 5 });
    const b = N.buildNotices(source([dep, bal, credit]));
    assert.equal(b.sendable, true);
    assert.deepEqual(b.notices.map(n => [n.kind, n.amount, n.blocked && n.blocked.code]), [['deposit', 1000, null], ['balance', 0, 'NOTICE_NOT_POSITIVE']]);
});

// ── Proofs ────────────────────────────────────────────────────────────────

test('a proof is a live remittance filed under exactly the transfer\'s supplier name; anything else linked stops the send', () => {
    const dep = [line('po_deposit', 1000, po(1))];
    const b = docs => N.buildNotices(source(dep, { documents: docs }));
    assert.deepEqual(b([proof(), proof({ id: 78, filename: 'b.pdf' })]).proofs.map(p => [p.id, p.filename]), [[77, 'proof.pdf'], [78, 'b.pdf']]);
    assert.equal(b([]).blocked.code, 'NO_PROOF');
    for (const bad of [{ docKind: 'balance_invoice' }, { docKind: 'other' }, { supplierName: 'Acme Medical Co., Ltd.' }, { supplierName: 'ACME MEDICAL CO., LTD' }, { supplierName: null }, { supplierName: '' }]) {
        const r = b([proof(), proof({ id: 78, filename: 'wrong.pdf', ...bad })]);
        assert.equal(r.sendable, false, JSON.stringify(bad));
        assert.equal(r.blocked.code, 'PROOF_NOT_SENDABLE', JSON.stringify(bad));
        assert.deepEqual(r.blocked.files, ['wrong.pdf']);
        // Never silently dropped: nothing is offered as a proof while one is wrong.
        assert.deepEqual(r.proofs, []);
    }
});

test('what a proof carries to the dialog: its id, name and address, nothing of the file\'s storage', () => {
    const [p] = N.buildNotices(source([line('po_deposit', 1000, po(1))])).proofs;
    assert.deepEqual(p, { id: 77, filename: 'proof.pdf', url: proof().url });
});

// ── What a supplier is told about a line ──────────────────────────────────

test('a line names its PO or container only when that reference is filed under exactly the transfer\'s supplier', () => {
    const lines = [
        line('po_deposit', 1000, po(1), { id: 1, targetId: 101 }),
        // Another spelling of the same company: the money is said, the PO is not.
        line('po_deposit', 500, po(2, { poSupplier: 'Acme Medical Co., Ltd.' }), { id: 2, targetId: 102 }),
        line('balance', 2000, { ...box('333'), invoiceNumber: 'INV-9' }, { id: 3, targetId: 9 }),
        line('balance', 700, box('334', { shipmentSupplier: 'ACME MEDICAL' }), { id: 4, targetId: 10 }),
    ];
    const b = N.buildNotices(source(lines));
    const [dep, bal] = b.notices;
    assert.deepEqual(dep.lines, [
        { type: 'deposit', amount: 1000, named: true, poId: 101, poNumber: 'PO_001J', container: null, shipmentId: null, invoiceNumber: null, jfCode: null, extraKind: null, containers: null },
        { type: 'deposit', amount: 500, named: false, poId: null, poNumber: null, container: null, shipmentId: null, invoiceNumber: null, jfCode: null, extraKind: null, containers: null },
    ]);
    assert.deepEqual(bal.lines.map(l => [l.type, l.amount, l.named, l.container, l.shipmentId, l.invoiceNumber]), [
        ['balance', 2000, true, '333', 5333, 'INV-9'],
        ['balance', 700, false, null, null, null],
    ]);
    // What the email may name, and where "enter ready dates" may lead.
    assert.deepEqual(dep.paidFor, ['PO_001J']);
    assert.deepEqual(dep.readyDatePos, [{ id: 101, poNumber: 'PO_001J' }]);
    assert.deepEqual(bal.paidFor, ['333']);
    assert.deepEqual(bal.readyDatePos, []);
    // The dialog says which lines the supplier will not see named.
    assert.deepEqual([dep.unnamed, bal.unnamed], [1, 1]);
});

test('an extra names a PO by the PO\'s own supplier, never by the extra\'s', () => {
    const mine = line('extra', 300, { ridesWith: 'balance', extraKind: 'mould', extraSupplier: SUPPLIER, ...po(4) }, { id: 1, targetId: 5 });
    const theirs = line('extra', 200, { ridesWith: 'balance', extraKind: 'freight', extraSupplier: SUPPLIER, ...po(5, { poSupplier: 'Other Factory Ltd' }) }, { id: 2, targetId: 6 });
    const onBox = line('extra', 100, { ridesWith: 'balance', extraKind: 'pallets', extraSupplier: SUPPLIER, shipmentId: 5333, shipmentReference: '333', shipmentSupplier: SUPPLIER }, { id: 3, targetId: 7 });
    const foreign = line('extra', 50, { ridesWith: 'balance', extraKind: 'other', extraSupplier: 'Other Factory Ltd', ...po(6) }, { id: 4, targetId: 8 });
    const [n] = N.buildNotices(source([mine, theirs, onBox, foreign])).notices;
    assert.deepEqual(n.lines.map(l => [l.type, l.amount, l.named, l.extraKind, l.poNumber, l.container]), [
        ['extra', 300, true, 'mould', 'PO_004J', null],
        // The extra is this supplier's; the PO it points at is another's: no PO number.
        ['extra', 200, true, 'freight', null, null],
        ['extra', 100, true, 'pallets', null, '333'],
        // Not this supplier's extra at all: its kind is not said either.
        ['extra', 50, false, null, null, null],
    ]);
    assert.deepEqual(n.paidFor, ['PO_004J', '333']);
});

test('a container is told by its number only, never by a draft\'s name', () => {
    const lines = [
        line('balance', 100, box('126. Air Freight'), { id: 1, targetId: 1 }),
        line('balance', 100, { shipmentId: 9, shipmentReference: 'DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar ETD 13 Oct', shipmentSupplier: SUPPLIER }, { id: 2, targetId: 2 }),
        line('balance', 100, { shipmentId: 10, shipmentReference: 'DRAFT-SEA-260923-141617 - TAM Container 2', shipmentSupplier: SUPPLIER }, { id: 3, targetId: 3 }),
    ];
    const [n] = N.buildNotices(source(lines)).notices;
    assert.deepEqual(n.lines.map(l => [l.container, l.shipmentId, l.named]), [['126. Air Freight', 5126, true], ['333', 9, true], [null, 10, true]]);
    assert.equal(JSON.stringify(n).includes('Sunmed'), false);
    assert.equal(JSON.stringify(n).includes('TAM'), false);
});

test('a PI line says its kind, its PO, and the containers its money was ticked against', () => {
    const pi = type => line('pi', 5000, { ...po(7), paymentType: type }, { id: 1, targetId: 70, paidWith: type === 'deposit' ? null : [{ shipmentReference: '301', amount: 3000 }, { shipmentReference: '308', amount: 2000 }] });
    const types = ['deposit', 'balance', 'full'].map(t => N.buildNotices(source([pi(t)])).notices[0].lines[0]);
    assert.deepEqual(types.map(l => [l.type, l.poNumber, l.containers]), [
        ['pi_deposit', 'PO_007J', null],
        ['pi_balance', 'PO_007J', [{ container: '301', amount: 3000 }, { container: '308', amount: 2000 }]],
        ['pi_full', 'PO_007J', [{ container: '301', amount: 3000 }, { container: '308', amount: 2000 }]],
    ]);
    // A container the money was ticked against is told by its number too, never a draft's name.
    const draft = N.buildNotices(source([line('pi', 100, { ...po(7), paymentType: 'balance' }, { paidWith: [{ shipmentReference: 'DRAFT-SEA-260923-141617 - 333 Sunmed Kingphar', amount: 100 }] })])).notices[0].lines[0];
    assert.deepEqual(draft.containers, [{ container: '333', amount: 100 }]);
    // Under another spelling of the supplier, neither the PO nor its containers are said.
    const other = N.buildNotices(source([line('pi', 5000, { ...po(7, { poSupplier: 'Acme' }), paymentType: 'balance' }, { paidWith: [{ shipmentReference: '301', amount: 5000 }] })])).notices[0].lines[0];
    assert.deepEqual([other.named, other.poNumber, other.containers], [false, null, null]);
});

test('QC units, repackaged lines, top-ups and credits are each said as what they are', () => {
    const lines = [
        line('qc', 124.1, { ...po(3), jfCode: 'JF0938_FQC' }, { id: 1, targetId: 1 }),
        line('repackaged', 60, { ...po(3), jfCode: 'JF1168' }, { id: 2, targetId: 2 }),
        line('balance', 500, { ...box('337'), topUp: true }, { id: 3, targetId: 3 }),
        line('extra', -25, { ridesWith: 'account', extraKind: 'credit', extraSupplier: SUPPLIER }, { id: 4, targetId: 4 }),
    ];
    const [n] = N.buildNotices(source(lines)).notices;
    assert.deepEqual(n.lines.map(l => [l.type, l.amount, l.poNumber, l.jfCode, l.container]), [
        ['qc', 124.1, 'PO_003J', 'JF0938_FQC', null],
        ['repackaged', 60, 'PO_003J', 'JF1168', null],
        ['top_up', 500, null, null, '337'],
        ['credit', -25, null, null, null],
    ]);
    assert.equal(n.amount, 659.1);
    // A balance asks for no ready dates, though two of its lines hang on a PO.
    assert.deepEqual(n.readyDatePos, []);
    assert.deepEqual(n.paidFor, ['PO_003J', '337']);
});

test('nothing internal rides along: no note, bank reference, supplier key or raw target', () => {
    const s = source([line('po_deposit', 1000, po(1))], { payment: { note: '[OVERRIDE] rushed, sign-off pending', bankRef: 'WF-TX-99881', supplierKey: 'acme medical co., ltd' } });
    const text = JSON.stringify(N.buildNotices(s));
    for (const secret of ['OVERRIDE', 'WF-TX-99881', 'acme medical co., ltd', 'supplierKey', 'bankRef', 's3Key', 'contentType', 'fileSize']) {
        assert.equal(text.includes(secret), false, secret);
    }
    // The proof's public address is the one thing of the file that is passed on.
    assert.equal(text.includes(proof().url), true);
});

// ── Tokens, links, blocks ─────────────────────────────────────────────────

test('the tokens an email\'s wording is filled with', () => {
    const b = N.buildNotices(source([
        line('po_deposit', 12000, po(1), { id: 1, targetId: 101 }),
        line('po_deposit', 3500.5, po(2), { id: 2, targetId: 102 }),
    ]));
    assert.deepEqual(N.tokensFor(b, b.notices[0]), { amount: '15,500.50', currency: 'USD', paidOn: '2026-11-14', paidFor: 'PO_001J, PO_002J' });
    // Nothing nameable: a dash, never an empty "for".
    const none = N.buildNotices(source([line('po_deposit', 10, po(1, { poSupplier: 'Other' }))]));
    assert.equal(N.tokensFor(none, none.notices[0]).paidFor, '—');
});

test('links: the payment on the portal, and an order\'s own page for its ready dates', () => {
    assert.equal(N.paymentPortalUrl('https://portal.example', 45), 'https://portal.example/payments?payment=45');
    assert.equal(N.readyDatesUrl('https://portal.example', 101), 'https://portal.example/orders/101');
    // Never the Emailed PDFs tab the PO email opens.
    assert.equal(N.readyDatesUrl('https://portal.example', 101).includes('tab='), false);
});

test('the portal block: the button to the payment, registration, and on a deposit the way to the ready dates', () => {
    const html = N.portalBlockHtml({
        kind: 'deposit', url: 'https://portal.example/payments?payment=45', registerUrl: 'https://portal.example/register', preapproved: true,
        readyDates: [{ poNumber: 'PO_001J', url: 'https://portal.example/orders/101' }, { poNumber: 'PO_<b>2', url: 'https://portal.example/orders/102' }],
    });
    assert.ok(html.includes('href="https://portal.example/payments?payment=45"'));
    assert.ok(html.includes('https://portal.example/register'));
    assert.ok(html.includes('There is no approval to wait for'));
    assert.ok(html.includes('href="https://portal.example/orders/101"'));
    assert.ok(html.includes('PO_001J'));
    assert.ok(html.includes('预计完成日期'));
    // A PO number is text, never markup.
    assert.ok(html.includes('PO_&lt;b&gt;2'));
    assert.equal(html.includes('PO_<b>2'), false);
    assert.ok(html.includes('No file is attached'));
    // A balance has no ready dates to ask for.
    const bal = N.portalBlockHtml({ kind: 'balance', url: 'https://portal.example/payments?payment=45', registerUrl: 'https://portal.example/register', preapproved: false, readyDates: [{ poNumber: 'PO_001J', url: 'https://portal.example/orders/101' }] });
    assert.equal(bal.includes('/orders/101'), false);
    assert.ok(bal.includes('our team will approve your access'));
    // And with no container to release, nothing more than the first box.
    assert.equal(/telex|电放/i.test(bal), false);
    // A deposit never asks for a telex release, whatever it is handed.
    assert.equal(/telex|电放/i.test(N.portalBlockHtml({ kind: 'deposit', url: 'https://portal.example/payments?payment=45', registerUrl: 'https://portal.example/register', preapproved: true, telex: ['333'] })), false);
});

test('the portal block of a balance asks for the telex release of each container, uploaded on the payment\'s own row', () => {
    const URL = 'https://portal.example/payments?payment=45';
    const block = telex => N.portalBlockHtml({ kind: 'balance', url: URL, registerUrl: 'https://portal.example/register', preapproved: true, telex });
    const two = block(['333', '334']);
    assert.ok(two.startsWith(block([])), 'the first box is the same, then the ask');
    assert.ok(two.includes('Please upload the <strong>telex release</strong> of each container this payment covers (containers 333, 334) on the portal, on this payment\'s own row.'));
    assert.ok(two.includes('请在门户中该笔付款所在行，上传本次付款所涉各货柜（货柜 333、334）的<strong>电放提单</strong>。'));
    assert.ok(two.includes('Upload the telex release / 上传电放提单'));
    // The button leads to the payment itself: twice in all, with the first box's.
    assert.equal(two.split(`href="${URL}"`).length - 1 >= 2, true);
    assert.equal(block([]).split('Upload the telex release').length - 1, 0);
    // One container is "container", not "containers".
    assert.ok(block(['333']).includes('this payment covers (container 333) on the portal'));
    // A container with no number: no list with a gap in it.
    const gap = block(['333', null]);
    assert.ok(gap.includes('of each container this payment covers on the portal, on this payment\'s own row.'));
    assert.ok(gap.includes('上传本次付款所涉各货柜的<strong>电放提单</strong>。'));
    // A number is text, never markup.
    assert.ok(block(['3<b>3']).includes('3&lt;b&gt;3'));
    assert.equal(block(['3<b>3']).includes('3<b>3'), false);
});

test('the containers a balance asks the telex release of: the ones the portal lists on the payment\'s row', () => {
    const built = N.buildNotices(source([
        line('po_deposit', 1000, po(1), { id: 1, targetId: 101 }),
        line('balance', 2000, box('333'), { id: 2, targetId: 9 }),
        // A top-up of the same container: once.
        line('balance', 100, box('333', { topUp: true }), { id: 3, targetId: 12 }),
        line('balance', 500, box('334'), { id: 4, targetId: 10 }),
        // A top-up on its own asks as a balance does.
        line('balance', 60, box('338', { topUp: true }), { id: 9, targetId: 13 }),
        // Filed under another company: told as its amount alone, and not asked about.
        line('balance', 300, box('335', { shipmentSupplier: 'Other Factory Ltd' }), { id: 5, targetId: 11 }),
        // QC units, a charge on a container, and an invoice paid through containers: the portal offers no upload for these.
        line('qc', 50, po(2, { jfCode: 'JF1_FQC' }), { id: 6, targetId: 30 }),
        line('extra', 20, { ...box('336'), extraKind: 'freight', ridesWith: 'balance', extraSupplier: SUPPLIER }, { id: 7, targetId: 40 }),
        line('pi', 700, po(3, { paymentType: 'balance' }), { id: 8, targetId: 50, paidWith: [{ shipmentReference: '337', amount: 700 }] }),
    ]));
    const [deposit, balance] = built.notices;
    assert.deepEqual(N.telexContainersOf(balance), ['333', '334', '338']);
    assert.deepEqual(N.telexContainersOf(deposit), []);
    // The rule itself, whatever built the lines: a line the supplier is not told of asks for nothing.
    assert.deepEqual(N.telexContainersOf({ kind: 'balance', lines: [{ type: 'balance', named: false, shipmentId: 5340, container: '340' }, { type: 'top_up', named: true, shipmentId: 5341, container: '341' }, { type: 'balance', named: true, shipmentId: null, container: '342' }] }), ['341']);
    // A shipment with no number in its name is still asked for, without one.
    const unnamed = N.buildNotices(source([line('balance', 10, box('Air freight', { shipmentId: 6001 }))])).notices[0];
    assert.deepEqual(N.telexContainersOf(unnamed), [null]);
    assert.deepEqual([N.telexContainersOf(null), N.telexContainersOf({ kind: 'balance' })], [[], []]);
});

test('ready-date links: one per order of the deposit, where the portal can open it for this supplier', () => {
    const BASE = 'https://portal.example';
    const notice = { kind: 'deposit', readyDatePos: [{ id: 101, poNumber: 'PO_001J' }, { id: 102, poNumber: 'PO_002J' }] };
    const links = [{ poNumber: 'PO_001J', url: `${BASE}/orders/101` }, { poNumber: 'PO_002J', url: `${BASE}/orders/102` }];
    const matched = [{ id: 900001, name: SUPPLIER }];
    assert.deepEqual(N.readyDatesFor(notice, { delivery: 'portal', reason: null, portalBase: BASE, suppliers: matched }), links);
    // No supplier of exactly that name: nobody on the portal can open these orders.
    assert.deepEqual(N.readyDatesFor(notice, { delivery: null, reason: 'no_supplier', portalBase: BASE, suppliers: [] }), []);
    // No portal address on this server: no link to make.
    assert.deepEqual(N.readyDatesFor(notice, { delivery: null, reason: 'portal_url_unset', portalBase: null, suppliers: matched }), []);
    // A balance asks for no ready dates.
    assert.deepEqual(N.readyDatesFor({ kind: 'balance', readyDatePos: [] }, { delivery: 'portal', reason: null, portalBase: BASE, suppliers: matched }), []);
    assert.deepEqual(N.readyDatesFor({ kind: 'balance' }, { delivery: 'portal', reason: null, portalBase: BASE, suppliers: matched }), []);
});

// ── Delivery ──────────────────────────────────────────────────────────────

test('delivery: a portal link with the address, the portal\'s table and an exact supplier; otherwise nothing goes. There is no switch', () => {
    const ok = { portalBase: 'https://portal.example', portalAvailable: true, supplierMatched: true };
    assert.deepEqual(N.deliveryFor(ok), { delivery: 'portal', reason: null });
    // Never 'attached': a payment that cannot go as a link does not go.
    assert.deepEqual(N.deliveryFor({ ...ok, portalBase: '' }), { delivery: null, reason: 'portal_url_unset' });
    assert.deepEqual(N.deliveryFor({ ...ok, portalAvailable: false }), { delivery: null, reason: 'portal_unavailable' });
    assert.deepEqual(N.deliveryFor({ ...ok, supplierMatched: false }), { delivery: null, reason: 'no_supplier' });
    assert.equal(N.DELIVERY, 'portal');
    // No PAYMENT_PROOF_PORTAL switch (the user, 2026-11-17): whatever is passed for one is not read.
    assert.deepEqual(N.deliveryFor({ ...ok, switchOn: false }), { delivery: 'portal', reason: null });
    assert.equal(N.portalSwitchOn, undefined);
});

test('a payment the portal cannot show is refused, with what is in the way and what to do about it', () => {
    assert.equal(N.portalRefusal({ delivery: 'portal', reason: null }, SUPPLIER), null);
    // The supplier's name matches no supplier record: the sender can fix that.
    assert.deepEqual(N.portalRefusal({ delivery: null, reason: 'no_supplier' }, SUPPLIER), {
        status: 409, code: 'SUPPLIER_NOT_ON_PORTAL', reason: 'no_supplier', allowedNext: ['edit_payment'],
        error: 'No supplier record is named exactly "Acme Medical Co., Ltd", so the supplier portal has nobody to show this payment to. Make the payment\'s supplier name and the supplier record match exactly, then send. The proof is not sent attached instead.',
    });
    // The portal is not live on this server: nothing the sender can do.
    const said = {
        portal_url_unset: 'the supplier portal address is not configured',
        portal_unavailable: 'the supplier portal is not available here',
    };
    for (const [reason, words] of Object.entries(said)) {
        assert.deepEqual(N.portalRefusal({ delivery: null, reason }, SUPPLIER), {
            status: 409, code: 'PORTAL_NOT_LIVE', reason, allowedNext: [],
            error: `A payment is sent to a supplier as a link to the supplier portal only, never with the proof attached. That cannot be done on this server yet: ${words}. Nothing can be sent until it is.`,
        }, reason);
    }
    // Nothing known of how it would go is not "it can go".
    assert.equal(N.portalRefusal(null, SUPPLIER).code, 'PORTAL_NOT_LIVE');
    assert.equal(N.portalRefusal({ delivery: 'attached', reason: null }, SUPPLIER).code, 'PORTAL_NOT_LIVE');
});

test('a send that still asks for "proof attached" is told it goes as a portal link now', () => {
    assert.equal(N.deliveryConflict('portal'), null);
    const c = N.deliveryConflict('attached');
    assert.deepEqual([c.status, c.code, c.delivery, c.reason, c.allowedNext], [409, 'DELIVERY_CHANGED', 'portal', null, ['portal']]);
    assert.equal(c.error, 'A proof of payment is no longer sent attached: this payment goes out as a link to the supplier portal. Check the wording and send again.');
    // No delivery at all is not "whatever": the dialog always says which it showed.
    assert.equal(N.deliveryConflict(undefined).code, 'DELIVERY_CHANGED');
});

test('the preview token changes with anything the dialog showed, and with nothing else', () => {
    const lines = () => [line('po_deposit', 1000, po(1), { id: 1, targetId: 101 }), line('balance', 2000, box('333'), { id: 2, targetId: 9 })];
    const token = (s, delivery = 'portal') => N.previewToken(N.buildNotices(s), delivery);
    const base = token(source(lines()));
    assert.match(base, /^[a-f0-9]{64}$/);
    // The order lines are stored in, and their row ids, are not what was shown.
    const shuffled = source(lines().reverse().map((l, i) => ({ ...l, id: 900 + i })));
    assert.equal(token(shuffled), base);
    // Nor the order of two lines inside one notice.
    const two = () => [line('po_deposit', 1000, po(1), { id: 1, targetId: 101 }), line('po_deposit', 500, po(2), { id: 2, targetId: 102 })];
    assert.equal(token(source(two().reverse())), token(source(two())));
    // An internal note changing is not a change to what the supplier is told.
    assert.equal(token(source(lines(), { payment: { amount: 3000, note: 'x', bankRef: 'y' } })), base);
    const differs = [
        source(lines(), { payment: { amount: 3000.01 } }),
        source(lines(), { payment: { amount: 3000, paidOn: '2026-11-15' } }),
        source(lines(), { payment: { amount: 3000, currency: 'EUR' } }),
        source(lines(), { payment: { amount: 3000, supplierName: 'Acme Medical Co., Ltd.' } }),
        source([lines()[0], line('balance', 2000, box('334'), { id: 2, targetId: 10 })]),
        source([lines()[0], line('balance', 2000.5, box('333'), { id: 2, targetId: 9 })], { payment: { amount: 3000 } }),
        source(lines(), { documents: [proof({ id: 78 })] }),
        source(lines(), { documents: [proof(), proof({ id: 79, filename: 'second.pdf' })] }),
    ];
    for (const [i, s] of differs.entries()) assert.notEqual(token(s), base, `case ${i}`);
    // Whether it can go at all is part of what the dialog showed.
    assert.notEqual(token(source(lines()), null), base);
});

// ── The stored wording ────────────────────────────────────────────────────

const { DEFAULT_EMAIL_TEMPLATES, PAYMENT_PROOF_TEMPLATES } = require('../src/lib/email-template-defaults');

const SENT_TEMPLATES = ['payment_proof_deposit_portal', 'payment_proof_balance_portal'];
const RETIRED_TEMPLATES = ['payment_proof_deposit_attached', 'payment_proof_balance_attached'];

test('two templates are sent, a deposit\'s and a balance\'s; the two "proof attached" ones are kept only for the old migrations', () => {
    // To a supplier. The third is a forwarder's own (2026-11-20; tools/test-forwarder-payment-notices-lib.js).
    assert.deepEqual(N.TEMPLATE_KEYS, { deposit: 'payment_proof_deposit_portal', balance: 'payment_proof_balance_portal', forwarder: 'payment_proof_forwarder_portal' });
    assert.deepEqual(N.NOTICE_KINDS.map(k => N.TEMPLATE_KEYS[k]), SENT_TEMPLATES);
    // Still in the list, for 2026-11-16_30 and 2026-11-17_10 to read; nothing names them to send.
    assert.deepEqual(PAYMENT_PROOF_TEMPLATES.map(t => t.key), ['payment_proof_deposit_portal', 'payment_proof_deposit_attached', 'payment_proof_balance_portal', 'payment_proof_balance_attached']);
    assert.equal(Object.values(N.TEMPLATE_KEYS).some(key => RETIRED_TEMPLATES.includes(key)), false);
    const sent = PAYMENT_PROOF_TEMPLATES.filter(t => SENT_TEMPLATES.includes(t.key));
    assert.deepEqual(sent.map(t => t.name), ['Deposit paid', 'Balance paid']);
    for (const t of sent) {
        // The dialog offers a template by this category; a default the send can fall back to.
        assert.equal(t.category, 'Payment proof', t.key);
        assert.equal(N.TEMPLATE_CATEGORY, 'Payment proof');
        assert.ok(DEFAULT_EMAIL_TEMPLATES.includes(t), t.key);
        // The figures are tokens, never typed: every one of them, in the body.
        for (const token of ['{amount}', '{currency}', '{paidOn}', '{paidFor}']) assert.ok(t.bodyHtml.includes(token), `${t.key} ${token}`);
        // No token this feature does not fill, no sender field.
        assert.deepEqual([...new Set((t.subject + t.bodyHtml).match(/\{\w+\}/g))].sort(), ['{amount}', '{currency}', '{paidFor}', '{paidOn}'], t.key);
        assert.equal(/\[\w+:\w+\]/.test(t.subject + t.bodyHtml), false, t.key);
        // English and Chinese, as the portal is.
        assert.match(t.bodyHtml, /[一-鿿]/, t.key);
        assert.match(t.subject, /[一-鿿]/, t.key);
        assert.ok(t.subject.length <= 255, t.key);
    }
});

test('the wording says what was paid and points below it: the blocks under it say the rest, once', () => {
    const t = Object.fromEntries(PAYMENT_PROOF_TEMPLATES.map(x => [x.key, x]));
    const said = kind => [
        '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
        '<p>Hi,</p>',
        `<p>We have sent a <strong>${kind.en}</strong> payment of <strong>{currency} {amount}</strong> on {paidOn}, for: {paidFor}.</p>`,
        '<p>The proof of payment, and anything we need from you for it, is below.</p>',
        '<p>您好，</p>',
        `<p>我们已于 {paidOn} 支付<strong>${kind.zh}</strong> <strong>{currency} {amount}</strong>，对应：{paidFor}。</p>`,
        '<p>付款凭证及需要您配合办理的事项，请见下方。</p>',
        '<p>Kind regards<br>JFA Medical LTD</p>',
        '</div>',
    ].join('');
    assert.equal(t.payment_proof_deposit_portal.bodyHtml, said({ en: 'deposit', zh: '定金' }));
    assert.equal(t.payment_proof_balance_portal.bodyHtml, said({ en: 'balance', zh: '尾款' }));
    assert.equal(t.payment_proof_deposit_portal.subject, 'Deposit paid – {currency} {amount} – {paidFor} / 定金已付');
    assert.equal(t.payment_proof_balance_portal.subject, 'Balance paid – {currency} {amount} – {paidFor} / 尾款已付');
    for (const key of SENT_TEMPLATES) {
        const body = t[key].bodyHtml;
        // Nothing the appended blocks say again: where the proof is, the ready dates, the telex release.
        assert.equal(/portal|门户|ready date|预计完成|telex|电放/i.test(body), false, key);
        // Nothing is attached, and nothing is asked for by reply (the user, 2026-11-17).
        assert.equal(/attach|附件|reply to this email|回复本邮件|confirm you have received/i.test(body), false, key);
        // What the template editor says of it: nothing attached, and what is appended.
        assert.match(t[key].description, /^Emailed to a supplier when a (deposit|balance) .*Nothing is attached: a button to the payment on the supplier portal/, key);
    }
    assert.match(t.payment_proof_deposit_portal.description, /a button to each order's ready dates are appended automatically/);
    assert.match(t.payment_proof_balance_portal.description, /a button to upload their telex release are appended automatically/);
});

// ── The state of a notice ─────────────────────────────────────────────────

const NOW = new Date('2026-11-16T12:00:00Z');
const send = (over = {}) => ({
    id: 1, noticeKind: 'deposit', status: 'sent', supplierName: SUPPLIER, delivery: 'portal',
    sentAt: '2026-11-16T11:00:00.000Z', createdAt: '2026-11-16T10:59:58.000Z', deadlineAt: '2026-11-16T11:00:58.000Z', ...over,
});

test('a send still "sending" past its deadline is an unknown outcome; before it, it is still going', () => {
    assert.equal(N.effectiveStatus(send({ status: 'sending', deadlineAt: '2026-11-16T11:59:59.000Z' }), NOW), 'unknown');
    assert.equal(N.effectiveStatus(send({ status: 'sending', deadlineAt: '2026-11-16T12:00:01.000Z' }), NOW), 'sending');
    for (const s of ['sent', 'failed', 'unknown']) assert.equal(N.effectiveStatus(send({ status: s }), NOW), s);
});

test('the state of each notice of a transfer, from its sends', () => {
    const states = (sends, kinds = ['deposit', 'balance'], name = SUPPLIER) => N.noticeStates(kinds, sends, name, NOW).map(s => [s.kind, s.state, s.lastSentAt, s.delivery]);
    assert.deepEqual(states([]), [['deposit', 'not_sent', null, null], ['balance', 'not_sent', null, null]]);
    assert.deepEqual(states([send()]), [['deposit', 'sent', '2026-11-16T11:00:00.000Z', 'portal'], ['balance', 'not_sent', null, null]]);
    // The latest send that went decides the date and the way it went.
    assert.deepEqual(states([send(), send({ id: 2, sentAt: '2026-11-16T11:30:00.000Z', delivery: 'attached' })], ['deposit']),
        [['deposit', 'sent', '2026-11-16T11:30:00.000Z', 'attached']]);
    // A failed send is as if it never happened.
    assert.deepEqual(states([send({ status: 'failed', sentAt: null })], ['deposit']), [['deposit', 'not_sent', null, null]]);
    // An unknown outcome needs a person, even after an earlier send went.
    assert.deepEqual(states([send(), send({ id: 2, status: 'unknown', sentAt: null })], ['deposit']).map(s => s[1]), ['unknown']);
    assert.deepEqual(states([send({ status: 'sending', sentAt: null, deadlineAt: '2026-11-16T11:00:00.000Z' })], ['deposit']).map(s => s[1]), ['unknown']);
    assert.deepEqual(states([send({ status: 'sending', sentAt: null, deadlineAt: '2026-11-16T12:00:30.000Z' })], ['deposit']).map(s => s[1]), ['sending']);
});

test('a send to a supplier the transfer no longer belongs to does not count as sent', () => {
    const moved = N.noticeStates(['deposit'], [send({ supplierName: 'Former Supplier Ltd' })], SUPPLIER, NOW);
    assert.deepEqual(moved.map(s => [s.state, s.lastSentAt]), [['not_sent', null]]);
    // Exact name: another spelling is another supplier here.
    assert.equal(N.noticeStates(['deposit'], [send({ supplierName: 'Acme Medical Co., Ltd.' })], SUPPLIER, NOW)[0].state, 'not_sent');
});

test('the kinds a transfer has, from its lines as the API lists them', () => {
    const kinds = lines => N.noticeKindsOf(lines);
    assert.deepEqual(kinds([{ kind: 'po_deposit', amount: 1 }]), ['deposit']);
    assert.deepEqual(kinds([{ kind: 'qc', amount: 1 }, { kind: 'pi', paymentType: 'deposit', amount: 1 }]), ['deposit', 'balance']);
    assert.deepEqual(kinds([{ kind: 'extra', ridesWith: 'account', amount: -1 }]), []);
    // A forwarder's transfer has no notice at all.
    assert.deepEqual(kinds([{ kind: 'extra', ridesWith: 'shipment', amount: 1 }, { kind: 'balance', amount: 1 }]), []);
    assert.deepEqual(kinds([]), []);
});

// ── The send's own rules ──────────────────────────────────────────────────

test('the request hash binds a send key to what was asked', () => {
    const req = (over = {}) => ({ to: ['a@x.com', 'B@x.com'], subject: 'S', body: '<p>b</p>', delivery: 'portal', previewToken: 't', ...over });
    const h = N.requestHash(req());
    assert.match(h, /^[a-f0-9]{64}$/);
    assert.equal(N.requestHash(req({ to: ['b@x.com', 'A@X.com'] })), h, 'the same people, in any order or case');
    for (const over of [{ to: ['a@x.com'] }, { subject: 'T' }, { body: '<p>c</p>' }, { delivery: 'attached' }, { previewToken: 'u' }]) {
        assert.notEqual(N.requestHash(req(over)), h, JSON.stringify(over));
    }
});

test('Front is called only while enough of the deadline is left for the call to end inside it', () => {
    const deadline = new Date('2026-11-16T12:01:00Z');
    assert.equal(N.mayCallFront(deadline, new Date('2026-11-16T12:00:39Z')), true);
    assert.equal(N.mayCallFront(deadline, new Date('2026-11-16T12:00:40Z')), false);
    assert.equal(N.mayCallFront(deadline, new Date('2026-11-16T12:01:30Z')), false);
    assert.equal(N.DEADLINE_SECONDS, 60);
});

test('who may send, and what a refusal looks like', () => {
    for (const role of ['admin', 'standard', 'accountant']) assert.equal(N.maySend(role), true, role);
    for (const role of ['readonly', 'warehouse', '', undefined, null]) assert.equal(N.maySend(role), false, String(role));
    const c = N.refusal('NO_PROOF');
    assert.deepEqual([c.status, c.code, Array.isArray(c.allowedNext)], [409, 'NO_PROOF', true]);
    assert.ok(c.error.length > 10);
    assert.equal(N.refusal('SEND_OUTCOME_UNKNOWN').status, 502);
});

// ── The wording already stored on a server ────────────────────────────────

const reword = require('../src/db/migrate/2026-11-17_10_reword_payment_proof_deposit_attached');

// A connection that answers the UPDATE with `affected` rows and the SELECT with `rows`.
function templateConn(affected, rows = []) {
    const asked = [];
    return {
        asked,
        query: async (sql, params) => {
            asked.push({ sql: String(sql).replace(/\s+/g, ' ').trim(), params });
            return /^UPDATE/.test(String(sql).trim()) ? [{ affectedRows: affected }] : [rows];
        },
    };
}
const runReword = async (conn) => { const said = []; await reword.up(conn, { log: m => said.push(m) }); return said; };

test('the reword of the stored deposit wording: only the text this app seeded is replaced', async () => {
    const now = PAYMENT_PROOF_TEMPLATES.find(t => t.key === 'payment_proof_deposit_attached');
    // What it looks for is the old ask, word for word; what it writes is today's.
    assert.ok(reword.OLD_BODY.includes('reply to this email with the estimated ready date of each item'));
    assert.ok(reword.OLD_BODY.includes('回复本邮件告知各产品的预计完成日期'));
    assert.notEqual(reword.OLD_BODY, now.bodyHtml);

    const seeded = templateConn(1);
    assert.deepEqual(await runReword(seeded), ['payment_proof_deposit_attached reworded']);
    assert.equal(seeded.asked.length, 1);
    assert.equal(seeded.asked[0].sql, 'UPDATE email_templates SET body_html = ?, description = ? WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?');
    assert.deepEqual(seeded.asked[0].params, [now.bodyHtml, now.description, 'payment_proof_deposit_attached', reword.OLD_BODY]);
});

test('the reword leaves an edited template alone and says so; a missing or already reworded one is said too', async () => {
    const now = PAYMENT_PROOF_TEMPLATES.find(t => t.key === 'payment_proof_deposit_attached');
    const edited = templateConn(0, [{ body_html: '<p>Our own words</p>' }]);
    assert.deepEqual(await runReword(edited), ['payment_proof_deposit_attached has been edited since it was seeded and was left as it is: check it asks for the ready dates on the portal, not by reply']);
    assert.equal(edited.asked.filter(a => /^UPDATE/.test(a.sql)).length, 1, 'nothing is written a second time');
    assert.deepEqual(await runReword(templateConn(0, [{ body_html: now.bodyHtml }])), ['payment_proof_deposit_attached already holds the new wording']);
    assert.deepEqual(await runReword(templateConn(0, [])), ['payment_proof_deposit_attached not found; nothing to reword']);
});

// ── Portal only: the stored templates ─────────────────────────────────────

const portalOnly = require('../src/db/migrate/2026-11-17_30_payment_proof_portal_only');

// A connection that answers each UPDATE with the next of `affected`, and a SELECT with `rows`.
function portalOnlyConn(affected, rows = []) {
    const asked = [];
    const left = [...affected];
    return {
        asked,
        query: async (sql, params) => {
            const text = String(sql).replace(/\s+/g, ' ').trim();
            asked.push({ sql: text, params });
            return /^UPDATE/.test(text) ? [{ affectedRows: left.shift() ?? 0 }] : [rows];
        },
    };
}
const runPortalOnly = async (conn) => { const said = []; await portalOnly.up(conn, { log: m => said.push(m) }); return said; };

test('portal only: the two templates that stay are reworded where they still hold what was seeded, and the attached pair is retired', async () => {
    const now = key => PAYMENT_PROOF_TEMPLATES.find(t => t.key === key);
    // What it looks for is the wording 2026-11-16_30 seeded, which said the rest in the body.
    assert.deepEqual(Object.keys(portalOnly.SEEDED), SENT_TEMPLATES);
    assert.ok(portalOnly.SEEDED.payment_proof_deposit_portal.bodyHtml.includes('It is not attached to this email. Please confirm you have received the payment, and enter the estimated ready date of each item on the order\'s page of the portal.'));
    assert.ok(portalOnly.SEEDED.payment_proof_balance_portal.bodyHtml.includes('Please arrange the telex release and send us the telex-released bill of lading.'));
    assert.deepEqual(Object.values(portalOnly.SEEDED).map(t => t.name), ['Deposit paid (supplier portal)', 'Balance paid (supplier portal)']);
    for (const key of SENT_TEMPLATES) assert.notEqual(portalOnly.SEEDED[key].bodyHtml, now(key).bodyHtml, key);
    assert.deepEqual(portalOnly.RETIRED, RETIRED_TEMPLATES);

    const conn = portalOnlyConn([1, 1, 2]);
    assert.deepEqual(await runPortalOnly(conn), ['payment_proof_deposit_portal reworded', 'payment_proof_balance_portal reworded', '2 "proof attached" template(s) retired']);
    const REWORD = 'UPDATE email_templates SET name = ?, description = ?, body_html = ? WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?';
    assert.deepEqual(conn.asked, [
        ...SENT_TEMPLATES.map(key => ({ sql: REWORD, params: [now(key).name, now(key).description, now(key).bodyHtml, key, portalOnly.SEEDED[key].bodyHtml] })),
        // Soft-deleted, as the template editor deletes one: the rows stay.
        { sql: 'UPDATE email_templates SET deleted_at = NOW() WHERE template_key IN (?, ?) AND deleted_at IS NULL', params: RETIRED_TEMPLATES },
    ]);
});

test('portal only: an edited template is left alone and named; a missing or already reworded one is said; retiring twice retires none', async () => {
    const now = key => PAYMENT_PROOF_TEMPLATES.find(t => t.key === key);
    const edited = portalOnlyConn([0, 0, 0], [{ body_html: '<p>Our own words</p>' }]);
    assert.deepEqual(await runPortalOnly(edited), [
        'payment_proof_deposit_portal has been edited since it was seeded and was left as it is: check it does not say the proof is attached',
        'payment_proof_balance_portal has been edited since it was seeded and was left as it is: check it does not say the proof is attached',
        '0 "proof attached" template(s) retired',
    ]);
    // Three writes asked for, none of them a second try at an edited template.
    assert.equal(edited.asked.filter(a => /^UPDATE/.test(a.sql)).length, 3);
    assert.deepEqual((await runPortalOnly(portalOnlyConn([0, 0, 0], []))).slice(0, 2), ['payment_proof_deposit_portal not found; nothing to reword', 'payment_proof_balance_portal not found; nothing to reword']);
    // Each key is compared with its own new wording.
    const already = { asked: [], query: async (sql, params) => { already.asked.push(params); return /^\s*UPDATE/.test(sql) ? [{ affectedRows: 0 }] : [[{ body_html: now(params[0]).bodyHtml }]]; } };
    assert.deepEqual((await runPortalOnly(already)).slice(0, 2), ['payment_proof_deposit_portal already holds the new wording', 'payment_proof_balance_portal already holds the new wording']);
});
