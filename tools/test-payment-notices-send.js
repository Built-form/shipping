'use strict';

// The "Send to supplier" of a recorded payment, step by step
// (src/services/payment-notices.js; docs/payment-proof-send.md, "The send"):
// what is checked, the one row written before Front is called, the single call
// to Front, and what each outcome leaves behind. Front is injected and the
// store is in memory, so every outcome can be made to happen: accepted,
// refused, no answer, accepted-but-not-recorded. The real store's statements
// (their guards and their order) are checked at the end against a scripted
// connection. No database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const S = require('../src/services/payment-notices');
const N = require('../src/lib/payment-notices');

const SUPPLIER = 'Acme Medical Co., Ltd';
const NOW = new Date('2026-11-16T12:00:00.000Z');
const PORTAL = 'https://portal.example';

const target = (over = {}) => ({
    paymentType: null, poId: null, poNumber: null, poSupplier: null, shipmentId: null, shipmentReference: null, shipmentSupplier: null,
    invoiceNumber: null, topUp: false, jfCode: null, extraKind: null, ridesWith: null, extraSupplier: null, ...over,
});
const depositLine = (over = {}) => ({ id: 1, kind: 'po_deposit', targetId: 101, amount: 1000, paidWith: null, target: target({ poId: 101, poNumber: 'PO_001J', poSupplier: SUPPLIER }), ...over });
const balanceLine = (over = {}) => ({ id: 2, kind: 'balance', targetId: 9, amount: 2000, paidWith: null, target: target({ shipmentId: 5333, shipmentReference: '333', shipmentSupplier: SUPPLIER }), ...over });
const proofDoc = (over = {}) => ({
    id: 77, docKind: 'remittance', supplierName: SUPPLIER, filename: 'proof.pdf',
    url: 'https://shipping-purchase-orders-test.s3.eu-north-1.amazonaws.com/shipment-payments/u1/proof.pdf',
    s3Key: 'shipment-payments/u1/proof.pdf', contentType: 'application/pdf', fileSize: 120000, ...over,
});
const sourceOf = (lines = [depositLine()], over = {}) => ({
    payment: { id: 45, supplierName: SUPPLIER, supplierKey: 'acme medical co., ltd', amount: lines.reduce((a, l) => a + l.amount, 0), currency: 'USD', paidOn: '2026-11-14', ...(over.payment || {}) },
    lines, documents: over.documents ?? [proofDoc()],
});
const PORTAL_DELIVERY = { delivery: 'portal', reason: null, portalBase: PORTAL, portalUrl: `${PORTAL}/payments?payment=45`, registerUrl: `${PORTAL}/register`, suppliers: [{ id: 900001, name: SUPPLIER }] };
// What cannot be sent (nothing goes out attached instead): nobody of that name on the portal...
const NO_SUPPLIER_DELIVERY = { delivery: null, reason: 'no_supplier', portalBase: PORTAL, portalUrl: null, registerUrl: null, suppliers: [] };
// ...or the portal's tables are not on this stage yet (it is not deployed here), though the supplier is on the portal.
const NOT_DEPLOYED_DELIVERY = { delivery: null, reason: 'portal_unavailable', portalBase: PORTAL, portalUrl: null, registerUrl: null, suppliers: [{ id: 900001, name: SUPPLIER }] };

// The store in memory. Its transitions are the ones the real store's guarded
// UPDATEs make (checked against the SQL below).
function memoryStore(over = {}) {
    const s = {
        source: sourceOf(), delivery: PORTAL_DELIVERY, rows: [], nextId: 1, calls: [], receipts: [], audits: [], logged: [],
        portalAccess: { status: 'ok', suppliers: [{ id: 900001, name: SUPPLIER }], poSupplierMatched: true, users: [{ email: 'li@acme.example', outcome: 'invited', suppliersAdded: 1 }] },
        ...over,
    };
    const byId = id => s.rows.find(r => r.id === id);
    s.findByKey = async key => { s.calls.push('findByKey'); return s.rows.find(r => r.sendKey === key) || null; };
    s.reserve = async ({ paymentId, decide, row }) => {
        s.calls.push('reserve');
        if (!s.source || s.source.payment.id !== paymentId) return { notFound: true };
        const d = decide(s.source, s.delivery, s.rows.filter(r => r.paymentId === paymentId));
        if (d.refusal) return { refusal: d.refusal };
        const fields = row(d);
        const dup = s.rows.find(r => r.sendKey === fields.sendKey);
        if (dup) return { duplicate: dup };
        const send = { id: s.nextId++, paymentId, status: 'sending', sentAt: null, sendError: null, receiptToken: null, resolvedAt: null, resolvedByEmail: null, ...fields };
        s.rows.push(send);
        return { send, source: s.source, delivery: s.delivery, ...d };
    };
    s.template = async key => { s.calls.push(`template:${key}`); return { subject: 'Stored subject {amount}', bodyHtml: '<p>stored {paidFor}</p>' }; };
    s.preapprove = async args => { s.calls.push('preapprove'); s.preapproveArgs = args; if (s.preapproveThrows) throw new Error('deadlock'); return s.portalAccess; };
    // No receipt is minted and no proof is read by a send: nothing is attached. Only a send from
    // before 2026-11-17 can still hold a receipt to take back.
    s.discardReceipt = async token => { s.calls.push(`discard:${token}`); };
    s.fail = async (id, reason) => { s.calls.push('fail'); const r = byId(id); if (r.status === 'sending') { r.status = 'failed'; r.sendError = reason; return true; } return false; };
    s.unknown = async (id, reason) => { s.calls.push('unknown'); if (s.unknownThrows) throw new Error('db down'); const r = byId(id); if (r.status === 'sending') { r.status = 'unknown'; r.sendError = reason; return true; } return false; };
    s.finalise = async (id, { front = null, byExecutor = false, resolvedBy = null }) => {
        s.calls.push('finalise');
        if (s.finaliseThrows) throw new Error('db down');
        const r = byId(id);
        const may = ['sending', 'unknown'].includes(r.status) || (byExecutor && r.status === 'failed' && r.resolvedAt);
        if (!may) return { ok: false, send: r };
        Object.assign(r, { status: 'sent', sentAt: r.sentAt || NOW.toISOString(), sendError: null, frontMessageUid: front ? front.frontMessageUid : r.frontMessageUid || null, resolvedByEmail: resolvedBy, resolvedAt: resolvedBy ? NOW.toISOString() : null });
        s.audits.push({ id, action: resolvedBy ? 'notice_resolved' : 'notice_sent' });
        return { ok: true, send: r };
    };
    s.resolveNotSent = async (id, { resolvedBy }) => {
        s.calls.push('resolveNotSent');
        const r = byId(id);
        if (N.effectiveStatus(r, s.clock ? s.clock() : NOW) !== 'unknown') return { ok: false, send: r };
        Object.assign(r, { status: 'failed', sendError: 'resolved: not sent', resolvedByEmail: resolvedBy, resolvedAt: NOW.toISOString() });
        return { ok: true, send: r };
    };
    s.getSend = async id => byId(id) || null;
    s.logReceipt = async fields => { s.calls.push('logReceipt'); s.logged.push(fields); };
    return s;
}

// What the dialog sends: built from a preview of the same store.
async function requestFor(store, kind = 'deposit', over = {}) {
    const p = await S.previewOf(store.source, store.delivery, [], NOW);
    return { sendKey: 'key-0001-aaaa', to: ['li@acme.example'], subject: 'Deposit paid – {currency} {amount}', body: '<p>We paid {amount} for {paidFor}.</p>', delivery: 'portal', previewToken: p.previewToken, ...over };
}
const frontOk = calls => async msg => { calls.push(msg); return { ok: true, frontMessageUid: 'msg-1', frontConversationId: 'cnv-1' }; };
const ctx = (store, sendFront, over = {}) => ({ store, sendFront, now: () => NOW, apiBaseUrl: 'https://api.example', ...over });
const ACTOR = { email: 'ops@jfa.example', userType: 'standard' };
const send = (store, sendFront, body, kind = 'deposit', over = {}) => S.sendNotice(ctx(store, sendFront, over.ctx), { paymentId: 45, kind, actor: over.actor || ACTOR, body });

// ── It goes out ───────────────────────────────────────────────────────────

test('a portal send: checked, written as "sending", recipients pre-approved, Front called once, then "sent"', async () => {
    const store = memoryStore();
    const front = [];
    const r = await send(store, frontOk(front), await requestFor(store));
    assert.equal(r.status, 200);
    assert.equal(front.length, 1);
    // The row was written before Front, and is what Front's answer finalised.
    assert.deepEqual(store.calls.filter(c => ['findByKey', 'reserve', 'preapprove', 'finalise', 'logReceipt'].includes(c)), ['findByKey', 'reserve', 'preapprove', 'finalise', 'logReceipt']);
    const [row] = store.rows;
    assert.deepEqual([row.status, row.noticeKind, row.delivery, row.supplierName, row.sendKey, row.sentByEmail], ['sent', 'deposit', 'portal', SUPPLIER, 'key-0001-aaaa', 'ops@jfa.example']);
    assert.deepEqual(row.sentTo, ['li@acme.example']);
    assert.match(row.requestHash, /^[a-f0-9]{64}$/);
    // One minute from the moment it was written.
    assert.equal(row.deadlineAt, '2026-11-16T12:01:00.000Z');
    // The figures in the wording are the payment's own, filled in here.
    assert.equal(front[0].subject, 'Deposit paid – USD 1,000.00');
    assert.ok(front[0].html.includes('We paid 1,000.00 for PO_001J.'));
    // The link to the payment, the way to the order's ready dates, nothing attached.
    assert.ok(front[0].html.includes(`href="${PORTAL}/payments?payment=45"`));
    assert.ok(front[0].html.includes(`href="${PORTAL}/orders/101"`));
    assert.ok(front[0].html.includes('There is no approval to wait for'));
    assert.equal(front[0].attachments, undefined);
    assert.deepEqual(front[0].to, ['li@acme.example']);
    // Recipients are linked to the transfer's supplier and to no other.
    assert.deepEqual([store.preapproveArgs.poSupplier, store.preapproveArgs.lineSuppliers, store.preapproveArgs.recipients], [SUPPLIER, undefined, ['li@acme.example']]);
    // On the Email Receipts page, confirmed when it is opened on the portal.
    assert.deepEqual(store.logged.map(l => [l.emailType, l.confirmVia, l.actionUrl, l.sendTable, l.sendId]), [['payment_proof', 'portal', `${PORTAL}/payments?payment=45`, 'supplier_payment_sends', row.id]]);
    assert.deepEqual([r.body.send.id, r.body.send.status, r.body.delivery, r.body.portalAccess.status], [row.id, 'sent', 'portal', 'ok']);
});

test('a balance email has no ready dates; a transfer with both parts sends each as its own email', async () => {
    const store = memoryStore({ source: sourceOf([depositLine(), balanceLine()]) });
    const front = [];
    const a = await send(store, frontOk(front), await requestFor(store, 'deposit', { sendKey: 'key-deposit-1' }), 'deposit');
    const b = await send(store, frontOk(front), await requestFor(store, 'balance', { sendKey: 'key-balance-1', subject: 'Balance {amount}', body: '<p>{paidFor}</p>' }), 'balance');
    assert.deepEqual([a.status, b.status], [200, 200]);
    assert.deepEqual(store.rows.map(r => [r.noticeKind, r.status]), [['deposit', 'sent'], ['balance', 'sent']]);
    assert.equal(front[0].subject, 'Deposit paid – USD 1,000.00');
    assert.equal(front[1].subject, 'Balance 2,000.00');
    assert.ok(front[1].html.includes('<p>333</p>'));
    assert.equal(front[1].html.includes('/orders/'), false);
    // What each asks for is under its wording, with a button: the deposit the ready dates, the balance the telex release.
    assert.ok(front[0].html.includes('Enter ready dates: PO_001J'));
    assert.equal(/telex/i.test(front[0].html), false);
    assert.ok(front[1].html.includes('Please upload the <strong>telex release</strong> of each container this payment covers (container 333) on the portal'));
    assert.ok(front[1].html.includes('Upload the telex release / 上传电放提单'));
    assert.equal(/ready date/i.test(front[1].html), false);
    assert.deepEqual(front.map(m => m.attachments), [undefined, undefined]);
});

test('nothing is ever attached: a payment the portal cannot show is refused before anything is written, with what is in the way', async () => {
    const cases = [
        [NO_SUPPLIER_DELIVERY, 'SUPPLIER_NOT_ON_PORTAL', 'no_supplier', ['edit_payment']],
        [NOT_DEPLOYED_DELIVERY, 'PORTAL_NOT_LIVE', 'portal_unavailable', []],
        [{ ...NOT_DEPLOYED_DELIVERY, reason: 'portal_url_unset', portalBase: null }, 'PORTAL_NOT_LIVE', 'portal_url_unset', []],
    ];
    for (const [delivery, code, reason, next] of cases) {
        const store = memoryStore({ delivery });
        const front = [];
        const r = await send(store, frontOk(front), await requestFor(store));
        assert.deepEqual([r.status, r.body.code, r.body.reason, r.body.allowedNext], [409, code, reason, next], reason);
        assert.match(r.body.error, reason === 'no_supplier' ? /^No supplier record is named exactly "Acme Medical Co\., Ltd"/ : /^A payment is sent to a supplier as a link to the supplier portal only, never with the proof attached\./, reason);
        assert.equal(front.length, 0, reason);
        assert.equal(store.rows.length, 0, reason);
        // Nobody is approved on the portal for an email that does not go.
        assert.equal(store.calls.includes('preapprove'), false, reason);
    }
});

test('a blank subject or body falls back to the stored template for that notice', async () => {
    const store = memoryStore();
    const front = [];
    await send(store, frontOk(front), await requestFor(store, 'deposit', { subject: '  ', body: '' }));
    assert.ok(store.calls.includes('template:payment_proof_deposit_portal'));
    assert.equal(front[0].subject, 'Stored subject 1,000.00');
    assert.ok(front[0].html.includes('<p>stored PO_001J</p>'));
    const mixed = memoryStore({ source: sourceOf([depositLine(), balanceLine()]) });
    await send(mixed, frontOk([]), await requestFor(mixed, 'balance', { subject: '', body: '<p>x</p>' }), 'balance');
    assert.deepEqual(mixed.calls.filter(c => c.startsWith('template:')), ['template:payment_proof_balance_portal']);
});

// ── It is refused before anything is written ──────────────────────────────

test('who may send: admin, standard and accountant; anyone else is refused before anything is read', async () => {
    for (const userType of ['admin', 'standard', 'accountant']) {
        const store = memoryStore();
        assert.equal((await send(store, frontOk([]), await requestFor(store), 'deposit', { actor: { email: 'a@x', userType } })).status, 200, userType);
    }
    const store = memoryStore();
    const r = await send(store, frontOk([]), await requestFor(store), 'deposit', { actor: { email: 'a@x', userType: 'warehouse' } });
    assert.deepEqual([r.status, r.body.code], [403, 'CANNOT_SEND']);
    assert.deepEqual(store.calls, []);
});

test('a bad request is a 400 with nothing written: recipients, key, kind, delivery, token', async () => {
    const bad = [
        [{ to: [] }, 'recipient'], [{ to: ['not an address'] }, 'address'], [{ to: 'li@acme.example' }, null],
        [{ sendKey: 'short' }, 'sendKey'], [{ sendKey: undefined }, 'sendKey'], [{ delivery: 'pigeon' }, 'delivery'], [{ previewToken: '' }, 'previewToken'],
    ];
    for (const [over, word] of bad) {
        const store = memoryStore();
        const front = [];
        const r = await send(store, frontOk(front), await requestFor(store, 'deposit', over));
        if (word === null) { assert.equal(r.status, 200, 'one address as a string is one recipient'); continue; }
        assert.equal(r.status, 400, JSON.stringify(over));
        assert.match(r.body.error, new RegExp(word, 'i'));
        assert.equal(front.length, 0);
        assert.equal(store.rows.length, 0);
    }
    const store = memoryStore();
    assert.equal((await send(store, frontOk([]), await requestFor(store), 'refund')).status, 400);
});

test('each thing that stops a transfer is a 409 with what to do next, and nothing is sent', async () => {
    const cases = [
        ['NO_PROOF', sourceOf([depositLine()], { documents: [] })],
        ['PROOF_NOT_SENDABLE', sourceOf([depositLine()], { documents: [proofDoc({ docKind: 'balance_invoice' })] })],
        ['PROOF_NOT_SENDABLE', sourceOf([depositLine()], { documents: [proofDoc({ supplierName: 'Another Factory Ltd' })] })],
        ['NOT_A_SUPPLIER_PAYMENT', sourceOf([{ id: 3, kind: 'extra', targetId: 5, amount: 900, paidWith: null, target: target({ ridesWith: 'shipment', extraKind: 'freight', extraSupplier: 'DCG' }) }])],
        ['LINE_TARGET_MISSING', sourceOf([depositLine(), { id: 2, kind: 'balance', targetId: 9, amount: 5, paidWith: null, target: null }])],
        ['NOTHING_SENT', sourceOf([depositLine()], { payment: { amount: 0 } })],
        ['LINES_EXCEED_TRANSFER', sourceOf([depositLine()], { payment: { amount: 900 } })],
    ];
    for (const [code, source] of cases) {
        const store = memoryStore({ source });
        const front = [];
        // The dialog would have shown the same refusal; a client that sends anyway is told again.
        const r = await send(store, frontOk(front), await requestFor(store));
        assert.deepEqual([r.status, r.body.code], [409, code], code);
        assert.ok(Array.isArray(r.body.allowedNext), code);
        assert.equal(front.length, 0, code);
        assert.equal(store.rows.length, 0, code);
    }
});

test('a part the payment does not have is a 404; a part that comes to nothing is refused', async () => {
    const store = memoryStore();
    const r = await send(store, frontOk([]), await requestFor(store), 'balance');
    assert.deepEqual([r.status, r.body.code], [404, 'NO_SUCH_NOTICE']);
    const credit = { id: 3, kind: 'extra', targetId: 5, amount: -2000, paidWith: null, target: target({ ridesWith: 'account', extraKind: 'credit', extraSupplier: SUPPLIER }) };
    const zero = memoryStore({ source: sourceOf([depositLine(), balanceLine(), credit]) });
    const z = await send(zero, frontOk([]), await requestFor(zero, 'balance'), 'balance');
    assert.deepEqual([z.status, z.body.code], [409, 'NOTICE_NOT_POSITIVE']);
});

test('the payment was edited after the dialog opened: 409 PAYMENT_CHANGED, nothing sent', async () => {
    const store = memoryStore();
    const body = await requestFor(store);
    // Someone changes the amount applied before Send is pressed.
    store.source = sourceOf([depositLine({ amount: 1200 })]);
    const front = [];
    const r = await send(store, frontOk(front), body);
    assert.deepEqual([r.status, r.body.code, r.body.allowedNext], [409, 'PAYMENT_CHANGED', ['reload']]);
    assert.equal(front.length, 0);
    assert.equal(store.rows.length, 0);
});

test('a dialog that still asks for "proof attached" is told it goes as a link now: 409 DELIVERY_CHANGED, nothing sent', async () => {
    const store = memoryStore();
    const front = [];
    const r = await send(store, frontOk(front), await requestFor(store, 'deposit', { delivery: 'attached' }));
    assert.deepEqual([r.status, r.body.code, r.body.delivery, r.body.reason, r.body.allowedNext], [409, 'DELIVERY_CHANGED', 'portal', null, ['portal']]);
    assert.equal(front.length, 0);
    assert.equal(store.rows.length, 0);
    // Where the portal cannot show it either, that is what it is told: there is no attached way to fall back to.
    const none = memoryStore({ delivery: NO_SUPPLIER_DELIVERY });
    assert.equal((await send(none, frontOk(front), await requestFor(none, 'deposit', { delivery: 'attached' }))).body.code, 'SUPPLIER_NOT_ON_PORTAL');
});

test('the portal stopped being able to show it since the dialog opened: refused, nothing sent', async () => {
    const store = memoryStore();
    const body = await requestFor(store);
    store.delivery = NO_SUPPLIER_DELIVERY;
    const front = [];
    const r = await send(store, frontOk(front), body);
    assert.deepEqual([r.status, r.body.code, r.body.reason], [409, 'SUPPLIER_NOT_ON_PORTAL', 'no_supplier']);
    assert.equal(front.length, 0);
    assert.equal(store.rows.length, 0);
});

test('proofs of any size and number go as a link: none is read, none rides on the email', async () => {
    const big = [1, 2, 3, 4, 5, 6, 7].map(id => proofDoc({ id, filename: `p${id}.zip`, contentType: 'application/zip', fileSize: 9 * 1024 * 1024 }));
    const store = memoryStore({ source: sourceOf([depositLine()], { documents: big }) });
    const front = [];
    assert.equal((await send(store, frontOk(front), await requestFor(store))).status, 200);
    assert.equal(front[0].attachments, undefined);
    assert.deepEqual(Object.keys(front[0]).sort(), ['html', 'subject', 'to']);
});

// ── It fails after the row is written, before Front ───────────────────────

test('pre-approval finds no such supplier or no portal after all: the send fails and says why, Front is not called, nothing is attached instead', async () => {
    for (const [status, code, reason] of [['no_supplier', 'SUPPLIER_NOT_ON_PORTAL', 'no_supplier'], ['unavailable', 'PORTAL_NOT_LIVE', 'portal_unavailable']]) {
        const store = memoryStore({ portalAccess: { status, suppliers: [], poSupplierMatched: false, users: [] } });
        const front = [];
        const r = await send(store, frontOk(front), await requestFor(store));
        assert.deepEqual([r.status, r.body.code, r.body.reason], [409, code, reason], status);
        assert.equal(front.length, 0);
        assert.deepEqual(store.rows.map(x => [x.status, x.sendError]), [['failed', `no portal delivery: ${reason}`]]);
    }
});

test('pre-approval failing for another reason does not stop the email: it goes without the "already approved" wording', async () => {
    const store = memoryStore({ preapproveThrows: true });
    const front = [];
    const r = await send(store, frontOk(front), await requestFor(store));
    assert.equal(r.status, 200);
    assert.equal(r.body.portalAccess.status, 'error');
    assert.equal(front[0].html.includes('There is no approval to wait for'), false);
    assert.ok(front[0].html.includes('our team will approve your access'));
    // A blocked address is not "already approved" either, and the sender is told who.
    const blocked = memoryStore({ portalAccess: { status: 'ok', suppliers: [{ id: 1, name: SUPPLIER }], poSupplierMatched: true, users: [{ email: 'li@acme.example', outcome: 'blocked', suppliersAdded: 0 }] } });
    const f2 = [];
    const b = await send(blocked, frontOk(f2), await requestFor(blocked));
    assert.equal(f2[0].html.includes('There is no approval to wait for'), false);
    assert.deepEqual(b.body.portalAccess.users, [{ email: 'li@acme.example', outcome: 'blocked', suppliersAdded: 0 }]);
});

test('too little of the deadline left: Front is not called at all', async () => {
    const store = memoryStore();
    let t = NOW.getTime();
    const front = [];
    // The clock jumps 45 s between writing the row and reaching Front (a slow pre-approval).
    store.preapprove = async () => { t += 45000; return store.portalAccess; };
    const r = await send(store, frontOk(front), await requestFor(store), 'deposit', { ctx: { now: () => new Date(t) } });
    assert.deepEqual([r.status, r.body.code], [502, 'SEND_FAILED']);
    assert.equal(front.length, 0);
    assert.deepEqual(store.rows.map(x => x.status), ['failed']);
});

// ── Front's answer ────────────────────────────────────────────────────────

test('Front refuses: failed, 502, and nothing more is done', async () => {
    const store = memoryStore();
    const r = await send(store, async () => ({ ok: false, reason: 'front_error', status: 422, detail: 'bad' }), await requestFor(store));
    assert.deepEqual([r.status, r.body.code, r.body.frontStatus], [502, 'FRONT_REFUSED', 422]);
    assert.deepEqual(store.rows.map(x => [x.status, x.sendError]), [['failed', 'front_error: 422']]);
    assert.equal(store.calls.includes('finalise'), false);
    assert.equal(store.calls.includes('logReceipt'), false);
    // Front not configured here is a definite "not sent" too.
    const nc = memoryStore();
    const n = await send(nc, async () => ({ ok: false, reason: 'front_not_configured' }), await requestFor(nc));
    assert.deepEqual([n.status, n.body.code, nc.rows[0].status], [502, 'FRONT_REFUSED', 'failed']);
});

test('Front does not answer: the outcome is unknown, said plainly, and never tried again by itself', async () => {
    const store = memoryStore();
    let calls = 0;
    const r = await send(store, async () => { calls += 1; return { ok: false, reason: 'network_error', detail: 'aborted' }; }, await requestFor(store));
    assert.deepEqual([r.status, r.body.code, r.body.allowedNext], [502, 'SEND_OUTCOME_UNKNOWN', ['resolve_sent', 'resolve_not_sent']]);
    assert.equal(calls, 1);
    assert.deepEqual(store.rows.map(x => x.status), ['unknown']);
    // It may have gone: its place on the Email Receipts page waits for whoever says it did.
    assert.equal(store.calls.includes('logReceipt'), false);
    assert.equal(r.body.send.id, store.rows[0].id);
    // Front throwing is the same as Front not answering.
    const thrown = memoryStore();
    const t = await send(thrown, async () => { throw new Error('socket hang up'); }, await requestFor(thrown));
    assert.deepEqual([t.status, t.body.code, thrown.rows[0].status], [502, 'SEND_OUTCOME_UNKNOWN', 'unknown']);
});

test('Front accepted but the result could not be recorded: unknown, not "failed" and not sent again', async () => {
    const store = memoryStore({ finaliseThrows: true });
    const front = [];
    const r = await send(store, frontOk(front), await requestFor(store));
    assert.deepEqual([r.status, r.body.code], [502, 'SEND_OUTCOME_UNKNOWN']);
    assert.equal(front.length, 1);
    assert.deepEqual(store.rows.map(x => x.status), ['unknown']);
    // Even marking it unknown fails: the row stays "sending" and reads as unknown once its deadline passes.
    const worse = memoryStore({ finaliseThrows: true, unknownThrows: true });
    const w = await send(worse, frontOk([]), await requestFor(worse));
    assert.deepEqual([w.status, w.body.code, worse.rows[0].status], [502, 'SEND_OUTCOME_UNKNOWN', 'sending']);
    assert.equal(N.effectiveStatus(worse.rows[0], new Date('2026-11-16T12:01:01Z')), 'unknown');
});

// ── The same click twice, and a second click ──────────────────────────────

test('the same key again returns what was recorded and sends nothing, whatever the payment looks like now', async () => {
    const store = memoryStore();
    const front = [];
    const body = await requestFor(store);
    const first = await send(store, frontOk(front), body);
    // The payment is edited after the email went.
    store.source = sourceOf([depositLine({ amount: 1500 })]);
    const again = await send(store, frontOk(front), body);
    assert.deepEqual([first.status, again.status], [200, 200]);
    assert.equal(front.length, 1);
    assert.equal(store.rows.length, 1);
    assert.equal(again.body.send.id, first.body.send.id);
    assert.equal(again.body.replayed, true);
    // The key looked up before anything else: no second reservation.
    assert.deepEqual(store.calls.filter(c => c === 'reserve').length, 1);
});

test('the same key with other content is refused: a key is one send', async () => {
    const store = memoryStore();
    const body = await requestFor(store);
    await send(store, frontOk([]), body);
    for (const over of [{ to: ['other@acme.example'] }, { subject: 'Changed' }, { body: '<p>changed</p>' }]) {
        const r = await send(store, frontOk([]), { ...body, ...over });
        assert.deepEqual([r.status, r.body.code], [409, 'SEND_KEY_REUSED'], JSON.stringify(over));
    }
    // And for another part of the payment, or another payment.
    const mixed = memoryStore({ source: sourceOf([depositLine(), balanceLine()]) });
    const b = await requestFor(mixed);
    await send(mixed, frontOk([]), b, 'deposit');
    assert.equal((await send(mixed, frontOk([]), b, 'balance')).body.code, 'SEND_KEY_REUSED');
    assert.equal(store.rows.length, 1);
});

test('a retried key whose send failed or is unknown is answered as that, not sent again', async () => {
    const store = memoryStore();
    const body = await requestFor(store);
    let calls = 0;
    const noAnswer = async () => { calls += 1; return { ok: false, reason: 'network_error' }; };
    await send(store, noAnswer, body);
    const again = await send(store, noAnswer, body);
    assert.deepEqual([again.status, again.body.code], [502, 'SEND_OUTCOME_UNKNOWN']);
    assert.equal(calls, 1);
    const failed = memoryStore();
    const fb = await requestFor(failed);
    const refuse = async () => { calls += 1; return { ok: false, reason: 'front_error', status: 500 }; };
    await send(failed, refuse, fb);
    const f2 = await send(failed, refuse, fb);
    assert.deepEqual([f2.status, f2.body.code], [502, 'FRONT_REFUSED']);
    assert.equal(calls, 2, 'one call for the first store, one for the second, none for the retries');
});

test('a second click while the first is still going, or after an unknown outcome, is refused', async () => {
    const store = memoryStore();
    // The first click's row, still going.
    store.rows.push({ id: 90, paymentId: 45, noticeKind: 'deposit', delivery: 'portal', supplierName: SUPPLIER, sendKey: 'first-click-key', requestHash: 'x', status: 'sending', deadlineAt: '2026-11-16T12:00:30.000Z', sentTo: [], sentAt: null });
    const going = await send(store, frontOk([]), await requestFor(store, 'deposit', { sendKey: 'second-click-key' }));
    assert.deepEqual([going.status, going.body.code, going.body.allowedNext], [409, 'SEND_IN_PROGRESS', ['wait']]);
    // Its deadline passes with no outcome: now it is unknown, and still blocks a new send.
    store.rows[0].deadlineAt = '2026-11-16T11:59:00.000Z';
    const unknown = await send(store, frontOk([]), await requestFor(store, 'deposit', { sendKey: 'third-click-key' }));
    assert.deepEqual([unknown.status, unknown.body.code, unknown.body.sendId], [409, 'UNRESOLVED_SEND', 90]);
    assert.equal(store.rows.length, 1);
    // The other part of the payment is not held up by it.
    const mixed = memoryStore({ source: sourceOf([depositLine(), balanceLine()]) });
    mixed.rows.push({ ...store.rows[0] });
    assert.equal((await send(mixed, frontOk([]), await requestFor(mixed, 'balance', { sendKey: 'balance-click-key' }), 'balance')).status, 200);
});

// ── A person says what happened ───────────────────────────────────────────

const resolve = (store, sendId, outcome, actor = ACTOR) => S.resolveSend({ store, now: () => NOW }, { sendId, outcome, actor });

test('an unknown send: "it went" finalises it as sent, with who said so; "it did not" makes it failed', async () => {
    const store = memoryStore();
    await send(store, async () => ({ ok: false, reason: 'network_error' }), await requestFor(store));
    const id = store.rows[0].id;
    const went = await resolve(store, id, 'sent');
    assert.deepEqual([went.status, went.body.send.status, store.rows[0].resolvedByEmail], [200, 'sent', 'ops@jfa.example']);
    // The portal email it turned out to be gets its place on the Email Receipts page.
    assert.deepEqual(store.logged.map(l => [l.confirmVia, l.sendId]), [['portal', id]]);
    // Resolved once: saying so again changes nothing.
    assert.deepEqual([(await resolve(store, id, 'not_sent')).body.code], ['NOT_UNKNOWN']);
    assert.equal(store.rows[0].status, 'sent');

    const other = memoryStore();
    await send(other, async () => ({ ok: false, reason: 'network_error' }), await requestFor(other));
    const not = await resolve(other, other.rows[0].id, 'not_sent');
    assert.deepEqual([not.status, not.body.send.status, other.rows[0].sendError], [200, 'failed', 'resolved: not sent']);
    // Nothing was minted for it, so there is nothing to take back, and no receipt is logged for an email that did not go.
    assert.equal(other.calls.some(c => c.startsWith('discard:')), false);
    assert.equal(other.calls.includes('logReceipt'), false);
    // And now it can be sent again.
    assert.equal((await send(other, frontOk([]), await requestFor(other, 'deposit', { sendKey: 'fresh-send-key' }))).status, 200);
});

test('a send from before 2026-11-17 that went out with the proof attached can still be resolved: its receipt is taken back, or kept', async () => {
    const old = over => ({ id: 90, paymentId: 45, noticeKind: 'deposit', delivery: 'attached', supplierName: SUPPLIER, sendKey: 'k-old-attached', requestHash: 'x', status: 'unknown', deadlineAt: '2026-11-16T11:00:00.000Z', sentTo: ['li@acme.example'], receiptToken: 'tok-old', ...over });
    const store = memoryStore();
    store.rows.push(old());
    const not = await resolve(store, 90, 'not_sent');
    assert.deepEqual([not.status, not.body.send.status, not.body.send.delivery], [200, 'failed', 'attached']);
    assert.ok(store.calls.includes('discard:tok-old'));
    // "It went": the receipt it was sent with stands; no portal receipt is logged for an attached email.
    const went = memoryStore();
    went.rows.push(old());
    assert.equal((await resolve(went, 90, 'sent')).body.send.status, 'sent');
    assert.equal(went.calls.includes('logReceipt'), false);
    assert.equal(went.calls.some(c => c.startsWith('discard:')), false);
});

test('only an unknown send can be resolved: not one still going, one that went, or one that failed', async () => {
    const store = memoryStore();
    store.rows.push({ id: 90, paymentId: 45, noticeKind: 'deposit', delivery: 'portal', supplierName: SUPPLIER, sendKey: 'k-still-going', requestHash: 'x', status: 'sending', deadlineAt: '2026-11-16T12:00:30.000Z', sentTo: [] });
    for (const outcome of ['sent', 'not_sent']) {
        const r = await resolve(store, 90, outcome);
        assert.deepEqual([r.status, r.body.code], [409, 'NOT_UNKNOWN'], outcome);
    }
    assert.equal(store.rows[0].status, 'sending');
    assert.equal(store.calls.includes('finalise'), false);
    for (const status of ['sent', 'failed']) {
        store.rows[0].status = status;
        assert.equal((await resolve(store, 90, 'sent')).body.code, 'NOT_UNKNOWN', status);
    }
    assert.deepEqual([(await resolve(store, 404, 'sent')).status, (await resolve(store, 90, 'maybe')).status], [404, 400]);
    assert.equal((await resolve(store, 90, 'sent', { email: 'w@x', userType: 'warehouse' })).status, 403);
});

test('Front\'s own answer outranks a person\'s "it did not": the sender\'s late success still records the send', async () => {
    const store = memoryStore();
    // The row was resolved "not sent" while the request that owns it was still waiting on a slow database.
    store.finalise = (orig => async (id, opts) => {
        const r = store.rows.find(x => x.id === id);
        if (opts.byExecutor && r.status === 'sending') Object.assign(r, { status: 'failed', sendError: 'resolved: not sent', resolvedAt: NOW.toISOString(), resolvedByEmail: 'someone@jfa.example' });
        return orig(id, opts);
    })(store.finalise);
    const r = await send(store, frontOk([]), await requestFor(store));
    assert.equal(r.status, 200);
    assert.equal(store.rows[0].status, 'sent');
});

// ── The dialog's preview ──────────────────────────────────────────────────

test('the preview: figures, each notice with its wording to start from, its state and its sends', async () => {
    const source = sourceOf([depositLine(), balanceLine()]);
    const sends = [{ id: 5, paymentId: 45, noticeKind: 'deposit', delivery: 'portal', supplierName: SUPPLIER, status: 'sent', sentAt: '2026-11-16T11:00:00.000Z', deadlineAt: '2026-11-16T11:01:00.000Z', sentTo: ['li@acme.example'], sentByEmail: 'ops@jfa.example', subject: 'Deposit paid', sendError: null }];
    const p = await S.previewOf(source, PORTAL_DELIVERY, sends, NOW);
    assert.deepEqual([p.paymentId, p.supplierName, p.sendable, p.blocked], [45, SUPPLIER, true, null]);
    assert.deepEqual(p.delivery, { delivery: 'portal', reason: null, portalSupplier: true });
    assert.deepEqual(p.transfer, { amount: 3000, currency: 'USD', paidOn: '2026-11-14', linesTotal: 3000, notItemised: 0, roundingDifference: 0 });
    assert.deepEqual(p.proofs, [{ id: 77, filename: 'proof.pdf', url: proofDoc().url }]);
    assert.match(p.previewToken, /^[a-f0-9]{64}$/);
    assert.deepEqual(p.notices.map(n => [n.kind, n.amount, n.state, n.lastSentAt, n.templateKey, n.portalUrl]), [
        ['deposit', 1000, 'sent', '2026-11-16T11:00:00.000Z', 'payment_proof_deposit_portal', `${PORTAL}/payments?payment=45`],
        ['balance', 2000, 'not_sent', null, 'payment_proof_balance_portal', `${PORTAL}/payments?payment=45`],
    ]);
    assert.deepEqual(p.notices[0].tokens, { amount: '1,000.00', currency: 'USD', paidOn: '2026-11-14', paidFor: 'PO_001J' });
    assert.deepEqual(p.notices[0].readyDates, [{ poNumber: 'PO_001J', url: `${PORTAL}/orders/101` }]);
    assert.deepEqual(p.notices[1].readyDates, []);
    assert.deepEqual(p.notices[0].sends.map(s => [s.id, s.status, s.sentTo, s.sentByEmail]), [[5, 'sent', ['li@acme.example'], 'ops@jfa.example']]);
    assert.deepEqual(p.notices[1].sends, []);
    // What each part asks for, with a button in its email: a deposit's orders, a balance's containers.
    assert.deepEqual(p.notices.map(n => n.telex), [[], ['333']]);
    // Nobody of that name on the portal: it cannot be sent, and says what to fix. There is no attached way.
    const a = await S.previewOf(source, NO_SUPPLIER_DELIVERY, [], NOW);
    assert.deepEqual(a.delivery, { delivery: null, reason: 'no_supplier', portalSupplier: false });
    assert.deepEqual([a.sendable, a.blocked.status, a.blocked.code, a.blocked.reason, a.blocked.allowedNext], [false, 409, 'SUPPLIER_NOT_ON_PORTAL', 'no_supplier', ['edit_payment']]);
    assert.deepEqual(a.notices.map(n => [n.templateKey, n.portalUrl, n.readyDates]), [['payment_proof_deposit_portal', null, []], ['payment_proof_balance_portal', null, []]]);
    assert.notEqual(a.previewToken, p.previewToken);
    // The portal not deployed on this stage: nothing can be sent from here yet, whoever the supplier is.
    const off = await S.previewOf(source, NOT_DEPLOYED_DELIVERY, [], NOW);
    assert.deepEqual(off.delivery, { delivery: null, reason: 'portal_unavailable', portalSupplier: true });
    assert.deepEqual([off.sendable, off.blocked.code, off.blocked.reason, off.blocked.allowedNext], [false, 'PORTAL_NOT_LIVE', 'portal_unavailable', []]);
    assert.deepEqual(off.notices.map(n => n.templateKey), ['payment_proof_deposit_portal', 'payment_proof_balance_portal']);
});

test('the preview of a transfer that cannot be sent says why: what is wrong with the payment first, then with the portal', async () => {
    const none = await S.previewOf(sourceOf([depositLine()], { documents: [] }), PORTAL_DELIVERY, [], NOW);
    assert.deepEqual([none.sendable, none.blocked.code, none.blocked.allowedNext], [false, 'NO_PROOF', ['attach_proof']]);
    // Both wrong: the payment's own fault is the one the sender can act on now.
    const both = await S.previewOf(sourceOf([depositLine()], { documents: [] }), NOT_DEPLOYED_DELIVERY, [], NOW);
    assert.equal(both.blocked.code, 'NO_PROOF');
    // A forwarder's payment is never told to "match the supplier record": it is not a supplier's at all.
    const forwarder = sourceOf([{ id: 3, kind: 'extra', targetId: 5, amount: 900, paidWith: null, target: target({ ridesWith: 'shipment', extraKind: 'freight', extraSupplier: 'DCG' }) }]);
    assert.equal((await S.previewOf(forwarder, NO_SUPPLIER_DELIVERY, [], NOW)).blocked.code, 'NOT_A_SUPPLIER_PAYMENT');
    // Proofs too large or too many to attach are no obstacle: nothing is attached.
    const big = [proofDoc({ fileSize: 16 * 1024 * 1024 }), ...[2, 3, 4, 5, 6].map(id => proofDoc({ id }))];
    assert.equal((await S.previewOf(sourceOf([depositLine()], { documents: big }), PORTAL_DELIVERY, [], NOW)).sendable, true);
});

// ── The real store's statements ───────────────────────────────────────────

// A connection that answers by pattern and records what it was asked.
function scripted(answers = []) {
    const log = [];
    const conn = {
        log,
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            log.push({ sql: s, params });
            for (const [pattern, answer] of answers) {
                if (pattern.test(s)) {
                    const out = typeof answer === 'function' ? answer(s, params) : answer;
                    if (out instanceof Error) throw out;
                    return out;
                }
            }
            return [[]];
        },
        async beginTransaction() { log.push({ sql: 'BEGIN' }); },
        async commit() { log.push({ sql: 'COMMIT' }); },
        async rollback() { log.push({ sql: 'ROLLBACK' }); },
    };
    return conn;
}
const storeOn = (conn, over = {}) => S.createStore({
    withConnection: fn => fn(conn),
    lockSupplierPaymentTargets: async (c, args) => { conn.log.push({ sql: 'LOCK', params: args }); return { paymentLive: over.paymentLive ?? true }; },
    getEmailTemplate: async () => ({ subject: 's', bodyHtml: 'b' }),
    recordAudit: async (c, a) => { conn.log.push({ sql: 'AUDIT', params: a }); },
    preapprove: async () => ({ status: 'ok', suppliers: [], poSupplierMatched: true, users: [] }),
    env: { SUPPLIER_PORTAL_URL: PORTAL },
    ...over,
});
const sqlOf = conn => conn.log.map(x => x.sql);

test('the store finalises with one guarded UPDATE, the receipt link and the audit in one transaction', async () => {
    const row = { id: 7, supplier_payment_id: 45, notice_kind: 'deposit', delivery: 'attached', supplier_name: SUPPLIER, status: 'sent', sent_to: '["li@acme.example"]', receipt_token: 'tok-1', deadline_at_iso: '2026-11-16T12:01:00Z', sent_at_iso: '2026-11-16T12:00:05Z' };
    const conn = scripted([[/^UPDATE supplier_payment_sends SET status = 'sent'/, [{ affectedRows: 1 }]], [/FROM supplier_payment_sends s WHERE s\.id = \?/, [[row]]]]);
    const r = await storeOn(conn).finalise(7, { front: { frontMessageUid: 'm', frontConversationId: 'c' }, byExecutor: true });
    assert.equal(r.ok, true);
    assert.equal(r.send.status, 'sent');
    const u = conn.log.find(x => x.sql.startsWith('UPDATE supplier_payment_sends SET status = \'sent\''));
    // From "sending" or "unknown"; and, for the request that holds Front's answer only, from a person's "not sent".
    assert.match(u.sql, /WHERE id = \? AND \(status IN \('sending', 'unknown'\) OR \(\? = 1 AND status = 'failed' AND resolved_at IS NOT NULL\)\)$/);
    assert.match(u.sql, /sent_at = COALESCE\(sent_at, UTC_TIMESTAMP\(\)\)/);
    assert.deepEqual(u.params.slice(-2), [7, 1]);
    const order = sqlOf(conn).map(s => (s === 'BEGIN' || s === 'COMMIT' || s === 'AUDIT' ? s : s.startsWith('UPDATE supplier_payment_sends') ? 'UPDATE send' : s.startsWith('UPDATE email_receipts') ? 'LINK receipt' : s.includes('FROM supplier_payment_sends') ? 'READ send' : 'other'));
    assert.deepEqual(order, ['BEGIN', 'UPDATE send', 'READ send', 'LINK receipt', 'AUDIT', 'COMMIT']);
    const link = conn.log.find(x => x.sql.startsWith('UPDATE email_receipts'));
    assert.deepEqual(link.params, ['supplier_payment_sends', 7, 'tok-1']);
});

test('the store: a send that is no longer "sending" or "unknown" is not finalised, and nothing else is written', async () => {
    const conn = scripted([[/^UPDATE supplier_payment_sends SET status = 'sent'/, [{ affectedRows: 0 }]], [/FROM supplier_payment_sends s WHERE s\.id = \?/, [[{ id: 7, status: 'failed', sent_to: '[]' }]]]]);
    const r = await storeOn(conn).finalise(7, { resolvedBy: 'ops@jfa.example' });
    assert.equal(r.ok, false);
    assert.equal(sqlOf(conn).some(s => s.startsWith('UPDATE email_receipts') || s === 'AUDIT'), false);
    assert.ok(sqlOf(conn).includes('ROLLBACK'));
    // A person may only resolve an unknown send: never the executor's override.
    const u = conn.log.find(x => x.sql.startsWith('UPDATE supplier_payment_sends SET status = \'sent\''));
    assert.match(u.sql, /WHERE id = \? AND \(status = 'unknown' OR \(status = 'sending' AND deadline_at < UTC_TIMESTAMP\(\)\)\)$/);
    // A send confirmed by hand keeps the attempt's own time, not the time it was confirmed.
    assert.match(u.sql, /sent_at = COALESCE\(sent_at, deadline_at - INTERVAL 60 SECOND\)/);
    assert.match(u.sql, /resolved_at = UTC_TIMESTAMP\(\), resolved_by_email = \?/);
});

test('the store: failing, marking unknown and "it did not go" are each guarded on the state they may leave', async () => {
    const conn = scripted([[/^UPDATE supplier_payment_sends/, [{ affectedRows: 1 }]], [/FROM supplier_payment_sends s WHERE s\.id = \?/, [[{ id: 7, status: 'failed', sent_to: '[]' }]]]]);
    const store = storeOn(conn);
    await store.fail(7, 'front_error: 422');
    await store.unknown(7, 'network_error');
    await store.resolveNotSent(7, { resolvedBy: 'ops@jfa.example' });
    const [fail, unknown, not] = conn.log.filter(x => x.sql.startsWith('UPDATE supplier_payment_sends'));
    assert.match(fail.sql, /SET status = 'failed', send_error = \? WHERE id = \? AND status = 'sending'$/);
    assert.deepEqual(fail.params, ['front_error: 422', 7]);
    assert.match(unknown.sql, /SET status = 'unknown', send_error = \? WHERE id = \? AND status = 'sending'$/);
    assert.match(not.sql, /SET status = 'failed', send_error = 'resolved: not sent', resolved_at = UTC_TIMESTAMP\(\), resolved_by_email = \? WHERE id = \? AND \(status = 'unknown' OR \(status = 'sending' AND deadline_at < UTC_TIMESTAMP\(\)\)\)$/);
});

test('the store reserves under the payment\'s locks: lock, read again, decide, insert, commit', async () => {
    const payRow = { id: 45, supplier_name: SUPPLIER, supplier_key: 'acme medical co., ltd', amount: '1000.00', currency: 'USD', paid_on: '2026-11-14' };
    const lineRow = { id: 1, target_kind: 'po_deposit', target_id: 101, amount: '1000.00', paid_with_json: null, payment_type: null, po_id: 101, po_number: 'PO_001J', po_supplier: SUPPLIER, jf_code: null, shipment_id: null, shipment_reference: null, bal_id: null, bal_supplier: null, invoice_number: null, bal_source: null, ex_id: null, extra_kind: null, rides_with: null, extra_supplier: null, ex_shipment_id: null, pip_id: null, order_id: null };
    const docRow = { id: 77, doc_kind: 'remittance', supplier_name: SUPPLIER, filename: 'proof.pdf', public_url: proofDoc().url, s3_key: 'shipment-payments/u1/proof.pdf', content_type: 'application/pdf', file_size: 120000 };
    const conn = scripted([
        [/FROM supplier_payments WHERE id = \? AND deleted_at IS NULL/, [[payRow]]],
        [/FROM supplier_payment_lines l/, [[lineRow]]],
        [/FROM shipment_payment_documents WHERE deleted_at IS NULL AND supplier_payment_id = \?/, [[docRow]]],
        [/FROM suppliers s WHERE s\.deleted_at IS NULL/, [[{ id: 900001, name: SUPPLIER }]]],
        [/payment_views LIMIT 1/, [[]]],
        [/FROM supplier_payment_sends s WHERE s\.supplier_payment_id IN/, [[]]],
        [/^INSERT INTO supplier_payment_sends/, [{ insertId: 12 }]],
        [/FROM supplier_payment_sends s WHERE s\.id = \?/, [[{ id: 12, supplier_payment_id: 45, notice_kind: 'deposit', delivery: 'portal', supplier_name: SUPPLIER, status: 'sending', sent_to: '["li@acme.example"]', send_key: 'key-0001-aaaa', request_hash: 'h', deadline_at_iso: '2026-11-16T12:01:00Z' }]]],
    ]);
    let decided = null;
    const out = await storeOn(conn).reserve({
        paymentId: 45,
        decide: (source, delivery, sends) => { decided = { source, delivery, sends }; return { notice: { kind: 'deposit' } }; },
        row: () => ({ noticeKind: 'deposit', delivery: 'portal', supplierName: SUPPLIER, sendKey: 'key-0001-aaaa', requestHash: 'h', deadlineAt: '2026-11-16T12:01:00.000Z', sentTo: ['li@acme.example'], subject: 'S', sentByEmail: 'ops@jfa.example' }),
    });
    assert.equal(out.send.id, 12);
    // What was decided on: the payment as read UNDER the lock.
    assert.deepEqual([decided.source.payment.amount, decided.source.lines[0].target.poNumber, decided.source.documents[0].docKind, decided.delivery.delivery], [1000, 'PO_001J', 'remittance', 'portal']);
    const steps = sqlOf(conn).map(s => (['BEGIN', 'COMMIT', 'LOCK', 'ROLLBACK'].includes(s) ? s : s.startsWith('INSERT INTO supplier_payment_sends') ? 'INSERT' : s.includes('FROM supplier_payments WHERE id') ? 'READ payment' : null)).filter(Boolean);
    assert.deepEqual(steps, ['READ payment', 'BEGIN', 'LOCK', 'READ payment', 'INSERT', 'COMMIT']);
    // The lock covers the payment and what its lines point at, as an edit's does.
    assert.deepEqual(conn.log.find(x => x.sql === 'LOCK').params, { paymentId: 45, lines: [{ kind: 'po_deposit', id: 101 }] });
    const ins = conn.log.find(x => x.sql.startsWith('INSERT INTO supplier_payment_sends'));
    assert.match(ins.sql, /\(supplier_payment_id, notice_kind, delivery, supplier_name, send_key, request_hash, status, deadline_at, sent_to, subject, sent_by_email\) VALUES \(\?, \?, \?, \?, \?, \?, 'sending', \?, \?, \?, \?\)$/);
    assert.deepEqual(ins.params, [45, 'deposit', 'portal', SUPPLIER, 'key-0001-aaaa', 'h', '2026-11-16 12:01:00', '["li@acme.example"]', 'S', 'ops@jfa.example']);
});

test('the store: a refusal, a payment gone, and a key already there each end without a row', async () => {
    const payRow = { id: 45, supplier_name: SUPPLIER, supplier_key: 'k', amount: '1000.00', currency: 'USD', paid_on: '2026-11-14' };
    const base = [
        [/FROM supplier_payments WHERE id = \? AND deleted_at IS NULL/, [[payRow]]],
        [/FROM supplier_payment_lines l/, [[]]],
        [/FROM shipment_payment_documents WHERE/, [[]]],
        [/FROM suppliers s WHERE/, [[]]],
        [/payment_views LIMIT 1/, [[]]],
        [/FROM supplier_payment_sends s WHERE s\.supplier_payment_id IN/, [[]]],
    ];
    const rowOf = () => ({ noticeKind: 'deposit', delivery: 'attached', supplierName: SUPPLIER, sendKey: 'k1-abcdefgh', requestHash: 'h', deadlineAt: '2026-11-16T12:01:00.000Z', sentTo: [], subject: 'S', sentByEmail: null });
    // Refused by the rules: rolled back, nothing inserted.
    const refused = scripted(base);
    const r = await storeOn(refused).reserve({ paymentId: 45, decide: () => ({ refusal: N.refusal('NO_PROOF') }), row: rowOf });
    assert.equal(r.refusal.code, 'NO_PROOF');
    assert.equal(sqlOf(refused).some(s => s.startsWith('INSERT')), false);
    assert.ok(sqlOf(refused).includes('ROLLBACK'));
    // The payment was deleted before the lock was taken.
    const gone = scripted(base);
    assert.deepEqual(await storeOn(gone, { paymentLive: false }).reserve({ paymentId: 45, decide: () => ({}), row: rowOf }), { notFound: true });
    const never = scripted([[/FROM supplier_payments WHERE id = \?/, [[]]]]);
    assert.deepEqual(await storeOn(never).reserve({ paymentId: 45, decide: () => ({}), row: rowOf }), { notFound: true });
    assert.equal(sqlOf(never).includes('BEGIN'), false);
    // Two requests with one key: the loser reads the winner's row.
    const dup = scripted([...base,
        [/^INSERT INTO supplier_payment_sends/, Object.assign(new Error('Duplicate entry'), { errno: 1062 })],
        [/FROM supplier_payment_sends s WHERE s\.send_key = \?/, [[{ id: 3, supplier_payment_id: 45, notice_kind: 'deposit', status: 'sending', sent_to: '[]', send_key: 'k1-abcdefgh' }]]],
    ]);
    const d = await storeOn(dup).reserve({ paymentId: 45, decide: () => ({ notice: {} }), row: rowOf });
    assert.equal(d.duplicate.id, 3);
    assert.ok(sqlOf(dup).includes('ROLLBACK'));
});

test('the store: a line added between the first read and the lock is not sent on a stale lock', async () => {
    const payRow = { id: 45, supplier_name: SUPPLIER, supplier_key: 'k', amount: '1000.00', currency: 'USD', paid_on: '2026-11-14' };
    const lineRow = id => ({ id, target_kind: 'balance', target_id: id, amount: '500.00', paid_with_json: null, bal_id: id, bal_supplier: SUPPLIER, shipment_id: 5333, shipment_reference: '333' });
    let reads = 0;
    const conn = scripted([
        [/FROM supplier_payments WHERE id = \? AND deleted_at IS NULL/, [[payRow]]],
        // Every read under a lock sees one more line than the read the lock was planned from.
        [/FROM supplier_payment_lines l/, () => { reads += 1; return [Array.from({ length: reads }, (_, i) => lineRow(i + 1))]; }],
        [/FROM shipment_payment_documents WHERE/, [[]]],
    ]);
    const r = await storeOn(conn).reserve({ paymentId: 45, decide: () => ({ notice: {} }), row: () => ({}) });
    assert.equal(r.refusal.code, 'PAYMENT_CHANGED');
    assert.equal(sqlOf(conn).some(s => s.startsWith('INSERT')), false);
    // Tried again once on fresh locks before giving up.
    assert.equal(sqlOf(conn).filter(s => s === 'LOCK').length, 2);
});

// ── The notice's own read of a payment ────────────────────────────────────

const LINE_BASE = {
    paid_with_json: null, pip_id: null, payment_type: null, po_id: null, po_number: null, po_supplier: null, order_id: null, jf_code: null,
    shipment_id: null, shipment_reference: null, bal_id: null, bal_supplier: null, invoice_number: null, bal_source: null,
    ex_id: null, extra_kind: null, rides_with: null, extra_supplier: null, ex_shipment_id: null,
};
const PAY_ROW = { id: 45, supplier_name: SUPPLIER, supplier_key: 'acme medical co., ltd', amount: '3790.60', currency: 'USD', paid_on: '2026-11-14' };
const loaderConn = (lines, docs = [], over = []) => scripted([
    ...over,
    [/FROM supplier_payments WHERE id = \? AND deleted_at IS NULL/, [[PAY_ROW]]],
    [/FROM supplier_payment_lines l/, [lines]],
    [/FROM shipment_payment_documents WHERE deleted_at IS NULL AND supplier_payment_id = \?/, [docs]],
]);

test('the loader: each kind of line with what it points at, and the supplier each reference is filed under', async () => {
    const lines = [
        { ...LINE_BASE, id: 1, target_kind: 'po_deposit', target_id: 101, amount: '1000.00', po_id: 101, po_number: 'PO_001J', po_supplier: SUPPLIER },
        { ...LINE_BASE, id: 2, target_kind: 'pi', target_id: 55, amount: '500.50', pip_id: 55, payment_type: 'full', po_id: 102, po_number: 'PO_002J', po_supplier: 'Acme (HK) Ltd', paid_with_json: '[{"shipmentReference":"301","amount":500.5},{"bad":1}]' },
        { ...LINE_BASE, id: 3, target_kind: 'balance', target_id: 9, amount: '2000.00', bal_id: 9, bal_supplier: SUPPLIER, shipment_id: 5333, shipment_reference: '333', invoice_number: 'INV-9', bal_source: 'top_up' },
        { ...LINE_BASE, id: 4, target_kind: 'qc', target_id: 7001, amount: '124.10', order_id: 7001, jf_code: 'JF0938_FQC', po_id: 103, po_number: 'PO_003J', po_supplier: SUPPLIER },
        { ...LINE_BASE, id: 5, target_kind: 'repackaged', target_id: 7002, amount: '76.00', order_id: 7002, jf_code: 'JF1168', po_id: 103, po_number: 'PO_003J', po_supplier: SUPPLIER },
        { ...LINE_BASE, id: 6, target_kind: 'extra', target_id: 6, amount: '90.00', ex_id: 6, extra_kind: 'pallets', rides_with: 'balance', extra_supplier: SUPPLIER, ex_shipment_id: 5333, shipment_id: 5333, shipment_reference: '333', po_id: 104, po_number: 'PO_004J', po_supplier: 'Other Factory Ltd' },
    ];
    const conn = loaderConn(lines, [{ id: 77, doc_kind: 'remittance', supplier_name: SUPPLIER, filename: 'proof.pdf', public_url: proofDoc().url, s3_key: 'shipment-payments/u1/proof.pdf', content_type: 'application/pdf', file_size: '120000' }]);
    const src = await S.loadNoticeSource(conn, 45);
    assert.deepEqual(src.payment, { id: 45, supplierName: SUPPLIER, supplierKey: 'acme medical co., ltd', amount: 3790.6, currency: 'USD', paidOn: '2026-11-14' });
    assert.deepEqual(src.lines.map(l => [l.id, l.kind, l.targetId, l.amount]), [[1, 'po_deposit', 101, 1000], [2, 'pi', 55, 500.5], [3, 'balance', 9, 2000], [4, 'qc', 7001, 124.1], [5, 'repackaged', 7002, 76], [6, 'extra', 6, 90]]);
    const t = Object.fromEntries(src.lines.map(l => [l.id, l.target]));
    assert.deepEqual([t[1].poId, t[1].poNumber, t[1].poSupplier, t[1].paymentType], [101, 'PO_001J', SUPPLIER, null]);
    // A PI: its kind, its PO with that PO's own supplier, and the containers its money was ticked against.
    assert.deepEqual([t[2].paymentType, t[2].poNumber, t[2].poSupplier], ['full', 'PO_002J', 'Acme (HK) Ltd']);
    assert.deepEqual(src.lines[1].paidWith, [{ shipmentReference: '301', amount: 500.5 }]);
    assert.equal(src.lines[0].paidWith, null);
    // A balance: its container, under the balance record's supplier; a top-up says so.
    assert.deepEqual([t[3].shipmentId, t[3].shipmentReference, t[3].shipmentSupplier, t[3].invoiceNumber, t[3].topUp, t[3].poId], [5333, '333', SUPPLIER, 'INV-9', true, null]);
    assert.deepEqual([t[4].jfCode, t[4].poNumber, t[4].topUp], ['JF0938_FQC', 'PO_003J', false]);
    assert.deepEqual([t[5].jfCode, t[5].poId], ['JF1168', 103]);
    // An extra: its own supplier, its container under THAT supplier, and its PO under the PO's own.
    assert.deepEqual([t[6].extraKind, t[6].ridesWith, t[6].extraSupplier, t[6].shipmentSupplier, t[6].poNumber, t[6].poSupplier], ['pallets', 'balance', SUPPLIER, SUPPLIER, 'PO_004J', 'Other Factory Ltd']);
    assert.deepEqual(src.documents, [{ id: 77, docKind: 'remittance', supplierName: SUPPLIER, filename: 'proof.pdf', url: proofDoc().url, s3Key: 'shipment-payments/u1/proof.pdf', contentType: 'application/pdf', fileSize: 120000 }]);
    // And through the rules: the PI under another company's name is not named, the extra's foreign PO neither.
    const [dep, bal] = N.buildNotices(src).notices;
    assert.deepEqual(dep.lines.map(l => [l.type, l.poNumber]), [['deposit', 'PO_001J']]);
    assert.deepEqual(bal.lines.map(l => [l.type, l.named, l.poNumber, l.container]), [
        ['pi_full', false, null, null], ['top_up', true, null, '333'], ['qc', true, 'PO_003J', null], ['repackaged', true, 'PO_003J', null], ['extra', true, null, '333'],
    ]);
});

test('the loader: a target that is deleted or gone is null, whatever kind of line it is', async () => {
    const gone = [
        { ...LINE_BASE, id: 1, target_kind: 'po_deposit', target_id: 999, amount: '1.00' },
        // The PI's payment row, or its invoice, is gone: the PO alone does not stand in for it.
        { ...LINE_BASE, id: 2, target_kind: 'pi', target_id: 56, amount: '1.00', po_id: 105, po_number: 'PO_005J', po_supplier: SUPPLIER },
        // The PI is there, its PO is deleted.
        { ...LINE_BASE, id: 3, target_kind: 'pi', target_id: 57, amount: '1.00', pip_id: 57, payment_type: 'deposit' },
        { ...LINE_BASE, id: 4, target_kind: 'balance', target_id: 10, amount: '1.00', shipment_id: 5333, shipment_reference: '333' },
        // The order line is deleted, its PO is not.
        { ...LINE_BASE, id: 5, target_kind: 'qc', target_id: 7003, amount: '1.00', po_id: 103, po_number: 'PO_003J', po_supplier: SUPPLIER },
        { ...LINE_BASE, id: 6, target_kind: 'repackaged', target_id: 7004, amount: '1.00', order_id: 7004 },
        { ...LINE_BASE, id: 7, target_kind: 'extra', target_id: 7, amount: '1.00', po_id: 103 },
    ];
    const src = await S.loadNoticeSource(loaderConn(gone), 45);
    assert.deepEqual(src.lines.map(l => l.target), [null, null, null, null, null, null, null]);
    assert.equal(N.buildNotices(src).blocked.code, 'LINE_TARGET_MISSING');
    // No such payment, or a deleted one: nothing.
    assert.equal(await S.loadNoticeSource(scripted([[/FROM supplier_payments WHERE id = \?/, [[]]]]), 45), null);
});

test('the loader reads only what a notice needs, and still answers a server from before two of its columns', async () => {
    const conn = loaderConn([]);
    await S.loadNoticeSource(conn, 45);
    const [pay, lines, docs] = conn.log.map(x => x.sql);
    // Never the note or the bank reference.
    assert.equal(pay, "SELECT id, supplier_name, supplier_key, amount, currency, DATE_FORMAT(paid_on, '%Y-%m-%d') AS paid_on FROM supplier_payments WHERE id = ? AND deleted_at IS NULL");
    assert.equal(/note|bank_ref/.test(conn.log.map(x => x.sql).join(' ')), false);
    // A line's target counts only while it is live: the PO, the order line, the balance record, the extra, and a PI's invoice.
    assert.match(lines, /LEFT JOIN purchase_order_invoice_payments pip ON l\.target_kind = 'pi' AND pip\.id = l\.target_id AND EXISTS \(SELECT 1 FROM purchase_order_invoices i WHERE i\.id = pip\.purchase_order_invoice_id AND i\.deleted_at IS NULL\)/);
    assert.match(lines, /LEFT JOIN orders o ON l\.target_kind IN \('qc', 'repackaged'\) AND o\.id = l\.target_id AND o\.deleted_at IS NULL/);
    assert.match(lines, /LEFT JOIN shipment_payments bal ON l\.target_kind = 'balance' AND bal\.id = l\.target_id AND bal\.deleted_at IS NULL/);
    assert.match(lines, /LEFT JOIN payment_extras ex ON l\.target_kind = 'extra' AND ex\.id = l\.target_id AND ex\.deleted_at IS NULL/);
    assert.match(lines, /LEFT JOIN purchase_orders po ON po\.deleted_at IS NULL AND po\.id = CASE l\.target_kind WHEN 'po_deposit' THEN l\.target_id WHEN 'pi' THEN pip\.purchase_order_id WHEN 'qc' THEN o\.purchase_order_id WHEN 'repackaged' THEN o\.purchase_order_id WHEN 'extra' THEN ex\.purchase_order_id END/);
    assert.match(lines, /WHERE l\.payment_id = \? ORDER BY l\.id$/);
    assert.equal(/description|unit_price|bank|amount_due|due_terms|status/.test(lines), false);
    assert.equal(docs, 'SELECT id, doc_kind, supplier_name, filename, public_url, s3_key, content_type, file_size FROM shipment_payment_documents WHERE deleted_at IS NULL AND supplier_payment_id = ? ORDER BY id');
    // paid_with_json not there yet (1054), then payment_extras not there (1146): asked again with less, twice.
    let asked = 0;
    const old = loaderConn([{ ...LINE_BASE, id: 1, target_kind: 'po_deposit', target_id: 101, amount: '5.00', po_id: 101, po_number: 'PO_001J', po_supplier: SUPPLIER }], [], [
        [/FROM supplier_payment_lines l/, (sql) => {
            asked += 1;
            if (sql.includes('l.paid_with_json')) return Object.assign(new Error('Unknown column'), { errno: 1054 });
            if (sql.includes('payment_extras')) return Object.assign(new Error('no such table'), { errno: 1146 });
            return [[{ ...LINE_BASE, id: 1, target_kind: 'po_deposit', target_id: 101, amount: '5.00', po_id: 101, po_number: 'PO_001J', po_supplier: SUPPLIER }]];
        }],
    ]);
    const src = await S.loadNoticeSource(old, 45);
    assert.equal(asked, 3);
    assert.deepEqual([src.lines.length, src.lines[0].target.poNumber], [1, 'PO_001J']);
    // Any other fault is a fault, and is not asked again with less.
    const broken = loaderConn([], [], [[/FROM supplier_payment_lines l/, Object.assign(new Error('gone away'), { errno: 2006 })]]);
    await assert.rejects(S.loadNoticeSource(broken, 45));
    assert.equal(broken.log.filter(x => x.sql.includes('FROM supplier_payment_lines l')).length, 1);
});

test('delivery from the database: the supplier matched exactly, the portal\'s own table as the sign it is there', async () => {
    const env = { SUPPLIER_PORTAL_URL: `${PORTAL}/` };
    const pay = { id: 45, supplierName: SUPPLIER };
    const withSupplier = () => scripted([[/FROM suppliers s WHERE/, [[{ id: 900001, name: SUPPLIER }, { id: 900002, name: SUPPLIER }]]]]);
    const conn = withSupplier();
    const d = await S.resolveNoticeDelivery(conn, pay, env);
    assert.deepEqual(d, {
        delivery: 'portal', reason: null, portalBase: PORTAL, portalUrl: `${PORTAL}/payments?payment=45`, registerUrl: `${PORTAL}/register`,
        suppliers: [{ id: 900001, name: SUPPLIER }, { id: 900002, name: SUPPLIER }],
    });
    assert.match(conn.log[0].sql, /FROM suppliers s WHERE s\.deleted_at IS NULL AND CONVERT\(s\.name USING utf8mb4\) COLLATE utf8mb4_0900_bin = CONVERT\(\? USING utf8mb4\) COLLATE utf8mb4_0900_bin ORDER BY s\.id$/);
    assert.deepEqual(conn.log[0].params, [SUPPLIER]);
    assert.equal(conn.log[1].sql, 'SELECT 1 AS ok FROM `jportal`.payment_views LIMIT 1');
    // No supplier of that name, no portal table, no address: it cannot go, each with its reason and no link.
    // An old PAYMENT_PROOF_PORTAL left in a stage's secret is not read: there is no switch.
    const reasons = [];
    reasons.push((await S.resolveNoticeDelivery(scripted(), pay, env)).reason);
    reasons.push((await S.resolveNoticeDelivery(scripted([[/FROM suppliers s WHERE/, [[{ id: 1, name: SUPPLIER }]]], [/payment_views/, Object.assign(new Error('no table'), { errno: 1146 })]]), pay, env)).reason);
    reasons.push((await S.resolveNoticeDelivery(withSupplier(), pay, {})).reason);
    assert.deepEqual(reasons, ['no_supplier', 'portal_unavailable', 'portal_url_unset']);
    assert.equal((await S.resolveNoticeDelivery(withSupplier(), pay, { ...env, PAYMENT_PROOF_PORTAL: 'off' })).delivery, 'portal');
    const off = await S.resolveNoticeDelivery(scripted([[/FROM suppliers s WHERE/, [[{ id: 900001, name: SUPPLIER }, { id: 900002, name: SUPPLIER }]]], [/payment_views/, Object.assign(new Error('no table'), { errno: 1146 })]]), pay, env);
    assert.deepEqual([off.delivery, off.portalUrl, off.registerUrl, off.suppliers.length], [null, null, null, 2]);
    // It still says where the portal is and who is on it.
    assert.equal(off.portalBase, PORTAL);
    assert.deepEqual(N.readyDatesFor({ readyDatePos: [{ id: 101, poNumber: 'PO_001J' }] }, off), [{ poNumber: 'PO_001J', url: `${PORTAL}/orders/101` }]);
    const noTable = await S.resolveNoticeDelivery(scripted([[/FROM suppliers s WHERE/, [[{ id: 1, name: SUPPLIER }]]], [/payment_views/, Object.assign(new Error('no table'), { errno: 1146 })]]), pay, env);
    assert.deepEqual([noTable.delivery, noTable.portalBase, noTable.suppliers], [null, PORTAL, [{ id: 1, name: SUPPLIER }]]);
    // Nobody of that name on the portal: no order of theirs can be opened there.
    assert.deepEqual(N.readyDatesFor({ readyDatePos: [{ id: 101, poNumber: 'PO_001J' }] }, await S.resolveNoticeDelivery(scripted(), pay, env)), []);
    // The portal's table failing for any other reason is a fault, not "no portal".
    await assert.rejects(S.resolveNoticeDelivery(scripted([[/payment_views/, Object.assign(new Error('gone away'), { errno: 2006 })]]), pay, env));
});

// ── Around the send: the list's markers, the reminder guard, Front's files ──

test('each listed transfer says the state of its notices; a server without the send log says nothing at all', async () => {
    const sendRowOf = over => ({ id: 1, supplier_payment_id: 45, notice_kind: 'deposit', delivery: 'portal', supplier_name: SUPPLIER, status: 'sent', sent_to: '[]', deadline_at_iso: '2026-11-16T11:01:00Z', sent_at_iso: '2026-11-16T11:00:00Z', ...over });
    const conn = scripted([[/FROM supplier_payment_sends s WHERE s\.supplier_payment_id IN \(\?, \?, \?\)/, [[sendRowOf({}), sendRowOf({ id: 2, supplier_payment_id: 46, notice_kind: 'balance', status: 'unknown', sent_at_iso: null })]]]]);
    const payments = [
        { id: 45, supplierName: SUPPLIER, lines: [{ kind: 'po_deposit', amount: 1 }, { kind: 'balance', amount: 2 }] },
        { id: 46, supplierName: SUPPLIER, lines: [{ kind: 'qc', amount: 1 }] },
        { id: 47, supplierName: 'DCG', lines: [{ kind: 'extra', ridesWith: 'shipment', amount: 1 }] },
    ];
    const out = await S.noticeSummaries(conn, payments, {}, NOW);
    assert.deepEqual(out.get(45), [
        { kind: 'deposit', state: 'sent', lastSentAt: '2026-11-16T11:00:00Z', delivery: 'portal' },
        { kind: 'balance', state: 'not_sent', lastSentAt: null, delivery: null },
    ]);
    assert.deepEqual(out.get(46), [{ kind: 'balance', state: 'unknown', lastSentAt: null, delivery: null }]);
    // A forwarder's transfer has nothing to send.
    assert.deepEqual(out.get(47), []);
    assert.deepEqual(conn.log[0].params, [45, 46, 47]);
    const none = scripted([[/supplier_payment_sends/, Object.assign(new Error('no such table'), { errno: 1146 })]]);
    assert.equal(await S.noticeSummaries(none, payments, {}, NOW), null);
    await assert.rejects(S.noticeSummaries(scripted([[/supplier_payment_sends/, Object.assign(new Error('gone'), { errno: 2006 })]]), payments, {}, NOW));
});

test('a reminder of a payment email is refused once the payment is deleted or belongs to another supplier', async () => {
    const receipt = { id: 5, send_table: 'supplier_payment_sends', send_id: 12 };
    const answer = current => scripted([[/FROM supplier_payment_sends s LEFT JOIN supplier_payments sp/, [current === null ? [] : [{ current }]]]]);
    assert.equal(await S.reminderRefusal(answer(1), receipt), null);
    for (const current of [0, null]) {
        const r = await S.reminderRefusal(answer(current), receipt);
        assert.deepEqual([r.status, r.code, r.allowedNext], [409, 'PAYMENT_NOT_CURRENT', ['send_again']], String(current));
    }
    const conn = answer(1);
    await S.reminderRefusal(conn, receipt);
    // A live payment, whose supplier is exactly the one the email went to.
    assert.match(conn.log[0].sql, /LEFT JOIN supplier_payments sp ON sp\.id = s\.supplier_payment_id AND sp\.deleted_at IS NULL WHERE s\.id = \?$/);
    assert.match(conn.log[0].sql, /CONVERT\(sp\.supplier_name USING utf8mb4\) COLLATE utf8mb4_0900_bin = CONVERT\(s\.supplier_name USING utf8mb4\) COLLATE utf8mb4_0900_bin/);
    assert.deepEqual(conn.log[0].params, [12]);
    // Any other email is not this guard's business, and asks the database nothing.
    const other = scripted();
    assert.equal(await S.reminderRefusal(other, { id: 6, send_table: 'purchase_order_document_sends', send_id: 3 }), null);
    assert.equal(await S.reminderRefusal(other, { id: 7, send_table: null, send_id: null }), null);
    assert.equal(other.log.length, 0);
});

test('Front: attachments ride as files on the request; without any, the request is as it always was', async () => {
    const { sendFrontMessage } = require('../src/services/front-send');
    const seen = [];
    const fetchImpl = async (url, init) => {
        seen.push({ url, init });
        return { ok: true, status: 202, text: async () => JSON.stringify({ message_uid: 'm1', _links: { related: { conversation: 'https://api2.frontapp.com/conversations/cnv_9' } } }) };
    };
    const env = { FRONT_CHANNEL_ID: 'cha_1', FRONT_API_TOKEN: 't' };
    const r = await sendFrontMessage({
        to: ['a@x.com'], subject: 'S', html: '<p>h</p>',
        attachments: [{ filename: 'proof.pdf', contentType: 'application/pdf', bytes: Buffer.from('PDF-1') }, { filename: 'scan.png', contentType: 'image/png', bytes: Buffer.from('PNG') }],
    }, { fetchImpl, env });
    assert.deepEqual(r, { ok: true, frontMessageUid: 'm1', frontConversationId: 'cnv_9' });
    const files = seen[0].init.body.getAll('attachments[]');
    assert.deepEqual(files.map(f => [f.name, f.type]), [['proof.pdf', 'application/pdf'], ['scan.png', 'image/png']]);
    assert.equal(await files[0].text(), 'PDF-1');
    assert.deepEqual(seen[0].init.body.getAll('to[]'), ['a@x.com']);
    await sendFrontMessage({ to: ['a@x.com'], subject: 'S', html: 'h' }, { fetchImpl, env });
    assert.equal(seen[1].init.body.getAll('attachments[]').length, 0);
    assert.deepEqual([...seen[1].init.body.keys()].sort(), ['body', 'body_format', 'options[archive]', 'subject', 'to[]']);
});

test('the store reads who opened a payment on the portal, and answers a server without the portal as nobody', async () => {
    const conn = scripted([[/payment_views v WHERE v\.supplier_payment_id = \?/, [[{ user_email: 'li@acme.example', first_iso: '2026-11-16T12:10:00Z', last_iso: '2026-11-16T13:00:00Z', view_count: 3 }]]]]);
    assert.deepEqual(await storeOn(conn).views(45), [{ email: 'li@acme.example', firstViewedAt: '2026-11-16T12:10:00Z', lastViewedAt: '2026-11-16T13:00:00Z', count: 3 }]);
    assert.match(conn.log[0].sql, /FROM `jportal`\.payment_views v WHERE v\.supplier_payment_id = \? ORDER BY v\.first_viewed_at, v\.user_email$/);
    const none = scripted([[/payment_views/, Object.assign(new Error('no such table'), { errno: 1146 })]]);
    assert.deepEqual(await storeOn(none).views(45), []);
});
