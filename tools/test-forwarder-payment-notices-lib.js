'use strict';

// Unit tests for "Send to forwarder" (src/lib/payment-notices.js;
// docs/payment-proof-send.md, "A forwarder's payment"): which transfer is a
// forwarder's, what its one email says, when it cannot be sent, who it may go
// to, the state of its send, and what counts as the forwarder having opened or
// acknowledged it. Pure: no database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const N = require('../src/lib/payment-notices');

const PAYEE = 'DCG Logistics';

// A line as the notice loader hands it over (src/services/payment-notices.js).
const line = (kind, amount, target = {}, extra = {}) => ({
    id: extra.id ?? 1, kind, targetId: extra.targetId ?? 10, amount, paidWith: null,
    target: target === null ? null : {
        paymentType: null, poId: null, poNumber: null, poSupplier: null,
        shipmentId: null, shipmentReference: null, shipmentSupplier: null,
        invoiceNumber: null, topUp: false, jfCode: null, extraKind: null, ridesWith: null, extraSupplier: null, forwarderId: null,
        ...target,
    },
});
// A forwarder's charge on container `ref`, linked to forwarder `forwarderId`.
const charge = (ref, amount, forwarderId = 7, over = {}) => line('extra', amount, {
    ridesWith: 'shipment', extraKind: 'freight', extraSupplier: PAYEE, shipmentSupplier: PAYEE,
    shipmentId: 5000 + Number(ref), shipmentReference: String(ref), forwarderId, ...(over.target || {}),
}, { id: over.id ?? Number(ref), targetId: over.targetId ?? 900 + Number(ref) });
const proof = (over = {}) => ({
    id: 77, docKind: 'remittance', supplierName: PAYEE, filename: 'proof.pdf',
    url: 'https://shipping-purchase-orders-test.s3.eu-north-1.amazonaws.com/shipment-payments/u1/proof.pdf', ...over,
});
const source = (lines, over = {}) => ({
    payment: { id: 45, supplierName: PAYEE, supplierKey: 'dcg logistics', amount: lines.reduce((a, l) => a + l.amount, 0), currency: 'USD', paidOn: '2026-11-21', ...(over.payment || {}) },
    lines,
    documents: over.documents ?? [proof()],
});

// ── Whose payment it is ───────────────────────────────────────────────────

test('whose transfer it is: every line a charge of one linked forwarder, or it says what is in the way', () => {
    const fwd = (forwarderId = 7) => ({ kind: 'extra', ridesWith: 'shipment', forwarderId });
    assert.equal(N.forwarderOfLines([{ kind: 'balance' }, { kind: 'extra', ridesWith: 'balance' }]), null);
    assert.equal(N.forwarderOfLines([]), null);
    assert.deepEqual(N.forwarderOfLines([fwd(), fwd()]), { forwarderId: 7, blocked: null });
    assert.deepEqual(N.forwarderOfLines([fwd(), { kind: 'balance' }]), { forwarderId: null, blocked: 'MIXED_PAYMENT' });
    // A credit on the account is not this forwarder's charge either.
    assert.deepEqual(N.forwarderOfLines([fwd(), { kind: 'extra', ridesWith: 'account' }]), { forwarderId: null, blocked: 'MIXED_PAYMENT' });
    assert.deepEqual(N.forwarderOfLines([fwd(), fwd(null)]), { forwarderId: null, blocked: 'FORWARDER_NOT_LINKED' });
    // From a server that has no forwarder_id column yet: the key is not there at all.
    assert.deepEqual(N.forwarderOfLines([{ kind: 'extra', ridesWith: 'shipment' }]), { forwarderId: null, blocked: 'FORWARDER_NOT_LINKED' });
    assert.deepEqual(N.forwarderOfLines([fwd(7), fwd(8)]), { forwarderId: null, blocked: 'FORWARDERS_DIFFER' });
    // Mixed is said before a missing link: the link would not make it sendable.
    assert.deepEqual(N.forwarderOfLines([fwd(null), { kind: 'po_deposit' }]), { forwarderId: null, blocked: 'MIXED_PAYMENT' });
});

// ── The one email ─────────────────────────────────────────────────────────

test('a forwarder\'s transfer is one email: each charge as its container, kind and amount, and nothing else', () => {
    const b = N.buildNotices(source([charge(339, 6666), charge(334, 1200.5, 7, { target: { extraKind: 'customs' } })]));
    assert.equal(b.audience, 'forwarder');
    assert.equal(b.forwarderId, 7);
    assert.equal(b.sendable, true);
    assert.equal(b.blocked, null);
    assert.deepEqual(b.notices.map(n => [n.kind, n.amount, n.paidFor, n.unnamed, n.blocked]), [['forwarder', 7866.5, ['339', '334'], 0, null]]);
    assert.deepEqual(b.notices[0].lines.map(l => [l.type, l.container, l.extraKind, l.amount, l.shipmentId, l.named]), [
        ['forwarder', '339', 'freight', 6666, 5339, true],
        ['forwarder', '334', 'customs', 1200.5, 5334, true],
    ]);
    // Nothing a supplier's line can carry is ever set on a forwarder's.
    for (const l of b.notices[0].lines) assert.deepEqual([l.poId, l.poNumber, l.invoiceNumber, l.jfCode, l.containers], [null, null, null, null, null]);
    assert.deepEqual(b.proofs.map(p => p.id), [77]);
    assert.deepEqual(N.tokensFor(b, b.notices[0]), { amount: '7,866.50', currency: 'USD', paidOn: '2026-11-21', paidFor: '339, 334' });
    assert.equal(N.TEMPLATE_KEYS.forwarder, 'payment_proof_forwarder_portal');
});

test('a container is told by its number only, never by the name our team gave the draft; one with no number is not told', () => {
    const told = ref => N.buildNotices(source([charge(339, 900, 7, { target: { shipmentReference: ref } })])).notices[0];
    assert.deepEqual([told('2026/11/18 - 334 Ningbo').lines[0].container, told('2026/11/18 - 334 Ningbo').paidFor], ['334', ['334']]);
    assert.deepEqual([told('Some internal name').lines[0].container, told('Some internal name').paidFor], [null, []]);
    // A dash where the wording names the containers, never the internal name.
    const b = N.buildNotices(source([charge(339, 900, 7, { target: { shipmentReference: 'Some internal name' } })]));
    assert.equal(N.tokensFor(b, b.notices[0]).paidFor, '—');
});

test('a supplier\'s transfer is told as before, and says it is a supplier\'s', () => {
    const b = N.buildNotices(source([line('balance', 2000, { shipmentId: 5333, shipmentReference: '333', shipmentSupplier: PAYEE })]));
    assert.equal(b.audience, 'supplier');
    assert.equal(b.forwarderId, null);
    assert.deepEqual(b.notices.map(n => n.kind), ['balance']);
});

test('what stops a forwarder\'s transfer, in the order it is said', () => {
    const code = (lines, over) => { const b = N.buildNotices(source(lines, over)); return [b.sendable, b.blocked && b.blocked.code, b.audience]; };
    const goods = line('balance', 500, { shipmentId: 5333, shipmentReference: '333', shipmentSupplier: PAYEE }, { id: 2, targetId: 9 });
    assert.deepEqual(code([charge(339, 900), goods]), [false, 'MIXED_PAYMENT', 'forwarder']);
    // Mixed wins over everything else that is wrong with it.
    assert.deepEqual(code([charge(339, 900, null), goods], { documents: [] }), [false, 'MIXED_PAYMENT', 'forwarder']);
    // Also over "no money went" and a deleted target: splitting it comes first.
    assert.deepEqual(code([charge(339, 900), goods], { payment: { amount: 0 } }), [false, 'MIXED_PAYMENT', 'forwarder']);
    assert.deepEqual(code([charge(339, 900), goods, line('extra', 100, null, { id: 3, targetId: 5 })]), [false, 'MIXED_PAYMENT', 'forwarder']);
    assert.deepEqual(code([charge(339, 900), charge(334, -900)], { payment: { amount: 0 } }), [false, 'NOTHING_SENT', 'forwarder']);
    assert.deepEqual(code([charge(339, 900), line('extra', 100, null, { id: 2, targetId: 5 })]), [false, 'LINE_TARGET_MISSING', 'forwarder']);
    assert.deepEqual(code([charge(339, 900, null)]), [false, 'FORWARDER_NOT_LINKED', 'forwarder']);
    assert.deepEqual(code([charge(339, 900, 7), charge(334, 100, 8)]), [false, 'FORWARDERS_DIFFER', 'forwarder']);
    assert.deepEqual(code([charge(339, 900.02)], { payment: { amount: 900 } }), [false, 'LINES_EXCEED_TRANSFER', 'forwarder']);
    assert.deepEqual(code([charge(339, 900)], { documents: [proof({ supplierName: 'Somebody Else' })] }), [false, 'PROOF_NOT_SENDABLE', 'forwarder']);
    assert.deepEqual(code([charge(339, 900)], { documents: [] }), [false, 'NO_PROOF', 'forwarder']);
    assert.deepEqual(code([charge(339, 900)]), [true, null, 'forwarder']);
    // No refusal reads as a supplier's any more.
    assert.equal(N.refusal('MIXED_PAYMENT').status, 409);
    assert.match(N.refusal('MIXED_PAYMENT').error, /forwarder/);
    assert.match(N.refusal('FORWARDER_NOT_LINKED').error, /Forwarders list/);
});

test('a forwarder\'s email that comes to nothing is not sent', () => {
    const b = N.buildNotices(source([charge(339, 900), charge(334, -900)], { payment: { amount: 50 } }));
    assert.equal(b.notices[0].blocked.code, 'NOTICE_NOT_POSITIVE');
});

// ── Whether it can go out, and to whom ────────────────────────────────────

test('it goes out as a portal link when the portal can show it and the forwarder has someone to send it to', () => {
    const ok = { portalBase: 'https://portal.example', portalAvailable: true, forwarder: { id: 7, name: 'DCG', removed: false }, contacts: [{ name: null, email: 'ops@dcg.example' }] };
    assert.deepEqual(N.forwarderDelivery(ok), { delivery: 'portal', reason: null });
    assert.deepEqual(N.forwarderDelivery({ ...ok, portalBase: '' }), { delivery: null, reason: 'portal_url_unset' });
    assert.deepEqual(N.forwarderDelivery({ ...ok, portalAvailable: false }), { delivery: null, reason: 'portal_unavailable' });
    assert.deepEqual(N.forwarderDelivery({ ...ok, forwarder: null }), { delivery: null, reason: 'forwarder_removed' });
    assert.deepEqual(N.forwarderDelivery({ ...ok, forwarder: { id: 7, name: 'DCG', removed: true } }), { delivery: null, reason: 'forwarder_removed' });
    assert.deepEqual(N.forwarderDelivery({ ...ok, contacts: [] }), { delivery: null, reason: 'no_contact' });

    assert.equal(N.forwarderRefusal({ delivery: 'portal', reason: null }, 'DCG'), null);
    const said = reason => { const r = N.forwarderRefusal({ delivery: null, reason }, 'DCG'); return [r.status, r.code, r.reason]; };
    assert.deepEqual(said('portal_url_unset'), [409, 'PORTAL_NOT_LIVE', 'portal_url_unset']);
    assert.deepEqual(said('portal_unavailable'), [409, 'PORTAL_NOT_LIVE', 'portal_unavailable']);
    assert.deepEqual(said('forwarder_removed'), [409, 'FORWARDER_REMOVED', 'forwarder_removed']);
    assert.deepEqual(said('no_contact'), [409, 'FORWARDER_NO_CONTACT', 'no_contact']);
    assert.match(N.forwarderRefusal({ delivery: null, reason: 'no_contact' }, 'DCG').error, /"DCG" has no contact/);
    // Nothing about it says "supplier portal ... supplier record".
    assert.doesNotMatch(N.forwarderRefusal({ delivery: null, reason: 'portal_unavailable' }, 'DCG').error, /supplier record/);
});

test('only the forwarder\'s own contacts can be sent it: trimmed, any case, each once, at least one', () => {
    const contacts = [{ name: 'Ann', email: 'ann@dcg.example' }, { name: null, email: 'ops@dcg.example' }];
    assert.deepEqual(N.recipientsAreContacts(['  Ann@DCG.example ', 'ops@dcg.example', 'ann@dcg.example'], contacts), { to: ['ann@dcg.example', 'ops@dcg.example'], notContacts: [] });
    assert.deepEqual(N.recipientsAreContacts(['ops@dcg.example'], contacts), { to: ['ops@dcg.example'], notContacts: [] });
    assert.deepEqual(N.recipientsAreContacts(['ann@dcg.example', 'boss@other.example'], contacts), { to: ['ann@dcg.example'], notContacts: ['boss@other.example'] });
    assert.deepEqual(N.recipientsAreContacts([], contacts), { to: [], notContacts: [] });
    assert.deepEqual(N.recipientsAreContacts(['ann@dcg.example'], []), { to: [], notContacts: ['ann@dcg.example'] });
    const r = N.refusal('NOT_A_CONTACT', { notContacts: ['boss@other.example'] });
    assert.deepEqual([r.status, r.code, r.notContacts], [400, 'NOT_A_CONTACT', ['boss@other.example']]);
});

test('the link leads to the forwarder\'s own page, and the block under the wording asks for nothing', () => {
    assert.equal(N.forwarderPaymentPortalUrl('https://portal.example', 45), 'https://portal.example/freight-payments?payment=45');
    const html = N.forwarderBlockHtml({ url: 'https://portal.example/freight-payments?payment=45', registerUrl: 'https://portal.example/register', preapproved: true });
    assert.ok(html.includes('https://portal.example/freight-payments?payment=45'));
    assert.ok(html.includes('https://portal.example/register'));
    assert.ok(html.includes('No file is attached'));
    assert.ok(html.includes('already approved'));
    assert.doesNotMatch(html, /telex|ready date|电放/i);
});

// ── What the dialog showed ────────────────────────────────────────────────

test('the preview token changes when the payment is another forwarder\'s, and a supplier\'s token does not carry one', () => {
    const a = N.buildNotices(source([charge(339, 900, 7)]));
    const b = N.buildNotices(source([charge(339, 900, 8)]));
    assert.notEqual(N.previewToken(a, 'portal'), N.previewToken(b, 'portal'));
    assert.equal(N.previewToken(a, 'portal'), N.previewToken(N.buildNotices(source([charge(339, 900, 7)])), 'portal'));
    assert.notEqual(N.previewToken(a, 'portal'), N.previewToken(N.buildNotices(source([charge(339, 901, 7)])), 'portal'));
});

// ── The state of the send ─────────────────────────────────────────────────

const NOW = new Date('2026-11-21T12:00:00Z');
const send = (over = {}) => ({
    id: 1, noticeKind: 'forwarder', delivery: 'portal', supplierName: PAYEE, forwarderId: 7, status: 'sent',
    sentAt: '2026-11-21T10:00:00Z', deadlineAt: '2026-11-21T10:01:00Z', ...over,
});
const fwdLine = (forwarderId = 7) => ({ kind: 'extra', ridesWith: 'shipment', forwarderId });

test('the forwarder part of a transfer, as the payments list says it: none for a supplier\'s or a mixed one', () => {
    assert.equal(N.forwarderNoticeOf([{ kind: 'balance' }], [], NOW), null);
    assert.equal(N.forwarderNoticeOf([fwdLine(), { kind: 'balance' }], [send()], NOW), null);
    assert.equal(N.forwarderNoticeOf([], [], NOW), null);
    assert.deepEqual(N.forwarderNoticeOf([fwdLine()], [], NOW), { kind: 'forwarder', state: 'not_sent', lastSentAt: null, delivery: null });
    // Not linked yet: the button still shows, and the dialog says why it cannot go.
    assert.deepEqual(N.forwarderNoticeOf([fwdLine(null)], [], NOW), { kind: 'forwarder', state: 'not_sent', lastSentAt: null, delivery: null });
    assert.deepEqual(N.forwarderNoticeOf([fwdLine()], [send(), send({ id: 2, sentAt: '2026-11-21T11:00:00Z' })], NOW),
        { kind: 'forwarder', state: 'sent', lastSentAt: '2026-11-21T11:00:00Z', delivery: 'portal' });
    // The supplier parts of such a transfer stay none, so an older ShipLine shows no button.
    assert.deepEqual(N.noticeKindsOf([fwdLine()]), []);
});

test('a send counts only for the forwarder the transfer belongs to now; an unknown or running one always shows', () => {
    const state = (lines, sends) => N.forwarderNoticeOf(lines, sends, NOW).state;
    assert.equal(state([fwdLine(8)], [send({ forwarderId: 7 })]), 'not_sent');
    assert.equal(state([fwdLine(null)], [send({ forwarderId: 7 })]), 'not_sent');
    // A supplier send of the same transfer, from before it became a forwarder's, is not this part's.
    assert.equal(state([fwdLine()], [send({ noticeKind: 'balance', forwarderId: null })]), 'not_sent');
    assert.equal(state([fwdLine()], [send({ status: 'failed', sentAt: null })]), 'not_sent');
    assert.equal(state([fwdLine()], [send(), send({ id: 2, status: 'unknown', sentAt: null })]), 'unknown');
    assert.equal(state([fwdLine()], [send({ status: 'sending', sentAt: null, deadlineAt: '2026-11-21T12:00:30Z' })]), 'sending');
    assert.equal(state([fwdLine()], [send({ status: 'sending', sentAt: null, deadlineAt: '2026-11-21T11:00:00Z' })]), 'unknown');
});

// ── What the forwarder did with it ────────────────────────────────────────

const ev = (over = {}) => ({ forwarderId: 7, event: 'opened', userEmail: 'ann@dcg.example', userName: 'Ann Lee', revision: 'r1', at: '2026-11-21T10:30:00Z', ...over });

test('opened and acknowledged are the present forwarder\'s events only, and an acknowledgement is current only at the present revision', () => {
    const events = [
        ev(), ev({ at: '2026-11-21T11:00:00Z' }), ev({ userEmail: 'ops@dcg.example', userName: null, at: '2026-11-21T10:45:00Z' }),
        ev({ event: 'acknowledged', at: '2026-11-21T11:05:00Z' }),
        // Another forwarder's, from when the payment was theirs.
        ev({ forwarderId: 8, userEmail: 'x@other.example', userName: 'X' }), ev({ forwarderId: 8, event: 'acknowledged', userEmail: 'x@other.example', userName: 'X', at: '2026-11-21T11:30:00Z' }),
    ];
    const got = N.forwarderEvidence(events, { forwarderId: 7, revision: 'r1' });
    assert.deepEqual(got.opened, [
        { email: 'ann@dcg.example', name: 'Ann Lee', firstAt: '2026-11-21T10:30:00Z', lastAt: '2026-11-21T11:00:00Z', count: 2 },
        { email: 'ops@dcg.example', name: null, firstAt: '2026-11-21T10:45:00Z', lastAt: '2026-11-21T10:45:00Z', count: 1 },
    ]);
    assert.deepEqual(got.acknowledgement, { byName: 'Ann Lee', byEmail: 'ann@dcg.example', at: '2026-11-21T11:05:00Z', current: true });
    // The payment changed since: the acknowledgement is of an earlier figure.
    assert.deepEqual(N.forwarderEvidence(events, { forwarderId: 7, revision: 'r2' }).acknowledgement, { byName: 'Ann Lee', byEmail: 'ann@dcg.example', at: '2026-11-21T11:05:00Z', current: false });
    // Not on the portal's page at all (never sent, or moved away): nothing is current.
    assert.equal(N.forwarderEvidence(events, { forwarderId: 7, revision: null }).acknowledgement.current, false);
    // The payment is forwarder 8's now: nothing of forwarder 7's is shown, by name or otherwise.
    const other = N.forwarderEvidence(events, { forwarderId: 8, revision: 'r1' });
    assert.deepEqual(other.opened.map(o => o.email), ['x@other.example']);
    assert.equal(other.acknowledgement.byEmail, 'x@other.example');
    assert.deepEqual(N.forwarderEvidence(events, { forwarderId: 9, revision: 'r1' }), { opened: [], acknowledgement: null });
    assert.deepEqual(N.forwarderEvidence(events, { forwarderId: null, revision: 'r1' }), { opened: [], acknowledgement: null });
});

test('with two acknowledgements, the one of the present revision is shown; else the latest', () => {
    const events = [
        ev({ event: 'acknowledged', revision: 'r1', at: '2026-11-21T11:00:00Z' }),
        ev({ event: 'acknowledged', revision: 'r2', userEmail: 'ops@dcg.example', userName: 'Ops', at: '2026-11-22T09:00:00Z' }),
    ];
    assert.deepEqual(N.forwarderEvidence(events, { forwarderId: 7, revision: 'r1' }).acknowledgement, { byName: 'Ann Lee', byEmail: 'ann@dcg.example', at: '2026-11-21T11:00:00Z', current: true });
    assert.deepEqual(N.forwarderEvidence(events, { forwarderId: 7, revision: 'r3' }).acknowledgement, { byName: 'Ops', byEmail: 'ops@dcg.example', at: '2026-11-22T09:00:00Z', current: false });
});

// -- The stored wording, and what the migrations add --

const { FORWARDER_PAYMENT_TEMPLATE, FORWARDER_PAYMENT_TEMPLATE_KEY, PAYMENT_PROOF_TEMPLATES, DEFAULT_EMAIL_TEMPLATES } = require('../src/lib/email-template-defaults');

test('the forwarder\'s wording: one template of its own, offered with the payment ones, filled from the payment', () => {
    const t = FORWARDER_PAYMENT_TEMPLATE;
    assert.equal(t.key, N.TEMPLATE_KEYS.forwarder);
    assert.equal(FORWARDER_PAYMENT_TEMPLATE_KEY, 'payment_proof_forwarder_portal');
    assert.deepEqual([t.name, t.category], ['Forwarder paid', N.TEMPLATE_CATEGORY]);
    assert.ok(DEFAULT_EMAIL_TEMPLATES.includes(t));
    // Not in the list the earlier migrations read as it was.
    assert.equal(PAYMENT_PROOF_TEMPLATES.some(x => x.key === t.key), false);
    assert.deepEqual([...new Set((t.subject + t.bodyHtml).match(/\{\w+\}/g))].sort(), ['{amount}', '{currency}', '{paidFor}', '{paidOn}']);
    for (const token of ['{amount}', '{currency}', '{paidOn}', '{paidFor}']) assert.ok(t.bodyHtml.includes(token), token);
    // English and Chinese, as the portal is; nothing a supplier's email asks for.
    assert.match(t.bodyHtml, /[一-鿿]/);
    assert.match(t.subject, /[一-鿿]/);
    assert.doesNotMatch(t.subject + t.bodyHtml, /deposit|balance|telex|ready date|attached/i);
    assert.ok(t.subject.length <= 255);
});

test('the seed adds the wording once and never overwrites an edited one', async () => {
    const seed = require('../src/db/migrate/2026-11-20_23_seed_forwarder_payment_template');
    const run = async affectedRows => {
        const asked = [];
        const said = [];
        await seed.up({ query: async (sql, params) => { asked.push({ sql: String(sql).replace(/\s+/g, ' ').trim(), params }); return [{ affectedRows }]; } }, { log: m => said.push(m) });
        return { asked, said };
    };
    const first = await run(1);
    assert.deepEqual(first.said, ['payment_proof_forwarder_portal created']);
    assert.equal(first.asked.length, 1);
    assert.equal(first.asked[0].sql, 'INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html) VALUES (?, ?, ?, ?, ?, ?)');
    const t = FORWARDER_PAYMENT_TEMPLATE;
    assert.deepEqual(first.asked[0].params, [t.key, t.name, t.category, t.description, t.subject, t.bodyHtml]);
    assert.deepEqual((await run(0)).said, ['payment_proof_forwarder_portal already existed']);
});

test('the send log gains the forwarder a send went to, once; the grant names the three forwarder views and nothing under them', async () => {
    const col = require('../src/db/migrate/2026-11-20_20_forwarder_payment_sends');
    assert.deepEqual(col.STEPS.map(s => s.slice(0, 3)), [['column', 'supplier_payment_sends', 'forwarder_id'], ['index', 'supplier_payment_sends', 'idx_payment_sends_forwarder']]);
    const runCol = async present => {
        const asked = [];
        const said = [];
        await col.up({ query: async sql => { const s = String(sql).replace(/\s+/g, ' ').trim(); asked.push(s); return /information_schema/.test(s) ? [[{ n: present ? 1 : 0 }]] : [{}]; } }, { log: m => said.push(m) });
        return { alters: asked.filter(s => s.startsWith('ALTER')), said };
    };
    assert.deepEqual(await runCol(false), {
        alters: ['ALTER TABLE supplier_payment_sends ADD COLUMN forwarder_id INT NULL AFTER supplier_name', 'ALTER TABLE supplier_payment_sends ADD KEY idx_payment_sends_forwarder (forwarder_id, supplier_payment_id)'],
        said: ['forwarder payment sends: 2 of 2 schema change(s) applied'],
    });
    assert.deepEqual(await runCol(true), { alters: [], said: ['forwarder payment sends: 0 of 2 schema change(s) applied'] });

    const grant = require('../src/db/migrate/2026-11-20_22_grant_portal_forwarder_payment_views_to_jportal');
    const runGrant = async exists => {
        const asked = [];
        const said = [];
        await grant.up({ query: async sql => { const s = String(sql).replace(/\s+/g, ' ').trim(); asked.push(s); return /mysql\.user/.test(s) ? [exists ? [{ 1: 1 }] : []] : /DATABASE\(\)/.test(s) ? [[{ schema: 'jfa' }]] : [{}]; } }, { log: m => said.push(m) });
        return { grants: asked.filter(s => s.startsWith('GRANT')), said };
    };
    assert.deepEqual((await runGrant(true)).grants, [
        "GRANT SELECT ON `jfa`.portal_forwarder_payments_v TO 'jportal_app'@'%'",
        "GRANT SELECT ON `jfa`.portal_forwarder_payment_lines_v TO 'jportal_app'@'%'",
        "GRANT SELECT ON `jfa`.portal_forwarder_payment_proofs_v TO 'jportal_app'@'%'",
    ]);
    const none = await runGrant(false);
    assert.deepEqual([none.grants, none.said], [[], ["no 'jportal_app'@'%' on this server; no grants to apply"]]);
});
