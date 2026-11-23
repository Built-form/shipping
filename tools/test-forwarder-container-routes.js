'use strict';

// The forwarder container routes (src/services/forwarder-container-routes.js;
// docs/forwarder-containers.md) against a scripted connection: what is read,
// what is written and in which order, and what is never written or sent when a
// request is refused. No database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { registerForwarderContainerRoutes } = require('../src/services/forwarder-container-routes');

const KEY = '3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f';
const err = (errno) => Object.assign(new Error(`errno ${errno}`), { errno });

// A connection that answers each statement from the first rule whose pattern
// matches it, and keeps everything it was asked.
function scripted(rules) {
    const asked = [];
    const conn = {
        query: async (sql, params = []) => {
            const text = String(sql).replace(/\s+/g, ' ').trim();
            asked.push({ sql: text, params });
            for (const [pattern, answer] of rules) {
                if (pattern.test(text)) {
                    const out = typeof answer === 'function' ? answer(params, text) : answer;
                    if (out instanceof Error) throw out;
                    return out;
                }
            }
            throw new Error(`unscripted statement: ${text.slice(0, 140)}`);
        },
        beginTransaction: async () => { asked.push({ sql: 'BEGIN', params: [] }); },
        commit: async () => { asked.push({ sql: 'COMMIT', params: [] }); },
        rollback: async () => { asked.push({ sql: 'ROLLBACK', params: [] }); },
    };
    return { conn, asked, wrote: pattern => asked.filter(a => pattern.test(a.sql)) };
}

// The shipment as sync.getShipment returns it: the row with the member and
// booking columns of shipmentSelect.
const SHIP_ROW = (over = {}) => ({
    id: 1214, reference: '339', name: 'DRAFT-SEA-261101 - 339', mode: 'SEA', stage: 'BOOKED', deleted_at: null, merged_into_id: null,
    forwarder_id: null, forwarder: null, tracking_ref: null, booking_ref: null, bl_number: null, vessel_name: null, origin_port: 'Ningbo',
    etd: '2026-10-21', eta: null, ata: null, loading_date: null,
    bk_count: 1, bk_id: 12, bk_status: 'confirmed', bk_ref: 'hello', bk_forwarder_id: 4, bk_forwarder_name: 'test2forwarder',
    member_count: 4, member_eta: null, member_etd: '2026-10-21', member_vessel: null, member_ext: null, member_awb: null,
    derived_stage: 'BOOKED', effective_stage: 'BOOKED',
    ...over,
});
const NO_BOOKING = { bk_count: 0, bk_id: null, bk_status: null, bk_ref: null, bk_forwarder_id: null, bk_forwarder_name: null };

const FORWARDERS = [{ id: 3, name: 'DCG', deleted_at: null }, { id: 4, name: 'test2forwarder', deleted_at: null }];
const CONTACTS = [{ id: 9, forwarder_id: 4, name: 'Mei', email: 'mei@fwd.example' }, { id: 10, forwarder_id: 4, name: null, email: 'ops@fwd.example' }];
const TEMPLATE = { subject: 'Container {containerName} – details needed', bodyHtml: '<p>Container <strong>{containerName}</strong> for {forwarder}, ref {bookingRef}.</p>' };

// Rules every read needs; a test puts its own in front.
const baseRules = ({ forwarders = FORWARDERS, contacts = CONTACTS, notices = [], lines = [], payments = [], details = [], uploads = [], documents = [] } = {}) => [
    [/FROM forwarders WHERE id IN/, () => [forwarders]],
    // The files forwarders sent for the container (the portal's table), newest first, and the bills of lading filed.
    [/forwarder_uploads u LEFT JOIN portal_forwarder_containers_v c .* WHERE u\.shipment_id = \?/, () => (uploads instanceof Error ? uploads : [uploads])],
    [/FROM shipment_forwarder_documents d WHERE d\.shipment_id = \?/, () => [documents]],
    // What forwarders proposed for the container (the portal's table), newest first.
    [/forwarder_container_details d LEFT JOIN forwarders f/, () => (details instanceof Error ? details : [details])],
    [/FROM forwarder_contacts WHERE forwarder_id = \? AND deleted_at IS NULL/, (p) => [contacts.filter(c => c.forwarder_id === Number(p[0]))]],
    [/FROM freight_container_notices n WHERE n\.shipment_id = \?/, () => [notices]],
    [/FROM portal_forwarder_payment_lines_v pl WHERE pl\.payment_id IN \(SELECT/, () => [lines]],
    [/FROM portal_forwarder_payments_v p JOIN forwarders f/, () => [payments]],
];

// What the routes read UNDER THE LOCK: the shipment row and its live booking.
// By default they say what the unlocked read said; `locked` makes them differ,
// as when something changed between the page's read and the decision.
const lockedOf = (shipment) => ({
    shipment: shipment && { id: shipment.id, reference: shipment.reference, stage: shipment.stage, deleted_at: shipment.deleted_at, merged_into_id: shipment.merged_into_id, forwarder_id: shipment.forwarder_id, forwarder: shipment.forwarder },
    bookings: shipment && shipment.bk_id != null ? [{ id: shipment.bk_id, status: shipment.bk_status, booking_ref: shipment.bk_ref, forwarder_id: shipment.bk_forwarder_id }] : [],
});

// `proposals`: the container's rows of the portal's forwarder_container_details,
// as the decisions lock them. `members`: its member orders, as they lock them
// (by default two orders holding what the shipment row says its orders hold).
// `updated`: what an UPDATE of a proposal answers (rows changed), a number or
// (sql, params) => number. `patch`: what writing the dates through PATCH's code does.
function build(rules, { shipment = SHIP_ROW(), locked, env = { SUPPLIER_PORTAL_URL: 'https://portal.example' }, front, evidence, preapproveResult, proposals = [], members, updated = 1, patch } = {}) {
    const routes = {};
    const app = Object.fromEntries(['get', 'post', 'delete', 'patch'].map(m => [m, (path, fn) => { routes[`${m.toUpperCase()} ${path}`] = fn; }]));
    const under = { ...lockedOf(shipment), ...(locked || {}) };
    const orders = members || (shipment ? [1, 2].map(id => ({ id, external_container_number: shipment.member_ext, awb_number: shipment.member_awb })) : []);
    const db = scripted([
        ...rules,
        [/FROM shipments WHERE id = \? FOR UPDATE/, () => [under.shipment ? [under.shipment] : []]],
        [/FROM freight_bookings fb JOIN freight_quote_requests fr .* FOR UPDATE OF fb/, () => [under.bookings]],
        [/FROM orders WHERE shipment_id = \? AND deleted_at IS NULL ORDER BY id FOR UPDATE/, () => [orders]],
        [/forwarder_container_details d WHERE d\.shipment_id = \? ORDER BY d\.id FOR UPDATE/, () => [[...proposals].sort((a, b) => a.id - b.id)]],
        [/^SELECT d\.shipment_id FROM `jportal`\.forwarder_container_details d WHERE d\.id = \?/, p => [proposals.filter(x => x.id === Number(p[0])).map(x => ({ shipment_id: x.shipment_id }))]],
        [/^UPDATE `jportal`\.forwarder_container_details SET status = /, (p, sql) => [{ affectedRows: typeof updated === 'function' ? updated(sql, p) : updated }]],
    ]);
    const sent = [];
    const audits = [];
    const receipts = [];
    const preapproved = [];
    const patched = [];
    const order = [];
    registerForwarderContainerRoutes(app, {
        withConnection: fn => fn(db.conn),
        ready: async () => {},
        recordAudit: async (conn, entry) => { audits.push(entry); },
        getEmailTemplate: async () => TEMPLATE,
        renderTemplate: (text, vars) => String(text).replace(/\{(\w+)\}/g, (m, k) => (k in vars ? String(vars[k]) : m)),
        sendFrontMessage: async (msg) => {
            order.push('front');
            sent.push(msg);
            return front ? front(msg) : { ok: true, frontMessageUid: 'msg_1', frontConversationId: 'cnv_1' };
        },
        preapprove: async (conn, args) => { preapproved.push(args); return preapproveResult || { status: 'ok', users: [] }; },
        getShipment: async () => shipment,
        patchShipment: async (conn, id, body, userEmail) => {
            db.asked.push({ sql: 'PATCH', params: [id, body, userEmail] });
            patched.push({ id, body, userEmail });
            if (patch) return patch(id, body, userEmail);
            return { id, warnings: [] };
        },
        readForwarderEvidence: async () => evidence || { events: new Map(), revisions: new Map() },
        logSentEmail: async (conn, fields) => { receipts.push(fields); },
        log: { error() {}, warn() {} },
        env,
    });
    const call = async (route, req = {}) => {
        let status = 200;
        let body = null;
        const res = { headersSent: false, status(s) { status = s; return this; }, json(b) { body = b; this.headersSent = true; return this; } };
        await routes[route]({ query: {}, params: { id: '1214' }, body: {}, userEmail: 'ops@us.com', userType: 'standard', ...req }, res);
        return { status, body };
    };
    return { call, db, sent, audits, receipts, preapproved, patched };
}

const TAB = 'GET /api/v1/shipments/:id(\\d+)/forwarder-portal';
const NOTICE = 'POST /api/v1/shipments/:id(\\d+)/forwarder-notice';

// ── the FORWARDER tab's read ─────────────────────────────────────────────

test('tab: a container that does not exist is 404 and nothing else is read', async () => {
    const t = build(baseRules(), { shipment: null });
    const r = await t.call(TAB);
    assert.equal(r.status, 404);
    assert.equal(r.body.code, 'NO_SUCH_SHIPMENT');
    assert.equal(t.db.asked.length, 0);
});

test('tab: a container with a confirmed booking is its forwarder\'s, in full', async () => {
    const t = build(baseRules());
    const r = await t.call(TAB);
    assert.equal(r.status, 200);
    assert.equal(r.body.shipmentId, 1214);
    assert.equal(r.body.reference, '339');
    assert.deepEqual(r.body.forwarder, { id: 4, name: 'test2forwarder', source: 'booking' });
    assert.deepEqual(r.body.visibility, { mode: 'full', why: null });
    assert.deepEqual(r.body.contacts, [{ name: 'Mei', email: 'mei@fwd.example' }, { name: null, email: 'ops@fwd.example' }]);
    assert.deepEqual(r.body.held, { ecn: null, loadingDate: null, etd: '2026-10-21', eta: null, bookingRef: 'hello', stage: 'BOOKED' });
    assert.deepEqual(r.body.asks, ['ecn', 'loadingDate', 'invoice', 'photos', 'billOfLading']);
    assert.equal(r.body.portalUrl, 'https://portal.example/freight-containers/1214');
    assert.equal(r.body.canDecide, true);
    assert.deepEqual(r.body.notices, []);
    assert.deepEqual(r.body.payments, []);
    assert.deepEqual(r.body.paymentOnly, []);
});

test('tab: what we hold comes from the member orders, as the container card shows it', async () => {
    const t = build(baseRules(), { shipment: SHIP_ROW({ member_ext: 'MSKU1234565', member_etd: '2026-12-01', member_eta: '2027-01-10', loading_date: '2026-11-28', effective_stage: 'IN_TRANSIT' }) });
    const r = await t.call(TAB);
    assert.deepEqual(r.body.held, { ecn: 'MSKU1234565', loadingDate: '2026-11-28', etd: '2026-12-01', eta: '2027-01-10', bookingRef: 'hello', stage: 'IN_TRANSIT' });
    assert.deepEqual(r.body.asks, ['invoice', 'photos', 'billOfLading']);
});

test('tab: a booking the forwarder has not confirmed shows the container to nobody', async () => {
    const t = build(baseRules(), { shipment: SHIP_ROW({ bk_status: 'requested' }) });
    const r = await t.call(TAB);
    assert.deepEqual(r.body.forwarder, { id: 4, name: 'test2forwarder', source: 'booking' });
    assert.deepEqual(r.body.visibility, { mode: 'none', why: 'booking_not_confirmed' });
});

test('tab: a forwarder picked by hand, with no booking', async () => {
    const t = build(baseRules({ contacts: [{ id: 1, forwarder_id: 3, name: 'Li', email: 'li@dcg.example' }] }), { shipment: SHIP_ROW({ ...NO_BOOKING, forwarder_id: 3, forwarder: 'DCG' }) });
    const r = await t.call(TAB);
    assert.deepEqual(r.body.forwarder, { id: 3, name: 'DCG', source: 'container' });
    assert.deepEqual(r.body.visibility, { mode: 'full', why: null });
    assert.deepEqual(r.body.contacts, [{ name: 'Li', email: 'li@dcg.example' }]);
});

test('tab: no forwarder, a typed one, a removed one, a draft: each says why nobody sees it', async () => {
    const cases = [
        [SHIP_ROW({ ...NO_BOOKING }), 'no_forwarder', null],
        [SHIP_ROW({ ...NO_BOOKING, forwarder: 'Some Freight Ltd' }), 'forwarder_not_listed', { id: null, name: 'Some Freight Ltd', source: 'container' }],
        [SHIP_ROW({ ...NO_BOOKING, forwarder_id: 7, forwarder: 'Gone Ltd' }), 'forwarder_removed', { id: 7, name: 'Gone Ltd', source: 'container' }],
        [SHIP_ROW({ ...NO_BOOKING, stage: 'DRAFT', reference: null, forwarder_id: 3, forwarder: 'DCG', effective_stage: 'DRAFT' }), 'not_real', { id: 3, name: 'DCG', source: 'container' }],
    ];
    for (const [shipment, why, forwarder] of cases) {
        const t = build(baseRules({ forwarders: [...FORWARDERS, { id: 7, name: 'Gone Ltd', deleted_at: '2026-11-01 10:00:00' }] }), { shipment });
        const r = await t.call(TAB);
        assert.deepEqual(r.body.visibility, { mode: 'none', why }, why);
        assert.deepEqual(r.body.forwarder, forwarder, why);
    }
});

test('tab: a forwarder with no contact cannot sign in: the tab is told there are none', async () => {
    const t = build(baseRules({ contacts: [] }));
    const r = await t.call(TAB);
    assert.deepEqual(r.body.visibility, { mode: 'full', why: null });
    assert.deepEqual(r.body.contacts, []);
});

test('tab: an accountant reads it and may not decide', async () => {
    const t = build(baseRules());
    const r = await t.call(TAB, { userType: 'accountant' });
    assert.equal(r.status, 200);
    assert.equal(r.body.canDecide, false);
});

test('tab: with no portal address configured there is no link to offer', async () => {
    const t = build(baseRules(), { env: {} });
    const r = await t.call(TAB);
    assert.equal(r.body.portalUrl, null);
});

test('tab: the emails sent about it, newest first as read', async () => {
    const notices = [{ id: 5, status: 'sent', sent_to: '["mei@fwd.example"]', subject: 'Container 339', sent_by_email: 'ops@us.com', created_at_iso: '2026-11-23T10:00:00Z', sent_at_iso: '2026-11-23T10:00:02Z', forwarder_id: 4 }];
    const t = build(baseRules({ notices }));
    const r = await t.call(TAB);
    assert.deepEqual(r.body.notices, [{ id: 5, status: 'sent', forwarderId: 4, sentTo: ['mei@fwd.example'], subject: 'Container 339', sentByEmail: 'ops@us.com', createdAt: '2026-11-23T10:00:00Z', sentAt: '2026-11-23T10:00:02Z' }]);
});

test('tab: the payments sent that pay this container, each with this container\'s share and what its forwarder did', async () => {
    const lines = [
        { payment_id: 16, shipment_id: 1214, shipment_reference: '339', amount: '4500.00', extra_kind: 'freight' },
        { payment_id: 16, shipment_id: 62, shipment_reference: '308', amount: '4875.00', extra_kind: 'freight' },
    ];
    const payments = [{ payment_id: 16, forwarder_id: 4, forwarder_name: 'test2forwarder', amount: '9375.00', currency: 'USD', paid_on: '2026-09-30', last_sent_iso: '2026-11-23T09:21:12Z', revision: 'rev1' }];
    const evidence = {
        events: new Map([[16, [
            { forwarderId: 4, event: 'opened', userEmail: 'mei@fwd.example', userName: 'Mei', revision: 'rev1', at: '2026-11-23T09:30:00Z' },
            { forwarderId: 4, event: 'acknowledged', userEmail: 'mei@fwd.example', userName: 'Mei', revision: 'rev1', at: '2026-11-23T09:31:00Z' },
            { forwarderId: 3, event: 'acknowledged', userEmail: 'li@dcg.example', userName: 'Li', revision: 'rev0', at: '2026-11-01T09:31:00Z' },
        ]]]),
        revisions: new Map([['16:4', 'rev1']]),
    };
    const t = build(baseRules({ lines, payments }), { evidence });
    const r = await t.call(TAB);
    assert.deepEqual(r.body.payments, [{
        paymentId: 16, forwarderId: 4, forwarderName: 'test2forwarder', paidOn: '2026-09-30', amount: 9375, currency: 'USD',
        share: 4500, lines: [{ kind: 'freight', amount: 4500 }], alsoPays: ['308'], sentAt: '2026-11-23T09:21:12Z',
        opened: [{ email: 'mei@fwd.example', name: 'Mei', firstAt: '2026-11-23T09:30:00Z', lastAt: '2026-11-23T09:30:00Z', count: 1 }],
        acknowledgement: { byName: 'Mei', byEmail: 'mei@fwd.example', at: '2026-11-23T09:31:00Z', current: true },
    }]);
    assert.deepEqual(r.body.paymentOnly, []);
});

test('tab: a forwarder paid on a container that is not theirs sees it for the payment only', async () => {
    const lines = [{ payment_id: 37, shipment_id: 1214, shipment_reference: '339', amount: '300.00', extra_kind: 'haulage' }];
    const payments = [{ payment_id: 37, forwarder_id: 3, forwarder_name: 'DCG', amount: '300.00', currency: 'GBP', paid_on: '2026-11-20', last_sent_iso: '2026-11-21T08:00:00Z', revision: 'r' }];
    const t = build(baseRules({ lines, payments }));
    const r = await t.call(TAB);
    assert.deepEqual(r.body.visibility, { mode: 'full', why: null });
    assert.equal(r.body.payments.length, 1);
    assert.equal(r.body.payments[0].forwarderId, 3);
    assert.deepEqual(r.body.paymentOnly, [{ forwarderId: 3, forwarderName: 'DCG' }]);
});

test('tab: a container nobody owns, with a payment sent on it: visible for that payment only', async () => {
    const lines = [{ payment_id: 37, shipment_id: 1214, shipment_reference: '339', amount: '300.00', extra_kind: 'freight' }];
    const payments = [{ payment_id: 37, forwarder_id: 3, forwarder_name: 'DCG', amount: '300.00', currency: 'GBP', paid_on: '2026-11-20', last_sent_iso: '2026-11-21T08:00:00Z', revision: 'r' }];
    const t = build(baseRules({ lines, payments }), { shipment: SHIP_ROW({ ...NO_BOOKING }) });
    const r = await t.call(TAB);
    assert.deepEqual(r.body.visibility, { mode: 'none', why: 'no_forwarder' });
    assert.deepEqual(r.body.paymentOnly, [{ forwarderId: 3, forwarderName: 'DCG' }]);
});

test('tab: a server without this feature\'s tables answers 503 NOT_MIGRATED', async () => {
    const t = build([[/FROM forwarders WHERE id IN/, () => [FORWARDERS]], [/FROM forwarder_contacts/, () => [CONTACTS]], [/freight_container_notices/, () => err(1146)]]);
    const r = await t.call(TAB);
    assert.equal(r.status, 503);
    assert.equal(r.body.code, 'NOT_MIGRATED');
});

// ── the email to the forwarder ───────────────────────────────────────────

const noticeRules = ({ existing = null, contacts = CONTACTS, forwarders = FORWARDERS } = {}) => [
    [/FROM freight_container_notices WHERE send_key = \? FOR UPDATE/, () => [existing ? [existing] : []]],
    [/INSERT INTO freight_container_notices/, () => [{ insertId: 77, affectedRows: 1 }]],
    [/UPDATE freight_container_notices SET status = 'sending'/, () => [{ affectedRows: 1 }]],
    [/UPDATE freight_container_notices SET status = 'sent'/, () => [{ affectedRows: 1 }]],
    [/UPDATE freight_container_notices SET status = 'failed'/, () => [{ affectedRows: 1 }]],
    ...baseRules({ contacts, forwarders }),
];

test('email: a role that may not decide is refused before anything is read', async () => {
    const t = build(noticeRules());
    const r = await t.call(NOTICE, { userType: 'accountant', body: { sendKey: KEY } });
    assert.equal(r.status, 403);
    assert.equal(r.body.code, 'CANNOT_DECIDE');
    assert.equal(t.db.asked.length, 0);
    assert.equal(t.sent.length, 0);
});

test('email: without a send key nothing is read or sent', async () => {
    const t = build(noticeRules());
    for (const body of [{}, { sendKey: 'abc' }, { sendKey: 42 }]) {
        const r = await t.call(NOTICE, { body });
        assert.equal(r.status, 400);
        assert.equal(r.body.code, 'BAD_FIELD');
        assert.equal(r.body.field, 'sendKey');
    }
    assert.equal(t.db.asked.length, 0);
    assert.equal(t.sent.length, 0);
});

test('email: with no portal address there is no link to send', async () => {
    const t = build(noticeRules(), { env: {} });
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'PORTAL_URL_UNSET');
    assert.equal(t.sent.length, 0);
});

test('email: a container its forwarder cannot see is not emailed about, and the reason is told', async () => {
    const t = build(noticeRules(), { shipment: SHIP_ROW({ bk_status: 'requested' }) });
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'NOT_VISIBLE');
    assert.equal(r.body.why, 'booking_not_confirmed');
    assert.equal(t.sent.length, 0);
    assert.equal(t.db.wrote(/INSERT INTO freight_container_notices/).length, 0);
});

test('email: goes to the forwarder\'s contacts, with the container, what is still needed and the button to its page', async () => {
    const t = build(noticeRules());
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 200);
    assert.equal(r.body.outcome, 'sent');
    assert.equal(r.body.noticeId, 77);
    assert.equal(t.sent.length, 1);
    assert.deepEqual(t.sent[0].to, ['mei@fwd.example', 'ops@fwd.example']);
    assert.equal(t.sent[0].subject, 'Container 339 – details needed');
    const html = t.sent[0].html;
    assert.ok(html.includes('Container <strong>339</strong> for test2forwarder, ref hello.'));
    assert.ok(html.includes('href="https://portal.example/freight-containers/1214"'));
    assert.ok(html.includes('<li>Container number / 柜号</li>'));
    assert.ok(html.includes('<li>Expected loading date / 预计装柜日期</li>'));
    assert.ok(!html.includes('<li>Expected departure date'), 'the departure date is held already');
    assert.ok(html.includes('href="https://portal.example/register"'));
});

test('email: the row is written as "sending" and committed before Front is called, then marked sent', async () => {
    const t = build(noticeRules());
    await t.call(NOTICE, { body: { sendKey: KEY } });
    const sqls = t.db.asked.map(a => a.sql);
    const insert = sqls.findIndex(s => /INSERT INTO freight_container_notices/.test(s));
    const commit = sqls.findIndex((s, i) => i > insert && s === 'COMMIT');
    const marked = sqls.findIndex(s => /UPDATE freight_container_notices SET status = 'sent'/.test(s));
    assert.ok(insert >= 0 && commit > insert && marked > commit, 'insert, commit, then sent');
    const ins = t.db.asked[insert];
    assert.ok(/'sending'/.test(ins.sql));
    assert.deepEqual(ins.params.slice(0, 4), [1214, '339', 4, KEY]);
    const upd = t.db.asked[marked];
    assert.deepEqual(upd.params, ['msg_1', 'cnv_1', JSON.stringify(['mei@fwd.example', 'ops@fwd.example']), 'Container 339 – details needed', 77]);
});

test('email: its recipients are pre-approved on the portal, it is on the receipts list as a portal email, and audited', async () => {
    const t = build(noticeRules());
    await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.deepEqual(t.preapproved.map(p => p.emails), [['mei@fwd.example', 'ops@fwd.example']]);
    assert.deepEqual(t.receipts, [{
        emailType: 'freight_container_details', sentTo: ['mei@fwd.example', 'ops@fwd.example'], subject: 'Container 339 – details needed',
        confirmVia: 'portal', sendTable: 'freight_container_notices', sendId: 77,
    }]);
    assert.equal(t.audits.length, 1);
    assert.equal(t.audits[0].entityType, 'freight_container_notice');
    assert.equal(t.audits[0].entityId, 77);
    assert.equal(t.audits[0].action, 'sent');
    assert.equal(t.audits[0].userEmail, 'ops@us.com');
    assert.deepEqual(t.audits[0].after.to, ['mei@fwd.example', 'ops@fwd.example']);
    assert.equal(t.audits[0].after.shipmentId, 1214);
    assert.equal(t.audits[0].after.forwarderId, 4);
});

test('email: the dialog may untick contacts, and its own subject and body are used, tokens filled and escaped', async () => {
    const t = build(noticeRules(), { shipment: SHIP_ROW({ bk_ref: 'A&B' }) });
    const r = await t.call(NOTICE, { body: { sendKey: KEY, to: ['OPS@fwd.example'], subject: 'Box {containerName}\nplease', body: '<p>Ref {bookingRef}</p>' } });
    assert.equal(r.status, 200);
    assert.deepEqual(t.sent[0].to, ['ops@fwd.example']);
    assert.equal(t.sent[0].subject, 'Box 339 please');
    assert.ok(t.sent[0].html.includes('<p>Ref A&amp;B</p>'));
});

test('email: an address that is not one of the forwarder\'s contacts is refused, and nothing is sent or written', async () => {
    const t = build(noticeRules());
    const r = await t.call(NOTICE, { body: { sendKey: KEY, to: ['mei@fwd.example', 'someone@else.example'] } });
    assert.equal(r.status, 400);
    assert.equal(r.body.code, 'NOT_A_CONTACT');
    assert.deepEqual(r.body.notContacts, ['someone@else.example']);
    assert.equal(t.sent.length, 0);
    assert.equal(t.db.wrote(/INSERT INTO freight_container_notices/).length, 0);
});

test('email: a forwarder with no contact, or a dialog with every box unticked, has nobody to email', async () => {
    const none = build(noticeRules({ contacts: [] }));
    const r1 = await none.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r1.status, 409);
    assert.equal(r1.body.code, 'NO_CONTACTS');
    const unticked = build(noticeRules());
    const r2 = await unticked.call(NOTICE, { body: { sendKey: KEY, to: [] } });
    assert.equal(r2.status, 409);
    assert.equal(r2.body.code, 'NO_CONTACTS');
    assert.equal(none.sent.length + unticked.sent.length, 0);
});

test('email: a key that already sent is answered as sent and emails nobody again', async () => {
    const t = build(noticeRules({ existing: { id: 77, shipment_id: 1214, forwarder_id: 4, status: 'sent' } }));
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 200);
    assert.equal(r.body.outcome, 'already_sent');
    assert.equal(r.body.noticeId, 77);
    assert.equal(t.sent.length, 0);
    assert.equal(t.db.wrote(/INSERT INTO freight_container_notices|UPDATE freight_container_notices/).length, 0);
});

test('email: a key whose send Front never answered is never sent again', async () => {
    const t = build(noticeRules({ existing: { id: 77, shipment_id: 1214, forwarder_id: 4, status: 'sending' } }));
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 200);
    assert.equal(r.body.outcome, 'send_unconfirmed');
    assert.equal(t.sent.length, 0);
    assert.equal(t.db.wrote(/INSERT INTO freight_container_notices|UPDATE freight_container_notices/).length, 0);
});

test('email: a key Front refused may try again, on the same row, once', async () => {
    const t = build(noticeRules({ existing: { id: 77, shipment_id: 1214, forwarder_id: 4, status: 'failed' } }));
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 200);
    assert.equal(r.body.outcome, 'sent');
    assert.equal(r.body.noticeId, 77);
    assert.equal(t.sent.length, 1);
    assert.equal(t.db.wrote(/INSERT INTO freight_container_notices/).length, 0);
    assert.equal(t.db.wrote(/UPDATE freight_container_notices SET status = 'sending'/).length, 1);
    assert.equal(t.db.wrote(/UPDATE freight_container_notices SET status = 'sent'/).length, 1);
});

test('email: a key made for another container or forwarder is refused', async () => {
    const t = build(noticeRules({ existing: { id: 70, shipment_id: 62, forwarder_id: 4, status: 'failed' } }));
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 409);
    assert.equal(r.body.code, 'SEND_KEY_CLASH');
    assert.equal(t.sent.length, 0);
});

test('email: Front refusing marks the row failed, so the same key can try again', async () => {
    const t = build(noticeRules(), { front: () => ({ ok: false, status: 422, detail: 'no' }) });
    const r = await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r.status, 502);
    assert.equal(r.body.code, 'FRONT_ERROR');
    assert.equal(t.db.wrote(/UPDATE freight_container_notices SET status = 'failed'/).length, 1);
    assert.equal(t.db.wrote(/UPDATE freight_container_notices SET status = 'sent'/).length, 0);
    assert.equal(t.receipts.length, 0);
    assert.equal(t.audits.length, 0);
});

test('email: whose container it is, is decided on the rows under the lock, not on the read the page made', async () => {
    // The page's read still shows the booking waiting; under the lock it is confirmed.
    const confirmed = build(noticeRules(), {
        shipment: SHIP_ROW({ bk_status: 'requested' }),
        locked: { bookings: [{ id: 12, status: 'confirmed', booking_ref: 'hello', forwarder_id: 4 }] },
    });
    const r1 = await confirmed.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r1.status, 200);
    assert.equal(r1.body.outcome, 'sent');

    // The page's read shows it confirmed; under the lock the booking is gone and no forwarder is picked.
    const gone = build(noticeRules(), { locked: { bookings: [] } });
    const r2 = await gone.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r2.status, 409);
    assert.equal(r2.body.code, 'NOT_VISIBLE');
    assert.equal(r2.body.why, 'no_forwarder');
    assert.equal(gone.sent.length, 0);
    assert.equal(gone.db.wrote(/INSERT INTO freight_container_notices/).length, 0);

    // Under the lock the container has been deleted.
    const deleted = build(noticeRules(), { locked: { shipment: null } });
    const r3 = await deleted.call(NOTICE, { body: { sendKey: KEY } });
    assert.equal(r3.status, 409);
    assert.equal(r3.body.why, 'gone');
    assert.equal(deleted.sent.length, 0);
});

test('email: goes to the forwarder the locked rows name: the contacts of another forwarder are never read for it', async () => {
    const t = build(noticeRules({ contacts: [...CONTACTS, { id: 1, forwarder_id: 3, name: 'Li', email: 'li@dcg.example' }] }));
    await t.call(NOTICE, { body: { sendKey: KEY } });
    assert.deepEqual(t.sent[0].to, ['mei@fwd.example', 'ops@fwd.example']);
    const reads = t.db.wrote(/FROM forwarder_contacts/);
    assert.ok(reads.length >= 1 && reads.every(q => Number(q.params[0]) === 4));
});

// ── a forwarder's proposal of the container's number and dates ───────────

const S = require('../src/lib/shipments');
const { REFUSAL_TEXT } = require('../src/services/forwarder-container-routes');

const TOKEN = 'a1b2c3d4e5f60718293a4b5c6d7e8f90';
const ECN = 'CSQU3054383';
const ASKED = { ecn: ECN, loadingDate: '2026-10-18', etd: '2026-10-22' };
// What SHIP_ROW() holds, as the card shows it.
const SEEN = { ecn: null, loadingDate: null, etd: '2026-10-21' };
const PROPOSAL = (over = {}) => ({
    id: 51, shipment_id: 1214, forwarder_id: 4, status: 'pending',
    requested_json: JSON.stringify(ASKED), base_json: JSON.stringify(SEEN),
    note: 'Loaded on the 18th', user_email: 'mei@fwd.example', user_name: 'Mei',
    claim_token: null, claim_json: null, claimed_by: null, decided_note: null, decided_by: null,
    created_at_iso: '2026-11-23T09:00:00Z', claimed_at_iso: null, decided_at_iso: null, lapsed: 0,
    forwarder_name: 'test2forwarder',
    ...over,
});
const CLAIMED = (over = {}) => PROPOSAL({
    status: 'applying', claim_token: TOKEN, claim_json: JSON.stringify(SEEN), claimed_by: 'ops@us.com', claimed_at_iso: '2026-11-23T10:00:00Z', ...over,
});
// The container once ShipLine has written the proposed number to its orders.
const WITH_ECN = (over = {}) => SHIP_ROW({ member_ext: ECN, ...over });

const CLAIM = 'POST /api/v1/forwarder-details/:id(\\d+)/claim';
const FINISH = 'POST /api/v1/forwarder-details/:id(\\d+)/finish';
const RELEASE = 'POST /api/v1/forwarder-details/:id(\\d+)/release';
const REJECT = 'POST /api/v1/forwarder-details/:id(\\d+)/reject';
const LIST = 'GET /api/v1/forwarder-submissions';
const SUMMARY = 'GET /api/v1/forwarder-submissions/summary';

const on = (id, body = {}, over = {}) => ({ params: { id: String(id) }, body, ...over });
const sqlOf = t => t.db.asked.map(a => a.sql);
const marked = (t, status) => t.db.wrote(new RegExp(`forwarder_container_details SET status = '${status}'`));
const refusal = (code, allowedNext, extra = {}) => ({ error: REFUSAL_TEXT[code], code, allowedNext, ...extra });
const LAST_LOCK = /forwarder_container_details d WHERE d\.shipment_id = \? ORDER BY d\.id FOR UPDATE/;
// Every statement of a decision's transaction up to its last lock.
const locksOf = (t) => {
    const sql = sqlOf(t);
    return sql.slice(sql.indexOf('BEGIN') + 1, sql.findIndex(s => LAST_LOCK.test(s)) + 1);
};
const tableOf = s => (/FROM (?:`jportal`\.)?(\w+)/.exec(s) || [])[1];
const otherForwarder = { shipment: { ...lockedOf(SHIP_ROW()).shipment, forwarder_id: 3 }, bookings: [] };

test('tab: the live proposal of the container\'s forwarder, and its older ones as history', async () => {
    const t = build(baseRules({
        details: [
            PROPOSAL({ id: 52 }),
            PROPOSAL({ id: 51, status: 'rejected', decided_note: 'Wrong container', decided_by: 'kim@us.com', decided_at_iso: '2026-11-23T08:00:00Z' }),
        ],
    }));
    const r = await t.call(TAB);
    assert.equal(r.status, 200);
    assert.equal(r.body.unavailable, false);
    assert.deepEqual(r.body.details.proposal, {
        id: 52, forwarderId: 4, forwarderName: 'test2forwarder', status: 'pending', waiting: true, live: true, actionable: true,
        requested: ASKED, base: { etd: '2026-10-21' }, note: 'Loaded on the 18th', byName: 'Mei', byEmail: 'mei@fwd.example',
        at: '2026-11-23T09:00:00Z', claimedBy: null, claimedAt: null, decidedNote: null, decidedBy: null, decidedAt: null,
    });
    assert.equal(r.body.details.history.length, 1);
    const old = r.body.details.history[0];
    assert.deepEqual(
        { id: old.id, status: old.status, waiting: old.waiting, live: old.live, actionable: old.actionable, decidedNote: old.decidedNote, decidedBy: old.decidedBy, decidedAt: old.decidedAt },
        { id: 51, status: 'rejected', waiting: false, live: false, actionable: false, decidedNote: 'Wrong container', decidedBy: 'kim@us.com', decidedAt: '2026-11-23T08:00:00Z' }
    );
    assert.deepEqual(r.body.details.strays, []);
    assert.equal(r.body.toReview, 1);
});

test('tab: the newest proposal decided does not bring an older undecided one back', async () => {
    const t = build(baseRules({
        details: [PROPOSAL({ id: 52, status: 'applied', decided_by: 'kim@us.com', decided_at_iso: '2026-11-23T11:00:00Z' }), PROPOSAL({ id: 51 })],
    }));
    const r = await t.call(TAB);
    const { proposal, history } = r.body.details;
    assert.deepEqual({ id: proposal.id, status: proposal.status, waiting: proposal.waiting, actionable: proposal.actionable }, { id: 52, status: 'applied', waiting: false, actionable: false });
    assert.deepEqual(history.map(h => ({ id: h.id, status: h.status, live: h.live, actionable: h.actionable })), [{ id: 51, status: 'pending', live: false, actionable: false }]);
    assert.equal(r.body.toReview, 0);
});

test('tab: an undecided proposal of a forwarder the container is no longer with can be dismissed, never accepted, and is not counted', async () => {
    const t = build(baseRules({
        details: [
            PROPOSAL({ id: 60, forwarder_id: 3, forwarder_name: 'DCG' }),
            PROPOSAL({ id: 51 }),
            PROPOSAL({ id: 40, forwarder_id: 5, forwarder_name: 'Gone Ltd', status: 'rejected' }),
        ],
    }));
    const r = await t.call(TAB);
    assert.equal(r.body.details.proposal.id, 51);
    assert.deepEqual(
        r.body.details.strays.map(s => ({ id: s.id, forwarderId: s.forwarderId, forwarderName: s.forwarderName, waiting: s.waiting, live: s.live, actionable: s.actionable })),
        [{ id: 60, forwarderId: 3, forwarderName: 'DCG', waiting: true, live: true, actionable: false }]
    );
    assert.equal(r.body.toReview, 1);
});

test('tab: a container nobody owns lists every undecided proposal as no longer theirs', async () => {
    const t = build(baseRules({ details: [PROPOSAL()] }), { shipment: SHIP_ROW({ ...NO_BOOKING }) });
    const r = await t.call(TAB);
    assert.equal(r.body.details.proposal, null);
    assert.deepEqual(r.body.details.strays.map(s => [s.id, s.actionable]), [[51, false]]);
    assert.equal(r.body.toReview, 0);
});

test('tab: an accept in progress says who and since when; one that lapsed reads as waiting again', async () => {
    let r = await build(baseRules({ details: [CLAIMED()] })).call(TAB);
    let p = r.body.details.proposal;
    assert.deepEqual(
        { status: p.status, waiting: p.waiting, actionable: p.actionable, claimedBy: p.claimedBy, claimedAt: p.claimedAt },
        { status: 'applying', waiting: false, actionable: true, claimedBy: 'ops@us.com', claimedAt: '2026-11-23T10:00:00Z' }
    );
    assert.equal(r.body.toReview, 1);

    r = await build(baseRules({ details: [CLAIMED({ lapsed: 1 })] })).call(TAB);
    p = r.body.details.proposal;
    assert.deepEqual(
        { status: p.status, waiting: p.waiting, actionable: p.actionable, claimedBy: p.claimedBy, claimedAt: p.claimedAt },
        { status: 'pending', waiting: true, actionable: true, claimedBy: null, claimedAt: null }
    );
});

test('tab: where the portal\'s tables do not exist the tab still reads, and says nothing a forwarder sent can be shown', async () => {
    for (const errno of [1146, 1049]) {
        const r = await build(baseRules({ details: err(errno) })).call(TAB);
        assert.equal(r.status, 200, `errno ${errno}`);
        assert.equal(r.body.unavailable, true);
        assert.deepEqual(r.body.details, { proposal: null, history: [], strays: [] });
        assert.equal(r.body.toReview, 0);
        assert.deepEqual(r.body.forwarder, { id: 4, name: 'test2forwarder', source: 'booking' });
    }
});

test('tab: a missing grant on the portal\'s table is an error, not an empty answer', async () => {
    const r = await build(baseRules({ details: err(1142) })).call(TAB);
    assert.equal(r.status, 500);
});

// ── claim ────────────────────────────────────────────────────────────────

test('decide: a role that may not decide is refused on every route before anything is read', async () => {
    for (const route of [CLAIM, FINISH, RELEASE, REJECT]) {
        const t = build(baseRules(), { proposals: [PROPOSAL()] });
        const r = await t.call(route, on(51, { expected: SEEN, claimToken: TOKEN, note: 'Wrong number' }, { userType: 'accountant' }));
        assert.equal(r.status, 403, route);
        assert.equal(r.body.code, 'CANNOT_DECIDE');
        assert.equal(t.db.asked.length, 0);
    }
});

test('claim: without what the card showed, nothing is read', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()] });
    const r = await t.call(CLAIM, on(51, {}));
    assert.equal(r.status, 400);
    assert.deepEqual({ code: r.body.code, field: r.body.field }, { code: 'BAD_FIELD', field: 'expected' });
    assert.equal(t.db.asked.length, 0);
});

test('claim: a proposal that does not exist is 404 and no transaction is opened', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()] });
    const r = await t.call(CLAIM, on(99, { expected: SEEN }));
    assert.deepEqual(r, { status: 404, body: refusal('NO_SUCH_PROPOSAL', ['reload']) });
    assert.equal(sqlOf(t).includes('BEGIN'), false);
});

test('claim: the live, waiting proposal of the container\'s own forwarder is claimed, with what the container held', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()] });
    const r = await t.call(CLAIM, on(51, { expected: SEEN }));
    assert.equal(r.status, 200);
    assert.match(r.body.claimToken, /^[a-f0-9]{32}$/);
    assert.deepEqual({ ...r.body, claimToken: null }, { claimToken: null, expiresInSeconds: 600, requested: ASKED, held: SEEN });
    const w = marked(t, 'applying');
    assert.equal(w.length, 1);
    assert.deepEqual(w[0].params, [r.body.claimToken, JSON.stringify(SEEN), 'ops@us.com', 51]);
    assert.match(w[0].sql, /claimed_at = UTC_TIMESTAMP\(\) WHERE id = \? AND status = 'pending'$/);
    assert.equal(sqlOf(t).at(-1), 'COMMIT');
    assert.deepEqual(t.patched, []);
});

test('claim: two claims never get the same token', async () => {
    const a = await build(baseRules(), { proposals: [PROPOSAL()] }).call(CLAIM, on(51, { expected: SEEN }));
    const b = await build(baseRules(), { proposals: [PROPOSAL()] }).call(CLAIM, on(51, { expected: SEEN }));
    assert.notEqual(a.body.claimToken, b.body.claimToken);
});

test('claim: the transaction opens with its locks, in one order, and reads nothing plainly before the last of them', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()] });
    await t.call(CLAIM, on(51, { expected: SEEN }));
    // Before the transaction: only which container the proposal is for.
    assert.equal(sqlOf(t).indexOf('BEGIN'), 1);
    const locks = locksOf(t);
    assert.deepEqual(locks.map(tableOf), ['shipments', 'freight_bookings', 'forwarders', 'orders', 'forwarder_container_details']);
    for (const s of locks) assert.match(s, /FOR (UPDATE|SHARE)/, s.slice(0, 80));
});

test('claim: each refusal says why and what can be done, and writes nothing', async () => {
    const cases = [
        ['someone else is accepting on this container', { proposals: [PROPOSAL(), PROPOSAL({ id: 50, status: 'applying', claimed_by: 'kim@us.com', claimed_at_iso: '2026-11-23T10:00:00Z' })] }, SEEN,
            refusal('CLAIM_IN_PROGRESS', ['reload'], { claimedBy: 'kim@us.com', claimedAt: '2026-11-23T10:00:00Z' })],
        ['the same proposal is being accepted in another tab', { proposals: [CLAIMED()] }, SEEN,
            refusal('CLAIM_IN_PROGRESS', ['reload'], { claimedBy: 'ops@us.com', claimedAt: '2026-11-23T10:00:00Z' })],
        ['already accepted', { proposals: [PROPOSAL({ status: 'applied' })] }, SEEN, refusal('ALREADY_DECIDED', ['reload'])],
        ['already sent back', { proposals: [PROPOSAL({ status: 'rejected' })] }, SEEN, refusal('ALREADY_DECIDED', ['reload'])],
        ['a newer proposal exists', { proposals: [PROPOSAL(), PROPOSAL({ id: 52 })] }, SEEN, refusal('SUPERSEDED', ['reload'])],
        ['the container went to another forwarder', { proposals: [PROPOSAL()], locked: otherForwarder }, SEEN, refusal('NOT_THEIR_CONTAINER', ['reject'], { why: null })],
        ['its booking is no longer confirmed', { proposals: [PROPOSAL()], locked: { bookings: [{ id: 12, status: 'requested', booking_ref: 'hello', forwarder_id: 4 }] } }, SEEN,
            refusal('NOT_THEIR_CONTAINER', ['reject'], { why: 'booking_not_confirmed' })],
        ['the container was deleted', { proposals: [PROPOSAL()], locked: { shipment: null, bookings: [] } }, SEEN, refusal('NOT_THEIR_CONTAINER', ['reject'], { why: 'gone' })],
        ['the container holds another date than the card showed', { proposals: [PROPOSAL()] }, { ...SEEN, etd: '2026-10-20' },
            refusal('CONTAINER_CHANGED', ['reload'], { fields: ['etd'], held: SEEN })],
        ['the container already has a number the card did not show', { proposals: [PROPOSAL()], shipment: SHIP_ROW({ member_ext: 'MSKU1234565' }) }, SEEN,
            refusal('CONTAINER_CHANGED', ['reload'], { fields: ['ecn'], held: { ...SEEN, ecn: 'MSKU1234565' } })],
        ['a number is proposed and the container has no order to carry it', { proposals: [PROPOSAL()], members: [] }, SEEN, refusal('NO_ORDERS', ['reject'])],
        ['the number proposed fails its check digit', { proposals: [PROPOSAL({ requested_json: JSON.stringify({ ecn: 'CSQU3054384' }) })] }, SEEN, refusal('BAD_PROPOSAL', ['reject'], { fields: ['ecn'] })],
        ['the proposal asks for nothing', { proposals: [PROPOSAL({ requested_json: '{}' })] }, SEEN, refusal('EMPTY_PROPOSAL', ['reject'])],
        ['loading would fall after the departure held', { proposals: [PROPOSAL({ requested_json: JSON.stringify({ loadingDate: '2026-10-25' }) })] }, SEEN,
            refusal('DATES_OUT_OF_ORDER', ['reject'], { fields: ['loadingDate', 'etd'] })],
    ];
    for (const [name, opts, expected, body] of cases) {
        const t = build(baseRules(), opts);
        const r = await t.call(CLAIM, on(51, { expected }));
        assert.deepEqual(r, { status: 409, body }, name);
        assert.equal(marked(t, 'applying').length, 0, name);
        assert.equal(sqlOf(t).at(-1), 'ROLLBACK', name);
        assert.equal(sqlOf(t).includes('COMMIT'), false, name);
    }
});

test('claim: a number is compared as it is read, so its case and spacing are not a change', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL({ requested_json: JSON.stringify({ etd: '2026-10-22' }) })], shipment: SHIP_ROW({ member_ext: 'MSKU1234565' }) });
    const r = await t.call(CLAIM, on(51, { expected: { ecn: 'msku 123456-5', loadingDate: null, etd: '2026-10-21' } }));
    assert.equal(r.status, 200);
    // What the claim keeps is what the container HOLDS, read under the locks, not what the card sent.
    assert.equal(marked(t, 'applying')[0].params[1], JSON.stringify({ ecn: 'MSKU1234565', loadingDate: null, etd: '2026-10-21' }));
});

test('claim: a proposal of dates only needs no order on the container', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL({ requested_json: JSON.stringify({ loadingDate: '2026-10-18' }) })], members: [] });
    const r = await t.call(CLAIM, on(51, { expected: SEEN }));
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.requested, { loadingDate: '2026-10-18' });
});

test('claim: a claim nobody finished is put back first, and the proposal can be claimed again', async () => {
    const t = build(baseRules(), { proposals: [CLAIMED({ claim_token: 'f'.repeat(32), claimed_by: 'kim@us.com', lapsed: 1 })] });
    const r = await t.call(CLAIM, on(51, { expected: SEEN }));
    assert.equal(r.status, 200);
    const back = marked(t, 'pending');
    assert.equal(back.length, 1);
    assert.deepEqual(back[0].params, [[51]]);
    assert.match(back[0].sql, /SET status = 'pending', claim_token = NULL, claim_json = NULL, claimed_by = NULL, claimed_at = NULL WHERE id IN \(\?\) AND status = 'applying'$/);
    const sql = sqlOf(t);
    assert.ok(sql.indexOf(back[0].sql) < sql.indexOf(marked(t, 'applying')[0].sql));
    assert.notEqual(r.body.claimToken, 'f'.repeat(32));
});

test('claim: a proposal decided between the lock and the write is not claimed', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()], updated: sql => (/SET status = 'applying'/.test(sql) ? 0 : 1) });
    const r = await t.call(CLAIM, on(51, { expected: SEEN }));
    assert.deepEqual(r, { status: 409, body: refusal('ALREADY_DECIDED', ['reload']) });
    assert.equal(sqlOf(t).at(-1), 'ROLLBACK');
});

test('claim: a deadlock is tried once more, then said as something to retry', async () => {
    let n = 0;
    const once = [/FROM shipments WHERE id = \? FOR UPDATE/, () => (n++ === 0 ? err(1213) : [[lockedOf(SHIP_ROW()).shipment]])];
    let t = build([once, ...baseRules()], { proposals: [PROPOSAL()] });
    let r = await t.call(CLAIM, on(51, { expected: SEEN }));
    assert.equal(r.status, 200);
    assert.equal(sqlOf(t).filter(s => s === 'BEGIN').length, 2);
    assert.equal(sqlOf(t).filter(s => s === 'ROLLBACK').length, 1);
    assert.equal(marked(t, 'applying').length, 1);

    t = build([[/FROM shipments WHERE id = \? FOR UPDATE/, () => err(1213)], ...baseRules()], { proposals: [PROPOSAL()] });
    r = await t.call(CLAIM, on(51, { expected: SEEN }));
    assert.deepEqual(r, { status: 503, body: { error: 'The container was busy. Try again.', code: 'RETRY', allowedNext: ['retry'] } });
    assert.equal(sqlOf(t).filter(s => s === 'BEGIN').length, 2);
    assert.equal(marked(t, 'applying').length, 0);
});

// ── finish ───────────────────────────────────────────────────────────────

test('finish: a token that is not a claim\'s is refused before anything is read', async () => {
    for (const claimToken of [undefined, '', 'nope', TOKEN.toUpperCase(), `${TOKEN}0`]) {
        const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()] });
        const r = await t.call(FINISH, on(51, { claimToken }));
        assert.equal(r.status, 400, String(claimToken));
        assert.deepEqual({ code: r.body.code, field: r.body.field }, { code: 'BAD_FIELD', field: 'claimToken' });
        assert.equal(t.db.asked.length, 0);
    }
});

test('finish: the dates are written through PATCH\'s own code and the proposal is marked accepted, in one transaction', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()] });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 200, body: { ok: true, status: 'applied', requested: ASKED } });
    assert.deepEqual(t.patched, [{ id: 1214, body: { loadingDate: '2026-10-18', etd: '2026-10-22' }, userEmail: 'ops@us.com' }]);
    const w = marked(t, 'applied');
    assert.equal(w.length, 1);
    assert.deepEqual(w[0].params, ['ops@us.com', JSON.stringify(ASKED), 51, TOKEN]);
    assert.match(w[0].sql, /WHERE id = \? AND status = 'applying' AND claim_token = \?$/);
    const sql = sqlOf(t);
    assert.equal(sql.filter(s => s === 'BEGIN').length, 1);
    assert.ok(sql.indexOf('BEGIN') < sql.indexOf('PATCH'));
    assert.ok(sql.indexOf('PATCH') < sql.indexOf(w[0].sql));
    assert.ok(sql.indexOf(w[0].sql) < sql.indexOf('COMMIT'));
    assert.deepEqual(t.audits, [{
        entityType: 'forwarder_container_details', entityId: 51, action: 'applied',
        before: { shipmentId: 1214, forwarderId: 4, held: SEEN, base: SEEN },
        after: { requested: ASKED, ecnReset: null },
        userEmail: 'ops@us.com',
    }]);
});

test('finish: the transaction opens with the same locks as a claim, and reads nothing plainly before the last of them', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()] });
    await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.equal(sqlOf(t).indexOf('BEGIN'), 1);
    const locks = locksOf(t);
    assert.deepEqual(locks.map(tableOf), ['shipments', 'freight_bookings', 'forwarders', 'orders', 'forwarder_container_details']);
    for (const s of locks) assert.match(s, /FOR (UPDATE|SHARE)/, s.slice(0, 80));
});

test('finish: a proposal of a number only writes no date', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED({ requested_json: JSON.stringify({ ecn: ECN }) })] });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 200, body: { ok: true, status: 'applied', requested: { ecn: ECN } } });
    assert.deepEqual(t.patched, []);
    assert.equal(marked(t, 'applied').length, 1);
});

test('finish: a proposal of dates only asks nothing of the orders\' numbers', async () => {
    const t = build(baseRules(), { proposals: [CLAIMED({ requested_json: JSON.stringify({ etd: '2026-10-22' }) })] });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.equal(r.status, 200);
    assert.deepEqual(t.patched, [{ id: 1214, body: { etd: '2026-10-22' }, userEmail: 'ops@us.com' }]);
});

test('finish: only the claim it was given the token of, while that claim stands', async () => {
    const cases = [
        ['another claim\'s token', CLAIMED({ claim_token: 'f'.repeat(32) })],
        ['the claim lapsed', CLAIMED({ lapsed: 1 })],
        ['released: waiting again', PROPOSAL()],
        ['already accepted', PROPOSAL({ status: 'applied' })],
        ['sent back meanwhile', PROPOSAL({ status: 'rejected' })],
    ];
    for (const [name, row] of cases) {
        const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [row] });
        const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
        assert.deepEqual(r, { status: 409, body: refusal('CLAIM_LOST', ['reload']) }, name);
        assert.deepEqual(t.patched, [], name);
        assert.equal(marked(t, 'applied').length, 0, name);
        assert.equal(sqlOf(t).at(-1), 'ROLLBACK', name);
    }
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()] });
    assert.deepEqual(await t.call(FINISH, on(99, { claimToken: TOKEN })), { status: 404, body: refusal('NO_SUCH_PROPOSAL', ['reload']) });
});

test('finish: refused, with nothing written, when the container is no longer that forwarder\'s', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()], locked: otherForwarder });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 409, body: refusal('NOT_THEIR_CONTAINER', ['reject'], { why: null }) });
    assert.deepEqual(t.patched, []);
    assert.equal(marked(t, 'applied').length, 0);
});

test('finish: refused while the proposed number is not on every order of the container', async () => {
    const heldNow = { ecn: ECN, loadingDate: null, etd: '2026-10-21' };
    const cases = [
        ['one order has none', [{ id: 1, external_container_number: ECN, awb_number: null }, { id: 2, external_container_number: null, awb_number: null }]],
        ['one order has another', [{ id: 1, external_container_number: ECN, awb_number: null }, { id: 2, external_container_number: 'MSKU1234565', awb_number: null }]],
        ['the last order left the container', []],
    ];
    for (const [name, members] of cases) {
        const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()], members });
        const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
        assert.deepEqual(r, { status: 409, body: refusal('NOT_ON_CONTAINER', ['reload'], { fields: ['ecn'], held: heldNow }) }, name);
        assert.deepEqual(t.patched, [], name);
        assert.equal(marked(t, 'applied').length, 0, name);
    }
    // Not written at all yet: the container still holds none.
    const t = build(baseRules(), { proposals: [CLAIMED()] });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 409, body: refusal('NOT_ON_CONTAINER', ['reload'], { fields: ['ecn'], held: SEEN }) });
});

test('finish: an air waybill number is read from the orders as the card reads it', async () => {
    const awb = '176-12345675';
    const t = build(baseRules(), {
        shipment: SHIP_ROW({ mode: 'AIR', member_awb: awb }),
        proposals: [CLAIMED({ requested_json: JSON.stringify({ ecn: awb }) })],
        members: [{ id: 1, external_container_number: 'OLD-TEXT', awb_number: awb }, { id: 2, external_container_number: null, awb_number: awb }],
    });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 200, body: { ok: true, status: 'applied', requested: { ecn: awb } } });
});

test('finish: refused when someone else changed a date since the claim', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN({ member_etd: '2026-10-25' }), proposals: [CLAIMED()] });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 409, body: refusal('CONTAINER_CHANGED', ['reload'], { fields: ['etd'], held: { ecn: ECN, loadingDate: null, etd: '2026-10-25' } }) });
    assert.deepEqual(t.patched, []);
    assert.equal(marked(t, 'applied').length, 0);
    assert.equal(sqlOf(t).at(-1), 'ROLLBACK');
});

test('finish: the departure date ShipLine\'s own reset wrote is the one difference allowed, and only when a number was replaced', async () => {
    const replacing = CLAIMED({ claim_json: JSON.stringify({ ecn: 'MSKU1234565', loadingDate: null, etd: '2026-10-21' }) });
    // The reset cleared the departure date.
    const cleared = WITH_ECN({ member_etd: null, etd: null });
    let t = build(baseRules(), { shipment: cleared, proposals: [replacing] });
    let r = await t.call(FINISH, on(51, { claimToken: TOKEN, ecnReset: { etd: null } }));
    assert.equal(r.status, 200);
    assert.deepEqual(t.audits[0].after, { requested: ASKED, ecnReset: { etd: null } });

    // Not told of a reset: the cleared date is someone else's change.
    t = build(baseRules(), { shipment: cleared, proposals: [replacing] });
    r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 409, body: refusal('CONTAINER_CHANGED', ['reload'], { fields: ['etd'], held: { ecn: ECN, loadingDate: null, etd: null } }) });

    // The reset wrote the 10th; someone then set the 15th: refused, whatever the proposal asks.
    t = build(baseRules(), { shipment: WITH_ECN({ member_etd: '2026-10-15' }), proposals: [replacing] });
    r = await t.call(FINISH, on(51, { claimToken: TOKEN, ecnReset: { etd: '2026-10-10' } }));
    assert.equal(r.body.code, 'CONTAINER_CHANGED');
    assert.deepEqual(r.body.fields, ['etd']);
    assert.deepEqual(t.patched, []);

    // A first number resets nothing, so a reset reported with it is not believed.
    t = build(baseRules(), { shipment: cleared, proposals: [CLAIMED()] });
    r = await t.call(FINISH, on(51, { claimToken: TOKEN, ecnReset: { etd: null } }));
    assert.equal(r.body.code, 'CONTAINER_CHANGED');
});

test('finish: a loading date the reset left after the departure date is refused as out of order', async () => {
    const replacing = CLAIMED({
        claim_json: JSON.stringify({ ecn: 'MSKU1234565', loadingDate: null, etd: '2026-10-21' }),
        requested_json: JSON.stringify({ ecn: ECN, loadingDate: '2026-10-18' }),
    });
    const t = build(baseRules(), { shipment: WITH_ECN({ member_etd: '2026-10-12' }), proposals: [replacing] });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN, ecnReset: { etd: '2026-10-12' } }));
    assert.deepEqual(r, { status: 409, body: refusal('DATES_OUT_OF_ORDER', ['reject'], { fields: ['loadingDate', 'etd'], held: { ecn: ECN, loadingDate: null, etd: '2026-10-12' } }) });
    assert.deepEqual(t.patched, []);
});

test('finish: when the date write fails nothing is marked, and the transaction is rolled back', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()], patch: () => { throw new S.ShipmentError(409, 'MERGED', 'Shipment 1214 was merged into 7.', { mergedIntoId: 7 }); } });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    // PATCH's own refusal, as PATCH answers it: its code and what it carries.
    assert.deepEqual(r, { status: 409, body: { error: 'Shipment 1214 was merged into 7.', code: 'MERGED', mergedIntoId: 7 } });
    assert.equal(marked(t, 'applied').length, 0);
    assert.deepEqual(t.audits, []);
    assert.equal(sqlOf(t).at(-1), 'ROLLBACK');
    assert.equal(sqlOf(t).includes('COMMIT'), false);
});

test('finish: when the mark does not take, the dates written with it are rolled back', async () => {
    const t = build(baseRules(), { shipment: WITH_ECN(), proposals: [CLAIMED()], updated: 0 });
    const r = await t.call(FINISH, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 409, body: refusal('CLAIM_LOST', ['reload']) });
    assert.equal(t.patched.length, 1);
    assert.deepEqual(t.audits, []);
    assert.equal(sqlOf(t).at(-1), 'ROLLBACK');
    assert.equal(sqlOf(t).includes('COMMIT'), false);
});

// ── release ──────────────────────────────────────────────────────────────

test('release: gives up the claim its token names, and only that one', async () => {
    let t = build(baseRules(), { proposals: [CLAIMED()] });
    let r = await t.call(RELEASE, on(51, { claimToken: TOKEN }));
    assert.deepEqual(r, { status: 200, body: { ok: true, released: true } });
    assert.equal(t.db.asked.length, 1);
    assert.deepEqual(t.db.asked[0].params, [51, TOKEN]);
    assert.match(t.db.asked[0].sql, /SET status = 'pending', claim_token = NULL, claim_json = NULL, claimed_by = NULL, claimed_at = NULL WHERE id = \? AND status = 'applying' AND claim_token = \?$/);

    // An old token changes no row.
    t = build(baseRules(), { proposals: [CLAIMED()], updated: 0 });
    r = await t.call(RELEASE, on(51, { claimToken: 'f'.repeat(32) }));
    assert.deepEqual(r, { status: 200, body: { ok: true, released: false } });

    // No token: no statement.
    t = build(baseRules(), { proposals: [CLAIMED()] });
    r = await t.call(RELEASE, on(51, {}));
    assert.deepEqual(r, { status: 200, body: { ok: true, released: false } });
    assert.equal(t.db.asked.length, 0);
});

// ── send back ────────────────────────────────────────────────────────────

test('send back: needs a note the forwarder can read', async () => {
    for (const note of [undefined, '', '  no ', 'x'.repeat(501)]) {
        const t = build(baseRules(), { proposals: [PROPOSAL()] });
        const r = await t.call(REJECT, on(51, { note }));
        assert.equal(r.status, 400, String(note).slice(0, 10));
        assert.deepEqual({ code: r.body.code, field: r.body.field }, { code: 'NOTE_REQUIRED', field: 'note' });
        assert.equal(t.db.asked.length, 0);
    }
});

test('send back: the note goes on the proposal, trimmed, with who decided', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()] });
    const r = await t.call(REJECT, on(51, { note: '  Wrong container number, please check.  ' }));
    assert.deepEqual(r, { status: 200, body: { ok: true, status: 'rejected' } });
    const w = marked(t, 'rejected');
    assert.equal(w.length, 1);
    assert.deepEqual(w[0].params, ['Wrong container number, please check.', 'ops@us.com', 51]);
    assert.match(w[0].sql, /WHERE id = \? AND status IN \('pending', 'applying'\)$/);
    assert.deepEqual(t.audits, [{
        entityType: 'forwarder_container_details', entityId: 51, action: 'rejected',
        before: { shipmentId: 1214, forwarderId: 4, requested: ASKED },
        after: { note: 'Wrong container number, please check.' },
        userEmail: 'ops@us.com',
    }]);
    assert.equal(sqlOf(t).at(-1), 'COMMIT');
});

test('send back: a proposal whose container is no longer its forwarder\'s can still be dismissed: ownership is not asked', async () => {
    const t = build(baseRules(), { proposals: [PROPOSAL()], locked: otherForwarder });
    const r = await t.call(REJECT, on(51, { note: 'No longer your container' }));
    assert.equal(r.status, 200);
    assert.equal(t.db.wrote(/FROM shipments WHERE id = \? FOR UPDATE/).length, 0);
    // Its first statement in the transaction is the lock on the proposals.
    assert.match(sqlOf(t)[sqlOf(t).indexOf('BEGIN') + 1], LAST_LOCK);
});

test('send back: a claim that lapsed does not stand in the way', async () => {
    const t = build(baseRules(), { proposals: [CLAIMED({ lapsed: 1 })] });
    const r = await t.call(REJECT, on(51, { note: 'Wrong number' }));
    assert.equal(r.status, 200);
    assert.equal(marked(t, 'rejected').length, 1);
});

test('send back: refused while it is being accepted, once decided, or once a newer one exists', async () => {
    const cases = [
        ['being accepted', [CLAIMED()], refusal('CLAIM_IN_PROGRESS', ['reload'], { claimedBy: 'ops@us.com', claimedAt: '2026-11-23T10:00:00Z' })],
        ['accepted', [PROPOSAL({ status: 'applied' })], refusal('ALREADY_DECIDED', ['reload'])],
        ['sent back', [PROPOSAL({ status: 'rejected' })], refusal('ALREADY_DECIDED', ['reload'])],
        ['superseded', [PROPOSAL(), PROPOSAL({ id: 52 })], refusal('SUPERSEDED', ['reload'])],
    ];
    for (const [name, proposals, body] of cases) {
        const t = build(baseRules(), { proposals });
        const r = await t.call(REJECT, on(51, { note: 'Wrong number' }));
        assert.deepEqual(r, { status: 409, body }, name);
        assert.equal(marked(t, 'rejected').length, 0, name);
        assert.deepEqual(t.audits, [], name);
    }
    const t = build(baseRules(), { proposals: [PROPOSAL()] });
    assert.deepEqual(await t.call(REJECT, on(99, { note: 'Wrong number' })), { status: 404, body: refusal('NO_SUCH_PROPOSAL', ['reload']) });
});

// ── what waits, across containers ────────────────────────────────────────

const LISTED = (over = {}) => ({
    id: 51, shipment_id: 1214, forwarder_id: 4, status: 'pending', requested_json: JSON.stringify(ASKED),
    user_email: 'mei@fwd.example', user_name: 'Mei', created_at_iso: '2026-11-23T09:00:00Z', decided_at_iso: null, lapsed: 0,
    reference: '339', mode: 'SEA', forwarder_name: 'test2forwarder', theirs: 1, live: 1,
    ...over,
});
// The list reads proposals, then files (none unless a test puts its own rule in front).
const listRules = rows => [
    [/forwarder_container_details d LEFT JOIN portal_forwarder_containers_v c/, () => (rows instanceof Error ? rows : [rows])],
    [/forwarder_uploads u LEFT JOIN portal_forwarder_containers_v c .* LEFT JOIN shipments s/, () => [[]]],
    ...baseRules(),
];
const LIST_ROWS = [
    LISTED({ id: 61, shipment_id: 9, reference: '301', status: 'applying' }),
    LISTED({ id: 60, shipment_id: 8, reference: '300', theirs: 0 }),
    LISTED(),
    LISTED({ id: 50, live: 0 }),
    LISTED({ id: 49, shipment_id: 7, reference: '299', status: 'applying', lapsed: 1 }),
];

test('list: what waits for our team on containers that are still their forwarder\'s, newest first', async () => {
    const t = build(listRules(LIST_ROWS));
    const r = await t.call(LIST);
    assert.equal(r.status, 200);
    assert.equal(r.body.unavailable, false);
    assert.equal(r.body.canDecide, true);
    assert.deepEqual(r.body.data.map(d => [d.id, d.status]), [[61, 'applying'], [51, 'pending'], [49, 'pending']]);
    assert.deepEqual(r.body.data[1], {
        kind: 'details', id: 51, shipmentId: 1214, reference: '339', mode: 'SEA', forwarderId: 4, forwarderName: 'test2forwarder',
        status: 'pending', waiting: true, live: true, theirs: true, actionable: true, requested: ASKED, byName: 'Mei', byEmail: 'mei@fwd.example',
        at: '2026-11-23T09:00:00Z', decidedAt: null,
    });
    assert.match(t.db.asked[0].sql, /WHERE d\.status IN \('pending', 'applying'\) HAVING theirs = 1 AND live = 1 ORDER BY d\.id DESC LIMIT 200$/);
});

test('list: with status=all, everything, each row saying whether it can still be acted on', async () => {
    const rows = [...LIST_ROWS, LISTED({ id: 40, status: 'applied', decided_at_iso: '2026-11-22T12:00:00Z' })];
    const t = build(listRules(rows));
    const r = await t.call(LIST, { query: { status: 'all' } });
    assert.deepEqual(
        r.body.data.map(d => [d.id, d.status, d.waiting, d.actionable]),
        [[61, 'applying', true, true], [60, 'pending', true, false], [51, 'pending', true, true], [50, 'pending', true, false], [49, 'pending', true, true], [40, 'applied', false, false]]
    );
    assert.equal(r.body.data[5].decidedAt, '2026-11-22T12:00:00Z');
    // Why a row cannot be acted on: a newer one replaced it, or the container is no longer theirs.
    assert.deepEqual(r.body.data.map(d => [d.id, d.live, d.theirs]), [[61, true, true], [60, true, false], [51, true, true], [50, false, true], [49, true, true], [40, true, true]]);
    assert.doesNotMatch(t.db.asked[0].sql, /HAVING|WHERE d\.status IN/);
});

test('list: an accountant reads it and is told they may not decide', async () => {
    const r = await build(listRules(LIST_ROWS)).call(LIST, { userType: 'accountant' });
    assert.equal(r.status, 200);
    assert.equal(r.body.canDecide, false);
    assert.equal(r.body.data.length, 3);
});

test('summary: the badge counts what the list shows', async () => {
    const r = await build(listRules(LIST_ROWS)).call(SUMMARY);
    assert.deepEqual(r, { status: 200, body: { toReview: 3, unavailable: false } });
});

test('list and summary: where the portal\'s tables do not exist there is nothing, said as such; any other fault is an error', async () => {
    for (const errno of [1146, 1049]) {
        assert.deepEqual(await build(listRules(err(errno))).call(LIST), { status: 200, body: { data: [], unavailable: true, canDecide: true } });
        assert.deepEqual(await build(listRules(err(errno))).call(SUMMARY), { status: 200, body: { toReview: 0, unavailable: true } });
    }
    assert.equal((await build(listRules(err(1142))).call(LIST)).status, 500);
    assert.equal((await build(listRules(err(1142))).call(SUMMARY)).status, 500);
});

// ── slice C on the tab and the list: the files a forwarder sent ──────────

const FILE_ROW = (over = {}) => ({
    id: 70, shipment_id: 1214, container_ref: '339', forwarder_id: 4, kind: 'invoice', user_email: 'mei@fwd.example', user_name: 'Mei',
    filename: 'invoice 339.pdf', content_type: 'application/pdf', file_size: 120000, final_key: `files/fwd/70/${'a'.repeat(32)}.pdf`, s3_etag: '"e1"', note: null,
    upload_status: 'sent', review_status: 'open', decided_note: null, decided_by: null, filed_key: null, filed_id: null,
    sent_at_iso: '2026-11-23T09:01:00Z', decided_at_iso: null, withdrawn_at_iso: null, forwarder_name: 'test2forwarder', theirs: 1,
    reference: '339', mode: 'SEA',
    ...over,
});

test('tab: the files the forwarder sent, by kind, with what became of each; never a storage key', async () => {
    const uploads = [
        FILE_ROW({ id: 73, kind: 'bill_of_lading', filename: 'bl.pdf', review_status: 'rejected', decided_note: 'Blurred', decided_by: 'kim@us.com', decided_at_iso: '2026-11-23T10:00:00Z' }),
        FILE_ROW({ id: 72, kind: 'loading_photo', filename: 'b.jpg', content_type: 'image/jpeg' }),
        FILE_ROW({ id: 71, kind: 'loading_photo', filename: 'a.jpg', content_type: 'image/jpeg', review_status: 'received', decided_by: 'kim@us.com', filed_id: 901, filed_key: 'container-photos/x/a.jpg' }),
        FILE_ROW({ id: 70 }),
    ];
    const t = build(baseRules({ uploads }));
    const r = await t.call(TAB);
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.uploads.invoice, [{
        id: 70, kind: 'invoice', shipmentId: 1214, forwarderId: 4, forwarderName: 'test2forwarder', filename: 'invoice 339.pdf', contentType: 'application/pdf', fileSize: 120000,
        note: null, byName: 'Mei', byEmail: 'mei@fwd.example', sentAt: '2026-11-23T09:01:00Z', state: 'open', waiting: true, theirs: true, actionable: true,
        decidedNote: null, decidedBy: null, decidedAt: null, filedId: null,
    }]);
    assert.deepEqual(r.body.uploads.loadingPhotos.map(u => [u.id, u.state, u.actionable, u.filedId]), [[72, 'open', true, null], [71, 'received', false, 901]]);
    assert.deepEqual(r.body.uploads.billOfLading.map(u => [u.id, u.state, u.decidedNote]), [[73, 'rejected', 'Blurred']]);
    assert.deepEqual(r.body.uploads.strays, []);
    // Two files wait: they are counted with any proposal.
    assert.equal(r.body.toReview, 2);
    for (const hidden of ['files/fwd', 'container-photos/x', '"e1"']) assert.equal(JSON.stringify(r.body).includes(hidden), false, hidden);
});

test('tab: a file of a forwarder the container is no longer with can be dismissed, never accepted, and is not counted', async () => {
    // 82: the view still says it is theirs (it lags the locked rows by a moment): this read decides, and it is not.
    const t = build(baseRules({ uploads: [
        FILE_ROW({ id: 82, forwarder_id: 3, forwarder_name: 'DCG', theirs: 1 }),
        FILE_ROW({ id: 80, forwarder_id: 3, forwarder_name: 'DCG', theirs: 0 }),
        FILE_ROW({ id: 81, forwarder_id: 3, forwarder_name: 'DCG', theirs: 0, review_status: 'rejected' }),
    ] }));
    const r = await t.call(TAB);
    assert.deepEqual(r.body.uploads.strays.map(u => [u.id, u.forwarderName, u.waiting, u.actionable]), [[82, 'DCG', true, false], [80, 'DCG', true, false]]);
    assert.deepEqual([r.body.uploads.invoice, r.body.toReview], [[], 0]);
});

test('tab: the bills of lading filed for the container, and who may remove each', async () => {
    const documents = [{ id: 902, shipment_id: 1214, forwarder_id: 4, forwarder_name: 'test2forwarder', kind: 'bill_of_lading', filename: 'bl.pdf', public_url: 'https://po.example/forwarder-documents/x/bl.pdf',
        content_type: 'application/pdf', file_size: 9, forwarder_upload_id: 73, uploaded_by_email: 'mei@fwd.example', accepted_by_email: 'kim@us.com', deleted_at: null, created_at_iso: '2026-11-23T12:00:00Z' }];
    let r = await build(baseRules({ documents })).call(TAB);
    assert.deepEqual(r.body.documents, [{
        id: 902, kind: 'bill_of_lading', forwarderId: 4, forwarderName: 'test2forwarder', filename: 'bl.pdf', url: 'https://po.example/forwarder-documents/x/bl.pdf', contentType: 'application/pdf', fileSize: 9,
        uploadedByEmail: 'mei@fwd.example', acceptedByEmail: 'kim@us.com', createdAt: '2026-11-23T12:00:00Z', canRemove: false,
    }]);
    r = await build(baseRules({ documents })).call(TAB, { userType: 'admin' });
    assert.equal(r.body.documents[0].canRemove, true);
    r = await build(baseRules({ documents })).call(TAB, { userEmail: 'kim@us.com' });
    assert.equal(r.body.documents[0].canRemove, true);
});

test('tab: where the portal\'s uploads table is missing, nothing a forwarder sent can be shown, and that is said', async () => {
    const r = await build(baseRules({ uploads: err(1146) })).call(TAB);
    assert.equal(r.status, 200);
    assert.equal(r.body.unavailable, true);
    assert.deepEqual(r.body.uploads, { invoice: [], loadingPhotos: [], billOfLading: [], strays: [] });
    assert.equal((await build(baseRules({ uploads: err(1142) })).call(TAB)).status, 500);
});

const fileListRule = rows => [/forwarder_uploads u LEFT JOIN portal_forwarder_containers_v c .* LEFT JOIN shipments s/, () => (rows instanceof Error ? rows : [rows])];

test('list: files and proposals together, the newest first, and the badge counts both', async () => {
    const files = [FILE_ROW({ id: 72, kind: 'loading_photo', filename: 'b.jpg', sent_at_iso: '2026-11-23T09:30:00Z' }), FILE_ROW({ id: 70, sent_at_iso: '2026-11-23T08:00:00Z' })];
    const rules = [fileListRule(files), ...listRules([LISTED()])];
    const r = await build(rules).call(LIST);
    assert.deepEqual(r.body.data.map(d => [d.kind, d.id, d.at]), [['loading_photo', 72, '2026-11-23T09:30:00Z'], ['details', 51, '2026-11-23T09:00:00Z'], ['invoice', 70, '2026-11-23T08:00:00Z']]);
    assert.deepEqual(r.body.data[0], {
        kind: 'loading_photo', id: 72, shipmentId: 1214, reference: '339', mode: 'SEA', forwarderId: 4, forwarderName: 'test2forwarder', status: 'pending',
        waiting: true, live: true, theirs: true, actionable: true, filename: 'b.jpg', fileSize: 120000, requested: {}, byName: 'Mei', byEmail: 'mei@fwd.example',
        at: '2026-11-23T09:30:00Z', decidedAt: null,
    });
    assert.deepEqual((await build(rules).call(SUMMARY)).body, { toReview: 3, unavailable: false });
    // The default list asks the database only for what waits on a container still theirs.
    const asked = build(rules);
    await asked.call(LIST);
    assert.match(asked.db.asked.find(a => /forwarder_uploads u LEFT JOIN/.test(a.sql)).sql, /WHERE u\.upload_status = 'sent' AND u\.review_status = 'open' AND c\.shipment_id IS NOT NULL ORDER BY u\.id DESC LIMIT 200$/);
});

test('list: with status=all, files that were decided or are no longer theirs are listed too, each saying so', async () => {
    const files = [
        FILE_ROW({ id: 75, review_status: 'received', decided_at_iso: '2026-11-23T12:00:00Z' }),
        FILE_ROW({ id: 74, review_status: 'rejected' }),
        FILE_ROW({ id: 73, theirs: 0 }),
    ];
    const asked = build([fileListRule(files), ...listRules([])]);
    const r = await asked.call(LIST, { query: { status: 'all' } });
    assert.deepEqual(r.body.data.map(d => [d.id, d.status, d.waiting, d.theirs, d.actionable]), [[75, 'applied', false, true, false], [74, 'rejected', false, true, false], [73, 'pending', true, false, false]]);
    assert.match(asked.db.asked.find(a => /forwarder_uploads u LEFT JOIN/.test(a.sql)).sql, /WHERE u\.upload_status = 'sent' ORDER BY u\.id DESC LIMIT 200$/);
});

test('list: where the portal\'s uploads table is missing, nothing is listed and that is said', async () => {
    const r = await build([fileListRule(err(1146)), ...listRules([LISTED()])]).call(LIST);
    assert.deepEqual(r, { status: 200, body: { data: [], unavailable: true, canDecide: true } });
    assert.equal((await build([fileListRule(err(1142)), ...listRules([])]).call(LIST)).status, 500);
});
