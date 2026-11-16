'use strict';

// Route tests for src/services/freight-quote-routes.js (docs/freight-quotes.md)
// against an in-memory stand-in for the five tables: a send to two forwarders
// and its retry, a Front failure, booking an option, the forwarder's
// confirmation closing the other link as `filled`, a cancel reopening it, and
// the contents email's claim. The fake answers the statements the routes
// actually send and throws on any other, so a changed query shows up here.
// No database, no Front.
//   node --test tools/test-freight-quote-routes.js

const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const express = require('express');
const { registerFreightQuoteRoutes } = require('../src/services/freight-quote-routes');

process.env.SUPPLIER_PORTAL_URL = 'https://portal.example';

const BATCH = 'a1b2c3d4-0000-4000-8000-000000000001';
const NOW = () => new Date('2026-11-16T10:00:00Z');
const future = days => new Date(NOW().getTime() + days * 86400000);

function harness({ userType = 'standard', frontOk = () => true } = {}) {
    const db = {
        forwarders: [
            { id: 1, name: 'DCG', notes: null, deleted_at: null },
            { id: 2, name: 'Savino', notes: null, deleted_at: null },
        ],
        contacts: [
            { id: 1, forwarder_id: 1, name: 'Amy', email: 'amy@dcg.com', deleted_at: null },
            { id: 2, forwarder_id: 2, name: null, email: 'ops@savino.com', deleted_at: null },
        ],
        requests: [], options: [], bookings: [],
        shipments: [{ id: 50, name: 'Draft 12', reference: null, stage: 'DRAFT', deleted_at: null, merged_into_id: null }],
        audits: [], receipts: [], sent: [], preapproved: [],
    };
    const live = b => b.status === 'requested' || b.status === 'confirmed';
    const requestRow = r => ({ ...r, expired: r.expires_at <= NOW() ? 1 : 0 });
    const ok = n => [{ affectedRows: n }];

    const conn = {
        beginTransaction: async () => {}, commit: async () => {}, rollback: async () => {},
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            // forwarders
            if (s.startsWith('SELECT id, name, notes, created_by_email, created_at, updated_at FROM forwarders')) {
                const ids = params[0];
                return [db.forwarders.filter(f => !f.deleted_at && (!ids || ids.includes(f.id)))];
            }
            if (s.startsWith('SELECT id, forwarder_id, name, email FROM forwarder_contacts')) {
                return [db.contacts.filter(c => !c.deleted_at && params[0].includes(c.forwarder_id))];
            }
            if (s.startsWith('SELECT email FROM forwarder_contacts')) {
                return [db.contacts.filter(c => !c.deleted_at && c.forwarder_id === params[0])];
            }
            // requests
            if (s.startsWith('SELECT id, sent_at, revoked_at FROM freight_quote_requests WHERE batch_id = ? AND forwarder_id = ?')) {
                return [db.requests.filter(r => r.batch_id === params[0] && r.forwarder_id === params[1])];
            }
            if (s.startsWith('INSERT INTO freight_quote_requests')) {
                const [token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, destination,
                    container_size, services, staff_note, sent_to, subject, created_by_email, days] = params;
                const id = db.requests.length + 1;
                db.requests.push({
                    id, token, batch_id, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, destination,
                    container_size, services, staff_note, sent_to, subject, created_by_email, created_at: NOW(), sent_at: null,
                    expires_at: future(days), revoked_at: null, revoke_reason: null, answer_count: 0, answered_at: null,
                    answered_by_name: null, answered_by_email: null, answer_source: null, answer_note: null, declined: 0, row_version: 1,
                });
                return [{ insertId: id }];
            }
            if (s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'send_failed'")
                || s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'staff'")) {
                const r = db.requests.find(x => x.id === params[0]);
                Object.assign(r, { revoked_at: NOW(), revoke_reason: /send_failed/.test(s) ? 'send_failed' : 'staff', row_version: r.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET sent_at = UTC_TIMESTAMP()')) {
                Object.assign(db.requests.find(x => x.id === params[2]), { sent_at: NOW(), front_message_uid: params[0] });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET row_version = row_version + 1, expires_at = GREATEST')) {
                db.requests.find(x => x.id === params[1]).row_version += 1;
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_quote_requests SET answer_count = ?')) {
                const [answerNo, email, note, declined, id] = params;
                const r = db.requests.find(x => x.id === id);
                Object.assign(r, { answer_count: answerNo, answered_at: NOW(), answered_by_email: email, answer_source: 'staff', answer_note: note, declined, row_version: r.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('SELECT r.id, r.token') && s.includes('WHERE r.id = ?')) {
                return [db.requests.filter(r => r.id === params[0]).map(requestRow)];
            }
            if (s.startsWith('SELECT r.id, r.token')) { // the board
                return [[...db.requests].reverse().map(requestRow)];
            }
            // closing and reopening `filled` requests
            if (s.startsWith('SELECT r.id FROM freight_quote_requests r WHERE r.revoked_at IS NULL AND EXISTS')) {
                const confirmedBatches = db.bookings.filter(b => b.status === 'confirmed')
                    .map(b => db.requests.find(r => r.id === b.request_id));
                return [db.requests.filter(r => !r.revoked_at
                    && confirmedBatches.some(c => c.batch_id === r.batch_id && c.id !== r.id)
                    && !db.bookings.some(b => b.request_id === r.id && live(b)))];
            }
            if (s.startsWith("UPDATE freight_quote_requests SET revoked_at = UTC_TIMESTAMP(), revoke_reason = 'filled'")) {
                const rows = db.requests.filter(r => params[0].includes(r.id) && !r.revoked_at);
                rows.forEach(r => Object.assign(r, { revoked_at: NOW(), revoke_reason: 'filled', row_version: r.row_version + 1 }));
                return ok(rows.length);
            }
            if (s.startsWith("SELECT COUNT(*) AS n FROM freight_bookings b JOIN freight_quote_requests r ON r.id = b.request_id WHERE r.batch_id = ? AND b.status = 'confirmed'")) {
                return [[{ n: db.bookings.filter(b => b.status === 'confirmed' && db.requests.find(r => r.id === b.request_id).batch_id === params[0]).length }]];
            }
            if (s.startsWith('UPDATE freight_quote_requests SET revoked_at = NULL, revoke_reason = NULL')) {
                const rows = db.requests.filter(r => r.batch_id === params[1] && r.revoke_reason === 'filled');
                rows.forEach(r => Object.assign(r, { revoked_at: null, revoke_reason: null, row_version: r.row_version + 1 }));
                return ok(rows.length);
            }
            // options
            if (s.startsWith('INSERT INTO freight_quote_options')) {
                for (let i = 0; i < params.length; i += 15) {
                    const [request_id, answer_no, line_no, loading_date, sailing_date, transit_days, port_from, port_to,
                        service, container_size, price, currency, carrier, valid_until, note] = params.slice(i, i + 15);
                    db.options.push({
                        id: db.options.length + 1, request_id, answer_no, line_no, loading_date, sailing_date, transit_days,
                        port_from, port_to, service, container_size, price, currency, carrier, valid_until, note,
                    });
                }
                return ok(params.length / 15);
            }
            if (s.startsWith('SELECT * FROM freight_quote_options WHERE id = ?')) {
                return [db.options.filter(o => o.id === params[0])];
            }
            if (s.startsWith('SELECT o.*, (o.answer_no = r.answer_count) AS is_current FROM freight_quote_options o')) {
                const booked = params[1] || [];
                return [db.options.map((o) => {
                    const r = db.requests.find(x => x.id === o.request_id);
                    return { ...o, is_current: o.answer_no === r.answer_count ? 1 : 0 };
                }).filter(o => o.is_current || booked.includes(o.id))];
            }
            // bookings
            if (s.startsWith('SELECT * FROM freight_bookings WHERE request_id IN (?)')) {
                return [db.bookings.filter(b => params[0].includes(b.request_id))];
            }
            if (s.startsWith('SELECT * FROM freight_bookings WHERE request_id = ? AND status IN (?) LIMIT 1')) {
                return [db.bookings.filter(b => b.request_id === params[0] && live(b)).slice(0, 1)];
            }
            if (s.startsWith('SELECT * FROM freight_bookings WHERE id = ?')) {
                return [db.bookings.filter(b => b.id === params[0])];
            }
            if (s.startsWith('SELECT id FROM freight_bookings WHERE shipment_id = ? AND status IN (?) AND id <> ?')) {
                return [db.bookings.filter(b => b.shipment_id === params[0] && live(b) && b.id !== params[2])];
            }
            if (s.startsWith('INSERT INTO freight_bookings')) {
                const [request_id, option_id, requested_by_email, request_note, sent_to, subject, shipment_id, draft_container_name] = params;
                if (db.bookings.some(b => b.request_id === request_id && live(b))) throw Object.assign(new Error('dup'), { errno: 1062 });
                const id = db.bookings.length + 1;
                db.bookings.push({
                    id, request_id, option_id, status: 'requested', requested_by_email, requested_at: NOW(), request_note, sent_to,
                    subject, shipment_id, draft_container_name, booking_ref: null, response_note: null, responded_at: null,
                    responded_by_name: null, responded_by_email: null, response_source: null, cancelled_at: null, cancel_reason: null,
                    contents_sent_at: null, contents_sent_to: null, contents_json: null, row_version: 1,
                });
                return [{ insertId: id }];
            }
            if (s.startsWith("UPDATE freight_bookings SET status = 'cancelled', cancel_reason = 'send_failed'")) {
                Object.assign(db.bookings.find(b => b.id === params[0]), { status: 'cancelled', cancel_reason: 'send_failed' });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_bookings SET front_message_uid = ?')) return ok(1);
            if (s.startsWith('UPDATE freight_bookings SET row_version = row_version + 1 WHERE id = ? AND row_version = ? AND status IN (?)')) {
                const b = db.bookings.find(x => x.id === params[0] && x.row_version === params[1] && live(x));
                if (!b) return ok(0);
                b.row_version += 1;
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_bookings SET contents_sent_at = UTC_TIMESTAMP()')) {
                const b = db.bookings.find(x => x.id === params[2]);
                Object.assign(b, { contents_sent_at: NOW(), contents_sent_to: params[0], contents_json: params[1], row_version: b.row_version + 1 });
                return ok(1);
            }
            if (s.startsWith('UPDATE freight_bookings SET ') && s.endsWith('row_version = row_version + 1 WHERE id = ?')) { // PATCH
                const b = db.bookings.find(x => x.id === params[params.length - 1]);
                const p = [...params];
                if (s.includes('shipment_id = ?')) Object.assign(b, { shipment_id: p.shift(), draft_container_name: p.shift(), contents_json: null, contents_sent_at: null });
                if (s.includes("status = 'confirmed'")) Object.assign(b, { status: 'confirmed', booking_ref: p.shift(), response_note: p.shift(), responded_by_email: p.shift(), response_source: 'staff', responded_at: NOW() });
                if (s.includes("status = 'cancelled'")) Object.assign(b, { status: 'cancelled', cancel_reason: 'staff', cancelled_by_email: p.shift(), cancelled_at: NOW() });
                b.row_version += 1;
                return ok(1);
            }
            // shipments, receipts
            if (s.startsWith('SELECT id, name, reference, stage, deleted_at, merged_into_id FROM shipments WHERE id = ?')) {
                return [db.shipments.filter(x => x.id === params[0])];
            }
            if (s.startsWith('SELECT id, name, reference, stage FROM shipments WHERE id IN (?)')) {
                return [db.shipments.filter(x => params[0].includes(x.id))];
            }
            if (s.startsWith('INSERT INTO email_receipts')) { db.receipts.push(params); return [{ insertId: db.receipts.length }]; }
            throw new Error(`unexpected statement: ${s.slice(0, 140)}`);
        },
    };

    const lines = [
        { sku: 'Gauze', jfCode: 'JF0001', lot: 'L1', poNumber: 'PO-1', orderedUnits: 1000, noOfCartons: 10, totalCbm: 1.2, cartonWeight: 12, supplier: 'Acme', port: 'Ningbo' },
    ];
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => { req.userType = userType; req.userEmail = 'staff@example.com'; next(); });
    const logs = [];
    registerFreightQuoteRoutes(app, {
        withConnection: fn => fn(conn),
        recordAudit: async (_c, a) => { db.audits.push(a); },
        getEmailTemplate: async (_c, key) => ({ subject: `${key} {containerSize}{sailingDate}`, bodyHtml: '<p>Body for {forwarder}</p>' }),
        renderTemplate: (str, vars) => String(str).replace(/\{(\w+)\}/g, (m, k) => (vars[k] != null ? String(vars[k]) : '')),
        loadForwarderQuoteForPdf: async () => ({ lines, originPorts: ['Ningbo'] }),
        generateDraftDocumentSet: async () => { throw new Error('not expected: attachDocuments is false in these tests'); },
        s3: null,
        sendFrontMessage: async (msg) => {
            db.sent.push(msg);
            return frontOk(msg) ? { ok: true, frontMessageUid: `m${db.sent.length}`, frontConversationId: 'c1' } : { ok: false, reason: 'front_error', status: 500 };
        },
        preapprove: async (_c, { emails }) => { db.preapproved.push(...emails); return { status: 'ok', users: [] }; },
        londonToday: () => '2026-11-16',
        log: { error: (...a) => logs.push(['error', ...a]), warn: (...a) => logs.push(['warn', ...a]), info: () => {} },
    });

    const call = (method, urlPath, body) => new Promise((resolve, reject) => {
        const server = app.listen(0, () => {
            const { port } = server.address();
            const req = http.request({ port, method, path: urlPath, headers: { 'content-type': 'application/json' } }, (res) => {
                let text = '';
                res.on('data', (d) => { text += d; });
                res.on('end', () => { server.close(); resolve({ status: res.statusCode, body: text ? JSON.parse(text) : null }); });
            });
            req.on('error', (e) => { server.close(); reject(e); });
            if (body !== undefined) req.write(JSON.stringify(body));
            req.end();
        });
    });
    return { db, call, logs };
}

const SEND = {
    forwarderIds: [1, 2], batchId: BATCH, windowStart: '2026-12-01', windowEnd: '2026-12-31',
    services: ['door_to_port'], containerSize: '40HQ',
};
const OPTION = {
    loadingDate: '2026-12-05', sailingDate: '2026-12-10', transitDays: 32, portFrom: 'Ningbo', portTo: 'Felixstowe',
    service: 'door_to_port', price: '1850', currency: 'USD',
};

// Two forwarders asked, both answered (typed in by staff), ready to book.
async function answered(h) {
    await h.call('POST', '/api/v1/freight-quotes', SEND);
    await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [OPTION] });
    await h.call('POST', '/api/v1/freight-quotes/2/options', { options: [{ ...OPTION, price: '1700' }] });
}

test('send: one row, one link and one email per forwarder; the same batchId again emails nobody', async () => {
    const h = harness();
    const first = await h.call('POST', '/api/v1/freight-quotes', SEND);
    assert.equal(first.status, 200);
    assert.deepEqual(first.body.results.map(r => r.outcome), ['sent', 'sent']);
    assert.equal(h.db.sent.length, 2);
    assert.deepEqual(h.db.sent[0].to, ['amy@dcg.com']);
    assert.match(h.db.sent[0].html, /https:\/\/portal\.example\/freight\/[a-f0-9]{48}/);
    assert.match(h.db.sent[0].html, /Body for DCG/);
    assert.deepEqual(h.db.preapproved, ['amy@dcg.com', 'ops@savino.com']);
    assert.equal(first.body.requests.length, 2);
    assert.equal(first.body.requests[0].state, 'open');
    assert.match(first.body.requests[0].url, /\/freight\//);
    // The audit trail never holds the token.
    assert.doesNotMatch(JSON.stringify(h.db.audits), /[a-f0-9]{48}/);

    const again = await h.call('POST', '/api/v1/freight-quotes', SEND);
    assert.deepEqual(again.body.results.map(r => r.outcome), ['already_sent', 'already_sent']);
    assert.equal(h.db.sent.length, 2);
    assert.equal(h.db.requests.length, 2);
});

test('send: a Front failure revokes that forwarder\'s link and the other still goes', async () => {
    const h = harness({ frontOk: msg => msg.to[0] !== 'amy@dcg.com' });
    const out = await h.call('POST', '/api/v1/freight-quotes', SEND);
    assert.deepEqual(out.body.results.map(r => r.outcome), ['failed', 'sent']);
    assert.equal(h.db.requests[0].revoke_reason, 'send_failed');
    assert.equal(h.db.requests[1].revoked_at, null);
});

test('send: refusals', async () => {
    assert.equal((await harness({ userType: 'accountant' }).call('POST', '/api/v1/freight-quotes', SEND)).body.code, 'READ_ONLY');
    const h = harness();
    assert.equal((await h.call('POST', '/api/v1/freight-quotes', { ...SEND, services: [] })).body.field, 'services');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes', { ...SEND, forwarderIds: [9] })).body.field, 'forwarderIds');
    assert.equal(h.db.sent.length, 0);
});

test('staff enter an answer; booking an option emails the link and holds the request', async () => {
    const h = harness();
    await answered(h);
    assert.equal(h.db.requests[0].answer_source, 'staff');
    const booked = await h.call('POST', '/api/v1/freight-quotes/options/1/book', { shipmentId: 50, note: 'Please hurry' });
    assert.equal(booked.status, 200);
    assert.equal(booked.body.bookings[0].status, 'requested');
    assert.equal(booked.body.bookings[0].containerName, 'Draft 12');
    assert.equal(booked.body.bookings[0].option.price, 1850);
    assert.equal(booked.body.requests.find(r => r.id === 1).state, 'booked');
    assert.equal(booked.body.requests.find(r => r.id === 2).state, 'answered');
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.match(mail.html, /USD 1,850\.00/);
    assert.match(mail.html, /Please hurry/);
    // One live booking per request; and a booked request takes no new answer.
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', {})).body.code, 'ALREADY_BOOKED');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [OPTION] })).body.code, 'HAS_BOOKING');
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/1/revoke')).body.code, 'HAS_BOOKING');
});

test('booking: an option from an older answer is refused; a Front failure cancels the booking', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/1/options', { options: [{ ...OPTION, price: '1900' }] }); // answer 2
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/1/book', {})).body.code, 'STALE_OPTION');

    const f = harness({ frontOk: msg => !/Confirm or decline/.test(msg.html) });
    await answered(f);
    const out = await f.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.equal(out.status, 502);
    assert.equal(f.db.bookings[0].status, 'cancelled');
    assert.equal(f.db.bookings[0].cancel_reason, 'send_failed');
});

test('a confirmation closes the other forwarder\'s link as filled; cancelling reopens it', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    // Staff mark it for a forwarder who confirmed by phone.
    const confirmed = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: ' BK-77 ' });
    assert.equal(confirmed.status, 200);
    assert.equal(confirmed.body.bookings[0].status, 'confirmed');
    assert.equal(confirmed.body.bookings[0].bookingRef, 'BK-77');
    assert.equal(confirmed.body.bookings[0].responseSource, 'staff');
    const other = confirmed.body.requests.find(r => r.id === 2);
    assert.equal(other.state, 'revoked');
    assert.equal(other.revokeReason, 'filled');
    assert.equal(other.url, null);

    const cancelled = await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'cancelled' });
    assert.equal(cancelled.body.bookings[0].status, 'cancelled');
    assert.equal(cancelled.body.requests.find(r => r.id === 1).state, 'answered');
    assert.equal(cancelled.body.requests.find(r => r.id === 2).state, 'answered');
    // And the next forwarder can be booked at once, with no new request.
    assert.equal((await h.call('POST', '/api/v1/freight-quotes/options/2/book', {})).status, 200);
    assert.equal((await h.call('PATCH', '/api/v1/freight-bookings/1', { status: 'confirmed', bookingRef: 'X' })).body.code, 'NOT_LIVE');
});

test('a forwarder\'s own confirmation (written by the portal) is picked up on the next read', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    Object.assign(h.db.bookings[0], { status: 'confirmed', booking_ref: 'BK-1', response_source: 'link' });
    const board = await h.call('GET', '/api/v1/freight-quotes');
    assert.equal(board.body.requests.find(r => r.id === 2).revokeReason, 'filled');
});

test('contents: preview by supplier, the email, the snapshot, and the claim that stops a second send', async () => {
    const h = harness();
    await answered(h);
    await h.call('POST', '/api/v1/freight-quotes/options/1/book', {});
    assert.equal((await h.call('GET', '/api/v1/freight-bookings/1/contents')).body.code, 'NO_CONTAINER');
    const attached = await h.call('PATCH', '/api/v1/freight-bookings/1', { shipmentId: 50 });
    const rowVersion = attached.body.bookings[0].rowVersion;

    const preview = await h.call('GET', '/api/v1/freight-bookings/1/contents');
    assert.equal(preview.body.containerName, 'Draft 12');
    assert.equal(preview.body.groups[0].supplier, 'Acme');
    assert.equal(preview.body.changedSinceSent, null);

    const body = { rowVersion, attachDocuments: false };
    const sent = await h.call('POST', '/api/v1/freight-bookings/1/contents-email', body);
    assert.equal(sent.status, 200);
    const mail = h.db.sent[h.db.sent.length - 1];
    assert.match(mail.html, /Acme/);
    assert.match(mail.html, /Subtotal/);
    assert.equal(sent.body.bookings[0].contentsChanged, false);
    assert.ok(sent.body.bookings[0].contentsSentAt);
    // The same call again (a retry of one that did send) is refused, not sent twice.
    const mails = h.db.sent.length;
    const retry = await h.call('POST', '/api/v1/freight-bookings/1/contents-email', body);
    assert.equal(retry.status, 409);
    assert.equal(retry.body.code, 'STALE');
    assert.equal(h.db.sent.length, mails);
    assert.equal((await h.call('POST', '/api/v1/freight-bookings/1/contents-email', {})).body.field, 'rowVersion');
});
