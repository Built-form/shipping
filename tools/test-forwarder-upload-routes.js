'use strict';

// A forwarder's files (docs/forwarder-containers.md, slice C): the routes that
// accept or send back an invoice, a loading photo or a bill of lading a
// forwarder uploaded on the supplier portal (src/services/forwarder-upload-routes.js),
// against a scripted connection and a scripted S3: what is read and written
// where, in which order, and what is never written when a request is refused.
// No database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { registerForwarderUploadRoutes, UPLOAD_REFUSAL_TEXT } = require('../src/services/forwarder-upload-routes');

process.env.PI_UPLOADS_BUCKET = 'portal-bucket';
const PDF = Buffer.from('%PDF-1.7\ninvoice');
const JPG = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0x10, 1, 2, 3]);
const FINAL_KEY = `files/fwd/70/${'a'.repeat(32)}.pdf`;
const UUID_KEY = /^[a-z-]+\/[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\/[A-Za-z0-9._-]+$/;
const err = (errno) => Object.assign(new Error(`errno ${errno}`), { errno });

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
            throw new Error(`unscripted statement: ${text.slice(0, 150)}`);
        },
        beginTransaction: async () => { asked.push({ sql: 'BEGIN', params: [] }); },
        commit: async () => { asked.push({ sql: 'COMMIT', params: [] }); },
        rollback: async () => { asked.push({ sql: 'ROLLBACK', params: [] }); },
    };
    return { conn, asked, wrote: pattern => asked.filter(a => pattern.test(a.sql)) };
}

const UPLOAD = (over = {}) => ({
    id: 70, shipment_id: 1214, container_ref: '339', forwarder_id: 4, kind: 'invoice', user_email: 'mei@fwd.example', user_name: 'Mei',
    filename: 'invoice 339.pdf', content_type: 'application/pdf', file_size: PDF.length, final_key: FINAL_KEY, s3_etag: '"e1"', note: null,
    upload_status: 'sent', review_status: 'open', decided_note: null, decided_by: null, filed_key: null, filed_id: null,
    sent_at_iso: '2026-11-23T09:01:00Z', decided_at_iso: null, withdrawn_at_iso: null, forwarder_name: 'test2forwarder', theirs: 1,
    // What the portal's view says its forwarder holds on the container (2026-11-24): the freight.
    does_freight: 1, does_delivery: 0, does_customs: 0,
    ...over,
});
const SHIPMENT = (over = {}) => ({
    id: 1214, reference: '339', mode: 'SEA', stage: 'BOOKED', deleted_at: null, merged_into_id: null, forwarder_id: 4, forwarder: 'test2forwarder',
    delivery_forwarder_id: null, customs_forwarder_id: null, ...over,
});

// `row`: the upload as it stands; a decision's own writes change it, so a later read sees them.
function build({ row = UPLOAD(), shipment = SHIPMENT(), bookings = [], forwarders = [{ id: 3, name: 'DCG', deleted_at: null }, { id: 4, name: 'test2forwarder', deleted_at: null }],
    payees = [], onBoard = ['Ningbo Medical Co'], duplicates = [], object = { bytes: PDF }, insertError = null, filed = null, userType = 'standard', documents = [], rules = [] } = {}) {
    const routes = {};
    const app = Object.fromEntries(['get', 'post', 'delete', 'patch'].map(m => [m, (path, fn) => { routes[`${m.toUpperCase()} ${path}`] = fn; }]));
    const state = { row: row ? { ...row } : null };
    const db = scripted([
        ...rules,
        [/FROM `jportal`\.forwarder_uploads u .*WHERE u\.id = \? FOR UPDATE/, () => [state.row ? [{ ...state.row }] : []]],
        [/FROM `jportal`\.forwarder_uploads u .*WHERE u\.id = \?$/, () => [state.row ? [{ ...state.row }] : []]],
        [/^UPDATE `jportal`\.forwarder_uploads SET filed_key = \?/, (p) => { if (state.row && state.row.filed_key == null) state.row.filed_key = p[0]; return [{ affectedRows: 1 }]; }],
        [/^UPDATE `jportal`\.forwarder_uploads SET review_status = 'received'/, (p) => { Object.assign(state.row, { review_status: 'received', decided_by: p[0], filed_id: p[1], decided_at_iso: '2026-11-23T12:00:00Z' }); return [{ affectedRows: 1 }]; }],
        [/^UPDATE `jportal`\.forwarder_uploads SET review_status = 'rejected'/, (p) => { Object.assign(state.row, { review_status: 'rejected', decided_note: p[0], decided_by: p[1], decided_at_iso: '2026-11-23T12:00:00Z' }); return [{ affectedRows: 1 }]; }],
        [/FROM shipments WHERE id = \? FOR UPDATE/, () => [shipment ? [shipment] : []]],
        [/FROM freight_bookings fb JOIN freight_quote_requests fr .* FOR UPDATE OF fb/, () => [bookings]],
        [/FROM forwarders WHERE id IN/, () => [forwarders]],
        [/FROM payment_extras ex WHERE ex\.shipment_id = \? AND ex\.forwarder_id = \?/, () => [payees.map(supplier_name => ({ supplier_name }))]],
        [/^SELECT DISTINCT o\.supplier AS name FROM orders o/, () => [onBoard.map(name => ({ name }))]],
        [/^SELECT id FROM shipment_payment_documents WHERE deleted_at IS NULL AND filename = \? AND file_size = \? AND shipment_id = \?/, () => [duplicates.map(id => ({ id }))]],
        [/^INSERT INTO shipment_payment_documents/, () => (insertError || [{ insertId: 900 }])],
        [/^INSERT INTO container_photos/, () => (insertError || [{ insertId: 901 }])],
        [/^INSERT INTO shipment_forwarder_documents/, () => (insertError || [{ insertId: 902 }])],
        [/^SELECT \* FROM shipment_payment_documents WHERE (id|forwarder_upload_id) = \?/, () => [filed === 'deleted' ? [] : [{ id: 900, filename: 'invoice 339.pdf', deleted_at: null }]]],
        [/^SELECT id, filename, deleted_at FROM container_photos WHERE (id|forwarder_upload_id) = \?/, () => [[{ id: 901, filename: 'door.jpg', deleted_at: null }]]],
        [/FROM shipment_forwarder_documents d WHERE d\.(id|forwarder_upload_id) = \?/, () => [documents.length ? documents : [{ id: 902, shipment_id: 1214, filename: 'bl.pdf', accepted_by_email: 'ops@us.com', deleted_at: null }]]],
        [/^UPDATE shipment_forwarder_documents SET deleted_at/, () => [{ affectedRows: 1 }]],
    ]);
    const s3Calls = [];
    const s3 = {
        send: async (cmd) => {
            const kind = cmd.constructor.name;
            s3Calls.push({ kind, input: cmd.input });
            if (kind === 'GetObjectCommand') {
                if (!object) throw Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey' });
                return { ContentLength: object.length ?? object.bytes.length, Body: { transformToByteArray: async () => object.bytes, destroy() {} } };
            }
            if (kind === 'PutObjectCommand') return {};
            throw new Error(`unexpected S3 command ${kind}`);
        },
    };
    const audits = [];
    const extracted = [];
    const signedFor = [];
    registerForwarderUploadRoutes(app, {
        withConnection: fn => fn(db.conn),
        ready: async () => {},
        s3, poBucket: 'po-bucket', publicS3Url: key => `https://po.example/${key}`,
        recordAudit: async (conn, entry) => { audits.push(entry); },
        startExtraction: (req, documentId) => { extracted.push(documentId); },
        getSignedUrl: async (client, cmd) => { signedFor.push(cmd.input); return `https://signed.example/${cmd.input.Key}`; },
        documentJson: d => ({ id: d.id, filename: d.filename }),
        log: { error() {}, warn() {} },
        env: {},
    });
    const call = async (route, req = {}) => {
        let status = 200;
        let body = null;
        const res = { headersSent: false, status(s) { status = s; return this; }, json(b) { body = b; this.headersSent = true; return this; } };
        await routes[route]({ query: {}, params: { id: '70' }, body: {}, userEmail: 'ops@us.com', userType, ...req }, res);
        return { status, body };
    };
    return { call, db, s3Calls, audits, extracted, signedFor, state };
}

const ACCEPT = 'POST /api/v1/forwarder-uploads/:id(\\d+)/accept';
const REJECT = 'POST /api/v1/forwarder-uploads/:id(\\d+)/reject';
const FILE = 'GET /api/v1/forwarder-uploads/:id(\\d+)/file';
const DELETE_DOC = 'DELETE /api/v1/forwarder-documents/:id(\\d+)';
const sqlOf = t => t.db.asked.map(a => a.sql);
const refusal = (code, allowedNext, extra = {}) => ({ error: UPLOAD_REFUSAL_TEXT[code], code, allowedNext, ...extra });

// ── accept ───────────────────────────────────────────────────────────────

test('accept: a role that may not decide is refused before anything is read', async () => {
    for (const route of [ACCEPT, REJECT]) {
        const t = build({ userType: 'accountant' });
        const r = await t.call(route, { body: { note: 'Wrong container' } });
        assert.equal(r.status, 403, route);
        assert.equal(r.body.code, 'CANNOT_DECIDE');
        assert.equal(t.db.asked.length, 0);
        assert.deepEqual(t.s3Calls, []);
    }
});

test('accept an invoice: copied to our bucket at a key made once, filed under the forwarder\'s name with its id, the upload marked, and then read', async () => {
    const t = build({ payees: ['DCG Logistics Ltd'] });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.filed, { as: 'invoice', id: 900, document: { id: 900, filename: 'invoice 339.pdf' } });
    assert.equal(r.body.replayed, false);
    assert.deepEqual([r.body.upload.id, r.body.upload.state, r.body.upload.decidedBy, r.body.upload.filedId], [70, 'received', 'ops@us.com', 900]);
    // The file: read from the portal's bucket BY ITS KEY, with no ETag condition, and written to ours.
    assert.deepEqual(t.s3Calls.map(c => c.kind), ['GetObjectCommand', 'PutObjectCommand']);
    assert.deepEqual(t.s3Calls[0].input, { Bucket: 'portal-bucket', Key: FINAL_KEY });
    const put = t.s3Calls[1].input;
    assert.equal(put.Bucket, 'po-bucket');
    assert.match(put.Key, /^shipment-payments\/[0-9a-f-]{36}\/invoice_339\.pdf$/);
    assert.match(put.Key, UUID_KEY);
    assert.equal(put.ContentType, 'application/pdf');
    assert.ok(put.Body.equals(PDF));
    assert.equal(t.state.row.filed_key, put.Key);
    // Filed as a balance invoice on the shipment, under the name the forwarder is paid under, with its id.
    const ins = t.db.wrote(/^INSERT INTO shipment_payment_documents/)[0];
    assert.match(ins.sql, /\(shipment_id, shipment_reference, supplier_name, supplier_key, payment_id, doc_kind, filename, s3_key, public_url, content_type, file_size, notes, extract_status, uploaded_by_email, forwarder_upload_id, forwarder_id\)/);
    assert.deepEqual(ins.params.slice(0, 4), [1214, '339', 'DCG Logistics Ltd', 'dcg logistics ltd']);
    assert.deepEqual(ins.params.slice(4), ['invoice 339.pdf', put.Key, `https://po.example/${put.Key}`, 'application/pdf', PDF.length,
        'Uploaded by the forwarder on the supplier portal (forwarder upload 70).', 'mei@fwd.example', 70, 4]);
    const mark = t.db.wrote(/SET review_status = 'received'/)[0];
    assert.deepEqual(mark.params, ['ops@us.com', 900, 70]);
    assert.match(mark.sql, /WHERE id = \? AND upload_status = 'sent' AND review_status = 'open'$/);
    assert.deepEqual(t.audits.map(a => [a.entityType, a.entityId, a.action]), [['shipment_payment_document', 900, 'create'], ['forwarder_upload', 70, 'accept']]);
    // Its reading starts only once everything is committed.
    assert.deepEqual(t.extracted, [900]);
    assert.ok(sqlOf(t).lastIndexOf('COMMIT') > sqlOf(t).findIndex(s => /^INSERT INTO shipment_payment_documents/.test(s)));
});

test('accept an invoice: with no payment to that forwarder on the container, it is filed under its name on the Forwarders list', async () => {
    const t = build();
    await t.call(ACCEPT);
    assert.deepEqual(t.db.wrote(/^INSERT INTO shipment_payment_documents/)[0].params.slice(2, 4), ['test2forwarder', 'test2forwarder']);
});

test('accept: the decision is made under the locks, and whose container it is, is read there', async () => {
    const t = build();
    await t.call(ACCEPT);
    const sql = sqlOf(t);
    const begin = sql.lastIndexOf('BEGIN');
    const order = sql.slice(begin + 1, begin + 5).map(s => (/FROM (?:`jportal`\.)?(\w+)/.exec(s) || [])[1]);
    assert.deepEqual(order, ['shipments', 'freight_bookings', 'forwarders', 'forwarder_uploads']);
    for (const s of sql.slice(begin + 1, begin + 5)) assert.match(s, /FOR (UPDATE|SHARE)/);
    // The copy is made before that transaction, never inside it.
    assert.equal(t.s3Calls.length, 2);
});

test('accept: refused, with nothing filed, when the container is no longer that forwarder\'s', async () => {
    for (const shipment of [SHIPMENT({ forwarder_id: 3, forwarder: 'DCG' }), SHIPMENT({ deleted_at: new Date() }), SHIPMENT({ forwarder_id: null, forwarder: null })]) {
        const t = build({ shipment });
        const r = await t.call(ACCEPT);
        assert.deepEqual(r, { status: 409, body: refusal('NOT_THEIR_CONTAINER', ['reject']) });
        assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
        assert.equal(t.db.wrote(/SET review_status/).length, 0);
        assert.deepEqual(t.extracted, []);
    }
    // Said by the portal's own view before anything is copied: it has no row for that forwarder.
    const t = build({ row: UPLOAD({ theirs: 0, does_freight: null, does_delivery: null, does_customs: null }) });
    assert.equal((await t.call(ACCEPT)).body.code, 'NOT_THEIR_CONTAINER');
    assert.deepEqual(t.s3Calls, []);
});

// ── several forwarders on one container, by duty (2026-11-24) ────────────

test('accept: a file is taken only from the company that holds its kind\'s duty; the delivery company\'s loading photo is refused', async () => {
    // Freight is forwarder 4's (picked), delivery is forwarder 3's.
    const shipment = SHIPMENT({ delivery_forwarder_id: 3 });
    const photo = UPLOAD({ forwarder_id: 3, forwarder_name: 'DCG', kind: 'loading_photo', filename: 'door.jpg', content_type: 'image/jpeg', file_size: JPG.length });
    // The view already says it: nothing is copied.
    let t = build({ shipment, row: { ...photo, does_freight: 0, does_delivery: 1 } });
    let r = await t.call(ACCEPT);
    assert.deepEqual(r, { status: 409, body: refusal('NOT_THEIR_DUTY', ['reject'], { kind: 'loading_photo' }) });
    assert.deepEqual(t.s3Calls, []);
    // The view said freight when the page was read, and the pick moved since: refused under the locks, nothing filed.
    t = build({ shipment, row: { ...photo, does_freight: 1 }, object: { bytes: JPG } });
    r = await t.call(ACCEPT);
    assert.deepEqual(r, { status: 409, body: refusal('NOT_THEIR_DUTY', ['reject'], { kind: 'loading_photo' }) });
    assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
    assert.equal(t.db.wrote(/SET review_status/).length, 0);
    // It can still be sent back.
    r = await build({ shipment, row: photo }).call(REJECT, { body: { note: 'Not yours to send' } });
    assert.equal(r.status, 200);
});

// ── the draft customs entry (2026-11-24) ─────────────────────────────────
//
// Forwarder 4 carries the freight; forwarder 3 (DCG) handles customs.

const CUSTOMS_SHIPMENT = (over = {}) => SHIPMENT({ customs_forwarder_id: 3, ...over });
const DRAFT = (over = {}) => UPLOAD({
    id: 70, forwarder_id: 3, forwarder_name: 'DCG', kind: 'customs_draft', filename: 'entry draft.pdf',
    does_freight: 0, does_delivery: 0, does_customs: 1, ...over,
});

test('accept a draft customs entry: approved, it is kept with the container\'s forwarder documents as its own kind', async () => {
    const t = build({ shipment: CUSTOMS_SHIPMENT(), row: DRAFT() });
    const r = await t.call(ACCEPT, { body: { kind: 'customs_draft' } });
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.filed.as, 'document');
    const [insert] = t.db.wrote(/^INSERT INTO shipment_forwarder_documents/);
    // shipment, reference, forwarder id, forwarder name, kind, filename, key, ...
    assert.deepEqual(insert.params.slice(0, 6), [1214, '339', 3, 'DCG', 'customs_draft', 'entry draft.pdf']);
    assert.match(insert.params[6], /^forwarder-documents\/[0-9a-f-]{36}\/entry_draft\.pdf$/);
    assert.equal(t.db.wrote(/SET review_status = 'received'/).length, 1);
    const audit = t.audits.find(a => a.entityType === 'shipment_forwarder_document');
    assert.equal(audit.after.kind, 'customs_draft');
    assert.equal(audit.after.forwarderId, 3);
    // A bill of lading is still filed as a bill of lading.
    const bl = build({ row: UPLOAD({ kind: 'bill_of_lading' }) });
    await bl.call(ACCEPT);
    assert.equal(bl.db.wrote(/^INSERT INTO shipment_forwarder_documents/)[0].params[4], 'bill_of_lading');
});

test('a draft customs entry is decided only by a ShipLine that names its kind: an older build can neither accept it nor send it back', async () => {
    for (const body of [{}, { kind: 'bill_of_lading' }, { preset: 'Proof of loading' }]) {
        const t = build({ shipment: CUSTOMS_SHIPMENT(), row: DRAFT() });
        const r = await t.call(ACCEPT, { body });
        assert.deepEqual(r, { status: 409, body: refusal('CLIENT_TOO_OLD', ['reload']) }, JSON.stringify(body));
        assert.deepEqual(t.s3Calls, [], 'nothing is copied');
        assert.equal(t.db.wrote(/^INSERT INTO|^UPDATE/).length, 0);
    }
    let t = build({ shipment: CUSTOMS_SHIPMENT(), row: DRAFT() });
    let r = await t.call(REJECT, { body: { note: 'The commodity code is wrong' } });
    assert.deepEqual(r, { status: 409, body: refusal('CLIENT_TOO_OLD', ['reload']) });
    assert.equal(t.db.wrote(/SET review_status/).length, 0);
    t = build({ shipment: CUSTOMS_SHIPMENT(), row: DRAFT() });
    r = await t.call(REJECT, { body: { note: 'The commodity code is wrong', kind: 'customs_draft' } });
    assert.equal(r.status, 200);
    assert.equal(t.state.row.review_status, 'rejected');
    // The original three kinds are decided exactly as before, with no kind named.
    assert.equal((await build().call(REJECT, { body: { note: 'Wrong container' } })).status, 200);
});

test('a draft customs entry is taken only from the company that handles customs', async () => {
    // Sent by the freight forwarder: it does not handle customs here.
    let t = build({ shipment: CUSTOMS_SHIPMENT(), row: DRAFT({ forwarder_id: 4, forwarder_name: 'test2forwarder', does_freight: 1, does_customs: 0 }) });
    let r = await t.call(ACCEPT, { body: { kind: 'customs_draft' } });
    assert.deepEqual(r, { status: 409, body: refusal('NOT_THEIR_DUTY', ['reject'], { kind: 'customs_draft' }) });
    assert.deepEqual(t.s3Calls, []);
    // Customs was handed to another company after the page was read: refused under the locks, nothing filed.
    t = build({ shipment: SHIPMENT({ customs_forwarder_id: 4 }), row: DRAFT() });
    r = await t.call(ACCEPT, { body: { kind: 'customs_draft' } });
    assert.deepEqual(r, { status: 409, body: refusal('NOT_THEIR_CONTAINER', ['reject']) });
    assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
});

test('accept an invoice from the company that delivers: filed under ITS name and id, not the freight forwarder\'s', async () => {
    const shipment = SHIPMENT({ delivery_forwarder_id: 3 });
    const t = build({ shipment, row: UPLOAD({ forwarder_id: 3, forwarder_name: 'DCG', does_freight: 0, does_delivery: 1 }) });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    const [insert] = t.db.wrote(/^INSERT INTO shipment_payment_documents/);
    assert.equal(insert.params[2], 'DCG', 'the name it is filed under');
    assert.equal(insert.params[insert.params.length - 1], 3, 'the forwarder id on the document');
    // The payee names looked up are that forwarder's on that container.
    assert.deepEqual(t.db.wrote(/FROM payment_extras ex WHERE ex\.shipment_id = \? AND ex\.forwarder_id = \?/)[0].params, [1214, 3]);
    // Every forwarder the container's rows name is locked with the decision: both picks.
    const locked = t.db.wrote(/FROM forwarders WHERE id IN \(\?\) ORDER BY id FOR SHARE/).pop();
    assert.deepEqual([...locked.params[0]].sort(), [3, 4]);
});

test('accept: an upload that was withdrawn, never sent, or already sent back is refused before anything is copied', async () => {
    const cases = [
        [UPLOAD({ upload_status: 'withdrawn' }), 'WITHDRAWN'],
        [UPLOAD({ upload_status: 'draft', final_key: null }), 'NOT_SENT'],
        [UPLOAD({ review_status: 'rejected' }), 'ALREADY_DECIDED'],
    ];
    for (const [row, code] of cases) {
        const t = build({ row });
        const r = await t.call(ACCEPT);
        assert.deepEqual(r, { status: 409, body: refusal(code, ['reload']) }, code);
        assert.deepEqual(t.s3Calls, []);
        assert.equal(t.db.wrote(/^(INSERT|UPDATE)/).length, 0);
    }
    const none = build({ row: null });
    assert.deepEqual(await none.call(ACCEPT), { status: 404, body: refusal('NO_SUCH_UPLOAD', ['reload']) });
});

test('accept: asked again for one already received, it answers what was filed and touches nothing', async () => {
    const t = build({ row: UPLOAD({ review_status: 'received', decided_by: 'kim@us.com', filed_id: 900, filed_key: 'shipment-payments/x/invoice_339.pdf' }) });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.equal(r.body.replayed, true);
    assert.deepEqual(r.body.filed, { as: 'invoice', id: 900, document: { id: 900, filename: 'invoice 339.pdf' } });
    assert.deepEqual(t.s3Calls, []);
    assert.equal(t.db.wrote(/^(INSERT|UPDATE)/).length, 0);
    assert.deepEqual(t.extracted, []);
    // What it was filed as has been deleted since: said, not hidden.
    const gone = build({ row: UPLOAD({ review_status: 'received', filed_id: 900 }), filed: 'deleted' });
    const again = await gone.call(ACCEPT);
    assert.deepEqual([again.status, again.body.filed, again.body.replayed], [200, null, true]);
});

test('accept: the bytes must be the file the portal recorded', async () => {
    const cases = [
        ['a picture where the row says PDF', { bytes: Buffer.concat([JPG, Buffer.alloc(PDF.length - JPG.length)]) }, 'FILE_NOT_ITS_TYPE'],
        ['another size', { bytes: Buffer.concat([PDF, Buffer.from('x')]) }, 'FILE_SIZE_CHANGED'],
        ['gone from the portal', null, 'FILE_UNAVAILABLE'],
    ];
    for (const [name, object, code] of cases) {
        const t = build({ object });
        const r = await t.call(ACCEPT);
        assert.deepEqual(r, { status: 409, body: refusal(code, ['reject']) }, name);
        assert.equal(t.s3Calls.some(c => c.kind === 'PutObjectCommand'), false, name);
        assert.equal(t.db.wrote(/^INSERT INTO/).length, 0, name);
        assert.equal(t.db.wrote(/SET review_status/).length, 0, name);
    }
    // Far too large: refused on its length, without reading it.
    let read = false;
    const big = build({ object: { length: 11 * 1024 * 1024, get bytes() { read = true; return PDF; } } });
    assert.equal((await big.call(ACCEPT)).body.code, 'FILE_TOO_LARGE');
    assert.equal(read, false);
});

test('accept: the key of our copy is made once and kept, so a second try writes over its own copy', async () => {
    const kept = 'shipment-payments/3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f/invoice_339.pdf';
    const t = build({ row: UPLOAD({ filed_key: kept }) });
    await t.call(ACCEPT);
    assert.equal(t.s3Calls[1].input.Key, kept);
    assert.equal(t.db.wrote(/SET filed_key = \?/).length, 0);
    assert.equal(t.db.wrote(/^INSERT INTO shipment_payment_documents/)[0].params[5], kept);
});

test('accept: a file replaced on the portal\'s row between the copy and the decision is refused', async () => {
    let n = 0;
    const moved = [/FROM `jportal`\.forwarder_uploads u .*WHERE u\.id = \? FOR UPDATE/, () => { n += 1; return [[UPLOAD({ filed_key: 'shipment-payments/3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f/invoice_339.pdf', ...(n >= 2 ? { final_key: `files/fwd/70/${'b'.repeat(32)}.pdf` } : {}) })]]; }];
    const t = build({ rules: [moved] });
    const r = await t.call(ACCEPT);
    assert.deepEqual(r, { status: 409, body: refusal('FILE_CHANGED', ['reload']) });
    assert.equal(t.db.wrote(/^INSERT INTO shipment_payment_documents/).length, 0);
    assert.equal(sqlOf(t).at(-1), 'ROLLBACK');
});

test('accept an invoice: refused under a name that is a supplier\'s with goods on board, and when the same file is on the shipment already', async () => {
    let t = build({ payees: ['Ningbo Medical Co'], onBoard: ['Ningbo Medical Co'] });
    let r = await t.call(ACCEPT);
    assert.deepEqual(r, { status: 409, body: refusal('FILING_NAME_CLASH', ['reject'], { name: 'Ningbo Medical Co' }) });
    assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
    assert.deepEqual(t.extracted, []);
    t = build({ duplicates: [55] });
    r = await t.call(ACCEPT);
    assert.deepEqual(r, { status: 409, body: refusal('DUPLICATE_DOCUMENT', ['reject'], { existingId: 55 }) });
    assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
    assert.equal(sqlOf(t).at(-1), 'ROLLBACK');
});

test('accept: filed by a request that ran beside this one: theirs stands, and both are told the same', async () => {
    const t = build({ insertError: err(1062) });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.equal(r.body.replayed, true);
    assert.deepEqual(r.body.filed, { as: 'invoice', id: 900, document: { id: 900, filename: 'invoice 339.pdf' } });
    assert.equal(t.db.wrote(/SET review_status = 'received'/).length, 0);
    assert.deepEqual(t.extracted, []);
});

test('accept: received by a request that held the locks first: this one is told what that one filed, and files nothing', async () => {
    // On a real server the row's lock serialises two accepts: the second never meets a
    // duplicate key, it meets the row already received once it has the locks.
    let t = null;
    t = build({ rules: [[/FROM shipments WHERE id = \? FOR UPDATE/, () => {
        Object.assign(t.state.row, { review_status: 'received', decided_by: 'someone.else@us.com', filed_id: 900, decided_at_iso: '2026-11-23T12:00:00Z' });
        return [[SHIPMENT()]];
    }]] });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.equal(r.body.replayed, true);
    assert.deepEqual(r.body.filed, { as: 'invoice', id: 900, document: { id: 900, filename: 'invoice 339.pdf' } });
    assert.equal(r.body.upload.state, 'received');
    assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
    assert.equal(t.db.wrote(/SET review_status = 'received'/).length, 0);
    assert.deepEqual(t.audits, []);
    assert.deepEqual(t.extracted, []);
});

test('accept: taken back, or received by another request, between the first read and the copy: nothing is copied for it', async () => {
    // The first read is a plain one; the row is read again, locked, before our copy is made.
    const UNDER_LOCK = /FROM `jportal`\.forwarder_uploads u .*WHERE u\.id = \? FOR UPDATE/;
    let t = null;
    t = build({ rules: [[UNDER_LOCK, () => { Object.assign(t.state.row, { upload_status: 'withdrawn', withdrawn_at_iso: '2026-11-23T12:00:00Z' }); return [[{ ...t.state.row }]]; }]] });
    let r = await t.call(ACCEPT);
    assert.equal(r.status, 409);
    assert.deepEqual(r.body, refusal('WITHDRAWN', ['reload']));
    assert.deepEqual(t.s3Calls, []);
    assert.equal(t.db.wrote(/SET filed_key/).length, 0);

    t = build({ rules: [[UNDER_LOCK, () => { Object.assign(t.state.row, { review_status: 'received', decided_by: 'someone.else@us.com', filed_id: 900 }); return [[{ ...t.state.row }]]; }]] });
    r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.equal(r.body.replayed, true);
    assert.deepEqual(r.body.filed, { as: 'invoice', id: 900, document: { id: 900, filename: 'invoice 339.pdf' } });
    assert.deepEqual(t.s3Calls, []);
    assert.equal(t.db.wrote(/^INSERT INTO/).length, 0);
    assert.deepEqual(t.audits, []);
});

test('accept a loading photo: with the container\'s photos, under its present number, as proof of loading', async () => {
    const photo = UPLOAD({ kind: 'loading_photo', filename: 'door.jpg', content_type: 'image/jpeg', file_size: JPG.length, final_key: `files/fwd/70/${'a'.repeat(32)}.jpg`, container_ref: '338', note: 'Left door' });
    const t = build({ row: photo, object: { bytes: JPG }, shipment: SHIPMENT({ reference: '339' }) });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.filed, { as: 'photo', id: 901 });
    const put = t.s3Calls[1].input;
    assert.match(put.Key, /^container-photos\/[0-9a-f-]{36}\/door\.jpg$/);
    const ins = t.db.wrote(/^INSERT INTO container_photos/)[0];
    assert.match(ins.sql, /\(container_kind, container_number, draft_container_id, container_name, filename, s3_key, content_type, file_size, preset, description, uploaded_by_email, forwarder_upload_id\)/);
    // The number it has NOW (339), not the one it was sent for (338).
    assert.deepEqual(ins.params, ['booked', '339', null, null, 'door.jpg', put.Key, 'image/jpeg', JPG.length, 'Proof of loading', 'Left door', 'mei@fwd.example', 70]);
    assert.deepEqual(t.extracted, []);
    assert.deepEqual(t.audits.map(a => [a.entityType, a.action]), [['container_photo', 'create'], ['forwarder_upload', 'accept']]);
    // The number it was sent for stays in the audit.
    assert.equal(t.audits[1].after.containerRefWhenSent, '338');
});

test('accept a loading photo: another preset can be named, and its description is the preset when the forwarder wrote no note', async () => {
    const photo = UPLOAD({ kind: 'loading_photo', filename: 'door.jpg', content_type: 'image/jpeg', file_size: JPG.length });
    const t = build({ row: photo, object: { bytes: JPG } });
    await t.call(ACCEPT, { body: { preset: '  Seal number  ' } });
    assert.deepEqual(t.db.wrote(/^INSERT INTO container_photos/)[0].params.slice(8, 10), ['Seal number', 'Seal number']);
});

test('accept a bill of lading: kept with the container\'s forwarder documents', async () => {
    const bl = UPLOAD({ kind: 'bill_of_lading', filename: 'B/L 339.pdf' });
    const t = build({ row: bl });
    const r = await t.call(ACCEPT);
    assert.equal(r.status, 200);
    assert.equal(r.body.filed.as, 'document');
    assert.equal(r.body.filed.id, 902);
    const put = t.s3Calls[1].input;
    assert.match(put.Key, /^forwarder-documents\/[0-9a-f-]{36}\/B_L_339\.pdf$/);
    const ins = t.db.wrote(/^INSERT INTO shipment_forwarder_documents/)[0];
    assert.match(ins.sql, /\(shipment_id, shipment_reference, forwarder_id, forwarder_name, kind, filename, s3_key, public_url, content_type, file_size, forwarder_upload_id, uploaded_by_email, accepted_by_email\)/);
    assert.deepEqual(ins.params, [1214, '339', 4, 'test2forwarder', 'bill_of_lading', 'B/L 339.pdf', put.Key, `https://po.example/${put.Key}`, 'application/pdf', PDF.length, 70, 'mei@fwd.example', 'ops@us.com']);
    // Never the B/L number on the shipment: that drives a payment flag.
    assert.equal(sqlOf(t).some(s => /bl_number/.test(s)), false);
});

// ── send back ────────────────────────────────────────────────────────────

test('send back: needs a note the forwarder can read', async () => {
    for (const note of [undefined, '', ' no ', 'x'.repeat(501)]) {
        const t = build();
        const r = await t.call(REJECT, { body: { note } });
        assert.equal(r.status, 400);
        assert.equal(r.body.code, 'NOTE_REQUIRED');
        assert.equal(t.db.asked.length, 0);
    }
});

test('send back: the note goes on the upload, with who decided; nothing is copied or filed', async () => {
    const t = build();
    const r = await t.call(REJECT, { body: { note: '  It is for another container.  ' } });
    assert.equal(r.status, 200);
    assert.deepEqual([r.body.upload.state, r.body.upload.decidedNote, r.body.upload.decidedBy], ['rejected', 'It is for another container.', 'ops@us.com']);
    const w = t.db.wrote(/SET review_status = 'rejected'/)[0];
    assert.deepEqual(w.params, ['It is for another container.', 'ops@us.com', 70]);
    assert.match(w.sql, /WHERE id = \? AND upload_status = 'sent' AND review_status = 'open'$/);
    assert.deepEqual(t.s3Calls, []);
    assert.deepEqual(t.audits.map(a => [a.entityType, a.entityId, a.action, a.after.note]), [['forwarder_upload', 70, 'reject', 'It is for another container.']]);
});

test('send back: one whose container is no longer its forwarder\'s can still be dismissed; one decided or withdrawn cannot', async () => {
    const stray = build({ row: UPLOAD({ theirs: 0 }), shipment: SHIPMENT({ forwarder_id: 3 }) });
    assert.equal((await stray.call(REJECT, { body: { note: 'No longer yours' } })).status, 200);
    assert.equal(stray.db.wrote(/FROM shipments WHERE id = \? FOR UPDATE/).length, 0);
    for (const [row, code] of [[UPLOAD({ review_status: 'received' }), 'ALREADY_DECIDED'], [UPLOAD({ upload_status: 'withdrawn' }), 'WITHDRAWN']]) {
        const t = build({ row });
        assert.deepEqual(await t.call(REJECT, { body: { note: 'Wrong file' } }), { status: 409, body: refusal(code, ['reload']) });
        assert.equal(t.db.wrote(/SET review_status/).length, 0);
    }
});

// ── the file, and a filed bill of lading ─────────────────────────────────

test('file: a five-minute link to the forwarder\'s own file in the portal\'s bucket', async () => {
    const t = build();
    const r = await t.call(FILE);
    assert.deepEqual(r, { status: 200, body: { url: `https://signed.example/${FINAL_KEY}`, filename: 'invoice 339.pdf', contentType: 'application/pdf', expiresIn: 300 } });
    assert.deepEqual([t.signedFor[0].Bucket, t.signedFor[0].Key, t.signedFor[0].ResponseContentType], ['portal-bucket', FINAL_KEY, 'application/pdf']);
    for (const row of [UPLOAD({ upload_status: 'draft', final_key: null }), null]) {
        assert.equal((await build({ row }).call(FILE)).status, 404);
    }
});

test('a filed bill of lading is removed by an admin or by whoever accepted it, and by nobody else', async () => {
    const doc = { id: 902, shipment_id: 1214, filename: 'bl.pdf', accepted_by_email: 'kim@us.com', deleted_at: null };
    let t = build({ documents: [doc], userType: 'admin' });
    let r = await t.call(DELETE_DOC, { params: { id: '902' } });
    assert.deepEqual(r, { status: 200, body: { ok: true, id: 902 } });
    assert.deepEqual(t.db.wrote(/^UPDATE shipment_forwarder_documents SET deleted_at/)[0].params, ['ops@us.com', 902]);
    assert.deepEqual(t.audits.map(a => [a.entityType, a.entityId, a.action]), [['shipment_forwarder_document', 902, 'delete']]);

    t = build({ documents: [{ ...doc, accepted_by_email: 'ops@us.com' }] });
    assert.equal((await t.call(DELETE_DOC, { params: { id: '902' } })).status, 200);

    t = build({ documents: [doc] });
    r = await t.call(DELETE_DOC, { params: { id: '902' } });
    assert.deepEqual([r.status, r.body.code], [409, 'NOT_YOURS_TO_REMOVE']);
    assert.equal(t.db.wrote(/^UPDATE shipment_forwarder_documents/).length, 0);

    t = build({ documents: [{ ...doc, deleted_at: new Date() }], userType: 'admin' });
    assert.equal((await t.call(DELETE_DOC, { params: { id: '902' } })).status, 404);
});

test('where the portal\'s table is not on this server, a decision says so', async () => {
    const t = build({ rules: [[/FROM `jportal`\.forwarder_uploads u/, () => err(1146)]] });
    const r = await t.call(ACCEPT);
    assert.deepEqual([r.status, r.body.code], [503, 'PORTAL_UNAVAILABLE']);
});
