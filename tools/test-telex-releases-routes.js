'use strict';

// The telex release routes (src/services/telex-release-routes.js;
// docs/telex-releases.md) against a scripted connection and a scripted S3:
// what is written where, in which order, and what is never written when a
// request is refused. No database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { registerTelexReleaseRoutes } = require('../src/services/telex-release-routes');

process.env.PI_UPLOADS_BUCKET = 'portal-bucket';
const SUPPLIER = 'Acme Medical Co., Ltd';
const PDF = Buffer.from('%PDF-1.7\nrelease');
const EXE = Buffer.from('MZ\x90\x00 program');

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
            throw new Error(`unscripted statement: ${text.slice(0, 120)}`);
        },
        beginTransaction: async () => { asked.push({ sql: 'BEGIN', params: [] }); },
        commit: async () => { asked.push({ sql: 'COMMIT', params: [] }); },
        rollback: async () => { asked.push({ sql: 'ROLLBACK', params: [] }); },
    };
    return { conn, asked, wrote: pattern => asked.filter(a => pattern.test(a.sql)) };
}

function build(rules, { s3Get } = {}) {
    const routes = {};
    const app = Object.fromEntries(['get', 'post', 'delete'].map(m => [m, (path, fn) => { routes[`${m.toUpperCase()} ${path}`] = fn; }]));
    const db = scripted(rules);
    const s3Calls = [];
    const s3 = {
        send: async (cmd) => {
            const name = cmd.constructor.name;
            s3Calls.push({ name, input: cmd.input });
            if (name === 'GetObjectCommand') {
                const bytes = s3Get ? s3Get(cmd.input) : PDF;
                if (bytes instanceof Error) throw bytes;
                return { ContentLength: bytes.length, Body: { transformToByteArray: async () => bytes, destroy() {} } };
            }
            return {};
        },
    };
    const audits = [];
    registerTelexReleaseRoutes(app, {
        withConnection: fn => fn(db.conn),
        ready: async () => {},
        s3, poBucket: 'po-bucket', publicS3Url: key => `https://po.example/${key}`,
        recordAudit: async (conn, entry) => { audits.push(entry); },
        log: { error() {}, warn() {} },
    });
    const call = async (route, req = {}) => {
        let status = null;
        let body = null;
        const res = { status(s) { status = s; return this; }, json(b) { body = b; return this; } };
        await routes[route]({ query: {}, params: {}, body: {}, userEmail: 'ops@us.com', userType: 'standard', ...req }, res);
        return { status, body };
    };
    return { call, db, s3Calls, audits, puts: () => s3Calls.filter(c => c.name === 'PutObjectCommand') };
}

const RELEASE_ROW = (over = {}) => ({
    id: 9, shipment_id: 61, shipment_reference: '333', supplier_name: SUPPLIER, filename: 'telex.pdf', public_url: 'https://po.example/telex-releases/u/telex.pdf',
    content_type: 'application/pdf', file_size: PDF.length, source: 'staff', portal_submission_id: null, notes: null, uploaded_by_email: 'ops@us.com', deleted_at: null,
    created_at_iso: '2026-11-16T10:00:00Z', ...over,
});
const SUBMISSION_ROW = (over = {}) => ({
    id: 3, transfer_id: 800, supplier_payment_id: 45, shipment_id: 61, supplier_name: SUPPLIER, container_ref: '333', user_email: 'factory@acme.example',
    status: 'open', decided_note: null, decided_by: null, release_id: null, created_at_iso: '2026-11-16T09:00:00Z', decided_at_iso: null,
    filename: 'TLX 333.pdf', content_type: 'application/pdf', file_size: PDF.length, final_s3_key: 'files/sup/800/abc.pdf', final_etag: '"etag-1"', file_status: 'sent',
    sent_at_iso: '2026-11-16T09:01:00Z', shipment_reference: '333', shipment_live: 1, ...over,
});

const POST = 'POST /api/v1/telex-releases';
const body = (over = {}) => ({ shipmentId: 61, supplierName: SUPPLIER, filename: 'telex.pdf', dataBase64: PDF.toString('base64'), ...over });

test('filing: a role that may not file is refused before anything is read or stored', async () => {
    const t = build([]);
    const r = await t.call(POST, { userType: 'accountant', body: body() });
    assert.deepEqual(r, { status: 403, body: { error: 'Your role cannot file or delete a telex release: admin and standard users do.', code: 'CANNOT_FILE' } });
    assert.equal(t.db.asked.length, 0);
    assert.equal(t.s3Calls.length, 0);
});

test('filing: no such shipment, or a file that is not a document, stores nothing', async () => {
    const gone = build([[/FROM shipments WHERE id = \? AND deleted_at IS NULL/, [[]]]]);
    assert.deepEqual(await gone.call(POST, { body: body() }), { status: 404, body: { code: 'NO_SUCH_SHIPMENT', error: 'That shipment does not exist.' } });
    assert.equal(gone.s3Calls.length, 0);

    const program = build([[/FROM shipments WHERE id = \?/, [[{ id: 61, reference: '333' }]]]]);
    assert.deepEqual(await program.call(POST, { body: body({ dataBase64: EXE.toString('base64') }) }),
        { status: 422, body: { code: 'BAD_TYPE', error: 'A telex release must be a PDF, a PNG or a JPEG.' } });
    assert.equal(program.s3Calls.length, 0);
    assert.equal(program.db.wrote(/^INSERT/).length, 0);
});

test('filing: the file goes to our bucket, the release is written and audited, and the portal is told', async () => {
    const t = build([
        [/FROM shipments WHERE id = \?/, [[{ id: 61, reference: '333' }]]],
        [/^INSERT INTO shipment_telex_releases/, [{ insertId: 9 }]],
        [/FROM shipment_telex_releases r WHERE r\.id = \?/, [[RELEASE_ROW()]]],
        [/^INSERT INTO `jportal`\.telex_submissions/, [{ insertId: 70 }]],
    ]);
    const r = await t.call(POST, { body: body({ notes: '  from   the forwarder ' }) });
    assert.equal(r.status, 201);
    assert.deepEqual(r.body.release, {
        id: 9, shipmentId: 61, shipmentReference: '333', supplierName: SUPPLIER, filename: 'telex.pdf', url: 'https://po.example/telex-releases/u/telex.pdf',
        contentType: 'application/pdf', fileSize: PDF.length, source: 'staff', portalSubmissionId: null, notes: null, uploadedByEmail: 'ops@us.com', createdAt: '2026-11-16T10:00:00Z',
    });
    const [put] = t.puts();
    assert.equal(put.input.Bucket, 'po-bucket');
    assert.match(put.input.Key, /^telex-releases\/[0-9a-f-]{36}\/telex\.pdf$/);
    assert.equal(put.input.ContentType, 'application/pdf');
    assert.equal(put.input.ContentDisposition, 'inline; filename="telex.pdf"');
    const [ins] = t.db.wrote(/^INSERT INTO shipment_telex_releases/);
    assert.deepEqual(ins.params, [61, '333', SUPPLIER, 'telex.pdf', put.input.Key, `https://po.example/${put.input.Key}`, 'application/pdf', PDF.length, 'from the forwarder', 'ops@us.com']);
    assert.match(ins.sql, /VALUES \(\?, \?, \?, \?, \?, \?, \?, \?, 'staff', \?, \?\)/);
    assert.deepEqual(t.audits, [{
        entityType: 'shipment_telex_release', entityId: 9, action: 'create', before: null,
        after: { shipmentId: 61, supplierName: SUPPLIER, filename: 'telex.pdf', fileSize: PDF.length, source: 'staff' }, userEmail: 'ops@us.com',
    }]);
    const [told] = t.db.wrote(/^INSERT INTO `jportal`\.telex_submissions/);
    assert.match(told.sql, /VALUES \(NULL, NULL, \?, \?, NULL, 'received', \?, UTC_TIMESTAMP\(\), \?\)/);
    assert.deepEqual(told.params, [61, SUPPLIER, 'ops@us.com', 9]);
});

test('filing: a server without the portal\'s table still files', async () => {
    const t = build([
        [/FROM shipments WHERE id = \?/, [[{ id: 61, reference: null }]]],
        [/^INSERT INTO shipment_telex_releases/, [{ insertId: 9 }]],
        [/FROM shipment_telex_releases r WHERE r\.id = \?/, [[RELEASE_ROW({ shipment_reference: null })]]],
        [/^INSERT INTO `jportal`\.telex_submissions/, err(1146)],
    ]);
    const r = await t.call(POST, { body: body() });
    assert.equal(r.status, 201);
    assert.equal(r.body.release.shipmentReference, null);
});

test('deleting: whoever filed it or an admin; soft, audited, and the portal stops saying received', async () => {
    const rules = [
        [/^SELECT r\.id, r\.shipment_id, r\.supplier_name, r\.filename, r\.uploaded_by_email, r\.deleted_at FROM shipment_telex_releases/, [[{ id: 9, shipment_id: 61, supplier_name: SUPPLIER, filename: 'telex.pdf', uploaded_by_email: 'ops@us.com', deleted_at: null }]]],
        [/^UPDATE shipment_telex_releases SET deleted_at = UTC_TIMESTAMP\(\), deleted_by_email = \? WHERE id = \? AND deleted_at IS NULL/, [{ affectedRows: 1 }]],
        [/^UPDATE `jportal`\.telex_submissions SET status = 'void' WHERE release_id = \? AND status = 'received'/, [{ affectedRows: 1 }]],
    ];
    const other = build(rules);
    assert.deepEqual(await other.call('DELETE /api/v1/telex-releases/:id(\\d+)', { params: { id: '9' }, userEmail: 'someone@us.com' }),
        { status: 403, body: { code: 'NOT_YOURS', error: 'Only an admin, or whoever filed this telex release, can delete it.' } });
    assert.equal(other.db.wrote(/^UPDATE/).length, 0);

    const admin = build(rules);
    assert.deepEqual(await admin.call('DELETE /api/v1/telex-releases/:id(\\d+)', { params: { id: '9' }, userEmail: 'boss@us.com', userType: 'admin' }), { status: 200, body: { deleted: true } });
    assert.deepEqual(admin.db.wrote(/^UPDATE shipment_telex_releases/)[0].params, ['boss@us.com', 9]);
    assert.deepEqual(admin.db.wrote(/^UPDATE `jportal`/).map(w => [w.sql, w.params]),
        [["UPDATE `jportal`.telex_submissions SET status = 'void' WHERE release_id = ? AND status = 'received'", [9]]]);
    assert.deepEqual(admin.audits, [{
        entityType: 'shipment_telex_release', entityId: 9, action: 'delete',
        before: { shipmentId: 61, supplierName: SUPPLIER, filename: 'telex.pdf' }, after: null, userEmail: 'boss@us.com',
    }]);
});

test('the uploads list: only what waits for us unless all are asked for; a server without the portal says so', async () => {
    const open = build([[/FROM `jportal`\.telex_submissions t/, [[SUBMISSION_ROW()]]]]);
    const r = await open.call('GET /api/v1/telex-submissions');
    assert.equal(r.status, 200);
    assert.deepEqual(r.body, {
        unavailable: false, canFile: true,
        data: [{
            id: 3, status: 'open', sent: true, supplierPaymentId: 45, shipmentId: 61, shipmentReference: '333', shipmentLive: true, containerRef: '333', supplierName: SUPPLIER,
            uploadedByEmail: 'factory@acme.example', createdAt: '2026-11-16T09:00:00Z', note: null, decidedBy: null, decidedAt: null, releaseId: null,
            file: { transferId: 800, filename: 'TLX 333.pdf', contentType: 'application/pdf', fileSize: PDF.length, sentAt: '2026-11-16T09:01:00Z' },
        }],
    });
    assert.match(open.db.asked[0].sql, /WHERE t\.transfer_id IS NOT NULL AND t\.status = 'open' AND f\.status = 'sent' AND f\.final_s3_key IS NOT NULL/);
    await open.call('GET /api/v1/telex-submissions', { query: { status: 'all' } });
    assert.match(open.db.asked[1].sql, /t\.status IN \('open', 'received', 'rejected'\)/);

    const none = build([[/FROM `jportal`\.telex_submissions t/, err(1146)]]);
    assert.deepEqual(await none.call('GET /api/v1/telex-submissions', { userType: 'viewer' }), { status: 200, body: { data: [], unavailable: true, canFile: false } });
});

const ACCEPT = 'POST /api/v1/telex-submissions/:id(\\d+)/accept';

test('accepting: the supplier\'s file is read pinned to its ETag, filed in our bucket under the upload\'s shipment and supplier, and the portal row points at it', async () => {
    let reads = 0;
    const t = build([
        [/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, () => { reads += 1; return [[reads < 3 ? SUBMISSION_ROW() : SUBMISSION_ROW({ status: 'received', release_id: 12, decided_by: 'ops@us.com' })]]; }],
        [/^INSERT INTO shipment_telex_releases/, [{ insertId: 12 }]],
        [/^UPDATE `jportal`\.telex_submissions SET status = 'received'/, [{ affectedRows: 1 }]],
        [/FROM shipment_telex_releases r WHERE r\.id = \?/, [[RELEASE_ROW({ id: 12, source: 'portal', portal_submission_id: 3, filename: 'TLX 333.pdf', uploaded_by_email: 'factory@acme.example' })]]],
    ]);
    const r = await t.call(ACCEPT, { params: { id: '3' } });
    assert.equal(r.status, 200);
    assert.equal(r.body.replayed, false);
    assert.equal(r.body.release.id, 12);
    assert.equal(r.body.release.source, 'portal');
    assert.equal(r.body.submission.status, 'received');
    const [get] = t.s3Calls;
    assert.deepEqual([get.name, get.input], ['GetObjectCommand', { Bucket: 'portal-bucket', Key: 'files/sup/800/abc.pdf', IfMatch: '"etag-1"' }]);
    const [put] = t.puts();
    assert.equal(put.input.Bucket, 'po-bucket');
    assert.match(put.input.Key, /^telex-releases\/[0-9a-f-]{36}\/TLX_333\.pdf$/);
    const [ins] = t.db.wrote(/^INSERT INTO shipment_telex_releases/);
    assert.match(ins.sql, /VALUES \(\?, \?, \?, \?, \?, \?, \?, \?, 'portal', \?, \?, \?\)/);
    assert.deepEqual(ins.params, [61, '333', SUPPLIER, 'TLX 333.pdf', put.input.Key, `https://po.example/${put.input.Key}`, 'application/pdf', PDF.length, 3,
        'Uploaded by the supplier on the supplier portal (telex upload 3).', 'factory@acme.example']);
    const [upd] = t.db.wrote(/^UPDATE `jportal`\.telex_submissions/);
    assert.match(upd.sql, /release_id = \? WHERE id = \? AND status = 'open'/);
    assert.deepEqual(upd.params, ['ops@us.com', 12, 3]);
    assert.equal(t.audits[0].after.telexSubmissionId, 3);
    // The second read of the upload is under a lock, inside the transaction.
    const order = t.db.asked.map(a => (a.sql === 'BEGIN' || a.sql === 'COMMIT' ? a.sql : /FOR UPDATE OF t/.test(a.sql) ? 'LOCK' : /^INSERT INTO shipment_telex/.test(a.sql) ? 'INSERT' : /^UPDATE `jportal`/.test(a.sql) ? 'UPDATE' : null)).filter(Boolean);
    assert.deepEqual(order, ['BEGIN', 'LOCK', 'INSERT', 'UPDATE', 'COMMIT']);
});

test('accepting: asked twice, or by two people at once, files once', async () => {
    const done = build([
        [/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW({ status: 'received', release_id: 12 })]]],
        [/FROM shipment_telex_releases r WHERE r\.id = \?/, [[RELEASE_ROW({ id: 12, source: 'portal', portal_submission_id: 3 })]]],
    ]);
    const again = await done.call(ACCEPT, { params: { id: '3' } });
    assert.equal(again.status, 200);
    assert.equal(again.body.replayed, true);
    assert.equal(again.body.release.id, 12);
    assert.equal(done.s3Calls.length, 0, 'nothing is read or stored again');

    const race = build([
        [/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW()]]],
        [/^INSERT INTO shipment_telex_releases/, err(1062)],
        [/FROM shipment_telex_releases r WHERE r\.portal_submission_id = \?/, [[RELEASE_ROW({ id: 12, source: 'portal', portal_submission_id: 3 })]]],
    ]);
    const lost = await race.call(ACCEPT, { params: { id: '3' } });
    assert.equal(lost.status, 200);
    assert.equal(lost.body.replayed, true);
    assert.equal(race.db.wrote(/^UPDATE `jportal`/).length, 0);
    assert.ok(race.db.asked.some(a => a.sql === 'ROLLBACK'));
});

test('accepting: refused for a role, a decided upload, a file that is a program or cannot be read; nothing is filed', async () => {
    const role = build([[/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW()]]]]);
    assert.equal((await role.call(ACCEPT, { params: { id: '3' }, userType: 'accountant' })).body.code, 'CANNOT_FILE');
    assert.equal(role.s3Calls.length, 0);

    const sentBack = build([[/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW({ status: 'rejected' })]]]]);
    assert.deepEqual(await sentBack.call(ACCEPT, { params: { id: '3' } }),
        { status: 409, body: { code: 'ALREADY_DECIDED', error: 'This upload was already sent back.', allowedNext: ['reload'] } });

    const program = build([[/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW()]]]], { s3Get: () => EXE });
    assert.deepEqual(await program.call(ACCEPT, { params: { id: '3' } }),
        { status: 422, body: { code: 'BAD_TYPE', error: 'A telex release must be a PDF, a PNG or a JPEG.', allowedNext: ['reject'] } });
    assert.equal(program.puts().length, 0);
    assert.equal(program.db.wrote(/^INSERT/).length, 0);

    const unreadable = build([[/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW()]]]], { s3Get: () => new Error('PreconditionFailed') });
    const r = await unreadable.call(ACCEPT, { params: { id: '3' } });
    assert.deepEqual([r.status, r.body.code, r.body.allowedNext], [409, 'FILE_UNAVAILABLE', ['reject']]);
    assert.equal(unreadable.db.wrote(/^INSERT/).length, 0);

    const noPortal = build([[/FROM `jportal`\.telex_submissions t/, err(1146)]]);
    assert.deepEqual(await noPortal.call(ACCEPT, { params: { id: '3' } }), { status: 503, body: { error: 'The supplier portal\'s uploads are not available on this server.', code: 'PORTAL_UNAVAILABLE' } });
});

const REJECT = 'POST /api/v1/telex-submissions/:id(\\d+)/reject';

test('sending back: needs words the supplier can act on, and writes them once', async () => {
    const rules = [
        [/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW()]]],
        [/^UPDATE `jportal`\.telex_submissions SET status = 'rejected'/, [{ affectedRows: 1 }]],
    ];
    const silent = build(rules);
    assert.deepEqual(await silent.call(REJECT, { params: { id: '3' }, body: {} }),
        { status: 400, body: { code: 'NOTE_REQUIRED', error: 'Say what is wrong with it, in at least 3 characters: the supplier reads this.' } });
    assert.equal(silent.db.wrote(/^UPDATE/).length, 0);

    const said = build(rules);
    const r = await said.call(REJECT, { params: { id: '3' }, body: { note: '  This is the draft B/L,\n not the release ' } });
    assert.equal(r.status, 200);
    const [upd] = said.db.wrote(/^UPDATE `jportal`\.telex_submissions/);
    assert.match(upd.sql, /decided_note = \? WHERE id = \? AND status = 'open'/);
    assert.deepEqual(upd.params, ['ops@us.com', 'This is the draft B/L, not the release', 3]);
    assert.deepEqual(said.audits, [{
        entityType: 'telex_submission', entityId: 3, action: 'reject', before: { status: 'open' },
        after: { status: 'rejected', note: 'This is the draft B/L, not the release', shipmentId: 61, supplierName: SUPPLIER }, userEmail: 'ops@us.com',
    }]);
    assert.equal(said.s3Calls.length, 0);
});

test('the supplier\'s file: a short link to the portal\'s copy, only once it has arrived', async () => {
    const notYet = build([[/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW({ file_status: 'draft', final_s3_key: null })]]]]);
    assert.deepEqual(await notYet.call('GET /api/v1/telex-submissions/:id(\\d+)/file', { params: { id: '3' } }), { status: 404, body: { error: 'Telex upload not found.', code: 'NOT_FOUND' } });
    // A row marked sent whose copy was never made has no file to show either.
    const noCopy = build([[/FROM `jportal`\.telex_submissions t .* WHERE t\.id = \?/, [[SUBMISSION_ROW({ file_status: 'sent', final_s3_key: null })]]]]);
    assert.equal((await noCopy.call('GET /api/v1/telex-submissions/:id(\\d+)/file', { params: { id: '3' } })).status, 404);
    assert.equal(noCopy.s3Calls.length, 0);
});

const LIST = 'GET /api/v1/telex-releases';
const OTHER = 'Other Factory Ltd';
const listRules = (over = {}) => [
    [/^SELECT id, reference FROM shipments WHERE id = \?$/, over.shipment ?? [[{ id: 61, reference: '333' }]]],
    [/^SELECT DISTINCT o\.supplier AS name FROM orders o/, over.onBoard ?? [[{ name: SUPPLIER }, { name: OTHER }]]],
    [/^SELECT DISTINCT p\.supplier_name AS name FROM shipment_payments p/, over.balances ?? [[{ name: SUPPLIER }]]],
    [/FROM shipment_telex_releases r WHERE r\.deleted_at IS NULL AND r\.shipment_id IN \(\?\)/, over.releases ?? [[RELEASE_ROW()]]],
    [/FROM `jportal`\.telex_submissions t .* WHERE t\.shipment_id IN \(\?\) AND t\.transfer_id IS NOT NULL/, over.uploads ?? [[SUBMISSION_ROW({ supplier_name: OTHER })]]],
];

test('the shipment\'s list: a block a supplier, from its lines and its balances, with what is filed and what the supplier uploaded', async () => {
    const t = build(listRules());
    const r = await t.call(LIST, { query: { shipmentId: '61' } });
    assert.equal(r.status, 200);
    assert.deepEqual(r.body, {
        shipmentId: 61, reference: '333', canFile: true,
        suppliers: [
            {
                supplierName: SUPPLIER, onBoard: true, balance: true, state: 'received', upload: null,
                releases: [{
                    id: 9, shipmentId: 61, shipmentReference: '333', supplierName: SUPPLIER, filename: 'telex.pdf', url: 'https://po.example/telex-releases/u/telex.pdf',
                    contentType: 'application/pdf', fileSize: PDF.length, source: 'staff', portalSubmissionId: null, notes: null, uploadedByEmail: 'ops@us.com', createdAt: '2026-11-16T10:00:00Z',
                }],
            },
            {
                supplierName: OTHER, onBoard: true, balance: false, state: 'uploaded', releases: [],
                upload: {
                    id: 3, status: 'open', sent: true, supplierPaymentId: 45, shipmentId: 61, shipmentReference: '333', shipmentLive: true, containerRef: '333', supplierName: OTHER,
                    uploadedByEmail: 'factory@acme.example', createdAt: '2026-11-16T09:00:00Z', note: null, decidedBy: null, decidedAt: null, releaseId: null,
                    file: { transferId: 800, filename: 'TLX 333.pdf', contentType: 'application/pdf', fileSize: PDF.length, sentAt: '2026-11-16T09:01:00Z' },
                },
            },
        ],
    });
    // Who is on the shipment: its live order lines, booked (orders.shipment_id) or still planned (shipment_lines).
    const [, onBoard, balances] = t.db.asked;
    assert.equal(onBoard.sql, "SELECT DISTINCT o.supplier AS name FROM orders o WHERE o.deleted_at IS NULL AND o.status <> 'DESTROYED'"
        + " AND o.supplier IS NOT NULL AND o.supplier <> '' AND (o.shipment_id = ? OR o.id IN (SELECT sl.order_id FROM shipment_lines sl WHERE sl.shipment_id = ?))");
    assert.deepEqual(onBoard.params, [61, 61]);
    assert.equal(balances.sql, "SELECT DISTINCT p.supplier_name AS name FROM shipment_payments p WHERE p.shipment_id = ? AND p.deleted_at IS NULL AND p.supplier_name IS NOT NULL AND p.supplier_name <> ''");
    assert.deepEqual(balances.params, [61]);
    assert.equal(t.db.wrote(/^(INSERT|UPDATE|DELETE)/).length, 0, 'a read writes nothing');
});

test('the shipment\'s list: anyone may read it, only some may file; no shipment, no list', async () => {
    const viewer = build(listRules());
    assert.equal((await viewer.call(LIST, { query: { shipmentId: '61' }, userType: 'viewer' })).body.canFile, false);
    assert.equal((await viewer.call(LIST, { query: { shipmentId: '61' }, userType: 'accountant' })).body.canFile, false);

    const none = build(listRules({ shipment: [[]] }));
    assert.deepEqual(await none.call(LIST, { query: { shipmentId: '61' } }), { status: 404, body: { code: 'NO_SUCH_SHIPMENT', error: 'That shipment does not exist.' } });
    for (const shipmentId of [undefined, '', 'abc', '0', '-3', '1.5']) {
        const bad = build([]);
        assert.deepEqual(await bad.call(LIST, { query: { shipmentId } }), { status: 400, body: { error: 'shipmentId is required.', code: 'BAD_FIELD' } }, String(shipmentId));
        assert.equal(bad.db.asked.length, 0);
    }
});

test('the shipment\'s list: a server without the portal lists what is filed; one without the table says so', async () => {
    const noPortal = build(listRules({ uploads: err(1142) }));
    const r = await noPortal.call(LIST, { query: { shipmentId: '61' } });
    assert.deepEqual(r.body.suppliers.map(x => [x.supplierName, x.state, x.upload]), [[SUPPLIER, 'received', null], [OTHER, 'awaiting', null]]);

    const noTable = build(listRules({ releases: err(1146) }));
    assert.deepEqual(await noTable.call(LIST, { query: { shipmentId: '61' } }),
        { status: 503, body: { error: 'Telex releases are not set up on this server yet: their table has not been migrated.', code: 'NOT_MIGRATED' } });
});

test('the shipment\'s list: a shipment with nothing filed still lists who is on it, and a deleted release is not read', async () => {
    const t = build(listRules({ balances: [[]], releases: [[]], uploads: [[]] }));
    const r = await t.call(LIST, { query: { shipmentId: '61' } });
    assert.deepEqual(r.body.suppliers, [
        { supplierName: SUPPLIER, onBoard: true, balance: false, state: 'awaiting', releases: [], upload: null },
        { supplierName: OTHER, onBoard: true, balance: false, state: 'awaiting', releases: [], upload: null },
    ]);
    assert.match(t.db.asked.find(a => /FROM shipment_telex_releases r/.test(a.sql)).sql, /WHERE r\.deleted_at IS NULL AND r\.shipment_id IN \(\?\) ORDER BY r\.created_at DESC, r\.id DESC$/);
});
