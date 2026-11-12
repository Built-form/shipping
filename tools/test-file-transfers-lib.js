'use strict';

// Unit tests for the supplier-portal file-transfer rules (src/lib/file-transfers.js)
// and the routes (src/services/file-transfer-routes.js) against a fake
// connection, a fake S3 client and a stubbed Front. No database, no AWS.
//   node --test tools/test-file-transfers-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const L = require('../src/lib/file-transfers');
const PI = require('../src/lib/pi-submissions');
const { DEFAULT_EMAIL_TEMPLATES, FILE_TRANSFER_TEMPLATE_KEY } = require('../src/lib/email-template-defaults');

const UUID = '0a1b2c3d-1111-4222-8333-444455556666';

// ── Rules ──────────────────────────────────────────────────────────────────

test('shares the decider rule and the id parsers with PI', () => {
    assert.equal(L.canDecide, PI.canDecide);
    assert.equal(L.positiveInt, PI.positiveInt);
    assert.equal(L.parseRowVersion, PI.parseRowVersion);
    assert.deepEqual(L.DECIDER_ROLES, ['admin', 'standard']);
});

test('the limit, the blocked list and the key patterns match the portal', () => {
    assert.equal(L.MAX_FILE_BYTES, 250 * 1024 * 1024);
    assert.ok(L.BLOCKED_EXTENSIONS.includes('exe') && L.BLOCKED_EXTENSIONS.includes('sh') && !L.BLOCKED_EXTENSIONS.includes('zip'));
    assert.equal(L.newStaffKey(UUID, 'Artwork FINAL.PDF'), `files/staff/${UUID}.pdf`);
    assert.equal(L.newStaffKey(UUID, 'README'), `files/staff/${UUID}.bin`);
    assert.equal(L.newStaffKey(UUID, 'tool.exe'), `files/staff/${UUID}.bin`);
    assert.throws(() => L.newStaffKey('nope', 'a.pdf'), /uuid/);
    assert.ok(L.isStaffKey(`files/staff/${UUID}.pdf`));
    assert.ok(L.isSupKey(`files/sup/12/${UUID}.xlsx`));
    assert.ok(L.isInKey(`files/in/${UUID}.xlsx`));
    assert.ok(!L.isSupKey(`files/in/${UUID}.xlsx`) && !L.isStaffKey(`files/sup/12/${UUID}.xlsx`));
});

test('extension and refusal', () => {
    assert.equal(L.extensionOf('Spec v2.XLSX'), 'xlsx');
    assert.equal(L.extensionOf('README'), null);
    assert.equal(L.fileRefusal({ filename: 'report.pdf.exe', fileSize: 10 }), 'FILE_BLOCKED');
    assert.equal(L.fileRefusal({ filename: 'setup.exe.pdf', fileSize: 10 }), null);
    assert.equal(L.fileRefusal({ filename: 'a.zip', fileSize: 0 }), 'FILE_EMPTY');
    assert.equal(L.fileRefusal({ filename: 'a.zip', fileSize: L.MAX_FILE_BYTES + 1 }), 'FILE_TOO_LARGE');
    assert.equal(L.fileRefusal({ filename: 'a.zip', fileSize: L.MAX_FILE_BYTES }), null);
});

test('a program by its first bytes', () => {
    assert.ok(L.sniffExecutable([0x4d, 0x5a, 0x90]));
    assert.ok(L.sniffExecutable([0x7f, 0x45, 0x4c, 0x46]));
    assert.ok(L.sniffExecutable([0xcf, 0xfa, 0xed, 0xfe]));
    assert.ok(L.sniffExecutable([0x23, 0x21, 0x2f]));
    assert.ok(!L.sniffExecutable([0x50, 0x4b, 3, 4]));
    assert.ok(!L.sniffExecutable([0x25, 0x50, 0x44, 0x46]));
    assert.ok(!L.sniffExecutable([]));
    assert.ok(!L.sniffExecutable(null));
});

test('content type, filename and note parsing', () => {
    assert.equal(L.normalizeContentType('Image/PNG '), 'image/png');
    assert.equal(L.normalizeContentType(''), 'application/octet-stream');
    assert.equal(L.normalizeContentType('text/plain; charset=utf-8'), 'application/octet-stream');
    assert.deepEqual(L.parseFilename(' 规格书.xlsx '), { value: '规格书.xlsx' });
    assert.ok(L.parseFilename('').error);
    assert.ok(L.parseFilename('a​.pdf').error);
    assert.ok(L.parseFilename('x'.repeat(256)).error);
    assert.deepEqual(L.parseTransferNote(undefined), { value: null });
    assert.deepEqual(L.parseTransferNote('  '), { value: null });
    assert.deepEqual(L.parseTransferNote(' line one\nline two '), { value: 'line one\nline two' });
    assert.ok(L.parseTransferNote('x'.repeat(2001)).error);
    assert.ok(L.parseTransferNote(12).error);
});

test('readableKey: a supplier file only once published, a staff file only once pinned', () => {
    assert.equal(L.readableKey({ direction: 'from_supplier', s3_key: `files/in/${UUID}.xlsx`, s3_etag: '"a"', final_s3_key: null }), null);
    assert.deepEqual(L.readableKey({ direction: 'from_supplier', final_s3_key: `files/sup/12/${UUID}.xlsx`, final_etag: '"f"' }), { key: `files/sup/12/${UUID}.xlsx`, etag: '"f"' });
    assert.equal(L.readableKey({ direction: 'to_supplier', s3_key: `files/staff/${UUID}.pdf`, s3_etag: null }), null);
    assert.deepEqual(L.readableKey({ direction: 'to_supplier', s3_key: `files/staff/${UUID}.pdf`, s3_etag: '"s"' }), { key: `files/staff/${UUID}.pdf`, etag: '"s"' });
    assert.equal(L.readableKey({ direction: 'to_supplier', s3_key: `files/in/${UUID}.pdf`, s3_etag: '"s"' }), null, 'never a staging key');
});

test('download names: ASCII beside the real name, the extension kept', () => {
    assert.equal(L.safeDownloadName('Spec v2 (final).XLSX'), 'Spec_v2_final.xlsx');
    assert.equal(L.safeDownloadName('规格书.xlsx'), '', 'nothing ASCII left: the route falls back');
    assert.equal(L.contentDisposition('规格书.xlsx', 'file-12.xlsx'), `attachment; filename="file-12.xlsx"; filename*=UTF-8''${encodeURIComponent('规格书.xlsx')}`);
    assert.match(L.contentDisposition('a"b.pdf', 'file-1.pdf'), /^attachment; filename="a_b\.pdf"; filename\*=UTF-8''a%22b\.pdf$/);
});

test('parseListQuery', () => {
    assert.deepEqual(L.parseListQuery({}).value, { statuses: ['sent'], direction: null, supplierId: null, unopened: false, limit: 100, offset: 0 });
    assert.deepEqual(L.parseListQuery({ status: 'history' }).value.statuses, ['withdrawn']);
    assert.deepEqual(L.parseListQuery({ status: 'all' }).value.statuses, ['sent', 'withdrawn']);
    assert.deepEqual(L.parseListQuery({ status: 'sent,withdrawn,sent' }).value.statuses, ['sent', 'withdrawn']);
    assert.ok(L.parseListQuery({ status: 'draft' }).error, 'drafts are never listed');
    assert.equal(L.parseListQuery({ direction: 'to_supplier' }).value.direction, 'to_supplier');
    assert.ok(L.parseListQuery({ direction: 'sideways' }).error);
    assert.equal(L.parseListQuery({ supplierId: '11', unopened: '1', limit: '9999', offset: '5' }).value.limit, 500);
    assert.equal(L.parseListQuery({ unopened: '1' }).value.unopened, true);
    assert.ok(L.parseListQuery({ supplierId: 'x' }).error);
});

test('parseCreateBody', () => {
    const ok = L.parseCreateBody({ supplierId: '11', filename: ' Artwork.pdf ', contentType: 'application/pdf', fileSize: 1234, note: ' Check the colours ' });
    assert.deepEqual(ok.value, { supplierId: 11, filename: 'Artwork.pdf', contentType: 'application/pdf', fileSize: 1234, note: 'Check the colours' });
    assert.equal(L.parseCreateBody({ filename: 'a.pdf', fileSize: 1 }).code, 'BAD_FIELD');
    assert.equal(L.parseCreateBody({ supplierId: 1, filename: 'a.exe', fileSize: 1 }).code, 'FILE_BLOCKED');
    assert.equal(L.parseCreateBody({ supplierId: 1, filename: 'a.zip', fileSize: L.MAX_FILE_BYTES + 1 }).code, 'FILE_TOO_LARGE');
    assert.equal(L.parseCreateBody({ supplierId: 1, filename: 'a.zip', fileSize: 0 }).code, 'BAD_FIELD');
    assert.equal(L.parseCreateBody({ supplierId: 1, filename: 'a.zip', fileSize: 1, contentType: 'nonsense' }).value.contentType, 'application/octet-stream');
});

const row = (over = {}) => ({
    id: 5, direction: 'from_supplier', status: 'sent', supplier_id: 11, supplier_name: 'Acme Ltd', live_supplier_name: 'Acme Limited', supplier_exists: 1,
    filename: 'Spec.xlsx', content_type: 'application/vnd.ms-excel', file_size: '1234', note: 'Latest', sender_email: 'li@acme.test',
    sent_at_iso: '2026-11-04T09:00:00Z', withdrawn_at_iso: null, withdrawn_by: null, notified_at_iso: null, notify_error: null,
    staff_first_viewed_at_iso: null, staff_viewers: 0, supplier_first_viewed_at_iso: null, supplier_viewers: 0,
    row_version: 1, created_at_iso: '2026-11-04T08:59:00Z', s3_key: `files/in/${UUID}.xlsx`, s3_etag: '"a"', final_s3_key: `files/sup/5/${UUID}.xlsx`, final_etag: '"f"', ...over,
});

test('transferToJson: the other side\'s views, the live name, never a key or an ETag', () => {
    const j = L.transferToJson(row({ staff_first_viewed_at_iso: '2026-11-04T10:00:00Z', staff_viewers: 2 }));
    assert.equal(j.supplierName, 'Acme Limited');
    assert.equal(j.supplierKey, 's:11');
    assert.equal(j.fileSize, 1234);
    assert.deepEqual([j.seen, j.seenAt, j.viewers], [true, '2026-11-04T10:00:00Z', 2]);
    assert.equal(j.notifiedAt, null, 'a supplier file has no email');
    for (const k of ['s3_key', 's3Key', 's3_etag', 'final_s3_key', 'finalS3Key']) assert.ok(!(k in j), k);
    const ours = L.transferToJson(row({ direction: 'to_supplier', sender_email: 'me@jfa.test', supplier_first_viewed_at_iso: '2026-11-04T11:00:00Z', supplier_viewers: 1, notify_error: 'front_error: 500' }));
    assert.deepEqual([ours.seen, ours.seenAt, ours.viewers, ours.notifyError], [true, '2026-11-04T11:00:00Z', 1, 'front_error: 500']);
    assert.equal(ours.sentBy, 'me@jfa.test');
});

test('the email: escaped tokens and a link to the portal\'s Files page', () => {
    const vars = L.buildEmailVars({ row: row({ direction: 'to_supplier', filename: 'A <b>.pdf', note: 'Line 1\n<Line 2>' }), portalUrl: 'https://portal.example.com/' });
    assert.equal(vars.subject.filename, 'A <b>.pdf');
    assert.equal(vars.body.filename, 'A &lt;b&gt;.pdf');
    assert.equal(vars.body.supplierName, 'Acme Limited');
    assert.match(vars.body.noteHtml, /Line 1<br>&lt;Line 2&gt;/);
    assert.match(vars.body.portalLinkHtml, /href="https:\/\/portal\.example\.com\/files"/);
    assert.equal(L.buildEmailVars({ row: row({ note: null }), portalUrl: '' }).body.portalLinkHtml, '');
    assert.equal(L.buildEmailVars({ row: row({ note: null }), portalUrl: '' }).body.noteHtml, '');
    const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === FILE_TRANSFER_TEMPLATE_KEY);
    assert.ok(t, 'the default template is in the list');
    assert.equal(L.TEMPLATE_KEY, FILE_TRANSFER_TEMPLATE_KEY);
    for (const token of ['{filename}', '{noteHtml}', '{portalLinkHtml}']) assert.ok(t.bodyHtml.includes(token), token);
});

// ── Routes, against fakes ──────────────────────────────────────────────────

const { registerFileTransferRoutes } = require('../src/services/file-transfer-routes');
const frontSend = require('../src/services/front-send');

function s3Error(name, status) {
    return Object.assign(new Error(name), { name, $metadata: { httpStatusCode: status } });
}

function harness({ userType = 'standard', userEmail = 'me@jfa.test' } = {}) {
    const db = {
        transfers: [],
        views: [],
        suppliers: [{ id: 11, name: 'Acme Limited', deleted_at: null }],
        users: [{ id: 1, email: 'li@acme.test', status: 'active', supplier_id: 11 }, { id: 2, email: 'old@acme.test', status: 'disabled', supplier_id: 11 }],
        audits: [],
        receipts: [],
        templates: [],
    };
    const objects = new Map();
    const s3Calls = [];
    const signed = [];
    // A real client (the presigner reads its config and middleware) with static
    // credentials, whose send() is replaced: nothing reaches AWS.
    const { S3Client } = require('@aws-sdk/client-s3');
    const s3 = new S3Client({ region: 'eu-north-1', credentials: { accessKeyId: 'AKIATEST', secretAccessKey: 'secret' } });
    s3.send = async cmd => {
        const name = cmd.constructor.name;
        s3Calls.push({ name, input: cmd.input });
        const o = objects.get(cmd.input.Key);
        if (name === 'HeadObjectCommand') {
            if (!o) throw s3Error('NotFound', 404);
            return { ETag: o.etag, ContentLength: o.size ?? o.bytes.length };
        }
        if (name === 'GetObjectCommand') {
            if (!o) throw s3Error('NoSuchKey', 404);
            if (cmd.input.IfMatch && cmd.input.IfMatch !== o.etag) throw s3Error('PreconditionFailed', 412);
            return { Body: { transformToByteArray: async () => o.bytes.slice(0, 16) } };
        }
        throw new Error();
    };

    const iso = d => (d ? new Date(d).toISOString().replace(/\.\d{3}Z$/, 'Z') : null);
    const decorate = r => {
        const live = db.suppliers.find(s => s.id === r.supplier_id && !s.deleted_at);
        const views = kind => db.views.filter(v => v.transfer_id === r.id && v.viewer_kind === kind);
        const first = kind => { const vs = views(kind); return vs.length ? iso(Math.min(...vs.map(v => v.first_viewed_at.getTime()))) : null; };
        return {
            ...r, sent_at_iso: iso(r.sent_at), withdrawn_at_iso: iso(r.withdrawn_at), notified_at_iso: iso(r.notified_at), created_at_iso: iso(r.created_at),
            live_supplier_name: live ? live.name : null, supplier_exists: live ? 1 : 0,
            staff_first_viewed_at_iso: first('staff'), staff_viewers: views('staff').length,
            supplier_first_viewed_at_iso: first('supplier'), supplier_viewers: views('supplier').length,
        };
    };
    const conn = {
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            if (s.startsWith('SELECT r.*')) {
                let rows = db.transfers;
                if (s.includes('WHERE r.id = ? AND NOT (r.direction')) rows = rows.filter(r => r.id === params[0] && !(r.direction === 'from_supplier' && !['sent', 'withdrawn'].includes(r.status)));
                else if (s.includes('WHERE r.id = ? AND r.status IN')) rows = rows.filter(r => r.id === params[0] && ['sent', 'withdrawn'].includes(r.status));
                else {
                    const m = /r\.status IN \(([^)]*)\)/.exec(s);
                    const n = m ? m[1].split(',').length : 0;
                    const statuses = params.slice(0, n);
                    let i = n;
                    rows = rows.filter(r => statuses.includes(r.status));
                    if (s.includes('r.direction = ?')) { const d = params[i++]; rows = rows.filter(r => r.direction === d); }
                    if (s.includes('r.supplier_id = ?')) { const sid = params[i++]; rows = rows.filter(r => r.supplier_id === sid); }
                    if (s.includes('NOT EXISTS')) rows = rows.filter(r => r.direction === 'from_supplier' && r.status === 'sent' && !db.views.some(v => v.transfer_id === r.id && v.viewer_kind === 'staff'));
                    rows = [...rows].sort((a, b) => (b.sent_at || 0) - (a.sent_at || 0) || b.id - a.id);
                    const lim = /LIMIT (\d+) OFFSET (\d+)/.exec(s);
                    if (lim) rows = rows.slice(Number(lim[2]), Number(lim[2]) + Number(lim[1]));
                }
                return [rows.map(decorate)];
            }
            if (s.startsWith('SELECT r.supplier_id, r.supplier_name, r.direction')) {
                return [db.transfers.filter(r => ['sent', 'withdrawn'].includes(r.status)).sort((a, b) => (a.sent_at || 0) - (b.sent_at || 0)).map(r => ({
                    ...r, sent_at_iso: iso(r.sent_at), live_supplier_name: (db.suppliers.find(x => x.id === r.supplier_id) || {}).name || null, supplier_exists: 1,
                    staff_seen: db.views.some(v => v.transfer_id === r.id && v.viewer_kind === 'staff') ? 1 : 0,
                }))];
            }
            if (s.startsWith('SELECT viewer_kind, viewer_email')) return [db.views.filter(v => v.transfer_id === params[0])];
            if (s.startsWith('SELECT DISTINCT u.email')) return [db.users.filter(u => u.supplier_id === params[0] && u.status === 'active').map(u => ({ email: u.email }))];
            if (s.startsWith('SELECT id, name FROM suppliers')) return [db.suppliers.filter(x => x.id === params[0] && !x.deleted_at)];
            if (s.startsWith('SELECT subject, body_html FROM email_templates')) return [db.templates.filter(t => t.template_key === params[0])];
            if (s.startsWith('INSERT INTO `jportal`.file_transfers')) {
                const [supplier_id, supplier_name, sender_email, filename, content_type, file_size, s3_key, note] = params;
                const id = db.transfers.length + 100;
                db.transfers.push({ id, direction: 'to_supplier', status: 'draft', supplier_id, supplier_name, user_id: null, sender_email, filename, content_type, file_size, s3_key, s3_etag: null, final_s3_key: null, final_etag: null, note, sent_at: null, withdrawn_at: null, withdrawn_by: null, notified_at: null, notify_error: null, row_version: 0, created_at: new Date() });
                return [{ insertId: id, affectedRows: 1 }];
            }
            if (s.startsWith('INSERT INTO email_receipts')) {
                const [, email_type, send_table, send_id, sent_to, subject, confirm_via, action_url] = params;
                db.receipts.push({ email_type, send_table, send_id, sent_to, subject, confirm_via, action_url });
                return [{ insertId: db.receipts.length, affectedRows: 1 }];
            }
            if (s.startsWith('INSERT INTO `jportal`.file_transfer_views')) {
                const [transfer_id, viewer_email] = params;
                const v = db.views.find(x => x.transfer_id === transfer_id && x.viewer_kind === 'staff' && x.viewer_email === viewer_email);
                if (v) { v.view_count += 1; v.last_viewed_at = new Date(); } else db.views.push({ transfer_id, viewer_kind: 'staff', viewer_email, first_viewed_at: new Date(), last_viewed_at: new Date(), view_count: 1 });
                return [{ affectedRows: 1 }];
            }
            if (s.startsWith('UPDATE `jportal`.file_transfers')) {
                const set = /SET (.*) WHERE/.exec(s)[1];
                const id = params[set.split(',').length - (set.includes('UTC_TIMESTAMP()') ? 1 : 0) - (set.includes('NULL') ? 1 : 0)];
                const t = db.transfers.find(x => x.id === params[set.split('?').length - 1]) || db.transfers.find(x => x.id === id);
                if (set.startsWith("status = 'sent'")) {
                    const [etag, size, rid] = params;
                    const r = db.transfers.find(x => x.id === rid);
                    if (!r || r.direction !== 'to_supplier' || r.status !== 'draft') return [{ affectedRows: 0 }];
                    Object.assign(r, { status: 'sent', s3_etag: etag, file_size: size, sent_at: new Date(), row_version: r.row_version + 1 });
                    return [{ affectedRows: 1 }];
                }
                if (set.startsWith('notified_at = UTC_TIMESTAMP()')) { const r = db.transfers.find(x => x.id === params[0]); Object.assign(r, { notified_at: new Date(), notify_error: null }); return [{ affectedRows: 1 }]; }
                if (set.startsWith('notify_error = ?')) { const r = db.transfers.find(x => x.id === params[1]); r.notify_error = params[0]; return [{ affectedRows: 1 }]; }
                if (set.startsWith("status = 'withdrawn'")) {
                    const [by, rid, rv] = params;
                    const r = db.transfers.find(x => x.id === rid && x.status === 'sent' && x.row_version === rv);
                    if (!r) return [{ affectedRows: 0 }];
                    Object.assign(r, { status: 'withdrawn', withdrawn_at: new Date(), withdrawn_by: by, row_version: r.row_version + 1 });
                    return [{ affectedRows: 1 }];
                }
                if (set.startsWith("status = 'discarded'")) {
                    const [rid, rv] = params;
                    const r = db.transfers.find(x => x.id === rid && x.status === 'draft' && x.row_version === rv);
                    if (!r) return [{ affectedRows: 0 }];
                    Object.assign(r, { status: 'discarded', row_version: r.row_version + 1 });
                    return [{ affectedRows: 1 }];
                }
                void t;
            }
            throw new Error(`unexpected statement: ${s.slice(0, 100)}`);
        },
    };

    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => { req.userType = userType; req.userEmail = userEmail; next(); });
    const logs = [];
    registerFileTransferRoutes(app, {
        withConnection: fn => fn(conn),
        pool: null,
        ready: async () => {},
        s3,
        recordAudit: async (_c, a) => { db.audits.push(a); },
        getEmailTemplate: async (_c, key) => ({ subject: `A file – {filename}`, bodyHtml: `<p>{filename}</p>{noteHtml}{portalLinkHtml}` }),
        renderTemplate: (str, vars) => String(str).replace(/\{(\w+)\}/g, (m, k) => (vars[k] != null ? String(vars[k]) : m)),
        log: { error: (...a) => logs.push(['error', ...a]), warn: (...a) => logs.push(['warn', ...a]), info: () => {} },
    });

    const call = (method, path, body) => new Promise((resolve, reject) => {
        const http = require('node:http');
        const server = app.listen(0, () => {
            const { port } = server.address();
            const req = http.request({ port, method, path, headers: { 'content-type': 'application/json' } }, res => {
                let text = '';
                res.on('data', d => { text += d; });
                res.on('end', () => { server.close(); resolve({ status: res.statusCode, body: text ? JSON.parse(text) : null }); });
            });
            req.on('error', e => { server.close(); reject(e); });
            if (body !== undefined) req.write(JSON.stringify(body));
            req.end();
        });
    });

    return { db, objects, s3Calls, signed, call, logs };
}

const PK = Uint8Array.from([0x50, 0x4b, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
const MZ = Uint8Array.from([0x4d, 0x5a, 0x90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

const supplierFile = (db, over = {}) => {
    const id = over.id || 5;
    db.transfers.push({
        id, direction: 'from_supplier', status: 'sent', supplier_id: 11, supplier_name: 'Acme Ltd', user_id: 7, sender_email: 'li@acme.test',
        filename: 'Spec.xlsx', content_type: 'application/vnd.ms-excel', file_size: 16, s3_key: `files/in/${UUID}.xlsx`, s3_etag: '"a"',
        final_s3_key: `files/sup/${id}/${UUID}.xlsx`, final_etag: '"f"', note: 'Latest', sent_at: new Date('2026-11-04T09:00:00Z'), withdrawn_at: null,
        withdrawn_by: null, notified_at: null, notify_error: null, row_version: 1, created_at: new Date('2026-11-04T08:59:00Z'), ...over,
    });
    return db.transfers.at(-1);
};

test('routes: suppliers and the list, with unopened counts; opening records a view and signs the final key', async () => {
    const h = harness();
    process.env.PI_UPLOADS_BUCKET = 'jportal-pi-uploads-test';
    const t = supplierFile(h.db);
    h.objects.set(t.final_s3_key, { etag: '"f"', bytes: PK });
    let r = await h.call('GET', '/api/v1/file-transfers/suppliers');
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.data.map(s => [s.supplierKey, s.supplierName, s.pending, s.total, s.oldestPendingAt]), [['s:11', 'Acme Limited', 1, 1, '2026-11-04T09:00:00Z']]);
    r = await h.call('GET', '/api/v1/file-transfers?unopened=1');
    assert.equal(r.body.data.length, 1);
    assert.equal(r.body.data[0].seen, false);
    assert.ok(!('s3Key' in r.body.data[0]));

    r = await h.call('POST', '/api/v1/file-transfers/5/open', {});
    assert.equal(r.status, 200, JSON.stringify(r.body));
    assert.equal(r.body.transfer.seen, true);
    assert.equal(r.body.transfer.viewers, 1);
    assert.deepEqual(r.body.transfer.views.map(v => [v.kind, v.email, v.count]), [['staff', 'me@jfa.test', 1]]);
    assert.ok(r.body.file && /files%2Fsup%2F5%2F|files\/sup\/5\//.test(r.body.file.url), r.body.file && r.body.file.url);
    assert.match(r.body.file.url, /response-content-disposition=attachment/);
    r = await h.call('GET', '/api/v1/file-transfers?unopened=1');
    assert.equal(r.body.data.length, 0, 'opened: no longer pending');
    r = await h.call('GET', '/api/v1/file-transfers/suppliers');
    assert.equal(r.body.data[0].pending, 0);
    // A second open by the same person counts once more, not as a new viewer.
    await h.call('POST', '/api/v1/file-transfers/5/open', {});
    assert.equal(h.db.views.length, 1);
    assert.equal(h.db.views[0].view_count, 2);
});

test('routes: read-only users look but never open, send or withdraw', async () => {
    const h = harness({ userType: 'accountant' });
    supplierFile(h.db);
    let r = await h.call('GET', '/api/v1/file-transfers');
    assert.equal(r.status, 200);
    r = await h.call('POST', '/api/v1/file-transfers/5/open', {});
    assert.equal(r.status, 403);
    assert.equal(r.body.code, 'CANNOT_DECIDE');
    r = await h.call('POST', '/api/v1/file-transfers', { supplierId: 11, filename: 'a.pdf', fileSize: 1 });
    assert.equal(r.status, 403);
});

test('routes: sending a file — create, uploaded (checked, pinned, emailed), notify again, withdraw', async () => {
    const h = harness();
    process.env.PI_UPLOADS_BUCKET = 'jportal-pi-uploads-test';
    process.env.SUPPLIER_PORTAL_URL = 'https://portal.example.com';
    delete process.env.PI_NOTIFY_REDIRECT_TO;
    const sent = [];
    const original = frontSend.sendFrontMessage;
    // The routes module captured sendFrontMessage at require time; swap the
    // module's export AND the captured one through the global fetch instead.
    const fetchCalls = [];
    const realFetch = globalThis.fetch;
    globalThis.fetch = async (url, init) => {
        fetchCalls.push({ url, init });
        sent.push(Object.fromEntries([...init.body.entries()].filter(([k]) => k !== 'body')));
        return { ok: true, status: 202, text: async () => JSON.stringify({ message_uid: 'msg-1' }) };
    };
    process.env.FRONT_CHANNEL_ID = 'cha_1';
    process.env.FRONT_API_TOKEN = 'tok';
    try {
        let r = await h.call('POST', '/api/v1/file-transfers', { supplierId: 11, filename: 'Artwork.pdf', contentType: 'application/pdf', fileSize: 16, note: 'Check the colours' });
        assert.equal(r.status, 201, JSON.stringify(r.body));
        const { transfer, upload } = r.body;
        assert.equal(transfer.status, 'draft');
        assert.equal(transfer.direction, 'to_supplier');
        assert.equal(transfer.sentBy, 'me@jfa.test');
        assert.equal(upload.method, 'PUT');
        assert.match(upload.url, /^https:\/\/jportal-pi-uploads-test\.s3\.eu-north-1\.amazonaws\.com\/files\/staff\/[0-9a-f-]{36}\.pdf\?/);
        assert.deepEqual(upload.headers, { 'Content-Type': 'application/pdf' });
        const row = h.db.transfers.find(x => x.id === transfer.id);
        assert.match(row.s3_key, /^files\/staff\/[0-9a-f-]{36}\.pdf$/);

        // Not there yet.
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/uploaded`, {});
        assert.equal(r.status, 409);
        assert.equal(r.body.code, 'FILE_MISSING');
        // A program: refused, the draft stays.
        h.objects.set(row.s3_key, { etag: '"e1"', bytes: MZ });
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/uploaded`, {});
        assert.equal(r.status, 422);
        assert.equal(r.body.code, 'FILE_BLOCKED');
        assert.equal(row.status, 'draft');
        // Too large.
        h.objects.set(row.s3_key, { etag: '"e2"', bytes: PK, size: L.MAX_FILE_BYTES + 1 });
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/uploaded`, {});
        assert.equal(r.status, 413);
        // The real thing.
        h.objects.set(row.s3_key, { etag: '"e3"', bytes: PK });
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/uploaded`, {});
        assert.equal(r.status, 200, JSON.stringify(r.body));
        assert.equal(r.body.transfer.status, 'sent');
        assert.equal(row.s3_etag, '"e3"');
        assert.deepEqual(r.body.notification, { sent: true, to: ['li@acme.test'] });
        assert.equal(r.body.transfer.notifiedAt !== null, true);
        assert.equal(sent.length, 1);
        assert.equal(sent[0]['to[]'], 'li@acme.test', 'the active portal user, never the disabled one');
        assert.equal(sent[0].subject, 'A file – Artwork.pdf');
        assert.match(fetchCalls[0].url, /channels\/cha_1\/messages/);
        assert.ok(h.db.audits.some(a => a.action === 'sent') && h.db.audits.some(a => a.action === 'notified'));
        // Listed on the Email Receipts page as a portal email: no confirm
        // button, no reminder (no action_url); opening the file confirms it.
        assert.deepEqual(h.db.receipts, [{
            email_type: 'file_transfer', send_table: 'file_transfers', send_id: transfer.id,
            sent_to: '["li@acme.test"]', subject: 'A file – Artwork.pdf', confirm_via: 'portal', action_url: null,
        }]);
        assert.doesNotMatch(String(fetchCalls[0].init.body.get('body')), /confirm-receipt/);

        // Again: idempotent, no second email.
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/uploaded`, {});
        assert.equal(r.status, 200);
        assert.equal(r.body.notification.reason, 'already_sent');
        assert.equal(sent.length, 1);

        // Notify: refused without force once sent; sent again with it.
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/notify`, {});
        assert.equal(r.status, 409);
        assert.equal(r.body.code, 'ALREADY_NOTIFIED');
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/notify`, { force: true });
        assert.equal(r.status, 200);
        assert.equal(sent.length, 2);

        // The supplier opened it: GET shows it.
        h.db.views.push({ transfer_id: transfer.id, viewer_kind: 'supplier', viewer_email: 'li@acme.test', first_viewed_at: new Date('2026-11-04T12:00:00Z'), last_viewed_at: new Date(), view_count: 1 });
        r = await h.call('GET', `/api/v1/file-transfers/${transfer.id}`);
        assert.equal(r.status, 200);
        assert.equal(r.body.transfer.seen, true);
        assert.deepEqual(r.body.transfer.views.map(v => v.email), ['li@acme.test']);
        assert.ok(r.body.file && /files\/staff\//.test(decodeURIComponent(r.body.file.url)));

        // Withdraw at the version seen.
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/withdraw`, { rowVersion: 0 });
        assert.equal(r.status, 409);
        assert.equal(r.body.code, 'STALE');
        r = await h.call('POST', `/api/v1/file-transfers/${transfer.id}/withdraw`, { rowVersion: r.body.rowVersion ?? row.row_version });
        assert.equal(r.status, 200, JSON.stringify(r.body));
        assert.equal(r.body.transfer.status, 'withdrawn');
        assert.equal(r.body.transfer.withdrawnBy, 'me@jfa.test');
    } finally {
        globalThis.fetch = realFetch;
        frontSend.sendFrontMessage = original;
    }
});

test('routes: no active portal user is recorded, not thrown; a draft can be discarded', async () => {
    const h = harness();
    process.env.PI_UPLOADS_BUCKET = 'jportal-pi-uploads-test';
    h.db.users = [];
    let r = await h.call('POST', '/api/v1/file-transfers', { supplierId: 11, filename: 'a.zip', fileSize: 16 });
    const id = r.body.transfer.id;
    const row = h.db.transfers.find(x => x.id === id);
    h.objects.set(row.s3_key, { etag: '"e"', bytes: PK });
    r = await h.call('POST', `/api/v1/file-transfers/${id}/uploaded`, {});
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.notification, { sent: false, to: [], reason: 'no_recipients' });
    assert.match(r.body.transfer.notifyError, /no_recipients/);

    r = await h.call('POST', '/api/v1/file-transfers', { supplierId: 11, filename: 'b.zip', fileSize: 16 });
    const draft = r.body.transfer;
    r = await h.call('POST', `/api/v1/file-transfers/${draft.id}/discard`, { rowVersion: 0 });
    assert.equal(r.status, 200);
    assert.equal(r.body.transfer.status, 'discarded');
    r = await h.call('GET', `/api/v1/file-transfers/${draft.id}`);
    assert.equal(r.status, 404, 'a discarded draft is nobody\'s');
    r = await h.call('POST', '/api/v1/file-transfers', { supplierId: 99, filename: 'c.zip', fileSize: 16 });
    assert.equal(r.status, 404);
    assert.equal(r.body.code, 'SUPPLIER_NOT_FOUND');
    r = await h.call('POST', '/api/v1/file-transfers', { supplierId: 11, filename: 'c.exe', fileSize: 16 });
    assert.equal(r.status, 422);
    assert.equal(r.body.code, 'FILE_BLOCKED');
});
