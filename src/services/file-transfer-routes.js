'use strict';

// /api/v1/file-transfers — files sent between a supplier and our team through
// the supplier portal (jportal.file_transfers; portal README section 18). Two
// directions:
//   from_supplier  a supplier's user uploaded it on the portal's Files page; it
//                  is listed here (SUPPLIER PORTAL > Files) until someone opens
//                  it, and stays for the record. Opening it (POST …/open) signs
//                  a download of the portal's FINAL copy and records a staff view
//                  (jportal.file_transfer_views, viewer_kind 'staff'), which the
//                  portal shows the supplier as "opened by our team".
//   to_supplier    a staff member sends one: POST creates a draft row and a
//                  presigned PUT into the portal's bucket (files/staff/<uuid>.<ext>),
//                  ShipLine PUTs the bytes straight to S3 (the bucket's CORS rule
//                  allows ShipLine's hosts), and `uploaded` checks the object
//                  (size, first bytes: a program is refused), pins its ETag,
//                  publishes the row (`sent`) and emails the supplier's active
//                  portal users through Front (template file_transfer_portal, a
//                  link to the portal's Files page). The supplier downloads it on
//                  the portal; their views come back here as `seen` / `views`.
// Nothing reads a transferred file: no check, no import, no Gemini.
// Rules: src/lib/file-transfers.js; frontend guide: docs/file-transfers-frontend.md.
//
// jportal is another schema on the same MySQL server (JPORTAL_DB_SCHEMA); this
// app's login is admin there, so no grants are kept. The portal's own login may
// never touch a to_supplier row's status (its grants, db/grants/jportal_app.sql
// there) and this app never moves a from_supplier row: each side owns its rows.
// The bucket is the portal's (PI_UPLOADS_BUCKET): PutObject + GetObject on
// files/staff/*, GetObject on files/sup/* (serverless.yml).
//
// Writes: a plain UPDATE guarded on the status (and rowVersion where the client
// saw one); the two sides never race on the same row, so no locking read is
// needed beyond that. S3 is only ever called outside a transaction.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { HeadObjectCommand, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const L = require('../lib/file-transfers');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const { sendFrontMessage } = require('./front-send');

const UPLOAD_URL_SECONDS = 15 * 60;
const FILE_URL_SECONDS = 15 * 60;
/** A draft still waiting for its file after this long is abandoned: no fresh PUT for it. */
const UPLOAD_WINDOW_SECONDS = 3600;
/** The first bytes read back to tell a program from a document. */
const SNIFF_BYTES = 16;

const isMissing = e => Boolean(e) && (e.name === 'NoSuchKey' || e.name === 'NotFound' || (e.$metadata && e.$metadata.httpStatusCode === 404));
const isPreconditionFailed = e => Boolean(e) && (e.name === 'PreconditionFailed' || (e.$metadata && e.$metadata.httpStatusCode === 412));

const CANNOT_DECIDE = userType => ({
    status: 403, code: 'CANNOT_DECIDE',
    error: `Your role (${userType || 'none'}) cannot do this — admin and standard users send, withdraw and open supplier files.`,
});
const UNAVAILABLE = { status: 503, code: 'PORTAL_UNAVAILABLE', error: 'The supplier portal’s file transfers are not available on this server yet.' };
const notFound = id => ({ status: 404, code: 'NOT_FOUND', error: `File transfer ${id} not found.` });

function registerFileTransferRoutes(app, deps) {
    const { withConnection, ready, s3, recordAudit, getEmailTemplate, renderTemplate, log } = deps;

    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.extra || {}) });
    const bucket = () => process.env.PI_UPLOADS_BUCKET || null;
    const jp = () => jportalSchema();

    // Every column of the row, the timestamps as ISO UTC strings, the live
    // supplier name (else the snapshot), and the other side's views: when the
    // first one was, and how many people. Read on every call.
    const selectSql = () => `SELECT r.*,
                       DATE_FORMAT(r.sent_at, '%Y-%m-%dT%H:%i:%sZ') AS sent_at_iso,
                       DATE_FORMAT(r.withdrawn_at, '%Y-%m-%dT%H:%i:%sZ') AS withdrawn_at_iso,
                       DATE_FORMAT(r.notified_at, '%Y-%m-%dT%H:%i:%sZ') AS notified_at_iso,
                       DATE_FORMAT(r.created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at_iso,
                       s.name AS live_supplier_name,
                       (s.id IS NOT NULL) AS supplier_exists,
                       (SELECT DATE_FORMAT(MIN(v.first_viewed_at), '%Y-%m-%dT%H:%i:%sZ') FROM ${jp()}.file_transfer_views v
                         WHERE v.transfer_id = r.id AND v.viewer_kind = 'staff') AS staff_first_viewed_at_iso,
                       (SELECT COUNT(*) FROM ${jp()}.file_transfer_views v
                         WHERE v.transfer_id = r.id AND v.viewer_kind = 'staff') AS staff_viewers,
                       (SELECT DATE_FORMAT(MIN(v.first_viewed_at), '%Y-%m-%dT%H:%i:%sZ') FROM ${jp()}.file_transfer_views v
                         WHERE v.transfer_id = r.id AND v.viewer_kind = 'supplier') AS supplier_first_viewed_at_iso,
                       (SELECT COUNT(*) FROM ${jp()}.file_transfer_views v
                         WHERE v.transfer_id = r.id AND v.viewer_kind = 'supplier') AS supplier_viewers
                  FROM ${jp()}.file_transfers r
                  LEFT JOIN suppliers s ON s.id = r.supplier_id AND s.deleted_at IS NULL`;

    // One row by id. `any` includes this app's own drafts (the send flow); the
    // default is what staff are shown (sent and withdrawn, either direction).
    const readRow = async (conn, id, { any = false } = {}) => {
        const where = any
            ? `WHERE r.id = ? AND NOT (r.direction = 'from_supplier' AND r.status NOT IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')}))`
            : `WHERE r.id = ? AND r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})`;
        const [rows] = await conn.query(`${selectSql()} ${where}`, [id, ...L.VISIBLE_STATUSES]);
        return rows[0] || null;
    };

    const readViews = async (conn, id) => {
        const [rows] = await conn.query(
            `SELECT viewer_kind, viewer_email, first_viewed_at, last_viewed_at, view_count
               FROM ${jp()}.file_transfer_views WHERE transfer_id = ? ORDER BY first_viewed_at, id`,
            [id]
        );
        return rows.map(L.viewToJson);
    };

    const toJson = (row, extra) => L.transferToJson(row, extra);

    // A 15-minute signed download of the row's readable key, served as an
    // attachment under the file's own name. → { url, expiresAt, state: { gone, changed } };
    // url null when there is nothing to read or the object is gone.
    const fileLink = async row => {
        const state = { gone: false, changed: false };
        const readable = L.readableKey(row);
        const b = bucket();
        if (!b || !readable) return { url: null, expiresAt: null, state };
        const fallback = `file-${row.id}.${L.storageExtension(row.filename)}`;
        try {
            const head = await s3.send(new HeadObjectCommand({ Bucket: b, Key: readable.key }));
            if (head.ETag && head.ETag !== readable.etag) {
                state.changed = true;
                return { url: null, expiresAt: null, state };
            }
            const url = await getSignedUrl(s3, new GetObjectCommand({
                Bucket: b, Key: readable.key,
                ResponseContentType: L.OCTET_STREAM,
                ResponseContentDisposition: L.contentDisposition(row.filename, fallback),
                ResponseCacheControl: 'private, no-store',
            }), { expiresIn: FILE_URL_SECONDS });
            return { url, expiresAt: new Date(Date.now() + FILE_URL_SECONDS * 1000).toISOString(), state };
        } catch (e) {
            if (isMissing(e)) state.gone = true;
            else log.warn('[file-transfers] file lookup failed', { id: row.id, error: e.message });
            return { url: null, expiresAt: null, state };
        }
    };

    // The signed PUT for a draft's key: content type and length signed in, so
    // the browser must send exactly what it declared. An absolute S3 URL:
    // ShipLine PUTs straight to the bucket (its CORS rule names ShipLine's hosts).
    const uploadTicket = async row => {
        const url = await getSignedUrl(s3, new PutObjectCommand({
            Bucket: bucket(), Key: row.s3_key, ContentType: row.content_type, ContentLength: Number(row.file_size),
        }), { expiresIn: UPLOAD_URL_SECONDS, signableHeaders: new Set(['content-type', 'content-length']) });
        return {
            url, method: 'PUT', headers: { 'Content-Type': row.content_type },
            expiresAt: new Date(Date.now() + UPLOAD_URL_SECONDS * 1000).toISOString(),
        };
    };

    // The supplier's active portal users: who the "a file is waiting" email goes to.
    const recipientsFor = async (conn, supplierId) => {
        const [rows] = await conn.query(
            `SELECT DISTINCT u.email FROM ${jp()}.supplier_users u
              INNER JOIN ${jp()}.user_supplier_access a ON a.user_id = u.id
              WHERE a.supplier_id = ? AND u.status = 'active'
              ORDER BY u.email`,
            [supplierId]
        );
        return rows.map(r => String(r.email || '').trim().toLowerCase()).filter(Boolean);
    };

    /**
     * Emails the supplier's active portal users that a file is waiting
     * (template file_transfer_portal) through Front. On test,
     * PI_NOTIFY_REDIRECT_TO sends it there instead. Sets notified_at, or
     * notify_error when the send failed or nobody could be emailed, and audits.
     * Never throws. → { sent, to, reason? }
     */
    const notifySupplier = async (conn, row, userEmail = null) => {
        const id = Number(row.id);
        let to = [];
        try {
            const original = await recipientsFor(conn, Number(row.supplier_id));
            const redirect = String(process.env.PI_NOTIFY_REDIRECT_TO || '').trim();
            to = redirect ? [redirect] : original;
            if (!to.length) {
                await conn.query(`UPDATE ${jp()}.file_transfers SET notify_error = ? WHERE id = ?`, ['no_recipients: the supplier has no active portal user', id]);
                await recordAudit(conn, { entityType: 'file_transfer', entityId: id, action: 'notify_failed', before: null, after: { reason: 'no_recipients' }, userEmail });
                return { sent: false, to, reason: 'no_recipients' };
            }
            const tpl = await getEmailTemplate(conn, L.TEMPLATE_KEY);
            const vars = L.buildEmailVars({ row, portalUrl: process.env.SUPPLIER_PORTAL_URL || '' });
            let subject = renderTemplate(tpl.subject, vars.subject) || `A file from JFA Medical – ${vars.subject.filename}`;
            if (redirect) subject = `[for ${original.join(', ') || 'nobody'}] ${subject}`;
            const html = renderTemplate(tpl.bodyHtml, vars.body);
            const sent = await sendFrontMessage({ to, subject, html });
            if (sent.ok) {
                await conn.query(`UPDATE ${jp()}.file_transfers SET notified_at = UTC_TIMESTAMP(), notify_error = NULL WHERE id = ?`, [id]);
            } else {
                const why = [sent.reason, sent.status, sent.detail].filter(v => v != null && v !== '').join(': ').slice(0, 500);
                await conn.query(`UPDATE ${jp()}.file_transfers SET notify_error = ? WHERE id = ?`, [why, id]);
            }
            await recordAudit(conn, {
                entityType: 'file_transfer', entityId: id, action: sent.ok ? 'notified' : 'notify_failed', before: null,
                after: {
                    template: L.TEMPLATE_KEY, to, ...(redirect ? { redirectedFrom: original } : {}),
                    ...(sent.ok ? { frontMessageUid: sent.frontMessageUid || null } : { reason: sent.reason, frontStatus: sent.status ?? null }),
                },
                userEmail,
            });
            return sent.ok ? { sent: true, to } : { sent: false, to, reason: sent.reason };
        } catch (e) {
            log.warn('[file-transfers] notify failed', { id, error: e.message });
            try {
                await conn.query(`UPDATE ${jp()}.file_transfers SET notify_error = ? WHERE id = ?`, [`error: ${e.message}`.slice(0, 500), id]);
            } catch { /* nothing more to do */ }
            return { sent: false, to, reason: 'error' };
        }
    };

    const handle = (site, fn) => async (req, res) => {
        try {
            await ready();
            await fn(req, res);
        } catch (error) {
            if (isSourceMissing(error)) return refuse(res, UNAVAILABLE);
            log.error(`[${site}]`, error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };

    // GET /api/v1/file-transfers/suppliers — every supplier with a file either
    // way that staff can see: how many of the supplier's files nobody here has
    // opened yet (pending), how many files in all, and the oldest unopened.
    // Suppliers with pending files first, oldest first.
    // 200 { data: FileTransferSupplierSummary[] }
    app.get('/api/v1/file-transfers/suppliers', handle('GET /file-transfers/suppliers', async (req, res) => {
        const data = await withConnection(async conn => {
            const [rows] = await conn.query(
                `SELECT r.supplier_id, r.supplier_name, r.direction, r.status, s.name AS live_supplier_name, (s.id IS NOT NULL) AS supplier_exists,
                        DATE_FORMAT(r.sent_at, '%Y-%m-%dT%H:%i:%sZ') AS sent_at_iso,
                        EXISTS (SELECT 1 FROM ${jp()}.file_transfer_views v WHERE v.transfer_id = r.id AND v.viewer_kind = 'staff') AS staff_seen
                   FROM ${jp()}.file_transfers r
                   LEFT JOIN suppliers s ON s.id = r.supplier_id AND s.deleted_at IS NULL
                  WHERE r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})
                  ORDER BY r.sent_at, r.id`,
                L.VISIBLE_STATUSES
            );
            const groups = new Map();
            for (const r of rows) {
                const sid = Number(r.supplier_id);
                let g = groups.get(`s:${sid}`);
                if (!g) {
                    g = {
                        supplierKey: `s:${sid}`, supplierId: sid, supplierName: r.live_supplier_name || r.supplier_name || `Supplier ${sid}`,
                        supplierExists: Number(r.supplier_exists) === 1, pending: 0, total: 0, fromSupplier: 0, toSupplier: 0, oldestPendingAt: null,
                    };
                    groups.set(g.supplierKey, g);
                }
                g.total += 1;
                if (r.direction === 'from_supplier') g.fromSupplier += 1; else g.toSupplier += 1;
                if (r.direction === 'from_supplier' && r.status === 'sent' && Number(r.staff_seen) !== 1) {
                    g.pending += 1;
                    if (r.sent_at_iso && (!g.oldestPendingAt || r.sent_at_iso < g.oldestPendingAt)) g.oldestPendingAt = r.sent_at_iso;
                }
            }
            return [...groups.values()].sort((a, b) => (b.pending > 0) - (a.pending > 0)
                || String(a.oldestPendingAt || '~').localeCompare(String(b.oldestPendingAt || '~'))
                || String(a.supplierName).localeCompare(String(b.supplierName)));
        });
        res.json({ data });
    }));

    // GET /api/v1/file-transfers?status=&direction=&supplierId=&unopened=&limit=&offset=
    // status: open (= sent, the default), history (= withdrawn), all, or
    // sent / withdrawn comma-separated. direction: from_supplier | to_supplier
    // (absent: both). unopened=1: only supplier files nobody here has opened.
    // Newest sent first. Drafts and discarded rows are never listed.
    // 200 { data: FileTransfer[], hasMore }
    app.get('/api/v1/file-transfers', handle('GET /file-transfers', async (req, res) => {
        const parsed = L.parseListQuery(req.query || {});
        if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
        const q = parsed.value;
        const where = [`r.status IN (${q.statuses.map(() => '?').join(', ')})`];
        const params = [...q.statuses];
        if (q.direction) { where.push('r.direction = ?'); params.push(q.direction); }
        if (q.supplierId) { where.push('r.supplier_id = ?'); params.push(q.supplierId); }
        if (q.unopened) {
            where.push(`r.direction = 'from_supplier' AND r.status = 'sent'
                        AND NOT EXISTS (SELECT 1 FROM ${jp()}.file_transfer_views v WHERE v.transfer_id = r.id AND v.viewer_kind = 'staff')`);
        }
        const out = await withConnection(async conn => {
            const [rows] = await conn.query(
                `${selectSql()} WHERE ${where.join(' AND ')} ORDER BY r.sent_at DESC, r.id DESC LIMIT ${q.limit + 1} OFFSET ${q.offset}`,
                params
            );
            return { data: rows.slice(0, q.limit).map(row => toJson(row)), hasMore: rows.length > q.limit };
        });
        res.json(out);
    }));

    // GET /api/v1/file-transfers/:id — the row with who opened it (`views`),
    // and a 15-minute download link (`file`; null when there is nothing to
    // read). Records nothing: use POST …/open to count as having opened it.
    // 200 { transfer, file: { url, expiresAt } | null, fileState } · 404
    app.get('/api/v1/file-transfers/:id(\\d+)', handle('GET /file-transfers/:id', async (req, res) => {
        const id = Number(req.params.id);
        const out = await withConnection(async conn => {
            const row = await readRow(conn, id);
            if (!row) return { fail: notFound(id) };
            const views = await readViews(conn, id);
            return { row, views };
        });
        if (out.fail) return refuse(res, out.fail);
        const link = await fileLink(out.row);
        res.json({
            transfer: toJson(out.row, { views: out.views }),
            file: link.url ? { url: link.url, expiresAt: link.expiresAt } : null,
            fileState: link.state,
        });
    }));

    // POST /api/v1/file-transfers/:id/open — "I am opening this": for a
    // supplier's file, records this user's view (the portal then shows "opened
    // by our team"), and answers as GET …/:id does, with the download link.
    // For a file we sent, just the link again. Admin and standard users.
    // 200 { transfer, file, fileState } · 403 · 404
    app.post('/api/v1/file-transfers/:id(\\d+)/open', handle('POST /file-transfers/:id/open', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const id = Number(req.params.id);
        const out = await withConnection(async conn => {
            const row = await readRow(conn, id);
            if (!row) return { fail: notFound(id) };
            if (row.direction === 'from_supplier') {
                await conn.query(
                    `INSERT INTO ${jp()}.file_transfer_views (transfer_id, viewer_kind, viewer_email)
                     VALUES (?, 'staff', ?)
                     ON DUPLICATE KEY UPDATE last_viewed_at = UTC_TIMESTAMP(), view_count = view_count + 1`,
                    [id, String(req.userEmail || '').toLowerCase()]
                );
            }
            const fresh = await readRow(conn, id);
            return { row: fresh || row, views: await readViews(conn, id) };
        });
        if (out.fail) return refuse(res, out.fail);
        const link = await fileLink(out.row);
        res.json({
            transfer: toJson(out.row, { views: out.views }),
            file: link.url ? { url: link.url, expiresAt: link.expiresAt } : null,
            fileState: link.state,
        });
    }));

    // POST /api/v1/file-transfers — body { supplierId, filename, contentType,
    // fileSize, note? }. A draft row for the supplier and a presigned PUT into
    // the portal's bucket; the browser then PUTs the bytes and calls
    // …/uploaded. Admin and standard users.
    // 201 { transfer, upload: { url, method, headers, expiresAt } }
    // 400 BAD_FIELD · 403 · 404 SUPPLIER_NOT_FOUND · 413 FILE_TOO_LARGE · 422 FILE_BLOCKED
    app.post('/api/v1/file-transfers', handle('POST /file-transfers', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const parsed = L.parseCreateBody(req.body || {});
        if (parsed.error) {
            const status = parsed.code === 'FILE_TOO_LARGE' ? 413 : parsed.code === 'FILE_BLOCKED' ? 422 : 400;
            return refuse(res, { status, code: parsed.code, error: parsed.error });
        }
        if (!bucket()) return res.status(500).json({ error: 'PI_UPLOADS_BUCKET is not configured.' });
        const v = parsed.value;
        const key = L.newStaffKey(uuidv4(), v.filename);
        const out = await withConnection(async conn => {
            const [sup] = await conn.query(`SELECT id, name FROM suppliers WHERE id = ? AND deleted_at IS NULL`, [v.supplierId]);
            if (!sup.length) return { fail: { status: 404, code: 'SUPPLIER_NOT_FOUND', error: `Supplier ${v.supplierId} not found.` } };
            const [ins] = await conn.query(
                `INSERT INTO ${jp()}.file_transfers
                    (direction, supplier_id, supplier_name, user_id, sender_email, filename, content_type, file_size, s3_key, note, status)
                 VALUES ('to_supplier', ?, ?, NULL, ?, ?, ?, ?, ?, ?, 'draft')`,
                [v.supplierId, String(sup[0].name || '').slice(0, 255), String(req.userEmail || '').toLowerCase(), v.filename, v.contentType, v.fileSize, key, v.note]
            );
            const id = Number(ins.insertId);
            await recordAudit(conn, {
                entityType: 'file_transfer', entityId: id, action: 'created', before: null,
                after: { direction: 'to_supplier', supplierId: v.supplierId, filename: v.filename, contentType: v.contentType, fileSize: v.fileSize, s3Key: key, note: v.note },
                userEmail: req.userEmail,
            });
            return { row: await readRow(conn, id, { any: true }) };
        });
        if (out.fail) return refuse(res, out.fail);
        const upload = await uploadTicket(out.row);
        res.status(201).json({ transfer: toJson(out.row), upload });
    }));

    // POST /api/v1/file-transfers/:id/upload-url — the signed PUT again for a
    // draft still waiting for its file within its first hour (the first link
    // expired: 15 minutes, and a big file on a slow line can need more).
    // 200 { upload } · 403 · 404 · 409 NOT_DRAFT | DRAFT_EXPIRED
    app.post('/api/v1/file-transfers/:id(\\d+)/upload-url', handle('POST /file-transfers/:id/upload-url', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const id = Number(req.params.id);
        const row = await withConnection(conn => readRow(conn, id, { any: true }));
        if (!row || row.direction !== 'to_supplier') return refuse(res, notFound(id));
        if (row.status !== 'draft' || row.s3_etag) return refuse(res, { status: 409, code: 'NOT_DRAFT', error: 'This file has already been sent.', extra: { status: row.status } });
        if (row.created_at && Date.now() - new Date(row.created_at).getTime() > UPLOAD_WINDOW_SECONDS * 1000) {
            return refuse(res, { status: 409, code: 'DRAFT_EXPIRED', error: 'This upload was started over an hour ago; start it again.' });
        }
        res.json({ upload: await uploadTicket(row) });
    }));

    // POST /api/v1/file-transfers/:id/uploaded — the PUT returned 2xx: check
    // the object (it is there, not empty, not over 250 MB, and its first bytes
    // are not a program), pin its ETag, publish the row and email the supplier.
    // Idempotent: a row already sent answers as it is, with no email.
    // 200 { transfer, notification: { sent, to, reason? } }
    // 403 · 404 · 409 FILE_MISSING | FILE_CHANGED · 413 FILE_TOO_LARGE · 422 FILE_BLOCKED · 502 FILE_UNAVAILABLE
    app.post('/api/v1/file-transfers/:id(\\d+)/uploaded', handle('POST /file-transfers/:id/uploaded', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const id = Number(req.params.id);
        const row = await withConnection(conn => readRow(conn, id, { any: true }));
        if (!row || row.direction !== 'to_supplier') return refuse(res, notFound(id));
        if (row.status === 'sent') return res.json({ transfer: toJson(row), notification: { sent: Boolean(row.notified_at), to: [], reason: 'already_sent' } });
        if (row.status !== 'draft') return refuse(res, { status: 409, code: 'NOT_DRAFT', error: `This file is ${row.status}.`, extra: { status: row.status } });
        const b = bucket();
        if (!b) return res.status(500).json({ error: 'PI_UPLOADS_BUCKET is not configured.' });

        // The object, outside any transaction.
        let head;
        try {
            head = await s3.send(new HeadObjectCommand({ Bucket: b, Key: row.s3_key }));
        } catch (e) {
            if (isMissing(e)) return refuse(res, { status: 409, code: 'FILE_MISSING', error: 'The file has not arrived in the bucket; upload it again.' });
            log.warn('[file-transfers/uploaded] head failed', { id, error: e.message });
            return refuse(res, { status: 502, code: 'FILE_UNAVAILABLE', error: 'The portal’s bucket could not be read; try again.' });
        }
        const size = Number(head.ContentLength || 0);
        const etag = typeof head.ETag === 'string' ? head.ETag : null;
        if (!etag || size <= 0) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'The uploaded file is empty.' });
        if (size > L.MAX_FILE_BYTES) return refuse(res, { status: 413, code: 'FILE_TOO_LARGE', error: `Files can be at most ${L.MAX_FILE_BYTES} bytes (250 MB).` });
        let bytes;
        try {
            const obj = await s3.send(new GetObjectCommand({ Bucket: b, Key: row.s3_key, Range: `bytes=0-${SNIFF_BYTES - 1}`, IfMatch: etag }));
            bytes = await obj.Body.transformToByteArray();
        } catch (e) {
            if (isMissing(e) || isPreconditionFailed(e)) return refuse(res, { status: 409, code: 'FILE_CHANGED', error: 'The file changed while it was being checked; upload it again.' });
            log.warn('[file-transfers/uploaded] read failed', { id, error: e.message });
            return refuse(res, { status: 502, code: 'FILE_UNAVAILABLE', error: 'The portal’s bucket could not be read; try again.' });
        }
        if (L.sniffExecutable(bytes)) {
            return refuse(res, { status: 422, code: 'FILE_BLOCKED', error: 'This file is a program; it cannot be sent. Discard it, or zip it and send that.' });
        }

        const out = await withConnection(async conn => {
            const [upd] = await conn.query(
                `UPDATE ${jp()}.file_transfers
                    SET status = 'sent', s3_etag = ?, file_size = ?, sent_at = UTC_TIMESTAMP(), row_version = row_version + 1
                  WHERE id = ? AND direction = 'to_supplier' AND status = 'draft'`,
                [etag, size, id]
            );
            const fresh = await readRow(conn, id, { any: true });
            if (!upd.affectedRows) {
                // Published meanwhile (a double click): answer with the row as it is.
                return { row: fresh, notification: { sent: Boolean(fresh && fresh.notified_at), to: [], reason: 'already_sent' } };
            }
            await recordAudit(conn, {
                entityType: 'file_transfer', entityId: id, action: 'sent', before: { status: 'draft' },
                after: { status: 'sent', fileSize: size, s3Etag: etag }, userEmail: req.userEmail,
            });
            const notification = await notifySupplier(conn, fresh, req.userEmail);
            return { row: await readRow(conn, id, { any: true }), notification };
        });
        res.json({ transfer: toJson(out.row), notification: out.notification });
    }));

    // POST /api/v1/file-transfers/:id/notify — body { force? }. (Re)sends the
    // "a file is waiting" email of a sent file. Allowed whenever notified_at is
    // unset, whatever notify_error says; force when it was already sent.
    // 200 { transfer, notification } · 403 · 404 · 409 NOT_SENT | ALREADY_NOTIFIED
    app.post('/api/v1/file-transfers/:id(\\d+)/notify', handle('POST /file-transfers/:id/notify', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const id = Number(req.params.id);
        const force = Boolean(req.body && req.body.force === true);
        const out = await withConnection(async conn => {
            const row = await readRow(conn, id);
            if (!row || row.direction !== 'to_supplier') return { fail: notFound(id) };
            if (row.status !== 'sent') return { fail: { status: 409, code: 'NOT_SENT', error: `This file is ${row.status}; only a sent one is emailed.`, extra: { status: row.status } } };
            if (row.notified_at && !force) {
                return { fail: { status: 409, code: 'ALREADY_NOTIFIED', error: 'The supplier was already emailed; send again with force.', extra: { notifiedAt: row.notified_at_iso || null } } };
            }
            const notification = await notifySupplier(conn, row, req.userEmail);
            return { row: await readRow(conn, id), notification };
        });
        if (out.fail) return refuse(res, out.fail);
        res.json({ transfer: toJson(out.row), notification: out.notification });
    }));

    // POST /api/v1/file-transfers/:id/withdraw — body { rowVersion }. Takes
    // back a file we sent: it disappears from the supplier's list (the object
    // stays; nothing deletes under files/). Admin and standard users.
    // 200 { transfer } · 400 · 403 · 404 · 409 NOT_SENT | STALE
    app.post('/api/v1/file-transfers/:id(\\d+)/withdraw', handle('POST /file-transfers/:id/withdraw', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const id = Number(req.params.id);
        const rv = L.parseRowVersion(req.body && req.body.rowVersion);
        if (rv.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: rv.error });
        const out = await withConnection(async conn => {
            const row = await readRow(conn, id);
            if (!row || row.direction !== 'to_supplier') return { fail: notFound(id) };
            if (row.status !== 'sent') return { fail: { status: 409, code: 'NOT_SENT', error: `This file is ${row.status}.`, extra: { status: row.status } } };
            if (Number(row.row_version) !== rv.value) return { fail: { status: 409, code: 'STALE', error: 'This file changed since it was loaded; reload.' } };
            const [upd] = await conn.query(
                `UPDATE ${jp()}.file_transfers
                    SET status = 'withdrawn', withdrawn_at = UTC_TIMESTAMP(), withdrawn_by = ?, row_version = row_version + 1
                  WHERE id = ? AND status = 'sent' AND row_version = ?`,
                [String(req.userEmail || '').toLowerCase(), id, rv.value]
            );
            if (!upd.affectedRows) return { fail: { status: 409, code: 'STALE', error: 'This file changed since it was loaded; reload.' } };
            await recordAudit(conn, { entityType: 'file_transfer', entityId: id, action: 'withdrawn', before: { status: 'sent' }, after: { status: 'withdrawn' }, userEmail: req.userEmail });
            return { row: await readRow(conn, id) };
        });
        if (out.fail) return refuse(res, out.fail);
        res.json({ transfer: toJson(out.row) });
    }));

    // POST /api/v1/file-transfers/:id/discard — body { rowVersion }. Drops one
    // of our drafts (the upload failed, or was refused as a program). The
    // object, if any, stays under files/staff/ unreferenced (known limit).
    // 200 { transfer } · 400 · 403 · 404 · 409 NOT_DRAFT | STALE
    app.post('/api/v1/file-transfers/:id(\\d+)/discard', handle('POST /file-transfers/:id/discard', async (req, res) => {
        if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
        const id = Number(req.params.id);
        const rv = L.parseRowVersion(req.body && req.body.rowVersion);
        if (rv.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: rv.error });
        const out = await withConnection(async conn => {
            const row = await readRow(conn, id, { any: true });
            if (!row || row.direction !== 'to_supplier') return { fail: notFound(id) };
            if (row.status !== 'draft') return { fail: { status: 409, code: 'NOT_DRAFT', error: `This file is ${row.status}.`, extra: { status: row.status } } };
            if (Number(row.row_version) !== rv.value) return { fail: { status: 409, code: 'STALE', error: 'This file changed since it was loaded; reload.' } };
            const [upd] = await conn.query(
                `UPDATE ${jp()}.file_transfers SET status = 'discarded', row_version = row_version + 1
                  WHERE id = ? AND status = 'draft' AND row_version = ?`,
                [id, rv.value]
            );
            if (!upd.affectedRows) return { fail: { status: 409, code: 'STALE', error: 'This file changed since it was loaded; reload.' } };
            await recordAudit(conn, { entityType: 'file_transfer', entityId: id, action: 'discarded', before: { status: 'draft' }, after: { status: 'discarded' }, userEmail: req.userEmail });
            return { row: await readRow(conn, id, { any: true }) };
        });
        if (out.fail) return refuse(res, out.fail);
        res.json({ transfer: toJson(out.row) });
    }));
}

module.exports = { registerFileTransferRoutes, UPLOAD_URL_SECONDS, FILE_URL_SECONDS };
