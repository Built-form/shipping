'use strict';

// /api/v1/container-photos — photos uploaded against one container (booked,
// or a draft), each with a description that can be picked from a list of
// pre-filled options ("Proof of cleaning", …) and edited.
//
// Upload is two steps, as for large packing lists: POST /upload-url returns a
// presigned S3 PUT (phone photos are routinely over the 6 MB API body cap),
// the browser PUTs the file, then POST /container-photos registers it with
// its description. Listing returns short-lived presigned URLs to show and to
// download each photo, so the page needs no extra request per image.
//
// A container is addressed exactly like a packing list (containerNumber —
// booked ref, carrier box, draft name or reserved number — or
// draftContainerId), and a draft's photos follow it into the booked
// container it becomes (packing-review.js containerScope).
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { PutObjectCommand, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const P = require('./packing-list-check');
const R = require('./packing-review');

const MAX_PHOTO_BYTES = 25 * 1024 * 1024;
const VIEW_URL_SECONDS = 3600;
const SAFE_FILENAME_RE = /[^A-Za-z0-9._-]+/g;
const PHOTO_KEY_RE = /^container-photos\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/[A-Za-z0-9._-]+$/;

const iso = v => v?.toISOString?.() ?? v ?? null;
const clip = (v, n) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, n) : null);

function registerContainerPhotoRoutes(app, deps) {
    const { withConnection, s3, poBucket, recordAudit, auditLogSchemaReady, log } = deps;

    async function photoToJson(r) {
        const safeName = String(r.filename || `photo-${r.id}`).replace(/"/g, '');
        const [url, downloadUrl] = await Promise.all([
            getSignedUrl(s3, new GetObjectCommand({
                Bucket: poBucket, Key: r.s3_key,
                ...(r.content_type ? { ResponseContentType: r.content_type } : {}),
            }), { expiresIn: VIEW_URL_SECONDS }),
            getSignedUrl(s3, new GetObjectCommand({
                Bucket: poBucket, Key: r.s3_key,
                ResponseContentDisposition: `attachment; filename="${safeName}"`,
            }), { expiresIn: VIEW_URL_SECONDS }),
        ]);
        return {
            id: r.id,
            containerKind: r.container_kind,
            containerNumber: r.container_number || null,
            draftContainerId: r.draft_container_id != null ? Number(r.draft_container_id) : null,
            containerName: r.container_name || null,
            filename: r.filename,
            contentType: r.content_type || null,
            fileSize: r.file_size != null ? Number(r.file_size) : null,
            preset: r.preset || null,
            description: r.description || null,
            uploadedByEmail: r.uploaded_by_email || null,
            createdAt: iso(r.created_at),
            updatedAt: iso(r.updated_at),
            updatedByEmail: r.updated_by_email || null,
            url,
            downloadUrl,
            urlExpiresInSeconds: VIEW_URL_SECONDS,
        };
    }

    const presetToJson = r => ({ id: r.id, label: r.label, sortOrder: Number(r.sort_order) || 0 });

    const sendFail = (res, f) => res.status(f.status).json({ error: f.error, code: f.code, ...(f.payload || {}) });

    // { containerNumber | draftContainerId } → a container, or a failure.
    // Unlike a packing list, a photo may go on a draft with no lines yet.
    async function resolveForRequest(conn, { containerNumber, draftContainerId }) {
        const typed = String(containerNumber ?? '').trim();
        if (!typed && (draftContainerId == null || draftContainerId === '')) {
            return { fail: { status: 400, error: 'containerNumber or draftContainerId is required.', code: 'BAD_FIELD' } };
        }
        const r = await P.resolveContainer(conn, { containerNumber: typed, draftContainerId });
        if (r && r.ambiguous) {
            return { fail: {
                status: 409, code: 'AMBIGUOUS_CONTAINER',
                error: `More than one open draft reserves ${typed}. Pick one and send its draftContainerId.`,
                payload: { candidates: r.ambiguous.map(c => ({ ...P.describeContainer(c), lineCount: c.lineCount, units: c.units })) },
            } };
        }
        if (!r) {
            return { fail: { status: 404, code: 'CONTAINER_NOT_FOUND',
                error: `No booked container or draft matches ${typed || `draft ${draftContainerId}`}.` } };
        }
        return { container: r.container };
    }

    // A live (not deleted) photo by id, for edits and deletes.
    async function loadPhoto(conn, id) {
        const [rows] = await conn.query(`SELECT * FROM container_photos WHERE id = ? AND deleted_at IS NULL`, [id]);
        return rows[0] || null;
    }

    // ── Presets ─────────────────────────────────────────────────────────────

    // GET /api/v1/container-photos/presets — the dropdown's options.
    app.get('/api/v1/container-photos/presets', async (req, res) => {
        try {
            const rows = await withConnection(async (conn) => {
                const [r] = await conn.query(
                    `SELECT id, label, sort_order FROM container_photo_presets
                      WHERE deleted_at IS NULL ORDER BY sort_order, label`
                );
                return r;
            });
            res.json({ data: rows.map(presetToJson) });
        } catch (error) {
            log.error('[GET /container-photos/presets]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/container-photos/presets  Body: { label }
    // Adds an option (or brings back a removed one with the same label).
    app.post('/api/v1/container-photos/presets', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const label = clip(req.body?.label, 100);
            if (!label) return res.status(400).json({ error: 'label is required.', code: 'BAD_FIELD' });
            const preset = await withConnection(async (conn) => {
                await conn.query(
                    `INSERT INTO container_photo_presets (label, sort_order, created_by_email)
                     VALUES (?, 1000, ?)
                     ON DUPLICATE KEY UPDATE deleted_at = NULL`,
                    [label, req.userEmail || null]
                );
                const [rows] = await conn.query(`SELECT id, label, sort_order FROM container_photo_presets WHERE label = ?`, [label]);
                await recordAudit(conn, {
                    entityType: 'container_photo_preset', entityId: rows[0].id, action: 'create',
                    before: null, after: { label }, userEmail: req.userEmail,
                });
                return presetToJson(rows[0]);
            });
            res.status(201).json({ preset });
        } catch (error) {
            log.error('[POST /container-photos/presets]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/container-photos/presets/:id — remove an option. Photos
    // keep the description they were given.
    app.delete('/api/v1/container-photos/presets/:id(\\d+)', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const ok = await withConnection(async (conn) => {
                const [rows] = await conn.query(`SELECT * FROM container_photo_presets WHERE id = ? AND deleted_at IS NULL`, [id]);
                if (!rows.length) return false;
                await conn.query(`UPDATE container_photo_presets SET deleted_at = NOW() WHERE id = ?`, [id]);
                await recordAudit(conn, {
                    entityType: 'container_photo_preset', entityId: id, action: 'delete',
                    before: { label: rows[0].label }, after: null, userEmail: req.userEmail,
                });
                return true;
            });
            if (!ok) return res.status(404).json({ error: `Option ${id} not found.`, code: 'NOT_FOUND' });
            res.status(204).end();
        } catch (error) {
            log.error('[DELETE /container-photos/presets/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // ── Photos ──────────────────────────────────────────────────────────────

    // GET /api/v1/container-photos?containerNumber=324 | ?draftContainerId=129
    // Oldest first (a loading sequence reads in order), each with view and
    // download URLs valid for an hour.
    app.get('/api/v1/container-photos', async (req, res) => {
        try {
            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, { containerNumber: req.query.containerNumber, draftContainerId: req.query.draftContainerId });
                if (r.fail) return r;
                const scope = await R.containerScope(conn, r.container);
                const [rows] = await conn.query(
                    `SELECT t.* FROM container_photos t
                      WHERE t.deleted_at IS NULL AND ${scope.sql}
                      ORDER BY t.created_at, t.id`,
                    scope.params
                );
                return { container: P.describeContainer(r.container), rows };
            });
            if (out.fail) return sendFail(res, out.fail);
            res.json({ container: out.container, data: await Promise.all(out.rows.map(photoToJson)) });
        } catch (error) {
            log.error('[GET /container-photos]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/container-photos/upload-url  Body: { filename, contentType }
    // PUT the photo to uploadUrl with uploadHeaders, then POST /container-photos.
    app.post('/api/v1/container-photos/upload-url', async (req, res) => {
        try {
            if (!poBucket) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
            const filename = clip(req.body?.filename, 200);
            if (!filename) return res.status(400).json({ error: 'filename is required.', code: 'BAD_FIELD' });
            const contentType = clip(req.body?.contentType, 100) || 'image/jpeg';
            if (!/^image\//i.test(contentType)) return res.status(400).json({ error: 'Only images can be uploaded as photos.', code: 'NOT_AN_IMAGE' });
            const s3Key = `container-photos/${uuidv4()}/${filename.replace(SAFE_FILENAME_RE, '_')}`;
            const uploadUrl = await getSignedUrl(
                s3, new PutObjectCommand({ Bucket: poBucket, Key: s3Key, ContentType: contentType }), { expiresIn: 600 }
            );
            res.status(201).json({
                uploadUrl, s3Key, uploadMethod: 'PUT', uploadHeaders: { 'Content-Type': contentType },
                expiresInSeconds: 600, maxBytes: MAX_PHOTO_BYTES,
            });
        } catch (error) {
            log.error('[POST /container-photos/upload-url]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/container-photos
    // Body: { containerNumber | draftContainerId, s3Key, filename, contentType?, preset?, description? }
    app.post('/api/v1/container-photos', async (req, res) => {
        try {
            await auditLogSchemaReady;
            if (!poBucket) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
            const b = req.body || {};
            const filename = clip(b.filename, 200);
            if (!filename) return res.status(400).json({ error: 'filename is required.', code: 'BAD_FIELD' });
            if (typeof b.s3Key !== 'string' || !PHOTO_KEY_RE.test(b.s3Key)) {
                return res.status(400).json({ error: 'An s3Key from /container-photos/upload-url is required.', code: 'BAD_FIELD' });
            }
            let head;
            try { head = await s3.send(new HeadObjectCommand({ Bucket: poBucket, Key: b.s3Key })); }
            catch { return res.status(409).json({ error: 'Photo not found in storage — PUT it to the uploadUrl first.', code: 'NOT_UPLOADED' }); }
            const size = Number(head.ContentLength) || 0;
            if (!size) return res.status(400).json({ error: 'The uploaded photo is empty.', code: 'BAD_FIELD' });
            if (size > MAX_PHOTO_BYTES) return res.status(413).json({ error: `Photo too large (max ${MAX_PHOTO_BYTES} bytes).`, code: 'TOO_LARGE' });
            const contentType = head.ContentType || clip(b.contentType, 100) || 'image/jpeg';
            const preset = clip(b.preset, 100);
            const description = clip(b.description, 1000) ?? preset;

            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, { containerNumber: b.containerNumber, draftContainerId: b.draftContainerId });
                if (r.fail) return r;
                const c = r.container;
                const [taken] = await conn.query(`SELECT id FROM container_photos WHERE s3_key = ? LIMIT 1`, [b.s3Key]);
                if (taken.length) return { fail: { status: 409, error: 'That photo is already registered.', code: 'DUPLICATE_UPLOAD', payload: { existingId: taken[0].id } } };
                const isDraft = c.kind === 'draft';
                const [ins] = await conn.query(
                    `INSERT INTO container_photos
                        (container_kind, container_number, draft_container_id, container_name,
                         filename, s3_key, content_type, file_size, preset, description, uploaded_by_email)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [c.kind, c.containerNumber || null, isDraft ? c.draftId : null, isDraft ? c.draftName : null,
                     filename, b.s3Key, contentType, size, preset, description, req.userEmail || null]
                );
                await recordAudit(conn, {
                    entityType: 'container_photo', entityId: ins.insertId, action: 'create', before: null,
                    after: { container: P.describeContainer(c), filename, preset, description },
                    userEmail: req.userEmail,
                });
                const [rows] = await conn.query(`SELECT * FROM container_photos WHERE id = ?`, [ins.insertId]);
                return { row: rows[0] };
            });
            if (out.fail) return sendFail(res, out.fail);
            res.status(201).json({ photo: await photoToJson(out.row) });
        } catch (error) {
            log.error('[POST /container-photos]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // PATCH /api/v1/container-photos/:id  Body: { preset?, description? }
    app.patch('/api/v1/container-photos/:id(\\d+)', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const b = req.body || {};
            const sets = [];
            const params = [];
            if ('preset' in b) { sets.push('preset = ?'); params.push(clip(b.preset, 100)); }
            if ('description' in b) { sets.push('description = ?'); params.push(clip(b.description, 1000)); }
            if (!sets.length) return res.status(400).json({ error: 'Nothing to change: send preset and/or description.', code: 'BAD_FIELD' });
            const out = await withConnection(async (conn) => {
                const before = await loadPhoto(conn, id);
                if (!before) return null;
                await conn.query(
                    `UPDATE container_photos SET ${sets.join(', ')}, updated_by_email = ? WHERE id = ?`,
                    [...params, req.userEmail || null, id]
                );
                const after = await loadPhoto(conn, id);
                await recordAudit(conn, {
                    entityType: 'container_photo', entityId: id, action: 'update',
                    before: { preset: before.preset, description: before.description },
                    after: { preset: after.preset, description: after.description },
                    userEmail: req.userEmail,
                });
                return after;
            });
            if (!out) return res.status(404).json({ error: `Photo ${id} not found.`, code: 'NOT_FOUND' });
            res.json({ photo: await photoToJson(out) });
        } catch (error) {
            log.error('[PATCH /container-photos/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/container-photos/:id — soft delete; the file stays in S3.
    app.delete('/api/v1/container-photos/:id(\\d+)', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const ok = await withConnection(async (conn) => {
                const row = await loadPhoto(conn, id);
                if (!row) return false;
                await conn.query(`UPDATE container_photos SET deleted_at = NOW(), deleted_by_email = ? WHERE id = ?`, [req.userEmail || null, id]);
                await recordAudit(conn, {
                    entityType: 'container_photo', entityId: id, action: 'delete',
                    before: { filename: row.filename, preset: row.preset, description: row.description }, after: null,
                    userEmail: req.userEmail,
                });
                return true;
            });
            if (!ok) return res.status(404).json({ error: `Photo ${id} not found.`, code: 'NOT_FOUND' });
            res.status(204).end();
        } catch (error) {
            log.error('[DELETE /container-photos/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerContainerPhotoRoutes };
