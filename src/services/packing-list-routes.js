'use strict';

// /api/v1/packing-lists — upload a supplier's packing list against a container,
// have Gemini read it, and compare every row with the orders we expect from
// that supplier in that container. See src/services/packing-list-check.js for
// the reading and the comparison, docs/packing-lists-frontend.md for the contract.
//
// Flow: GET /context?containerNumber=324|328 (the container — booked or draft —
// and the supplier picker) → POST with the file (201, reading starts in the
// background) → poll GET /:id until status is 'succeeded' | 'failed'. GET /:id
// recomputes the comparison against the container's lines as they are NOW, so
// fixing an order or an allocation and reloading shows the difference gone
// without re-reading the PDF.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { PutObjectCommand, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const P = require('./packing-list-check');
const R = require('./packing-review');

// Two ways in. dataBase64 in the JSON body is capped at 4 MB: base64 grows it
// by a third and the HTTP API / Lambda request cap is 6 MB, beyond which the
// gateway answers a bare 413 before we ever see it. A bigger (scanned) list
// goes up via POST /upload-url (a presigned S3 PUT, as QC reports do) and is
// then registered with its s3Key — up to 18 MB, what Gemini takes inline.
const MAX_INLINE_BYTES = 4 * 1024 * 1024;
const MAX_S3_BYTES = 18 * 1024 * 1024;
const SAFE_FILENAME_RE = /[^A-Za-z0-9._-]+/g;
const UPLOAD_KEY_RE = /^packing-lists\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\/[A-Za-z0-9._-]+$/;

function registerPackingListRoutes(app, deps) {
    const { pool, withConnection, s3, poBucket, recordAudit, auditLogSchemaReady, log } = deps;

    // No cold-start DDL: packing_lists comes from the deploy-time migration
    // src/db/migrate/2026-09-21_50_packing_lists.sql (tools/migrate.js).
    const schemaReady = Promise.resolve();

    const parseJson = v => {
        if (v == null) return null;
        if (typeof v === 'object') return v;
        try { return JSON.parse(v); } catch { return null; }
    };
    const rowToJson = R.packingListToJson;

    // A refusal from the review service, or a 500.
    function sendReviewError(res, e, label) {
        if (e instanceof R.ReviewError) return res.status(e.status).json({ error: e.message, code: e.code, ...(e.payload || {}) });
        log.error(label, e);
        return res.status(500).json({ error: 'An internal error occurred.' });
    }

    // Background read on its own connections (pool of one — the service never
    // holds one across the model call). Lambda must be told to wait for it.
    function startCheck(req, id, model) {
        if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const userEmail = req.userEmail || null;
        (async () => {
            try {
                const out = await P.runPackingListCheck(pool, { id, userEmail, model });
                if (out && out.failed) log.warn('[packing-lists] check failed', { id, code: out.failed });
            } catch (e) {
                log.warn('[packing-lists] check threw', { id, error: e.message });
            }
        })();
    }

    // Resolve { containerNumber | draftContainerId } to a container with lines,
    // or a { status, error, code, payload } failure. Shared by /context and POST.
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
                error: `No booked container or open draft matches ${typed || `draft ${draftContainerId}`}.` } };
        }
        if (r.container.kind === 'draft' && !r.container.lineCount) {
            return { fail: { status: 422, code: 'CONTAINER_EMPTY', error: `Draft ${r.container.draftName} has no lines.` } };
        }
        return { container: r.container };
    }

    const sendFail = (res, f) => res.status(f.status).json({ error: f.error, code: f.code, ...(f.payload || {}) });

    // GET /api/v1/packing-lists/context?containerNumber=324 | ?draftContainerId=129
    // The container as we know it (booked or draft) plus the suppliers on
    // board with THIS container's units (the picker).
    app.get('/api/v1/packing-lists/context', async (req, res) => {
        try {
            await schemaReady;
            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, {
                    containerNumber: req.query.containerNumber, draftContainerId: req.query.draftContainerId,
                });
                if (r.fail) return r;
                return { container: P.describeContainer(r.container), suppliers: await P.loadContainerSuppliers(conn, r.container) };
            });
            if (out.fail) return sendFail(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[GET /packing-lists/context]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/packing-lists/upload-url  Body: { filename, contentType? }
    // For files over 4 MB: PUT the bytes to uploadUrl, then POST
    // /api/v1/packing-lists with the returned s3Key instead of dataBase64.
    app.post('/api/v1/packing-lists/upload-url', async (req, res) => {
        try {
            if (!poBucket) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
            const b = req.body || {};
            const filename = typeof b.filename === 'string' ? b.filename.trim().slice(0, 200) : '';
            if (!filename) return res.status(400).json({ error: 'filename is required.', code: 'BAD_FIELD' });
            const safe = filename.replace(SAFE_FILENAME_RE, '_');
            const s3Key = `packing-lists/${uuidv4()}/${safe}`;
            const contentType = (typeof b.contentType === 'string' && b.contentType.trim().slice(0, 100)) || 'application/pdf';
            const uploadUrl = await getSignedUrl(
                s3, new PutObjectCommand({ Bucket: poBucket, Key: s3Key, ContentType: contentType }), { expiresIn: 600 }
            );
            res.status(201).json({
                uploadUrl, s3Key, uploadMethod: 'PUT', uploadHeaders: { 'Content-Type': contentType },
                expiresInSeconds: 600, maxBytes: MAX_S3_BYTES,
            });
        } catch (error) {
            log.error('[POST /packing-lists/upload-url]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/packing-lists
    // Body: { containerNumber | draftContainerId, supplierKey | supplierId | supplierName,
    //         filename, contentType?, dataBase64 | s3Key, force?, model?, replacesId? }
    // 201 { packingList } with status 'processing'; poll GET /:id. With
    // replacesId the new document is a corrected version of that one, which
    // drops out of the live check (see packing-review.js).
    app.post('/api/v1/packing-lists', async (req, res) => {
        try {
            await schemaReady;
            await auditLogSchemaReady;
            if (!poBucket) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
            const b = req.body || {};

            const filename = typeof b.filename === 'string' ? b.filename.trim().slice(0, 200) : '';
            if (!filename) return res.status(400).json({ error: 'filename is required.', code: 'BAD_FIELD' });
            const contentTypeIn = (typeof b.contentType === 'string' && b.contentType.trim().slice(0, 100)) || null;

            // The file: inline, or already in S3 from /upload-url.
            let buffer = null;
            let upload = null;   // { s3Key, fileSize, contentType } for an S3 upload
            if (typeof b.dataBase64 === 'string' && b.dataBase64) {
                buffer = Buffer.from(b.dataBase64.replace(/^data:[^;]+;base64,/, ''), 'base64');
                if (!buffer.length) return res.status(400).json({ error: 'dataBase64 decoded to an empty file.', code: 'BAD_FIELD' });
                if (buffer.length > MAX_INLINE_BYTES) {
                    return res.status(413).json({ error: `File too large to send inline (max ${MAX_INLINE_BYTES} bytes) — use POST /api/v1/packing-lists/upload-url.`, code: 'TOO_LARGE' });
                }
            } else if (typeof b.s3Key === 'string' && UPLOAD_KEY_RE.test(b.s3Key)) {
                let head;
                try { head = await s3.send(new HeadObjectCommand({ Bucket: poBucket, Key: b.s3Key })); }
                catch { return res.status(409).json({ error: 'File not found in S3 — PUT it to the uploadUrl first.', code: 'NOT_UPLOADED' }); }
                const size = Number(head.ContentLength) || 0;
                if (!size) return res.status(400).json({ error: 'The uploaded file is empty.', code: 'BAD_FIELD' });
                if (size > MAX_S3_BYTES) return res.status(413).json({ error: `File too large (max ${MAX_S3_BYTES} bytes).`, code: 'TOO_LARGE' });
                upload = { s3Key: b.s3Key, fileSize: size, contentType: head.ContentType || contentTypeIn };
            } else {
                return res.status(400).json({ error: 'dataBase64, or an s3Key from /upload-url, is required.', code: 'BAD_FIELD' });
            }
            const model = typeof b.model === 'string' && b.model.trim() ? b.model.trim() : null;
            let replacesId = null;
            if (b.replacesId != null && b.replacesId !== '') {
                replacesId = Number(b.replacesId);
                if (!Number.isInteger(replacesId) || replacesId <= 0) {
                    return res.status(400).json({ error: 'replacesId must be a packing list id.', code: 'BAD_FIELD' });
                }
            }

            const result = await withConnection(async (conn) => {
                const resolved = await resolveForRequest(conn, {
                    containerNumber: b.containerNumber, draftContainerId: b.draftContainerId,
                });
                if (resolved.fail) return resolved;
                const container = resolved.container;

                const supplierKey = await P.resolveSupplierChoice(conn, {
                    supplierKey: b.supplierKey, supplierId: b.supplierId, supplierName: b.supplierName,
                });
                if (!supplierKey) {
                    return { fail: { status: 400, error: 'Choose a supplier: supplierKey, supplierId or supplierName.', code: 'BAD_FIELD' } };
                }
                const suppliers = await P.loadContainerSuppliers(conn, container);
                const onBoard = suppliers.find(s => s.supplierKey === supplierKey);
                // A supplier with nothing in the container would make every row
                // "not expected" — almost always the wrong container or supplier.
                if (!onBoard && b.force !== true) {
                    return { fail: {
                        status: 422, code: 'SUPPLIER_NOT_ON_CONTAINER',
                        error: `That supplier has no lines in ${container.label}. Send force: true to check it anyway.`,
                        payload: { suppliers },
                    } };
                }
                let supplierName = onBoard ? onBoard.supplierName : null;
                if (!supplierName && /^s:\d+$/.test(supplierKey)) {
                    const [s] = await conn.query(`SELECT name FROM suppliers WHERE id = ?`, [Number(supplierKey.slice(2))]);
                    supplierName = s[0]?.name || null;
                }
                supplierName = supplierName || (typeof b.supplierName === 'string' ? b.supplierName.trim().slice(0, 255) : null);

                let s3Key;
                let contentType;
                let fileSize;
                if (upload) {
                    // One S3 object backs one packing list.
                    const [taken] = await conn.query(`SELECT id FROM packing_lists WHERE s3_key = ? LIMIT 1`, [upload.s3Key]);
                    if (taken.length) {
                        return { fail: { status: 409, error: 'That upload is already registered.', code: 'DUPLICATE_UPLOAD', payload: { existingId: taken[0].id } } };
                    }
                    ({ s3Key, fileSize } = upload);
                    contentType = upload.contentType || 'application/pdf';
                } else {
                    const safe = filename.replace(SAFE_FILENAME_RE, '_');
                    s3Key = `packing-lists/${uuidv4()}/${safe}`;
                    contentType = contentTypeIn || 'application/pdf';
                    fileSize = buffer.length;
                    await s3.send(new PutObjectCommand({
                        Bucket: poBucket, Key: s3Key, Body: buffer, ContentType: contentType,
                        ContentDisposition: `inline; filename="${safe}"`,
                    }));
                }
                const isDraft = container.kind === 'draft';
                const [ins] = await conn.query(
                    `INSERT INTO packing_lists
                        (container_kind, container_number, draft_container_id, container_name,
                         supplier_key, supplier_name, filename, s3_key, content_type, file_size, status, uploaded_by_email)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing', ?)`,
                    [
                        container.kind, container.containerNumber || null,
                        isDraft ? container.draftId : null, isDraft ? container.draftName : null,
                        supplierKey, supplierName, filename, s3Key, contentType, fileSize, req.userEmail || null,
                    ]
                );
                await recordAudit(conn, {
                    entityType: 'packing_list', entityId: ins.insertId, action: 'create', before: null,
                    after: { container: P.describeContainer(container), supplierKey, supplierName, filename, fileSize, replacesId },
                    userEmail: req.userEmail,
                });
                if (replacesId != null) {
                    try {
                        await R.supersede(conn, { container, supplierKey, oldId: replacesId, newId: ins.insertId, userEmail: req.userEmail });
                    } catch (e) {
                        if (!(e instanceof R.ReviewError)) throw e;
                        // The file is in S3 and the row exists; keep it as an
                        // extra document rather than lose the upload, but say so.
                        log.warn('[POST /packing-lists] replace refused, kept as a new document', { id: ins.insertId, replacesId, code: e.code });
                        const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [ins.insertId]);
                        return { row: rows[0], replaceRefused: { code: e.code, error: e.message } };
                    }
                }
                const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [ins.insertId]);
                return { row: rows[0] };
            });
            if (result.fail) return sendFail(res, result.fail);
            res.status(201).json({ packingList: rowToJson(result.row), ...(result.replaceRefused ? { replaceRefused: result.replaceRefused } : {}) });
            startCheck(req, result.row.id, model);
        } catch (error) {
            log.error('[POST /packing-lists]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // ── The review: status, check, sign-offs, approval ─────────────────────

    // GET /api/v1/packing-lists/status?containerNumber=324 | ?draftContainerId=129
    // The container's packing review at a glance: every supplier with lines in
    // it, that supplier's documents and live check, the approval, and one
    // overall verdict. Computed from the lines as they are now.
    app.get('/api/v1/packing-lists/status', async (req, res) => {
        try {
            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, { containerNumber: req.query.containerNumber, draftContainerId: req.query.draftContainerId });
                if (r.fail) return r;
                return R.buildContainerStatus(conn, r.container);
            });
            if (out.fail) return sendFail(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[GET /packing-lists/status]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/packing-lists/check?containerNumber=324&supplierKey=s:2
    // One supplier's full check: its active documents pooled and compared
    // with its lines in the container, sign-offs applied, older versions listed.
    app.get('/api/v1/packing-lists/check', async (req, res) => {
        try {
            const supplierKey = String(req.query.supplierKey || '').trim();
            if (!supplierKey) return res.status(400).json({ error: 'supplierKey is required.', code: 'BAD_FIELD' });
            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, { containerNumber: req.query.containerNumber, draftContainerId: req.query.draftContainerId });
                if (r.fail) return r;
                const check = await R.buildSupplierCheck(conn, r.container, supplierKey);
                const suppliers = await P.loadContainerSuppliers(conn, r.container);
                const onBoard = suppliers.find(s => s.supplierKey === supplierKey) || null;
                const approvalRow = await R.loadActiveApproval(conn, r.container);
                return {
                    container: P.describeContainer(r.container),
                    supplier: {
                        supplierKey,
                        supplierName: onBoard?.supplierName ?? check.packingLists[0]?.supplierName ?? check.superseded[0]?.supplierName ?? supplierKey,
                        units: onBoard?.units ?? 0, orderCount: onBoard?.orderCount ?? 0, onBoard: !!onBoard,
                    },
                    packingLists: check.packingLists,
                    superseded: check.superseded,
                    processing: check.processing,
                    failed: check.failed,
                    signOffs: check.signOffs,
                    comparison: check.comparison,
                    approval: approvalRow ? R.approvalToJson(approvalRow) : null,
                };
            });
            if (out.fail) return sendFail(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[GET /packing-lists/check]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/packing-lists/sign-offs
    // Body: { containerNumber | draftContainerId, supplierKey, lineKey, note? }
    // Accept one line's difference. 201 { signOff, check } — the check is the
    // supplier's comparison after the sign-off.
    app.post('/api/v1/packing-lists/sign-offs', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const b = req.body || {};
            const supplierKey = String(b.supplierKey || '').trim();
            const lineKey = String(b.lineKey || '').trim();
            if (!supplierKey || !lineKey) return res.status(400).json({ error: 'supplierKey and lineKey are required.', code: 'BAD_FIELD' });
            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, { containerNumber: b.containerNumber, draftContainerId: b.draftContainerId });
                if (r.fail) return r;
                const result = await R.signOffLine(conn, { container: r.container, supplierKey, lineKey, note: b.note, userEmail: req.userEmail });
                const check = await R.buildSupplierCheck(conn, r.container, supplierKey);
                return { ...result, comparison: check.comparison, signOffs: check.signOffs };
            });
            if (out.fail) return sendFail(res, out.fail);
            res.status(out.created ? 201 : 200).json(out);
        } catch (error) {
            sendReviewError(res, error, '[POST /packing-lists/sign-offs]');
        }
    });

    // DELETE /api/v1/packing-lists/sign-offs/:id — take a sign-off back.
    app.delete('/api/v1/packing-lists/sign-offs/:id(\\d+)', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            await withConnection(conn => R.revokeSignOff(conn, { id, reason: 'revoked', userEmail: req.userEmail }));
            res.status(204).end();
        } catch (error) {
            sendReviewError(res, error, '[DELETE /packing-lists/sign-offs/:id]');
        }
    });

    // GET /api/v1/packing-lists/approvals — every container currently approved
    // (for badges); each says what it was approved as.
    app.get('/api/v1/packing-lists/approvals', async (req, res) => {
        try {
            const data = await withConnection(conn => R.listActiveApprovals(conn));
            res.json({ data });
        } catch (error) {
            log.error('[GET /packing-lists/approvals]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/packing-lists/approvals
    // Body: { containerNumber | draftContainerId, note? }
    // Approve the container's packing as it stands, whatever the checks say.
    // 201 { approval }; 409 ALREADY_APPROVED while one is active.
    app.post('/api/v1/packing-lists/approvals', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const b = req.body || {};
            const out = await withConnection(async (conn) => {
                const r = await resolveForRequest(conn, { containerNumber: b.containerNumber, draftContainerId: b.draftContainerId });
                if (r.fail) return r;
                return { approval: await R.approveContainer(conn, { container: r.container, note: b.note, userEmail: req.userEmail }) };
            });
            if (out.fail) return sendFail(res, out.fail);
            res.status(201).json(out);
        } catch (error) {
            sendReviewError(res, error, '[POST /packing-lists/approvals]');
        }
    });

    // POST /api/v1/packing-lists/approvals/:id/withdraw  Body: { note? }
    app.post('/api/v1/packing-lists/approvals/:id(\\d+)/withdraw', async (req, res) => {
        try {
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const out = await withConnection(conn => R.withdrawApproval(conn, { id, note: req.body?.note, userEmail: req.userEmail }));
            res.json(out);
        } catch (error) {
            sendReviewError(res, error, '[POST /packing-lists/approvals/:id/withdraw]');
        }
    });

    // POST /api/v1/packing-lists/:id/analyze — read the PDF again (after a
    // failure, or to try another model: body { model }).
    app.post('/api/v1/packing-lists/:id(\\d+)/analyze', async (req, res) => {
        try {
            await schemaReady;
            const id = Number(req.params.id);
            const model = req.body && typeof req.body.model === 'string' && req.body.model.trim() ? req.body.model.trim() : null;
            const out = await withConnection(async (conn) => {
                const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ? AND deleted_at IS NULL`, [id]);
                const row = rows[0];
                if (!row) return null;
                // A Lambda killed mid-read leaves 'processing'; after ten minutes a re-run may take over.
                const since = new Date(row.analyzed_at || row.created_at).getTime();
                if (row.status === 'processing' && Date.now() - since < 10 * 60_000) return { row, already: true };
                // Clear the previous result so a failed re-run can't sit next to
                // an old verdict.
                await conn.query(
                    `UPDATE packing_lists
                        SET status = 'processing', error_message = NULL, analyzed_at = NOW(),
                            verdict = NULL, discrepancy_count = NULL, row_count = NULL, model_used = NULL,
                            invoice_number = NULL, document_date = NULL, supplier_printed = NULL,
                            extract_json = NULL, comparison_json = NULL
                      WHERE id = ?`,
                    [id]
                );
                const [rb] = await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [id]);
                return { row: rb[0] };
            });
            if (!out) return res.status(404).json({ error: `Packing list ${id} not found.`, code: 'NOT_FOUND' });
            res.status(202).json({ packingList: rowToJson(out.row), alreadyRunning: !!out.already });
            if (!out.already) startCheck(req, id, model);
        } catch (error) {
            log.error('[POST /packing-lists/:id/analyze]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/packing-lists?containerNumber=324 | ?draftContainerId=129 — newest first, no detail.
    // containerNumber matches what was stored at upload (booked reference, a
    // draft's reserved number or name) and also what it resolves to now: the
    // draft under its current name, or the booked container plus the lists
    // uploaded against the draft that became it.
    app.get('/api/v1/packing-lists', async (req, res) => {
        try {
            await schemaReady;
            const where = ['deleted_at IS NULL'];
            const params = [];
            let draftIdFilter = null;
            if (req.query.draftContainerId != null && req.query.draftContainerId !== '') {
                draftIdFilter = Number(req.query.draftContainerId);
                if (!Number.isInteger(draftIdFilter) || draftIdFilter <= 0) {
                    return res.status(400).json({ error: 'draftContainerId must be a positive integer.', code: 'BAD_FIELD' });
                }
                where.push('draft_container_id = ?');
                params.push(draftIdFilter);
            }
            if (req.query.supplierKey) { where.push('supplier_key = ?'); params.push(String(req.query.supplierKey).trim()); }
            const typed = req.query.containerNumber ? String(req.query.containerNumber).trim() : '';
            const rows = await withConnection(async (conn) => {
                if (typed) {
                    const ors = ['container_number = ?', 'container_name = ?'];
                    const orParams = [typed, typed];
                    const r = await P.resolveContainer(conn, { containerNumber: typed });
                    const c = r && r.container;
                    if (c && c.kind === 'draft' && c.draftId != null) {
                        ors.push('draft_container_id = ?');
                        orParams.push(c.draftId);
                    } else if (c && c.kind === 'booked') {
                        ors.push('container_number = ?');
                        orParams.push(c.containerNumber);
                        if (c.followedFrom?.draftId != null) { ors.push('draft_container_id = ?'); orParams.push(c.followedFrom.draftId); }
                        ors.push(`draft_container_id IN (SELECT id FROM draft_containers WHERE closed_reason = 'converted' AND container_number = ?)`);
                        orParams.push(c.containerNumber);
                    }
                    where.push(`(${ors.join(' OR ')})`);
                    params.push(...orParams);
                }
                const [r] = await conn.query(
                    `SELECT id, container_kind, container_number, draft_container_id, container_name,
                            supplier_key, supplier_name, filename, content_type, file_size, status,
                            verdict, discrepancy_count, row_count, invoice_number, document_date, supplier_printed,
                            model_used, error_message, version, replaces_id, superseded_by_id, superseded_at,
                            uploaded_by_email, created_at, analyzed_at
                       FROM packing_lists WHERE ${where.join(' AND ')} ORDER BY id DESC LIMIT 200`,
                    params
                );
                return r;
            });
            res.json({ data: rows.map(rowToJson) });
        } catch (error) {
            log.error('[GET /packing-lists]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/packing-lists/:id — the document, what was read from it, and
    // its supplier's live check: every active document for that supplier in
    // the container pooled and compared with the lines as they are now, with
    // sign-offs applied. A draft is followed through renames and, once booked,
    // to its container (comparison.container.followedFrom says so).
    // ?snapshot=1 returns what THIS document alone said when it was read; so
    // does a draft whose lines can no longer be found (liveUnavailableReason).
    app.get('/api/v1/packing-lists/:id(\\d+)', async (req, res) => {
        try {
            await schemaReady;
            const id = Number(req.params.id);
            const out = await withConnection(async (conn) => {
                const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ? AND deleted_at IS NULL`, [id]);
                const row = rows[0];
                if (!row) return null;
                const extracted = parseJson(row.extract_json);
                let comparison = parseJson(row.comparison_json);
                let live = false;
                let liveUnavailableReason = null;
                let check = null;
                let approval = null;
                if (req.query.snapshot !== '1') {
                    const container = await P.resolveStoredContainer(conn, row);
                    if (container.kind === 'draft' && !container.lineCount) {
                        liveUnavailableReason = `Draft ${container.draftName} has no lines any more and no booked container was found for it.`;
                    } else {
                        check = await R.buildSupplierCheck(conn, container, row.supplier_key);
                        const approvalRow = await R.loadActiveApproval(conn, container);
                        approval = approvalRow ? R.approvalToJson(approvalRow) : null;
                        if (check.comparison) { comparison = check.comparison; live = true; }
                    }
                }
                return { row, extracted, comparison, live, liveUnavailableReason, check, approval };
            });
            if (!out) return res.status(404).json({ error: `Packing list ${id} not found.`, code: 'NOT_FOUND' });
            res.json({
                packingList: rowToJson(out.row),
                extracted: out.extracted,
                comparison: out.comparison,
                comparisonIsLive: out.live,
                ...(out.liveUnavailableReason ? { liveUnavailableReason: out.liveUnavailableReason } : {}),
                packingLists: out.check ? out.check.packingLists : [],
                superseded: out.check ? out.check.superseded : [],
                signOffs: out.check ? out.check.signOffs : [],
                approval: out.approval,
            });
        } catch (error) {
            log.error('[GET /packing-lists/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/packing-lists/:id/download — audited, short-lived presigned URL.
    app.get('/api/v1/packing-lists/:id(\\d+)/download', async (req, res) => {
        try {
            await schemaReady;
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const disposition = req.query.disposition === 'attachment' ? 'attachment' : 'inline';
            const row = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `SELECT id, filename, s3_key, content_type FROM packing_lists WHERE id = ? AND deleted_at IS NULL`, [id]
                );
                return rows[0] || null;
            });
            if (!row) return res.status(404).json({ error: `Packing list ${id} not found.`, code: 'NOT_FOUND' });
            const safeName = String(row.filename || `packing-list-${id}.pdf`).replace(/"/g, '');
            const url = await getSignedUrl(s3, new GetObjectCommand({
                Bucket: poBucket, Key: row.s3_key,
                ResponseContentDisposition: `${disposition}; filename="${safeName}"`,
                ...(row.content_type ? { ResponseContentType: row.content_type } : {}),
            }), { expiresIn: 300 });
            await withConnection(conn => recordAudit(conn, {
                entityType: 'packing_list', entityId: id, action: 'packing_list_downloaded', before: null,
                after: { disposition, filename: row.filename }, userEmail: req.userEmail,
            }));
            res.json({ id, url, disposition, expiresInSeconds: 300 });
        } catch (error) {
            log.error('[GET /packing-lists/:id/download]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // DELETE /api/v1/packing-lists/:id — soft delete; the S3 object stays. A
    // deleted replacement puts the version it replaced back into the check.
    app.delete('/api/v1/packing-lists/:id(\\d+)', async (req, res) => {
        try {
            await schemaReady;
            await auditLogSchemaReady;
            const id = Number(req.params.id);
            const ok = await withConnection(async (conn) => {
                const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ? AND deleted_at IS NULL`, [id]);
                if (!rows.length) return false;
                await conn.query(`UPDATE packing_lists SET deleted_at = NOW() WHERE id = ?`, [id]);
                await recordAudit(conn, {
                    entityType: 'packing_list', entityId: id, action: 'delete',
                    before: rowToJson(rows[0]), after: null, userEmail: req.userEmail,
                });
                await R.restorePredecessor(conn, { deletedRow: rows[0], userEmail: req.userEmail });
                return true;
            });
            if (!ok) return res.status(404).json({ error: `Packing list ${id} not found.`, code: 'NOT_FOUND' });
            res.status(204).end();
        } catch (error) {
            log.error('[DELETE /packing-lists/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerPackingListRoutes };
