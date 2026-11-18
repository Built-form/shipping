'use strict';

// Telex releases (docs/telex-releases.md): the telex-released bill of lading a
// supplier sends once a container's balance is paid, filed per supplier per
// shipment in shipment_telex_releases.
//
//   GET    /api/v1/telex-releases?shipmentId=N
//          a shipment's suppliers, each with where its telex release stands,
//          the files filed and the supplier's upload waiting for us: what the
//          TELEX tab of the shipment card lists. Any signed-in user.
//   POST   /api/v1/telex-releases
//          { shipmentId, supplierName, filename, dataBase64, notes? }: our team
//          file one themselves (PDF, PNG or JPEG, at most 15 MB).
//   DELETE /api/v1/telex-releases/:id
//          an admin, or whoever filed it. Soft.
//   GET    /api/v1/telex-submissions[?status=all]
//          what suppliers uploaded on the portal's Payments page: those waiting
//          for us, or all of them. { data, unavailable } (unavailable: the
//          portal's table is not on this server).
//   GET    /api/v1/telex-submissions/:id/file
//          a five-minute link to the supplier's file, to look before deciding.
//   POST   /api/v1/telex-submissions/:id/accept
//          files it: the file is copied into our bucket and a release is
//          written for the upload's shipment and supplier. Answered the same
//          when asked twice.
//   POST   /api/v1/telex-submissions/:id/reject  { note }
//          sends it back: the supplier reads the note and uploads again.
// Filing, deleting, accepting and sending back: admin and standard users. Every
// refusal carries `code`; a 409 also `allowedNext`.
//
// A supplier's upload is a portal File (jportal.file_transfers, sent from the
// supplier) tagged for a payment and a container (jportal.telex_submissions).
// The portal reads only its own tables, so it learns what happened here from
// that row: `received` (with release_id), `rejected` (with the note), and a
// row of our own, with no file, when we file a release ourselves, so the
// supplier is not asked for one we already hold.
//
// The rules are in src/lib/telex-releases.js. Registered from orders.js;
// authed through the greedy /api/v1/{proxy+} route.

const { v4: uuidv4 } = require('uuid');
const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const T = require('../lib/telex-releases');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');

const ER_NO_SUCH_TABLE = 1146;
const ER_DUP_ENTRY = 1062;
const ISO = "'%Y-%m-%dT%H:%i:%sZ'";
const VIEW_SECONDS = 300;
const NOT_MIGRATED = { error: 'Telex releases are not set up on this server yet: their table has not been migrated.', code: 'NOT_MIGRATED' };

const RELEASE_COLUMNS = `r.id, r.shipment_id, r.shipment_reference, r.supplier_name, r.filename, r.public_url, r.content_type,
        r.file_size, r.source, r.portal_submission_id, r.notes, r.uploaded_by_email, r.deleted_at,
        DATE_FORMAT(r.created_at, ${ISO}) AS created_at_iso`;

const releaseJson = r => ({
    id: r.id,
    shipmentId: r.shipment_id,
    shipmentReference: r.shipment_reference || null,
    supplierName: r.supplier_name,
    filename: r.filename,
    url: r.public_url,
    contentType: r.content_type,
    fileSize: Number(r.file_size),
    source: r.source,
    portalSubmissionId: r.portal_submission_id == null ? null : Number(r.portal_submission_id),
    notes: r.notes || null,
    uploadedByEmail: r.uploaded_by_email || null,
    createdAt: r.created_at_iso,
});

// A portal upload with its file. `sent`: the file arrived and was not withdrawn.
const SUBMISSION_SQL = jp => `
    SELECT t.id, t.transfer_id, t.supplier_payment_id, t.shipment_id, t.supplier_name, t.container_ref, t.user_email,
           t.status, t.decided_note, t.decided_by, t.release_id,
           DATE_FORMAT(t.created_at, ${ISO}) AS created_at_iso,
           DATE_FORMAT(t.decided_at, ${ISO}) AS decided_at_iso,
           f.filename, f.content_type, f.file_size, f.final_s3_key, f.final_etag, f.status AS file_status,
           DATE_FORMAT(f.sent_at, ${ISO}) AS sent_at_iso,
           s.reference AS shipment_reference, (s.id IS NOT NULL AND s.deleted_at IS NULL) AS shipment_live
      FROM ${jp}.telex_submissions t
      LEFT JOIN ${jp}.file_transfers f ON f.id = t.transfer_id AND f.direction = 'from_supplier'
      LEFT JOIN shipments s ON s.id = t.shipment_id`;

const fileSent = r => r.file_status === 'sent' && !!r.final_s3_key;

const submissionJson = r => ({
    id: Number(r.id),
    status: r.status,
    sent: fileSent(r),
    supplierPaymentId: r.supplier_payment_id == null ? null : Number(r.supplier_payment_id),
    shipmentId: Number(r.shipment_id),
    shipmentReference: r.shipment_reference || null,
    shipmentLive: !!r.shipment_live,
    containerRef: r.container_ref || null,
    supplierName: r.supplier_name,
    uploadedByEmail: r.user_email || null,
    createdAt: r.created_at_iso,
    note: r.decided_note || null,
    decidedBy: r.decided_by || null,
    decidedAt: r.decided_at_iso || null,
    releaseId: r.release_id == null ? null : Number(r.release_id),
    file: r.transfer_id == null ? null : {
        transferId: Number(r.transfer_id),
        filename: r.filename || null,
        contentType: r.content_type || null,
        fileSize: r.file_size == null ? null : Number(r.file_size),
        sentAt: r.sent_at_iso || null,
    },
});

/**
 * The filed releases and the portal's uploads for a set of shipments, as the
 * rules read them. → { releases, submissions }, or null on a server without
 * the releases table. The portal's table being absent is no uploads.
 */
async function loadFor(conn, shipmentIds, env = process.env) {
    const ids = [...new Set((shipmentIds || []).filter(id => id != null))];
    if (!ids.length) return { releases: [], submissions: [] };
    let rows;
    try {
        [rows] = await conn.query(
            `SELECT ${RELEASE_COLUMNS} FROM shipment_telex_releases r
              WHERE r.deleted_at IS NULL AND r.shipment_id IN (?)
              ORDER BY r.created_at DESC, r.id DESC`,
            [ids]
        );
    } catch (e) {
        if (e && e.errno === ER_NO_SUCH_TABLE) return null;
        throw e;
    }
    let uploads = [];
    try {
        [uploads] = await conn.query(`${SUBMISSION_SQL(jportalSchema(env))} WHERE t.shipment_id IN (?) AND t.transfer_id IS NOT NULL`, [ids]);
    } catch (e) {
        if (!isSourceMissing(e)) throw e;
    }
    return {
        releases: rows.map(releaseJson),
        submissions: uploads.map(submissionJson),
    };
}

// Who a shipment's telex releases are for. Its order lines: a booked shipment's
// are the live orders pointing at it, a draft's or a planned one's are its
// shipment_lines. And its balance records, whose supplier_name is the one the
// portal knows a paid balance by.
const ON_BOARD_SQL = `SELECT DISTINCT o.supplier AS name
      FROM orders o
     WHERE o.deleted_at IS NULL AND o.status <> 'DESTROYED' AND o.supplier IS NOT NULL AND o.supplier <> ''
       AND (o.shipment_id = ? OR o.id IN (SELECT sl.order_id FROM shipment_lines sl WHERE sl.shipment_id = ?))`;
const BALANCES_SQL = `SELECT DISTINCT p.supplier_name AS name
      FROM shipment_payments p
     WHERE p.shipment_id = ? AND p.deleted_at IS NULL AND p.supplier_name IS NOT NULL AND p.supplier_name <> ''`;

function registerTelexReleaseRoutes(app, deps) {
    const { withConnection, ready, s3, poBucket, publicS3Url, recordAudit, log } = deps;
    const portalBucket = () => process.env.PI_UPLOADS_BUCKET || null;

    const handle = (site, fn) => async (req, res) => {
        try {
            await ready();
            const r = await fn(req);
            res.status(r.status).json(r.body);
        } catch (error) {
            if (error && error.errno === ER_NO_SUCH_TABLE) return res.status(503).json(NOT_MIGRATED);
            log.error(`[${site}]`, error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };
    const answer = (status, body) => ({ status, body });
    const refused = (r) => { const { status, ...body } = r; return answer(status, body); };
    const actorOf = req => ({ email: req.userEmail || null, userType: req.userType });

    // What the portal is told about a release our team hold. Never stops the
    // filing: the portal's table may not be on this server, and a supplier
    // asked for a release we already have is an annoyance, not a fault.
    const tellPortal = async (what, run) => {
        try {
            await withConnection(run);
        } catch (e) {
            if (!isSourceMissing(e)) log.warn(`[telex-releases] the portal was not told (${what})`, { error: e.message, errno: e.errno });
        }
    };

    app.get('/api/v1/telex-releases', handle('GET /telex-releases', async (req) => {
        const shipmentId = Number(req.query.shipmentId);
        if (!Number.isInteger(shipmentId) || shipmentId <= 0) return answer(400, { error: 'shipmentId is required.', code: 'BAD_FIELD' });
        const out = await withConnection(async (conn) => {
            const [shipments] = await conn.query('SELECT id, reference FROM shipments WHERE id = ?', [shipmentId]);
            if (!shipments.length) return { none: true };
            const [onBoard] = await conn.query(ON_BOARD_SQL, [shipmentId, shipmentId]);
            const [balances] = await conn.query(BALANCES_SQL, [shipmentId]);
            const loaded = await loadFor(conn, [shipmentId]);
            return { shipment: shipments[0], onBoard, balances, loaded };
        });
        if (out.none) return refused(T.uploadRefusal({ shipment: null }));
        if (!out.loaded) return answer(503, NOT_MIGRATED);
        return answer(200, {
            shipmentId: out.shipment.id,
            reference: out.shipment.reference || null,
            suppliers: T.suppliersOf({
                onBoard: out.onBoard.map(r => r.name),
                balances: out.balances.map(r => r.name),
                releases: out.loaded.releases,
                submissions: out.loaded.submissions,
            }),
            canFile: T.mayFile(req.userType),
        });
    }));

    app.post('/api/v1/telex-releases', handle('POST /telex-releases', async (req) => {
        const actor = actorOf(req);
        if (!T.mayFile(actor.userType)) return answer(403, { error: 'Your role cannot file or delete a telex release: admin and standard users do.', code: 'CANNOT_FILE' });
        if (!poBucket) return answer(500, { error: 'PO_DOCS_BUCKET env var not configured.' });
        const b = req.body || {};
        const shipmentId = Number(b.shipmentId);
        const supplierName = typeof b.supplierName === 'string' ? b.supplierName.slice(0, 255) : '';
        const filename = typeof b.filename === 'string' ? b.filename.trim().slice(0, 200) : '';
        let bytes = null;
        if (typeof b.dataBase64 === 'string') {
            try { bytes = Buffer.from(b.dataBase64.replace(/^data:[^;]+;base64,/, ''), 'base64'); } catch { bytes = null; }
        }
        const notes = T.cleanNote(b.notes) || null;

        const shipment = Number.isInteger(shipmentId) && shipmentId > 0
            ? await withConnection(async (conn) => {
                const [rows] = await conn.query('SELECT id, reference FROM shipments WHERE id = ? AND deleted_at IS NULL', [shipmentId]);
                return rows[0] || null;
            })
            : null;
        const no = T.uploadRefusal({ filename, bytes, supplierName, shipment });
        if (no) return refused(no);

        const contentType = T.typeOf(bytes);
        const s3Key = T.s3KeyFor(uuidv4(), filename, contentType);
        await s3.send(new PutObjectCommand({
            Bucket: poBucket, Key: s3Key, Body: bytes, ContentType: contentType,
            ContentDisposition: `inline; filename="${s3Key.slice(s3Key.lastIndexOf('/') + 1)}"`,
        }));

        const row = await withConnection(async (conn) => {
            const [ins] = await conn.query(
                `INSERT INTO shipment_telex_releases
                    (shipment_id, shipment_reference, supplier_name, filename, s3_key, public_url, content_type, file_size, source, notes, uploaded_by_email)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'staff', ?, ?)`,
                [shipment.id, shipment.reference || null, supplierName, filename, s3Key, publicS3Url(s3Key), contentType, bytes.length, notes, actor.email]
            );
            await recordAudit(conn, {
                entityType: 'shipment_telex_release', entityId: ins.insertId, action: 'create', before: null,
                after: { shipmentId: shipment.id, supplierName, filename, fileSize: bytes.length, source: 'staff' },
                userEmail: actor.email,
            });
            const [back] = await conn.query(`SELECT ${RELEASE_COLUMNS} FROM shipment_telex_releases r WHERE r.id = ?`, [ins.insertId]);
            return back[0];
        });

        // A row of our own on the portal's side, with no file: this supplier is
        // no longer asked for this container's release.
        await tellPortal('a release filed here', conn => conn.query(
            `INSERT INTO ${jportalSchema()}.telex_submissions
                (transfer_id, supplier_payment_id, shipment_id, supplier_name, container_ref, status, decided_by, decided_at, release_id)
             VALUES (NULL, NULL, ?, ?, NULL, 'received', ?, UTC_TIMESTAMP(), ?)`,
            [shipment.id, supplierName, actor.email || 'shipline', row.id]
        ));
        return answer(201, { release: releaseJson(row) });
    }));

    app.delete('/api/v1/telex-releases/:id(\\d+)', handle('DELETE /telex-releases/:id', async (req) => {
        const actor = actorOf(req);
        const id = Number(req.params.id);
        const out = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT r.id, r.shipment_id, r.supplier_name, r.filename, r.uploaded_by_email, r.deleted_at FROM shipment_telex_releases r WHERE r.id = ?`, [id]
            );
            const row = rows[0] ? { ...rows[0], uploadedByEmail: rows[0].uploaded_by_email, deletedAt: rows[0].deleted_at } : null;
            const no = T.deleteRefusal(row, actor);
            if (no) return { no };
            const [done] = await conn.query(
                'UPDATE shipment_telex_releases SET deleted_at = UTC_TIMESTAMP(), deleted_by_email = ? WHERE id = ? AND deleted_at IS NULL', [actor.email, id]
            );
            if (done.affectedRows !== 1) return { no: T.deleteRefusal(null, actor) };
            await recordAudit(conn, {
                entityType: 'shipment_telex_release', entityId: id, action: 'delete',
                before: { shipmentId: row.shipment_id, supplierName: row.supplier_name, filename: row.filename }, after: null,
                userEmail: actor.email,
            });
            return { ok: true };
        });
        if (out.no) return refused(out.no);
        // The portal stops saying "received" for it: the supplier can upload again.
        await tellPortal('a release deleted here', conn => conn.query(
            `UPDATE ${jportalSchema()}.telex_submissions SET status = 'void' WHERE release_id = ? AND status = 'received'`, [id]
        ));
        return answer(200, { deleted: true });
    }));

    app.get('/api/v1/telex-submissions', handle('GET /telex-submissions', async (req) => {
        const all = req.query.status === 'all';
        let rows;
        try {
            rows = await withConnection(async (conn) => {
                const [found] = await conn.query(
                    `${SUBMISSION_SQL(jportalSchema())}
                      WHERE t.transfer_id IS NOT NULL AND ${all ? "t.status IN ('open', 'received', 'rejected')" : "t.status = 'open'"}
                        AND f.status = 'sent' AND f.final_s3_key IS NOT NULL
                      ORDER BY t.created_at DESC, t.id DESC
                      LIMIT 300`
                );
                return found;
            });
        } catch (e) {
            if (isSourceMissing(e)) return answer(200, { data: [], unavailable: true, canFile: T.mayFile(req.userType) });
            throw e;
        }
        return answer(200, { data: rows.map(submissionJson), unavailable: false, canFile: T.mayFile(req.userType) });
    }));

    const readSubmission = async (conn, id, lock = false) => {
        const [rows] = await conn.query(`${SUBMISSION_SQL(jportalSchema())} WHERE t.id = ? AND t.transfer_id IS NOT NULL${lock ? ' FOR UPDATE OF t' : ''}`, [id]);
        return rows[0] || null;
    };
    const gateOf = row => (row ? { status: row.status, sent: fileSent(row), shipmentLive: !!row.shipment_live } : null);
    const portalMissing = () => answer(503, { error: 'The supplier portal\'s uploads are not available on this server.', code: 'PORTAL_UNAVAILABLE' });

    app.get('/api/v1/telex-submissions/:id(\\d+)/file', handle('GET /telex-submissions/:id/file', async (req) => {
        const id = Number(req.params.id);
        const bucket = portalBucket();
        if (!bucket) return answer(500, { error: 'PI_UPLOADS_BUCKET env var not configured.' });
        let row;
        try { row = await withConnection(conn => readSubmission(conn, id)); } catch (e) { if (isSourceMissing(e)) return portalMissing(); throw e; }
        if (!row || !fileSent(row)) return answer(404, { error: 'Telex upload not found.', code: 'NOT_FOUND' });
        const url = await getSignedUrl(s3, new GetObjectCommand({
            Bucket: bucket, Key: row.final_s3_key,
            ResponseContentType: row.content_type || 'application/octet-stream',
            ResponseContentDisposition: 'inline',
        }), { expiresIn: VIEW_SECONDS });
        return answer(200, { url, filename: row.filename, expiresIn: VIEW_SECONDS });
    }));

    app.post('/api/v1/telex-submissions/:id(\\d+)/accept', handle('POST /telex-submissions/:id/accept', async (req) => {
        const actor = actorOf(req);
        const id = Number(req.params.id);
        const bucket = portalBucket();
        if (!bucket || !poBucket) return answer(500, { error: 'PI_UPLOADS_BUCKET / PO_DOCS_BUCKET not configured.' });

        // 1. The rules, before anything is read from S3. Asked twice, it is
        //    answered with what the first one filed.
        let row;
        try { row = await withConnection(conn => readSubmission(conn, id)); } catch (e) { if (isSourceMissing(e)) return portalMissing(); throw e; }
        if (row && row.status === 'received' && row.release_id != null && T.mayFile(actor.userType)) {
            const [filed] = await withConnection(conn => conn.query(`SELECT ${RELEASE_COLUMNS} FROM shipment_telex_releases r WHERE r.id = ?`, [row.release_id]));
            if (filed[0]) return answer(200, { release: releaseJson(filed[0]), submission: submissionJson(row), replayed: true });
        }
        const no = T.decisionRefusal(gateOf(row), { userType: actor.userType, action: 'accept' });
        if (no) return refused(no);

        // 2. The file, outside any transaction: read pinned to the ETag the
        //    portal stored, checked for what it is, written to our bucket.
        let bytes;
        try {
            const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: row.final_s3_key, ...(row.final_etag ? { IfMatch: row.final_etag } : {}) }));
            if (Number(obj.ContentLength) > T.MAX_BYTES) {
                if (obj.Body && typeof obj.Body.destroy === 'function') obj.Body.destroy();
                return refused(T.uploadRefusal({ filename: row.filename, bytes: { length: T.MAX_BYTES + 1 }, supplierName: row.supplier_name, shipment: { id: row.shipment_id } }));
            }
            bytes = Buffer.from(await obj.Body.transformToByteArray());
        } catch (e) {
            log.warn('[POST /telex-submissions/:id/accept] the file could not be read', { id, error: e.message });
            return answer(409, { error: 'The supplier\'s file could not be read from the portal. Send it back and ask for it again.', code: 'FILE_UNAVAILABLE', allowedNext: ['reject'] });
        }
        const bad = T.uploadRefusal({ filename: row.filename || 'telex-release', bytes, supplierName: row.supplier_name, shipment: { id: row.shipment_id } });
        if (bad) return refused({ ...bad, status: bad.status === 404 ? 409 : bad.status, allowedNext: ['reject'] });
        const contentType = T.typeOf(bytes);
        const filename = String(row.filename || 'telex-release').trim().slice(0, 200);
        const s3Key = T.s3KeyFor(uuidv4(), filename, contentType);
        await s3.send(new PutObjectCommand({
            Bucket: poBucket, Key: s3Key, Body: bytes, ContentType: contentType,
            ContentDisposition: `inline; filename="${s3Key.slice(s3Key.lastIndexOf('/') + 1)}"`,
        }));

        // 3. One transaction: the upload locked and checked again, the release
        //    written, the portal's row pointed at it.
        const out = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const locked = await readSubmission(conn, id, true);
                const again = T.decisionRefusal(gateOf(locked), { userType: actor.userType, action: 'accept' });
                if (again) { await conn.rollback(); return { no: again }; }
                let insertId;
                try {
                    const [ins] = await conn.query(
                        `INSERT INTO shipment_telex_releases
                            (shipment_id, shipment_reference, supplier_name, filename, s3_key, public_url, content_type, file_size, source, portal_submission_id, notes, uploaded_by_email)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'portal', ?, ?, ?)`,
                        [
                            locked.shipment_id, locked.shipment_reference || null, locked.supplier_name, filename, s3Key, publicS3Url(s3Key), contentType, bytes.length, id,
                            `Uploaded by the supplier on the supplier portal (telex upload ${id}).`, locked.user_email || null,
                        ]
                    );
                    insertId = ins.insertId;
                } catch (e) {
                    // Filed by a request that ran beside this one: theirs stands.
                    if (e && e.errno === ER_DUP_ENTRY) { await conn.rollback(); return { duplicate: true }; }
                    throw e;
                }
                const [done] = await conn.query(
                    `UPDATE ${jportalSchema()}.telex_submissions
                        SET status = 'received', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = NULL, release_id = ?
                      WHERE id = ? AND status = 'open'`,
                    [actor.email || 'unknown', insertId, id]
                );
                if (done.affectedRows !== 1) throw new Error(`telex_submissions ${id} was not open at accept`);
                await recordAudit(conn, {
                    entityType: 'shipment_telex_release', entityId: insertId, action: 'create', before: null,
                    after: { shipmentId: locked.shipment_id, supplierName: locked.supplier_name, filename, fileSize: bytes.length, source: 'portal', telexSubmissionId: id, uploadedByEmail: locked.user_email || null },
                    userEmail: actor.email,
                });
                await conn.commit();
                const [back] = await conn.query(`SELECT ${RELEASE_COLUMNS} FROM shipment_telex_releases r WHERE r.id = ?`, [insertId]);
                return { release: back[0], submission: await readSubmission(conn, id) };
            } catch (e) {
                await conn.rollback();
                throw e;
            }
        });
        if (out.no) return refused(out.no);
        if (out.duplicate) {
            const again = await withConnection(async (conn) => {
                const sub = await readSubmission(conn, id);
                const [filed] = await conn.query(`SELECT ${RELEASE_COLUMNS} FROM shipment_telex_releases r WHERE r.portal_submission_id = ?`, [id]);
                return { sub, filed: filed[0] || null };
            });
            if (!again.filed) return answer(409, { error: 'This upload was decided meanwhile. Reload.', code: 'ALREADY_DECIDED', allowedNext: ['reload'] });
            return answer(200, { release: releaseJson(again.filed), submission: submissionJson(again.sub), replayed: true });
        }
        return answer(200, { release: releaseJson(out.release), submission: submissionJson(out.submission), replayed: false });
    }));

    app.post('/api/v1/telex-submissions/:id(\\d+)/reject', handle('POST /telex-submissions/:id/reject', async (req) => {
        const actor = actorOf(req);
        const id = Number(req.params.id);
        const note = T.cleanNote(req.body && req.body.note);
        let out;
        try {
            out = await withConnection(async (conn) => {
                await conn.beginTransaction();
                try {
                    const locked = await readSubmission(conn, id, true);
                    const no = T.decisionRefusal(gateOf(locked), { userType: actor.userType, action: 'reject', note });
                    if (no) { await conn.rollback(); return { no }; }
                    await conn.query(
                        `UPDATE ${jportalSchema()}.telex_submissions
                            SET status = 'rejected', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?
                          WHERE id = ? AND status = 'open'`,
                        [actor.email || 'unknown', note, id]
                    );
                    await recordAudit(conn, {
                        entityType: 'telex_submission', entityId: id, action: 'reject',
                        before: { status: 'open' },
                        after: { status: 'rejected', note, shipmentId: locked.shipment_id, supplierName: locked.supplier_name },
                        userEmail: actor.email,
                    });
                    await conn.commit();
                    return { submission: await readSubmission(conn, id) };
                } catch (e) {
                    await conn.rollback();
                    throw e;
                }
            });
        } catch (e) {
            if (isSourceMissing(e)) return portalMissing();
            throw e;
        }
        if (out.no) return refused(out.no);
        return answer(200, { submission: submissionJson(out.submission) });
    }));
}

module.exports = { registerTelexReleaseRoutes, loadFor, releaseJson, submissionJson };
