'use strict';

// A forwarder's files (docs/forwarder-containers.md, slice C): the invoice,
// proof-of-loading photos and bill of lading a freight forwarder uploads on a
// container's page of the supplier portal, and what our team do with each.
//
//   GET    /api/v1/forwarder-uploads/:id/file     a five-minute link to the file, in the portal's bucket
//   POST   /api/v1/forwarder-uploads/:id/accept   { preset? }  file it with the container
//   POST   /api/v1/forwarder-uploads/:id/reject   { note }     send it back; the forwarder reads the note
//   DELETE /api/v1/forwarder-documents/:id        remove a filed bill of lading (an admin, or whoever accepted it)
// Deciding: admin and standard users (403 CANNOT_DECIDE otherwise; the tab's
// read says `canDecide` first, and ShipLine never calls a route that would
// answer 403).
//
// The portal keeps one row of its forwarder_uploads per file, the file in its
// own bucket at `final_key`: a key of one finalise attempt's own, which never
// changes once the row is `sent`. So the object our team view is the object
// read here.
//
// ACCEPT, in the order the telex release's accept taught us:
//   1. The rules, before anything is read from S3. Asked again for one already
//      received, it answers with what was filed.
//   2. The row is given its `filed_key` if it has none (our bucket's folder for
//      the kind, a random part made ONCE and kept, a clean name), in a short
//      transaction. Then, outside any transaction: the object is read from the
//      portal's bucket BY ITS KEY, with no ETag condition (the ETag on the row
//      is the staging object's, and S3 does not promise a copy keeps it),
//      checked for what it is and its size, and written to our bucket at that
//      key. A retry writes over its own copy and never makes a second.
//   3. One transaction under the locks (the shipment, its live booking, their
//      forwarders, then the upload's row): still sent and undecided, still the
//      same file, the container still that forwarder's; the target row is
//      inserted with `forwarder_upload_id` (unique: a request that ran beside
//      this one files once, and both are answered with that row); the portal's
//      row is marked `received`.
//   4. An invoice's reading starts after the response, as for a staff upload.
//
// What a file is filed as:
//   invoice         shipment_payment_documents, a balance invoice on the
//                   shipment, under the name that forwarder is paid under there
//                   (else its name on the Forwarders list), with forwarder_id
//   loading_photo   container_photos, under the container's PRESENT number,
//                   preset "Proof of loading" unless the call names another
//   bill_of_lading  shipment_forwarder_documents (never shipments.bl_number,
//                   which drives a payment flag)
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { v4: uuidv4 } = require('uuid');
const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl: defaultGetSignedUrl } = require('@aws-sdk/s3-request-presigner');
const FC = require('../lib/forwarder-containers');
const { supplierKey } = require('../lib/payment-documents');
const { jportalSchema } = require('../lib/jportal');
const defaultLog = require('../lib/logger');

const ER_NO_SUCH_TABLE = 1146;
const ER_BAD_DB = 1049;
const ER_DUP_ENTRY = 1062;
const ER_LOCK_DEADLOCK = 1213;
const ISO = "'%Y-%m-%dT%H:%i:%sZ'";
const VIEW_SECONDS = 300;
const DEFAULT_PHOTO_PRESET = 'Proof of loading';
const LIST_LIMIT = 200;

const UPLOAD_REFUSAL_TEXT = Object.freeze({
    NO_SUCH_UPLOAD: 'That upload no longer exists.',
    WITHDRAWN: 'The forwarder took this file back.',
    NOT_SENT: 'This file has not been sent to us yet.',
    ALREADY_DECIDED: 'This file was already accepted or sent back.',
    NOT_THEIR_CONTAINER: 'This container is no longer that forwarder’s, so their file cannot be accepted. It can be dismissed.',
    NOT_THEIR_DUTY: 'This forwarder no longer does the part of the job this file is about, so it cannot be accepted. It can be dismissed.',
    CLIENT_TOO_OLD: 'This page is older than what the forwarder sent, so it cannot show it. Reload ShipLine, then decide.',
    FILE_UNAVAILABLE: 'The forwarder’s file could not be read from the portal. Send it back and ask for it again.',
    FILE_NOT_ITS_TYPE: 'The file is not the kind of file it was sent as. Send it back.',
    FILE_SIZE_CHANGED: 'The file is not the size the portal recorded. Send it back and ask for it again.',
    FILE_TOO_LARGE: 'The file is too large for its kind. Send it back.',
    FILE_CHANGED: 'The upload changed while it was being accepted. Reload and look again.',
    BAD_REFERENCE: 'This customs entry came with no valid MRN (18 letters and digits), so it cannot be kept. Send it back.',
    NO_FILING_NAME: 'This forwarder has no name to file an invoice under. Send it back, or fix the forwarder on the Forwarders list.',
    FILING_NAME_CLASH: 'The name this invoice would be filed under is that of a supplier with goods on this container, so it would be taken for theirs. Send it back, or upload it by hand in the INVOICES tab.',
    DUPLICATE_DOCUMENT: 'A file of this name and size is already on this shipment.',
    RETRY: 'The container was busy. Try again.',
});
const NOT_MIGRATED = { error: 'The forwarder container page is not set up on this server yet: its tables have not been migrated.', code: 'NOT_MIGRATED' };
const PORTAL_UNAVAILABLE = { error: 'What forwarders send cannot be read on this server: the portal’s tables are not here.', code: 'PORTAL_UNAVAILABLE' };

class Refusal extends Error {
    constructor(status, code, extra, error) {
        super(error || UPLOAD_REFUSAL_TEXT[code] || code);
        this.status = status;
        this.body = { error: error || UPLOAD_REFUSAL_TEXT[code] || code, code, ...(extra || {}) };
    }
}
const refuse = (code, allowedNext = ['reload'], extra = {}, status = 409) => new Refusal(status, code, { allowedNext, ...extra });
const gateRefusal = no => new Refusal(no.status, no.code, { allowedNext: no.allowedNext, ...(no.kind ? { kind: no.kind } : {}) });
// A missing table here is the portal's, unless MySQL names one of ours (a server whose own
// migration has not run answers NOT_MIGRATED).
const OWN_TABLES = /shipment_forwarder_documents|shipment_payment_documents|container_photos|payment_extras/i;
const portalMissing = e => Boolean(e) && (e.errno === ER_NO_SUCH_TABLE || e.errno === ER_BAD_DB) && !OWN_TABLES.test(String(e.sqlMessage || e.message || ''));

const UPLOAD_COLUMNS = `u.id, u.shipment_id, u.container_ref, u.forwarder_id, u.kind, u.user_email, u.user_name, u.filename, u.content_type, u.file_size,
        u.final_key, u.s3_etag, u.note, u.upload_status, u.review_status, u.decided_note, u.decided_by, u.filed_key, u.filed_id,
        DATE_FORMAT(u.sent_at, ${ISO}) AS sent_at_iso, DATE_FORMAT(u.decided_at, ${ISO}) AS decided_at_iso, DATE_FORMAT(u.withdrawn_at, ${ISO}) AS withdrawn_at_iso`;
// With whose it is: the portal's own view saying what that forwarder holds on the container now
// (a full row: the freight holder's 'booking' / 'container', or since 2026-11-24 a delivery or
// customs holder's 'duty'). `theirs` is then: it still holds the duty the file's kind belongs to.
const UPLOAD_FROM = jp => `FROM ${jp}.forwarder_uploads u
           LEFT JOIN portal_forwarder_containers_v c
                  ON c.shipment_id = u.shipment_id AND c.forwarder_id = u.forwarder_id AND c.link_source IN ('booking', 'container', 'duty')
           LEFT JOIN forwarders f ON f.id = u.forwarder_id`;
const UPLOAD_SELECT = jp => `SELECT ${UPLOAD_COLUMNS}, f.name AS forwarder_name, (c.shipment_id IS NOT NULL) AS theirs,
        c.does_freight, c.does_delivery, c.does_customs ${UPLOAD_FROM(jp)}`;

const STATE_OF = Object.freeze({ open: 'open', received: 'received', rejected: 'rejected', withdrawn: 'withdrawn', not_sent: 'not_sent' });

/**
 * An upload as ShipLine shows it. Never a storage key. `held`: the duties its forwarder holds
 * on the container (by default, what the joined view row says).
 */
function uploadJson(row, held = FC.dutiesOfFlags(row)) {
    const state = STATE_OF[FC.uploadReviewState(row)];
    const theirs = FC.kindAllowed(row.kind, held);
    // A customs entry's note is its MRN (the portal has one text column to write): shown as
    // the file's reference. A value that is not an MRN stays readable as the note it is.
    const ref = FC.uploadReference(row);
    return {
        id: Number(row.id),
        kind: row.kind,
        shipmentId: Number(row.shipment_id),
        forwarderId: Number(row.forwarder_id),
        forwarderName: row.forwarder_name || null,
        filename: row.filename,
        contentType: row.content_type || null,
        fileSize: Number(row.file_size),
        note: ref && ref.reference ? null : (row.note || null),
        reference: (ref && ref.reference) || null,
        byName: row.user_name || null,
        byEmail: row.user_email || null,
        sentAt: row.sent_at_iso || null,
        state,
        waiting: state === 'open',
        // Its forwarder still holds the duty this kind of file belongs to: it can be accepted. Otherwise only dismissed.
        theirs,
        actionable: state === 'open' && theirs,
        // What its forwarder holds on the container now.
        duties: held,
        decidedNote: row.decided_note || null,
        decidedBy: row.decided_by || null,
        decidedAt: row.decided_at_iso || null,
        filedId: row.filed_id == null ? null : Number(row.filed_id),
    };
}

const GROUP_OF = Object.freeze({ invoice: 'invoice', loading_photo: 'loadingPhotos', bill_of_lading: 'billOfLading', customs_draft: 'customsDraft', customs_entry: 'customsEntry' });

const noFiles = () => Object.fromEntries(Object.values(GROUP_OF).map(group => [group, []]));

/**
 * The files forwarders sent for a container, as the FORWARDER tab shows them.
 * `duties`: who holds what on the container now (dutiesOf), which is what says
 * whose a file still is here, not the view's row.
 * → { unavailable, byForwarder, strays }:
 *   byForwarder  Map of forwarder id → { invoice, loadingPhotos, billOfLading, customsDraft, customsEntry }
 *                for every forwarder that HOLDS A DUTY on the container: what it
 *                sent, newest first, whatever became of it. A file of a kind its
 *                forwarder no longer holds the duty for is here too, not actionable.
 *   strays       undecided files of a forwarder that holds NOTHING on the
 *                container now: they can be dismissed, never accepted.
 */
async function readUploads(conn, shipmentId, duties, env = process.env) {
    const out = { unavailable: false, byForwarder: new Map(), strays: [] };
    let rows;
    try {
        [rows] = await conn.query(
            `${UPLOAD_SELECT(jportalSchema(env))} WHERE u.shipment_id = ? AND u.upload_status = 'sent' ORDER BY u.id DESC LIMIT 400`,
            [shipmentId]
        );
    } catch (e) {
        if (!(e && (e.errno === ER_NO_SUCH_TABLE || e.errno === ER_BAD_DB))) throw e;
        return { ...out, unavailable: true };
    }
    for (const row of rows) {
        const fid = Number(row.forwarder_id);
        const held = FC.heldBy(duties, fid);
        const json = uploadJson(row, held);
        if (held.length) {
            if (!out.byForwarder.has(fid)) out.byForwarder.set(fid, noFiles());
            if (GROUP_OF[row.kind]) out.byForwarder.get(fid)[GROUP_OF[row.kind]].push(json);
        } else if (json.waiting) {
            out.strays.push(json);
        }
    }
    return out;
}

const DOCUMENT_COLUMNS = `d.id, d.shipment_id, d.forwarder_id, d.forwarder_name, d.kind, d.reference, d.filename, d.public_url, d.content_type, d.file_size,
        d.forwarder_upload_id, d.uploaded_by_email, d.accepted_by_email, d.deleted_at, DATE_FORMAT(d.created_at, ${ISO}) AS created_at_iso`;
const documentJson = (d, actor) => ({
    id: Number(d.id),
    kind: d.kind,
    // A customs entry's MRN; null for every other kind.
    reference: d.reference || null,
    forwarderId: d.forwarder_id == null ? null : Number(d.forwarder_id),
    forwarderName: d.forwarder_name || null,
    filename: d.filename,
    url: d.public_url,
    contentType: d.content_type || null,
    fileSize: Number(d.file_size),
    uploadedByEmail: d.uploaded_by_email || null,
    acceptedByEmail: d.accepted_by_email || null,
    createdAt: d.created_at_iso || null,
    canRemove: Boolean(actor) && (actor.userType === 'admin' || (Boolean(actor.email) && actor.email === d.accepted_by_email)),
});

/** A container's filed forwarder documents (its bills of lading), newest first. */
async function readDocuments(conn, shipmentId, actor) {
    const [rows] = await conn.query(
        `SELECT ${DOCUMENT_COLUMNS} FROM shipment_forwarder_documents d WHERE d.shipment_id = ? AND d.deleted_at IS NULL ORDER BY d.id DESC`,
        [shipmentId]
    );
    return rows.map(d => documentJson(d, actor));
}

/**
 * What forwarders uploaded, across containers, for the Freight page's list:
 * what waits on a container still theirs (the default), or everything sent.
 * → rows shaped like a proposal's row of that list, `kind` the upload's.
 */
async function listUploads(conn, all, env = process.env) {
    const jp = jportalSchema(env);
    const [rows] = await conn.query(
        `SELECT ${UPLOAD_COLUMNS}, f.name AS forwarder_name, (c.shipment_id IS NOT NULL) AS theirs,
                c.does_freight, c.does_delivery, c.does_customs,
                COALESCE(c.reference, s.reference, u.container_ref) AS reference, COALESCE(c.mode, s.mode) AS mode
           ${UPLOAD_FROM(jp)}
           LEFT JOIN shipments s ON s.id = u.shipment_id
          WHERE u.upload_status = 'sent'${all ? '' : " AND u.review_status = 'open' AND c.shipment_id IS NOT NULL"}
          ORDER BY u.id DESC
          LIMIT ${LIST_LIMIT}`
    );
    const out = [];
    for (const r of rows) {
        const u = uploadJson(r);
        // "Still theirs" needs the kind's duty, which the SQL above cannot say: a waiting
        // file of a company that has since lost that duty is listed only with `all`.
        if (!all && !u.actionable) continue;
        out.push({
            kind: u.kind, id: u.id, shipmentId: u.shipmentId, reference: r.reference || null, mode: r.mode || null,
            forwarderId: u.forwarderId, forwarderName: u.forwarderName, duties: u.duties,
            status: u.state === 'open' ? 'pending' : u.state === 'received' ? 'applied' : u.state,
            waiting: u.waiting, live: true, theirs: u.theirs, actionable: u.actionable,
            filename: u.filename, fileSize: u.fileSize, requested: {},
            byName: u.byName, byEmail: u.byEmail, at: u.sentAt, decidedAt: u.decidedAt,
        });
    }
    return out;
}

function registerForwarderUploadRoutes(app, deps) {
    const {
        withConnection, recordAudit, s3, poBucket, publicS3Url,
        ready = async () => {},
        // The usual background read of a filed invoice, after the response.
        startExtraction = () => {},
        // A shipment payment document as the invoice routes answer it.
        documentJson: invoiceJson = d => ({ id: d.id, filename: d.filename }),
        getSignedUrl = defaultGetSignedUrl,
        log = defaultLog,
        env = process.env,
    } = deps;
    const portalBucket = () => env.PI_UPLOADS_BUCKET || process.env.PI_UPLOADS_BUCKET || null;
    const jp = () => jportalSchema(env);

    const route = (label, fn) => async (req, res) => {
        try {
            await ready();
            await fn(req, res);
        } catch (e) {
            if (res.headersSent) return;
            if (e instanceof Refusal) return res.status(e.status).json(e.body);
            if (portalMissing(e)) return res.status(503).json(PORTAL_UNAVAILABLE);
            if (e && e.errno === ER_NO_SUCH_TABLE) return res.status(503).json(NOT_MIGRATED);
            log.error(`[${label}]`, e);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };
    const mustDecide = (req) => {
        if (!FC.mayDecide(req.userType)) {
            throw new Refusal(403, 'CANNOT_DECIDE', null, 'Your role cannot decide what a forwarder sends: admin and standard users do.');
        }
    };
    const actorOf = req => ({ email: req.userEmail || null, userType: req.userType });
    const idOf = (req) => {
        const id = Number(req.params.id);
        if (!Number.isSafeInteger(id) || id <= 0) throw new Refusal(400, 'BAD_FIELD', { field: 'id' }, 'Not an id.');
        return id;
    };

    const readOne = async (conn, id) => (await conn.query(`${UPLOAD_SELECT(jp())} WHERE u.id = ?`, [id]))[0][0] || null;
    const lockOne = async (conn, id) => (await conn.query(`SELECT ${UPLOAD_COLUMNS} FROM ${jp()}.forwarder_uploads u WHERE u.id = ? FOR UPDATE`, [id]))[0][0] || null;

    async function inTransaction(conn, fn) {
        await conn.beginTransaction();
        try {
            const out = await fn();
            await conn.commit();
            return out;
        } catch (e) {
            await conn.rollback().catch(() => {});
            throw e;
        }
    }
    // The legacy order route locks in another order: a deadlock is tried once more.
    async function inDecision(conn, fn) {
        for (let attempt = 1; ; attempt++) {
            try {
                return await inTransaction(conn, fn);
            } catch (e) {
                if (!e || e.errno !== ER_LOCK_DEADLOCK) throw e;
                if (attempt >= 2) throw new Refusal(503, 'RETRY', { allowedNext: ['retry'] });
            }
        }
    }

    // What an upload was filed as, by the id on its row; null when that was removed since.
    async function filedOf(conn, row, by = 'id') {
        const key = by === 'id' ? row.filed_id : row.id;
        if (key == null) return null;
        if (row.kind === 'invoice') {
            const [rows] = await conn.query(`SELECT * FROM shipment_payment_documents WHERE ${by === 'id' ? 'id' : 'forwarder_upload_id'} = ?`, [key]);
            return rows[0] && !rows[0].deleted_at ? { as: 'invoice', id: Number(rows[0].id), document: invoiceJson(rows[0]) } : null;
        }
        if (row.kind === 'loading_photo') {
            const [rows] = await conn.query(`SELECT id, filename, deleted_at FROM container_photos WHERE ${by === 'id' ? 'id' : 'forwarder_upload_id'} = ?`, [key]);
            return rows[0] && !rows[0].deleted_at ? { as: 'photo', id: Number(rows[0].id) } : null;
        }
        const [rows] = await conn.query(`SELECT ${DOCUMENT_COLUMNS} FROM shipment_forwarder_documents d WHERE d.${by === 'id' ? 'id' : 'forwarder_upload_id'} = ?`, [key]);
        return rows[0] && !rows[0].deleted_at ? { as: 'document', id: Number(rows[0].id), document: documentJson(rows[0], null) } : null;
    }

    // ── GET /api/v1/forwarder-uploads/:id/file ──────────────────────────────
    app.get('/api/v1/forwarder-uploads/:id(\\d+)/file', route('GET /forwarder-uploads/:id/file', async (req, res) => {
        const id = idOf(req);
        const bucket = portalBucket();
        if (!bucket) throw new Refusal(500, 'NOT_CONFIGURED', null, 'PI_UPLOADS_BUCKET env var not configured.');
        const row = await withConnection(conn => readOne(conn, id));
        if (!row || row.upload_status !== 'sent' || !row.final_key) throw new Refusal(404, 'NOT_FOUND', null, 'Upload not found.');
        const url = await getSignedUrl(s3, new GetObjectCommand({
            Bucket: bucket, Key: row.final_key,
            ResponseContentType: row.content_type || 'application/octet-stream',
            ResponseContentDisposition: 'inline',
        }), { expiresIn: VIEW_SECONDS });
        res.json({ url, filename: row.filename, contentType: row.content_type || null, expiresIn: VIEW_SECONDS });
    }));

    // ── POST /api/v1/forwarder-uploads/:id/accept ───────────────────────────
    app.post('/api/v1/forwarder-uploads/:id(\\d+)/accept', route('POST /forwarder-uploads/:id/accept', async (req, res) => {
        mustDecide(req);
        const id = idOf(req);
        const actor = actorOf(req);
        const bucket = portalBucket();
        if (!bucket || !poBucket) throw new Refusal(500, 'NOT_CONFIGURED', null, 'PI_UPLOADS_BUCKET / PO_DOCS_BUCKET not configured.');
        const preset = typeof (req.body || {}).preset === 'string' && req.body.preset.trim() ? req.body.preset.trim().slice(0, 100) : DEFAULT_PHOTO_PRESET;

        // 1. The rules, before anything is read from S3.
        const first = await withConnection(async (conn) => {
            const row = await readOne(conn, id);
            if (row && row.upload_status === 'sent' && row.review_status === 'received') {
                return { replay: { filed: await filedOf(conn, row), upload: uploadJson(row), replayed: true } };
            }
            return { row };
        });
        if (first.replay) return res.json(first.replay);
        const row = first.row;
        // A kind of file from after 2026-11-24 (a draft customs entry) is decided only by a
        // ShipLine that NAMES it: a build from before it has no words for it, and must not
        // file it (or, below, send it back) unseen.
        if (row && !FC.clientKnowsKind(row.kind, (req.body || {}).kind)) throw refuse('CLIENT_TOO_OLD', ['reload']);
        // Whose it is, as the portal's view has it now; asked again under the locks below.
        const early = FC.uploadGate({ row, duties: row ? FC.dutiesFromFlags(row) : null, action: 'accept' });
        if (early) throw gateRefusal(early);
        // A customs entry is kept under its MRN: one that came with none, or with something
        // that is not one, is refused here, before anything is copied. It can be sent back.
        const reference = FC.uploadReference(row);
        if (reference && reference.problem) throw refuse('BAD_REFERENCE', ['reject']);

        // Received by a request that ran beside this one: theirs stands, and this caller is told what it filed.
        const toldWhatWasFiled = async () => {
            const again = await withConnection(async (conn) => {
                const now = await readOne(conn, id);
                return { now, filed: now ? await filedOf(conn, now, 'upload') : null };
            });
            if (!again.filed) throw refuse('ALREADY_DECIDED');
            return res.json({ filed: again.filed, upload: uploadJson(again.now), replayed: true });
        };
        const receivedAlready = r => Boolean(r) && r.upload_status === 'sent' && r.review_status === 'received';

        // 2. Where our copy goes (made once, kept), then the copy itself, outside any transaction.
        // The row is read again here, locked: the first read was a plain one, and nothing is
        // copied for a file taken back or decided since.
        const filedKey = await withConnection(conn => inTransaction(conn, async () => {
            const locked = await lockOne(conn, id);
            if (!locked) throw refuse('NO_SUCH_UPLOAD', ['reload'], {}, 404);
            if (receivedAlready(locked)) return null;
            const gone = FC.uploadGate({ row: locked, duties: null, action: 'reject' });
            if (gone) throw gateRefusal(gone);
            if (locked.filed_key) return locked.filed_key;
            const key = FC.filedKeyFor({ kind: locked.kind, random: uuidv4(), filename: locked.filename, contentType: locked.content_type });
            await conn.query(`UPDATE ${jp()}.forwarder_uploads SET filed_key = ? WHERE id = ? AND filed_key IS NULL`, [key, id]);
            return key;
        }));
        if (filedKey === null) return toldWhatWasFiled();
        let bytes;
        try {
            const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: row.final_key }));
            const cap = (FC.UPLOAD_RULES[row.kind] || { maxBytes: 0 }).maxBytes;
            if (Number(obj.ContentLength) > cap) {
                if (obj.Body && typeof obj.Body.destroy === 'function') obj.Body.destroy();
                throw refuse('FILE_TOO_LARGE', ['reject']);
            }
            bytes = Buffer.from(await obj.Body.transformToByteArray());
        } catch (e) {
            if (e instanceof Refusal) throw e;
            log.warn('[POST /forwarder-uploads/:id/accept] the file could not be read', { id, error: e.message });
            throw refuse('FILE_UNAVAILABLE', ['reject']);
        }
        const wrong = FC.uploadFileRefusal({ row, bytes });
        if (wrong) throw refuse(wrong, ['reject']);
        const shownName = filedKey.slice(filedKey.lastIndexOf('/') + 1);
        await s3.send(new PutObjectCommand({
            Bucket: poBucket, Key: filedKey, Body: bytes, ContentType: row.content_type,
            ContentDisposition: `inline; filename="${shownName}"`,
        }));

        // 3. One transaction under the locks.
        const { lockOwnership } = require('./forwarder-container-routes');
        const out = await withConnection(conn => inDecision(conn, async () => {
            const { shipment, duties, forwarders } = await lockOwnership(conn, Number(row.shipment_id));
            const locked = await lockOne(conn, id);
            // Received meanwhile by a request that held these locks first: theirs stands, and
            // this caller is told what it filed (the row's lock is what serialises two accepts).
            if (receivedAlready(locked)) return { duplicate: true };
            // Its forwarder holds, on the locked rows, the duty this kind of file belongs to.
            const no = FC.uploadGate({ row: locked, duties, action: 'accept' });
            if (no) throw gateRefusal(no);
            // Whose file it is: the company that sent it, which the gate has just found still holds that duty.
            const owner = { forwarderId: Number(locked.forwarder_id) };
            const forwarder = forwarders.get(owner.forwarderId) || null;
            // Both are fixed once the row is sent: a second line of defence.
            if (locked.final_key !== row.final_key || (locked.s3_etag || null) !== (row.s3_etag || null) || locked.filed_key !== filedKey) throw refuse('FILE_CHANGED', ['reload']);

            const reference = shipment.reference || String(shipment.id);
            // The file's own reference number (a customs entry's MRN), from the locked row.
            const fileRef = FC.uploadReference(locked);
            if (fileRef && fileRef.problem) throw refuse('BAD_REFERENCE', ['reject']);
            const filename = String(locked.filename || shownName).trim().slice(0, 200) || shownName;
            const uploader = locked.user_email || null;
            let filed;
            try {
                if (locked.kind === 'invoice') {
                    const [payees] = await conn.query(
                        'SELECT ex.supplier_name FROM payment_extras ex WHERE ex.shipment_id = ? AND ex.forwarder_id = ? AND ex.deleted_at IS NULL ORDER BY ex.id DESC LIMIT 20',
                        [shipment.id, owner.forwarderId]
                    );
                    const name = FC.filingName({ payeeNames: payees.map(p => p.supplier_name), listName: forwarder && forwarder.name });
                    if (!name) throw refuse('NO_FILING_NAME', ['reject']);
                    const [onBoard] = await conn.query(
                        `SELECT DISTINCT o.supplier AS name FROM orders o
                          WHERE o.deleted_at IS NULL AND o.status <> 'DESTROYED' AND o.supplier IS NOT NULL AND o.supplier <> ''
                            AND (o.shipment_id = ? OR o.id IN (SELECT sl.order_id FROM shipment_lines sl WHERE sl.shipment_id = ?))`,
                        [shipment.id, shipment.id]
                    );
                    if (FC.filingNameClash(name, onBoard.map(o => o.name))) throw refuse('FILING_NAME_CLASH', ['reject'], { name });
                    const [dupes] = await conn.query(
                        'SELECT id FROM shipment_payment_documents WHERE deleted_at IS NULL AND filename = ? AND file_size = ? AND shipment_id = ? LIMIT 1',
                        [filename, bytes.length, shipment.id]
                    );
                    if (dupes.length) throw refuse('DUPLICATE_DOCUMENT', ['reject'], { existingId: Number(dupes[0].id) });
                    const [ins] = await conn.query(
                        `INSERT INTO shipment_payment_documents
                            (shipment_id, shipment_reference, supplier_name, supplier_key, payment_id, doc_kind,
                             filename, s3_key, public_url, content_type, file_size, notes, extract_status, uploaded_by_email, forwarder_upload_id, forwarder_id)
                         VALUES (?, ?, ?, ?, NULL, 'balance_invoice', ?, ?, ?, ?, ?, ?, 'processing', ?, ?, ?)`,
                        [shipment.id, reference, name, supplierKey(name), filename, filedKey, publicS3Url(filedKey), locked.content_type, bytes.length,
                            `Uploaded by the forwarder on the supplier portal (forwarder upload ${id}).`, uploader, id, Number(owner.forwarderId)]
                    );
                    filed = { as: 'invoice', id: Number(ins.insertId) };
                    await recordAudit(conn, {
                        entityType: 'shipment_payment_document', entityId: filed.id, action: 'create', before: null,
                        after: { shipmentId: shipment.id, supplierName: name, forwarderId: Number(owner.forwarderId), filename, docKind: 'balance_invoice', fileSize: bytes.length, source: 'portal', forwarderUploadId: id, uploadedByEmail: uploader },
                        userEmail: actor.email,
                    });
                } else if (locked.kind === 'loading_photo') {
                    const description = (locked.note && String(locked.note).trim().slice(0, 1000)) || preset;
                    const [ins] = await conn.query(
                        `INSERT INTO container_photos
                            (container_kind, container_number, draft_container_id, container_name,
                             filename, s3_key, content_type, file_size, preset, description, uploaded_by_email, forwarder_upload_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        ['booked', reference, null, null, filename, filedKey, locked.content_type, bytes.length, preset, description, uploader, id]
                    );
                    filed = { as: 'photo', id: Number(ins.insertId) };
                    await recordAudit(conn, {
                        entityType: 'container_photo', entityId: filed.id, action: 'create', before: null,
                        after: { container: reference, filename, preset, description, source: 'portal', forwarderUploadId: id },
                        userEmail: actor.email,
                    });
                } else {
                    const [ins] = await conn.query(
                        `INSERT INTO shipment_forwarder_documents
                            (shipment_id, shipment_reference, forwarder_id, forwarder_name, kind, filename, s3_key, public_url, content_type, file_size,
                             forwarder_upload_id, uploaded_by_email, accepted_by_email, reference)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        // Kept as its own kind: a bill of lading, or (2026-11-24) an approved draft customs
                        // entry, or the customs entry itself with its MRN as `reference`.
                        [shipment.id, reference, Number(owner.forwarderId), (forwarder && forwarder.name) || null, locked.kind, filename, filedKey, publicS3Url(filedKey),
                            locked.content_type, bytes.length, id, uploader, actor.email || null, fileRef ? fileRef.reference : null]
                    );
                    filed = { as: 'document', id: Number(ins.insertId) };
                    await recordAudit(conn, {
                        entityType: 'shipment_forwarder_document', entityId: filed.id, action: 'create', before: null,
                        after: { shipmentId: shipment.id, forwarderId: Number(owner.forwarderId), kind: locked.kind, filename, fileSize: bytes.length, forwarderUploadId: id, ...(fileRef ? { reference: fileRef.reference } : {}) },
                        userEmail: actor.email,
                    });
                }
            } catch (e) {
                // Filed by a request that ran beside this one: theirs stands.
                if (e && e.errno === ER_DUP_ENTRY) return { duplicate: true };
                throw e;
            }
            const [done] = await conn.query(
                `UPDATE ${jp()}.forwarder_uploads
                    SET review_status = 'received', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = NULL, filed_id = ?
                  WHERE id = ? AND upload_status = 'sent' AND review_status = 'open'`,
                [actor.email || 'shipline', filed.id, id]
            );
            // The row is locked, so this cannot miss; if it does, nothing is committed.
            if (done.affectedRows !== 1) throw new Error(`forwarder_uploads ${id} was not open at accept`);
            await recordAudit(conn, {
                entityType: 'forwarder_upload', entityId: id, action: 'accept',
                before: { reviewStatus: 'open' },
                after: { reviewStatus: 'received', kind: locked.kind, filedAs: filed.as, filedId: filed.id, shipmentId: shipment.id, forwarderId: Number(owner.forwarderId), containerRefWhenSent: locked.container_ref || null },
                userEmail: actor.email,
            });
            return { filed };
        }));

        if (out.duplicate) return toldWhatWasFiled();
        const reply = await withConnection(async (conn) => {
            const now = await readOne(conn, id);
            return { filed: (await filedOf(conn, { ...now, filed_id: out.filed.id })) || out.filed, upload: uploadJson(now) };
        });
        res.json({ ...reply, replayed: false });
        // 4. An invoice's usual read, after the response.
        if (out.filed.as === 'invoice') startExtraction(req, out.filed.id);
    }));

    // ── POST /api/v1/forwarder-uploads/:id/reject ───────────────────────────
    // Allowed on a file whose container is no longer its forwarder's (a dismissal): it asks nothing of ownership.
    app.post('/api/v1/forwarder-uploads/:id(\\d+)/reject', route('POST /forwarder-uploads/:id/reject', async (req, res) => {
        mustDecide(req);
        const id = idOf(req);
        const actor = actorOf(req);
        const problem = FC.noteProblem((req.body || {}).note);
        if (problem) {
            throw new Refusal(400, 'NOTE_REQUIRED', { field: 'note' },
                problem === 'long' ? `The note must be ${FC.NOTE_MAX} characters or fewer.` : `Say what is wrong in at least ${FC.NOTE_MIN} characters: the forwarder reads it.`);
        }
        const note = FC.cleanNote(req.body.note);
        const upload = await withConnection(async (conn) => {
            await inDecision(conn, async () => {
                const locked = await lockOne(conn, id);
                const no = FC.uploadGate({ row: locked, duties: null, action: 'reject' });
                if (no) throw gateRefusal(no);
                // Sending back is a decision too: a ShipLine build that has no words for
                // this kind of file must name it (`kind`), or it cannot send it back.
                if (!FC.clientKnowsKind(locked.kind, (req.body || {}).kind)) throw refuse('CLIENT_TOO_OLD', ['reload']);
                await conn.query(
                    `UPDATE ${jp()}.forwarder_uploads
                        SET review_status = 'rejected', decided_note = ?, decided_by = ?, decided_at = UTC_TIMESTAMP()
                      WHERE id = ? AND upload_status = 'sent' AND review_status = 'open'`,
                    [note, actor.email || 'shipline', id]
                );
                await recordAudit(conn, {
                    entityType: 'forwarder_upload', entityId: id, action: 'reject',
                    before: { reviewStatus: 'open' },
                    after: { reviewStatus: 'rejected', kind: locked.kind, shipmentId: Number(locked.shipment_id), forwarderId: Number(locked.forwarder_id), note },
                    userEmail: actor.email,
                });
            });
            return uploadJson(await readOne(conn, id));
        });
        res.json({ ok: true, upload });
    }));

    // ── DELETE /api/v1/forwarder-documents/:id ──────────────────────────────
    // A filed bill of lading, removed (kept in the table, out of every list) by an
    // admin or by whoever accepted it. 409, never 403, for anyone else who may decide.
    app.delete('/api/v1/forwarder-documents/:id(\\d+)', route('DELETE /forwarder-documents/:id', async (req, res) => {
        mustDecide(req);
        const id = idOf(req);
        const actor = actorOf(req);
        await withConnection(async (conn) => {
            const [rows] = await conn.query(`SELECT ${DOCUMENT_COLUMNS} FROM shipment_forwarder_documents d WHERE d.id = ?`, [id]);
            const doc = rows[0];
            if (!doc || doc.deleted_at) throw new Refusal(404, 'NOT_FOUND', null, 'Document not found.');
            if (!(actor.userType === 'admin' || (actor.email && actor.email === doc.accepted_by_email))) {
                throw new Refusal(409, 'NOT_YOURS_TO_REMOVE', { allowedNext: [] }, 'Only an admin, or whoever accepted this document, can remove it.');
            }
            await conn.query('UPDATE shipment_forwarder_documents SET deleted_at = UTC_TIMESTAMP(), deleted_by_email = ? WHERE id = ? AND deleted_at IS NULL', [actor.email || null, id]);
            await recordAudit(conn, {
                entityType: 'shipment_forwarder_document', entityId: id, action: 'delete',
                before: { shipmentId: Number(doc.shipment_id), filename: doc.filename }, after: null, userEmail: actor.email,
            });
        });
        res.json({ ok: true, id });
    }));
}

module.exports = { registerForwarderUploadRoutes, UPLOAD_REFUSAL_TEXT, readUploads, readDocuments, listUploads, uploadJson };
