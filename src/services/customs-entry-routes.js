'use strict';

// Customs-entry checks (docs/customs-entry-check.md): an AI report on each
// customs document of a container (a forwarder's draft entry, the entry itself,
// or a file our team add), saying whether it matches what we have on file. The
// reading, the sums and the judgement are src/services/customs-entry-check.js;
// this file is where a report is asked for, kept and read.
//
//   GET  /api/v1/shipments/:id/customs-checks
//        the container's customs documents (what forwarders sent, waiting or
//        filed, and files our team added), each with its newest report in short
//   POST /api/v1/shipments/:id/customs-checks        start a report on:
//          { uploadId }               a forwarder's upload
//          { documentId }             a filed document
//          { checkId }                the file of an earlier report (run it again)
//          { missing: true }          every document that has never had one: what
//                                     the page asks when it opens, so that each
//                                     document sent has its report
//          { filename, dataBase64, kind? }   a file of our own: kept, then checked
//        202 { started: [check], alreadyRunning: [check] }; poll the GET below
//   GET  /api/v1/customs-checks/:id                  one report in full
//
// A report is one row of customs_entry_checks, made `processing` here and
// finished in the background after the response (the Lambda is told to wait, as
// for a packing list). Running it again makes a new row: a document's rows are
// its history. Any signed-in staff user may read and ask.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const K = require('./customs-entry-check');
const C = require('../lib/customs-entry-compare');
const FC = require('../lib/forwarder-containers');
const { isAccessDenied } = require('../lib/portal-scan');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const defaultLog = require('../lib/logger');

const ER_NO_SUCH_TABLE = 1146;
const ISO = "'%Y-%m-%dT%H:%i:%sZ'";
const KINDS = Object.freeze(['customs_draft', 'customs_entry']);
const FILE_TYPES = Object.freeze(['application/pdf', 'image/png', 'image/jpeg']);
// base64 in a JSON body, under the gateway's 6 MB cap (as packing-list-routes.js).
const MAX_INLINE_BYTES = 4 * 1024 * 1024;
// A run killed mid-way (a deploy, a timeout) leaves `processing`: after this long it is shown as cut short and may be run again.
const STUCK_MINUTES = 15;
// `missing` starts no more than this many in one call: they run one after another in that call's Lambda.
const MAX_PER_CALL = 4;
const SAFE_FILENAME_RE = /[^A-Za-z0-9._-]+/g;
const NOT_MIGRATED = { error: 'Customs-entry checks are not set up on this server yet: their table has not been migrated.', code: 'NOT_MIGRATED' };

class Refusal extends Error {
    constructor(status, code, error, extra) {
        super(error);
        this.status = status;
        this.body = { error, code, ...(extra || {}) };
    }
}

const CHECK_COLUMNS = `c.id, c.shipment_id, c.source, c.forwarder_upload_id, c.forwarder_document_id, c.kind, c.filename, c.bucket, c.s3_key,
        c.content_type, c.file_size, c.status, c.verdict, c.mrn, c.basis_kind, c.finding_count, c.error_code, c.error_message, c.requested_by_email,
        DATE_FORMAT(c.created_at, ${ISO}) AS created_at_iso, DATE_FORMAT(c.analyzed_at, ${ISO}) AS analyzed_at_iso,
        (c.status = 'processing' AND c.created_at < NOW() - INTERVAL ${STUCK_MINUTES} MINUTE) AS stuck,
        JSON_UNQUOTE(JSON_EXTRACT(c.report_json, '$.summary')) AS summary`;

const parseJson = (v) => { if (v == null) return null; if (typeof v === 'object') return v; try { return JSON.parse(v); } catch { return null; } };

/** A report as the page reads it: in short, or `full` with the report itself. Never a storage key. */
function checkJson(r, { full = false } = {}) {
    const cutShort = Boolean(Number(r.stuck));
    const status = cutShort ? 'failed' : r.status;
    const done = status === 'succeeded';
    return {
        id: Number(r.id),
        shipmentId: Number(r.shipment_id),
        source: r.source,
        uploadId: r.forwarder_upload_id == null ? null : Number(r.forwarder_upload_id),
        documentId: r.forwarder_document_id == null ? null : Number(r.forwarder_document_id),
        kind: r.kind || null,
        filename: r.filename,
        status,
        verdict: done ? r.verdict : null,
        headline: done && r.verdict ? (C.HEADLINE[r.verdict] || null) : null,
        summary: done && r.summary && r.summary !== 'null' ? r.summary : null,
        mrn: r.mrn || null,
        basisKind: r.basis_kind || null,
        findingCount: r.finding_count == null ? null : Number(r.finding_count),
        error: status !== 'failed' ? null : cutShort
            ? { code: 'INTERRUPTED', message: 'The check was cut short (a deploy or a timeout). Run it again.' }
            : { code: r.error_code || 'FAILED', message: r.error_message || null },
        requestedByEmail: r.requested_by_email || null,
        createdAt: r.created_at_iso || null,
        analyzedAt: r.analyzed_at_iso || null,
        ...(full ? { report: parseJson(r.report_json) } : {}),
    };
}

function registerCustomsEntryRoutes(app, deps) {
    const {
        pool, withConnection, s3, poBucket,
        recordAudit = async () => {},
        ready = async () => {},
        runCheck = K.runCustomsEntryCheck,
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
            if (e && e.errno === ER_NO_SUCH_TABLE && /customs_entry_checks/i.test(String(e.sqlMessage || e.message || ''))) return res.status(503).json(NOT_MIGRATED);
            log.error(`[${label}]`, e);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    };
    const idOf = (v, field = 'id') => {
        const id = Number(v);
        if (!Number.isSafeInteger(id) || id <= 0) throw new Refusal(400, 'BAD_FIELD', 'Not an id.', { field });
        return id;
    };

    /** The file a report reads. A portal file its virus scan has not cleared is refused by the bucket: said as such. */
    async function fetchFile(row) {
        const bucket = row.bucket === 'portal' ? portalBucket() : poBucket;
        if (!bucket) throw Object.assign(new Error('The bucket this file is in is not configured on this server.'), { code: 'NOT_CONFIGURED' });
        try {
            const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: row.s3_key }));
            return Buffer.from(await obj.Body.transformToByteArray());
        } catch (e) {
            if (row.bucket === 'portal' && isAccessDenied(e)) {
                throw Object.assign(new Error('The portal has not released this file: its virus scan has not cleared it.'), { code: 'FILE_REFUSED' });
            }
            throw e;
        }
    }

    /**
     * The container's customs documents, newest first, each with where its file
     * is and its newest report (`check`, a row; null when it has never had one).
     * An upload that was accepted and its filed document are ONE document, read
     * from our own copy.
     */
    async function documentsOf(conn, shipmentId) {
        let uploads = [];
        try {
            [uploads] = await conn.query(
                `SELECT u.id, u.kind, u.filename, u.content_type, u.file_size, u.final_key, u.note, u.upload_status, u.review_status,
                        f.name AS forwarder_name, DATE_FORMAT(u.sent_at, ${ISO}) AS at_iso
                   FROM ${jp()}.forwarder_uploads u LEFT JOIN forwarders f ON f.id = u.forwarder_id
                  WHERE u.shipment_id = ? AND u.kind IN (?)
                  ORDER BY u.id DESC LIMIT 100`,
                [shipmentId, KINDS]
            );
        } catch (e) {
            // No portal tables on this server: there are then no uploads to list.
            if (!isSourceMissing(e)) throw e;
        }
        const [filed] = await conn.query(
            `SELECT d.id, d.kind, d.filename, d.content_type, d.file_size, d.s3_key, d.reference, d.forwarder_upload_id, d.forwarder_name,
                    DATE_FORMAT(d.created_at, ${ISO}) AS at_iso
               FROM shipment_forwarder_documents d
              WHERE d.shipment_id = ? AND d.deleted_at IS NULL AND d.kind IN (?)
              ORDER BY d.id DESC LIMIT 100`,
            [shipmentId, KINDS]
        );
        const [checks] = await conn.query(`SELECT ${CHECK_COLUMNS} FROM customs_entry_checks c WHERE c.shipment_id = ? ORDER BY c.id DESC LIMIT 400`, [shipmentId]);

        const out = [];
        const filedByUpload = new Map(filed.filter(d => d.forwarder_upload_id != null).map(d => [Number(d.forwarder_upload_id), d]));
        for (const u of uploads) {
            const state = FC.uploadReviewState(u);
            if (state === 'withdrawn' || state === 'not_sent') continue;
            const d = filedByUpload.get(Number(u.id)) || null;
            const ref = FC.uploadReference(u);
            out.push({
                key: `upload:${u.id}`, source: 'upload', uploadId: Number(u.id), documentId: d ? Number(d.id) : null,
                kind: u.kind, filename: u.filename, contentType: u.content_type || null, fileSize: Number(u.file_size) || null,
                reference: (d && d.reference) || (ref && ref.reference) || null, forwarderName: u.forwarder_name || null,
                state: state === 'open' ? 'waiting' : state === 'received' ? 'filed' : 'sent_back', at: u.at_iso || null,
                file: d ? { bucket: 'ours', s3Key: d.s3_key } : { bucket: 'portal', s3Key: u.final_key },
            });
        }
        const listed = new Set(out.map(o => o.uploadId));
        for (const d of filed) {
            if (d.forwarder_upload_id != null && listed.has(Number(d.forwarder_upload_id))) continue;
            out.push({
                key: `document:${d.id}`, source: 'document', uploadId: null, documentId: Number(d.id),
                kind: d.kind, filename: d.filename, contentType: d.content_type || null, fileSize: Number(d.file_size) || null,
                reference: d.reference || null, forwarderName: d.forwarder_name || null, state: 'filed', at: d.at_iso || null,
                file: { bucket: 'ours', s3Key: d.s3_key },
            });
        }
        // A file of our own is a document by its key in our bucket: its reports are the rows that read it.
        const staffSeen = new Set();
        for (const c of checks) {
            if (c.source !== 'staff' || staffSeen.has(c.s3_key)) continue;
            staffSeen.add(c.s3_key);
            const first = checks.filter(x => x.source === 'staff' && x.s3_key === c.s3_key).pop();
            out.push({
                key: `staff:${first.id}`, source: 'staff', uploadId: null, documentId: null,
                kind: c.kind || null, filename: c.filename, contentType: c.content_type || null, fileSize: c.file_size == null ? null : Number(c.file_size),
                reference: c.mrn || null, forwarderName: null, state: 'ours', at: first.created_at_iso || null,
                file: { bucket: 'ours', s3Key: c.s3_key },
            });
        }
        for (const doc of out) {
            const mine = checks.filter(c => (doc.source === 'staff'
                ? c.source === 'staff' && c.s3_key === doc.file.s3Key
                : (doc.uploadId != null && Number(c.forwarder_upload_id) === doc.uploadId) || (doc.documentId != null && Number(c.forwarder_document_id) === doc.documentId)));
            doc.check = mine[0] || null;
            doc.runs = mine.length;
        }
        return out;
    }

    const documentJson = ({ file, check, ...doc }) => ({ ...doc, check: check ? checkJson(check) : null });
    const running = check => Boolean(check) && check.status === 'processing' && !Number(check.stuck);

    /** A `processing` row for a document. → the row. */
    async function open(conn, shipmentId, doc, userEmail) {
        const [ins] = await conn.query(
            `INSERT INTO customs_entry_checks
                (shipment_id, source, forwarder_upload_id, forwarder_document_id, kind, filename, bucket, s3_key, content_type, file_size, status, requested_by_email)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing', ?)`,
            [shipmentId, doc.source, doc.source === 'upload' ? doc.uploadId : null, doc.source === 'document' ? doc.documentId : null, doc.kind || null,
                String(doc.filename || 'customs-entry').slice(0, 200), doc.file.bucket, doc.file.s3Key, doc.contentType || null, doc.fileSize || null, userEmail || null]
        );
        await recordAudit(conn, {
            entityType: 'customs_entry_check', entityId: ins.insertId, action: 'create', before: null,
            after: { shipmentId, source: doc.source, uploadId: doc.uploadId, documentId: doc.documentId, kind: doc.kind || null, filename: doc.filename },
            userEmail,
        });
        const [rows] = await conn.query(`SELECT ${CHECK_COLUMNS} FROM customs_entry_checks c WHERE c.id = ?`, [ins.insertId]);
        return rows[0];
    }

    /** After the response: the rows' checks, one after another (the pool has one connection). */
    function startRuns(req, ids) {
        if (!ids.length) return;
        if (req && req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const deadline = req && req.lambdaContext && typeof req.lambdaContext.getRemainingTimeInMillis === 'function'
            ? Date.now() + req.lambdaContext.getRemainingTimeInMillis() : null;
        (async () => {
            for (const id of ids) {
                try {
                    const out = await runCheck(pool, { id, fetchFile, deadline });
                    if (out && out.failed) log.warn('[customs-checks] check failed', { id, code: out.failed });
                } catch (e) {
                    log.warn('[customs-checks] check threw', { id, error: e.message });
                }
            }
        })();
    }

    async function mustHaveShipment(conn, id) {
        const [rows] = await conn.query('SELECT id, reference FROM shipments WHERE id = ? AND deleted_at IS NULL', [id]);
        if (!rows[0]) throw new Refusal(404, 'NO_SUCH_SHIPMENT', `Shipment ${id} not found.`);
        return rows[0];
    }

    // ── GET /api/v1/shipments/:id/customs-checks ────────────────────────────
    app.get('/api/v1/shipments/:id(\\d+)/customs-checks', route('GET /shipments/:id/customs-checks', async (req, res) => {
        const id = idOf(req.params.id);
        res.json(await withConnection(async (conn) => {
            await mustHaveShipment(conn, id);
            const documents = await documentsOf(conn, id);
            return { shipmentId: id, documents: documents.map(documentJson), unchecked: documents.filter(d => !d.check).length };
        }));
    }));

    // ── POST /api/v1/shipments/:id/customs-checks ───────────────────────────
    app.post('/api/v1/shipments/:id(\\d+)/customs-checks', route('POST /shipments/:id/customs-checks', async (req, res) => {
        const id = idOf(req.params.id);
        const b = req.body || {};
        const userEmail = req.userEmail || null;

        // A file of our own is put in our bucket first, outside any connection.
        let own = null;
        if (b.dataBase64 !== undefined) {
            if (typeof b.dataBase64 !== 'string' || !b.dataBase64) throw new Refusal(400, 'BAD_FIELD', 'dataBase64 is not a file.', { field: 'dataBase64' });
            if (!poBucket) throw new Refusal(500, 'NOT_CONFIGURED', 'PO_DOCS_BUCKET env var not configured.');
            const bytes = Buffer.from(b.dataBase64, 'base64');
            if (!bytes.length || bytes.length > MAX_INLINE_BYTES) throw new Refusal(413, 'FILE_TOO_LARGE', `The file must be ${MAX_INLINE_BYTES / (1024 * 1024)} MB or smaller.`);
            const contentType = FC.uploadType(bytes);
            if (!FILE_TYPES.includes(contentType)) throw new Refusal(400, 'FILE_NOT_ITS_TYPE', 'The file is not a PDF, PNG or JPEG.');
            const filename = (typeof b.filename === 'string' && b.filename.trim() ? b.filename.trim() : 'customs-entry.pdf').slice(0, 200);
            await withConnection(conn => mustHaveShipment(conn, id));
            const safe = filename.replace(SAFE_FILENAME_RE, '_');
            const s3Key = `customs-checks/${uuidv4()}/${safe}`;
            await s3.send(new PutObjectCommand({ Bucket: poBucket, Key: s3Key, Body: bytes, ContentType: contentType, ContentDisposition: `inline; filename="${safe}"` }));
            own = {
                key: null, source: 'staff', uploadId: null, documentId: null, kind: KINDS.includes(b.kind) ? b.kind : null,
                filename, contentType, fileSize: bytes.length, file: { bucket: 'ours', s3Key }, check: null,
            };
        }

        const out = await withConnection(async (conn) => {
            await mustHaveShipment(conn, id);
            const started = [];
            const alreadyRunning = [];
            const take = async (doc) => {
                if (running(doc.check)) alreadyRunning.push(doc.check);
                else started.push(await open(conn, id, doc, userEmail));
            };
            if (own) { await take(own); return { started, alreadyRunning }; }

            const documents = await documentsOf(conn, id);
            if (b.missing === true) {
                for (const doc of documents.filter(d => !d.check).slice(0, MAX_PER_CALL)) await take(doc);
                return { started, alreadyRunning };
            }
            let doc = null;
            if (b.uploadId !== undefined) doc = documents.find(d => d.uploadId === idOf(b.uploadId, 'uploadId')) || null;
            else if (b.documentId !== undefined) doc = documents.find(d => d.documentId === idOf(b.documentId, 'documentId')) || null;
            else if (b.checkId !== undefined) {
                const checkId = idOf(b.checkId, 'checkId');
                const [rows] = await conn.query('SELECT id, source, forwarder_upload_id, forwarder_document_id, s3_key FROM customs_entry_checks WHERE id = ? AND shipment_id = ?', [checkId, id]);
                const c = rows[0];
                doc = !c ? null : documents.find(d => (c.source === 'staff'
                    ? d.source === 'staff' && d.file.s3Key === c.s3_key
                    : (c.forwarder_upload_id != null && d.uploadId === Number(c.forwarder_upload_id)) || (c.forwarder_document_id != null && d.documentId === Number(c.forwarder_document_id)))) || null;
            } else {
                throw new Refusal(400, 'BAD_FIELD', 'Say what to check: uploadId, documentId, checkId, missing, or a file (filename and dataBase64).', { field: 'uploadId' });
            }
            if (!doc) throw new Refusal(404, 'NO_SUCH_DOCUMENT', 'That customs document is not on this container (any more).', { allowedNext: ['reload'] });
            await take(doc);
            return { started, alreadyRunning };
        });

        res.status(202).json({ started: out.started.map(c => checkJson(c)), alreadyRunning: out.alreadyRunning.map(c => checkJson(c)) });
        startRuns(req, out.started.map(c => Number(c.id)));
    }));

    // ── GET /api/v1/customs-checks/:id ──────────────────────────────────────
    app.get('/api/v1/customs-checks/:id(\\d+)', route('GET /customs-checks/:id', async (req, res) => {
        const id = idOf(req.params.id);
        const row = await withConnection(async (conn) => {
            const [rows] = await conn.query(`SELECT ${CHECK_COLUMNS}, c.report_json FROM customs_entry_checks c WHERE c.id = ?`, [id]);
            return rows[0] || null;
        });
        if (!row) throw new Refusal(404, 'NOT_FOUND', `Customs check ${id} not found.`);
        res.json({ check: checkJson(row, { full: true }) });
    }));

    /**
     * For the forwarder-upload routes: a customs document was just accepted.
     * Starts its report unless it has had one (the page that showed it waiting
     * usually asked already). Never throws and never delays the accept.
     */
    function startForUpload(req, { shipmentId, uploadId }) {
        if (req && req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        (async () => {
            try {
                await ready();
                const started = await withConnection(async (conn) => {
                    const doc = (await documentsOf(conn, shipmentId)).find(d => d.uploadId === uploadId);
                    return doc && !doc.check ? open(conn, shipmentId, doc, req.userEmail || null) : null;
                });
                if (started) startRuns(req, [Number(started.id)]);
            } catch (e) {
                log.warn('[customs-checks] could not start the check of an accepted upload', { shipmentId, uploadId, error: e.message });
            }
        })();
    }

    return { startForUpload };
}

module.exports = { registerCustomsEntryRoutes, checkJson, KINDS };
