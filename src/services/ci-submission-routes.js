'use strict';

// /api/v1/ci-submissions — commercial invoices suppliers uploaded through the
// supplier portal (jportal) with no shipment, read by the ciCheck Lambda
// (src/services/ci-check.js), which suggests one of the supplier's booked
// shipments. Staff see the reading, the ranked candidate shipments with their
// evidence and a "does it belong here" preview against any shipment, then
// import it on the shipment they confirm or choose — a
// shipment_payment_documents row (doc_kind 'balance_invoice'), read in the
// background by runShipmentPaymentExtraction exactly as an invoice staff
// upload from an email is (POST /api/v1/shipment-payment-documents), so it
// shows on the Payments page with its fit and check — or reject it with a
// note the supplier sees. Rules: src/lib/ci-submissions.js; frontend guide:
// docs/ci-submissions-frontend.md.
//
// jportal is another schema on the same MySQL server (JPORTAL_DB_SCHEMA).
// This app's login needs SELECT and the column-level UPDATEs in the frontend
// guide on <jportal>.ci_submissions; nothing here creates or deletes its rows.
// The files live in the portal's private bucket (PI_UPLOADS_BUCKET, ci/*,
// s3:GetObject only); an import copies the bytes into this app's PO bucket.
//
// The portal's "Send invoices" page takes whatever the supplier calls an
// invoice, so staff say what each one is at import (body.kind, user
// 2026-11-06): 'commercial' (the default) files it on the shipment they
// confirm; 'qc' files it on the supplier alone with NO shipment — exactly
// what the Payments page's "Upload QC invoice" does — so the read treats
// every line as a QC line (lib/payment-documents.js readPlaceOf). A wrong
// choice is put right with POST /shipment-payment-documents/:id/refile
// (payment-document-refile-routes.js), which moves the document and reads it
// again; the row's confirmed_* columns follow.
//
// Import, in order:
//   1. plain reads (gates): the row (status, rowVersion), the supplier group
//      (pl-results.js portalGroup; SUPPLIER_NOT_FOUND when none is live), the
//      blockers; for a commercial invoice the shipment (resolveShipment:
//      SHIPMENT_NOT_FOUND, MERGED, NOT_BOOKED — the staff upload's
//      requireBookedShipment rules) and the supplier name the document is
//      filed under (the group's spelling on that shipment's POs with the most
//      on board, or the body's supplierName when it is on the shipment; else
//      422 SUPPLIER_NOT_ON_SHIPMENT unless `force`); for a QC invoice the
//      group's one live name, or the body's supplierName when the group has
//      several (422 SUPPLIER_REQUIRED without it; 422 SUPPLIER_NOT_IN_GROUP
//      for a name outside the group unless `force`); and the duplicate guard
//      (a live document with the same file name and size in that place — the
//      shipment, or the supplier alone: 409 DUPLICATE_DOCUMENT unless `force`);
//   2. outside any transaction: GetObject (IfMatch the ETag the portal pinned
//      when it sent the invoice), size and magic bytes, PutObject into the PO
//      bucket at shipment-payments/<uuid>/<name>, the staff upload's layout;
//   3. one transaction: lock the row, the gates again, INSERT the document
//      (extract_status 'processing', uploaded_by_email the supplier's), mark
//      the row imported, audit both (recordAuditStrict);
//   4. after the response: runShipmentPaymentExtraction, on its own
//      connections (the pool has ONE connection), as the staff upload route's
//      startShipmentPaymentExtraction does.
// A deadlock is retried once, then answered 409 CONFLICT. An import that fails
// after step 2 leaves an unreferenced copy in the PO bucket, as a failed staff
// upload can.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { HeadObjectCommand, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const L = require('../lib/ci-submissions');
const S = require('../lib/shipments');
const { jportalSchema } = require('../lib/jportal');
const { recordAuditStrict } = require('../lib/audit');
const PL = require('./pl-results');
const SP = require('./shipment-payments');
const C = require('./ci-check');
const { sameSupplier } = require('./shipment-payment-extract');

const FILE_URL_SECONDS = 15 * 60;
// A staff re-read runs after the response, in this API Lambda: well inside
// its timeout, and above the reader's own retries.
const RECHECK_RUN_MS = 280 * 1000;

const isDeadlock = err => Boolean(err) && (err.code === 'ER_LOCK_DEADLOCK' || err.errno === 1213);
const isLockTimeout = err => Boolean(err) && (err.code === 'ER_LOCK_WAIT_TIMEOUT' || err.errno === 1205);
const isMissing = e => Boolean(e) && (e.name === 'NoSuchKey' || e.name === 'NotFound' || (e.$metadata && e.$metadata.httpStatusCode === 404));
const isPreconditionFailed = e => Boolean(e) && (e.name === 'PreconditionFailed' || (e.$metadata && e.$metadata.httpStatusCode === 412));

const CONFLICT = { status: 409, code: 'CONFLICT', error: 'Someone changed this submission at the same moment; reload and try again.' };
const CANNOT_DECIDE = userType => ({
    status: 403, code: 'CANNOT_DECIDE',
    error: `Your role (${userType || 'none'}) cannot do this — admin and standard users import, reject and re-read supplier invoices.`,
});
const NOT_IMPORTABLE = blockers => ({
    status: 422, code: 'NOT_IMPORTABLE', error: 'This invoice cannot be imported as it stands.', extra: { blockers },
});
const SUPPLIER_NOT_FOUND = row => ({
    status: 422, code: 'SUPPLIER_NOT_FOUND',
    error: `Supplier ${row.supplier_id} (${row.supplier_name}) no longer exists, so its purchase orders cannot be found.`,
    extra: { blockers: ['supplier_not_found'] },
});
const FILE_UNAVAILABLE = { status: 502, code: 'FILE_UNAVAILABLE', error: 'The file could not be read from the supplier portal’s storage; try again.' };
const notFound = id => ({ status: 404, code: 'NOT_FOUND', error: `Invoice submission ${id} not found.` });

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));
const parseJson = L.parseJson;

// The extraction for staff: the model's reading as stored, plus the
// supplierPrinted alias the review panel reads.
function extractionOf(row) {
    const x = parseJson(row.extract_json);
    if (!x || typeof x !== 'object') return null;
    return { ...x, supplierPrinted: x.supplierName ?? null };
}

// The stored extraction the matcher can use: any reading the check kept (a
// failed write keeps what the model returned).
function usableExtract(row) {
    const x = parseJson(row.extract_json);
    return x && typeof x === 'object' ? C.normaliseExtract({ ...x }) : null;
}

// A shipment for the API (the candidate's identifiers, from a shipments row).
const shipmentJson = s => (s ? {
    id: Number(s.id),
    reference: s.reference ?? null,
    name: s.name ?? null,
    stage: s.stage ?? null,
    mode: s.mode ?? null,
    trackingRef: s.tracking_ref ?? null,
    blNumber: s.bl_number ?? null,
    bookingRef: s.booking_ref ?? null,
    etd: s.etd ?? null,
    eta: s.eta ?? null,
    booked: S.isBookedStage(s.stage),
} : null);

// Who is on a shipment, by the PO's supplier name: { supplierName, poNumbers,
// onBoard, inGroup }, most on board first. The SUPPLIER_NOT_ON_SHIPMENT
// payload and the preview's picker.
function suppliersOnShipment(memberPos, groupNames) {
    const keys = new Set((groupNames || []).map(SP.supplierKey));
    const byKey = new Map();
    for (const p of memberPos || []) {
        const name = String(p.supplier || '').trim();
        if (!name) continue;
        let g = byKey.get(p.supplierKey);
        if (!g) { g = { supplierName: name, poNumbers: [], onBoard: 0, inGroup: keys.has(p.supplierKey) }; byKey.set(p.supplierKey, g); }
        g.poNumbers.push(p.poNumber);
        g.onBoard = Math.round((g.onBoard + (Number(p.valueInShipment) || 0)) * 100) / 100;
    }
    return [...byKey.values()].map(g => ({ ...g, poNumbers: [...new Set(g.poNumbers)].sort() })).sort((a, b) => b.onBoard - a.onBoard);
}

function registerCiSubmissionRoutes(app, deps) {
    const {
        withConnection, pool, ready, s3, poBucket, publicS3Url,
        shipmentPaymentDocumentRowToJson, runShipmentPaymentExtraction, log,
    } = deps;

    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.extra || {}) });
    const ciBucket = () => process.env.PI_UPLOADS_BUCKET || null;
    // One checker for staff re-reads, with this app's S3 client.
    let checker = null;
    const getChecker = () => (checker || (checker = C.createCiChecker({ pool, s3 })));

    // Every column of the row, the timestamps as ISO UTC strings, whether
    // its supplier row is live (supplier_exists), and for an imported row its
    // document's live place (filed_*): the supplier it was filed under
    // (filed_supplier_name: the row's own supplier_name is the lowest id of the
    // uploader's group, which for a multi-supplier login is often not the one
    // on the shipment), its shipment (none for a QC invoice; it may have been
    // refiled since the import, so the document, not the row's confirmed_*
    // snapshot, says what it is — lib importedAsOf) and whether it was deleted.
    // Read on every call: an invalid JPORTAL_DB_SCHEMA is a 500 on these
    // routes, not a failed import.
    const selectSql = () => `SELECT r.*,
                       DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso,
                       DATE_FORMAT(r.decided_at, '%Y-%m-%dT%H:%i:%sZ') AS decided_at_iso,
                       DATE_FORMAT(r.checked_at, '%Y-%m-%dT%H:%i:%sZ') AS checked_at_iso,
                       DATE_FORMAT(r.created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at_iso,
                       TIMESTAMPDIFF(SECOND, r.check_started_at, UTC_TIMESTAMP()) AS check_age_seconds,
                       (s.id IS NOT NULL) AS supplier_exists,
                       d.supplier_name AS filed_supplier_name,
                       d.shipment_id AS filed_shipment_id,
                       d.shipment_reference AS filed_shipment_reference,
                       d.doc_kind AS filed_doc_kind,
                       d.deleted_at AS filed_deleted_at
                  FROM ${jportalSchema()}.ci_submissions r
                  LEFT JOIN suppliers s ON s.id = r.supplier_id AND s.deleted_at IS NULL
                  LEFT JOIN shipment_payment_documents d ON d.id = r.document_id`;

    const readRow = async (conn, id) => {
        const [rows] = await conn.query(`${selectSql()} WHERE r.id = ? AND r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})`, [id, ...L.VISIBLE_STATUSES]);
        return rows[0] || null;
    };
    const supplierOf = row => ({ exists: Number(row.supplier_exists) === 1 });

    // Live POs under each spelling: what makes one spelling of a company lead.
    const poCountsFor = async (conn, names) => {
        const counts = new Map();
        const list = [...new Set((names || []).filter(n => n && String(n).trim()))];
        if (!list.length) return counts;
        const [rows] = await conn.query(
            `SELECT supplier, COUNT(*) AS n FROM purchase_orders WHERE deleted_at IS NULL AND supplier IN (?) GROUP BY supplier`, [list]
        );
        for (const r of rows) counts.set(r.supplier, Number(r.n) || 0);
        return counts;
    };
    // The group's spellings collapsed to one per company (lib collapseSpellings):
    // the roster holds Shipsline-era duplicates ("SUNMED") beside the native
    // row, and a login is granted all of them; pickers show the one the POs use.
    const clustersOf = (names, counts) => L.collapseSpellings(names, { same: sameSupplier, weight: n => counts.get(n) || 0 });
    const groupClusters = async (conn, names) => clustersOf(names, await poCountsFor(conn, names));
    const blockersOf = row => (row.status === 'submitted' ? L.importBlockers({ row, supplier: supplierOf(row) }) : []);
    const toJson = (row, { blockers = [] } = {}) => L.submissionToJson(row, { blockers });

    // A pending (submitted) row loaded by the routes, refused when it cannot be decided.
    const stateRefusal = (row, id, rowVersion) => {
        if (!row || !L.VISIBLE_STATUSES.includes(row.status)) return notFound(id);
        if (row.status !== 'submitted') {
            return {
                status: 409, code: 'NOT_SUBMITTED', error: `Invoice submission ${id} is ${row.status}, not waiting for review.`,
                extra: { status: row.status, documentId: row.document_id == null ? null : Number(row.document_id) },
            };
        }
        if (Number(row.row_version) !== rowVersion) {
            return { status: 409, code: 'STALE', error: 'The supplier or someone else changed this submission since it was loaded; reload and check it again.' };
        }
        return null;
    };

    // fn(conn) in a transaction; a returned { fail } rolls back. Retried once
    // on a deadlock; a second deadlock or a lock-wait timeout is { fail: CONFLICT }.
    const inTransaction = async (site, fn) => {
        const once = () => withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const out = await fn(conn);
                if (out && out.fail) await conn.rollback();
                else await conn.commit();
                return out;
            } catch (e) {
                try {
                    await conn.rollback();
                } catch (rollbackError) {
                    log.warn(`[${site}] rollback failed`, rollbackError.message);
                }
                throw e;
            }
        });
        try {
            return await once();
        } catch (e) {
            if (isLockTimeout(e)) return { fail: CONFLICT };
            if (!isDeadlock(e)) throw e;
        }
        try {
            return await once();
        } catch (e) {
            if (isDeadlock(e) || isLockTimeout(e)) return { fail: CONFLICT };
            throw e;
        }
    };

    // The shipment a request names, live: { shipment } (a shipments row) or
    // { fail }. Merged and not-booked are refusals, as for a staff upload.
    const resolveShipment = async (conn, choice, { requireBooked = true } = {}) => {
        const r = await SP.resolveShipment(conn, { id: choice.shipmentId, reference: choice.shipmentReference });
        if (r.notFound) return { fail: { status: 404, code: 'SHIPMENT_NOT_FOUND', error: 'Shipment not found.' } };
        if (r.merged) {
            return { fail: {
                status: 409, code: 'MERGED', error: `That shipment was merged into ${r.mergedIntoId}.`,
                extra: { mergedIntoId: r.mergedIntoId },
            } };
        }
        if (requireBooked && !S.isBookedStage(r.row.stage)) {
            return { fail: {
                status: 422, code: 'NOT_BOOKED', error: 'Invoices are filed on a shipment once it is booked.',
                extra: { stage: r.row.stage },
            } };
        }
        const cand = await C.loadShipmentCandidate(conn, r.row.id);
        if (!cand) return { fail: { status: 404, code: 'SHIPMENT_NOT_FOUND', error: 'Shipment not found.' } };
        return { shipment: cand.shipment, memberPos: cand.memberPos };
    };

    // A live document with the same file name and size in the same place —
    // the shipment, or (a QC invoice) the supplier alone: the staff upload's
    // duplicate guard.
    const duplicateOf = async (conn, row, { shipmentId = null, supplierName = null } = {}) => {
        const [dupes] = await conn.query(
            `SELECT id FROM shipment_payment_documents
              WHERE deleted_at IS NULL AND filename = ? AND file_size = ?
                AND ${shipmentId != null ? 'shipment_id = ?' : 'shipment_id IS NULL AND supplier_key = ?'}
              ORDER BY id LIMIT 1`,
            [String(row.filename || '').slice(0, 200), Number(row.file_size), shipmentId != null ? shipmentId : SP.supplierKey(supplierName)]
        );
        return dupes.length ? Number(dupes[0].id) : null;
    };

    // The file link: an imported row's copy in the PO bucket (the document's
    // own URL), else the portal bucket's object, which the portal's lifecycle
    // removes after 180 days. No connection is held.
    // → { url, expiresAt, state: { gone, changed } }; url null when gone.
    const fileLink = async (row, document) => {
        const state = { gone: false, changed: false };
        if (document && !document.deleted_at && document.s3_key) {
            return { url: document.public_url || publicS3Url(document.s3_key), expiresAt: null, state };
        }
        const bucket = ciBucket();
        if (!bucket || !L.isCiKey(row.s3_key)) return { url: null, expiresAt: null, state };
        const name = L.safeFilename(row.filename, row.content_type) || `ci-${row.id}.${L.ALLOWED_TYPES[row.content_type] || 'bin'}`;
        const type = L.ALLOWED_TYPES[row.content_type] ? row.content_type : 'application/octet-stream';
        try {
            const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.s3_key }));
            if (row.s3_etag && head.ETag && head.ETag !== row.s3_etag) state.changed = true;
            const url = await getSignedUrl(s3, new GetObjectCommand({
                Bucket: bucket, Key: row.s3_key, ResponseContentType: type, ResponseContentDisposition: `inline; filename="${name}"`,
            }), { expiresIn: FILE_URL_SECONDS });
            return { url, expiresAt: new Date(Date.now() + FILE_URL_SECONDS * 1000).toISOString(), state };
        } catch (e) {
            if (isMissing(e)) state.gone = true;
            else log.warn('[ci-submissions] file lookup failed', { id: row.id, error: e.message });
            return { url: null, expiresAt: null, state };
        }
    };

    // The shipment payment document a row was imported as, deleted or not.
    const documentOf = async (conn, row) => {
        const docId = row.document_id == null ? null : Number(row.document_id);
        if (!docId) return null;
        const [rows] = await conn.query(`SELECT * FROM shipment_payment_documents WHERE id = ?`, [docId]);
        return rows[0] || null;
    };
    const documentJson = d => (d ? { ...shipmentPaymentDocumentRowToJson(d), deleted: d.deleted_at != null } : null);

    // The usual background read of a filed invoice, after the response: the
    // staff upload route's startShipmentPaymentExtraction.
    const startExtraction = (req, documentId) => {
        if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const userEmail = req.userEmail || null;
        (async () => {
            try {
                const out = await runShipmentPaymentExtraction(pool, { documentId, userEmail });
                if (out && out.failed) log.warn('[ci-submissions/import] extraction failed', { documentId, code: out.failed });
            } catch (e) {
                log.warn('[ci-submissions/import] extraction threw', { documentId, error: e.message });
            }
        })();
    };

    // GET /api/v1/ci-submissions?status=&supplierKey=&supplierId=&limit=&offset=
    // status: pending (= submitted, the default), history (= imported, rejected,
    // withdrawn), one or more of submitted / imported / rejected / withdrawn
    // comma-separated, decided or all. Drafts and discarded rows are never
    // listed. supplierKey ('s:<id>') and supplierId both filter on the row's
    // supplier_id (the lowest id of its group, which the portal writes).
    // Oldest first for submitted alone (the order they came in), newest change
    // first otherwise. The extraction and the candidates are left out (the
    // detail has them).
    // 200 { data: CiSubmission[], hasMore }
    app.get('/api/v1/ci-submissions', async (req, res) => {
        try {
            const q0 = req.query || {};
            const status = q0.status === 'pending' ? 'submitted' : q0.status === 'history' ? 'decided' : q0.status;
            const parsed = L.parseListQuery({ ...q0, status });
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const q = parsed.value;
            const supplierKey = q0.supplierKey !== undefined && q0.supplierKey !== '' ? String(q0.supplierKey).trim() : null;
            const keyMatch = supplierKey ? /^s:(\d+)$/.exec(supplierKey) : null;
            if (supplierKey && !keyMatch) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'supplierKey must be a supplier group key (s:<id>).' });
            const where = [`r.status IN (${q.statuses.map(() => '?').join(', ')})`];
            const params = [...q.statuses];
            if (q.supplierId) { where.push('r.supplier_id = ?'); params.push(q.supplierId); }
            if (keyMatch) { where.push('r.supplier_id = ?'); params.push(Number(keyMatch[1])); }
            const order = q.statuses.length === 1 && q.statuses[0] === 'submitted'
                ? 'r.submitted_at, r.id'
                : 'r.updated_at DESC, r.id DESC';
            const out = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `${selectSql()} WHERE ${where.join(' AND ')} ORDER BY ${order} LIMIT ${q.limit + 1} OFFSET ${q.offset}`,
                    params
                );
                const hasMore = rows.length > q.limit;
                return { data: rows.slice(0, q.limit).map(row => toJson(row, { blockers: blockersOf(row) })), hasMore };
            });
            res.json(out);
        } catch (error) {
            log.error('[GET /ci-submissions]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/ci-submissions/suppliers — every supplier (keyed by the
    // rows' supplier_id: the lowest id of the group the portal wrote, so one
    // entry per group) with submissions staff can see: how many wait for
    // review, how many in all, and the oldest pending. supplierName is the
    // live name of supplier_id, else the rows' snapshot. Suppliers with
    // pending rows first, oldest first.
    // 200 { data: CiSupplierSummary[] }
    app.get('/api/v1/ci-submissions/suppliers', async (req, res) => {
        try {
            const data = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `SELECT r.supplier_id, r.supplier_name, r.group_supplier_ids, r.status,
                            DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso
                       FROM ${jportalSchema()}.ci_submissions r
                      WHERE r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})
                      ORDER BY r.submitted_at, r.id`,
                    L.VISIBLE_STATUSES
                );
                const groups = new Map();
                for (const r of rows) {
                    const sid = Number(r.supplier_id);
                    const key = `s:${sid}`;
                    let g = groups.get(key);
                    if (!g) {
                        g = { supplierKey: key, supplierId: sid, supplierName: r.supplier_name, supplierIds: new Set(), pending: 0, total: 0, oldestSubmittedAt: null };
                        groups.set(key, g);
                    }
                    for (const id of L.parseGroupSupplierIds(r)) g.supplierIds.add(id);
                    g.total += 1;
                    if (r.status === 'submitted') {
                        g.pending += 1;
                        if (r.submitted_at_iso && (!g.oldestSubmittedAt || r.submitted_at_iso < g.oldestSubmittedAt)) g.oldestSubmittedAt = r.submitted_at_iso;
                    }
                }
                const live = new Map();
                if (groups.size) {
                    const ids = new Set();
                    for (const g of groups.values()) { ids.add(g.supplierId); for (const id of g.supplierIds) ids.add(id); }
                    const [names] = await conn.query(`SELECT id, name FROM suppliers WHERE id IN (?) AND deleted_at IS NULL`, [[...ids]]);
                    for (const n of names) live.set(Number(n.id), n.name);
                }
                // supplierNames: one live name per company in the group, the row's
                // own company first: the roster holds several spellings of one
                // supplier ("SUNMED", "Suzhou Sunmed Co., Ltd", "Suzhou Sunmed
                // Co.,Ltd."), and a multi-supplier login's invoices can be for any
                // company. The spelling with the most live POs leads, and names the
                // rail (supplierName) too.
                const counts = await poCountsFor(conn, [...live.values()]);
                const groupNames = (g, primary) => clustersOf(
                    [primary, ...[...g.supplierIds].sort((a, b) => a - b).map(id => live.get(id))], counts,
                ).map(c => c.name);
                return [...groups.values()]
                    .map(g => {
                        const names = groupNames(g, live.get(g.supplierId) || g.supplierName);
                        const supplierName = names[0] || live.get(g.supplierId) || g.supplierName;
                        return { ...g, supplierName, supplierNames: names, supplierIds: [...g.supplierIds].sort((a, b) => a - b) };
                    })
                    .sort((a, b) => (b.pending > 0) - (a.pending > 0)
                        || String(a.oldestSubmittedAt || '~').localeCompare(String(b.oldestSubmittedAt || '~'))
                        || String(a.supplierName).localeCompare(String(b.supplierName)));
            });
            res.json({ data });
        } catch (error) {
            log.error('[GET /ci-submissions/suppliers]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/ci-submissions/:id — everything the review panel shows: the
    // row, the check, the reading, the candidates as ranked at check time and
    // as they rank now (live: re-scored against today's shipments from the
    // stored reading, no Gemini; null with liveError when that failed or there
    // is no reading), a link to the file, the shipment payment document it was
    // imported as (with the usual read: extractStatus, extracted.fit / .check),
    // the blockers and this submission's audit trail (shipping's audit_log; the
    // supplier's own actions are in jportal's audit_logs).
    // 200 CiSubmissionDetail · 404 NOT_FOUND
    app.get('/api/v1/ci-submissions/:id(\\d+)', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const out = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                if (!row) return null;
                const document = await documentOf(conn, row);
                const extract = usableExtract(row);
                let live = null;
                let liveError = null;
                // The group's companies, one spelling each: a QC invoice is filed under one of them.
                let groupNames = [];
                try {
                    const group = await PL.portalGroup(conn, row);
                    groupNames = (await groupClusters(conn, group.names)).map(c => c.name);
                    if (row.status === 'submitted' && extract) {
                        if (PL.groupGone(group)) liveError = 'The supplier no longer exists.';
                        else live = await C.suggestShipment(conn, { group, extract });
                    }
                } catch (e) {
                    liveError = e.message;
                    log.warn('[GET /ci-submissions/:id] live candidates failed', { id, error: e.message });
                }
                if (row.status === 'submitted' && !extract && !liveError) {
                    liveError = 'The file has not been read.';
                }
                const [auditRows] = await conn.query(
                    `SELECT action, user_email, created_at, after_json FROM audit_log
                      WHERE entity_type = 'ci_submission' AND entity_id = ? ORDER BY created_at, id`,
                    [id]
                );
                return { row, document, live, liveError, auditRows, groupNames };
            });
            if (!out) return refuse(res, notFound(id));
            const { row, document, live } = out;

            const file = await fileLink(row, row.status === 'imported' ? document : null);
            const blockers = row.status === 'submitted' ? L.importBlockers({ row, supplier: supplierOf(row), file: file.state }) : [];
            const stored = L.parseStoredCandidates(row.candidates_json);
            res.json({
                submission: toJson(row, { blockers }),
                check: L.checkToJson(row),
                extraction: extractionOf(row),
                candidates: {
                    considered: stored.considered,
                    stored: stored.top.map(L.candidateToJson),
                    live: live ? live.ranked.slice(0, L.STORED_CANDIDATE_LIMIT).map(L.candidateToJson) : null,
                    liveConsidered: live ? live.considered : null,
                    liveConfidence: live ? live.confidence : null,
                    liveReason: live ? live.reason : null,
                    liveError: out.liveError,
                },
                fileUrl: file.url,
                fileExpiresAt: file.expiresAt,
                document: documentJson(document),
                groupNames: out.groupNames,
                canImport: row.status === 'submitted' && blockers.length === 0,
                canRecheck: L.planEvent('staff', row, Date.now()).claim,
                audit: out.auditRows.map(a => ({
                    action: a.action, userEmail: a.user_email || null, createdAt: iso(a.created_at), after: parseJson(a.after_json),
                })),
            });
        } catch (error) {
            log.error('[GET /ci-submissions/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/ci-submissions/:id/preview?shipmentId= | ?shipmentReference= [&supplierName=]
    // Does it belong on that shipment? The candidate evidence for any shipment
    // staff pick (assessFit with the stored reading, the POs it names there,
    // its goods value against what the supplier has on board), whatever the
    // shipment's stage (`shipment.booked` says whether Import will take it),
    // the supplier it would be filed under (supplierName: the query's, else
    // the group's spelling there), who is on the shipment, and a live document
    // it would duplicate. No Gemini call.
    // 200 { shipment, supplierName, groupOnBoard, suppliers, evidence, extractionUsable, duplicateDocumentId }
    // 400 BAD_FIELD · 404 NOT_FOUND | SHIPMENT_NOT_FOUND · 409 MERGED · 422 SUPPLIER_NOT_FOUND
    app.get('/api/v1/ci-submissions/:id(\\d+)/preview', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const choice = L.parseShipmentChoice(req.query || {});
            if (choice.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: choice.error });
            const supplierName = typeof req.query.supplierName === 'string' && req.query.supplierName.trim() ? req.query.supplierName.trim().slice(0, 255) : null;
            const out = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                if (!row) return { fail: notFound(id) };
                const group = await PL.portalGroup(conn, row);
                if (PL.groupGone(group)) return { fail: SUPPLIER_NOT_FOUND(row) };
                const r = await resolveShipment(conn, choice.value, { requireBooked: false });
                if (r.fail) return r;
                const extract = usableExtract(row);
                const evidence = extract
                    ? C.candidateEvidence({ extract, shipment: r.shipment, memberPos: r.memberPos, groupNames: group.names, supplierName })
                    : null;
                const suppliers = suppliersOnShipment(r.memberPos, group.names);
                return {
                    shipment: shipmentJson(r.shipment),
                    supplierName: evidence ? evidence.supplierName : (supplierName || suppliers.find(s => s.inGroup)?.supplierName || null),
                    groupOnBoard: suppliers.some(s => s.inGroup),
                    suppliers,
                    evidence: evidence ? L.candidateToJson(evidence) : null,
                    extractionUsable: Boolean(extract),
                    duplicateDocumentId: await duplicateOf(conn, row, { shipmentId: Number(r.shipment.id) }),
                };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[GET /ci-submissions/:id/preview]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/ci-submissions/:id/import
    // Body: { rowVersion, kind?, shipmentId | shipmentReference, supplierName?, note?, force? }
    // kind 'commercial' (default): files the invoice on that shipment and
    // reads it as a staff upload is (see the header). supplierName: file it
    // under that name (one of the shipment's PO supplier names, or any name
    // with force). force: file it although the supplier has no POs on the
    // shipment, or a document with the same name and size is already there.
    // kind 'qc': files it on the supplier alone, no shipment (the Payments
    // page's "Upload QC invoice"); supplierName: one of the group's live
    // names, required when the group has several (or any name with force).
    // note: shown to the supplier.
    // 200 { submission, document }
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND | SHIPMENT_NOT_FOUND
    // 409 NOT_SUBMITTED | STALE | CONFLICT | MERGED | DUPLICATE_DOCUMENT
    // 422 NOT_IMPORTABLE | NOT_BOOKED | SUPPLIER_NOT_FOUND | SUPPLIER_NOT_ON_SHIPMENT
    //     | SUPPLIER_REQUIRED | SUPPLIER_NOT_IN_GROUP · 502 FILE_UNAVAILABLE
    app.post('/api/v1/ci-submissions/:id(\\d+)/import', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseImportBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            const bucket = ciBucket();
            if (!bucket || !poBucket) return res.status(500).json({ error: 'PI_UPLOADS_BUCKET / PO_DOCS_BUCKET not configured.' });
            await ready();

            // The gates, on plain reads: the row, the supplier group, the
            // shipment, the name it is filed under and the duplicate guard.
            // → { fail } or what was found.
            const gates = async (conn, row) => {
                const refusal = stateRefusal(row, id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const group = await PL.portalGroup(conn, row);
                if (PL.groupGone(group)) return { fail: SUPPLIER_NOT_FOUND(row) };
                const blockers = L.importBlockers({ row, supplier: { exists: true } });
                if (blockers.length) return { fail: NOT_IMPORTABLE(blockers) };
                if (body.kind === 'qc') {
                    // On the supplier alone: one of the group's companies, under the
                    // spelling its POs use (a duplicate spelling asked for resolves
                    // to it; any name with force).
                    const clusters = await groupClusters(conn, group.names);
                    const choice = L.qcSupplierChoice({ clusters, wanted: body.supplierName, force: body.force, key: SP.supplierKey });
                    if (choice.fail) {
                        const names = choice.fail.suppliers;
                        return { fail: choice.fail.code === 'SUPPLIER_REQUIRED' ? {
                            status: 422, code: 'SUPPLIER_REQUIRED',
                            error: `This login holds ${names.length} suppliers: say which one the QC invoice is for (supplierName).`,
                            extra: { suppliers: names },
                        } : {
                            status: 422, code: 'SUPPLIER_NOT_IN_GROUP',
                            error: `${body.supplierName} is not one of this login's suppliers (${names.join(', ')}). Send force: true to file it under that name anyway.`,
                            extra: { suppliers: names },
                        } };
                    }
                    const supplierName = choice.supplierName;
                    const duplicateId = await duplicateOf(conn, row, { supplierName });
                    if (duplicateId && !body.force) {
                        return { fail: {
                            status: 409, code: 'DUPLICATE_DOCUMENT',
                            error: `${row.filename} is already a QC invoice of this supplier.`, extra: { existingId: duplicateId },
                        } };
                    }
                    return { group, shipment: null, supplierName, onShipment: null, duplicateId };
                }
                const r = await resolveShipment(conn, body);
                if (r.fail) return r;
                const { shipment, memberPos } = r;
                const suppliers = suppliersOnShipment(memberPos, group.names);
                let supplierName = null;
                if (body.supplierName) {
                    const onShipment = suppliers.find(s => SP.supplierKey(s.supplierName) === SP.supplierKey(body.supplierName));
                    supplierName = onShipment ? onShipment.supplierName : (body.force ? body.supplierName : null);
                } else {
                    supplierName = suppliers.find(s => s.inGroup)?.supplierName
                        || (body.force ? (row.suggested_supplier_name || row.supplier_name) : null);
                }
                if (!supplierName) {
                    return { fail: {
                        status: 422, code: 'SUPPLIER_NOT_ON_SHIPMENT',
                        error: body.supplierName
                            ? `${body.supplierName} has no purchase orders on ${shipment.reference || shipment.id}. Send force: true to file it under that name anyway.`
                            : `That supplier has no purchase orders on ${shipment.reference || shipment.id}. Send force: true to file it there anyway.`,
                        extra: { suppliers },
                    } };
                }
                const duplicateId = await duplicateOf(conn, row, { shipmentId: Number(shipment.id) });
                if (duplicateId && !body.force) {
                    return { fail: {
                        status: 409, code: 'DUPLICATE_DOCUMENT',
                        error: `${row.filename} is already on this shipment.`, extra: { existingId: duplicateId },
                    } };
                }
                return { group, shipment, supplierName, onShipment: suppliers.some(s => SP.supplierKey(s.supplierName) === SP.supplierKey(supplierName)), duplicateId };
            };

            // 1. Plain reads.
            const pre = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                const g = await gates(conn, row);
                return g.fail ? g : { ...g, row };
            });
            if (pre.fail) return refuse(res, pre.fail);
            const { row } = pre;

            // 2. The bytes, exactly as the portal validated them, copied into the PO bucket.
            let bytes;
            try {
                const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: row.s3_key, IfMatch: row.s3_etag }));
                if (Number(obj.ContentLength) > L.MAX_FILE_BYTES) {
                    if (obj.Body && typeof obj.Body.destroy === 'function') obj.Body.destroy();
                    return refuse(res, NOT_IMPORTABLE(['too_large']));
                }
                bytes = Buffer.from(await obj.Body.transformToByteArray());
            } catch (e) {
                if (isMissing(e)) return refuse(res, NOT_IMPORTABLE(['file_gone']));
                if (isPreconditionFailed(e)) return refuse(res, NOT_IMPORTABLE(['file_changed']));
                log.warn('[POST /ci-submissions/:id/import] get failed', { id, error: e.message });
                return refuse(res, FILE_UNAVAILABLE);
            }
            if (bytes.length > L.MAX_FILE_BYTES) return refuse(res, NOT_IMPORTABLE(['too_large']));
            if (L.sniffType(bytes) !== row.content_type) return refuse(res, NOT_IMPORTABLE(['bad_type']));
            const s3Key = L.destKey({ submissionId: id, filename: row.filename, contentType: row.content_type, token: uuidv4() });
            const shownName = s3Key.slice(s3Key.lastIndexOf('/') + 1);
            try {
                await s3.send(new PutObjectCommand({
                    Bucket: poBucket, Key: s3Key, Body: bytes, ContentType: row.content_type,
                    ContentDisposition: `inline; filename="${shownName}"`,
                }));
            } catch (e) {
                log.warn('[POST /ci-submissions/:id/import] put failed', { id, error: e.message });
                return refuse(res, FILE_UNAVAILABLE);
            }

            // 3. One transaction.
            const out = await inTransaction('POST /ci-submissions/:id/import', async (conn) => {
                const jp = jportalSchema();
                const [locked] = await conn.query(`SELECT * FROM ${jp}.ci_submissions WHERE id = ? FOR UPDATE`, [id]);
                const lockedRow = locked[0];
                const g = await gates(conn, lockedRow);
                if (g.fail) return g;
                const { shipment, supplierName } = g;
                // A QC invoice has no shipment: the place is what makes it one.
                const shipmentId = shipment ? shipment.id : null;
                const reference = shipment ? (shipment.reference || String(shipment.id)) : null;
                const filename = String(lockedRow.filename || shownName).trim().slice(0, 200) || shownName;

                const [ins] = await conn.query(
                    `INSERT INTO shipment_payment_documents
                        (shipment_id, shipment_reference, supplier_name, supplier_key, payment_id, doc_kind,
                         filename, s3_key, public_url, content_type, file_size, notes, extract_status, uploaded_by_email)
                     VALUES (?, ?, ?, ?, NULL, 'balance_invoice', ?, ?, ?, ?, ?, ?, 'processing', ?)`,
                    [
                        shipmentId, reference, supplierName, SP.supplierKey(supplierName),
                        filename, s3Key, publicS3Url(s3Key), lockedRow.content_type, bytes.length,
                        `Uploaded by the supplier through the supplier portal (invoice upload ${id})${body.kind === 'qc' ? ', filed as a QC invoice' : ''}.`,
                        lockedRow.submitted_by_email || null,
                    ]
                );
                const documentId = ins.insertId;
                await recordAuditStrict(conn, {
                    entityType: 'shipment_payment_document', entityId: documentId, action: 'create', before: null,
                    after: {
                        shipmentId, supplierName, filename, docKind: 'balance_invoice', fileSize: bytes.length, kind: body.kind,
                        source: 'portal', ciSubmissionId: id, uploadedByEmail: lockedRow.submitted_by_email || null,
                    },
                    userEmail: req.userEmail,
                });

                const [done] = await conn.query(
                    `UPDATE ${jp}.ci_submissions
                        SET status = 'imported', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            document_id = ?, confirmed_shipment_id = ?, confirmed_shipment_reference = ?,
                            row_version = row_version + 1
                      WHERE id = ? AND status = 'submitted' AND row_version = ?`,
                    [req.userEmail || 'unknown', body.note, documentId, shipmentId, reference == null ? null : String(reference).slice(0, 100), id, body.rowVersion]
                );
                // The row is locked, so this cannot miss; if it does, nothing is committed.
                if (done.affectedRows !== 1) throw new Error(`ci_submissions ${id} was not submitted at import`);

                await recordAuditStrict(conn, {
                    entityType: 'ci_submission', entityId: id, action: 'import',
                    before: { status: 'submitted', rowVersion: body.rowVersion },
                    after: {
                        status: 'imported', documentId, kind: body.kind, shipmentId, shipmentReference: reference, supplierName,
                        onShipment: g.onShipment, force: body.force, duplicateOf: g.duplicateId, note: body.note,
                        groupSupplierIds: g.group.supplierIds,
                        suggestedShipmentId: lockedRow.suggested_shipment_id == null ? null : Number(lockedRow.suggested_shipment_id),
                        matchConfidence: lockedRow.match_confidence || null, matchReason: lockedRow.match_reason || null,
                    },
                    userEmail: req.userEmail,
                });
                return { documentId };
            });
            if (out.fail) return refuse(res, out.fail);

            const reply = await withConnection(async (conn) => {
                const after = await readRow(conn, id);
                const [doc] = await conn.query(`SELECT * FROM shipment_payment_documents WHERE id = ?`, [out.documentId]);
                return { submission: toJson(after), document: documentJson(doc[0]) };
            });
            res.json(reply);

            // 4. The usual read, after the response.
            startExtraction(req, out.documentId);
        } catch (error) {
            log.error('[POST /ci-submissions/:id/import]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/ci-submissions/:id/reject — body { rowVersion, note }.
    // Not accepted: the note is required and shown to the supplier word for
    // word. Nothing is filed; the supplier may upload another.
    // 200 { submission }
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND · 409 NOT_SUBMITTED | STALE | CONFLICT
    app.post('/api/v1/ci-submissions/:id(\\d+)/reject', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseRejectBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            await ready();

            const out = await inTransaction('POST /ci-submissions/:id/reject', async (conn) => {
                const jp = jportalSchema();
                const [locked] = await conn.query(`SELECT * FROM ${jp}.ci_submissions WHERE id = ? FOR UPDATE`, [id]);
                const refusal = stateRefusal(locked[0], id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const [done] = await conn.query(
                    `UPDATE ${jp}.ci_submissions
                        SET status = 'rejected', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            row_version = row_version + 1
                      WHERE id = ? AND status = 'submitted' AND row_version = ?`,
                    [req.userEmail || 'unknown', body.note, id, body.rowVersion]
                );
                if (done.affectedRows !== 1) throw new Error(`ci_submissions ${id} was not submitted at reject`);
                await recordAuditStrict(conn, {
                    entityType: 'ci_submission', entityId: id, action: 'reject',
                    before: { status: 'submitted', rowVersion: body.rowVersion },
                    after: {
                        status: 'rejected', note: body.note,
                        suggestedShipmentId: locked[0].suggested_shipment_id == null ? null : Number(locked[0].suggested_shipment_id),
                        matchConfidence: locked[0].match_confidence || null,
                    },
                    userEmail: req.userEmail,
                });
                return {};
            });
            if (out.fail) return refuse(res, out.fail);

            const reply = await withConnection(async (conn) => ({ submission: toJson(await readRow(conn, id)) }));
            res.json(reply);
        } catch (error) {
            log.error('[POST /ci-submissions/:id/reject]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/ci-submissions/:id/recheck — "Read again": runs the check
    // (ci-check.js, staff claim) after the response, in this Lambda. For a
    // submitted row whose read failed, never started, or is wrong; the live
    // candidates in the detail already follow new bookings without one. Poll
    // the detail until check.status leaves 'pending'.
    // 202 { submission } · 403 CANNOT_DECIDE · 404 NOT_FOUND
    // 409 NOT_RECHECKABLE { reason: not_submitted | in_progress | attempts_exhausted }
    app.post('/api/v1/ci-submissions/:id(\\d+)/recheck', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            await ready();
            const row = await withConnection(conn => readRow(conn, id));
            if (!row) return refuse(res, notFound(id));
            const plan = L.planEvent('staff', row, Date.now());
            if (!plan.claim) {
                return refuse(res, {
                    status: 409, code: 'NOT_RECHECKABLE', extra: { reason: plan.reason },
                    error: plan.reason === 'in_progress' ? 'This invoice is being read right now.'
                        : plan.reason === 'attempts_exhausted' ? `This invoice has been read ${L.STAFF_MAX_CHECK_ATTEMPTS} times; choose the shipment yourself.`
                            : `Invoice submission ${id} is ${row.status}, not waiting for review.`,
                });
            }
            res.status(202).json({ submission: toJson(row, { blockers: blockersOf(row) }) });

            if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
            const userEmail = req.userEmail || null;
            (async () => {
                try {
                    const result = await getChecker().run({ submissionId: id }, { staff: true, userEmail, deadline: Date.now() + RECHECK_RUN_MS });
                    log.info('[ci-submissions/recheck] done', result);
                } catch (e) {
                    log.warn('[ci-submissions/recheck] threw', { id, error: e.message });
                }
            })();
        } catch (error) {
            log.error('[POST /ci-submissions/:id/recheck]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

module.exports = { registerCiSubmissionRoutes, suppliersOnShipment };
