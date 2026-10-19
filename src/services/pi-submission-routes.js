'use strict';

// /api/v1/pi-submissions — proforma invoices suppliers uploaded through the
// supplier portal (jportal), checked by the piCheck Lambda
// (src/services/pi-check.js) and submitted for review. Staff import them (the
// same rows a staff upload makes: purchase_order_invoices with
// pi_submission_id, a purchase_order_invoice_checks row copied from the
// supplier's check with triggered_by 'supplier_portal', and the
// purchase_order_invoice_payments row) or reject them with a note the
// supplier sees. The supplier is emailed either way, through Front. Plus
// POST /api/v1/purchase-order-invoices/:id/verify-bank, the bank-detail
// verification a portal PI needs before it can be paid
// (src/lib/bank-verification.js). Rules: src/lib/pi-submissions.js; frontend
// guide: docs/pi-submissions-frontend.md.
//
// jportal is another schema on the same MySQL server (JPORTAL_DB_SCHEMA).
// This app's login needs SELECT and a column-level UPDATE on
// <jportal>.pi_submissions (the GRANT is in the frontend guide); nothing here
// creates or deletes its rows. The files live in the portal's private bucket
// (PI_UPLOADS_BUCKET, s3:GetObject only); an import copies the bytes into
// this app's PO bucket.
//
// Import, in order (B4 of the plan):
//   1. plain reads: the row (status, rowVersion), the PO (live, still the
//      supplier's), the blockers, the check's usability and the bank gate;
//   2. outside any transaction: GetObject (IfMatch the ETag piCheck pinned),
//      size and magic bytes, PutObject into the PO bucket;
//   3. one transaction: lock the row, re-check it and the PO, insert the
//      invoice, the check (when usable) and the payment row, a pending
//      re-check when asked for or when the check is unusable, mark the row
//      imported, audit (recordAuditStrict: a failed audit insert fails it);
//   4. after commit: email the supplier, respond;
//   5. the pending re-check runs after the response on a fresh pool connection
//      (the pool has ONE connection, so never inside withConnection).
// A deadlock is retried once, then answered 409 CONFLICT. An import that
// fails after step 2 leaves an unreferenced copy in the PO bucket, as a failed
// staff upload can.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { HeadObjectCommand, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const L = require('../lib/pi-submissions');
const bank = require('../lib/bank-verification');
const { jportalSchema, PO_SUPPLIER_IS } = require('../lib/jportal');
const { recordAuditStrict } = require('../lib/audit');
const { sendFrontMessage } = require('./front-send');

const FILE_URL_SECONDS = 15 * 60;

const isDeadlock = err => Boolean(err) && (err.code === 'ER_LOCK_DEADLOCK' || err.errno === 1213);
const isLockTimeout = err => Boolean(err) && (err.code === 'ER_LOCK_WAIT_TIMEOUT' || err.errno === 1205);
const isDuplicate = err => Boolean(err) && (err.code === 'ER_DUP_ENTRY' || err.errno === 1062);
const isMissing = e => Boolean(e) && (e.name === 'NoSuchKey' || e.name === 'NotFound' || (e.$metadata && e.$metadata.httpStatusCode === 404));
const isPreconditionFailed = e => Boolean(e) && (e.name === 'PreconditionFailed' || (e.$metadata && e.$metadata.httpStatusCode === 412));

const CONFLICT = { status: 409, code: 'CONFLICT', error: 'Someone changed this submission at the same moment; reload and try again.' };
const CANNOT_DECIDE = userType => ({
    status: 403, code: 'CANNOT_DECIDE',
    error: `Your role (${userType || 'none'}) cannot do this — admin and standard users import, reject and verify supplier PIs.`,
});
const NOT_IMPORTABLE = blockers => ({
    status: 422, code: 'NOT_IMPORTABLE', error: 'This PI cannot be imported as it stands.', extra: { blockers },
});
const FILE_UNAVAILABLE = { status: 502, code: 'FILE_UNAVAILABLE', error: 'The file could not be read from the supplier portal’s storage; try again.' };

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));

function registerPiSubmissionRoutes(app, deps) {
    const {
        withConnection, pool, ready, s3, poBucket, publicS3Url,
        invoiceRowToJson, invoiceCheckRowToJson, invoicePaymentRowToJson,
        createPendingInvoiceCheck, recordInvoiceCheck, upsertInvoicePaymentTerms,
        loadPoForCheck, recordAudit, getEmailTemplate, renderTemplate, log,
    } = deps;

    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.extra || {}) });
    const piBucket = () => process.env.PI_UPLOADS_BUCKET || null;

    // Every column of the row, the timestamps as ISO UTC strings, and whether
    // its PO is live (po_exists) and still the supplier's (po_owned: the PO's
    // supplier is exactly the row's supplier_name snapshot, which jportal took
    // from the PO when its access rule admitted it). Read on every call: an
    // invalid JPORTAL_DB_SCHEMA is a 500 on these routes, not a failed import.
    const selectSql = () => `SELECT r.*,
                       DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso,
                       DATE_FORMAT(r.decided_at, '%Y-%m-%dT%H:%i:%sZ') AS decided_at_iso,
                       DATE_FORMAT(r.notified_at, '%Y-%m-%dT%H:%i:%sZ') AS notified_at_iso,
                       DATE_FORMAT(r.checked_at, '%Y-%m-%dT%H:%i:%sZ') AS checked_at_iso,
                       (po.id IS NOT NULL) AS po_exists,
                       (po.id IS NOT NULL AND CONVERT(po.supplier USING utf8mb4) COLLATE utf8mb4_0900_bin
                                            = CONVERT(r.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin) AS po_owned
                  FROM ${jportalSchema()}.pi_submissions r
                  LEFT JOIN purchase_orders po ON po.id = r.purchase_order_id AND po.deleted_at IS NULL`;

    const readRow = async (conn, id) => {
        const [rows] = await conn.query(`${selectSql()} WHERE r.id = ? AND r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})`, [id, ...L.VISIBLE_STATUSES]);
        return rows[0] || null;
    };
    const poStateOf = row => ({ exists: Number(row.po_exists) === 1, owned: Number(row.po_owned) === 1 });

    // The PO now, re-read (plain read) inside a transaction after the row lock.
    const poStateNow = async (conn, row) => {
        const [found] = await conn.query(
            `SELECT po.id, (${PO_SUPPLIER_IS}) AS owned FROM purchase_orders po WHERE po.id = ? AND po.deleted_at IS NULL`,
            [row.supplier_name, row.purchase_order_id]
        );
        return { exists: found.length === 1, owned: found.length === 1 && Number(found[0].owned) === 1 };
    };

    /**
     * The supplier's "previous" bank details: the newest payment row of
     * another live PI of theirs — verified rows first (their
     * bank_verified_json), then legacy PIs with bank fields. Portal PIs are
     * theirs through pi_submission_id → pi_submissions.supplier_id; legacy
     * (staff-uploaded) ones through the PO's supplier name, the exact match
     * jportal uses (purchase_orders.company_id is the buyer, not the supplier).
     * → { invoiceId, poNumber, verifiedAt, details } or null.
     */
    const previousBank = async (conn, row, cache = null) => {
        const exclude = row.imported_invoice_id != null ? Number(row.imported_invoice_id) : 0;
        const cacheKey = `${row.supplier_id}:${exclude}`;
        if (cache && cache.has(cacheKey)) return cache.get(cacheKey);
        const [names] = await conn.query(`SELECT name FROM suppliers WHERE id = ? AND deleted_at IS NULL LIMIT 1`, [row.supplier_id]);
        const supplierName = names.length ? names[0].name : row.supplier_name;
        const [rows] = await conn.query(
            `SELECT p.*, po.po_number
               FROM purchase_order_invoice_payments p
               JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
               JOIN purchase_orders po ON po.id = p.purchase_order_id AND po.deleted_at IS NULL
               LEFT JOIN ${jportalSchema()}.pi_submissions ps ON ps.id = i.pi_submission_id
              WHERE p.purchase_order_invoice_id <> ?
                AND ((i.pi_submission_id IS NOT NULL AND ps.supplier_id = ? AND p.bank_verification_status = 'verified')
                  OR (i.pi_submission_id IS NULL AND ${PO_SUPPLIER_IS}
                      AND (p.bank_verification_status = 'verified'
                           OR COALESCE(NULLIF(p.beneficiary_name, ''), NULLIF(p.bank_name, ''), NULLIF(p.account_number, ''),
                                       NULLIF(p.iban, ''), NULLIF(p.swift_bic, ''), NULLIF(p.intermediary_bank, '')) IS NOT NULL)))
              ORDER BY COALESCE(p.bank_verification_status = 'verified', 0) DESC,
                       COALESCE(p.bank_verified_at, p.updated_at) DESC, p.id DESC
              LIMIT 1`,
            [exclude, row.supplier_id, supplierName]
        );
        let out = null;
        if (rows.length) {
            const p = rows[0];
            const verified = p.bank_verification_status === bank.VERIFIED;
            out = {
                invoiceId: Number(p.purchase_order_invoice_id),
                poNumber: p.po_number || null,
                verifiedAt: verified ? iso(p.bank_verified_at) : null,
                details: (verified && bank.parseVerifiedJson(p.bank_verified_json)) || bank.bankDetailsOf(p, { fromRow: true }),
            };
        }
        if (cache) cache.set(cacheKey, out);
        return out;
    };

    // The bank gate's view of a submission: `not_checked` when the check is
    // unusable, else compareBankDetails(previous, the details the check read).
    const bankCheckFor = async (conn, row, usability, cache = null) => {
        const previous = await previousBank(conn, row, cache);
        if (!usability.usable) return { state: 'not_checked', fields: [], current: null, previous };
        const current = bank.bankDetailsOf(usability.result.paymentTerms);
        const cmp = bank.compareBankDetails(previous ? previous.details : null, current);
        return { state: cmp.state, fields: cmp.fields, current, previous };
    };

    // The submission as the list shows it. `po` (loadPoForCheck output, or
    // null) is the PO now, for poChanged.
    const listItem = (row, { usability, bankState, poNow }) => {
        const diff = usability.usable ? L.diffPo(row.po_snapshot_json, poNow) : { changed: false };
        return L.submissionToJson(row, {
            poChanged: diff.changed,
            bankState,
            blockers: row.status === 'submitted' ? L.importBlockers({ row, po: poStateOf(row) }) : [],
        });
    };

    // A pending (submitted) row loaded by the routes, refused when it cannot be decided.
    const stateRefusal = (row, id, rowVersion) => {
        if (!row || !L.VISIBLE_STATUSES.includes(row.status)) return { status: 404, code: 'NOT_FOUND', error: `PI submission ${id} not found.` };
        if (row.status !== 'submitted') {
            return {
                status: 409, code: 'NOT_SUBMITTED', error: `PI submission ${id} is ${row.status}, not waiting for review.`,
                extra: { status: row.status, importedInvoiceId: row.imported_invoice_id != null ? Number(row.imported_invoice_id) : null },
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

    /**
     * Emails the supplier the decision on `row` (a pi_submissions row, now
     * imported or rejected) through Front, from the template for `status`
     * (looked up by key, so the template's category never matters). On test,
     * PI_NOTIFY_REDIRECT_TO sends it there instead. Sets notified_at, or
     * notify_error when the send failed, and audits. Never throws.
     * If the process dies after Front accepts the message but before
     * notified_at is written, a later send repeats it: accepted.
     * → { sent, to?, reason? }
     */
    const notifyDecision = async (conn, row, status, note, userEmail = null) => {
        const jp = jportalSchema();
        const id = Number(row.id);
        let to = null;
        try {
            const key = L.templateKeyFor(status);
            if (!key) return { sent: false, reason: 'not_decided' };
            const original = String(row.submitted_by_email || '').trim();
            const redirect = String(process.env.PI_NOTIFY_REDIRECT_TO || '').trim();
            to = redirect || original;
            if (!to) return { sent: false, reason: 'no_recipient' };
            const tpl = await getEmailTemplate(conn, key);
            const vars = L.buildEmailVars({ row, status, note, portalUrl: process.env.SUPPLIER_PORTAL_URL || '' });
            let subject = renderTemplate(tpl.subject, vars.subject) || `PI – ${vars.subject.poNumber}`;
            if (redirect) subject = `[for ${original}] ${subject}`;
            const html = renderTemplate(tpl.bodyHtml, vars.body);
            const sent = await sendFrontMessage({ to, subject, html });
            if (sent.ok) {
                await conn.query(`UPDATE ${jp}.pi_submissions SET notified_at = UTC_TIMESTAMP(), notify_error = NULL WHERE id = ?`, [id]);
            } else {
                const why = [sent.reason, sent.status, sent.detail].filter(v => v != null && v !== '').join(': ').slice(0, 500);
                await conn.query(`UPDATE ${jp}.pi_submissions SET notify_error = ? WHERE id = ?`, [why, id]);
            }
            await recordAudit(conn, {
                entityType: 'pi_submission', entityId: id, action: sent.ok ? 'notified' : 'notify_failed', before: null,
                after: {
                    status, template: key, to, ...(redirect ? { redirectedFrom: original } : {}),
                    ...(sent.ok ? { frontMessageUid: sent.frontMessageUid || null } : { reason: sent.reason, frontStatus: sent.status ?? null }),
                },
                userEmail,
            });
            return sent.ok ? { sent: true, to } : { sent: false, to, reason: sent.reason };
        } catch (e) {
            log.warn('[pi-submissions] notify failed', { id, error: e.message });
            try {
                await conn.query(`UPDATE ${jportalSchema()}.pi_submissions SET notify_error = ? WHERE id = ?`, [`error: ${e.message}`.slice(0, 500), id]);
            } catch { /* nothing more to do */ }
            return { sent: false, ...(to ? { to } : {}), reason: 'error' };
        }
    };

    // GET /api/v1/pi-submissions?status=&supplierId=&purchaseOrderId=&limit=&offset=
    // status: submitted (default), imported, rejected, withdrawn — comma-separated
    // — or decided / all. Drafts and discarded rows are never listed. Oldest
    // first for submitted alone (the order they came in), newest change first
    // otherwise. The check result and the PO snapshot are left out (the detail has them).
    // 200 { data: PiSubmission[], hasMore }
    app.get('/api/v1/pi-submissions', async (req, res) => {
        try {
            const parsed = L.parseListQuery(req.query || {});
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const q = parsed.value;
            const where = [`r.status IN (${q.statuses.map(() => '?').join(', ')})`];
            const params = [...q.statuses];
            if (q.supplierId) { where.push('r.supplier_id = ?'); params.push(q.supplierId); }
            if (q.purchaseOrderId) { where.push('r.purchase_order_id = ?'); params.push(q.purchaseOrderId); }
            const order = q.statuses.length === 1 && q.statuses[0] === 'submitted'
                ? 'r.submitted_at, r.id'
                : 'r.updated_at DESC, r.id DESC';
            const out = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `${selectSql()} WHERE ${where.join(' AND ')} ORDER BY ${order} LIMIT ${q.limit + 1} OFFSET ${q.offset}`,
                    params
                );
                const hasMore = rows.length > q.limit;
                const page = rows.slice(0, q.limit);
                const bankCache = new Map();
                const poCache = new Map();
                const data = [];
                for (const row of page) {
                    const usability = L.checkUsability(row);
                    let poNow = null;
                    // The PO now, for poChanged: only while the PI waits for review.
                    if (row.status === 'submitted' && usability.usable) {
                        const poId = Number(row.purchase_order_id);
                        if (!poCache.has(poId)) poCache.set(poId, await loadPoForCheck(conn, poId));
                        poNow = poCache.get(poId);
                    }
                    const b = await bankCheckFor(conn, row, usability, bankCache);
                    data.push(listItem(row, { usability, bankState: b.state, poNow }));
                }
                return { data, hasMore };
            });
            res.json(out);
        } catch (error) {
            log.error('[GET /pi-submissions]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/pi-submissions/suppliers — every supplier with PIs waiting
    // for review: how many, on how many POs, how many with a major verdict, and
    // the oldest. Oldest first. Drives the badge.
    // 200 { data: PiSupplierSummary[] }
    app.get('/api/v1/pi-submissions/suppliers', async (req, res) => {
        try {
            const rows = await withConnection(async (conn) => {
                const [r] = await conn.query(
                    `SELECT r.supplier_id, r.supplier_name, r.purchase_order_id, r.verdict, r.check_status,
                            DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso
                       FROM ${jportalSchema()}.pi_submissions r
                      WHERE r.status = 'submitted'
                      ORDER BY r.submitted_at, r.id`
                );
                return r;
            });
            const bySupplier = new Map();
            for (const r of rows) {
                const sid = Number(r.supplier_id);
                const s = bySupplier.get(sid) || {
                    supplierId: sid, supplierName: r.supplier_name, pending: 0, purchaseOrders: new Set(),
                    majorCount: 0, oldestSubmittedAt: null,
                };
                s.pending += 1;
                s.purchaseOrders.add(Number(r.purchase_order_id));
                if (r.check_status === 'succeeded' && r.verdict === 'major_discrepancies') s.majorCount += 1;
                if (r.submitted_at_iso && (!s.oldestSubmittedAt || r.submitted_at_iso < s.oldestSubmittedAt)) s.oldestSubmittedAt = r.submitted_at_iso;
                bySupplier.set(sid, s);
            }
            const data = [...bySupplier.values()]
                .map(s => ({ ...s, purchaseOrders: s.purchaseOrders.size }))
                .sort((a, b) => String(a.oldestSubmittedAt || '').localeCompare(String(b.oldestSubmittedAt || '')) || a.supplierId - b.supplierId);
            res.json({ data });
        } catch (error) {
            log.error('[GET /pi-submissions/suppliers]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/pi-submissions/:id — everything the review panel shows: the
    // check (with `usable`), the PO as checked vs now, a 15-minute link to the
    // file, the PO's existing PIs, the blockers, the full bank check and this
    // submission's audit trail (shipping's audit_log; the supplier's own
    // actions are in jportal's audit_logs).
    // 200 PiSubmissionDetail · 404 NOT_FOUND
    app.get('/api/v1/pi-submissions/:id(\\d+)', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const out = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                if (!row) return null;
                const usability = L.checkUsability(row);
                const poCurrent = Number(row.po_exists) === 1 ? await loadPoForCheck(conn, Number(row.purchase_order_id)) : null;
                const bankCheck = await bankCheckFor(conn, row, usability);
                const [invRows] = await conn.query(
                    `SELECT i.id, i.filename, i.uploaded_at, i.pi_submission_id,
                            (SELECT c.verdict FROM purchase_order_invoice_checks c
                              WHERE c.purchase_order_invoice_id = i.id
                              ORDER BY c.created_at DESC, c.id DESC LIMIT 1) AS verdict
                       FROM purchase_order_invoices i
                      WHERE i.purchase_order_id = ? AND i.deleted_at IS NULL
                      ORDER BY i.uploaded_at DESC, i.id DESC`,
                    [row.purchase_order_id]
                );
                const [others] = await conn.query(
                    `SELECT id, filename, DATE_FORMAT(submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso
                       FROM ${jportalSchema()}.pi_submissions
                      WHERE purchase_order_id = ? AND status = 'submitted' AND id <> ?
                      ORDER BY submitted_at, id`,
                    [row.purchase_order_id, id]
                );
                let importedInvoice = null;
                if (row.imported_invoice_id != null) {
                    const [inv] = await conn.query(`SELECT * FROM purchase_order_invoices WHERE id = ?`, [row.imported_invoice_id]);
                    if (inv.length) {
                        const [checks] = await conn.query(
                            `SELECT * FROM purchase_order_invoice_checks WHERE purchase_order_invoice_id = ? ORDER BY created_at DESC, id DESC LIMIT 1`,
                            [row.imported_invoice_id]
                        );
                        const [pays] = await conn.query(`SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`, [row.imported_invoice_id]);
                        importedInvoice = {
                            ...invoiceRowToJson(inv[0]),
                            deleted: inv[0].deleted_at != null,
                            latestCheck: invoiceCheckRowToJson(checks[0]) || null,
                            payment: pays.length ? invoicePaymentRowToJson(pays[0]) : null,
                        };
                    }
                }
                const [auditRows] = await conn.query(
                    `SELECT action, user_email, created_at, after_json FROM audit_log
                      WHERE entity_type = 'pi_submission' AND entity_id = ? ORDER BY created_at, id`,
                    [id]
                );
                return { row, usability, poCurrent, bankCheck, invRows, others, importedInvoice, auditRows };
            });
            if (!out) return refuse(res, { status: 404, code: 'NOT_FOUND', error: `PI submission ${id} not found.` });
            const { row, usability, poCurrent, bankCheck } = out;

            // The file: is it still there and unchanged, and a link to it. No
            // connection is held for these.
            let file = null;
            const fileState = { gone: false, changed: false };
            const bucket = piBucket();
            if (bucket && L.isPiKey(row.s3_key)) {
                try {
                    const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.s3_key }));
                    if (row.s3_etag && head.ETag && head.ETag !== row.s3_etag) fileState.changed = true;
                    const url = await getSignedUrl(s3, new GetObjectCommand({
                        Bucket: bucket, Key: row.s3_key,
                        ResponseContentType: L.ALLOWED_TYPES[row.content_type] ? row.content_type : 'application/octet-stream',
                        ResponseContentDisposition: `inline; filename="${L.safeFilename(row.filename, row.content_type) || `pi-${row.id}.${L.ALLOWED_TYPES[row.content_type] || 'bin'}`}"`,
                    }), { expiresIn: FILE_URL_SECONDS });
                    file = { url, expiresAt: new Date(Date.now() + FILE_URL_SECONDS * 1000).toISOString() };
                } catch (e) {
                    if (isMissing(e)) fileState.gone = true;
                    else log.warn('[GET /pi-submissions/:id] file lookup failed', { id, error: e.message });
                }
            }
            const blockers = row.status === 'submitted' ? L.importBlockers({ row, po: poStateOf(row), file: fileState }) : [];
            const poDiff = usability.usable || row.po_snapshot_json ? L.diffPo(row.po_snapshot_json, poCurrent) : L.diffPo(null, null);
            res.json({
                ...L.submissionToJson(row, { poChanged: poDiff.changed, bankState: bankCheck.state, blockers }),
                check: L.checkToJson(row, usability),
                poSnapshot: L.parseJson(row.po_snapshot_json),
                poCurrent: poCurrent ? JSON.parse(JSON.stringify(poCurrent)) : null,
                poDiff,
                file,
                poInvoices: out.invRows.map(r => ({
                    id: Number(r.id), filename: r.filename, uploadedAt: iso(r.uploaded_at),
                    verdict: r.verdict || null, piSubmissionId: r.pi_submission_id != null ? Number(r.pi_submission_id) : null,
                })),
                otherPending: out.others.map(r => ({ id: Number(r.id), filename: r.filename, submittedAt: r.submitted_at_iso || null })),
                importedInvoice: out.importedInvoice,
                canImport: row.status === 'submitted' && blockers.length === 0,
                bankCheck,
                audit: out.auditRows.map(a => ({
                    action: a.action, userEmail: a.user_email || null, createdAt: iso(a.created_at), after: L.parseJson(a.after_json),
                })),
            });
        } catch (error) {
            log.error('[GET /pi-submissions/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/pi-submissions/:id/import
    // Body: { rowVersion, note?, recheck?, bankVerification?: { method, note? } }
    // note: shown to the supplier. recheck: re-run the check against the PO as
    // it is now (the panel ticks it when the PO changed or the check is unusable).
    // bankVerification: required (422 BANK_CHANGE_UNCONFIRMED without it) when
    // the check is usable and the bank details changed or there is no history;
    // refused (400) when the check is unusable — verify after the re-check, or
    // by hand with verify-bank.
    // 200 { submission, invoice, check, notification }
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND · 409 NOT_SUBMITTED | STALE | CONFLICT
    // 422 NOT_IMPORTABLE | BANK_CHANGE_UNCONFIRMED · 502 FILE_UNAVAILABLE
    app.post('/api/v1/pi-submissions/:id(\\d+)/import', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseImportBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            let verification = null;
            if (body.bankVerification) {
                const v = bank.parseVerification(body.bankVerification, { allowDetails: false });
                if (v.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: `bankVerification: ${v.error}` });
                verification = v.value;
            }
            const bucket = piBucket();
            if (!bucket || !poBucket) return res.status(500).json({ error: 'PI_UPLOADS_BUCKET / PO_DOCS_BUCKET not configured.' });
            await ready();

            // 1. Plain reads: what we are about to import, and the gates.
            const pre = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                const refusal = stateRefusal(row, id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const blockers = L.importBlockers({ row, po: poStateOf(row) });
                if (blockers.length) return { fail: NOT_IMPORTABLE(blockers) };
                const usability = L.checkUsability(row);
                const bankCheck = await bankCheckFor(conn, row, usability);
                const poNow = usability.usable ? await loadPoForCheck(conn, Number(row.purchase_order_id)) : null;
                return { row, usability, bankCheck, poChanged: usability.usable ? L.diffPo(row.po_snapshot_json, poNow).changed : null };
            });
            if (pre.fail) return refuse(res, pre.fail);
            const { row, usability, bankCheck } = pre;
            if (!usability.usable && verification) {
                return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'bankVerification cannot be sent: the supplier’s check is not usable, so no bank details were read. Verify them after the re-check, or enter them with verify-bank.' });
            }
            if (usability.usable && (bankCheck.state === 'changed' || bankCheck.state === 'no_history') && !verification) {
                return refuse(res, {
                    status: 422, code: 'BANK_CHANGE_UNCONFIRMED',
                    error: bankCheck.state === 'changed'
                        ? 'The bank details on this PI differ from the supplier’s last verified ones. Record how you confirmed them (bankVerification).'
                        : 'This supplier has no verified bank details on file. Record how you confirmed the ones on this PI (bankVerification).',
                    extra: { bankCheck: { state: bankCheck.state, fields: bankCheck.fields } },
                });
            }

            // 2. The bytes, exactly as checked, copied into the PO bucket.
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
                log.warn('[POST /pi-submissions/:id/import] get failed', { id, error: e.message });
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
                log.warn('[POST /pi-submissions/:id/import] put failed', { id, error: e.message });
                return refuse(res, FILE_UNAVAILABLE);
            }

            // 3. One transaction.
            const result = usability.result;
            const out = await inTransaction('POST /pi-submissions/:id/import', async (conn) => {
                const jp = jportalSchema();
                const [locked] = await conn.query(`SELECT * FROM ${jp}.pi_submissions WHERE id = ? FOR UPDATE`, [id]);
                const refusal = stateRefusal(locked[0], id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const lockedRow = locked[0];
                const po = await poStateNow(conn, lockedRow);
                const blockers = L.importBlockers({ row: lockedRow, po });
                if (blockers.length) return { fail: NOT_IMPORTABLE(blockers) };
                // The previous details could have moved since step 1 (another
                // import committed): the gate is decided again on what is now.
                const bankNow = await bankCheckFor(conn, lockedRow, usability);
                if (usability.usable && (bankNow.state === 'changed' || bankNow.state === 'no_history') && !verification) {
                    return { fail: { status: 422, code: 'BANK_CHANGE_UNCONFIRMED', error: 'The supplier’s bank details on file changed while you were looking; check them again.', extra: { bankCheck: { state: bankNow.state, fields: bankNow.fields } } } };
                }

                let invoiceId;
                try {
                    const [ins] = await conn.query(
                        `INSERT INTO purchase_order_invoices
                            (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email, pi_submission_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [
                            lockedRow.purchase_order_id, String(lockedRow.filename || shownName).trim().slice(0, 200) || shownName,
                            s3Key, publicS3Url(s3Key), lockedRow.content_type, bytes.length,
                            lockedRow.supplier_note ? `Supplier note (portal): ${lockedRow.supplier_note}`.slice(0, 4000) : null,
                            lockedRow.submitted_by_email, id,
                        ]
                    );
                    invoiceId = ins.insertId;
                } catch (e) {
                    if (isDuplicate(e)) return { fail: CONFLICT };
                    throw e;
                }

                let checkRow = null;
                let payment;
                if (usability.usable) {
                    const u = {
                        input: lockedRow.input_tokens, output: lockedRow.output_tokens, total: lockedRow.total_tokens,
                    };
                    const [chk] = await conn.query(
                        `INSERT INTO purchase_order_invoice_checks
                            (purchase_order_invoice_id, status, verdict, discrepancy_count, result_json,
                             model_used, input_tokens, output_tokens, total_tokens, triggered_by, triggered_by_email)
                         VALUES (?, 'succeeded', ?, ?, ?, ?, ?, ?, ?, 'supplier_portal', ?)`,
                        [
                            invoiceId, result.overallVerdict, result.discrepancies.length, JSON.stringify(result),
                            lockedRow.model_used || null, u.input ?? null, u.output ?? null, u.total ?? null,
                            lockedRow.submitted_by_email,
                        ]
                    );
                    const [cr] = await conn.query(`SELECT * FROM purchase_order_invoice_checks WHERE id = ?`, [chk.insertId]);
                    checkRow = cr[0];
                    // Verified at import whenever the gate passed: confirmed by
                    // staff, or the same as the supplier's last verified details.
                    let v = null;
                    const fingerprint = bank.bankFingerprint(bankNow.current);
                    if (verification) {
                        v = { fingerprint, details: bankNow.current, method: verification.method, note: verification.note, by: req.userEmail };
                    } else if (bankNow.state === 'unchanged') {
                        v = {
                            fingerprint, details: bankNow.current, method: bank.MATCHES_PREVIOUS,
                            note: `Matches invoice ${bankNow.previous.invoiceId}${bankNow.previous.poNumber ? ` (${bankNow.previous.poNumber})` : ''}`,
                            by: req.userEmail,
                        };
                    }
                    payment = await upsertInvoicePaymentTerms(conn, {
                        invoiceId, purchaseOrderId: lockedRow.purchase_order_id, checkId: chk.insertId,
                        paymentTerms: result.paymentTerms || null, modelUsed: lockedRow.model_used || null,
                        verification: v, userEmail: req.userEmail,
                    });
                } else {
                    // Always a payment row: empty, needs_verification, until the re-check fills it.
                    payment = await upsertInvoicePaymentTerms(conn, {
                        invoiceId, purchaseOrderId: lockedRow.purchase_order_id, checkId: null,
                        paymentTerms: null, modelUsed: null, userEmail: req.userEmail,
                    });
                }

                let pendingCheck = null;
                if (body.recheck || !usability.usable) {
                    pendingCheck = await createPendingInvoiceCheck(conn, {
                        invoiceId, triggeredBy: 'import_recheck', triggeredByEmail: req.userEmail,
                    });
                }

                const [done] = await conn.query(
                    `UPDATE ${jp}.pi_submissions
                        SET status = 'imported', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            imported_invoice_id = ?, row_version = row_version + 1
                      WHERE id = ? AND status = 'submitted' AND row_version = ?`,
                    [req.userEmail || 'unknown', body.note, invoiceId, id, body.rowVersion]
                );
                // The row is locked, so this cannot miss; if it does, nothing is committed.
                if (done.affectedRows !== 1) throw new Error(`pi_submissions ${id} was not submitted at import`);

                await recordAuditStrict(conn, {
                    entityType: 'purchase_order', entityId: Number(lockedRow.purchase_order_id), action: 'invoice_uploaded',
                    before: null,
                    after: { invoiceId, filename: lockedRow.filename, fileSize: bytes.length, source: 'supplier_portal', piSubmissionId: id, uploadedByEmail: lockedRow.submitted_by_email },
                    userEmail: req.userEmail,
                });
                if (checkRow) {
                    await recordAuditStrict(conn, {
                        entityType: 'purchase_order', entityId: Number(lockedRow.purchase_order_id), action: 'invoice_checked',
                        before: null,
                        after: { invoiceId, checkId: checkRow.id, verdict: checkRow.verdict, discrepancyCount: checkRow.discrepancy_count, modelUsed: checkRow.model_used, triggeredBy: 'supplier_portal' },
                        userEmail: req.userEmail,
                    });
                }
                await recordAuditStrict(conn, {
                    entityType: 'pi_submission', entityId: id, action: 'import',
                    before: { status: 'submitted', rowVersion: body.rowVersion },
                    after: {
                        status: 'imported', invoiceId, note: body.note, recheck: Boolean(pendingCheck), checkUsable: usability.usable,
                        bankCheck: bankNow.state,
                        bankVerification: verification ? { method: verification.method, note: verification.note }
                            : (bankNow.state === 'unchanged' && usability.usable ? { method: bank.MATCHES_PREVIOUS, previousInvoiceId: bankNow.previous.invoiceId } : null),
                        bankVerificationStatus: payment ? payment.status : null,
                        bankBefore: bankNow.previous ? bankNow.previous.details : null,
                        bankAfter: bankNow.current,
                        poChangedSinceCheck: pre.poChanged,
                    },
                    userEmail: req.userEmail,
                });
                return { invoiceId, checkId: checkRow ? checkRow.id : null, pendingCheck, purchaseOrderId: Number(lockedRow.purchase_order_id) };
            });
            if (out.fail) return refuse(res, out.fail);

            // 4. The supplier's email, then the answer.
            const reply = await withConnection(async (conn) => {
                const decided = await readRow(conn, id);
                const notification = await notifyDecision(conn, decided, 'imported', body.note, req.userEmail);
                const after = await readRow(conn, id);
                const [inv] = await conn.query(`SELECT * FROM purchase_order_invoices WHERE id = ?`, [out.invoiceId]);
                const [pay] = await conn.query(`SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`, [out.invoiceId]);
                let check = out.pendingCheck || null;
                if (out.checkId) {
                    const [c] = await conn.query(`SELECT * FROM purchase_order_invoice_checks WHERE id = ?`, [out.checkId]);
                    check = invoiceCheckRowToJson(c[0]);
                }
                return {
                    submission: L.submissionToJson(after, { bankState: bankCheck.state }),
                    invoice: { ...invoiceRowToJson(inv[0]), payment: pay.length ? invoicePaymentRowToJson(pay[0]) : null },
                    check,
                    ...(out.pendingCheck && out.checkId ? { recheck: out.pendingCheck } : {}),
                    notification,
                };
            });
            res.json(reply);

            // 5. The re-check, after the response, on its own connection. Same
            // callbackWaitsForEmptyEventLoop dance as the staff upload route.
            if (out.pendingCheck) {
                if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
                const userEmail = req.userEmail;
                (async () => {
                    const bgConn = await pool.getConnection();
                    try {
                        const checkOut = await recordInvoiceCheck(bgConn, {
                            purchaseOrderId: out.purchaseOrderId, invoiceId: out.invoiceId,
                            triggeredBy: 'import_recheck', triggeredByEmail: userEmail, existingCheckId: out.pendingCheck.id,
                        });
                        await recordAudit(bgConn, {
                            entityType: 'purchase_order', entityId: out.purchaseOrderId, action: 'invoice_checked', before: null,
                            after: {
                                invoiceId: out.invoiceId, checkId: checkOut.row.id, verdict: checkOut.row.verdict,
                                discrepancyCount: checkOut.row.discrepancyCount, modelUsed: checkOut.row.modelUsed, triggeredBy: 'import_recheck',
                            },
                            userEmail,
                        });
                    } catch (e) {
                        log.warn('[pi-submissions/import] background re-check failed', { invoiceId: out.invoiceId, code: e.code, error: e.message });
                    } finally {
                        bgConn.release();
                    }
                })();
            }
        } catch (error) {
            log.error('[POST /pi-submissions/:id/import]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/pi-submissions/:id/reject — body { rowVersion, note }.
    // The note is required and shown to the supplier word for word. Nothing is
    // written to the PO; the supplier can upload a corrected PI.
    // 200 { submission, notification }
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND · 409 NOT_SUBMITTED | STALE | CONFLICT
    app.post('/api/v1/pi-submissions/:id(\\d+)/reject', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseRejectBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            await ready();

            const out = await inTransaction('POST /pi-submissions/:id/reject', async (conn) => {
                const jp = jportalSchema();
                const [locked] = await conn.query(`SELECT * FROM ${jp}.pi_submissions WHERE id = ? FOR UPDATE`, [id]);
                const refusal = stateRefusal(locked[0], id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const [done] = await conn.query(
                    `UPDATE ${jp}.pi_submissions
                        SET status = 'rejected', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            row_version = row_version + 1
                      WHERE id = ? AND status = 'submitted' AND row_version = ?`,
                    [req.userEmail || 'unknown', body.note, id, body.rowVersion]
                );
                if (done.affectedRows !== 1) throw new Error(`pi_submissions ${id} was not submitted at reject`);
                await recordAuditStrict(conn, {
                    entityType: 'pi_submission', entityId: id, action: 'reject',
                    before: { status: 'submitted', rowVersion: body.rowVersion },
                    after: { status: 'rejected', note: body.note, purchaseOrderId: Number(locked[0].purchase_order_id) },
                    userEmail: req.userEmail,
                });
                return {};
            });
            if (out.fail) return refuse(res, out.fail);

            const reply = await withConnection(async (conn) => {
                const decided = await readRow(conn, id);
                const notification = await notifyDecision(conn, decided, 'rejected', body.note, req.userEmail);
                const after = await readRow(conn, id);
                return { submission: L.submissionToJson(after, { bankState: 'not_checked' }), notification };
            });
            res.json(reply);
        } catch (error) {
            log.error('[POST /pi-submissions/:id/reject]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/pi-submissions/:id/notify — body { force? }.
    // (Re)sends the decision email of an imported or rejected submission. Any
    // decided row with notified_at unset can be sent, whatever notify_error
    // says; force is needed only when it was already sent.
    // 200 { submission, notification } · 403 CANNOT_DECIDE · 404 NOT_FOUND
    // 409 NOT_DECIDED | ALREADY_NOTIFIED
    app.post('/api/v1/pi-submissions/:id(\\d+)/notify', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseNotifyBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            await ready();
            const out = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                if (!row) return { fail: { status: 404, code: 'NOT_FOUND', error: `PI submission ${id} not found.` } };
                if (!L.DECIDED_STATUSES.includes(row.status)) {
                    return { fail: { status: 409, code: 'NOT_DECIDED', error: `PI submission ${id} is ${row.status}; only an imported or rejected one is emailed.`, extra: { status: row.status } } };
                }
                if (row.notified_at && !parsed.value.force) {
                    return { fail: { status: 409, code: 'ALREADY_NOTIFIED', error: 'The supplier was already emailed; send again with force.', extra: { notifiedAt: row.notified_at_iso || null } } };
                }
                const notification = await notifyDecision(conn, row, row.status, row.decided_note, req.userEmail);
                const after = await readRow(conn, id);
                return { submission: L.submissionToJson(after, { bankState: 'not_checked' }), notification };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[POST /pi-submissions/:id/notify]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/purchase-order-invoices/:id/verify-bank
    // Body: { method: 'phone_known_number'|'existing_email_thread'|'other', note?, bankDetails? }
    // Records that someone confirmed a portal PI's bank details, which makes it
    // payable. bankDetails: the details as confirmed with the supplier — the
    // only way to put bank details on a PI by hand (when the check read none,
    // or misread them); they replace the ones on the payment row.
    // 200 Payment (the invoice's payment JSON)
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND · 409 NO_BANK_DETAILS | NOT_PORTAL_PI | CONFLICT
    app.post('/api/v1/purchase-order-invoices/:id(\\d+)/verify-bank', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const invoiceId = Number(req.params.id);
            const parsed = bank.parseVerification(req.body, { allowDetails: true });
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const v = parsed.value;
            await ready();

            const out = await inTransaction('POST /purchase-order-invoices/:id/verify-bank', async (conn) => {
                const [inv] = await conn.query(
                    `SELECT id, purchase_order_id, pi_submission_id FROM purchase_order_invoices WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
                    [invoiceId]
                );
                if (!inv.length) return { fail: { status: 404, code: 'NOT_FOUND', error: `Invoice ${invoiceId} not found.` } };
                if (inv[0].pi_submission_id == null) {
                    return { fail: { status: 409, code: 'NOT_PORTAL_PI', error: 'Only a PI that came through the supplier portal needs its bank details verified.' } };
                }
                const poId = Number(inv[0].purchase_order_id);
                const [pays] = await conn.query(`SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ? FOR UPDATE`, [invoiceId]);
                const before = pays[0] || null;
                const details = v.bankDetails || (before ? bank.bankDetailsOf(before, { fromRow: true }) : null);
                const fingerprint = details ? bank.bankFingerprint(details) : '';
                if (!fingerprint) {
                    return { fail: { status: 409, code: 'NO_BANK_DETAILS', error: 'There are no bank details on this PI to verify. Enter the details you confirmed with the supplier (bankDetails).' } };
                }
                const verifiedCols = [fingerprint, JSON.stringify(details), req.userEmail || null, v.method, v.note];
                if (!before) {
                    await conn.query(
                        `INSERT INTO purchase_order_invoice_payments
                            (purchase_order_invoice_id, purchase_order_id, beneficiary_name, bank_name, bank_address,
                             account_number, iban, swift_bic, intermediary_bank, bank_fingerprint,
                             bank_verified_fingerprint, bank_verified_json, bank_verified_by, bank_verification_method,
                             bank_verification_note, bank_verified_at, bank_verification_status)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?)`,
                        [
                            invoiceId, poId, details.beneficiaryName, details.bankName, details.bankAddress,
                            details.accountNumber, details.iban, details.swiftBic, details.intermediaryBank, fingerprint,
                            ...verifiedCols, bank.VERIFIED,
                        ]
                    );
                } else {
                    const sets = [];
                    const vals = [];
                    if (v.bankDetails) {
                        for (const f of bank.BANK_FIELDS) { sets.push(`${f.col} = ?`); vals.push(details[f.key]); }
                    }
                    sets.push('bank_fingerprint = ?', 'bank_verified_fingerprint = ?', 'bank_verified_json = ?', 'bank_verified_by = ?',
                        'bank_verification_method = ?', 'bank_verification_note = ?', 'bank_verified_at = NOW()', 'bank_verification_status = ?');
                    vals.push(fingerprint, ...verifiedCols, bank.VERIFIED);
                    await conn.query(`UPDATE purchase_order_invoice_payments SET ${sets.join(', ')} WHERE id = ?`, [...vals, before.id]);
                }
                const [rb] = await conn.query(`SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`, [invoiceId]);
                await recordAuditStrict(conn, {
                    entityType: 'purchase_order', entityId: poId, action: 'bank_verified',
                    before: {
                        invoiceId, bankVerificationStatus: before ? before.bank_verification_status || null : null,
                        bank: before ? bank.bankDetailsOf(before, { fromRow: true }) : null,
                    },
                    after: {
                        invoiceId, bankVerificationStatus: bank.VERIFIED, bank: details,
                        method: v.method, note: v.note, enteredByHand: Boolean(v.bankDetails),
                    },
                    userEmail: req.userEmail,
                });
                return { payment: rb[0] };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(invoicePaymentRowToJson(out.payment));
        } catch (error) {
            log.error('[POST /purchase-order-invoices/:id/verify-bank]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    return { notifyDecision };
}

module.exports = { registerPiSubmissionRoutes };
