'use strict';

// The supplier-portal PI check (Lambda jportal-pi-check-<env>,
// src/handlers/pi-check.js). Checks a PI a supplier uploaded through jportal
// against the PO, with the staff check's own prompt, schema and PO loader
// (src/services/po-invoice-check.js), and writes the result to
// jportal.pi_submissions. Rules and constants: src/lib/pi-submissions.js.
//
// Triggers (any of them may arrive twice; the claim makes that harmless):
//   - the portal bucket's EventBridge "Object Created" event (shipping's own
//     rule, serverless.yml) — may claim only an awaiting_upload row;
//   - a direct async invoke { submissionId } from jportal's `uploaded` and
//     `recheck` — may also claim a stale pending row or a retryable failure.
//
// Steps:
//   1. parse the trigger; read the row; planEvent decides whether to try.
//   2. direct only: HeadObject. A missing object stops here, no attempt used.
//   3. claim: one guarded UPDATE (status draft, attempts < 3, claimable state)
//      → pending, attempt + 1. No row changed: someone else has it; stop.
//      STALE_CHECK_SECONDS (600) is well above this function's 300 s timeout,
//      so a claim's run is dead before the row can be claimed again.
//   4. GetObject. Missing after the claim: back to awaiting_upload with the
//      attempt given back. Over 10 MiB, or bytes that are not the declared
//      type: failed FILE_INVALID (the object stays, tagged draft, for the
//      bucket's lifecycle). Otherwise pin s3_etag + file_size.
//   5. load the PO: live, and po.supplier exactly the row's supplier_name
//      (PO_NOT_FOUND), then loadPoForCheck (PO_NO_LINES).
//   6. checkDocument with SUPPLIER_PORTAL_ADDENDUM and the Lambda's deadline.
//   7. write succeeded / failed + code, guarded on the attempt; audit it in
//      shipping's audit_log (entity_type 'pi_submission').
// No database connection is held during the S3 or Gemini calls (the pool has
// one connection). Every write after the claim is guarded on check_attempt and
// check_status = 'pending', so a run that lost its claim writes nothing.
// run() throws only when writing the failed row itself fails.

const { S3Client, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const L = require('../lib/pi-submissions');
const { jportalSchema, PO_SUPPLIER_IS } = require('../lib/jportal');
const { recordAudit } = require('../lib/audit');
const { classifyGeminiError } = require('../lib/gemini-retry');
const poCheck = require('./po-invoice-check');
const log = require('../lib/logger');

const REGION = process.env.AWS_REGION || 'eu-north-1';
const DEFAULT_RUN_MS = 300 * 1000;

const isMissing = e => Boolean(e) && (e.name === 'NoSuchKey' || e.name === 'NotFound' || e.Code === 'NoSuchKey'
    || (e.$metadata && e.$metadata.httpStatusCode === 404));

function createPiChecker({
    pool,
    bucket = process.env.PI_UPLOADS_BUCKET,
    s3 = new S3Client({ region: REGION }),
    checkDocument = poCheck.checkDocument,
    loadPoForCheck = poCheck.loadPoForCheck,
    now = Date.now,
} = {}) {
    const withConn = async (fn) => {
        const conn = await pool.getConnection();
        try { return await fn(conn); } finally { conn.release(); }
    };

    const SELECT = () => `SELECT id, status, check_status, check_attempt, error_code, s3_key, s3_etag,
                                 content_type, file_size, purchase_order_id, po_number, supplier_name, row_version,
                                 TIMESTAMPDIFF(SECOND, check_started_at, UTC_TIMESTAMP()) AS check_age_seconds
                            FROM ${jportalSchema()}.pi_submissions`;

    const audit = (submissionId, action, after) => withConn(conn => recordAudit(conn, {
        entityType: 'pi_submission', entityId: submissionId, action, before: null, after, userEmail: null,
    })).catch(e => log.warn('[pi-check] audit failed', { submissionId, action, error: e.message }));

    // Writes the failed row for the claimed attempt. Throws if the write fails.
    const fail = async (row, attempt, code, message, poSnapshot = null) => {
        const r = await withConn(conn => conn.query(
            `UPDATE ${jportalSchema()}.pi_submissions
                SET check_status = 'failed', error_code = ?, error_message = ?, checked_at = UTC_TIMESTAMP(),
                    po_snapshot_json = COALESCE(?, po_snapshot_json), row_version = row_version + 1
              WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
            [code, String(message || code).slice(0, 1000), poSnapshot ? JSON.stringify(poSnapshot) : null, row.id, attempt]
        ));
        const written = r[0].affectedRows === 1;
        if (written) await audit(Number(row.id), 'check_failed', { attempt, errorCode: code });
        return { outcome: 'failed', submissionId: Number(row.id), code, written };
    };

    /**
     * Handles one invocation. `event` is the Lambda event; `deadline` the
     * epoch ms by which the run must be done (the Lambda's remaining time).
     * → { outcome, submissionId?, reason?, code? }
     */
    async function run(event, { deadline = now() + DEFAULT_RUN_MS } = {}) {
        const trigger = L.parseTrigger(event, { bucket });
        if (trigger.ignore) return { outcome: 'ignored', reason: trigger.ignore };
        if (!bucket) {
            log.error('[pi-check] PI_UPLOADS_BUCKET is not configured');
            return { outcome: 'ignored', reason: 'not_configured' };
        }

        // 1. The row, and whether this trigger may claim it.
        let row;
        try {
            row = await withConn(async (conn) => {
                const [rows] = trigger.source === 'event'
                    ? await conn.query(`${SELECT()} WHERE s3_key = ?`, [trigger.key])
                    : await conn.query(`${SELECT()} WHERE id = ?`, [trigger.submissionId]);
                return rows[0] || null;
            });
        } catch (e) {
            log.error('[pi-check] could not read the submission', { trigger, error: e.message });
            return { outcome: 'skipped', reason: 'read_failed' };
        }
        const plan = L.planEvent(trigger.source, row, now());
        if (!plan.claim) return { outcome: 'skipped', submissionId: row ? Number(row.id) : null, reason: plan.reason };

        // 2. Direct path: the object must be there before an attempt is spent.
        if (trigger.source === 'direct') {
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.s3_key }));
            } catch (e) {
                if (!isMissing(e)) log.warn('[pi-check] head failed', { submissionId: row.id, error: e.message });
                return { outcome: 'skipped', submissionId: Number(row.id), reason: isMissing(e) ? 'file_missing' : 'head_failed' };
            }
        }

        // 3. Claim.
        const guard = L.claimWhere(trigger.source);
        let attempt;
        try {
            attempt = await withConn(async (conn) => {
                const [r] = await conn.query(
                    `UPDATE ${jportalSchema()}.pi_submissions
                        SET check_status = 'pending', check_attempt = check_attempt + 1, check_started_at = UTC_TIMESTAMP(),
                            error_code = NULL, error_message = NULL, row_version = row_version + 1
                      WHERE id = ? AND ${guard.sql}`,
                    [row.id, ...guard.params]
                );
                if (r.affectedRows !== 1) return null;
                // Fresh pending: nothing else can claim it now, so this read is ours.
                const [[c]] = await conn.query(`SELECT check_attempt FROM ${jportalSchema()}.pi_submissions WHERE id = ?`, [row.id]);
                return Number(c.check_attempt);
            });
        } catch (e) {
            log.error('[pi-check] claim failed', { submissionId: row.id, error: e.message });
            return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_failed' };
        }
        if (attempt === null) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };

        let poSnapshot = null;
        try {
            // 4. The file.
            let bytes = null;
            let etag;
            let declaredSize = null;
            try {
                const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: row.s3_key }));
                etag = obj.ETag || null;
                declaredSize = Number(obj.ContentLength);
                if (declaredSize > L.MAX_FILE_BYTES) {
                    // Not read: the size alone fails it.
                    if (obj.Body && typeof obj.Body.destroy === 'function') obj.Body.destroy();
                } else {
                    bytes = Buffer.from(await obj.Body.transformToByteArray());
                }
            } catch (e) {
                // Missing after the claim (or unreadable): give the attempt back.
                if (isMissing(e)) log.warn('[pi-check] object missing after claim', { submissionId: row.id });
                else log.error('[pi-check] get failed after claim', { submissionId: row.id, error: e.message });
                await withConn(conn => conn.query(
                    `UPDATE ${jportalSchema()}.pi_submissions
                        SET check_status = 'awaiting_upload', check_attempt = check_attempt - 1, check_started_at = NULL,
                            row_version = row_version + 1
                      WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
                    [row.id, attempt]
                ));
                return { outcome: 'released', submissionId: Number(row.id), reason: isMissing(e) ? 'file_missing' : 'fetch_failed' };
            }
            if (bytes === null || bytes.length > L.MAX_FILE_BYTES) {
                const size = bytes === null ? declaredSize : bytes.length;
                return await fail(row, attempt, 'FILE_INVALID', `File is ${size} bytes; the limit is ${L.MAX_FILE_BYTES}.`);
            }
            const sniffed = L.sniffType(bytes);
            if (!L.ALLOWED_TYPES[row.content_type] || sniffed !== row.content_type) {
                return await fail(row, attempt, 'FILE_INVALID', `The file is not a valid ${row.content_type} (read as ${sniffed || 'unknown'}).`);
            }
            const pinned = await withConn(conn => conn.query(
                `UPDATE ${jportalSchema()}.pi_submissions SET s3_etag = ?, file_size = ?, row_version = row_version + 1
                  WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
                [etag, bytes.length, row.id, attempt]
            ));
            if (pinned[0].affectedRows !== 1) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };

            // 5. The PO, still live and still this supplier's.
            const po = await withConn(async (conn) => {
                const [found] = await conn.query(
                    `SELECT po.id FROM purchase_orders po WHERE po.id = ? AND po.deleted_at IS NULL AND ${PO_SUPPLIER_IS}`,
                    [row.purchase_order_id, row.supplier_name]
                );
                if (!found.length) return null;
                return loadPoForCheck(conn, row.purchase_order_id);
            });
            if (!po) return await fail(row, attempt, 'PO_NOT_FOUND', `Purchase order ${row.purchase_order_id} not found for this supplier.`);
            poSnapshot = po;
            if (!po.lines.length) return await fail(row, attempt, 'PO_NO_LINES', `Purchase order ${row.purchase_order_id} has no line items.`, poSnapshot);

            // 6. The model.
            let out;
            try {
                out = await checkDocument({
                    po, bytes, mimeType: row.content_type,
                    extraInstructions: poCheck.supplierPortalInstructions(po.poNumber),
                    deadline,
                });
            } catch (e) {
                const c = classifyGeminiError(e);
                if (c.code === 'NOT_CONFIGURED') log.error('[pi-check] Gemini is not configured or refused the key', { submissionId: row.id, error: e.message });
                else log.warn('[pi-check] check failed', { submissionId: row.id, code: c.code, error: e.message });
                return await fail(row, attempt, c.code, e.message, poSnapshot);
            }
            const v = L.validateCheckResult(out.result);
            if (!v.ok) return await fail(row, attempt, 'MODEL_BAD_OUTPUT', `Model output failed the shape check: ${v.reason}`, poSnapshot);

            // 7. The result.
            const u = out.usage || {};
            const [done] = await withConn(conn => conn.query(
                `UPDATE ${jportalSchema()}.pi_submissions
                    SET check_status = 'succeeded', verdict = ?, discrepancy_count = ?, result_json = ?, po_snapshot_json = ?,
                        model_used = ?, input_tokens = ?, output_tokens = ?, total_tokens = ?,
                        error_code = NULL, error_message = NULL, checked_at = UTC_TIMESTAMP(), row_version = row_version + 1
                  WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
                [
                    v.verdict, v.discrepancyCount, JSON.stringify(out.result), JSON.stringify(poSnapshot),
                    String(out.model || '').slice(0, 64) || null,
                    u.promptTokenCount ?? null, u.candidatesTokenCount ?? null, u.totalTokenCount ?? null,
                    row.id, attempt,
                ]
            ));
            if (done.affectedRows !== 1) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };
            await audit(Number(row.id), 'check_succeeded', {
                attempt, verdict: v.verdict, discrepancyCount: v.discrepancyCount, modelUsed: out.model || null, source: trigger.source,
            });
            return { outcome: 'succeeded', submissionId: Number(row.id), verdict: v.verdict };
        } catch (e) {
            // Anything unexpected after the claim (a database error, say) is
            // recorded as the model being unavailable, so the supplier can
            // check again. Only a failure to write that throws.
            log.error('[pi-check] unexpected error', { submissionId: row.id, error: e.message });
            return fail(row, attempt, 'MODEL_UNAVAILABLE', `Internal error: ${e.message}`, poSnapshot);
        }
    }

    return { run };
}

module.exports = { createPiChecker, isMissing };
