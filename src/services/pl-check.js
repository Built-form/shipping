'use strict';

// The supplier-portal packing-list check (Lambda jportal-pl-check-<env>,
// src/handlers/pl-check.js). Reads a packing list a supplier uploaded through
// jportal with the staff check's own prompt and schema
// (src/services/packing-list-check.js readPackingList), suggests which of the
// supplier's containers it belongs to (src/lib/pl-match.js, through
// src/services/pl-results.js) and writes the check, match and a supplier-safe
// result to jportal.pl_submissions. Rules and constants:
// src/lib/pl-submissions.js. The claim pattern is pi-check.js's.
//
// Triggers (any of them may arrive twice; the claim makes that harmless):
//   - the portal bucket's EventBridge "Object Created" event for pl/* (shipping's
//     own rule, serverless.yml) — may claim only an awaiting_upload row;
//   - a direct async invoke { submissionId } from jportal's `uploaded` and
//     `recheck` — may also claim a stale pending row or a retryable failure;
//   - a direct invoke { submissionId, action: 'refresh' } recomputes the
//     supplier's result with no Gemini call (pl-results.js refreshSubmission;
//     the plRefresh Lambda is the usual way in, this is the fallback).
//
// Steps:
//   1. parse the trigger; read the row; planEvent decides whether to try.
//   2. direct only: HeadObject. A missing object stops here, no attempt used.
//   3. claim: one guarded UPDATE (status draft, attempts < 3, claimable state)
//      → pending, attempt + 1. No row changed: someone else has it; stop.
//      STALE_CHECK_SECONDS (600) is well above this function's 300 s timeout.
//   4. GetObject. Missing after the claim: back to awaiting_upload with the
//      attempt given back. Over 10 MiB, or bytes that are not the declared
//      type: failed FILE_INVALID. Otherwise pin s3_etag + file_size.
//   5. the supplier group (pl-results.js portalGroup: the ids the portal wrote
//      into group_supplier_ids at create, with their live names; failed
//      SUPPLIER_NOT_FOUND when none of them is live — before a Gemini call is
//      spent). group_supplier_ids is the portal's and is never written here.
//   6. readPackingList with the Lambda's deadline. No rows read: failed
//      NO_ROWS (the extraction is kept; the supplier may still send it, and
//      Import then reads the file again). Model errors are classified by
//      lib/gemini-retry.js classifyGeminiError (DOCUMENT_UNPROCESSABLE,
//      MODEL_UNAVAILABLE, NOT_CONFIGURED, MODEL_NO_JSON).
//   7. on one connection: where the replaced submission was filed, the
//      candidates scored, the provisional pooled result for a confident
//      match ('unmatched' otherwise), and duplicateOf (another submitted or
//      imported row of the supplier's with the same ETag — an early warning;
//      the refusal is the portal's, at submit).
//   8. write it all in ONE guarded UPDATE (the check, match and result column
//      groups); audit in shipping's audit_log (entity_type 'pl_submission').
// No database connection is held during the S3 or Gemini calls (the pool has
// one connection) and none is nested. Every write after the claim is guarded
// on check_attempt and check_status = 'pending', so a run that lost its claim
// writes nothing. run() throws only when writing the failed row itself fails.

const { S3Client, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const L = require('../lib/pl-submissions');
const { jportalSchema } = require('../lib/jportal');
const { recordAudit } = require('../lib/audit');
const { classifyGeminiError } = require('../lib/gemini-retry');
const P = require('./packing-list-check');
const PL = require('./pl-results');
const { isMissing } = require('./pi-check');
const log = require('../lib/logger');

const REGION = process.env.AWS_REGION || 'eu-north-1';
const DEFAULT_RUN_MS = 300 * 1000;

function createPlChecker({
    pool,
    bucket = process.env.PI_UPLOADS_BUCKET,
    s3 = new S3Client({ region: REGION }),
    readPackingList = P.readPackingList,
    refreshSubmission = PL.refreshSubmission,
    now = Date.now,
} = {}) {
    const withConn = async (fn) => {
        const conn = await pool.getConnection();
        try { return await fn(conn); } finally { conn.release(); }
    };

    const SELECT = () => `SELECT id, status, check_status, check_attempt, error_code, s3_key, s3_etag,
                                 content_type, file_size, filename, supplier_id, supplier_name, group_supplier_ids, replaces_submission_id, row_version,
                                 TIMESTAMPDIFF(SECOND, check_started_at, UTC_TIMESTAMP()) AS check_age_seconds
                            FROM ${jportalSchema()}.pl_submissions`;

    const audit = (submissionId, action, after) => withConn(conn => recordAudit(conn, {
        entityType: 'pl_submission', entityId: submissionId, action, before: null, after, userEmail: null,
    })).catch(e => log.warn('[pl-check] audit failed', { submissionId, action, error: e.message }));

    // Writes the failed row for the claimed attempt. `read` (what the model
    // returned, when it got that far) is kept so staff can still see it.
    // Throws if the write fails.
    const fail = async (row, attempt, code, message, { read = null } = {}) => {
        const u = read?.usage || {};
        const parsed = read?.parsed || null;
        const r = await withConn(conn => conn.query(
            `UPDATE ${jportalSchema()}.pl_submissions
                SET check_status = 'failed', error_code = ?, error_message = ?, checked_at = UTC_TIMESTAMP(),
                    model_used = COALESCE(?, model_used), input_tokens = COALESCE(?, input_tokens),
                    output_tokens = COALESCE(?, output_tokens), total_tokens = COALESCE(?, total_tokens),
                    extract_json = COALESCE(?, extract_json), row_count = COALESCE(?, row_count),
                    invoice_number = COALESCE(?, invoice_number),
                    row_version = row_version + 1
              WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
            [
                code, String(message || code).slice(0, 1000),
                read ? String(read.modelUsed || '').slice(0, 64) || null : null,
                u.promptTokenCount ?? null, u.candidatesTokenCount ?? null, u.totalTokenCount ?? null,
                parsed ? JSON.stringify(parsed) : null, parsed ? parsed.lines.length : null,
                parsed?.invoiceNumber ? String(parsed.invoiceNumber).slice(0, 100) : null,
                row.id, attempt,
            ]
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
        if (trigger.source === 'direct' && trigger.action === 'refresh') {
            try {
                return { outcome: 'refreshed', ...(await refreshSubmission(pool, trigger.submissionId)) };
            } catch (e) {
                log.error('[pl-check] refresh failed; the last result stays', { submissionId: trigger.submissionId, error: e.message });
                return { outcome: 'refresh_failed', submissionId: trigger.submissionId, reason: e.message };
            }
        }
        if (!bucket) {
            log.error('[pl-check] PI_UPLOADS_BUCKET is not configured');
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
            log.error('[pl-check] could not read the submission', { trigger, error: e.message });
            return { outcome: 'skipped', reason: 'read_failed' };
        }
        const plan = L.planEvent(trigger.source, row, now());
        if (!plan.claim) return { outcome: 'skipped', submissionId: row ? Number(row.id) : null, reason: plan.reason };

        // 2. Direct path: the object must be there before an attempt is spent.
        if (trigger.source === 'direct') {
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.s3_key }));
            } catch (e) {
                if (!isMissing(e)) log.warn('[pl-check] head failed', { submissionId: row.id, error: e.message });
                return { outcome: 'skipped', submissionId: Number(row.id), reason: isMissing(e) ? 'file_missing' : 'head_failed' };
            }
        }

        // 3. Claim.
        const guard = L.claimWhere(trigger.source);
        let attempt;
        try {
            attempt = await withConn(async (conn) => {
                const [r] = await conn.query(
                    `UPDATE ${jportalSchema()}.pl_submissions
                        SET check_status = 'pending', check_attempt = check_attempt + 1, check_started_at = UTC_TIMESTAMP(),
                            error_code = NULL, error_message = NULL, row_version = row_version + 1
                      WHERE id = ? AND ${guard.sql}`,
                    [row.id, ...guard.params]
                );
                if (r.affectedRows !== 1) return null;
                // Fresh pending: nothing else can claim it now, so this read is ours.
                const [[c]] = await conn.query(`SELECT check_attempt FROM ${jportalSchema()}.pl_submissions WHERE id = ?`, [row.id]);
                return Number(c.check_attempt);
            });
        } catch (e) {
            log.error('[pl-check] claim failed', { submissionId: row.id, error: e.message });
            return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_failed' };
        }
        if (attempt === null) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };

        let read = null;
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
                if (isMissing(e)) log.warn('[pl-check] object missing after claim', { submissionId: row.id });
                else log.error('[pl-check] get failed after claim', { submissionId: row.id, error: e.message });
                await withConn(conn => conn.query(
                    `UPDATE ${jportalSchema()}.pl_submissions
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
                `UPDATE ${jportalSchema()}.pl_submissions SET s3_etag = ?, file_size = ?, row_version = row_version + 1
                  WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
                [etag, bytes.length, row.id, attempt]
            ));
            if (pinned[0].affectedRows !== 1) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };

            // 5. The supplier group, before a Gemini call is spent on a
            //    supplier that is gone.
            const group = await withConn(conn => PL.portalGroup(conn, row));
            if (PL.groupGone(group)) {
                return await fail(row, attempt, 'SUPPLIER_NOT_FOUND', `Supplier ${row.supplier_id} (${row.supplier_name}) no longer exists (group ${JSON.stringify(group.supplierIds)}).`);
            }

            // 6. The model.
            try {
                read = await readPackingList({ bytes, contentType: row.content_type, deadline });
            } catch (e) {
                const c = classifyGeminiError(e);
                if (c.code === 'NOT_CONFIGURED') log.error('[pl-check] Gemini is not configured or refused the key', { submissionId: row.id, error: e.message });
                else log.warn('[pl-check] read failed', { submissionId: row.id, code: c.code, error: e.message });
                return await fail(row, attempt, c.code, e.message);
            }
            const extracted = read.parsed;
            if (!Array.isArray(extracted.lines)) extracted.lines = [];
            if (!extracted.lines.length) {
                return await fail(row, attempt, 'NO_ROWS', 'No packing rows could be read from the file.', { read });
            }

            // 7. The suggestion and the supplier's result, on one connection.
            const scored = await withConn(async (conn) => {
                const replacesContainer = await PL.replacesContainerFor(conn, row);
                const s = await PL.suggestContainer(conn, {
                    group, extracted, filename: row.filename, replacesContainer, submissionId: Number(row.id),
                });
                const extraction = L.toSupplierExtraction(extracted);
                let state = 'unmatched';
                let container = null;
                let comparison = null;
                if (s.match.confidence === 'confident' && s.winnerContainer) {
                    state = 'provisional';
                    container = s.winnerContainer;
                    ({ comparison } = await PL.pooledComparison(conn, {
                        container, group, extraDocument: { packingListId: null, filename: row.filename, extracted },
                    }));
                }
                const result = await PL.buildResult(conn, { state, container, extraction, comparison, group });
                const [dups] = etag ? await conn.query(
                    `SELECT id FROM ${jportalSchema()}.pl_submissions
                      WHERE supplier_id = ? AND s3_etag = ? AND status IN ('submitted', 'imported') AND id <> ?
                      ORDER BY id LIMIT 1`,
                    [row.supplier_id, etag, row.id]
                ) : [[]];
                if (dups.length) result.duplicateOf = Number(dups[0].id);
                return {
                    match: PL.matchColumns(s), logFields: s.logFields, state, result,
                    verdict: comparison?.summary?.verdict ?? null,
                    discrepancyCount: comparison?.summary?.discrepancyCount ?? null,
                };
            });

            // 8. One guarded write: the check, match and result column groups.
            const u = read.usage || {};
            const m = scored.match;
            const [done] = await withConn(conn => conn.query(
                `UPDATE ${jportalSchema()}.pl_submissions
                    SET check_status = 'succeeded', error_code = NULL, error_message = NULL, checked_at = UTC_TIMESTAMP(),
                        model_used = ?, input_tokens = ?, output_tokens = ?, total_tokens = ?,
                        extract_json = ?, row_count = ?, invoice_number = ?,
                        candidates_json = ?, match_confidence = ?, match_reason = ?,
                        suggested_kind = ?, suggested_container_number = ?, suggested_draft_id = ?, suggested_label = ?,
                        verdict = ?, discrepancy_count = ?, result_json = ?, result_state = ?, result_at = UTC_TIMESTAMP(),
                        row_version = row_version + 1
                  WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND status = 'draft'`,
                [
                    String(read.modelUsed || '').slice(0, 64) || null,
                    u.promptTokenCount ?? null, u.candidatesTokenCount ?? null, u.totalTokenCount ?? null,
                    JSON.stringify(extracted), extracted.lines.length,
                    extracted.invoiceNumber ? String(extracted.invoiceNumber).slice(0, 100) : null,
                    JSON.stringify(m.candidates), m.confidence, m.reason,
                    m.suggested.kind, m.suggested.containerNumber, m.suggested.draftId, m.suggested.label,
                    scored.verdict, scored.discrepancyCount, JSON.stringify(scored.result), scored.state,
                    row.id, attempt,
                ]
            ));
            if (done.affectedRows !== 1) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };
            await audit(Number(row.id), 'check_succeeded', {
                attempt, source: trigger.source, modelUsed: read.modelUsed || null, rows: extracted.lines.length,
                matchConfidence: m.confidence, matchReason: m.reason, suggestedLabel: m.suggested.label,
                resultState: scored.state, verdict: scored.verdict, discrepancyCount: scored.discrepancyCount,
                duplicateOf: scored.result.duplicateOf ?? null,
            });
            return {
                outcome: 'succeeded', submissionId: Number(row.id),
                confidence: m.confidence, reason: m.reason, suggestedLabel: m.suggested.label, resultState: scored.state,
            };
        } catch (e) {
            // Anything unexpected after the claim (a database error, say) is
            // recorded as the model being unavailable, so the supplier can
            // check again. Only a failure to write that throws.
            log.error('[pl-check] unexpected error', { submissionId: row.id, error: e.message });
            return fail(row, attempt, 'MODEL_UNAVAILABLE', `Internal error: ${e.message}`, { read });
        }
    }

    return { run };
}

module.exports = { createPlChecker };
