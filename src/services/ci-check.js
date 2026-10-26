'use strict';

// The supplier-portal commercial-invoice check (Lambda jportal-ci-check-<env>,
// src/handlers/ci-check.js). Reads an invoice a supplier uploaded through
// jportal with the balance-invoice reader's own prompt and schema
// (src/services/shipment-payment-extract.js readDocument, given the bytes and
// a "sent through the portal, shipment unknown" context), then suggests which
// of the supplier's booked shipments it belongs to: each candidate is judged
// by assessFit — the same "does this belong here" check a staff upload on a
// shipment gets — plus the purchase orders and goods value it names, and
// ranked by src/lib/ci-submissions.js rankCandidates. The suggestion is for
// staff only (ShipLine's review panel); the supplier is shown nothing of it.
// Rules and constants: src/lib/ci-submissions.js. The claim pattern is
// pl-check.js's.
//
// Triggers (any of them may arrive twice; the claim makes that harmless):
//   - the portal bucket's EventBridge "Object Created" event for ci/* (shipping's
//     own rule, serverless.yml) — may claim only an awaiting_upload row;
//   - a direct async invoke { submissionId } from jportal's `uploaded` — may
//     also claim a stale pending row or a retryable failure;
//   - a staff re-read (POST /api/v1/ci-submissions/:id/recheck), run in-process
//     with { staff: true } — any submitted row not being read right now
//     (lib planEvent 'staff').
//
// Steps:
//   1. parse the trigger; read the row; planEvent decides whether to try.
//   2. direct and staff: HeadObject. A missing object stops here, no attempt used.
//   3. claim: one guarded UPDATE → pending, attempt + 1. No row changed:
//      someone else has it; stop.
//   4. GetObject. Missing after the claim: back to awaiting_upload with the
//      attempt given back. Over 10 MiB, or bytes that are not the declared
//      type: failed FILE_INVALID. Otherwise pin s3_etag + file_size.
//   5. the supplier group (pl-results.js portalGroup: the ids the portal wrote
//      into group_supplier_ids at create, with their live names; failed
//      SUPPLIER_NOT_FOUND when none of them is live — before a Gemini call is
//      spent) and its QC units (an invoice may bill them).
//   6. readDocument. Model errors are classified by lib/gemini-retry.js
//      classifyGeminiError (DOCUMENT_UNPROCESSABLE, MODEL_UNAVAILABLE,
//      NOT_CONFIGURED, MODEL_NO_JSON). A document that is not an invoice (a
//      remittance, say) still succeeds: staff decide.
//   7. the candidates (suggestShipment: the group's booked shipments, open and
//      newest first, at most CANDIDATE_LIMIT) scored and ranked.
//   8. write the check and match column groups in ONE guarded UPDATE; audit in
//      shipping's audit_log (entity_type 'ci_submission').
// No database connection is held during the S3 or Gemini calls (the pool has
// one connection) and none is nested. Every write after the claim is guarded
// on check_attempt and check_status = 'pending', so a run that lost its claim
// writes nothing — and so does one whose row staff decided meanwhile.
// run() throws only when writing the failed row itself fails.

const { S3Client, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const L = require('../lib/ci-submissions');
const { jportalSchema } = require('../lib/jportal');
const { recordAudit } = require('../lib/audit');
const { classifyGeminiError } = require('../lib/gemini-retry');
const { BOOKED_STAGES } = require('../lib/shipments');
const E = require('./shipment-payment-extract');
const { supplierKey, loadQcLines } = require('./shipment-payments');
const PL = require('./pl-results');
const { isMissing } = require('./pi-check');
const log = require('../lib/logger');

const REGION = process.env.AWS_REGION || 'eu-north-1';
const DEFAULT_RUN_MS = 300 * 1000;

const round = (n, places) => { const f = 10 ** places; return Math.round(n * f) / f; };

// ── Candidates ───────────────────────────────────────────────────────────
// What assessFit compares a document with, plus what the review panel shows.
const SHIPMENT_COLUMNS = `s.id, s.reference, s.name, s.stage, s.mode, s.tracking_ref, s.bl_number, s.booking_ref,
                          DATE_FORMAT(s.etd, '%Y-%m-%d') AS etd, DATE_FORMAT(s.eta, '%Y-%m-%d') AS eta`;

// The POs on each of these shipments with what each has on board — the shape
// shipment-payments.js loadMemberPurchaseOrders returns, for many at once.
// → Map(shipmentId → memberPos[])
async function loadMemberPos(conn, shipmentIds) {
    const out = new Map();
    if (!shipmentIds.length) return out;
    const [rows] = await conn.query(
        `SELECT o.shipment_id, po.id, po.po_number, po.supplier, po.currency,
                COUNT(*) AS line_count,
                SUM(o.quantity) AS units_in_shipment,
                ROUND(SUM(o.quantity * COALESCE(o.unit_price, 0)), 2) AS value_in_shipment
           FROM orders o
           JOIN purchase_orders po ON po.id = o.purchase_order_id AND po.deleted_at IS NULL
          WHERE o.deleted_at IS NULL AND o.shipment_id IN (?)
          GROUP BY o.shipment_id, po.id, po.po_number, po.supplier, po.currency
          ORDER BY o.shipment_id, value_in_shipment DESC, po.id`,
        [shipmentIds]
    );
    for (const r of rows) {
        const sid = Number(r.shipment_id);
        if (!out.has(sid)) out.set(sid, []);
        out.get(sid).push({
            id: r.id,
            poNumber: r.po_number,
            supplier: r.supplier || null,
            supplierKey: supplierKey(r.supplier),
            currency: (r.currency || 'USD').toUpperCase(),
            lineCount: Number(r.line_count) || 0,
            unitsInShipment: Number(r.units_in_shipment) || 0,
            valueInShipment: r.value_in_shipment != null ? Number(r.value_in_shipment) : 0,
        });
    }
    return out;
}

/**
 * The group's candidate shipments: booked or later (lib/shipments.js
 * BOOKED_STAGES), live and not merged, holding a live order line of a purchase
 * order filed under one of the group's names. The open ones first (CLOSED
 * last), newest first, at most `limit`. → [{ shipment, memberPos }]
 */
async function loadCandidates(conn, names, { limit = L.CANDIDATE_LIMIT } = {}) {
    if (!names || !names.length) return [];
    const [ships] = await conn.query(
        `SELECT ${SHIPMENT_COLUMNS}
           FROM shipments s
          WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL AND s.stage IN (?)
            AND EXISTS (SELECT 1 FROM orders o
                          JOIN purchase_orders po ON po.id = o.purchase_order_id AND po.deleted_at IS NULL
                         WHERE o.shipment_id = s.id AND o.deleted_at IS NULL AND po.supplier IN (?))
          ORDER BY (s.stage = 'CLOSED'), COALESCE(s.etd, DATE(s.booked_at), DATE(s.created_at)) DESC, s.id DESC
          LIMIT ${Number(limit)}`,
        [BOOKED_STAGES, names]
    );
    if (!ships.length) return [];
    const pos = await loadMemberPos(conn, ships.map(s => Number(s.id)));
    return ships.map(s => ({ shipment: s, memberPos: pos.get(Number(s.id)) || [] }));
}

/** One shipment as a candidate, whatever its stage (the staff preview). */
async function loadShipmentCandidate(conn, shipmentId) {
    const [rows] = await conn.query(`SELECT ${SHIPMENT_COLUMNS} FROM shipments s WHERE s.id = ?`, [shipmentId]);
    if (!rows.length) return null;
    const pos = await loadMemberPos(conn, [Number(shipmentId)]);
    return { shipment: rows[0], memberPos: pos.get(Number(shipmentId)) || [] };
}

// The goods value a document bills: its lines (the goods only — charges are
// read apart), else its total less its charges. null when it states neither.
function goodsOf(extract) {
    let lines = 0;
    for (const l of Array.isArray(extract?.lines) ? extract.lines : []) {
        const amount = Number(l?.amount);
        const value = l?.amount != null && Number.isFinite(amount)
            ? amount
            : (l?.qty != null && l?.unitPrice != null ? Number(l.qty) * Number(l.unitPrice) : 0);
        if (Number.isFinite(value) && value > 0) lines += value;
    }
    if (lines > 0) return round(lines, 2);
    const total = E.money(extract?.totalAmount);
    if (total == null) return null;
    const charges = E.chargesOf(extract).reduce((a, c) => a + c.amount, 0);
    return round(total - charges, 2);
}

/**
 * The evidence for one candidate, as lib rankCandidates reads it. The supplier
 * name is `supplierName` when given (a staff override), else the group's
 * spelling on that shipment's POs with the most on board — the name the
 * document would be filed under. → evidence (see lib/ci-submissions.js)
 */
function candidateEvidence({ extract, shipment, memberPos, groupNames, supplierName = null }) {
    const keys = new Set((groupNames || []).map(supplierKey));
    const pos = memberPos || [];
    const mine = pos.filter(p => keys.has(p.supplierKey));
    let name = supplierName;
    if (!name) {
        const byName = new Map();
        for (const p of mine) byName.set(p.supplier, (byName.get(p.supplier) || 0) + (Number(p.valueInShipment) || 0));
        name = [...byName.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;
    }
    const filedAs = name ? pos.filter(p => p.supplierKey === supplierKey(name)) : [];
    const own = filedAs.length ? filedAs : mine;

    const fit = E.assessFit({ extract, shipment, supplierName: name, memberPos: pos, qcPos: [], amount: E.documentAmountOf(extract) });
    const check = key => {
        const c = fit.checks.find(x => x.key === key);
        return c ? c.ok : null;
    };

    // The group's purchase orders on board that the document names.
    const index = E.buildRefIndex(pos);
    const refs = [...new Set([...(extract.poRefs || []), ...(extract.lines || []).map(l => l && l.poRef)]
        .map(r => String(r ?? '').trim()).filter(Boolean))];
    const poHits = new Set();
    for (const r of refs) {
        const po = E.matchPoRef(r, index);
        if (po && keys.has(po.supplierKey)) poHits.add(po.poNumber);
    }

    const onBoard = round(own.reduce((a, p) => a + (Number(p.valueInShipment) || 0), 0), 2);
    const goods = goodsOf(extract);
    return {
        shipmentId: Number(shipment.id),
        reference: shipment.reference ?? null,
        name: shipment.name ?? null,
        stage: shipment.stage ?? null,
        mode: shipment.mode ?? null,
        trackingRef: shipment.tracking_ref ?? null,
        blNumber: shipment.bl_number ?? null,
        bookingRef: shipment.booking_ref ?? null,
        etd: shipment.etd ?? null,
        eta: shipment.eta ?? null,
        supplierName: name,
        onBoard,
        currency: fit.currency ?? own[0]?.currency ?? null,
        goods,
        amountRatio: onBoard > 0 && goods != null && goods > 0 ? round(goods / onBoard, 4) : null,
        container: check('container'),
        pos: check('pos'),
        supplier: check('supplier'),
        amountOver: check('amount') === false,
        currencyMiss: check('currency') === false,
        poHits: [...poHits].sort(),
        fitVerdict: fit.verdict,
        checks: fit.checks,
    };
}

/**
 * The suggestion for an extraction: the group's candidates, each one's
 * evidence, ranked. Holds the connection only for the two reads.
 * → { ranked, confidence, reason, suggested, considered, groupNames }
 */
async function suggestShipment(conn, { group, extract }) {
    const groupNames = group?.names || [];
    const candidates = await loadCandidates(conn, groupNames);
    const evidence = candidates.map(c => candidateEvidence({ extract, shipment: c.shipment, memberPos: c.memberPos, groupNames }));
    return { ...L.rankCandidates(evidence), considered: candidates.length, groupNames };
}

// The extraction with the arrays the matcher reads always present.
function normaliseExtract(parsed) {
    const x = parsed && typeof parsed === 'object' ? parsed : {};
    for (const k of ['containerRefs', 'blRefs', 'poRefs', 'lines', 'otherCharges']) if (!Array.isArray(x[k])) x[k] = [];
    return x;
}

// The group's QC units on any of its POs, for the reader's QC section.
async function groupQcLines(conn, group) {
    const keys = new Set((group?.names || []).map(supplierKey));
    if (!keys.size) return [];
    return (await loadQcLines(conn)).filter(l => keys.has(l.supplierKey) || (group.names || []).some(n => E.sameSupplier(l.supplier, n)));
}

function createCiChecker({
    pool,
    bucket = process.env.PI_UPLOADS_BUCKET,
    s3 = new S3Client({ region: REGION }),
    readDocument = E.readDocument,
    portalGroup = PL.portalGroup,
    loadQc = groupQcLines,
    suggest = suggestShipment,
    now = Date.now,
} = {}) {
    const withConn = async (fn) => {
        const conn = await pool.getConnection();
        try { return await fn(conn); } finally { conn.release(); }
    };

    const SELECT = () => `SELECT id, status, check_status, check_attempt, error_code, s3_key, s3_etag,
                                 content_type, file_size, filename, supplier_id, supplier_name, group_supplier_ids, row_version,
                                 TIMESTAMPDIFF(SECOND, check_started_at, UTC_TIMESTAMP()) AS check_age_seconds
                            FROM ${jportalSchema()}.ci_submissions`;

    const audit = (submissionId, action, after, userEmail = null) => withConn(conn => recordAudit(conn, {
        entityType: 'ci_submission', entityId: submissionId, action, before: null, after, userEmail,
    })).catch(e => log.warn('[ci-check] audit failed', { submissionId, action, error: e.message }));

    // Writes the failed row for the claimed attempt. `read` (what the model
    // returned, when it got that far) is kept so staff can still see it.
    // Throws if the write fails.
    const fail = async (row, attempt, code, message, { read = null, userEmail = null } = {}) => {
        const u = read?.usage || {};
        const parsed = read?.parsed || null;
        const r = await withConn(conn => conn.query(
            `UPDATE ${jportalSchema()}.ci_submissions
                SET check_status = 'failed', error_code = ?, error_message = ?, checked_at = UTC_TIMESTAMP(),
                    model_used = COALESCE(?, model_used), input_tokens = COALESCE(?, input_tokens),
                    output_tokens = COALESCE(?, output_tokens), total_tokens = COALESCE(?, total_tokens),
                    extract_json = COALESCE(?, extract_json), invoice_number = COALESCE(?, invoice_number),
                    row_version = row_version + 1
              WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND ${L.CHECKABLE_SQL}`,
            [
                code, String(message || code).slice(0, 1000),
                read ? String(read.modelUsed || '').slice(0, 64) || null : null,
                u.promptTokenCount ?? null, u.candidatesTokenCount ?? null, u.totalTokenCount ?? null,
                parsed ? JSON.stringify(parsed) : null,
                parsed?.invoiceNumber ? String(parsed.invoiceNumber).slice(0, 100) : null,
                row.id, attempt,
            ]
        ));
        const written = r[0].affectedRows === 1;
        if (written) await audit(Number(row.id), 'check_failed', { attempt, errorCode: code }, userEmail);
        return { outcome: 'failed', submissionId: Number(row.id), code, written };
    };

    /**
     * Handles one invocation. `event` is the Lambda event; `deadline` the
     * epoch ms by which the run must be done (the Lambda's remaining time).
     * `staff` (the routes' in-process re-read only, with `userEmail` for the
     * audit): `event` is { submissionId } and the staff claim applies.
     * → { outcome, submissionId?, reason?, code? }
     */
    async function run(event, { deadline = now() + DEFAULT_RUN_MS, staff = false, userEmail = null } = {}) {
        let trigger;
        if (staff) {
            const submissionId = L.positiveInt(event && event.submissionId);
            trigger = submissionId ? { source: 'staff', submissionId } : { ignore: 'unrecognised_event' };
        } else {
            trigger = L.parseTrigger(event, { bucket });
        }
        if (trigger.ignore) return { outcome: 'ignored', reason: trigger.ignore };
        if (!bucket) {
            log.error('[ci-check] PI_UPLOADS_BUCKET is not configured');
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
            log.error('[ci-check] could not read the submission', { trigger, error: e.message });
            return { outcome: 'skipped', reason: 'read_failed' };
        }
        const plan = L.planEvent(trigger.source, row, now());
        if (!plan.claim) return { outcome: 'skipped', submissionId: row ? Number(row.id) : null, reason: plan.reason };

        // 2. Direct and staff: the object must be there before an attempt is spent.
        if (trigger.source !== 'event') {
            try {
                await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.s3_key }));
            } catch (e) {
                if (!isMissing(e)) log.warn('[ci-check] head failed', { submissionId: row.id, error: e.message });
                return { outcome: 'skipped', submissionId: Number(row.id), reason: isMissing(e) ? 'file_missing' : 'head_failed' };
            }
        }

        // 3. Claim.
        const guard = L.claimWhere(trigger.source);
        let attempt;
        try {
            attempt = await withConn(async (conn) => {
                const [r] = await conn.query(
                    `UPDATE ${jportalSchema()}.ci_submissions
                        SET check_status = 'pending', check_attempt = check_attempt + 1, check_started_at = UTC_TIMESTAMP(),
                            error_code = NULL, error_message = NULL, row_version = row_version + 1
                      WHERE id = ? AND ${guard.sql}`,
                    [row.id, ...guard.params]
                );
                if (r.affectedRows !== 1) return null;
                // Fresh pending: nothing else can claim it now, so this read is ours.
                const [[c]] = await conn.query(`SELECT check_attempt FROM ${jportalSchema()}.ci_submissions WHERE id = ?`, [row.id]);
                return Number(c.check_attempt);
            });
        } catch (e) {
            log.error('[ci-check] claim failed', { submissionId: row.id, error: e.message });
            return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_failed' };
        }
        if (attempt === null) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };
        if (trigger.source === 'staff') await audit(Number(row.id), 'recheck', { attempt, previous: row.check_status }, userEmail);

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
                if (isMissing(e)) log.warn('[ci-check] object missing after claim', { submissionId: row.id });
                else log.error('[ci-check] get failed after claim', { submissionId: row.id, error: e.message });
                await withConn(conn => conn.query(
                    `UPDATE ${jportalSchema()}.ci_submissions
                        SET check_status = 'awaiting_upload', check_attempt = check_attempt - 1, check_started_at = NULL,
                            row_version = row_version + 1
                      WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND ${L.CHECKABLE_SQL}`,
                    [row.id, attempt]
                ));
                return { outcome: 'released', submissionId: Number(row.id), reason: isMissing(e) ? 'file_missing' : 'fetch_failed' };
            }
            if (bytes === null || bytes.length > L.MAX_FILE_BYTES) {
                const size = bytes === null ? declaredSize : bytes.length;
                return await fail(row, attempt, 'FILE_INVALID', `File is ${size} bytes; the limit is ${L.MAX_FILE_BYTES}.`, { userEmail });
            }
            const sniffed = L.sniffType(bytes);
            if (!L.ALLOWED_TYPES[row.content_type] || sniffed !== row.content_type) {
                return await fail(row, attempt, 'FILE_INVALID', `The file is not a valid ${row.content_type} (read as ${sniffed || 'unknown'}).`, { userEmail });
            }
            // The portal pins the ETag when it sends the invoice; the same
            // bytes pin the same value, so this only fills it in for a draft.
            const pinned = await withConn(conn => conn.query(
                `UPDATE ${jportalSchema()}.ci_submissions SET s3_etag = ?, file_size = ?, row_version = row_version + 1
                  WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND ${L.CHECKABLE_SQL}`,
                [etag, bytes.length, row.id, attempt]
            ));
            if (pinned[0].affectedRows !== 1) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };

            // 5. The supplier group, before a Gemini call is spent on a
            //    supplier that is gone, and its QC units.
            const group = await withConn(conn => portalGroup(conn, row));
            if (PL.groupGone(group)) {
                return await fail(row, attempt, 'SUPPLIER_NOT_FOUND', `Supplier ${row.supplier_id} (${row.supplier_name}) no longer exists (group ${JSON.stringify(group.supplierIds)}).`, { userEmail });
            }
            const qcLines = await withConn(conn => loadQc(conn, group));

            // 6. The model.
            try {
                read = await readDocument({
                    s3Key: row.s3_key, contentType: row.content_type, bytes,
                    context: { shipment: null, shipmentCurrency: null, purchaseOrders: [], qcLines, portalUpload: true },
                });
            } catch (e) {
                const c = classifyGeminiError(e);
                if (c.code === 'NOT_CONFIGURED') log.error('[ci-check] Gemini is not configured or refused the key', { submissionId: row.id, error: e.message });
                else log.warn('[ci-check] read failed', { submissionId: row.id, code: c.code, error: e.message });
                return await fail(row, attempt, c.code, e.message, { userEmail });
            }
            const extract = normaliseExtract(read.parsed);

            // 7. The suggestion.
            const s = await withConn(conn => suggest(conn, { group, extract }));
            const top = s.suggested;

            // 8. One guarded write: the check and match column groups.
            const u = read.usage || {};
            const [done] = await withConn(conn => conn.query(
                `UPDATE ${jportalSchema()}.ci_submissions
                    SET check_status = 'succeeded', error_code = NULL, error_message = NULL, checked_at = UTC_TIMESTAMP(),
                        model_used = ?, input_tokens = ?, output_tokens = ?, total_tokens = ?,
                        extract_json = ?, invoice_number = ?,
                        candidates_json = ?, match_confidence = ?, match_reason = ?,
                        suggested_shipment_id = ?, suggested_shipment_reference = ?, suggested_supplier_name = ?,
                        row_version = row_version + 1
                  WHERE id = ? AND check_attempt = ? AND check_status = 'pending' AND ${L.CHECKABLE_SQL}`,
                [
                    String(read.modelUsed || '').slice(0, 64) || null,
                    u.promptTokenCount ?? null, u.candidatesTokenCount ?? null, u.totalTokenCount ?? null,
                    JSON.stringify(extract),
                    extract.invoiceNumber ? String(extract.invoiceNumber).slice(0, 100) : null,
                    JSON.stringify(L.storedCandidates({ ranked: s.ranked, considered: s.considered, groupNames: s.groupNames })),
                    s.confidence, s.reason,
                    top ? top.shipmentId : null,
                    top && top.reference ? String(top.reference).slice(0, 100) : null,
                    top && top.supplierName ? String(top.supplierName).slice(0, 255) : null,
                    row.id, attempt,
                ]
            ));
            if (done.affectedRows !== 1) return { outcome: 'skipped', submissionId: Number(row.id), reason: 'claim_lost' };
            await audit(Number(row.id), 'check_succeeded', {
                attempt, source: trigger.source, modelUsed: read.modelUsed || null,
                documentKind: extract.documentKind || null, invoiceNumber: extract.invoiceNumber || null,
                matchConfidence: s.confidence, matchReason: s.reason, considered: s.considered,
                suggestedShipmentId: top ? top.shipmentId : null, suggestedReference: top ? top.reference : null,
            }, userEmail);
            return {
                outcome: 'succeeded', submissionId: Number(row.id),
                confidence: s.confidence, reason: s.reason, suggestedReference: top ? top.reference : null, considered: s.considered,
            };
        } catch (e) {
            // Anything unexpected after the claim (a database error, say) is
            // recorded as the model being unavailable, so a later trigger or
            // a staff re-read can try again. Only a failure to write that throws.
            log.error('[ci-check] unexpected error', { submissionId: row.id, error: e.message });
            return fail(row, attempt, 'MODEL_UNAVAILABLE', `Internal error: ${e.message}`, { read, userEmail });
        }
    }

    return { run };
}

module.exports = {
    createCiChecker,
    loadMemberPos,
    loadCandidates,
    loadShipmentCandidate,
    goodsOf,
    candidateEvidence,
    suggestShipment,
    normaliseExtract,
    groupQcLines,
};
