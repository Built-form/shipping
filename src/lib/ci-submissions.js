'use strict';

// ── Commercial invoices uploaded by suppliers through jportal ───────────────
// The invoice for the goods in a shipment, which staff otherwise receive by
// email and upload on the shipment themselves (POST
// /api/v1/shipment-payment-documents, doc_kind 'balance_invoice'). Not a
// proforma: a PI is per purchase order (pi-submissions.js); a CI is per
// shipment. A supplier uploads one on the portal's "Send invoices" page with
// no shipment — they do not know our shipment numbers. The file goes to the
// portal's bucket (jportal-pi-uploads-<env>, key ci/<uuid>.<pdf|png|jpg>) and
// a row to jportal.ci_submissions; the portal sends it to staff as soon as
// the file has arrived. Shipping's ciCheck Lambda (src/services/ci-check.js)
// reads it with the balance-invoice reader's own prompt and schema
// (shipment-payment-extract.js readDocument) and SUGGESTS which of the
// supplier's booked shipments it belongs to (rankCandidates below) — for
// staff only: the supplier is shown nothing of the check. Staff confirm or
// choose the shipment and import it — a shipment_payment_documents row on
// that shipment, read exactly as a staff upload is — or reject it with a note
// (src/services/ci-submission-routes.js).
//
// Who writes what on jportal.ci_submissions: jportal creates rows and moves
// draft→submitted / draft→discarded / submitted→withdrawn; shipping writes the
// check, match and decision column groups (the GRANT per group is in
// docs/ci-submissions-frontend.md). Every status, check or decision change
// bumps row_version.
//
// Pure: no database, no clock, no network. The claim rules, file sniffing,
// filename and decider rules are PI's and PL's (src/lib/pi-submissions.js,
// src/lib/pl-submissions.js), re-exported so the flows cannot drift.
// Constants shared with jportal (its src/lib/ciSubmissions.js — keep in
// step): the key pattern, the allowed types, the 10 MiB limit, the statuses.

const PI = require('./pi-submissions');
const PLS = require('./pl-submissions');

const {
    STALE_CHECK_SECONDS, MAX_CHECK_ATTEMPTS, RETRYABLE_CODES, MAX_FILE_BYTES, ALLOWED_TYPES,
    CHECK_STATUSES, VISIBLE_STATUSES, DECIDED_STATUSES, DECIDER_ROLES,
    canDecide, sniffType, safeFilename, parseJson,
    positiveInt, parseRowVersion, parseNote,
} = PI;

// The supplier group (the ids the portal wrote at create) and the draft-or-
// submitted claim are PL's: a CI is sent on arrival too, so its check usually
// runs on a submitted row.
const { CHECKABLE_SQL, parseGroupSupplierIds, parseListQuery, parseRejectBody } = PLS;

const CI_KEY_RE = /^ci\/[0-9a-f-]{36}\.(pdf|png|jpg)$/;
const isCiKey = key => typeof key === 'string' && CI_KEY_RE.test(key);

const STATUSES = ['draft', 'submitted', 'imported', 'rejected', 'withdrawn', 'discarded'];
const CONFIDENCES = ['confident', 'unsure', 'none'];
// Why the top candidate won (match_reason). null with confidence 'none'.
//   container   the invoice prints that shipment's container, B/L or booking number
//   po_numbers  it names the group's purchase orders on board there, and no other candidate as many
//   amount      it names POs on several candidates; its goods value matches this one's
//   single      the group's only booked shipment, and nothing on the invoice contradicts it
//   conflict    two candidates tie on the deciding evidence: staff choose
const MATCH_REASONS = ['container', 'po_numbers', 'amount', 'single', 'conflict'];

// A staff re-read ("Read again" in ShipLine) may claim any submitted row that
// is not being read right now, for a few more attempts than the portal's
// triggers get: a check that failed for good still deserves a human retry.
const STAFF_MAX_CHECK_ATTEMPTS = MAX_CHECK_ATTEMPTS + 2;

// The candidates one check scores (the group's booked shipments, the open and
// most recent first) and the ones candidates_json keeps.
const CANDIDATE_LIMIT = 30;
const STORED_CANDIDATE_LIMIT = 5;

// A goods value within these of what the group has on board "is" that box.
const AMOUNT_EXACT = 0.01;
const AMOUNT_CLOSE = 0.05;

const NOTE_MAX = PLS.NOTE_MAX;

// Why a submission cannot be imported as it stands. The codes are API surface.
const BLOCKERS = {
    not_submitted: 'The submission is not waiting for review.',
    supplier_not_found: 'The supplier no longer exists.',
    bad_key: 'The file is not where an uploaded invoice should be.',
    bad_type: 'The file is not a PDF, PNG or JPEG.',
    too_large: 'The file is larger than 10 MiB.',
    no_etag: 'The file was never validated.',
    file_gone: 'The file is no longer in the portal’s storage.',
    file_changed: 'The file has changed since it was validated.',
};

const isObj = v => v !== null && typeof v === 'object' && !Array.isArray(v);
const num = v => (v === null || v === undefined ? null : Number(v));

// ── Files ────────────────────────────────────────────────────────────────────
// Where the imported copy goes in shipping's PO bucket: the staff upload's
// shipment-payments/<token>/<name> layout (POST /shipment-payment-documents),
// the name falling back to ci-<submissionId>.<ext> when the sanitised name is
// empty (an all-Chinese file name, say).
function destKey({ submissionId, filename, contentType, token }) {
    const ext = ALLOWED_TYPES[contentType] || 'bin';
    const name = safeFilename(filename, contentType) || `ci-${submissionId}.${ext}`;
    return `shipment-payments/${token}/${name}`;
}

// ── Triggers and the claim ──────────────────────────────────────────────────
/**
 * What invoked the ciCheck Lambda.
 *   - EventBridge "Object Created" from the portal's bucket: { source: 'event', key }
 *     (keys that are not ci/<uuid>.<ext>, or another bucket, are ignored)
 *   - a direct invoke { submissionId } (jportal's `uploaded`): { source: 'direct', submissionId }
 * A staff re-read is not a Lambda event: the routes run the checker in-process
 * with { staff: true } (ci-check.js run), which no payload can ask for.
 * → one of those, or { ignore: reason }.
 */
function parseTrigger(event, { bucket = null } = {}) {
    const e = isObj(event) ? event : {};
    if (e.source === 'aws.s3' || e['detail-type'] === 'Object Created') {
        const d = isObj(e.detail) ? e.detail : {};
        const name = isObj(d.bucket) ? d.bucket.name : null;
        if (bucket && name !== bucket) return { ignore: 'other_bucket' };
        let key = isObj(d.object) ? d.object.key : null;
        if (typeof key === 'string' && !isCiKey(key)) {
            // S3 event keys may be URL-encoded; a valid CI key never needs it.
            try { key = decodeURIComponent(key.replace(/\+/g, ' ')); } catch { /* keep as is */ }
        }
        if (!isCiKey(key)) return { ignore: 'not_a_ci_key' };
        return { source: 'event', key };
    }
    const submissionId = positiveInt(e.submissionId);
    if (submissionId) return { source: 'direct', submissionId };
    return { ignore: 'unrecognised_event' };
}

// A pending claim older than STALE_CHECK_SECONDS, from the SQL-computed
// check_age_seconds the checker's SELECT carries (the column is UTC).
const isStalePending = row => row.check_status === 'pending'
    && (row.check_age_seconds === null || row.check_age_seconds === undefined || Number(row.check_age_seconds) > STALE_CHECK_SECONDS);

/**
 * Whether a trigger may claim the row. 'event' and 'direct' are PL's rules (a
 * draft or a submitted row; see pl-submissions.js planEvent). 'staff' (a
 * re-read from ShipLine): a submitted row only, whatever its check says except
 * a read in progress, while check_attempt < STAFF_MAX_CHECK_ATTEMPTS.
 * → { claim: boolean, reason }
 */
function planEvent(source, row, now) {
    if (source !== 'staff') return PLS.planEvent(source, row, now);
    if (!row) return { claim: false, reason: 'not_found' };
    if (row.status !== 'submitted') return { claim: false, reason: 'not_submitted' };
    if (Number(row.check_attempt) >= STAFF_MAX_CHECK_ATTEMPTS) return { claim: false, reason: 'attempts_exhausted' };
    if (row.check_status === 'pending') {
        return isStalePending(row) ? { claim: true, reason: 'stale_pending' } : { claim: false, reason: 'in_progress' };
    }
    return { claim: true, reason: `staff_${row.check_status}` };
}

// The guard on the claim UPDATE, for planEvent's states. → { sql, params }
function claimWhere(source) {
    if (source !== 'staff') return PLS.claimWhere(source);
    return {
        sql: `status = 'submitted' AND check_attempt < ${STAFF_MAX_CHECK_ATTEMPTS}
              AND (check_status IN ('awaiting_upload', 'succeeded', 'failed')
                   OR (check_status = 'pending' AND (check_started_at IS NULL OR check_started_at < UTC_TIMESTAMP() - INTERVAL ${STALE_CHECK_SECONDS} SECOND)))`,
        params: [],
    };
}

// ── Matching ────────────────────────────────────────────────────────────────
// Each candidate shipment arrives as the evidence ci-check.js candidateEvidence
// builds from assessFit (the same "does this belong here" check a staff upload
// gets) and the invoice's references:
//   { shipmentId, reference, ..., container: true|false|null, pos: true|false|null,
//     poHits: [poNumber], amountRatio: goods / on board | null, amountOver,
//     currencyMiss, fitVerdict }
// in recency order (the open and newest first), which breaks ties.

const closeness = e => (e.amountRatio == null || !Number.isFinite(Number(e.amountRatio)) ? null : Math.abs(Number(e.amountRatio) - 1));
const amountExact = e => { const d = closeness(e); return d !== null && d <= AMOUNT_EXACT; };
const amountClose = e => { const d = closeness(e); return d !== null && d <= AMOUNT_CLOSE; };
const hits = e => (Array.isArray(e.poHits) ? e.poHits.length : 0);
// Something on the invoice points away from this shipment.
const contradicted = e => e.container === false || e.pos === false || e.amountOver === true || e.currencyMiss === true;

/**
 * One candidate's score. A container / B/L number that is this shipment's
 * outweighs everything; each of the group's purchase orders named on board
 * adds (up to four); a goods value equal to what the group has on board adds;
 * anything pointing elsewhere subtracts. Only the order matters: confidence
 * is decided on the evidence itself (rankCandidates).
 */
function scoreEvidence(e) {
    let score = 0;
    if (e.container === true) score += 100;
    else if (e.container === false) score -= 60;
    score += 25 * Math.min(hits(e), 4);
    if (e.pos === false) score -= 25;
    if (e.amountOver === true) score -= 30;
    else if (amountExact(e)) score += 30;
    else if (amountClose(e)) score += 15;
    if (e.currencyMiss === true) score -= 15;
    return score;
}

/**
 * Ranks the candidates and decides how sure the suggestion is.
 *   confident  container: the invoice names this box and no other candidate's
 *              (or another's too, but this one also has more of the POs);
 *              po_numbers: it names more of the group's POs here than on any
 *              other candidate, and nothing contradicts it;
 *              amount: POs tie, and only this one's goods value matches
 *              (within AMOUNT_CLOSE); single: the only candidate, nothing
 *              contradicts it and the goods value matches.
 *   unsure     there is a best guess, but no more (a tie: conflict; the only
 *              candidate with nothing to confirm it: single; POs named but
 *              something contradicts it).
 *   none       nothing ties the invoice to any candidate: suggested is null
 *              and staff choose.
 * → { ranked: [evidence + { score, rank }], confidence, reason, suggested }
 */
function rankCandidates(evidence) {
    const ranked = (Array.isArray(evidence) ? evidence : [])
        .map((e, i) => ({ ...e, score: scoreEvidence(e), order: i }))
        .sort((a, b) => b.score - a.score || a.order - b.order)
        .map(({ order, ...e }, i) => ({ ...e, rank: i + 1 }));
    const out = (confidence, reason) => ({ ranked, confidence, reason, suggested: confidence === 'none' ? null : ranked[0] });
    if (!ranked.length) return out('none', null);
    const [a, ...rest] = ranked;

    if (a.container === true) {
        const rival = rest.find(e => e.container === true);
        if (rival && hits(rival) >= hits(a)) return out('unsure', 'conflict');
        return a.pos === false ? out('unsure', 'conflict') : out('confident', 'container');
    }
    if (hits(a) > 0) {
        const tied = rest.filter(e => hits(e) >= hits(a) && e.container !== false);
        if (tied.length) {
            // A PO split over several boxes: the goods value tells them apart.
            if (amountClose(a) && !tied.some(amountClose) && !contradicted(a)) return out('confident', 'amount');
            return out('unsure', 'conflict');
        }
        return contradicted(a) ? out('unsure', 'po_numbers') : out('confident', 'po_numbers');
    }
    if (ranked.length === 1 && !contradicted(a)) {
        return amountClose(a) ? out('confident', 'single') : out('unsure', 'single');
    }
    return out('none', null);
}

/**
 * candidates_json as stored: the top STORED_CANDIDATE_LIMIT, each one's
 * checks kept for the review panel, and how many were considered.
 */
function storedCandidates({ ranked, considered, groupNames = [] }) {
    return {
        v: 1,
        considered: Number(considered) || 0,
        groupNames,
        top: (ranked || []).slice(0, STORED_CANDIDATE_LIMIT),
    };
}

// The stored candidates read back: { considered, groupNames, top }.
function parseStoredCandidates(value) {
    const v = parseJson(value);
    if (!isObj(v)) return { considered: 0, groupNames: [], top: [] };
    return {
        considered: Number(v.considered) || 0,
        groupNames: Array.isArray(v.groupNames) ? v.groupNames : [],
        top: Array.isArray(v.top) ? v.top.filter(isObj) : [],
    };
}

// One candidate for the API.
function candidateToJson(c) {
    return {
        shipmentId: num(c.shipmentId),
        reference: c.reference ?? null,
        name: c.name ?? null,
        stage: c.stage ?? null,
        mode: c.mode ?? null,
        trackingRef: c.trackingRef ?? null,
        blNumber: c.blNumber ?? null,
        bookingRef: c.bookingRef ?? null,
        etd: c.etd ?? null,
        eta: c.eta ?? null,
        supplierName: c.supplierName ?? null,
        onBoard: num(c.onBoard),
        currency: c.currency ?? null,
        goods: num(c.goods),
        amountRatio: num(c.amountRatio),
        container: c.container ?? null,
        pos: c.pos ?? null,
        poHits: Array.isArray(c.poHits) ? c.poHits : [],
        amountOver: c.amountOver === true,
        currencyMiss: c.currencyMiss === true,
        fitVerdict: c.fitVerdict ?? null,
        checks: Array.isArray(c.checks) ? c.checks : [],
        score: num(c.score),
        rank: num(c.rank),
    };
}

// ── Import ──────────────────────────────────────────────────────────────────
/**
 * Why `row` cannot be imported now.
 *   supplier: { exists } — the group still has a live supplier; null = not looked up.
 *   file: { gone, changed } from the S3 step, when it ran.
 * → blocker codes (keys of BLOCKERS), [] when importable.
 */
function importBlockers({ row, supplier = null, file = null }) {
    const out = [];
    if (!row || row.status !== 'submitted') out.push('not_submitted');
    if (row) {
        if (supplier && !supplier.exists) out.push('supplier_not_found');
        if (!isCiKey(row.s3_key)) out.push('bad_key');
        if (!ALLOWED_TYPES[row.content_type]) out.push('bad_type');
        else if (isCiKey(row.s3_key) && row.s3_key.slice(row.s3_key.lastIndexOf('.') + 1) !== ALLOWED_TYPES[row.content_type]) out.push('bad_type');
        if (row.file_size != null && Number(row.file_size) > MAX_FILE_BYTES) out.push('too_large');
        if (!row.s3_etag) out.push('no_etag');
    }
    if (file && file.gone) out.push('file_gone');
    else if (file && file.changed) out.push('file_changed');
    return [...new Set(out)];
}

// ── Request parsing ─────────────────────────────────────────────────────────
// The shipment a request names: { shipmentId, shipmentReference }, at least
// one of them (the id wins). → { value } or { error }
function parseShipmentChoice(b) {
    const src = isObj(b) ? b : {};
    let shipmentId = null;
    if (src.shipmentId !== undefined && src.shipmentId !== null && src.shipmentId !== '') {
        shipmentId = positiveInt(src.shipmentId);
        if (!shipmentId) return { error: 'shipmentId must be a positive integer.' };
    }
    const shipmentReference = typeof src.shipmentReference === 'string' && src.shipmentReference.trim()
        ? src.shipmentReference.trim().slice(0, 100) : null;
    if (!shipmentId && !shipmentReference) return { error: 'shipmentId or shipmentReference is required.' };
    return { value: { shipmentId, shipmentReference } };
}

// POST /:id/import body { rowVersion, shipmentId | shipmentReference, supplierName?, note?, force? }.
function parseImportBody(body) {
    const b = isObj(body) ? body : {};
    const rv = parseRowVersion(b.rowVersion);
    if (rv.error) return { error: rv.error };
    const s = parseShipmentChoice(b);
    if (s.error) return { error: s.error };
    let supplierName = null;
    if (b.supplierName !== undefined && b.supplierName !== null && b.supplierName !== '') {
        if (typeof b.supplierName !== 'string' || !b.supplierName.trim()) return { error: 'supplierName must be a non-empty string.' };
        supplierName = b.supplierName.trim().slice(0, 255);
    }
    if (b.force !== undefined && b.force !== null && typeof b.force !== 'boolean') return { error: 'force must be true or false.' };
    const note = parseNote(b.note);
    if (note.error) return { error: note.error };
    return { value: { rowVersion: rv.value, ...s.value, supplierName, force: b.force === true, note: note.value } };
}

// ── JSON ────────────────────────────────────────────────────────────────────
/**
 * One submission for the staff API (the list item; the detail adds to it).
 * `row` is a ci_submissions row read with the *_iso columns of the routes'
 * SELECT. extras: { blockers }
 */
function submissionToJson(row, { blockers = [] } = {}) {
    const suggestedId = num(row.suggested_shipment_id);
    const confirmedId = num(row.confirmed_shipment_id);
    return {
        id: Number(row.id),
        supplierId: Number(row.supplier_id),
        supplierKey: `s:${Number(row.supplier_id)}`,
        supplierName: row.supplier_name,
        groupSupplierIds: parseGroupSupplierIds(row),
        supplierExists: row.supplier_exists === undefined ? null : Number(row.supplier_exists) === 1,
        submittedByEmail: row.submitted_by_email,
        filename: row.filename,
        contentType: row.content_type,
        fileSize: num(row.file_size),
        status: row.status,
        checkStatus: row.check_status,
        errorCode: row.error_code || null,
        invoiceNumber: row.invoice_number || null,
        matchConfidence: row.match_confidence || null,
        matchReason: row.match_reason || null,
        suggested: suggestedId || row.suggested_shipment_reference ? {
            shipmentId: suggestedId,
            shipmentReference: row.suggested_shipment_reference || null,
            supplierName: row.suggested_supplier_name || null,
        } : null,
        confirmed: confirmedId || row.confirmed_shipment_reference ? {
            shipmentId: confirmedId,
            shipmentReference: row.confirmed_shipment_reference || null,
            // The name the document was filed under; null when it is gone.
            supplierName: row.filed_supplier_name || null,
        } : null,
        documentId: num(row.document_id),
        submittedAt: row.submitted_at_iso || null,
        decidedBy: row.decided_by || null,
        decidedAt: row.decided_at_iso || null,
        decidedNote: row.decided_note || null,
        rowVersion: Number(row.row_version),
        createdAt: row.created_at_iso || null,
        blockers,
    };
}

// The check part of the detail.
function checkToJson(row) {
    return {
        status: row.check_status,
        modelUsed: row.model_used || null,
        inputTokens: num(row.input_tokens),
        outputTokens: num(row.output_tokens),
        totalTokens: num(row.total_tokens),
        errorCode: row.error_code || null,
        errorMessage: row.error_message || null,
        checkedAt: row.checked_at_iso || null,
        attempts: Number(row.check_attempt) || 0,
        maxStaffAttempts: STAFF_MAX_CHECK_ATTEMPTS,
        invoiceNumber: row.invoice_number || null,
    };
}

module.exports = {
    // PI's and PL's rules, shared
    STALE_CHECK_SECONDS,
    MAX_CHECK_ATTEMPTS,
    RETRYABLE_CODES,
    MAX_FILE_BYTES,
    ALLOWED_TYPES,
    CHECK_STATUSES,
    VISIBLE_STATUSES,
    DECIDED_STATUSES,
    DECIDER_ROLES,
    CHECKABLE_SQL,
    canDecide,
    sniffType,
    safeFilename,
    parseJson,
    positiveInt,
    parseRowVersion,
    parseNote,
    parseGroupSupplierIds,
    parseListQuery,
    parseRejectBody,
    // CI's own
    CI_KEY_RE,
    STATUSES,
    CONFIDENCES,
    MATCH_REASONS,
    STAFF_MAX_CHECK_ATTEMPTS,
    CANDIDATE_LIMIT,
    STORED_CANDIDATE_LIMIT,
    AMOUNT_EXACT,
    AMOUNT_CLOSE,
    NOTE_MAX,
    BLOCKERS,
    isCiKey,
    destKey,
    parseTrigger,
    planEvent,
    claimWhere,
    scoreEvidence,
    rankCandidates,
    storedCandidates,
    parseStoredCandidates,
    candidateToJson,
    importBlockers,
    parseShipmentChoice,
    parseImportBody,
    submissionToJson,
    checkToJson,
};
