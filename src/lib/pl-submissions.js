'use strict';

// ── Packing lists uploaded by suppliers through jportal ─────────────────────
// A supplier uploads a packing list on the portal's "Send packing lists" page.
// The file goes to the portal's bucket (jportal-pi-uploads-<env>, key
// pl/<uuid>.<pdf|png|jpg>) and a row to jportal.pl_submissions. Shipping's
// plCheck Lambda (src/services/pl-check.js) reads it with the staff check's
// prompt (packing-list-check.js readPackingList), suggests a container
// (src/lib/pl-match.js) and writes a supplier-safe result; plRefresh
// (src/services/pl-results.js) recomputes that result whenever the supplier
// opens the page. The supplier sends it; staff confirm the container and
// import it into the existing packing-list check, or ask for a correction
// (src/services/pl-submission-routes.js).
//
// Who writes what on jportal.pl_submissions: jportal creates rows and moves
// draft→submitted / draft→discarded / submitted→withdrawn; shipping writes the
// check, match, result and decision column groups (the GRANT per group is in
// docs/pl-submissions-frontend.md). Every status, check, decision or result
// change bumps row_version (a result refresh only when the result changed).
//
// Pure: no database, no clock, no network. The claim rules, file sniffing,
// filename and decider rules are PI's (src/lib/pi-submissions.js), re-exported
// so the two flows cannot drift. Constants shared with jportal (its
// src/lib/plSubmissions.js — keep in step): the key pattern, the allowed types,
// the 10 MiB limit, the result states and the line cap.

const PI = require('./pi-submissions');

const {
    STALE_CHECK_SECONDS, MAX_CHECK_ATTEMPTS, RETRYABLE_CODES, MAX_FILE_BYTES, ALLOWED_TYPES,
    CHECK_STATUSES, VISIBLE_STATUSES, DECIDED_STATUSES, DECIDER_ROLES,
    canDecide, sniffType, safeFilename, planEvent, claimWhere, parseJson,
    positiveInt, parseRowVersion, parseNote, escapeHtml,
} = PI;

const PL_KEY_RE = /^pl\/[0-9a-f-]{36}\.(pdf|png|jpg)$/;
const isPlKey = key => typeof key === 'string' && PL_KEY_RE.test(key);

const STATUSES = ['draft', 'submitted', 'imported', 'rejected', 'withdrawn', 'discarded'];
const CONFIDENCES = ['confident', 'unsure', 'none'];
const MATCH_REASONS = ['replaces', 'box_number', 'po_numbers', 'lines', 'single', 'conflict'];
// result_json.state (v:1). 'restricted' is added by the portal's group gate.
const RESULT_STATES = ['provisional', 'confirmed', 'unmatched', 'in_review', 'replaced', 'restricted'];
const RESULT_VERSION = 1;
// result_json is for one page; the portal trims again with an allow-list.
const RESULT_LINE_LIMIT = 300;
const EXTRACTION_LINE_LIMIT = 300;

const NOTE_MAX = 1000;
const LIST_LIMIT_DEFAULT = 100;
const LIST_LIMIT_MAX = 500;

// Why a submission cannot be imported as it stands. The codes are API surface.
const BLOCKERS = {
    not_submitted: 'The submission is not waiting for review.',
    supplier_not_found: 'The supplier no longer exists.',
    bad_key: 'The file is not where an uploaded packing list should be.',
    bad_type: 'The file is not a PDF, PNG or JPEG.',
    too_large: 'The file is larger than 10 MiB.',
    no_etag: 'The file was never validated by the check.',
    file_gone: 'The file is no longer in the portal’s storage.',
    file_changed: 'The file has changed since it was checked.',
};

const isObj = v => v !== null && typeof v === 'object' && !Array.isArray(v);
const uniq = arr => [...new Set(arr.filter(v => v != null && v !== ''))];
const num = v => (v === null || v === undefined ? null : Number(v));

// ── Files ────────────────────────────────────────────────────────────────────
// Where the imported copy goes in shipping's PO bucket: the staff upload's
// packing-lists/<token>/<name> layout (packing-list-routes.js), the name
// falling back to pl-<submissionId>.<ext> when the sanitised name is empty.
function destKey({ submissionId, filename, contentType, token }) {
    const ext = ALLOWED_TYPES[contentType] || 'bin';
    const name = safeFilename(filename, contentType) || `pl-${submissionId}.${ext}`;
    return `packing-lists/${token}/${name}`;
}

// ── Triggers ─────────────────────────────────────────────────────────────────
/**
 * What invoked the plCheck / plRefresh Lambda.
 *   - EventBridge "Object Created" from the portal's bucket: { source: 'event', key }
 *     (keys that are not pl/<uuid>.<ext>, or another bucket, are ignored)
 *   - a direct invoke { submissionId, action? }: { source: 'direct', submissionId,
 *     action: 'check' | 'refresh' } ('check' unless action is 'refresh')
 * → one of those, or { ignore: reason }.
 */
function parseTrigger(event, { bucket = null } = {}) {
    const e = isObj(event) ? event : {};
    if (e.source === 'aws.s3' || e['detail-type'] === 'Object Created') {
        const d = isObj(e.detail) ? e.detail : {};
        const name = isObj(d.bucket) ? d.bucket.name : null;
        if (bucket && name !== bucket) return { ignore: 'other_bucket' };
        let key = isObj(d.object) ? d.object.key : null;
        if (typeof key === 'string' && !isPlKey(key)) {
            // S3 event keys may be URL-encoded; a valid PL key never needs it.
            try { key = decodeURIComponent(key.replace(/\+/g, ' ')); } catch { /* keep as is */ }
        }
        if (!isPlKey(key)) return { ignore: 'not_a_pl_key' };
        return { source: 'event', key };
    }
    const submissionId = positiveInt(e.submissionId);
    if (!submissionId) return { ignore: 'unrecognised_event' };
    if (e.action !== undefined && e.action !== null && e.action !== 'check' && e.action !== 'refresh') return { ignore: 'unknown_action' };
    return { source: 'direct', submissionId, action: e.action === 'refresh' ? 'refresh' : 'check' };
}

// ── Import ──────────────────────────────────────────────────────────────────
/**
 * Why `row` cannot be imported now.
 *   supplier: { exists } — the row's supplier is still live; null = not looked up.
 *   file: { gone, changed } from the S3 step, when it ran.
 * → blocker codes (keys of BLOCKERS), [] when importable.
 */
function importBlockers({ row, supplier = null, file = null }) {
    const out = [];
    if (!row || row.status !== 'submitted') out.push('not_submitted');
    if (row) {
        if (supplier && !supplier.exists) out.push('supplier_not_found');
        if (!isPlKey(row.s3_key)) out.push('bad_key');
        if (!ALLOWED_TYPES[row.content_type]) out.push('bad_type');
        else if (isPlKey(row.s3_key) && row.s3_key.slice(row.s3_key.lastIndexOf('.') + 1) !== ALLOWED_TYPES[row.content_type]) out.push('bad_type');
        if (row.file_size != null && Number(row.file_size) > MAX_FILE_BYTES) out.push('too_large');
        if (!row.s3_etag) out.push('no_etag');
    }
    if (file && file.gone) out.push('file_gone');
    else if (file && file.changed) out.push('file_changed');
    return [...new Set(out)];
}

// ── The supplier group ──────────────────────────────────────────────────────
/**
 * The supplier ids behind a row: group_supplier_ids as the portal wrote it at
 * create (the set of supplier ids the uploader has access to — the authority;
 * shipping never writes it), sorted and de-duplicated; a legacy row with none
 * is the group of its own supplier_id. → number[] ([] when neither is usable)
 */
function parseGroupSupplierIds(row) {
    const raw = parseJson(row?.group_supplier_ids);
    const ids = (Array.isArray(raw) ? raw : []).map(Number).filter(n => Number.isInteger(n) && n > 0);
    if (!ids.length) {
        const own = Number(row?.supplier_id);
        return Number.isInteger(own) && own > 0 ? [own] : [];
    }
    return [...new Set(ids)].sort((a, b) => a - b);
}

// ── Supplier-safe views ─────────────────────────────────────────────────────
const month = v => (v == null || v === '' ? null : String(v).slice(0, 7));

/**
 * The extraction as the supplier may see it: their own file, read back. The
 * printed totals against the sum of the rows say whether the reading can be
 * trusted (a missed row shows here first). Rows beyond EXTRACTION_LINE_LIMIT
 * are counted, not listed.
 */
function toSupplierExtraction(extracted) {
    const x = parseJson(extracted);
    if (!isObj(x)) return null;
    const lines = Array.isArray(x.lines) ? x.lines.filter(isObj) : [];
    const sumQty = lines.reduce((s, l) => s + (Number(l.quantity) || 0), 0);
    const sumCtn = lines.reduce((s, l) => s + (Number(l.cartons) || 0), 0);
    const printedQty = x.totalQuantity ?? null;
    const printedCtn = x.totalCartons ?? null;
    return {
        rows: lines.length,
        invoiceNumber: x.invoiceNumber ?? null,
        documentDate: x.documentDate ?? null,
        supplierName: x.supplierName ?? null,
        containerNumbers: Array.isArray(x.containerNumbers) ? x.containerNumbers.map(String) : [],
        printed: {
            totalQuantity: printedQty, totalCartons: printedCtn,
            totalGrossWeightKg: x.totalGrossWeightKg ?? null, totalNetWeightKg: x.totalNetWeightKg ?? null, totalCbm: x.totalCbm ?? null,
        },
        sumOfRows: { quantity: sumQty, cartons: sumCtn },
        totalsOk: (printedQty == null || Number(printedQty) === sumQty) && (printedCtn == null || Number(printedCtn) === sumCtn),
        lines: lines.slice(0, EXTRACTION_LINE_LIMIT).map(l => ({
            jfCode: l.jfCode ?? null, poNumber: l.poNumber ?? null, lotNumber: l.lotNumber ?? null,
            mfgDate: l.mfgDate ?? null, expDate: l.expDate ?? null, description: l.description ?? null,
            cartons: l.cartons ?? null, quantity: l.quantity ?? null, unit: l.unit ?? null,
        })),
        linesTruncated: lines.length > EXTRACTION_LINE_LIMIT,
    };
}

/**
 * A comparison (packing-list-check.js buildComparison, pooled over the
 * supplier's documents in the container) reduced to what the supplier may
 * see. Stripped: skippedLines, fingerprint, lineKey, matchMethod, sign-offs
 * except `accepted`, order / allocation / packing-list ids, orderQuantity,
 * order status, the container's draftContainerId and followedFrom.
 * `elsewhere` keeps only placements whose order is in the uploader's group
 * (`groupOrderIds`: the caller re-checked them against the group's names), each as
 * { label, quantity } — a booked container's or draft's name, which suppliers
 * already see on the emailed packing-list page; null for a planned container
 * or unallocated stock ("not yet in a shipment"). Lines beyond
 * RESULT_LINE_LIMIT are counted, not listed.
 */
function toSupplierComparison(comparison, { groupOrderIds = new Set() } = {}) {
    const c = parseJson(comparison);
    if (!isObj(c)) return null;
    const allowed = groupOrderIds instanceof Set ? groupOrderIds : new Set(groupOrderIds || []);
    const lines = Array.isArray(c.lines) ? c.lines : [];
    const summary = isObj(c.summary) ? {
        verdict: c.summary.verdict ?? null,
        lines: num(c.summary.lines), matched: num(c.summary.matched), mismatched: num(c.summary.mismatched),
        missingFromPackingList: num(c.summary.missingFromPackingList), notExpected: num(c.summary.notExpected),
        discrepancyCount: num(c.summary.discrepancyCount), signedOff: num(c.summary.signedOff), outstanding: num(c.summary.outstanding),
        expectedUnits: num(c.summary.expectedUnits), packedUnits: num(c.summary.packedUnits),
        packingListRows: num(c.summary.packingListRows), documents: num(c.summary.documents),
    } : null;
    const extractionCheck = isObj(c.extractionCheck) ? {
        printedTotalQuantity: c.extractionCheck.printedTotalQuantity ?? null, sumOfRowsQuantity: c.extractionCheck.sumOfRowsQuantity ?? null,
        printedTotalCartons: c.extractionCheck.printedTotalCartons ?? null, sumOfRowsCartons: c.extractionCheck.sumOfRowsCartons ?? null,
        ok: c.extractionCheck.ok !== false,
    } : null;
    const documents = (Array.isArray(c.documents) ? c.documents : []).filter(isObj).map(d => ({
        filename: d.filename ?? null, invoiceNumber: d.invoiceNumber ?? null, rows: num(d.rows),
        printedTotalQuantity: d.printedTotalQuantity ?? null, sumOfRowsQuantity: d.sumOfRowsQuantity ?? null,
        printedTotalCartons: d.printedTotalCartons ?? null, sumOfRowsCartons: d.sumOfRowsCartons ?? null,
        ok: d.ok !== false,
    }));
    const duplicateInvoices = (Array.isArray(c.duplicateInvoices) ? c.duplicateInvoices : []).filter(isObj)
        .map(d => ({ invoiceNumber: d.invoiceNumber ?? null, count: Array.isArray(d.packingListIds) ? d.packingListIds.length : 2 }));
    return {
        summary,
        extractionCheck,
        documents,
        duplicateInvoices,
        lines: lines.slice(0, RESULT_LINE_LIMIT).map(l => toSupplierLine(l, allowed)),
        linesTruncated: lines.length > RESULT_LINE_LIMIT,
    };
}

function toSupplierLine(l, allowed) {
    const orders = Array.isArray(l.expected?.orders) ? l.expected.orders : [];
    const rows = Array.isArray(l.packed?.rows) ? l.packed.rows : [];
    const expected = l.expected ? {
        quantity: num(l.expected.quantity),
        cartons: l.expected.cartons ?? null,
        lots: uniq(orders.map(o => o.lotNumber)),
        dates: { mfg: uniq(orders.map(o => month(o.mfgDate))), exp: uniq(orders.map(o => month(o.expDate))) },
    } : null;
    const packed = l.packed ? {
        quantity: num(l.packed.quantity),
        cartons: l.packed.cartons ?? null,
        unit: l.packed.unit ?? null,
        lots: uniq(rows.map(r => r.lotNumber)),
        dates: { mfg: uniq(rows.map(r => month(r.mfgDate))), exp: uniq(rows.map(r => month(r.expDate))) },
    } : null;
    const elsewhere = (Array.isArray(l.elsewhere) ? l.elsewhere : [])
        .filter(e => isObj(e) && e.orderId != null && allowed.has(Number(e.orderId)))
        .map(e => ({
            label: (e.kind === 'booked' || e.kind === 'draft') && e.where ? String(e.where) : null,
            quantity: num(e.quantity),
        }));
    return {
        status: l.status,
        severity: l.severity ?? null,
        accepted: l.accepted === true,
        jfCode: l.jfCode ?? null,
        poNumbers: Array.isArray(l.poNumbers) ? l.poNumbers : [],
        productName: l.productName ?? null,
        expected,
        packed,
        differences: (Array.isArray(l.differences) ? l.differences : []).map(d => ({
            field: d.field, severity: d.severity, expected: d.expected ?? null, actual: d.actual ?? null, message: d.message ?? null,
        })),
        elsewhere,
    };
}

// ── Request parsing ─────────────────────────────────────────────────────────
// GET /pl-submissions query. → { value: { statuses, supplierId, limit, offset } } or { error }
function parseListQuery(q = {}) {
    const status = q.status === undefined || q.status === '' ? 'submitted' : String(q.status);
    let statuses;
    if (status === 'all') statuses = VISIBLE_STATUSES;
    else if (status === 'decided') statuses = [...DECIDED_STATUSES, 'withdrawn'];
    else {
        statuses = status.split(',').map(s => s.trim()).filter(Boolean);
        if (!statuses.length || statuses.some(s => !VISIBLE_STATUSES.includes(s))) {
            return { error: `status must be one of ${VISIBLE_STATUSES.join(', ')} (comma-separated), decided or all.` };
        }
    }
    const value = { statuses: [...new Set(statuses)], supplierId: null, limit: LIST_LIMIT_DEFAULT, offset: 0 };
    if (q.supplierId !== undefined && q.supplierId !== '') {
        const id = positiveInt(q.supplierId);
        if (!id) return { error: 'supplierId must be a positive integer.' };
        value.supplierId = id;
    }
    if (q.limit !== undefined && q.limit !== '') {
        const n = positiveInt(q.limit);
        if (!n) return { error: 'limit must be a positive integer.' };
        value.limit = Math.min(n, LIST_LIMIT_MAX);
    }
    if (q.offset !== undefined && q.offset !== '') {
        const n = Number(q.offset);
        if (!Number.isSafeInteger(n) || n < 0) return { error: 'offset must be a non-negative integer.' };
        value.offset = n;
    }
    return { value };
}

// The container a request names: { containerNumber, draftContainerId }, at
// least one of them. → { value } or { error }
function parseContainerChoice(b) {
    const containerNumber = typeof b.containerNumber === 'string' && b.containerNumber.trim() ? b.containerNumber.trim().slice(0, 100) : null;
    let draftContainerId = null;
    if (b.draftContainerId !== undefined && b.draftContainerId !== null && b.draftContainerId !== '') {
        draftContainerId = positiveInt(b.draftContainerId);
        if (!draftContainerId) return { error: 'draftContainerId must be a positive integer.' };
    }
    if (!containerNumber && !draftContainerId) return { error: 'containerNumber or draftContainerId is required.' };
    return { value: { containerNumber, draftContainerId } };
}

// POST /:id/import body { rowVersion, containerNumber | draftContainerId, replacesId?, force?, note? }.
function parseImportBody(body) {
    const b = isObj(body) ? body : {};
    const rv = parseRowVersion(b.rowVersion);
    if (rv.error) return { error: rv.error };
    const c = parseContainerChoice(b);
    if (c.error) return { error: c.error };
    let replacesId = null;
    if (b.replacesId !== undefined && b.replacesId !== null && b.replacesId !== '') {
        replacesId = positiveInt(b.replacesId);
        if (!replacesId) return { error: 'replacesId must be a packing list id.' };
    }
    if (b.force !== undefined && b.force !== null && typeof b.force !== 'boolean') return { error: 'force must be true or false.' };
    const note = parseNote(b.note);
    if (note.error) return { error: note.error };
    return { value: { rowVersion: rv.value, ...c.value, replacesId, force: b.force === true, note: note.value } };
}

// POST /:id/reject body { rowVersion, note } — the note is required.
function parseRejectBody(body) {
    const b = isObj(body) ? body : {};
    const rv = parseRowVersion(b.rowVersion);
    if (rv.error) return { error: rv.error };
    const note = parseNote(b.note, { required: true });
    if (note.error) return { error: note.error };
    return { value: { rowVersion: rv.value, note: note.value } };
}

// ── JSON ────────────────────────────────────────────────────────────────────
// { kind, containerNumber, draftId, label } from a row's suggested_* or
// confirmed_* columns; null when no container is recorded.
function containerFromColumns(row, prefix) {
    const kind = row[`${prefix}_kind`];
    const label = row[`${prefix}_label`];
    if (!kind && !label) return null;
    return {
        kind: kind || null,
        containerNumber: row[`${prefix}_container_number`] || null,
        draftContainerId: num(row[`${prefix}_draft_id`]),
        label: label || null,
    };
}

/**
 * One submission for the staff API (the list item; the detail adds to it).
 * `row` is a pl_submissions row read with the *_iso columns of the routes'
 * SELECT. extras: { blockers }
 */
function submissionToJson(row, { blockers = [] } = {}) {
    return {
        id: Number(row.id),
        supplierId: Number(row.supplier_id),
        supplierName: row.supplier_name,
        submittedByEmail: row.submitted_by_email,
        filename: row.filename,
        contentType: row.content_type,
        fileSize: num(row.file_size),
        replacesSubmissionId: num(row.replaces_submission_id),
        status: row.status,
        checkStatus: row.check_status,
        errorCode: row.error_code || null,
        rowCount: num(row.row_count),
        invoiceNumber: row.invoice_number || null,
        matchConfidence: row.match_confidence || null,
        matchReason: row.match_reason || null,
        suggested: containerFromColumns(row, 'suggested'),
        confirmed: containerFromColumns(row, 'confirmed'),
        verdict: row.verdict || null,
        discrepancyCount: num(row.discrepancy_count),
        resultState: row.result_state || null,
        resultAt: row.result_at_iso || null,
        supplierNote: row.supplier_note || null,
        warningsAcknowledged: Number(row.warnings_acknowledged) === 1,
        submittedAt: row.submitted_at_iso || null,
        decidedBy: row.decided_by || null,
        decidedAt: row.decided_at_iso || null,
        decidedNote: row.decided_note || null,
        packingListId: num(row.packing_list_id),
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
        rowCount: num(row.row_count),
        invoiceNumber: row.invoice_number || null,
    };
}

module.exports = {
    // PI's rules, shared
    STALE_CHECK_SECONDS,
    MAX_CHECK_ATTEMPTS,
    RETRYABLE_CODES,
    MAX_FILE_BYTES,
    ALLOWED_TYPES,
    CHECK_STATUSES,
    VISIBLE_STATUSES,
    DECIDED_STATUSES,
    DECIDER_ROLES,
    canDecide,
    sniffType,
    safeFilename,
    planEvent,
    claimWhere,
    parseJson,
    positiveInt,
    parseRowVersion,
    parseNote,
    escapeHtml,
    // PL's own
    PL_KEY_RE,
    STATUSES,
    CONFIDENCES,
    MATCH_REASONS,
    RESULT_STATES,
    RESULT_VERSION,
    RESULT_LINE_LIMIT,
    EXTRACTION_LINE_LIMIT,
    NOTE_MAX,
    LIST_LIMIT_DEFAULT,
    LIST_LIMIT_MAX,
    BLOCKERS,
    isPlKey,
    destKey,
    parseTrigger,
    importBlockers,
    parseGroupSupplierIds,
    toSupplierExtraction,
    toSupplierComparison,
    parseListQuery,
    parseContainerChoice,
    parseImportBody,
    parseRejectBody,
    containerFromColumns,
    submissionToJson,
    checkToJson,
};
