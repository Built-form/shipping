'use strict';

// ── Proforma invoices uploaded by suppliers through jportal ──────────────────
// A supplier uploads a PI from a PO's page in the supplier portal (the jportal
// repo). The file goes to the portal's bucket (jportal-pi-uploads-<env>,
// key pi/<uuid>.<pdf|png|jpg>) and a row to jportal.pi_submissions; shipping's
// piCheck Lambda (src/services/pi-check.js) checks it against the PO with the
// same Gemini prompt staff uploads use, and writes the result back to the row.
// The supplier then submits or discards it. Submitted PIs wait for staff, who
// import them (the same rows a staff upload makes: purchase_order_invoices +
// a check + a payment row) or reject them (src/services/pi-submission-routes.js).
//
// Who writes what on jportal.pi_submissions: jportal creates rows and moves
// draft→submitted / draft→discarded / submitted→withdrawn; shipping writes the
// check columns, s3_etag / file_size (piCheck) and submitted→imported /
// submitted→rejected with the decided_* and notify columns. Every status or
// check change bumps row_version.
//
// Pure: no database, no clock (callers pass `now`), no network. Constants
// shared with jportal (its src/lib/piSubmissions.js — keep the two in step;
// shipping's claim is authoritative, the portal's copy only decides which
// buttons to show): STALE_CHECK_SECONDS, MAX_CHECK_ATTEMPTS, RETRYABLE_CODES,
// the key pattern, the allowed types and the 10 MiB limit.

const STALE_CHECK_SECONDS = 600;
const MAX_CHECK_ATTEMPTS = 3;
const RETRYABLE_CODES = ['MODEL_UNAVAILABLE', 'NOT_CONFIGURED', 'MODEL_NO_JSON', 'MODEL_BAD_OUTPUT'];
const PI_KEY_RE = /^pi\/[0-9a-f-]{36}\.(pdf|png|jpg)$/;
const MAX_FILE_BYTES = 10 * 1024 * 1024;
// content type → the extension the key (and the imported copy) carries.
const ALLOWED_TYPES = { 'application/pdf': 'pdf', 'image/png': 'png', 'image/jpeg': 'jpg' };

const CHECK_STATUSES = ['awaiting_upload', 'pending', 'succeeded', 'failed'];
// The statuses staff see. Drafts and discarded rows are the supplier's own business.
const VISIBLE_STATUSES = ['submitted', 'imported', 'rejected', 'withdrawn'];
const DECIDED_STATUSES = ['imported', 'rejected'];

// Admin and standard users decide (import, reject, verify bank details, send
// the email); read-only users only look. The payment sign-off rule
// (src/lib/payment-reviews.js REVIEWER_ROLES).
const DECIDER_ROLES = ['admin', 'standard'];
const canDecide = userType => DECIDER_ROLES.includes(String(userType));

const VERDICTS = ['pass', 'minor_discrepancies', 'major_discrepancies'];
const DISCREPANCY_TYPES = [
    'unit_price_mismatch', 'quantity_mismatch', 'line_total_mismatch', 'missing_on_invoice',
    'extra_on_invoice', 'supplier_mismatch', 'currency_mismatch', 'payment_terms_mismatch',
    'header_total_mismatch', 'other',
];

const TEMPLATE_KEYS = { imported: 'supplier_pi_imported', rejected: 'supplier_pi_rejected' };

const NOTE_MAX = 1000;
const LIST_LIMIT_DEFAULT = 100;
const LIST_LIMIT_MAX = 500;

// Why a submission cannot be imported as it stands. The codes are API surface.
const BLOCKERS = {
    not_submitted: 'The submission is not waiting for review.',
    po_deleted: 'The purchase order has been deleted.',
    po_supplier_changed: 'The purchase order no longer belongs to the supplier who sent this.',
    bad_key: 'The file is not where an uploaded PI should be.',
    bad_type: 'The file is not a PDF, PNG or JPEG.',
    too_large: 'The file is larger than 10 MiB.',
    no_etag: 'The file was never validated by the check.',
    file_gone: 'The file is no longer in the portal’s storage.',
    file_changed: 'The file has changed since it was checked.',
};

const isPiKey = key => typeof key === 'string' && PI_KEY_RE.test(key);

// ── Files ────────────────────────────────────────────────────────────────────
// The type the bytes are, from their first bytes: a PDF has `%PDF-` within its
// first 1024 bytes, PNG and JPEG their signatures. null for anything else.
function sniffType(bytes) {
    if (!bytes || !bytes.length) return null;
    const b = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
    if (b.length >= 8 && b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47
        && b[4] === 0x0d && b[5] === 0x0a && b[6] === 0x1a && b[7] === 0x0a) return 'image/png';
    if (b.length >= 3 && b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) return 'image/jpeg';
    if (b.subarray(0, 1024).indexOf('%PDF-', 0, 'latin1') >= 0) return 'application/pdf';
    return null;
}

// A filename safe for an S3 key and a Content-Disposition header: ASCII
// letters, digits, dot, dash and underscore, the extension forced to the
// type's. '' when nothing usable is left (an all-Chinese name, say).
function safeFilename(name, contentType) {
    const ext = ALLOWED_TYPES[contentType] || null;
    let stem = String(name == null ? '' : name).normalize('NFKC').trim();
    stem = stem.replace(/\.[A-Za-z0-9]{1,5}$/, '');
    stem = stem.replace(/[^A-Za-z0-9._-]+/g, '_').replace(/_{2,}/g, '_').replace(/^[._-]+|[._-]+$/g, '');
    if (!/[A-Za-z0-9]/.test(stem)) return '';
    stem = stem.slice(0, 120);
    return ext ? `${stem}.${ext}` : stem;
}

// Where the imported copy goes in shipping's PO bucket: the staff upload's
// invoices/<token>/<name> layout, the name falling back to
// pi-<submissionId>.<ext> when the sanitised name is empty.
function destKey({ submissionId, filename, contentType, token }) {
    const ext = ALLOWED_TYPES[contentType] || 'bin';
    const name = safeFilename(filename, contentType) || `pi-${submissionId}.${ext}`;
    return `invoices/${token}/${name}`;
}

// ── The check ────────────────────────────────────────────────────────────────
const toMs = v => {
    if (v === null || v === undefined || v === '') return null;
    const d = v instanceof Date ? v : new Date(String(v).replace(' ', 'T') + (/[zZ]|[+-]\d\d:?\d\d$/.test(String(v)) ? '' : 'Z'));
    const t = d.getTime();
    return Number.isNaN(t) ? null : t;
};

// Stale: claimed longer ago than STALE_CHECK_SECONDS. Prefer check_age_seconds
// when the caller's SELECT computed it in SQL (TIMESTAMPDIFF against
// UTC_TIMESTAMP()): the column is UTC and the driver reads DATETIMEs as local.
const isStalePending = (row, now) => {
    if (!row || row.check_status !== 'pending') return false;
    if (row.check_age_seconds !== undefined && row.check_age_seconds !== null) {
        return Number(row.check_age_seconds) > STALE_CHECK_SECONDS;
    }
    const started = toMs(row.check_started_at);
    return started === null || now - started > STALE_CHECK_SECONDS * 1000;
};

/**
 * Whether a trigger may claim the row for a check. `source` is 'event' (the
 * bucket's Object Created event) or 'direct' (an invoke with { submissionId }:
 * jportal's `uploaded` and `recheck`). The claim UPDATE re-checks the same
 * states atomically (claimWhere); this only saves a write that would miss.
 *   event:  only awaiting_upload — the event for an already-claimed upload is a duplicate
 *   direct: awaiting_upload, pending older than STALE_CHECK_SECONDS, or failed
 *           with a code in RETRYABLE_CODES
 * → { claim: boolean, reason }
 */
function planEvent(source, row, now) {
    if (!row) return { claim: false, reason: 'not_found' };
    if (row.status !== 'draft') return { claim: false, reason: 'not_draft' };
    if (Number(row.check_attempt) >= MAX_CHECK_ATTEMPTS) return { claim: false, reason: 'attempts_exhausted' };
    if (row.check_status === 'awaiting_upload') return { claim: true, reason: 'awaiting_upload' };
    if (source !== 'direct') return { claim: false, reason: 'already_claimed' };
    if (isStalePending(row, now)) return { claim: true, reason: 'stale_pending' };
    if (row.check_status === 'failed' && RETRYABLE_CODES.includes(row.error_code)) return { claim: true, reason: 'retryable_failure' };
    return { claim: false, reason: row.check_status === 'pending' ? 'in_progress' : 'not_retryable' };
}

/**
 * What invoked the piCheck Lambda.
 *   - EventBridge "Object Created" from the portal's bucket: { source: 'event', key }
 *     (keys that are not pi/<uuid>.<ext>, or another bucket, are ignored)
 *   - a direct invoke { submissionId }: { source: 'direct', submissionId }
 * → one of those, or { ignore: reason }.
 */
function parseTrigger(event, { bucket = null } = {}) {
    const e = isObj(event) ? event : {};
    if (e.source === 'aws.s3' || e['detail-type'] === 'Object Created') {
        const d = isObj(e.detail) ? e.detail : {};
        const name = isObj(d.bucket) ? d.bucket.name : null;
        if (bucket && name !== bucket) return { ignore: 'other_bucket' };
        let key = isObj(d.object) ? d.object.key : null;
        if (typeof key === 'string' && !isPiKey(key)) {
            // S3 event keys may be URL-encoded; a valid PI key never needs it.
            try { key = decodeURIComponent(key.replace(/\+/g, ' ')); } catch { /* keep as is */ }
        }
        if (!isPiKey(key)) return { ignore: 'not_a_pi_key' };
        return { source: 'event', key };
    }
    const submissionId = positiveInt(e.submissionId);
    if (submissionId) return { source: 'direct', submissionId };
    return { ignore: 'unrecognised_event' };
}

// The guard on the claim UPDATE, for planEvent's states (SQL on the
// pi_submissions row, UTC). → { sql, params }
function claimWhere(source) {
    const states = [`check_status = 'awaiting_upload'`];
    const params = [];
    if (source === 'direct') {
        states.push(`(check_status = 'pending' AND (check_started_at IS NULL OR check_started_at < UTC_TIMESTAMP() - INTERVAL ${STALE_CHECK_SECONDS} SECOND))`);
        states.push(`(check_status = 'failed' AND error_code IN (${RETRYABLE_CODES.map(() => '?').join(', ')}))`);
        params.push(...RETRYABLE_CODES);
    }
    return {
        sql: `status = 'draft' AND check_attempt < ${MAX_CHECK_ATTEMPTS} AND (${states.join(' OR ')})`,
        params,
    };
}

// A JSON column as mysql2 returns it (object, or a string on some servers).
function parseJson(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string') {
        try { return JSON.parse(value); } catch { return null; }
    }
    return value;
}

const isObj = v => v !== null && typeof v === 'object' && !Array.isArray(v);

/**
 * A shape check of a check result (po-invoice-check.js RESPONSE_SCHEMA), not a
 * full schema validation: the parts the import copies and the pages read must
 * be there. Recomputes the verdict and the discrepancy count from the result
 * itself. → { ok: true, verdict, discrepancyCount } or { ok: false, reason }.
 */
function validateCheckResult(result) {
    const r = parseJson(result);
    if (!isObj(r)) return { ok: false, reason: 'not_an_object' };
    if (!VERDICTS.includes(r.overallVerdict)) return { ok: false, reason: 'bad_verdict' };
    if (!Array.isArray(r.discrepancies)) return { ok: false, reason: 'no_discrepancies' };
    for (const d of r.discrepancies) {
        if (!isObj(d) || !DISCREPANCY_TYPES.includes(d.type)) return { ok: false, reason: 'bad_discrepancy' };
    }
    for (const part of ['summary', 'header', 'totals']) {
        if (!isObj(r[part])) return { ok: false, reason: `no_${part}` };
    }
    if (r.paymentTerms !== undefined && r.paymentTerms !== null && !isObj(r.paymentTerms)) return { ok: false, reason: 'bad_payment_terms' };
    if (r.otherCharges !== undefined && r.otherCharges !== null && !Array.isArray(r.otherCharges)) return { ok: false, reason: 'bad_other_charges' };
    return { ok: true, verdict: r.overallVerdict, discrepancyCount: r.discrepancies.length };
}

/**
 * Whether the supplier's check can be relied on at import (copied as the
 * invoice's check, its bank details compared). Not when it never succeeded
 * (the supplier submitted anyway), or its stored result is malformed or
 * disagrees with the row's verdict / count.
 * → { usable, reason|null, result|null }
 */
function checkUsability(row) {
    if (!row || row.check_status !== 'succeeded') {
        return { usable: false, reason: row && row.check_status === 'failed' ? 'check_failed' : 'check_not_finished', result: null };
    }
    const result = parseJson(row.result_json);
    const v = validateCheckResult(result);
    if (!v.ok) return { usable: false, reason: `bad_result:${v.reason}`, result: null };
    if ((row.verdict != null && row.verdict !== v.verdict)
        || (row.discrepancy_count != null && Number(row.discrepancy_count) !== v.discrepancyCount)) {
        return { usable: false, reason: 'verdict_mismatch', result: null };
    }
    return { usable: true, reason: null, result };
}

// ── Import ──────────────────────────────────────────────────────────────────
/**
 * Why `row` cannot be imported now.
 *   po: { exists, owned } — the PO is live, and its supplier is still exactly
 *       the row's supplier_name (the caller's SQL); null = not looked up.
 *   file: { gone, changed } from the S3 step, when it ran.
 * → blocker codes (keys of BLOCKERS), [] when importable.
 */
function importBlockers({ row, po = null, file = null }) {
    const out = [];
    if (!row || row.status !== 'submitted') out.push('not_submitted');
    if (row) {
        if (po && !po.exists) out.push('po_deleted');
        else if (po && !po.owned) out.push('po_supplier_changed');
        if (!isPiKey(row.s3_key)) out.push('bad_key');
        if (!ALLOWED_TYPES[row.content_type]) out.push('bad_type');
        else if (isPiKey(row.s3_key) && row.s3_key.slice(row.s3_key.lastIndexOf('.') + 1) !== ALLOWED_TYPES[row.content_type]) out.push('bad_type');
        if (row.file_size != null && Number(row.file_size) > MAX_FILE_BYTES) out.push('too_large');
        if (!row.s3_etag) out.push('no_etag');
    }
    if (file && file.gone) out.push('file_gone');
    else if (file && file.changed) out.push('file_changed');
    return [...new Set(out)];
}

// ── PO diff ─────────────────────────────────────────────────────────────────
const HEADER_FIELDS = ['poNumber', 'supplier', 'buyer', 'currency', 'notes', 'subtotal', 'shippingTotal', 'grandTotal'];
const LINE_FIELDS = ['description', 'quantity', 'unitPrice', 'lineTotal'];

const normValue = v => {
    if (v === undefined || v === null || v === '') return null;
    if (typeof v === 'number') return Math.round(v * 10000) / 10000;
    if (typeof v === 'string' && v.trim() !== '' && /^-?\d+(\.\d+)?$/.test(v.trim())) return Math.round(Number(v) * 10000) / 10000;
    return typeof v === 'string' ? v.trim() : v;
};

// Lines keyed by SKU (or description when there is none); a key seen again
// gets #2, #3… in PO order, so a SKU at two prices still pairs up.
function keyedLines(po) {
    const out = new Map();
    const seen = new Map();
    for (const l of (po && Array.isArray(po.lines) ? po.lines : [])) {
        const base = (l && (l.sku || l.description)) ? String(l.sku || l.description).trim() : '(no sku)';
        const n = (seen.get(base) || 0) + 1;
        seen.set(base, n);
        out.set(n === 1 ? base : `${base}#${n}`, l);
    }
    return out;
}

/**
 * The PO as the supplier's check saw it (po_snapshot_json) against the PO now
 * (loadPoForCheck). → { changed, comparable, header: [{ field, then, now }],
 *   lines: { added: Line[], removed: Line[], changed: [{ key, field, then, now }] } }
 * comparable is false when either side is missing (the check never loaded the
 * PO, or the PO is gone); changed is then false.
 */
function diffPo(thenPo, nowPo) {
    const out = { changed: false, comparable: false, header: [], lines: { added: [], removed: [], changed: [] } };
    const a = parseJson(thenPo);
    // A JSON round trip, so a live Date compares as the snapshot stored it.
    const b = nowPo ? JSON.parse(JSON.stringify(nowPo)) : null;
    if (!isObj(a) || !isObj(b)) return out;
    out.comparable = true;
    for (const field of HEADER_FIELDS) {
        if (normValue(a[field]) !== normValue(b[field])) out.header.push({ field, then: a[field] ?? null, now: b[field] ?? null });
    }
    const la = keyedLines(a);
    const lb = keyedLines(b);
    for (const [key, line] of la) {
        if (!lb.has(key)) { out.lines.removed.push(line); continue; }
        const other = lb.get(key);
        for (const field of LINE_FIELDS) {
            if (normValue(line[field]) !== normValue(other[field])) out.lines.changed.push({ key, field, then: line[field] ?? null, now: other[field] ?? null });
        }
    }
    for (const [key, line] of lb) if (!la.has(key)) out.lines.added.push(line);
    out.changed = out.header.length > 0 || out.lines.added.length > 0 || out.lines.removed.length > 0 || out.lines.changed.length > 0;
    return out;
}

// ── Request parsing ─────────────────────────────────────────────────────────
// A positive id from a query string or a JSON body, or null.
function positiveInt(raw) {
    const n = typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : raw;
    return Number.isSafeInteger(n) && n > 0 ? n : null;
}

// → { value } or { error }
function parseRowVersion(raw) {
    if (raw === undefined || raw === null || raw === '') return { error: 'rowVersion is required: the version of the submission you were looking at.' };
    const n = Number(raw);
    if (!Number.isInteger(n) || n < 0) return { error: 'rowVersion must be a non-negative integer.' };
    return { value: n };
}

function parseNote(raw, { required = false } = {}) {
    if (raw === undefined || raw === null || raw === '') {
        return required ? { error: 'note is required: tell the supplier why (they will see it).' } : { value: null };
    }
    if (typeof raw !== 'string') return { error: 'note must be a string.' };
    const note = raw.trim();
    if (!note && required) return { error: 'note is required: tell the supplier why (they will see it).' };
    if (note.length > NOTE_MAX) return { error: `note must be at most ${NOTE_MAX} characters.` };
    return { value: note || null };
}

// GET /pi-submissions query. → { value: { statuses, supplierId, purchaseOrderId, limit, offset } } or { error }
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
    const value = { statuses: [...new Set(statuses)], supplierId: null, purchaseOrderId: null, limit: LIST_LIMIT_DEFAULT, offset: 0 };
    for (const key of ['supplierId', 'purchaseOrderId']) {
        if (q[key] === undefined || q[key] === '') continue;
        const id = positiveInt(q[key]);
        if (!id) return { error: `${key} must be a positive integer.` };
        value[key] = id;
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

// POST /:id/import body { rowVersion, note?, recheck?, bankVerification? }.
// bankVerification is parsed by the caller (src/lib/bank-verification.js
// parseVerification), which knows whether it may be sent.
function parseImportBody(body) {
    const b = isObj(body) ? body : {};
    const rv = parseRowVersion(b.rowVersion);
    if (rv.error) return { error: rv.error };
    const note = parseNote(b.note);
    if (note.error) return { error: note.error };
    if (b.recheck !== undefined && b.recheck !== null && typeof b.recheck !== 'boolean') return { error: 'recheck must be true or false.' };
    const hasBank = b.bankVerification !== undefined && b.bankVerification !== null;
    return { value: { rowVersion: rv.value, note: note.value, recheck: b.recheck === true, bankVerification: hasBank ? b.bankVerification : null } };
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

// POST /:id/notify body { force? }.
function parseNotifyBody(body) {
    const b = isObj(body) ? body : {};
    if (b.force !== undefined && b.force !== null && typeof b.force !== 'boolean') return { error: 'force must be true or false.' };
    return { value: { force: b.force === true } };
}

// ── JSON ────────────────────────────────────────────────────────────────────
const num = v => (v === null || v === undefined ? null : Number(v));

/**
 * One submission for the API (the list item; the detail adds to it). `row` is
 * a pi_submissions row read with the *_iso columns of the routes' SELECT.
 *   extras: { poChanged, bankState, blockers }
 */
function submissionToJson(row, { poChanged = false, bankState = 'not_checked', blockers = [] } = {}) {
    return {
        id: Number(row.id),
        purchaseOrderId: Number(row.purchase_order_id),
        poNumber: row.po_number || null,
        supplierId: Number(row.supplier_id),
        supplierName: row.supplier_name,
        filename: row.filename,
        contentType: row.content_type,
        fileSize: num(row.file_size),
        status: row.status,
        submittedByEmail: row.submitted_by_email,
        submittedAt: row.submitted_at_iso || null,
        supplierNote: row.supplier_note || null,
        warningsAcknowledged: Number(row.warnings_acknowledged) === 1,
        checkStatus: row.check_status,
        verdict: row.verdict || null,
        discrepancyCount: num(row.discrepancy_count),
        decidedBy: row.decided_by || null,
        decidedAt: row.decided_at_iso || null,
        decidedNote: row.decided_note || null,
        importedInvoiceId: num(row.imported_invoice_id),
        notifiedAt: row.notified_at_iso || null,
        notifyError: row.notify_error || null,
        rowVersion: Number(row.row_version),
        poChanged: Boolean(poChanged),
        bankCheck: { state: bankState },
        blockers,
    };
}

// The check part of the detail.
function checkToJson(row, usability) {
    return {
        status: row.check_status,
        verdict: row.verdict || null,
        discrepancyCount: num(row.discrepancy_count),
        result: parseJson(row.result_json),
        modelUsed: row.model_used || null,
        inputTokens: num(row.input_tokens),
        outputTokens: num(row.output_tokens),
        totalTokens: num(row.total_tokens),
        errorCode: row.error_code || null,
        errorMessage: row.error_message || null,
        checkedAt: row.checked_at_iso || null,
        attempts: Number(row.check_attempt) || 0,
        usable: Boolean(usability && usability.usable),
        unusableReason: usability && !usability.usable ? usability.reason : null,
    };
}

// ── Email ───────────────────────────────────────────────────────────────────
const escapeHtml = s => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

const templateKeyFor = status => TEMPLATE_KEYS[status] || null;

/**
 * The tokens for the decision email (templates supplier_pi_imported /
 * supplier_pi_rejected). Everything in `body` is HTML-escaped (the supplier
 * chose the filename, staff wrote the note); `subject` is plain text.
 *   row: the pi_submissions row. status: 'imported' | 'rejected'.
 *   note: decided_note. portalUrl: SUPPLIER_PORTAL_URL (no trailing slash needed).
 * Body tokens: {poNumber} {filename} {note} {noteHtml} {portalLinkHtml}.
 */
function buildEmailVars({ row, status, note, portalUrl }) {
    const poNumber = String(row.po_number || `PO ${row.purchase_order_id}`).replace(/[\r\n]+/g, ' ');
    const text = note ? String(note) : '';
    const base = portalUrl ? String(portalUrl).replace(/\/+$/, '') : '';
    let portalLinkHtml = '';
    if (base) {
        const href = status === 'rejected'
            ? `${base}/orders/${encodeURIComponent(row.purchase_order_id)}/proforma`
            : `${base}/proforma-invoices/${encodeURIComponent(row.id)}`;
        const label = status === 'rejected'
            ? 'Upload a corrected PI / 上传更正后的形式发票'
            : 'View in the supplier portal / 在供应商门户中查看';
        portalLinkHtml = `<p><a href="${escapeHtml(href)}">${label}</a></p>`;
    }
    return {
        subject: { poNumber },
        body: {
            poNumber: escapeHtml(poNumber),
            filename: escapeHtml(row.filename || ''),
            note: escapeHtml(text),
            noteHtml: text
                ? `<p><strong>Note from our team / 我方备注：</strong><br>${escapeHtml(text).replace(/\r?\n/g, '<br>')}</p>`
                : '',
            portalLinkHtml,
        },
    };
}

module.exports = {
    STALE_CHECK_SECONDS,
    MAX_CHECK_ATTEMPTS,
    RETRYABLE_CODES,
    PI_KEY_RE,
    MAX_FILE_BYTES,
    ALLOWED_TYPES,
    CHECK_STATUSES,
    VISIBLE_STATUSES,
    DECIDED_STATUSES,
    DECIDER_ROLES,
    VERDICTS,
    DISCREPANCY_TYPES,
    TEMPLATE_KEYS,
    BLOCKERS,
    canDecide,
    isPiKey,
    sniffType,
    safeFilename,
    destKey,
    parseTrigger,
    planEvent,
    claimWhere,
    parseJson,
    validateCheckResult,
    checkUsability,
    importBlockers,
    diffPo,
    positiveInt,
    parseRowVersion,
    parseNote,
    parseListQuery,
    parseImportBody,
    parseRejectBody,
    parseNotifyBody,
    submissionToJson,
    checkToJson,
    escapeHtml,
    templateKeyFor,
    buildEmailVars,
};
