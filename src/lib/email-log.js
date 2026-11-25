'use strict';

// COPIED from the supplier portal (portal/src/lib/emailLog.js, README section 26 there), the
// original; JFPRO (api/email-log.js) and Workflows (api/src/lib/emailLog.js) carry copies too.
// The four must stay the same rules: change them together (each repo's tests pin the same
// masking cases).
//
// The email log: one row in jportal.email_log for every email this service hands to SES
// (src/services/email-send.js), with its content, for the portal Support app's Emails page.
//
// Two rules make the table safe to read:
//   - No secret in a link survives. Every http(s) URL in both bodies is masked before the row
//     is written: a #fragment, any query value of 12+ characters (or under a name like token,
//     key, sig, code), and any path segment that looks like a token (20+ URL-safe characters
//     with a digit, mixed case, or no separator) become [redacted]. That covers the Help guest
//     link (#k=...), the artwork and quote-decline tokens, receipt-confirm links and presigned
//     S3 URLs. A long, harmless segment can be masked too; masking too much is the safe side.
//   - Recording NEVER throws and never changes what the caller does: the email has already
//     gone (or not). A failed INSERT is a warning, a missing table one warning per container.

const EMAIL_LOG_APPS = Object.freeze(['portal', 'shipping', 'jfpro', 'workflows']);
const EMAIL_LOG_STATUSES = Object.freeze(['accepted', 'failed', 'unknown']);
const BODY_MAX = 1_000_000;
const MASK = '[redacted]';

const URL_RE = /\bhttps?:\/\/[^\s"'<>]+/gi;
const SENSITIVE_PARAM = /token|key|sig|secret|code|auth|pass|session|credential|amz|^[kt]$/i;
const SAFE_CHARS = /^[A-Za-z0-9_\-.~%]+$/;

function maskSegment(seg) {
    if (seg.length < 20 || !SAFE_CHARS.test(seg)) return seg;
    const digits = /[0-9]/.test(seg);
    const mixed = /[a-z]/.test(seg) && /[A-Z]/.test(seg);
    const separated = /[-_.]/.test(seg);
    return digits || mixed || !separated ? MASK : seg;
}

/** One URL with its secrets masked (see the header). Scheme and host are kept. */
function redactUrl(url) {
    const m = /^(https?:\/\/[^/?#]*)([^?#]*)(\?[^#]*)?(#.*)?$/i.exec(String(url));
    if (!m) return url;
    const [, origin, path, query = '', fragment = ''] = m;
    const maskedPath = path.split('/').map(maskSegment).join('/');
    const maskedQuery = query.replace(/(\?|&amp;|&)([^=&#]*)=([^&#]*)/g, (all, sep, name, value) => (
        value && (value.length >= 12 || SENSITIVE_PARAM.test(name)) ? `${sep}${name}=${MASK}` : all
    ));
    return origin + maskedPath + maskedQuery + (fragment.length > 1 ? `#${MASK}` : fragment);
}

/** Every URL in an HTML or text body masked. null stays null. */
function redactBody(body) {
    if (body === null || body === undefined) return null;
    return String(body).replace(URL_RE, (u) => redactUrl(u));
}

/** The status a sender's outcome is shown as: accepted, failed, or unknown (no answer: it may have gone). */
function statusOf(result) {
    if (result && result.ok) return { status: 'accepted', reason: null };
    const reason = String((result && result.reason) || 'error').slice(0, 64);
    return { status: reason === 'network_error' ? 'unknown' : 'failed', reason };
}

const list = (v) => (Array.isArray(v) ? v : String(v ?? '').split(','))
    .filter((x) => typeof x === 'string').map((x) => x.trim()).filter(Boolean);

const cut = (v, max) => {
    const s = v === null || v === undefined ? '' : String(v);
    return s ? s.slice(0, max) : null;
};

function sizeOf(a) {
    if (Number.isFinite(Number(a.size))) return Number(a.size);
    const b = a.bytes ?? a.content;
    if (b && typeof b.length === 'number') return b.length;
    if (b && typeof b.byteLength === 'number') return b.byteLength;
    return null;
}

/**
 * The column values of one row, in COLUMNS order. Pure. `to` and `replyTo` are a string
 * (comma-separated) or an array; `attachments` are [{ filename, contentType|type, bytes|size }].
 */
function emailLogValues({
    app, kind = null, stage = null, provider = 'ses', from = null, replyTo = null, to, subject,
    html = null, text = null, attachments = [], result,
}) {
    let truncated = false;
    const body = (v) => {
        const r = redactBody(v);
        if (r === null || r === '') return null;
        if (r.length > BODY_MAX) { truncated = true; return r.slice(0, BODY_MAX); }
        return r;
    };
    const files = (Array.isArray(attachments) ? attachments : [])
        .filter((a) => a && (a.filename || a.name))
        .map((a) => ({
            filename: String(a.filename || a.name).slice(0, 255),
            contentType: String(a.contentType || a.type || 'application/octet-stream').slice(0, 127),
            size: sizeOf(a),
        }));
    const { status, reason } = statusOf(result);
    const htmlBody = body(html);
    const textBody = body(text);
    return [
        String(app).slice(0, 16),
        cut(kind, 64),
        cut(stage, 16),
        provider === 'front' ? 'front' : 'ses',
        cut(from, 320),
        cut(list(replyTo).join(', '), 1000),
        list(to).join(', ').slice(0, 10000),
        String(subject ?? '').replace(/[\r\n]+/g, ' ').slice(0, 500),
        htmlBody,
        textBody,
        truncated ? 1 : 0,
        files.length ? JSON.stringify(files) : null,
        files.length,
        status,
        reason,
        cut(result && !result.ok ? result.detail : null, 500),
        cut(result && result.ok ? result.messageId : null, 255),
    ];
}

const COLUMNS = Object.freeze([
    'app', 'kind', 'stage', 'provider', 'from_address', 'reply_to', 'to_addresses', 'subject',
    'html_body', 'text_body', 'body_truncated', 'attachments', 'attachment_count',
    'status', 'reason', 'detail', 'message_id',
]);

/** The INSERT, for `table` (`email_log`, or `jportal`.email_log from another schema). */
function insertSql(table = 'email_log') {
    return `INSERT INTO ${table} (${COLUMNS.join(', ')}) VALUES (${COLUMNS.map(() => '?').join(', ')})`;
}

// MySQL errnos for "the table is not there for this login": unknown database, table or column
// command denied, no such table.
const MISSING = new Set([1049, 1142, 1143, 1146]);
let warnedMissing = false;

/**
 * Writes one row through `query(sql, params)`. NEVER throws. → true when written.
 * `table` as insertSql. `log` is { warn } (default console).
 */
async function recordEmail(query, input, { table = 'email_log', log = console } = {}) {
    try {
        await query(insertSql(table), emailLogValues(input));
        return true;
    } catch (err) {
        if (MISSING.has(err?.errno)) {
            if (!warnedMissing) log.warn('[email-log] the email_log table is not there for this login; emails are not logged', { code: err?.code });
            warnedMissing = true;
        } else {
            log.warn('[email-log] logging an email failed; the email itself is unaffected', { code: err?.code, name: err?.name });
        }
        return false;
    }
}

module.exports = {
    EMAIL_LOG_APPS, EMAIL_LOG_STATUSES, BODY_MAX, MASK, COLUMNS,
    redactUrl, redactBody, statusOf, emailLogValues, insertSql, recordEmail,
};
