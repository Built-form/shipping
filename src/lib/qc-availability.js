'use strict';

// QC visit availability links — the pure rules (docs/qc-availability.md).
//
// Staff send the factory and the inspector a no-login link into the supplier
// portal (jportal) where each ticks the whole China days they could attend.
// The request rows live in jportal's schema (<jportal>.qc_availability_requests,
// owned and migrated by jportal); shipping inserts them, revokes them and
// moves their proposed date, and reads the answers back. This module holds
// everything about them that needs no database: input validation, the link
// URL and token, the request state (the same precedence jportal's handler
// and ShipLine's panel use), the side-by-side collation of both parties'
// answers, how open links follow the visit's lifecycle, and the email
// fallbacks. The SQL is in src/services/qc-availability-routes.js.
//
// Dates are China calendar days ('YYYY-MM-DD'); "today" is always
// chinaToday() from src/lib/qc-visits.js, never the Lambda's UTC day.

const crypto = require('crypto');

// isYmd / ymdOf as in src/lib/qc-visits.js (kept local so this module has no
// dependency on the visit rules).
const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
function isYmd(v) {
    const m = typeof v === 'string' ? DATE_RE.exec(v) : null;
    if (!m) return false;
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    return d.getUTCFullYear() === Number(m[1]) && d.getUTCMonth() === Number(m[2]) - 1 && d.getUTCDate() === Number(m[3]);
}
function ymdOf(v) {
    if (v == null || v === '') return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString().slice(0, 10);
    const s = String(v);
    return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : null;
}

const PARTIES = ['factory', 'inspector'];
const LANGS = ['en', 'zh'];
const STATES = ['open', 'answered', 'expired', 'revoked'];
const REVOKE_REASONS = ['staff', 'superseded', 'date_moved', 'confirmed', 'completed', 'cancelled'];
// A window is at most 31 tickable days (start and end included).
const MAX_WINDOW_DAYS = 31;
// The default window: the proposed date and the next 5 days.
const DEFAULT_WINDOW_EXTRA_DAYS = 5;
const DEFAULT_EXPIRES_DAYS = 30;
const MAX_EXPIRES_DAYS = 90;
const NAME_MAX = 255;
const EMAIL_MAX = 255;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
// The visit status a lifecycle move revokes open links with.
const STATUS_REVOKE_REASON = { CONFIRMED: 'confirmed', COMPLETED: 'completed', CANCELLED: 'cancelled' };
// Shown in an email preview when sending would create the link.
const AVAILABILITY_PLACEHOLDER = '(availability link will be created when sent)';

// ── dates ───────────────────────────────────────────────────────────────────

function ymdToUtcMs(ymd) {
    const [y, m, d] = String(ymd).split('-').map(Number);
    return Date.UTC(y, m - 1, d);
}

// 'YYYY-MM-DD' + n calendar days (UTC arithmetic: no DST can drop a day).
function addDaysYmd(ymd, n) {
    return new Date(ymdToUtcMs(ymd) + Number(n) * 86400000).toISOString().slice(0, 10);
}

// Whole days from a to b ('YYYY-MM-DD'); negative when b is before a.
function daysBetween(a, b) {
    return Math.round((ymdToUtcMs(b) - ymdToUtcMs(a)) / 86400000);
}

// Every day from start to end, both included.
function daysInRange(start, end) {
    const out = [];
    if (!isYmd(start) || !isYmd(end) || end < start) return out;
    for (let d = start; d <= end; d = addDaysYmd(d, 1)) out.push(d);
    return out;
}

// A TIMESTAMP as ISO 8601 UTC (Date from mysql2, or a 'YYYY-MM-DD HH:MM:SS'
// string, which is UTC in this app), or null.
function isoTs(v) {
    if (v == null || v === '') return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString();
    const s = String(v);
    const t = Date.parse(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(s) ? `${s.replace(' ', 'T')}Z` : s);
    return Number.isNaN(t) ? null : new Date(t).toISOString();
}

const msOf = (v) => {
    const s = isoTs(v);
    return s ? Date.parse(s) : NaN;
};

const nowMs = now => (now instanceof Date ? now.getTime() : (now == null ? Date.now() : new Date(now).getTime()));

// available_days as a sorted, de-duplicated array of 'YYYY-MM-DD' (the JSON
// column arrives parsed from mysql2, or as text), or null when unset.
function parseDays(v) {
    if (v == null) return null;
    let arr = v;
    if (Buffer.isBuffer(arr)) arr = arr.toString('utf8');
    if (typeof arr === 'string') {
        try { arr = JSON.parse(arr); } catch { return null; }
    }
    if (!Array.isArray(arr)) return null;
    return [...new Set(arr.filter(d => typeof d === 'string' && isYmd(d)))].sort();
}

const flag = v => v === true || v === 1 || v === '1' || (Buffer.isBuffer(v) && v[0] === 1);

// ── window and input ────────────────────────────────────────────────────────

function defaultWindow(visitDate) {
    const start = ymdOf(visitDate);
    if (!start) return null;
    return { start, end: addDaysYmd(start, DEFAULT_WINDOW_EXTRA_DAYS) };
}

const defaultLang = party => (party === 'factory' ? 'zh' : 'en');

const bad = (field, message) => ({ error: { code: 'BAD_FIELD', field, message } });

const blank = v => v == null || (typeof v === 'string' && v.trim() === '');

/**
 * POST /qc-visits/:id/availability-links body → the columns to insert, or
 * { error: { code: 'BAD_FIELD', field, message } }.
 *   body: { party, windowStartDate?, windowEndDate?, recipientName?,
 *           recipientEmail?, preferredLang?, expiresInDays? }
 *   visit: the qc_visits row (visit_date is the proposed date; the route has
 *          already refused a visit without one).
 *   today: the China day (chinaToday()).
 * Missing window ends default to the visit date and visit date + 5; a
 * missing language defaults to zh for the factory, en for the inspector.
 */
function validateLinkInput(body, visit, today) {
    const b = body && typeof body === 'object' ? body : {};
    const party = typeof b.party === 'string' ? b.party.trim().toLowerCase() : b.party;
    if (!PARTIES.includes(party)) return bad('party', 'party must be factory or inspector.');

    const proposed = ymdOf(visit && visit.visit_date);
    if (!proposed) return bad('visitDate', 'Set the visit date first.');
    const def = defaultWindow(proposed);

    const start = blank(b.windowStartDate) ? def.start : b.windowStartDate;
    const end = blank(b.windowEndDate) ? def.end : b.windowEndDate;
    if (!isYmd(start)) return bad('windowStartDate', 'windowStartDate must be a real date, YYYY-MM-DD.');
    if (!isYmd(end)) return bad('windowEndDate', 'windowEndDate must be a real date, YYYY-MM-DD.');
    if (end < start) return bad('windowEndDate', 'windowEndDate must be on or after windowStartDate.');
    if (isYmd(today) && end < today) return bad('windowEndDate', 'The window has already ended (China time).');
    if (daysBetween(start, end) + 1 > MAX_WINDOW_DAYS) {
        return bad('windowEndDate', `The window can be at most ${MAX_WINDOW_DAYS} days.`);
    }
    if (proposed < start || proposed > end) {
        return bad('windowStartDate', `The window must include the proposed date (${proposed}).`);
    }

    let recipientName = null;
    if (!blank(b.recipientName)) {
        if (typeof b.recipientName !== 'string') return bad('recipientName', 'recipientName must be text.');
        recipientName = b.recipientName.trim().replace(/\s+/g, ' ');
        if (recipientName.length > NAME_MAX) return bad('recipientName', `recipientName can be at most ${NAME_MAX} characters.`);
    }
    let recipientEmail = null;
    if (!blank(b.recipientEmail)) {
        if (typeof b.recipientEmail !== 'string' || !EMAIL_RE.test(b.recipientEmail.trim())
            || b.recipientEmail.trim().length > EMAIL_MAX) {
            return bad('recipientEmail', 'recipientEmail must be an email address.');
        }
        recipientEmail = b.recipientEmail.trim();
    }

    let preferredLang;
    if (b.preferredLang === undefined) preferredLang = defaultLang(party);
    else if (b.preferredLang === null || b.preferredLang === '') preferredLang = null;
    else if (LANGS.includes(b.preferredLang)) preferredLang = b.preferredLang;
    else return bad('preferredLang', 'preferredLang must be en, zh or null.');

    let expiresInDays = DEFAULT_EXPIRES_DAYS;
    if (!blank(b.expiresInDays)) {
        const n = Number(b.expiresInDays);
        if (!Number.isInteger(n) || n < 1 || n > MAX_EXPIRES_DAYS) {
            return bad('expiresInDays', `expiresInDays must be a whole number from 1 to ${MAX_EXPIRES_DAYS}.`);
        }
        expiresInDays = n;
    }

    return {
        value: {
            party,
            window_start_date: start,
            window_end_date: end,
            proposed_date: proposed,
            recipient_name: recipientName,
            recipient_email: recipientEmail,
            preferred_lang: preferredLang,
            expiresInDays,
        },
    };
}

// ── token, URL, state, JSON ─────────────────────────────────────────────────

// 5 lower-case base36 characters (~60 million codes), short enough to type
// from a phone. Lower case only: the token column's collation is case-insensitive.
// No '.', so the portal's CloudFront SPA rewrite serves the page. Older links
// carry 48 hex characters; jportal accepts both.
const TOKEN_ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';
const TOKEN_LENGTH = 5;
function newToken() {
    let out = '';
    while (out.length < TOKEN_LENGTH) {
        for (const b of crypto.randomBytes(TOKEN_LENGTH)) {
            // 252 = 7 * 36: reject the top bytes so every character is uniform.
            if (b < 252 && out.length < TOKEN_LENGTH) out += TOKEN_ALPHABET[b % 36];
        }
    }
    return out;
}

// `/a/<token>`: the short form of jportal's `/availability/<token>` page.
function linkUrl(portalUrl, token) {
    const base = String(portalUrl || '').trim().replace(/\/+$/, '');
    if (!base || !token) return null;
    return `${base}/a/${token}`;
}

/**
 * revoked > expired > answered > open. Only open and answered can still be
 * answered; an expired link keeps (and reports) its saved answer.
 */
function requestState(row, now) {
    if (!row) return null;
    if (row.revoked_at != null) return 'revoked';
    const exp = msOf(row.expires_at);
    // Fail closed like the portal: an unreadable expiry counts as expired.
    if (Number.isNaN(exp) || exp <= nowMs(now)) return 'expired';
    if (row.answered_at != null) return 'answered';
    return 'open';
}

const isLive = (row, now) => {
    const s = requestState(row, now);
    return s === 'open' || s === 'answered';
};

function linkToJson(row, { portalUrl, now } = {}) {
    const state = requestState(row, now);
    const answered = row.answered_at != null;
    return {
        id: Number(row.id),
        party: row.party,
        url: state === 'open' || state === 'answered' ? linkUrl(portalUrl, row.token) : null,
        state,
        revokeReason: row.revoke_reason || null,
        recipientName: row.recipient_name || null,
        windowStartDate: ymdOf(row.window_start_date),
        windowEndDate: ymdOf(row.window_end_date),
        proposedDate: ymdOf(row.proposed_date),
        preferredLang: row.preferred_lang || null,
        expiresAt: isoTs(row.expires_at),
        revokedAt: isoTs(row.revoked_at),
        createdByEmail: row.created_by_email || null,
        createdAt: isoTs(row.created_at),
        availableDays: answered ? (parseDays(row.available_days) || []) : parseDays(row.available_days),
        noneAvailable: flag(row.none_available),
        answeredByName: row.answered_by_name || null,
        answerNote: row.answer_note || null,
        answeredAt: isoTs(row.answered_at),
        answerCount: Number(row.answer_count) || 0,
    };
}

// Newest first: created_at, then id.
function newestFirst(a, b) {
    const d = (msOf(b.created_at) || 0) - (msOf(a.created_at) || 0);
    return d !== 0 ? d : Number(b.id) - Number(a.id);
}

/**
 * Both parties' answers side by side. Per party, the newest request that is
 * not revoked (an expired one still counts: its answer is real data).
 *   → { days: [{ date, factory, inspector, both }], parties: { factory, inspector } }
 * days is the union of the two windows, sorted. A cell is true (ticked),
 * false (answered, not ticked, or "none available"), or null (that party has
 * no answer yet, or the day is outside its window). both: both true.
 */
function collate(rows, now) {
    const parties = { factory: null, inspector: null };
    const chosen = {};
    for (const p of PARTIES) {
        const mine = (rows || []).filter(r => r.party === p && r.revoked_at == null).sort(newestFirst);
        if (!mine.length) continue;
        const r = mine[0];
        chosen[p] = r;
        const answered = r.answered_at != null;
        parties[p] = {
            linkId: Number(r.id),
            state: requestState(r, now),
            revokeReason: r.revoke_reason || null,
            windowStartDate: ymdOf(r.window_start_date),
            windowEndDate: ymdOf(r.window_end_date),
            availableDays: answered ? (parseDays(r.available_days) || []) : null,
            noneAvailable: flag(r.none_available),
            answeredAt: isoTs(r.answered_at),
            answeredByName: r.answered_by_name || null,
            answerNote: r.answer_note || null,
            expiresAt: isoTs(r.expires_at),
        };
    }
    const all = new Set();
    for (const p of PARTIES) {
        if (parties[p]) for (const d of daysInRange(parties[p].windowStartDate, parties[p].windowEndDate)) all.add(d);
    }
    const cell = (p, date) => {
        const s = parties[p];
        if (!s || s.answeredAt == null) return null;
        if (date < s.windowStartDate || date > s.windowEndDate) return null;
        if (s.noneAvailable) return false;
        return s.availableDays.includes(date);
    };
    const days = [...all].sort().map((date) => {
        const factory = cell('factory', date);
        const inspector = cell('inspector', date);
        return { date, factory, inspector, both: factory === true && inspector === true };
    });
    return { days, parties };
}

// ── lifecycle ───────────────────────────────────────────────────────────────

// { status, visitDate } from a qc_visits row or an already-camelCase object.
function visitState(v) {
    if (!v) return { status: null, visitDate: null };
    return {
        status: v.status == null ? null : String(v.status),
        visitDate: ymdOf(v.visit_date !== undefined ? v.visit_date : v.visitDate),
    };
}

// Did the status or the visit date change? (The only changes links follow.)
function visitChanged(before, after) {
    const b = visitState(before);
    const a = visitState(after);
    return a.status !== b.status || a.visitDate !== b.visitDate;
}

/**
 * What the visit's change means for its links. Revokes apply to every link
 * not yet revoked, expired ones included (an expired link's saved answer
 * still counts in collate, so it must stop counting once the visit moves on);
 * the proposed_date move applies to live (open / answered) links only.
 * Already-revoked links are left as they are.
 *   links:  request rows of the visit
 *   change: { before, after } — each a qc_visits row or { status, visitDate }
 * → [{ id, action: 'revoke', reason } | { id, action: 'move', proposedDate }]
 * Decisions (plan §5): a move into CONFIRMED / COMPLETED / CANCELLED revokes
 * with that reason; a new visit date outside a link's window revokes it
 * ('date_moved'); a new date inside it moves the page's proposed badge.
 */
function linkSyncPatch(links, { before, after } = {}, now) {
    const b = visitState(before);
    const a = visitState(after);
    const statusChanged = a.status !== b.status;
    const dateChanged = a.visitDate !== b.visitDate;
    const out = [];
    if (!statusChanged && !dateChanged) return out;
    const statusReason = statusChanged ? STATUS_REVOKE_REASON[a.status] || null : null;
    for (const l of links || []) {
        if (l.revoked_at != null) continue;
        const id = Number(l.id);
        if (statusReason) {
            out.push({ id, action: 'revoke', reason: statusReason });
            continue;
        }
        if (!dateChanged) continue;
        const start = ymdOf(l.window_start_date);
        const end = ymdOf(l.window_end_date);
        if (!a.visitDate || a.visitDate < start || a.visitDate > end) {
            out.push({ id, action: 'revoke', reason: 'date_moved' });
        } else if (isLive(l, now) && ymdOf(l.proposed_date) !== a.visitDate) {
            out.push({ id, action: 'move', proposedDate: a.visitDate });
        }
    }
    return out;
}

// ── email ───────────────────────────────────────────────────────────────────

function esc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

const INTRO = {
    en: 'Please tick the days you could attend (China time) here:',
    zh: '请在此勾选您可以安排的日期（中国时间）：',
};

// The lead-in for an audience: English and Chinese for the factory, English
// for the inspector.
const introText = audience => (audience === 'factory' ? `${INTRO.en} / ${INTRO.zh}` : INTRO.en);

// The paragraph the {availabilityLink} body token renders to (and the HTML
// fallback appends). `url` null → the preview placeholder.
function availabilityHtml(url, audience) {
    const target = url
        ? `<a href="${esc(url)}">${esc(url)}</a>`
        : `<em>${esc(AVAILABILITY_PLACEHOLDER)}</em>`;
    const intro = audience === 'factory'
        ? `${esc(INTRO.en)}<br>${esc(INTRO.zh)}`
        : esc(INTRO.en);
    return `<p>${intro}<br>${target}</p>`;
}

function availabilityText(url, audience) {
    return `${introText(audience)} ${url || AVAILABILITY_PLACEHOLDER}`;
}

// The {availabilityLink} token's values: { text, html }; both '' when the
// email carries no link.
function availabilityVars(url, audience, { placeholder = false } = {}) {
    if (!url && !placeholder) return { text: '', html: '' };
    return { text: url || AVAILABILITY_PLACEHOLDER, html: availabilityHtml(url, audience) };
}

const hasUrl = (s, url) => typeof s === 'string' && !!url && (s.includes(url) || s.includes(esc(url)));

// Any availability link in a body (with its scheme and host when present):
// a posted body may carry an older link (a preview taken before the link was
// superseded or revoked). Both forms: `/a/<5 base36>` and the older
// `/availability/<48 hex>`.
const LINK_RE = /(?:https?:\/\/[^\s"'<>]*?)?\/(?:a\/[a-z0-9]{5}(?![a-z0-9])|availability\/[a-f0-9]{48}(?![a-f0-9]))/g;

// Every availability link in `body` replaced by `replacement` (the current
// URL, HTML-escaped for an HTML body).
function replaceStaleLinks(body, replacement) {
    return String(body).replace(LINK_RE, replacement);
}

const escRe = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * A body sent WITHOUT an availability link: the preview's placeholder is
 * removed wherever it is (the whole placeholder paragraph when it is intact,
 * else the bare text, escaped or not), so it can never reach a recipient.
 */
function stripAvailabilityPlaceholderHtml(html) {
    let body = html == null ? '' : String(html);
    for (const aud of PARTIES) body = body.split(availabilityHtml(null, aud)).join('');
    for (const ph of [`<em>${esc(AVAILABILITY_PLACEHOLDER)}</em>`, esc(AVAILABILITY_PLACEHOLDER), AVAILABILITY_PLACEHOLDER]) {
        body = body.split(ph).join('');
    }
    return body;
}

function stripAvailabilityPlaceholderText(text) {
    let body = text == null ? '' : String(text);
    for (const aud of PARTIES) {
        body = body.replace(new RegExp(`\\n*${escRe(availabilityText(null, aud))}`, 'g'), '');
    }
    return body.split(AVAILABILITY_PLACEHOLDER).join('');
}

/**
 * The HTML body with the availability link in it, whatever the template
 * (or the staff-edited body) did: a preview placeholder left in the body is
 * replaced by the link; a body without the URL gets the paragraph appended.
 * `url` null → only the placeholder paragraph is appended (preview).
 */
function appendAvailabilityHtml(html, url, audience) {
    let body = html == null ? '' : String(html);
    if (url) {
        body = replaceStaleLinks(body, esc(url));
        if (hasUrl(body, url)) return body;
        const anchor = `<a href="${esc(url)}">${esc(url)}</a>`;
        for (const ph of [`<em>${esc(AVAILABILITY_PLACEHOLDER)}</em>`, esc(AVAILABILITY_PLACEHOLDER), AVAILABILITY_PLACEHOLDER]) {
            if (body.includes(ph)) body = body.split(ph).join(anchor);
        }
        if (hasUrl(body, url)) return body;
    } else if (body.includes(esc(AVAILABILITY_PLACEHOLDER)) || body.includes(AVAILABILITY_PLACEHOLDER)) {
        return body;
    }
    return `${body}${availabilityHtml(url, audience)}`;
}

// The same for a plain-text rendering.
function appendAvailabilityText(text, url, audience) {
    let body = text == null ? '' : String(text);
    if (url) {
        body = replaceStaleLinks(body, url);
        if (body.includes(url)) return body;
        if (body.includes(AVAILABILITY_PLACEHOLDER)) return body.split(AVAILABILITY_PLACEHOLDER).join(url);
    } else if (body.includes(AVAILABILITY_PLACEHOLDER)) {
        return body;
    }
    return `${body}${body ? '\n\n' : ''}${availabilityText(url, audience)}`;
}

module.exports = {
    PARTIES,
    LANGS,
    STATES,
    REVOKE_REASONS,
    MAX_WINDOW_DAYS,
    DEFAULT_EXPIRES_DAYS,
    MAX_EXPIRES_DAYS,
    AVAILABILITY_PLACEHOLDER,

    addDaysYmd,
    daysBetween,
    daysInRange,
    isoTs,
    parseDays,
    defaultWindow,
    defaultLang,
    validateLinkInput,
    newToken,
    linkUrl,
    requestState,
    isLive,
    linkToJson,
    collate,
    visitState,
    visitChanged,
    linkSyncPatch,
    availabilityHtml,
    availabilityText,
    availabilityVars,
    appendAvailabilityHtml,
    appendAvailabilityText,
    replaceStaleLinks,
    stripAvailabilityPlaceholderHtml,
    stripAvailabilityPlaceholderText,
};
