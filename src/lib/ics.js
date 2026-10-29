'use strict';

// A hand-built iCalendar (RFC 5545) invitation for a QC visit: the .ics
// attached to the factory / inspector email and served by
// GET /api/v1/qc-visits/:id/ics (src/services/qc-visit-comms-routes.js).
// No library: one VEVENT, a VTIMEZONE for the visit's zone, CRLF line
// endings, lines folded at 75 octets, TEXT values escaped.
//
// The visit is China wall-clock (qc_visits.visit_date + window_*, tz
// 'Asia/Shanghai'), so DTSTART carries TZID and the calendar client converts
// it. With no start time the event is all-day (DTSTART;VALUE=DATE).
// METHOD:CANCEL with the same UID and a higher SEQUENCE withdraws a REQUEST.
//
// No DB, no AWS: tested by tools/test-ics-lib.js.

const CRLF = '\r\n';
const PRODID = '-//ShipLine//QC Visits//EN';

// Zones we can describe. Asia/Shanghai has had no DST since 1991.
const TIMEZONES = {
    'Asia/Shanghai': { offset: '+0800', name: 'CST' },
};

// TEXT value escaping (RFC 5545 3.3.11): backslash, semicolon, comma, newline.
function escapeText(v) {
    return String(v == null ? '' : v)
        .replace(/\\/g, '\\\\')
        .replace(/;/g, '\\;')
        .replace(/,/g, '\\,')
        .replace(/\r\n?|\n/g, '\\n');
}

// A parameter value (e.g. CN): always quoted; DQUOTE and control characters
// are not allowed inside a quoted parameter, so they are dropped.
function paramValue(v) {
    // eslint-disable-next-line no-control-regex
    return `"${String(v == null ? '' : v).replace(/["\x00-\x08\x0A-\x1F\x7F]/g, '').trim()}"`;
}

// Fold one content line to at most 75 octets per physical line (the leading
// space of a continuation counts), never splitting a UTF-8 character.
function foldLine(line) {
    const parts = [];
    let cur = '';
    let curBytes = 0;
    let limit = 75;
    for (const ch of String(line)) {
        const b = Buffer.byteLength(ch, 'utf8');
        if (curBytes + b > limit) {
            parts.push(cur);
            cur = '';
            curBytes = 0;
            limit = 74; // the continuation's leading space takes one octet
        }
        cur += ch;
        curBytes += b;
    }
    parts.push(cur);
    return parts.join(`${CRLF} `);
}

const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
const HM_RE = /^([01]\d|2[0-3]):([0-5]\d)(?::[0-5]\d)?$/;

function compactDate(ymd) {
    const m = DATE_RE.exec(String(ymd || ''));
    if (!m) throw new Error(`buildIcs: date must be YYYY-MM-DD, got ${JSON.stringify(ymd)}`);
    return `${m[1]}${m[2]}${m[3]}`;
}

function hhmm(v) {
    if (v == null || v === '') return null;
    const m = HM_RE.exec(String(v));
    if (!m) throw new Error(`buildIcs: time must be HH:MM, got ${JSON.stringify(v)}`);
    return `${m[1]}${m[2]}`;
}

function nextDay(ymd) {
    const m = DATE_RE.exec(ymd);
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]) + 1));
    return d.toISOString().slice(0, 10).replace(/-/g, '');
}

const utcStamp = d => d.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');

// CONTRACT-GAP: `now` (DTSTAMP, default the current time) is an extra
// optional input so tests are deterministic.
function buildIcs({
    uid,
    sequence = 0,
    method = 'REQUEST',
    summary = '',
    description = '',
    location = '',
    date,
    start = null,
    end = null,
    tz = 'Asia/Shanghai',
    organizerEmail = null,
    attendees = [],
    now = new Date(),
} = {}) {
    if (!uid) throw new Error('buildIcs: uid is required');
    if (method !== 'REQUEST' && method !== 'CANCEL') throw new Error(`buildIcs: method must be REQUEST or CANCEL, got ${method}`);
    const zone = TIMEZONES[tz];
    if (!zone) throw new Error(`buildIcs: unsupported time zone ${tz}`);
    const day = compactDate(date);
    const s = hhmm(start);
    let e = hhmm(end);
    if (s && e && e <= s) e = null; // a window that ends before it starts: keep the start only

    const lines = [
        'BEGIN:VCALENDAR',
        `PRODID:${PRODID}`,
        'VERSION:2.0',
        'CALSCALE:GREGORIAN',
        `METHOD:${method}`,
    ];
    // Always present (the contract asks for it); only a timed DTSTART uses it.
    lines.push(
        'BEGIN:VTIMEZONE',
        `TZID:${tz}`,
        'BEGIN:STANDARD',
        'DTSTART:19700101T000000',
        `TZOFFSETFROM:${zone.offset}`,
        `TZOFFSETTO:${zone.offset}`,
        `TZNAME:${zone.name}`,
        'END:STANDARD',
        'END:VTIMEZONE',
    );
    lines.push(
        'BEGIN:VEVENT',
        `UID:${escapeText(uid)}`,
        `SEQUENCE:${Math.max(0, Math.trunc(Number(sequence) || 0))}`,
        `DTSTAMP:${utcStamp(now instanceof Date ? now : new Date(now))}`,
    );
    if (s) {
        lines.push(`DTSTART;TZID=${tz}:${day}T${s}00`);
        if (e) lines.push(`DTEND;TZID=${tz}:${day}T${e}00`);
        else lines.push('DURATION:PT1H');
    } else {
        lines.push(`DTSTART;VALUE=DATE:${day}`, `DTEND;VALUE=DATE:${nextDay(date)}`);
    }
    lines.push(`SUMMARY:${escapeText(summary)}`);
    if (description) lines.push(`DESCRIPTION:${escapeText(description)}`);
    if (location) lines.push(`LOCATION:${escapeText(location)}`);
    if (organizerEmail) lines.push(`ORGANIZER:mailto:${String(organizerEmail).trim()}`);
    for (const a of attendees || []) {
        if (!a || !a.email) continue;
        const cn = a.name ? `;CN=${paramValue(a.name)}` : '';
        lines.push(`ATTENDEE${cn};ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:${String(a.email).trim()}`);
    }
    lines.push(
        `STATUS:${method === 'CANCEL' ? 'CANCELLED' : 'CONFIRMED'}`,
        'TRANSP:OPAQUE',
        'END:VEVENT',
        'END:VCALENDAR',
    );
    return lines.map(foldLine).join(CRLF) + CRLF;
}

module.exports = { buildIcs, escapeText, foldLine, paramValue };
