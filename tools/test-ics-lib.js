'use strict';

// Unit tests for src/lib/ics.js — the hand-built iCalendar invitation sent
// with QC visit emails and served by GET /api/v1/qc-visits/:id/ics: CRLF
// lines, 75-octet folding, RFC 5545 text escaping, the Asia/Shanghai
// VTIMEZONE, timed and all-day events, and METHOD:CANCEL.
//   node --test tools/test-ics-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildIcs, escapeText, foldLine, paramValue } = require('../src/lib/ics');

const NOW = new Date('2026-10-27T09:15:30.123Z');
const base = (over = {}) => ({
    uid: 'qc-visit-12@shipline',
    sequence: 0,
    method: 'REQUEST',
    summary: 'QC visit QCV_00012 — Acme',
    description: 'Inspect 3 lines',
    location: 'Main plant, 1 Road, Dongguan',
    date: '2026-11-12',
    start: '09:30',
    end: '12:00',
    organizerEmail: 'qc@example.com',
    attendees: [{ name: 'Li Wei', email: 'li@acme.cn' }, { name: 'AQI', email: 'ops@aqi.com' }],
    now: NOW,
    ...over,
});

// Unfold (RFC 5545 3.1) and split into logical lines.
const unfold = ics => ics.replace(/\r\n /g, '').split('\r\n').filter(l => l !== '');
// The VEVENT's lines named `name` (the VTIMEZONE has its own DTSTART).
function prop(ics, name) {
    const lines = unfold(ics);
    const event = lines.slice(lines.indexOf('BEGIN:VEVENT'), lines.indexOf('END:VEVENT') + 1);
    return event.filter(l => l === name || l.startsWith(`${name}:`) || l.startsWith(`${name};`));
}

test('every line ends in CRLF, there are no bare LFs, and the file ends with CRLF', () => {
    const ics = buildIcs(base({ description: 'line one\nline two\r\nline three' }));
    assert.ok(ics.endsWith('\r\n'));
    assert.equal(/[^\r]\n/.test(ics), false, 'no LF without CR');
    assert.equal(/\r[^\n]/.test(ics), false, 'no CR without LF');
    const lines = unfold(ics);
    assert.equal(lines[0], 'BEGIN:VCALENDAR');
    assert.equal(lines[lines.length - 1], 'END:VCALENDAR');
});

test('a timed visit: VTIMEZONE for Asia/Shanghai at +0800 and TZID on DTSTART / DTEND', () => {
    const ics = buildIcs(base());
    const lines = unfold(ics);
    for (const l of ['VERSION:2.0', 'METHOD:REQUEST', 'BEGIN:VTIMEZONE', 'TZID:Asia/Shanghai', 'TZOFFSETFROM:+0800', 'TZOFFSETTO:+0800',
        'END:VTIMEZONE', 'BEGIN:VEVENT', 'UID:qc-visit-12@shipline', 'SEQUENCE:0', 'DTSTAMP:20261027T091530Z',
        'DTSTART;TZID=Asia/Shanghai:20261112T093000', 'DTEND;TZID=Asia/Shanghai:20261112T120000',
        'STATUS:CONFIRMED', 'END:VEVENT']) {
        assert.ok(lines.includes(l), l);
    }
    assert.ok(lines.indexOf('END:VTIMEZONE') < lines.indexOf('BEGIN:VEVENT'), 'the zone comes before the event');
    assert.ok(lines.some(l => l.startsWith('PRODID:')));
    assert.deepEqual(prop(ics, 'ORGANIZER'), ['ORGANIZER:mailto:qc@example.com']);
    assert.deepEqual(prop(ics, 'ATTENDEE'), [
        'ATTENDEE;CN="Li Wei";ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:li@acme.cn',
        'ATTENDEE;CN="AQI";ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:ops@aqi.com',
    ]);
});

test('MySQL TIME values (HH:MM:SS) are accepted', () => {
    const ics = buildIcs(base({ start: '09:30:00', end: '12:00:00' }));
    assert.deepEqual(prop(ics, 'DTSTART'), ['DTSTART;TZID=Asia/Shanghai:20261112T093000']);
});

test('no start time: an all-day event on the visit date (DTEND is the next day)', () => {
    const ics = buildIcs(base({ start: null, end: null, date: '2026-12-31' }));
    assert.deepEqual(prop(ics, 'DTSTART'), ['DTSTART;VALUE=DATE:20261231']);
    assert.deepEqual(prop(ics, 'DTEND'), ['DTEND;VALUE=DATE:20270101']);
    assert.equal(/TZID=/.test(ics), false, 'no TZID on an all-day event');
    assert.deepEqual(prop(buildIcs(base({ start: '', end: '', date: '2028-02-28' })), 'DTEND'), ['DTEND;VALUE=DATE:20280229'], 'leap day');
});

test('a start without a usable end lasts an hour', () => {
    for (const end of [null, '09:00', '09:30']) {
        const ics = buildIcs(base({ end }));
        assert.deepEqual(prop(ics, 'DTEND'), [], String(end));
        assert.deepEqual(prop(ics, 'DURATION'), ['DURATION:PT1H'], String(end));
    }
});

test('METHOD:CANCEL keeps the UID, carries the sequence and marks the event cancelled', () => {
    const ics = buildIcs(base({ method: 'CANCEL', sequence: 3 }));
    const lines = unfold(ics);
    assert.ok(lines.includes('METHOD:CANCEL'));
    assert.ok(lines.includes('STATUS:CANCELLED'));
    assert.ok(lines.includes('SEQUENCE:3'));
    assert.ok(lines.includes('UID:qc-visit-12@shipline'));
    assert.ok(!lines.includes('STATUS:CONFIRMED'));
});

test('text values are escaped per RFC 5545: backslash, semicolon, comma, newline', () => {
    assert.equal(escapeText('a\\b;c,d\ne\r\nf\rg'), 'a\\\\b\\;c\\,d\\ne\\nf\\ng');
    assert.equal(escapeText(null), '');
    assert.equal(escapeText('Colon: fine'), 'Colon: fine', 'colons need no escape in a value');
    const ics = buildIcs(base({ summary: 'QC; visit, now', location: 'Room 1, Bldg\\2', description: 'one\ntwo' }));
    assert.deepEqual(prop(ics, 'SUMMARY'), ['SUMMARY:QC\\; visit\\, now']);
    assert.deepEqual(prop(ics, 'LOCATION'), ['LOCATION:Room 1\\, Bldg\\\\2']);
    assert.deepEqual(prop(ics, 'DESCRIPTION'), ['DESCRIPTION:one\\ntwo']);
});

test('empty description and location are left out; attendees without email are skipped', () => {
    const ics = buildIcs(base({ description: '', location: null, organizerEmail: null, attendees: [{ name: 'Nobody' }, null, { email: 'x@y.com' }] }));
    assert.deepEqual(prop(ics, 'DESCRIPTION'), []);
    assert.deepEqual(prop(ics, 'LOCATION'), []);
    assert.deepEqual(prop(ics, 'ORGANIZER'), []);
    assert.deepEqual(prop(ics, 'ATTENDEE'), ['ATTENDEE;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:x@y.com']);
});

test('CN parameters are quoted and lose double quotes and control characters', () => {
    assert.equal(paramValue('Li "Big" Wei, QC; Lead:1'), '"Li Big Wei, QC; Lead:1"');
    assert.equal(paramValue('a\nb'), '"ab"');
    const ics = buildIcs(base({ attendees: [{ name: 'Wang, "AQI"', email: 'w@aqi.com' }] }));
    assert.match(unfold(ics).find(l => l.startsWith('ATTENDEE')), /^ATTENDEE;CN="Wang, AQI";/);
});

test('long lines fold at 75 octets with a leading space, and unfold back exactly', () => {
    const long = 'x'.repeat(300);
    const ics = buildIcs(base({ description: long }));
    for (const physical of ics.split('\r\n')) {
        assert.ok(Buffer.byteLength(physical, 'utf8') <= 75, `${Buffer.byteLength(physical, 'utf8')} octets: ${physical.slice(0, 20)}…`);
    }
    assert.deepEqual(prop(ics, 'DESCRIPTION'), [`DESCRIPTION:${long}`]);
    const folded = foldLine('A'.repeat(75));
    assert.equal(folded, 'A'.repeat(75), 'exactly 75 octets is not folded');
    assert.equal(foldLine('A'.repeat(76)), `${'A'.repeat(75)}\r\n A`);
    assert.equal(foldLine('A'.repeat(75 + 74 + 1)), `${'A'.repeat(75)}\r\n ${'A'.repeat(74)}\r\n A`, 'continuations hold 74 + the space');
});

test('folding never splits a multi-byte UTF-8 character', () => {
    const text = '检验工厂地址：广东省东莞市'.repeat(12); // 3 octets each
    const ics = buildIcs(base({ location: text }));
    for (const physical of ics.split('\r\n')) {
        const bytes = Buffer.from(physical, 'utf8');
        assert.ok(bytes.length <= 75, `${bytes.length} octets`);
        assert.equal(bytes.toString('utf8'), physical, 'each physical line is valid UTF-8 on its own');
        assert.equal(physical.includes('�'), false);
    }
    assert.deepEqual(prop(ics, 'LOCATION'), [`LOCATION:${text}`]);
    const emoji = foldLine(`X${'😀'.repeat(30)}`); // 4 octets each, surrogate pairs in JS
    for (const physical of emoji.split('\r\n')) assert.ok(Buffer.byteLength(physical, 'utf8') <= 75);
    assert.equal(emoji.replace(/\r\n /g, ''), `X${'😀'.repeat(30)}`);
});

test('sequence is a non-negative whole number; DTSTAMP is UTC', () => {
    assert.deepEqual(prop(buildIcs(base({ sequence: '2' })), 'SEQUENCE'), ['SEQUENCE:2']);
    assert.deepEqual(prop(buildIcs(base({ sequence: -4 })), 'SEQUENCE'), ['SEQUENCE:0']);
    assert.deepEqual(prop(buildIcs(base({ sequence: undefined })), 'SEQUENCE'), ['SEQUENCE:0']);
    assert.match(prop(buildIcs(base({ now: undefined })), 'DTSTAMP')[0], /^DTSTAMP:\d{8}T\d{6}Z$/);
});

test('bad input throws: no uid, bad date or time, unknown method or zone', () => {
    assert.throws(() => buildIcs(base({ uid: '' })), /uid/);
    assert.throws(() => buildIcs(base({ date: '12/11/2026' })), /date/);
    assert.throws(() => buildIcs(base({ date: null })), /date/);
    assert.throws(() => buildIcs(base({ start: '9:30' })), /time/);
    assert.throws(() => buildIcs(base({ end: '25:00' })), /time/);
    assert.throws(() => buildIcs(base({ method: 'PUBLISH' })), /method/);
    assert.throws(() => buildIcs(base({ tz: 'Europe/London' })), /time zone/);
    assert.ok(buildIcs(base({ tz: undefined })).includes('TZID:Asia/Shanghai'), 'tz defaults to Asia/Shanghai');
});
