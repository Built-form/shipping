'use strict';

// Unit tests for src/lib/qc-availability.js — the pure rules behind QC visit
// availability links (docs/qc-availability.md): window defaults and
// validation (China day, 31-day cap), token and URL, request state, the Link
// JSON, collation of both parties' answers, lifecycle sync and the email
// fallbacks — plus createOrReuseLink / syncAvailabilityLinks from
// src/services/qc-availability-routes.js against a fake connection.
// No database.
//   node --test tools/test-qc-availability-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const A = require('../src/lib/qc-availability');
const { chinaToday } = require('../src/lib/qc-visits');
const { SOURCE_MISSING, isSourceMissing } = require('../src/lib/jportal');
const R = require('../src/services/qc-availability-routes');

const NOW = new Date('2026-11-10T08:00:00Z');
const visit = (over = {}) => ({
    id: 7, ref: 'QCV_00007', status: 'PROPOSED', visit_date: '2026-11-12',
    supplier_name: 'Acme Co., Ltd.', factory_label: 'Plant 2', factory_city: 'Dongguan',
    factory_contact_name: 'Ms Li', inspector_name: 'Mr Wang', ...over,
});
const link = (over = {}) => ({
    id: 1, token: 'a'.repeat(48), visit_id: 7, visit_ref: 'QCV_00007', party: 'factory',
    recipient_name: 'Ms Li', recipient_email: 'li@example.com',
    proposed_date: '2026-11-12', window_start_date: '2026-11-12', window_end_date: '2026-11-17',
    preferred_lang: 'zh', expires_at: new Date('2026-12-10T08:00:00Z'), revoked_at: null, revoke_reason: null,
    created_by_email: 'ops@example.com', created_at: new Date('2026-11-10T07:00:00Z'),
    available_days: null, none_available: 0, answered_by_name: null, answer_note: null,
    answered_at: null, answer_count: 0, row_version: 1, ...over,
});
const silentLog = { info() {}, warn() {}, error() {} };

// ── dates and window ────────────────────────────────────────────────────────

test('addDaysYmd / daysInRange across month, year and leap day', () => {
    assert.equal(A.addDaysYmd('2026-11-28', 5), '2026-12-03');
    assert.equal(A.addDaysYmd('2026-12-30', 3), '2027-01-02');
    assert.equal(A.addDaysYmd('2028-02-28', 1), '2028-02-29');
    assert.equal(A.addDaysYmd('2026-03-28', 2), '2026-03-30'); // UK DST Sunday is irrelevant
    assert.deepEqual(A.daysInRange('2026-10-24', '2026-10-27'), ['2026-10-24', '2026-10-25', '2026-10-26', '2026-10-27']);
    assert.deepEqual(A.daysInRange('2026-10-27', '2026-10-24'), []);
    assert.equal(A.daysBetween('2026-11-12', '2026-12-12'), 30);
});

test('defaultWindow: the proposed date and the next 5 days', () => {
    assert.deepEqual(A.defaultWindow('2026-11-12'), { start: '2026-11-12', end: '2026-11-17' });
    assert.deepEqual(A.defaultWindow(new Date('2026-11-12T00:00:00Z')), { start: '2026-11-12', end: '2026-11-17' });
    assert.equal(A.defaultWindow(null), null);
});

// ── validateLinkInput ───────────────────────────────────────────────────────

test('validateLinkInput: defaults (window, language by party, 30-day expiry)', () => {
    const f = A.validateLinkInput({ party: 'factory' }, visit(), '2026-11-10');
    assert.deepEqual(f.value, {
        party: 'factory', window_start_date: '2026-11-12', window_end_date: '2026-11-17', proposed_date: '2026-11-12',
        recipient_name: null, recipient_email: null, preferred_lang: 'zh', expiresInDays: 30,
    });
    const i = A.validateLinkInput({ party: 'inspector', recipientName: '  Mr   Wang ', recipientEmail: ' w@example.com ' }, visit(), '2026-11-10');
    assert.equal(i.value.preferred_lang, 'en');
    assert.equal(i.value.recipient_name, 'Mr Wang');
    assert.equal(i.value.recipient_email, 'w@example.com');
    assert.equal(A.validateLinkInput({ party: 'inspector', preferredLang: null }, visit(), '2026-11-10').value.preferred_lang, null);
    assert.equal(A.validateLinkInput({ party: 'inspector', preferredLang: 'zh' }, visit(), '2026-11-10').value.preferred_lang, 'zh');
});

test('validateLinkInput: refusals carry code BAD_FIELD and the field', () => {
    const v = visit();
    const today = '2026-11-10';
    const field = body => {
        const r = A.validateLinkInput(body, v, today);
        assert.ok(r.error, `expected an error for ${JSON.stringify(body)}`);
        assert.equal(r.error.code, 'BAD_FIELD');
        assert.equal(typeof r.error.message, 'string');
        return r.error.field;
    };
    assert.equal(field({ party: 'supplier' }), 'party');
    assert.equal(field({}), 'party');
    assert.equal(field({ party: 'factory', windowStartDate: '2026-02-30' }), 'windowStartDate');
    assert.equal(field({ party: 'factory', windowEndDate: '12/11/2026' }), 'windowEndDate');
    assert.equal(field({ party: 'factory', windowStartDate: '2026-11-12', windowEndDate: '2026-11-11' }), 'windowEndDate');
    // the proposed date must be inside the window
    assert.equal(field({ party: 'factory', windowStartDate: '2026-11-13', windowEndDate: '2026-11-20' }), 'windowStartDate');
    assert.equal(field({ party: 'factory', windowStartDate: '2026-11-05', windowEndDate: '2026-11-11' }), 'windowStartDate');
    assert.equal(field({ party: 'factory', preferredLang: 'fr' }), 'preferredLang');
    for (const n of [0, 91, 1.5, 'x', -3]) assert.equal(field({ party: 'factory', expiresInDays: n }), 'expiresInDays');
    assert.equal(field({ party: 'factory', recipientEmail: 'not-an-email' }), 'recipientEmail');
    assert.equal(field({ party: 'factory', recipientName: 'x'.repeat(256) }), 'recipientName');
    assert.equal(A.validateLinkInput({ party: 'factory' }, visit({ visit_date: null }), today).error.field, 'visitDate');
});

test('validateLinkInput: expiresInDays 1..90', () => {
    assert.equal(A.validateLinkInput({ party: 'factory', expiresInDays: 1 }, visit(), '2026-11-10').value.expiresInDays, 1);
    assert.equal(A.validateLinkInput({ party: 'factory', expiresInDays: '90' }, visit(), '2026-11-10').value.expiresInDays, 90);
});

test('validateLinkInput: 31-day cap, start and end included', () => {
    const ok = A.validateLinkInput({ party: 'factory', windowStartDate: '2026-11-12', windowEndDate: '2026-12-12' }, visit(), '2026-11-10');
    assert.ok(ok.value, '31 days is allowed');
    const tooLong = A.validateLinkInput({ party: 'factory', windowStartDate: '2026-11-12', windowEndDate: '2026-12-13' }, visit(), '2026-11-10');
    assert.equal(tooLong.error.field, 'windowEndDate');
    const single = A.validateLinkInput({ party: 'factory', windowStartDate: '2026-11-12', windowEndDate: '2026-11-12' }, visit(), '2026-11-10');
    assert.ok(single.value, 'a one-day window is allowed');
});

test('validateLinkInput: the window must not have ended on the China day (15:59 vs 16:00 UTC)', () => {
    const before = chinaToday(new Date('2026-11-11T15:59:00Z'));
    const after = chinaToday(new Date('2026-11-11T16:00:00Z'));
    assert.equal(before, '2026-11-11');
    assert.equal(after, '2026-11-12');
    const v = visit({ visit_date: '2026-11-08' });
    const body = { party: 'factory', windowStartDate: '2026-11-08', windowEndDate: '2026-11-11' };
    assert.ok(A.validateLinkInput(body, v, before).value, 'still the 11th in China at 15:59 UTC');
    assert.equal(A.validateLinkInput(body, v, after).error.field, 'windowEndDate');
    // a start in the past is fine as long as the window has not ended
    assert.ok(A.validateLinkInput({ party: 'factory', windowStartDate: '2026-11-08', windowEndDate: '2026-11-12' }, v, after).value);
});

// ── token, URL, state ───────────────────────────────────────────────────────

test('newToken: 5 lowercase base36 characters', () => {
    const a = A.newToken();
    const b = A.newToken();
    assert.match(a, /^[a-z0-9]{5}$/);
    assert.notEqual(a, b);
});

test('linkUrl: trailing slashes dropped; no portal URL → null', () => {
    assert.equal(A.linkUrl('https://portal.example.com/', 'abc'), 'https://portal.example.com/a/abc');
    assert.equal(A.linkUrl('https://portal.example.com///', 'abc'), 'https://portal.example.com/a/abc');
    assert.equal(A.linkUrl('https://portal.example.com', 'abc'), 'https://portal.example.com/a/abc');
    assert.equal(A.linkUrl('', 'abc'), null);
    assert.equal(A.linkUrl(undefined, 'abc'), null);
    assert.equal(A.linkUrl('   ', 'abc'), null);
});

test('requestState: revoked > expired > answered > open', () => {
    const answered = { answered_at: new Date('2026-11-10T07:30:00Z') };
    const past = { expires_at: new Date('2026-11-10T07:59:59Z') };
    assert.equal(A.requestState(link(), NOW), 'open');
    assert.equal(A.requestState(link(answered), NOW), 'answered');
    assert.equal(A.requestState(link({ ...answered, ...past }), NOW), 'expired');
    assert.equal(A.requestState(link({ ...answered, ...past, revoked_at: NOW }), NOW), 'revoked');
    assert.equal(A.requestState(link({ revoked_at: '2026-11-09 10:00:00' }), NOW), 'revoked');
    // exactly at expiry → expired; strings are UTC
    assert.equal(A.requestState(link({ expires_at: '2026-11-10 08:00:00' }), NOW), 'expired');
    assert.equal(A.requestState(link({ expires_at: '2026-11-10 08:00:01' }), NOW), 'open');
});

// ── Link JSON ───────────────────────────────────────────────────────────────

test('linkToJson: the contract shape; url only while open or answered; never the token', () => {
    const portalUrl = 'https://portal.example.com';
    const j = A.linkToJson(link(), { portalUrl, now: NOW });
    assert.deepEqual(Object.keys(j).sort(), [
        'answerCount', 'answerNote', 'answeredAt', 'answeredByName', 'availableDays', 'createdAt', 'createdByEmail',
        'expiresAt', 'id', 'noneAvailable', 'party', 'preferredLang', 'proposedDate', 'recipientName', 'revokeReason',
        'revokedAt', 'state', 'url', 'windowEndDate', 'windowStartDate',
    ]);
    assert.equal(j.url, `${portalUrl}/a/${'a'.repeat(48)}`);
    assert.equal(j.state, 'open');
    assert.equal(j.availableDays, null);
    assert.equal(j.noneAvailable, false);
    assert.equal(j.expiresAt, '2026-12-10T08:00:00.000Z');
    assert.equal(j.windowStartDate, '2026-11-12');
    assert.ok(!('token' in j) && !('recipientEmail' in j));

    const answered = A.linkToJson(link({
        answered_at: '2026-11-10 07:30:00.123', available_days: '["2026-11-14","2026-11-12","2026-11-12"]',
        answered_by_name: 'Li', answer_note: 'Mornings', answer_count: 2,
    }), { portalUrl, now: NOW });
    assert.equal(answered.state, 'answered');
    assert.ok(answered.url);
    assert.deepEqual(answered.availableDays, ['2026-11-12', '2026-11-14']);
    assert.equal(answered.answeredAt, '2026-11-10T07:30:00.123Z');
    assert.equal(answered.answerCount, 2);

    const revoked = A.linkToJson(link({ revoked_at: NOW, revoke_reason: 'staff' }), { portalUrl, now: NOW });
    assert.equal(revoked.url, null);
    assert.equal(revoked.revokeReason, 'staff');
    assert.equal(revoked.revokedAt, NOW.toISOString());
    const expired = A.linkToJson(link({ expires_at: new Date('2026-11-01T00:00:00Z') }), { portalUrl, now: NOW });
    assert.equal(expired.url, null);
    assert.equal(expired.state, 'expired');
    // no portal URL configured → url null even while open
    assert.equal(A.linkToJson(link(), { portalUrl: '', now: NOW }).url, null);
    // JSON arriving parsed, and none_available as a BIT buffer
    const parsed = A.linkToJson(link({ answered_at: NOW, available_days: [], none_available: Buffer.from([1]) }), { portalUrl, now: NOW });
    assert.deepEqual(parsed.availableDays, []);
    assert.equal(parsed.noneAvailable, true);
});

// ── collate ─────────────────────────────────────────────────────────────────

test('collate: one party answered, the other not → the other is null everywhere', () => {
    const rows = [
        link({ id: 1, answered_at: NOW, available_days: ['2026-11-12', '2026-11-14'] }),
        link({ id: 2, party: 'inspector', window_start_date: '2026-11-12', window_end_date: '2026-11-14' }),
    ];
    const c = A.collate(rows, NOW);
    assert.equal(c.days.length, 6);
    assert.deepEqual(c.days[0], { date: '2026-11-12', factory: true, inspector: null, both: false });
    assert.deepEqual(c.days[1], { date: '2026-11-13', factory: false, inspector: null, both: false });
    assert.equal(c.parties.factory.linkId, 1);
    assert.equal(c.parties.factory.state, 'answered');
    assert.deepEqual(c.parties.factory.availableDays, ['2026-11-12', '2026-11-14']);
    assert.equal(c.parties.inspector.state, 'open');
    assert.equal(c.parties.inspector.availableDays, null);
    assert.deepEqual(Object.keys(c.parties.inspector).sort(), [
        'answerNote', 'answeredAt', 'answeredByName', 'availableDays', 'expiresAt', 'linkId', 'noneAvailable',
        'revokeReason', 'state', 'windowEndDate', 'windowStartDate',
    ]);
});

test('collate: overlap, union of windows, null outside a window, none available', () => {
    const rows = [
        link({ id: 1, answered_at: NOW, available_days: '["2026-11-13","2026-11-14","2026-11-17"]' }),
        link({
            id: 2, party: 'inspector', window_start_date: '2026-11-14', window_end_date: '2026-11-20',
            answered_at: NOW, available_days: ['2026-11-14', '2026-11-15', '2026-11-17', '2026-11-20'],
        }),
    ];
    const c = A.collate(rows, NOW);
    assert.equal(c.days[0].date, '2026-11-12');
    assert.equal(c.days[c.days.length - 1].date, '2026-11-20');
    assert.equal(c.days.length, 9);
    const day = d => c.days.find(x => x.date === d);
    assert.deepEqual(day('2026-11-12'), { date: '2026-11-12', factory: false, inspector: null, both: false });
    assert.deepEqual(day('2026-11-14'), { date: '2026-11-14', factory: true, inspector: true, both: true });
    assert.deepEqual(day('2026-11-15'), { date: '2026-11-15', factory: false, inspector: true, both: false });
    assert.deepEqual(day('2026-11-17'), { date: '2026-11-17', factory: true, inspector: true, both: true });
    assert.deepEqual(day('2026-11-20'), { date: '2026-11-20', factory: null, inspector: true, both: false });
    assert.deepEqual(c.days.filter(d => d.both).map(d => d.date), ['2026-11-14', '2026-11-17']);

    const none = A.collate([link({ answered_at: NOW, none_available: 1, available_days: [] })], NOW);
    assert.ok(none.days.every(d => d.factory === false && d.inspector === null && !d.both));
    assert.equal(none.parties.factory.noneAvailable, true);
});

test('collate: latest non-revoked per party; an expired answer still counts; nothing → empty', () => {
    const rows = [
        link({ id: 1, created_at: new Date('2026-11-01T00:00:00Z'), answered_at: NOW, available_days: ['2026-11-12'] }),
        link({ id: 3, created_at: new Date('2026-11-09T00:00:00Z'), revoked_at: NOW, revoke_reason: 'staff', answered_at: NOW, available_days: ['2026-11-13'] }),
        link({ id: 2, created_at: new Date('2026-11-05T00:00:00Z'), expires_at: new Date('2026-11-09T00:00:00Z'), answered_at: NOW, available_days: ['2026-11-15'] }),
    ];
    const c = A.collate(rows, NOW);
    assert.equal(c.parties.factory.linkId, 2);
    assert.equal(c.parties.factory.state, 'expired');
    assert.equal(c.days.find(d => d.date === '2026-11-15').factory, true);
    assert.equal(c.days.find(d => d.date === '2026-11-13').factory, false);
    assert.equal(c.parties.inspector, null);

    assert.deepEqual(A.collate([], NOW), { days: [], parties: { factory: null, inspector: null } });
    assert.deepEqual(A.collate([link({ revoked_at: NOW })], NOW), { days: [], parties: { factory: null, inspector: null } });
    // same created_at: the higher id wins
    const tie = A.collate([link({ id: 4 }), link({ id: 9 })], NOW);
    assert.equal(tie.parties.factory.linkId, 9);
});

// ── linkSyncPatch ───────────────────────────────────────────────────────────

test('linkSyncPatch: CANCELLED / COMPLETED / CONFIRMED revoke every live link with that reason', () => {
    const links = [link({ id: 1 }), link({ id: 2, party: 'inspector', answered_at: NOW })];
    for (const [status, reason] of [['CANCELLED', 'cancelled'], ['COMPLETED', 'completed'], ['CONFIRMED', 'confirmed']]) {
        const out = A.linkSyncPatch(links, { before: { status: 'PROPOSED', visit_date: '2026-11-12' }, after: { status, visit_date: '2026-11-12' } }, NOW);
        assert.deepEqual(out, [{ id: 1, action: 'revoke', reason }, { id: 2, action: 'revoke', reason }]);
    }
    // cancelling with the date also moved: the status reason wins
    const both = A.linkSyncPatch([link()], { before: { status: 'PROPOSED', visitDate: '2026-11-12' }, after: { status: 'CANCELLED', visitDate: '2026-12-25' } }, NOW);
    assert.deepEqual(both, [{ id: 1, action: 'revoke', reason: 'cancelled' }]);
});

test('linkSyncPatch: date inside the window moves the badge; outside revokes date_moved; cleared revokes', () => {
    const before = { status: 'PROPOSED', visitDate: '2026-11-12' };
    assert.deepEqual(
        A.linkSyncPatch([link()], { before, after: { status: 'PROPOSED', visitDate: '2026-11-15' } }, NOW),
        [{ id: 1, action: 'move', proposedDate: '2026-11-15' }]
    );
    assert.deepEqual(
        A.linkSyncPatch([link()], { before, after: { status: 'PROPOSED', visitDate: '2026-11-17' } }, NOW),
        [{ id: 1, action: 'move', proposedDate: '2026-11-17' }]
    );
    assert.deepEqual(
        A.linkSyncPatch([link()], { before, after: { status: 'PROPOSED', visitDate: '2026-11-18' } }, NOW),
        [{ id: 1, action: 'revoke', reason: 'date_moved' }]
    );
    assert.deepEqual(
        A.linkSyncPatch([link()], { before, after: { status: 'PROPOSED', visitDate: '2026-11-11' } }, NOW),
        [{ id: 1, action: 'revoke', reason: 'date_moved' }]
    );
    assert.deepEqual(
        A.linkSyncPatch([link()], { before, after: { status: 'PROPOSED', visitDate: null } }, NOW),
        [{ id: 1, action: 'revoke', reason: 'date_moved' }]
    );
    // a CONFIRMED visit re-proposed by a date change inside the window: the link stays
    assert.deepEqual(
        A.linkSyncPatch([link()], { before: { status: 'CONFIRMED', visitDate: '2026-11-12' }, after: { status: 'PROPOSED', visitDate: '2026-11-13' } }, NOW),
        [{ id: 1, action: 'move', proposedDate: '2026-11-13' }]
    );
});

test('linkSyncPatch: nothing changed, or only revoked links → nothing', () => {
    const same = { status: 'PROPOSED', visitDate: '2026-11-12' };
    assert.deepEqual(A.linkSyncPatch([link()], { before: same, after: same }, NOW), []);
    const revoked = [link({ id: 1, revoked_at: NOW }), link({ id: 2, revoked_at: NOW, expires_at: new Date('2026-11-01T00:00:00Z') })];
    assert.deepEqual(A.linkSyncPatch(revoked, { before: same, after: { status: 'CANCELLED', visitDate: '2026-11-12' } }, NOW), []);
    // a link already showing the new date is not rewritten
    assert.deepEqual(A.linkSyncPatch([link({ proposed_date: '2026-11-13' })], { before: same, after: { status: 'PROPOSED', visitDate: '2026-11-13' } }, NOW), []);
    // DRAFT → PROPOSED revokes nothing
    assert.deepEqual(A.linkSyncPatch([link()], { before: { status: 'DRAFT', visitDate: '2026-11-12' }, after: same }, NOW), []);
    assert.equal(A.visitChanged(same, { status: 'PROPOSED', visit_date: '2026-11-12' }), false);
    assert.equal(A.visitChanged(same, { status: 'CONFIRMED', visit_date: '2026-11-12' }), true);
});

// ── email helpers ───────────────────────────────────────────────────────────

const URL = 'https://portal.example.com/availability/abc123';

test('appendAvailabilityHtml: appended when absent, left alone when present, placeholder replaced', () => {
    const body = '<div><p>Hello</p></div>';
    const out = A.appendAvailabilityHtml(body, URL, 'factory');
    assert.ok(out.startsWith(body));
    assert.ok(out.includes(`<a href="${URL}">${URL}</a>`));
    assert.ok(out.includes('请在此勾选'), 'the factory gets the Chinese line too');
    assert.equal(A.appendAvailabilityHtml(out, URL, 'factory'), out, 'idempotent');
    const insp = A.appendAvailabilityHtml(body, URL, 'inspector');
    assert.ok(insp.includes(URL) && !insp.includes('请在此勾选'));
    // a body the preview rendered with the placeholder (staff edited it, then sent)
    const preview = A.appendAvailabilityHtml(body, null, 'factory');
    assert.ok(preview.includes(A.AVAILABILITY_PLACEHOLDER));
    assert.equal(A.appendAvailabilityHtml(preview, null, 'factory'), preview, 'the placeholder is not added twice');
    const sent = A.appendAvailabilityHtml(preview, URL, 'factory');
    assert.ok(sent.includes(URL) && !sent.includes(A.AVAILABILITY_PLACEHOLDER));
    assert.equal((sent.match(/availability\/abc123/g) || []).length, 2, 'href + text, once');
    // URLs are escaped in the HTML
    const amp = A.appendAvailabilityHtml('', 'https://p.example.com/availability/x?a=1&b=2', 'inspector');
    assert.ok(amp.includes('a=1&amp;b=2') && !amp.includes('a=1&b=2'));
    assert.equal(A.appendAvailabilityHtml(amp, 'https://p.example.com/availability/x?a=1&b=2', 'inspector'), amp);
});

test('appendAvailabilityText: appended when absent, left alone when present, placeholder replaced', () => {
    const out = A.appendAvailabilityText('Hello', URL, 'factory');
    assert.ok(out.startsWith('Hello\n\n') && out.endsWith(URL) && out.includes('请在此勾选'));
    assert.equal(A.appendAvailabilityText(out, URL, 'factory'), out);
    assert.equal(A.appendAvailabilityText(`Link: ${A.AVAILABILITY_PLACEHOLDER}`, URL, 'inspector'), `Link: ${URL}`);
    const ph = A.appendAvailabilityText('', null, 'inspector');
    assert.ok(ph.endsWith(A.AVAILABILITY_PLACEHOLDER) && !ph.startsWith('\n'));
    assert.equal(A.appendAvailabilityText(ph, null, 'inspector'), ph);
});

test('availabilityVars: the {availabilityLink} token in text and html', () => {
    assert.deepEqual(A.availabilityVars(null, 'factory'), { text: '', html: '' });
    const v = A.availabilityVars(URL, 'factory');
    assert.equal(v.text, URL);
    assert.ok(v.html.startsWith('<p>') && v.html.includes(`href="${URL}"`));
    const p = A.availabilityVars(null, 'inspector', { placeholder: true });
    assert.equal(p.text, A.AVAILABILITY_PLACEHOLDER);
    assert.ok(p.html.includes(A.AVAILABILITY_PLACEHOLDER));
    // a body rendered with the token already carries the link: no second copy
    const body = `<div>${v.html}</div>`;
    assert.equal(A.appendAvailabilityHtml(body, URL, 'factory'), body);
});

// ── SOURCE_MISSING ──────────────────────────────────────────────────────────

test('SOURCE_MISSING / isSourceMissing', () => {
    assert.deepEqual([...SOURCE_MISSING].sort(), [1049, 1142, 1143, 1146]);
    assert.equal(isSourceMissing({ errno: 1146 }), true);
    assert.equal(isSourceMissing({ errno: 1213 }), false);
    assert.equal(isSourceMissing(null), false);
});

// ── the SQL helpers against a fake connection ───────────────────────────────

const missing = () => Object.assign(new Error("Table 'jportal.qc_availability_requests' doesn't exist"), { errno: 1146, code: 'ER_NO_SUCH_TABLE' });

function fakeConn(handler) {
    const calls = [];
    return {
        calls,
        async query(sql, params) {
            calls.push({ sql: sql.replace(/\s+/g, ' ').trim(), params });
            const out = await handler(calls[calls.length - 1].sql, params);
            return out === undefined ? [{ affectedRows: 1 }] : out;
        },
        async beginTransaction() { calls.push({ sql: 'BEGIN' }); },
        async commit() { calls.push({ sql: 'COMMIT' }); },
        async rollback() { calls.push({ sql: 'ROLLBACK' }); },
    };
}

test('syncAvailabilityLinks: nothing changed → no query', async () => {
    const conn = fakeConn(() => { throw new Error('no query expected'); });
    const out = await R.syncAvailabilityLinks(conn, { visitId: 7, before: visit(), after: { status: 'PROPOSED', visit_date: '2026-11-12' }, log: silentLog });
    assert.deepEqual(out, { revoked: [], moved: [] });
    assert.equal(conn.calls.length, 0);
    // a patch without status or visit_date (e.g. notes only) is "unchanged" too
    const out2 = await R.syncAvailabilityLinks(conn, { visitId: 7, before: visit(), after: { notes: 'x' }, log: silentLog });
    assert.deepEqual(out2, { revoked: [], moved: [] });
});

test('syncAvailabilityLinks: cancel revokes, bumps row_version, audits, inside a savepoint', async () => {
    const audits = [];
    const conn = fakeConn((sql) => {
        if (sql.startsWith('SELECT')) return [[link({ id: 1 }), link({ id: 2, party: 'inspector' })]];
        return undefined;
    });
    const out = await R.syncAvailabilityLinks(conn, {
        visitId: 7, before: visit(), after: { status: 'CANCELLED' }, userEmail: 'ops@example.com',
        recordAudit: async (c, a) => audits.push(a), log: silentLog, now: NOW,
    });
    assert.deepEqual(out.revoked, [{ id: 1, reason: 'cancelled' }, { id: 2, reason: 'cancelled' }]);
    const sqls = conn.calls.map(c => c.sql);
    assert.equal(sqls[0], 'SAVEPOINT qc_availability_sync');
    assert.match(sqls[1], /^SELECT \* FROM `jportal`\.qc_availability_requests WHERE visit_id = \? AND revoked_at IS NULL$/);
    assert.ok(sqls.filter(s => s.startsWith('UPDATE')).every(s => /revoke_reason = \?, row_version = row_version \+ 1/.test(s) && /revoked_at IS NULL/.test(s)));
    assert.equal(sqls[sqls.length - 1], 'RELEASE SAVEPOINT qc_availability_sync');
    assert.equal(sqls.some(s => /FOR UPDATE/.test(s)), false, 'no locking read on the jportal table');
    assert.equal(sqls.some(s => /available_days|answered_|none_available|answer_count|answer_ip/.test(s) && s.startsWith('UPDATE')), false);
    assert.deepEqual(audits, [{
        entityType: 'qc_visit', entityId: 7, action: 'availability_links_revoked',
        before: null, after: { linkIds: [1, 2], reason: 'cancelled' }, userEmail: 'ops@example.com',
    }]);
});

test('syncAvailabilityLinks: a date move inside the window updates proposed_date only', async () => {
    const conn = fakeConn(sql => (sql.startsWith('SELECT') ? [[link()]] : undefined));
    const out = await R.syncAvailabilityLinks(conn, { visitId: 7, before: visit(), after: { visit_date: '2026-11-14' }, log: silentLog, now: NOW });
    assert.deepEqual(out, { revoked: [], moved: [{ id: 1, proposedDate: '2026-11-14' }] });
    const upd = conn.calls.find(c => c.sql.startsWith('UPDATE'));
    assert.match(upd.sql, /SET proposed_date = \?, row_version = row_version \+ 1/);
    assert.deepEqual(upd.params, ['2026-11-14', 1]);
});

test('syncAvailabilityLinks: a missing jportal table is swallowed (rolled back to its savepoint)', async () => {
    const logged = [];
    const conn = fakeConn((sql) => { if (sql.startsWith('SELECT')) throw missing(); return undefined; });
    const out = await R.syncAvailabilityLinks(conn, {
        visitId: 7, before: visit(), after: { status: 'CANCELLED' },
        log: { ...silentLog, info: (...a) => logged.push(a) }, now: NOW,
    });
    assert.equal(out.skipped, true);
    assert.deepEqual(conn.calls.map(c => c.sql), ['SAVEPOINT qc_availability_sync', conn.calls[1].sql, 'ROLLBACK TO SAVEPOINT qc_availability_sync']);
    assert.equal(logged.length, 1);
});

test('syncAvailabilityLinks: another error is swallowed too; a lost transaction is rethrown', async () => {
    const warn = [];
    const conn = fakeConn((sql) => { if (sql.startsWith('UPDATE')) throw Object.assign(new Error('lock wait'), { errno: 1205 }); return sql.startsWith('SELECT') ? [[link()]] : undefined; });
    const out = await R.syncAvailabilityLinks(conn, { visitId: 7, before: visit(), after: { status: 'CANCELLED' }, log: { ...silentLog, warn: (...a) => warn.push(a) }, now: NOW });
    assert.equal(out.skipped, true);
    assert.equal(warn.length, 1);

    const deadlock = Object.assign(new Error('deadlock'), { errno: 1213 });
    const gone = fakeConn((sql) => {
        if (sql.startsWith('SELECT')) throw deadlock;
        if (sql.startsWith('ROLLBACK TO')) throw new Error('SAVEPOINT does not exist');
        return undefined;
    });
    await assert.rejects(R.syncAvailabilityLinks(gone, { visitId: 7, before: visit(), after: { status: 'CANCELLED' }, log: silentLog, now: NOW }), /deadlock/);
});

test('createOrReuseLink: refusals before any query', async () => {
    const conn = fakeConn(() => { throw new Error('no query expected'); });
    const code = async (v, opts) => {
        try { await R.createOrReuseLink(conn, v, 'factory', { portalUrl: 'https://p.example.com', today: '2026-11-10', ...opts }); }
        catch (e) { assert.ok(e instanceof R.AvailabilityFail); return [e.status, e.code, e.payload ? e.payload.field : undefined]; }
        return null;
    };
    assert.deepEqual(await code(visit(), { portalUrl: '' }), [409, 'PORTAL_URL_UNSET', undefined]);
    assert.deepEqual(await code(visit({ status: 'CANCELLED' })), [409, 'TERMINAL', undefined]);
    assert.deepEqual(await code(visit({ status: 'COMPLETED' })), [409, 'TERMINAL', undefined]);
    assert.deepEqual(await code(visit({ visit_date: null })), [409, 'NO_DATE', undefined]);
    assert.deepEqual(await code(visit(), { input: { windowEndDate: '2026-11-01' } }), [400, 'BAD_FIELD', 'windowEndDate']);
    assert.equal(conn.calls.length, 0);
});

test('createOrReuseLink: supersedes the open link and inserts a snapshot; audit carries no token', async () => {
    const audits = [];
    let inserted;
    const conn = fakeConn((sql, params) => {
        if (sql.startsWith('SELECT id FROM')) return [[{ id: 3 }]];
        if (sql.startsWith('INSERT')) { inserted = params; return [{ insertId: 11 }]; }
        if (sql.startsWith('SELECT * FROM')) return [[link({ id: 11, token: params && inserted ? inserted[0] : 'x' })]];
        return undefined;
    });
    const out = await R.createOrReuseLink(conn, visit(), 'inspector', {
        input: { recipientName: 'Mr Wang', windowEndDate: '2026-11-20' }, userEmail: 'ops@example.com',
        recordAudit: async (c, a) => audits.push(a), portalUrl: 'https://p.example.com', today: '2026-11-10', now: NOW,
    });
    assert.equal(out.created, true);
    assert.deepEqual(out.supersededIds, [3]);
    const sqls = conn.calls.map(c => c.sql);
    assert.match(sqls[1], /SET revoked_at = CURRENT_TIMESTAMP, revoke_reason = 'superseded', row_version = row_version \+ 1/);
    assert.match(inserted[0], /^[a-z0-9]{5}$/);
    assert.deepEqual(inserted.slice(1), [
        7, 'QCV_00007', 'inspector', 'Mr Wang', null, 'Acme Co., Ltd.', 'Plant 2', 'Dongguan',
        '2026-11-12', '2026-11-12', '2026-11-20', 'en', 30, 'ops@example.com',
    ]);
    assert.equal(audits.length, 1);
    assert.equal(audits[0].action, 'availability_link_created');
    assert.deepEqual(audits[0].after, {
        linkId: 11, party: 'inspector', windowStartDate: '2026-11-12', windowEndDate: '2026-11-20',
        proposedDate: '2026-11-12', expiresInDays: 30, supersededLinkIds: [3],
    });
    assert.equal(JSON.stringify(audits).includes(inserted[0]), false);
});

test('createOrReuseLink: reuse returns the live link; a missing table is 503 PORTAL_UNAVAILABLE', async () => {
    const live = link({ id: 5, answered_at: NOW });
    const conn = fakeConn(sql => (sql.startsWith('SELECT') ? [[link({ id: 6, expires_at: new Date('2026-11-01T00:00:00Z') }), live]] : undefined));
    const out = await R.createOrReuseLink(conn, visit(), 'factory', { reuse: true, portalUrl: 'https://p.example.com', today: '2026-11-10', now: NOW });
    assert.equal(out.created, false);
    assert.equal(out.row.id, 5);
    assert.equal(conn.calls.length, 1);

    const gone = fakeConn(() => { throw missing(); });
    await assert.rejects(
        R.createOrReuseLink(gone, visit(), 'factory', { portalUrl: 'https://p.example.com', today: '2026-11-10', now: NOW }),
        e => e instanceof R.AvailabilityFail && e.status === 503 && e.code === 'PORTAL_UNAVAILABLE' && e.error === R.PORTAL_UNAVAILABLE_MESSAGE
    );
});

test('emailAvailabilityLink: preview creates nothing; send reuses or creates in its own transaction', async () => {
    const portalUrl = 'https://p.example.com';
    const none = fakeConn(() => [[]]);
    assert.deepEqual(await R.emailAvailabilityLink(none, visit(), 'factory', { portalUrl, now: NOW }), { url: null, linkId: null, created: false, enabled: true });
    assert.equal(none.calls.some(c => /^(INSERT|UPDATE)/.test(c.sql)), false);
    const unset = fakeConn(() => { throw new Error('no query expected'); });
    const u = await R.emailAvailabilityLink(unset, visit(), 'factory', { portalUrl: '', now: NOW });
    assert.equal(u.url, null);
    assert.equal(u.enabled, false, 'portal URL unset → not enabled (no placeholder in the preview)');
    assert.equal(u.unavailable, undefined);
    const missingConn = fakeConn(() => { throw missing(); });
    const m = await R.emailAvailabilityLink(missingConn, visit(), 'factory', { portalUrl, now: NOW, log: silentLog });
    assert.equal(m.url, null);
    assert.equal(m.enabled, false, 'table missing → not enabled');
    assert.equal(m.unavailable, true);

    const existing = fakeConn(() => [[link({ id: 5 })]]);
    const p = await R.emailAvailabilityLink(existing, visit(), 'factory', { portalUrl, now: NOW });
    assert.equal(p.url, `${portalUrl}/a/${'a'.repeat(48)}`);

    let inserted;
    const send = fakeConn((sql, params) => {
        if (sql.startsWith('SELECT * FROM qc_visits')) return [[visit()]];
        if (sql.startsWith('SELECT * FROM') && /party = \?/.test(sql)) return [[]];
        if (sql.startsWith('SELECT id FROM')) return [[]];
        if (sql.startsWith('INSERT')) { inserted = params; return [{ insertId: 12 }]; }
        if (sql.startsWith('SELECT * FROM')) return [[link({ id: 12, token: inserted[0] })]];
        return undefined;
    });
    const s = await R.emailAvailabilityLink(send, visit(), 'factory', {
        create: true, to: ['li@example.com', 'x@example.com'], portalUrl, now: NOW,
    });
    assert.equal(s.created, true);
    assert.equal(s.url, `${portalUrl}/a/${inserted[0]}`);
    assert.equal(inserted[4], 'Ms Li');
    assert.equal(inserted[5], 'li@example.com');
    assert.equal(inserted[11], '2026-11-17', 'default window');
    assert.equal(send.calls[0].sql, 'BEGIN');
    assert.equal(send.calls[1].sql, 'SELECT * FROM qc_visits WHERE id = ? FOR UPDATE', 'the visit is locked first');
    assert.equal(send.calls[send.calls.length - 1].sql, 'COMMIT');
});

test('emailAvailabilityLink (send): TERMINAL / NO_DATE / NOT_FOUND are checked on the freshly locked row', async () => {
    const portalUrl = 'https://p.example.com';
    const run = async (fresh) => {
        const conn = fakeConn((sql) => {
            if (sql.startsWith('SELECT * FROM qc_visits')) return [fresh ? [fresh] : []];
            throw new Error(`unexpected ${sql}`);
        });
        try {
            // the caller's copy is stale: still PROPOSED with a date
            await R.emailAvailabilityLink(conn, visit(), 'factory', { create: true, to: ['li@example.com'], portalUrl, now: NOW });
        } catch (e) {
            assert.ok(e instanceof R.AvailabilityFail);
            assert.equal(conn.calls[conn.calls.length - 1].sql, 'ROLLBACK');
            return `${e.status} ${e.code}`;
        }
        return 'ok';
    };
    assert.equal(await run(visit({ status: 'CANCELLED' })), '409 TERMINAL');
    assert.equal(await run(visit({ visit_date: null })), '409 NO_DATE');
    assert.equal(await run(null), '404 NOT_FOUND');
});

test('createOrReuseLink (reuse): a live custom-window link is reused even when the visit date is past (no BAD_FIELD)', async () => {
    // visit date 2026-11-01, today 2026-11-10: a new default window (…-11-06)
    // would be refused, but the live link (window to 2026-11-20) is reused.
    const live = link({ id: 8, proposed_date: '2026-11-01', window_start_date: '2026-11-01', window_end_date: '2026-11-20' });
    const conn = fakeConn(sql => (sql.startsWith('SELECT') ? [[live]] : undefined));
    const out = await R.createOrReuseLink(conn, visit({ visit_date: '2026-11-01' }), 'factory', {
        reuse: true, input: { recipientEmail: 'li@example.com' }, portalUrl: 'https://p.example.com', today: '2026-11-10', now: NOW,
    });
    assert.equal(out.created, false);
    assert.equal(out.row.id, 8);
    // with no live link the same call is validated and refused
    const none = fakeConn(sql => (sql.startsWith('SELECT') ? [[]] : undefined));
    await assert.rejects(
        R.createOrReuseLink(none, visit({ visit_date: '2026-11-01' }), 'factory', { reuse: true, portalUrl: 'https://p.example.com', today: '2026-11-10', now: NOW }),
        e => e instanceof R.AvailabilityFail && e.code === 'BAD_FIELD' && e.payload.field === 'windowEndDate'
    );
    assert.equal(none.calls.some(c => c.sql.startsWith('INSERT')), false);
    // a bad party is refused before any lookup
    const noQuery = fakeConn(() => { throw new Error('no query expected'); });
    await assert.rejects(
        R.createOrReuseLink(noQuery, visit(), 'supplier', { reuse: true, portalUrl: 'https://p.example.com', today: '2026-11-10', now: NOW }),
        e => e.code === 'BAD_FIELD' && e.payload.field === 'party'
    );
});

test('linkSyncPatch: expired links are revoked too (status and date_moved), but never moved', () => {
    const expired = link({ id: 4, expires_at: new Date('2026-11-01T00:00:00Z'), answered_at: new Date('2026-10-30T00:00:00Z'), available_days: ['2026-11-13'] });
    const before = { status: 'PROPOSED', visitDate: '2026-11-12' };
    assert.deepEqual(A.linkSyncPatch([expired], { before, after: { status: 'CANCELLED', visitDate: '2026-11-12' } }, NOW),
        [{ id: 4, action: 'revoke', reason: 'cancelled' }]);
    assert.deepEqual(A.linkSyncPatch([expired], { before, after: { status: 'PROPOSED', visitDate: '2026-11-30' } }, NOW),
        [{ id: 4, action: 'revoke', reason: 'date_moved' }]);
    assert.deepEqual(A.linkSyncPatch([expired], { before, after: { status: 'PROPOSED', visitDate: '2026-11-14' } }, NOW), [],
        'an expired link inside the window is not moved');
});

test('cancel: an expired but answered link is revoked, so collate stops showing its answer', async () => {
    const expired = link({ id: 4, expires_at: new Date('2026-11-01T00:00:00Z'), answered_at: new Date('2026-10-30T00:00:00Z'), available_days: ['2026-11-13'] });
    assert.equal(A.collate([expired], NOW).parties.factory.linkId, 4, 'counted before the cancel');
    const rows = [expired];
    const conn = fakeConn((sql, params) => {
        if (sql.startsWith('SELECT')) return [rows.filter(r => r.revoked_at == null)];
        if (sql.startsWith('UPDATE')) {
            const r = rows.find(x => x.id === params[1]);
            r.revoked_at = NOW;
            r.revoke_reason = params[0];
            return [{ affectedRows: 1 }];
        }
        return undefined;
    });
    const out = await R.syncAvailabilityLinks(conn, { visitId: 7, before: visit(), after: { status: 'CANCELLED' }, log: silentLog, now: NOW });
    assert.deepEqual(out.revoked, [{ id: 4, reason: 'cancelled' }]);
    const c = A.collate(rows, NOW);
    assert.equal(c.parties.factory, null);
    assert.deepEqual(c.days, []);
});

test('appendAvailabilityHtml / Text: an older availability URL in the body becomes the current one', () => {
    const oldUrl = `https://portal.example.com/availability/${'b'.repeat(48)}`;
    const newUrl = `https://portal.example.com/a/${'c'.repeat(5)}`;
    const html = `<p>Hi</p><p>Tick here:<br><a href="${oldUrl}">${oldUrl}</a></p>`;
    const out = A.appendAvailabilityHtml(html, newUrl, 'factory');
    assert.equal(out, `<p>Hi</p><p>Tick here:<br><a href="${newUrl}">${newUrl}</a></p>`, 'replaced in place, nothing appended');
    assert.ok(!out.includes(oldUrl));
    // a bare path (no host) is replaced too
    const path = A.appendAvailabilityHtml(`<p>/availability/${'b'.repeat(48)}</p>`, newUrl, 'inspector');
    assert.equal(path, `<p>${newUrl}</p>`);
    const text = A.appendAvailabilityText(`Hi\n\nTick here: ${oldUrl}`, newUrl, 'inspector');
    assert.equal(text, `Hi\n\nTick here: ${newUrl}`);
    // a 48-hex token that is part of a longer hex string is not a link
    const longer = `<p>/availability/${'b'.repeat(60)}</p>`;
    assert.ok(A.appendAvailabilityHtml(longer, newUrl, 'inspector').startsWith(longer));
});

test('stripAvailabilityPlaceholder*: a send without a link never carries the placeholder', () => {
    const body = '<div><p>Hello</p></div>';
    for (const aud of ['factory', 'inspector']) {
        const preview = A.appendAvailabilityHtml(body, null, aud);
        assert.ok(preview.includes(A.AVAILABILITY_PLACEHOLDER));
        assert.equal(A.stripAvailabilityPlaceholderHtml(preview), body, 'the whole placeholder paragraph goes');
    }
    // edited by staff: the bare text (escaped or not) anywhere
    const edited = `<p>See ${A.AVAILABILITY_PLACEHOLDER} and <em>(availability link will be created when sent)</em></p>`;
    const stripped = A.stripAvailabilityPlaceholderHtml(edited);
    assert.ok(!stripped.includes(A.AVAILABILITY_PLACEHOLDER));
    assert.equal(A.stripAvailabilityPlaceholderHtml(body), body, 'no placeholder → unchanged');
    const text = A.appendAvailabilityText('Hello', null, 'factory');
    assert.equal(A.stripAvailabilityPlaceholderText(text), 'Hello');
    assert.equal(A.stripAvailabilityPlaceholderText(`a ${A.AVAILABILITY_PLACEHOLDER} b`), 'a  b');
});
