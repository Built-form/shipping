'use strict';

// Unit tests for src/lib/qc-visits.js — the pure rules behind QC visits
// (/api/v1/qc-visits, /qc-factories, /qc-inspectors): code lists,
// validators, the AQL table, remaining-to-inspect maths, readiness warnings
// per stage, the clearing rule, confirmations and the state machine.
// No database.
//   node --test tools/test-qc-visits-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/qc-visits');

// ── fixtures ────────────────────────────────────────────────────────────────

const order = (id, extra = {}) => ({
    id, quantity: 1000, status: 'READY_FOR_QC', supplier: 'Acme Co., Ltd.', estimatedReadyDate: '2026-11-01',
    actualReadyDate: null, jfCode: `JF${id}`, poNumber: `PO${id}`, purchaseOrderId: 900 + id, deleted: false, ...extra,
});

// A clean ctx: one line, nothing to warn about at any stage.
function ctx(over = {}) {
    const base = {
        visit: {
            id: 10, status: 'DRAFT', visitDate: '2026-11-12', windowStart: '09:30', windowEnd: '12:00',
            supplierName: '  ACME co.,   Ltd. ', factory: { id: 3, deleted: false, city: 'Dongguan' }, inspector: { id: 1, deleted: false },
        },
        lines: [{ orderId: 1, quantityToInspect: 200, poQuantitySnapshot: 1000 }],
        moves: [],
        orders: { 1: order(1) },
        otherLines: [],
        fqcSiblingParents: [],
        repackable: { JF1: true },
        kitStatus: { JF1: 'docs' },
        inspectorSameDay: [],
        acknowledge: [],
    };
    const out = { ...base, ...over };
    if (over.visit) out.visit = { ...base.visit, ...over.visit };
    return out;
}

const keysOf = ws => ws.map(w => w.key);
const find = (ws, key, orderId) => ws.find(w => w.key === key && (orderId === undefined || w.orderId === orderId));

// ── code lists ──────────────────────────────────────────────────────────────

test('code lists are exactly the contract\'s', () => {
    assert.deepEqual(L.VISIT_STATUSES, ['DRAFT', 'PROPOSED', 'CONFIRMED', 'COMPLETED', 'CANCELLED']);
    assert.deepEqual(L.OPEN_STATUSES, ['DRAFT', 'PROPOSED', 'CONFIRMED']);
    assert.deepEqual(L.TERMINAL_STATUSES, ['COMPLETED', 'CANCELLED']);
    assert.deepEqual(L.CANDIDATE_ORDER_STATUSES, ['IN_PRODUCTION', 'READY_FOR_QC']);
    assert.deepEqual(L.PRODUCTION_STATES, ['FINISHED_PACKED', 'FINISHED_UNPACKED', 'PARTIAL_80', 'INLINE', 'PRE_PRODUCTION']);
    assert.deepEqual(Object.keys(L.PRODUCTION_STATE_LABELS), L.PRODUCTION_STATES);
    assert.equal(L.PRODUCTION_STATE_LABELS.FINISHED_PACKED, '100% finished and packed in export cartons');
    assert.equal(L.PRODUCTION_STATE_LABELS.PRE_PRODUCTION, 'Before production (materials / first-off)');
    assert.deepEqual(L.CANCEL_CATEGORIES, ['schedule_change', 'factory_not_ready', 'inspector_no_show', 'goods_not_at_site', 'order_changed', 'duplicate', 'other']);
    assert.deepEqual(Object.keys(L.CANCEL_CATEGORY_LABELS), L.CANCEL_CATEGORIES);
    assert.equal(L.CANCEL_CATEGORY_LABELS.factory_not_ready, 'Factory not ready (wasted trip)');
    assert.deepEqual(L.WASTED_TRIP_CATEGORIES, ['factory_not_ready', 'goods_not_at_site']);
    assert.deepEqual(L.SAMPLE_METHODS, ['full', 'aql', 'custom', 'destructive']);
    assert.deepEqual(L.PARTIES, ['factory', 'inspector']);
    assert.deepEqual(L.CONFIRMATION_KINDS, ['proposed', 'confirmed', 'declined', 'counter']);
    assert.deepEqual(L.CHANNELS, ['email', 'wechat', 'phone', 'portal', 'other']);
    assert.deepEqual(L.INSPECTOR_KINDS, ['internal', 'agency']);
    assert.deepEqual(L.EDITOR_ROLES, ['admin', 'standard']);
});

// ── small helpers ───────────────────────────────────────────────────────────

test('admin and standard users edit; accountants and the rest read', () => {
    assert.equal(L.canEdit('admin'), true);
    assert.equal(L.canEdit('standard'), true);
    for (const t of ['accountant', 'warehouse', 'exec', '', undefined, null]) assert.equal(L.canEdit(t), false, String(t));
});

test('isYmd: real dates only; isHm: HH:MM from 00:00 to 23:59', () => {
    for (const ok of ['2026-11-12', '2028-02-29', '2026-12-31']) assert.equal(L.isYmd(ok), true, ok);
    for (const bad of ['2026-02-29', '2026-13-01', '2026-1-1', '12/11/2026', '', null, 20261112, '2026-11-12T00:00']) assert.equal(L.isYmd(bad), false, String(bad));
    for (const ok of ['00:00', '09:30', '23:59', '12:00']) assert.equal(L.isHm(ok), true, ok);
    for (const bad of ['24:00', '9:30', '09:60', '09:30:00', '', null, 930]) assert.equal(L.isHm(bad), false, String(bad));
});

test('normSupplier, normJfCode, isFqcCode, formatRef', () => {
    assert.equal(L.normSupplier('  Acme   Co.,\tLtd. '), 'acme co., ltd.');
    assert.equal(L.normSupplier(null), '');
    assert.equal(L.normJfCode(' jf123_fqc '), 'JF123');
    assert.equal(L.normJfCode('JF123_FQC'), 'JF123');
    assert.equal(L.normJfCode('jf_fqc_x'), 'JF_FQC_X', 'only a trailing _FQC is stripped');
    assert.equal(L.normJfCode(undefined), '');
    assert.equal(L.isFqcCode('JF1_FQC'), true);
    assert.equal(L.isFqcCode(' jf1_fqc '), true);
    assert.equal(L.isFqcCode('JF1'), false);
    assert.equal(L.isFqcCode(null), false);
    assert.equal(L.formatRef(1), 'QCV_00001');
    assert.equal(L.formatRef(12345), 'QCV_12345');
    assert.equal(L.formatRef(123456), 'QCV_123456');
});

test('chinaToday is the date at +08:00: it turns over at 16:00 UTC', () => {
    assert.equal(L.chinaToday(new Date('2026-10-27T15:59:59.999Z')), '2026-10-27');
    assert.equal(L.chinaToday(new Date('2026-10-27T16:00:00.000Z')), '2026-10-28');
    assert.equal(L.chinaToday(new Date('2026-10-27T23:59:59Z')), '2026-10-28');
    assert.equal(L.chinaToday(new Date('2026-10-28T00:00:00Z')), '2026-10-28');
    assert.equal(L.chinaToday(new Date('2026-12-31T16:00:00Z')), '2027-01-01');
    assert.equal(L.chinaToday('2026-03-29T01:00:00Z'), '2026-03-29', 'UK clock changes do not matter');
    assert.match(L.chinaToday(), /^\d{4}-\d{2}-\d{2}$/);
});

// ── AQL ─────────────────────────────────────────────────────────────────────

test('aqlSampleSize: ISO 2859-1 general level II at every lot-size boundary', () => {
    const cases = [
        [2, 2], [8, 2], [9, 3], [15, 3], [16, 5], [25, 5], [26, 8], [50, 8], [51, 13], [90, 13],
        [91, 20], [150, 20], [151, 32], [280, 32], [281, 50], [500, 50], [501, 80], [1200, 80],
        [1201, 125], [3200, 125], [3201, 200], [10000, 200], [10001, 315], [35000, 315],
        [35001, 500], [150000, 500], [150001, 800], [500000, 800], [500001, 1250], [5000000, 1250],
    ];
    for (const [lot, n] of cases) {
        assert.equal(L.aqlSampleSize(lot), n, `GII lot ${lot}`);
        assert.equal(L.aqlSampleSize(lot, 'GII'), n, `GII lot ${lot}`);
    }
});

test('aqlSampleSize: levels I and III use their own code letters', () => {
    const gi = [[8, 2], [15, 2], [25, 3], [50, 5], [90, 5], [150, 8], [280, 13], [500, 20], [1200, 32], [3200, 50],
        [10000, 80], [35000, 125], [150000, 200], [500000, 315], [500001, 500]];
    for (const [lot, n] of gi) assert.equal(L.aqlSampleSize(lot, 'GI'), n, `GI lot ${lot}`);
    const giii = [[8, 3], [15, 5], [25, 8], [50, 13], [90, 20], [150, 32], [280, 50], [500, 80], [1200, 125], [3200, 200],
        [10000, 315], [35000, 500], [150000, 800], [500000, 1250], [500001, 2000]];
    for (const [lot, n] of giii) assert.equal(L.aqlSampleSize(lot, 'GIII'), n, `GIII lot ${lot}`);
    assert.equal(L.aqlCodeLetter(1000, 'GII'), 'J');
    assert.equal(L.aqlCodeLetter(1000, 'GIII'), 'K');
    assert.equal(L.aqlCodeLetter(1000, 'GI'), 'G');
});

test('aqlSampleSize: capped at the lot, 0 for an empty lot, level spellings', () => {
    assert.equal(L.aqlSampleSize(2, 'GIII'), 2, 'code B is 3 units, but the lot is 2');
    assert.equal(L.aqlSampleSize(1), 1);
    assert.equal(L.aqlSampleSize(0), 0);
    assert.equal(L.aqlSampleSize(-5), 0);
    assert.equal(L.aqlSampleSize(NaN), 0);
    assert.equal(L.aqlSampleSize(null), 0);
    assert.equal(L.aqlSampleSize('1000'), 80);
    assert.equal(L.aqlSampleSize(1000.9), 80);
    assert.equal(L.aqlSampleSize(1000, 'II'), 80);
    assert.equal(L.aqlSampleSize(1000, 'iii'), 125);
    assert.equal(L.aqlSampleSize(1000, 'G-I'), 32);
    assert.equal(L.aqlSampleSize(1000, 'Level III'), 125);
    assert.equal(L.aqlSampleSize(1000, 'bogus'), 80, 'unknown level falls back to GII');
});

// ── remaining ───────────────────────────────────────────────────────────────

test('remainingToInspect: quantity less what open visits book, never below 0', () => {
    assert.equal(L.remainingToInspect(1000, []), 1000);
    assert.equal(L.remainingToInspect(1000, [200, 300]), 500);
    assert.equal(L.remainingToInspect(1000, [600, 600]), 0);
    assert.equal(L.remainingToInspect(1000, undefined), 1000);
    assert.equal(L.remainingToInspect(null, [5]), 0);
    assert.equal(L.remainingToInspect('100', ['30', null]), 70);
});

test('re-inspection: completed visits do not reduce remaining, open ones do', () => {
    const otherLines = [
        { visitId: 7, ref: 'QCV_00007', status: 'COMPLETED', visitDate: '2026-10-01', orderId: 1, quantityToInspect: 1000 },
        { visitId: 8, ref: 'QCV_00008', status: 'CANCELLED', visitDate: '2026-10-05', orderId: 1, quantityToInspect: 1000 },
    ];
    let ws = L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 1000, poQuantitySnapshot: 1000 }], otherLines }), 'create');
    assert.equal(find(ws, 'qty_exceeds'), undefined, 'a clone of a completed visit is not blocked by its source');
    assert.ok(find(ws, 'already_inspected', 1));
    otherLines.push({ visitId: 9, ref: 'QCV_00009', status: 'PROPOSED', visitDate: '2026-11-20', orderId: 1, quantityToInspect: 400 });
    ws = L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 601, poQuantitySnapshot: 1000 }], otherLines }), 'create');
    const w = find(ws, 'qty_exceeds', 1);
    assert.equal(w.level, 'block');
    assert.deepEqual(w.meta, { quantityToInspect: 601, remaining: 600, quantity: 1000 });
    ws = L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 600, poQuantitySnapshot: 1000 }], otherLines }), 'create');
    assert.equal(find(ws, 'qty_exceeds'), undefined, 'exactly the remainder is fine');
});

test('the visit\'s own lines never count against it (otherLines rows with its id are ignored)', () => {
    const otherLines = [{ visitId: 10, ref: 'QCV_00010', status: 'DRAFT', visitDate: '2026-11-12', orderId: 1, quantityToInspect: 1000 }];
    const ws = L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 1000, poQuantitySnapshot: 1000 }], otherLines }), 'view');
    assert.deepEqual(ws, []);
});

// ── validators ──────────────────────────────────────────────────────────────

test('validateVisitFields: camelCase in, qc_visits columns out', () => {
    const r = L.validateVisitFields({
        supplierName: '  Acme  ', factoryId: 3, inspectorId: '4', visitDate: '2026-11-12', windowStart: '09:30', windowEnd: '12:00',
        expectedProductionState: 'FINISHED_PACKED', reportDeadlineHours: 48, escalationNote: ' call Li ', notes: '',
        factory: { label: 'Main plant', address: '1 Road', city: 'Dongguan', province: 'Guangdong', contact: { name: 'Li', title: 'QC', email: 'li@acme.cn', wechat: 'li_w', phone: '+86 1' } },
        inspector: { name: 'Wang', company: 'AQI', email: 'wang@aqi.com', wechat: null, phone: '' },
        unknownKey: 'ignored',
    });
    assert.equal(r.error, undefined, r.error);
    assert.deepEqual(r.value, {
        supplier_name: 'Acme', factory_id: 3, inspector_id: 4, visit_date: '2026-11-12', window_start: '09:30', window_end: '12:00',
        expected_production_state: 'FINISHED_PACKED', report_deadline_hours: 48, escalation_note: 'call Li', notes: null,
        factory_label: 'Main plant', factory_address: '1 Road', factory_city: 'Dongguan', factory_province: 'Guangdong',
        factory_contact_name: 'Li', factory_contact_title: 'QC', factory_contact_email: 'li@acme.cn', factory_contact_wechat: 'li_w', factory_contact_phone: '+86 1',
        inspector_name: 'Wang', inspector_company: 'AQI', inspector_email: 'wang@aqi.com', inspector_wechat: null, inspector_phone: null,
    });
});

test('validateVisitFields: create needs supplierName; partial takes only what is sent', () => {
    assert.deepEqual(L.validateVisitFields({}), { error: 'supplierName is required.', field: 'supplierName' });
    assert.equal(L.validateVisitFields(null).field, 'supplierName');
    assert.deepEqual(L.validateVisitFields({ supplierName: 'A' }).value, { supplier_name: 'A' });
    assert.deepEqual(L.validateVisitFields({}, { partial: true }).value, {});
    assert.deepEqual(L.validateVisitFields({ notes: 'x', visitDate: null }, { partial: true }).value, { notes: 'x', visit_date: null });
    assert.deepEqual(L.validateVisitFields({ factory: { contact: { phone: '1' } } }, { partial: true }).value, { factory_contact_phone: '1' });
    assert.equal(L.validateVisitFields({ supplierName: '   ' }, { partial: true }).field, 'supplierName', 'cannot blank the supplier');
});

test('validateVisitFields: bad values name their field', () => {
    const bad = [
        [{ visitDate: '2026-02-30' }, 'visitDate'],
        [{ windowStart: '9:30' }, 'windowStart'],
        [{ windowEnd: '24:00' }, 'windowEnd'],
        [{ factoryId: 0 }, 'factoryId'],
        [{ inspectorId: 'abc' }, 'inspectorId'],
        [{ expectedProductionState: 'DONE' }, 'expectedProductionState'],
        [{ reportDeadlineHours: 0 }, 'reportDeadlineHours'],
        [{ reportDeadlineHours: 241 }, 'reportDeadlineHours'],
        [{ reportDeadlineHours: 2.5 }, 'reportDeadlineHours'],
        [{ escalationNote: 'x'.repeat(501) }, 'escalationNote'],
        [{ notes: 5 }, 'notes'],
        [{ factory: 'x' }, 'factory'],
        [{ factory: { label: 'x'.repeat(101) } }, 'factory.label'],
        [{ factory: { contact: { email: 'not-an-email' } } }, 'factory.contact.email'],
        [{ inspector: { email: 'a@b' } }, 'inspector.email'],
        [{ inspector: { name: 'x'.repeat(256) } }, 'inspector.name'],
    ];
    for (const [body, field] of bad) {
        const r = L.validateVisitFields(body, { partial: true });
        assert.equal(r.field, field, JSON.stringify(body));
        assert.ok(r.error);
    }
    assert.equal(L.validateVisitFields({ reportDeadlineHours: 1 }, { partial: true }).value.report_deadline_hours, 1);
    assert.equal(L.validateVisitFields({ reportDeadlineHours: 240 }, { partial: true }).value.report_deadline_hours, 240);
    assert.equal(L.validateVisitFields({ reportDeadlineHours: null }, { partial: true }).value.report_deadline_hours, null);
});

test('validateVisitFields: windowEnd must be later than windowStart', () => {
    assert.deepEqual(L.validateVisitFields({ supplierName: 'A', windowStart: '12:00', windowEnd: '09:00' }),
        { error: 'windowEnd must be later than windowStart.', field: 'windowEnd' });
    assert.equal(L.validateVisitFields({ supplierName: 'A', windowStart: '09:00', windowEnd: '09:00' }).field, 'windowEnd', 'equal is not later');
    assert.ok(L.validateVisitFields({ supplierName: 'A', windowStart: '09:00', windowEnd: '09:01' }).value);
    assert.ok(L.validateVisitFields({ supplierName: 'A', windowStart: '09:00' }).value, 'one end alone is fine');
    // A PATCH of one end is checked against the stored other end.
    const current = { window_start: '10:00:00', window_end: '12:00:00' };
    assert.equal(L.validateVisitFields({ windowEnd: '09:00' }, { partial: true, current }).field, 'windowEnd');
    assert.equal(L.validateVisitFields({ windowStart: '13:00' }, { partial: true, current }).field, 'windowEnd');
    assert.ok(L.validateVisitFields({ windowStart: '11:00' }, { partial: true, current }).value);
    assert.ok(L.validateVisitFields({ windowStart: null }, { partial: true, current }).value, 'clearing an end is fine');
    assert.ok(L.validateVisitFields({ notes: 'x' }, { partial: true, current: { window_start: '12:00', window_end: '09:00' } }).value,
        'an untouched window is not re-judged');
});

test('validateLines: normalised lines and moves', () => {
    const r = L.validateLines([
        { orderId: 1, quantityToInspect: 80 },
        { orderId: '2', quantityToInspect: 5, sampleMethod: 'aql', aql: { level: 'GII', critical: 0, major: '2.5', minor: 4 }, note: '  n ' },
        { orderId: 3, quantityToInspect: 3, sampleMethod: 'destructive' },
    ], [{ orderId: 3, toFactoryId: 5, moveBy: '2026-11-10', arrangedBy: 'Factory', note: null }]);
    assert.equal(r.error, undefined, r.error);
    assert.deepEqual(r.lines[0], { orderId: 1, quantityToInspect: 80, sampleMethod: 'full', aql: null, note: null });
    assert.deepEqual(r.lines[1], { orderId: 2, quantityToInspect: 5, sampleMethod: 'aql', aql: { level: 'GII', critical: 0, major: 2.5, minor: 4 }, note: 'n' });
    assert.deepEqual(r.lines[2], { orderId: 3, quantityToInspect: 3, sampleMethod: 'destructive', aql: null, note: null });
    assert.deepEqual(r.moves, [{ orderId: 3, toFactoryId: 5, moveBy: '2026-11-10', arrangedBy: 'Factory', note: null }]);
    assert.deepEqual(L.validateLines([{ orderId: 1, quantityToInspect: 1 }]).moves, [], 'moves are optional');
});

test('validateLines: refusals', () => {
    const line = (o = {}) => ({ orderId: 1, quantityToInspect: 1, ...o });
    const cases = [
        [null, undefined, 'lines'],
        [[], undefined, 'lines'],
        [Array.from({ length: 501 }, (_, i) => line({ orderId: i + 1 })), undefined, 'lines'],
        [[line(), line()], undefined, 'lines[1].orderId'],
        [[line({ orderId: 0 })], undefined, 'lines[0].orderId'],
        [[line({ orderId: undefined })], undefined, 'lines[0].orderId'],
        [[line({ quantityToInspect: 0 })], undefined, 'lines[0].quantityToInspect'],
        [[line({ quantityToInspect: 1.5 })], undefined, 'lines[0].quantityToInspect'],
        [[line({ quantityToInspect: '5' })], undefined, 'lines[0].quantityToInspect'],
        [[line({ sampleMethod: 'some' })], undefined, 'lines[0].sampleMethod'],
        [[line({ aql: { major: 120 } })], undefined, 'lines[0].aql.major'],
        [[line({ note: 'x'.repeat(501) })], undefined, 'lines[0].note'],
        [[line()], 'x', 'moves'],
        [[line()], [{ orderId: 2 }], 'moves[0].orderId'],
        [[line()], [{ orderId: 1 }, { orderId: 1 }], 'moves[1].orderId'],
        [[line()], [{ orderId: 1, moveBy: '2026-11-31' }], 'moves[0].moveBy'],
        [[line()], [{ orderId: 1, toFactoryId: -1 }], 'moves[0].toFactoryId'],
    ];
    for (const [lines, moves, field] of cases) {
        const r = L.validateLines(lines, moves);
        assert.equal(r.field, field, `${JSON.stringify(lines).slice(0, 80)} / ${JSON.stringify(moves)}`);
        assert.ok(r.error);
    }
    assert.ok(L.validateLines(Array.from({ length: 500 }, (_, i) => line({ orderId: i + 1 }))).lines, '500 lines is the limit');
});

test('validateConfirmation: columns out; a counter needs its date; counter fields only on a counter', () => {
    assert.deepEqual(L.validateConfirmation({ party: 'factory', kind: 'confirmed', channel: 'wechat', onDate: '2026-11-01', byName: ' Li ', counterDate: '2026-11-20' }).value, {
        party: 'factory', kind: 'confirmed', channel: 'wechat', on_date: '2026-11-01',
        counter_date: null, counter_window_start: null, counter_window_end: null, by_name: 'Li', note: null,
    });
    assert.deepEqual(L.validateConfirmation({ party: 'inspector', kind: 'counter', channel: 'email', counterDate: '2026-11-20', counterWindowStart: '10:00', counterWindowEnd: '15:00' }).value, {
        party: 'inspector', kind: 'counter', channel: 'email', on_date: null,
        counter_date: '2026-11-20', counter_window_start: '10:00', counter_window_end: '15:00', by_name: null, note: null,
    });
    const bad = [
        [{ kind: 'confirmed', channel: 'email' }, 'party'],
        [{ party: 'buyer', kind: 'confirmed', channel: 'email' }, 'party'],
        [{ party: 'factory', channel: 'email' }, 'kind'],
        [{ party: 'factory', kind: 'maybe', channel: 'email' }, 'kind'],
        [{ party: 'factory', kind: 'confirmed', channel: 'fax' }, 'channel'],
        [{ party: 'factory', kind: 'confirmed', channel: 'email', onDate: '1/11/2026' }, 'onDate'],
        [{ party: 'factory', kind: 'counter', channel: 'email' }, 'counterDate'],
        [{ party: 'factory', kind: 'counter', channel: 'email', counterDate: '2026-11-20', counterWindowStart: '15:00', counterWindowEnd: '10:00' }, 'counterWindowEnd'],
        [{ party: 'factory', kind: 'confirmed', channel: 'email', note: 'x'.repeat(501) }, 'note'],
        [{ party: 'factory', kind: 'confirmed', channel: 'email', byName: 'x'.repeat(256) }, 'byName'],
    ];
    for (const [body, field] of bad) assert.equal(L.validateConfirmation(body).field, field, JSON.stringify(body));
    assert.equal(L.validateConfirmation(null).field, 'party');
});

test('validateFactory and validateInspector', () => {
    assert.deepEqual(L.validateFactory({ supplierName: 'Acme', label: 'Main', city: 'Dongguan', contact: { name: 'Li', email: 'li@acme.cn' }, isDefault: true }).value, {
        supplier_name: 'Acme', label: 'Main', city: 'Dongguan', contact_name: 'Li', contact_email: 'li@acme.cn', is_default: 1,
    });
    assert.equal(L.validateFactory({ supplierName: 'Acme' }).field, 'label');
    assert.equal(L.validateFactory({ label: 'Main' }).field, 'supplierName');
    assert.deepEqual(L.validateFactory({ isDefault: false, supplierCode: ' S01 ' }, { partial: true }).value, { is_default: 0, supplier_code: 'S01' });
    assert.equal(L.validateFactory({ isDefault: 'yes' }, { partial: true }).field, 'isDefault');
    assert.equal(L.validateFactory({ contact: { email: 'x' } }, { partial: true }).field, 'contact.email');
    assert.equal(L.validateFactory({ supplierCode: 'x'.repeat(65) }, { partial: true }).field, 'supplierCode');

    assert.deepEqual(L.validateInspector({ name: ' Wang ' }).value, { name: 'Wang', kind: 'agency' });
    assert.deepEqual(L.validateInspector({ name: 'Zhou', kind: 'internal', baseCity: 'Ningbo', active: false, email: 'z@x.com' }).value,
        { name: 'Zhou', kind: 'internal', base_city: 'Ningbo', active: 0, email: 'z@x.com' });
    assert.deepEqual(L.validateInspector({ active: true }, { partial: true }).value, { active: 1 });
    assert.equal(L.validateInspector({}).field, 'name');
    assert.equal(L.validateInspector({ name: 'A', kind: 'freelance' }).field, 'kind');
    assert.equal(L.validateInspector({ email: 'nope' }, { partial: true }).field, 'email');
});

// ── confirmations ───────────────────────────────────────────────────────────

test('latestByParty: newest row per party by created_at then id; proposed rows ignored', () => {
    const rows = [
        { id: 5, party: 'factory', kind: 'confirmed', created_at: '2026-11-02 10:00:00.000' },
        { id: 2, party: 'factory', kind: 'declined', created_at: '2026-11-01 10:00:00.000' },
        { id: 7, party: 'factory', kind: 'proposed', created_at: '2026-11-03 10:00:00.000' },
        { id: 3, party: 'inspector', kind: 'confirmed', created_at: new Date('2026-11-01T09:00:00Z') },
        { id: 4, party: 'inspector', kind: 'counter', created_at: new Date('2026-11-01T09:00:00Z') },
    ];
    const l = L.latestByParty(rows);
    assert.equal(l.factory.id, 5, 'a later "proposed" does not reset the factory');
    assert.equal(l.inspector.id, 4, 'same timestamp: the higher id wins');
    assert.deepEqual(L.latestByParty([]), { factory: null, inspector: null });
    assert.deepEqual(L.latestByParty([{ id: 1, party: 'factory', kind: 'proposed', created_at: '2026-11-01' }]), { factory: null, inspector: null });
    assert.equal(rows[0].id, 5, 'input order untouched');
});

test('confirmationTimestamps: the latest row\'s time when it is a confirmation, else null', () => {
    const t1 = new Date('2026-11-01T09:00:00.123Z');
    const t2 = new Date('2026-11-02T09:00:00.456Z');
    assert.deepEqual(L.confirmationTimestamps([
        { id: 1, party: 'factory', kind: 'confirmed', created_at: t1 },
        { id: 2, party: 'inspector', kind: 'confirmed', created_at: t2 },
    ]), { factory_confirmed_at: t1, inspector_confirmed_at: t2 });
    assert.deepEqual(L.confirmationTimestamps([
        { id: 1, party: 'factory', kind: 'confirmed', created_at: t1 },
        { id: 2, party: 'factory', kind: 'counter', created_at: t2 },
    ]), { factory_confirmed_at: null, inspector_confirmed_at: null }, 'a later counter clears the factory');
    assert.deepEqual(L.confirmationTimestamps([
        { id: 1, party: 'factory', kind: 'declined', created_at: t1 },
        { id: 2, party: 'factory', kind: 'confirmed', created_at: t2 },
        { id: 3, party: 'factory', kind: 'proposed', created_at: new Date('2026-11-03T00:00:00Z') },
    ]), { factory_confirmed_at: t2, inspector_confirmed_at: null });
    assert.deepEqual(L.confirmationTimestamps(undefined), { factory_confirmed_at: null, inspector_confirmed_at: null });
});

// ── the clearing rule ───────────────────────────────────────────────────────

test('clearingFor follows the plan §6 table', () => {
    const both = { clearFactory: true, clearInspector: true, reopen: true };
    const insp = { clearFactory: false, clearInspector: true, reopen: true };
    const none = { clearFactory: false, clearInspector: false, reopen: false };
    for (const f of ['visitDate', 'windowStart', 'windowEnd', 'factoryId', 'factory.label', 'factory.address', 'factory.city', 'factory.province', 'lines', 'moves']) {
        assert.deepEqual(L.clearingFor([f]), both, f);
    }
    for (const f of ['inspectorId', 'inspector.name', 'inspector.company', 'inspector.email', 'inspector.wechat', 'inspector.phone']) {
        assert.deepEqual(L.clearingFor([f]), insp, f);
    }
    for (const f of ['factory.contact.name', 'factory.contact.email', 'factory.contact.phone', 'notes', 'expectedProductionState', 'reportDeadlineHours', 'escalationNote', 'supplierName', 'attachments']) {
        assert.deepEqual(L.clearingFor([f]), none, f);
    }
    assert.deepEqual(L.clearingFor([]), none);
    assert.deepEqual(L.clearingFor(undefined), none);
    assert.deepEqual(L.clearingFor(['notes', 'inspectorId']), insp);
    assert.deepEqual(L.clearingFor(['inspectorId', 'visitDate']), both);
    assert.deepEqual(L.clearingFor(['factory.contact.name', 'notes']), none);
    // qc_visits column names work too.
    assert.deepEqual(L.clearingFor(['visit_date']), both);
    assert.deepEqual(L.clearingFor(['factory_city']), both);
    assert.deepEqual(L.clearingFor(['factory_contact_email']), none);
    assert.deepEqual(L.clearingFor(['inspector_phone']), insp);
    assert.deepEqual(L.clearingFor(['escalation_note', 'report_deadline_hours']), none);
});

// ── warnings ────────────────────────────────────────────────────────────────

test('a clean visit has no warnings at any stage', () => {
    for (const stage of ['create', 'propose', 'view']) assert.deepEqual(L.computeWarnings(ctx(), stage), [], stage);
    assert.throws(() => L.computeWarnings(ctx(), 'later'), /unknown stage/);
});

// Each key: a ctx that raises it, the orderId it carries, and its level per
// stage (null = not raised), straight from contract §5.
const WARNING_CASES = {
    qty_exceeds: [() => ctx({ lines: [{ orderId: 1, quantityToInspect: 1001, poQuantitySnapshot: null }] }), 1, ['block', 'block', 'warn']],
    line_left_stage: [() => ctx({ orders: { 1: order(1, { status: 'READY' }) } }), 1, ['block', 'warn', 'warn']],
    order_deleted: [() => ctx({ orders: { 1: order(1, { deleted: true }) } }), 1, ['block', 'block', 'warn']],
    multi_factory: [() => ctx({ orders: { 1: order(1, { supplier: 'Other Ltd' }) } }), 1, ['block', 'block', 'warn']],
    window_invalid: [() => ctx({ visit: { windowStart: '12:00', windowEnd: '12:00' } }), null, ['block', 'block', 'warn']],
    factory_deleted: [() => ctx({ visit: { factory: { id: 3, deleted: true, city: 'Dongguan' } } }), null, [null, 'block', 'warn']],
    inspector_deleted: [() => ctx({ visit: { inspector: { id: 1, deleted: true } } }), null, [null, 'block', 'warn']],
    kit_missing: [() => ctx({ kitStatus: { JF1: 'none' } }), 1, ['warn', 'block', 'warn']],
    ready_after_visit: [() => ctx({ orders: { 1: order(1, { estimatedReadyDate: '2026-11-13' }) } }), 1, ['warn', 'warn', 'warn']],
    already_on_visit: [() => ctx({ otherLines: [{ visitId: 11, ref: 'QCV_00011', status: 'CONFIRMED', visitDate: '2026-11-20', orderId: 1, quantityToInspect: 10 }] }), 1, ['warn', 'warn', 'warn']],
    already_inspected: [() => ctx({ otherLines: [{ visitId: 7, ref: 'QCV_00007', status: 'COMPLETED', visitDate: '2026-10-01', orderId: 1, quantityToInspect: 120 }] }), 1, ['warn', 'warn', 'warn']],
    fqc_missing: [() => ctx({ repackable: { JF1: false } }), 1, ['warn', 'warn', null]],
    po_shrank: [() => ctx({ orders: { 1: order(1, { quantity: 900 }) } }), 1, [null, 'warn', 'warn']],
    inspector_clash: [() => ctx({ inspectorSameDay: [{ visitId: 12, ref: 'QCV_00012', city: 'Ningbo' }] }), null, [null, 'warn', 'warn']],
    factory_no_city: [() => ctx({ visit: { factory: { id: 3, deleted: false, city: null } } }), null, [null, 'warn', 'warn']],
};

test('every warning key fires at the contract\'s level for each stage', () => {
    assert.deepEqual(Object.keys(WARNING_CASES).sort(), Object.keys(L.WARNING_LEVELS).sort(), 'every key is covered');
    for (const [key, [make, orderId, levels]] of Object.entries(WARNING_CASES)) {
        ['create', 'propose', 'view'].forEach((stage, i) => {
            const ws = L.computeWarnings(make(), stage);
            const w = find(ws, key);
            if (levels[i] === null) {
                assert.equal(w, undefined, `${key} is not raised at ${stage}`);
            } else {
                assert.ok(w, `${key} raised at ${stage}`);
                assert.equal(w.level, levels[i], `${key} at ${stage}`);
                assert.equal(w.orderId, orderId, `${key} orderId`);
                assert.equal(typeof w.message, 'string');
                assert.ok(w.message.length > 5);
            }
            assert.equal(ws.filter(x => x.key !== key).length, 0, `${key} at ${stage} raises nothing else: ${keysOf(ws)}`);
        });
    }
});

test('messages name the JF code and PO', () => {
    const ws = L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 1001, poQuantitySnapshot: 1000 }] }), 'create');
    assert.match(find(ws, 'qty_exceeds').message, /JF1 \(PO PO1\)/);
    const done = L.computeWarnings(WARNING_CASES.already_inspected[0](), 'view');
    assert.match(find(done, 'already_inspected').message, /120 on QCV_00007 \(2026-10-01\)/);
    assert.deepEqual(find(done, 'already_inspected').meta.visits, [{ visitId: 7, ref: 'QCV_00007', visitDate: '2026-10-01', quantityToInspect: 120 }]);
});

test('kit_missing: blocks at propose unless acknowledged; only kit_missing can be acknowledged', () => {
    const c = ctx({ kitStatus: { JF1: 'none' }, acknowledge: ['kit_missing'] });
    assert.equal(find(L.computeWarnings(c, 'propose'), 'kit_missing').level, 'warn');
    assert.equal(find(L.computeWarnings(c, 'create'), 'kit_missing').level, 'warn');
    const q = ctx({ lines: [{ orderId: 1, quantityToInspect: 1001, poQuantitySnapshot: 1001 }], acknowledge: ['qty_exceeds'] });
    assert.equal(find(L.computeWarnings(q, 'propose'), 'qty_exceeds').level, 'block');
    assert.deepEqual(L.computeWarnings(ctx({ kitStatus: { JF1: 'docs' } }), 'propose'), [], 'a code with QC documents is fine');
    assert.deepEqual(L.computeWarnings(ctx({ kitStatus: {} }), 'propose'), [], 'unknown kit status (view unreadable) is not "none"');
    assert.equal(find(L.computeWarnings(ctx({ kitStatus: { JF1: 'none' } }), 'view'), 'kit_missing').message,
        'JF1 (PO PO1) has no China factory QC document in JFPRO.');
    const fqc = ctx({ orders: { 1: order(1, { jfCode: 'jf1_fqc' }) }, kitStatus: { JF1: 'none' } });
    assert.equal(find(L.computeWarnings(fqc, 'view'), 'kit_missing').meta.jfCode, 'JF1', '_FQC lines use the parent kit');
});

test('order_deleted: a missing order counts, and hides the other per-line checks', () => {
    const ws = L.computeWarnings(ctx({ orders: {}, kitStatus: { JF1: 'none' } }), 'create');
    assert.deepEqual(keysOf(ws), ['order_deleted']);
    assert.match(ws[0].message, /Order 1/);
});

test('line_left_stage: in production and ready for QC are fine; a destroyed _FQC or destructive sample is expected', () => {
    assert.deepEqual(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'IN_PRODUCTION' }) } }), 'create').filter(w => w.key === 'line_left_stage'), []);
    assert.equal(find(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'DESTROYED' }) } }), 'view'), 'line_left_stage').level, 'warn',
        'a destroyed ordinary line is flagged');
    assert.deepEqual(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'DESTROYED', jfCode: 'JF1_FQC' }) } }), 'view'), []);
    assert.deepEqual(L.computeWarnings(ctx({
        lines: [{ orderId: 1, quantityToInspect: 3, poQuantitySnapshot: 1000, sampleMethod: 'destructive' }],
        orders: { 1: order(1, { status: 'DESTROYED' }) },
    }), 'view'), []);
    assert.equal(find(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'READY', jfCode: 'JF1_FQC' }) } }), 'create'), 'line_left_stage').level, 'block',
        'an _FQC line in any other status is treated like the rest');
});

test('multi_factory: supplier names compare normalised; a move row clears it; no host supplier, no check', () => {
    assert.deepEqual(L.computeWarnings(ctx({ orders: { 1: order(1, { supplier: 'acme CO., LTD.' }) } }), 'create'), []);
    const other = { orders: { 1: order(1, { supplier: 'Other Ltd' }) } };
    assert.deepEqual(L.computeWarnings(ctx({ ...other, moves: [{ orderId: 1 }] }), 'create'), []);
    assert.deepEqual(L.computeWarnings(ctx({ ...other, visit: { supplierName: '' } }), 'create'), []);
    assert.equal(find(L.computeWarnings(ctx(other), 'create'), 'multi_factory').meta.supplier, 'Other Ltd');
});

test('ready_after_visit: estimate after the visit date, or in production with no dates at all', () => {
    assert.deepEqual(L.computeWarnings(ctx({ orders: { 1: order(1, { estimatedReadyDate: '2026-11-12' }) } }), 'view'), [], 'ready on the day is fine');
    assert.ok(find(L.computeWarnings(ctx({ orders: { 1: order(1, { estimatedReadyDate: new Date('2026-11-20T00:00:00Z') }) } }), 'view'), 'ready_after_visit'));
    assert.ok(find(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'IN_PRODUCTION', estimatedReadyDate: null }) } }), 'view'), 'ready_after_visit'));
    assert.deepEqual(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'IN_PRODUCTION', estimatedReadyDate: null, actualReadyDate: '2026-11-01' }) } }), 'view'), []);
    assert.deepEqual(L.computeWarnings(ctx({ orders: { 1: order(1, { status: 'READY_FOR_QC', estimatedReadyDate: null }) } }), 'view'), [], 'ready for QC without dates is fine');
    assert.deepEqual(L.computeWarnings(ctx({ visit: { visitDate: null }, orders: { 1: order(1, { estimatedReadyDate: '2027-01-01' }) } }), 'view'), [], 'no visit date, nothing to compare');
});

test('already_on_visit lists every other open visit once; cancelled visits are ignored', () => {
    const otherLines = [
        { visitId: 11, ref: 'QCV_00011', status: 'DRAFT', visitDate: null, orderId: 1, quantityToInspect: 10 },
        { visitId: 12, ref: 'QCV_00012', status: 'PROPOSED', visitDate: '2026-11-20', orderId: 1, quantityToInspect: 20 },
        { visitId: 13, ref: 'QCV_00013', status: 'CANCELLED', visitDate: '2026-11-21', orderId: 1, quantityToInspect: 900 },
    ];
    const ws = L.computeWarnings(ctx({ otherLines }), 'create');
    assert.deepEqual(keysOf(ws), ['already_on_visit']);
    assert.match(ws[0].message, /QCV_00011, QCV_00012/);
    assert.deepEqual(ws[0].meta.visits.map(v => v.visitId), [11, 12]);
});

test('fqc_missing: only for a non-repackable, non-_FQC line whose PO has no _FQC sibling; unknown repackable is skipped', () => {
    assert.deepEqual(L.computeWarnings(ctx({ repackable: { JF1: false }, fqcSiblingParents: [1] }), 'create'), []);
    assert.deepEqual(L.computeWarnings(ctx({ repackable: { JF1: null } }), 'create'), []);
    assert.deepEqual(L.computeWarnings(ctx({ repackable: {} }), 'create'), []);
    assert.deepEqual(L.computeWarnings(ctx({ repackable: { JF1: false }, orders: { 1: order(1, { jfCode: 'JF1_FQC' }) } }), 'create'), []);
});

test('po_shrank: below the snapshot or below the quantity to inspect; no snapshot, no check', () => {
    assert.ok(find(L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 200, poQuantitySnapshot: 1000 }], orders: { 1: order(1, { quantity: 999 }) } }), 'view'), 'po_shrank'));
    const ws = L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 500, poQuantitySnapshot: 400 }], orders: { 1: order(1, { quantity: 450 }) } }), 'view');
    assert.deepEqual(find(ws, 'po_shrank').meta, { quantity: 450, poQuantitySnapshot: 400, quantityToInspect: 500 });
    assert.deepEqual(L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 200, poQuantitySnapshot: null }], orders: { 1: order(1, { quantity: 300 }) } }), 'view'), []);
    assert.deepEqual(L.computeWarnings(ctx({ lines: [{ orderId: 1, quantityToInspect: 200, poQuantitySnapshot: 900 }] }), 'view'), [], 'a PO that grew is fine');
});

test('inspector_clash: another city that day, or any visit when a city is unknown; the same city is fine', () => {
    assert.deepEqual(L.computeWarnings(ctx({ inspectorSameDay: [{ visitId: 12, ref: 'QCV_00012', city: ' dongguan ' }] }), 'view'), []);
    assert.ok(find(L.computeWarnings(ctx({ inspectorSameDay: [{ visitId: 12, ref: 'QCV_00012', city: null }] }), 'view'), 'inspector_clash'));
    const noCity = ctx({ visit: { factory: { id: 3, deleted: false, city: '' } }, inspectorSameDay: [{ visitId: 12, ref: 'QCV_00012', city: 'Dongguan' }] });
    assert.deepEqual(keysOf(L.computeWarnings(noCity, 'propose')).sort(), ['factory_no_city', 'inspector_clash']);
    assert.deepEqual(L.computeWarnings(ctx({ inspectorSameDay: [{ visitId: 10, ref: 'QCV_00010', city: 'Ningbo' }] }), 'view'), [], 'the visit itself is not a clash');
    const w = find(L.computeWarnings(ctx({ inspectorSameDay: [{ visitId: 12, ref: 'QCV_00012', city: 'Ningbo' }, { visitId: 13, ref: null, city: 'Xiamen' }] }), 'view'), 'inspector_clash');
    assert.match(w.message, /QCV_00012 \(Ningbo\), QCV_00013 \(Xiamen\)/);
});

test('no factory or inspector picked yet: nothing about them is raised', () => {
    assert.deepEqual(L.computeWarnings(ctx({ visit: { factory: null, inspector: null } }), 'propose'), []);
});

test('one warning per (key, orderId), several lines each get their own', () => {
    const c = ctx({
        lines: [
            { orderId: 1, quantityToInspect: 2000, poQuantitySnapshot: 1000 },
            { orderId: 2, quantityToInspect: 5000, poQuantitySnapshot: 1000 },
            { orderId: 1, quantityToInspect: 3000, poQuantitySnapshot: 1000 },
        ],
        orders: { 1: order(1), 2: order(2) },
        repackable: {}, kitStatus: {},
    });
    const ws = L.computeWarnings(c, 'create').filter(w => w.key === 'qty_exceeds');
    assert.deepEqual(ws.map(w => w.orderId), [1, 2]);
});

test('TIME values from MySQL (HH:MM:SS) are read as HH:MM', () => {
    const ws = L.computeWarnings(ctx({ visit: { windowStart: '14:00:00', windowEnd: '09:00:00' } }), 'create');
    assert.deepEqual(find(ws, 'window_invalid').meta, { windowStart: '14:00', windowEnd: '09:00' });
});

// ── state machine ───────────────────────────────────────────────────────────

const draft = (over = {}) => ({ id: 10, status: 'DRAFT', visit_date: '2026-11-12', factory_id: 3, inspector_id: 1, ...over });
const conf = (party, kind, id, at) => ({ id, party, kind, created_at: at || `2026-11-0${id} 09:00:00.000` });

test('terminal visits refuse everything; impossible moves are bad_transition', () => {
    for (const status of ['COMPLETED', 'CANCELLED']) {
        for (const to of ['DRAFT', 'PROPOSED', 'CONFIRMED', 'COMPLETED', 'CANCELLED']) {
            assert.deepEqual(keysOf(L.transitionBlockers({ status }, to)), ['terminal'], `${status} -> ${to}`);
        }
    }
    const bad = [['DRAFT', 'CONFIRMED'], ['DRAFT', 'COMPLETED'], ['DRAFT', 'DRAFT'], ['PROPOSED', 'DRAFT'], ['PROPOSED', 'COMPLETED'],
        ['PROPOSED', 'PROPOSED'], ['CONFIRMED', 'PROPOSED'], ['CONFIRMED', 'DRAFT'], ['DRAFT', 'ARCHIVED'], ['DRAFT', undefined]];
    for (const [status, to] of bad) {
        const b = L.transitionBlockers(draft({ status }), to);
        assert.deepEqual(keysOf(b), ['bad_transition'], `${status} -> ${to}`);
        assert.equal(b[0].level, 'block');
        assert.equal(b[0].orderId, null);
    }
});

test('DRAFT -> PROPOSED needs a date, a factory and an inspector, plus no blocking warnings', () => {
    assert.deepEqual(L.transitionBlockers(draft(), 'PROPOSED', { warnings: [] }), []);
    const b = L.transitionBlockers(draft({ visit_date: null, factory_id: null, inspector_id: null }), 'PROPOSED');
    assert.deepEqual(b.map(x => [x.key, x.meta.field]), [['missing_field', 'visitDate'], ['missing_field', 'factoryId'], ['missing_field', 'inspectorId']]);
    // Snapshots without ids are enough.
    assert.deepEqual(L.transitionBlockers(draft({ factory_id: null, inspector_id: null, factory_label: 'Main', factory_address: '1 Road', inspector_name: 'Wang' }), 'PROPOSED'), []);
    assert.equal(L.transitionBlockers(draft({ factory_id: null, factory_label: 'Main' }), 'PROPOSED')[0].meta.field, 'factoryId', 'a label without an address is not enough');
    // The API shape works as well.
    const api = { status: 'DRAFT', visitDate: '2026-11-12', factory: { id: 3 }, inspector: { id: null, name: null } };
    assert.deepEqual(L.transitionBlockers(api, 'PROPOSED').map(x => x.meta.field), ['inspectorId']);
    // Block warnings are carried through, warns are not.
    const warnings = [
        { key: 'qty_exceeds', level: 'block', orderId: 1, message: 'm' },
        { key: 'ready_after_visit', level: 'warn', orderId: 1, message: 'm' },
        { key: 'factory_deleted', level: 'block', orderId: null, message: 'm' },
    ];
    assert.deepEqual(keysOf(L.transitionBlockers(draft(), 'PROPOSED', { warnings })), ['qty_exceeds', 'factory_deleted']);
    // ...only when proposing.
    assert.deepEqual(L.transitionBlockers(draft(), 'CANCELLED', { warnings }), []);
});

test('PROPOSED -> CONFIRMED only when both latest confirmations are confirmed', () => {
    const p = draft({ status: 'PROPOSED' });
    assert.deepEqual(L.transitionBlockers(p, 'CONFIRMED', { confirmations: [conf('factory', 'confirmed', 1), conf('inspector', 'confirmed', 2)] }), []);
    let b = L.transitionBlockers(p, 'CONFIRMED', { confirmations: [conf('factory', 'confirmed', 1)] });
    assert.deepEqual(keysOf(b), ['confirmations_incomplete']);
    assert.deepEqual(b[0].meta, { factory: 'confirmed', inspector: null });
    assert.match(b[0].message, /inspector/);
    b = L.transitionBlockers(p, 'CONFIRMED', { confirmations: [conf('factory', 'confirmed', 1), conf('inspector', 'confirmed', 2), conf('factory', 'counter', 3)] });
    assert.deepEqual(b[0].meta, { factory: 'counter', inspector: 'confirmed' });
    assert.deepEqual(L.transitionBlockers(p, 'CONFIRMED', {
        confirmations: [conf('factory', 'declined', 1), conf('inspector', 'confirmed', 2), conf('factory', 'confirmed', 3), conf('factory', 'proposed', 4)],
    }), [], 'a later "proposed" row changes nothing');
    assert.deepEqual(keysOf(L.transitionBlockers(p, 'CONFIRMED')), ['confirmations_incomplete']);
});

test('CONFIRMED -> COMPLETED from the visit day (China time) on', () => {
    const c = draft({ status: 'CONFIRMED' });
    assert.deepEqual(L.transitionBlockers(c, 'COMPLETED', { today: '2026-11-12' }), []);
    assert.deepEqual(L.transitionBlockers(c, 'COMPLETED', { today: '2026-11-13' }), []);
    const b = L.transitionBlockers(c, 'COMPLETED', { today: '2026-11-11' });
    assert.deepEqual(keysOf(b), ['visit_in_future']);
    assert.deepEqual(b[0].meta, { visitDate: '2026-11-12', today: '2026-11-11' });
    assert.equal(L.transitionBlockers(draft({ status: 'CONFIRMED', visit_date: null }), 'COMPLETED', { today: '2026-11-12' })[0].key, 'missing_field');
    // Without `today` it uses chinaToday(): a visit far in the past completes, one far ahead does not.
    assert.deepEqual(L.transitionBlockers(draft({ status: 'CONFIRMED', visit_date: '2000-01-01' }), 'COMPLETED'), []);
    assert.deepEqual(keysOf(L.transitionBlockers(draft({ status: 'CONFIRMED', visit_date: '2999-01-01' }), 'COMPLETED')), ['visit_in_future']);
});

test('cancelling: DRAFT needs nothing; PROPOSED and CONFIRMED need a category; "other" needs a reason', () => {
    assert.deepEqual(L.transitionBlockers(draft(), 'CANCELLED'), []);
    assert.deepEqual(keysOf(L.transitionBlockers(draft(), 'CANCELLED', { cancelCategory: 'nonsense' })), ['cancel_category_required']);
    for (const status of ['PROPOSED', 'CONFIRMED']) {
        assert.deepEqual(keysOf(L.transitionBlockers(draft({ status }), 'CANCELLED')), ['cancel_category_required'], status);
        assert.deepEqual(keysOf(L.transitionBlockers(draft({ status }), 'CANCELLED', { cancelCategory: '' })), ['cancel_category_required'], status);
        for (const cat of L.CANCEL_CATEGORIES.filter(c => c !== 'other')) {
            assert.deepEqual(L.transitionBlockers(draft({ status }), 'CANCELLED', { cancelCategory: cat }), [], `${status} ${cat}`);
        }
        const b = L.transitionBlockers(draft({ status }), 'CANCELLED', { cancelCategory: 'other', reason: '  ' });
        assert.deepEqual(b.map(x => [x.key, x.meta.field]), [['missing_field', 'reason']]);
        assert.deepEqual(L.transitionBlockers(draft({ status }), 'CANCELLED', { cancelCategory: 'other', reason: 'Buyer asked' }), []);
    }
    assert.deepEqual(L.transitionBlockers(draft({ status: 'PROPOSED' }), 'CANCELLED', { cancelCategory: 'duplicate', reason: 'x'.repeat(501) }).map(x => x.meta.field), ['reason']);
});

test('applyTransition: the column patch for each move', () => {
    const now = new Date('2026-11-12T08:00:00Z');
    assert.deepEqual(L.applyTransition(draft(), 'PROPOSED', { now }), { status: 'PROPOSED', proposed_at: now });
    assert.deepEqual(L.applyTransition(draft({ status: 'PROPOSED' }), 'CONFIRMED', { now }), { status: 'CONFIRMED', confirmed_at: now });
    assert.deepEqual(L.applyTransition(draft({ status: 'CONFIRMED' }), 'COMPLETED', { now }), { status: 'COMPLETED', completed_at: now });
    assert.deepEqual(L.applyTransition(draft({ status: 'CONFIRMED' }), 'CANCELLED', { now, cancelCategory: 'factory_not_ready', reason: ' goods still on the line ' }),
        { status: 'CANCELLED', cancelled_at: now, cancel_category: 'factory_not_ready', wasted_trip: 1, cancel_reason: 'goods still on the line' });
    assert.equal(L.applyTransition(draft({ status: 'PROPOSED' }), 'CANCELLED', { now, cancelCategory: 'goods_not_at_site' }).wasted_trip, 1);
    for (const cat of ['schedule_change', 'inspector_no_show', 'order_changed', 'duplicate', 'other']) {
        assert.equal(L.applyTransition(draft({ status: 'PROPOSED' }), 'CANCELLED', { now, cancelCategory: cat }).wasted_trip, 0, cat);
    }
    assert.deepEqual(L.applyTransition(draft(), 'CANCELLED', { now }),
        { status: 'CANCELLED', cancelled_at: now, cancel_category: null, wasted_trip: 0, cancel_reason: null }, 'a draft cancels silently');
    assert.ok(L.applyTransition(draft(), 'PROPOSED').proposed_at instanceof Date, 'now defaults to the current time');
    assert.throws(() => L.applyTransition(draft({ status: 'CONFIRMED' }), 'PROPOSED'), /not allowed/);
    assert.throws(() => L.applyTransition(draft({ status: 'COMPLETED' }), 'CANCELLED'), /not allowed/);
});
