'use strict';

// QC Visits — pure rules behind /api/v1/qc-visits, /qc-factories and
// /qc-inspectors (routes: src/services/qc-visit-routes.js,
// qc-visit-comms-routes.js; plan and contract: ShipLine
// docs/plans/qc-visits.md, qc-visits-contract.md). Inspection kits are JFPRO
// documents (src/lib/qc-kit-docs.js, src/services/qc-kit-routes.js).
//
// A QC visit arranges an inspector's trip to a factory in China: which order
// lines, how many units of each, at which factory, by whom, on which day, and
// whether the factory and the inspector have both agreed the slot. This file
// holds everything that needs no database: the code lists, the request-body
// validators (camelCase in, DB columns out), the readiness warnings, the
// state machine, the confirmation rules, the AQL sample-size table and the
// remaining-to-inspect maths. The frontend mirrors the warnings and the AQL
// table in src/components/qcVisits/qcVisitsMath.ts.
//
// No DB, no AWS: tested by tools/test-qc-visits-lib.js.

// ── code lists ──────────────────────────────────────────────────────────────

const VISIT_STATUSES = ['DRAFT', 'PROPOSED', 'CONFIRMED', 'COMPLETED', 'CANCELLED'];
const OPEN_STATUSES = ['DRAFT', 'PROPOSED', 'CONFIRMED'];
const TERMINAL_STATUSES = ['COMPLETED', 'CANCELLED'];
// Server dialect (orders.status); the frontend spells IN_PRODUCTION
// UNDER_PRODUCTION.
const CANDIDATE_ORDER_STATUSES = ['IN_PRODUCTION', 'READY_FOR_QC'];
const PRODUCTION_STATES = ['FINISHED_PACKED', 'FINISHED_UNPACKED', 'PARTIAL_80', 'INLINE', 'PRE_PRODUCTION'];
const PRODUCTION_STATE_LABELS = {
    FINISHED_PACKED: '100% finished and packed in export cartons',
    FINISHED_UNPACKED: '100% finished, not yet packed',
    PARTIAL_80: 'At least 80% finished',
    INLINE: 'During production (inline inspection)',
    PRE_PRODUCTION: 'Before production (materials / first-off)',
};
const CANCEL_CATEGORIES = ['schedule_change', 'factory_not_ready', 'inspector_no_show', 'goods_not_at_site', 'order_changed', 'duplicate', 'other'];
const CANCEL_CATEGORY_LABELS = {
    schedule_change: 'Schedule change',
    factory_not_ready: 'Factory not ready (wasted trip)',
    inspector_no_show: 'Inspector no-show',
    goods_not_at_site: 'Goods not at site (wasted trip)',
    order_changed: 'Order changed',
    duplicate: 'Duplicate',
    other: 'Other',
};
// The inspector travelled and could not inspect: accounting recharges the
// factory (qc_visits.wasted_trip).
const WASTED_TRIP_CATEGORIES = ['factory_not_ready', 'goods_not_at_site'];
const SAMPLE_METHODS = ['full', 'aql', 'custom', 'destructive'];
const PARTIES = ['factory', 'inspector'];
const CONFIRMATION_KINDS = ['proposed', 'confirmed', 'declined', 'counter'];
const CHANNELS = ['email', 'wechat', 'phone', 'portal', 'other'];
const INSPECTOR_KINDS = ['internal', 'agency'];
const EDITOR_ROLES = ['admin', 'standard'];

// ── small helpers ───────────────────────────────────────────────────────────

const canEdit = userType => EDITOR_ROLES.includes(String(userType));

const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
const HM_RE = /^([01]\d|2[0-3]):([0-5]\d)$/;

// A real calendar date, YYYY-MM-DD.
function isYmd(v) {
    const m = typeof v === 'string' ? DATE_RE.exec(v) : null;
    if (!m) return false;
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    return d.getUTCFullYear() === Number(m[1]) && d.getUTCMonth() === Number(m[2]) - 1 && d.getUTCDate() === Number(m[3]);
}

// 'HH:MM', 00:00 to 23:59.
const isHm = v => typeof v === 'string' && HM_RE.test(v);

// Supplier names are free text everywhere; they match case-folded with
// runs of whitespace collapsed (like the QA PDF's supplier lookup).
const normSupplier = name => String(name == null ? '' : name).trim().replace(/\s+/g, ' ').toLowerCase();

// The product key of a JF code (kits, repackable): trimmed, upper-cased,
// _FQC sample suffix dropped.
const normJfCode = code => String(code == null ? '' : code).trim().toUpperCase().replace(/_FQC$/, '');

const isFqcCode = code => /_FQC$/i.test(String(code == null ? '' : code).trim());

const formatRef = id => `QCV_${String(Math.trunc(Number(id))).padStart(5, '0')}`;

// Today's date in China (Asia/Shanghai is a fixed +08:00, no DST).
function chinaToday(now = new Date()) {
    const t = now instanceof Date ? now.getTime() : new Date(now).getTime();
    return new Date(t + 8 * 3600 * 1000).toISOString().slice(0, 10);
}

// A DATE / DATETIME value as 'YYYY-MM-DD' (strings from the pool's
// dateStrings, or Date objects), or null.
function ymdOf(v) {
    if (v == null || v === '') return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString().slice(0, 10);
    const s = String(v);
    return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : null;
}

// A TIME value as 'HH:MM' ('09:30' or '09:30:00' from MySQL), or null.
function hmOf(v) {
    if (v == null || v === '') return null;
    const s = String(v);
    return /^\d{2}:\d{2}/.test(s) ? s.slice(0, 5) : null;
}

function timeOf(v) {
    if (v == null) return NaN;
    if (v instanceof Date) return v.getTime();
    if (typeof v === 'number') return v;
    const s = String(v);
    // MySQL 'YYYY-MM-DD HH:MM:SS(.mmm)' is UTC in this app.
    const iso = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}/.test(s) ? `${s.replace(' ', 'T')}Z` : s;
    return Date.parse(iso);
}

// ── AQL: ISO 2859-1, single sampling, normal inspection ─────────────────────

// Table 1: lot size range -> sample size code letter per general inspection
// level (GI, GII, GIII). The table starts at a lot of 2; a lot of 1 uses the
// first row (and is capped at the lot size below).
const AQL_CODE_LETTERS = [
    { max: 8, GI: 'A', GII: 'A', GIII: 'B' },
    { max: 15, GI: 'A', GII: 'B', GIII: 'C' },
    { max: 25, GI: 'B', GII: 'C', GIII: 'D' },
    { max: 50, GI: 'C', GII: 'D', GIII: 'E' },
    { max: 90, GI: 'C', GII: 'E', GIII: 'F' },
    { max: 150, GI: 'D', GII: 'F', GIII: 'G' },
    { max: 280, GI: 'E', GII: 'G', GIII: 'H' },
    { max: 500, GI: 'F', GII: 'H', GIII: 'J' },
    { max: 1200, GI: 'G', GII: 'J', GIII: 'K' },
    { max: 3200, GI: 'H', GII: 'K', GIII: 'L' },
    { max: 10000, GI: 'J', GII: 'L', GIII: 'M' },
    { max: 35000, GI: 'K', GII: 'M', GIII: 'N' },
    { max: 150000, GI: 'L', GII: 'N', GIII: 'P' },
    { max: 500000, GI: 'M', GII: 'P', GIII: 'Q' },
    { max: Infinity, GI: 'N', GII: 'Q', GIII: 'R' },
];
// Table 2-A: code letter -> sample size.
const AQL_SAMPLE_SIZES = {
    A: 2, B: 3, C: 5, D: 8, E: 13, F: 20, G: 32, H: 50, J: 80, K: 125, L: 200, M: 315, N: 500, P: 800, Q: 1250, R: 2000,
};

// 'GII', 'II', 'G-II', 'g2', 'level ii' -> 'GII'; anything else -> 'GII'.
function aqlLevelOf(level) {
    const s = String(level == null ? '' : level).toUpperCase().replace(/[^A-Z0-9]/g, '').replace(/^LEVEL/, '').replace(/^G/, '');
    if (s === 'I' || s === '1') return 'GI';
    if (s === 'III' || s === '3') return 'GIII';
    return 'GII';
}

function aqlCodeLetter(lotSize, level = 'GII') {
    const n = Math.floor(Number(lotSize));
    if (!Number.isFinite(n) || n <= 0) return null;
    const row = AQL_CODE_LETTERS.find(r => n <= r.max);
    return row[aqlLevelOf(level)];
}

// Units to pull for a lot: the ISO 2859-1 sample size, never more than the lot.
function aqlSampleSize(lotSize, level = 'GII') {
    const letter = aqlCodeLetter(lotSize, level);
    if (!letter) return 0;
    return Math.min(AQL_SAMPLE_SIZES[letter], Math.floor(Number(lotSize)));
}

// Units of an order still free to book: its quantity less what the OPEN
// visits already book (completed visits do not count: a re-inspection is
// never blocked by its source; see already_inspected).
function remainingToInspect(orderQuantity, openLineQuantities) {
    const q = Number(orderQuantity) || 0;
    const booked = (Array.isArray(openLineQuantities) ? openLineQuantities : [])
        .reduce((s, n) => s + (Number(n) || 0), 0);
    return Math.max(0, q - booked);
}

// ── validators ──────────────────────────────────────────────────────────────
// Each returns { value } or { error, field }. Inside, a FieldError carries the
// first problem out.

class FieldError extends Error {
    constructor(field, message) { super(message); this.field = field; }
}
const fail = (field, message) => { throw new FieldError(field, message); };

function run(fn) {
    try {
        return fn();
    } catch (e) {
        if (e instanceof FieldError) return { error: e.message, field: e.field };
        throw e;
    }
}

const has = (o, k) => o != null && Object.prototype.hasOwnProperty.call(o, k) && o[k] !== undefined;

// Optional text: null / '' -> null; trimmed; at most `max` characters.
function optText(v, field, max) {
    if (v == null) return null;
    if (typeof v !== 'string') fail(field, `${field} must be text.`);
    const s = v.trim();
    if (!s) return null;
    if (s.length > max) fail(field, `${field} cannot exceed ${max} characters.`);
    return s;
}

function reqText(v, field, max) {
    const s = optText(v, field, max);
    if (s == null) fail(field, `${field} is required.`);
    return s;
}

const EMAIL_RE = /^[^\s@<>,;]+@[^\s@<>,;]+\.[^\s@<>,;]+$/;
function optEmail(v, field) {
    const s = optText(v, field, 255);
    if (s != null && !EMAIL_RE.test(s)) fail(field, `${field} must be an email address.`);
    return s;
}

function optId(v, field) {
    if (v == null || v === '') return null;
    const n = typeof v === 'string' && /^\d+$/.test(v.trim()) ? Number(v.trim()) : v;
    if (!Number.isInteger(n) || n <= 0) fail(field, `${field} must be a positive whole number.`);
    return n;
}

function optInt(v, field, min, max) {
    if (v == null || v === '') return null;
    if (!Number.isInteger(v) || v < min || v > max) fail(field, `${field} must be a whole number from ${min} to ${max}.`);
    return v;
}

function optNumber(v, field, min, max) {
    if (v == null || v === '') return null;
    const n = typeof v === 'string' && v.trim() !== '' ? Number(v) : v;
    if (typeof n !== 'number' || !Number.isFinite(n) || n < min || n > max) fail(field, `${field} must be a number from ${min} to ${max}.`);
    return n;
}

function optYmd(v, field) {
    if (v == null || v === '') return null;
    if (!isYmd(v)) fail(field, `${field} must be a real date, YYYY-MM-DD.`);
    return v;
}

function optHm(v, field) {
    if (v == null || v === '') return null;
    if (!isHm(v)) fail(field, `${field} must be a time, HH:MM (00:00 to 23:59).`);
    return v;
}

function optOneOf(v, field, list) {
    if (v == null || v === '') return null;
    if (!list.includes(v)) fail(field, `${field} must be one of ${list.join(', ')}.`);
    return v;
}

function optBool(v, field) {
    if (v == null) return null;
    if (typeof v === 'boolean') return v;
    if (v === 0 || v === 1) return v === 1;
    fail(field, `${field} must be true or false.`);
}

function optObject(v, field) {
    if (v == null) return null;
    if (typeof v !== 'object' || Array.isArray(v)) fail(field, `${field} must be an object.`);
    return v;
}

const CONTACT_FIELDS = [
    ['name', 'name', 255],
    ['title', 'title', 255],
    ['email', 'email', 255],
    ['wechat', 'wechat', 255],
    ['phone', 'phone', 255],
];

// Header fields of a visit (POST /qc-visits, PATCH /qc-visits/:id).
// camelCase in, qc_visits columns out; only the fields present in the body
// are in `value` (with `partial`, nothing is required). `current` (optional,
// the visit row as stored) lets a PATCH that sends only one end of the window
// be checked against the other end already stored.
function validateVisitFields(body, { partial = false, current = null } = {}) {
    return run(() => {
        const b = body == null ? {} : optObject(body, 'body');
        const value = {};
        if (has(b, 'supplierName') || !partial) value.supplier_name = reqText(b.supplierName, 'supplierName', 255);
        if (has(b, 'factoryId')) value.factory_id = optId(b.factoryId, 'factoryId');
        if (has(b, 'inspectorId')) value.inspector_id = optId(b.inspectorId, 'inspectorId');
        if (has(b, 'visitDate')) value.visit_date = optYmd(b.visitDate, 'visitDate');
        if (has(b, 'windowStart')) value.window_start = optHm(b.windowStart, 'windowStart');
        if (has(b, 'windowEnd')) value.window_end = optHm(b.windowEnd, 'windowEnd');
        if (has(b, 'expectedProductionState')) {
            value.expected_production_state = optOneOf(b.expectedProductionState, 'expectedProductionState', PRODUCTION_STATES);
        }
        if (has(b, 'reportDeadlineHours')) value.report_deadline_hours = optInt(b.reportDeadlineHours, 'reportDeadlineHours', 1, 240);
        if (has(b, 'escalationNote')) value.escalation_note = optText(b.escalationNote, 'escalationNote', 500);
        if (has(b, 'notes')) value.notes = optText(b.notes, 'notes', 10000);

        if (has(b, 'factory')) {
            const f = optObject(b.factory, 'factory') || {};
            if (has(f, 'label')) value.factory_label = optText(f.label, 'factory.label', 100);
            if (has(f, 'address')) value.factory_address = optText(f.address, 'factory.address', 500);
            if (has(f, 'city')) value.factory_city = optText(f.city, 'factory.city', 100);
            if (has(f, 'province')) value.factory_province = optText(f.province, 'factory.province', 100);
            if (has(f, 'contact')) {
                const c = optObject(f.contact, 'factory.contact') || {};
                for (const [k, col, max] of CONTACT_FIELDS) {
                    if (!has(c, k)) continue;
                    const field = `factory.contact.${k}`;
                    value[`factory_contact_${col}`] = k === 'email' ? optEmail(c[k], field) : optText(c[k], field, max);
                }
            }
        }
        if (has(b, 'inspector')) {
            const i = optObject(b.inspector, 'inspector') || {};
            for (const k of ['name', 'company', 'email', 'wechat', 'phone']) {
                if (!has(i, k)) continue;
                const field = `inspector.${k}`;
                value[`inspector_${k}`] = k === 'email' ? optEmail(i[k], field) : optText(i[k], field, 255);
            }
        }

        const start = has(value, 'window_start') ? value.window_start : hmOf(current && current.window_start);
        const end = has(value, 'window_end') ? value.window_end : hmOf(current && current.window_end);
        if ((has(value, 'window_start') || has(value, 'window_end')) && start && end && end <= start) {
            fail('windowEnd', 'windowEnd must be later than windowStart.');
        }
        return { value };
    });
}

function validateAql(a, field) {
    if (a == null) return null;
    const o = optObject(a, field);
    return {
        level: optText(o.level, `${field}.level`, 32),
        critical: optNumber(o.critical, `${field}.critical`, 0, 99.99),
        major: optNumber(o.major, `${field}.major`, 0, 99.99),
        minor: optNumber(o.minor, `${field}.minor`, 0, 99.99),
    };
}

const MAX_LINES = 500;

// The line set of a visit (POST /qc-visits, PUT /qc-visits/:id/lines).
// CONTRACT-GAP: the contract does not fix the output shape; lines and moves
// come back as normalised camelCase objects (defaults filled, text trimmed):
//   lines: [{ orderId, quantityToInspect, sampleMethod, aql: {level,critical,major,minor}|null, note }]
//   moves: [{ orderId, toFactoryId, moveBy, arrangedBy, note }]
// The route adds visit_id / po_quantity_snapshot / from_supplier_name.
function validateLines(lines, moves) {
    return run(() => {
        if (!Array.isArray(lines)) fail('lines', 'lines must be a list.');
        if (lines.length < 1) fail('lines', 'A visit needs at least one line.');
        if (lines.length > MAX_LINES) fail('lines', `A visit can hold at most ${MAX_LINES} lines.`);
        const seen = new Set();
        const outLines = lines.map((l, i) => {
            const field = `lines[${i}]`;
            const o = optObject(l, field);
            if (!o) fail(field, `${field} is required.`);
            const orderId = optId(o.orderId, `${field}.orderId`);
            if (orderId == null) fail(`${field}.orderId`, `${field}.orderId is required.`);
            if (seen.has(orderId)) fail(`${field}.orderId`, `Order ${orderId} is on the visit twice.`);
            seen.add(orderId);
            const q = o.quantityToInspect;
            if (!Number.isInteger(q) || q < 1 || q > 10000000) {
                fail(`${field}.quantityToInspect`, 'quantityToInspect must be a whole number of at least 1.');
            }
            return {
                orderId,
                quantityToInspect: q,
                sampleMethod: optOneOf(o.sampleMethod, `${field}.sampleMethod`, SAMPLE_METHODS) || 'full',
                aql: validateAql(o.aql, `${field}.aql`),
                note: optText(o.note, `${field}.note`, 500),
            };
        });

        if (moves != null && !Array.isArray(moves)) fail('moves', 'moves must be a list.');
        const moved = new Set();
        const outMoves = (moves || []).map((m, i) => {
            const field = `moves[${i}]`;
            const o = optObject(m, field);
            if (!o) fail(field, `${field} is required.`);
            const orderId = optId(o.orderId, `${field}.orderId`);
            if (orderId == null) fail(`${field}.orderId`, `${field}.orderId is required.`);
            if (!seen.has(orderId)) fail(`${field}.orderId`, `Order ${orderId} has a move but is not on the visit.`);
            if (moved.has(orderId)) fail(`${field}.orderId`, `Order ${orderId} has two moves.`);
            moved.add(orderId);
            return {
                orderId,
                toFactoryId: optId(o.toFactoryId, `${field}.toFactoryId`),
                moveBy: optYmd(o.moveBy, `${field}.moveBy`),
                arrangedBy: optText(o.arrangedBy, `${field}.arrangedBy`, 255),
                note: optText(o.note, `${field}.note`, 500),
            };
        });
        return { lines: outLines, moves: outMoves };
    });
}

// POST /qc-visits/:id/confirmations. Out: qc_visit_confirmations columns.
// A counter-proposal must carry the date it proposes (accept-counter copies
// it onto the visit); counter fields are dropped from other kinds.
function validateConfirmation(body) {
    return run(() => {
        const b = optObject(body, 'body') || {};
        const party = optOneOf(b.party, 'party', PARTIES);
        if (!party) fail('party', `party must be one of ${PARTIES.join(', ')}.`);
        const kind = optOneOf(b.kind, 'kind', CONFIRMATION_KINDS);
        if (!kind) fail('kind', `kind must be one of ${CONFIRMATION_KINDS.join(', ')}.`);
        const channel = optOneOf(b.channel, 'channel', CHANNELS);
        if (!channel) fail('channel', `channel must be one of ${CHANNELS.join(', ')}.`);
        const value = {
            party, kind, channel,
            on_date: optYmd(b.onDate, 'onDate'),
            counter_date: null,
            counter_window_start: null,
            counter_window_end: null,
            by_name: optText(b.byName, 'byName', 255),
            note: optText(b.note, 'note', 500),
        };
        if (kind === 'counter') {
            value.counter_date = optYmd(b.counterDate, 'counterDate');
            if (!value.counter_date) fail('counterDate', 'A counter-proposal needs the date it proposes.');
            value.counter_window_start = optHm(b.counterWindowStart, 'counterWindowStart');
            value.counter_window_end = optHm(b.counterWindowEnd, 'counterWindowEnd');
            if (value.counter_window_start && value.counter_window_end && value.counter_window_end <= value.counter_window_start) {
                fail('counterWindowEnd', 'counterWindowEnd must be later than counterWindowStart.');
            }
        }
        return { value };
    });
}

// POST / PATCH /qc-factories. Out: qc_factories columns (is_default 0/1).
function validateFactory(body, { partial = false } = {}) {
    return run(() => {
        const b = optObject(body, 'body') || {};
        const value = {};
        if (has(b, 'supplierName') || !partial) value.supplier_name = reqText(b.supplierName, 'supplierName', 255);
        if (has(b, 'supplierCode')) value.supplier_code = optText(b.supplierCode, 'supplierCode', 64);
        if (has(b, 'label') || !partial) value.label = reqText(b.label, 'label', 100);
        if (has(b, 'address')) value.address = optText(b.address, 'address', 500);
        if (has(b, 'city')) value.city = optText(b.city, 'city', 100);
        if (has(b, 'province')) value.province = optText(b.province, 'province', 100);
        if (has(b, 'contact')) {
            const c = optObject(b.contact, 'contact') || {};
            for (const [k, col, max] of CONTACT_FIELDS) {
                if (!has(c, k)) continue;
                value[`contact_${col}`] = k === 'email' ? optEmail(c[k], `contact.${k}`) : optText(c[k], `contact.${k}`, max);
            }
        }
        if (has(b, 'isDefault')) value.is_default = optBool(b.isDefault, 'isDefault') ? 1 : 0;
        if (has(b, 'notes')) value.notes = optText(b.notes, 'notes', 10000);
        return { value };
    });
}

// POST / PATCH /qc-inspectors. Out: qc_inspectors columns (active 0/1).
function validateInspector(body, { partial = false } = {}) {
    return run(() => {
        const b = optObject(body, 'body') || {};
        const value = {};
        if (has(b, 'name') || !partial) value.name = reqText(b.name, 'name', 255);
        if (has(b, 'kind')) value.kind = optOneOf(b.kind, 'kind', INSPECTOR_KINDS) || 'agency';
        else if (!partial) value.kind = 'agency';
        if (has(b, 'company')) value.company = optText(b.company, 'company', 255);
        if (has(b, 'email')) value.email = optEmail(b.email, 'email');
        if (has(b, 'wechat')) value.wechat = optText(b.wechat, 'wechat', 255);
        if (has(b, 'phone')) value.phone = optText(b.phone, 'phone', 255);
        if (has(b, 'baseCity')) value.base_city = optText(b.baseCity, 'baseCity', 100);
        if (has(b, 'notes')) value.notes = optText(b.notes, 'notes', 10000);
        if (has(b, 'active')) value.active = optBool(b.active, 'active') === false ? 0 : 1;
        return { value };
    });
}

// ── confirmations ───────────────────────────────────────────────────────────

const sortRows = rows => [...(rows || [])].sort((a, b) => {
    const ta = timeOf(a.created_at != null ? a.created_at : a.createdAt);
    const tb = timeOf(b.created_at != null ? b.created_at : b.createdAt);
    if (ta !== tb && !(Number.isNaN(ta) && Number.isNaN(tb))) {
        if (Number.isNaN(ta)) return -1;
        if (Number.isNaN(tb)) return 1;
        return ta - tb;
    }
    return (Number(a.id) || 0) - (Number(b.id) || 0);
});

// The row that decides each party's state: its latest confirmation row,
// ignoring 'proposed' rows (they only record that the slot was sent).
function latestByParty(confirmations) {
    const out = { factory: null, inspector: null };
    for (const r of sortRows(confirmations)) {
        if (r.kind === 'proposed') continue;
        if (r.party === 'factory' || r.party === 'inspector') out[r.party] = r;
    }
    return out;
}

// qc_visits.factory_confirmed_at / inspector_confirmed_at: the latest row's
// created_at when it is a 'confirmed', else null (a later decline or counter
// clears it).
function confirmationTimestamps(confirmations) {
    const l = latestByParty(confirmations);
    const at = r => (r && r.kind === 'confirmed' ? (r.created_at != null ? r.created_at : r.createdAt) : null);
    return { factory_confirmed_at: at(l.factory), inspector_confirmed_at: at(l.inspector) };
}

// ── the clearing rule (plan §6): what an edit to a CONFIRMED visit undoes ───

const CLEARS_BOTH = new Set([
    'visitDate', 'windowStart', 'windowEnd', 'factoryId', 'factory',
    'factory.label', 'factory.address', 'factory.city', 'factory.province',
    'lines', 'moves',
    // the same, as qc_visits columns
    'visit_date', 'window_start', 'window_end', 'factory_id',
    'factory_label', 'factory_address', 'factory_city', 'factory_province',
]);

function clearingFor(changedFields) {
    let clearFactory = false;
    let clearInspector = false;
    for (const raw of changedFields || []) {
        const f = String(raw);
        if (f.startsWith('factory.contact') || f.startsWith('factory_contact_')) continue; // contact only: nothing
        if (CLEARS_BOTH.has(f)) {
            clearFactory = true;
            clearInspector = true;
        } else if (f === 'inspectorId' || f === 'inspector' || f.startsWith('inspector.')
            || f === 'inspector_id' || f.startsWith('inspector_')) {
            clearInspector = true;
        }
    }
    return { clearFactory, clearInspector, reopen: clearFactory || clearInspector };
}

// ── readiness warnings (contract §5) ────────────────────────────────────────

// Level per stage; null = not raised at that stage.
const WARNING_LEVELS = {
    qty_exceeds: { create: 'block', propose: 'block', view: 'warn' },
    line_left_stage: { create: 'block', propose: 'warn', view: 'warn' },
    order_deleted: { create: 'block', propose: 'block', view: 'warn' },
    multi_factory: { create: 'block', propose: 'block', view: 'warn' },
    window_invalid: { create: 'block', propose: 'block', view: 'warn' },
    factory_deleted: { create: null, propose: 'block', view: 'warn' },
    inspector_deleted: { create: null, propose: 'block', view: 'warn' },
    kit_missing: { create: 'warn', propose: 'block', view: 'warn' }, // propose: warn once acknowledged
    ready_after_visit: { create: 'warn', propose: 'warn', view: 'warn' },
    already_on_visit: { create: 'warn', propose: 'warn', view: 'warn' },
    already_inspected: { create: 'warn', propose: 'warn', view: 'warn' },
    fqc_missing: { create: 'warn', propose: 'warn', view: null },
    po_shrank: { create: null, propose: 'warn', view: 'warn' },
    inspector_clash: { create: null, propose: 'warn', view: 'warn' },
    factory_no_city: { create: null, propose: 'warn', view: 'warn' },
};
const ACKNOWLEDGEABLE = ['kit_missing'];
const STAGES = ['create', 'propose', 'view'];

function orderLabel(order, orderId) {
    const jf = order && order.jfCode ? String(order.jfCode) : `Order ${orderId}`;
    return order && order.poNumber ? `${jf} (PO ${order.poNumber})` : jf;
}

const sameCity = (a, b) => normSupplier(a) === normSupplier(b);

function computeWarnings(ctx, stage = 'view') {
    if (!STAGES.includes(stage)) throw new Error(`computeWarnings: unknown stage ${stage}`);
    const c = ctx || {};
    const visit = c.visit || {};
    const lines = c.lines || [];
    const moves = c.moves || [];
    const orders = c.orders || {};
    const otherLines = (c.otherLines || []).filter(r => visit.id == null || Number(r.visitId) !== Number(visit.id));
    const fqcParents = new Set((c.fqcSiblingParents || []).map(Number));
    const repackable = c.repackable || {};
    // { [normJfCode]: 'docs' | 'none' } (JFPRO China factory QC documents); a
    // code missing from it is unknown and raises no kit_missing.
    const kitStatus = c.kitStatus || {};
    const acknowledged = new Set((c.acknowledge || []).filter(k => ACKNOWLEDGEABLE.includes(k)));
    const movedIds = new Set(moves.map(m => Number(m.orderId)));

    const out = [];
    const seen = new Set();
    const add = (key, orderId, message, meta) => {
        let level = WARNING_LEVELS[key][stage];
        if (!level) return;
        if (key === 'kit_missing' && stage === 'propose' && acknowledged.has(key)) level = 'warn';
        const id = orderId == null ? null : Number(orderId);
        const dedup = `${key}:${id}`;
        if (seen.has(dedup)) return;
        seen.add(dedup);
        const w = { key, level, orderId: id, message };
        if (meta) w.meta = meta;
        out.push(w);
    };

    // Visit-level checks.
    const ws = hmOf(visit.windowStart);
    const we = hmOf(visit.windowEnd);
    if (ws && we && we <= ws) add('window_invalid', null, `The window ends (${we}) before it starts (${ws}).`, { windowStart: ws, windowEnd: we });
    if (visit.factory && visit.factory.deleted) add('factory_deleted', null, 'The factory has been deleted: pick a live one.', { factoryId: visit.factory.id == null ? null : visit.factory.id });
    if (visit.inspector && visit.inspector.deleted) add('inspector_deleted', null, 'The inspector has been deleted or deactivated: pick another.', { inspectorId: visit.inspector.id == null ? null : visit.inspector.id });

    const visitDate = ymdOf(visit.visitDate);
    const hostSupplier = normSupplier(visit.supplierName);

    for (const line of lines) {
        const orderId = Number(line.orderId);
        const order = orders[orderId] || orders[String(orderId)] || null;
        const label = orderLabel(order, orderId);
        if (!order || order.deleted) {
            add('order_deleted', orderId, `${label} has been deleted.`);
            continue;
        }
        const qty = Number(line.quantityToInspect) || 0;
        const jf = normJfCode(order.jfCode);
        const destructive = line.sampleMethod === 'destructive' || isFqcCode(order.jfCode);

        // qty_exceeds: against what OPEN other visits leave.
        const others = otherLines.filter(r => Number(r.orderId) === orderId);
        const openOthers = others.filter(r => OPEN_STATUSES.includes(r.status));
        if (order.quantity != null && Number.isFinite(Number(order.quantity))) {
            const remaining = remainingToInspect(order.quantity, openOthers.map(r => r.quantityToInspect));
            if (qty > remaining) {
                add('qty_exceeds', orderId,
                    `${label}: ${qty} to inspect but only ${remaining} of ${Number(order.quantity)} remain unbooked.`,
                    { quantityToInspect: qty, remaining, quantity: Number(order.quantity) });
            }
        }

        // line_left_stage. CONTRACT-GAP: ctx.lines has no sampleMethod in the
        // contract; a line counts as destructive when it says so or its JF
        // code ends in _FQC. A destroyed destructive sample is expected.
        if (!CANDIDATE_ORDER_STATUSES.includes(order.status) && !(destructive && order.status === 'DESTROYED')) {
            add('line_left_stage', orderId, `${label} is no longer in production or ready for QC (status ${order.status || 'unknown'}).`, { status: order.status || null });
        }

        // multi_factory: needs a host supplier to compare against.
        if (hostSupplier && normSupplier(order.supplier) !== hostSupplier && !movedIds.has(orderId)) {
            add('multi_factory', orderId, `${label} is at ${order.supplier || 'another supplier'}, not ${visit.supplierName}: record a goods move or remove it.`, { supplier: order.supplier || null });
        }

        if (jf && kitStatus[jf] === 'none') {
            add('kit_missing', orderId, `${label} has no China factory QC document in JFPRO.`, { jfCode: jf });
        }

        const est = ymdOf(order.estimatedReadyDate);
        if ((visitDate && est && est > visitDate)
            || (order.status === 'IN_PRODUCTION' && !ymdOf(order.actualReadyDate) && !est)) {
            add('ready_after_visit', orderId,
                est ? `${label} is expected ready ${est}, after the visit.` : `${label} is in production with no ready date.`,
                { estimatedReadyDate: est });
        }

        if (openOthers.length) {
            const refs = openOthers.map(r => r.ref || formatRef(r.visitId));
            add('already_on_visit', orderId, `${label} is already on ${refs.join(', ')}.`, {
                visits: openOthers.map(r => ({ visitId: Number(r.visitId), ref: r.ref || null, status: r.status, visitDate: ymdOf(r.visitDate), quantityToInspect: Number(r.quantityToInspect) || 0 })),
            });
        }
        const done = others.filter(r => r.status === 'COMPLETED');
        if (done.length) {
            const parts = done.map(r => `${Number(r.quantityToInspect) || 0} on ${r.ref || formatRef(r.visitId)}${ymdOf(r.visitDate) ? ` (${ymdOf(r.visitDate)})` : ''}`);
            add('already_inspected', orderId, `${label} was already inspected: ${parts.join(', ')}.`, {
                visits: done.map(r => ({ visitId: Number(r.visitId), ref: r.ref || null, visitDate: ymdOf(r.visitDate), quantityToInspect: Number(r.quantityToInspect) || 0 })),
            });
        }

        if (!isFqcCode(order.jfCode) && jf && repackable[jf] === false && !fqcParents.has(orderId)) {
            add('fqc_missing', orderId, `${label} is not repackable and its PO has no _FQC sample line.`, { jfCode: jf });
        }

        const snap = line.poQuantitySnapshot;
        if (snap != null && order.quantity != null) {
            const q = Number(order.quantity);
            if (q < Number(snap) || q < qty) {
                add('po_shrank', orderId, `${label}: the PO quantity is now ${q} (was ${Number(snap)} when booked, ${qty} to inspect).`,
                    { quantity: q, poQuantitySnapshot: Number(snap), quantityToInspect: qty });
            }
        }
    }

    // inspector_clash: other open visits of the inspector that day.
    const myCity = visit.factory ? visit.factory.city : null;
    const clashes = (c.inspectorSameDay || []).filter(o => (visit.id == null || Number(o.visitId) !== Number(visit.id))
        && (!o.city || !myCity || !sameCity(o.city, myCity)));
    if (clashes.length) {
        add('inspector_clash', null,
            `The inspector has another visit that day elsewhere: ${clashes.map(o => `${o.ref || formatRef(o.visitId)}${o.city ? ` (${o.city})` : ''}`).join(', ')}.`,
            { visits: clashes.map(o => ({ visitId: Number(o.visitId), ref: o.ref || null, city: o.city || null })) });
    }

    if (visit.factory && !(visit.factory.city && String(visit.factory.city).trim())) {
        add('factory_no_city', null, 'The factory has no city: trip grouping and clash checks need it.', { factoryId: visit.factory.id == null ? null : visit.factory.id });
    }

    return out;
}

// ── state machine (plan §6) ─────────────────────────────────────────────────

// Allowed manual transitions. CONFIRMED -> PROPOSED happens through edits and
// confirmations, never through POST /transition.
const TRANSITIONS = {
    DRAFT: ['PROPOSED', 'CANCELLED'],
    PROPOSED: ['CONFIRMED', 'CANCELLED'],
    CONFIRMED: ['COMPLETED', 'CANCELLED'],
    COMPLETED: [],
    CANCELLED: [],
};

// Read a visit field from either the API shape (camelCase, nested factory /
// inspector) or the qc_visits row (snake_case).
// CONTRACT-GAP: the contract does not say which shape `visit` is; both work.
function vget(visit, camel, snake) {
    if (!visit) return null;
    if (visit[camel] !== undefined) return visit[camel];
    if (visit[snake] !== undefined) return visit[snake];
    return null;
}
function nested(visit, obj, key, snake) {
    if (visit && visit[obj] && typeof visit[obj] === 'object' && visit[obj][key] !== undefined) return visit[obj][key];
    return visit ? visit[snake] : null;
}

const blocker = (key, message, meta) => {
    const w = { key, level: 'block', orderId: null, message };
    if (meta) w.meta = meta;
    return w;
};

function transitionBlockers(visit, to, { warnings = [], confirmations = [], cancelCategory = null, reason = null, today = null } = {}) {
    const from = vget(visit, 'status', 'status');
    if (TERMINAL_STATUSES.includes(from)) {
        return [blocker('terminal', `This visit is ${String(from).toLowerCase()}: it can no longer change.`, { status: from })];
    }
    if (!VISIT_STATUSES.includes(to) || !(TRANSITIONS[from] || []).includes(to)) {
        return [blocker('bad_transition', `A ${from || 'unknown'} visit cannot move to ${to}.`, { from: from || null, to: to == null ? null : to })];
    }
    const out = [];
    if (to === 'PROPOSED') {
        if (!ymdOf(vget(visit, 'visitDate', 'visit_date'))) out.push(blocker('missing_field', 'Pick a visit date.', { field: 'visitDate' }));
        const factoryId = nested(visit, 'factory', 'id', 'factory_id') ?? vget(visit, 'factoryId', 'factory_id');
        const fLabel = nested(visit, 'factory', 'label', 'factory_label');
        const fAddress = nested(visit, 'factory', 'address', 'factory_address');
        if (factoryId == null && !(fLabel && fAddress)) out.push(blocker('missing_field', 'Pick the factory (or enter its name and address).', { field: 'factoryId' }));
        const inspectorId = nested(visit, 'inspector', 'id', 'inspector_id') ?? vget(visit, 'inspectorId', 'inspector_id');
        const iName = nested(visit, 'inspector', 'name', 'inspector_name');
        if (inspectorId == null && !iName) out.push(blocker('missing_field', 'Pick the inspector.', { field: 'inspectorId' }));
        for (const w of warnings || []) if (w && w.level === 'block') out.push(w);
    } else if (to === 'CONFIRMED') {
        const l = latestByParty(confirmations);
        const fk = l.factory ? l.factory.kind : null;
        const ik = l.inspector ? l.inspector.kind : null;
        if (fk !== 'confirmed' || ik !== 'confirmed') {
            const missing = [fk !== 'confirmed' ? 'factory' : null, ik !== 'confirmed' ? 'inspector' : null].filter(Boolean);
            out.push(blocker('confirmations_incomplete', `Waiting for the ${missing.join(' and the ')} to confirm.`, { factory: fk, inspector: ik }));
        }
    } else if (to === 'COMPLETED') {
        const d = ymdOf(vget(visit, 'visitDate', 'visit_date'));
        const t = today || chinaToday();
        if (!d) out.push(blocker('missing_field', 'The visit has no date.', { field: 'visitDate' }));
        else if (d > t) out.push(blocker('visit_in_future', `The visit is on ${d}; it can be completed from that day (China time).`, { visitDate: d, today: t }));
    } else if (to === 'CANCELLED') {
        // A DRAFT was never sent anywhere: cancelling it needs no category.
        if (cancelCategory != null && cancelCategory !== '' && !CANCEL_CATEGORIES.includes(cancelCategory)) {
            out.push(blocker('cancel_category_required', `cancelCategory must be one of ${CANCEL_CATEGORIES.join(', ')}.`, { field: 'cancelCategory' }));
        } else if (from !== 'DRAFT' && !cancelCategory) {
            out.push(blocker('cancel_category_required', 'Say why the visit is cancelled.', { field: 'cancelCategory' }));
        }
        const r = typeof reason === 'string' ? reason.trim() : '';
        if (cancelCategory === 'other' && !r) out.push(blocker('missing_field', 'Describe the reason for "Other".', { field: 'reason' }));
        if (r.length > 500) out.push(blocker('missing_field', 'reason cannot exceed 500 characters.', { field: 'reason' }));
    }
    return out;
}

// The qc_visits column patch for an allowed transition (check
// transitionBlockers first; this throws on a transition that is never
// allowed).
function applyTransition(visit, to, { cancelCategory = null, reason = null, now = new Date() } = {}) {
    const from = vget(visit, 'status', 'status');
    if (!(TRANSITIONS[from] || []).includes(to)) throw new Error(`applyTransition: ${from} -> ${to} is not allowed`);
    const patch = { status: to };
    if (to === 'PROPOSED') patch.proposed_at = now;
    else if (to === 'CONFIRMED') patch.confirmed_at = now;
    else if (to === 'COMPLETED') patch.completed_at = now;
    else if (to === 'CANCELLED') {
        const cat = CANCEL_CATEGORIES.includes(cancelCategory) ? cancelCategory : null;
        const r = typeof reason === 'string' ? reason.trim().slice(0, 500) : '';
        patch.cancelled_at = now;
        patch.cancel_category = cat;
        patch.wasted_trip = WASTED_TRIP_CATEGORIES.includes(cat) ? 1 : 0;
        patch.cancel_reason = r || null;
    }
    return patch;
}

module.exports = {
    VISIT_STATUSES,
    OPEN_STATUSES,
    TERMINAL_STATUSES,
    CANDIDATE_ORDER_STATUSES,
    PRODUCTION_STATES,
    PRODUCTION_STATE_LABELS,
    CANCEL_CATEGORIES,
    CANCEL_CATEGORY_LABELS,
    WASTED_TRIP_CATEGORIES,
    SAMPLE_METHODS,
    PARTIES,
    CONFIRMATION_KINDS,
    CHANNELS,
    INSPECTOR_KINDS,
    EDITOR_ROLES,

    canEdit,
    isYmd,
    isHm,
    normSupplier,
    normJfCode,
    isFqcCode,
    formatRef,
    chinaToday,
    aqlSampleSize,
    remainingToInspect,

    validateVisitFields,
    validateLines,
    validateConfirmation,
    validateFactory,
    validateInspector,

    latestByParty,
    confirmationTimestamps,
    clearingFor,
    computeWarnings,
    transitionBlockers,
    applyTransition,

    // extras (not in the contract; handy for routes and tests)
    WARNING_LEVELS,
    AQL_CODE_LETTERS,
    AQL_SAMPLE_SIZES,
    aqlCodeLetter,
    ymdOf,
    hmOf,
};
