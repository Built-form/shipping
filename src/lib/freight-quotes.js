'use strict';

// Freight quotes (docs/freight-quotes.md): asking freight forwarders for
// sailings and prices, booking one, and sending the container's contents.
// The pure rules only, no database: the link's token and address, what a
// forwarder, a request and an answer may say, a request's state, the fixed
// blocks of the three emails, and the contents of a container by supplier.
// The SQL is in src/services/freight-quote-routes.js. The portal mirrors
// TOKEN_RE, the vocabularies, requestState and the option rules in its
// src/lib/freightQuotes.js; ShipLine mirrors requestState in
// src/components/freight/freightQuotes.ts.

const crypto = require('crypto');
const { FONT, INK, MUTED, BORDER, CODE_BG, escapeHtml, button, linkFallback, small, facts } = require('./email-layout');
const { parseRecipients, normalizeEmail } = require('./po-portal-email');
const { requestLinesSnapshot, parseRequestLines, requestDrift } = require('./request-to-send-drift');

/** 48 lower-case hex characters: unguessable, and the link's whole permission. */
const TOKEN_RE = /^[a-f0-9]{48}$/;
const newToken = () => crypto.randomBytes(24).toString('hex');
const isToken = v => typeof v === 'string' && TOKEN_RE.test(v);

/** The page on the supplier portal (no sign-in). `base` has no trailing slash. */
const freightUrl = (base, token) => `${base}/freight/${encodeURIComponent(token)}`;

const SERVICES = ['door_to_door', 'door_to_port', 'port_to_port', 'port_to_door'];
const SERVICE_LABELS = {
    door_to_door: 'Door to door',
    door_to_port: 'Door to port',
    port_to_port: 'Port to port',
    port_to_door: 'Port to door',
};
const SERVICE_LABELS_ZH = {
    door_to_door: '门到门',
    door_to_port: '门到港',
    port_to_port: '港到港',
    port_to_door: '港到门',
};
const CONTAINER_SIZES = ['20GP', '40GP', '40HQ'];
const CURRENCIES = ['USD', 'GBP', 'EUR', 'CNY'];

/** How long a link stays open, and what staff may choose instead. */
const EXPIRY_DAYS = 30;
const EXPIRY_MIN_DAYS = 7;
const EXPIRY_MAX_DAYS = 90;
/** The longest date range one request may ask about. */
const WINDOW_MAX_DAYS = 92;
/** A sailing this many days outside the range asked for is still accepted (and flagged in ShipLine). */
const SAILING_MARGIN_DAYS = 14;
const MAX_OPTIONS = 40;
/** Saves one link accepts in its life (the portal refuses the next). */
const ANSWER_LIMIT = 20;
const MAX_FORWARDERS_PER_SEND = 10;
const MAX_CONTACTS = 10;

const BOOKING_STATUSES = ['requested', 'confirmed', 'declined', 'cancelled'];
/** A booking that holds its request: no other booking, no new answer. */
const LIVE_BOOKING_STATUSES = ['requested', 'confirmed'];
const REVOKE_REASONS = ['staff', 'send_failed', 'filled', 'forwarder_removed'];

const NAME_MAX = 255;
const NOTE_MAX = 1000;
const OPTION_NOTE_MAX = 500;
const PORT_MAX = 100;
const REF_MAX = 100;
const TRANSIT_MAX_DAYS = 120;
const PRICE_RE = /^\d{1,7}(\.\d{1,2})?$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Control and invisible format characters: never part of a name, a port or a reference.
const UNPRINTABLE = /[\p{Cc}\p{Cf}]/u;

// ── small helpers ───────────────────────────────────────────────────────────

const YMD_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

/** A real calendar day written YYYY-MM-DD (2026-02-30 is not one). */
function isYmd(value) {
    if (typeof value !== 'string') return false;
    const m = YMD_RE.exec(value);
    if (!m) return false;
    const y = Number(m[1]), mo = Number(m[2]), d = Number(m[3]);
    if (y < 2000 || y > 2100) return false;
    const date = new Date(Date.UTC(y, mo - 1, d));
    return date.getUTCFullYear() === y && date.getUTCMonth() === mo - 1 && date.getUTCDate() === d;
}

/** A DATE column ('YYYY-MM-DD' from the pool, or a Date) as 'YYYY-MM-DD', else null. */
function toYmd(value) {
    if (value === null || value === undefined || value === '') return null;
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
    const text = String(value).slice(0, 10);
    return YMD_RE.test(text) ? text : null;
}

function addDays(ymd, days) {
    const [y, m, d] = ymd.split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d + days)).toISOString().slice(0, 10);
}

function daysBetween(a, b) {
    const ms = ymd => { const [y, m, d] = ymd.split('-').map(Number); return Date.UTC(y, m - 1, d); };
    return Math.round((ms(b) - ms(a)) / 86400000);
}

/** The day the sailing arrives: sailing date + transit days. Null when either is unusable. */
function arrivalDate(sailingDate, transitDays) {
    const day = toYmd(sailingDate);
    const n = Number(transitDays);
    if (!day || !Number.isInteger(n) || n < 0) return null;
    return addDays(day, n);
}

/** A JSON column (parsed by the driver, or still text) as an array. */
function jsonArray(value) {
    if (Array.isArray(value)) return value;
    if (typeof value !== 'string') return [];
    try { const parsed = JSON.parse(value); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
}

/** The services of a request row, in the vocabulary's order. */
const servicesOf = value => SERVICES.filter(s => jsonArray(value).includes(s));

const bad = (field, error) => ({ code: 'BAD_FIELD', field, error });

// An optional single line of text: trimmed, null when blank; an error object when unusable.
function optionalText(value, field, max) {
    if (value === undefined || value === null) return { value: null };
    if (typeof value !== 'string') return bad(field, `${field} must be text.`);
    const text = value.trim();
    if (!text) return { value: null };
    if (text.length > max) return bad(field, `${field} must be at most ${max} characters.`);
    if (UNPRINTABLE.test(text.replace(/[\r\n\t]/g, ''))) return bad(field, `${field} holds characters that cannot be used.`);
    return { value: text };
}

function requiredLine(value, field, max) {
    const out = optionalText(value, field, max);
    if (out.code) return out;
    if (out.value === null) return bad(field, `${field} is required.`);
    if (/[\r\n\t]/.test(out.value)) return bad(field, `${field} must be one line.`);
    return out;
}

// ── forwarders ──────────────────────────────────────────────────────────────

/**
 * A forwarder as the list's form sends it: { name, notes?, contacts: [{ name?, email }] }.
 * At least one contact; each email one plain address, kept trimmed and
 * lower-case (how the portal matches a signed-in user); duplicates collapse.
 * → { value: { name, notes, contacts } } or { code, field, error }.
 */
function validateForwarder(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) return bad('body', 'A forwarder is required.');
    const name = requiredLine(body.name, 'name', NAME_MAX);
    if (name.code) return name;
    const notes = optionalText(body.notes, 'notes', NOTE_MAX);
    if (notes.code) return notes;
    if (!Array.isArray(body.contacts) || !body.contacts.length) return bad('contacts', 'Add at least one contact with an email address.');
    if (body.contacts.length > MAX_CONTACTS) return bad('contacts', `A forwarder has at most ${MAX_CONTACTS} contacts.`);
    const contacts = [];
    const seen = new Set();
    for (let i = 0; i < body.contacts.length; i++) {
        const c = body.contacts[i] || {};
        const field = `contacts[${i}]`;
        const contactName = optionalText(c.name, `${field}.name`, NAME_MAX);
        if (contactName.code) return contactName;
        const { addresses, invalid } = parseRecipients(typeof c.email === 'string' ? c.email : '');
        if (invalid.length || addresses.length !== 1) return bad(`${field}.email`, 'Each contact needs one valid email address.');
        const email = normalizeEmail(addresses[0]);
        if (email.length > NAME_MAX) return bad(`${field}.email`, 'That email address is too long.');
        if (seen.has(email)) continue;
        seen.add(email);
        contacts.push({ name: contactName.value, email });
    }
    return { value: { name: name.value, notes: notes.value, contacts } };
}

// ── requests ────────────────────────────────────────────────────────────────

/**
 * The send dialog's body: { forwarderIds, batchId?, windowStart, windowEnd,
 * origin?, destination?, containerSize?, services, note?, expiryDays? }.
 * `today` is 'YYYY-MM-DD' (UK): the range may not lie wholly in the past.
 * → { value } or { code, field, error }.
 */
function validateRequest(body, today) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) return bad('body', 'A request is required.');

    if (!Array.isArray(body.forwarderIds) || !body.forwarderIds.length) return bad('forwarderIds', 'Choose at least one forwarder.');
    const forwarderIds = [...new Set(body.forwarderIds.map(Number))];
    if (forwarderIds.some(id => !Number.isSafeInteger(id) || id <= 0)) return bad('forwarderIds', 'forwarderIds must be forwarder ids.');
    if (forwarderIds.length > MAX_FORWARDERS_PER_SEND) return bad('forwarderIds', `One request goes to at most ${MAX_FORWARDERS_PER_SEND} forwarders.`);

    let batchId = null;
    if (body.batchId !== undefined && body.batchId !== null) {
        if (typeof body.batchId !== 'string' || !UUID_RE.test(body.batchId)) return bad('batchId', 'batchId must be a UUID.');
        batchId = body.batchId.toLowerCase();
    }

    const { windowStart, windowEnd } = body;
    if (!isYmd(windowStart)) return bad('windowStart', 'windowStart must be a date (YYYY-MM-DD).');
    if (!isYmd(windowEnd)) return bad('windowEnd', 'windowEnd must be a date (YYYY-MM-DD).');
    if (windowEnd < windowStart) return bad('windowEnd', 'The range must end on or after the day it starts.');
    if (daysBetween(windowStart, windowEnd) > WINDOW_MAX_DAYS) return bad('windowEnd', `The range may cover at most ${WINDOW_MAX_DAYS} days.`);
    if (isYmd(today) && windowEnd < today) return bad('windowEnd', 'The range is in the past.');

    const origin = optionalText(body.origin, 'origin', NAME_MAX);
    if (origin.code) return origin;
    const destination = optionalText(body.destination, 'destination', NAME_MAX);
    if (destination.code) return destination;

    const containerSize = body.containerSize === undefined || body.containerSize === null || body.containerSize === ''
        ? '40HQ' : String(body.containerSize);
    if (!CONTAINER_SIZES.includes(containerSize)) return bad('containerSize', `containerSize must be one of ${CONTAINER_SIZES.join(', ')}.`);

    if (!Array.isArray(body.services) || !body.services.length) return bad('services', 'Choose at least one service.');
    if (body.services.some(s => !SERVICES.includes(s))) return bad('services', `services must be from ${SERVICES.join(', ')}.`);
    const services = SERVICES.filter(s => body.services.includes(s));

    const note = optionalText(body.note, 'note', NOTE_MAX);
    if (note.code) return note;

    let expiryDays = EXPIRY_DAYS;
    if (body.expiryDays !== undefined && body.expiryDays !== null) {
        expiryDays = Number(body.expiryDays);
        if (!Number.isInteger(expiryDays) || expiryDays < EXPIRY_MIN_DAYS || expiryDays > EXPIRY_MAX_DAYS) {
            return bad('expiryDays', `expiryDays must be a whole number from ${EXPIRY_MIN_DAYS} to ${EXPIRY_MAX_DAYS}.`);
        }
    }

    return {
        value: {
            forwarderIds, batchId, windowStart, windowEnd,
            origin: origin.value || 'China', destination: destination.value || 'UK',
            containerSize, services, note: note.value, expiryDays,
        },
    };
}

/**
 * The state of a request, one precedence shared with the portal and ShipLine:
 * revoked > expired > booked > answered > open. `booked` = a booking on it is
 * requested or confirmed. Derived, never stored: a cancelled or declined
 * booking puts the request back to `answered` with nothing to undo.
 * `row` = { revoked_at, expired, answered_at }, `expired` being the database's
 * own answer to `expires_at <= UTC_TIMESTAMP()`.
 */
function requestState(row, hasLiveBooking) {
    if (row.revoked_at) return 'revoked';
    if (row.expired === true || Number(row.expired) === 1) return 'expired';
    if (hasLiveBooking) return 'booked';
    return row.answered_at ? 'answered' : 'open';
}

/** The states in which an answer may be saved. */
const isEditable = state => state === 'open' || state === 'answered';

// ── options ─────────────────────────────────────────────────────────────────

/**
 * The options of one answer against its request: `request` =
 * { windowStart, windowEnd, services, containerSize }. Rules, the same on the
 * portal:
 *   - at most MAX_OPTIONS;
 *   - loadingDate and sailingDate real days, loading on or before sailing;
 *   - sailingDate inside the range asked for, give or take SAILING_MARGIN_DAYS;
 *   - transitDays a whole number from 1 to TRANSIT_MAX_DAYS;
 *   - portFrom and portTo one line each;
 *   - service one of those the request asked for;
 *   - containerSize from the list (the size asked for when left out);
 *   - price a positive amount with at most two decimals; currency from the list;
 *   - carrier, validUntil and note optional.
 * → { options } (price as a '1234.50' string) or { code, field, error }.
 */
function validateOptions(raw, request) {
    if (!Array.isArray(raw)) return bad('options', 'options must be a list.');
    if (raw.length > MAX_OPTIONS) return bad('options', `An answer holds at most ${MAX_OPTIONS} options.`);
    const earliest = addDays(request.windowStart, -SAILING_MARGIN_DAYS);
    const latest = addDays(request.windowEnd, SAILING_MARGIN_DAYS);
    const options = [];
    for (let i = 0; i < raw.length; i++) {
        const o = raw[i];
        const at = name => `options[${i}].${name}`;
        if (!o || typeof o !== 'object' || Array.isArray(o)) return bad(`options[${i}]`, 'Each option must be an object.');

        if (!isYmd(o.loadingDate)) return bad(at('loadingDate'), 'The loading date must be a date (YYYY-MM-DD).');
        if (!isYmd(o.sailingDate)) return bad(at('sailingDate'), 'The sailing date must be a date (YYYY-MM-DD).');
        if (o.loadingDate > o.sailingDate) return bad(at('loadingDate'), 'The loading date must be on or before the sailing date.');
        if (o.sailingDate < earliest || o.sailingDate > latest) {
            return bad(at('sailingDate'), `The sailing date must be between ${earliest} and ${latest}.`);
        }

        const transitDays = Number(o.transitDays);
        if (typeof o.transitDays === 'boolean' || o.transitDays === null || o.transitDays === ''
            || !Number.isInteger(transitDays) || transitDays < 1 || transitDays > TRANSIT_MAX_DAYS) {
            return bad(at('transitDays'), `The transit time must be a whole number of days from 1 to ${TRANSIT_MAX_DAYS}.`);
        }

        const portFrom = requiredLine(o.portFrom, at('portFrom'), PORT_MAX);
        if (portFrom.code) return portFrom;
        const portTo = requiredLine(o.portTo, at('portTo'), PORT_MAX);
        if (portTo.code) return portTo;

        if (!request.services.includes(o.service)) return bad(at('service'), 'The service must be one of those asked for.');

        const containerSize = o.containerSize === undefined || o.containerSize === null || o.containerSize === ''
            ? request.containerSize : String(o.containerSize);
        if (!CONTAINER_SIZES.includes(containerSize)) return bad(at('containerSize'), `The container size must be one of ${CONTAINER_SIZES.join(', ')}.`);

        const priceText = typeof o.price === 'number' && Number.isFinite(o.price) ? String(o.price) : String(o.price ?? '').trim();
        if (!PRICE_RE.test(priceText) || Number(priceText) <= 0) {
            return bad(at('price'), 'The price must be an amount above zero with at most two decimals.');
        }
        if (!CURRENCIES.includes(o.currency)) return bad(at('currency'), `The currency must be one of ${CURRENCIES.join(', ')}.`);

        const carrier = optionalText(o.carrier, at('carrier'), PORT_MAX);
        if (carrier.code) return carrier;
        let validUntil = null;
        if (o.validUntil !== undefined && o.validUntil !== null && o.validUntil !== '') {
            if (!isYmd(o.validUntil)) return bad(at('validUntil'), 'Valid until must be a date (YYYY-MM-DD).');
            validUntil = o.validUntil;
        }
        const note = optionalText(o.note, at('note'), OPTION_NOTE_MAX);
        if (note.code) return note;

        options.push({
            loadingDate: o.loadingDate, sailingDate: o.sailingDate, transitDays,
            portFrom: portFrom.value, portTo: portTo.value, service: o.service, containerSize,
            price: Number(priceText).toFixed(2), currency: o.currency,
            carrier: carrier.value, validUntil, note: note.value,
        });
    }
    return { options };
}

/**
 * An answer staff type in for a forwarder who replied by email:
 * { options, declined?, note? }. Either some options or `declined`.
 * → { value: { options, declined, note } } or { code, field, error }.
 */
function validateStaffAnswer(body, request) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) return bad('body', 'An answer is required.');
    const declined = body.declined === true;
    const checked = validateOptions(body.options === undefined ? [] : body.options, request);
    if (checked.code) return checked;
    if (declined && checked.options.length) return bad('options', 'A forwarder that cannot quote has no options.');
    if (!declined && !checked.options.length) return bad('options', 'Add at least one option, or say the forwarder cannot quote.');
    const note = optionalText(body.note, 'note', NOTE_MAX);
    if (note.code) return note;
    return { value: { options: checked.options, declined, note: note.value } };
}

/** A booking reference as staff or the forwarder type it. → { value } or { code, field, error }. */
const validateBookingRef = value => requiredLine(value, 'bookingRef', REF_MAX);

// ── the emails' fixed blocks ────────────────────────────────────────────────

/** '1850.00', 'USD' → 'USD 1,850.00'. */
function money(price, currency) {
    const n = Number(price);
    if (!Number.isFinite(n)) return '';
    const [whole, cents] = n.toFixed(2).split('.');
    return `${currency || ''} ${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}.${cents}`.trim();
}

const serviceLabel = s => (SERVICE_LABELS[s] ? `${SERVICE_LABELS[s]} / ${SERVICE_LABELS_ZH[s]}` : String(s || ''));

const boxOpen = `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:18px 0;"><tr><td style="padding:16px 18px 6px;border:1px solid ${BORDER};border-radius:8px;background:${CODE_BG};">`;
const boxClose = '</td></tr></table>';
const lead = html => `<p style="margin:0 0 12px;font-family:${FONT};font-size:15px;line-height:1.6;color:${INK};">${html}</p>`;
const foot = html => `<p style="margin:0 0 10px;font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">${html}</p>`;

/** What a request asks for, as the two-column list both link emails carry. */
function requestFacts(request) {
    const rows = [
        ['Sailing between / 开船日期', `${escapeHtml(request.windowStart)} – ${escapeHtml(request.windowEnd)}`],
        ['From / 起运地', escapeHtml(request.origin || '')],
        ['To / 目的地', escapeHtml(request.destination || '')],
        ['Container / 柜型', escapeHtml(request.containerSize)],
        ['Service / 服务方式', (request.services || []).map(s => escapeHtml(serviceLabel(s))).join('<br>')],
    ];
    if (request.note) rows.push(['Note / 备注', escapeHtml(request.note).replace(/\r?\n/g, '<br>')]);
    return facts(rows);
}

/**
 * The fixed block under the quote request's body: what is asked, the button,
 * the link as text, how to sign in instead, and when the link stops working.
 * `expiresOn` is 'YYYY-MM-DD'; `registerUrl` the portal's register page.
 */
function quoteBlockHtml({ request, url, expiresOn, registerUrl }) {
    const reg = escapeHtml(registerUrl || '');
    const regLink = reg ? `<a href="${reg}" style="color:#1f5fbf;word-break:break-all;">${reg}</a>` : '';
    const until = escapeHtml(expiresOn || '');
    return [
        boxOpen,
        lead('<strong>Freight quote request</strong> / <strong>海运报价请求</strong>'),
        requestFacts(request),
        button(url, 'Enter your quote / 填写报价'),
        linkFallback(url),
        small('Open the page and add one line per sailing you can offer: loading date, sailing date, transit time, ports, service and price. No sign-in is needed. You can come back and change your answer until we book.'),
        small('打开页面后，请为每个可提供的船期添加一行：装柜日期、开船日期、航程天数、港口、服务方式和价格。无需登录。在我们订舱之前，您可以随时回来修改。'),
        regLink ? small(`To see all our requests in one place, register at ${regLink} with the email address this message was sent to. / 如需集中查看我们的全部请求，请使用接收本邮件的邮箱地址在 ${regLink} 注册。`) : '',
        until ? foot(`This link works until ${until}. / 此链接有效期至 ${until}。`) : '',
        boxClose,
    ].join('');
}

/** One option as a two-column list: [[label, valueHtml], …]. */
function optionFacts(option) {
    const arrives = arrivalDate(option.sailingDate, option.transitDays);
    const rows = [
        ['Loading / 装柜日期', escapeHtml(option.loadingDate)],
        ['Sailing / 开船日期', escapeHtml(option.sailingDate)],
        ['Transit / 航程', `${escapeHtml(option.transitDays)} days / 天${arrives ? ` (arrives ${escapeHtml(arrives)})` : ''}`],
        ['From / 起运港', escapeHtml(option.portFrom)],
        ['To / 目的港', escapeHtml(option.portTo)],
        ['Service / 服务方式', escapeHtml(serviceLabel(option.service))],
        ['Container / 柜型', escapeHtml(option.containerSize)],
        ['Price / 价格', `<strong>${escapeHtml(money(option.price, option.currency))}</strong>`],
    ];
    if (option.carrier) rows.push(['Carrier / 船公司', escapeHtml(option.carrier)]);
    return rows;
}

/** The fixed block under the booking request's body: the sailing, the note, the button. */
function bookingBlockHtml({ option, url, note }) {
    return [
        boxOpen,
        lead('<strong>Please book this sailing</strong> / <strong>请预订此船期</strong>'),
        facts(note ? [...optionFacts(option), ['Note / 备注', escapeHtml(note).replace(/\r?\n/g, '<br>')]] : optionFacts(option)),
        button(url, 'Confirm or decline the booking / 确认或拒绝订舱'),
        linkFallback(url),
        small('Open the page, enter your booking reference and press Confirm; or decline if the space is no longer available. No sign-in is needed.'),
        small('打开页面后，请填写订舱号并点击“确认”；如舱位已无法提供，请选择拒绝。无需登录。'),
        boxClose,
    ].join('');
}

// ── a container's contents by supplier ──────────────────────────────────────

const round = (n, places) => Number(Number(n || 0).toFixed(places));
const NO_SUPPLIER = '(no supplier)';

/**
 * The lines of a container (loadForwarderQuoteForPdf's `lines`) grouped by
 * supplier, as the contents email and its preview show them. Suppliers match
 * trimmed and ignoring case, as the per-supplier PDFs do; the first spelling
 * names the group; lines without a supplier come last.
 * → { groups: [{ supplier, ports, poNumbers, lines: [{ product, jfCode, lot,
 *   poNumber, units, cartons, cbm, weightKg }], totals }], totals } with
 *   totals = { lines, units, cartons, cbm, weightKg }.
 */
function groupContentsBySupplier(lines) {
    const byKey = new Map();
    for (const l of lines || []) {
        const name = String(l.supplier || '').trim();
        const key = name.toLowerCase();
        if (!byKey.has(key)) byKey.set(key, { supplier: name || NO_SUPPLIER, named: Boolean(name), ports: [], poNumbers: [], lines: [] });
        const g = byKey.get(key);
        const port = String(l.port || '').trim();
        if (port && !g.ports.includes(port)) g.ports.push(port);
        const po = String(l.poNumber || '').trim();
        if (po && !g.poNumbers.includes(po)) g.poNumbers.push(po);
        const cartons = Number(l.noOfCartons) || 0;
        g.lines.push({
            product: l.sku || l.jfCode || '',
            jfCode: l.jfCode || '',
            lot: l.lot || '',
            poNumber: po,
            units: Number(l.orderedUnits) || 0,
            cartons,
            cbm: round(l.totalCbm, 3),
            weightKg: round((Number(l.cartonWeight) || 0) * cartons, 1),
        });
    }
    const sum = rows => ({
        lines: rows.length,
        units: rows.reduce((n, r) => n + r.units, 0),
        cartons: rows.reduce((n, r) => n + r.cartons, 0),
        cbm: round(rows.reduce((n, r) => n + r.cbm, 0), 3),
        weightKg: round(rows.reduce((n, r) => n + r.weightKg, 0), 1),
    });
    const groups = [...byKey.values()]
        .sort((a, b) => (a.named === b.named ? a.supplier.localeCompare(b.supplier) : (a.named ? -1 : 1)))
        .map(({ named, ...g }) => ({ ...g, totals: sum(g.lines) }));
    return { groups, totals: sum(groups.flatMap(g => g.lines)) };
}

const TH = `padding:6px 8px;border-bottom:1px solid ${BORDER};font-family:${FONT};font-size:12px;color:${MUTED};text-align:left;white-space:nowrap;`;
const TD = `padding:6px 8px;border-bottom:1px solid ${BORDER};font-family:${FONT};font-size:13px;color:${INK};vertical-align:top;`;
const NUM = 'text-align:right;white-space:nowrap;';

/**
 * The fixed block of the contents email: the totals, then one table per
 * supplier (its port and PO numbers in the heading, a subtotal row at the foot).
 * `contents` is groupContentsBySupplier's result.
 */
function contentsHtml(contents, { containerName, option, bookingRef } = {}) {
    const { groups, totals } = contents;
    const head = [
        ['Container / 集装箱', escapeHtml(containerName || '')],
        bookingRef ? ['Booking ref / 订舱号', escapeHtml(bookingRef)] : null,
        option ? ['Sailing / 开船日期', `${escapeHtml(option.sailingDate)} · ${escapeHtml(option.portFrom)} → ${escapeHtml(option.portTo)}`] : null,
        ['Suppliers / 供应商', String(groups.length)],
        ['Cartons / 箱数', String(totals.cartons)],
        ['Volume / 体积', `${totals.cbm.toFixed(3)} CBM`],
        ['Gross weight / 毛重', `${totals.weightKg.toFixed(1)} kg`],
    ].filter(Boolean);
    const cell = (html, extra = '') => `<td style="${TD}${extra}">${html}</td>`;
    const tables = groups.map((g) => {
        const where = [g.ports.join(', '), g.poNumbers.length ? `PO ${g.poNumbers.join(', ')}` : ''].filter(Boolean).join(' · ');
        const rows = g.lines.map(l => `<tr>${[
            cell(escapeHtml(l.product)),
            cell(escapeHtml(l.jfCode)),
            cell(escapeHtml(l.lot)),
            cell(escapeHtml(l.poNumber)),
            cell(String(l.units), NUM),
            cell(String(l.cartons), NUM),
            cell(l.cbm.toFixed(3), NUM),
            cell(l.weightKg.toFixed(1), NUM),
        ].join('')}</tr>`).join('');
        const bold = 'font-weight:700;';
        const subtotal = `<tr>${[
            `<td colspan="4" style="${TD}${bold}">Subtotal / 小计</td>`,
            cell(String(g.totals.units), NUM + bold),
            cell(String(g.totals.cartons), NUM + bold),
            cell(g.totals.cbm.toFixed(3), NUM + bold),
            cell(g.totals.weightKg.toFixed(1), NUM + bold),
        ].join('')}</tr>`;
        return [
            `<p style="margin:18px 0 6px;font-family:${FONT};font-size:15px;line-height:1.4;font-weight:700;color:${INK};">${escapeHtml(g.supplier)}`
                + `${where ? ` <span style="font-weight:500;color:${MUTED};">· ${escapeHtml(where)}</span>` : ''}</p>`,
            `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;margin:0 0 8px;">`,
            `<tr>${['Product', 'Code', 'LOT', 'PO'].map(h => `<th style="${TH}">${h}</th>`).join('')}`
                + `${['Units', 'Cartons', 'CBM', 'Kg'].map(h => `<th style="${TH}${NUM}">${h}</th>`).join('')}</tr>`,
            rows,
            subtotal,
            '</table>',
        ].join('');
    }).join('');
    return `${facts(head)}${tables}`;
}

/** What a contents email listed, kept on the booking: the same shape a Request to send keeps. */
const contentsSnapshot = lines => requestLinesSnapshot(lines, 'generate');

/**
 * Whether the container still holds what the contents email listed: lines
 * added or removed, units or LOT changed (src/lib/request-to-send-drift.js, the
 * rule the Request to send flag uses). `snapshot` is contents_json as stored;
 * `lines` the container's lines now. No usable snapshot → null (nothing sent).
 * → { stale, changes } or null.
 */
function contentsDrift(snapshot, lines) {
    const then = parseRequestLines(snapshot);
    if (!then) return null;
    return requestDrift(then.lines, contentsSnapshot(lines).lines);
}

module.exports = {
    TOKEN_RE, newToken, isToken, freightUrl,
    SERVICES, SERVICE_LABELS, SERVICE_LABELS_ZH, CONTAINER_SIZES, CURRENCIES,
    EXPIRY_DAYS, EXPIRY_MIN_DAYS, EXPIRY_MAX_DAYS, WINDOW_MAX_DAYS, SAILING_MARGIN_DAYS, MAX_OPTIONS,
    ANSWER_LIMIT, MAX_FORWARDERS_PER_SEND, MAX_CONTACTS,
    BOOKING_STATUSES, LIVE_BOOKING_STATUSES, REVOKE_REASONS, NOTE_MAX,
    isYmd, toYmd, addDays, daysBetween, arrivalDate, jsonArray, servicesOf,
    validateForwarder, validateRequest, requestState, isEditable, validateOptions, validateStaffAnswer,
    validateBookingRef, optionalText,
    money, serviceLabel, requestFacts, quoteBlockHtml, optionFacts, bookingBlockHtml,
    groupContentsBySupplier, contentsHtml, contentsSnapshot, contentsDrift,
};
