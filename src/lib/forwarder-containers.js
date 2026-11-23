'use strict';

const { supplierKey } = require('./payment-documents');

// A forwarder's container page on the supplier portal
// (docs/forwarder-containers.md). Pure rules; the routes are in
// src/services/forwarder-container-routes.js, the portal's view in
// src/db/migrate/2026-11-23_11_portal_forwarder_container_views.sql.
//
// WHOSE CONTAINER IT IS. One rule, held twice: here (ownerOf, on rows a route
// has locked) and in the view portal_forwarder_containers_v (what the portal
// reads). tools/test-portal-forwarder-container-view.js holds the two together
// on the same fixtures.
//
//   - Only a real container: not deleted, not merged away, with a number, and
//     a stored stage from BOOKED on. (The stage SHOWN is the effective one, the
//     later of stored and what the member orders justify; that can never be
//     earlier than the stored one, and an open or cancelled stage is never
//     derived, so the two cannot disagree on this.)
//   - Its forwarder is the one the API shows (src/lib/forwarder-links.js): the
//     live booking's when it has one, else the pair on the container itself.
//   - A live booking must be CONFIRMED. One still waiting for the forwarder
//     shows the container to nobody, a forwarder picked by hand included: the
//     booking outranks the pair.
//   - The forwarder must be on the list (an id, not typed text) and live.
//
// A forwarder that is not the owner but was SENT a payment with a charge on
// the container sees it for that payment only (visibilityFor).

const REAL_STAGES = Object.freeze(['BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED']);
const LIVE_BOOKING_STATUSES = Object.freeze(['requested', 'confirmed']);
// The Freight page's own number (FINISHED_AFTER_DAYS in src/lib/freight-quotes.js).
const FINISHED_AFTER_DAYS = 14;
// Who may accept, send back, dismiss or email: as a telex release (FILE_ROLES).
const DECIDE_ROLES = Object.freeze(['admin', 'standard']);

const mayDecide = userType => DECIDE_ROLES.includes(userType);
const idOrNull = v => (v == null ? null : Number(v));
const cents = v => Math.round(Number(v) * 100);

/** The container half of the rule: a live, real container. */
function isReal(shipment) {
    if (!shipment || shipment.deleted_at || shipment.merged_into_id != null) return false;
    if (!String(shipment.reference ?? '').trim()) return false;
    return REAL_STAGES.includes(shipment.stage);
}

/**
 * Whose container it is.
 *   shipment      the shipments row (or null)
 *   booking       its live or newest booking with the request's forwarder:
 *                 { status, forwarder_id }, or null
 *   forwarderLive id => is that forwarder on the list and not removed
 * → { forwarderId, source: 'booking' | 'container' }
 *   or { forwarderId: null, why } with why one of
 *   gone | not_real | booking_not_confirmed | no_forwarder | forwarder_not_listed | forwarder_removed
 */
function ownerOf({ shipment, booking, forwarderLive }) {
    const none = why => ({ forwarderId: null, why });
    if (!shipment || shipment.deleted_at || shipment.merged_into_id != null) return none('gone');
    if (!isReal(shipment)) return none('not_real');
    const liveBooking = booking && LIVE_BOOKING_STATUSES.includes(booking.status) ? booking : null;
    let found;
    if (liveBooking) {
        if (liveBooking.status !== 'confirmed') return none('booking_not_confirmed');
        found = { forwarderId: idOrNull(liveBooking.forwarder_id), source: 'booking' };
    } else if (shipment.forwarder_id != null) {
        found = { forwarderId: Number(shipment.forwarder_id), source: 'container' };
    } else {
        return none(String(shipment.forwarder ?? '').trim() ? 'forwarder_not_listed' : 'no_forwarder');
    }
    if (found.forwarderId == null) return none('no_forwarder');
    if (!forwarderLive(found.forwarderId)) return none('forwarder_removed');
    return found;
}

/**
 * What one forwarder is shown of a container.
 *   owner             ownerOf's answer
 *   real              isReal(shipment)
 *   paidForwarderIds  the forwarders with a SENT payment that has a charge on it
 * → { mode: 'full', source: 'booking' | 'container' }   may send things for it
 *   { mode: 'payment', source: 'payment' }               its payments only
 *   { mode: 'none', source: null }
 */
function visibilityFor({ owner, real, forwarderId, paidForwarderIds }) {
    const id = Number(forwarderId);
    if (owner && owner.forwarderId != null && Number(owner.forwarderId) === id) return { mode: 'full', source: owner.source };
    if (real && (paidForwarderIds || []).map(Number).includes(id)) return { mode: 'payment', source: 'payment' };
    return { mode: 'none', source: null };
}

const dayNumber = (ymd) => {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(ymd ?? ''));
    return m ? Math.round(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])) / 86400000) : null;
};

/** Finished: more than FINISHED_AFTER_DAYS days after it arrived. Dates only. */
function isFinished(arrivedOn, today) {
    const a = dayNumber(arrivedOn);
    const t = dayNumber(today);
    if (a == null || t == null) return false;
    return t > a + FINISHED_AFTER_DAYS;
}

/**
 * The payments with a charge on one container, from the rows of
 * portal_forwarder_payment_lines_v ({ payment_id, shipment_id,
 * shipment_reference, amount, extra_kind }) of any number of payments.
 * → [{ paymentId, share, lines: [{ kind, amount }], alsoPays: [reference] }]
 *   in payment order. `alsoPays`: the other containers of the same transfer.
 */
function paymentsOnContainer(lines, shipmentId) {
    const sid = Number(shipmentId);
    const byPayment = new Map();
    for (const l of lines || []) {
        const pid = Number(l.payment_id);
        if (!byPayment.has(pid)) byPayment.set(pid, { mine: [], others: [] });
        const group = byPayment.get(pid);
        if (l.shipment_id != null && Number(l.shipment_id) === sid) {
            group.mine.push({ kind: l.extra_kind || null, amount: cents(l.amount) / 100 });
        } else {
            const ref = String(l.shipment_reference ?? '').trim();
            if (ref && !group.others.includes(ref)) group.others.push(ref);
        }
    }
    return [...byPayment.entries()]
        .filter(([, g]) => g.mine.length)
        .sort((a, b) => a[0] - b[0])
        .map(([paymentId, g]) => ({
            paymentId,
            share: g.mine.reduce((n, l) => n + cents(l.amount), 0) / 100,
            lines: g.mine,
            alsoPays: g.others,
        }));
}

// ── the email to the forwarder ───────────────────────────────────────────

const SEND_KEY_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const isSendKey = v => typeof v === 'string' && SEND_KEY_RE.test(v);

/**
 * What a send key already stands for (the freight quote request's retry
 * rule): a key that has a row is reported as it stands and never emailed
 * again, except one Front refused.
 *   new | already_sent | send_unconfirmed | retry | clash
 */
function noticeState(row, { shipmentId, forwarderId }) {
    if (!row) return 'new';
    if (Number(row.shipment_id) !== Number(shipmentId) || Number(row.forwarder_id) !== Number(forwarderId)) return 'clash';
    if (row.status === 'sent') return 'already_sent';
    if (row.status === 'failed') return 'retry';
    return 'send_unconfirmed';
}

/** What the email asks for: what we do not hold yet, and always the papers. */
function containerAsks(held) {
    const h = held || {};
    const asks = [];
    if (!h.ecn) asks.push('ecn');
    if (!h.loadingDate) asks.push('loadingDate');
    if (!h.etd) asks.push('etd');
    asks.push('invoice', 'photos', 'billOfLading');
    return asks;
}

const containerUrl = (base, shipmentId) => `${String(base || '').replace(/\/+$/, '')}/freight-containers/${encodeURIComponent(shipmentId)}`;

// ── slice B: the container's number and dates, proposed by its forwarder ──
//
// A forwarder proposes; nothing changes until our team accept. The portal
// writes a row of its forwarder_container_details (insert only: the LIVE
// proposal of a forwarder for a container is its newest row, so a correction
// is another row). Accepting is three steps (docs/forwarder-containers.md):
//   claim   under locks: the proposal is the live one and waiting, the
//           container is still that forwarder's, it holds what the card showed,
//           and no other accept is in progress on it;
//   (the ECN, if one is proposed, is written by ShipLine in the browser, where
//    its wrong-number checks live)
//   finish  under locks: the ECN is on every member order, nothing else moved
//           since the claim, and then the dates are written and the proposal
//           is marked accepted, in one transaction.

const DETAIL_FIELDS = Object.freeze(['ecn', 'loadingDate', 'etd']);
// A claim nobody finished or released is no claim after this long.
const CLAIM_MINUTES = 10;
const NOTE_MIN = 3;
const NOTE_MAX = 500;

const validDay = (v) => {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(v ?? '').trim());
    if (!m) return null;
    const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
    return d.getUTCFullYear() === Number(m[1]) && d.getUTCMonth() === Number(m[2]) - 1 && d.getUTCDate() === Number(m[3]) ? m[0] : null;
};

/** A carrier number as it is COMPARED: upper case, no spaces or hyphens. null when empty. */
const normEcn = (v) => String(v ?? '').toUpperCase().replace(/[\s-]+/g, '') || null;

// ISO 6346: each letter has a value (11 and its multiples are skipped), each of
// the first ten characters is weighted by a power of two, and the sum modulo 11
// (10 counting as 0) is the eleventh character.
const LETTER_VALUES = (() => {
    const out = {};
    let n = 10;
    for (const ch of 'ABCDEFGHIJKLMNOPQRSTUVWXYZ') {
        if (n % 11 === 0) n += 1;
        out[ch] = n;
        n += 1;
    }
    return out;
})();
function checkDigitOk(number) {
    let sum = 0;
    for (let i = 0; i < 10; i++) {
        const ch = number[i];
        sum += (i < 4 ? LETTER_VALUES[ch] : Number(ch)) * (2 ** i);
    }
    return (sum % 11) % 10 === Number(number[10]);
}

/**
 * What is wrong with a proposed carrier number; null when nothing.
 *   empty | shape | check_digit
 * SEA (and an unknown mode): four letters, seven digits, the ISO 6346 check
 * digit. AIR: an air waybill number, taken as text of 5 to 40 characters.
 */
function ecnProblem(mode, value) {
    const raw = String(value ?? '').trim();
    if (!raw) return 'empty';
    if (mode === 'AIR') return raw.length >= 5 && raw.length <= 40 ? null : 'shape';
    const n = normEcn(raw);
    if (!/^[A-Z]{4}\d{7}$/.test(n)) return 'shape';
    return checkDigitOk(n) ? null : 'check_digit';
}

/** What a proposal asks for: any of ecn, loadingDate, etd, and nothing else. From requested_json. */
function proposalOf(json) {
    let raw = json;
    if (typeof json === 'string') { try { raw = JSON.parse(json); } catch { raw = null; } }
    const out = {};
    if (!raw || typeof raw !== 'object') return out;
    const ecn = typeof raw.ecn === 'string' ? raw.ecn.trim().toUpperCase() : '';
    if (ecn) out.ecn = ecn;
    for (const key of ['loadingDate', 'etd']) {
        const day = validDay(raw[key]);
        if (day) out[key] = day;
    }
    return out;
}

/** The live proposal of one forwarder among a container's rows: the newest by id, whatever its status. */
function liveProposal(rows, forwarderId) {
    let best = null;
    for (const r of rows || []) {
        if (Number(r.forwarder_id) !== Number(forwarderId)) continue;
        if (!best || Number(r.id) > Number(best.id)) best = r;
    }
    return best;
}

/** Waiting for our team: pending, or claimed and the claim lapsed (`lapsed` from the query). */
const isWaiting = row => Boolean(row) && (row.status === 'pending' || (row.status === 'applying' && Number(row.lapsed) === 1));
const claimActive = row => Boolean(row) && row.status === 'applying' && Number(row.lapsed) !== 1;

/**
 * Why a proposal cannot be claimed; null when it can. `rows`: EVERY proposal
 * row of the container (all forwarders), read under the lock, each with
 * `lapsed`; `ownerForwarderId`: whose container it is on the locked rows.
 */
function claimRefusal({ rows, id, ownerForwarderId }) {
    const row = (rows || []).find(r => Number(r.id) === Number(id));
    if (!row) return { code: 'NO_SUCH_PROPOSAL' };
    const busy = (rows || []).find(claimActive);
    if (busy) return { code: 'CLAIM_IN_PROGRESS', claimedBy: busy.claimed_by || null, claimedAt: busy.claimed_at_iso || null };
    if (!isWaiting(row)) return { code: 'ALREADY_DECIDED' };
    if (Number(liveProposal(rows, row.forwarder_id).id) !== Number(row.id)) return { code: 'SUPERSEDED' };
    if (ownerForwarderId == null || Number(ownerForwarderId) !== Number(row.forwarder_id)) return { code: 'NOT_THEIR_CONTAINER' };
    return null;
}

const sameField = (field, a, b) => (field === 'ecn' ? normEcn(a) === normEcn(b) : (a || null) === (b || null));

/** The fields in which what was expected differs from what the container holds. */
function heldDiff(expected, held) {
    const e = expected || {};
    const h = held || {};
    return DETAIL_FIELDS.filter(f => !sameField(f, e[f], h[f]));
}

/** Loading on or before departure, once this proposal is applied over what is held. */
function datesInOrder(requested, held) {
    const r = requested || {};
    if (!r.loadingDate && !r.etd) return true;
    const loading = r.loadingDate || (held && held.loadingDate) || null;
    const etd = r.etd || (held && held.etd) || null;
    return !(loading && etd && loading > etd);
}

/**
 * What the container must hold at finish, apart from what the proposal changes:
 * what the claim saw (`claim`), with one allowed difference. An accept that
 * REPLACED an existing ECN runs ShipLine's reset in the browser, which may move
 * the departure date; `ecnReset: { etd }` is the date that reset wrote (taken
 * from its own patch, never read back), and stands in for the claim's.
 */
function finishBaseline({ claim, requested, ecnReset }) {
    const c = claim || {};
    const base = { ecn: c.ecn ?? null, loadingDate: c.loadingDate ?? null, etd: c.etd ?? null };
    const was = normEcn(c.ecn);
    const replaced = Boolean(requested && requested.ecn) && Boolean(was) && was !== normEcn(requested.ecn);
    if (replaced && ecnReset && Object.prototype.hasOwnProperty.call(ecnReset, 'etd')) base.etd = validDay(ecnReset.etd);
    return base;
}

/**
 * Why a claimed proposal cannot be finished; null when it can.
 *   memberEcns  the carrier number each member order holds (null for none)
 *   held        what the container holds now, as the API derives it
 */
function finishRefusal({ requested, claim, ecnReset, memberEcns, held }) {
    const r = requested || {};
    const h = held || {};
    if (r.ecn) {
        const want = normEcn(r.ecn);
        const members = memberEcns || [];
        if (!members.length || members.some(e => normEcn(e) !== want) || normEcn(h.ecn) !== want) return { code: 'NOT_ON_CONTAINER', fields: ['ecn'] };
    }
    const baseline = finishBaseline({ claim, requested: r, ecnReset });
    const moved = DETAIL_FIELDS.filter(f => !(f === 'ecn' && r.ecn) && !sameField(f, h[f], baseline[f]));
    if (moved.length) return { code: 'CONTAINER_CHANGED', fields: moved };
    if (!datesInOrder(r, h)) return { code: 'DATES_OUT_OF_ORDER', fields: ['loadingDate', 'etd'] };
    return null;
}

/** What waits for our team: waiting rows on containers that are still that forwarder's. */
function toReviewCount({ proposals, uploads }) {
    const count = list => (list || []).filter(x => x.waiting && x.actionable).length;
    return count(proposals) + count(uploads);
}

const cleanNote = v => String(v ?? '').trim();
/** A note the forwarder reads: 'short' | 'long' | null. */
function noteProblem(v) {
    const note = cleanNote(v);
    if (note.length < NOTE_MIN) return 'short';
    return note.length > NOTE_MAX ? 'long' : null;
}

// ── slice C: a forwarder's files (invoice, loading photos, bill of lading) ──
//
// The portal keeps one row of its forwarder_uploads per file, with the file in
// its own bucket at a key that never changes once the row is `sent`. Our team
// accept it (it is copied to our bucket and filed: an invoice with the
// container's invoices, a photo with its photos, a bill of lading with its
// forwarder documents) or send it back with a note the forwarder reads.

const UPLOAD_KINDS = Object.freeze(['invoice', 'loading_photo', 'bill_of_lading']);
const DOCUMENT_TYPES = Object.freeze(['application/pdf', 'image/png', 'image/jpeg']);
const PICTURE_TYPES = Object.freeze(['image/png', 'image/jpeg']);
const MB = 1024 * 1024;
// The same caps the portal applies before it signs an address (an invoice's is the invoice store's own).
const UPLOAD_RULES = Object.freeze({
    invoice: Object.freeze({ types: DOCUMENT_TYPES, maxBytes: 10 * MB }),
    loading_photo: Object.freeze({ types: PICTURE_TYPES, maxBytes: 15 * MB }),
    bill_of_lading: Object.freeze({ types: DOCUMENT_TYPES, maxBytes: 15 * MB }),
});
const UPLOAD_EXTENSIONS = Object.freeze({ 'application/pdf': 'pdf', 'image/png': 'png', 'image/jpeg': 'jpg' });
// Our bucket's folder for each kind: an invoice beside the invoices, a photo beside the photos.
const FILED_FOLDERS = Object.freeze({ invoice: 'shipment-payments', loading_photo: 'container-photos', bill_of_lading: 'forwarder-documents' });

const PNG_SIGNATURE = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
const PDF_MARKER = [0x25, 0x50, 0x44, 0x46, 0x2d];
const startsAt = (bytes, signature, offset) => signature.every((b, i) => bytes[offset + i] === b);

/** The content type the bytes really are (PDF, PNG or JPEG), or null. As the portal tells it. */
function uploadType(bytes) {
    if (!bytes || typeof bytes.length !== 'number') return null;
    if (bytes.length >= PNG_SIGNATURE.length && startsAt(bytes, PNG_SIGNATURE, 0)) return 'image/png';
    if (bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return 'image/jpeg';
    const end = Math.min(bytes.length, 1024) - PDF_MARKER.length;
    for (let i = 0; i <= end; i++) if (startsAt(bytes, PDF_MARKER, i)) return 'application/pdf';
    return null;
}

/**
 * Why the bytes read at accept are not the file the portal recorded; null when they are.
 *   FILE_NOT_ITS_TYPE   not the type on the row, or not one its kind takes
 *   FILE_SIZE_CHANGED   not the size on the row
 *   FILE_TOO_LARGE      over the kind's cap
 */
function uploadFileRefusal({ row, bytes }) {
    const rules = UPLOAD_RULES[row.kind];
    const found = uploadType(bytes);
    if (!rules || !found || found !== row.content_type || !rules.types.includes(found)) return 'FILE_NOT_ITS_TYPE';
    if (bytes.length !== Number(row.file_size)) return 'FILE_SIZE_CHANGED';
    if (bytes.length > rules.maxBytes) return 'FILE_TOO_LARGE';
    return null;
}

/** open | received | rejected | withdrawn | not_sent: where an upload stands for our team. */
function uploadReviewState(row) {
    if (row.upload_status === 'withdrawn') return 'withdrawn';
    if (row.upload_status !== 'sent' || !row.final_key) return 'not_sent';
    return row.review_status === 'received' || row.review_status === 'rejected' ? row.review_status : 'open';
}

/**
 * Why an upload cannot be accepted or sent back; null when it can. `ownerForwarderId`:
 * whose container it is on the locked rows. One that is no longer its forwarder's can be
 * sent back (dismissed), never accepted.
 */
function uploadGate({ row, ownerForwarderId, action }) {
    const no = (status, code, allowedNext = ['reload']) => ({ status, code, allowedNext });
    if (!row) return no(404, 'NO_SUCH_UPLOAD');
    const state = uploadReviewState(row);
    if (state === 'withdrawn') return no(409, 'WITHDRAWN');
    if (state === 'not_sent') return no(409, 'NOT_SENT');
    if (state !== 'open') return no(409, 'ALREADY_DECIDED');
    if (action === 'accept' && (ownerForwarderId == null || Number(ownerForwarderId) !== Number(row.forwarder_id))) {
        return no(409, 'NOT_THEIR_CONTAINER', ['reject']);
    }
    return null;
}

/**
 * The name an accepted invoice is filed under: the payee name of the newest live payment
 * to that forwarder on that container (`payeeNames`, newest first), else its name on the
 * Forwarders list. So it lands where that forwarder's charges already are.
 */
function filingName({ payeeNames, listName } = {}) {
    for (const name of payeeNames || []) {
        const cleaned = typeof name === 'string' ? name.trim() : '';
        if (cleaned) return cleaned;
    }
    const listed = typeof listName === 'string' ? listName.trim() : '';
    return listed || null;
}

/** The name is exactly that of a supplier with goods on the container (as the invoice store compares names). */
function filingNameClash(name, suppliersOnBoard) {
    const key = supplierKey(name);
    return Boolean(key) && (suppliersOnBoard || []).some(s => supplierKey(s) === key);
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const FILED_NAME_MAX = 120;

/**
 * Where our team's copy of an accepted file lives in our bucket:
 * `<the kind's folder>/<random>/<a clean name with the type's extension>`. `random` is a
 * uuid made ONCE for the upload and kept on its row, so a retry writes over its own copy
 * and the address cannot be worked out from the upload's id and name.
 */
function filedKeyFor({ kind, random, filename, contentType }) {
    const folder = FILED_FOLDERS[kind];
    if (!folder) throw new Error('filedKeyFor: not a kind a forwarder uploads');
    if (typeof random !== 'string' || !UUID_RE.test(random)) throw new Error('filedKeyFor: random must be a lower-case uuid');
    const ext = UPLOAD_EXTENSIONS[contentType];
    if (!ext) throw new Error('filedKeyFor: not a type a forwarder uploads');
    let safe = String(filename || '').replace(/[^A-Za-z0-9._-]+/g, '_');
    const has = safe.toLowerCase();
    const sameType = has.endsWith(`.${ext}`) || (ext === 'jpg' && has.endsWith('.jpeg'));
    if (!sameType) safe = `${safe}.${ext}`;
    if (safe.length > FILED_NAME_MAX) {
        const dot = safe.lastIndexOf('.');
        safe = safe.slice(0, FILED_NAME_MAX - (safe.length - dot)) + safe.slice(dot);
    }
    return `${folder}/${random}/${safe}`;
}

module.exports = {
    REAL_STAGES, LIVE_BOOKING_STATUSES, FINISHED_AFTER_DAYS, DECIDE_ROLES,
    mayDecide, isReal, ownerOf, visibilityFor, isFinished, paymentsOnContainer,
    isSendKey, noticeState, containerAsks, containerUrl,
    DETAIL_FIELDS, CLAIM_MINUTES, NOTE_MIN, NOTE_MAX,
    validDay, normEcn, ecnProblem, proposalOf, liveProposal, isWaiting, claimRefusal, heldDiff, datesInOrder,
    finishBaseline, finishRefusal, toReviewCount, cleanNote, noteProblem,
    UPLOAD_KINDS, UPLOAD_RULES, uploadType, uploadFileRefusal, uploadReviewState, uploadGate, filingName, filingNameClash, filedKeyFor,
};
