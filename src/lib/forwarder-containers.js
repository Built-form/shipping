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

// ── three duties, one holder each (2026-11-24) ───────────────────────────
//
// A container is no longer one forwarder's. It has three DUTIES, each with at
// most one holder, and one forwarder may hold several:
//
//   freight   origin to the UK port: the number, the loading and departure
//             dates, the loading photos, the bill of lading (all of what a
//             forwarder was asked for before this)
//   delivery  UK port to the warehouse: the delivery date
//   customs   the customs entry: the draft entry
//
// "End to end" is one forwarder holding freight and delivery; "port to port" is
// freight alone. WHO HOLDS A DUTY is ownerOf's rule, applied per duty: the live
// booking that covers it (its `scope`) outranks the pick on the container, and a
// covering booking still waiting for the forwarder leaves THAT duty with nobody.
// Customs is picked by hand only. ownerOf above is exactly the freight duty.
//
// One rule, held twice, as before: dutiesOf (on rows a route has locked) and
// the view portal_forwarder_containers_v (2026-11-24_11).

const DUTIES = Object.freeze(['freight', 'delivery', 'customs']);
// What a booking may cover: the freight to the UK port, the freight and the
// delivery to the warehouse, or the delivery alone (a booking for the UK leg,
// made on a delivery request to a UK haulier).
const BOOKING_SCOPES = Object.freeze(['port_to_port', 'end_to_end', 'delivery']);
// A booking that says nothing covers what every booking covered before: freight.
const DEFAULT_SCOPE = 'port_to_port';
const SCOPE_DUTIES = Object.freeze({
    port_to_port: Object.freeze(['freight']),
    end_to_end: Object.freeze(['freight', 'delivery']),
    delivery: Object.freeze(['delivery']),
});

// ── a quote request's kind, and the scope its booking may have ───────────
//
// A request is for FREIGHT (sailings, to a freight forwarder) or for DELIVERY
// (collection at the UK port and delivery to the warehouse, to a UK haulier).
// The kind is set when the request is made and never changes: its options and
// its bookings are read by it. A booking's scope FOLLOWS its request's kind,
// so a client can never turn a haulier's booking into a freight booking (or
// the reverse) by what it sends: a delivery request books delivery only, and a
// freight request never does. That is what lets a container hold two live
// bookings, one per leg, without either reading as the other.
const REQUEST_KINDS = Object.freeze(['freight', 'delivery']);
const DEFAULT_KIND = 'freight';
const KIND_SCOPES = Object.freeze({ freight: Object.freeze(['port_to_port', 'end_to_end']), delivery: Object.freeze(['delivery']) });
/** The kind a new request is given. Nothing said is a freight request, as every request was. → { kind } | { error } */
function kindGiven(value) {
    if (value === undefined) return { kind: DEFAULT_KIND };
    return typeof value === 'string' && REQUEST_KINDS.includes(value)
        ? { kind: value }
        : { error: `kind must be one of: ${REQUEST_KINDS.join(', ')}.` };
}
/**
 * The scope of a booking on a request of `kind` (a row from before kinds has
 * none: freight), given what the caller sent. → { scope } | { error }
 */
function scopeFor(kind, given) {
    if (kind === 'delivery') {
        return given === undefined || given === 'delivery' ? { scope: 'delivery' } : { error: 'a delivery request books delivery only: scope cannot be set on it.' };
    }
    if (given === undefined) return { scope: DEFAULT_SCOPE };
    return typeof given === 'string' && KIND_SCOPES.freight.includes(given)
        ? { scope: given }
        : { error: `scope must be one of: ${KIND_SCOPES.freight.join(', ')}.` };
}
/** Do two bookings' scopes cover a duty in common? Then they cannot both be live on one container. */
function scopesOverlap(a, b) {
    const of = s => SCOPE_DUTIES[s || DEFAULT_SCOPE] || [];
    return of(a).some(duty => of(b).includes(duty));
}
/**
 * What a booking is said to cover. Nothing said (`undefined`) is DEFAULT_SCOPE:
 * an older client that sends no scope can never grant the delivery duty, and
 * the option's own service text is never read for it.
 * → { scope } | { error }
 */
function scopeGiven(value) {
    // What a FREIGHT booking may be given: never 'delivery', which only a delivery request's booking has (scopeFor).
    if (value === undefined) return { scope: DEFAULT_SCOPE };
    return typeof value === 'string' && ['port_to_port', 'end_to_end'].includes(value)
        ? { scope: value }
        : { error: 'scope must be one of: port_to_port, end_to_end.' };
}
// The pick on the container for each duty.
const PICK_COLUMN = Object.freeze({ freight: 'forwarder_id', delivery: 'delivery_forwarder_id', customs: 'customs_forwarder_id' });

/**
 * Who holds each duty of a container.
 *   shipment       the shipments row (or null), with its three picks
 *   bookings       its bookings with the request's forwarder:
 *                  [{ id, status, scope, forwarder_id }] (only live ones count)
 *   forwarderLive  id => is that forwarder on the list and not removed
 * → { freight, delivery, customs }, each
 *   { forwarderId, source: 'booking' | 'container', bookingId? }
 *   or { forwarderId: null, why } with ownerOf's reasons.
 */
function dutiesOf({ shipment, bookings, forwarderLive }) {
    const none = why => ({ forwarderId: null, why });
    const all = why => Object.fromEntries(DUTIES.map(d => [d, none(why)]));
    if (!shipment || shipment.deleted_at || shipment.merged_into_id != null) return all('gone');
    if (!isReal(shipment)) return all('not_real');
    const liveBookings = (bookings || []).filter(b => b && LIVE_BOOKING_STATUSES.includes(b.status));
    const out = {};
    for (const duty of DUTIES) {
        // At most one live booking covers a duty (a unique key); the newest, should rows ever disagree.
        const covering = liveBookings
            .filter(b => (SCOPE_DUTIES[b.scope || DEFAULT_SCOPE] || []).includes(duty))
            .sort((a, b) => Number(b.id || 0) - Number(a.id || 0))[0] || null;
        let found;
        if (covering) {
            if (covering.status !== 'confirmed') { out[duty] = none('booking_not_confirmed'); continue; }
            found = { forwarderId: idOrNull(covering.forwarder_id), source: 'booking', ...(covering.id != null ? { bookingId: Number(covering.id) } : {}) };
        } else if (shipment[PICK_COLUMN[duty]] != null) {
            found = { forwarderId: Number(shipment[PICK_COLUMN[duty]]), source: 'container' };
        } else {
            // Only the freight pick was ever typed as text.
            out[duty] = none(duty === 'freight' && String(shipment.forwarder ?? '').trim() ? 'forwarder_not_listed' : 'no_forwarder');
            continue;
        }
        if (found.forwarderId == null) out[duty] = none('no_forwarder');
        else if (!forwarderLive(found.forwarderId)) out[duty] = none('forwarder_removed');
        else out[duty] = found;
    }
    return out;
}

/** The duties one forwarder holds, in DUTIES' order. */
function heldBy(duties, forwarderId) {
    if (forwarderId == null || !duties) return [];
    const id = Number(forwarderId);
    return DUTIES.filter(d => duties[d] && duties[d].forwarderId != null && Number(duties[d].forwarderId) === id);
}

/** The forwarders attached to a container: [{ forwarderId, duties, sources }], the freight holder first. */
function attachedOf(duties) {
    const out = [];
    for (const duty of DUTIES) {
        const h = duties && duties[duty];
        if (!h || h.forwarderId == null) continue;
        let entry = out.find(e => e.forwarderId === Number(h.forwarderId));
        if (!entry) { entry = { forwarderId: Number(h.forwarderId), duties: [], sources: {} }; out.push(entry); }
        entry.duties.push(duty);
        entry.sources[duty] = h.source;
    }
    return out;
}

/** The duties a row of portal_forwarder_containers_v (or a row joined to it) says its forwarder holds. */
const dutiesOfFlags = row => DUTIES.filter(d => Number(row && row[`does_${d}`]) === 1);

/** Those flags shaped as dutiesOf's answer, for the one forwarder the row is of (`row.forwarder_id`). */
function dutiesFromFlags(row) {
    const held = dutiesOfFlags(row);
    return Object.fromEntries(DUTIES.map(d => [d, held.includes(d) && row.forwarder_id != null
        ? { forwarderId: Number(row.forwarder_id), source: 'view' }
        : { forwarderId: null, why: 'no_forwarder' }]));
}

/**
 * The view's link_source for one forwarder: 'booking' or 'container' ONLY for
 * the freight holder (what those two words meant before, and all an older
 * portal build treats as a row it may write for); 'duty' for a holder of
 * delivery and/or customs without freight; null for one that holds nothing.
 */
function linkSourceOf(duties, forwarderId) {
    const held = heldBy(duties, forwarderId);
    if (!held.length) return null;
    return held.includes('freight') ? duties.freight.source : 'duty';
}

/**
 * What one forwarder is shown of a container, by duty.
 * → { mode: 'full', source: 'booking' | 'container' | 'duty', duties: [...] }
 *   { mode: 'payment', source: 'payment', duties: [] }   its payments only
 *   { mode: 'none', source: null, duties: [] }
 */
function accessFor({ duties, real, forwarderId, paidForwarderIds }) {
    const held = heldBy(duties, forwarderId);
    if (held.length) return { mode: 'full', source: linkSourceOf(duties, forwarderId), duties: held };
    if (real && (paidForwarderIds || []).map(Number).includes(Number(forwarderId))) return { mode: 'payment', source: 'payment', duties: [] };
    return { mode: 'none', source: null, duties: [] };
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

/**
 * Finished: more than FINISHED_AFTER_DAYS days after the later of the day it
 * arrived and the latest delivery day held (`deliveryOn`). Dates only. One rule
 * for every forwarder of the container, so the one that delivers is not shut
 * out before it has. While a delivery holder exists and no order holds a
 * delivery date (`awaitingDelivery`), it is not finished until the container
 * is CLOSED (`closed`: every order received); then the fortnight from the
 * arrival applies.
 */
function isFinished(arrivedOn, today, { deliveryOn = null, awaitingDelivery = false, closed = false } = {}) {
    const a = dayNumber(arrivedOn);
    const t = dayNumber(today);
    if (a == null || t == null) return false;
    if (awaitingDelivery && !closed) return false;
    const d = dayNumber(deliveryOn);
    return t > Math.max(a, d == null ? a : d) + FINISHED_AFTER_DAYS;
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

/**
 * What the email asks one company for: of ITS duties, what we do not hold yet,
 * and always its papers. `held`: the duties that company holds (freight alone
 * when not said, as every forwarder was before 2026-11-24). A company that
 * carries only the UK leg, or only customs, is never asked for the number, the
 * loading date, the loading photos or the bill of lading.
 */
function containerAsks(held, dutiesHeld = ['freight']) {
    const h = held || {};
    const mine = dutiesHeld || [];
    if (!mine.length) return [];
    const asks = [];
    if (mine.includes('freight')) {
        if (!h.ecn) asks.push('ecn');
        if (!h.loadingDate) asks.push('loadingDate');
        if (!h.etd) asks.push('etd');
    }
    if (mine.includes('delivery') && !h.deliveryDate) asks.push('deliveryDate');
    asks.push('invoice');
    if (mine.includes('freight')) asks.push('photos', 'billOfLading');
    // The draft first, for us to check, when they send one (some never do); then the entry itself.
    if (mine.includes('customs')) asks.push('customsDraft', 'customsEntry');
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

// What a forwarder could propose before 2026-11-24, and all a ShipLine build from before then shows.
const ORIGINAL_FIELDS = Object.freeze(['ecn', 'loadingDate', 'etd']);
// With the delivery date (2026-11-24): proposed by whoever delivers to the warehouse.
const DETAIL_FIELDS = Object.freeze([...ORIGINAL_FIELDS, 'deliveryDate']);
// The duty each field belongs to: a forwarder proposes only the fields of the duties it holds.
const FIELD_DUTY = Object.freeze({ ecn: 'freight', loadingDate: 'freight', etd: 'freight', deliveryDate: 'delivery' });
/** The fields of a proposal (`requested`) that are outside the duties its forwarder holds. */
const fieldsOutside = (requested, dutiesHeld) => Object.keys(requested || {}).filter(f => !(dutiesHeld || []).includes(FIELD_DUTY[f]));
/**
 * Does the client deciding a proposal know everything it carries? A field
 * beyond the original three must be NAMED by the call (`named`: the keys of a
 * claim's `expected`, a send-back's `fields`): a ShipLine build from before
 * that field shows three rows and names three, and must not decide a fourth it
 * cannot show. A proposal of the original three is decided exactly as before.
 */
const clientKnows = (requested, named) => Object.keys(requested || {})
    .filter(f => !ORIGINAL_FIELDS.includes(f))
    .every(f => Array.isArray(named) && named.includes(f));
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

// ── the delivery date: a wall-clock value, as text, to the minute ────────
//
// orders.delivery_date is a DATETIME that staff type on the container card,
// always with a time. A forwarder proposes a day, with a time when one is
// booked. ONE form everywhere it is compared: 'YYYY-MM-DD' when the hour and
// minute are 00:00, else 'YYYY-MM-DDTHH:MM'. "No time" and midnight are the
// same thing by design, and seconds never count. What we HOLD comes out of SQL
// in this form already (DELIVERY_SQL below, the same expression the portal's
// view uses), so no Date object is involved; what comes IN goes through
// normDelivery. The portal and ShipLine carry the same rule and the same
// test vectors.

/** A delivery value in the one form, or null when it is not one. */
function normDelivery(v) {
    const m = /^(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?)?$/.exec(typeof v === 'string' ? v.trim() : '');
    if (!m || !validDay(m[1])) return null;
    if (m[2] === undefined) return m[1];
    if (Number(m[2]) > 23 || Number(m[3]) > 59) return null;
    return m[2] === '00' && m[3] === '00' ? m[1] : `${m[1]}T${m[2]}:${m[3]}`;
}
/** The day a delivery value falls on. */
const deliveryDay = (v) => { const n = normDelivery(v); return n ? n.slice(0, 10) : null; };
/** A delivery value as it is written to orders.delivery_date: the shape a hand-typed one has. */
const deliverySql = (v) => { const n = normDelivery(v); return n ? `${n.slice(0, 10)} ${n.length > 10 ? n.slice(11) : '00:00'}:00` : null; };
/**
 * The delivery value of a column, in the one form, for a SELECT. A zero date
 * (some imported rows hold one) is no date.
 */
const deliverySelect = col => `IF(${col} >= '1000-01-01 00:00:00', IF(DATE_FORMAT(${col}, '%H:%i') = '00:00', DATE_FORMAT(${col}, '%Y-%m-%d'), DATE_FORMAT(${col}, '%Y-%m-%dT%H:%i')), NULL)`;

const byText = (a, b) => (a === b ? 0 : a === null ? -1 : b === null ? 1 : a < b ? -1 : 1);
/**
 * What a container holds for delivery, from the value of EVERY member order
 * (null for an order without one).
 * → { deliveryDate, deliveryMixed }: the value they all share (null when none
 *   has one), or deliveryDate null with deliveryMixed the distinct values in
 *   order, null first when an order has none.
 */
function heldDelivery(values) {
    const distinct = [...new Set((values || []).map(v => normDelivery(v)))].sort(byText);
    if (distinct.length <= 1) return { deliveryDate: distinct[0] ?? null, deliveryMixed: null };
    return { deliveryDate: null, deliveryMixed: distinct };
}
/** The delivery days a container holds, whether its orders agree or not. */
function heldDeliveryDays(held) {
    const h = held || {};
    const values = Array.isArray(h.deliveryMixed) ? h.deliveryMixed : [h.deliveryDate];
    return values.map(deliveryDay).filter(Boolean);
}

/**
 * What a claim records of the container: what it holds (`held`: ecn,
 * loadingDate, etd), and, per member order (`members`: [{ id, delivery_at }],
 * read under the lock), which orders are on it and what each holds for
 * delivery. finishRefusal compares all of it.
 */
function claimSnapshot(held, members) {
    const rows = [...(members || [])].sort((a, b) => Number(a.id) - Number(b.id));
    return {
        ...held,
        ...heldDelivery(rows.map(m => m.delivery_at)),
        members: rows.map(m => Number(m.id)),
        deliveryByOrder: Object.fromEntries(rows.map(m => [Number(m.id), normDelivery(m.delivery_at)])),
    };
}

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

/** What a proposal asks for: any of ecn, loadingDate, etd, deliveryDate, and nothing else. From requested_json. */
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
    const delivery = normDelivery(raw.deliveryDate);
    if (delivery) out.deliveryDate = delivery;
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
 * Why a proposal's forwarder may no longer have it accepted; null when it may.
 * `duties`: who holds what on the locked rows (dutiesOf). A proposal is
 * accepted or dismissed WHOLE: one field outside the duties its forwarder
 * holds now makes all of it dismiss-only.
 *   NOT_THEIR_CONTAINER   the forwarder holds nothing on the container
 *   NOT_THEIR_DUTY        it holds something, but not what `fields` need
 */
function dutyRefusal({ row, duties }) {
    const held = heldBy(duties, row.forwarder_id);
    if (!held.length) return { code: 'NOT_THEIR_CONTAINER' };
    const outside = fieldsOutside(proposalOf(row.requested_json), held);
    return outside.length ? { code: 'NOT_THEIR_DUTY', fields: outside } : null;
}

/**
 * Why a proposal cannot be claimed; null when it can. `rows`: EVERY proposal
 * row of the container (all forwarders), read under the lock, each with
 * `lapsed`; `duties`: who holds what on the locked rows (dutiesOf).
 */
function claimRefusal({ rows, id, duties }) {
    const row = (rows || []).find(r => Number(r.id) === Number(id));
    if (!row) return { code: 'NO_SUCH_PROPOSAL' };
    const busy = (rows || []).find(claimActive);
    if (busy) return { code: 'CLAIM_IN_PROGRESS', claimedBy: busy.claimed_by || null, claimedAt: busy.claimed_at_iso || null };
    if (!isWaiting(row)) return { code: 'ALREADY_DECIDED' };
    if (Number(liveProposal(rows, row.forwarder_id).id) !== Number(row.id)) return { code: 'SUPERSEDED' };
    return dutyRefusal({ row, duties });
}

const sameField = (field, a, b) => (field === 'ecn' ? normEcn(a) === normEcn(b) : (a || null) === (b || null));
const mixedKey = v => (Array.isArray(v) ? JSON.stringify(v.map(x => normDelivery(x)).sort(byText)) : '');

/**
 * The fields in which what was expected differs from what the container holds.
 * The delivery date is compared only when `expected` NAMES it (it has the key):
 * a ShipLine build from before the delivery date names three fields, and a
 * proposal of those three is decided on those three, as before. One that
 * carries a delivery date cannot be decided by such a build at all (clientKnows).
 */
function heldDiff(expected, held) {
    const e = expected || {};
    const h = held || {};
    const moved = ORIGINAL_FIELDS.filter(f => !sameField(f, e[f], h[f]));
    if (Object.prototype.hasOwnProperty.call(e, 'deliveryDate')
        && (normDelivery(e.deliveryDate) !== normDelivery(h.deliveryDate) || mixedKey(e.deliveryMixed) !== mixedKey(h.deliveryMixed))) {
        moved.push('deliveryDate');
    }
    return moved;
}

/**
 * The dates that would be out of order once this proposal is applied over what
 * is held, or null: loading on or before departure, departure on or before
 * EVERY delivery day (each one held, when the orders differ), by the day. A
 * pair is judged only when the proposal touches one of its sides: what we hold
 * in disorder already is not this proposal's to answer for. So a delivery date
 * before the departure is refused, and so is a departure moved past a delivery
 * date already held.
 * → null | ['loadingDate', 'etd'] | ['etd', 'deliveryDate'] | ['loadingDate', 'deliveryDate']
 */
function orderProblem(requested, held) {
    const r = requested || {};
    const h = held || {};
    const loading = r.loadingDate || h.loadingDate || null;
    const etd = r.etd || h.etd || null;
    const deliveries = r.deliveryDate ? [deliveryDay(r.deliveryDate)].filter(Boolean) : heldDeliveryDays(h);
    if ((r.loadingDate || r.etd) && loading && etd && loading > etd) return ['loadingDate', 'etd'];
    if ((r.etd || r.deliveryDate) && etd && deliveries.some(d => d < etd)) return ['etd', 'deliveryDate'];
    if ((r.loadingDate || r.deliveryDate) && loading && deliveries.some(d => d < loading)) return ['loadingDate', 'deliveryDate'];
    return null;
}
const datesInOrder = (requested, held) => orderProblem(requested, held) === null;

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
 *   members     the member orders now, read under the lock: [{ id, delivery_at }]
 * The one INTENDED difference from what the claim saw is the number: when the
 * proposal carries one, it must be on every member order now (the browser wrote
 * it between the claim and this), with the departure date its own reset wrote
 * (finishBaseline). Everything else must be as the claim recorded it: the
 * loading and departure dates, WHICH orders are on the container, and what each
 * holds for delivery. A claim from before those two were recorded (in flight
 * across a deploy) is not judged on them.
 */
function finishRefusal({ requested, claim, ecnReset, memberEcns, held, members }) {
    const r = requested || {};
    const h = held || {};
    const c = claim || {};
    if (r.ecn) {
        const want = normEcn(r.ecn);
        const ecns = memberEcns || [];
        if (!ecns.length || ecns.some(e => normEcn(e) !== want) || normEcn(h.ecn) !== want) return { code: 'NOT_ON_CONTAINER', fields: ['ecn'] };
    }
    const baseline = finishBaseline({ claim, requested: r, ecnReset });
    const moved = ORIGINAL_FIELDS.filter(f => !(f === 'ecn' && r.ecn) && !sameField(f, h[f], baseline[f]));
    if (moved.length) return { code: 'CONTAINER_CHANGED', fields: moved };
    const now = claimSnapshot({}, members);
    if (Array.isArray(c.members) && JSON.stringify(c.members.map(Number).sort((a, b) => a - b)) !== JSON.stringify(now.members)) {
        return { code: 'CONTAINER_CHANGED', fields: ['members'] };
    }
    if (c.deliveryByOrder && typeof c.deliveryByOrder === 'object'
        && now.members.some(id => normDelivery(c.deliveryByOrder[id]) !== now.deliveryByOrder[id])) {
        return { code: 'CONTAINER_CHANGED', fields: ['deliveryDate'] };
    }
    // A delivery date needs an order to carry it, as a number does.
    if (r.deliveryDate && !now.members.length) return { code: 'NO_ORDERS', fields: ['deliveryDate'] };
    const order = orderProblem(r, { ...h, deliveryDate: now.deliveryDate, deliveryMixed: now.deliveryMixed });
    if (order) return { code: 'DATES_OUT_OF_ORDER', fields: order };
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

// The kinds a forwarder could send before 2026-11-24, and all a ShipLine build from before then shows.
const ORIGINAL_KINDS = Object.freeze(['invoice', 'loading_photo', 'bill_of_lading']);
// With the DRAFT customs entry (2026-11-24): sent by whoever handles the container's customs, for our
// team to approve or send back before it is lodged. 13 characters: the portal's kind column holds 16.
// customs_draft: a draft entry, for us to approve or send back. customs_entry: the entry
// itself, as customs took it, with its MRN. Neither needs the other.
const UPLOAD_KINDS = Object.freeze([...ORIGINAL_KINDS, 'customs_draft', 'customs_entry']);
// The duty a kind of file belongs to. An invoice (null) is anyone's who holds a duty: each company bills its own part.
const KIND_DUTY = Object.freeze({ invoice: null, loading_photo: 'freight', bill_of_lading: 'freight', customs_draft: 'customs', customs_entry: 'customs' });
/**
 * Does the client deciding a file know its kind? A kind beyond the original three must be
 * NAMED by the call (`named`: the `kind` it sends): a ShipLine build from before that kind
 * must not accept or send back a file it has no words for.
 */
const clientKnowsKind = (kind, named) => ORIGINAL_KINDS.includes(kind) || named === kind;
/** May a forwarder that holds `dutiesHeld` send a file of this kind? */
function kindAllowed(kind, dutiesHeld) {
    if (!Object.prototype.hasOwnProperty.call(KIND_DUTY, kind)) return false;
    const mine = dutiesHeld || [];
    return KIND_DUTY[kind] === null ? mine.length > 0 : mine.includes(KIND_DUTY[kind]);
}
const DOCUMENT_TYPES = Object.freeze(['application/pdf', 'image/png', 'image/jpeg']);
const PICTURE_TYPES = Object.freeze(['image/png', 'image/jpeg']);
const MB = 1024 * 1024;
// The same caps the portal applies before it signs an address (an invoice's is the invoice store's own).
const UPLOAD_RULES = Object.freeze({
    invoice: Object.freeze({ types: DOCUMENT_TYPES, maxBytes: 10 * MB }),
    loading_photo: Object.freeze({ types: PICTURE_TYPES, maxBytes: 15 * MB }),
    bill_of_lading: Object.freeze({ types: DOCUMENT_TYPES, maxBytes: 15 * MB }),
    customs_draft: Object.freeze({ types: DOCUMENT_TYPES, maxBytes: 15 * MB }),
    customs_entry: Object.freeze({ types: DOCUMENT_TYPES, maxBytes: 15 * MB }),
});
const UPLOAD_EXTENSIONS = Object.freeze({ 'application/pdf': 'pdf', 'image/png': 'png', 'image/jpeg': 'jpg' });
// Our bucket's folder for each kind: an invoice beside the invoices, a photo beside the photos,
// a bill of lading and a draft customs entry with the forwarder documents.
const FILED_FOLDERS = Object.freeze({ invoice: 'shipment-payments', loading_photo: 'container-photos', bill_of_lading: 'forwarder-documents', customs_draft: 'forwarder-documents', customs_entry: 'forwarder-documents' });

// ── the MRN of a customs entry ───────────────────────────────────────────
//
// A customs entry is sent with its MRN (Movement Reference Number): 18
// characters, two digits (the year), two letters (the country), then fourteen
// letters or digits. The shape is checked, not the check character.
//
// The portal keeps it in the upload's `note`: the one text column of that row
// its login may write, so no new column and no new grant there. For this kind
// the note IS the MRN and nothing else; on accept it is written, in its one
// form, to shipment_forwarder_documents.reference.

/** An MRN as it is kept and compared: upper case, no spaces or hyphens. */
const normMrn = v => String(v ?? '').toUpperCase().replace(/[\s-]+/g, '');
/** What is wrong with an MRN: 'missing' | 'shape' | null. */
function mrnProblem(v) {
    const mrn = normMrn(v);
    if (!mrn) return 'missing';
    return /^\d{2}[A-Z]{2}[A-Z0-9]{14}$/.test(mrn) ? null : 'shape';
}
/**
 * The reference number a file carries, for a kind that has one:
 * { reference } | { problem: 'missing' | 'shape' }; null for a kind with none
 * (its note is a note).
 */
function uploadReference(row) {
    if (!row || row.kind !== 'customs_entry') return null;
    const problem = mrnProblem(row.note);
    return problem ? { problem } : { reference: normMrn(row.note) };
}

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
 * Why an upload cannot be accepted or sent back; null when it can. `duties`: who holds
 * what on the locked rows (dutiesOf). A file from a company that no longer holds its
 * kind's duty can be sent back (dismissed), never accepted: NOT_THEIR_CONTAINER when the
 * company holds nothing on the container, NOT_THEIR_DUTY (`kind`) when it holds another.
 */
function uploadGate({ row, duties, action }) {
    const no = (status, code, allowedNext = ['reload']) => ({ status, code, allowedNext });
    if (!row) return no(404, 'NO_SUCH_UPLOAD');
    const state = uploadReviewState(row);
    if (state === 'withdrawn') return no(409, 'WITHDRAWN');
    if (state === 'not_sent') return no(409, 'NOT_SENT');
    if (state !== 'open') return no(409, 'ALREADY_DECIDED');
    if (action === 'accept') {
        const held = heldBy(duties, row.forwarder_id);
        if (!held.length) return no(409, 'NOT_THEIR_CONTAINER', ['reject']);
        if (!kindAllowed(row.kind, held)) return { ...no(409, 'NOT_THEIR_DUTY', ['reject']), kind: row.kind };
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
    DUTIES, BOOKING_SCOPES, DEFAULT_SCOPE, SCOPE_DUTIES, REQUEST_KINDS, DEFAULT_KIND, KIND_SCOPES, kindGiven, scopeFor, scopesOverlap, PICK_COLUMN, scopeGiven, dutiesOf, heldBy, attachedOf, linkSourceOf, accessFor, dutiesOfFlags, dutiesFromFlags,
    isSendKey, noticeState, containerAsks, containerUrl,
    ORIGINAL_FIELDS, DETAIL_FIELDS, FIELD_DUTY, fieldsOutside, clientKnows, CLAIM_MINUTES, NOTE_MIN, NOTE_MAX,
    normDelivery, deliveryDay, deliverySql, deliverySelect, heldDelivery, claimSnapshot,
    validDay, normEcn, ecnProblem, proposalOf, liveProposal, isWaiting, dutyRefusal, claimRefusal, heldDiff, orderProblem, datesInOrder,
    finishBaseline, finishRefusal, toReviewCount, cleanNote, noteProblem,
    ORIGINAL_KINDS, UPLOAD_KINDS, KIND_DUTY, kindAllowed, clientKnowsKind, normMrn, mrnProblem, uploadReference, UPLOAD_RULES, uploadType, uploadFileRefusal, uploadReviewState, uploadGate, filingName, filingNameClash, filedKeyFor,
};
