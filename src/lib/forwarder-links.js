'use strict';

// A container's forwarder as a row of ShipLine's Forwarders list (2026-11-20).
// Pure rules; the routes are in src/services/shipment-routes.js,
// shipment-book.js and shipment-sync.js (merge); the backfill is
// tools/backfill-forwarder-ids.js.
//
// - A container shows the forwarder of its live freight booking (requested or
//   confirmed) when it has one — derived when read, never written, because the
//   forwarder confirms on the supplier portal, whose login writes only
//   freight_bookings. Otherwise its own pair (forwarder_id, forwarder), else
//   the text typed before the list existed.
// - shipments.forwarder and shipments.forwarder_id are one value: every write
//   sets both. Assigning resolves a forwarder; a merge preserves a stored pair
//   as it is (a renamed or removed forwarder's pair is never re-resolved).
// - The payment side (payment_extras.forwarder_id) is in src/lib/payment-extras.js.

const FORWARDER_MAX = 255;

// The same key as supplierKey (src/services/shipment-payments.js).
const nameKey = v => String(v ?? '').trim().replace(/\s+/g, ' ').toLowerCase();
const strictId = v => ((typeof v === 'number' || (typeof v === 'string' && /^\s*\d+\s*$/.test(v)))
    && Number.isSafeInteger(Number(v)) && Number(v) > 0 ? Number(v) : NaN);
const idOrNull = v => (v == null ? null : Number(v));

// What a shipment row says about its forwarder. With the booking join
// (bk_* columns from shipmentSelect) it also says whether a live booking holds
// the container; without it (a raw SELECT * FROM shipments) it gives the
// container's own pair and makes no claim about a booking.
function effectiveForwarder(row) {
    const own = { forwarder: row.forwarder || null, forwarderId: idOrNull(row.forwarder_id) };
    const ownSource = own.forwarder || own.forwarderId != null ? 'container' : null;
    if (row.bk_count === undefined) return { ...own, forwarderSource: ownSource };
    if (Number(row.bk_count) > 0 && row.bk_id != null) {
        return {
            forwarder: row.bk_forwarder_name || null,
            forwarderId: idOrNull(row.bk_forwarder_id),
            forwarderSource: 'booking',
            booking: { id: Number(row.bk_id), status: row.bk_status, bookingRef: row.bk_ref || null },
        };
    }
    return { ...own, forwarderSource: ownSource, booking: null };
}

// PATCH /shipments/:id — { forwarderId?, forwarder? } against the stored pair
// { id, name }. One of:
//   { action: 'untouched' }          nothing changes
//   { action: 'clear' }              no forwarder
//   { action: 'byId', id }           that live forwarder, by its name in the list
//   { action: 'byText', text }       the text, linked when exactly one live
//                                    forwarder carries that name (pairFromText)
//   { action: 'set', id: null, name } the text as typed, unlinked on purpose
//   { error }
// The stored forwarder sent back by id, or its name sent back as text, changes
// nothing: a pair is never refreshed from the list after a rename.
function decideShipmentForwarder({ stored, body }) {
    const idGiven = body.forwarderId !== undefined;
    const textGiven = body.forwarder !== undefined;
    const text = textGiven && body.forwarder != null ? String(body.forwarder).trim() : '';
    if (idGiven && body.forwarderId !== null) {
        const n = strictId(body.forwarderId);
        if (Number.isNaN(n)) return { error: 'forwarderId must be a forwarder id, or null.' };
        if (stored.id != null && Number(stored.id) === n) return { action: 'untouched' };
        return { action: 'byId', id: n };
    }
    if (text.length > FORWARDER_MAX) return { error: `forwarder must be ${FORWARDER_MAX} characters or fewer.` };
    if (idGiven) return text ? { action: 'set', id: null, name: text } : { action: 'clear' };
    if (!textGiven) return { action: 'untouched' };
    if (!text) return { action: 'clear' };
    if (stored.name != null && nameKey(stored.name) === nameKey(text)) return { action: 'untouched' };
    return { action: 'byText', text };
}

// The booking modal (POST /shipments/:id/book) sends the forwarder as text,
// and has always left the stored one alone when it sends none (COALESCE).
function decideBookForwarder({ stored, text }) {
    const t = text == null ? '' : String(text).trim();
    if (!t) return { action: 'untouched' };
    if (stored.name != null && nameKey(stored.name) === nameKey(t)) return { action: 'untouched' };
    return { action: 'byText', text: t.slice(0, FORWARDER_MAX) };
}

// Text to a pair: linked when exactly one live forwarder has that name (then
// the list's spelling), else the text unlinked. `forwarders`: [{ id, name, deleted_at }].
function pairFromText(text, forwarders) {
    const t = String(text ?? '').trim();
    const key = nameKey(t);
    const hits = forwarders.filter(f => !f.deleted_at && nameKey(f.name) === key);
    return hits.length === 1 ? { id: Number(hits[0].id), name: hits[0].name } : { id: null, name: t };
}

// A merge: the survivor keeps its own pair if it has one, else takes the
// loser's — both columns together, never one from each.
function mergeForwarderPair(survivor, loser) {
    const has = s => s.forwarder_id != null || Boolean(s.forwarder && String(s.forwarder).trim());
    const from = has(survivor) ? survivor : loser;
    return { forwarder: from.forwarder ?? null, forwarder_id: from.forwarder_id ?? null };
}

// The backfill's match of a typed name to the list: an exact name to exactly
// one live forwarder; else `similar` (sameSupplier from
// shipment-payment-extract.js) when it names exactly one. A name that also
// matches a removed forwarder is a collision, left for a person to decide.
//   { result: 'linked', id, name, how: 'exact' | 'similar' }
//   { result: 'ambiguous', ids } | { result: 'collision', ids, deletedIds } | { result: 'unmatched' }
function matchForBackfill(name, forwarders, similar) {
    const key = nameKey(name);
    if (!key) return { result: 'unmatched' };
    const live = forwarders.filter(f => !f.deleted_at);
    const gone = forwarders.filter(f => f.deleted_at);
    const matches = f => nameKey(f.name) === key || Boolean(similar && similar(name, f.name));
    const exact = live.filter(f => nameKey(f.name) === key);
    const hits = exact.length ? exact : live.filter(matches);
    const goneHits = gone.filter(matches);
    if (goneHits.length) return { result: 'collision', ids: hits.map(f => Number(f.id)), deletedIds: goneHits.map(f => Number(f.id)) };
    if (!hits.length) return { result: 'unmatched' };
    if (hits.length > 1) return { result: 'ambiguous', ids: hits.map(f => Number(f.id)) };
    return { result: 'linked', id: Number(hits[0].id), name: hits[0].name, how: exact.length ? 'exact' : 'similar' };
}

// tools/backfill-forwarder-ids.js --apply: a manifest row the person
// reviewed is written only while the row and the forwarder are as they were
// reviewed. `entry`: { value, forwarderId, forwarderName }; `row`: the row now
// as { forwarder_id, value } (null = gone); `forwarder`: { name, deleted_at }
// now (null = gone). null = write it, else why not.
function revalidateLink({ entry, row, forwarder }) {
    if (!row) return 'the row is gone';
    if (row.forwarder_id != null) return 'the row is linked already';
    if (row.value !== entry.value) return 'its name changed since the review';
    if (!forwarder) return 'the forwarder is gone';
    if (forwarder.deleted_at) return 'the forwarder was removed';
    if (forwarder.name !== entry.forwarderName) return 'the forwarder was renamed since the review';
    return null;
}

// --repair: a linked row --check listed is relinked only while it still has
// the link and the name that were reviewed. null = write it, else why not.
function revalidateRepair({ entry, row }) {
    if (!row) return 'the row is gone';
    if (Number(row.forwarder_id) !== Number(entry.forwarderId)) return 'its link changed since the review';
    if (row.value !== entry.value) return 'its name changed since the review';
    return null;
}

// tools/verify-shipments.js: a live booking recorded its container's name
// when it was attached (draft_container_name). It still names that container
// when it equals the container's name or reference, or when the number the
// recorded draft name reserved (hintReference, S.parseNameHint) is the
// container's reference now (the draft "… - 325" booked as 325). Nothing
// recorded: nothing to compare. A miss is reported for review, not refused —
// it is how an id handed to another container after a re-seed shows.
function bookingNameMatches({ bookedName, name, reference, hintReference }) {
    const booked = nameKey(bookedName);
    if (!booked) return true;
    if (booked === nameKey(name) || booked === nameKey(reference)) return true;
    return Boolean(hintReference && reference && nameKey(hintReference) === nameKey(reference));
}

// tools/backfill-shipments.js --apply: shipment ids are AUTO_INCREMENT, so
// seeding an emptied table hands old ids to other containers. Refused while
// rows elsewhere still point at shipment ids. null, or the reason.
function reseedGuard({ shipmentCount, dependants }) {
    if (Number(shipmentCount) > 0) return null;
    const held = Object.entries(dependants || {}).filter(([, n]) => Number(n) > 0);
    if (!held.length) return null;
    return `shipments is empty but rows still point at shipment ids (${held.map(([t, n]) => `${t}: ${n}`).join(', ')}). `
        + 'Seeding would hand those ids to other containers: remap them first.';
}

module.exports = {
    FORWARDER_MAX,
    nameKey,
    effectiveForwarder,
    decideShipmentForwarder,
    decideBookForwarder,
    pairFromText,
    mergeForwarderPair,
    matchForBackfill,
    revalidateLink,
    revalidateRepair,
    bookingNameMatches,
    reseedGuard,
};
