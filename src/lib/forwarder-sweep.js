'use strict';

// Housekeeping rules for a forwarder's container page (docs/forwarder-containers.md).
// Pure: the tools that use them (tools/sweep-forwarder-copies.js,
// tools/backfill-container-forwarders.js) do the reading and, when told to, the
// writing. Nothing here runs on a schedule.

const SWEEP_AFTER_DAYS = 7;
const DAY_MS = 24 * 3600 * 1000;
const olderThan = (iso, nowMs, days) => {
    const at = iso instanceof Date ? iso.getTime() : Date.parse(iso);
    return Number.isFinite(at) && nowMs - at > days * DAY_MS;
};

/**
 * Whether OUR bucket's copy of an upload (the object at its `filed_key`) can be
 * removed. Accepting copies the forwarder's file to `filed_key` before the
 * transaction that files it; a crash in between, or a refusal after the copy,
 * leaves one object nothing points at. It can go only once the upload can no
 * longer be accepted:
 *   rejected   more than SWEEP_AFTER_DAYS after it was sent back (decided_at)
 *   withdrawn  more than SWEEP_AFTER_DAYS after it was taken back (withdrawn_at:
 *              never by when it was sent)
 * Never an undecided upload's (an accept may be about to use it), however old;
 * and never a received one's: that copy IS the filed document.
 *   row: { filed_key, upload_status, review_status, decided_at, withdrawn_at }
 */
function copySweepable(row, nowMs) {
    if (!row || typeof row.filed_key !== 'string' || row.filed_key === '') return false;
    if (row.review_status === 'received') return false;
    if (row.upload_status === 'withdrawn') return olderThan(row.withdrawn_at, nowMs, SWEEP_AFTER_DAYS);
    if (row.upload_status === 'sent' && row.review_status === 'rejected') return olderThan(row.decided_at, nowMs, SWEEP_AFTER_DAYS);
    return false;
}

/**
 * The forwarder a container can be given, from the payments already linked to
 * forwarders on it: the ONE forwarder all of them name. null (with why) when
 * it already has one, a live booking holds it, it has arrived, it is not a real
 * container, no payment on it is linked to a forwarder, or they name more than
 * one.
 *   container: { stage, deleted_at, merged_into_id, reference, forwarder_id, forwarder, arrived }
 *   liveBooking: boolean
 *   paidForwarderIds: the forwarder_id of each live forwarder charge on it (null for one not linked)
 */
function forwarderToGive({ container, liveBooking, paidForwarderIds }) {
    const no = why => ({ forwarderId: null, why });
    const c = container || {};
    if (c.deleted_at || c.merged_into_id != null || !c.reference || !['BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED'].includes(c.stage)) return no('not_real');
    if (c.forwarder_id != null) return no('has_forwarder');
    if (String(c.forwarder || '').trim()) return no('has_typed_forwarder');
    if (liveBooking) return no('has_booking');
    if (c.arrived) return no('arrived');
    const all = paidForwarderIds || [];
    if (all.length === 0) return no('no_payment');
    if (all.some(id => id == null)) return no('unlinked_payment');
    const distinct = [...new Set(all.map(Number))];
    if (distinct.length !== 1) return no('several_forwarders');
    return { forwarderId: distinct[0], why: null };
}

module.exports = { SWEEP_AFTER_DAYS, copySweepable, forwarderToGive };
