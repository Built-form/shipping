'use strict';

// Housekeeping rules of a forwarder's container page (src/lib/forwarder-sweep.js;
// docs/forwarder-containers.md): which of our bucket's copies may be removed, and
// which forwarder a container can be given from its payments. Pure.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { SWEEP_AFTER_DAYS, copySweepable, forwarderToGive } = require('../src/lib/forwarder-sweep');

const NOW = Date.parse('2026-12-01T12:00:00Z');
const DAY = 24 * 3600 * 1000;
const ago = days => new Date(NOW - days * DAY).toISOString();
const row = (over = {}) => ({ filed_key: 'shipment-payments/x/invoice.pdf', upload_status: 'sent', review_status: 'open', decided_at: null, withdrawn_at: null, ...over });

test('sweep: an undecided upload\'s copy is never removed, however old', () => {
    assert.equal(copySweepable(row(), NOW), false);
    assert.equal(copySweepable(row({ sent_at: ago(400) }), NOW), false);
});

test('sweep: an accepted upload\'s copy is the filed document: never removed', () => {
    assert.equal(copySweepable(row({ review_status: 'received', decided_at: ago(400) }), NOW), false);
    // Even one that was received and then somehow withdrawn on the portal's side.
    assert.equal(copySweepable(row({ review_status: 'received', upload_status: 'withdrawn', withdrawn_at: ago(400) }), NOW), false);
});

test('sweep: a copy of an upload sent back goes a week after it was sent back', () => {
    assert.equal(SWEEP_AFTER_DAYS, 7);
    assert.equal(copySweepable(row({ review_status: 'rejected', decided_at: ago(7.1) }), NOW), true);
    assert.equal(copySweepable(row({ review_status: 'rejected', decided_at: ago(6.9) }), NOW), false);
    assert.equal(copySweepable(row({ review_status: 'rejected', decided_at: null }), NOW), false);
});

test('sweep: a copy of an upload taken back goes a week after it was TAKEN BACK, not after it was sent', () => {
    // Sent ten days ago, withdrawn today: it stays.
    assert.equal(copySweepable(row({ upload_status: 'withdrawn', sent_at: ago(10), withdrawn_at: ago(0.1) }), NOW), false);
    assert.equal(copySweepable(row({ upload_status: 'withdrawn', sent_at: ago(30), withdrawn_at: ago(8) }), NOW), true);
    assert.equal(copySweepable(row({ upload_status: 'withdrawn', withdrawn_at: null }), NOW), false);
    // A date as MySQL hands it to a script.
    assert.equal(copySweepable(row({ upload_status: 'withdrawn', withdrawn_at: new Date(NOW - 8 * DAY) }), NOW), true);
});

test('sweep: no copy, no row, an upload still being sent: nothing to remove', () => {
    assert.equal(copySweepable(row({ filed_key: null, review_status: 'rejected', decided_at: ago(30) }), NOW), false);
    assert.equal(copySweepable(null, NOW), false);
    assert.equal(copySweepable(row({ upload_status: 'draft' }), NOW), false);
    assert.equal(copySweepable(row({ upload_status: 'finalising' }), NOW), false);
});

const container = (over = {}) => ({ stage: 'BOOKED', deleted_at: null, merged_into_id: null, reference: '301', forwarder_id: null, forwarder: null, arrived: false, ...over });
const give = (over = {}) => forwarderToGive({ container: container(), liveBooking: false, paidForwarderIds: [3, 3], ...over });

test('backfill: a container whose forwarder payments all name one forwarder can be given that forwarder', () => {
    assert.deepEqual(give(), { forwarderId: 3, why: null });
    assert.deepEqual(give({ paidForwarderIds: [3] }), { forwarderId: 3, why: null });
    assert.deepEqual(give({ container: container({ stage: 'IN_TRANSIT' }) }), { forwarderId: 3, why: null });
});

test('backfill: never over a forwarder it has, a booking that holds it, or once it has arrived', () => {
    assert.deepEqual(give({ container: container({ forwarder_id: 4 }) }), { forwarderId: null, why: 'has_forwarder' });
    assert.deepEqual(give({ container: container({ forwarder: 'Typed Co' }) }), { forwarderId: null, why: 'has_typed_forwarder' });
    assert.deepEqual(give({ liveBooking: true }), { forwarderId: null, why: 'has_booking' });
    assert.deepEqual(give({ container: container({ arrived: true }) }), { forwarderId: null, why: 'arrived' });
});

test('backfill: only a real container, with payments that are all linked and agree', () => {
    for (const c of [container({ stage: 'DRAFT' }), container({ stage: 'CANCELLED' }), container({ deleted_at: new Date() }), container({ merged_into_id: 9 }), container({ reference: null })]) {
        assert.deepEqual(give({ container: c }), { forwarderId: null, why: 'not_real' });
    }
    assert.deepEqual(give({ paidForwarderIds: [] }), { forwarderId: null, why: 'no_payment' });
    assert.deepEqual(give({ paidForwarderIds: [3, null] }), { forwarderId: null, why: 'unlinked_payment' });
    assert.deepEqual(give({ paidForwarderIds: [3, 4] }), { forwarderId: null, why: 'several_forwarders' });
    assert.deepEqual(give({ paidForwarderIds: ['3', 3] }), { forwarderId: 3, why: null });
});
