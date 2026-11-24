'use strict';

// The rules of a forwarder's container page (src/lib/forwarder-containers.js;
// docs/forwarder-containers.md). Pure: no database, no network.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const FC = require('../src/lib/forwarder-containers');

const ship = (over = {}) => ({ id: 61, reference: '339', stage: 'BOOKED', deleted_at: null, merged_into_id: null, forwarder_id: null, forwarder: null, ...over });
const live = ids => id => ids.includes(Number(id));

// ── whose container it is ────────────────────────────────────────────────

test('ownerOf: a confirmed booking makes its forwarder the owner, over the container\'s own pair', () => {
    assert.deepEqual(
        FC.ownerOf({ shipment: ship({ forwarder_id: 3, forwarder: 'DCG' }), booking: { status: 'confirmed', forwarder_id: 4 }, forwarderLive: live([3, 4]) }),
        { forwarderId: 4, source: 'booking' }
    );
});

test('ownerOf: a booking still waiting for the forwarder shows the container to nobody, a forwarder picked by hand included', () => {
    assert.deepEqual(
        FC.ownerOf({ shipment: ship({ forwarder_id: 3, forwarder: 'DCG' }), booking: { status: 'requested', forwarder_id: 4 }, forwarderLive: live([3, 4]) }),
        { forwarderId: null, why: 'booking_not_confirmed' }
    );
});

test('ownerOf: with no live booking, the forwarder picked on the container', () => {
    assert.deepEqual(
        FC.ownerOf({ shipment: ship({ forwarder_id: 3, forwarder: 'DCG' }), booking: null, forwarderLive: live([3]) }),
        { forwarderId: 3, source: 'container' }
    );
});

test('ownerOf: a forwarder typed as text, not picked from the list, is nobody on the portal', () => {
    assert.deepEqual(
        FC.ownerOf({ shipment: ship({ forwarder: 'Some Freight Ltd' }), booking: null, forwarderLive: live([3]) }),
        { forwarderId: null, why: 'forwarder_not_listed' }
    );
});

test('ownerOf: no forwarder at all', () => {
    assert.deepEqual(FC.ownerOf({ shipment: ship(), booking: null, forwarderLive: live([3]) }), { forwarderId: null, why: 'no_forwarder' });
});

test('ownerOf: a forwarder taken off the list owns nothing, by booking or by hand', () => {
    assert.deepEqual(
        FC.ownerOf({ shipment: ship({ forwarder_id: 3 }), booking: null, forwarderLive: live([]) }),
        { forwarderId: null, why: 'forwarder_removed' }
    );
    assert.deepEqual(
        FC.ownerOf({ shipment: ship(), booking: { status: 'confirmed', forwarder_id: 4 }, forwarderLive: live([3]) }),
        { forwarderId: null, why: 'forwarder_removed' }
    );
});

test('ownerOf: only a real container: every stage from BOOKED on, with a number', () => {
    for (const stage of ['BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED']) {
        assert.deepEqual(FC.ownerOf({ shipment: ship({ stage, forwarder_id: 3 }), booking: null, forwarderLive: live([3]) }), { forwarderId: 3, source: 'container' }, stage);
    }
    for (const stage of ['DRAFT', 'PLANNED', 'CANCELLED']) {
        assert.deepEqual(FC.ownerOf({ shipment: ship({ stage, forwarder_id: 3 }), booking: null, forwarderLive: live([3]) }), { forwarderId: null, why: 'not_real' }, stage);
    }
    assert.deepEqual(FC.ownerOf({ shipment: ship({ reference: null, forwarder_id: 3 }), booking: null, forwarderLive: live([3]) }), { forwarderId: null, why: 'not_real' });
    assert.deepEqual(FC.ownerOf({ shipment: ship({ reference: '  ', forwarder_id: 3 }), booking: null, forwarderLive: live([3]) }), { forwarderId: null, why: 'not_real' });
});

test('ownerOf: a deleted, merged or missing container is gone, whatever else it says', () => {
    assert.deepEqual(FC.ownerOf({ shipment: null, booking: null, forwarderLive: live([3]) }), { forwarderId: null, why: 'gone' });
    assert.deepEqual(FC.ownerOf({ shipment: ship({ deleted_at: '2026-11-01 10:00:00', forwarder_id: 3 }), booking: null, forwarderLive: live([3]) }), { forwarderId: null, why: 'gone' });
    assert.deepEqual(FC.ownerOf({ shipment: ship({ merged_into_id: 70, forwarder_id: 3 }), booking: { status: 'confirmed', forwarder_id: 3 }, forwarderLive: live([3]) }), { forwarderId: null, why: 'gone' });
});

test('ownerOf: a declined or cancelled booking is no booking: the container\'s own pair decides', () => {
    for (const status of ['declined', 'cancelled']) {
        assert.deepEqual(
            FC.ownerOf({ shipment: ship({ forwarder_id: 3 }), booking: { status, forwarder_id: 4 }, forwarderLive: live([3, 4]) }),
            { forwarderId: 3, source: 'container' }, status
        );
    }
});

// ── who holds each duty (2026-11-24) ─────────────────────────────────────
//
// Three duties, one holder each: freight (origin to the UK port), delivery (UK
// port to the warehouse), customs. Per duty: the live booking that covers it,
// else the pick on the container.

const nobody = why => ({ forwarderId: null, why });
const bk = (over = {}) => ({ id: 12, status: 'confirmed', scope: 'port_to_port', forwarder_id: 4, ...over });
const dutiesOf = (shipment, bookings = [], alive = [3, 4, 5, 6]) => FC.dutiesOf({ shipment, bookings, forwarderLive: live(alive) });

test('dutiesOf: nothing picked, nothing booked: nobody holds anything', () => {
    assert.deepEqual(dutiesOf(ship()), { freight: nobody('no_forwarder'), delivery: nobody('no_forwarder'), customs: nobody('no_forwarder') });
});

test('dutiesOf: the three picks on the container, each its own duty', () => {
    assert.deepEqual(dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 5, customs_forwarder_id: 6 })), {
        freight: { forwarderId: 3, source: 'container' },
        delivery: { forwarderId: 5, source: 'container' },
        customs: { forwarderId: 6, source: 'container' },
    });
});

test('dutiesOf: end to end by hand is the same forwarder in both picks', () => {
    const d = dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 3 }));
    assert.deepEqual(d.freight, { forwarderId: 3, source: 'container' });
    assert.deepEqual(d.delivery, { forwarderId: 3, source: 'container' });
    assert.deepEqual(d.customs, nobody('no_forwarder'));
});

test('dutiesOf: a port-to-port booking holds freight only; delivery stays with the pick', () => {
    assert.deepEqual(dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 5 }), [bk()]), {
        freight: { forwarderId: 4, source: 'booking', bookingId: 12 },
        delivery: { forwarderId: 5, source: 'container' },
        customs: nobody('no_forwarder'),
    });
});

test('dutiesOf: an end-to-end booking holds freight and delivery, and outranks both picks', () => {
    assert.deepEqual(dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 5, customs_forwarder_id: 5 }), [bk({ scope: 'end_to_end' })]), {
        freight: { forwarderId: 4, source: 'booking', bookingId: 12 },
        delivery: { forwarderId: 4, source: 'booking', bookingId: 12 },
        customs: { forwarderId: 5, source: 'container' },
    });
});

test('dutiesOf: a booking still waiting leaves the duties it covers with nobody, and no other', () => {
    assert.deepEqual(dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 5, customs_forwarder_id: 6 }), [bk({ status: 'requested' })]), {
        freight: nobody('booking_not_confirmed'),
        delivery: { forwarderId: 5, source: 'container' },
        customs: { forwarderId: 6, source: 'container' },
    });
    const d = dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 5, customs_forwarder_id: 6 }), [bk({ status: 'requested', scope: 'end_to_end' })]);
    assert.deepEqual(d.freight, nobody('booking_not_confirmed'));
    assert.deepEqual(d.delivery, nobody('booking_not_confirmed'));
    assert.deepEqual(d.customs, { forwarderId: 6, source: 'container' });
});

test('dutiesOf: a booking with no scope on its row is port to port (rows read before the column existed)', () => {
    assert.deepEqual(dutiesOf(ship({ delivery_forwarder_id: 5 }), [{ id: 12, status: 'confirmed', forwarder_id: 4 }]).delivery, { forwarderId: 5, source: 'container' });
});

test('dutiesOf: a declined or cancelled booking is no booking', () => {
    for (const status of ['declined', 'cancelled']) {
        assert.deepEqual(dutiesOf(ship({ forwarder_id: 3 }), [bk({ status, scope: 'end_to_end' })]).freight, { forwarderId: 3, source: 'container' }, status);
        assert.deepEqual(dutiesOf(ship({ forwarder_id: 3 }), [bk({ status, scope: 'end_to_end' })]).delivery, nobody('no_forwarder'), status);
    }
});

test('dutiesOf: a holder taken off the list holds nothing, whichever duty and however it came', () => {
    const d = dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 5, customs_forwarder_id: 6 }), [], [3]);
    assert.deepEqual(d.freight, { forwarderId: 3, source: 'container' });
    assert.deepEqual(d.delivery, nobody('forwarder_removed'));
    assert.deepEqual(d.customs, nobody('forwarder_removed'));
    assert.deepEqual(dutiesOf(ship({ forwarder_id: 3 }), [bk({ scope: 'end_to_end' })], [3]).delivery, nobody('forwarder_removed'), 'the booking still outranks the pick');
});

test('dutiesOf: typed text is a forwarder for freight only, and it is nobody on the portal', () => {
    const d = dutiesOf(ship({ forwarder: 'Some Freight Ltd' }));
    assert.deepEqual(d.freight, nobody('forwarder_not_listed'));
    assert.deepEqual(d.delivery, nobody('no_forwarder'));
});

test('dutiesOf: only a real container has holders at all', () => {
    const picks = { forwarder_id: 3, delivery_forwarder_id: 5, customs_forwarder_id: 6 };
    for (const duty of FC.DUTIES) {
        assert.deepEqual(dutiesOf(ship({ ...picks, stage: 'DRAFT' }))[duty], nobody('not_real'), duty);
        assert.deepEqual(dutiesOf(ship({ ...picks, deleted_at: '2026-11-01 10:00:00' }))[duty], nobody('gone'), duty);
        assert.deepEqual(dutiesOf(null)[duty], nobody('gone'), duty);
    }
});

test('dutiesOf: the freight duty is ownerOf, on every case ownerOf is tested on', () => {
    const cases = [
        [ship({ forwarder_id: 3, forwarder: 'DCG' }), { status: 'confirmed', forwarder_id: 4 }],
        [ship({ forwarder_id: 3, forwarder: 'DCG' }), { status: 'requested', forwarder_id: 4 }],
        [ship({ forwarder_id: 3 }), null],
        [ship({ forwarder: 'Typed' }), null],
        [ship(), null],
        [ship({ stage: 'DRAFT', forwarder_id: 3 }), null],
        [ship({ forwarder_id: 3 }), { status: 'cancelled', forwarder_id: 4 }],
    ];
    for (const [shipment, booking] of cases) {
        const owner = FC.ownerOf({ shipment, booking, forwarderLive: live([3, 4]) });
        const freight = FC.dutiesOf({ shipment, bookings: booking ? [{ id: 1, ...booking }] : [], forwarderLive: live([3, 4]) }).freight;
        assert.equal(freight.forwarderId, owner.forwarderId);
        assert.equal(freight.why, owner.why);
        assert.equal(freight.source, owner.source);
    }
});

test('attachedOf: one entry per forwarder that holds something, the freight holder first', () => {
    const d = dutiesOf(ship({ delivery_forwarder_id: 5, customs_forwarder_id: 5 }), [bk()]);
    assert.deepEqual(FC.attachedOf(d), [
        { forwarderId: 4, duties: ['freight'], sources: { freight: 'booking' } },
        { forwarderId: 5, duties: ['delivery', 'customs'], sources: { delivery: 'container', customs: 'container' } },
    ]);
    assert.deepEqual(FC.attachedOf(dutiesOf(ship())), []);
    // End to end and customs, one company: one entry.
    assert.deepEqual(FC.attachedOf(dutiesOf(ship({ customs_forwarder_id: 4 }), [bk({ scope: 'end_to_end' })])), [
        { forwarderId: 4, duties: ['freight', 'delivery', 'customs'], sources: { freight: 'booking', delivery: 'booking', customs: 'container' } },
    ]);
});

test('heldBy: the duties one forwarder holds', () => {
    const d = dutiesOf(ship({ delivery_forwarder_id: 5, customs_forwarder_id: 5 }), [bk()]);
    assert.deepEqual(FC.heldBy(d, 4), ['freight']);
    assert.deepEqual(FC.heldBy(d, '5'), ['delivery', 'customs']);
    assert.deepEqual(FC.heldBy(d, 9), []);
    assert.deepEqual(FC.heldBy(d, null), []);
});

test('linkSourceOf: booking or container only for the freight holder; another holder is "duty" (what an older portal cannot write for)', () => {
    const d = dutiesOf(ship({ delivery_forwarder_id: 5, customs_forwarder_id: 6 }), [bk()]);
    assert.equal(FC.linkSourceOf(d, 4), 'booking');
    assert.equal(FC.linkSourceOf(d, 5), 'duty');
    assert.equal(FC.linkSourceOf(d, 6), 'duty');
    assert.equal(FC.linkSourceOf(d, 9), null);
    assert.equal(FC.linkSourceOf(dutiesOf(ship({ forwarder_id: 3, delivery_forwarder_id: 3 })), 3), 'container');
});

test('accessFor: a holder of any duty sees it in full; one only paid on it sees the payment; anyone else nothing', () => {
    const d = dutiesOf(ship({ delivery_forwarder_id: 5 }), [bk()]);
    assert.deepEqual(FC.accessFor({ duties: d, real: true, forwarderId: 4, paidForwarderIds: [3] }), { mode: 'full', source: 'booking', duties: ['freight'] });
    assert.deepEqual(FC.accessFor({ duties: d, real: true, forwarderId: 5, paidForwarderIds: [5] }), { mode: 'full', source: 'duty', duties: ['delivery'] });
    assert.deepEqual(FC.accessFor({ duties: d, real: true, forwarderId: 3, paidForwarderIds: [3] }), { mode: 'payment', source: 'payment', duties: [] });
    assert.deepEqual(FC.accessFor({ duties: d, real: true, forwarderId: 6, paidForwarderIds: [3] }), { mode: 'none', source: null, duties: [] });
    assert.deepEqual(FC.accessFor({ duties: dutiesOf(ship({ stage: 'DRAFT' })), real: false, forwarderId: 3, paidForwarderIds: [3] }), { mode: 'none', source: null, duties: [] });
});

test('scopeGiven: what a booking is said to cover; saying nothing never grants the delivery', () => {
    assert.deepEqual(FC.scopeGiven(undefined), { scope: 'port_to_port' });
    assert.deepEqual(FC.scopeGiven('port_to_port'), { scope: 'port_to_port' });
    assert.deepEqual(FC.scopeGiven('end_to_end'), { scope: 'end_to_end' });
    for (const bad of [null, '', 'delivery', 'END_TO_END', 5, {}]) {
        assert.deepEqual(FC.scopeGiven(bad), { error: 'scope must be one of: port_to_port, end_to_end.' }, String(bad));
    }
});

test('dutiesOfFlags: the duties a row of the portal\'s view says its forwarder holds', () => {
    assert.deepEqual(FC.dutiesOfFlags({ does_freight: 1, does_delivery: 0, does_customs: '1' }), ['freight', 'customs']);
    assert.deepEqual(FC.dutiesOfFlags({ does_freight: 0, does_delivery: 0, does_customs: 0 }), []);
    assert.deepEqual(FC.dutiesOfFlags({}), [], 'no row joined: nothing held');
    assert.deepEqual(FC.dutiesOfFlags(null), []);
    // The same flags, shaped as dutiesOf's answer for that one forwarder.
    const d = FC.dutiesFromFlags({ forwarder_id: 5, does_freight: 0, does_delivery: 1, does_customs: 1 });
    assert.deepEqual(FC.heldBy(d, 5), ['delivery', 'customs']);
    assert.deepEqual(FC.heldBy(d, 4), []);
    assert.deepEqual(FC.heldBy(FC.dutiesFromFlags({ forwarder_id: 5 }), 5), []);
});

test('what each duty is asked for: the fields and the kinds of file', () => {
    assert.deepEqual(FC.DUTIES, ['freight', 'delivery', 'customs']);
    assert.deepEqual(FC.BOOKING_SCOPES, ['port_to_port', 'end_to_end']);
    assert.deepEqual(FC.SCOPE_DUTIES, { port_to_port: ['freight'], end_to_end: ['freight', 'delivery'] });
    assert.equal(FC.FIELD_DUTY.ecn, 'freight');
    assert.equal(FC.FIELD_DUTY.loadingDate, 'freight');
    assert.equal(FC.FIELD_DUTY.etd, 'freight');
    assert.equal(FC.KIND_DUTY.loading_photo, 'freight');
    assert.equal(FC.KIND_DUTY.bill_of_lading, 'freight');
    assert.equal(FC.KIND_DUTY.invoice, null, 'an invoice: any duty');
    // Every field and every kind the rules know has a duty.
    for (const f of FC.DETAIL_FIELDS) assert.ok(FC.DUTIES.includes(FC.FIELD_DUTY[f]), f);
    for (const k of FC.UPLOAD_KINDS) assert.ok(FC.KIND_DUTY[k] === null || FC.DUTIES.includes(FC.KIND_DUTY[k]), k);
});

test('fieldsOutside: the fields of a proposal its forwarder does not hold the duty for', () => {
    assert.deepEqual(FC.fieldsOutside({ ecn: 'MSKU1234565', etd: '2026-12-01' }, ['freight']), []);
    assert.deepEqual(FC.fieldsOutside({ ecn: 'MSKU1234565', etd: '2026-12-01' }, ['delivery', 'customs']), ['ecn', 'etd']);
    assert.deepEqual(FC.fieldsOutside({}, []), []);
});

test('kindAllowed: a file kind against the duties its forwarder holds', () => {
    assert.equal(FC.kindAllowed('invoice', ['customs']), true);
    assert.equal(FC.kindAllowed('invoice', []), false);
    assert.equal(FC.kindAllowed('loading_photo', ['freight']), true);
    assert.equal(FC.kindAllowed('loading_photo', ['delivery', 'customs']), false);
    assert.equal(FC.kindAllowed('bill_of_lading', ['delivery']), false);
    assert.equal(FC.kindAllowed('telex', ['freight', 'delivery', 'customs']), false);
});

// ── what a forwarder is shown of a container ─────────────────────────────

test('visibility: the owner sees it in full; a forwarder only paid on it sees the payment; anyone else nothing', () => {
    const owner = { forwarderId: 4, source: 'booking' };
    assert.deepEqual(FC.visibilityFor({ owner, real: true, forwarderId: 4, paidForwarderIds: [3] }), { mode: 'full', source: 'booking' });
    assert.deepEqual(FC.visibilityFor({ owner, real: true, forwarderId: 3, paidForwarderIds: [3] }), { mode: 'payment', source: 'payment' });
    assert.deepEqual(FC.visibilityFor({ owner, real: true, forwarderId: 5, paidForwarderIds: [3] }), { mode: 'none', source: null });
});

test('visibility: the owner who is also paid on it has one row, the full one', () => {
    assert.deepEqual(
        FC.visibilityFor({ owner: { forwarderId: 3, source: 'container' }, real: true, forwarderId: 3, paidForwarderIds: [3] }),
        { mode: 'full', source: 'container' }
    );
});

test('visibility: a payment shows a container only while the container is a real, live one', () => {
    const nobody = { forwarderId: null, why: 'no_forwarder' };
    assert.deepEqual(FC.visibilityFor({ owner: nobody, real: true, forwarderId: 3, paidForwarderIds: [3] }), { mode: 'payment', source: 'payment' });
    assert.deepEqual(FC.visibilityFor({ owner: { forwarderId: null, why: 'not_real' }, real: false, forwarderId: 3, paidForwarderIds: [3] }), { mode: 'none', source: null });
    assert.deepEqual(FC.visibilityFor({ owner: { forwarderId: null, why: 'gone' }, real: false, forwarderId: 3, paidForwarderIds: [3] }), { mode: 'none', source: null });
});

test('isReal: the container half of the rule, as the payment-only rows need it', () => {
    assert.equal(FC.isReal(ship()), true);
    assert.equal(FC.isReal(ship({ stage: 'DRAFT' })), false);
    assert.equal(FC.isReal(ship({ deleted_at: '2026-11-01 10:00:00' })), false);
    assert.equal(FC.isReal(ship({ merged_into_id: 9 })), false);
    assert.equal(FC.isReal(ship({ reference: '' })), false);
    assert.equal(FC.isReal(null), false);
});

// ── finished ─────────────────────────────────────────────────────────────

test('finished: more than 14 days after it arrived, by the date alone', () => {
    assert.equal(FC.FINISHED_AFTER_DAYS, 14);
    assert.equal(FC.isFinished('2026-11-01', '2026-11-15'), false, 'the 14th day');
    assert.equal(FC.isFinished('2026-11-01', '2026-11-16'), true, 'the 15th day');
    assert.equal(FC.isFinished('2026-11-01', '2026-11-01'), false);
    assert.equal(FC.isFinished(null, '2026-11-16'), false, 'not arrived: never finished');
    assert.equal(FC.isFinished('2026-12-20', '2027-01-04'), true, 'across a year end');
});

test('finished: the fortnight runs from the later of the arrival and the delivery day', () => {
    assert.equal(FC.isFinished('2026-11-01', '2026-11-16', { deliveryOn: '2026-11-10' }), false, 'delivered on the 10th: open until the 24th');
    assert.equal(FC.isFinished('2026-11-01', '2026-11-24', { deliveryOn: '2026-11-10' }), false, 'the 14th day after delivery');
    assert.equal(FC.isFinished('2026-11-01', '2026-11-25', { deliveryOn: '2026-11-10' }), true);
    assert.equal(FC.isFinished('2026-11-01', '2026-11-16', { deliveryOn: '2026-10-20' }), true, 'a delivery day before the arrival changes nothing');
    assert.equal(FC.isFinished(null, '2026-12-25', { deliveryOn: '2026-11-10' }), false, 'not arrived: never finished');
});

test('finished: with a delivery holder and no delivery date held, it stays open until the container is closed', () => {
    assert.equal(FC.isFinished('2026-11-01', '2026-12-25', { awaitingDelivery: true, closed: false }), false);
    assert.equal(FC.isFinished('2026-11-01', '2026-12-25', { awaitingDelivery: true, closed: true }), true);
    assert.equal(FC.isFinished('2026-11-01', '2026-11-10', { awaitingDelivery: true, closed: true }), false, 'closed, but inside the fortnight');
    assert.equal(FC.isFinished('2026-11-01', '2026-12-25', { awaitingDelivery: false, closed: false }), true, 'no delivery holder: today\'s rule');
});

// ── a payment on a container ─────────────────────────────────────────────

test('paymentsOnContainer: this container\'s charges, the other containers the transfer also pays, per payment', () => {
    const lines = [
        { payment_id: 16, shipment_id: 61, shipment_reference: '301', amount: '4500.00', extra_kind: 'freight' },
        { payment_id: 16, shipment_id: 61, shipment_reference: '301', amount: '375.00', extra_kind: 'customs' },
        { payment_id: 16, shipment_id: 62, shipment_reference: '308', amount: '4500.00', extra_kind: 'freight' },
        { payment_id: 37, shipment_id: 62, shipment_reference: '308', amount: '12522.70', extra_kind: 'freight' },
    ];
    assert.deepEqual(FC.paymentsOnContainer(lines, 61), [
        { paymentId: 16, share: 4875, lines: [{ kind: 'freight', amount: 4500 }, { kind: 'customs', amount: 375 }], alsoPays: ['308'] },
    ]);
    assert.deepEqual(FC.paymentsOnContainer(lines, 62), [
        { paymentId: 16, share: 4500, lines: [{ kind: 'freight', amount: 4500 }], alsoPays: ['301'] },
        { paymentId: 37, share: 12522.7, lines: [{ kind: 'freight', amount: 12522.7 }], alsoPays: [] },
    ]);
    assert.deepEqual(FC.paymentsOnContainer(lines, 99), []);
});

test('paymentsOnContainer: a charge with no container id belongs to no container, and is still named in "also pays"', () => {
    const lines = [
        { payment_id: 5, shipment_id: 61, shipment_reference: '301', amount: '100.00', extra_kind: 'freight' },
        { payment_id: 5, shipment_id: null, shipment_reference: '290', amount: '200.00', extra_kind: 'freight' },
        { payment_id: 5, shipment_id: null, shipment_reference: null, amount: '50.00', extra_kind: 'other' },
    ];
    assert.deepEqual(FC.paymentsOnContainer(lines, 61), [
        { paymentId: 5, share: 100, lines: [{ kind: 'freight', amount: 100 }], alsoPays: ['290'] },
    ]);
});

test('paymentsOnContainer: money adds up to the cent', () => {
    const lines = [
        { payment_id: 1, shipment_id: 61, shipment_reference: '301', amount: '0.10', extra_kind: 'freight' },
        { payment_id: 1, shipment_id: 61, shipment_reference: '301', amount: '0.20', extra_kind: 'customs' },
    ];
    assert.equal(FC.paymentsOnContainer(lines, 61)[0].share, 0.3);
});

// ── the email to the forwarder ───────────────────────────────────────────

test('noticeState: a key seen before is reported as it stands and never sent twice', () => {
    const where = { shipmentId: 61, forwarderId: 4 };
    assert.equal(FC.noticeState(null, where), 'new');
    assert.equal(FC.noticeState({ shipment_id: 61, forwarder_id: 4, status: 'sent' }, where), 'already_sent');
    assert.equal(FC.noticeState({ shipment_id: 61, forwarder_id: 4, status: 'sending' }, where), 'send_unconfirmed');
    assert.equal(FC.noticeState({ shipment_id: 61, forwarder_id: 4, status: 'failed' }, where), 'retry');
});

test('noticeState: a key made for one container and forwarder is refused for another', () => {
    assert.equal(FC.noticeState({ shipment_id: 62, forwarder_id: 4, status: 'failed' }, { shipmentId: 61, forwarderId: 4 }), 'clash');
    assert.equal(FC.noticeState({ shipment_id: 61, forwarder_id: 3, status: 'sent' }, { shipmentId: 61, forwarderId: 4 }), 'clash');
});

test('sendKey: a uuid as the dialog makes it, nothing else', () => {
    assert.equal(FC.isSendKey('3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f'), true);
    assert.equal(FC.isSendKey('3F2C9A40-1B7D-4C1E-9F51-0A2B3C4D5E6F'), true);
    assert.equal(FC.isSendKey(''), false);
    assert.equal(FC.isSendKey('abc'), false);
    assert.equal(FC.isSendKey(null), false);
    assert.equal(FC.isSendKey('3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f; DROP'), false);
});

test('containerAsks: the email asks only for what we do not hold yet, and always for the papers', () => {
    assert.deepEqual(FC.containerAsks({ ecn: null, loadingDate: null, etd: null }), ['ecn', 'loadingDate', 'etd', 'invoice', 'photos', 'billOfLading']);
    assert.deepEqual(FC.containerAsks({ ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-01' }), ['loadingDate', 'invoice', 'photos', 'billOfLading']);
    assert.deepEqual(FC.containerAsks({ ecn: 'MSKU1234565', loadingDate: '2026-11-28', etd: '2026-12-01' }), ['invoice', 'photos', 'billOfLading']);
});

test('containerAsks: each company is asked only for its own duties\' things', () => {
    const none = { ecn: null, loadingDate: null, etd: null };
    assert.deepEqual(FC.containerAsks(none, ['freight']), ['ecn', 'loadingDate', 'etd', 'invoice', 'photos', 'billOfLading']);
    assert.deepEqual(FC.containerAsks(none, ['delivery']), ['deliveryDate', 'invoice'], 'never the number, the loading date, photos or the bill of lading');
    assert.deepEqual(FC.containerAsks({ ...none, deliveryDate: '2026-12-20' }, ['delivery']), ['invoice'], 'a delivery date we hold is not asked for');
    assert.deepEqual(FC.containerAsks(none, ['customs']), ['invoice', 'customsDraft']);
    assert.deepEqual(FC.containerAsks(none, ['freight', 'delivery']), ['ecn', 'loadingDate', 'etd', 'deliveryDate', 'invoice', 'photos', 'billOfLading']);
    assert.deepEqual(FC.containerAsks(none, ['delivery', 'customs']), ['deliveryDate', 'invoice', 'customsDraft']);
    assert.deepEqual(FC.containerAsks(none, []), []);
});

test('containerUrl: the portal page of one container', () => {
    assert.equal(FC.containerUrl('https://portal.example', 1214), 'https://portal.example/freight-containers/1214');
    assert.equal(FC.containerUrl('https://portal.example/', 1214), 'https://portal.example/freight-containers/1214');
});

test('mayDecide: admin and standard users; an accountant and a read-only user only read', () => {
    assert.equal(FC.mayDecide('admin'), true);
    assert.equal(FC.mayDecide('standard'), true);
    assert.equal(FC.mayDecide('accountant'), false);
    assert.equal(FC.mayDecide('readonly'), false);
    assert.equal(FC.mayDecide(undefined), false);
});

// ── the email's fixed block and its template ─────────────────────────────

const F = require('../src/lib/freight-quotes');
const { DEFAULT_EMAIL_TEMPLATES, FREIGHT_CONTAINER_TEMPLATE, FREIGHT_CONTAINER_TEMPLATE_KEY, FREIGHT_TEMPLATES } = require('../src/lib/email-template-defaults');

test('containerBlockHtml: the container, only what is asked, the button to its page, and how to register', () => {
    const html = F.containerBlockHtml({
        containerName: '339', bookingRef: 'BK<1>', asks: ['ecn', 'invoice'],
        url: 'https://portal.example/freight-containers/1214', registerUrl: 'https://portal.example/register',
    });
    assert.ok(html.includes('>339<'), 'the container');
    assert.ok(html.includes('BK&lt;1&gt;') && !html.includes('BK<1>'), 'the booking reference, escaped');
    assert.ok(html.includes('<li>Container number / 柜号</li>'));
    assert.ok(html.includes('<li>Your invoice / 贵司发票</li>'));
    assert.equal((html.match(/<li>/g) || []).length, 2, 'nothing that was not asked');
    assert.ok(html.includes('href="https://portal.example/freight-containers/1214"'));
    assert.ok(html.includes('Open the container'));
    assert.ok(html.includes('href="https://portal.example/register"'));
    assert.ok(!/No sign-in is needed/.test(html), 'this page needs a sign-in');
});

test('containerBlockHtml: no booking reference, nothing asked, no register address: none of those parts', () => {
    const html = F.containerBlockHtml({ containerName: '339', bookingRef: null, asks: [], url: 'https://portal.example/freight-containers/1214' });
    assert.ok(!html.includes('Booking ref'));
    assert.ok(!html.includes('<ul'));
    assert.ok(!html.includes('Register at'));
    assert.ok(html.includes('Open the container'));
});

test('containerBlockHtml: every ask the rules can make has a label', () => {
    for (const ask of FC.containerAsks({})) assert.equal(typeof F.CONTAINER_ASK_LABELS[ask], 'string', ask);
    // ... whichever duties the company holds.
    for (const ask of FC.containerAsks({}, FC.DUTIES)) assert.equal(typeof F.CONTAINER_ASK_LABELS[ask], 'string', ask);
    assert.deepEqual(FC.containerAsks({}, FC.DUTIES), ['ecn', 'loadingDate', 'etd', 'deliveryDate', 'invoice', 'photos', 'billOfLading', 'customsDraft']);
});

test('the container email template: a Freight template of its own, in the defaults, outside the four the old seeds loop over', () => {
    assert.equal(FREIGHT_CONTAINER_TEMPLATE_KEY, 'freight_container_details');
    assert.equal(FREIGHT_CONTAINER_TEMPLATE.category, 'Freight');
    assert.equal(DEFAULT_EMAIL_TEMPLATES.filter(t => t.key === 'freight_container_details').length, 1);
    assert.equal(FREIGHT_TEMPLATES.some(t => t.key === 'freight_container_details'), false);
    assert.ok(FREIGHT_CONTAINER_TEMPLATE.subject.includes('{containerName}'));
    assert.ok(FREIGHT_CONTAINER_TEMPLATE.bodyHtml.includes('{containerName}'));
});

// ── slice B: the container's number and dates, proposed by its forwarder ──

test('normEcn: as a carrier number is compared: trimmed, upper case, no spaces or hyphens inside', () => {
    assert.equal(FC.normEcn(' msku 123456-5 '), 'MSKU1234565');
    assert.equal(FC.normEcn('MSKU1234565'), 'MSKU1234565');
    assert.equal(FC.normEcn(''), null);
    assert.equal(FC.normEcn(null), null);
    assert.equal(FC.normEcn('   '), null);
});

test('ecnProblem: a sea container number is four letters and seven digits with the right check digit', () => {
    assert.equal(FC.ecnProblem('SEA', 'MSKU1234565'), null);
    assert.equal(FC.ecnProblem('SEA', 'CSQU3054383'), null, 'the ISO 6346 worked example');
    assert.equal(FC.ecnProblem('SEA', 'msku 1234565'), null, 'compared as normalised');
    assert.equal(FC.ecnProblem('SEA', 'MSKU1234566'), 'check_digit');
    assert.equal(FC.ecnProblem('SEA', 'MSKU1235465'), 'check_digit', 'two digits swapped');
    assert.equal(FC.ecnProblem('SEA', 'MSK1234565'), 'shape');
    assert.equal(FC.ecnProblem('SEA', 'MSKU123456'), 'shape');
    assert.equal(FC.ecnProblem('SEA', ''), 'empty');
    assert.equal(FC.ecnProblem(null, 'MSKU1234566'), 'check_digit', 'an unknown mode is read as sea');
});

test('ecnProblem: by air it is an air waybill number, taken as text of a sensible length', () => {
    assert.equal(FC.ecnProblem('AIR', '112-90654874'), null);
    assert.equal(FC.ecnProblem('AIR', '877951641921'), null);
    assert.equal(FC.ecnProblem('AIR', '1234'), 'shape');
    assert.equal(FC.ecnProblem('AIR', 'x'.repeat(41)), 'shape');
    assert.equal(FC.ecnProblem('AIR', ''), 'empty');
});

// ── the delivery date (2026-11-24): a wall-clock value, as text, to the minute ──

test('normDelivery: one form, to the minute; no time and midnight are the same thing; seconds never count', () => {
    // The vectors the portal's and ShipLine's normalisers are tested with too.
    assert.equal(FC.normDelivery('2026-12-01'), '2026-12-01');
    assert.equal(FC.normDelivery('2026-12-01T00:00'), '2026-12-01', 'midnight is "no time"');
    assert.equal(FC.normDelivery('2026-12-01 00:00:30'), '2026-12-01', 'seconds do not make a time');
    assert.equal(FC.normDelivery('2026-12-01T09:30'), '2026-12-01T09:30');
    assert.equal(FC.normDelivery('2026-12-01 09:30:00'), '2026-12-01T09:30', 'as MySQL writes it');
    assert.equal(FC.normDelivery('2026-12-01T09:30:59.123'), '2026-12-01T09:30');
    assert.equal(FC.normDelivery(' 2026-12-01T23:59 '), '2026-12-01T23:59');
    for (const bad of ['', null, undefined, 'soon', '01/12/2026', '2026-02-30', '2026-12-01T24:00', '2026-12-01T09:60', '2026-12-01T9:30', '2026-12-01T09', 20261201]) {
        assert.equal(FC.normDelivery(bad), null, String(bad));
    }
});

test('deliveryDay and deliverySql: the day it falls on, and how it is written to an order', () => {
    assert.equal(FC.deliveryDay('2026-12-01T09:30'), '2026-12-01');
    assert.equal(FC.deliveryDay('2026-12-01'), '2026-12-01');
    assert.equal(FC.deliveryDay('never'), null);
    assert.equal(FC.deliverySql('2026-12-01T09:30'), '2026-12-01 09:30:00');
    assert.equal(FC.deliverySql('2026-12-01'), '2026-12-01 00:00:00', 'the shape a hand-typed value has');
    assert.equal(FC.deliverySql('never'), null);
});

test('heldDelivery: what a container holds is what EVERY order on it holds; else it is mixed', () => {
    assert.deepEqual(FC.heldDelivery(['2026-12-01T09:30', '2026-12-01T09:30']), { deliveryDate: '2026-12-01T09:30', deliveryMixed: null });
    assert.deepEqual(FC.heldDelivery([null, null]), { deliveryDate: null, deliveryMixed: null });
    assert.deepEqual(FC.heldDelivery([]), { deliveryDate: null, deliveryMixed: null }, 'no order: nothing held');
    // One order without, or two different days: nothing is "the" date, and the tab is told what is there.
    assert.deepEqual(FC.heldDelivery(['2026-12-01', null]), { deliveryDate: null, deliveryMixed: [null, '2026-12-01'] });
    assert.deepEqual(FC.heldDelivery(['2026-12-03', '2026-12-01', '2026-12-03']), { deliveryDate: null, deliveryMixed: ['2026-12-01', '2026-12-03'] });
    // Values are compared in the one form.
    assert.deepEqual(FC.heldDelivery(['2026-12-01T00:00', '2026-12-01 00:00:00']), { deliveryDate: '2026-12-01', deliveryMixed: null });
});

test('the delivery date is the delivery duty\'s, and the fourth thing a proposal can ask', () => {
    assert.deepEqual(FC.DETAIL_FIELDS, ['ecn', 'loadingDate', 'etd', 'deliveryDate']);
    assert.deepEqual(FC.ORIGINAL_FIELDS, ['ecn', 'loadingDate', 'etd']);
    assert.equal(FC.FIELD_DUTY.deliveryDate, 'delivery');
    assert.deepEqual(FC.proposalOf('{"deliveryDate":"2026-12-20T00:00","etd":"2026-12-15"}'), { etd: '2026-12-15', deliveryDate: '2026-12-20' });
    assert.deepEqual(FC.proposalOf({ deliveryDate: '2026-12-20 14:30:00' }), { deliveryDate: '2026-12-20T14:30' });
    assert.deepEqual(FC.proposalOf({ deliveryDate: 'next week' }), {});
    assert.deepEqual(FC.fieldsOutside({ etd: '2026-12-15', deliveryDate: '2026-12-20' }, ['freight']), ['deliveryDate']);
    assert.deepEqual(FC.fieldsOutside({ etd: '2026-12-15', deliveryDate: '2026-12-20' }, ['delivery']), ['etd']);
    assert.deepEqual(FC.fieldsOutside({ etd: '2026-12-15', deliveryDate: '2026-12-20' }, ['freight', 'delivery']), []);
});

test('clientKnows: a decision must name every field beyond the original three that it decides', () => {
    assert.equal(FC.clientKnows({ ecn: 'MSKU1234565', etd: '2026-12-15' }, []), true, 'a proposal of the original three is decided as before');
    assert.equal(FC.clientKnows({ ecn: 'MSKU1234565', etd: '2026-12-15' }, undefined), true);
    assert.equal(FC.clientKnows({ deliveryDate: '2026-12-20' }, ['ecn', 'loadingDate', 'etd']), false, 'a build from before the delivery date');
    assert.equal(FC.clientKnows({ deliveryDate: '2026-12-20' }, ['ecn', 'loadingDate', 'etd', 'deliveryDate']), true);
    assert.equal(FC.clientKnows({ etd: '2026-12-15', deliveryDate: '2026-12-20' }, ['deliveryDate']), true);
    assert.equal(FC.clientKnows({ deliveryDate: '2026-12-20' }, null), false);
});

test('heldDiff: the delivery date is compared when the card says it showed one, in the one form, mixed values included', () => {
    const held = { ecn: null, loadingDate: null, etd: '2026-12-15', deliveryDate: '2026-12-20', deliveryMixed: null };
    assert.deepEqual(FC.heldDiff({ ecn: null, loadingDate: null, etd: '2026-12-15', deliveryDate: '2026-12-20T00:00' }, held), []);
    assert.deepEqual(FC.heldDiff({ ecn: null, loadingDate: null, etd: '2026-12-15', deliveryDate: null }, held), ['deliveryDate']);
    // A build from before the delivery date never names it: for that card it is not compared.
    assert.deepEqual(FC.heldDiff({ ecn: null, loadingDate: null, etd: '2026-12-15' }, held), []);
    // Mixed values: the card must have shown the same ones.
    const mixed = { ...held, deliveryDate: null, deliveryMixed: [null, '2026-12-20'] };
    assert.deepEqual(FC.heldDiff({ etd: '2026-12-15', deliveryDate: null, deliveryMixed: [null, '2026-12-20'] }, mixed), []);
    assert.deepEqual(FC.heldDiff({ etd: '2026-12-15', deliveryDate: null, deliveryMixed: null }, mixed), ['deliveryDate']);
    assert.deepEqual(FC.heldDiff({ etd: '2026-12-15', deliveryDate: null, deliveryMixed: ['2026-12-20', '2026-12-21'] }, mixed), ['deliveryDate']);
});

test('orderProblem: loading, then departure, then every delivery day; a pair is judged only when the proposal touches it', () => {
    const held = { loadingDate: '2026-12-10', etd: '2026-12-15', deliveryDate: '2026-12-20', deliveryMixed: null };
    assert.equal(FC.orderProblem({ deliveryDate: '2026-12-15T08:00' }, held), null, 'delivered on the day it leaves is in order');
    assert.deepEqual(FC.orderProblem({ deliveryDate: '2026-12-14' }, held), ['etd', 'deliveryDate']);
    // The other direction: a freight proposal that moves the departure past a delivery date already held.
    assert.deepEqual(FC.orderProblem({ etd: '2026-12-21' }, held), ['etd', 'deliveryDate']);
    assert.equal(FC.orderProblem({ etd: '2026-12-20' }, held), null);
    assert.deepEqual(FC.orderProblem({ loadingDate: '2026-12-16' }, held), ['loadingDate', 'etd']);
    assert.deepEqual(FC.orderProblem({ loadingDate: '2026-12-22' }, { ...held, etd: null }), ['loadingDate', 'deliveryDate']);
    // Against every delivery day held, when the orders differ.
    assert.deepEqual(FC.orderProblem({ etd: '2026-12-19' }, { ...held, deliveryDate: null, deliveryMixed: [null, '2026-12-18', '2026-12-25'] }), ['etd', 'deliveryDate']);
    assert.equal(FC.orderProblem({ etd: '2026-12-18' }, { ...held, deliveryDate: null, deliveryMixed: [null, '2026-12-18', '2026-12-25'] }), null);
    // What we hold in disorder is not a proposal's to answer for when it touches neither side.
    assert.equal(FC.orderProblem({ loadingDate: '2026-12-01' }, { loadingDate: null, etd: '2026-12-25', deliveryDate: '2026-12-20', deliveryMixed: null }), null);
    assert.equal(FC.orderProblem({}, { loadingDate: '2026-12-30', etd: '2026-12-25', deliveryDate: '2026-12-20' }), null);
    // datesInOrder is the same answer as yes or no.
    assert.equal(FC.datesInOrder({ etd: '2026-12-21' }, held), false);
    assert.equal(FC.datesInOrder({ etd: '2026-12-20' }, held), true);
});

test('finishRefusal: the orders on the container, and what each holds for delivery, are what the claim saw', () => {
    const members = [{ id: 1, delivery_at: null }, { id: 2, delivery_at: '2026-12-01' }];
    const claim = { ecn: null, loadingDate: null, etd: '2026-11-20', members: [1, 2], deliveryByOrder: { 1: null, 2: '2026-12-01' } };
    const base = { requested: { deliveryDate: '2026-12-05T10:00' }, claim, ecnReset: null, memberEcns: [null, null], held: { ecn: null, loadingDate: null, etd: '2026-11-20' }, members };
    assert.equal(FC.finishRefusal(base), null);
    // A delivery date typed on one order since the claim.
    assert.deepEqual(FC.finishRefusal({ ...base, members: [{ id: 1, delivery_at: '2026-12-02' }, { id: 2, delivery_at: '2026-12-01' }] }), { code: 'CONTAINER_CHANGED', fields: ['deliveryDate'] });
    // An order joined, or one left.
    assert.deepEqual(FC.finishRefusal({ ...base, members: [...members, { id: 3, delivery_at: null }] }), { code: 'CONTAINER_CHANGED', fields: ['members'] });
    assert.deepEqual(FC.finishRefusal({ ...base, members: [members[0]] }), { code: 'CONTAINER_CHANGED', fields: ['members'] });
    // The same value in another shape is no change.
    assert.equal(FC.finishRefusal({ ...base, members: [{ id: 1, delivery_at: null }, { id: 2, delivery_at: '2026-12-01T00:00' }] }), null);
    // A delivery date before the departure the container holds.
    assert.deepEqual(FC.finishRefusal({ ...base, requested: { deliveryDate: '2026-11-19' } }), { code: 'DATES_OUT_OF_ORDER', fields: ['etd', 'deliveryDate'] });
    // A delivery date needs an order to carry it.
    assert.deepEqual(FC.finishRefusal({ ...base, members: [], claim: { ...claim, members: [], deliveryByOrder: {} } }), { code: 'NO_ORDERS', fields: ['deliveryDate'] });
    // A claim made before this was recorded (one in flight across a deploy) is not judged on it.
    assert.equal(FC.finishRefusal({ ...base, claim: { ecn: null, loadingDate: null, etd: '2026-11-20' }, requested: { etd: '2026-11-21' }, members: [{ id: 9, delivery_at: null }] }), null);
});

test('claimSnapshot: what a claim records of the container, per order', () => {
    assert.deepEqual(FC.claimSnapshot({ ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' }, [{ id: 2, delivery_at: '2026-12-20T00:00' }, { id: 1, delivery_at: null }]), {
        ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15', deliveryDate: null, deliveryMixed: [null, '2026-12-20'],
        members: [1, 2], deliveryByOrder: { 1: null, 2: '2026-12-20' },
    });
});

test('proposalOf: what a forwarder asked for, only the things it can ask, cleaned', () => {
    assert.deepEqual(FC.proposalOf('{"ecn":"msku1234565","loadingDate":"2026-12-10","etd":"2026-12-15","vessel":"x"}'), { ecn: 'MSKU1234565', loadingDate: '2026-12-10', etd: '2026-12-15' });
    assert.deepEqual(FC.proposalOf({ etd: '2026-12-15' }), { etd: '2026-12-15' });
    assert.deepEqual(FC.proposalOf('{"loadingDate":"10/12/2026","ecn":""}'), {}, 'a date that is not a day, and an empty number, ask for nothing');
    assert.deepEqual(FC.proposalOf('not json'), {});
    assert.deepEqual(FC.proposalOf(null), {});
});

test('liveProposal: the newest row of a forwarder for a container, chosen before its status is looked at', () => {
    const rows = [
        { id: 5, forwarder_id: 4, status: 'pending' },
        { id: 9, forwarder_id: 4, status: 'rejected' },
        { id: 7, forwarder_id: 4, status: 'pending' },
        { id: 12, forwarder_id: 3, status: 'pending' },
    ];
    assert.equal(FC.liveProposal(rows, 4).id, 9, 'the newest, though it is decided: the older pending ones do not come back');
    assert.equal(FC.liveProposal(rows, 3).id, 12);
    assert.equal(FC.liveProposal(rows, 8), null);
    assert.equal(FC.liveProposal([], 4), null);
});

test('isWaiting: pending, or a claim that lapsed; a claim in progress and a decided row are not', () => {
    assert.equal(FC.isWaiting({ status: 'pending' }), true);
    assert.equal(FC.isWaiting({ status: 'applying', lapsed: 1 }), true);
    assert.equal(FC.isWaiting({ status: 'applying', lapsed: 0 }), false);
    assert.equal(FC.isWaiting({ status: 'applied' }), false);
    assert.equal(FC.isWaiting({ status: 'rejected' }), false);
});

test('claimRefusal: only the live, waiting proposal of the container\'s own forwarder, with no other accept in progress', () => {
    const row = (over = {}) => ({ id: 9, forwarder_id: 4, status: 'pending', lapsed: 0, claimed_by: null, claimed_at_iso: null, requested_json: '{"ecn":"MSKU1234565"}', ...over });
    const ok = { rows: [row()], id: 9, duties: dutiesOf(ship({ forwarder_id: 4 })) };
    assert.equal(FC.claimRefusal(ok), null);
    assert.deepEqual(FC.claimRefusal({ ...ok, rows: [row(), row({ id: 11 })] }), { code: 'SUPERSEDED' });
    assert.deepEqual(FC.claimRefusal({ ...ok, rows: [row({ status: 'applied' })] }), { code: 'ALREADY_DECIDED' });
    assert.deepEqual(FC.claimRefusal({ ...ok, rows: [row({ status: 'rejected' })] }), { code: 'ALREADY_DECIDED' });
    assert.deepEqual(FC.claimRefusal({ ...ok, duties: dutiesOf(ship({ forwarder_id: 3 })) }), { code: 'NOT_THEIR_CONTAINER' });
    assert.deepEqual(FC.claimRefusal({ ...ok, duties: dutiesOf(ship()) }), { code: 'NOT_THEIR_CONTAINER' });
    // Still attached, but no longer for what it asks: freight went to 3, forwarder 4 keeps customs.
    assert.deepEqual(FC.claimRefusal({ ...ok, duties: dutiesOf(ship({ forwarder_id: 3, customs_forwarder_id: 4 })) }), { code: 'NOT_THEIR_DUTY', fields: ['ecn'] });
    // Holding freight by booking is as good as by hand.
    assert.equal(FC.claimRefusal({ ...ok, duties: dutiesOf(ship(), [bk()]) }), null);
    assert.deepEqual(FC.claimRefusal({ ...ok, id: 77 }), { code: 'NO_SUCH_PROPOSAL' });
    // An accept in progress on this container: by this proposal from another tab, or by another forwarder's.
    assert.deepEqual(FC.claimRefusal({ ...ok, rows: [row({ status: 'applying', claimed_by: 'ann@us.com', claimed_at_iso: '2026-11-23T10:00:00Z' })] }),
        { code: 'CLAIM_IN_PROGRESS', claimedBy: 'ann@us.com', claimedAt: '2026-11-23T10:00:00Z' });
    assert.deepEqual(FC.claimRefusal({ ...ok, rows: [row(), row({ id: 3, forwarder_id: 3, status: 'applying', claimed_by: 'bob@us.com', claimed_at_iso: '2026-11-23T10:01:00Z' })] }),
        { code: 'CLAIM_IN_PROGRESS', claimedBy: 'bob@us.com', claimedAt: '2026-11-23T10:01:00Z' });
    // A claim that lapsed is no claim: the row can be claimed again.
    assert.equal(FC.claimRefusal({ ...ok, rows: [row({ status: 'applying', lapsed: 1, claimed_by: 'ann@us.com' })] }), null);
    assert.equal(FC.claimRefusal({ ...ok, rows: [row(), row({ id: 3, forwarder_id: 3, status: 'applying', lapsed: 1 })] }), null);
});

test('sameHeld: what the card showed against what the container holds now', () => {
    const held = { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' };
    assert.deepEqual(FC.heldDiff({ ecn: 'msku1234565', loadingDate: null, etd: '2026-12-15' }, held), []);
    assert.deepEqual(FC.heldDiff({ ecn: null, loadingDate: null, etd: '2026-12-15' }, held), ['ecn']);
    assert.deepEqual(FC.heldDiff({ ecn: 'MSKU1234565', loadingDate: '2026-12-10', etd: '2026-12-16' }, held), ['loadingDate', 'etd']);
    assert.deepEqual(FC.heldDiff({}, { ecn: null, loadingDate: null, etd: null }), [], 'nothing said and nothing held are the same');
    assert.deepEqual(FC.heldDiff(null, held), ['ecn', 'etd']);
});

test('datesInOrder: loading on or before departure, taking what is held for what is not proposed', () => {
    assert.equal(FC.datesInOrder({ loadingDate: '2026-12-10', etd: '2026-12-15' }, {}), true);
    assert.equal(FC.datesInOrder({ loadingDate: '2026-12-15', etd: '2026-12-15' }, {}), true);
    assert.equal(FC.datesInOrder({ loadingDate: '2026-12-16', etd: '2026-12-15' }, {}), false);
    assert.equal(FC.datesInOrder({ loadingDate: '2026-12-16' }, { etd: '2026-12-15' }), false, 'against the departure we hold');
    assert.equal(FC.datesInOrder({ etd: '2026-12-09' }, { loadingDate: '2026-12-10' }), false, 'against the loading date we hold');
    assert.equal(FC.datesInOrder({ loadingDate: '2026-12-16' }, { etd: null }), true, 'one date alone is in order');
    assert.equal(FC.datesInOrder({}, { loadingDate: '2026-12-16', etd: '2026-12-15' }), true, 'nothing proposed: what we hold is not this proposal\'s to judge');
});

test('finishBaseline: what the container must hold at finish is what the claim saw, but for the departure date the accept\'s own ECN reset wrote', () => {
    const claim = { ecn: 'OLDU1234560', loadingDate: '2026-12-10', etd: '2026-12-15' };
    // No reset reported: the claim, as it was.
    assert.deepEqual(FC.finishBaseline({ claim, requested: { ecn: 'MSKU1234565' }, ecnReset: null }), claim);
    // An ECN replaced an existing one, and the browser's reset moved the departure date.
    assert.deepEqual(FC.finishBaseline({ claim, requested: { ecn: 'MSKU1234565' }, ecnReset: { etd: '2026-12-01' } }), { ...claim, etd: '2026-12-01' });
    assert.deepEqual(FC.finishBaseline({ claim, requested: { ecn: 'MSKU1234565' }, ecnReset: { etd: null } }), { ...claim, etd: null }, 'the reset may have cleared it');
    // A reset is taken only when this proposal REPLACED an ECN: a first ECN resets nothing.
    assert.deepEqual(FC.finishBaseline({ claim: { ...claim, ecn: null }, requested: { ecn: 'MSKU1234565' }, ecnReset: { etd: '2026-12-01' } }), { ...claim, ecn: null });
    assert.deepEqual(FC.finishBaseline({ claim, requested: { ecn: 'oldu1234560' }, ecnReset: { etd: '2026-12-01' } }), claim, 'the same number is no replacement');
    assert.deepEqual(FC.finishBaseline({ claim, requested: { etd: '2026-12-20' }, ecnReset: { etd: '2026-12-01' } }), claim, 'no ECN in the proposal: nothing was reset');
    // What is not a day is not a date.
    assert.deepEqual(FC.finishBaseline({ claim, requested: { ecn: 'MSKU1234565' }, ecnReset: { etd: 'soon' } }), { ...claim, etd: null });
});

test('finishRefusal: the ECN is on every order, and nothing else moved since the claim', () => {
    const claim = { ecn: null, loadingDate: null, etd: '2026-12-15' };
    const base = { requested: { ecn: 'MSKU1234565', etd: '2026-12-20' }, claim, ecnReset: null, memberEcns: ['MSKU1234565', 'msku1234565'], held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' } };
    assert.equal(FC.finishRefusal(base), null);
    // The ECN never landed, landed on some orders only, or there is no order to carry it.
    assert.deepEqual(FC.finishRefusal({ ...base, memberEcns: ['MSKU1234565', null] }), { code: 'NOT_ON_CONTAINER', fields: ['ecn'] });
    assert.deepEqual(FC.finishRefusal({ ...base, memberEcns: [] }), { code: 'NOT_ON_CONTAINER', fields: ['ecn'] });
    assert.deepEqual(FC.finishRefusal({ ...base, held: { ...base.held, ecn: 'OTHR1234560' } }), { code: 'NOT_ON_CONTAINER', fields: ['ecn'] });
    // Someone else changed the departure date between the claim and now.
    assert.deepEqual(FC.finishRefusal({ ...base, held: { ...base.held, etd: '2026-12-17' } }), { code: 'CONTAINER_CHANGED', fields: ['etd'] });
    // ... or a value this proposal does not touch.
    assert.deepEqual(FC.finishRefusal({ ...base, held: { ...base.held, loadingDate: '2026-12-01' } }), { code: 'CONTAINER_CHANGED', fields: ['loadingDate'] });
    // A proposal with no ECN: the number we hold must be the one the claim saw.
    const datesOnly = { requested: { etd: '2026-12-20' }, claim: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' }, ecnReset: null, memberEcns: ['MSKU1234565'], held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' } };
    assert.equal(FC.finishRefusal(datesOnly), null);
    assert.deepEqual(FC.finishRefusal({ ...datesOnly, held: { ...datesOnly.held, ecn: null } }), { code: 'CONTAINER_CHANGED', fields: ['ecn'] });
    // The dates that would result are out of order.
    assert.deepEqual(FC.finishRefusal({ ...base, requested: { ecn: 'MSKU1234565', loadingDate: '2026-12-18' }, held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' } }), { code: 'DATES_OUT_OF_ORDER', fields: ['loadingDate', 'etd'] });
});

test('finishRefusal: a reset by this accept is not someone else\'s change, and someone else\'s change after it still is', () => {
    const claim = { ecn: 'OLDU1234560', loadingDate: null, etd: '2026-12-15' };
    const base = { requested: { ecn: 'MSKU1234565', etd: '2026-12-20' }, claim, memberEcns: ['MSKU1234565'] };
    // The reset wrote the 10th; the container holds the 10th: only this accept touched it.
    assert.equal(FC.finishRefusal({ ...base, ecnReset: { etd: '2026-12-10' }, held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-10' } }), null);
    // The reset wrote the 10th; someone then set the 15th; the proposal asks for the 20th.
    assert.deepEqual(FC.finishRefusal({ ...base, ecnReset: { etd: '2026-12-10' }, held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-15' } }), { code: 'CONTAINER_CHANGED', fields: ['etd'] });
    // No reset was reported, and the date moved: someone else's.
    assert.deepEqual(FC.finishRefusal({ ...base, ecnReset: null, held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-10' } }), { code: 'CONTAINER_CHANGED', fields: ['etd'] });
    // A loading-only proposal whose departure date the reset moved earlier: out of order.
    assert.deepEqual(FC.finishRefusal({ requested: { ecn: 'MSKU1234565', loadingDate: '2026-12-12' }, claim, memberEcns: ['MSKU1234565'], ecnReset: { etd: '2026-12-10' }, held: { ecn: 'MSKU1234565', loadingDate: null, etd: '2026-12-10' } }),
        { code: 'DATES_OUT_OF_ORDER', fields: ['loadingDate', 'etd'] });
});

test('toReviewCount: what waits for our team, on containers that are still that forwarder\'s', () => {
    assert.equal(FC.toReviewCount({ proposals: [], uploads: [] }), 0);
    assert.equal(FC.toReviewCount({ proposals: [{ waiting: true, actionable: true }, { waiting: true, actionable: false }, { waiting: false, actionable: true }], uploads: [{ waiting: true, actionable: true }, { waiting: true, actionable: true }, { waiting: true, actionable: false }] }), 3);
});

test('noteProblem: a note the forwarder will read needs at least three characters and at most 500', () => {
    assert.equal(FC.noteProblem('This is the draft B/L'), null);
    assert.equal(FC.noteProblem('  ok  '), 'short');
    assert.equal(FC.noteProblem(''), 'short');
    assert.equal(FC.noteProblem(null), 'short');
    assert.equal(FC.noteProblem('x'.repeat(501)), 'long');
    assert.equal(FC.cleanNote('  two  words  '), 'two  words');
});

// ── slice C: a forwarder's files (invoice, loading photos, bill of lading) ──

const MB = 1024 * 1024;
const PDF = Buffer.from([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37]);
const PNG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0]);
const JPG = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0x10]);
const UUID = '3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f';
const upload = (over = {}) => ({
    id: 70, shipment_id: 1214, forwarder_id: 4, kind: 'invoice', filename: 'invoice 339.pdf', content_type: 'application/pdf', file_size: PDF.length,
    upload_status: 'sent', review_status: 'open', final_key: 'files/fwd/70/' + 'a'.repeat(32) + '.pdf', s3_etag: '"e1"', ...over,
});

test('uploads: the draft customs entry is the customs duty\'s, the fourth kind, and kept with the forwarder documents', () => {
    assert.deepEqual(FC.UPLOAD_KINDS, ['invoice', 'loading_photo', 'bill_of_lading', 'customs_draft']);
    assert.deepEqual(FC.ORIGINAL_KINDS, ['invoice', 'loading_photo', 'bill_of_lading']);
    assert.equal(FC.KIND_DUTY.customs_draft, 'customs');
    assert.deepEqual(FC.UPLOAD_RULES.customs_draft, { types: ['application/pdf', 'image/png', 'image/jpeg'], maxBytes: 15 * MB });
    assert.equal('customs_draft'.length <= 16, true, 'it fits the portal\'s kind column');
    assert.equal(FC.kindAllowed('customs_draft', ['customs']), true);
    assert.equal(FC.kindAllowed('customs_draft', ['freight', 'delivery']), false);
    assert.match(FC.filedKeyFor({ kind: 'customs_draft', random: '3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f', filename: 'entry draft.pdf', contentType: 'application/pdf' }), /^forwarder-documents\/3f2c9a40-1b7d-4c1e-9f51-0a2b3c4d5e6f\/entry_draft\.pdf$/);
    // A decision on a kind beyond the original three must name it.
    assert.equal(FC.clientKnowsKind('invoice', undefined), true);
    assert.equal(FC.clientKnowsKind('customs_draft', undefined), false);
    assert.equal(FC.clientKnowsKind('customs_draft', 'bill_of_lading'), false);
    assert.equal(FC.clientKnowsKind('customs_draft', 'customs_draft'), true);
    // Only the company that handles customs sends one.
    const duties = dutiesOf(ship({ forwarder_id: 4, customs_forwarder_id: 5 }));
    const row = { id: 70, forwarder_id: 5, kind: 'customs_draft', upload_status: 'sent', review_status: 'open', final_key: 'k' };
    assert.equal(FC.uploadGate({ row, duties, action: 'accept' }), null);
    assert.deepEqual(FC.uploadGate({ row: { ...row, forwarder_id: 4 }, duties, action: 'accept' }), { status: 409, code: 'NOT_THEIR_DUTY', allowedNext: ['reject'], kind: 'customs_draft' });
});

test('uploads: the original three kinds, and what each may be', () => {
    assert.deepEqual(FC.UPLOAD_RULES.invoice, { types: ['application/pdf', 'image/png', 'image/jpeg'], maxBytes: 10 * MB });
    assert.deepEqual(FC.UPLOAD_RULES.loading_photo, { types: ['image/png', 'image/jpeg'], maxBytes: 15 * MB });
    assert.deepEqual(FC.UPLOAD_RULES.bill_of_lading, { types: ['application/pdf', 'image/png', 'image/jpeg'], maxBytes: 15 * MB });
});

test('uploads: a file is what its first bytes are', () => {
    assert.equal(FC.uploadType(PDF), 'application/pdf');
    assert.equal(FC.uploadType(PNG), 'image/png');
    assert.equal(FC.uploadType(JPG), 'image/jpeg');
    assert.equal(FC.uploadType(Buffer.from('<html>')), null);
    assert.equal(FC.uploadType(Buffer.from([0x4d, 0x5a, 0x90, 0])), null);
    assert.equal(FC.uploadType(null), null);
});

test('uploads: the bytes read at accept must be the file the portal recorded', () => {
    assert.equal(FC.uploadFileRefusal({ row: upload(), bytes: PDF }), null);
    // A picture where the row says PDF: both are allowed for an invoice, and it is still refused.
    assert.equal(FC.uploadFileRefusal({ row: upload(), bytes: Buffer.concat([JPG, Buffer.alloc(2)]) }), 'FILE_NOT_ITS_TYPE');
    // A PDF filed as a photo.
    assert.equal(FC.uploadFileRefusal({ row: upload({ kind: 'loading_photo', content_type: 'application/pdf' }), bytes: PDF }), 'FILE_NOT_ITS_TYPE');
    assert.equal(FC.uploadFileRefusal({ row: upload({ kind: 'loading_photo', content_type: 'image/jpeg', file_size: JPG.length }), bytes: JPG }), null);
    assert.equal(FC.uploadFileRefusal({ row: upload({ file_size: PDF.length + 1 }), bytes: PDF }), 'FILE_SIZE_CHANGED');
    assert.equal(FC.uploadFileRefusal({ row: upload({ kind: 'telex' }), bytes: PDF }), 'FILE_NOT_ITS_TYPE');
    const big = Buffer.concat([PDF, Buffer.alloc(10 * MB)]);
    assert.equal(FC.uploadFileRefusal({ row: upload({ file_size: big.length }), bytes: big }), 'FILE_TOO_LARGE');
    assert.equal(FC.uploadFileRefusal({ row: upload({ kind: 'bill_of_lading', file_size: big.length }), bytes: big }), null);
});

test('uploads: what may be decided, and when', () => {
    const gate = (row, action = 'accept', owner = 4) => FC.uploadGate({ row, duties: dutiesOf(ship({ forwarder_id: owner })), action });
    assert.equal(gate(upload()), null);
    assert.deepEqual(gate(null), { status: 404, code: 'NO_SUCH_UPLOAD', allowedNext: ['reload'] });
    assert.deepEqual(gate(upload({ upload_status: 'withdrawn' })), { status: 409, code: 'WITHDRAWN', allowedNext: ['reload'] });
    assert.deepEqual(gate(upload({ upload_status: 'draft' })), { status: 409, code: 'NOT_SENT', allowedNext: ['reload'] });
    assert.deepEqual(gate(upload({ upload_status: 'finalising' })), { status: 409, code: 'NOT_SENT', allowedNext: ['reload'] });
    assert.deepEqual(gate(upload({ review_status: 'received' })), { status: 409, code: 'ALREADY_DECIDED', allowedNext: ['reload'] });
    assert.deepEqual(gate(upload({ review_status: 'rejected' })), { status: 409, code: 'ALREADY_DECIDED', allowedNext: ['reload'] });
    // No longer that forwarder's container: it can be sent back (dismissed), never accepted.
    assert.deepEqual(gate(upload(), 'accept', 3), { status: 409, code: 'NOT_THEIR_CONTAINER', allowedNext: ['reject'] });
    assert.deepEqual(gate(upload(), 'accept', null), { status: 409, code: 'NOT_THEIR_CONTAINER', allowedNext: ['reject'] });
    assert.equal(gate(upload(), 'reject', 3), null);
    assert.equal(gate(upload(), 'reject', null), null);
    // A file with no final key was never really sent.
    assert.deepEqual(gate(upload({ final_key: null })), { status: 409, code: 'NOT_SENT', allowedNext: ['reload'] });
});

test('uploads: a file is accepted only from the company that holds its kind\'s duty', () => {
    // Freight is forwarder 4's (by booking), delivery and customs are forwarder 5's.
    const duties = dutiesOf(ship({ delivery_forwarder_id: 5, customs_forwarder_id: 5 }), [bk()]);
    const gate = (row, action = 'accept') => FC.uploadGate({ row, duties, action });
    assert.equal(gate(upload({ kind: 'loading_photo' })), null);
    assert.equal(gate(upload({ kind: 'bill_of_lading' })), null);
    // An invoice: from anyone who holds a duty.
    assert.equal(gate(upload({ forwarder_id: 5 })), null);
    assert.deepEqual(gate(upload({ forwarder_id: 5, kind: 'loading_photo' })), { status: 409, code: 'NOT_THEIR_DUTY', allowedNext: ['reject'], kind: 'loading_photo' });
    assert.deepEqual(gate(upload({ forwarder_id: 5, kind: 'bill_of_lading' })), { status: 409, code: 'NOT_THEIR_DUTY', allowedNext: ['reject'], kind: 'bill_of_lading' });
    assert.equal(gate(upload({ forwarder_id: 5, kind: 'loading_photo' }), 'reject'), null, 'it can still be sent back');
    assert.deepEqual(gate(upload({ forwarder_id: 6 })), { status: 409, code: 'NOT_THEIR_CONTAINER', allowedNext: ['reject'] });
});

test('uploads: an invoice is filed under the name its forwarder is paid under, else its name on the Forwarders list', () => {
    assert.equal(FC.filingName({ payeeNames: ['DCG Logistics Ltd', 'DCG'], listName: 'DCG' }), 'DCG Logistics Ltd');
    assert.equal(FC.filingName({ payeeNames: ['  ', null, 'DCG Freight'], listName: 'DCG' }), 'DCG Freight');
    assert.equal(FC.filingName({ payeeNames: [], listName: ' DCG ' }), 'DCG');
    assert.equal(FC.filingName({ payeeNames: [], listName: '' }), null);
    assert.equal(FC.filingName({}), null);
});

test('uploads: an invoice is never filed under a name that is exactly a supplier\'s with goods on board', () => {
    assert.equal(FC.filingNameClash('DCG', ['Ningbo Medical Co', 'Other']), false);
    assert.equal(FC.filingNameClash('Ningbo Medical Co', ['Ningbo Medical Co']), true);
    // As the invoice store compares names: case and spacing aside.
    assert.equal(FC.filingNameClash('  ningbo   medical co ', ['Ningbo Medical Co']), true);
    // A look-alike is not caught: only the same name.
    assert.equal(FC.filingNameClash('Ningbo Medical Co.', ['Ningbo Medical Co']), false);
    assert.equal(FC.filingNameClash('DCG', []), false);
    assert.equal(FC.filingNameClash(null, ['x']), false);
});

test('uploads: where our team\'s copy lives: the kind\'s own folder, a random part made once, a clean name', () => {
    assert.equal(FC.filedKeyFor({ kind: 'invoice', random: UUID, filename: 'invoice 339.pdf', contentType: 'application/pdf' }), `shipment-payments/${UUID}/invoice_339.pdf`);
    assert.equal(FC.filedKeyFor({ kind: 'loading_photo', random: UUID, filename: 'door.JPG', contentType: 'image/jpeg' }), `container-photos/${UUID}/door.JPG`);
    assert.equal(FC.filedKeyFor({ kind: 'bill_of_lading', random: UUID, filename: 'B/L 339.pdf', contentType: 'application/pdf' }), `forwarder-documents/${UUID}/B_L_339.pdf`);
    // The name says what the bytes are: a wrong or missing extension gets the right one.
    assert.equal(FC.filedKeyFor({ kind: 'loading_photo', random: UUID, filename: 'door.png', contentType: 'image/jpeg' }), `container-photos/${UUID}/door.png.jpg`);
    assert.equal(FC.filedKeyFor({ kind: 'invoice', random: UUID, filename: '发票', contentType: 'application/pdf' }), `shipment-payments/${UUID}/_.pdf`);
    assert.equal(FC.filedKeyFor({ kind: 'invoice', random: UUID, filename: '../../etc/passwd', contentType: 'application/pdf' }), `shipment-payments/${UUID}/.._.._etc_passwd.pdf`);
    const long = FC.filedKeyFor({ kind: 'invoice', random: UUID, filename: `${'a'.repeat(300)}.pdf`, contentType: 'application/pdf' });
    assert.ok(long.length <= `shipment-payments/${UUID}/`.length + 124, String(long.length));
    assert.ok(long.endsWith('.pdf'));
    // A photo's key is one the photo routes would have made themselves.
    assert.match(FC.filedKeyFor({ kind: 'loading_photo', random: UUID, filename: 'a b.jpg', contentType: 'image/jpeg' }), /^container-photos\/[0-9a-f-]{36}\/[A-Za-z0-9._-]+$/);
    assert.throws(() => FC.filedKeyFor({ kind: 'invoice', random: 'not-a-uuid', filename: 'a.pdf', contentType: 'application/pdf' }), /random/);
    assert.throws(() => FC.filedKeyFor({ kind: 'telex', random: UUID, filename: 'a.pdf', contentType: 'application/pdf' }), /kind/);
    assert.throws(() => FC.filedKeyFor({ kind: 'invoice', random: UUID, filename: 'a.zip', contentType: 'application/zip' }), /type/);
});

test('uploads: where one stands for our team', () => {
    assert.equal(FC.uploadReviewState(upload()), 'open');
    assert.equal(FC.uploadReviewState(upload({ review_status: 'received' })), 'received');
    assert.equal(FC.uploadReviewState(upload({ review_status: 'rejected' })), 'rejected');
    assert.equal(FC.uploadReviewState(upload({ upload_status: 'withdrawn' })), 'withdrawn');
    assert.equal(FC.uploadReviewState(upload({ upload_status: 'draft' })), 'not_sent');
});
