'use strict';

// Unit tests for src/lib/sample-orders.js: the rules of a sample order (a
// purchase order of kind 'sample' and its lines).
// Run: node --test tools/test-sample-orders-lib.js

const { test } = require('node:test');
const assert = require('node:assert/strict');
const S = require('../src/lib/sample-orders');

const PO = { id: 7, kind: 'sample', committed: false };

// A sample line as the orders table holds it.
const row = (over = {}) => ({
    id: 10, order_kind: 'sample', purchase_order_id: 7, status: 'PO_SENT',
    jf_code: 'JF1016', lot_number: 'SA0031-S01-V03-CN0011', asin: null,
    quantity: 2, unit_price: 0,
    po_number: 'PO_00431J', po_date: '2026-11-25', supplier: 'Acme Co., Ltd.',
    shipped_date: null, arrived_date: null, sample_received_qty: null, sample_ever_sent: 0,
    container_number: null, awb_number: null, external_container_number: null, shipment_id: null,
    courier_name: null, courier_tracking: null,
    ...over,
});

const write = (before, after, ctx = {}) => S.validateSampleWrite(before, after, {
    via: 'update', isAdmin: false, reason: null, po: PO, lotChecked: null, ...ctx,
});

// ── kind ─────────────────────────────────────────────────────────────────

test('normalizeKind: nothing sent is stock, sample is sample, anything else is refused', () => {
    assert.equal(S.normalizeKind(undefined), 'stock');
    assert.equal(S.normalizeKind(null), 'stock');
    assert.equal(S.normalizeKind(''), 'stock');
    assert.equal(S.normalizeKind('stock'), 'stock');
    assert.equal(S.normalizeKind(' Sample '), 'sample');
    assert.equal(S.normalizeKind('samples'), null);
    assert.equal(S.normalizeKind(1), null);
});

test('isSample reads the row either way it is spelled', () => {
    assert.equal(S.isSample({ order_kind: 'sample' }), true);
    assert.equal(S.isSample({ orderKind: 'sample' }), true);
    assert.equal(S.isSample({ order_kind: 'stock' }), false);
    assert.equal(S.isSample({}), false);
    assert.equal(S.isSample(null), false);
});

// ── states and moves ─────────────────────────────────────────────────────

test('the path is SCHEDULED, PO_SENT, ON_AIR, ARRIVED, RECEIVED, one step at a time', () => {
    assert.equal(S.canMove('SCHEDULED', 'PO_SENT'), true);
    assert.equal(S.canMove('PO_SENT', 'ON_AIR'), true);
    assert.equal(S.canMove('ON_AIR', 'ARRIVED_AT_WAREHOUSE'), true);
    assert.equal(S.canMove('ARRIVED_AT_WAREHOUSE', 'RECEIVED', { via: 'sample-received' }), true);
    // No skipping.
    assert.equal(S.canMove('SCHEDULED', 'ON_AIR'), false);
    assert.equal(S.canMove('PO_SENT', 'ARRIVED_AT_WAREHOUSE'), false);
    assert.equal(S.canMove('ON_AIR', 'RECEIVED', { via: 'sample-received' }), false);
});

test('a sample never enters a production, container or sea stage', () => {
    for (const to of ['IN_PRODUCTION', 'READY_FOR_QC', 'READY', 'CONSOLIDATED', 'ON_SEA', 'REPACKAGED', 'PARTIALLY_RECEIVED', 'MINTSOFT']) {
        for (const from of S.SAMPLE_STATUSES) {
            assert.equal(S.canMove(from, to, { isAdmin: true, via: 'sample-received' }), false, `${from} -> ${to}`);
        }
    }
});

test('RECEIVED is reached through the sample-received route only', () => {
    assert.equal(S.canMove('ARRIVED_AT_WAREHOUSE', 'RECEIVED'), false);
    assert.equal(S.canMove('ARRIVED_AT_WAREHOUSE', 'RECEIVED', { via: 'status', isAdmin: true }), false);
    assert.equal(S.canMove('ARRIVED_AT_WAREHOUSE', 'RECEIVED', { via: 'sample-received' }), true);
});

test('DESTROYED: an admin, from On Air or Arrived only', () => {
    assert.equal(S.canMove('ON_AIR', 'DESTROYED', { isAdmin: true }), true);
    assert.equal(S.canMove('ARRIVED_AT_WAREHOUSE', 'DESTROYED', { isAdmin: true }), true);
    assert.equal(S.canMove('ON_AIR', 'DESTROYED'), false);
    assert.equal(S.canMove('PO_SENT', 'DESTROYED', { isAdmin: true }), false);
    assert.equal(S.canMove('RECEIVED', 'DESTROYED', { isAdmin: true }), false);
});

test('writing a sample off says why', () => {
    const onAir = row({ status: 'ON_AIR', shipped_date: '2026-11-26', sample_ever_sent: 1 });
    assert.equal(write(onAir, { ...onAir, status: 'DESTROYED' }, { isAdmin: true }).code, 'SAMPLE_REASON_REQUIRED');
    assert.equal(write(onAir, { ...onAir, status: 'DESTROYED' }, { isAdmin: true, reason: ' ' }).code, 'SAMPLE_REASON_REQUIRED');
    const ok = write(onAir, { ...onAir, status: 'DESTROYED' }, { isAdmin: true, reason: 'courier lost the parcel' });
    assert.equal(ok.ok, true);
    assert.equal(ok.statusChanged, true);
    // Not being an admin is the first thing said.
    assert.equal(write(onAir, { ...onAir, status: 'DESTROYED' }, { reason: 'courier lost the parcel' }).code, 'SAMPLE_STATUS');
});

test('corrections: an admin, one step back, never to SCHEDULED', () => {
    for (const [from, to] of [['RECEIVED', 'ARRIVED_AT_WAREHOUSE'], ['ARRIVED_AT_WAREHOUSE', 'ON_AIR'], ['ON_AIR', 'PO_SENT'], ['DESTROYED', 'ON_AIR']]) {
        assert.equal(S.canMove(from, to, { isAdmin: true }), true, `${from} -> ${to}`);
        assert.equal(S.canMove(from, to), false, `${from} -> ${to} (not admin)`);
    }
    assert.equal(S.canMove('PO_SENT', 'SCHEDULED', { isAdmin: true }), false);
    assert.equal(S.canMove('RECEIVED', 'ON_AIR', { isAdmin: true }), false);
});

test('allowedNext lists what the caller may do next', () => {
    assert.deepEqual(S.allowedNext('SCHEDULED'), ['PO_SENT']);
    assert.deepEqual(S.allowedNext('PO_SENT'), ['ON_AIR']);
    assert.deepEqual(S.allowedNext('ON_AIR'), ['ARRIVED_AT_WAREHOUSE']);
    assert.deepEqual(S.allowedNext('ARRIVED_AT_WAREHOUSE'), ['RECEIVED']);
    assert.deepEqual(S.allowedNext('RECEIVED'), []);
    assert.deepEqual(S.allowedNext('ON_AIR', { isAdmin: true }), ['ARRIVED_AT_WAREHOUSE', 'DESTROYED', 'PO_SENT']);
    assert.deepEqual(S.allowedNext('ARRIVED_AT_WAREHOUSE', { isAdmin: true }), ['RECEIVED', 'DESTROYED', 'ON_AIR']);
    assert.deepEqual(S.allowedNext('RECEIVED', { isAdmin: true }), ['ARRIVED_AT_WAREHOUSE']);
    assert.deepEqual(S.allowedNext('DESTROYED', { isAdmin: true }), ['ON_AIR']);
    assert.deepEqual(S.allowedNext('IN_PRODUCTION', { isAdmin: true }), []);
});

// ── the validator: creating ──────────────────────────────────────────────

test('create: at SCHEDULED or PO_SENT only', () => {
    assert.equal(write(null, row({ status: 'SCHEDULED', po_number: null, po_date: null, supplier: null }), { via: 'create', lotChecked: true }).ok, true);
    assert.equal(write(null, row(), { via: 'create', lotChecked: true }).ok, true);
    const late = write(null, row({ status: 'ON_AIR', shipped_date: '2026-11-26' }), { via: 'create', lotChecked: true });
    assert.equal(late.ok, false);
    assert.equal(late.code, 'SAMPLE_STATUS');
    assert.deepEqual(late.allowedNext, ['SCHEDULED', 'PO_SENT']);
});

test('create: no price is a free sample, stored as 0; a price is kept; a negative one is refused', () => {
    for (const blank of [null, undefined, '']) {
        const r = write(null, row({ unit_price: blank }), { via: 'create', lotChecked: true });
        assert.equal(r.ok, true);
        assert.equal(r.set.unit_price, 0);
    }
    const priced = write(null, row({ unit_price: '12.50' }), { via: 'create', lotChecked: true });
    assert.equal(priced.ok, true);
    assert.equal(priced.set.unit_price, 12.5);
    const neg = write(null, row({ unit_price: -1 }), { via: 'create', lotChecked: true });
    assert.equal(neg.code, 'SAMPLE_PRICE');
    assert.equal(write(null, row({ unit_price: 'abc' }), { via: 'create', lotChecked: true }).code, 'SAMPLE_PRICE');
});

test('create: quantity is a whole number from 1 to 50', () => {
    for (const bad of [0, -1, 1.5, 51, null, 'x']) {
        assert.equal(write(null, row({ quantity: bad }), { via: 'create', lotChecked: true }).code, 'SAMPLE_QUANTITY', String(bad));
    }
    assert.equal(write(null, row({ quantity: 50 }), { via: 'create', lotChecked: true }).ok, true);
    assert.equal(write(null, row({ quantity: '3' }), { via: 'create', lotChecked: true }).set.quantity, 3);
    assert.equal(S.QTY_MAX, 50);
});

test('create: needs a product and a sample lot that the register confirms', () => {
    assert.equal(write(null, row({ jf_code: ' ' }), { via: 'create', lotChecked: true }).code, 'SAMPLE_PRODUCT_REQUIRED');
    assert.equal(write(null, row({ lot_number: null }), { via: 'create', lotChecked: true }).code, 'SAMPLE_LOT_REQUIRED');
    assert.equal(write(null, row(), { via: 'create', lotChecked: false }).code, 'SAMPLE_LOT_NOT_ELIGIBLE');
    // Not looked up at all is not a pass.
    assert.equal(write(null, row(), { via: 'create', lotChecked: null }).code, 'SAMPLE_LOT_UNCHECKED');
});

test('create: only on a sample PO, with no ASIN stored', () => {
    assert.equal(write(null, row(), { via: 'create', lotChecked: true, po: { id: 7, kind: 'stock', committed: false } }).code, 'SAMPLE_PO_REQUIRED');
    assert.equal(write(null, row(), { via: 'create', lotChecked: true, po: null }).code, 'SAMPLE_PO_REQUIRED');
    assert.equal(write(null, row({ purchase_order_id: 8 }), { via: 'create', lotChecked: true }).code, 'SAMPLE_PO_REQUIRED');
    const r = write(null, row({ asin: 'B0ABCDEFGH' }), { via: 'create', lotChecked: true });
    assert.equal(r.ok, true);
    assert.equal(r.set.asin, null);
});

// ── the validator: what a row may never hold ─────────────────────────────

test('a sample holds no container, AWB, external number or shipment', () => {
    for (const col of ['container_number', 'awb_number', 'external_container_number', 'shipment_id']) {
        const r = write(row(), row({ [col]: col === 'shipment_id' ? 12 : '104. Air Freight' }));
        assert.equal(r.ok, false, col);
        assert.equal(r.code, 'SAMPLE_NO_CONTAINER', col);
    }
});

test('courier and tracking hold 64 characters each', () => {
    assert.equal(write(row(), row({ courier_tracking: 'x'.repeat(64) })).ok, true);
    assert.equal(write(row(), row({ courier_tracking: 'x'.repeat(65) })).code, 'SAMPLE_COURIER');
    assert.equal(write(row(), row({ courier_name: 'x'.repeat(65) })).code, 'SAMPLE_COURIER');
});

test('kind and PO never change', () => {
    assert.equal(write(row(), row({ order_kind: 'stock' })).code, 'SAMPLE_KIND_FIXED');
    assert.equal(write(row(), row({ purchase_order_id: 8 }), { po: { id: 8, kind: 'sample', committed: false } }).code, 'SAMPLE_PO_FIXED');
});

// ── the validator: moving ────────────────────────────────────────────────

test('each state needs everything the earlier ones needed', () => {
    const unsent = write(row({ status: 'SCHEDULED', po_number: null }), row({ status: 'PO_SENT', po_number: null, supplier: '' }));
    assert.equal(unsent.code, 'SAMPLE_MISSING_FIELDS');
    assert.deepEqual(unsent.missing, ['poNumber', 'supplier']);

    const noDate = write(row(), row({ status: 'ON_AIR' }));
    assert.equal(noDate.code, 'SAMPLE_MISSING_FIELDS');
    assert.deepEqual(noDate.missing, ['shippedDate']);

    const arrived = write(row({ status: 'ON_AIR', shipped_date: '2026-11-26', sample_ever_sent: 1 }),
        row({ status: 'ARRIVED_AT_WAREHOUSE', shipped_date: null, sample_ever_sent: 1 }));
    assert.deepEqual(arrived.missing, ['shippedDate', 'arrivedDate']);
});

test('a refused move names what is allowed', () => {
    const r = write(row(), row({ status: 'ARRIVED_AT_WAREHOUSE', shipped_date: '2026-11-26', arrived_date: '2026-11-30' }));
    assert.equal(r.ok, false);
    assert.equal(r.http, 409);
    assert.equal(r.code, 'SAMPLE_STATUS');
    assert.deepEqual(r.allowedNext, ['ON_AIR']);
    const prod = write(row(), row({ status: 'IN_PRODUCTION' }));
    assert.equal(prod.code, 'SAMPLE_STATUS');
});

test('reaching On Air marks the line as sent for good, and says its status changed', () => {
    const r = write(row(), row({ status: 'ON_AIR', shipped_date: '2026-11-26' }));
    assert.equal(r.ok, true);
    assert.equal(r.set.sample_ever_sent, 1);
    assert.equal(r.statusChanged, true);
    assert.equal(r.supersedePending, true);
    // A correction back does not clear the mark.
    const back = write(row({ status: 'ON_AIR', shipped_date: '2026-11-26', sample_ever_sent: 1, courier_name: 'DHL', courier_tracking: '123' }),
        row({ status: 'PO_SENT', shipped_date: '2026-11-26', sample_ever_sent: 1, courier_name: 'DHL', courier_tracking: '123' }), { isAdmin: true });
    assert.equal(back.ok, true);
    assert.equal(Object.prototype.hasOwnProperty.call(back.set, 'sample_ever_sent'), false);
    assert.deepEqual({ d: back.set.shipped_date, n: back.set.courier_name, t: back.set.courier_tracking }, { d: null, n: null, t: null });
});

test('each correction clears exactly what its step had set', () => {
    const arrived = row({ status: 'ARRIVED_AT_WAREHOUSE', shipped_date: '2026-11-26', arrived_date: '2026-11-30', sample_ever_sent: 1 });
    const toAir = write(arrived, { ...arrived, status: 'ON_AIR' }, { isAdmin: true });
    assert.equal(toAir.ok, true);
    assert.equal(toAir.set.arrived_date, null);
    assert.equal(Object.prototype.hasOwnProperty.call(toAir.set, 'shipped_date'), false);

    const received = { ...arrived, status: 'RECEIVED', sample_received_qty: 2 };
    const reopened = write(received, { ...received, status: 'ARRIVED_AT_WAREHOUSE' }, { isAdmin: true });
    assert.equal(reopened.ok, true);
    assert.equal(reopened.set.sample_received_qty, null);
    assert.equal(reopened.clearReceivedStamp, true);
    assert.equal(write(received, { ...received, status: 'ARRIVED_AT_WAREHOUSE' }).code, 'SAMPLE_STATUS');
});

test('a plain status write cannot set RECEIVED; the receive route can', () => {
    const arrived = row({ status: 'ARRIVED_AT_WAREHOUSE', shipped_date: '2026-11-26', arrived_date: '2026-11-30', sample_ever_sent: 1 });
    const viaPut = write(arrived, { ...arrived, status: 'RECEIVED', sample_received_qty: 2 }, { via: 'update', isAdmin: true });
    assert.equal(viaPut.code, 'SAMPLE_STATUS');
    const viaRoute = write(arrived, { ...arrived, status: 'RECEIVED', sample_received_qty: 2 }, { via: 'sample-received' });
    assert.equal(viaRoute.ok, true);
    assert.equal(write(arrived, { ...arrived, status: 'RECEIVED' }, { via: 'sample-received' }).code, 'SAMPLE_MISSING_FIELDS');
});

// ── the validator: identity ──────────────────────────────────────────────

test('product and lot can be corrected until the line is first sent, with a fresh lookup', () => {
    const changed = row({ lot_number: 'SA0032-S01-V01-CN0011' });
    assert.equal(write(row(), changed, { lotChecked: true }).ok, true);
    assert.equal(write(row(), changed, { lotChecked: null }).code, 'SAMPLE_LOT_UNCHECKED');
    assert.equal(write(row(), changed, { lotChecked: false }).code, 'SAMPLE_LOT_NOT_ELIGIBLE');
    // Nothing about the identity moved: no lookup is needed.
    assert.equal(write(row(), row({ quantity: 3 }), { lotChecked: null }).ok, true);
});

test('once sent, product and lot never change, even after a correction back to PO Sent', () => {
    const sent = row({ sample_ever_sent: 1 });
    assert.equal(write(sent, { ...sent, lot_number: 'SA0032-S01-V01-CN0011' }, { lotChecked: true, isAdmin: true }).code, 'SAMPLE_IDENTITY_FIXED');
    assert.equal(write(sent, { ...sent, jf_code: 'JF1017' }, { lotChecked: true, isAdmin: true }).code, 'SAMPLE_IDENTITY_FIXED');
});

test('a change to status, product, lot or quantity supersedes a pending supplier request', () => {
    assert.equal(write(row(), row({ quantity: 3 })).supersedePending, true);
    assert.equal(write(row(), row({ lot_number: 'SA0032-S01-V01-CN0011' }), { lotChecked: true }).supersedePending, true);
    assert.equal(write(row(), row({ jf_code: 'JF1017' }), { lotChecked: true }).supersedePending, true);
    const untouched = write(row(), row({ courier_name: 'DHL' }));
    assert.equal(untouched.ok, true);
    assert.equal(untouched.supersedePending, false);
    assert.equal(untouched.statusChanged, false);
});

// ── the validator: edits that change the bill ────────────────────────────

test('before the PO is committed, quantity and price are free to change', () => {
    assert.equal(write(row(), row({ quantity: 5, unit_price: 4 })).ok, true);
});

test('once the PO is committed, the bill changes only by an admin with a reason', () => {
    const po = { id: 7, kind: 'sample', committed: true };
    for (const change of [{ quantity: 1 }, { unit_price: 9 }]) {
        assert.equal(write(row(), row(change), { po }).code, 'SAMPLE_BILL_LOCKED');
        assert.equal(write(row(), row(change), { po, isAdmin: true }).code, 'SAMPLE_REASON_REQUIRED');
        assert.equal(write(row(), row(change), { po, isAdmin: true, reason: '  ' }).code, 'SAMPLE_REASON_REQUIRED');
        const ok = write(row(), row(change), { po, isAdmin: true, reason: 'supplier sent a new quote' });
        assert.equal(ok.ok, true);
        assert.equal(ok.billChanged, true);
    }
    // Adding a line to a committed PO is the same kind of edit.
    assert.equal(write(null, row(), { via: 'create', lotChecked: true, po }).code, 'SAMPLE_BILL_LOCKED');
    assert.equal(write(null, row(), { via: 'create', lotChecked: true, po, isAdmin: true, reason: 'second sample agreed' }).ok, true);
    // A move that leaves the bill alone is not.
    assert.equal(write(row(), row({ status: 'ON_AIR', shipped_date: '2026-11-26' }), { po }).ok, true);
});

// ── deleting ─────────────────────────────────────────────────────────────

test('a line that was never sent can be deleted; one that was sent cannot', () => {
    assert.equal(S.validateSampleDelete(row(), { po: PO }).ok, true);
    const sent = S.validateSampleDelete(row({ status: 'PO_SENT', sample_ever_sent: 1 }), { po: PO, isAdmin: true, reason: 'x' });
    assert.equal(sent.ok, false);
    assert.equal(sent.code, 'SAMPLE_SENT_NO_DELETE');
});

test('deleting a line of a committed PO is an admin edit with a reason', () => {
    const po = { id: 7, kind: 'sample', committed: true };
    assert.equal(S.validateSampleDelete(row(), { po }).code, 'SAMPLE_BILL_LOCKED');
    assert.equal(S.validateSampleDelete(row(), { po, isAdmin: true }).code, 'SAMPLE_REASON_REQUIRED');
    assert.equal(S.validateSampleDelete(row(), { po, isAdmin: true, reason: 'ordered twice' }).ok, true);
});

// ── the purchase order itself ────────────────────────────────────────────

test('PO: supplier and currency are frozen once it is committed', () => {
    const open = { id: 7, kind: 'sample', committed: false, supplier: 'Acme Co., Ltd.', currency: 'USD', shippingTotal: 0 };
    assert.equal(S.validateSamplePoEdit(open, { supplier: 'Other Ltd' }, {}).ok, true);
    const committed = { ...open, committed: true };
    assert.equal(S.validateSamplePoEdit(committed, { supplier: 'Other Ltd' }, { isAdmin: true, reason: 'x' }).code, 'SAMPLE_PO_FROZEN');
    assert.equal(S.validateSamplePoEdit(committed, { currency: 'GBP' }, { isAdmin: true, reason: 'x' }).code, 'SAMPLE_PO_FROZEN');
    // The same value is not a change.
    assert.equal(S.validateSamplePoEdit(committed, { supplier: 'Acme Co., Ltd.', currency: 'usd' }, {}).ok, true);
});

test('PO: the shipping total follows the bill rule, and the kind never changes', () => {
    const committed = { id: 7, kind: 'sample', committed: true, supplier: 'Acme Co., Ltd.', currency: 'USD', shippingTotal: 0 };
    assert.equal(S.validateSamplePoEdit(committed, { shippingTotal: 25 }, {}).code, 'SAMPLE_BILL_LOCKED');
    assert.equal(S.validateSamplePoEdit(committed, { shippingTotal: 25 }, { isAdmin: true }).code, 'SAMPLE_REASON_REQUIRED');
    assert.equal(S.validateSamplePoEdit(committed, { shippingTotal: 25 }, { isAdmin: true, reason: 'courier charge added' }).ok, true);
    assert.equal(S.validateSamplePoEdit({ ...committed, committed: false }, { shippingTotal: 25 }, {}).ok, true);
    assert.equal(S.validateSamplePoEdit(committed, { kind: 'stock' }, { isAdmin: true, reason: 'x' }).code, 'SAMPLE_KIND_FIXED');
});

test('PO: deleted only with no live line and no recorded money', () => {
    assert.equal(S.validateSamplePoDelete({ liveLines: 0, moneyRecorded: false }).ok, true);
    assert.equal(S.validateSamplePoDelete({ liveLines: 1, moneyRecorded: false }).code, 'SAMPLE_PO_HAS_LINES');
    assert.equal(S.validateSamplePoDelete({ liveLines: 0, moneyRecorded: true }).code, 'SAMPLE_PO_HAS_MONEY');
});

// ── receiving ────────────────────────────────────────────────────────────

test('receive: a whole number from 1 to 50, and a note when it differs from the order', () => {
    assert.deepEqual(S.parseSampleReceive({ quantity: 2 }, { quantity: 2 }), { ok: true, quantity: 2, note: null });
    assert.deepEqual(S.parseSampleReceive({ quantity: '2', note: '  box dented ' }, { quantity: 2 }), { ok: true, quantity: 2, note: 'box dented' });
    for (const bad of [0, 51, 1.5, null, 'x']) {
        assert.equal(S.parseSampleReceive({ quantity: bad }, { quantity: 2 }).code, 'SAMPLE_QUANTITY', String(bad));
    }
    assert.equal(S.parseSampleReceive({ quantity: 1 }, { quantity: 2 }).code, 'SAMPLE_NOTE_REQUIRED');
    assert.equal(S.parseSampleReceive({ quantity: 3, note: ' ' }, { quantity: 2 }).code, 'SAMPLE_NOTE_REQUIRED');
    assert.deepEqual(S.parseSampleReceive({ quantity: 1, note: 'one lost in transit' }, { quantity: 2 }), { ok: true, quantity: 1, note: 'one lost in transit' });
});

// ── a stock line and a sample lot ────────────────────────────────────────

test('a stock line may not take a sample lot, but keeps one it already carries', () => {
    assert.equal(S.stockLotRefusal({ lotIsSample: true, lotChanged: true }).code, 'SAMPLE_LOT_ON_STOCK');
    assert.equal(S.stockLotRefusal({ lotIsSample: true, lotChanged: false }), null);
    assert.equal(S.stockLotRefusal({ lotIsSample: false, lotChanged: true }), null);
});

// ── the arming row ───────────────────────────────────────────────────────

test('the arming row is named for one stage, and only that stage', () => {
    assert.equal(S.armingRowName('test'), 'sample_orders_on:test');
    assert.equal(S.armingRowName('PROD'), 'sample_orders_on:prod');
    assert.equal(S.armingRowName(''), null);
    assert.equal(S.armingRowName(undefined), null);
    assert.equal(S.isArmed(['sample_orders_on:prod'], 'test'), false);
    assert.equal(S.isArmed(['sample_orders_on:prod', 'sample_orders_on:test'], 'test'), true);
    assert.equal(S.isArmed([], 'prod'), false);
    assert.equal(S.isArmed(['sample_orders_on:test'], ''), false);
});
