'use strict';

// Unit tests for src/lib/local-orders.js: the rules for a LOCAL order line
// (docs/local-orders.md), pure.
// Run: node --test tools/test-local-orders-lib.js

const { test } = require('node:test');
const assert = require('node:assert/strict');

const L = require('../src/lib/local-orders');
const S = require('../src/lib/sample-orders');
const { rowToOrder } = require('../src/lib/order-shape');

const localPo = { id: 11, kind: 'local' };
const localRow = (over = {}) => ({
    id: 30, order_kind: 'local', purchase_order_id: 11, status: 'PO_SENT',
    jf_code: 'JF1016', lot_number: null, exp_date: null, port: null,
    quantity: 120, unit_price: 2.5,
    container_number: null, external_container_number: null, awb_number: null, shipment_id: null,
    ...over,
});
// What POST /orders builds for a new line: no kind yet, the server decides it.
const newLine = (over = {}) => { const r = localRow(over); delete r.id; delete r.order_kind; return r; };

test('local is a kind a PO is made with; an unknown kind is still refused', () => {
    assert.equal(L.KIND_LOCAL, 'local');
    assert.deepEqual(S.KINDS, ['stock', 'sample', 'local']);
    assert.equal(S.normalizeKind('local'), 'local');
    assert.equal(S.normalizeKind(' LOCAL '), 'local');
    assert.equal(S.normalizeKind('import'), null);
    assert.equal(S.normalizeKind(undefined), 'stock');
});

test('isLocal reads a database row and an API order alike, and nothing else', () => {
    assert.equal(L.isLocal({ order_kind: 'local' }), true);
    assert.equal(L.isLocal({ orderKind: 'local' }), true);
    assert.equal(L.isLocal({ order_kind: 'stock' }), false);
    assert.equal(L.isLocal({ order_kind: 'sample' }), false);
    assert.equal(L.isLocal(null), false);
});

test('a line created on a local PO is local: the kind is stamped; no port, lot or expiry is asked for', () => {
    const out = L.validateLocalWrite(null, newLine(), { po: localPo });
    assert.equal(out.ok, true);
    assert.deepEqual(out.set, { order_kind: 'local' });
});

test('an existing local line writes nothing extra', () => {
    const out = L.validateLocalWrite(localRow(), localRow({ quantity: 96 }), { po: localPo });
    assert.equal(out.ok, true);
    assert.deepEqual(out.set, {});
});

test('a local line is made, sent, on the road, delivered and booked in: those stages and no other', () => {
    // On Air/Road: the supplier's van (the user, 2026-11-27), in no container and with no AWB.
    for (const status of ['SCHEDULED', 'PO_SENT', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'PARTIALLY_RECEIVED', 'RECEIVED']) {
        assert.equal(L.validateLocalWrite(localRow(), localRow({ status }), { po: localPo }).ok, true, status);
    }
    for (const status of ['IN_PRODUCTION', 'READY_FOR_QC', 'READY', 'CONSOLIDATED', 'ON_SEA', 'IN_WAREHOUSE', 'DESTROYED', 'REPACKAGED']) {
        const out = L.validateLocalWrite(localRow(), localRow({ status }), { po: localPo });
        assert.equal(out.ok, false, status);
        assert.equal(out.code, 'LOCAL_NOT_SHIPPED');
        assert.equal(out.http, 409);
        assert.deepEqual(out.allowedNext, ['ON_AIR', 'ARRIVED_AT_WAREHOUSE']);
    }
    const born = L.validateLocalWrite(null, newLine({ status: 'IN_PRODUCTION' }), { po: localPo });
    assert.equal(born.code, 'LOCAL_NOT_SHIPPED');
    assert.deepEqual(born.allowedNext, ['SCHEDULED', 'PO_SENT']);
    // Made as a draft or as sent; it arrives later (the user, 2026-11-27).
    for (const status of ['SCHEDULED', 'PO_SENT']) {
        assert.equal(L.validateLocalWrite(null, newLine({ status }), { po: localPo }).ok, true, status);
    }
    for (const status of ['ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED']) {
        assert.equal(L.validateLocalWrite(null, newLine({ status }), { po: localPo }).code, 'LOCAL_NOT_SHIPPED', status);
    }
});

test('a local line travels in nothing: no container, external container, AWB or shipment', () => {
    for (const [field, value] of [['container_number', 'JF-0042'], ['external_container_number', 'MSCU1234567'], ['awb_number', '176-12345675'], ['shipment_id', 5]]) {
        const out = L.validateLocalWrite(localRow(), localRow({ [field]: value }), { po: localPo });
        assert.equal(out.ok, false, field);
        assert.equal(out.code, 'LOCAL_NOT_SHIPPED');
        assert.equal(out.field, field);
    }
    // Blank is nothing: what an edit form sends for an empty box.
    assert.equal(L.validateLocalWrite(localRow(), localRow({ container_number: '  ', awb_number: '' }), { po: localPo }).ok, true);
});

test('a local line stays on its own local PO', () => {
    for (const po of [null, { id: 11, kind: 'stock' }, { id: 11, kind: 'sample' }, { id: 12, kind: 'local' }]) {
        const out = L.validateLocalWrite(localRow(), localRow(), { po });
        assert.equal(out.ok, false, JSON.stringify(po));
        assert.equal(out.code, 'LOCAL_PO_REQUIRED');
    }
    const off = L.validateLocalWrite(localRow(), localRow({ purchase_order_id: null }), { po: null });
    assert.equal(off.code, 'LOCAL_PO_REQUIRED');
});

test('the next stage a local line can take', () => {
    assert.deepEqual(L.allowedNext(null), ['SCHEDULED', 'PO_SENT']);
    assert.deepEqual(L.allowedNext('SCHEDULED'), ['PO_SENT']);
    // The road step is optional.
    assert.deepEqual(L.allowedNext('PO_SENT'), ['ON_AIR', 'ARRIVED_AT_WAREHOUSE']);
    assert.deepEqual(L.allowedNext('ON_AIR'), ['ARRIVED_AT_WAREHOUSE']);
    // Received is reached by booking the stock into Mintsoft.
    assert.deepEqual(L.allowedNext('ARRIVED_AT_WAREHOUSE'), ['RECEIVED']);
    assert.deepEqual(L.allowedNext('RECEIVED'), []);
});

test('booking in: a stock line needs lot and expiry; a local line needs neither', () => {
    assert.deepEqual(L.missingForBookIn({ kind: 'stock', lot: '', expiry: null }), ['lot_number', 'exp_date']);
    assert.deepEqual(L.missingForBookIn({ kind: 'stock', lot: 'L1', expiry: null }), ['exp_date']);
    assert.deepEqual(L.missingForBookIn({ kind: 'stock', lot: 'L1', expiry: '2028-01-31' }), []);
    assert.deepEqual(L.missingForBookIn({ kind: 'local', lot: '', expiry: null }), []);
    assert.deepEqual(L.missingForBookIn({ kind: 'local', lot: 'L1', expiry: null }), []);
});

test('refuseLocal: only for a local row, with the route\'s own words', () => {
    assert.equal(L.refuseLocal({ order_kind: 'stock' }, 'packed into a container'), null);
    assert.equal(L.refuseLocal({ order_kind: 'sample' }, 'packed into a container'), null);
    const out = L.refuseLocal(localRow(), 'packed into a container');
    assert.equal(out.code, 'LOCAL_NOT_SHIPPED');
    assert.equal(out.http, 409);
    assert.match(out.message, /local order cannot be packed into a container/);
});

test('an order row reads its kind as stored: stock, sample or local; anything else is stock', () => {
    const base = { id: 1, status: 'PO_SENT', dates: null };
    assert.equal(rowToOrder({ ...base, order_kind: 'local' }).orderKind, 'local');
    assert.equal(rowToOrder({ ...base, order_kind: 'sample' }).orderKind, 'sample');
    assert.equal(rowToOrder({ ...base, order_kind: 'stock' }).orderKind, 'stock');
    assert.equal(rowToOrder({ ...base }).orderKind, 'stock');
});
