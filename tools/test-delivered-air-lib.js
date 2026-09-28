'use strict';

// Unit tests for src/lib/delivered-air.js — the Record payment search for
// delivered air freight, and the double-payment check. No database.
//   node --test tools/test-delivered-air-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { groupDeliveredAir, attachBalances, findPaidClash } = require('../src/lib/delivered-air');

const row = (extra) => ({
    shipment_id: 44, reference: '104. Air Freight', stage: 'CLOSED', shipment_arrived_at: null,
    purchase_order_id: 1, po_number: 'FOREU-1', po_supplier: 'Foreu Corporation', currency: 'USD',
    jf_code: 'JF0887', quantity: 100, unit_price: '5.0000', status: 'RECEIVED',
    arrived_date: '2026-07-10', delivery_date: null, awb_number: '123-456',
    ...extra,
});

const ROWS = [
    row({}),
    row({ jf_code: 'JF1129', quantity: 50, unit_price: null, arrived_date: '2026-07-12', awb_number: '123-456' }),
    // another supplier on the same shipment
    row({ purchase_order_id: 2, po_number: 'OTHER-1', po_supplier: 'Other Co', quantity: 10 }),
    // booked, still in the air: not delivered
    row({ shipment_id: 45, reference: '105. Air Freight', stage: 'BOOKED', purchase_order_id: 3, po_number: 'FOREU-3', status: 'ON_AIR', arrived_date: null }),
    // stage lags, but every line of the supplier is received: delivered (datetime delivery date)
    row({ shipment_id: 46, reference: '106. Air Freight', stage: 'BOOKED', purchase_order_id: 4, po_number: 'FOREU-4', quantity: 20, unit_price: '2.5000', arrived_date: null, delivery_date: new Date(2026, 7, 3, 10, 0, 0), awb_number: null }),
    // another currency
    row({ shipment_id: 47, reference: '107. Air Freight', purchase_order_id: 5, po_number: 'FOREU-5', currency: 'EUR' }),
];

const isForeu = (name) => /foreu/i.test(name || '');

test('groups delivered air shipments by PO for the supplier, newest delivery first', () => {
    const out = groupDeliveredAir(ROWS, { isSupplier: isForeu, currency: 'USD' });
    assert.deepEqual(out.map(s => s.shipmentId), [46, 44]);
    assert.deepEqual(out[1], {
        shipmentId: 44, reference: '104. Air Freight', stage: 'CLOSED', deliveredOn: '2026-07-10', awbNumbers: ['123-456'],
        purchaseOrders: [{
            purchaseOrderId: 1, poNumber: 'FOREU-1', supplier: 'Foreu Corporation', currency: 'USD',
            lines: 2, units: 150, value: 500, unpricedLines: 1, jfCodes: ['JF0887', 'JF1129'],
        }],
    });
    assert.equal(out[0].deliveredOn, '2026-08-03');
    assert.equal(out[0].purchaseOrders[0].value, 50);
});

test('no currency filter: every currency comes back', () => {
    const out = groupDeliveredAir(ROWS, { isSupplier: isForeu, currency: null });
    assert.deepEqual(out.map(s => s.shipmentId).sort(), [44, 46, 47]);
});

test('a shipment that is only another supplier\'s is left out', () => {
    const out = groupDeliveredAir(ROWS, { isSupplier: n => n === 'Other Co', currency: 'USD' });
    assert.deepEqual(out.map(s => s.shipmentId), [44]);
    assert.deepEqual(out[0].purchaseOrders.map(p => p.poNumber), ['OTHER-1']);
});

test('shipment arrived or closed counts as delivered even when a line status lags', () => {
    const rows = [row({ stage: 'ARRIVED', status: 'ON_AIR' })];
    assert.deepEqual(groupDeliveredAir(rows, { isSupplier: isForeu, currency: 'USD' }).map(s => s.shipmentId), [44]);
    const booked = [row({ stage: 'BOOKED', status: 'ON_AIR' })];
    assert.deepEqual(groupDeliveredAir(booked, { isSupplier: isForeu, currency: 'USD' }), []);
});

test('an open and a paid balance on the same PO: paid is what shows', () => {
    const shipments = groupDeliveredAir([row({})], { isSupplier: isForeu, currency: 'USD' });
    attachBalances(shipments, [
        { id: 20, shipment_id: 44, amount: '300.00', status: 'paid', paid_on: '2026-08-20', purchase_order_id: 1 },
        { id: 21, shipment_id: 44, amount: '200.00', status: 'pending', paid_on: null, purchase_order_id: 1 },
    ]);
    assert.equal(shipments[0].purchaseOrders[0].balance.id, 20);
});

test('no date on the lines: the shipment\'s arrival, else null', () => {
    const rows = [row({ arrived_date: null, shipment_arrived_at: new Date(2026, 8, 2, 9, 0, 0) })];
    assert.equal(groupDeliveredAir(rows, { isSupplier: isForeu, currency: 'USD' })[0].deliveredOn, '2026-09-02');
    const none = [row({ arrived_date: null })];
    assert.equal(groupDeliveredAir(none, { isSupplier: isForeu, currency: 'USD' })[0].deliveredOn, null);
});

test('balances: a paid one allocated to a PO marks that PO only; one with no split covers the box', () => {
    const shipments = groupDeliveredAir([
        ...ROWS,
        row({ purchase_order_id: 6, po_number: 'FOREU-6', quantity: 5 }),
    ], { isSupplier: isForeu, currency: 'USD' });
    attachBalances(shipments, [
        { id: 9, shipment_id: 44, amount: '500.00', status: 'paid', paid_on: '2026-08-20', purchase_order_id: 1 },
        { id: 11, shipment_id: 46, amount: '50.00', status: 'pending', paid_on: null, purchase_order_id: null },
    ]);
    const s44 = shipments.find(s => s.shipmentId === 44);
    assert.deepEqual(s44.purchaseOrders.find(p => p.purchaseOrderId === 1).balance, { id: 9, status: 'paid', paidOn: '2026-08-20', amount: 500 });
    assert.equal(s44.purchaseOrders.find(p => p.purchaseOrderId === 6).balance, null);
    const s46 = shipments.find(s => s.shipmentId === 46);
    assert.deepEqual(s46.purchaseOrders[0].balance, { id: 11, status: 'pending', paidOn: null, amount: 50 });
});

test('paid clash: same PO, or a paid balance with no split, blocks; another PO does not', () => {
    const paid = [{ id: 9, paid_on: '2026-08-20', purchase_order_id: 1 }];
    assert.equal(findPaidClash(paid, [1]).id, 9);
    assert.equal(findPaidClash(paid, [2]), null);
    assert.equal(findPaidClash([{ id: 12, paid_on: '2026-08-21', purchase_order_id: null }], [2]).id, 12);
    assert.equal(findPaidClash([], [1]), null);
});
