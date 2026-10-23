'use strict';

// Unit tests for src/lib/balance-correction.js — a balance record stored at its
// invoice's goods total, brought down to the balance while it is being paid
// (Kingphar 124. Air Freight: invoice 9,305 at 30/70, balance 6,513.50).
// Expected figures by hand. No database.
//   node --test tools/test-balance-correction-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { correctBalance } = require('../src/lib/balance-correction');

const one = [{ purchaseOrderId: 336, poRef: '', amount: 9305, source: 'extracted' }];

test('a goods-total record comes down to the balance, its split with it', () => {
    const r = correctBalance({ amount: 9305, applied: 0, balanceAmount: 6513.5, allocations: one });
    assert.equal(r.amount, 6513.5);
    assert.deepEqual(r.allocations, [{ purchaseOrderId: 336, poRef: '', amount: 6513.5, source: 'extracted' }]);
});

test('two POs: each keeps its share, and the split still adds up to the cent', () => {
    // 10,000 split 3,333.33 / 6,666.67, down to 7,000 (70 %): 2,333.33 / 4,666.67.
    const r = correctBalance({
        amount: 10000, applied: 0, balanceAmount: 7000,
        allocations: [{ purchaseOrderId: 1, amount: 3333.33 }, { purchaseOrderId: 2, amount: 6666.67 }],
    });
    assert.equal(r.amount, 7000);
    assert.deepEqual(r.allocations.map(a => a.amount), [2333.33, 4666.67]);
});

test('a record split only in part stays split in part', () => {
    // 6,000 of 10,000 named; at 70 % that is 4,200 of 7,000.
    const r = correctBalance({ amount: 10000, applied: 0, balanceAmount: 7000, allocations: [{ purchaseOrderId: 1, amount: 6000 }] });
    assert.deepEqual(r.allocations.map(a => a.amount), [4200]);
});

test('no split on the record: none made', () => {
    const r = correctBalance({ amount: 9305, applied: 0, balanceAmount: 6513.5, allocations: [] });
    assert.equal(r.amount, 6513.5);
    assert.deepEqual(r.allocations, []);
});

test('nothing to do: no figure given, the same figure, or a higher one — a record is never raised', () => {
    assert.equal(correctBalance({ amount: 9305, applied: 0, balanceAmount: null, allocations: one }), null);
    assert.equal(correctBalance({ amount: 9305, applied: 0, balanceAmount: 9305, allocations: one }), null);
    assert.equal(correctBalance({ amount: 9305, applied: 0, balanceAmount: 9400, allocations: one }), null);
});

test('part paid before: down to the balance, never below what was already applied', () => {
    assert.equal(correctBalance({ amount: 9305, applied: 1000, balanceAmount: 6513.5, allocations: one }).amount, 6513.5);
    const r = correctBalance({ amount: 9305, applied: 7000, balanceAmount: 6513.5, allocations: one });
    assert.equal(r.code, 'BALANCE_BELOW_APPLIED');
    assert.equal(r.amount, undefined);
});

// Sunmed 123. Air Freight (user, 2026-10-19: NOT_FULLY_ALLOCATED, 98.72 unallocated):
// the record is the invoice TOTAL 2,477.68 = the goods split 780 / 345 / 1,249.68
// (2,374.68) + packaging 103 tied to no PO — the packaging is an extra of its own.
// Brought down to the goods with `keepSplit`, it drops what no PO carries and keeps
// each PO's share, so it is fully split and settles. Scaling it left 98.72 unsplit.
const sunmed = [{ purchaseOrderId: 272, amount: 780 }, { purchaseOrderId: 297, amount: 345 }, { purchaseOrderId: 324, amount: 1249.68 }];

test('keepSplit: what no PO carries goes first; each PO keeps its share', () => {
    const r = correctBalance({ amount: 2477.68, applied: 0, balanceAmount: 2374.68, allocations: sunmed, keepSplit: true });
    assert.equal(r.amount, 2374.68);
    assert.deepEqual(r.allocations.map(a => a.amount), [780, 345, 1249.68]);
});

test('keepSplit: the split itself above the balance still comes down, by its shares', () => {
    // 125. Air Freight: 4,219.04 all named to one PO, packaging 84 inside it; down to 4,135.04.
    const r = correctBalance({ amount: 4219.04, applied: 0, balanceAmount: 4135.04, allocations: [{ purchaseOrderId: 333, amount: 4219.04 }], keepSplit: true });
    assert.deepEqual(r.allocations.map(a => a.amount), [4135.04]);
});

test('without keepSplit (a goods-total record): scaled, as before', () => {
    const r = correctBalance({ amount: 2477.68, applied: 0, balanceAmount: 2374.68, allocations: sunmed });
    assert.notDeepEqual(r.allocations.map(a => a.amount), [780, 345, 1249.68]);
});

test('keepSplit: a split below the new figure is never raised — what no PO carries just shrinks', () => {
    // 2,500 with 2,300 named and 200 on no PO, brought to 2,400: the named 2,300 stays as it is.
    const r = correctBalance({ amount: 2500, applied: 0, balanceAmount: 2400, allocations: [{ purchaseOrderId: 1, amount: 2300 }], keepSplit: true });
    assert.equal(r.amount, 2400);
    assert.deepEqual(r.allocations.map(a => a.amount), [2300]);
});
