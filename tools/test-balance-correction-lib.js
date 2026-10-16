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
