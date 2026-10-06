'use strict';

// Unit tests for src/lib/payment-due-dates.js — custom due dates set by hand
// on ShipLine's Payments flow page (/api/v1/payment-due-dates): the keys they
// hang on, the request body, who may set one, and how a container rename
// re-keys them. No database.
//   node --test tools/test-payment-due-dates-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/payment-due-dates');

test('admin and standard users set due dates; accountants and the rest do not', () => {
    assert.equal(L.canSet('admin'), true);
    assert.equal(L.canSet('standard'), true);
    assert.equal(L.canSet('accountant'), false);
    assert.equal(L.canSet('read_only'), false);
    assert.equal(L.canSet(undefined), false);
});

test('a payment key sets the whole payment; an item key sets one row', () => {
    assert.deepEqual(L.parseTargetKey('deposit:306'), { key: 'deposit:306', scope: 'payment' });
    assert.deepEqual(L.parseTargetKey('balance:USD:330|suzhou sunmed co.,ltd.'), { key: 'balance:USD:330|suzhou sunmed co.,ltd.', scope: 'payment' });
    assert.deepEqual(L.parseTargetKey('item:derived:bal:368:330'), { key: 'item:derived:bal:368:330', scope: 'item' });
    assert.deepEqual(L.parseTargetKey('  item:stated:17:none '), { key: 'item:stated:17:none', scope: 'item' });
    // A QC invoice is paid when the user chooses: no due date to set.
    assert.ok(L.parseTargetKey('qc:412').error);
    for (const bad of ['', 'item:', 'item: ', 'deposit:x', 'balance:USD:330', `item:${'x'.repeat(260)}`, 42, null]) {
        assert.ok(L.parseTargetKey(bad).error, `expected an error for ${JSON.stringify(bad)}`);
    }
});

test('the body: key, a YYYY-MM-DD date, an optional note of up to 500 characters', () => {
    assert.deepEqual(L.parseBody({ key: 'deposit:306', dueDate: '2026-11-20' }).value, { key: 'deposit:306', scope: 'payment', dueDate: '2026-11-20', note: null });
    assert.deepEqual(L.parseBody({ key: 'item:derived:bal:368:330', dueDate: '2026-11-20', note: '  agreed with supplier ' }).value,
        { key: 'item:derived:bal:368:330', scope: 'item', dueDate: '2026-11-20', note: 'agreed with supplier' });
    assert.equal(L.parseBody({ key: 'deposit:306', dueDate: '2026-11-20', note: '' }).value.note, null);
    assert.equal(L.parseBody({ key: 'deposit:306', dueDate: '2026-11-20', note: 'x'.repeat(500) }).value.note.length, 500);
    for (const bad of [
        {}, { key: 'deposit:306' }, { key: 'deposit:306', dueDate: '20/11/2026' }, { key: 'deposit:306', dueDate: '2026-13-01' },
        { key: 'deposit:306', dueDate: '2026-02-30' }, { key: 'deposit:306', dueDate: 20261120 }, { key: 'deposit:306', dueDate: '2026-11-20', note: 'x'.repeat(501) },
        { key: 'deposit:306', dueDate: '2026-11-20', note: 7 }, { key: 'qc:412', dueDate: '2026-11-20' },
    ]) {
        assert.ok(L.parseBody(bad).error, `expected an error for ${JSON.stringify(bad)}`);
    }
    assert.ok(L.parseBody(null).error);
});

test('a row as the page reads it', () => {
    const row = {
        id: 7, target_key: 'balance:USD:330|sunmed', due_date: new Date('2026-11-20T00:00:00Z'), note: null,
        set_by_email: 'ops@example.com', setter_name: 'Ops', created_at: new Date('2026-10-06T09:00:00Z'), updated_at: new Date('2026-10-06T09:30:00Z'),
    };
    assert.deepEqual(L.rowToJson(row), {
        id: 7, key: 'balance:USD:330|sunmed', scope: 'payment', dueDate: '2026-11-20', note: null,
        setByEmail: 'ops@example.com', setByName: 'Ops', setAt: '2026-10-06T09:30:00.000Z',
    });
    assert.equal(L.rowToJson({ ...row, due_date: '2026-11-20', setter_name: null, target_key: 'item:derived:bal:1:330' }).setByName, null);
    assert.equal(L.rowToJson({ ...row, target_key: 'item:derived:bal:1:330' }).scope, 'item');
});

test('rename 330 -> 331: balance keys and item keys ending in the container follow; others are left alone', () => {
    assert.equal(L.rekeyTargetKey('balance:USD:330|sunmed', '330', '331'), 'balance:USD:331|sunmed');
    assert.equal(L.rekeyTargetKey('item:derived:bal:368:330', '330', '331'), 'item:derived:bal:368:331');
    assert.equal(L.rekeyTargetKey('item:stated:17:charges:330', '330', '331'), 'item:stated:17:charges:331');
    // The orders store the number as typed; the match ignores case, the new number is kept as given.
    assert.equal(L.rekeyTargetKey('item:derived:bal:368:104. Air Freight', '104. AIR FREIGHT', '105. Air Freight'), 'item:derived:bal:368:105. Air Freight');
    for (const untouched of ['balance:USD:3300|sunmed', 'item:derived:bal:368:3300', 'item:derived:bal:330:none', 'deposit:330', 'item:derived:bal:368:none@open:330', 'balance:USD:-|sunmed']) {
        assert.equal(L.rekeyTargetKey(untouched, '330', '331'), null, untouched);
    }
});
