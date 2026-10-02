'use strict';

// Unit tests for src/lib/payment-alerts.js — dismissed "Needs attention" lines.
// Run: node --test tools/test-payment-alerts-lib.js

const { test } = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/payment-alerts');

test('only an admin dismisses or restores', () => {
    assert.equal(L.canDismiss('admin'), true);
    assert.equal(L.canDismiss('standard'), false);
    assert.equal(L.canDismiss('accountant'), false);
    assert.equal(L.canDismiss(undefined), false);
});

test('a dismissal as the page sends it', () => {
    const out = L.parseDismissBody({
        alertKey: '  overpaid|USD|306||sunmed|1a2b3c4d ', kind: 'overpaid', currency: 'usd', amount: '12.345',
        poNumber: ' PO_00306J ', shipmentReference: '307', supplierName: 'Sunmed', detail: 'Paid 12.35 more than the PO value', note: '',
    });
    assert.deepEqual(out, {
        value: {
            alertKey: 'overpaid|USD|306||sunmed|1a2b3c4d', kind: 'overpaid', currency: 'USD', amount: 12.35,
            poNumber: 'PO_00306J', shipmentReference: '307', supplierName: 'Sunmed', detail: 'Paid 12.35 more than the PO value', note: null,
        },
    });
});

test('only the key and the kind are required', () => {
    assert.deepEqual(L.parseDismissBody({ alertKey: 'k', kind: 'unvalued' }).value, {
        alertKey: 'k', kind: 'unvalued', currency: null, amount: null,
        poNumber: null, shipmentReference: null, supplierName: null, detail: null, note: null,
    });
    assert.equal(L.parseDismissBody({ kind: 'unvalued' }).error, 'alertKey is required.');
    assert.equal(L.parseDismissBody({ alertKey: '   ', kind: 'unvalued' }).error, 'alertKey is required.');
    assert.equal(L.parseDismissBody({ alertKey: 'k' }).error, 'kind is required — the kind of alert being dismissed.');
    assert.equal(L.parseDismissBody(null).error, 'alertKey is required.');
});

test('a key over 255 characters, a bad currency and a bad amount are refused', () => {
    assert.equal(L.parseDismissBody({ alertKey: 'x'.repeat(255), kind: 'k' }).error, undefined);
    assert.equal(L.parseDismissBody({ alertKey: 'x'.repeat(256), kind: 'k' }).error, 'alertKey cannot exceed 255 characters.');
    assert.equal(L.parseDismissBody({ alertKey: 'k', kind: 'k', currency: 'DOLLARS' }).error, 'currency must be a three-letter code.');
    assert.equal(L.parseDismissBody({ alertKey: 'k', kind: 'k', currency: 5 }).error, 'currency must be a three-letter code.');
    assert.equal(L.parseDismissBody({ alertKey: 'k', kind: 'k', amount: 'lots' }).error, 'amount must be a number.');
    // A negative figure is a real one (an overpayment, a credit).
    assert.equal(L.parseDismissBody({ alertKey: 'k', kind: 'k', amount: -5 }).value.amount, -5);
});

test('long text is cut to what the columns hold', () => {
    const v = L.parseDismissBody({ alertKey: 'k', kind: 'k'.repeat(60), detail: 'd'.repeat(1500), note: 'n'.repeat(600), supplierName: 's'.repeat(300) }).value;
    assert.equal(v.kind.length, 40);
    assert.equal(v.detail.length, 1000);
    assert.equal(v.note.length, 500);
    assert.equal(v.supplierName.length, 255);
});

test('a row as the API returns it', () => {
    const at = new Date('2026-10-02T14:00:00.000Z');
    assert.deepEqual(L.dismissalRowToJson({
        id: 7, alert_key: 'k', kind: 'overpaid', currency: 'USD', po_number: 'PO_1', shipment_reference: null, supplier_name: 'Sunmed',
        detail: 'said this', amount: '12.50', note: null, dismissed_by_email: 'a@b.co', dismisser_name: 'Ann', dismissed_at: at,
        restored_at: null, restored_by_email: null,
    }), {
        id: 7, alertKey: 'k', kind: 'overpaid', currency: 'USD', poNumber: 'PO_1', shipmentReference: null, supplierName: 'Sunmed',
        detail: 'said this', amount: 12.5, note: null, dismissedByEmail: 'a@b.co', dismissedByName: 'Ann', dismissedAt: '2026-10-02T14:00:00.000Z',
        restoredAt: null, restoredByEmail: null,
    });
    assert.equal(L.dismissalRowToJson({ id: 1, alert_key: 'k', kind: 'k', amount: null, dismissed_by_email: 'a@b.co', dismissed_at: at }).amount, null);
});
