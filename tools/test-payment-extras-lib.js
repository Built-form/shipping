'use strict';

// Unit tests for src/lib/payment-extras.js — extra charges and credits on
// payments: what a request may say, how money applied to one settles it, and
// when editing, deleting or un-marking one is refused. No database.
//   node --test tools/test-payment-extras-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/payment-extras');

const deposit = (over = {}) => ({
    supplierName: ' Suzhou Sunmed Co.,Ltd. ', currency: 'usd', amount: '500', kind: 'mould', description: ' Mould for JF1375 ',
    ridesWith: 'deposit', purchaseOrderId: 395, ...over,
});

test('who may add, change and mark extras paid: admin, standard, accountant', () => {
    assert.deepEqual(L.EDITOR_ROLES, ['admin', 'standard', 'accountant']);
    assert.equal(L.canEdit('accountant'), true);
    assert.equal(L.canEdit('standard'), true);
    assert.equal(L.canEdit('warehouse'), false);
    assert.equal(L.canEdit(undefined), false);
});

test('a charge riding with a PO deposit: cleaned up, amount to the cent', () => {
    assert.deepEqual(L.parseExtraBody(deposit({ amount: '500.004', note: '  from the quote ' })), {
        value: {
            supplierName: 'Suzhou Sunmed Co.,Ltd.', currency: 'USD', amount: 500, kind: 'mould', description: 'Mould for JF1375',
            ridesWith: 'deposit', purchaseOrderId: 395, shipmentId: null, shipmentReference: null, dueDate: null,
            sourceKind: null, sourceId: null, note: 'from the quote',
        },
    });
});

test('a credit riding with a container balance, PO optional, from an uploaded invoice', () => {
    const v = L.parseExtraBody({
        supplierName: 'Sunmed', currency: 'USD', amount: -120.5, kind: 'discount', ridesWith: 'balance',
        shipmentId: 62, shipmentReference: ' 301 ', sourceKind: 'shipment_document', sourceId: 11, dueDate: '2026-10-05',
    }).value;
    assert.equal(v.amount, -120.5);
    assert.equal(v.purchaseOrderId, null);
    assert.equal(v.shipmentId, 62);
    assert.equal(v.shipmentReference, '301');
    assert.equal(v.dueDate, '2026-10-05');
    assert.deepEqual([v.sourceKind, v.sourceId], ['shipment_document', 11]);
    // A box named by its reference alone (no shipment row) is enough.
    assert.equal(L.parseExtraBody({ supplierName: 'Sunmed', currency: 'USD', amount: 50, kind: 'handling', ridesWith: 'balance', shipmentReference: '104. Air Freight' }).value.shipmentReference, '104. Air Freight');
});

test('what a request may not say', () => {
    const bad = [
        [deposit({ supplierName: '  ' }), /supplierName/],
        [deposit({ currency: 'US' }), /currency/],
        [deposit({ amount: 0 }), /amount/],
        [deposit({ amount: 'lots' }), /amount/],
        [deposit({ kind: 'bribe' }), /kind/],
        [deposit({ ridesWith: 'whenever' }), /ridesWith/],
        [deposit({ purchaseOrderId: null }), /deposit.*purchase order/i],
        [deposit({ shipmentId: 62 }), /deposit/i],
        [deposit({ ridesWith: 'balance', purchaseOrderId: 395 }), /balance.*container/i],
        [deposit({ dueDate: '5 Oct' }), /dueDate/],
        [deposit({ sourceKind: 'shipment_document' }), /source/],
        [deposit({ sourceKind: 'email', sourceId: 3 }), /source/],
        [deposit({ description: 'x'.repeat(256) }), /description/],
        // A discount is money off: it cannot be a charge.
        [deposit({ kind: 'discount', amount: 50 }), /discount/i],
    ];
    for (const [body, re] of bad) assert.match(L.parseExtraBody(body).error || '', re, JSON.stringify(body));
});

test('a line on an extra: same sign as the extra, never more than is left', () => {
    assert.equal(L.checkLine({ lineAmount: 200, extraAmount: 500, appliedBefore: 300 }), null);
    assert.equal(L.checkLine({ lineAmount: -120.5, extraAmount: -120.5, appliedBefore: 0 }), null);
    assert.equal(L.checkLine({ lineAmount: 200.004, extraAmount: 500, appliedBefore: 300 }), null); // within a cent
    const over = L.checkLine({ lineAmount: 250, extraAmount: 500, appliedBefore: 300 });
    assert.equal(over.code, 'OVER_APPLIED');
    assert.equal(over.remaining, 200);
    assert.equal(L.checkLine({ lineAmount: -130, extraAmount: -120.5, appliedBefore: 0 }).code, 'OVER_APPLIED');
    assert.equal(L.checkLine({ lineAmount: 120.5, extraAmount: -120.5, appliedBefore: 0 }).code, 'SIGN_MISMATCH');
    assert.equal(L.checkLine({ lineAmount: -10, extraAmount: 500, appliedBefore: 0 }).code, 'SIGN_MISMATCH');
});

test('settled: what was applied covers it within a bank charge (max of 1 and 1 %), either sign', () => {
    assert.equal(L.settles(500, 500), true);
    assert.equal(L.settles(495, 500), true);      // 1 % of 500 = 5
    assert.equal(L.settles(494.99, 500), false);
    assert.equal(L.settles(49.01, 50), true);     // floor of 1
    assert.equal(L.settles(48.99, 50), false);
    assert.equal(L.settles(-120.5, -120.5), true);
    assert.equal(L.settles(-100, -120.5), false);
    assert.equal(L.settles(120.5, -120.5), false); // wrong way round
    assert.equal(L.settles(0, 0.5), false);        // nothing applied never settles
});

test('editing one with money applied: no new currency or supplier, no flip, not below what was applied', () => {
    const current = { amount: 500, currency: 'USD', supplierKey: 'sunmed' };
    assert.equal(L.decideEdit({ current, next: { amount: 900, currency: 'EUR', supplierKey: 'x' }, applied: 0 }), null);
    assert.equal(L.decideEdit({ current, next: { amount: 450, currency: 'USD', supplierKey: 'sunmed' }, applied: 300 }), null);
    for (const next of [
        { amount: 500, currency: 'EUR', supplierKey: 'sunmed' },
        { amount: 500, currency: 'USD', supplierKey: 'foreu' },
        { amount: -500, currency: 'USD', supplierKey: 'sunmed' },
        { amount: 299, currency: 'USD', supplierKey: 'sunmed' },
    ]) {
        const d = L.decideEdit({ current, next, applied: 300 });
        assert.equal(d && d.status, 409, JSON.stringify(next));
        assert.equal(d.code, 'EXTRA_HAS_PAYMENTS');
    }
});

test('deleting: only while no money is applied to it', () => {
    assert.equal(L.decideDelete({ applied: 0 }), null);
    assert.equal(L.decideDelete({ applied: 0.004 }), null);
    assert.equal(L.decideDelete({ applied: -20 }).code, 'EXTRA_HAS_PAYMENTS');
});

test('marking paid by hand, and back: a transfer that paid it is undone by editing the transfer', () => {
    assert.deepEqual(L.decideStatus({ status: 'open', settledByPaymentId: null, want: 'paid' }), { action: 'set' });
    assert.deepEqual(L.decideStatus({ status: 'paid', settledByPaymentId: null, want: 'open' }), { action: 'set' });
    assert.deepEqual(L.decideStatus({ status: 'paid', settledByPaymentId: null, want: 'paid' }), { action: 'none' });
    assert.equal(L.decideStatus({ status: 'paid', settledByPaymentId: 77, want: 'open' }).code, 'SETTLED_BY_TRANSFER');
    assert.equal(L.decideStatus({ status: 'open', settledByPaymentId: null, want: 'arranged' }).code, 'BAD_FIELD');
});

test('labels and JSON', () => {
    assert.equal(L.lineLabel({ po_number: 'PO_00395J', shipment_reference: null, kind: 'mould' }), 'PO_00395J mould cost');
    assert.equal(L.lineLabel({ po_number: null, shipment_reference: '301', kind: 'handling' }), '301 handling fee');
    assert.equal(L.lineLabel({ po_number: null, shipment_reference: null, kind: 'bank_charge' }), 'Bank charge');
    const at = new Date('2026-09-29T09:15:00Z');
    assert.deepEqual(L.extraRowToJson({
        id: 7, supplier_name: 'Sunmed', supplier_key: 'sunmed', currency: 'USD', amount: '500.00', kind: 'mould', description: null,
        rides_with: 'deposit', purchase_order_id: 395, po_number: 'PO_00395J', shipment_id: null, shipment_reference: null,
        due_date: null, source_kind: null, source_id: null, status: 'open', paid_on: null, settled_by_payment_id: null, note: null,
        created_by_email: 'ann@x.com', created_at: at, updated_by_email: null, updated_at: at,
    }, { applied: 200 }), {
        id: 7, supplierName: 'Sunmed', supplierKey: 'sunmed', currency: 'USD', amount: 500, kind: 'mould', label: 'PO_00395J mould cost',
        description: null, ridesWith: 'deposit', purchaseOrderId: 395, poNumber: 'PO_00395J', shipmentId: null, shipmentReference: null,
        dueDate: null, sourceKind: null, sourceId: null, status: 'open', paidOn: null, settledByPaymentId: null,
        applied: 200, remaining: 300, note: null, createdByEmail: 'ann@x.com', createdAt: '2026-09-29T09:15:00.000Z',
        updatedByEmail: null, updatedAt: '2026-09-29T09:15:00.000Z', settlements: [],
    });
    // Paid: nothing left, whatever was applied.
    assert.equal(L.extraRowToJson({ id: 8, amount: '-50', status: 'paid', kind: 'discount' }, { applied: 0 }).remaining, 0);
});

// Forwarder / freight payments (user, 2026-09-30): a cost of the shipment
// itself, paid to its own payee (the forwarder) — rides with the container,
// names no PO, in its own currency.
test('a shipment cost: payee, container, no PO; freight/customs/duty/delivery kinds', () => {
    const v = L.parseExtraBody({
        supplierName: ' DCG Logistics ', currency: 'gbp', amount: '1850', kind: 'freight', ridesWith: 'shipment',
        shipmentId: 62, shipmentReference: ' 301 ', description: ' INV-5521 ', dueDate: '2026-10-10',
    }).value;
    assert.deepEqual(v, {
        supplierName: 'DCG Logistics', currency: 'GBP', amount: 1850, kind: 'freight', description: 'INV-5521',
        ridesWith: 'shipment', purchaseOrderId: null, shipmentId: 62, shipmentReference: '301', dueDate: '2026-10-10',
        sourceKind: null, sourceId: null, note: null,
    });
    for (const kind of ['customs', 'duty', 'delivery']) assert.equal(L.parseExtraBody({ supplierName: 'DCG', currency: 'GBP', amount: 10, kind, ridesWith: 'shipment', shipmentReference: '301' }).value.kind, kind);
    assert.ok(L.RIDES_WITH.includes('shipment')); // the full list is pinned by the credit-note test below
    assert.equal(L.lineLabel({ kind: 'customs', shipment_reference: '301' }), '301 customs clearance');
});

test('a shipment cost may not name a PO, and must name its container', () => {
    const cost = (over = {}) => ({ supplierName: 'DCG', currency: 'GBP', amount: 10, kind: 'freight', ridesWith: 'shipment', shipmentReference: '301', ...over });
    assert.match(L.parseExtraBody(cost({ purchaseOrderId: 395 })).error || '', /shipment.*purchase order/i);
    assert.match(L.parseExtraBody(cost({ shipmentReference: null })).error || '', /shipment.*container/i);
});

// Supplier credit notes (user, 2026-10-01): a credit held with a supplier,
// tied to no PO and no container — "on account" until a transfer uses it.
// It may cover what a transfer pays in full: then no money is sent.
test('a supplier credit note: on account, negative, no PO, no container', () => {
    const v = L.parseExtraBody({ supplierName: ' Sunmed ', currency: 'usd', amount: -4500, kind: 'credit_note', ridesWith: 'account', description: ' CN-2026-014 ' }).value;
    assert.deepEqual(v, {
        supplierName: 'Sunmed', currency: 'USD', amount: -4500, kind: 'credit_note', description: 'CN-2026-014',
        ridesWith: 'account', purchaseOrderId: null, shipmentId: null, shipmentReference: null, dueDate: null,
        sourceKind: null, sourceId: null, note: null,
    });
    assert.deepEqual(L.RIDES_WITH, ['deposit', 'balance', 'shipment', 'account']);
    assert.equal(L.lineLabel({ kind: 'credit_note', rides_with: 'account' }), 'Credit note');
});
test('a credit note is a credit, and belongs to the supplier alone', () => {
    const note = (over = {}) => ({ supplierName: 'Sunmed', currency: 'USD', amount: -100, kind: 'credit_note', ridesWith: 'account', ...over });
    assert.match(L.parseExtraBody(note({ amount: 100 })).error || '', /credit note.*negative/i);
    assert.match(L.parseExtraBody(note({ purchaseOrderId: 395 })).error || '', /credit note.*purchase order|container/i);
    assert.match(L.parseExtraBody(note({ shipmentReference: '301' })).error || '', /credit note.*purchase order|container/i);
});
test('credits in a transfer: against something paid, never more than it, and they may cover it all with no money sent', () => {
    const lines = (...amounts) => amounts.map(amount => ({ amount }));
    // Money sent, credits take part of it off: fine.
    assert.equal(L.checkCreditUse({ lines: lines(5000, -500), amount: 4500 }), null);
    // Credits cover everything ticked, nothing sent: settled by credit.
    assert.equal(L.checkCreditUse({ lines: lines(4000, -4000), amount: 0 }), null);
    assert.equal(L.checkCreditUse({ lines: lines(300, 700, -450, -550), amount: 0 }), null);
    // Nothing sent and no credit: not a payment.
    assert.equal(L.checkCreditUse({ lines: lines(300), amount: 0 }).code, 'NOTHING_SENT');
    // A credit with nothing paid.
    assert.equal(L.checkCreditUse({ lines: lines(-120), amount: 1 }).code, 'CREDIT_ALONE');
    assert.equal(L.checkCreditUse({ lines: lines(-120), amount: 0 }).code, 'CREDIT_ALONE');
    // More credit than what is paid.
    assert.equal(L.checkCreditUse({ lines: lines(300, -450), amount: 0 }).code, 'CREDIT_EXCEEDS');
    // Money sent although the credits already cover everything: refused as before.
    assert.equal(L.checkCreditUse({ lines: lines(300, -300), amount: 50 }).code, 'CREDIT_ALONE');
    // No credits: nothing to say.
    assert.equal(L.checkCreditUse({ lines: lines(300, 200), amount: 500 }), null);
});
