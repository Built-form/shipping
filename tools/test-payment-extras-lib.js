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
            forwarderId: null, forwarderIdGiven: false, freightSource: null,
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
        dueDate: null, sourceKind: null, sourceId: null, forwarderId: null, freightSource: null, status: 'open', paidOn: null, settledByPaymentId: null,
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
        forwarderId: null, forwarderIdGiven: false, freightSource: null,
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
        forwarderId: null, forwarderIdGiven: false, freightSource: null,
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

// Forwarder links (2026-11-20, docs/payment-extras-frontend.md): a forwarder
// payment names the forwarder from ShipLine's Forwarders list by id; the name
// stays as the recorded snapshot. A price from a freight booking or a stock
// movement is its own source, in columns of its own (never source_kind).
const fwd = (over = {}) => ({ supplierName: 'DCG', currency: 'GBP', amount: 1850, kind: 'freight', ridesWith: 'shipment', shipmentId: 62, shipmentReference: '301', ...over });
const pick = (v, ...keys) => Object.fromEntries(keys.map(k => [k, v[k]]));

test('forwarderId: a positive id, null (unlink) or left out — only on a forwarder payment', () => {
    assert.deepEqual(pick(L.parseExtraBody(fwd({ forwarderId: 7 })).value, 'forwarderId', 'forwarderIdGiven'), { forwarderId: 7, forwarderIdGiven: true });
    assert.deepEqual(pick(L.parseExtraBody(fwd({ forwarderId: '7' })).value, 'forwarderId', 'forwarderIdGiven'), { forwarderId: 7, forwarderIdGiven: true });
    assert.deepEqual(pick(L.parseExtraBody(fwd({ forwarderId: null })).value, 'forwarderId', 'forwarderIdGiven'), { forwarderId: null, forwarderIdGiven: true });
    assert.deepEqual(pick(L.parseExtraBody(fwd()).value, 'forwarderId', 'forwarderIdGiven'), { forwarderId: null, forwarderIdGiven: false });
    for (const bad of [0, -1, 1.5, 'x', true]) {
        assert.equal(L.parseExtraBody(fwd({ forwarderId: bad })).error, 'forwarderId must be a forwarder id, or null.', String(bad));
    }
    // Another kind of extra: a forwarder id makes no sense; null is harmless.
    assert.equal(L.parseExtraBody(fwd({ ridesWith: 'balance', forwarderId: 7 })).error, 'Only a forwarder payment (ridesWith "shipment") names a forwarder.');
    assert.equal(L.parseExtraBody(fwd({ ridesWith: 'balance', forwarderId: null })).value.forwarderIdGiven, false);
});

test('freightSource: a booking or a stock movement, only on a forwarder payment, never with an invoice source', () => {
    assert.deepEqual(L.parseExtraBody(fwd({ freightSource: { kind: 'booking', id: 5 } })).value.freightSource, { kind: 'booking', id: 5 });
    assert.deepEqual(L.parseExtraBody(fwd({ freightSource: { kind: 'movement', id: '9' } })).value.freightSource, { kind: 'movement', id: 9 });
    assert.equal(L.parseExtraBody(fwd()).value.freightSource, null);
    assert.equal(L.parseExtraBody(fwd({ freightSource: null })).value.freightSource, null);
    const msg = 'freightSource must be { kind: "booking" | "movement", id }.';
    for (const bad of [{ kind: 'quote', id: 5 }, { kind: 'booking' }, { kind: 'booking', id: 0 }, 'booking:5', 5]) {
        assert.equal(L.parseExtraBody(fwd({ freightSource: bad })).error, msg, JSON.stringify(bad));
    }
    assert.equal(L.parseExtraBody(fwd({ freightSource: { kind: 'booking', id: 5 }, sourceKind: 'shipment_document', sourceId: 11 })).error,
        'A price from a freight booking cannot also come from an invoice (sourceKind).');
    assert.equal(L.parseExtraBody(fwd({ ridesWith: 'balance', freightSource: { kind: 'booking', id: 5 } })).error,
        'Only a forwarder payment (ridesWith "shipment") can come from a freight booking.');
});

test('the payee link on create: by id, unlinked on purpose, or by name', () => {
    const next = (o) => ({ forwarderIdGiven: false, forwarderId: null, supplierName: 'DCG', supplierKey: 'dcg', ...o });
    assert.deepEqual(L.decidePayeeLink({ current: null, next: next({ forwarderIdGiven: true, forwarderId: 7 }) }), { action: 'byId', id: 7 });
    assert.deepEqual(L.decidePayeeLink({ current: null, next: next({ forwarderIdGiven: true, forwarderId: null }) }), { action: 'unlink', name: 'DCG' });
    assert.deepEqual(L.decidePayeeLink({ current: null, next: next() }), { action: 'byName', name: 'DCG' });
});

test('the payee link on edit: kept, re-resolved, unlinked or moved — never refreshed after a rename', () => {
    const current = { forwarderId: 7, supplierName: 'DCG Logistics', supplierKey: 'dcg logistics' };
    const next = (o) => ({ forwarderIdGiven: false, forwarderId: null, supplierName: 'DCG Logistics', supplierKey: 'dcg logistics', ...o });
    // Old client, same name: the link and the recorded name stay.
    assert.deepEqual(L.decidePayeeLink({ current, next: next() }), { action: 'keep' });
    // Old client, another name: resolved by that name (or left unlinked).
    assert.deepEqual(L.decidePayeeLink({ current, next: next({ supplierName: 'Savino', supplierKey: 'savino' }) }), { action: 'byName', name: 'Savino' });
    // Unlinked on purpose.
    assert.deepEqual(L.decidePayeeLink({ current, next: next({ forwarderIdGiven: true, forwarderId: null, supplierName: 'Somebody', supplierKey: 'somebody' }) }), { action: 'unlink', name: 'Somebody' });
    // The same forwarder: the recorded name stays, even if the list now spells it otherwise or it was removed.
    assert.deepEqual(L.decidePayeeLink({ current, next: next({ forwarderIdGiven: true, forwarderId: 7, supplierName: 'DCG Global', supplierKey: 'dcg global' }) }), { action: 'keep' });
    // Another forwarder: its name from the list; it must be live.
    assert.deepEqual(L.decidePayeeLink({ current, next: next({ forwarderIdGiven: true, forwarderId: 8 }) }), { action: 'byId', id: 8 });
    // An extra with no link yet, same name: nothing to keep, resolve by name.
    assert.deepEqual(L.decidePayeeLink({ current: { forwarderId: null, supplierName: 'DCG', supplierKey: 'dcg' }, next: next({ supplierName: 'DCG', supplierKey: 'dcg' }) }), { action: 'byName', name: 'DCG' });
});

test('editing with money applied: the forwarder cannot change, even to one spelled the same', () => {
    const current = { amount: 500, currency: 'GBP', supplierKey: 'dcg', forwarderId: 7 };
    const d = L.decideEdit({ current, next: { amount: 500, currency: 'GBP', supplierKey: 'dcg', forwarderId: 8 }, applied: 100 });
    assert.deepEqual(d, { status: 409, code: 'EXTRA_HAS_PAYMENTS', error: 'Money has been applied to it — its forwarder cannot change.' });
    assert.equal(L.decideEdit({ current, next: { amount: 500, currency: 'GBP', supplierKey: 'dcg', forwarderId: 7 }, applied: 100 }), null);
    // Nothing applied: free to change.
    assert.equal(L.decideEdit({ current, next: { amount: 500, currency: 'GBP', supplierKey: 'dcg', forwarderId: 8 }, applied: 0 }), null);
    // Linking an unlinked one under the same name, or unlinking: the payee (its name) is unchanged, so allowed.
    assert.equal(L.decideEdit({ current: { ...current, forwarderId: null }, next: { amount: 500, currency: 'GBP', supplierKey: 'dcg', forwarderId: 8 }, applied: 100 }), null);
    assert.equal(L.decideEdit({ current, next: { amount: 500, currency: 'GBP', supplierKey: 'dcg', forwarderId: null }, applied: 100 }), null);
});

test('a price from a freight booking: its container, forwarder, kind of payment and source are fixed', () => {
    const current = { freightBookingId: 5, freightMovementId: null, ridesWith: 'shipment', shipmentId: 62, shipmentReference: '301', forwarderId: 7, supplierKey: 'dcg' };
    const next = (o) => ({ freightSource: null, ridesWith: 'shipment', shipmentId: 62, shipmentReference: '301', forwarderIdGiven: false, forwarderId: null, supplierKey: 'dcg', ...o });
    const locked = (error) => ({ status: 422, code: 'SOURCE_LOCKED', error });
    assert.equal(L.decideFreightLock({ current, next: next() }), null);
    assert.equal(L.decideFreightLock({ current, next: next({ freightSource: { kind: 'booking', id: 5 } }) }), null);
    assert.equal(L.decideFreightLock({ current, next: next({ forwarderIdGiven: true, forwarderId: 7 }) }), null);
    assert.deepEqual(L.decideFreightLock({ current, next: next({ freightSource: { kind: 'movement', id: 5 } }) }), locked('It came from a freight booking — where it came from cannot change.'));
    assert.deepEqual(L.decideFreightLock({ current, next: next({ shipmentId: 63 }) }), locked('It came from a freight booking — its container cannot change.'));
    assert.deepEqual(L.decideFreightLock({ current, next: next({ shipmentReference: '302' }) }), locked('It came from a freight booking — its container cannot change.'));
    assert.deepEqual(L.decideFreightLock({ current, next: next({ ridesWith: 'balance' }) }), locked('It came from a freight booking — it stays a forwarder payment.'));
    assert.deepEqual(L.decideFreightLock({ current, next: next({ forwarderIdGiven: true, forwarderId: 8 }) }), locked('It came from a freight booking — its forwarder cannot change.'));
    assert.deepEqual(L.decideFreightLock({ current, next: next({ forwarderIdGiven: true, forwarderId: null }) }), locked('It came from a freight booking — its forwarder cannot change.'));
    assert.deepEqual(L.decideFreightLock({ current, next: next({ supplierKey: 'savino' }) }), locked('It came from a freight booking — its forwarder cannot change.'));
    // A stock movement's price says so too.
    assert.deepEqual(L.decideFreightLock({ current: { ...current, freightBookingId: null, freightMovementId: 12 }, next: next({ shipmentId: 63 }) }), locked('It came from a stock movement — its container cannot change.'));
    // An extra not from a booking: no lock.
    assert.equal(L.decideFreightLock({ current: { ...current, freightBookingId: null }, next: next({ shipmentId: 63 }) }), null);
});

test('JSON: the forwarder link and where a freight price came from', () => {
    const base = { id: 9, amount: '1850', status: 'open', kind: 'freight', rides_with: 'shipment' };
    const j = (o) => L.extraRowToJson({ ...base, ...o }, { applied: 0 });
    assert.deepEqual(pick(j({ forwarder_id: 7, freight_booking_id: 5 }), 'forwarderId', 'freightSource'), { forwarderId: 7, freightSource: { kind: 'booking', id: 5 } });
    assert.deepEqual(j({ freight_movement_id: 12 }).freightSource, { kind: 'movement', id: 12 });
    assert.deepEqual(pick(j({}), 'forwarderId', 'freightSource'), { forwarderId: null, freightSource: null });
});
