'use strict';

// Unit tests for src/lib/payment-documents.js — who may delete an uploaded
// payment document (a supplier's invoice, or a proof of payment). No database.
//   node --test tools/test-payment-documents-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/payment-documents');

// A proof of payment filed from Record payment that no payment uses.
const proof = (over = {}) => ({
    id: 70, doc_kind: 'remittance', supplier_payment_id: null, payment_id: null, uploaded_by_email: 'Accounts@Built-Form.co.uk', ...over,
});

test('an admin may delete any upload', () => {
    assert.equal(L.deleteRefusal({ userType: 'admin', userEmail: 'boss@x.com', doc: proof() }), null);
    assert.equal(L.deleteRefusal({ userType: 'admin', userEmail: 'boss@x.com', doc: proof({ doc_kind: 'balance_invoice', payment_id: 5 }) }), null);
    assert.equal(L.deleteRefusal({ userType: 'admin', userEmail: null, doc: proof({ supplier_payment_id: 9 }) }), null);
});

test('whoever uploaded a proof no payment uses may take it back — the email compared whatever its case', () => {
    assert.equal(L.deleteRefusal({ userType: 'accountant', userEmail: 'accounts@built-form.co.uk', doc: proof() }), null);
    assert.equal(L.deleteRefusal({ userType: 'standard', userEmail: ' ACCOUNTS@built-form.co.uk ', doc: proof() }), null);
});

test('someone else\'s proof: admin only', () => {
    const r = L.deleteRefusal({ userType: 'accountant', userEmail: 'other@built-form.co.uk', doc: proof() });
    assert.deepEqual(r, { status: 403, code: 'ADMIN_ONLY', error: 'Admin access required — only an admin, or whoever uploaded a proof that no payment uses, can delete it.' });
    assert.equal(L.deleteRefusal({ userType: 'accountant', userEmail: null, doc: proof() }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ userType: 'accountant', userEmail: 'accounts@built-form.co.uk', doc: proof({ uploaded_by_email: null }) }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ userType: undefined, userEmail: '', doc: proof({ uploaded_by_email: '' }) }).code, 'ADMIN_ONLY');
});

test('a proof a payment uses is not the uploader\'s to delete: it is that payment\'s evidence', () => {
    const who = { userType: 'accountant', userEmail: 'accounts@built-form.co.uk' };
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ supplier_payment_id: 9 }) }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ payment_id: 4 }) }).code, 'ADMIN_ONLY');
});

test('only a proof of payment: a supplier\'s invoice the uploader filed stays admin-only', () => {
    const who = { userType: 'accountant', userEmail: 'accounts@built-form.co.uk' };
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ doc_kind: 'balance_invoice' }) }).code, 'ADMIN_ONLY');
    assert.equal(L.deleteRefusal({ ...who, doc: proof({ doc_kind: 'other' }) }).code, 'ADMIN_ONLY');
});

// ── Where an upload was filed decides how it is read (user, 2026-11-02) ──
// On a shipment: that container's invoice. On the supplier alone through
// "Upload QC invoice": a QC invoice. On the balance of a PO's combined /
// repackaged goods (no container will ever bill them): their balance invoice,
// the PO named. A proof of payment on the supplier stays a proof.
test('the upload place: a shipment, the repackaged goods of a PO, QC units, or the supplier alone', () => {
    assert.equal(L.readPlaceOf({ shipment_id: 307, doc_kind: 'balance_invoice', purchase_order_id: null }), 'shipment');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'balance_invoice', purchase_order_id: 500 }), 'repackaged');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'balance_invoice', purchase_order_id: null }), 'qc');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'balance_invoice' }), 'qc');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'remittance', purchase_order_id: null }), 'supplier');
    assert.equal(L.readPlaceOf({ shipment_id: null, doc_kind: 'remittance', purchase_order_id: 500 }), 'supplier');
});

// What an upload naming a PO must be: the balance invoice for goods on it
// that are marked Combined / Repackaged, with no shipment.
const repack = (over = {}) => ({
    docKind: 'balance_invoice', hasShipment: false, purchaseOrderId: 500,
    po: { id: 500, poNumber: 'PO_00394J', supplier: 'Roosin Medical Co., Ltd.' }, repackagedLines: 1, ...over,
});

test('a balance invoice for a PO\'s combined / repackaged goods is allowed', () => {
    assert.equal(L.repackagedUploadRefusal(repack()), null);
    assert.equal(L.repackagedUploadRefusal(repack({ purchaseOrderId: '500' })), null);
});

test('an upload naming a PO: a balance invoice, no shipment, a live PO with goods marked Combined / Repackaged', () => {
    assert.deepEqual(L.repackagedUploadRefusal(repack({ purchaseOrderId: 'x' })), { status: 400, code: 'BAD_FIELD', error: 'purchaseOrderId must be a purchase order id.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ purchaseOrderId: 0 })), { status: 400, code: 'BAD_FIELD', error: 'purchaseOrderId must be a purchase order id.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ hasShipment: true })), { status: 400, code: 'BAD_FIELD', error: 'An invoice for combined / repackaged goods names a purchase order, not a shipment — send one or the other.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ docKind: 'remittance' })), { status: 400, code: 'BAD_FIELD', error: 'Only a balance invoice is filed against a purchase order\'s combined / repackaged goods.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ po: null })), { status: 404, code: 'NOT_FOUND', error: 'Purchase order 500 not found.' });
    assert.deepEqual(L.repackagedUploadRefusal(repack({ repackagedLines: 0 })), { status: 422, code: 'NOT_REPACKAGED', error: 'PO_00394J has no goods marked Combined / Repackaged — upload its invoice on the container it ships in.' });
});
