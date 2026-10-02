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
