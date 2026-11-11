'use strict';

// Unit tests for src/lib/payment-terms.js — the server's port of ShipLine's
// payment-terms parser (Shipline/src/components/payments/paymentsFlowMath.ts),
// with the vectors documented at the top of that file — and for
// loadDepositGate (src/services/deposit-gate.js) against a fake connection.
// No database.
//   node --test tools/test-payment-terms-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const T = require('../src/lib/payment-terms');
const { loadDepositGate } = require('../src/services/deposit-gate');

// input → [depositPct, trigger, offset, confidence]
const VECTORS = [
    ['30% deposit on order confirmation, 70% balance before shipment', 30, 'before_dispatch', 0, 'parsed'],
    ['30% T/T in advance, 70% B/L copy.', 30, 'bl', 0, 'parsed'],
    ['50% deposit, 50% before shipment', 50, 'before_dispatch', 0, 'parsed'],
    ['100% before dispatch', 0, 'before_dispatch', 0, 'parsed'],
    ['30% deposit, balance 30 days after B/L', 30, 'bl', 30, 'parsed'],
    ['net 30', 0, 'bl', 30, 'partial'],
    ['T/T 60 days from BL date', 0, 'bl', 60, 'parsed'],
    ['100% T/T 30 days after delivery', 0, 'delivery', 30, 'parsed'],
    ['payment before dispatch', 0, 'before_dispatch', 0, 'parsed'],
    ['Deposit 30% paid', 30, 'unknown', 0, 'partial'],
    ['', null, 'unknown', 0, 'none'],
    ['As per contract', null, 'unknown', 0, 'none'],
    ['L/C at sight', null, 'unknown', 0, 'none'],
    ['30% deposit, 70% against copy of B/L within 5 days', 30, 'bl', 5, 'parsed'],
    ['40% deposit, 60% before delivery', 40, 'before_dispatch', 0, 'parsed'],
    ['30% deposit, 70% 45 days after arrival at destination port', 30, 'arrival', 45, 'parsed'],
    ['50% deposit 50% balance 7 days before shipment', 50, 'before_dispatch', -7, 'parsed'],
    ['30% deposit, 40% before shipment, 30% 30 days after delivery', 30, 'before_dispatch', 0, 'partial'],
    ['100% in advance', 100, 'order', 0, 'parsed'],
    ['30% deposit; balance after receiving B/L copy', 30, 'bl', 0, 'parsed'],
    ['30% deposit, 70% before shipment. Deposit paid on 12/03', 30, 'before_dispatch', 0, 'parsed'],
    ['30% deposit, balance thirty days after B/L', 30, 'bl', 30, 'parsed'],
    ['30% deposit, 70% B/L copy, 30 days', 30, 'bl', 30, 'parsed'],
    ['30D/70B BOL', 30, 'bl', 0, 'parsed'],
    ['100D', 100, 'order', 0, 'parsed'],
    ['100B BOL', 0, 'bl', 0, 'parsed'],
    ['30D/70B 30 Days before BOL', 30, 'bl', -30, 'parsed'],
    ['50D/50B Before dispatch', 50, 'before_dispatch', 0, 'parsed'],
];

test('parsePaymentTerms: ShipLine\'s documented vectors', () => {
    for (const [input, pct, trigger, offset, confidence] of VECTORS) {
        const r = T.parsePaymentTerms(input);
        assert.deepEqual([r.depositPct, r.balanceTrigger, r.balanceOffsetDays, r.confidence], [pct, trigger, offset, confidence], input);
    }
    assert.equal(T.parsePaymentTerms('Deposit is NOT required. 100% against BL copy').depositPct, 0);
    assert.equal(T.parsePaymentTerms(null).depositPct, null);
});

test('resolveTermsRule: the better-understood source wins; the PI\'s extracted % back-fills', () => {
    assert.equal(T.resolveTermsRule(null, '100B BOL').depositPct, 0);
    assert.equal(T.resolveTermsRule({ dueTerms: '30% deposit, 70% before shipment', rawTermsText: null, depositPercentage: null }, '100B BOL').source, 'invoice');
    assert.equal(T.resolveTermsRule({ dueTerms: '30% deposit, 70% before shipment', rawTermsText: null, depositPercentage: null }, '100B BOL').depositPct, 30);
    // A PI whose text says nothing leaves the supplier's JFPRO terms in force.
    assert.equal(T.resolveTermsRule({ dueTerms: 'As per contract', rawTermsText: null, depositPercentage: null }, '100B BOL').depositPct, 0);
    assert.equal(T.resolveTermsRule({ dueTerms: null, rawTermsText: 'As per contract', depositPercentage: 40 }, null).depositPct, 40);
    assert.equal(T.resolveTermsRule(null, null).depositPct, null);
});

test('ruleDepositPct: the supplier\'s rule, else the default, punctuation-blind', () => {
    const rules = [
        { scope: 'default', supplier_name: '', deposit_pct: '30.000' },
        { scope: 'supplier', supplier_name: 'suzhou sunmed co., ltd', deposit_pct: '0.000' },
        { scope: 'supplier', supplier_name: 'acme', deposit_pct: null },
    ];
    assert.equal(T.ruleDepositPct(rules, ['Suzhou Sunmed Co.,Ltd.']), 0);
    assert.equal(T.ruleDepositPct(rules, ['Acme']), 30);
    assert.equal(T.ruleDepositPct(rules, ['Other']), 30);
    assert.equal(T.ruleDepositPct([], ['Other']), null);
    assert.equal(T.ruleDepositPct([{ scope: 'default', supplier_name: '', deposit_pct: null }], ['Other']), null);
});

test('depositPctInForce: a company rule\'s fixed % over the terms', () => {
    const rules = [{ scope: 'supplier', supplier_name: 'acme', deposit_pct: 50 }];
    assert.equal(T.depositPctInForce({ rules, supplierNames: ['Acme'], newestPi: null, supplierTerms: '100B BOL' }), 50);
    assert.equal(T.depositPctInForce({ rules, supplierNames: ['Other'], newestPi: null, supplierTerms: '100B BOL' }), 0);
    assert.equal(T.depositPctInForce({ rules: [], supplierNames: ['Other'], newestPi: null, supplierTerms: null }), null);
});

// A connection that answers loadDepositGate's reads from a small fixture.
function fakeConn({ rules = [], pi = null, terms = null, paid = false, receipt = false }) {
    const asked = [];
    return {
        asked,
        query: async (sql) => {
            const s = sql.replace(/\s+/g, ' ').trim();
            asked.push(s);
            if (s.startsWith('SELECT DISTINCT supplier FROM orders')) return [[{ supplier: 'Acme Ltd' }]];
            if (s.startsWith('SELECT scope, supplier_name, deposit_pct FROM payment_rules')) return [rules];
            if (s.startsWith('SELECT p.due_terms')) return [pi ? [pi] : []];
            if (s.startsWith('SELECT default_payment_terms FROM suppliers')) return [terms ? [{ default_payment_terms: terms }] : []];
            if (s.startsWith('SELECT 1 FROM supplier_payment_lines')) return [paid ? [{ 1: 1 }] : []];
            if (s.startsWith('SELECT 1 FROM po_deposit_receipts')) return [receipt ? [{ 1: 1 }] : []];
            throw new Error(`unexpected statement: ${s.slice(0, 60)}`);
        },
    };
}
const PO = { id: 9, supplier: 'Acme Ltd' };

test('loadDepositGate: no deposit in the terms → clear, and the payments are not even read', async () => {
    const conn = fakeConn({ terms: '100B BOL' });
    assert.deepEqual(await loadDepositGate(conn, PO), { depositPct: 0, depositPaid: false, clear: true, reason: 'no_deposit' });
    assert.ok(conn.asked.every(s => !s.startsWith('SELECT 1 FROM')));
});

test('loadDepositGate: a deposit required and not paid → held', async () => {
    assert.deepEqual(await loadDepositGate(fakeConn({ terms: '30D/70B BOL' }), PO), { depositPct: 30, depositPaid: false, clear: false, reason: 'deposit_unpaid' });
});

test('loadDepositGate: a deposit paid, or confirmed received by the supplier → clear', async () => {
    assert.equal((await loadDepositGate(fakeConn({ terms: '30D/70B BOL', paid: true }), PO)).reason, 'deposit_paid');
    assert.equal((await loadDepositGate(fakeConn({ terms: '30D/70B BOL', receipt: true }), PO)).reason, 'deposit_paid');
});

test('loadDepositGate: nothing says what the terms are → held, unless a deposit was paid anyway', async () => {
    assert.equal((await loadDepositGate(fakeConn({}), PO)).reason, 'terms_unknown');
    assert.equal((await loadDepositGate(fakeConn({ paid: true }), PO)).clear, true);
});

test('loadDepositGate: the company rule and the newest PI are read too', async () => {
    const rule = [{ scope: 'supplier', supplier_name: 'acme ltd', deposit_pct: '0.000' }];
    assert.equal((await loadDepositGate(fakeConn({ rules: rule, terms: '30D/70B BOL' }), PO)).reason, 'no_deposit');
    const pi = { due_terms: '50% deposit, 50% before shipment', raw_terms_text: null, deposit_percentage: '50.000' };
    assert.equal((await loadDepositGate(fakeConn({ pi, terms: null }), PO)).depositPct, 50);
});
