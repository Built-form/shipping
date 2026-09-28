'use strict';

// Unit tests for the balance-invoice reader's pure checks
// (src/services/shipment-payment-extract.js): a goods-value (commercial)
// invoice, the amount a record is made for, and an invoice issued under
// another company name. No database, no model call.
//   node --test tools/test-invoice-check-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { compareInvoiceToContainer, assessFit, documentAmountOf, money } = require('../src/services/shipment-payment-extract');

const SUPPLIER = 'Jianhui HK Industry Limited';
const KEY = 'jianhui hk industry limited';
// PO_00306J on container 307: six lines worth 14,382.20, a 30 % deposit PI on file.
const OUR_LINES = [
    { jfCode: 'JF1001', productName: 'A', quantity: 1000, unitPrice: 2.5 },
    { jfCode: 'JF1002', productName: 'B', quantity: 2000, unitPrice: 1.2 },
    { jfCode: 'JF1003', productName: 'C', quantity: 500, unitPrice: 4.8 },
    { jfCode: 'JF1004', productName: 'D', quantity: 1200, unitPrice: 2 },
    { jfCode: 'JF1005', productName: 'E', quantity: 1500, unitPrice: 1.6 },
    { jfCode: 'JF1006', productName: 'F', quantity: 1000, unitPrice: 2.2822 },
];
const CONTEXT = {
    shipment: { reference: '307' }, shipmentCurrency: 'USD',
    purchaseOrders: [{
        id: 306, poNumber: 'PO_00306J', supplier: SUPPLIER, supplierKey: KEY, currency: 'USD', valueInShipment: 14382.2,
        deposit: { amountDue: 4314.66, depositPercentage: 30, paymentStatus: 'pending' }, lines: OUR_LINES,
    }],
};
const invoiceLines = (lines) => lines.map(l => ({ poRef: 'PO_00306J', jfCode: l.jfCode, description: l.productName, qty: l.quantity, unitPrice: l.unitPrice, amount: Math.round(l.quantity * l.unitPrice * 100) / 100 }));
const CI = { documentKind: 'balance_invoice', currency: 'USD', totalAmount: 14382.2, amountDueNow: null, depositDeducted: null, lines: invoiceLines(OUR_LINES) };

test('money: nothing stays nothing', () => {
    assert.equal(money(null), null);
    assert.equal(money(undefined), null);
    assert.equal(money(''), null);
    assert.equal(money(0), 0);
    assert.equal(money('10067.54'), 10067.54);
});

test('a goods-value invoice (states nothing payable, deducts nothing) is checked on its goods', () => {
    const c = compareInvoiceToContainer({ extract: CI, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.invoiceBasis, 'goods');
    assert.equal(c.verdict, 'match');
    assert.equal(c.goodsOnBoard, 14382.2);
    assert.equal(c.goodsDelta, 0);
    assert.equal(c.invoiceBalance, null);
});

test('a goods invoice short of the goods on board: differs', () => {
    const c = compareInvoiceToContainer({ extract: { ...CI, totalAmount: 12100, lines: invoiceLines(OUR_LINES.slice(0, 5)) }, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.invoiceBasis, 'goods');
    assert.equal(c.verdict, 'differs');
    assert.equal(c.goodsDelta, -2282.2);
});

test('an invoice stating an amount due: the server reports what it asks and checks the lines only — the page judges the balance (it knows the terms)', () => {
    const c = compareInvoiceToContainer({ extract: { ...CI, amountDueNow: 14382.2 }, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.invoiceBasis, 'balance');
    assert.equal(c.invoiceBalance, 14382.2);
    assert.equal(c.verdict, 'match');
});

test('an invoice that nets the deposit off keeps comparing what it asks for', () => {
    const c = compareInvoiceToContainer({ extract: { ...CI, totalAmount: 10067.54, depositDeducted: 4314.66 }, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.invoiceBasis, 'balance');
    assert.equal(c.verdict, 'match');
    assert.equal(c.balanceDelta, 0);
});

test('the figure a document shows: what it states payable, else its total', () => {
    assert.equal(documentAmountOf({ amountDueNow: 5000, totalAmount: 9000 }), 5000);
    assert.equal(documentAmountOf({ amountDueNow: null, totalAmount: 9000 }), 9000);
    assert.equal(documentAmountOf({ amountDueNow: null, totalAmount: null }), null);
});

const MEMBER_POS = [{ id: 306, poNumber: 'PO_00306J', supplier: SUPPLIER, supplierKey: KEY, currency: 'USD', valueInShipment: 14382.2 }];
const fit = (extra) => assessFit({
    extract: { supplierName: 'DONGGUAN JIUHUI INDUSTRIAL LIMITED', currency: 'USD', containerRefs: [], blRefs: [], poRefs: [], lines: [], bank: null, documentKind: 'balance_invoice', ...extra },
    shipment: { reference: '307' }, supplierName: SUPPLIER, memberPos: MEMBER_POS, amount: 10067.54,
});

test('issued under another company name but citing this supplier\'s PO: a warning, not a mismatch', () => {
    const f = fit({ poRefs: ['PO_00306J'] });
    assert.equal(f.verdict, 'match');
    const sup = f.checks.find(c => c.key === 'supplier');
    assert.equal(sup.ok, null);
    assert.match(sup.text, /DONGGUAN JIUHUI/);
    assert.match(sup.text, /PO_00306J/);
});

test('another company name and none of our POs: still a mismatch', () => {
    assert.equal(fit({}).verdict, 'mismatch');
    const elsewhere = fit({ poRefs: ['PO_77777Q'] });
    assert.equal(elsewhere.verdict, 'mismatch');
    assert.equal(elsewhere.checks.find(c => c.key === 'supplier').ok, false);
});

test('a remittance paid to another name is a mismatch even when it cites our PO', () => {
    assert.equal(fit({ documentKind: 'remittance', poRefs: ['PO_00306J'] }).verdict, 'mismatch');
});
