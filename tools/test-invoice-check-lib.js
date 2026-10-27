'use strict';

// Unit tests for the balance-invoice reader's pure checks
// (src/services/shipment-payment-extract.js): a goods-value (commercial)
// invoice, the amount a record is made for, and an invoice issued under
// another company name. No database, no model call.
//   node --test tools/test-invoice-check-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { compareInvoiceToContainer, assessFit, documentAmountOf, money, RESPONSE_SCHEMA, promptFor, shouldCheck, noPaymentReason } = require('../src/services/shipment-payment-extract');
const piCheck = require('../src/services/po-invoice-check');
const { EXTRA_KINDS } = require('../src/lib/payment-extras');

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

// ── Charges that are not goods (handling, mould, bank charges, credits) ──
const CHARGES = [
    { description: 'HANDLING FEE', amount: 350, kind: 'handling' },
    { description: 'Bank charges', amount: 25, kind: 'bank_charge' },
    { description: 'Less: damaged cartons', amount: -100, kind: 'discount' },
];

test('charges read off the invoice stay out of the goods check', () => {
    const c = compareInvoiceToContainer({ extract: { ...CI, totalAmount: 14382.2 + 275, otherCharges: CHARGES }, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.invoiceBasis, 'goods');
    assert.equal(c.verdict, 'match');
    assert.equal(c.chargesTotal, 275);
    assert.equal(c.invoiceGoods, 14382.2);
    assert.equal(c.goodsDelta, 0);
    assert.deepEqual(c.otherCharges, CHARGES);
    // A stated balance with a fee in it is compared net of the fee.
    const b = compareInvoiceToContainer({
        extract: { ...CI, totalAmount: 10067.54 + 350, depositDeducted: 4314.66, otherCharges: [CHARGES[0]] }, context: CONTEXT, supplierName: SUPPLIER,
    });
    assert.equal(b.invoiceBasis, 'balance');
    assert.equal(b.invoiceBalance, 10067.54);
    assert.equal(b.balanceDelta, 0);
});

test('a read from before charges were read: no charges, as before', () => {
    const c = compareInvoiceToContainer({ extract: CI, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.chargesTotal, 0);
    assert.deepEqual(c.otherCharges, []);
    // Nonsense in the field is dropped, not added up.
    const junk = compareInvoiceToContainer({ extract: { ...CI, otherCharges: [{ description: 'x', amount: null, kind: 'other' }, null, 'fee'] }, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(junk.chargesTotal, 0);
    assert.deepEqual(junk.otherCharges, []);
    // A kind we have no name for is "other".
    const odd = compareInvoiceToContainer({ extract: { ...CI, totalAmount: 14387.2, otherCharges: [{ description: ' Surcharge ', amount: '5', kind: 'surprise' }] }, context: CONTEXT, supplierName: SUPPLIER });
    assert.deepEqual(odd.otherCharges, [{ description: 'Surcharge', amount: 5, kind: 'other' }]);
});

test('a mould fee above 5 % of the goods does not make the invoice another shipment\'s', () => {
    const extract = { supplierName: SUPPLIER, currency: 'USD', containerRefs: [], blRefs: [], poRefs: ['PO_00306J'], lines: [], bank: null, documentKind: 'balance_invoice' };
    const withMould = assessFit({ extract: { ...extract, otherCharges: [{ description: 'Mould cost', amount: 3000, kind: 'mould' }] }, shipment: { reference: '307' }, supplierName: SUPPLIER, memberPos: MEMBER_POS, amount: 14382.2 + 3000 });
    assert.equal(withMould.checks.find(c => c.key === 'amount').ok, null);
    assert.equal(withMould.verdict, 'match');
    // Unread as a charge, the same total is flagged, as before.
    const unread = assessFit({ extract, shipment: { reference: '307' }, supplierName: SUPPLIER, memberPos: MEMBER_POS, amount: 14382.2 + 3000 });
    assert.equal(unread.checks.find(c => c.key === 'amount').ok, false);
});

test('both readers ask for charges in the kinds an extra can be', () => {
    for (const schema of [RESPONSE_SCHEMA, piCheck.RESPONSE_SCHEMA]) {
        assert.ok(schema.required.includes('otherCharges'));
        const item = schema.properties.otherCharges.items;
        assert.deepEqual(item.required, ['description', 'amount', 'kind']);
        assert.deepEqual(item.properties.kind.enum, EXTRA_KINDS);
    }
});

// ── QC units (_FQC lines): billed on their own invoice, never on board ──
// The supplier bills QC sample units on their own invoice, often for several
// POs at once — some with goods in this box, some not (user, 2026-09-29: they
// are paid with the box the invoice is uploaded on). The check matches them to
// the supplier's QC lines on any PO, never calls the box's goods missing
// because of them, and never matches another supplier's.
const qc = (poId, poNumber, jfCode, quantity, unitPrice, supplier = SUPPLIER) => ({
    poId, poNumber, supplier, supplierKey: supplier.toLowerCase(), jfCode, productName: jfCode.replace(/_FQC$/, ''), quantity, unitPrice,
});
const QC_LINES = [
    qc(306, 'PO_00306J', 'JF1001_FQC', 10, 2.5),
    qc(306, 'PO_00306J', 'JF1002_FQC', 20, 1.2),
    qc(311, 'PO_00311J', 'JF2001_FQC', 5, 7.3),
    qc(311, 'PO_00311J', 'JF1001_FQC', 4, 2.5),
    qc(290, 'PO_00290X', 'JF1001_FQC', 10, 2.5, 'Other Trading Co Ltd'),
];
const CONTEXT_QC = { ...CONTEXT, qcLines: QC_LINES };
const qcLine = (poRef, jfCode, qty, unitPrice) => ({ poRef, jfCode, description: 'QC samples', qty, unitPrice, amount: Math.round(qty * unitPrice * 100) / 100 });
const QC_INVOICE = {
    documentKind: 'balance_invoice', currency: 'USD', totalAmount: 85.5, amountDueNow: null, depositDeducted: null,
    lines: [qcLine('PO_00306J', 'JF1001_FQC', 10, 2.5), qcLine('PO_00306J', 'JF1002_FQC', 20, 1.2), qcLine('PO_00311J', 'JF2001_FQC', 5, 7.3)],
};

test('a QC units invoice for this box\'s PO and another: matches, and the box\'s goods are not "missing"', () => {
    const c = compareInvoiceToContainer({ extract: QC_INVOICE, context: CONTEXT_QC, supplierName: SUPPLIER });
    assert.equal(c.verdict, 'match');
    assert.equal(c.invoiceKind, 'qc');
    assert.deepEqual(c.qcLines.map(l => [l.poId, l.poNumber, l.jfCode, l.ourQty, l.invoiceQty, l.ourTotal, l.invoiceTotal, l.issues.length]), [
        [306, 'PO_00306J', 'JF1001_FQC', 10, 10, 25, 25, 0],
        [306, 'PO_00306J', 'JF1002_FQC', 20, 20, 24, 24, 0],
        [311, 'PO_00311J', 'JF2001_FQC', 5, 5, 36.5, 36.5, 0],
    ]);
    assert.deepEqual(c.missingOurLines, []);
    assert.deepEqual(c.extraInvoiceLines, []);
    assert.equal(c.matchedLines, 0);
    assert.equal(c.qcOurTotal, 85.5);
    assert.equal(c.qcInvoiceTotal, 85.5);
    assert.equal(c.goodsDelta, 0);
});

test('goods and QC units on one invoice: the goods are checked without the QC units', () => {
    const c = compareInvoiceToContainer({
        extract: { ...CI, totalAmount: 14382.2 + 25, lines: [...invoiceLines(OUR_LINES), qcLine('PO_00306J', 'JF1001_FQC', 10, 2.5)] },
        context: CONTEXT_QC, supplierName: SUPPLIER,
    });
    assert.equal(c.verdict, 'match');
    assert.equal(c.invoiceKind, 'goods_and_qc');
    assert.equal(c.matchedLines, 6);
    assert.equal(c.qcLines.length, 1);
    assert.equal(c.invoiceGoods, 14382.2);
    assert.equal(c.goodsDelta, 0);
    // Goods short on a mixed invoice are still missing.
    const short = compareInvoiceToContainer({
        extract: { ...CI, totalAmount: 12100 + 25, lines: [...invoiceLines(OUR_LINES.slice(0, 5)), qcLine('PO_00306J', 'JF1001_FQC', 10, 2.5)] },
        context: CONTEXT_QC, supplierName: SUPPLIER,
    });
    assert.equal(short.verdict, 'differs');
    assert.deepEqual(short.missingOurLines.map(l => l.jfCode), ['JF1006']);
});

test('a QC line that does not agree with the PO: differs, said against the PO', () => {
    const c = compareInvoiceToContainer({
        extract: { ...QC_INVOICE, totalAmount: 90.5, lines: [qcLine('PO_00306J', 'JF1001_FQC', 12, 2.5), ...QC_INVOICE.lines.slice(1)] },
        context: CONTEXT_QC, supplierName: SUPPLIER,
    });
    assert.equal(c.verdict, 'differs');
    assert.deepEqual(c.qcLines[0].issues, ['quantity 12 on the invoice, 10 on the PO', 'line total 30.00 on the invoice, 25.00 on the PO']);
});

test('a QC code none of the supplier\'s POs has: an extra line, differs', () => {
    const c = compareInvoiceToContainer({
        extract: { ...QC_INVOICE, lines: [...QC_INVOICE.lines, qcLine('PO_00306J', 'JF9999_FQC', 1, 1)] },
        context: CONTEXT_QC, supplierName: SUPPLIER,
    });
    assert.equal(c.verdict, 'differs');
    assert.deepEqual(c.extraInvoiceLines.map(l => l.jfCode), ['JF9999_FQC']);
});

test('a QC code on two of the supplier\'s POs: the one it names; with no PO named, the one in this box — never another supplier\'s', () => {
    const named = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine('PO_00311J', 'JF1001_FQC', 4, 2.5)] }, context: CONTEXT_QC, supplierName: SUPPLIER });
    assert.deepEqual(named.qcLines.map(l => [l.poId, l.ourQty]), [[311, 4]]);
    const unnamed = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine(null, 'JF1001_FQC', 10, 2.5)] }, context: CONTEXT_QC, supplierName: SUPPLIER });
    assert.deepEqual(unnamed.qcLines.map(l => [l.poId, l.ourQty]), [[306, 10]]);
    const other = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine('PO_00290X', 'JF1001_FQC', 10, 2.5)] }, context: CONTEXT_QC, supplierName: SUPPLIER });
    assert.deepEqual(other.qcLines, []);
    assert.equal(other.extraInvoiceLines.length, 1);
});

test('the PO in this box first whatever order the lines come in; else the newest PO', () => {
    const reversed = { ...CONTEXT, qcLines: [...QC_LINES].reverse() };
    const unnamed = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine(null, 'JF1001_FQC', 10, 2.5)] }, context: reversed, supplierName: SUPPLIER });
    assert.deepEqual(unnamed.qcLines.map(l => l.poId), [306]);
    const twoOff = { ...CONTEXT, qcLines: [qc(311, 'PO_00311J', 'JF3003_FQC', 1, 1), qc(312, 'PO_00312J', 'JF3003_FQC', 1, 1), qc(309, 'PO_00309J', 'JF3003_FQC', 1, 1)] };
    const newest = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine(null, 'JF3003_FQC', 1, 1)] }, context: twoOff, supplierName: SUPPLIER });
    assert.deepEqual(newest.qcLines.map(l => l.poId), [312]);
});

test('a QC unit is never taken for the goods it samples, by name or as a PO\'s last line', () => {
    const byName = compareInvoiceToContainer({
        extract: { ...QC_INVOICE, lines: [{ ...qcLine('PO_00306J', 'JF1001_FQC', 10, 2.5), description: 'A' }] },
        context: CONTEXT_QC, supplierName: SUPPLIER,
    });
    assert.deepEqual(byName.qcLines.map(l => l.jfCode), ['JF1001_FQC']);
    assert.equal(byName.matchedLines, 0);
});

test('an invoice of goods none of which is in this box: it bills goods, so the box\'s goods are missing', () => {
    const stranger = (jfCode) => ({ poRef: 'PO_00306J', jfCode, description: 'Z', qty: 1, unitPrice: 1, amount: 1 });
    const c = compareInvoiceToContainer({ extract: { ...CI, totalAmount: 2, lines: [stranger('JF7001'), stranger('JF7002')] }, context: CONTEXT_QC, supplierName: SUPPLIER });
    assert.equal(c.invoiceKind, 'goods');
    assert.equal(c.missingOurLines.length, 6);
    assert.equal(c.verdict, 'differs');
});

test('a QC invoice on a box where the supplier has no goods: still checked against the POs', () => {
    const c = compareInvoiceToContainer({ extract: QC_INVOICE, context: { ...CONTEXT_QC, purchaseOrders: [] }, supplierName: SUPPLIER });
    assert.equal(c.verdict, 'match');
    assert.equal(c.qcLines.length, 3);
});

test('an _FQC line that travels in the box is goods, not a QC unit', () => {
    const onBoard = [...OUR_LINES, { jfCode: 'JF1007_FQC', productName: 'G', quantity: 10, unitPrice: 1 }];
    const ctx = { ...CONTEXT_QC, purchaseOrders: [{ ...CONTEXT.purchaseOrders[0], valueInShipment: 14392.2, lines: onBoard }] };
    const c = compareInvoiceToContainer({ extract: { ...CI, totalAmount: 14392.2, lines: invoiceLines(onBoard) }, context: ctx, supplierName: SUPPLIER });
    assert.equal(c.verdict, 'match');
    assert.equal(c.invoiceKind, 'goods');
    assert.equal(c.matchedLines, 7);
    assert.deepEqual(c.qcLines, []);
});

test('a context without QC lines (older callers): QC lines are strangers, as before', () => {
    const c = compareInvoiceToContainer({ extract: QC_INVOICE, context: CONTEXT, supplierName: SUPPLIER });
    assert.equal(c.verdict, 'differs');
    assert.equal(c.extraInvoiceLines.length, 3);
    assert.deepEqual(c.qcLines, []);
});

test('a QC invoice naming another PO of this supplier belongs here; another supplier\'s PO does not', () => {
    const qcPos = [{ id: 311, poNumber: 'PO_00311J', supplier: SUPPLIER, supplierKey: KEY }];
    const extract = { supplierName: SUPPLIER, currency: 'USD', containerRefs: [], blRefs: [], poRefs: ['PO_00311J'], lines: QC_INVOICE.lines.slice(2), bank: null, documentKind: 'balance_invoice' };
    const f = assessFit({ extract, shipment: { reference: '307' }, supplierName: SUPPLIER, memberPos: MEMBER_POS, qcPos, amount: 36.5 });
    const pos = f.checks.find(c => c.key === 'pos');
    assert.equal(pos.ok, true);
    assert.equal(pos.text, 'Names PO_00311J (QC units).');
    assert.equal(f.verdict, 'match');
    // Without the QC POs (older callers) it points elsewhere, as before.
    assert.equal(assessFit({ extract, shipment: { reference: '307' }, supplierName: SUPPLIER, memberPos: MEMBER_POS, amount: 36.5 }).verdict, 'mismatch');
    // Another supplier's PO passed in by mistake still does not belong here.
    const others = [{ id: 290, poNumber: 'PO_00290X', supplier: 'Other Trading Co Ltd', supplierKey: 'other trading co ltd' }];
    const otherFit = assessFit({ extract: { ...extract, poRefs: ['PO_00290X'], lines: [] }, shipment: { reference: '307' }, supplierName: SUPPLIER, memberPos: MEMBER_POS, qcPos: others, amount: 25 });
    assert.equal(otherFit.checks.find(c => c.key === 'pos').ok, false);
});

test('the reader is told the supplier\'s QC units, newest PO first; no section without them', () => {
    const p = promptFor({ ...CONTEXT, qcLines: QC_LINES.slice(0, 4) });
    assert.match(p, /## QC sample units on this supplier's purchase orders/);
    assert.ok(p.includes('- PO_00311J: JF2001_FQC JF2001 × 5 @ 7.3; JF1001_FQC JF1001 × 4 @ 2.5'));
    assert.ok(p.includes('- PO_00306J: JF1001_FQC JF1001 × 10 @ 2.5; JF1002_FQC JF1002 × 20 @ 1.2'));
    assert.ok(p.indexOf('- PO_00311J') < p.indexOf('- PO_00306J'));
    assert.doesNotMatch(promptFor(CONTEXT), /QC sample units/);
});

// ── A QC invoice uploaded against the supplier, no container ─────────────
// QC units belong to no container (user, 2026-09-29): their invoice is
// uploaded on the supplier (the Payments page's QC tab) and read and checked
// all the same. A proof of payment on the supplier stays as it was.
test('which reads are checked line by line: any invoice on a container; on the supplier alone, one when the supplier has QC units', () => {
    assert.equal(shouldCheck({ documentKind: 'balance_invoice', hasShipment: true, hasQcLines: false }), true);
    assert.equal(shouldCheck({ documentKind: 'deposit_invoice', hasShipment: true, hasQcLines: false }), true);
    assert.equal(shouldCheck({ documentKind: 'balance_invoice', hasShipment: false, hasQcLines: true }), true);
    assert.equal(shouldCheck({ documentKind: 'balance_invoice', hasShipment: false, hasQcLines: false }), false);
    assert.equal(shouldCheck({ documentKind: 'remittance', hasShipment: false, hasQcLines: true }), false);
    assert.equal(shouldCheck({ documentKind: 'other', hasShipment: true, hasQcLines: true }), false);
});

test('what a read says it made: a QC invoice on the supplier is checked, never "needs its container"', () => {
    assert.equal(noPaymentReason({ documentKind: 'remittance', hasShipment: false, invoiceKind: null }), 'remittance');
    assert.equal(noPaymentReason({ documentKind: 'balance_invoice', hasShipment: false, invoiceKind: 'qc' }), 'checked');
    assert.equal(noPaymentReason({ documentKind: 'balance_invoice', hasShipment: false, invoiceKind: 'goods' }), 'no_shipment');
    assert.equal(noPaymentReason({ documentKind: 'balance_invoice', hasShipment: false, invoiceKind: null }), 'no_shipment');
    assert.equal(noPaymentReason({ documentKind: 'balance_invoice', hasShipment: true, invoiceKind: 'goods' }), 'checked');
});

test('the reader is told a supplier upload is a QC invoice, with the supplier\'s QC units — not "most likely a remittance"', () => {
    const p = promptFor({ shipment: null, shipmentCurrency: null, purchaseOrders: [], qcLines: QC_LINES.slice(0, 2) });
    assert.match(p, /uploaded against a supplier as an invoice for QC sample units/);
    assert.match(p, /- PO_00306J: JF1001_FQC/);
    assert.doesNotMatch(p, /remittance advice/);
    assert.match(promptFor({ shipment: null, shipmentCurrency: null, purchaseOrders: [] }), /remittance advice/);
});

// A QC code on two of the supplier's POs with no poRef read (user, 2026-09-30:
// invoice 26/89132 bills JF0938_FQC × 62 for SUNMED-93; the read ticked
// PO_00333J's 50). The PO printed in the line's text counts as its reference;
// failing that, the PO whose quantity is the invoice's — "newest" is last.
const TWO_POS = { ...CONTEXT, qcLines: [
    qc(94, 'SUNMED-93', 'JF0938_FQC', 62, null),
    qc(333, 'PO_00333J', 'JF0938_FQC', 50, 2.27),
] };
test('no poRef, but the PO number is printed in the line: that PO', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine(null, 'JF0938_FQC', 62, 2.27), description: 'BY INSPECTION CUTIDERM BUTTERFLY SUNMED-93 LOT 0938009' }] }, context: TWO_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.ourQty, l.invoiceQty, l.issues]), [['SUNMED-93', 62, 62, []]]);
});
test('no poRef and no PO in the text: the PO whose quantity is the invoice\'s, before the newest', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine(null, 'JF0938_FQC', 62, 2.27)] }, context: TWO_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.ourQty, l.invoiceQty, l.issues]), [['SUNMED-93', 62, 62, []]]);
    const fifty = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine(null, 'JF0938_FQC', 50, 2.27)] }, context: TWO_POS, supplierName: SUPPLIER });
    assert.deepEqual(fifty.qcLines.map(l => l.poNumber), ['PO_00333J']);
});
test('a number in the text that is not a PO of this supplier changes nothing', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine(null, 'JF0938_FQC', 7, 2.27), description: 'LOT 0938009 MFG 05.2026 BOX OF 100' }] }, context: TWO_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => l.poNumber), ['PO_00333J']);
});
test('a PO printed in the line outranks the quantity: 62 units with "PO_00333J" in the text go to PO_00333J, mismatch and all', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine(null, 'JF0938_FQC', 62, 2.27), description: 'BY INSPECTION PO_00333J LOT 0938009' }] }, context: TWO_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.ourQty, l.invoiceQty]), [['PO_00333J', 50, 62]]);
    assert.equal(c.qcLines[0].issues.length, 2);
});
test('a digits-only token in the text never names a PO, even one numbered with digits alone', () => {
    const numeric = { ...CONTEXT, qcLines: [qc(500, '100', 'JF0938_FQC', 50, 2.27), qc(94, 'SUNMED-93', 'JF0938_FQC', 62, null)] };
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine(null, 'JF0938_FQC', 62, 2.27), description: 'BOX OF 100' }] }, context: numeric, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => l.poNumber), ['SUNMED-93']);
});

// The model maps a line's PO to one of ours and can map it wrong (2026-10-01,
// invoice 26/89132: every line set to PO_00333J, though the lines print
// SUNMED-93 and SUNMED-96). The PO printed on the line is the document's own
// word and outranks the model's poRef.
const THREE_POS = { ...CONTEXT, qcLines: [
    qc(94, 'SUNMED-93', 'JF0938_FQC', 62, null),
    qc(97, 'SUNMED-96', 'JF0993_FQC', 40, null),
    qc(333, 'PO_00333J', 'JF0938_FQC', 50, 2.27),
    qc(333, 'PO_00333J', 'JF0993_FQC', 40, 1.83),
    qc(298, 'PO_00298J', 'JF1168_FQC', 20, 0.95),
    qc(393, 'PO_00393J', 'JF1168_FQC', 23, 1),
    qc(95, 'SUNMED-95', 'JF1168_FQC', 26, null),
] };
test('the model names PO_00333J but the line prints SUNMED-93: SUNMED-93, no issues', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine('PO_00333J', 'JF0938_FQC', 62, 2.27), description: 'BY INSPECTION CUTIDERM BUTTERFLY SUNMED-93 LOT 0938009' }] }, context: THREE_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.ourQty, l.invoiceQty, l.issues]), [['SUNMED-93', 62, 62, []]]);
});
test('the model names PO_00333J, the line prints SUNMED-96 and the quantities agree on both: SUNMED-96 still', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine('PO_00333J', 'JF0993_FQC', 40, 1.83), description: 'CUTI-ADH-10X10-50 SUNMED-96 LOT 0993015' }] }, context: THREE_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => l.poNumber), ['SUNMED-96']);
});
test('the model names a PO whose quantity differs, nothing printed, and exactly one other PO has the invoice\'s quantity: that one', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine('PO_00393J', 'JF1168_FQC', 26, 0.95)] }, context: THREE_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.ourQty, l.invoiceQty]), [['SUNMED-95', 26, 26]]);
    // Two other POs with that quantity: the model's PO stands, mismatch and all.
    const two = { ...THREE_POS, qcLines: [...THREE_POS.qcLines, qc(86, 'SUNMED-86', 'JF1168_FQC', 26, null)] };
    const c2 = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine('PO_00393J', 'JF1168_FQC', 26, 0.95)] }, context: two, supplierName: SUPPLIER });
    assert.deepEqual(c2.qcLines.map(l => [l.poNumber, l.issues.length > 0]), [['PO_00393J', true]]);
});
test('the model names the right PO and it matches: unchanged', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [qcLine('PO_00298J', 'JF1168_FQC', 20, 0.95)] }, context: THREE_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.issues]), [['PO_00298J', []]]);
});
test('two POs printed on one line: the first printed wins; goods lines are untouched by the rule', () => {
    const c = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine(null, 'JF0938_FQC', 62, 2.27), description: 'SUNMED-93 (replaces PO_00333J) LOT 0938009' }] }, context: THREE_POS, supplierName: SUPPLIER });
    assert.deepEqual(c.qcLines.map(l => l.poNumber), ['SUNMED-93']);
    const swapped = compareInvoiceToContainer({ extract: { ...QC_INVOICE, lines: [{ ...qcLine(null, 'JF0938_FQC', 62, 2.27), description: 'PO_00333J (was SUNMED-93)' }] }, context: THREE_POS, supplierName: SUPPLIER });
    assert.deepEqual(swapped.qcLines.map(l => [l.poNumber, l.issues.length > 0]), [['PO_00333J', true]]);
});

// ── One product on several lines (user, 2026-10-26) ──────────────────────
// Sunmed's invoice 26/89138 for container 313 bills the full cartons and the
// partial carton of one product on two lines (JF0119: 1,500 + 15); the box
// holds one line of 1,515. Checked one line against one, in printed order,
// the 15 was taken for the PO's last unbilled product (JF0147, "quantity 15 on
// the invoice") and JF0147's own line was called "not in the box". The lines
// of one product are added up and compared as one, on either side, whatever
// order they are printed in, and a line is never checked against another
// product the box holds.
const SPLIT_CTX = {
    shipment: { reference: '313' }, shipmentCurrency: 'USD',
    purchaseOrders: [{
        id: 298, poNumber: 'PO_00298J', supplier: SUPPLIER, supplierKey: KEY, currency: 'USD', valueInShipment: 3002.91, deposit: null,
        lines: [
            { jfCode: 'JF0234', productName: 'Wound dressing', quantity: 2130, unitPrice: 0.38 },
            { jfCode: 'JF0119', productName: 'Butterfly closure', quantity: 1515, unitPrice: 0.61 },
            { jfCode: 'JF0147', productName: 'Adhesive strip', quantity: 1548, unitPrice: 0.82 },
        ],
    }],
};
const sl = (jfCode, qty, unitPrice, more = {}) => ({ poRef: 'PO_00298J', jfCode, piRef: null, sku: null, description: null, qty, unitPrice, amount: Math.round(qty * unitPrice * 100) / 100, ...more });
const SPLIT_LINES = [sl('JF0234', 2100, 0.38), sl('JF0234', 30, 0.38), sl('JF0119', 1500, 0.61), sl('JF0119', 15, 0.61), sl('JF0147', 1548, 0.82)];
const SPLIT_CI = { documentKind: 'balance_invoice', currency: 'USD', totalAmount: 3002.91, amountDueNow: null, depositDeducted: null, lines: SPLIT_LINES };
// The invoice's total is what its lines add up to.
const splitCheck = (lines, context = SPLIT_CTX) => compareInvoiceToContainer({
    extract: { ...SPLIT_CI, totalAmount: Math.round(lines.reduce((a, l) => a + l.amount, 0) * 100) / 100, lines }, context, supplierName: SUPPLIER,
});
const summaryOf = c => ({
    verdict: c.verdict,
    lines: c.lines.map(l => [l.poNumber, l.jfCode, l.ourQty, l.invoiceQty, l.ourTotal, l.invoiceTotal, l.issues]),
    missing: c.missingOurLines.map(l => [l.poNumber, l.jfCode, l.qty]),
    extra: c.extraInvoiceLines.map(l => [l.jfCode, l.qty]),
});

test('one product billed on two invoice lines (full cartons + a partial carton) is added up: matches', () => {
    const c = splitCheck(SPLIT_LINES);
    assert.deepEqual(summaryOf(c), {
        verdict: 'match',
        lines: [
            ['PO_00298J', 'JF0234', 2130, 2130, 809.4, 809.4, []],
            ['PO_00298J', 'JF0119', 1515, 1515, 924.15, 924.15, []],
            ['PO_00298J', 'JF0147', 1548, 1548, 1269.36, 1269.36, []],
        ],
        missing: [],
        extra: [],
    });
    assert.equal(c.matchedLines, 3);
    assert.equal(c.invoiceLinesMatched, 5);
});

test('the order the invoice lines are printed in changes nothing', () => {
    const want = summaryOf(splitCheck(SPLIT_LINES));
    const [a, b, d, e, f] = SPLIT_LINES;
    for (const order of [[f, a, b, d, e], [e, d, f, b, a], [b, f, e, a, d]]) {
        assert.deepEqual(summaryOf(splitCheck(order)), want);
    }
    // A real difference reads the same whatever the order.
    const off = [a, b, d, sl('JF0119', 20, 0.61), f];
    const wantOff = summaryOf(splitCheck(off));
    assert.deepEqual(summaryOf(splitCheck([...off].reverse())), wantOff);
});

test('one product on two lines in the box (a split order row) against one invoice line: matches', () => {
    const ctx = { ...SPLIT_CTX, purchaseOrders: [{ ...SPLIT_CTX.purchaseOrders[0], lines: [
        { jfCode: 'JF0234', productName: 'Wound dressing', quantity: 2130, unitPrice: 0.38 },
        { jfCode: 'JF0119', productName: 'Butterfly closure', quantity: 1500, unitPrice: 0.61 },
        { jfCode: 'JF0147', productName: 'Adhesive strip', quantity: 1548, unitPrice: 0.82 },
        { jfCode: 'JF0119', productName: 'Butterfly closure', quantity: 15, unitPrice: 0.61 },
    ] }] };
    const c = splitCheck([sl('JF0234', 2130, 0.38), sl('JF0119', 1515, 0.61), sl('JF0147', 1548, 0.82)], ctx);
    assert.equal(c.verdict, 'match');
    assert.deepEqual(c.lines.map(l => [l.jfCode, l.ourQty, l.ourRows, l.invoiceQty, l.issues]), [
        ['JF0234', 2130, 1, 2130, []],
        ['JF0119', 1515, 2, 1515, []],
        ['JF0147', 1548, 1, 1548, []],
    ]);
    assert.deepEqual(c.missingOurLines, []);
    // Not billed at all: missing once, for both rows.
    const unbilled = splitCheck([sl('JF0234', 2130, 0.38), sl('JF0147', 1548, 0.82)], ctx);
    assert.deepEqual(unbilled.missingOurLines.map(l => [l.jfCode, l.qty, l.total]), [['JF0119', 1515, 924.15]]);
});

test('a QC unit none of the supplier\'s QC lines takes is still never taken for the goods it samples', () => {
    const c = compareInvoiceToContainer({
        extract: { ...QC_INVOICE, lines: [{ ...qcLine('PO_00306J', 'JF1001_FQC', 10, 2.5), description: 'A' }] },
        context: CONTEXT, supplierName: SUPPLIER,
    });
    assert.equal(c.matchedLines, 0);
    assert.deepEqual(c.extraInvoiceLines.map(l => l.jfCode), ['JF1001_FQC']);
});

test('a split line that does not add up: one difference, in the invoice\'s own figures', () => {
    const c = splitCheck([sl('JF0234', 2130, 0.38), sl('JF0119', 15, 0.61), sl('JF0119', 1500, 0.61), sl('JF0119', 20, 0.61), sl('JF0147', 1548, 0.82)]);
    assert.equal(c.verdict, 'differs');
    assert.deepEqual(c.lines.find(l => l.jfCode === 'JF0119').issues, [
        'quantity 1,500 + 20 + 15 = 1,535 on the invoice, 1,515 in the container',
        'line total 936.35 on the invoice, 924.15 in the container',
    ]);
    assert.deepEqual(c.missingOurLines, []);
    assert.deepEqual(c.extraInvoiceLines, []);
});

test('a line whose code is in the box is never checked against another product', () => {
    // JF0147 is not billed: it is missing — the second JF0119 line is not "JF0147, quantity 15".
    const c = splitCheck([sl('JF0234', 2130, 0.38), sl('JF0119', 1500, 0.61), sl('JF0119', 15, 0.61)]);
    assert.equal(c.verdict, 'differs');
    assert.deepEqual(c.lines.map(l => [l.jfCode, l.invoiceQty, l.issues]), [['JF0234', 2130, []], ['JF0119', 1515, []]]);
    assert.deepEqual(c.missingOurLines.map(l => [l.jfCode, l.qty, l.total]), [['JF0147', 1548, 1269.36]]);
    assert.deepEqual(c.extraInvoiceLines, []);
});

test('each checked line carries the invoice lines it was checked against, as printed, and how they were matched', () => {
    const c = splitCheck([
        sl('JF0234', 2130, 0.38),
        sl('JF0119', 1500, 0.61), sl(null, 15, 0.61, { description: 'Butterfly closure' }),
        // No code of ours, no name of ours: the PO's one product left, and the only line left.
        sl(null, 1548, 0.61, { sku: 'SM-147', description: 'ADH STRIP 10CM' }),
    ]);
    const jf0119 = c.lines.find(l => l.jfCode === 'JF0119');
    assert.deepEqual(jf0119.invoiceRows.map(r => [r.jfCode, r.description, r.qty, r.matchedBy]), [['JF0119', null, 1500, 'code'], [null, 'Butterfly closure', 15, 'description']]);
    const jf0147 = c.lines.find(l => l.jfCode === 'JF0147');
    assert.deepEqual(jf0147.invoiceRows.map(r => [r.sku, r.description, r.qty, r.unitPrice, r.matchedBy]), [['SM-147', 'ADH STRIP 10CM', 1548, 0.61, 'only_line']]);
    assert.deepEqual(jf0147.issues, ['unit price 0.61 on the invoice, 0.82 on the PO', 'line total 944.28 on the invoice, 1269.36 in the container']);
});

test('the supplier\'s own code on two lines with no code of ours: added up, then matched as one', () => {
    const c = splitCheck([
        sl('JF0234', 2130, 0.38), sl('JF0119', 1515, 0.61),
        sl(null, 1500, 0.82, { sku: 'SM-147' }), sl(null, 48, 0.82, { sku: 'SM-147' }),
    ]);
    assert.equal(c.verdict, 'match');
    assert.deepEqual(c.lines.find(l => l.jfCode === 'JF0147').invoiceRows.map(r => [r.qty, r.matchedBy]), [[1500, 'only_line'], [48, 'only_line']]);
});

// The reader sometimes lists a charge among the goods (TEST read 22, invoice
// 26/89138: "printing plate charges JF0880", 120, no quantity, no price). It
// carries our code, but it is not goods: added to JF0880 it would read as a
// line-total difference on the bandages.
test('a line with an amount but no quantity and no price is never added to a product, even with our code on it', () => {
    const c = splitCheck([...SPLIT_LINES, { ...sl('JF0147', 0, 0), qty: null, unitPrice: null, amount: 120, description: 'printing plate charges JF0147' }]);
    assert.deepEqual(c.lines.find(l => l.jfCode === 'JF0147').issues, []);
    assert.deepEqual(c.extraInvoiceLines.map(l => [l.jfCode, l.qty, l.amount]), [['JF0147', null, 120]]);
    // Nor taken for the PO's one unbilled product.
    const alone = splitCheck([...SPLIT_LINES.slice(0, 4), { ...sl(null, 0, 0), qty: null, unitPrice: null, amount: 120, description: 'printing plate charges' }]);
    assert.deepEqual(alone.missingOurLines.map(l => l.jfCode), ['JF0147']);
    assert.equal(alone.extraInvoiceLines.length, 1);
});

test('two unknown lines left on a PO with one product unbilled: no guess by elimination; a quantity that fits places one', () => {
    const c = splitCheck([
        sl('JF0234', 2130, 0.38), sl('JF0119', 1515, 0.61),
        sl(null, 1548, 0.82, { sku: 'SM-147' }), sl(null, 10, 1, { sku: 'SM-999' }),
    ]);
    assert.deepEqual(c.lines.find(l => l.jfCode === 'JF0147').invoiceRows.map(r => [r.sku, r.matchedBy]), [['SM-147', 'quantity']]);
    assert.deepEqual(c.extraInvoiceLines.map(l => [l.sku, l.qty]), [['SM-999', 10]]);
    assert.equal(c.verdict, 'differs');
});

test('a code of ours billed under another of the supplier\'s POs in the box: checked against it, the PO said', () => {
    const ctx = { ...SPLIT_CTX, purchaseOrders: [
        { ...SPLIT_CTX.purchaseOrders[0], lines: SPLIT_CTX.purchaseOrders[0].lines.filter(l => l.jfCode !== 'JF0119') },
        { id: 305, poNumber: 'PO_00305J', supplier: SUPPLIER, supplierKey: KEY, currency: 'USD', valueInShipment: 924.15, deposit: null, lines: [SPLIT_CTX.purchaseOrders[0].lines[1]] },
    ] };
    const c = splitCheck(SPLIT_LINES, ctx);
    assert.equal(c.verdict, 'differs');
    const jf0119 = c.lines.find(l => l.jfCode === 'JF0119');
    assert.deepEqual([jf0119.poNumber, jf0119.ourQty, jf0119.invoiceQty, jf0119.issues], ['PO_00305J', 1515, 1515, ['billed under PO_00298J on the invoice, in the container under PO_00305J']]);
    assert.deepEqual(c.extraInvoiceLines, []);
    assert.deepEqual(c.missingOurLines, []);
    // No PO on the line and the code on two POs: checked against both together.
    const both = { ...ctx, purchaseOrders: ctx.purchaseOrders.map(p => (p.id === 298 ? { ...p, lines: [...p.lines, { jfCode: 'JF0119', productName: 'Butterfly closure', quantity: 100, unitPrice: 0.61 }] } : p)) };
    const n = splitCheck([sl('JF0234', 2130, 0.38), sl('JF0147', 1548, 0.82), { ...sl('JF0119', 1615, 0.61), poRef: null }], both);
    const jf = n.lines.find(l => l.jfCode === 'JF0119');
    assert.deepEqual([jf.poNumber, jf.ourQty, jf.issues], ['PO_00298J, PO_00305J', 1615, []]);
    assert.equal(n.verdict, 'match');
    // One of them billed by a line naming its PO: the line with no PO goes to the other.
    const f = splitCheck([sl('JF0234', 2130, 0.38), sl('JF0147', 1548, 0.82), sl('JF0119', 100, 0.61), { ...sl('JF0119', 1515, 0.61), poRef: null }], both);
    assert.deepEqual(f.lines.filter(l => l.jfCode === 'JF0119').map(l => [l.poNumber, l.ourQty, l.invoiceQty, l.issues]), [['PO_00298J', 100, 100, []], ['PO_00305J', 1515, 1515, []]]);
});

// ── A part carton the reader codes as QC units (user, 2026-10-27) ────────
// Sunmed's INV-26_89131 for box 314 bills JF1402 on two lines, 4,992 + 24;
// the box holds 5,016. PO_00333J also carries JF1402_FQC × 24 (destroyed QC
// units, owed on their own). The reader is shown that QC unit and coded the
// 24 as JF1402_FQC: the check took it for the QC units and called the box
// short by 24. A line coded _FQC whose product is in the box, with no QC
// wording, is goods when the box only adds up with it.
const BOX_314 = {
    shipment: { reference: '314' }, shipmentCurrency: 'USD',
    purchaseOrders: [{
        id: 333, poNumber: 'PO_00333J', supplier: SUPPLIER, supplierKey: KEY, currency: 'USD', valueInShipment: 12540, deposit: null,
        lines: [{ jfCode: 'JF1402', productName: 'CUTI-ADH-10x15-50', quantity: 5016, unitPrice: 2.5 }],
    }],
    qcLines: [qc(333, 'PO_00333J', 'JF1402_FQC', 24, 2.5)],
};
const l314 = (jfCode, qty, more = {}) => ({ poRef: 'PO_00333J', jfCode, piRef: null, sku: 'CUTI-ADH-10x15-50', description: 'Cutiderm Adhesive Wound Dressing 10 x 15cm - Pack of 50', qty, unitPrice: 2.5, amount: Math.round(qty * 2.5 * 100) / 100, ...more });
const check314 = (lines, context = BOX_314) => compareInvoiceToContainer({
    extract: { documentKind: 'balance_invoice', currency: 'USD', totalAmount: Math.round(lines.reduce((a, l) => a + l.amount, 0) * 100) / 100, amountDueNow: null, depositDeducted: null, lines },
    context, supplierName: SUPPLIER,
});

test('a part carton coded as QC units, its product in the box and the box adding up only with it: goods', () => {
    const c = check314([l314('JF1402', 4992), l314('JF1402_FQC', 24)]);
    assert.equal(c.verdict, 'match');
    assert.deepEqual(c.qcLines, []);
    assert.deepEqual(c.lines.map(l => [l.jfCode, l.invoiceQty, l.ourQty, l.issues]), [['JF1402', 5016, 5016, []]]);
    assert.deepEqual(c.lines[0].invoiceRows.map(r => [r.jfCode, r.qty, r.matchedBy]), [['JF1402', 4992, 'code'], ['JF1402_FQC', 24, 'qc_code_goods']]);
    // Its money is goods money, not QC units'.
    assert.deepEqual([c.qcBilled, c.invoiceGoods, c.goodsDelta, c.invoiceKind], [0, 12540, 0, 'goods']);
    // Printed the other way round: the same.
    const r = check314([l314('JF1402_FQC', 24), l314('JF1402', 4992)]);
    assert.deepEqual([r.verdict, r.qcLines.length, r.lines[0].invoiceQty], ['match', 0, 5016]);
});

test('the box holds only the goods: the line coded as QC units bills the QC units, as before', () => {
    const ctx = { ...BOX_314, purchaseOrders: [{ ...BOX_314.purchaseOrders[0], lines: [{ ...BOX_314.purchaseOrders[0].lines[0], quantity: 4992 }] }] };
    const c = check314([l314('JF1402', 4992), l314('JF1402_FQC', 24)], ctx);
    assert.equal(c.verdict, 'match');
    assert.deepEqual(c.qcLines.map(l => [l.poNumber, l.jfCode, l.ourQty, l.invoiceQty, l.issues]), [['PO_00333J', 'JF1402_FQC', 24, 24, []]]);
    assert.deepEqual(c.lines.map(l => [l.jfCode, l.invoiceQty, l.ourQty]), [['JF1402', 4992, 4992]]);
});

test('a line that says it bills inspection units stays QC units, even when the box would add up with it', () => {
    const c = check314([l314('JF1402', 4992), l314('JF1402_FQC', 24, { description: 'BY INSPECTION Cutiderm Adhesive Wound Dressing 10 x 15cm' })]);
    assert.deepEqual(c.qcLines.map(l => [l.jfCode, l.invoiceQty]), [['JF1402_FQC', 24]]);
    assert.deepEqual(c.lines.map(l => [l.jfCode, l.invoiceQty, l.ourQty]), [['JF1402', 4992, 5016]]);
    assert.equal(c.verdict, 'differs');
});

test('the reader is told a part carton is goods, whatever QC unit has its quantity', () => {
    const p = promptFor(BOX_314);
    assert.match(p, /Use an _FQC code only when the document says the line bills QC or inspection units/);
});
