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
