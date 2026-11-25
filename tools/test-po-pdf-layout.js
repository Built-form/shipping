'use strict';

// Unit tests for src/services/po-pdf.js — where the header block puts the
// supplier's name and address. Builds a real PDF and records every text() call
// pdfkit receives along the way. No database, no S3.
//   node --test tools/test-po-pdf-layout.js

const test = require('node:test');
const assert = require('node:assert/strict');
const PDFDocument = require('pdfkit');
const { buildPoPdf } = require('../src/services/po-pdf');

const LINES = [{ sku: 'JF1001', name: 'Nitrile Gloves', quantity: 100, unitPrice: 1.5 }];

// About 350pt wide in bold 10pt Helvetica: too long for one line of the
// supplier column, short enough for two.
const LONG_NAME = 'Turkuaz Saglik Hizmetleri Medikal Temizlik Kimyasal Urunler San. ve Tic. A.S.';
const SHORT_NAME = 'Suzhou Sunmed Co., Ltd.';

// Every text() call buildPoPdf makes, in order.
async function textCalls(po) {
    const calls = [];
    const original = PDFDocument.prototype.text;
    PDFDocument.prototype.text = function (text, x, y, options) {
        calls.push({ text, x, y, options });
        return original.apply(this, arguments);
    };
    try {
        await buildPoPdf({ poNumber: 'PO_00406J', orderDate: '2026-09-23', ...po }, LINES);
    } finally {
        PDFDocument.prototype.text = original;
    }
    return calls;
}

const callFor = (calls, text) => calls.find(c => c.text === text);

test('a long supplier name wraps inside the supplier column, clear of the delivery address', async () => {
    const calls = await textCalls({ supplier: LONG_NAME });
    const name = callFor(calls, LONG_NAME);
    assert.equal(name.x, 50);
    assert.equal(name.y, 180);
    // The delivery column starts at x=280, so the name may reach 270 at most.
    assert.equal(name.options?.width, 220);
});

test('the supplier address starts below a name that wrapped onto two lines', async () => {
    const calls = await textCalls({ supplier: LONG_NAME, supplierAddress: ['Organize Sanayi', 'Istanbul'] });
    // Two 12pt lines of name from y=180, then the usual 3pt gap.
    assert.equal(callFor(calls, 'Organize Sanayi').y, 207);
    assert.equal(callFor(calls, 'Istanbul').y, 219);
});

test('a short supplier name leaves the address where it has always been', async () => {
    const calls = await textCalls({ supplier: SHORT_NAME, supplierAddress: ['Suzhou', 'China'] });
    assert.equal(callFor(calls, SHORT_NAME).y, 180);
    assert.equal(callFor(calls, 'Suzhou').y, 195);
    assert.equal(callFor(calls, 'China').y, 207);
});

test('a purchase order with no supplier still prints a dash in the supplier column', async () => {
    const calls = await textCalls({ supplier: '' });
    const dash = calls.find(c => c.text === '-' && c.x === 50);
    assert.equal(dash.y, 180);
});

// ── A sample order (docs/sample-orders.md) ───────────────────────────────────
// Its own heading, the sample code on each line, and no price printed for a
// sample that has none.

async function textsOf(po, lines) {
    const texts = [];
    const original = PDFDocument.prototype.text;
    PDFDocument.prototype.text = function (text) {
        texts.push(String(text));
        return original.apply(this, arguments);
    };
    try {
        await buildPoPdf({ poNumber: 'PO_00431J', orderDate: '2026-11-25', supplier: SHORT_NAME, currency: 'USD', ...po }, lines);
    } finally {
        PDFDocument.prototype.text = original;
    }
    return texts;
}

const FREE = { sku: 'JF1016', name: 'Wrist Brace', quantity: 2, unitPrice: 0, sampleCode: 'SA0031-S01-V03-CN0011' };
const PRICED = { sku: 'JF1017', name: 'Knee Brace', quantity: 3, unitPrice: 4.5, sampleCode: 'SA0032-S01-V01-CN0011' };

test('a stock PO is headed PURCHASE ORDER and prints a price of 0 as it always has', async () => {
    const texts = await textsOf({}, [{ sku: 'JF1001', name: 'Nitrile Gloves', quantity: 100, unitPrice: 0 }]);
    assert.equal(texts[0], 'PURCHASE ORDER');
    assert.ok(texts.includes('0.00000'));
    assert.ok(texts.includes('Total Cost'));
    assert.equal(texts.some(t => /Sample code/.test(t)), false);
});

test('a sample order is headed SAMPLE ORDER and names the sample code on each line', async () => {
    const texts = await textsOf({ kind: 'sample' }, [PRICED]);
    assert.equal(texts[0], 'SAMPLE ORDER');
    assert.equal(texts.includes('PURCHASE ORDER'), false);
    assert.ok(texts.includes('Knee Brace\nSample code: SA0032-S01-V01-CN0011'));
    assert.ok(texts.includes('4.50000'));
    assert.ok(texts.includes('13.50'));
    assert.ok(texts.includes('Total Cost'));
});

// The user, 2026-11-25: "exclude the price column dont say free". A sample with
// no price is not priced at 0 and is not called free on the paper: the price
// columns are simply not there.

// Every text() call with where it was put, for a sample order.
async function callsOf(po, lines) {
    const calls = [];
    const original = PDFDocument.prototype.text;
    PDFDocument.prototype.text = function (text, x, y, options) {
        calls.push({ text: String(text), x, y, options });
        return original.apply(this, arguments);
    };
    try {
        await buildPoPdf({ poNumber: 'PO_00431J', orderDate: '2026-11-25', supplier: SHORT_NAME, currency: 'USD', ...po }, lines);
    } finally {
        PDFDocument.prototype.text = original;
    }
    return calls;
}

const saysFree = texts => texts.some(t => /free/i.test(t));

test('a sample order with no priced line has no price columns, no totals and no word about price', async () => {
    const texts = await textsOf({ kind: 'sample', shippingTotal: 0 }, [FREE]);
    assert.ok(texts.includes('Ref'));
    assert.ok(texts.includes('Item'));
    assert.ok(texts.includes('Quantity'));
    assert.equal(texts.includes('Unit Price'), false);
    assert.equal(texts.includes('Line Price'), false);
    assert.equal(saysFree(texts), false);
    assert.equal(texts.some(t => /^0\.0+$/.test(t)), false);
    for (const label of ['Currency', 'Shipping Total', 'Total Cost']) assert.equal(texts.includes(label), false, label);
    // The line itself and the rest of the page are still there.
    assert.ok(texts.includes('Wrist Brace\nSample code: SA0031-S01-V03-CN0011'));
    assert.ok(texts.includes('2'));
    assert.ok(texts.includes('Comments:'));
});

test('with no price columns the item takes their room and the quantity sits at the table\'s right edge', async () => {
    const calls = await callsOf({ kind: 'sample' }, [FREE]);
    const at = text => calls.find(c => c.text === text);
    assert.deepEqual([at('Ref').x, at('Ref').options.width], [50, 70]);
    assert.deepEqual([at('Item').x, at('Item').options.width], [120, 340]);
    assert.deepEqual([at('Quantity').x, at('Quantity').options.width], [460, 80]);
    const item = at('Wrist Brace\nSample code: SA0031-S01-V03-CN0011');
    assert.deepEqual([item.x, item.options.width], [120, 340]);
    const qty = calls.find(c => c.text === '2' && c.x === 460);
    assert.ok(qty, 'the quantity is drawn in the last column');
    // The table still ends where it always did: 50 + 490.
    assert.equal(at('Quantity').x + at('Quantity').options.width, 540);
    // A priced sample order keeps the five columns where they were.
    const priced = await callsOf({ kind: 'sample' }, [PRICED]);
    const p = text => priced.find(c => c.text === text);
    assert.deepEqual([p('Item').x, p('Item').options.width], [120, 200]);
    assert.deepEqual([p('Quantity').x, p('Unit Price').x, p('Line Price').x], [320, 380, 450]);
});

test('unpriced lines with a shipping charge: still no price columns, and the totals show what is owed', async () => {
    const texts = await textsOf({ kind: 'sample', shippingTotal: 25 }, [FREE]);
    assert.equal(texts.includes('Unit Price'), false);
    assert.equal(texts.includes('Line Price'), false);
    assert.equal(saysFree(texts), false);
    assert.ok(texts.includes('Shipping Total'));
    assert.ok(texts.includes('Total Cost'));
    assert.equal(texts.filter(t => t === '25.00').length, 2);
});

test('an unpriced line beside a priced one: the price columns stay, and the unpriced line\'s cells are left empty', async () => {
    const calls = await callsOf({ kind: 'sample' }, [FREE, PRICED]);
    const texts = calls.map(c => c.text);
    assert.ok(texts.includes('Unit Price'));
    assert.ok(texts.includes('Line Price'));
    assert.equal(saysFree(texts), false);
    // The priced line's price and total; the order's total.
    assert.ok(texts.includes('4.50000'));
    assert.equal(texts.filter(t => t === '13.50').length, 2);
    assert.ok(texts.includes('Total Cost'));
    // Nothing is printed in the price columns of the unpriced line: no 0, no word.
    assert.equal(texts.includes('0.00000'), false);
    const inPriceColumns = calls.filter(c => (c.x === 380 || c.x === 450) && c.options && c.options.width === 70);
    assert.deepEqual(inPriceColumns.map(c => c.text), ['Unit Price', 'Line Price', '4.50000', '13.50']);
});

test('a stock PO keeps its price columns and its 0 whatever its lines cost', async () => {
    const texts = await textsOf({ kind: 'stock', shippingTotal: 0 }, [{ sku: 'JF1001', name: 'Nitrile Gloves', quantity: 100, unitPrice: 0 }]);
    assert.ok(texts.includes('Unit Price'));
    assert.ok(texts.includes('Line Price'));
    assert.ok(texts.includes('0.00000'));
    assert.ok(texts.includes('Total Cost'));
});
