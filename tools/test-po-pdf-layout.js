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
