'use strict';

// Unit tests for the Request to send staleness rules
// (src/lib/request-to-send-drift.js): the snapshot saved at generate time, the
// current-lines mapping, the CSV backfill parse, and the diff. No database.
//   node --test tools/test-request-to-send-drift-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/request-to-send-drift');
// Pure helpers of the read-side service (it only touches the DB through `conn`).
const { flaggable, latestPerSupplier } = require('../src/services/request-to-send-status');

// A PDF line as loadForwarderQuoteForPdf builds it (only the fields that matter).
const pdf = (jfCode, lot, orderedUnits, extra = {}) => ({
    sku: extra.sku ?? `${jfCode} name`, jfCode, lot, orderedUnits, supplier: extra.supplier ?? 'Sunmed',
    cartonWeight: 9, unitsPerCarton: 50, port: 'Ningbo', poNumber: 'PO_00410J',
});
// A snapshot / current line.
const ln = (jfCode, lot, units, sku = null) => ({ jfCode, sku: sku ?? (jfCode ? `${jfCode} name` : null), lot, units, supplier: 'Sunmed' });

// ── Snapshot + mapping ─────────────────────────────────────────────────────
test('requestLinesSnapshot: keeps item, lot, units, supplier; drops carton/port/PO', () => {
    assert.deepEqual(L.requestLinesSnapshot([pdf('JF1357', 'L-01', 2000), pdf('', '', 10, { sku: 'MXT-EAB' })], 'generate'), {
        v: 1,
        source: 'generate',
        lines: [
            { jfCode: 'JF1357', sku: 'JF1357 name', lot: 'L-01', units: 2000, supplier: 'Sunmed' },
            { jfCode: null, sku: 'MXT-EAB', lot: null, units: 10, supplier: 'Sunmed' },
        ],
    });
});

test('currentRequestLines: same sku rule as the PDF (name, else jf, else asin)', () => {
    const rows = [
        { jf_code: 'JF1', asin: 'B1', product_name: 'Tape', lot_number: 'L1', allocated: '12', supplier: ' Sunmed ' },
        { jf_code: 'JF2', asin: 'B2', product_name: null, lot_number: null, allocated: 3, supplier: 'Sunmed' },
        { jf_code: null, asin: 'B3', product_name: '', lot_number: '', allocated: 1, supplier: null },
    ];
    assert.deepEqual(L.currentRequestLines(rows), [
        { jfCode: 'JF1', sku: 'Tape', lot: 'L1', units: 12, supplier: 'Sunmed' },
        { jfCode: 'JF2', sku: 'JF2', lot: null, units: 3, supplier: 'Sunmed' },
        { jfCode: null, sku: 'B3', lot: null, units: 1, supplier: null },
    ]);
});

test('parseRequestLines: string or object; anything unusable → null', () => {
    assert.deepEqual(
        L.parseRequestLines('{"v":1,"source":"csv-backfill","lines":[{"jfCode":"JF1","sku":"x","lot":"L1","units":"5","supplier":"S"}]}'),
        { source: 'csv-backfill', lines: [{ jfCode: 'JF1', sku: 'x', lot: 'L1', units: 5, supplier: 'S' }] },
    );
    assert.deepEqual(L.parseRequestLines({ v: 1, lines: [{ jfCode: 'JF1', units: 2 }] }), {
        source: null, lines: [{ jfCode: 'JF1', sku: null, lot: null, units: 2, supplier: null }],
    });
    for (const bad of [null, undefined, '{oops', { v: 1 }, '[]']) assert.equal(L.parseRequestLines(bad), null);
});

test('sameSupplier: trim and case only', () => {
    assert.equal(L.sameSupplier(' Suzhou Sunmed Co.,Ltd. ', 'suzhou sunmed co.,ltd.'), true);
    assert.equal(L.sameSupplier('Sunmed', 'Sunmed Ltd'), false);
    assert.equal(L.sameSupplier(null, ''), true);
});

// ── CSV backfill ───────────────────────────────────────────────────────────
test('csvRequestLines: the stored Request-to-send CSV (BOM, CRLF, quoted cells)', () => {
    const csv = '﻿SKU,HW/JF Code,LOT,Ordered Units,Carton Weight (kg),Units per Carton,Carton H (cm),Carton L (cm),Carton W (cm),Carton CBM,No. of Cartons,Total CBM,Total Weight (kg),Supplier,Port,PO Number\r\n'
        + '"MaxTape EAB Tape WHITE 5cm x 4.5m, Pack of 6",JF1357,L-01,2000,9.00,50,30.0,40.0,20.0,0.0240,40,0.9600,360.00,"Suzhou Sunmed Co.,Ltd.",Ningbo,PO_00410J\r\n'
        + 'MXT-EAB-5.0-6WHT,,,10,,,,,,,,,,"Suzhou Sunmed Co.,Ltd.",Ningbo,PO_00410J\r\n';
    assert.deepEqual(L.csvRequestLines(csv), [
        { jfCode: 'JF1357', sku: 'MaxTape EAB Tape WHITE 5cm x 4.5m, Pack of 6', lot: 'L-01', units: 2000, supplier: 'Suzhou Sunmed Co.,Ltd.' },
        { jfCode: null, sku: 'MXT-EAB-5.0-6WHT', lot: null, units: 10, supplier: 'Suzhou Sunmed Co.,Ltd.' },
    ]);
});

test('csvRequestLines: not a Request-to-send CSV → null', () => {
    assert.equal(L.csvRequestLines('SKU,Qty\r\nX,1\r\n'), null);
    assert.equal(L.csvRequestLines(''), null);
});

// ── requestDrift ───────────────────────────────────────────────────────────
test('requestDrift: no snapshot → null (no baseline)', () => {
    assert.equal(L.requestDrift(null, [ln('JF1', 'L1', 5)]), null);
});

test('requestDrift: unchanged → clean', () => {
    assert.deepEqual(L.requestDrift([ln('JF1', 'L1', 5), ln('JF2', 'L2', 7)], [ln('JF1', 'L1', 5), ln('JF2', 'L2', 7)]),
        { stale: false, changes: [] });
});

test('requestDrift: units changed', () => {
    assert.deepEqual(L.requestDrift([ln('JF1', 'L1', 500)], [ln('JF1', 'L1', 600)]), {
        stale: true, changes: [{ kind: 'units', item: 'JF1', lot: 'L1', unitsWas: 500, unitsNow: 600 }],
    });
});

test('requestDrift: lot changed (units carried when they changed too)', () => {
    assert.deepEqual(L.requestDrift([ln('JF1', 'L1', 500)], [ln('JF1', 'L2', 500)]).changes,
        [{ kind: 'lot', item: 'JF1', lotWas: 'L1', lot: 'L2' }]);
    assert.deepEqual(L.requestDrift([ln('JF1', 'L1', 500)], [ln('JF1', null, 450)]).changes,
        [{ kind: 'lot', item: 'JF1', lotWas: 'L1', lot: null, unitsWas: 500, unitsNow: 450 }]);
});

test('requestDrift: item added and item removed, snapshot items first', () => {
    assert.deepEqual(L.requestDrift([ln('JF1', 'L1', 5), ln('JF2', null, 3)], [ln('JF1', 'L1', 5), ln('JF9', 'L9', 8)]).changes, [
        { kind: 'removed', item: 'JF2', lot: null, unitsWas: 3 },
        { kind: 'added', item: 'JF9', lot: 'L9', unitsNow: 8 },
    ]);
});

test('requestDrift: split rows or a partial pack with the same total → clean', () => {
    assert.equal(L.requestDrift([ln('JF1', 'L1', 100)], [ln('JF1', 'L1', 60), ln('JF1', 'L1', 40)]).stale, false);
    assert.equal(L.requestDrift([ln('JF1', 'L1', 60), ln('JF1', 'L1', 40)], [ln('JF1', 'L1', 100)]).stale, false);
});

test('requestDrift: one item in two lots, only the lot that moved is reported', () => {
    assert.deepEqual(L.requestDrift(
        [ln('JF1', 'L1', 10), ln('JF1', 'L2', 5)],
        [ln('JF1', 'L1', 10), ln('JF1', 'L3', 5)],
    ).changes, [
        { kind: 'removed', item: 'JF1', lot: 'L2', unitsWas: 5 },
        { kind: 'added', item: 'JF1', lot: 'L3', unitsNow: 5 },
    ]);
    assert.deepEqual(L.requestDrift(
        [ln('JF1', 'L1', 10), ln('JF1', 'L2', 5)],
        [ln('JF1', 'L1', 10), ln('JF1', 'L2', 8)],
    ).changes, [{ kind: 'units', item: 'JF1', lot: 'L2', unitsWas: 5, unitsNow: 8 }]);
});

test('requestDrift: case and whitespace in item and lot are not changes', () => {
    assert.equal(L.requestDrift([ln(' jf1 ', 'l-01 ', 5)], [ln('JF1', 'L-01', 5)]).stale, false);
});

test('requestDrift: no JF code → keyed by sku', () => {
    assert.equal(L.requestDrift([ln(null, null, 5, 'Hook')], [ln('', null, 5, 'HOOK')]).stale, false);
    assert.deepEqual(L.requestDrift([ln(null, null, 5, 'Hook')], [ln(null, null, 5, 'Bar')]).changes, [
        { kind: 'removed', item: 'Hook', lot: null, unitsWas: 5 },
        { kind: 'added', item: 'Bar', lot: null, unitsNow: 5 },
    ]);
});

test('requestDrift: JF code wins over sku, so a product rename is not a change', () => {
    assert.equal(L.requestDrift([ln('JF1', 'L1', 5, 'Old name')], [ln('JF1', 'L1', 5, 'New name')]).stale, false);
});

test('requestDrift: every line removed (supplier gone from the container)', () => {
    assert.deepEqual(L.requestDrift([ln('JF1', 'L1', 5)], []), {
        stale: true, changes: [{ kind: 'removed', item: 'JF1', lot: 'L1', unitsWas: 5 }],
    });
});

// ── Read-side rules (src/services/request-to-send-status.js) ──────────────
test('flaggable: drafts and booked containers until the goods arrive', () => {
    assert.equal(flaggable({ stage: 'DRAFT' }, []), true);
    assert.equal(flaggable({ stage: 'BOOKED' }, ['CONSOLIDATED']), true);
    assert.equal(flaggable({ stage: 'BOOKED' }, ['CONSOLIDATED', 'ON_SEA']), true, 'in transit');
    assert.equal(flaggable({ stage: 'IN_TRANSIT' }, []), true);
    assert.equal(flaggable({ stage: 'BOOKED' }, ['ON_SEA', 'ARRIVED_AT_WAREHOUSE']), false, 'arrived');
    assert.equal(flaggable({ stage: 'BOOKED' }, ['RECEIVED']), false, 'received (stored stage lags)');
    assert.equal(flaggable({ stage: 'ARRIVED' }, ['ON_SEA']), false, 'stored stage never goes back');
});

test('latestPerSupplier: newest Request to send per supplier (trim/case), other types ignored', () => {
    const rows = [
        { id: 9, type: 'forwarder-quote', supplier: 'Sunmed' },
        { id: 8, type: 'supplier-quote', supplier: 'Sunmed ' },
        { id: 7, type: 'supplier-quote', supplier: 'Roosin' },
        { id: 6, type: 'supplier-quote', supplier: 'sunmed' },
    ];
    assert.deepEqual(latestPerSupplier(rows).map(d => d.id), [8, 7]);
});

test('a lot of "-" is no lot (the CSV writer blanks "-" cells, the PDF prints them)', () => {
    assert.equal(L.requestDrift([ln(null, '', 1000, 'AMF-MINI-BAG')], [ln(null, '-', 1000, 'AMF-MINI-BAG')]).stale, false);
    assert.deepEqual(L.currentRequestLines([{ jf_code: 'JF1', lot_number: ' - ', allocated: 1 }])[0].lot, null);
});
