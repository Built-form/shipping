'use strict';

// Unit tests for the pure packing-list comparison (no DB, no Gemini).
//   node --test tools/test-packing-list-compare.js

const test = require('node:test');
const assert = require('node:assert/strict');
const { comparePackingList, toMonth, parseDims, baseJf, parseTypedReference } = require('../src/services/packing-list-check');

const order = (o) => ({
    id: 1, jf_code: 'JF0001', po_number: 'PO_00001J', lot_number: '0001001', quantity: 100,
    units_per_carton: 50, carton_weight: '10.000', carton_height: '30.000', carton_width: '20.000',
    carton_depth: '40.000', mfg_date: '2026-07-01', exp_date: '2031-06-01', status: 'CONSOLIDATED',
    product_name: 'Thing', ...o,
});
const row = (r) => ({
    jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: '0001001', mfgDate: '2026-07', expDate: '2031-06',
    description: 'Thing', cartons: 2, quantity: 100, unit: 'BOX', grossWeightPerCarton: 10,
    netWeightPerCarton: 9, cartonDimensionsCm: '40X20X30', ...r,
});
const pl = (lines, totals = {}) => ({ lines, totalQuantity: null, totalCartons: null, ...totals });

test('helpers', () => {
    assert.equal(toMonth('07.2031'), '2031-07');
    assert.equal(toMonth('2031-7'), '2031-07');
    assert.equal(toMonth('2031-06-01'), '2031-06');
    assert.equal(toMonth(null), null);
    assert.deepEqual(parseDims('59.5X25X4 5'.replace(' ', '')), [25, 45, 59.5]);
    assert.equal(parseDims('junk'), null);
    assert.equal(baseJf('JF0197_FQC'), 'JF0197');
});

test('an exact match has no differences', () => {
    const c = comparePackingList([order()], pl([row()], { totalQuantity: 100, totalCartons: 2 }));
    assert.equal(c.summary.verdict, 'match');
    assert.equal(c.lines[0].status, 'match');
    assert.deepEqual(c.lines[0].differences, []);
    assert.equal(c.extractionCheck.ok, true);
});

test('split rows (full + part carton) sum to one order', () => {
    const c = comparePackingList(
        [order({ quantity: 8080, units_per_carton: 500 })],
        pl([row({ cartons: 16, quantity: 8000 }), row({ cartons: 1, quantity: 80 })])
    );
    assert.equal(c.lines[0].status, 'match');
    assert.equal(c.lines[0].packed.quantity, 8080);
});

test('quantity, lot and expiry differences are errors', () => {
    const c = comparePackingList([order()], pl([row({ quantity: 90, cartons: 2, lotNumber: '0001002', expDate: '2031-07' })]));
    const f = Object.fromEntries(c.lines[0].differences.map(d => [d.field, d.severity]));
    assert.equal(f.quantity, 'error');
    assert.equal(f.lot, 'error');
    assert.equal(f.expDate, 'error');
    assert.equal(c.lines[0].status, 'mismatch');
});

test('lots compare without leading zeros', () => {
    const c = comparePackingList([order({ lot_number: '1001' })], pl([row({ lotNumber: '0001001' })]));
    assert.equal(c.lines[0].status, 'match');
});

test('carton weight/size differences are info only and do not fail the line', () => {
    const c = comparePackingList([order()], pl([row({ grossWeightPerCarton: 12, cartonDimensionsCm: '45X20X30' })]));
    assert.equal(c.lines[0].status, 'match');
    assert.deepEqual(c.lines[0].differences.map(d => d.severity), ['info', 'info']);
});

test('PO missing on the packing list falls back to product code with a warning', () => {
    const c = comparePackingList([order()], pl([row({ poNumber: null })]));
    assert.equal(c.lines.length, 1);
    assert.equal(c.lines[0].matchMethod, 'jf_only');
    assert.equal(c.lines[0].differences[0].field, 'poNumber');
});

test('missing and unexpected lines', () => {
    const c = comparePackingList(
        [order(), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J' })],
        pl([row(), row({ jfCode: 'JF0003', poNumber: 'PO_00003J' })])
    );
    const byStatus = Object.fromEntries(c.lines.map(l => [l.status, l.jfCode]));
    assert.equal(byStatus.missing, 'JF0002');
    assert.equal(byStatus.unexpected, 'JF0003');
    assert.equal(c.summary.discrepancyCount, 2);
});

test('FQC sample orders group with their base product', () => {
    const c = comparePackingList(
        [order({ quantity: 100 }), order({ id: 2, jf_code: 'JF0001_FQC', quantity: 5 })],
        pl([row({ quantity: 105, cartons: 3 })])
    );
    assert.equal(c.lines.length, 1);
    assert.equal(c.lines[0].status, 'match');
});

test('a split line compares against this container\'s share, not the whole order', () => {
    // Draft line: 45,000 of a 192,600 order are in this container.
    const draftLine = order({ quantity: 45000, order_quantity: 192600, allocation_id: 7, units_per_carton: 300 });
    const ok = comparePackingList([draftLine], pl([row({ quantity: 45000, cartons: 150 })]));
    assert.equal(ok.lines[0].status, 'match');
    assert.equal(ok.lines[0].expected.orders[0].quantity, 45000);
    assert.equal(ok.lines[0].expected.orders[0].orderQuantity, 192600);

    const whole = comparePackingList([draftLine], pl([row({ quantity: 192600, cartons: 642 })]));
    const qty = whole.lines[0].differences.find(d => d.field === 'quantity');
    assert.equal(qty.severity, 'error');
    assert.equal(qty.expected, 45000);
    assert.equal(qty.actual, 192600);
});

test('split siblings share their part carton (rounded up once per carton size)', () => {
    // 350 + 470 at 20/ctn = 820 units = 41 cartons, not 18 + 24 = 42.
    const c = comparePackingList(
        [order({ quantity: 350, units_per_carton: 20 }), order({ id: 2, quantity: 470, units_per_carton: 20 })],
        pl([row({ quantity: 820, cartons: 41 })])
    );
    assert.equal(c.lines[0].expected.cartons, 41);
    assert.equal(c.lines[0].status, 'match');
});

test('typed container references', () => {
    assert.deepEqual(parseTypedReference('328'), { seq: 328, mode: 'SEA', bare: true });
    for (const t of ['123. Air Freight', '123. air freight', '123. Airfreight', '123 air freight']) {
        assert.deepEqual(parseTypedReference(t), { seq: 123, mode: 'AIR', bare: false }, t);
    }
    assert.equal(parseTypedReference('324 Sunmed'), null);
    assert.equal(parseTypedReference('MRKU4645188'), null);
    assert.equal(parseTypedReference(''), null);
});

test('rows not adding up to the printed total flag the extraction', () => {
    const c = comparePackingList([order()], pl([row()], { totalQuantity: 150, totalCartons: 2 }));
    assert.equal(c.extractionCheck.ok, false);
    assert.equal(c.summary.verdict, 'differences');
});
