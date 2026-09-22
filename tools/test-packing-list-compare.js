'use strict';

// Unit tests for the pure packing-list comparison (no DB, no Gemini).
//   node --test tools/test-packing-list-compare.js

const test = require('node:test');
const assert = require('node:assert/strict');
const {
    comparePackingList, compareDocuments, applySignOffs, toMonth, parseDims, baseJf, parseTypedReference,
} = require('../src/services/packing-list-check');
const { approvalState } = require('../src/services/packing-review');

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

// ── Several documents, sign-offs, approval ──────────────────────────────────

test('two packing lists for one supplier are pooled, with a reading check per document', () => {
    const orders = [order({ quantity: 100 }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J', lot_number: '0002001', quantity: 50 })];
    const docA = { packingListId: 11, filename: 'a.pdf', extracted: pl([row()], { totalQuantity: 100, totalCartons: 2 }) };
    const docB = { packingListId: 12, filename: 'b.pdf', extracted: pl([row({ jfCode: 'JF0002', poNumber: 'PO_00002J', lotNumber: '0002001', quantity: 50, cartons: 1 })], { totalQuantity: 999, totalCartons: 1 }) };
    const c = compareDocuments(orders, [docA, docB]);
    assert.equal(c.summary.matched, 2);
    assert.equal(c.summary.missingFromPackingList, 0);
    assert.equal(c.summary.documents, 2);
    // Each packed row remembers its document.
    assert.equal(c.lines.find(l => l.jfCode === 'JF0002').packed.rows[0].packingListId, 12);
    // b.pdf's rows don't add up to its printed total; a.pdf's do.
    assert.deepEqual(c.documents.map(d => [d.packingListId, d.ok]), [[11, true], [12, false]]);
    assert.equal(c.extractionCheck.ok, false);
    assert.equal(c.summary.verdict, 'differences');
});

test('line keys are stable across matches, misses and extras', () => {
    const c = comparePackingList(
        [order(), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00002J' })],
        pl([row(), row({ jfCode: 'JF0003', poNumber: 'PO_00003J' })])
    );
    const keys = Object.fromEntries(c.lines.map(l => [l.status, l.lineKey]));
    assert.equal(keys.match, 'JF0001|PO00001J');
    assert.equal(keys.missing, 'JF0002|PO00002J');
    assert.equal(keys.unexpected, 'JF0003|PO00003J');
    for (const l of c.lines) assert.match(l.fingerprint, /^[0-9a-f]{40}$/);
});

test('a sign-off accepts a line while it is unchanged, goes stale when it changes', () => {
    const differ = () => comparePackingList([order()], pl([row({ lotNumber: '0001002' })]));
    const first = differ();
    const line = first.lines[0];
    assert.equal(line.status, 'mismatch');
    const signOff = { id: 1, lineKey: line.lineKey, fingerprint: line.fingerprint, note: 'lot relabelled', signedByEmail: 'a@b' };

    const accepted = applySignOffs(differ(), [signOff]);
    assert.equal(accepted.lines[0].accepted, true);
    assert.equal(accepted.lines[0].signOff.stale, false);
    assert.equal(accepted.summary.signedOff, 1);
    assert.equal(accepted.summary.outstanding, 0);
    assert.equal(accepted.summary.verdict, 'accepted');

    // The quantity now differs too: the old sign-off no longer covers it.
    const changed = applySignOffs(comparePackingList([order()], pl([row({ lotNumber: '0001002', quantity: 90 })])), [signOff]);
    assert.equal(changed.lines[0].accepted, undefined);
    assert.equal(changed.lines[0].signOff.stale, true);
    assert.equal(changed.summary.outstanding, 1);
    assert.equal(changed.summary.verdict, 'differences');

    // The line was fixed: the sign-off is moot, the line just matches.
    const fixed = applySignOffs(comparePackingList([order()], pl([row()])), [signOff]);
    assert.equal(fixed.lines[0].status, 'match');
    assert.equal(fixed.lines[0].signOff.moot, true);
    assert.equal(fixed.summary.verdict, 'match');
});

test('an informational difference does not change a fingerprint', () => {
    const a = comparePackingList([order()], pl([row({ lotNumber: '0001002' })])).lines[0];
    const b = comparePackingList([order()], pl([row({ lotNumber: '0001002', grossWeightPerCarton: 12 })])).lines[0];
    assert.equal(a.fingerprint, b.fingerprint);
});

test('an approval goes stale when a line, or the paperwork, changes', () => {
    const state = (lines, ids) => ({ verdict: 'differences', outstanding: 1, discrepancyCount: 1, packingListIds: ids, lines });
    const snapshot = { overall: { outstanding: 1, suppliersWithoutList: 0 }, suppliers: {
        's:2': { supplierName: 'Sunmed', ...state({ 'JF0001|PO00001J': { status: 'mismatch', fingerprint: 'aaa', accepted: false } }, [1]) },
    } };
    const row = { id: 9, container_kind: 'booked', container_number: '324', snapshot_json: JSON.stringify(snapshot), approved_by_email: 'a@b', approved_at: new Date('2026-09-22T09:00:00Z') };
    const suppliers = [{ supplierKey: 's:2', supplierName: 'Sunmed', onBoard: true, packingLists: [{ id: 1 }] }];

    const same = approvalState(row, suppliers, new Map([['s:2', state({ 'JF0001|PO00001J': { status: 'mismatch', fingerprint: 'aaa', accepted: true } }, [1])]]));
    assert.equal(same.stale, false, 'a later sign-off is not a change to the goods');

    const lineChanged = approvalState(row, suppliers, new Map([['s:2', state({ 'JF0001|PO00001J': { status: 'mismatch', fingerprint: 'bbb', accepted: false } }, [1])]]));
    assert.equal(lineChanged.stale, true);
    assert.deepEqual(lineChanged.changesSinceApproval.map(c => c.kind), ['line_changed']);

    const newDoc = approvalState(row, suppliers, new Map([['s:2', state({ 'JF0001|PO00001J': { status: 'mismatch', fingerprint: 'aaa', accepted: false }, 'JF0002|PO00002J': { status: 'unexpected', fingerprint: 'ccc', accepted: false } }, [1, 2])]]));
    assert.deepEqual(newDoc.changesSinceApproval.map(c => c.kind).sort(), ['documents_changed', 'line_added']);
});

test('a row with no product code matches by PO, else by a unique quantity, with a warning', () => {
    // By quantity: "Tape" 10,000 is the only expected line of 10,000.
    const byQty = comparePackingList(
        [order({ jf_code: 'JF1331', po_number: 'PO_00308J', quantity: 10000, units_per_carton: 100 }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00309J', quantity: 500 })],
        pl([row({ jfCode: null, poNumber: null, lotNumber: null, quantity: 10000, cartons: 100, description: 'Tape', mfgDate: null, expDate: null })])
    );
    const tape = byQty.lines.find(l => l.jfCode === 'JF1331');
    assert.equal(tape.matchMethod, 'no_code');
    assert.equal(tape.status, 'mismatch');
    // The row prints no lot either, so that warning stands alongside.
    assert.deepEqual(tape.differences.map(d => d.field).sort(), ['lot', 'productCode']);
    assert.ok(tape.differences.every(d => d.severity === 'warning'));
    assert.equal(byQty.summary.notExpected, 0);
    assert.equal(byQty.summary.missingFromPackingList, 1);   // JF0002

    // By PO, even when the quantity is off.
    const byPo = comparePackingList(
        [order({ jf_code: 'JF1331', po_number: 'PO_00308J', quantity: 10000 })],
        pl([row({ jfCode: null, poNumber: 'PO_00308J', lotNumber: '0001001', quantity: 9000, cartons: 90, description: 'Tape' })])
    );
    assert.equal(byPo.lines[0].matchMethod, 'no_code');
    assert.deepEqual(byPo.lines[0].differences.map(d => d.field).sort(), ['cartons', 'productCode', 'quantity']);

    // Two expected lines of the same quantity: too ambiguous, the row stays unexpected.
    const ambiguous = comparePackingList(
        [order({ jf_code: 'JF1331', quantity: 500 }), order({ id: 2, jf_code: 'JF0002', po_number: 'PO_00309J', quantity: 500 })],
        pl([row({ jfCode: null, poNumber: null, quantity: 500, description: 'Tape' })])
    );
    assert.equal(ambiguous.summary.notExpected, 1);
    assert.equal(ambiguous.summary.missingFromPackingList, 2);
});
