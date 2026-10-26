'use strict';

// Unit tests for the PO PDF line snapshot rules (src/lib/po-line-snapshot.js):
// the snapshot saved at generate time, its defensive parse, and the audit-log
// rebuild used by tools/backfill-po-line-snapshots.js. No database, no AWS.
//   node --test tools/test-po-line-snapshot-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/po-line-snapshot');

// ── pdfLineSnapshot ────────────────────────────────────────────────────────
test('pdfLineSnapshot: keeps codes raw, numbers the qty and price, null price stays null', () => {
    const rows = [
        { jf_code: 'JF0974', asin: 'B07L4627TB', unit_price: '1.2000', product_name: 'Bar', quantity: 500, first_id: 3 },
        { jf_code: null, asin: 'B0ASINONLY', unit_price: null, product_name: 'Hook', quantity: '40', first_id: 9 },
    ];
    assert.deepEqual(L.pdfLineSnapshot(rows, 'generate'), {
        v: 1,
        source: 'generate',
        lines: [
            { jfCode: 'JF0974', asin: 'B07L4627TB', quantity: 500, unitPrice: 1.2 },
            { jfCode: null, asin: 'B0ASINONLY', quantity: 40, unitPrice: null },
        ],
    });
});

test('pdfLineSnapshot: empty codes become null', () => {
    assert.deepEqual(
        L.pdfLineSnapshot([{ jf_code: '', asin: '', unit_price: '0.5000', quantity: 1 }], 'generate').lines,
        [{ jfCode: null, asin: null, quantity: 1, unitPrice: 0.5 }],
    );
});

// ── parseLinesJson ─────────────────────────────────────────────────────────
test('parseLinesJson: a JSON string (mysql2 can hand back either)', () => {
    const s = '{"v":1,"source":"generate","lines":[{"jfCode":"JF1","asin":"B1","quantity":"12","unitPrice":"2.5"}]}';
    assert.deepEqual(L.parseLinesJson(s), {
        source: 'generate',
        lines: [{ jfCode: 'JF1', asin: 'B1', quantity: 12, unitPrice: 2.5 }],
    });
});

test('parseLinesJson: an already-parsed object; missing source is null', () => {
    assert.deepEqual(L.parseLinesJson({ v: 1, lines: [{ jfCode: 'JF1', quantity: 3, unitPrice: null }] }), {
        source: null,
        lines: [{ jfCode: 'JF1', asin: null, quantity: 3, unitPrice: null }],
    });
});

test('parseLinesJson: null, undefined, malformed or lines-less → null (no baseline)', () => {
    assert.equal(L.parseLinesJson(null), null);
    assert.equal(L.parseLinesJson(undefined), null);
    assert.equal(L.parseLinesJson('{not json'), null);
    assert.equal(L.parseLinesJson({ v: 1, source: 'generate' }), null);
    assert.equal(L.parseLinesJson('[1,2]'), null);
});

// ── reconstructPoLinesAt ───────────────────────────────────────────────────
const T = '2026-10-01T10:00:00.000Z';
const H = 3600 * 1000;
const at = ms => new Date(Date.parse(T) + ms);
const order = over => ({
    id: 1, jfCode: 'JF1', asin: 'B1', quantity: 100, unitPrice: 2,
    purchaseOrderId: 7, createdAt: at(-24 * H), deletedAt: null, ...over,
});
const upd = (id, orderId, ms, before) => ({ id, orderId, action: 'update', createdAt: at(ms), before });
const rebuild = (orders, orderEvents = [], poEvents = []) =>
    L.reconstructPoLinesAt({ poId: 7, at: T, orders, orderEvents, poEvents });

test('rebuild: no events → current lines', () => {
    assert.deepEqual(rebuild([order({}), order({ id: 2, jfCode: 'JF2', asin: 'B2', quantity: 50, unitPrice: 3 })]), [
        { jfCode: 'JF1', asin: 'B1', quantity: 100, unitPrice: 2 },
        { jfCode: 'JF2', asin: 'B2', quantity: 50, unitPrice: 3 },
    ]);
});

test('rebuild: a field takes the before value of the OLDEST update after T', () => {
    const events = [
        upd(10, 1, -1 * H, { quantity: 50 }),  // before T: ignored
        upd(12, 1, 2 * H, { quantity: 200 }),
        upd(11, 1, 1 * H, { quantity: 80 }),   // oldest after T
    ];
    assert.deepEqual(rebuild([order({ quantity: 300 })], events), [
        { jfCode: 'JF1', asin: 'B1', quantity: 80, unitPrice: 2 },
    ]);
});

test('rebuild: two updates in the same second → lower audit id is older', () => {
    const events = [upd(21, 1, H, { quantity: 90 }), upd(20, 1, H, { quantity: 70 })];
    assert.equal(rebuild([order({ quantity: 300 })], events)[0].quantity, 70);
});

test('rebuild: an update in the same second as generation counts as before it', () => {
    assert.equal(rebuild([order({ quantity: 20 })], [upd(30, 1, 0, { quantity: 10 })])[0].quantity, 20);
});

test('rebuild: price, jf code and asin are undone too; snake_case keys accepted', () => {
    const events = [
        upd(40, 1, H, { unitPrice: 1.75, jfCode: 'JFOLD' }),
        upd(41, 2, H, { unit_price: '1.5000', jf_code: 'JFSNAKE', asin: 'BOLD' }),
    ];
    const orders = [order({ unitPrice: 2.25 }), order({ id: 2, jfCode: 'JFNEW', asin: 'BNEW', unitPrice: 9 })];
    assert.deepEqual(rebuild(orders, events), [
        { jfCode: 'JFOLD', asin: 'B1', quantity: 100, unitPrice: 1.75 },
        { jfCode: 'JFSNAKE', asin: 'BOLD', quantity: 100, unitPrice: 1.5 },
    ]);
});

test('rebuild: a price set from null after T restores null', () => {
    assert.equal(rebuild([order({ unitPrice: 4 })], [upd(42, 1, H, { unitPrice: null })])[0].unitPrice, null);
});

test('rebuild: create/delete come from row timestamps, edges strict', () => {
    const orders = [
        order({ id: 1 }),
        order({ id: 2, jfCode: 'JFNEW', createdAt: at(1000) }),           // created after T: out
        order({ id: 3, jfCode: 'JFAT', createdAt: at(0) }),              // created at T: in
        order({ id: 4, jfCode: 'JFDELLATER', deletedAt: at(H) }),        // deleted after T: in
        order({ id: 5, jfCode: 'JFDELBEFORE', deletedAt: at(-H) }),      // deleted before T: out
        order({ id: 6, jfCode: 'JFDELAT', deletedAt: at(0) }),           // deleted at T: out
    ];
    assert.deepEqual(rebuild(orders).map(l => l.jfCode), ['JF1', 'JFAT', 'JFDELLATER']);
});

test('rebuild: moved onto this PO after T is out; moved off it after T is in', () => {
    const orders = [
        order({ id: 1 }),
        order({ id: 2, jfCode: 'JFMOVEDIN', purchaseOrderId: 7 }),
        order({ id: 3, jfCode: 'JFMOVEDOUT', purchaseOrderId: 9 }),
    ];
    const events = [
        upd(50, 2, H, { purchaseOrderId: 8 }),
        upd(51, 3, H, { purchaseOrderId: '7' }),
    ];
    assert.deepEqual(rebuild(orders, events).map(l => l.jfCode), ['JF1', 'JFMOVEDOUT']);
});

test('rebuild: PO-level attach/detach events (importer relink writes no order diff)', () => {
    const orders = [
        order({ id: 1 }),
        order({ id: 2, jfCode: 'JFATTACHED', purchaseOrderId: 7 }),
        order({ id: 3, jfCode: 'JFDETACHED', purchaseOrderId: null }),
        order({ id: 4, jfCode: 'JFOTHERPO', purchaseOrderId: 7 }),
    ];
    const poEvents = [
        { id: 60, poId: 7, action: 'order_attached', orderId: 2, createdAt: at(H) },
        { id: 61, poId: 7, action: 'order_detached', orderId: 3, createdAt: at(H) },
        { id: 62, poId: 8, action: 'order_attached', orderId: 4, createdAt: at(H) }, // another PO: ignored
    ];
    assert.deepEqual(rebuild(orders, [], poEvents).map(l => l.jfCode), ['JF1', 'JFDETACHED', 'JFOTHERPO']);
});

test('rebuild: the oldest membership event wins across order and PO events', () => {
    const orders = [order({ id: 1, purchaseOrderId: 7 })];
    const orderEvents = [upd(71, 1, 2 * H, { purchaseOrderId: null })];          // re-attached later
    const poEvents = [{ id: 70, poId: 7, action: 'order_detached', orderId: 1, createdAt: at(H) }];
    assert.deepEqual(rebuild(orders, orderEvents, poEvents).map(l => l.jfCode), ['JF1']);
});

test('rebuild: split after T — clone created later, original qty restored, same total', () => {
    const orders = [
        order({ id: 1, quantity: 60 }),
        order({ id: 9, quantity: 40, createdAt: at(H) }),
    ];
    assert.deepEqual(rebuild(orders, [upd(80, 1, H, { quantity: 100 })]), [
        { jfCode: 'JF1', asin: 'B1', quantity: 100, unitPrice: 2 },
    ]);
});

test('rebuild: rows with the same codes and price are summed, ordered by lowest order id', () => {
    const orders = [
        order({ id: 4, quantity: 10 }),
        order({ id: 3, jfCode: 'JF3', asin: 'B3', quantity: 5, unitPrice: 1 }),
        order({ id: 2, quantity: 15 }),
        order({ id: 5, quantity: 1, unitPrice: 2.5 }),
    ];
    assert.deepEqual(rebuild(orders), [
        { jfCode: 'JF1', asin: 'B1', quantity: 25, unitPrice: 2 },
        { jfCode: 'JF3', asin: 'B3', quantity: 5, unitPrice: 1 },
        { jfCode: 'JF1', asin: 'B1', quantity: 1, unitPrice: 2.5 },
    ]);
});

// ── countDrift ─────────────────────────────────────────────────────────────
const line = (jfCode, quantity, unitPrice, asin = null) => ({ jfCode, asin, quantity, unitPrice });

test('countDrift: identical → 0', () => {
    assert.equal(L.countDrift([line('JF1', 10, 2)], [line('JF1', 10, 2)]), 0);
});

test('countDrift: qty or price change on one item → 1', () => {
    assert.equal(L.countDrift([line('JF1', 10, 2), line('JF2', 5, 1)], [line('JF1', 11, 2), line('JF2', 5, 1)]), 1);
    assert.equal(L.countDrift([line('JF1', 10, 2)], [line('JF1', 10, 2.01)]), 1);
});

test('countDrift: item swap → 2 (old removed, new added)', () => {
    assert.equal(L.countDrift([line('JF1', 10, 2)], [line('JF2', 10, 2)]), 2);
});

test('countDrift: split rows of the same item and price → 0', () => {
    assert.equal(L.countDrift([line('JF1', 10, 2)], [line('JF1', 6, 2), line('JF1', 4, 2)]), 0);
});

test('countDrift: code case/whitespace, asin-only items, null price = 0 → 0', () => {
    assert.equal(L.countDrift(
        [line(' jf1 ', 10, null), line(null, 3, 1, 'b0asin')],
        [line('JF1', 10, 0), line('', 3, 1, 'B0ASIN')],
    ), 0);
});

test('countDrift: price compared at 4 dp', () => {
    assert.equal(L.countDrift([line('JF1', 10, 1.23454)], [line('JF1', 10, 1.2345)]), 0);
    assert.equal(L.countDrift([line('JF1', 10, 1.2346)], [line('JF1', 10, 1.2345)]), 1);
});
