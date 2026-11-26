'use strict';

// Unit tests for src/lib/po-change-requests.js — supplier changes staged in jportal.
// Run: node --test tools/test-po-change-requests-lib.js

const { test } = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/po-change-requests');

const line = (over = {}) => ({
    id: 10, purchaseOrderId: 7, status: 'IN_PRODUCTION',
    estimatedReadyDate: '2026-11-01', actualReadyDate: null, ...over,
});

test('a date change writes only the column that changes', () => {
    const plan = L.planApply({ estimatedReadyDate: '2026-11-20' }, line(), 7, true);
    assert.deepEqual(plan.blockers, []);
    assert.deepEqual(plan.cols, { estimated_ready_date: '2026-11-20' });
    assert.deepEqual(plan.before, { estimatedReadyDate: '2026-11-01' });
    assert.deepEqual(plan.after, { estimatedReadyDate: '2026-11-20' });
    assert.equal(plan.moveToQc, false);
});

test('null clears a date; a value already live is a no-op', () => {
    assert.deepEqual(L.planApply({ estimatedReadyDate: null }, line(), 7, true).cols, { estimated_ready_date: null });
    const same = L.planApply({ estimatedReadyDate: '2026-11-01' }, line(), 7, true);
    assert.deepEqual(same.blockers, []);
    assert.deepEqual(same.cols, {});
});

test('ready for QC with the actual date in the same request', () => {
    const plan = L.planApply({ actualReadyDate: '2026-10-14', markReadyForQc: true }, line(), 7, true);
    assert.deepEqual(plan.blockers, []);
    assert.deepEqual(plan.cols, { actual_ready_date: '2026-10-14' });
    assert.equal(plan.moveToQc, true);
    assert.deepEqual(plan.after, { actualReadyDate: '2026-10-14', status: 'READY_FOR_QC' });
});

test('ready for QC needs an actual ready date, stored or requested', () => {
    assert.deepEqual(L.planApply({ markReadyForQc: true }, line(), 7, true).blockers, ['qc_no_date']);
    assert.deepEqual(L.planApply({ markReadyForQc: true }, line({ actualReadyDate: '2026-10-10' }), 7, true).blockers, []);
    // Clearing the date in the same request leaves none.
    assert.deepEqual(
        L.planApply({ actualReadyDate: null, markReadyForQc: true }, line({ actualReadyDate: '2026-10-10' }), 7, true).blockers,
        ['qc_no_date']
    );
});

test('ready for QC only from IN_PRODUCTION; already there is a no-op', () => {
    assert.deepEqual(L.planApply({ markReadyForQc: true }, line({ status: 'PO_SENT', actualReadyDate: '2026-10-10' }), 7, true).blockers, ['qc_from']);
    const already = L.planApply({ markReadyForQc: true }, line({ status: 'READY_FOR_QC' }), 7, true);
    assert.deepEqual(already.blockers, []);
    assert.equal(already.moveToQc, false);
});

test('a line past the factory, deleted or on another PO is blocked', () => {
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, line({ status: 'ON_SEA' }), 7, true).blockers, ['line_locked']);
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, null, 7, true).blockers, ['line_missing']);
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, line({ purchaseOrderId: 8 }), 7, true).blockers, ['line_missing']);
});

test('a row with no purchase order id fails closed as line_missing', () => {
    for (const po of [null, undefined]) {
        const plan = L.planApply({ estimatedReadyDate: '2026-12-01' }, line(), po, true);
        assert.deepEqual(plan.blockers, ['line_missing']);
        assert.deepEqual(plan.cols, {});
    }
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, line({ purchaseOrderId: null }), null, true).blockers, ['line_missing']);
});

test('a PO deleted or no longer the supplier\'s is blocked; only true lets it through', () => {
    for (const owned of [false, undefined, null, 1, 'true']) {
        const plan = L.planApply({ estimatedReadyDate: '2026-12-01' }, line(), 7, owned);
        assert.deepEqual(plan.blockers, ['po_supplier_changed']);
        assert.deepEqual(plan.cols, {});
        assert.equal(plan.moveToQc, false);
    }
    // A line that is gone says so first.
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, null, 7, false).blockers, ['line_missing']);
    assert.ok(L.BLOCKERS.po_supplier_changed);
});

test('bad dates and empty requests are blocked, nothing written', () => {
    const bad = L.planApply({ estimatedReadyDate: '2026-02-30' }, line(), 7, true);
    assert.deepEqual(bad.blockers, ['bad_date']);
    assert.deepEqual(bad.cols, {});
    assert.deepEqual(L.planApply({}, line(), 7, true).blockers, ['nothing_requested']);
    assert.deepEqual(L.planApply({ markReadyForQc: false }, line(), 7, true).blockers, ['nothing_requested']);
});

const row = (over = {}) => ({
    id: 5, supplier_id: 3, supplier_name: 'Acme', purchase_order_id: 7, po_number: 'PO_1', order_id: 10,
    jf_code: 'JF1', product_name: 'Widget', status: 'pending', row_version: 0,
    base_json: { status: 'IN_PRODUCTION', estimatedReadyDate: '2026-11-01', actualReadyDate: null },
    requested_json: JSON.stringify({ estimatedReadyDate: '2026-11-20', actualReadyDate: '2026-10-14', markReadyForQc: true }),
    submitted_by_email: 's@acme.test', submitted_at_iso: '2026-10-15T09:00:00Z',
    live_id: 10, live_purchase_order_id: 7, live_status: 'IN_PRODUCTION',
    live_estimated_ready_date: '2026-11-05', live_actual_ready_date: null, po_owned: 1,
    ...over,
});

test('the API shape: old (live) and new value per field', () => {
    const r = L.requestToJson(row());
    assert.equal(r.canApply, true);
    assert.deepEqual(r.blockers, []);
    assert.deepEqual(r.changes, [
        { field: 'estimatedReadyDate', oldValue: '2026-11-05', newValue: '2026-11-20', submittedOldValue: '2026-11-01', changedSinceSubmit: true, alreadyApplied: false },
        { field: 'actualReadyDate', oldValue: null, newValue: '2026-10-14', submittedOldValue: null, changedSinceSubmit: false, alreadyApplied: false },
        { field: 'status', oldValue: 'IN_PRODUCTION', newValue: 'READY_FOR_QC', submittedOldValue: 'IN_PRODUCTION', changedSinceSubmit: false, alreadyApplied: false },
    ]);
    assert.equal(r.submittedAt, '2026-10-15T09:00:00Z');
});

test('a row whose line is gone cannot be applied; decided rows carry no blockers', () => {
    const gone = L.requestToJson(row({ live_id: null }));
    assert.equal(gone.canApply, false);
    assert.equal(gone.blockers[0].code, 'line_missing');
    assert.equal(gone.changes[0].oldValue, null);
    const applied = L.requestToJson(row({ status: 'applied' }));
    assert.equal(applied.canApply, false);
    assert.deepEqual(applied.blockers, []);
});

test('the list shows po_supplier_changed when po_owned is not 1', () => {
    for (const po_owned of [0, null, undefined]) {
        const r = L.requestToJson(row({ po_owned }));
        assert.equal(r.canApply, false);
        assert.deepEqual(r.blockers.map(x => x.code), ['po_supplier_changed']);
        assert.equal(r.blockers[0].message, L.BLOCKERS.po_supplier_changed);
    }
    // mysql2 may hand the EXISTS back as a string.
    assert.equal(L.requestToJson(row({ po_owned: '1' })).canApply, true);
    // Decided rows are not re-checked.
    assert.deepEqual(L.requestToJson(row({ status: 'applied', po_owned: 0 })).blockers, []);
});

test('history rows show the value when the supplier sent it as old, not today\'s', () => {
    const applied = L.requestToJson(row({
        status: 'applied', live_estimated_ready_date: '2026-11-20', live_actual_ready_date: '2026-10-14', live_status: 'READY_FOR_QC',
    }));
    assert.deepEqual(applied.changes[0], {
        field: 'estimatedReadyDate', oldValue: '2026-11-01', newValue: '2026-11-20',
        submittedOldValue: '2026-11-01', changedSinceSubmit: false, alreadyApplied: false,
    });
    assert.equal(applied.changes[2].oldValue, 'IN_PRODUCTION');
});

test('schema name is whitelisted', () => {
    assert.equal(L.jportalSchema({}), '`jportal`');
    assert.equal(L.jportalSchema({ JPORTAL_DB_SCHEMA: 'jportal_test' }), '`jportal_test`');
    assert.throws(() => L.jportalSchema({ JPORTAL_DB_SCHEMA: 'x`; DROP' }));
});

test('note and ids', () => {
    assert.deepEqual(L.parseNote('  ok '), { value: 'ok' });
    assert.deepEqual(L.parseNote(undefined), { value: null });
    assert.ok(L.parseNote('x'.repeat(501)).error);
    assert.equal(L.positiveInt('12'), 12);
    assert.equal(L.positiveInt(0), null);
    assert.equal(L.positiveInt('1.5'), null);
    assert.equal(L.positiveInt(Number.MAX_SAFE_INTEGER), Number.MAX_SAFE_INTEGER);
    assert.equal(L.positiveInt(Number.MAX_SAFE_INTEGER + 1), null);
    assert.equal(L.positiveInt('9007199254740993'), null);
    assert.equal(L.positiveInt(1e21), null);
    assert.equal(L.positiveInt(-3), null);
    assert.equal(L.positiveInt(null), null);
});

// ── A sample order: "mark as sent" (docs/sample-orders.md) ───────────────────
// The one thing a supplier does on a sample order's page. It carries what they
// were looking at (base: the line's status, product, quantity, and when the
// page was loaded), so a request about a line that has since changed is stale.

const sampleLine = (over = {}) => line({
    status: 'PO_SENT', orderKind: 'sample', jfCode: 'JF1016', quantity: 2,
    shippedDate: null, courierName: null, courierTracking: null,
    statusChangedAt: '2026-11-25T10:00:00Z', poDate: '2026-11-25', ...over,
});
const seen = (over = {}) => ({ status: 'PO_SENT', jfCode: 'JF1016', quantity: 2, seenAt: '2026-11-26T08:00:00Z', ...over });
const sent = (over = {}) => ({ markSent: { sentOn: '2026-11-26', courier: 'DHL', tracking: '1234567890', ...over } });
const when = (over = {}) => ({ base: seen(), today: '2026-11-27', ...over });

test('mark as sent: a sample line goes from PO Sent to On Air with the date and the courier', () => {
    const plan = L.planApply(sent(), sampleLine(), 7, true, when());
    assert.deepEqual(plan.blockers, []);
    assert.deepEqual(plan.cols, { shipped_date: '2026-11-26', courier_name: 'DHL', courier_tracking: '1234567890' });
    assert.equal(plan.moveToSent, true);
    assert.equal(plan.moveToQc, false);
    assert.deepEqual(plan.before, { status: 'PO_SENT', shippedDate: null, courierName: null, courierTracking: null });
    assert.deepEqual(plan.after, { status: 'ON_AIR', shippedDate: '2026-11-26', courierName: 'DHL', courierTracking: '1234567890' });
});

test('mark as sent: only for a sample, and a sample takes no ready date and no QC move', () => {
    assert.deepEqual(L.planApply(sent(), line({ status: 'PO_SENT' }), 7, true, when()).blockers, ['sample_only']);
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, sampleLine(), 7, true, when()).blockers, ['not_for_sample']);
    assert.deepEqual(L.planApply({ markReadyForQc: true }, sampleLine(), 7, true, when()).blockers, ['not_for_sample']);
    assert.deepEqual(L.planApply({ ...sent(), actualReadyDate: '2026-11-26' }, sampleLine(), 7, true, when()).blockers, ['not_for_sample']);
});

test('mark as sent: only while the line is waiting to be sent, and never backward', () => {
    for (const status of ['SCHEDULED', 'ON_AIR', 'ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'DESTROYED']) {
        const plan = L.planApply(sent(), sampleLine({ status }), 7, true, when({ base: seen({ status }) }));
        assert.deepEqual(plan.blockers, ['sent_from'], status);
        assert.deepEqual(plan.cols, {});
        assert.equal(plan.moveToSent, false);
    }
});

test('mark as sent: the date is a real date, not ahead of today and not before the order', () => {
    assert.deepEqual(L.planApply(sent({ sentOn: '26/11/2026' }), sampleLine(), 7, true, when()).blockers, ['bad_date']);
    assert.deepEqual(L.planApply({ markSent: {} }, sampleLine(), 7, true, when()).blockers, ['bad_date']);
    assert.deepEqual(L.planApply(sent({ sentOn: '2026-11-28' }), sampleLine(), 7, true, when()).blockers, ['sent_future']);
    assert.deepEqual(L.planApply(sent({ sentOn: '2026-11-27' }), sampleLine(), 7, true, when()).blockers, []);
    assert.deepEqual(L.planApply(sent({ sentOn: '2026-11-24' }), sampleLine(), 7, true, when()).blockers, ['sent_before_order']);
    assert.deepEqual(L.planApply(sent({ sentOn: '2026-11-25' }), sampleLine(), 7, true, when()).blockers, []);
});

test('mark as sent: stale when the line is no longer what the supplier was looking at', () => {
    const stale = (base) => L.planApply(sent(), sampleLine(), 7, true, when({ base })).blockers;
    assert.deepEqual(stale(seen({ quantity: 3 })), ['stale']);
    assert.deepEqual(stale(seen({ jfCode: 'JF1017' })), ['stale']);
    assert.deepEqual(stale(seen({ status: 'SCHEDULED' })), ['stale']);
    // The page was loaded before the line last changed status (a dispatch and
    // its correction in between leave status, product and quantity matching).
    assert.deepEqual(stale(seen({ seenAt: '2026-11-25T09:59:59Z' })), ['stale']);
    assert.deepEqual(stale(seen({ seenAt: '2026-11-25T10:00:00Z' })), []);
    // No record of what they saw is not a pass.
    assert.deepEqual(stale(seen({ seenAt: undefined })), ['stale']);
    assert.deepEqual(stale({}), ['stale']);
    assert.deepEqual(L.planApply(sent(), sampleLine(), 7, true, { today: '2026-11-27' }).blockers, ['stale']);
    // The case of the product code is not a difference.
    assert.deepEqual(stale(seen({ jfCode: 'jf1016' })), []);
});

test('mark as sent: courier details staff entered first are kept unless told otherwise', () => {
    const staffFirst = sampleLine({ courierName: 'FedEx', courierTracking: '99' });
    const kept = L.planApply(sent(), staffFirst, 7, true, when());
    assert.deepEqual(kept.blockers, []);
    assert.deepEqual(kept.cols, { shipped_date: '2026-11-26' });
    assert.deepEqual(kept.after, { status: 'ON_AIR', shippedDate: '2026-11-26' });
    const taken = L.planApply(sent(), staffFirst, 7, true, when({ useSupplierCourier: true }));
    assert.deepEqual(taken.cols, { shipped_date: '2026-11-26', courier_name: 'DHL', courier_tracking: '1234567890' });
    // One of the two already there: only the empty one is filled.
    const half = L.planApply(sent(), sampleLine({ courierName: 'FedEx' }), 7, true, when());
    assert.deepEqual(half.cols, { shipped_date: '2026-11-26', courier_tracking: '1234567890' });
});

test('mark as sent: courier and tracking are optional, plain text, 64 characters each', () => {
    const bare = L.planApply({ markSent: { sentOn: '2026-11-26' } }, sampleLine(), 7, true, when());
    assert.deepEqual(bare.blockers, []);
    assert.deepEqual(bare.cols, { shipped_date: '2026-11-26' });
    assert.deepEqual(L.planApply(sent({ courier: '  DHL  ', tracking: ' ' }), sampleLine(), 7, true, when()).cols,
        { shipped_date: '2026-11-26', courier_name: 'DHL' });
    assert.deepEqual(L.planApply(sent({ tracking: 'x'.repeat(65) }), sampleLine(), 7, true, when()).blockers, ['bad_courier']);
    assert.deepEqual(L.planApply(sent({ courier: 12345 }), sampleLine(), 7, true, when()).blockers, ['bad_courier']);
});

test('mark as sent: ownership and a missing line block it like any other request', () => {
    assert.deepEqual(L.planApply(sent(), sampleLine(), 7, false, when()).blockers, ['po_supplier_changed']);
    assert.deepEqual(L.planApply(sent(), null, 7, true, when()).blockers, ['line_missing']);
});

test('a stock line applies as it always did when the new options are passed', () => {
    const plan = L.planApply({ estimatedReadyDate: '2026-11-20' }, line(), 7, true, when());
    assert.deepEqual(plan.blockers, []);
    assert.deepEqual(plan.cols, { estimated_ready_date: '2026-11-20' });
    assert.equal(plan.moveToSent, false);
});

test('lineFromRow reads the sample fields, and a row without them is a stock line', () => {
    const l = L.lineFromRow({
        id: 10, purchase_order_id: 7, status: 'PO_SENT', estimated_ready_date: null, actual_ready_date: null,
        order_kind: 'sample', jf_code: 'JF1016', quantity: '2', shipped_date: null, courier_name: 'DHL', courier_tracking: null,
        sample_status_changed_at_iso: '2026-11-25T10:00:00Z', po_date: new Date('2026-11-25T00:00:00Z'),
    });
    assert.equal(l.orderKind, 'sample');
    assert.equal(l.quantity, 2);
    assert.equal(l.jfCode, 'JF1016');
    assert.equal(l.courierName, 'DHL');
    assert.equal(l.statusChangedAt, '2026-11-25T10:00:00Z');
    assert.equal(l.poDate, '2026-11-25');
    assert.equal(L.lineFromRow({ id: 1, purchase_order_id: 7, status: 'PO_SENT' }).orderKind, 'stock');
});

test('requestToJson: a mark-as-sent request shows the move and what it would write', () => {
    const row = {
        id: 5, status: 'pending', row_version: 1, supplier_id: 3, supplier_name: 'Acme Co., Ltd.',
        purchase_order_id: 7, po_number: 'PO_00431J', order_id: 10, jf_code: 'JF1016', product_name: 'Thing',
        base_json: JSON.stringify(seen()), requested_json: JSON.stringify(sent()),
        submitted_by_email: 'amy@acme.example', submitted_at_iso: '2026-11-26T08:01:00Z',
        live_id: 10, live_purchase_order_id: 7, live_status: 'PO_SENT',
        live_estimated_ready_date: null, live_actual_ready_date: null,
        live_order_kind: 'sample', live_jf_code: 'JF1016', live_quantity: 2, live_shipped_date: null,
        live_courier_name: null, live_courier_tracking: null,
        live_sample_status_changed_at_iso: '2026-11-25T10:00:00Z', live_po_date: '2026-11-25',
        po_owned: 1,
    };
    const json = L.requestToJson(row, { today: '2026-11-27' });
    assert.equal(json.orderKind, 'sample');
    assert.equal(json.canApply, true);
    assert.deepEqual(json.changes.map(c => [c.field, c.oldValue, c.newValue]), [
        ['status', 'PO_SENT', 'ON_AIR'],
        ['shippedDate', null, '2026-11-26'],
        ['courierName', null, 'DHL'],
        ['courierTracking', null, '1234567890'],
    ]);
    // The line moved on in ShipLine: it is shown as blocked, with the reason.
    const moved = L.requestToJson({ ...row, live_status: 'ON_AIR' }, { today: '2026-11-27' });
    assert.equal(moved.canApply, false);
    assert.deepEqual(moved.blockers.map(b => b.code), ['sent_from']);
    assert.equal(typeof moved.blockers[0].message, 'string');
    // A stock request is unchanged, and says it is one.
    const stock = L.requestToJson({ ...row, requested_json: '{"estimatedReadyDate":"2026-12-01"}', live_order_kind: 'stock', live_status: 'IN_PRODUCTION' });
    assert.equal(stock.orderKind, 'stock');
    assert.equal(stock.canApply, true);
});
