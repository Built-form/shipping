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
    const plan = L.planApply({ estimatedReadyDate: '2026-11-20' }, line(), 7);
    assert.deepEqual(plan.blockers, []);
    assert.deepEqual(plan.cols, { estimated_ready_date: '2026-11-20' });
    assert.deepEqual(plan.before, { estimatedReadyDate: '2026-11-01' });
    assert.deepEqual(plan.after, { estimatedReadyDate: '2026-11-20' });
    assert.equal(plan.moveToQc, false);
});

test('null clears a date; a value already live is a no-op', () => {
    assert.deepEqual(L.planApply({ estimatedReadyDate: null }, line(), 7).cols, { estimated_ready_date: null });
    const same = L.planApply({ estimatedReadyDate: '2026-11-01' }, line(), 7);
    assert.deepEqual(same.blockers, []);
    assert.deepEqual(same.cols, {});
});

test('ready for QC with the actual date in the same request', () => {
    const plan = L.planApply({ actualReadyDate: '2026-10-14', markReadyForQc: true }, line(), 7);
    assert.deepEqual(plan.blockers, []);
    assert.deepEqual(plan.cols, { actual_ready_date: '2026-10-14' });
    assert.equal(plan.moveToQc, true);
    assert.deepEqual(plan.after, { actualReadyDate: '2026-10-14', status: 'READY_FOR_QC' });
});

test('ready for QC needs an actual ready date, stored or requested', () => {
    assert.deepEqual(L.planApply({ markReadyForQc: true }, line(), 7).blockers, ['qc_no_date']);
    assert.deepEqual(L.planApply({ markReadyForQc: true }, line({ actualReadyDate: '2026-10-10' }), 7).blockers, []);
    // Clearing the date in the same request leaves none.
    assert.deepEqual(
        L.planApply({ actualReadyDate: null, markReadyForQc: true }, line({ actualReadyDate: '2026-10-10' }), 7).blockers,
        ['qc_no_date']
    );
});

test('ready for QC only from IN_PRODUCTION; already there is a no-op', () => {
    assert.deepEqual(L.planApply({ markReadyForQc: true }, line({ status: 'PO_SENT', actualReadyDate: '2026-10-10' }), 7).blockers, ['qc_from']);
    const already = L.planApply({ markReadyForQc: true }, line({ status: 'READY_FOR_QC' }), 7);
    assert.deepEqual(already.blockers, []);
    assert.equal(already.moveToQc, false);
});

test('a line past the factory, deleted or on another PO is blocked', () => {
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, line({ status: 'ON_SEA' }), 7).blockers, ['line_locked']);
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, null, 7).blockers, ['line_missing']);
    assert.deepEqual(L.planApply({ estimatedReadyDate: '2026-12-01' }, line({ purchaseOrderId: 8 }), 7).blockers, ['line_missing']);
});

test('bad dates and empty requests are blocked, nothing written', () => {
    const bad = L.planApply({ estimatedReadyDate: '2026-02-30' }, line(), 7);
    assert.deepEqual(bad.blockers, ['bad_date']);
    assert.deepEqual(bad.cols, {});
    assert.deepEqual(L.planApply({}, line(), 7).blockers, ['nothing_requested']);
    assert.deepEqual(L.planApply({ markReadyForQc: false }, line(), 7).blockers, ['nothing_requested']);
});

const row = (over = {}) => ({
    id: 5, supplier_id: 3, supplier_name: 'Acme', purchase_order_id: 7, po_number: 'PO_1', order_id: 10,
    jf_code: 'JF1', product_name: 'Widget', status: 'pending', row_version: 0,
    base_json: { status: 'IN_PRODUCTION', estimatedReadyDate: '2026-11-01', actualReadyDate: null },
    requested_json: JSON.stringify({ estimatedReadyDate: '2026-11-20', actualReadyDate: '2026-10-14', markReadyForQc: true }),
    submitted_by_email: 's@acme.test', submitted_at_iso: '2026-10-15T09:00:00Z',
    live_id: 10, live_purchase_order_id: 7, live_status: 'IN_PRODUCTION',
    live_estimated_ready_date: '2026-11-05', live_actual_ready_date: null,
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
});
