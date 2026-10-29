'use strict';

// Unit tests for src/lib/qc-alerts.js — the 'qc_visit' daily alerts.
// Run: node --test tools/test-qc-alerts-lib.js

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildQcVisitAlerts, addDays, daysBetween } = require('../src/lib/qc-alerts');

const TODAY = '2026-11-10';

function visit(over = {}) {
    return {
        id: 1, ref: 'QCV_00001', status: 'PROPOSED', supplier_name: 'Acme Ltd',
        factory_label: 'Main plant', factory_city: 'Dongguan', inspector_name: 'AQI',
        visit_date: '2026-11-20', window_start: '09:30:00', window_end: '12:00:00',
        proposed_at: new Date('2026-11-01T02:00:00Z'), factory_confirmed_at: null, inspector_confirmed_at: null,
        ...over,
    };
}
function order(over = {}) {
    return {
        id: 100, jf_code: 'JF123', product_name: 'Widget', po_number: 'PO_00100', purchase_order_id: 10,
        supplier: 'Acme Ltd', status: 'READY_FOR_QC', quantity: 500, deleted_at: null, ...over,
    };
}
function line(over = {}) {
    return { visit_id: 1, order_id: 100, quantity_to_inspect: 500, po_quantity_snapshot: 500, sample_method: 'full', ...over };
}
function kinds(alerts) {
    return alerts.map((a) => a.meta.kind).sort();
}
function only(alerts, kind) {
    const found = alerts.filter((a) => a.meta.kind === kind);
    assert.equal(found.length, 1, `expected one ${kind}, got ${found.length}`);
    return found[0];
}

test('date helpers', () => {
    assert.equal(addDays('2026-11-10', 7), '2026-11-17');
    assert.equal(addDays('2026-12-30', 3), '2027-01-02');
    assert.equal(daysBetween('2026-11-01', '2026-11-10'), 9);
    assert.equal(daysBetween('2026-11-10', '2026-11-01'), -9);
});

test('nothing in, nothing out', () => {
    assert.deepEqual(buildQcVisitAlerts({ today: TODAY }), []);
});

test('every alert carries the common shape', () => {
    const alerts = buildQcVisitAlerts({
        visits: [visit({ proposed_at: '2026-11-01 02:00:00' })],
        lines: [line()],
        orders: [order()],
        today: TODAY,
    });
    assert.ok(alerts.length > 0);
    for (const a of alerts) {
        assert.equal(a.type, 'qc_visit');
        assert.ok(['today', 'overdue'].includes(a.severity));
        assert.match(a.eventDate, /^\d{4}-\d{2}-\d{2}$/);
        assert.equal(typeof a.title, 'string');
        assert.equal(typeof a.entityId, 'string');
        assert.equal(a.dedupKey, `qc_visit:${a.meta.kind}:${a.entityId}`);
    }
});

test('unconfirmed: quiet for 3 days, today after 3, overdue after 7', () => {
    const at = (proposed) => buildQcVisitAlerts({ visits: [visit({ proposed_at: proposed })], today: TODAY })
        .filter((a) => a.meta.kind === 'unconfirmed');
    // Proposed 3 China days ago → not yet.
    assert.equal(at(new Date('2026-11-07T01:00:00Z')).length, 0);
    // 4 days → today.
    const four = at(new Date('2026-11-06T01:00:00Z'));
    assert.equal(four.length, 1);
    assert.equal(four[0].severity, 'today');
    // 7 days → still today; 8 days → overdue.
    assert.equal(at(new Date('2026-11-03T01:00:00Z'))[0].severity, 'today');
    assert.equal(at(new Date('2026-11-02T01:00:00Z'))[0].severity, 'overdue');
});

test('unconfirmed uses the China day of proposed_at (UTC evening is the next China day)', () => {
    // 2026-11-06 17:00 UTC = 2026-11-07 01:00 China → 3 days → no alert.
    const alerts = buildQcVisitAlerts({ visits: [visit({ proposed_at: new Date('2026-11-06T17:00:00Z') })], today: TODAY });
    assert.equal(alerts.filter((a) => a.meta.kind === 'unconfirmed').length, 0);
});

test('unconfirmed shape and who it is waiting on', () => {
    const a = only(buildQcVisitAlerts({
        visits: [visit({ factory_confirmed_at: new Date('2026-11-02T00:00:00Z') })],
        today: TODAY,
    }), 'unconfirmed');
    assert.equal(a.dedupKey, 'qc_visit:unconfirmed:1');
    assert.equal(a.entityType, 'qc_visit');
    assert.equal(a.entityId, '1');
    assert.equal(a.eventDate, '2026-11-20');
    assert.deepEqual(a.meta, { kind: 'unconfirmed', visitId: 1, ref: 'QCV_00001', visitDate: '2026-11-20' });
    assert.match(a.body, /waiting on inspector/);
    assert.doesNotMatch(a.body, /factory and/);
    assert.match(a.title, /QCV_00001/);
});

test('unconfirmed never fires without proposed_at or for non-PROPOSED visits', () => {
    assert.equal(buildQcVisitAlerts({ visits: [visit({ proposed_at: null })], today: TODAY }).length, 0);
    assert.equal(buildQcVisitAlerts({ visits: [visit({ status: 'DRAFT' })], today: TODAY }).length, 0);
});

test('due_soon: CONFIRMED visit today … today+7, severity today', () => {
    const run = (d) => buildQcVisitAlerts({ visits: [visit({ status: 'CONFIRMED', visit_date: d })], today: TODAY });
    for (const d of ['2026-11-10', '2026-11-13', '2026-11-17']) {
        const a = only(run(d), 'due_soon');
        assert.equal(a.severity, 'today');
        assert.equal(a.eventDate, d);
        assert.equal(a.dedupKey, 'qc_visit:due_soon:1');
        assert.deepEqual(a.meta, { kind: 'due_soon', visitId: 1, ref: 'QCV_00001', visitDate: d });
    }
    assert.equal(run('2026-11-18').length, 0);
    assert.match(only(run('2026-11-10'), 'due_soon').title, /today/);
    assert.match(only(run('2026-11-12'), 'due_soon').body, /09:30–12:00 China time/);
    // A PROPOSED visit is never due_soon.
    assert.equal(buildQcVisitAlerts({ visits: [visit({ visit_date: '2026-11-12', proposed_at: null })], today: TODAY }).length, 0);
});

test('completion_due: CONFIRMED with the visit date passed, overdue', () => {
    const a = only(buildQcVisitAlerts({
        visits: [visit({ status: 'CONFIRMED', visit_date: '2026-11-09' })], today: TODAY,
    }), 'completion_due');
    assert.equal(a.severity, 'overdue');
    assert.equal(a.eventDate, '2026-11-09');
    assert.equal(a.dedupKey, 'qc_visit:completion_due:1');
    assert.equal(a.entityType, 'qc_visit');
    // CONFIRMED with no date: neither kind.
    assert.equal(buildQcVisitAlerts({ visits: [visit({ status: 'CONFIRMED', visit_date: null })], today: TODAY }).length, 0);
});

test('po_shrank: below the snapshot or below the quantity to inspect', () => {
    const run = (o, l) => buildQcVisitAlerts({
        visits: [visit({ proposed_at: null })], lines: [l], orders: [o], today: TODAY,
    }).filter((a) => a.meta.kind === 'po_shrank');
    assert.equal(run(order({ quantity: 500 }), line()).length, 0);
    const a = run(order({ quantity: 400 }), line({ quantity_to_inspect: 50 }))[0];
    assert.equal(a.severity, 'today');
    assert.equal(a.dedupKey, 'qc_visit:po_shrank:1');
    assert.deepEqual(a.meta, { kind: 'po_shrank', visitId: 1, ref: 'QCV_00001', visitDate: '2026-11-20', orderIds: [100] });
    assert.match(a.body, /JF123 500→400/);
    // Snapshot unknown, but the PO is now smaller than the quantity booked.
    assert.equal(run(order({ quantity: 40 }), line({ quantity_to_inspect: 50, po_quantity_snapshot: null })).length, 1);
    // Grown PO is fine.
    assert.equal(run(order({ quantity: 900 }), line()).length, 0);
});

test('line_left_stage: moved on, deleted or missing; a destroyed destructive sample is fine', () => {
    const run = (orders, lines) => buildQcVisitAlerts({
        visits: [visit({ proposed_at: null })], lines, orders, today: TODAY,
    }).filter((a) => a.meta.kind === 'line_left_stage');
    assert.equal(run([order({ status: 'IN_PRODUCTION' })], [line()]).length, 0);
    const a = run(
        [order({ status: 'READY' }), order({ id: 101, deleted_at: new Date() })],
        [line(), line({ order_id: 101 }), line({ order_id: 102 })],
    )[0];
    assert.equal(a.dedupKey, 'qc_visit:line_left_stage:1');
    assert.deepEqual(a.meta.orderIds, [100, 101, 102]);
    assert.match(a.body, /JF123 READY/);
    assert.match(a.title, /3 lines/);
    assert.equal(run([order({ jf_code: 'JF123_FQC', status: 'DESTROYED' })], [line({ sample_method: 'destructive' })]).length, 0);
    assert.equal(run([order({ status: 'DESTROYED' })], [line()]).length, 1);
});

test('unvisited_ready: READY_FOR_QC on no open visit, keyed per order', () => {
    const alerts = buildQcVisitAlerts({
        visits: [],
        orders: [order(), order({ id: 101, status: 'IN_PRODUCTION' }), order({ id: 102, deleted_at: new Date() })],
        today: TODAY,
    });
    assert.equal(alerts.length, 1);
    const a = alerts[0];
    assert.equal(a.dedupKey, 'qc_visit:unvisited_ready:100');
    assert.equal(a.entityType, 'order');
    assert.equal(a.entityId, '100');
    assert.equal(a.severity, 'today');
    assert.equal(a.eventDate, TODAY);
    assert.deepEqual(a.meta, { kind: 'unvisited_ready', orderId: 100 });
});

test('unvisited_ready: an open visit covers the order, a terminal one does not', () => {
    const run = (status) => buildQcVisitAlerts({
        visits: [visit({ status, proposed_at: null, visit_date: null })], lines: [line()], orders: [order()], today: TODAY,
    });
    for (const s of ['DRAFT', 'PROPOSED', 'CONFIRMED']) assert.deepEqual(kinds(run(s)), [], s);
    for (const s of ['COMPLETED', 'CANCELLED']) assert.deepEqual(kinds(run(s)), ['unvisited_ready'], s);
});

test('unvisited_ready: an _FQC sample rides with its candidate parent', () => {
    const fqc = order({ id: 101, jf_code: 'jf123_fqc', quantity: 2 });
    // Parent ready too → only the parent alerts.
    assert.deepEqual(
        buildQcVisitAlerts({ orders: [order(), fqc], today: TODAY }).map((a) => a.entityId),
        ['100'],
    );
    // Parent still in production → parent is a candidate, sample skipped.
    assert.deepEqual(
        buildQcVisitAlerts({ orders: [order({ status: 'IN_PRODUCTION' }), fqc], today: TODAY }).map((a) => a.entityId),
        [],
    );
    // No parent on the PO (or parent on another PO) → the sample alerts alone.
    assert.deepEqual(
        buildQcVisitAlerts({ orders: [order({ purchase_order_id: 11 }), fqc], today: TODAY }).map((a) => a.entityId),
        ['100', '101'],
    );
});

test('terminal visits never alert, whatever their state', () => {
    for (const status of ['COMPLETED', 'CANCELLED']) {
        const alerts = buildQcVisitAlerts({
            visits: [visit({ status, visit_date: '2026-11-01', proposed_at: new Date('2026-10-01T00:00:00Z') })],
            lines: [line()],
            orders: [order({ status: 'READY', quantity: 1 })],
            today: TODAY,
        });
        assert.deepEqual(alerts, [], status);
    }
});

test('one visit can carry several kinds, each with its own dedup key', () => {
    const alerts = buildQcVisitAlerts({
        visits: [visit({ id: 7, ref: null, status: 'CONFIRMED', visit_date: '2026-11-08' })],
        lines: [line({ visit_id: 7 }), line({ visit_id: 7, order_id: 101 })],
        orders: [order({ quantity: 10 }), order({ id: 101, status: 'READY' })],
        today: TODAY,
    });
    assert.deepEqual(kinds(alerts), ['completion_due', 'line_left_stage', 'po_shrank']);
    assert.deepEqual(alerts.map((a) => a.dedupKey).sort(), [
        'qc_visit:completion_due:7', 'qc_visit:line_left_stage:7', 'qc_visit:po_shrank:7',
    ]);
    // ref falls back to the formatted id.
    assert.ok(alerts.every((a) => a.meta.ref === 'QCV_00007'));
    assert.equal(new Set(alerts.map((a) => a.dedupKey)).size, alerts.length);
});

test('title and body fit daily_alerts (VARCHAR 255 / 1000) on a visit with many lines', () => {
    const lines = [];
    const orders = [];
    for (let i = 0; i < 120; i++) {
        lines.push(line({ order_id: 1000 + i, po_quantity_snapshot: 500, quantity_to_inspect: 500 }));
        orders.push(order({ id: 1000 + i, jf_code: `JF_LONG_CODE_${i}`, quantity: 100, status: 'SHIPPED' }));
    }
    orders.push(order({ id: 9, jf_code: null, product_name: 'P'.repeat(400), status: 'READY_FOR_QC' }));
    const alerts = buildQcVisitAlerts({ visits: [visit({ status: 'CONFIRMED' })], lines, orders, today: TODAY });
    assert.deepEqual(kinds(alerts), ['line_left_stage', 'po_shrank', 'unvisited_ready']);
    for (const a of alerts) {
        assert.ok(a.title.length <= 255, `${a.meta.kind} title ${a.title.length}`);
        assert.ok(a.body == null || a.body.length <= 1000, `${a.meta.kind} body ${a.body && a.body.length}`);
    }
    assert.ok(only(alerts, 'po_shrank').body.endsWith('…'));
    assert.equal(only(alerts, 'po_shrank').meta.orderIds.length, 120);
});
