'use strict';

// Unit tests for src/lib/draft-audit.js — the draft-container registry + audit
// writer. Runs against a fake connection (no DB, no server):
//
//   node --test tools/test-draft-audit-lib.js
//
// The HTTP-level smoke test (needs a running server + DB) is
// tools/test-draft-container-audit.js.

const test = require('node:test');
const assert = require('node:assert/strict');

const lib = require('../src/lib/draft-audit');

// A scripted mysql2-style connection: `respond(sql, params)` returns the
// `[rows]` / `[header]` tuple for each query; every call is recorded so the
// test can assert on what was written.
function fakeConn(respond) {
    const calls = [];
    return {
        calls,
        async query(sql, params) {
            calls.push({ sql: sql.replace(/\s+/g, ' ').trim(), params: params || [] });
            return respond(sql, params || [], calls.length);
        },
    };
}

const has = (calls, fragment) => calls.filter(c => c.sql.includes(fragment));
const auditInserts = calls => has(calls, 'INSERT INTO audit_log').map(c => ({
    entityType: c.params[0], entityId: c.params[1], action: c.params[2],
    before: c.params[3] ? JSON.parse(c.params[3]) : null,
    after: c.params[4] ? JSON.parse(c.params[4]) : null,
    userEmail: c.params[5],
}));

test('ensureDraftRegistered: unknown name → inserts registry row + "create" event', async () => {
    let inserted = false;
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) {
            return [inserted ? [{ id: 7, name: 'DRAFT-SEA-1 - 268', closed_reason: null, container_number: null }] : []];
        }
        if (sql.includes('INSERT INTO draft_containers')) { inserted = true; return [{ affectedRows: 1, insertId: 7 }]; }
        return [{ affectedRows: 1 }];
    });
    const r = await lib.ensureDraftRegistered(conn, 'DRAFT-SEA-1 - 268', 'me@x.com');
    assert.deepEqual({ id: r.id, name: r.name, created: r.created }, { id: 7, name: 'DRAFT-SEA-1 - 268', created: true });
    const ins = has(conn.calls, 'INSERT INTO draft_containers');
    assert.equal(ins.length, 1);
    assert.deepEqual(ins[0].params, ['DRAFT-SEA-1 - 268', 'me@x.com']);
    const events = auditInserts(conn.calls);
    assert.equal(events.length, 1);
    assert.equal(events[0].entityType, 'draft_container');
    assert.equal(events[0].entityId, 7);
    assert.equal(events[0].action, 'create');
    assert.equal(events[0].after.draftName, 'DRAFT-SEA-1 - 268');
    assert.equal(events[0].userEmail, 'me@x.com');
});

test('ensureDraftRegistered: known open name → touches, no insert, no event', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[{ id: 3, name: 'D1', closed_reason: null, container_number: null }]];
        return [{ affectedRows: 1 }];
    });
    const r = await lib.ensureDraftRegistered(conn, 'D1', 'me@x.com');
    assert.equal(r.created, false);
    assert.equal(has(conn.calls, 'INSERT INTO draft_containers').length, 0);
    assert.equal(auditInserts(conn.calls).length, 0);
    assert.equal(has(conn.calls, 'last_activity_at = CURRENT_TIMESTAMP').length, 1);
});

test('ensureDraftRegistered: closed name receiving activity → reopened event, close fields cleared', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[{ id: 3, name: 'D1', closed_reason: 'converted', container_number: '268' }]];
        return [{ affectedRows: 1 }];
    });
    const r = await lib.ensureDraftRegistered(conn, 'D1', 'me@x.com');
    assert.equal(r.reopened, true);
    assert.equal(has(conn.calls, 'closed_reason = NULL').length, 1);
    const events = auditInserts(conn.calls);
    assert.equal(events.length, 1);
    assert.equal(events[0].action, 'reopened');
    assert.equal(events[0].before.closedReason, 'converted');
    assert.equal(events[0].before.containerNumber, '268');
});

test('recordDraftAudit: never throws, explicit createdAt goes into the row', async () => {
    const failing = fakeConn(() => { throw new Error('boom'); });
    await lib.recordDraftAudit(failing, { draftId: 1, action: 'line_added', after: { a: 1 } });

    const conn = fakeConn(() => [{ affectedRows: 1 }]);
    await lib.recordDraftAudit(conn, { draftId: 1, action: 'line_added', after: { a: 1 }, userEmail: 'u@x', createdAt: '2026-01-01 10:00:00' });
    assert.equal(conn.calls.length, 1);
    assert.ok(conn.calls[0].sql.includes('created_at'));
    assert.equal(conn.calls[0].params[6], '2026-01-01 10:00:00');
});

test('lineSnapshot: maps an allocation+order row to the event payload', () => {
    const snap = lib.lineSnapshot({
        id: 11, order_id: 629, allocated: '500', jf_code: 'JF1234', asin: 'B0AAAAAAAA',
        product_name: 'Widget', supplier: 'Acme', po_number: 'PO-1', order_quantity: 1200,
    });
    assert.deepEqual(snap, {
        allocationId: 11, orderId: 629, allocated: 500, jfCode: 'JF1234', asin: 'B0AAAAAAAA',
        productName: 'Widget', supplier: 'Acme', poNumber: 'PO-1', orderQuantity: 1200,
    });
});

test('renameDraft: moves allocations, documents and QA docs, keeps the registry id, writes "renamed"', async () => {
    const conn = fakeConn((sql, params) => {
        if (sql.includes('SELECT id, name, closed_reason')) {
            // `from` is registered; `to` is unknown.
            return [params[0] === 'OLD' ? [{ id: 5, name: 'OLD', closed_reason: null, container_number: null }] : []];
        }
        if (sql.includes('SELECT COUNT(*) AS n FROM draft_container_allocations')) return [[{ n: params[0] === 'OLD' ? 3 : 0 }]];
        if (sql.includes('UPDATE draft_container_allocations SET draft_container_name')) return [{ affectedRows: 3 }];
        if (sql.includes('UPDATE draft_container_documents SET draft_container_name')) return [{ affectedRows: 2 }];
        if (sql.includes('UPDATE quality_assurance_documents SET draft_container_name')) return [{ affectedRows: 1 }];
        return [{ affectedRows: 1 }];
    });
    const r = await lib.renameDraft(conn, { from: 'OLD', to: 'NEW', userEmail: 'me@x.com' });
    assert.deepEqual(r, { id: 5, name: 'NEW', allocations: 3, documents: 2, qaDocuments: 1 });
    assert.deepEqual(has(conn.calls, 'UPDATE draft_container_allocations SET draft_container_name')[0].params, ['NEW', 'OLD']);
    assert.deepEqual(has(conn.calls, 'UPDATE draft_containers SET name')[0].params, ['NEW', 5]);
    const events = auditInserts(conn.calls);
    assert.equal(events.length, 1);
    assert.equal(events[0].action, 'renamed');
    assert.equal(events[0].entityId, 5);
    assert.equal(events[0].before.draftName, 'OLD');
    assert.equal(events[0].after.draftName, 'NEW');
});

test('renameDraft: target name held by another draft → conflict, nothing written', async () => {
    const conn = fakeConn((sql, params) => {
        if (sql.includes('SELECT id, name, closed_reason')) {
            return [[{ id: params[0] === 'OLD' ? 5 : 9, name: params[0], closed_reason: null, container_number: null }]];
        }
        if (sql.includes('SELECT COUNT(*) AS n FROM draft_container_allocations')) return [[{ n: 3 }]];
        return [{ affectedRows: 1 }];
    });
    const r = await lib.renameDraft(conn, { from: 'OLD', to: 'NEW', userEmail: 'me@x.com' });
    assert.deepEqual(r, { conflict: true });
    assert.equal(has(conn.calls, 'UPDATE draft_container_allocations').length, 0);
    assert.equal(auditInserts(conn.calls).length, 0);
});

test('renameDraft: unknown source → notFound', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[]];
        if (sql.includes('SELECT COUNT(*) AS n FROM draft_container_allocations')) return [[{ n: 0 }]];
        return [{ affectedRows: 0 }];
    });
    assert.deepEqual(await lib.renameDraft(conn, { from: 'GHOST', to: 'NEW' }), { notFound: true });
});

test('closeDraft (converted): snapshots lines, deletes allocations, closes the registry row, writes "converted"', async () => {
    const lines = [
        { id: 1, order_id: 10, allocated: 500, jf_code: 'JF1', asin: 'A1', product_name: 'P1', supplier: 'S', po_number: 'PO', order_quantity: 500 },
        { id: 2, order_id: 11, allocated: 250, jf_code: 'JF2', asin: 'A2', product_name: 'P2', supplier: 'S', po_number: 'PO', order_quantity: 300 },
    ];
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[{ id: 5, name: 'D', closed_reason: null, container_number: null }]];
        if (sql.includes('FROM draft_container_allocations dca')) return [lines];
        if (sql.includes('DELETE FROM draft_container_allocations WHERE draft_container_name')) return [{ affectedRows: 2 }];
        return [{ affectedRows: 1 }];
    });
    const r = await lib.closeDraft(conn, {
        name: 'D', reason: 'converted', userEmail: 'me@x.com',
        details: { containerNumber: '268', externalContainerNumber: 'MSCU1234567', vesselName: 'EVER GIVEN', eta: '2026-10-01', etd: '2026-09-20', freightType: 'SEA', port: 'Ningbo' },
    });
    assert.equal(r.id, 5);
    assert.equal(r.deleted, 2);
    assert.equal(r.lines.length, 2);
    const close = has(conn.calls, 'UPDATE draft_containers SET closed_reason');
    assert.equal(close.length, 1);
    assert.deepEqual(close[0].params, ['converted', 'me@x.com', '268', 5]);
    const events = auditInserts(conn.calls);
    assert.equal(events.length, 1);
    assert.equal(events[0].action, 'converted');
    assert.equal(events[0].before.lineCount, 2);
    assert.equal(events[0].before.totalUnits, 750);
    assert.equal(events[0].before.lines[1].jfCode, 'JF2');
    assert.equal(events[0].after.containerNumber, '268');
    assert.equal(events[0].after.vesselName, 'EVER GIVEN');
});

test('closeDraft (deleted): container_number stays NULL, after payload is null', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[{ id: 5, name: 'D', closed_reason: null, container_number: null }]];
        if (sql.includes('FROM draft_container_allocations dca')) return [[{ id: 1, order_id: 10, allocated: 5 }]];
        if (sql.includes('DELETE FROM draft_container_allocations WHERE draft_container_name')) return [{ affectedRows: 1 }];
        return [{ affectedRows: 1 }];
    });
    const r = await lib.closeDraft(conn, { name: 'D', reason: 'deleted', userEmail: 'me@x.com' });
    assert.equal(r.deleted, 1);
    assert.deepEqual(has(conn.calls, 'UPDATE draft_containers SET closed_reason')[0].params, ['deleted', 'me@x.com', null, 5]);
    const events = auditInserts(conn.calls);
    assert.equal(events[0].action, 'deleted');
    assert.equal(events[0].after, null);
});

test('closeDraft: already closed and empty → no-op, no second event', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[{ id: 5, name: 'D', closed_reason: 'deleted', container_number: null }]];
        if (sql.includes('FROM draft_container_allocations dca')) return [[]];
        return [{ affectedRows: 0 }];
    });
    const r = await lib.closeDraft(conn, { name: 'D', reason: 'deleted', userEmail: 'me@x.com' });
    assert.deepEqual(r, { id: 5, name: 'D', deleted: 0, alreadyClosed: true });
    assert.equal(auditInserts(conn.calls).length, 0);
});

test('closeDraft: unknown name with no allocations → notFound', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('SELECT id, name, closed_reason')) return [[]];
        if (sql.includes('FROM draft_container_allocations dca')) return [[]];
        return [{ affectedRows: 0 }];
    });
    assert.deepEqual(await lib.closeDraft(conn, { name: 'GHOST', reason: 'deleted' }), { notFound: true });
});

test('rowToDraftRecord: status derives from closed_reason, then live line count', () => {
    const base = { id: 1, name: 'D', created_by_email: null, created_at: new Date('2026-01-01T00:00:00Z'), last_activity_at: null, closed_at: null, closed_by_email: null, closed_reason: null, container_number: null, line_count: 0, total_units: 0, document_count: 0, qa_document_count: 0, event_count: 0 };
    assert.equal(lib.rowToDraftRecord({ ...base }).status, 'empty');
    assert.equal(lib.rowToDraftRecord({ ...base, line_count: 2 }).status, 'open');
    assert.equal(lib.rowToDraftRecord({ ...base, closed_reason: 'converted', container_number: '268' }).status, 'converted');
    assert.equal(lib.rowToDraftRecord({ ...base, closed_reason: 'deleted', line_count: 1 }).status, 'deleted');
    const rec = lib.rowToDraftRecord({ ...base, line_count: '2', total_units: '750' });
    assert.equal(rec.createdAt, '2026-01-01T00:00:00.000Z');
    assert.equal(rec.lineCount, 2);
    assert.equal(rec.totalUnits, 750);
});

test('registry counts only lines whose order is still live — "open" must mean what the Draft tab shows', async () => {
    // The Draft tab lists allocations INNER JOINed to live orders. A row left
    // behind under a deleted order is invisible there, so it must not keep a
    // draft "open" (or pad its line / unit counts) in the registry.
    const conn = fakeConn(() => [[]]);
    await lib.listDraftRecords(conn, {});
    const sql = conn.calls[0].sql;
    const lineCount = sql.slice(sql.indexOf('(SELECT COUNT(*) FROM draft_container_allocations'), sql.indexOf('AS line_count'));
    const totalUnits = sql.slice(sql.indexOf('(SELECT COALESCE(SUM(a.allocated)'), sql.indexOf('AS total_units'));
    for (const [label, fragment] of [['line_count', lineCount], ['total_units', totalUnits]]) {
        assert.ok(fragment.includes('INNER JOIN orders'), `${label} joins orders`);
        assert.ok(fragment.includes('deleted_at IS NULL'), `${label} skips soft-deleted orders`);
    }
});

test('backfillDraftRegistry: claims the migration marker once; a lost claim does nothing', async () => {
    const won = fakeConn((sql) => {
        if (sql.includes('INSERT IGNORE INTO app_migrations')) return [{ affectedRows: 1 }];
        return [{ affectedRows: 0 }];
    });
    assert.deepEqual(await lib.backfillDraftRegistry(won), { ran: true });
    assert.ok(has(won.calls, 'INSERT IGNORE INTO draft_containers').length >= 1, 'registry rows are backfilled');
    assert.ok(has(won.calls, "'line_added'").length >= 1, 'existing allocations become line_added events');
    assert.ok(has(won.calls, "'document_generated'").length >= 1);
    assert.ok(has(won.calls, "'document_sent'").length >= 1);
    assert.ok(has(won.calls, "'qa_document_generated'").length >= 1);
    assert.ok(has(won.calls, "'qa_document_sent'").length >= 1);
    assert.ok(has(won.calls, 'COMMIT').length === 1);

    const lost = fakeConn((sql) => {
        if (sql.includes('INSERT IGNORE INTO app_migrations')) return [{ affectedRows: 0 }];
        return [{ affectedRows: 0 }];
    });
    assert.deepEqual(await lib.backfillDraftRegistry(lost), { ran: false });
    assert.equal(has(lost.calls, 'INSERT IGNORE INTO draft_containers').length, 0);
});

test('backfillDraftRegistry: a failure rolls back so the marker is released for the next cold start', async () => {
    const conn = fakeConn((sql) => {
        if (sql.includes('INSERT IGNORE INTO app_migrations')) return [{ affectedRows: 1 }];
        if (sql.includes('INSERT IGNORE INTO draft_containers')) throw new Error('Table missing');
        return [{ affectedRows: 0 }];
    });
    await assert.rejects(() => lib.backfillDraftRegistry(conn), /Table missing/);
    assert.equal(has(conn.calls, 'ROLLBACK').length, 1);
    assert.equal(has(conn.calls, 'COMMIT').length, 0);
});
