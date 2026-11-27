'use strict';

// Unit tests for the local-order side of src/services/sample-orders.js: the
// guard every order write calls, the readers' kind filters, and whether this
// database can hold a local order. Against a connection that answers from a
// small in-memory state (the same shape as tools/test-sample-orders-guard.js).
// Run: node --test tools/test-local-orders-guard.js

const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

process.env.APP_STAGE = 'test';
const G = require('../src/services/sample-orders');

const SAMPLE_LOT = 'SA0031-S01-V03-CN0011';
// CHECK_CLAUSE exactly as MySQL 8 on TEST returns it (read 2026-11-27): the
// quotes come back escaped, \'local\'. A fixture without the backslashes hid a
// localReady that never said yes on a real database.
const LOCAL_CHECKS = [
    { CONSTRAINT_NAME: 'chk_purchase_orders_kind', CHECK_CLAUSE: "(`kind` in (_utf8mb4\\'stock\\',_utf8mb4\\'sample\\',_utf8mb4\\'local\\'))" },
    { CONSTRAINT_NAME: 'chk_orders_order_kind', CHECK_CLAUSE: "(`order_kind` in (_utf8mb4\\'stock\\',_utf8mb4\\'sample\\',_utf8mb4\\'local\\'))" },
];
const SAMPLE_ONLY_CHECKS = LOCAL_CHECKS.map(c => ({ ...c, CHECK_CLAUSE: c.CHECK_CLAUSE.replace(",_utf8mb4\\'local\\'", '') }));
// The same clause without the escaping, as another server version may show it.
const UNESCAPED_LOCAL_CHECKS = LOCAL_CHECKS.map(c => ({ ...c, CHECK_CLAUSE: c.CHECK_CLAUSE.split("\\'").join("'") }));

function world(over = {}) {
    return {
        columns: 7,
        armed: [],
        pos: { 7: { kind: 'sample' }, 9: { kind: 'stock' }, 11: { kind: 'local' } },
        sampleLots: [{ lot: SAMPLE_LOT, jf: 'JF1016' }],
        checks: LOCAL_CHECKS,
        sql: [],
        ...over,
    };
}

function conn(state) {
    return {
        async query(sql, params = []) {
            state.sql.push(sql);
            if (/information_schema\.COLUMNS/.test(sql)) return [[{ n: state.columns }]];
            if (/information_schema\.CHECK_CONSTRAINTS/.test(sql)) return [state.checks];
            if (/FROM app_migrations/.test(sql)) return [state.armed.filter(n => n === params[0]).map(name => ({ name }))];
            if (/FROM purchase_orders WHERE id = \?/.test(sql)) {
                const p = state.pos[params[0]];
                return [p ? [{ id: params[0], kind: p.kind, supplier: 'Brookfield Packaging Ltd', currency: 'GBP', shipping_total: 0, deleted_at: null }] : []];
            }
            if (/FROM sample_lots_v/.test(sql)) {
                const [lot] = params;
                return [state.sampleLots.filter(l => l.lot.toUpperCase() === String(lot).toUpperCase()).map((h, i) => ({ lot_id: i + 1 }))];
            }
            throw new Error(`unexpected SQL: ${sql}`);
        },
    };
}

const localRow = (over = {}) => ({
    id: 30, order_kind: 'local', purchase_order_id: 11, status: 'PO_SENT',
    jf_code: 'JF1016', lot_number: null, exp_date: null, port: null, asin: 'B0TEST0001',
    quantity: 120, unit_price: 2.5, po_number: 'PO_00450J', po_date: '2026-11-27', supplier: 'Brookfield Packaging Ltd',
    container_number: null, external_container_number: null, awb_number: null, shipment_id: null,
    ...over,
});
const newLocal = (over = {}) => { const r = localRow(over); delete r.id; delete r.order_kind; return r; };
const stockRow = (over = {}) => ({
    id: 20, order_kind: 'stock', purchase_order_id: 9, status: 'PO_SENT', jf_code: 'JF1016',
    lot_number: '0055000', quantity: 500, unit_price: 1.2, ...over,
});
const standard = { userType: 'standard', body: {} };

beforeEach(() => G.resetCaches());

test('a line created on a local PO is local: kind stamped, nothing armed and no lot looked up', async () => {
    const w = world();
    const out = await G.guardWrite(conn(w), { beforeRow: null, afterRow: newLocal(), via: 'create', req: standard });
    assert.equal(out.refusal, undefined);
    assert.equal(out.sample, false);
    assert.equal(out.local, true);
    assert.deepEqual(out.set, { order_kind: 'local' });
    assert.equal(w.sql.some(s => /app_migrations/.test(s)), false);
    assert.equal(w.sql.some(s => /sample_lots_v/.test(s)), false);
});

test('a local line is edited and moved with nothing extra to write', async () => {
    for (const [via, after] of [['update', localRow({ quantity: 96 })], ['status', localRow({ status: 'ARRIVED_AT_WAREHOUSE' })]]) {
        const out = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: after, via, req: standard });
        assert.equal(out.refusal, undefined, via);
        assert.equal(out.local, true);
        assert.deepEqual(out.set, {});
    }
});

test('a local line refuses a shipping stage or a container, by any write', async () => {
    const stage = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: localRow({ status: 'ON_SEA' }), via: 'status', req: standard });
    assert.equal(stage.refusal.code, 'LOCAL_NOT_SHIPPED');
    assert.deepEqual(stage.refusal.allowedNext, ['ON_AIR', 'ARRIVED_AT_WAREHOUSE']);
    // On the road is allowed, with no AWB; with one it is refused.
    const road = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: localRow({ status: 'ON_AIR' }), via: 'status', req: standard });
    assert.equal(road.refusal, undefined);
    const awb = await G.guardWrite(conn(world()), { beforeRow: localRow({ status: 'ON_AIR' }), afterRow: localRow({ status: 'ON_AIR', awb_number: '176-12345675' }), via: 'update', req: standard });
    assert.equal(awb.refusal.code, 'LOCAL_NOT_SHIPPED');
    const box = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: localRow({ container_number: 'JF-0042' }), via: 'update', req: standard });
    assert.equal(box.refusal.code, 'LOCAL_NOT_SHIPPED');
});

test('a stock line cannot be created on, or moved to, a local PO; a local line cannot leave its PO', async () => {
    const created = await G.guardWrite(conn(world()), { beforeRow: null, afterRow: stockRow({ purchase_order_id: 11, id: undefined }), via: 'create', req: standard });
    // Created on a local PO, the line IS local: the server decides from the PO.
    assert.equal(created.local, true);

    const moved = await G.guardWrite(conn(world()), { beforeRow: stockRow(), afterRow: stockRow({ purchase_order_id: 11 }), via: 'update', req: standard });
    assert.equal(moved.refusal.code, 'LOCAL_PO_REQUIRED');

    for (const poId of [9, 7, null]) {
        const out = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: localRow({ purchase_order_id: poId }), via: 'update', req: standard });
        assert.ok(out.refusal, String(poId));
        assert.ok(['LOCAL_PO_REQUIRED', 'SAMPLE_PO_REQUIRED'].includes(out.refusal.code), out.refusal.code);
    }
});

test('a local line may not take a sample\'s code, as a stock line may not', async () => {
    const out = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: localRow({ lot_number: SAMPLE_LOT }), via: 'update', req: standard });
    assert.equal(out.refusal.code, 'SAMPLE_LOT_ON_STOCK');
    const ordinary = await G.guardWrite(conn(world()), { beforeRow: localRow(), afterRow: localRow({ lot_number: 'B2611' }), via: 'update', req: standard });
    assert.equal(ordinary.refusal, undefined);
});

test('loadPo reads a local PO as local', async () => {
    assert.equal((await G.loadPo(conn(world()), 11)).kind, 'local');
    assert.equal((await G.loadPo(conn(world()), 9)).kind, 'stock');
});

test('notSampleSql keeps stock and local lines and leaves samples out; stockOnlySql keeps stock alone', async () => {
    assert.equal(await G.notSampleSql(conn(world())), " AND orders.order_kind <> 'sample'");
    assert.equal(await G.notSampleSql(conn(world()), 'o'), " AND o.order_kind <> 'sample'");
    await assert.rejects(() => G.notSampleSql(conn(world()), 'o; DROP'), /bad alias/);
    assert.equal(await G.stockOnlySql(conn(world())), " AND orders.order_kind = 'stock'");
    G.resetCaches(); // the schema answer is kept a minute
    assert.equal(await G.notSampleSql(conn(world({ columns: 3 }))), '');
});

test('localReady: both kind checks must allow local; a database without them holds none', async () => {
    assert.equal(await G.localReady(conn(world())), true);
    G.resetCaches();
    assert.equal(await G.localReady(conn(world({ checks: UNESCAPED_LOCAL_CHECKS }))), true);
    G.resetCaches();
    assert.equal(await G.localReady(conn(world({ checks: SAMPLE_ONLY_CHECKS }))), false);
    G.resetCaches();
    assert.equal(await G.localReady(conn(world({ checks: LOCAL_CHECKS.slice(0, 1) }))), false);
    G.resetCaches();
    assert.equal(await G.localReady(conn(world({ columns: 3 }))), false);
});

test('refuseLocal: the pack, split, booking and email-suggestion routes refuse a local line', () => {
    assert.equal(G.refuseLocal(stockRow(), 'packed into a container'), null);
    assert.equal(G.refuseLocal(localRow(), 'packed into a container').code, 'LOCAL_NOT_SHIPPED');
});
