'use strict';

// Unit tests for src/services/sample-orders.js: the guard every order write
// calls, against a connection that answers from a small in-memory state.
// Run: node --test tools/test-sample-orders-guard.js

const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

process.env.APP_STAGE = 'test';
const G = require('../src/services/sample-orders');

const LOT = 'SA0031-S01-V03-CN0011';

function world(over = {}) {
    return {
        columns: 7,
        armed: ['sample_orders_on:test'],
        pos: { 7: { kind: 'sample' }, 9: { kind: 'stock' } },
        sampleLots: [{ lot: LOT, jf: 'JF1016' }],
        emailed: new Set(),
        money: new Set(),
        lookupThrows: false,
        superseded: [],
        sql: [],
        ...over,
    };
}

function conn(state) {
    return {
        async query(sql, params = []) {
            state.sql.push(sql);
            if (/information_schema\.COLUMNS/.test(sql)) return [[{ n: state.columns }]];
            if (/FROM app_migrations/.test(sql)) return [state.armed.filter(n => n === params[0]).map(name => ({ name }))];
            if (/FROM purchase_orders WHERE id = \?/.test(sql)) {
                const p = state.pos[params[0]];
                return [p ? [{ id: params[0], kind: p.kind, supplier: 'Acme Co., Ltd.', currency: 'USD', shipping_total: 0, deleted_at: null }] : []];
            }
            if (/FROM sample_lots_v/.test(sql)) {
                if (state.lookupThrows) throw Object.assign(new Error("Table 'jfa.sample_lots_v' doesn't exist"), { errno: 1146 });
                const [lot, jf] = params;
                const hit = state.sampleLots.filter(l => l.lot.toUpperCase() === String(lot).toUpperCase()
                    && (jf === undefined || l.jf.toUpperCase() === String(jf).toUpperCase()));
                return [hit.map((h, i) => ({ lot_id: i + 1 }))];
            }
            if (/purchase_order_document_sends/.test(sql)) return [state.emailed.has(params[0]) ? [{ hit: 1 }] : []];
            if (/supplier_payment_lines/.test(sql)) return [state.money.has(params[0]) ? [{ hit: 1 }] : []];
            if (/po_change_requests/.test(sql)) {
                if (state.noPortal) throw Object.assign(new Error("Table 'jportal.po_change_requests' doesn't exist"), { errno: 1146 });
                state.superseded.push(params[1]);
                return [{ affectedRows: 1 }];
            }
            throw new Error(`unexpected SQL: ${sql}`);
        },
    };
}

const sampleRow = (over = {}) => ({
    id: 10, order_kind: 'sample', purchase_order_id: 7, status: 'PO_SENT',
    jf_code: 'JF1016', lot_number: LOT, asin: null, quantity: 2, unit_price: 0,
    po_number: 'PO_00431J', po_date: '2026-11-25', supplier: 'Acme Co., Ltd.',
    shipped_date: null, arrived_date: null, sample_received_qty: null, sample_ever_sent: 0,
    container_number: null, awb_number: null, external_container_number: null, shipment_id: null,
    courier_name: null, courier_tracking: null, ...over,
});
// What POST /orders builds for a new line: no kind yet, the server decides it.
const newLine = (over = {}) => { const r = sampleRow(over); delete r.id; delete r.order_kind; return r; };
const stockRow = (over = {}) => ({
    id: 20, order_kind: 'stock', purchase_order_id: 9, status: 'PO_SENT', jf_code: 'JF1016',
    lot_number: '0055000', quantity: 500, unit_price: 1.2, ...over,
});

const admin = (reason) => ({ userType: 'admin', body: reason ? { reason } : {} });
const standard = { userType: 'standard', body: {} };

beforeEach(() => G.resetCaches());

// ── a database with no sample columns ────────────────────────────────────

test('where the sample columns do not exist, every write is a stock write and nothing is asked', async () => {
    const w = world({ columns: 3 });
    const out = await G.guardWrite(conn(w), { beforeRow: null, afterRow: newLine(), via: 'create', req: standard });
    assert.equal(out.refusal, undefined);
    assert.equal(out.sample, false);
    assert.deepEqual(out.set, {});
    assert.equal(w.sql.length, 1); // the capability read, and no other
    assert.equal(await G.stockOnlySql(conn(w)), '');
    assert.equal(await G.armed(conn(w), { fresh: true }), false);
});

test('stockOnlySql filters on the kind once the column exists, and refuses an odd alias', async () => {
    assert.equal(await G.stockOnlySql(conn(world())), " AND orders.order_kind = 'stock'");
    assert.equal(await G.stockOnlySql(conn(world()), 'o'), " AND o.order_kind = 'stock'");
    await assert.rejects(() => G.stockOnlySql(conn(world()), 'o; DROP'), /bad alias/);
});

// ── stock lines ──────────────────────────────────────────────────────────

test('a stock line cannot be created on, or moved to, a sample PO', async () => {
    const onSample = await G.guardWrite(conn(world({ armed: [] })), {
        beforeRow: stockRow(), afterRow: stockRow({ purchase_order_id: 7 }), via: 'update', req: standard,
    });
    assert.equal(onSample.refusal.code, 'SAMPLE_PO_REQUIRED');
    assert.equal(onSample.refusal.http, 409);
});

test('a stock line may not take a sample lot, keeps one it has, and is not held up by a failed lookup', async () => {
    const take = await G.guardWrite(conn(world()), { beforeRow: stockRow(), afterRow: stockRow({ lot_number: LOT.toLowerCase() }), via: 'update', req: standard });
    assert.equal(take.refusal.code, 'SAMPLE_LOT_ON_STOCK');

    const created = await G.guardWrite(conn(world()), { beforeRow: null, afterRow: stockRow({ lot_number: LOT }), via: 'create', req: standard });
    assert.equal(created.refusal.code, 'SAMPLE_LOT_ON_STOCK');

    const w = world();
    const keeps = await G.guardWrite(conn(w), { beforeRow: stockRow({ lot_number: LOT }), afterRow: stockRow({ lot_number: LOT, quantity: 400 }), via: 'update', req: standard });
    assert.equal(keeps.refusal, undefined);
    assert.equal(w.sql.some(s => /sample_lots_v/.test(s)), false);

    const down = await G.guardWrite(conn(world({ lookupThrows: true })), { beforeRow: stockRow(), afterRow: stockRow({ lot_number: '0055001' }), via: 'update', req: standard });
    assert.equal(down.refusal, undefined);
    assert.equal(down.sample, false);

    const ordinary = await G.guardWrite(conn(world()), { beforeRow: stockRow(), afterRow: stockRow({ lot_number: '0055001' }), via: 'update', req: standard });
    assert.equal(ordinary.refusal, undefined);
});

// ── creating a sample ────────────────────────────────────────────────────

test('a line created on a sample PO is a sample: kind stamped, no price stored as 0', async () => {
    const out = await G.guardWrite(conn(world()), { beforeRow: null, afterRow: newLine({ unit_price: null }), via: 'create', req: standard });
    assert.equal(out.refusal, undefined);
    assert.equal(out.sample, true);
    assert.equal(out.set.order_kind, 'sample');
    assert.equal(out.set.unit_price, 0);
    assert.equal(out.statusChanged, true);
});

test('creating needs this stage armed: no row, or another stage\'s row, refuses', async () => {
    for (const armedRows of [[], ['sample_orders_on:dev']]) {
        G.resetCaches();
        const out = await G.guardWrite(conn(world({ armed: armedRows })), { beforeRow: null, afterRow: newLine(), via: 'create', req: standard });
        assert.equal(out.refusal.code, 'SAMPLES_DISABLED', JSON.stringify(armedRows));
        assert.equal(out.refusal.http, 409);
    }
});

test('the arming row is read fresh on a create, not from a remembered answer', async () => {
    const w = world();
    assert.equal(await G.armed(conn(w)), true);          // remembered as armed
    w.armed = [];                                        // the database was rebuilt under the server
    assert.equal(await G.armed(conn(w)), true);          // display may lag a minute
    const out = await G.guardWrite(conn(w), { beforeRow: null, afterRow: newLine(), via: 'create', req: standard });
    assert.equal(out.refusal.code, 'SAMPLES_DISABLED');  // a create may not
});

test('an existing sample line stays usable while the feature is switched off', async () => {
    const out = await G.guardWrite(conn(world({ armed: [] })), {
        beforeRow: sampleRow(), afterRow: sampleRow({ status: 'ON_AIR', shipped_date: '2026-11-26' }), via: 'status', req: standard,
    });
    assert.equal(out.refusal, undefined);
    assert.equal(out.sample, true);
    assert.equal(out.set.sample_ever_sent, 1);
    assert.equal(out.supersedePending, true);
});

test('the lot must be a sample of that product in the register; an unreadable register refuses', async () => {
    const wrongProduct = await G.guardWrite(conn(world()), { beforeRow: null, afterRow: newLine({ jf_code: 'JF2000' }), via: 'create', req: standard });
    assert.equal(wrongProduct.refusal.code, 'SAMPLE_LOT_NOT_ELIGIBLE');
    const notASample = await G.guardWrite(conn(world()), { beforeRow: null, afterRow: newLine({ lot_number: '0055000' }), via: 'create', req: standard });
    assert.equal(notASample.refusal.code, 'SAMPLE_LOT_NOT_ELIGIBLE');
    const down = await G.guardWrite(conn(world({ lookupThrows: true })), { beforeRow: null, afterRow: newLine(), via: 'create', req: standard });
    assert.equal(down.refusal.code, 'SAMPLE_LOT_UNCHECKED');
    assert.equal(down.refusal.http, 503);
});

test('the lot lookup locks the register rows only when the caller is in a transaction', async () => {
    const locked = world();
    await G.guardWrite(conn(locked), { beforeRow: null, afterRow: newLine(), via: 'create', req: standard, lockLot: true });
    assert.equal(locked.sql.filter(s => /sample_lots_v/.test(s)).every(s => /FOR SHARE/.test(s)), true);
    const plain = world();
    await G.guardWrite(conn(plain), { beforeRow: null, afterRow: newLine(), via: 'create', req: standard });
    assert.equal(plain.sql.filter(s => /sample_lots_v/.test(s)).some(s => /FOR SHARE/.test(s)), false);
    // An edit that leaves product and lot alone does not look the lot up at all.
    const edit = world();
    await G.guardWrite(conn(edit), { beforeRow: sampleRow(), afterRow: sampleRow({ quantity: 3 }), via: 'update', req: standard });
    assert.equal(edit.sql.some(s => /sample_lots_v/.test(s)), false);
});

// ── a committed PO ───────────────────────────────────────────────────────

test('once the PO is emailed or has money on it, the bill changes only by an admin with a reason', async () => {
    for (const state of [{ emailed: new Set([7]) }, { money: new Set([7]) }]) {
        const write = (req) => G.guardWrite(conn(world(state)), { beforeRow: sampleRow(), afterRow: sampleRow({ quantity: 1 }), via: 'update', req });
        assert.equal((await write(standard)).refusal.code, 'SAMPLE_BILL_LOCKED');
        assert.equal((await write(admin())).refusal.code, 'SAMPLE_REASON_REQUIRED');
        const ok = await write(admin('supplier re-quoted'));
        assert.equal(ok.refusal, undefined);
        assert.equal(ok.billChanged, true);
    }
    const open = await G.guardWrite(conn(world()), { beforeRow: sampleRow(), afterRow: sampleRow({ quantity: 1 }), via: 'update', req: standard });
    assert.equal(open.refusal, undefined);
});

test('poCommitted: emailed, or money, or neither', async () => {
    assert.equal(await G.poCommitted(conn(world()), 7), false);
    assert.equal(await G.poCommitted(conn(world({ emailed: new Set([7]) })), 7), true);
    assert.equal(await G.poCommitted(conn(world({ money: new Set([7]) })), 7), true);
    assert.equal(await G.poMoneyRecorded(conn(world({ emailed: new Set([7]) })), 7), false);
});

// ── the rest ─────────────────────────────────────────────────────────────

test('refuseSample: only for a sample row, and it says where the line can go', () => {
    assert.equal(G.refuseSample(stockRow(), 'packed into a container'), null);
    const r = G.refuseSample(sampleRow(), 'packed into a container');
    assert.equal(r.code, 'SAMPLE_NOT_STOCK');
    assert.equal(r.http, 409);
    assert.equal(r.message, 'A sample order cannot be packed into a container.');
    assert.deepEqual(r.allowedNext, ['ON_AIR']);
});

test('toBody is what a route sends: the message as error, the code, and the hints', () => {
    const body = G.toBody({ ok: false, http: 409, code: 'SAMPLE_STATUS', message: 'No.', allowedNext: ['ON_AIR'] });
    assert.deepEqual(body, { error: 'No.', code: 'SAMPLE_STATUS', allowedNext: ['ON_AIR'] });
});

test('superseding a pending request: writes the row, and a server with no portal schema has none', async () => {
    const w = world();
    assert.equal(await G.supersedePendingMarkSent(conn(w), 10, 'ops@example.com'), 1);
    assert.deepEqual(w.superseded, [10]);
    assert.equal(await G.supersedePendingMarkSent(conn(world({ noPortal: true })), 10, 'ops@example.com'), 0);
});

test('who is asking: an admin by user type, a reason from the body', () => {
    assert.equal(G.isAdmin({ userType: 'admin' }), true);
    assert.equal(G.isAdmin({ userType: 'standard' }), false);
    assert.equal(G.isAdmin(null), false);
    assert.equal(G.reasonOf({ body: { reason: '  typo  ' } }), 'typo');
    assert.equal(G.reasonOf({ body: { reason: '   ' } }), null);
    assert.equal(G.reasonOf({ body: {} }), null);
});
