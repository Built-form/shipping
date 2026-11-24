'use strict';

// What PATCH /shipments/:id does for the forwarders of a container (2026-11-24,
// docs/forwarder-containers.md), against a scripted connection: the delivery and
// customs picks, and the delivery date, which lives on the orders only. The rest
// of the route is exercised over HTTP by tools/test-shipments-api.js.
//
//   npm run test:unit

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { registerShipmentRoutes } = require('../src/services/shipment-routes');
const { ShipmentError } = require('../src/lib/shipments');

const SHIP = (over = {}) => ({
    id: 1214, name: 'DRAFT-SEA-261101 - 339', reference: '339', mode: 'SEA', mode_source: 'user', stage: 'BOOKED', deleted_at: null, merged_into_id: null,
    forwarder: 'DCG', forwarder_id: 3, delivery_forwarder_id: null, customs_forwarder_id: null, tracking_ref: null,
    ...over,
});
const FORWARDERS = [{ id: 3, deleted_at: null }, { id: 5, deleted_at: null }, { id: 7, deleted_at: '2026-11-01 10:00:00' }];
// `delivery_at`: what the order holds for delivery, in the one form, as the route's own SELECT reads it.
const ORDER = (id, delivery_at = null) => ({ id, shipment_id: 1214, delivery_date: delivery_at ? new Date(`${delivery_at.slice(0, 10)}T12:00:00Z`) : null, delivery_at });

function build({ ship = SHIP(), orders = [ORDER(1), ORDER(2)] } = {}) {
    const asked = [];
    const audits = [];
    const conn = {
        query: async (sql, params = []) => {
            const text = String(sql).replace(/\s+/g, ' ').trim();
            asked.push({ sql: text, params });
            if (/^SELECT \* FROM shipments WHERE id = \? FOR UPDATE/.test(text)) return [ship ? [ship] : []];
            if (/^SELECT id, deleted_at FROM forwarders WHERE id = \? FOR SHARE/.test(text)) return [FORWARDERS.filter(f => f.id === Number(params[0]))];
            if (/^UPDATE shipments SET /.test(text)) return [{ affectedRows: 1 }];
            if (/FROM orders WHERE shipment_id = \? AND deleted_at IS NULL ORDER BY id FOR UPDATE/.test(text)) return [orders];
            if (/^SELECT \* FROM orders WHERE 1 = 1 AND orders\.id = \?/.test(text)) return [orders.filter(o => o.id === Number(params[0]))];
            if (/^UPDATE orders SET /.test(text)) return [{ affectedRows: 1 }];
            throw new Error(`unscripted statement: ${text.slice(0, 140)}`);
        },
    };
    const app = Object.fromEntries(['get', 'post', 'put', 'patch', 'delete'].map(m => [m, () => {}]));
    const { patchShipmentIn } = registerShipmentRoutes(app, {
        withConnection: fn => fn(conn),
        ready: async () => {},
        ORDER_SELECT: 'SELECT * FROM orders WHERE 1 = 1',
        rowToOrder: row => ({ id: row.id }),
        recordAudit: async (c, entry) => { audits.push(entry); },
        draftAudit: {},
        log: { error() {}, warn() {} },
    });
    const patch = body => patchShipmentIn(conn, 1214, body, 'ops@us.com');
    return { patch, asked, audits, wrote: pattern => asked.filter(a => pattern.test(a.sql)) };
}
const refusal = async (promise) => {
    try { await promise; } catch (e) { if (e instanceof ShipmentError) return { status: e.status, code: e.code, field: (e.payload || {}).field }; throw e; }
    return null;
};

// ── the delivery and customs picks ───────────────────────────────────────

test('picks: a forwarder from the list is written on the shipment, read under a shared lock, and audited', async () => {
    const t = build();
    await t.patch({ deliveryForwarderId: 5, customsForwarderId: '5' });
    const [update] = t.wrote(/^UPDATE shipments SET /);
    assert.equal(update.sql, 'UPDATE shipments SET delivery_forwarder_id = ?, customs_forwarder_id = ? WHERE id = ?');
    assert.deepEqual(update.params, [5, 5, 1214]);
    // The forwarder rows were LOCKING reads: one being taken off the list waits for this write.
    assert.deepEqual(t.wrote(/FROM forwarders WHERE id = \? FOR SHARE/).map(a => a.params[0]), [5, 5]);
    const audit = t.audits.find(a => a.entityType === 'shipment');
    assert.deepEqual([audit.before, audit.after], [{ deliveryForwarderId: null, customsForwarderId: null }, { deliveryForwarderId: 5, customsForwarderId: 5 }]);
});

test('picks: null clears one; the same value again changes nothing', async () => {
    let t = build({ ship: SHIP({ delivery_forwarder_id: 5, customs_forwarder_id: 5 }) });
    await t.patch({ deliveryForwarderId: null });
    assert.deepEqual(t.wrote(/^UPDATE shipments SET /).map(a => [a.sql, a.params]), [['UPDATE shipments SET delivery_forwarder_id = ? WHERE id = ?', [null, 1214]]]);
    assert.equal(t.wrote(/FOR SHARE/).length, 0, 'clearing reads no forwarder');
    t = build({ ship: SHIP({ delivery_forwarder_id: 5 }) });
    const out = await t.patch({ deliveryForwarderId: 5 });
    assert.equal(out.unchanged, true);
    assert.equal(t.wrote(/^UPDATE /).length, 0);
});

test('picks: not an id, not on the list, or removed from it: 400 naming the field, and nothing is written', async () => {
    for (const [body, field] of [
        [{ deliveryForwarderId: 'x' }, 'deliveryForwarderId'], [{ deliveryForwarderId: 0 }, 'deliveryForwarderId'],
        [{ customsForwarderId: 999 }, 'customsForwarderId'], [{ customsForwarderId: 7 }, 'customsForwarderId'],
    ]) {
        const t = build();
        assert.deepEqual(await refusal(t.patch(body)), { status: 400, code: 'BAD_FIELD', field }, JSON.stringify(body));
        assert.equal(t.wrote(/^UPDATE /).length, 0);
    }
});

test('picks: they do not touch the freight pair, and a draft takes them too', async () => {
    const t = build({ ship: SHIP({ stage: 'DRAFT', reference: null }) });
    await t.patch({ customsForwarderId: 5 });
    assert.deepEqual(t.wrote(/^UPDATE shipments SET /).map(a => a.sql), ['UPDATE shipments SET customs_forwarder_id = ? WHERE id = ?']);
});

// ── the delivery date: on the orders only ────────────────────────────────

test('delivery date: written to EVERY member order in the shape a hand-typed value has, one audited update each, and never on the shipment', async () => {
    const t = build();
    await t.patch({ deliveryDate: '2026-12-20T09:30' });
    assert.equal(t.wrote(/^UPDATE shipments SET /).length, 0, 'no shipment column holds it');
    assert.deepEqual(t.wrote(/^UPDATE orders SET /).map(a => [a.sql, a.params]), [
        ['UPDATE orders SET delivery_date = ? WHERE id = ?', ['2026-12-20 09:30:00', 1]],
        ['UPDATE orders SET delivery_date = ? WHERE id = ?', ['2026-12-20 09:30:00', 2]],
    ]);
    assert.deepEqual(t.audits.filter(a => a.entityType === 'order').map(a => [a.entityId, a.action, a.userEmail]), [[1, 'update', 'ops@us.com'], [2, 'update', 'ops@us.com']]);
    assert.deepEqual(t.audits.find(a => a.entityType === 'shipment').after, { deliveryDate: '2026-12-20T09:30' });
    // What each order holds is read in the one form, in SQL, under the lock.
    assert.match(t.wrote(/FROM orders WHERE shipment_id = \? AND deleted_at IS NULL ORDER BY id FOR UPDATE/)[0].sql, /DATE_FORMAT\(delivery_date, '%H:%i'\) = '00:00'.* AS delivery_at/);
});

test('delivery date: a day alone is midnight; an order that holds it already is left alone', async () => {
    const t = build({ orders: [ORDER(1, '2026-12-20'), ORDER(2, '2026-12-19T08:00'), ORDER(3)] });
    await t.patch({ deliveryDate: '2026-12-20' });
    assert.deepEqual(t.wrote(/^UPDATE orders SET /).map(a => a.params), [['2026-12-20 00:00:00', 2], ['2026-12-20 00:00:00', 3]]);
    // Midnight said outright is the same value.
    const again = build({ orders: [ORDER(1, '2026-12-20')] });
    const out = await again.patch({ deliveryDate: '2026-12-20T00:00' });
    assert.equal(again.wrote(/^UPDATE orders SET /).length, 0);
    assert.equal(again.audits.filter(a => a.entityType === 'order').length, 0);
    assert.equal(out.id, 1214);
});

test('delivery date: null clears it on every order that holds one', async () => {
    const t = build({ orders: [ORDER(1, '2026-12-20'), ORDER(2)] });
    await t.patch({ deliveryDate: null });
    assert.deepEqual(t.wrote(/^UPDATE orders SET /).map(a => a.params), [[null, 1]]);
});

test('delivery date: not a date is 400 BAD_DATE; on a shipment that is not booked it is 422; nothing is written either way', async () => {
    for (const bad of ['20/12/2026', '2026-12-20T25:00', '2026-02-30', 'soon', 5]) {
        const t = build();
        assert.deepEqual(await refusal(t.patch({ deliveryDate: bad })), { status: 400, code: 'BAD_DATE', field: 'deliveryDate' }, String(bad));
        assert.equal(t.wrote(/^UPDATE /).length, 0);
    }
    const draft = build({ ship: SHIP({ stage: 'DRAFT', reference: null }) });
    assert.deepEqual(await refusal(draft.patch({ deliveryDate: '2026-12-20' })), { status: 422, code: 'NOT_BOOKED', field: 'deliveryDate' });
    assert.equal(draft.wrote(/^UPDATE /).length, 0);
});

test('delivery date: goes with a loading date and a departure date in one call, as an accepted proposal writes them', async () => {
    const t = build();
    await t.patch({ loadingDate: '2026-12-01', etd: '2026-12-05', deliveryDate: '2026-12-20' });
    assert.deepEqual(t.wrote(/^UPDATE shipments SET /).map(a => a.sql), ['UPDATE shipments SET loading_date = ?, etd = ? WHERE id = ?']);
    assert.deepEqual(t.wrote(/^UPDATE orders SET /).map(a => [a.sql, a.params]), [
        ['UPDATE orders SET estimated_departure_date = ?, delivery_date = ? WHERE id = ?', ['2026-12-05', '2026-12-20 00:00:00', 1]],
        ['UPDATE orders SET estimated_departure_date = ?, delivery_date = ? WHERE id = ?', ['2026-12-05', '2026-12-20 00:00:00', 2]],
    ]);
});
