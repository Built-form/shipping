'use strict';

// Unit tests for src/services/sample-orders-check.js: the activation check
// that gates arming sample orders on a stage.
// Run: node --test tools/test-sample-orders-check.js

const { test } = require('node:test');
const assert = require('node:assert/strict');
const C = require('../src/services/sample-orders-check');

// A database in good order. Each key is one question the check asks.
function world(over = {}) {
    return {
        columns: ['order_kind', 'courier_name', 'courier_tracking', 'sample_received_qty', 'sample_status_changed_at', 'sample_ever_sent', 'sample_lot_key', 'kind'],
        indexes: ['uq_orders_sample_lot_key', 'idx_orders_lot_number'],
        viewReadable: true,
        grants: { jportal_app: ['purchase_orders.kind'], jflow_app: ['*'] },
        users: ['jportal_app', 'jflow_app'],
        kindMismatch: [], orphanSamples: [], unknownKinds: [], inContainer: [], badLots: [], badStatus: [],
        ...over,
    };
}

function conn(w) {
    return {
        async query(sql, params = []) {
            const text = String(sql).replace(/\s+/g, ' ').trim();
            if (/FROM information_schema\.COLUMNS/.test(text)) return [w.columns.map(c => ({ name: c }))];
            if (/FROM information_schema\.STATISTICS/.test(text)) return [w.indexes.map(i => ({ name: i }))];
            if (/FROM sample_lots_v LIMIT 1/.test(text)) {
                if (!w.viewReadable) throw Object.assign(new Error("Table 'jfa.sample_lots_v' doesn't exist"), { errno: 1146 });
                return [[]];
            }
            if (/FROM mysql\.user/.test(text)) return [w.users.includes(params[0]) ? [{ ok: 1 }] : []];
            if (/PRIVILEGES/.test(text)) {
                // One statement over the four privilege tables: global or schema
                // ('*'), the table ('t.*'), or the column ('t.c').
                const [user, , , table, , , column] = params;
                const held = w.grants[user] || [];
                const hit = held.includes('*') || held.includes(`${table}.*`) || held.includes(`${table}.${column}`);
                return [hit ? [{ ok: 1 }] : []];
            }
            if (/o\.order_kind <> po\.kind/.test(text)) return [w.kindMismatch];
            if (/purchase_order_id IS NULL/.test(text)) return [w.orphanSamples];
            if (/order_kind NOT IN/.test(text)) return [w.unknownKinds];
            if (/container_number IS NOT NULL/.test(text)) return [w.inContainer];
            if (/NOT EXISTS \(SELECT 1 FROM sample_lots_v/.test(text)) return [w.badLots];
            if (/o\.status NOT IN/.test(text)) return [w.badStatus];
            throw new Error(`unexpected SQL: ${text}`);
        },
    };
}

const byName = (results, name) => results.find(r => r.name === name);
const run = (w, opts = {}) => C.runChecks(conn(w), { portalUser: 'jportal_app', jflowUser: 'jflow_app', jflowReportsSamples: true, ...opts });

test('a database in good order passes every check and may be armed', async () => {
    const results = await run(world());
    assert.deepEqual(results.filter(r => r.status !== 'ok').map(r => r.name), []);
    const s = C.summarise(results);
    assert.deepEqual(s, { ok: true, canArm: true, failed: [], unchecked: [] });
});

test('a missing column, index or view fails the schema checks', async () => {
    const noColumn = await run(world({ columns: ['order_kind', 'kind'] }));
    assert.equal(byName(noColumn, 'schema: columns').status, 'fail');
    assert.match(byName(noColumn, 'schema: columns').detail, /sample_lot_key/);
    const noIndex = await run(world({ indexes: ['idx_orders_lot_number'] }));
    assert.equal(byName(noIndex, 'schema: unique key on the sample code').status, 'fail');
    const noView = await run(world({ viewReadable: false }));
    assert.equal(byName(noView, 'schema: sample_lots_v can be read').status, 'fail');
    assert.equal(C.summarise(noView).canArm, false);
});

test('where the schema is missing, the data checks are not run, and say so', async () => {
    const results = await run(world({ columns: [] }));
    assert.equal(byName(results, 'data: every line has its PO\'s kind').status, 'unchecked');
    assert.equal(C.summarise(results).canArm, false);
});

test('the portal login must be able to read the kind; a server with no portal login has nothing to check', async () => {
    const noGrant = await run(world({ grants: { jportal_app: [], jflow_app: ['*'] } }));
    assert.equal(byName(noGrant, 'portal login can read purchase_orders.kind').status, 'fail');
    const tableGrant = await run(world({ grants: { jportal_app: ['purchase_orders.*'], jflow_app: ['*'] } }));
    assert.equal(byName(tableGrant, 'portal login can read purchase_orders.kind').status, 'ok');
    const noLogin = await run(world({ users: ['jflow_app'] }));
    const r = byName(noLogin, 'portal login can read purchase_orders.kind');
    assert.equal(r.status, 'ok');
    assert.match(r.detail, /no .* login on this server/);
});

test('JFlow: its login must read both kind columns, and it must say its model knows samples', async () => {
    const half = await run(world({ grants: { jportal_app: ['purchase_orders.kind'], jflow_app: ['purchase_orders.kind'] } }));
    assert.equal(byName(half, 'JFlow login can read the kind columns').status, 'fail');
    // Not told the login or the answer: unchecked, which blocks arming without failing the report.
    const untold = await run(world(), { jflowUser: null, jflowReportsSamples: null });
    assert.equal(byName(untold, 'JFlow login can read the kind columns').status, 'unchecked');
    assert.equal(byName(untold, 'JFlow reports the sample payment rule').status, 'unchecked');
    const s = C.summarise(untold);
    assert.equal(s.ok, true);
    assert.equal(s.canArm, false);
    assert.deepEqual(s.unchecked, ['JFlow login can read the kind columns', 'JFlow reports the sample payment rule']);
    const old = await run(world(), { jflowReportsSamples: false });
    assert.equal(byName(old, 'JFlow reports the sample payment rule').status, 'fail');
});

test('each data check names the rows that fail it', async () => {
    const cases = [
        ['kindMismatch', 'data: every line has its PO\'s kind'],
        ['orphanSamples', 'data: every sample line has a PO'],
        ['unknownKinds', 'data: no unknown kind'],
        ['inContainer', 'data: no sample in a container, AWB or shipment'],
        ['badLots', 'data: every sample\'s lot is a sample of its product in JFPRO'],
        ['badStatus', 'data: every sample is in a sample state'],
    ];
    for (const [key, name] of cases) {
        const results = await run(world({ [key]: [{ id: 41 }, { id: 42 }] }));
        const r = byName(results, name);
        assert.equal(r.status, 'fail', name);
        assert.deepEqual(r.rows, [{ id: 41 }, { id: 42 }], name);
        assert.equal(C.summarise(results).canArm, false, name);
        assert.deepEqual(C.summarise(results).failed, [name]);
    }
});

// JFlow's health route sits behind its gateway's sign-in: the answer is read
// with a token, and anything short of a clear yes is a no, with the reason.
const fakeFetch = (status, body, seen = []) => async (url, init) => {
    seen.push({ url, init });
    return { ok: status >= 200 && status < 300, status, json: async () => { if (body === undefined) throw new Error('not json'); return body; } };
};

test('JFlow health: no URL is unasked; a yes is read from either shape', async () => {
    assert.deepEqual(await C.askJflowHealth(null, { fetchImpl: fakeFetch(200, {}) }), { answer: null, detail: null });
    assert.equal((await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(200, { paymentsFlow: { sampleOrders: true } }) })).answer, true);
    assert.equal((await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(200, { sampleOrders: true }) })).answer, true);
});

test('JFlow health: an older JFlow, a model without the rule, or a truthy non-true is a no', async () => {
    for (const body of [{ status: 'ok' }, { paymentsFlow: { sampleOrders: false } }, { paymentsFlow: { sampleOrders: 'yes' } }, { sampleOrders: 1 }, null]) {
        const got = await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(200, body) });
        assert.equal(got.answer, false, JSON.stringify(body));
        assert.match(got.detail, /does not report the sample rule/);
    }
});

test('JFlow health: the token is sent as a bearer and never repeated in the detail', async () => {
    const seen = [];
    await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(200, { sampleOrders: true }, seen), token: 'tok-123' });
    assert.equal(seen[0].init.headers.authorization, 'Bearer tok-123');
    const none = [];
    await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(200, { sampleOrders: true }, none) });
    assert.equal('authorization' in none[0].init.headers, false);
    const refused = await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(401, { message: 'Unauthorized' }), token: 'tok-123' });
    assert.equal(refused.answer, false);
    assert.match(refused.detail, /answered 401/);
    assert.match(refused.detail, /JFLOW_TOKEN/);
    assert.doesNotMatch(refused.detail, /tok-123/);
});

test('JFlow health: a route that cannot be reached, or does not answer JSON, is a no with the reason', async () => {
    const down = await C.askJflowHealth('https://j/health', { fetchImpl: async () => { throw new Error('getaddrinfo ENOTFOUND j'); } });
    assert.equal(down.answer, false);
    assert.match(down.detail, /ENOTFOUND/);
    const html = await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(200, undefined) });
    assert.equal(html.answer, false);
    const error = await C.askJflowHealth('https://j/health', { fetchImpl: fakeFetch(503, {}) });
    assert.equal(error.answer, false);
    assert.match(error.detail, /answered 503/);
    assert.doesNotMatch(error.detail, /JFLOW_TOKEN/);
});
