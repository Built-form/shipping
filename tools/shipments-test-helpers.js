'use strict';

// Shared fixtures for the shipments HTTP suites (tools/test-shipments-*.js,
// tools/test-order-split.js). They create, pack, book and close REAL orders,
// so guard() refuses to run unless the server reports a TEST database, and the
// same one this process's .env points at (the suites also read it directly).
//
// Every fixture is tracked and removed by cleanup(): orders are soft-deleted
// (through the API, or directly when receipts block the API), test drafts are
// closed 'deleted', and memberless test shipments are deleted so their
// references are released.

require('dotenv').config();
const axios = require('axios');
const { getPool, closePool } = require('../src/db');
const sync = require('../src/services/shipment-sync');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3031';
const api = axios.create({ baseURL: BASE, validateStatus: () => true });
const STAMP = Date.now();
const created = { orders: [], drafts: new Set(), planned: new Set(), shipments: new Set() };
let pass = 0;
let fail = 0;
let seq = 0;

function check(label, cond, detail = '') {
    if (cond) { pass++; console.log(`  PASS  ${label}`); }
    else { fail++; console.log(`  FAIL  ${label}${detail ? ' — ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)) : ''}`); }
    return !!cond;
}

function section(title) { console.log(`\n${title}`); }

async function guard() {
    const r = await api.get('/api/v1/_dev/db');
    if (r.status !== 200 || !r.data || !r.data.looksTest) {
        console.error(`Refusing to run: ${BASE} does not report a TEST database (GET /api/v1/_dev/db -> ${r.status}).`);
        console.error('Start the local server with `npm run dev:local` (tools/dev-server.js) pointed at the TEST instance.');
        process.exit(2);
    }
    if (r.data.host !== process.env.DB_HOST || r.data.database !== process.env.DB_NAME) {
        console.error(`Refusing to run: the server writes to ${r.data.host}/${r.data.database}, this process reads ${process.env.DB_HOST}/${process.env.DB_NAME}.`);
        process.exit(2);
    }
    console.log(`Base URL: ${BASE}  (TEST database ${r.data.host}/${r.data.database})  stamp ${STAMP}`);
}

async function sql(query, params = []) {
    const [rows] = await getPool().query(query, params);
    return rows;
}

// A test draft name with a real-looking stamp ('DRAFT-SEA-990101-123456 - …').
// The label carries no leading number unless `hint` is given.
function draftName(label, { mode = 'SEA', hint = null, kind = 'DRAFT' } = {}) {
    seq++;
    const six = String((STAMP + seq) % 1000000).padStart(6, '0');
    const name = `${kind}-${mode}-990101-${six} - ${hint != null ? `${hint} ` : ''}SHIPTEST ${label} ${STAMP}`;
    (kind === 'PLANNED' ? created.planned : created.drafts).add(name);
    return name;
}

function reference(label) {
    return `SHIPTEST-${STAMP}-${label}`;
}

async function createOrder(overrides = {}) {
    seq++;
    const body = {
        status: 'READY', jfCode: `SHIPTEST-${STAMP}-${seq}`, productName: `Shipments test fixture ${seq}`,
        quantity: 100, supplier: 'Shiptest Supplier', poNumber: `SHIPTEST-PO-${STAMP}`, ...overrides,
    };
    const r = await api.post('/api/v1/orders', body);
    if (r.status !== 201) throw new Error(`fixture order failed: ${r.status} ${JSON.stringify(r.data)}`);
    created.orders.push(r.data.id);
    return r.data;
}

async function getOrder(id) {
    const rows = await sql(`SELECT * FROM orders WHERE id = ?`, [id]);
    return rows[0] || null;
}

async function shipmentsWhere(params) {
    const r = await api.get('/api/v1/shipments', { params: { limit: 2000, ...params } });
    if (r.status !== 200) throw new Error(`GET /shipments ${r.status} ${JSON.stringify(r.data)}`);
    return r.data.data;
}

async function shipmentByName(name) {
    const rows = await shipmentsWhere({ name });
    return rows[0] || null;
}

async function shipmentByReference(ref) {
    const rows = await shipmentsWhere({ reference: ref });
    if (rows[0]) created.shipments.add(rows[0].id);
    return rows[0] || null;
}

async function shipment(id) {
    const r = await api.get(`/api/v1/shipments/${id}`);
    return r.status === 200 ? r.data : null;
}

function track(shipmentId) {
    if (shipmentId) created.shipments.add(shipmentId);
}

async function addDraftLine(orderId, name, allocated) {
    const r = await api.post('/api/v1/draft-containers', { orderId, draftContainerName: name, allocated });
    if (r.status !== 201) throw new Error(`draft line failed: ${r.status} ${JSON.stringify(r.data)}`);
    return r.data;
}

// Re-run the membership sync for `ids`, as tools/verify-shipments.js --fix
// would after a legacy write the hooks did not see.
async function resyncOrders(ids) {
    const conn = await getPool().getConnection();
    try {
        await conn.beginTransaction();
        await sync.syncOrderMembership(conn, ids);
        await conn.commit();
    } catch (e) {
        await conn.rollback();
        throw e;
    } finally {
        conn.release();
    }
}

async function verify() {
    const conn = await getPool().getConnection();
    try { return await sync.verifyAll(conn, {}); } finally { conn.release(); }
}

function verifySummary(v) {
    return Object.entries(v.hard).filter(([, rows]) => rows.length)
        .map(([k, rows]) => `${k}: ${rows.length} ${JSON.stringify(rows.slice(0, 3))}`).join(' | ');
}

async function cleanup() {
    console.log('\nCleanup');
    // A partial pack splits the order server-side and the slice copies its JF
    // code, so no suite tracks it: sweep this run's fixtures by code as well.
    const strays = await sql(`SELECT id FROM orders WHERE deleted_at IS NULL AND jf_code LIKE ?`, [`SHIPTEST-${STAMP}-%`]);
    for (const { id } of strays) if (!created.orders.includes(id)) created.orders.push(id);
    let orders = 0;
    for (const id of created.orders) {
        const r = await api.delete(`/api/v1/orders/${id}`);
        if (r.status === 200) { orders++; continue; }
        if (r.status === 409 || r.status === 404) {
            // Receipts block the API delete: soft-delete directly, then let the
            // shadow follow (as a hook would).
            await sql(`UPDATE orders SET deleted_at = COALESCE(deleted_at, NOW()) WHERE id = ?`, [id]);
            const conn = await getPool().getConnection();
            try {
                await conn.beginTransaction();
                await sync.syncOrderMembership(conn, [id]);
                await conn.commit();
            } catch (e) {
                await conn.rollback();
                console.log(`  (membership resync for ${id} failed: ${e.message})`);
            } finally {
                conn.release();
            }
            orders++;
        }
    }
    for (const name of created.drafts) {
        await api.post('/api/v1/draft-containers/close', { name, reason: 'deleted' });
    }
    for (const name of created.planned) {
        const r = await api.get('/api/v1/planned-containers', { params: { name } });
        for (const row of (r.data && r.data.data) || []) await api.delete(`/api/v1/planned-containers/${row.id}`);
    }
    // Shipments carrying a test name or reference that are now memberless or
    // open: delete them so their keys are released.
    const rows = await sql(
        `SELECT id FROM shipments WHERE deleted_at IS NULL AND merged_into_id IS NULL
            AND (name LIKE ? OR reference LIKE ? OR id IN (${[0, ...created.shipments].join(',')}))`,
        [`%SHIPTEST%${STAMP}%`, `%SHIPTEST-${STAMP}%`]
    );
    let shipments = 0;
    for (const r of rows) {
        let d = await api.delete(`/api/v1/shipments/${r.id}`);
        if (d.status === 409 && d.data && d.data.code === 'NOT_DELETABLE') {
            // A memberless shipment moved past BOOKED: DELETE takes only BOOKED
            // ones, so force it back first (the dev server runs as admin).
            await api.post(`/api/v1/shipments/${r.id}/transition`, { stage: 'BOOKED', force: true, note: 'test cleanup' });
            d = await api.delete(`/api/v1/shipments/${r.id}`);
        }
        if (d.status === 200) shipments++;
    }
    console.log(`  soft-deleted ${orders} fixture order(s), closed ${created.drafts.size} draft name(s), deleted ${shipments} test shipment(s)`);
}

async function finish() {
    try { await cleanup(); } catch (e) { console.log(`  cleanup error: ${e.message}`); }
    try {
        const v = await verify();
        check('verify: no hard drift after cleanup', v.ok, verifySummary(v));
    } catch (e) {
        check('verify ran', false, e.message);
    }
    console.log(`\n${pass} passed, ${fail} failed`);
    await closePool();
    process.exit(fail ? 1 : 0);
}

module.exports = {
    api, BASE, STAMP, check, section, guard, sql, draftName, reference, createOrder, getOrder,
    shipmentsWhere, shipmentByName, shipmentByReference, shipment, track, addDraftLine, resyncOrders, verify, verifySummary,
    finish, created, counts: () => ({ pass, fail }), fail: (label, err) => check(label, false, err && (err.stack || err.message || err)),
};
