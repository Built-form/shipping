'use strict';

// Frozen-shape check for the shipments rollout: every key the legacy read
// routes returned before the change must come back byte-for-byte the same.
// Only two additive keys are allowed: `shipmentId` on each order and the
// top-level `shipments` side-map on GET /orders. Also asserts that no order
// audit row carries a shipmentId key.
//
//   1. against the PREVIOUS code (e.g. a worktree at the last deploy tag):
//        TEST_BASE_URL=http://localhost:3032 node tools/test-orders-shape-frozen.js capture baseline.json
//   2. straight after, against the new code on the same DB, before anything
//      else writes to it:
//        TEST_BASE_URL=http://localhost:3031 node tools/test-orders-shape-frozen.js compare baseline.json
//   3. any time (e.g. after the HTTP suites):
//        node tools/test-orders-shape-frozen.js audit
//
// Read-only.

require('dotenv').config();
const fs = require('fs');
const axios = require('axios');
const { getPool, closePool } = require('../src/db');
const { deepEqual } = require('../src/lib/audit');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3031';
const [mode, file] = process.argv.slice(2);
const ENDPOINTS = [
    '/api/v1/orders',
    '/api/v1/draft-containers',
    '/api/v1/planned-containers',
    '/api/v1/draft-container-registry',
    '/api/v1/containers',
];
const ALLOWED_ORDER_KEYS = new Set(['shipmentId']);
const ALLOWED_TOP_KEYS = { '/api/v1/orders': new Set(['shipments']) };

const api = axios.create({ baseURL: BASE, validateStatus: () => true, maxContentLength: Infinity, maxBodyLength: Infinity });
let pass = 0;
let fail = 0;
function check(label, cond, detail = '') {
    if (cond) { pass++; console.log(`  PASS  ${label}`); } else { fail++; console.log(`  FAIL  ${label}${detail ? ' — ' + detail : ''}`); }
}

async function fetchAll() {
    const out = {};
    for (const ep of ENDPOINTS) {
        const r = await api.get(ep, { transformResponse: x => x });
        if (r.status !== 200) throw new Error(`${ep} -> ${r.status}`);
        out[ep] = { bytes: Buffer.byteLength(r.data), body: JSON.parse(r.data) };
    }
    return out;
}

function diffKeys(a, b) {
    const keys = new Set([...Object.keys(a || {}), ...Object.keys(b || {})]);
    return [...keys].filter(k => !deepEqual(a[k], b[k]));
}

async function auditCheck() {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [[r]] = await conn.query(`
            SELECT COUNT(*) AS n FROM audit_log
             WHERE entity_type = 'order'
               AND (JSON_CONTAINS_PATH(COALESCE(before_json, JSON_OBJECT()), 'one', '$.shipmentId')
                    OR JSON_CONTAINS_PATH(COALESCE(after_json, JSON_OBJECT()), 'one', '$.shipmentId'))`);
        check('no order audit row carries a shipmentId key', Number(r.n) === 0, `${r.n} row(s)`);
    } finally {
        conn.release();
        await closePool();
    }
}

async function guard() {
    const r = await api.get('/api/v1/_dev/db');
    if (r.status === 200) return r.data.looksTest;
    // The previous code's dev server has no /_dev/db: fall back to .env.
    return /test/i.test(process.env.DB_HOST || '') || /test/i.test(process.env.DB_NAME || '');
}

(async () => {
    try {
        if (!['capture', 'compare', 'audit'].includes(mode) || (mode !== 'audit' && !file)) {
            console.error('usage: test-orders-shape-frozen.js capture <file> | compare <file> | audit');
            process.exit(2);
        }
        if (mode === 'audit') { await auditCheck(); return; }
        if (!(await guard())) { console.error('Refusing: the server does not report a TEST database.'); process.exit(2); }
        console.log(`Base URL: ${BASE}`);
        if (mode === 'capture') {
            const snap = await fetchAll();
            fs.writeFileSync(file, JSON.stringify(snap));
            for (const ep of ENDPOINTS) console.log(`  captured ${ep}: ${snap[ep].bytes} bytes`);
            return;
        }
        const baseline = JSON.parse(fs.readFileSync(file, 'utf8'));
        const now = await fetchAll();
        for (const ep of ENDPOINTS) {
            const was = baseline[ep].body;
            const is = now[ep].body;
            const delta = now[ep].bytes - baseline[ep].bytes;
            console.log(`\n${ep}  (${baseline[ep].bytes} -> ${now[ep].bytes} bytes, ${delta >= 0 ? '+' : ''}${delta})`);
            const allowedTop = ALLOWED_TOP_KEYS[ep] || new Set();
            const extraTop = Object.keys(is).filter(k => !(k in was));
            check('no unexpected new top-level keys', extraTop.every(k => allowedTop.has(k)), extraTop.join(', '));
            for (const k of Object.keys(was)) {
                if (k === 'data' && ep === '/api/v1/orders') continue;
                check(`top-level "${k}" unchanged`, deepEqual(was[k], is[k]), diffKeys(was[k], is[k]).slice(0, 5).join(', '));
            }
            if (ep !== '/api/v1/orders') continue;
            const byId = new Map(is.data.map(o => [o.id, o]));
            let missing = 0; let changed = 0; let extra = 0;
            const samples = [];
            for (const o of was.data) {
                const n = byId.get(o.id);
                if (!n) { missing++; continue; }
                const bad = Object.keys(o).filter(k => !deepEqual(o[k], n[k]));
                if (bad.length) { changed++; if (samples.length < 5) samples.push(`${o.id}: ${bad.join(',')}`); }
                const add = Object.keys(n).filter(k => !(k in o) && !ALLOWED_ORDER_KEYS.has(k));
                if (add.length) { extra++; if (samples.length < 5) samples.push(`${o.id} +${add.join(',')}`); }
            }
            check(`all ${was.data.length} orders still present`, missing === 0, `${missing} missing`);
            check('every pre-existing order key deep-equal', changed === 0, samples.join(' | '));
            check('no unexpected per-order keys', extra === 0, samples.join(' | '));
            check('shipments side-map is an object', is.shipments && typeof is.shipments === 'object' && !Array.isArray(is.shipments));
            const linked = is.data.filter(o => o.shipmentId != null);
            check('every shipmentId resolves in the side-map', linked.every(o => is.shipments[o.shipmentId]),
                `${linked.filter(o => !is.shipments[o.shipmentId]).length} unresolved`);
            console.log(`        (${linked.length} orders carry a shipmentId, ${Object.keys(is.shipments || {}).length} shipments in the side-map)`);
        }
        await auditCheck();
    } catch (err) {
        fail++;
        console.error('CRASH:', err.message);
    } finally {
        if (mode !== 'capture') console.log(`\n${pass} passed, ${fail} failed`);
        process.exit(fail ? 1 : 0);
    }
})();
