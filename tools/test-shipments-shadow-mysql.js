'use strict';

// shadow() against real MySQL: what a fake connection cannot show.
//
//   node tools/test-shipments-shadow-mysql.js
//
// Starts its own in-process server (auth bypassed, like tools/dev-server.js) on
// a free port with SHIPMENTS_SHADOW_FAULT set for one autocommit route and one
// transactional route, so the shadow throws AFTER it has written. Asserts, for
// both: the legacy request still answers 2xx, its legacy write is committed, no
// partial shadow row survives, and exactly one coalescing failure row is left.
// Then drives shadow() directly to prove the coalescing (a repeat bumps
// occurrences on the same row; a resolved key starts a fresh row).
// TEST database only; needs the shadow armed.

process.env.IS_OFFLINE = '1';
process.env.LOCAL_USER_TYPE = 'admin';
process.env.SHIPMENTS_SHADOW_FAULT = 'POST /draft-containers,POST /draft-containers/rename';
require('dotenv').config();

const axios = require('axios');
const { getPool, closePool } = require('../src/db');
const sync = require('../src/services/shipment-sync');

let pass = 0;
let fail = 0;
function check(label, cond, detail = '') {
    if (cond) { pass++; console.log(`  PASS  ${label}`); } else { fail++; console.log(`  FAIL  ${label}${detail ? ' — ' + JSON.stringify(detail) : ''}`); }
}
const q = async (sql, params = []) => (await getPool().query(sql, params))[0];

(async () => {
    const host = process.env.DB_HOST || '';
    if (!/test/i.test(host) && !/test/i.test(process.env.DB_NAME || '')) {
        console.error(`Refusing: ${host} does not look like the TEST instance.`);
        process.exit(2);
    }
    const { app } = require('../src/handlers/orders');
    const server = app.listen(0);
    await new Promise(r => server.once('listening', r));
    const api = axios.create({ baseURL: `http://localhost:${server.address().port}`, validateStatus: () => true });
    const stamp = Date.now();
    const orderIds = [];
    const names = [];
    try {
        if (!(await q(`SELECT name FROM app_migrations WHERE name = ?`, [sync.BACKFILL_MARKER])).length) {
            console.error('The shadow is not armed here: run tools/backfill-shipments.js --apply first.');
            process.exitCode = 2;
            return;
        }
        const o = await api.post('/api/v1/orders', {
            status: 'READY', jfCode: `SHIPTEST-${stamp}-F`, productName: 'Shadow fault fixture', quantity: 10,
        });
        orderIds.push(o.data.id);

        console.log('\n1. Autocommit route with an injected shadow fault (POST /draft-containers)');
        const name = `DRAFT-SEA-990101-${String(stamp % 1e6).padStart(6, '0')} - SHIPTEST fault ${stamp}`;
        names.push(name);
        let r = await api.post('/api/v1/draft-containers', { orderId: o.data.id, draftContainerName: name, allocated: 5 });
        check('the legacy request still answers 201', r.status === 201, r.data);
        check('its legacy write committed', (await q(`SELECT COUNT(*) AS n FROM draft_container_allocations WHERE draft_container_name = ?`, [name]))[0].n === 1);
        check('no partial shadow row survived', (await q(`SELECT COUNT(*) AS n FROM shipments WHERE open_key = ?`, [`D:${name}`]))[0].n === 0);
        let f = await q(`SELECT occurrences FROM shipment_sync_failures WHERE site = 'POST /draft-containers' AND key_value = ? AND resolved_at IS NULL`, [name]);
        check('exactly one failure row, occurrences 1', f.length === 1 && f[0].occurrences === 1, f);

        console.log('\n2. Transactional route with an injected shadow fault (POST /draft-containers/rename)');
        const renamed = `${name} R`;
        names.push(renamed);
        r = await api.post('/api/v1/draft-containers/rename', { from: name, to: renamed });
        check('the legacy request still answers 200', r.status === 200, r.data);
        check('the rename committed (savepoint rollback kept the legacy transaction)',
            (await q(`SELECT COUNT(*) AS n FROM draft_container_allocations WHERE draft_container_name = ?`, [renamed]))[0].n === 1);
        check('no partial shadow row survived', (await q(`SELECT COUNT(*) AS n FROM shipments WHERE open_key IN (?, ?)`, [`D:${name}`, `D:${renamed}`]))[0].n === 0);
        f = await q(`SELECT occurrences FROM shipment_sync_failures WHERE site = 'POST /draft-containers/rename' AND key_value = ? AND resolved_at IS NULL`, [renamed]);
        check('exactly one failure row for the rename', f.length === 1 && f[0].occurrences === 1, f);

        console.log('\n3. Coalescing, driven directly');
        // connectionLimit is 1: while this connection is held, every query must
        // go through it (asking the pool for another would wait forever).
        const conn = await getPool().getConnection();
        const cq = async (sql, params = []) => (await conn.query(sql, params))[0];
        const key = { site: 'shadow-test', inTx: false, keyKind: 'test', keyValue: `k${stamp}` };
        const boom = async (c) => {
            await c.query(`INSERT INTO shipments (name, open_key, stage, origin) VALUES (?, ?, 'DRAFT', 'api')`, [`SHIPTEST ${stamp}`, `D:SHIPTEST ${stamp}`]);
            throw new Error('boom');
        };
        try {
            await sync.shadow(conn, key, boom);
            await sync.shadow(conn, key, boom);
            let rows = await cq(`SELECT id, occurrences, resolved_at FROM shipment_sync_failures WHERE site = 'shadow-test' AND key_value = ?`, [key.keyValue]);
            check('two failures for one key -> one row, occurrences 2', rows.length === 1 && rows[0].occurrences === 2, rows);
            check('the partial INSERT was rolled back both times',
                (await cq(`SELECT COUNT(*) AS n FROM shipments WHERE open_key = ?`, [`D:SHIPTEST ${stamp}`]))[0].n === 0);
            await cq(`UPDATE shipment_sync_failures SET resolved_at = NOW(), dedup_key = NULL WHERE id = ?`, [rows[0].id]);
            await sync.shadow(conn, key, boom);
            rows = await cq(`SELECT occurrences, resolved_at FROM shipment_sync_failures WHERE site = 'shadow-test' AND key_value = ? ORDER BY id`, [key.keyValue]);
            check('after resolving, the next failure starts a fresh row', rows.length === 2 && rows[1].occurrences === 1 && rows[1].resolved_at === null, rows);
            const [[tx]] = await conn.query(
                `SELECT COUNT(*) AS t FROM information_schema.innodb_trx WHERE trx_mysql_thread_id = CONNECTION_ID()`
            );
            check('no transaction leaked on the pooled connection', Number(tx.t) === 0, tx);
        } finally {
            conn.release();
        }
    } catch (err) {
        fail++;
        console.error('CRASH:', err);
    } finally {
        // Cleanup: legacy rows, the fixture, and this run's failure rows.
        for (const n of names) await api.post('/api/v1/draft-containers/close', { name: n, reason: 'deleted' });
        for (const id of orderIds) await api.delete(`/api/v1/orders/${id}`);
        await q(`DELETE FROM shipment_sync_failures WHERE (site = 'shadow-test' AND key_value = ?) OR key_value IN (?)`,
            [`k${stamp}`, names.length ? names : ['']]);
        // The fault only suppressed the shadow: re-sync the names so verify stays clean.
        const conn = await getPool().getConnection();
        try {
            for (const n of names) {
                await conn.beginTransaction();
                await sync.syncDraft(conn, n);
                await conn.commit();
            }
        } finally {
            conn.release();
        }
        const conn2 = await getPool().getConnection();
        try {
            const v = await sync.verifyAll(conn2, {});
            check('verify: no hard drift after cleanup', v.ok, Object.entries(v.hard).filter(([, x]) => x.length).map(([k, x]) => `${k}:${x.length}`).join(' '));
        } finally {
            conn2.release();
        }
        console.log(`\n${pass} passed, ${fail} failed`);
        server.close();
        await closePool();
        process.exit(fail ? 1 : (process.exitCode || 0));
    }
})();
