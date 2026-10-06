#!/usr/bin/env node
'use strict';

// HTTP suite for due dates set by hand (/api/v1/payment-due-dates). Drives the
// real Express app in-process against the TEST database, like
// tools/test-payment-reviews.js.
//
//   node tools/test-payment-due-dates.js
//
// Locally every request is local@dev; the role is read from LOCAL_USER_TYPE on
// each request, so the suite switches it between calls. Everything it writes
// uses keys stamped with this run and is deleted again — audit rows included.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';

require('dotenv').config();
const http = require('http');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');

const STAMP = Date.now();
const DEP_KEY = `deposit:8${String(STAMP % 1e9).padStart(9, '0')}`;
const BAL_KEY = `balance:USD:DDTEST-${STAMP}|ddtest supplier`;
const ITEM_KEY = `item:derived:bal:8${STAMP % 1e6}:DDTEST-${STAMP}`;
const ME = 'local@dev';

let base;
let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail)}`}`);
    return ok;
};
const as = role => { process.env.LOCAL_USER_TYPE = role; };
const call = async (method, path, body) => {
    const res = await fetch(`${base}${path}`, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : {},
        body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json;
    try { json = JSON.parse(text); } catch { json = text; }
    return { status: res.status, body: json };
};
const sql = async (q, p = []) => (await getPool().query(q, p))[0];
const keys = [DEP_KEY, BAL_KEY, ITEM_KEY];

async function cleanup() {
    const rows = await sql(`SELECT id FROM payment_due_dates WHERE target_key IN (?, ?, ?)`, keys);
    if (rows.length) await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_due_date' AND entity_id IN (?)`, [rows.map(r => r.id)]);
    // Rows deleted through the API outlive their row only in the audit log: found by key.
    await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_due_date' AND (JSON_UNQUOTE(JSON_EXTRACT(after_json, '$.key')) IN (?, ?, ?) OR JSON_UNQUOTE(JSON_EXTRACT(before_json, '$.key')) IN (?, ?, ?))`, [...keys, ...keys]);
    await sql(`DELETE FROM payment_due_dates WHERE target_key IN (?, ?, ?)`, keys);
}

(async () => {
    const host = process.env.DB_HOST || '';
    const db = process.env.DB_NAME || '';
    if (!/test/i.test(host) && !/test/i.test(db)) {
        console.error(`Refusing to run: ${host}/${db} does not look like the TEST database.`);
        process.exit(2);
    }
    const server = http.createServer(app).listen(0);
    await new Promise(r => server.once('listening', r));
    base = `http://127.0.0.1:${server.address().port}`;
    console.log(`TEST database ${host}/${db} · run ${STAMP}`);

    try {
        console.log('\nList');
        const empty = await call('GET', '/api/v1/payment-due-dates');
        check('GET → 200 with data', empty.status === 200 && Array.isArray(empty.body.data), empty);
        check('nothing of this run yet', !(empty.body.data || []).some(d => keys.includes(d.key)));

        console.log('\nSetting');
        const set = await call('PUT', '/api/v1/payment-due-dates', { key: DEP_KEY, dueDate: '2026-11-20', note: ' agreed with the supplier ' });
        check('PUT → 200 with the row, stamped with the caller', set.status === 200 && set.body.dueDate?.key === DEP_KEY && set.body.dueDate?.dueDate === '2026-11-20'
            && set.body.dueDate?.setByEmail === ME && set.body.dueDate?.note === 'agreed with the supplier' && set.body.dueDate?.scope === 'payment', set);
        const id = set.body.dueDate?.id;
        const again = await call('PUT', '/api/v1/payment-due-dates', { key: DEP_KEY, dueDate: '2026-11-27' });
        check('PUT again → 200, same row, new date, note cleared', again.status === 200 && again.body.dueDate?.id === id && again.body.dueDate?.dueDate === '2026-11-27' && again.body.dueDate?.note === null, again);
        const same = await call('PUT', '/api/v1/payment-due-dates', { key: DEP_KEY, dueDate: '2026-11-27' });
        check('PUT the same again → 200, unchanged', same.status === 200 && same.body.unchanged === true && same.body.dueDate?.id === id, same);
        const bal = await call('PUT', '/api/v1/payment-due-dates', { key: BAL_KEY, dueDate: '2026-12-01' });
        check('a balance key → 200, scope payment', bal.status === 200 && bal.body.dueDate?.scope === 'payment', bal);
        const item = await call('PUT', '/api/v1/payment-due-dates', { key: ITEM_KEY, dueDate: '2026-12-05' });
        check('an item key → 200, scope item', item.status === 200 && item.body.dueDate?.scope === 'item', item);
        const listed = await call('GET', '/api/v1/payment-due-dates');
        const mine = (listed.body.data || []).filter(d => keys.includes(d.key));
        check('list → the three rows', mine.length === 3 && mine.find(d => d.key === DEP_KEY)?.dueDate === '2026-11-27', mine);
        const audits = await sql(`SELECT action FROM audit_log WHERE entity_type = 'payment_due_date' AND entity_id = ? ORDER BY id`, [id]);
        check('create and update are audited, the unchanged PUT is not', audits.map(a => a.action).join(',') === 'create,update', audits);

        console.log('\nRefusals');
        for (const [label, body] of [
            ['no key', { dueDate: '2026-11-20' }],
            ['a QC invoice key', { key: 'qc:412', dueDate: '2026-11-20' }],
            ['a malformed key', { key: 'balance:USD:330', dueDate: '2026-11-20' }],
            ['no date', { key: DEP_KEY }],
            ['a date in another shape', { key: DEP_KEY, dueDate: '20/11/2026' }],
            ['a date that does not exist', { key: DEP_KEY, dueDate: '2026-02-30' }],
            ['a note too long', { key: DEP_KEY, dueDate: '2026-11-20', note: 'x'.repeat(501) }],
        ]) {
            const r = await call('PUT', '/api/v1/payment-due-dates', body);
            check(`${label} → 400 BAD_FIELD`, r.status === 400 && r.body.code === 'BAD_FIELD', r);
        }
        as('accountant');
        const acc = await call('PUT', '/api/v1/payment-due-dates', { key: DEP_KEY, dueDate: '2026-11-20' });
        check('an accountant → 403 CANNOT_SET_DUE_DATE', acc.status === 403 && acc.body.code === 'CANNOT_SET_DUE_DATE', acc);
        const accDel = await call('DELETE', `/api/v1/payment-due-dates/${id}`);
        check('an accountant clearing one → 403', accDel.status === 403 && accDel.body.code === 'CANNOT_SET_DUE_DATE', accDel);
        const accList = await call('GET', '/api/v1/payment-due-dates');
        check('an accountant still reads them', accList.status === 200 && (accList.body.data || []).some(d => d.key === DEP_KEY), accList);
        as('read_only');
        const ro = await call('PUT', '/api/v1/payment-due-dates', { key: DEP_KEY, dueDate: '2026-11-20' });
        check('a read-only user → 403', ro.status === 403, ro);
        as('standard');
        check('the date is untouched by the refused calls', (await sql(`SELECT due_date FROM payment_due_dates WHERE id = ?`, [id]))[0]?.due_date?.toISOString?.().slice(0, 10) === '2026-11-27');

        console.log('\nClearing');
        const del = await call('DELETE', `/api/v1/payment-due-dates/${id}`);
        check('DELETE → 200 with what was removed', del.status === 200 && del.body.removed?.id === id && del.body.removed?.dueDate === '2026-11-27', del);
        check('the row is gone', (await sql(`SELECT id FROM payment_due_dates WHERE id = ?`, [id])).length === 0);
        const twice = await call('DELETE', `/api/v1/payment-due-dates/${id}`);
        check('DELETE again → 404 NOT_FOUND', twice.status === 404 && twice.body.code === 'NOT_FOUND', twice);
        const delAudit = await sql(`SELECT action FROM audit_log WHERE entity_type = 'payment_due_date' AND entity_id = ? ORDER BY id`, [id]);
        check('the delete is audited', delAudit.map(a => a.action).join(',') === 'create,update,delete', delAudit);
        as('admin');
        const byAdmin = await call('DELETE', `/api/v1/payment-due-dates/${bal.body.dueDate.id}`);
        check('an admin clears one too', byAdmin.status === 200, byAdmin);
        as('standard');
        const after = await call('GET', '/api/v1/payment-due-dates');
        check('list → only the item row of this run is left', (after.body.data || []).filter(d => keys.includes(d.key)).map(d => d.key).join() === ITEM_KEY, after.body.data);
    } catch (e) {
        failures++;
        console.error('ERROR', e);
    } finally {
        await cleanup().catch(e => console.error('cleanup failed', e));
        server.close();
        await closePool().catch(() => {});
    }
    console.log(`\n${passes} passed, ${failures} failed`);
    process.exit(failures ? 1 : 0);
})();
