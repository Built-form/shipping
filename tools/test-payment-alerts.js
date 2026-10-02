#!/usr/bin/env node
'use strict';

// HTTP suite for dismissed "Needs attention" lines
// (/api/v1/payment-alert-dismissals) and the QC sign-off key
// (/api/v1/payment-reviews with qc:<document id>). Drives the real Express app
// in-process against the TEST database, like tools/test-payment-reviews.js.
//
//   node tools/test-payment-alerts.js
//
// Locally every request is local@dev; the role is read from LOCAL_USER_TYPE on
// each request, so the suite switches it between calls. Everything it writes
// uses keys stamped with this run and is deleted again — audit rows included.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'admin';

require('dotenv').config();
const http = require('http');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');

const STAMP = Date.now();
const KEY = `overpaid|USD|9${STAMP}||patest supplier|${STAMP.toString(16)}`;
const KEY2 = `unvalued|USD|9${STAMP}||patest supplier|${(STAMP + 1).toString(16)}`;
const QC_KEY = `qc:9${String(STAMP % 1e8).padStart(8, '0')}`;

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

async function cleanup() {
    const rows = await sql(`SELECT id FROM payment_alert_dismissals WHERE alert_key IN (?, ?)`, [KEY, KEY2]);
    if (rows.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_alert_dismissal' AND entity_id IN (?)`, [rows.map(r => r.id)]);
        await sql(`DELETE FROM payment_alert_dismissals WHERE alert_key IN (?, ?)`, [KEY, KEY2]);
    }
    const reviews = await sql(`SELECT id FROM payment_reviews WHERE payment_key = ?`, [QC_KEY]);
    if (reviews.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_review' AND entity_id IN (?)`, [reviews.map(r => r.id)]);
        await sql(`DELETE FROM payment_reviews WHERE payment_key = ?`, [QC_KEY]);
    }
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
    const P = '/api/v1/payment-alert-dismissals';
    try {
        await cleanup();
        const body = { alertKey: KEY, kind: 'overpaid', currency: 'USD', poNumber: 'PO_PATEST', supplierName: 'PATEST Supplier', detail: 'Paid 12.50 more than the PO value', amount: 12.5 };

        // ── Who may dismiss ──
        for (const role of ['standard', 'accountant', 'warehouse']) {
            as(role);
            const r = await call('POST', P, body);
            check(`a ${role} user cannot dismiss an alert (403 CANNOT_DISMISS)`, r.status === 403 && r.body.code === 'CANNOT_DISMISS', r);
        }
        as('admin');
        let r = await call('POST', P, { kind: 'overpaid' });
        check('no key: 400 BAD_FIELD', r.status === 400 && r.body.code === 'BAD_FIELD' && r.body.error === 'alertKey is required.', r);

        // ── Dismiss ──
        r = await call('POST', P, body);
        const d = r.body.dismissal;
        check('an admin dismisses it: 201 with what it said', r.status === 201 && d && d.alertKey === KEY && d.kind === 'overpaid' && d.currency === 'USD'
            && d.poNumber === 'PO_PATEST' && d.supplierName === 'PATEST Supplier' && d.detail === 'Paid 12.50 more than the PO value' && d.amount === 12.5
            && d.dismissedByEmail === 'local@dev' && typeof d.dismissedAt === 'string' && d.restoredAt === null, r);
        r = await call('POST', P, body);
        check('dismissing the same key again adds nothing: 200 with the same row', r.status === 200 && r.body.dismissal.id === d.id, r);
        const count = await sql(`SELECT COUNT(*) AS n FROM payment_alert_dismissals WHERE alert_key = ?`, [KEY]);
        check('one row for the key', count[0].n === 1, count);
        const audit = await sql(`SELECT action FROM audit_log WHERE entity_type = 'payment_alert_dismissal' AND entity_id = ?`, [d.id]);
        check('the dismissal is in the audit log', audit.length === 1 && audit[0].action === 'create', audit);

        // ── Everyone sees it ──
        as('standard');
        r = await call('GET', P);
        check('a standard user sees the dismissal', r.status === 200 && Array.isArray(r.body.data) && r.body.data.some(x => x.id === d.id && x.alertKey === KEY), { status: r.status, n: r.body.data?.length });
        r = await call('DELETE', `${P}/${d.id}`);
        check('…but cannot restore it (403 CANNOT_DISMISS)', r.status === 403 && r.body.code === 'CANNOT_DISMISS', r);

        // ── Restore ──
        as('admin');
        r = await call('DELETE', `${P}/${d.id}`);
        check('an admin restores it: 200, marked restored, row kept', r.status === 200 && r.body.dismissal.id === d.id && typeof r.body.dismissal.restoredAt === 'string' && r.body.dismissal.restoredByEmail === 'local@dev', r);
        r = await call('GET', P);
        check('a restored alert is no longer in the list', r.status === 200 && !r.body.data.some(x => x.id === d.id), { n: r.body.data?.length });
        r = await call('DELETE', `${P}/${d.id}`);
        check('restoring twice: 409 NOT_ACTIVE', r.status === 409 && r.body.code === 'NOT_ACTIVE', r);
        r = await call('DELETE', `${P}/999999999`);
        check('an unknown dismissal: 404 NOT_FOUND', r.status === 404 && r.body.code === 'NOT_FOUND', r);
        r = await call('POST', P, body);
        check('dismissing it again after a restore adds a new row: 201', r.status === 201 && r.body.dismissal.id !== d.id, r);
        r = await call('POST', P, { alertKey: KEY2, kind: 'unvalued' });
        check('only the key and the kind are needed: 201', r.status === 201 && r.body.dismissal.currency === null && r.body.dismissal.amount === null, r);

        // ── A QC invoice's sign-off key ──
        as('standard');
        r = await call('POST', '/api/v1/payment-reviews', { paymentKey: QC_KEY, currency: 'USD', amount: 19, supplierName: 'PATEST Supplier', poNumbers: ['PO_PATEST'] });
        check('a QC invoice is signed off under qc:<document id>: 201, kind qc', r.status === 201 && r.body.review.paymentKey === QC_KEY && r.body.review.kind === 'qc' && r.body.review.amount === 19 && r.body.review.containerRef === null, r);
        r = await call('GET', `/api/v1/payment-reviews?paymentKey=${encodeURIComponent(QC_KEY)}`);
        check('…and its history reads back', r.status === 200 && r.body.data.length === 1 && r.body.data[0].kind === 'qc', r);
        r = await call('POST', '/api/v1/payment-reviews', { paymentKey: 'qc:0', currency: 'USD', amount: 19 });
        check('a malformed QC key: 400', r.status === 400, r);
    } catch (e) {
        failures++;
        console.error('SUITE ERROR', e);
    } finally {
        try { await cleanup(); } catch (e) { console.error('cleanup failed', e); failures++; }
        server.close();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
