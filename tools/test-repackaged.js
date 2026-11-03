#!/usr/bin/env node
'use strict';

// HTTP suite for the REPACKAGED order status ("Combined / Repackaged", user
// 2026-11-02) and the `repackaged` line kind of /api/v1/supplier-payments.
// Goods combined into, or repackaged as, another product at the factory never
// ship, so their balance is due from the day they were marked — and a transfer
// pays it line by line, like a QC unit. The server checks each line points at
// a live REPACKAGED order line, in the transfer's currency, and never takes
// more than the line's value. What is owed on it is the page's (it knows the terms).
//
//   node tools/test-repackaged.js
//
// Drives the real Express app in-process against the TEST database. Everything
// it writes hangs off purchase orders and a supplier stamped with this run and
// is deleted again — transfers, their lines, order lines and audit rows.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';

require('dotenv').config();
const http = require('http');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');

const STAMP = Date.now();
const SUP = `RX Test Supplier ${STAMP}`;
const PO_NUMBER = `RXTEST-${STAMP}`;
const EUR_PO = `RXTEST-EUR-${STAMP}`;
const CODE = `RX${STAMP}`;
const TODAY = new Date().toISOString().slice(0, 10);

let base;
let failures = 0;
let passes = 0;
const poIds = [];
const orderIds = [];
const paymentIds = new Set();
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail)}`}`);
    return ok;
};
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
const pay = async (amount, lines, currency = 'USD') => {
    const r = await call('POST', '/api/v1/supplier-payments', { supplierName: SUP, amount, currency, paidOn: TODAY, lines });
    if (r.status === 201) paymentIds.add(r.body.id);
    return r;
};
const order = async (poId, jfCode, quantity, unitPrice, status) => {
    const ins = await sql(
        `INSERT INTO orders (jf_code, product_name, quantity, unit_price, status, purchase_order_id) VALUES (?, 'Repackaged line test', ?, ?, ?, ?)`,
        [jfCode, quantity, unitPrice, status, poId]
    );
    orderIds.push(ins.insertId);
    return ins.insertId;
};

async function cleanup() {
    const payments = await sql(`SELECT id FROM supplier_payments WHERE supplier_name = ?`, [SUP]);
    const pids = [...new Set([...payments.map(p => p.id), ...paymentIds])];
    if (pids.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'supplier_payment' AND entity_id IN (?)`, [pids]);
        await sql(`DELETE FROM supplier_payment_lines WHERE payment_id IN (?)`, [pids]);
        await sql(`DELETE FROM supplier_payments WHERE id IN (?)`, [pids]);
    }
    if (orderIds.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'order' AND entity_id IN (?)`, [orderIds]);
        await sql(`DELETE FROM orders WHERE id IN (?)`, [orderIds]);
    }
    if (poIds.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'purchase_order' AND entity_id IN (?)`, [poIds]);
        await sql(`DELETE FROM purchase_orders WHERE id IN (?)`, [poIds]);
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
    console.log(`TEST database ${host}/${db} · run ${STAMP}`);

    try {
        const po = await sql(`INSERT INTO purchase_orders (po_number, supplier, currency, shipping_total) VALUES (?, ?, 'USD', 0)`, [PO_NUMBER, SUP]);
        poIds.push(po.insertId);
        const eur = await sql(`INSERT INTO purchase_orders (po_number, supplier, currency, shipping_total) VALUES (?, ?, 'EUR', 0)`, [EUR_PO, SUP]);
        poIds.push(eur.insertId);
        const combined = await order(po.insertId, CODE, 100, 2, 'READY');       // value 200.00
        const goods = await order(po.insertId, `${CODE}B`, 50, 2, 'IN_PRODUCTION');
        const unpriced = await order(po.insertId, `${CODE}C`, 5, null, 'REPACKAGED');
        const eurLine = await order(eur.insertId, `${CODE}D`, 2, 10, 'REPACKAGED');

        console.log('\nMarking a line Combined / Repackaged');
        const before = Date.now();
        const moved = await call('PATCH', `/api/v1/orders/${combined}/status`, { status: 'REPACKAGED' });
        check('PATCH status REPACKAGED → 200', moved.status === 200 && moved.body.status === 'REPACKAGED', moved);
        const stamped = moved.body.dates && moved.body.dates.repackaged;
        check('the day it was marked is stamped in dates.repackaged',
            typeof stamped === 'string' && Date.parse(stamped) >= before - 1000 && Date.parse(stamped) <= Date.now() + 1000, moved.body.dates);

        console.log('\nPaying a repackaged line');
        const first = await pay(120, [{ kind: 'repackaged', id: combined, amount: 120 }]);
        check('a transfer paying one repackaged line → 201', first.status === 201, first);
        const line = first.body.lines && first.body.lines[0];
        check('its line names the PO and the product',
            line && line.kind === 'repackaged' && line.targetId === combined && line.label === `${PO_NUMBER} combined / repackaged ${CODE}`
            && line.poNumber === PO_NUMBER && line.purchaseOrderId === po.insertId && line.targetAmount === 200 && line.targetMissing === false, line);
        const listed = await call('GET', `/api/v1/supplier-payments?supplier=${encodeURIComponent(SUP)}`);
        const feedLine = (listed.body.data || []).flatMap(p => p.lines).find(l => l.kind === 'repackaged');
        check('the transfers feed returns it', !!feedLine && feedLine.label === `${PO_NUMBER} combined / repackaged ${CODE}`, listed.body);
        const more = await pay(81, [{ kind: 'repackaged', id: combined, amount: 81 }]);
        check('more than its value in all → 422 OVER_APPLIED', more.status === 422 && more.body.code === 'OVER_APPLIED', more.body);
        const rest = await pay(80, [{ kind: 'repackaged', id: combined, amount: 80 }]);
        check('the rest of its value → 201', rest.status === 201, rest.body);
        const noPrice = await pay(12.5, [{ kind: 'repackaged', id: unpriced, amount: 12.5 }]);
        check('a repackaged line with no price takes what is typed → 201', noPrice.status === 201, noPrice.body);

        console.log('\nWhat is refused');
        const notRepackaged = await pay(10, [{ kind: 'repackaged', id: goods, amount: 10 }]);
        check('a line not marked repackaged → 422 NOT_REPACKAGED', notRepackaged.status === 422 && notRepackaged.body.code === 'NOT_REPACKAGED', notRepackaged.body);
        const inEur = await pay(10, [{ kind: 'repackaged', id: eurLine, amount: 10 }]);
        check('a repackaged line on a EUR PO from a USD transfer → 422 CURRENCY_MISMATCH', inEur.status === 422 && inEur.body.code === 'CURRENCY_MISMATCH', inEur.body);
        const nothing = await pay(10, [{ kind: 'repackaged', id: 999999999, amount: 10 }]);
        check('a line that does not exist → 404 TARGET_NOT_FOUND', nothing.status === 404 && nothing.body.code === 'TARGET_NOT_FOUND', nothing.body);
        const qcKind = await pay(10, [{ kind: 'qc', id: unpriced, amount: 10 }]);
        check('a repackaged goods line is still not a QC unit → 422 NOT_QC_UNIT', qcKind.status === 422 && qcKind.body.code === 'NOT_QC_UNIT', qcKind.body);
    } catch (e) {
        failures++;
        console.log('FAIL  run stopped:', e.message);
    } finally {
        try { await cleanup(); console.log('\ncleaned up'); } catch (e) { failures++; console.log('FAIL  cleanup:', e.message); }
        server.close();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
