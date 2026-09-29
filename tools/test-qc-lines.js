#!/usr/bin/env node
'use strict';

// HTTP suite for the `qc` line kind of /api/v1/supplier-payments: a transfer
// paying QC units (our _FQC lines) item by item. QC units belong to no
// container (user, 2026-09-29): the page lists them and the user ticks the ones
// a transfer pays; what is still owed on each is the page's (it knows the
// terms). The server checks each line points at a live _FQC line, in the
// transfer's currency, and never takes more than the line's value.
//
//   node tools/test-qc-lines.js
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
const SUP = `QX Test Supplier ${STAMP}`;
const PO_NUMBER = `QXTEST-${STAMP}`;
const EUR_PO = `QXTEST-EUR-${STAMP}`;
const CODE = `QX${STAMP}`;
const TODAY = new Date().toISOString().slice(0, 10);

let base;
let failures = 0;
let passes = 0;
const poIds = [];
const orderIds = [];
const docIds = [];
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
const order = async (poId, jfCode, quantity, unitPrice, status = 'DESTROYED') => {
    const ins = await sql(
        `INSERT INTO orders (jf_code, product_name, quantity, unit_price, status, purchase_order_id) VALUES (?, 'QC line test', ?, ?, ?, ?)`,
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
    if (docIds.length) await sql(`DELETE FROM shipment_payment_documents WHERE id IN (?)`, [docIds]);
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
        const goods = await order(po.insertId, CODE, 100, 10, 'ON_SEA');
        const qc = await order(po.insertId, `${CODE}_FQC`, 10, 10);          // value 100.00
        const unpriced = await order(po.insertId, `${CODE}B_FQC`, 5, null);
        const eurQc = await order(eur.insertId, `${CODE}C_FQC`, 2, 10);

        console.log('\nPaying QC units item by item');
        const first = await pay(70, [{ kind: 'qc', id: qc, amount: 70 }]);
        check('a transfer paying one QC unit → 201', first.status === 201, first);
        const line = first.body.lines && first.body.lines[0];
        check('its line names the PO and the product, never the _FQC suffix',
            line && line.kind === 'qc' && line.targetId === qc && line.label === `${PO_NUMBER} QC units ${CODE}` && line.poNumber === PO_NUMBER
            && line.purchaseOrderId === po.insertId && line.targetAmount === 100 && line.targetMissing === false, line);
        const listed = await call('GET', `/api/v1/supplier-payments?supplier=${encodeURIComponent(SUP)}`);
        const feedLine = (listed.body.data || []).flatMap(p => p.lines).find(l => l.kind === 'qc');
        check('the transfers feed returns it', !!feedLine && feedLine.label === `${PO_NUMBER} QC units ${CODE}`, listed.body);
        const second = await pay(30, [{ kind: 'qc', id: qc, amount: 30 }]);
        check('the rest on another transfer → 201', second.status === 201, second);
        const more = await pay(1, [{ kind: 'qc', id: qc, amount: 1 }]);
        check('more than its value in all → 422 OVER_APPLIED', more.status === 422 && more.body.code === 'OVER_APPLIED', more.body);
        const noPrice = await pay(12.5, [{ kind: 'qc', id: unpriced, amount: 12.5 }]);
        check('a QC unit with no price on the PO takes what is typed → 201', noPrice.status === 201, noPrice.body);

        console.log('\nWhat is refused');
        const goodsLine = await pay(10, [{ kind: 'qc', id: goods, amount: 10 }]);
        check('a goods line is not a QC unit → 422 NOT_QC_UNIT', goodsLine.status === 422 && goodsLine.body.code === 'NOT_QC_UNIT', goodsLine.body);
        const inEur = await pay(10, [{ kind: 'qc', id: eurQc, amount: 10 }]);
        check('a QC unit on a EUR PO from a USD transfer → 422 CURRENCY_MISMATCH', inEur.status === 422 && inEur.body.code === 'CURRENCY_MISMATCH', inEur.body);
        const negative = await pay(10, [{ kind: 'qc', id: qc, amount: -5 }, { kind: 'qc', id: unpriced, amount: 15 }]);
        check('a negative QC line → 400 BAD_LINES', negative.status === 400 && negative.body.code === 'BAD_LINES', negative.body);
        const nothing = await pay(10, [{ kind: 'qc', id: 999999999, amount: 10 }]);
        check('a line that does not exist → 404 TARGET_NOT_FOUND', nothing.status === 404 && nothing.body.code === 'TARGET_NOT_FOUND', nothing.body);

        console.log('\nDeleting a transfer frees what it paid');
        process.env.LOCAL_USER_TYPE = 'admin';   // deleting a transfer is admin-only
        const del = await call('DELETE', `/api/v1/supplier-payments/${second.body.id}`);
        process.env.LOCAL_USER_TYPE = 'standard';
        check('delete the second transfer → 2xx', del.status >= 200 && del.status < 300, del);
        const again = await pay(30, [{ kind: 'qc', id: qc, amount: 30 }]);
        check('its 30 can be paid again → 201', again.status === 201, again.body);

        console.log('\nA QC invoice on the supplier is not a proof of payment');
        const doc = async (docKind, filename) => (await sql(
            `INSERT INTO shipment_payment_documents (shipment_id, supplier_name, supplier_key, doc_kind, filename, s3_key, extract_status)
             VALUES (NULL, ?, ?, ?, ?, ?, 'succeeded')`,
            [SUP, SUP.toLowerCase(), docKind, filename, `test/${STAMP}/${filename}`]
        )).insertId;
        const qcDoc = await doc('balance_invoice', 'QC-invoice.pdf');
        const proof = await doc('remittance', 'bank-proof.pdf');
        docIds.push(qcDoc, proof);
        const feed = await call('GET', `/api/v1/supplier-payments?supplier=${encodeURIComponent(SUP)}`);
        const loose = (feed.body.documents || []).map(d => d.id);
        check('the proof waits to be applied; the QC invoice does not', loose.includes(proof) && !loose.includes(qcDoc), loose);

        console.log('\nA QC line deleted since');
        await sql(`UPDATE orders SET deleted_at = NOW() WHERE id = ?`, [qc]);
        const after = await call('GET', `/api/v1/supplier-payments?supplier=${encodeURIComponent(SUP)}`);
        const gone = (after.body.data || []).find(p => p.id === first.body.id)?.lines[0];
        check('its line says so', gone && gone.targetMissing === true && gone.label === null, gone);
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
