#!/usr/bin/env node
'use strict';

// HTTP suite for extra charges and credits (/api/v1/payment-extras) and the
// `extra` line kind of /api/v1/supplier-payments. Drives the real Express app
// in-process against the TEST database, like tools/test-payment-reviews.js.
//
//   node tools/test-payment-extras.js
//
// Locally every request is local@dev; the role is read from LOCAL_USER_TYPE on
// each request, so the suite switches it between calls. Everything it writes
// hangs off a purchase order and a supplier name stamped with this run and is
// deleted again — transfers, their lines and audit rows included. It never
// touches a real balance or PI: the transfers it records pay only its extras.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';

require('dotenv').config();
const http = require('http');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');

const STAMP = Date.now();
const SUP = `PX Test Supplier ${STAMP}`;
// A forwarder paid for a container (ridesWith 'shipment'): its own payee.
const FWD = `PX Test Forwarder ${STAMP}`;
const PO_NUMBER = `PXTEST-${STAMP}`;
const BOX = `PXBOX-${STAMP}`;
const TODAY = new Date().toISOString().slice(0, 10);

let base;
let failures = 0;
let passes = 0;
let poId = null;
const paymentIds = new Set();
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
const extrasOf = async () => (await call('GET', `/api/v1/payment-extras?supplier=${encodeURIComponent(SUP)}`)).body.data || [];
const pay = async (amount, lines) => {
    const r = await call('POST', '/api/v1/supplier-payments', { supplierName: SUP, amount, currency: 'USD', paidOn: TODAY, lines });
    if (r.status === 201) paymentIds.add(r.body.id);
    return r;
};

async function cleanup() {
    // The PO goes whatever else fails (e.g. the table not migrated yet).
    let extras = [];
    try { extras = await sql(`SELECT id FROM payment_extras WHERE supplier_name IN (?, ?)`, [SUP, FWD]); } catch (e) { if (e.errno !== 1146) throw e; }
    const payments = await sql(`SELECT id FROM supplier_payments WHERE supplier_name IN (?, ?)`, [SUP, FWD]);
    const pids = [...new Set([...payments.map(p => p.id), ...paymentIds])];
    const xids = extras.map(x => x.id);
    if (xids.length) await sql(`DELETE FROM audit_log WHERE entity_type = 'payment_extra' AND entity_id IN (?)`, [xids]);
    if (pids.length) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'supplier_payment' AND entity_id IN (?)`, [pids]);
        await sql(`DELETE FROM supplier_payment_lines WHERE payment_id IN (?)`, [pids]);
        await sql(`DELETE FROM supplier_payments WHERE id IN (?)`, [pids]);
    }
    if (xids.length) await sql(`DELETE FROM payment_extras WHERE id IN (?)`, [xids]);
    if (poId) {
        await sql(`DELETE FROM audit_log WHERE entity_type = 'purchase_order' AND entity_id = ?`, [poId]);
        await sql(`DELETE FROM purchase_orders WHERE id = ?`, [poId]);
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
        const ins = await sql(`INSERT INTO purchase_orders (po_number, supplier, currency, shipping_total) VALUES (?, ?, 'USD', 0)`, [PO_NUMBER, SUP]);
        poId = ins.insertId;
        const mould = { supplierName: SUP, currency: 'USD', amount: 500, kind: 'mould', description: 'Mould for the test SKU', ridesWith: 'deposit', purchaseOrderId: poId };

        console.log('\nWho may add');
        as('warehouse');
        const wh = await call('POST', '/api/v1/payment-extras', mould);
        check('warehouse POST → 403 CANNOT_EDIT_EXTRAS', wh.status === 403 && wh.body.code === 'CANNOT_EDIT_EXTRAS', wh);
        as('accountant');
        const created = await call('POST', '/api/v1/payment-extras', mould);
        check('accountant adds a mould cost to the deposit → 201', created.status === 201 && created.body.id > 0, created);
        const charge = created.body;
        check('it names its PO and reads as open, all of it left',
            charge.poNumber === PO_NUMBER && charge.label === `${PO_NUMBER} mould cost` && charge.status === 'open' && charge.remaining === 500 && charge.createdByEmail === 'local@dev', charge);
        const audit = await sql(`SELECT action FROM audit_log WHERE entity_type = 'payment_extra' AND entity_id = ?`, [charge.id]);
        check('audited', audit.some(a => a.action === 'create'), audit);

        console.log('\nWhat is refused');
        const eur = await call('POST', '/api/v1/payment-extras', { ...mould, currency: 'EUR' });
        check('another currency than the PO → 422 CURRENCY_MISMATCH', eur.status === 422 && eur.body.code === 'CURRENCY_MISMATCH', eur);
        const noPo = await call('POST', '/api/v1/payment-extras', { ...mould, purchaseOrderId: 999999999 });
        check('a PO that does not exist → 404', noPo.status === 404, noPo);
        const withBox = await call('POST', '/api/v1/payment-extras', { ...mould, shipmentReference: BOX });
        check('a deposit extra naming a container → 400', withBox.status === 400 && withBox.body.code === 'BAD_FIELD', withBox);
        const plusDiscount = await call('POST', '/api/v1/payment-extras', { ...mould, kind: 'discount' });
        check('a positive discount → 400', plusDiscount.status === 400, plusDiscount);

        console.log('\nA credit on a container balance, from an invoice');
        as('standard');
        const creditBody = { supplierName: SUP, currency: 'USD', amount: -120, kind: 'discount', description: 'Damaged cartons', ridesWith: 'balance', shipmentReference: BOX, sourceKind: 'shipment_document', sourceId: 999999999 };
        const creditRes = await call('POST', '/api/v1/payment-extras', creditBody);
        check('standard adds a credit riding with a box → 201, negative, no PO', creditRes.status === 201 && creditRes.body.amount === -120 && creditRes.body.purchaseOrderId === null && creditRes.body.shipmentReference === BOX && creditRes.body.remaining === -120, creditRes);
        const credit = creditRes.body;
        const dup = await call('POST', '/api/v1/payment-extras', creditBody);
        check('the same line from the same invoice again → 409 ALREADY_ADDED', dup.status === 409 && dup.body.code === 'ALREADY_ADDED' && dup.body.extraId === credit.id, dup);
        const listed = await extrasOf();
        check('the list has both', listed.length === 2 && listed.some(x => x.id === charge.id) && listed.some(x => x.id === credit.id), listed.map(x => x.id));

        console.log('\nRecord payment offers them');
        const open = await call('GET', `/api/v1/supplier-payments/open-items?supplier=${encodeURIComponent(SUP)}&currency=USD`);
        const openExtras = (open.body.items || []).filter(i => i.kind === 'extra');
        check('open-items lists the charge and the credit, the credit negative',
            openExtras.length === 2 && openExtras.find(i => i.id === charge.id)?.remaining === 500 && openExtras.find(i => i.id === credit.id)?.remaining === -120 && openExtras.find(i => i.id === credit.id)?.ridesWith === 'balance', openExtras);

        console.log('\nA transfer pays them');
        as('accountant');
        const signWrong = await pay(120, [{ kind: 'extra', id: credit.id, amount: 120 }]);
        check('a credit applied as a positive amount → 422 SIGN_MISMATCH', signWrong.status === 422 && signWrong.body.code === 'SIGN_MISMATCH', signWrong);
        const alone = await pay(1, [{ kind: 'extra', id: credit.id, amount: -120 }]);
        check('a credit on its own → 422 CREDIT_ALONE', alone.status === 422 && alone.body.code === 'CREDIT_ALONE', alone);
        const tooMuch = await pay(370, [{ kind: 'extra', id: charge.id, amount: 500 }, { kind: 'extra', id: credit.id, amount: -130 }]);
        check('more credit than there is → 422 OVER_APPLIED', tooMuch.status === 422 && tooMuch.body.code === 'OVER_APPLIED', tooMuch);
        const paid = await pay(380, [{ kind: 'extra', id: charge.id, amount: 500 }, { kind: 'extra', id: credit.id, amount: -120 }]);
        check('charge 500 less credit 120, 380 sent → 201, nothing unexplained', paid.status === 201 && paid.body.linesTotal === 380 && paid.body.unexplained === 0, paid);
        const lineOf = id => (paid.body.lines || []).find(l => l.kind === 'extra' && l.targetId === id);
        check('its lines say what they paid', lineOf(charge.id)?.label === `${PO_NUMBER} mould cost` && lineOf(charge.id)?.extraKind === 'mould' && lineOf(charge.id)?.ridesWith === 'deposit' && lineOf(credit.id)?.amount === -120 && lineOf(credit.id)?.shipmentReference === BOX, paid.body.lines);
        let after = await extrasOf();
        const paidCharge = after.find(x => x.id === charge.id);
        check('both settled by the transfer', after.every(x => x.status === 'paid' && x.settledByPaymentId === paid.body.id && x.remaining === 0 && x.paidOn === TODAY), after);
        check('the charge shows the transfer that paid it', paidCharge.settlements?.[0]?.paymentId === paid.body.id && paidCharge.applied === 500, paidCharge.settlements);

        console.log('\nWhile money is applied');
        as('standard');
        const del = await call('DELETE', `/api/v1/payment-extras/${charge.id}`);
        check('deleting it → 409 EXTRA_HAS_PAYMENTS', del.status === 409 && del.body.code === 'EXTRA_HAS_PAYMENTS', del);
        const lower = await call('PUT', `/api/v1/payment-extras/${charge.id}`, { ...mould, amount: 300 });
        check('taking it below what was applied → 409', lower.status === 409 && lower.body.code === 'EXTRA_HAS_PAYMENTS', lower);
        const unmark = await call('POST', `/api/v1/payment-extras/${credit.id}/status`, { status: 'open' });
        check('opening one a transfer settled → 409 SETTLED_BY_TRANSFER', unmark.status === 409 && unmark.body.code === 'SETTLED_BY_TRANSFER', unmark);
        const raised = await call('PUT', `/api/v1/payment-extras/${charge.id}`, { ...mould, amount: 700 });
        check('raising it to 700 → 200, open again with 200 left', raised.status === 200 && raised.body.status === 'open' && raised.body.remaining === 200 && raised.body.settledByPaymentId === null, raised);
        const back = await call('PUT', `/api/v1/payment-extras/${charge.id}`, { ...mould, amount: 500 });
        check('back to 500 → paid again by the same transfer', back.status === 200 && back.body.status === 'paid' && back.body.settledByPaymentId === paid.body.id, back);

        console.log('\nPaid by hand');
        const freight = await call('POST', '/api/v1/payment-extras', { ...mould, amount: 75, kind: 'freight', description: 'Courier for samples' });
        const byHand = await call('POST', `/api/v1/payment-extras/${freight.body.id}/status`, { status: 'paid', paidOn: '2026-09-01' });
        check('mark paid → paid on that date, no transfer', byHand.status === 200 && byHand.body.status === 'paid' && byHand.body.paidOn === '2026-09-01' && byHand.body.settledByPaymentId === null, byHand);
        const reopened = await call('POST', `/api/v1/payment-extras/${freight.body.id}/status`, { status: 'open' });
        check('and open again', reopened.status === 200 && reopened.body.status === 'open' && reopened.body.paidOn === null, reopened);
        as('warehouse');
        const whStatus = await call('POST', `/api/v1/payment-extras/${freight.body.id}/status`, { status: 'paid' });
        check('warehouse cannot mark paid → 403', whStatus.status === 403, whStatus);

        console.log('\nA forwarder paid for a container (ridesWith shipment)');
        as('standard');
        const fwdBody = { supplierName: FWD, currency: 'GBP', amount: 1850, kind: 'freight', description: 'INV-5521', ridesWith: 'shipment', shipmentReference: BOX };
        const withPo = await call('POST', '/api/v1/payment-extras', { ...fwdBody, purchaseOrderId: poId });
        check('naming a PO → 400', withPo.status === 400 && /purchase order/i.test(withPo.body.error || ''), withPo);
        const fwd = await call('POST', '/api/v1/payment-extras', fwdBody);
        check('POST → 201, the container kept, no PO, GBP', fwd.status === 201 && fwd.body.ridesWith === 'shipment' && fwd.body.shipmentReference === BOX
            && fwd.body.purchaseOrderId === null && fwd.body.currency === 'GBP' && fwd.body.label === `${BOX} freight`, fwd);
        const customs = await call('POST', '/api/v1/payment-extras', { ...fwdBody, amount: 240, kind: 'customs', description: null });
        check('customs clearance → 201', customs.status === 201 && customs.body.kind === 'customs', customs);
        const fwdPay = await call('POST', '/api/v1/supplier-payments', {
            supplierName: FWD, amount: 2090, currency: 'GBP', paidOn: TODAY,
            lines: [{ kind: 'extra', id: fwd.body.id, amount: 1850 }, { kind: 'extra', id: customs.body.id, amount: 240 }],
        });
        if (fwdPay.status === 201) paymentIds.add(fwdPay.body.id);
        check('one transfer to the forwarder pays both → 201', fwdPay.status === 201, fwdPay);
        const fwdAfter = (await call('GET', `/api/v1/payment-extras?supplier=${encodeURIComponent(FWD)}`)).body.data || [];
        check('both paid by that transfer', fwdAfter.length === 2 && fwdAfter.every(x => x.status === 'paid' && x.settledByPaymentId === fwdPay.body.id), fwdAfter);
        const wrongPayee = await call('POST', '/api/v1/supplier-payments', { supplierName: SUP, amount: 1850, currency: 'GBP', paidOn: TODAY, lines: [{ kind: 'extra', id: fwd.body.id, amount: 1850 }] });
        if (wrongPayee.status === 201) paymentIds.add(wrongPayee.body.id);
        check('a transfer to someone else cannot pay the forwarder\'s cost', wrongPayee.status >= 400, wrongPayee);

        console.log('\nA supplier credit note (ridesWith account)');
        as('standard');
        const noteBody = { supplierName: SUP, currency: 'USD', amount: -450, kind: 'credit_note', description: 'CN-TEST-1', ridesWith: 'account' };
        const notePos = await call('POST', '/api/v1/payment-extras', { ...noteBody, amount: 450 });
        check('a positive credit note → 400', notePos.status === 400 && /negative/i.test(notePos.body.error || ''), notePos);
        const noteWithPo = await call('POST', '/api/v1/payment-extras', { ...noteBody, purchaseOrderId: poId });
        check('a credit note naming a PO → 400', noteWithPo.status === 400, noteWithPo);
        const note = await call('POST', '/api/v1/payment-extras', noteBody);
        check('POST → 201, on account: no PO, no container, 450 of credit left', note.status === 201 && note.body.ridesWith === 'account' && note.body.purchaseOrderId === null
            && note.body.shipmentReference === null && note.body.remaining === -450 && note.body.label === 'Credit note', note);
        const owed = await call('POST', '/api/v1/payment-extras', { ...mould, amount: 300, kind: 'handling', description: 'Handling for the credit test' });
        const tooMuchCredit = await pay(0, [{ kind: 'extra', id: owed.body.id, amount: 300 }, { kind: 'extra', id: note.body.id, amount: -450 }]);
        check('more credit than what is paid → 422 CREDIT_EXCEEDS', tooMuchCredit.status === 422 && tooMuchCredit.body.code === 'CREDIT_EXCEEDS', tooMuchCredit);
        const nothingSent = await pay(0, [{ kind: 'extra', id: owed.body.id, amount: 300 }]);
        check('nothing sent and no credit → 422 NOTHING_SENT', nothingSent.status === 422 && nothingSent.body.code === 'NOTHING_SENT', nothingSent);
        const byCredit = await pay(0, [{ kind: 'extra', id: owed.body.id, amount: 300 }, { kind: 'extra', id: note.body.id, amount: -300 }]);
        check('300 owed, 300 of the credit, nothing sent → 201', byCredit.status === 201 && Number(byCredit.body.amount) === 0, byCredit);
        const afterCredit = await extrasOf();
        const owedAfter = afterCredit.find(x => x.id === owed.body.id);
        const noteAfter = afterCredit.find(x => x.id === note.body.id);
        check('the charge is paid by it; the credit note has 150 left and stays open',
            !!owedAfter && !!noteAfter && owedAfter.status === 'paid' && owedAfter.settledByPaymentId === byCredit.body.id
            && noteAfter.status === 'open' && noteAfter.remaining === -150 && noteAfter.applied === -300, { owedAfter, noteAfter });
        as('admin');
        const delByCredit = await call('DELETE', `/api/v1/supplier-payments/${byCredit.body.id}`);
        const noteBack = (await extrasOf()).find(x => x.id === note.body.id);
        check('deleting that record gives the credit back in full', delByCredit.status === 204 && !!noteBack && noteBack.remaining === -450 && noteBack.applied === 0, { delByCredit, noteBack });

        console.log('\nDeleting the transfer puts them back');
        as('admin');
        const delPay = await call('DELETE', `/api/v1/supplier-payments/${paid.body.id}`);
        check('admin deletes the transfer → 204', delPay.status === 204, delPay);
        after = await extrasOf();
        check('the charge and the credit are open again, nothing applied',
            after.filter(x => x.id === charge.id || x.id === credit.id).every(x => x.status === 'open' && x.applied === 0 && x.settledByPaymentId === null), after);
        const delCharge = await call('DELETE', `/api/v1/payment-extras/${charge.id}`);
        check('now the charge can be deleted → 204', delCharge.status === 204, delCharge);
        check('and is gone from the list', !(await extrasOf()).some(x => x.id === charge.id));
    } catch (e) {
        failures++;
        console.error('Suite threw:', e);
    } finally {
        await cleanup().catch(e => { failures++; console.error('Cleanup failed:', e.message); });
        server.close();
        await closePool();
    }
    console.log(`\n${passes} passed, ${failures} failed.`);
    process.exit(failures === 0 ? 0 : 1);
})();
