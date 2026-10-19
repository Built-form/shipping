#!/usr/bin/env node
'use strict';

// One supplier-portal PI end to end on the TEST stage: the PDF is put in
// jportal-pi-uploads-test and a draft row made as jportal makes them; the
// piCheck runner checks it (real Gemini); the row is submitted as jportal's
// submit would; then POST /api/v1/pi-submissions/:id/import (in-process
// Express) imports it and emails the decision through Front.
//
//   PI_NOTIFY_REDIRECT_TO=<a mailbox you can read> node tools/test-pi-import-flow.js
//
// Needs: the TEST database in .env, AWS credentials for the test bucket,
// GEMINI_API_KEY, FRONT_API_TOKEN + FRONT_CHANNEL_ID, a fixture login
// (tools/pi-test-helpers.js), and PI_NOTIFY_REDIRECT_TO — it refuses to run
// without one, so no supplier is ever emailed. Check that mailbox afterwards.
// The submission, the invoice and its rows are removed again unless --keep.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';
process.env.PI_UPLOADS_BUCKET = process.env.PI_UPLOADS_BUCKET || 'jportal-pi-uploads-test';

const http = require('http');
const H = require('./pi-test-helpers');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');
const { createPiChecker } = require('../src/services/pi-check');
const poCheck = require('../src/services/po-invoice-check');
const { jportalSchema } = require('../src/lib/jportal');

const KEEP = process.argv.includes('--keep');
let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail).slice(0, 600)}`}`);
    return ok;
};

(async () => {
    const env = H.refuseUnlessTest();
    if (!/@/.test(process.env.PI_NOTIFY_REDIRECT_TO || '')) {
        console.error('Refusing to run: set PI_NOTIFY_REDIRECT_TO to a mailbox you can read.');
        process.exit(2);
    }
    for (const k of ['GEMINI_API_KEY', 'FRONT_API_TOKEN', 'FRONT_CHANNEL_ID']) {
        if (!process.env[k]) { console.error(`Refusing to run: ${k} is not set.`); process.exit(2); }
    }
    const pool = getPool();
    const server = http.createServer(app).listen(0);
    await new Promise(r => server.once('listening', r));
    const base = `http://127.0.0.1:${server.address().port}`;
    console.log(`TEST database ${env.host}/${env.db} · bucket ${env.bucket} · email → ${process.env.PI_NOTIFY_REDIRECT_TO}`);

    let id = null;
    try {
        const po = await H.pickPo(pool);
        const c = await pool.getConnection();
        let poForCheck;
        try { poForCheck = await poCheck.loadPoForCheck(c, po.id); } finally { c.release(); }
        console.log(`Using PO ${po.id} (${po.po_number}, ${po.supplier})\n`);

        // 1. Upload + draft, as jportal does.
        const key = H.newKey();
        const pdf = await H.makePdf(H.piLines(poForCheck, {
            bank: { beneficiaryName: po.supplier, bankName: 'Test Bank of Shenzhen', accountNumber: '6222 0000 1111 2222', swiftBic: 'TESTCNSZ' },
        }));
        await H.putPortalObject(key, pdf, 'application/pdf');
        id = await H.insertSubmission({
            supplier_id: po.supplier_id, supplier_name: po.supplier, purchase_order_id: po.id, po_number: po.po_number,
            s3_key: key, file_size: pdf.length, filename: `PI ${po.po_number}.pdf`,
        });

        // 2. The check.
        const ran = await createPiChecker({ pool }).run({ submissionId: id });
        check('piCheck → succeeded', ran.outcome === 'succeeded', ran);

        // 3. Submit, as jportal's submit does.
        const fx = await H.fixtures();
        await fx.query(
            `UPDATE ${jportalSchema()}.pi_submissions
                SET status = 'submitted', submitted_at = UTC_TIMESTAMP(), warnings_acknowledged = 1,
                    supplier_note = 'End-to-end test submission, please ignore.', row_version = row_version + 1
              WHERE id = ? AND status = 'draft'`,
            [id]
        );

        // 4. Import.
        const d = await (await fetch(`${base}/api/v1/pi-submissions/${id}`)).json();
        check('detail → usable, canImport', d.check?.usable === true && d.canImport === true, d);
        const body = { rowVersion: d.rowVersion, note: 'Imported by the end-to-end test.', recheck: false };
        if (d.bankCheck?.state === 'changed' || d.bankCheck?.state === 'no_history') {
            body.bankVerification = { method: 'other', note: 'End-to-end test' };
        }
        const res = await fetch(`${base}/api/v1/pi-submissions/${id}/import`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
        });
        const out = await res.json();
        check('import → 200', res.status === 200, out);

        const [inv] = await pool.query(`SELECT * FROM purchase_order_invoices WHERE pi_submission_id = ?`, [id]);
        check('purchase_order_invoices row with pi_submission_id and the supplier as uploader',
            inv.length === 1 && inv[0].uploaded_by_email === 'pi-test@example.invalid' && Number(inv[0].purchase_order_id) === Number(po.id), inv);
        const invoiceId = inv[0] && inv[0].id;
        const [checks] = await pool.query(`SELECT * FROM purchase_order_invoice_checks WHERE purchase_order_invoice_id = ?`, [invoiceId]);
        check('the check copied with triggered_by supplier_portal', checks.length === 1 && checks[0].status === 'succeeded' && checks[0].triggered_by === 'supplier_portal', checks);
        const [pays] = await pool.query(`SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`, [invoiceId]);
        check('the payment row with verification fields', pays.length === 1 && ['verified', 'needs_verification'].includes(pays[0].bank_verification_status)
            && pays[0].bank_fingerprint != null, pays[0]);
        const row = await H.readSubmission(pool, id);
        check('the submission is imported, pointing at the invoice', row.status === 'imported' && Number(row.imported_invoice_id) === Number(invoiceId) && row.decided_by === 'local@dev', row);
        check('notified_at is set', row.notified_at != null && row.notify_error == null, { notified_at: row.notified_at, notify_error: row.notify_error, notification: out.notification });
        check('the email went to the redirect address', out.notification?.sent === true && out.notification.to === process.env.PI_NOTIFY_REDIRECT_TO, out.notification);
        console.log(`\nNow check ${process.env.PI_NOTIFY_REDIRECT_TO} for "PI accepted – ${po.po_number}".`);
    } catch (e) {
        failures++;
        console.error('ERROR', e);
    } finally {
        if (id && !KEEP) await H.cleanupSubmissions(pool, [id]).catch(e => console.error('cleanup failed', e.message));
        await H.closeFixtures();
        server.close();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
