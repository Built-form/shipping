#!/usr/bin/env node
'use strict';

// HTTP suite for /api/v1/pi-submissions, verify-bank and the two payment
// gates. Drives the real Express app in-process against the TEST database and
// the portal's TEST bucket, like tools/test-payment-reviews.js.
//
//   node tools/test-pi-submissions.js
//
// Locally every request is local@dev; the role comes from LOCAL_USER_TYPE on
// each request. Submissions are inserted as jportal would leave them
// (submitted, with a check result written as piCheck writes it) through the
// fixture login (tools/pi-test-helpers.js). No Gemini call is needed: a
// re-check is simulated by upsertInvoicePaymentTerms, which is what
// recordInvoiceCheck calls with the new extraction. The emails go nowhere
// real: PI_NOTIFY_REDIRECT_TO is forced to an @example.invalid address, and
// without Front configured they are recorded as failed (notify_error).
// Everything it creates is removed again.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';
process.env.PI_UPLOADS_BUCKET = process.env.PI_UPLOADS_BUCKET || 'jportal-pi-uploads-test';
process.env.PI_NOTIFY_REDIRECT_TO = process.env.PI_NOTIFY_REDIRECT_TO || 'pi-test-notify@example.invalid';

const http = require('http');
const H = require('./pi-test-helpers');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');
const { upsertInvoicePaymentTerms } = require('../src/services/po-invoice-check');
const { jportalSchema } = require('../src/lib/jportal');

const STAMP = Date.now();
let base;
let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail).slice(0, 600)}`}`);
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
const BANK_A = { beneficiaryName: `PI Test Supplier ${STAMP}`, bankName: 'Test Bank', accountNumber: `11${STAMP}`, swiftBic: 'TESTGB2L' };
const BANK_B = { ...BANK_A, accountNumber: `99${STAMP}` };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
    const env = H.refuseUnlessTest();
    const pool = getPool();
    const server = http.createServer(app).listen(0);
    await new Promise(r => server.once('listening', r));
    base = `http://127.0.0.1:${server.address().port}`;
    console.log(`TEST database ${env.host}/${env.db} · bucket ${env.bucket} · run ${STAMP}`);

    const created = [];
    const scratchPos = [];
    const staffInvoices = [];
    try {
        const po = await H.pickPo(pool);
        const pdf = await H.makePdf(['PROFORMA INVOICE', `PO ${po.po_number}`]);
        // A submitted PI on `poId`, with a usable check (unless `check` says
        // otherwise) and the object in the bucket, ETag pinned.
        const submit = async ({ poId = po.id, poNumber = po.po_number, bank = null, checkStatus = 'succeeded', verdict = 'pass', withObject = true } = {}) => {
            const key = H.newKey();
            const etag = withObject ? await H.putPortalObject(key, pdf, 'application/pdf') : null;
            const result = checkStatus === 'succeeded' ? H.checkResult({ verdict, bank, poNumber }) : null;
            const id = await H.insertSubmission({
                supplier_id: po.supplier_id, supplier_name: po.supplier, purchase_order_id: poId, po_number: poNumber,
                s3_key: key, s3_etag: etag, file_size: pdf.length, filename: `PI ${STAMP}.pdf`,
                check_status: checkStatus, check_attempt: 1, verdict: result ? verdict : null, discrepancy_count: result ? 0 : null,
                result_json: result, po_snapshot_json: null, error_code: checkStatus === 'failed' ? 'MODEL_UNAVAILABLE' : null,
                status: 'submitted', supplier_note: 'Please see attached', warnings_acknowledged: 1,
                submitted_at: new Date().toISOString().slice(0, 19).replace('T', ' '),
            });
            created.push(id);
            return id;
        };
        const detail = async id => (await call('GET', `/api/v1/pi-submissions/${id}`)).body;
        const paymentOf = async invoiceId => (await sql(`SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`, [invoiceId]))[0] || null;
        const auditActions = async (type, id) => (await sql(`SELECT action FROM audit_log WHERE entity_type = ? AND entity_id = ? ORDER BY id`, [type, id])).map(r => r.action);
        const decide = async (id, path, body) => {
            const d = await detail(id);
            return call('POST', `/api/v1/pi-submissions/${id}/${path}`, { rowVersion: d.rowVersion, ...body });
        };

        console.log('\nRoles');
        const s1 = await submit({ bank: BANK_A });
        as('readonly');
        for (const [path, body] of [['import', { rowVersion: 0 }], ['reject', { rowVersion: 0, note: 'x' }], ['notify', {}]]) {
            const r = await call('POST', `/api/v1/pi-submissions/${s1}/${path}`, body);
            check(`readonly → ${path} 403 CANNOT_DECIDE`, r.status === 403 && r.body.code === 'CANNOT_DECIDE', r);
        }
        const rv = await call('POST', '/api/v1/purchase-order-invoices/1/verify-bank', { method: 'phone_known_number' });
        check('readonly → verify-bank 403 CANNOT_DECIDE', rv.status === 403 && rv.body.code === 'CANNOT_DECIDE', rv);
        const ro = await call('GET', `/api/v1/pi-submissions/${s1}`);
        check('readonly can still look', ro.status === 200 && ro.body.id === s1, ro);
        as('standard');

        console.log('\nList and detail');
        const list = await call('GET', `/api/v1/pi-submissions?purchaseOrderId=${po.id}`);
        check('list → { data, hasMore }, without the result', list.status === 200 && Array.isArray(list.body.data) && typeof list.body.hasMore === 'boolean'
            && list.body.data.some(s => s.id === s1) && !('check' in list.body.data[0]), list.body);
        const sup = await call('GET', '/api/v1/pi-submissions/suppliers');
        check('suppliers → the supplier with a pending count', sup.status === 200 && sup.body.data.some(s => s.supplierId === Number(po.supplier_id) && s.pending >= 1), sup.body);
        const d1 = await detail(s1);
        check('detail → usable check, a file link, canImport', d1.check?.usable === true && d1.file?.url && d1.canImport === true, d1);
        check('detail → bankCheck carries state, fields, current, previous', ['changed', 'no_history', 'unchanged'].includes(d1.bankCheck?.state)
            && Array.isArray(d1.bankCheck.fields) && d1.bankCheck.current?.accountNumber === BANK_A.accountNumber, d1.bankCheck);

        console.log('\nBank gate at import');
        const stale = await call('POST', `/api/v1/pi-submissions/${s1}/import`, { rowVersion: d1.rowVersion + 7 });
        check('a stale rowVersion → 409 STALE', stale.status === 409 && stale.body.code === 'STALE', stale);
        if (d1.bankCheck.state === 'no_history' || d1.bankCheck.state === 'changed') {
            const gated = await decide(s1, 'import', {});
            check(`${d1.bankCheck.state} without bankVerification → 422 BANK_CHANGE_UNCONFIRMED`, gated.status === 422 && gated.body.code === 'BANK_CHANGE_UNCONFIRMED', gated);
        }
        const noNote = await decide(s1, 'import', { bankVerification: { method: 'other' } });
        check('method "other" without a note → 400 BAD_FIELD', noNote.status === 400 && noNote.body.code === 'BAD_FIELD', noNote);
        const imp1 = await decide(s1, 'import', { note: 'Thanks', bankVerification: { method: 'phone_known_number', note: 'Called +86 number on file' } });
        check('import → 200 { submission, invoice, check, notification }', imp1.status === 200 && imp1.body.submission?.status === 'imported'
            && imp1.body.invoice?.piSubmissionId === s1 && imp1.body.check?.triggeredBy === 'supplier_portal' && imp1.body.notification, imp1);
        const inv1 = imp1.body.invoice?.id;
        const pay1 = await paymentOf(inv1);
        check('payment row verified at import', pay1?.bank_verification_status === 'verified' && pay1?.bank_verification_method === 'phone_known_number', pay1);
        const again = await decide(s1, 'import', {});
        check('importing twice → 409 NOT_SUBMITTED with the invoice', again.status === 409 && again.body.code === 'NOT_SUBMITTED' && again.body.importedInvoiceId === inv1, again);

        const s2 = await submit({ bank: BANK_A });
        const d2 = await detail(s2);
        check('the same details again → unchanged', d2.bankCheck?.state === 'unchanged' && d2.bankCheck.previous?.invoiceId === inv1, d2.bankCheck);
        const imp2 = await decide(s2, 'import', {});
        const pay2 = await paymentOf(imp2.body.invoice?.id);
        check('an unchanged import ends verified (matches_previous)', imp2.status === 200 && pay2?.bank_verification_status === 'verified' && pay2.bank_verification_method === 'matches_previous', { imp2, pay2 });
        check('…with no bank_details_unverified audit row', !(await auditActions('purchase_order', po.id)).includes('bank_details_unverified'));

        const s3 = await submit({ bank: null });
        const d3 = await detail(s3);
        check('no details read → not_extracted', d3.bankCheck?.state === 'not_extracted', d3.bankCheck);
        const imp3 = await decide(s3, 'import', {});
        check('a not_extracted import ends needs_verification', imp3.status === 200 && (await paymentOf(imp3.body.invoice?.id))?.bank_verification_status === 'needs_verification', imp3);

        console.log('\nAn unusable check');
        const s4 = await submit({ checkStatus: 'failed' });
        const d4 = await detail(s4);
        check('detail → not usable, bank not_checked', d4.check?.usable === false && d4.bankCheck?.state === 'not_checked', d4);
        const withBank = await decide(s4, 'import', { bankVerification: { method: 'phone_known_number' } });
        check('bankVerification on an unusable check → 400 BAD_FIELD', withBank.status === 400 && withBank.body.code === 'BAD_FIELD', withBank);
        const imp4 = await decide(s4, 'import', {});
        const inv4 = imp4.body.invoice?.id;
        const pay4 = await paymentOf(inv4);
        check('import → payment row exists, empty, needs_verification', imp4.status === 200 && pay4 && pay4.bank_fingerprint === '' && pay4.bank_verification_status === 'needs_verification', pay4);
        check('…and a pending re-check', imp4.body.check?.status === 'pending' && imp4.body.check?.triggeredBy === 'import_recheck', imp4.body.check);
        const paid4 = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${inv4}/payment-status`, { paymentStatus: 'paid' });
        check('PUT payment-status paid → 409 BANK_UNVERIFIED', paid4.status === 409 && paid4.body.code === 'BANK_UNVERIFIED', paid4);
        await sleep(15000); // the background re-check on a 3-line test PDF fails or finishes by now
        const paid4b = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${inv4}/payment-status`, { paymentStatus: 'paid' });
        check('…still 409 after the background re-check', paid4b.status === 409 && paid4b.body.code === 'BANK_UNVERIFIED', paid4b);
        const nothing = await call('POST', `/api/v1/purchase-order-invoices/${inv4}/verify-bank`, { method: 'phone_known_number' });
        if ((await paymentOf(inv4)).bank_fingerprint === '') {
            check('verify-bank with no details and no bankDetails → 409 NO_BANK_DETAILS', nothing.status === 409 && nothing.body.code === 'NO_BANK_DETAILS', nothing);
        }
        const typed = await call('POST', `/api/v1/purchase-order-invoices/${inv4}/verify-bank`, { method: 'existing_email_thread', note: 'From the March thread', bankDetails: BANK_A });
        check('verify-bank with bankDetails → verified', typed.status === 200 && typed.body.bankVerificationStatus === 'verified' && typed.body.bankVerified?.accountNumber === BANK_A.accountNumber, typed);
        const paid4c = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${inv4}/payment-status`, { paymentStatus: 'paid' });
        check('…then payment succeeds', paid4c.status === 200 && paid4c.body.paymentStatus === 'paid', paid4c);
        await sql(`UPDATE purchase_order_invoice_payments SET payment_status = 'pending' WHERE purchase_order_invoice_id = ?`, [inv4]);

        console.log('\nBank gate at payment');
        // A ↻ re-check that reads different details: what recordInvoiceCheck does.
        const c = await pool.getConnection();
        try {
            await upsertInvoicePaymentTerms(c, { invoiceId: inv1, purchaseOrderId: po.id, checkId: null, paymentTerms: H.checkResult({ bank: BANK_B }).paymentTerms, modelUsed: 'test', userEmail: 'local@dev' });
        } finally { c.release(); }
        check('a re-check with other details → needs_verification', (await paymentOf(inv1)).bank_verification_status === 'needs_verification');
        check('…audited bank_details_unverified', (await auditActions('purchase_order', po.id)).includes('bank_details_unverified'));
        const paid1 = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${inv1}/payment-status`, { paymentStatus: 'paid' });
        check('PUT payment-status paid → 409 BANK_UNVERIFIED', paid1.status === 409 && paid1.body.code === 'BANK_UNVERIFIED', paid1);
        const arranged = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${inv1}/payment-status`, { paymentStatus: 'skipped' });
        check('…skipped is still allowed', arranged.status === 200, arranged);
        await sql(`UPDATE purchase_order_invoice_payments SET payment_status = 'pending' WHERE purchase_order_invoice_id = ?`, [inv1]);
        const payRow1 = await paymentOf(inv1);
        const transfer = await call('POST', '/api/v1/supplier-payments', {
            supplierName: po.supplier, amount: 1, currency: payRow1.currency || 'USD', paidOn: '2026-10-20',
            lines: [{ kind: 'pi', id: payRow1.id, amount: 1 }],
        });
        check('a supplier transfer line on it → 422 BANK_UNVERIFIED', transfer.status === 422 && transfer.body.code === 'BANK_UNVERIFIED', transfer);
        const verified = await call('POST', `/api/v1/purchase-order-invoices/${inv1}/verify-bank`, { method: 'phone_known_number' });
        check('verify-bank (details on file) → verified', verified.status === 200 && verified.body.bankVerificationStatus === 'verified', verified);

        console.log('\nThe positive gate: no payment row');
        await sql(`DELETE FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`, [inv1]);
        const noRow = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${inv1}/payment-status`, { paymentStatus: 'paid' });
        check('PUT payment-status → refused (404, no payment row)', noRow.status === 404, noRow);
        const noRowTransfer = await call('POST', '/api/v1/supplier-payments', {
            supplierName: po.supplier, amount: 1, currency: 'USD', paidOn: '2026-10-20', lines: [{ kind: 'pi', id: payRow1.id, amount: 1 }],
        });
        check('a transfer line to the deleted row → refused', noRowTransfer.status >= 400, noRowTransfer);

        console.log('\nA staff-uploaded PI is not affected');
        const [ins] = await getPool().query(
            `INSERT INTO purchase_order_invoices (purchase_order_id, filename, s3_key, content_type, file_size, uploaded_by_email)
             VALUES (?, ?, ?, 'application/pdf', 1, 'local@dev')`, [po.id, `staff-${STAMP}.pdf`, `invoices/test/${STAMP}.pdf`]);
        staffInvoices.push(ins.insertId);
        await sql(`INSERT INTO purchase_order_invoice_payments (purchase_order_invoice_id, purchase_order_id, bank_name) VALUES (?, ?, 'X')`, [ins.insertId, po.id]);
        const staffPaid = await call('PUT', `/api/v1/purchase-orders/${po.id}/invoices/${ins.insertId}/payment-status`, { paymentStatus: 'paid' });
        check('PUT payment-status paid → 200', staffPaid.status === 200 && staffPaid.body.bankVerificationStatus === null, staffPaid);
        const staffVerify = await call('POST', `/api/v1/purchase-order-invoices/${ins.insertId}/verify-bank`, { method: 'phone_known_number' });
        check('verify-bank on it → 409 NOT_PORTAL_PI', staffVerify.status === 409 && staffVerify.body.code === 'NOT_PORTAL_PI', staffVerify);

        console.log('\nReject and notify');
        const s5 = await submit({ bank: BANK_A });
        const noReason = await decide(s5, 'reject', {});
        check('reject without a note → 400 BAD_FIELD', noReason.status === 400, noReason);
        const rej = await decide(s5, 'reject', { note: 'Wrong currency, please reissue in USD.' });
        check('reject → 200, rejected, the note kept', rej.status === 200 && rej.body.submission?.status === 'rejected'
            && rej.body.submission.decidedNote === 'Wrong currency, please reissue in USD.' && rej.body.notification, rej);
        check('…audited reject', (await auditActions('pi_submission', s5)).includes('reject'));
        await (await H.fixtures()).query(`UPDATE ${jportalSchema()}.pi_submissions SET notified_at = NULL, notify_error = NULL WHERE id = ?`, [s5]);
        const n = await call('POST', `/api/v1/pi-submissions/${s5}/notify`, {});
        check('notify on a decided row with both email columns null → 200 and a send attempt', n.status === 200 && typeof n.body.notification?.sent === 'boolean'
            && n.body.notification.to === process.env.PI_NOTIFY_REDIRECT_TO, n);
        const row5 = await H.readSubmission(pool, s5);
        check('…recorded as notified_at or notify_error', Boolean(row5.notified_at) !== Boolean(row5.notify_error), row5);
        if (row5.notified_at) {
            const twice = await call('POST', `/api/v1/pi-submissions/${s5}/notify`, {});
            check('notify again without force → 409 ALREADY_NOTIFIED', twice.status === 409 && twice.body.code === 'ALREADY_NOTIFIED', twice);
        }

        console.log('\nBlockers and races');
        const [scratch] = await getPool().query(
            `INSERT INTO purchase_orders (po_number, supplier, currency, deleted_at) VALUES (?, ?, 'USD', NOW())`, [`PITEST-${STAMP}`, po.supplier]);
        scratchPos.push(scratch.insertId);
        const s6 = await submit({ poId: scratch.insertId, poNumber: `PITEST-${STAMP}` });
        const del = await decide(s6, 'import', { bankVerification: { method: 'phone_known_number' } });
        check('a deleted PO → 422 NOT_IMPORTABLE po_deleted', del.status === 422 && del.body.code === 'NOT_IMPORTABLE' && del.body.blockers?.includes('po_deleted'), del);
        const s7 = await submit({ bank: BANK_A });
        const d7 = await detail(s7);
        const [a, b] = await Promise.all([
            call('POST', `/api/v1/pi-submissions/${s7}/import`, { rowVersion: d7.rowVersion }),
            call('POST', `/api/v1/pi-submissions/${s7}/import`, { rowVersion: d7.rowVersion }),
        ]);
        const statuses = [a.status, b.status].sort();
        check('two imports at once → one 200, one 409', statuses[0] === 200 && statuses[1] === 409, { a, b });
        const invCount = await sql(`SELECT COUNT(*) AS n FROM purchase_order_invoices WHERE pi_submission_id = ?`, [s7]);
        check('…and one invoice', Number(invCount[0].n) === 1, invCount);
        const s8 = await submit({ bank: BANK_A, withObject: false });
        const gone = await decide(s8, 'import', {});
        check('the file gone from the bucket → 422 file_gone', gone.status === 422 && gone.body.blockers?.includes('file_gone'), gone);
    } catch (e) {
        failures++;
        console.error('ERROR', e);
    } finally {
        await H.cleanupSubmissions(getPool(), created).catch(e => console.error('cleanup failed', e.message));
        if (staffInvoices.length) {
            await sql(`DELETE FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id IN (?)`, [staffInvoices]).catch(() => {});
            await sql(`DELETE FROM purchase_order_invoices WHERE id IN (?)`, [staffInvoices]).catch(() => {});
        }
        if (scratchPos.length) await sql(`DELETE FROM purchase_orders WHERE id IN (?)`, [scratchPos]).catch(() => {});
        await H.closeFixtures();
        server.close();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
