#!/usr/bin/env node
'use strict';

// The supplier-portal PI check (src/services/pi-check.js) end to end on the
// TEST stage: real PDFs in jportal-pi-uploads-test, real rows in
// jportal.pi_submissions, the runner invoked in-process (as the piCheck
// Lambda would be) and, for the success cases, a real Gemini call.
//
//   node tools/test-pi-check-flow.js
//
// Needs: the TEST database in .env, AWS credentials that can put / delete
// objects in jportal-pi-uploads-test, GEMINI_API_KEY, and a fixture login
// (tools/pi-test-helpers.js). Everything it creates is removed again.

process.env.PI_UPLOADS_BUCKET = process.env.PI_UPLOADS_BUCKET || 'jportal-pi-uploads-test';
const H = require('./pi-test-helpers');
const { getPool, closePool } = require('../src/db');
const { createPiChecker } = require('../src/services/pi-check');
const poCheck = require('../src/services/po-invoice-check');
const { withGeminiRetry } = require('../src/lib/gemini-retry');

let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail)}`}`);
    return ok;
};
const s3Event = key => ({
    source: 'aws.s3', 'detail-type': 'Object Created',
    detail: { bucket: { name: process.env.PI_UPLOADS_BUCKET }, object: { key } },
});
const sdkErr = (status, message) => Object.assign(new Error(message || `got status: ${status}`), { status });

(async () => {
    const env = H.refuseUnlessTest();
    console.log(`TEST database ${env.host}/${env.db} · bucket ${env.bucket}`);
    const pool = getPool();
    const created = [];
    try {
        const po = await H.pickPo(pool);
        const poForCheck = await pool.getConnection().then(async (c) => { try { return await poCheck.loadPoForCheck(c, po.id); } finally { c.release(); } });
        console.log(`Using PO ${po.id} (${po.po_number}, ${po.supplier}), ${poForCheck.lines.length} line(s)\n`);
        const base = { supplier_id: po.supplier_id, supplier_name: po.supplier, purchase_order_id: po.id, po_number: po.po_number };
        const goodPdf = await H.makePdf(H.piLines(poForCheck));
        const checker = createPiChecker({ pool });

        console.log('Event path, good PDF');
        const k1 = H.newKey();
        const etag = await H.putPortalObject(k1, goodPdf, 'application/pdf');
        const id1 = await H.insertSubmission({ ...base, s3_key: k1, file_size: goodPdf.length });
        created.push(id1);
        const r1 = await checker.run(s3Event(k1));
        check('event → succeeded', r1.outcome === 'succeeded', r1);
        let row = await H.readSubmission(pool, id1);
        check('row succeeded, attempt 1', row.check_status === 'succeeded' && Number(row.check_attempt) === 1, row);
        check('s3_etag pinned to the object’s ETag', row.s3_etag === etag, { stored: row.s3_etag, etag });
        check('file_size is the bytes read', Number(row.file_size) === goodPdf.length, row.file_size);
        const snap = typeof row.po_snapshot_json === 'string' ? JSON.parse(row.po_snapshot_json) : row.po_snapshot_json;
        check('po_snapshot_json = loadPoForCheck', JSON.stringify(snap) === JSON.stringify(JSON.parse(JSON.stringify(poForCheck))), { snap });
        check('verdict and count recorded', ['pass', 'minor_discrepancies', 'major_discrepancies'].includes(row.verdict) && row.discrepancy_count != null, row.verdict);
        const dup = await checker.run(s3Event(k1));
        check('a duplicate event is a no-op', dup.outcome === 'skipped' && Number((await H.readSubmission(pool, id1)).check_attempt) === 1, dup);
        const [aud] = await pool.query(`SELECT action FROM audit_log WHERE entity_type = 'pi_submission' AND entity_id = ?`, [id1]);
        check('audited check_succeeded once', aud.length === 1 && aud[0].action === 'check_succeeded', aud);

        console.log('\nDirect path, missing object');
        const k2 = H.newKey();
        const id2 = await H.insertSubmission({ ...base, s3_key: k2 });
        created.push(id2);
        const r2 = await checker.run({ submissionId: id2 });
        row = await H.readSubmission(pool, id2);
        check('no claim, no attempt used', r2.reason === 'file_missing' && row.check_status === 'awaiting_upload' && Number(row.check_attempt) === 0, { r2, row });

        console.log('\nA .txt sent as a PDF');
        const k3 = H.newKey();
        await H.putPortalObject(k3, Buffer.from('this is not a pdf'), 'application/pdf');
        const id3 = await H.insertSubmission({ ...base, s3_key: k3 });
        created.push(id3);
        const r3 = await checker.run({ submissionId: id3 });
        row = await H.readSubmission(pool, id3);
        check('failed FILE_INVALID', r3.code === 'FILE_INVALID' && row.error_code === 'FILE_INVALID' && row.s3_etag === null, row);

        console.log('\nForced model errors');
        const k4 = H.newKey();
        await H.putPortalObject(k4, goodPdf, 'application/pdf');
        const id4 = await H.insertSubmission({ ...base, s3_key: k4 });
        created.push(id4);
        const bad = createPiChecker({ pool, checkDocument: async () => { throw Object.assign(new Error('Gemini call failed'), { code: 'MODEL_CALL_FAILED', cause: sdkErr(400, 'INVALID_ARGUMENT') }); } });
        const r4 = await bad.run({ submissionId: id4 });
        check('a forced 400 → DOCUMENT_UNPROCESSABLE', r4.code === 'DOCUMENT_UNPROCESSABLE', r4);

        const k5 = H.newKey();
        await H.putPortalObject(k5, goodPdf, 'application/pdf');
        const id5 = await H.insertSubmission({ ...base, s3_key: k5 });
        created.push(id5);
        let calls = 0;
        const flaky = createPiChecker({
            pool,
            // The real retry, with a deadline that leaves room for one retry only.
            checkDocument: async ({ deadline }) => {
                try {
                    return await withGeminiRetry('forced-503', async () => { calls += 1; throw sdkErr(503); }, 5, { deadline });
                } catch (e) {
                    throw Object.assign(new Error(`Gemini call failed: ${e.message}`), { code: 'MODEL_CALL_FAILED', cause: e });
                }
            },
        });
        const r5 = await flaky.run({ submissionId: id5 }, { deadline: Date.now() + 95 * 1000 });
        check('a forced 503 retries, then MODEL_UNAVAILABLE', r5.code === 'MODEL_UNAVAILABLE' && calls >= 2, { r5, calls });
        const r5b = await checker.run({ submissionId: id5 });
        check('…and Check again (direct) succeeds on the real model', r5b.outcome === 'succeeded'
            && Number((await H.readSubmission(pool, id5)).check_attempt) === 2, r5b);

        console.log('\nA PI quoting another PO number');
        const k6 = H.newKey();
        const otherPdf = await H.makePdf(H.piLines(poForCheck, { quotePo: 'PO_99999Z' }));
        await H.putPortalObject(k6, otherPdf, 'application/pdf');
        const id6 = await H.insertSubmission({ ...base, s3_key: k6 });
        created.push(id6);
        await checker.run({ submissionId: id6 });
        row = await H.readSubmission(pool, id6);
        check('major_discrepancies', row.check_status === 'succeeded' && row.verdict === 'major_discrepancies', { status: row.check_status, verdict: row.verdict });
    } catch (e) {
        failures++;
        console.error('ERROR', e);
    } finally {
        await H.cleanupSubmissions(pool, created).catch(e => console.error('cleanup failed', e.message));
        await H.closeFixtures();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
