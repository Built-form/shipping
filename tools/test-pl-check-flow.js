#!/usr/bin/env node
'use strict';

// The supplier-portal packing-list check (src/services/pl-check.js) end to
// end on the TEST stage: real PDFs in jportal-pi-uploads-test (under pl/),
// real rows in jportal.pl_submissions, the runner invoked in-process (as the
// plCheck Lambda would be) and, for the first case, a real Gemini read. The
// other cases stub readPackingList so they are deterministic and free.
//
//   node tools/test-pl-check-flow.js
//
// Needs: the TEST database in .env, AWS credentials that can put / delete
// objects in jportal-pi-uploads-test, GEMINI_API_KEY, a fixture login
// (tools/pl-test-helpers.js), and a booked container on the test database
// holding live lines of a supplier that exists in `suppliers`. Rows are
// inserted as the portal creates them: group_supplier_ids = the supplier ids
// whose names are on the container's lines (pickContainer), supplier_id the
// lowest, supplier_name its live name. Everything it creates is removed again.

process.env.PI_UPLOADS_BUCKET = process.env.PI_UPLOADS_BUCKET || 'jportal-pi-uploads-test';
const H = require('./pl-test-helpers');
const { getPool, closePool } = require('../src/db');
const { createPlChecker } = require('../src/services/pl-check');
const P = require('../src/services/packing-list-check');
const PL = require('../src/services/pl-results');
const L = require('../src/lib/pl-submissions');
const { iso6346Valid } = require('../src/lib/pl-match');

let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail ?? null).slice(0, 700)}`}`);
    return ok;
};
const skip = (label, why) => console.log(`SKIP  ${label}  (${why})`);
const s3Event = key => ({
    source: 'aws.s3', 'detail-type': 'Object Created',
    detail: { bucket: { name: process.env.PI_UPLOADS_BUCKET }, object: { key } },
});
const sdkErr = (status, message) => Object.assign(new Error(message || `got status: ${status}`), { status });
const json = v => (typeof v === 'string' ? JSON.parse(v) : v);
// A checker whose read returns `extracted` without Gemini.
const stubbed = (pool, extracted) => createPlChecker({ pool, readPackingList: async () => ({ parsed: JSON.parse(JSON.stringify(extracted)), modelUsed: 'stub', usage: null }) });

(async () => {
    const env = H.refuseUnlessTest();
    console.log(`TEST database ${env.host}/${env.db} · bucket ${env.bucket}`);
    const pool = getPool();
    const created = [];
    const staffPls = [];
    try {
        const picked = await H.pickContainer(pool);
        if (!picked) throw new Error('No booked container with live lines of a registered supplier on the test database.');
        const { container, ref, supplierId, supplierName, group, staffKey, rows } = picked;
        console.log(`Using container ${ref}, supplier ${supplierId} (${supplierName}), group ${JSON.stringify(group.supplierIds)} = ${JSON.stringify(group.names)} (staff key ${staffKey}), ${rows.length} line(s)\n`);
        // The row as the portal creates it: the granted ids, the lowest as supplier_id, its live name.
        const base = { supplier_id: supplierId, supplier_name: supplierName, group_supplier_ids: group.supplierIds };
        const sameGroup = row => JSON.stringify((json(row.group_supplier_ids) || []).map(Number)) === JSON.stringify(group.supplierIds);
        const extracted = H.extractedFrom(rows, { invoiceNumber: `PLTEST-${Date.now()}`, containerNumbers: [ref] });
        const goodPdf = await H.makePdf(H.plLines(extracted));
        const checker = createPlChecker({ pool });
        const auditActions = async id => (await pool.query(`SELECT action FROM audit_log WHERE entity_type = 'pl_submission' AND entity_id = ? ORDER BY id`, [id]))[0].map(r => r.action);

        console.log('Event path, a real read of a PDF built from the container’s lines');
        const k1 = H.newKey();
        const etag = await H.putPortalObject(k1, goodPdf, 'application/pdf');
        const id1 = await H.insertSubmission({ ...base, s3_key: k1, file_size: goodPdf.length, filename: 'PL-324.pdf' });
        created.push(id1);
        let r1 = await checker.run(s3Event(k1));
        let attempts1 = 1;
        if (r1.outcome === 'failed' && L.RETRYABLE_CODES.includes(r1.code)) {
            // Gemini itself is unavailable (no key, quota, billing): the row is
            // a retryable failure, which a Check again (direct) may claim. Read
            // it with the stub instead, so the rest of the suite still runs.
            const why = (await H.readSubmission(pool, id1)).error_message;
            skip('a real Gemini read', `${r1.code}: ${String(why || '').slice(0, 160)}`);
            r1 = await stubbed(pool, extracted).run({ submissionId: id1 });
            attempts1 = 2;
        }
        check('event → succeeded', r1.outcome === 'succeeded', r1);
        let row = await H.readSubmission(pool, id1);
        check(`row succeeded, attempt ${attempts1}`, row.check_status === 'succeeded' && Number(row.check_attempt) === attempts1, { status: row.check_status, attempt: row.check_attempt, error: row.error_code });
        check('s3_etag pinned, file_size is the bytes read', row.s3_etag === etag && Number(row.file_size) === goodPdf.length, { stored: row.s3_etag, etag, size: row.file_size });
        const x1 = json(row.extract_json);
        check('extract_json holds the rows read; row_count and invoice_number copied', x1 && x1.lines.length > 0 && Number(row.row_count) === x1.lines.length && row.invoice_number === x1.invoiceNumber, { rows: x1?.lines?.length, row_count: row.row_count, inv: row.invoice_number });
        check('the container is suggested as confident', row.match_confidence === 'confident' && row.suggested_kind === 'booked' && row.suggested_container_number === ref,
            { confidence: row.match_confidence, reason: row.match_reason, suggested: row.suggested_container_number, candidates: json(row.candidates_json)?.top?.map(c => [c.key, c.score, c.coverage]) });
        check('match_reason is one of the rules', ['replaces', 'box_number', 'po_numbers', 'lines', 'single', 'conflict'].includes(row.match_reason), row.match_reason);
        const cands = json(row.candidates_json);
        check('candidates_json keeps the top 5 with every ratio, the hits and the conflicts, plus the top-level conflicts', cands && Array.isArray(cands.top) && cands.top.length > 0 && Array.isArray(cands.conflicts)
            && ['coverage', 'exact', 'recall', 'lineHit', 'poShare', 'boxHit', 'refHit', 'replacesHit', 'nearBox', 'score', 'conflicts'].every(k => k in cands.top[0]), cands?.top?.[0]);
        check('group_supplier_ids is the portal’s, untouched by the check', sameGroup(row), { stored: row.group_supplier_ids, portal: group.supplierIds });
        const res1 = json(row.result_json);
        check('result_json is provisional, v1, with the container', res1?.v === 1 && res1.state === 'provisional' && row.result_state === 'provisional' && res1.container?.kind === 'booked', res1 && { v: res1.v, state: res1.state, container: res1.container });
        check('result_json contains only allowed keys (no ids, fingerprints, sign-off emails)', H.resultLeaks(res1).length === 0, H.resultLeaks(res1));
        check('verdict and count recorded', ['match', 'differences'].includes(row.verdict) && row.discrepancy_count != null, { verdict: row.verdict, count: row.discrepancy_count });
        check('row_version bumped by the claim, the pin and the result', Number(row.row_version) >= 3, row.row_version);
        const dup = await checker.run(s3Event(k1));
        check('a duplicate event is a no-op', dup.outcome === 'skipped' && Number((await H.readSubmission(pool, id1)).check_attempt) === attempts1, dup);
        check('audited check_succeeded once', (await auditActions(id1)).filter(a => a === 'check_succeeded').length === 1, await auditActions(id1));

        console.log('\nRefresh (direct invoke with action refresh): the same answer, no bump');
        const rr = await checker.run({ submissionId: id1, action: 'refresh' });
        const after = await H.readSubmission(pool, id1);
        check('refresh → refreshed, written, nothing changed', rr.outcome === 'refreshed' && rr.written === true && rr.changed === false && Number(after.row_version) === Number(row.row_version), { rr, before: row.row_version, after: after.row_version });

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
        check('failed FILE_INVALID, no ETag', r3.code === 'FILE_INVALID' && row.error_code === 'FILE_INVALID' && row.s3_etag === null, row);

        console.log('\nForced model errors (readPackingList stubbed)');
        const k4 = H.newKey();
        await H.putPortalObject(k4, goodPdf, 'application/pdf');
        const id4 = await H.insertSubmission({ ...base, s3_key: k4 });
        created.push(id4);
        const bad = createPlChecker({ pool, readPackingList: async () => { throw Object.assign(new Error('Gemini call failed'), { code: 'MODEL_CALL_FAILED', cause: sdkErr(400, 'INVALID_ARGUMENT') }); } });
        const r4 = await bad.run({ submissionId: id4 });
        row = await H.readSubmission(pool, id4);
        check('a forced 400 → DOCUMENT_UNPROCESSABLE, group_supplier_ids untouched', r4.code === 'DOCUMENT_UNPROCESSABLE' && row.error_code === 'DOCUMENT_UNPROCESSABLE'
            && sameGroup(row), { r4, group: row.group_supplier_ids });
        // DOCUMENT_UNPROCESSABLE is not retryable, so a Check again on id4 is
        // refused (planEvent: not_retryable); the retryable path gets its own row.
        const r4x = await stubbed(pool, extracted).run({ submissionId: id4 });
        check('…a Check again on a non-retryable failure is refused, no attempt used', r4x.outcome === 'skipped' && r4x.reason === 'not_retryable' && Number((await H.readSubmission(pool, id4)).check_attempt) === 1, r4x);
        const k4b = H.newKey();
        await H.putPortalObject(k4b, goodPdf, 'application/pdf');
        const id4b = await H.insertSubmission({ ...base, s3_key: k4b });
        created.push(id4b);
        const r4b = await createPlChecker({ pool, readPackingList: async () => { throw Object.assign(new Error('Gemini call failed'), { code: 'MODEL_CALL_FAILED', cause: sdkErr(503) }); } }).run({ submissionId: id4b });
        check('a forced 503 → MODEL_UNAVAILABLE (retryable), group_supplier_ids untouched', r4b.code === 'MODEL_UNAVAILABLE' && Number((await H.readSubmission(pool, id4b)).check_attempt) === 1
            && sameGroup(await H.readSubmission(pool, id4b)), r4b);
        const r4c = await stubbed(pool, extracted).run({ submissionId: id4b });
        check('…and Check again (direct) claims the failed row and succeeds', r4c.outcome === 'succeeded' && Number((await H.readSubmission(pool, id4b)).check_attempt) === 2, r4c);

        console.log('\nNo rows read');
        const k5 = H.newKey();
        await H.putPortalObject(k5, goodPdf, 'application/pdf');
        const id5 = await H.insertSubmission({ ...base, s3_key: k5 });
        created.push(id5);
        const r5 = await stubbed(pool, { ...extracted, lines: [] }).run({ submissionId: id5 });
        row = await H.readSubmission(pool, id5);
        check('failed NO_ROWS with the (empty) extraction and the group kept', r5.code === 'NO_ROWS' && row.error_code === 'NO_ROWS' && json(row.extract_json)?.lines?.length === 0
            && Number(row.row_count) === 0 && row.model_used === 'stub' && sameGroup(row), { r5, row_count: row.row_count, model: row.model_used, group: row.group_supplier_ids });
        check('audited check_failed', (await auditActions(id5)).includes('check_failed'));

        console.log('\nSupplier gone: no id of the group has a live suppliers row');
        const k6 = H.newKey();
        await H.putPortalObject(k6, goodPdf, 'application/pdf');
        const id6 = await H.insertSubmission({ ...base, supplier_id: 999999999, group_supplier_ids: [999999999, 999999998], s3_key: k6 });
        created.push(id6);
        let geminiCalls = 0;
        const r6 = await createPlChecker({ pool, readPackingList: async () => { geminiCalls += 1; throw new Error('must not be called'); } }).run({ submissionId: id6 });
        row = await H.readSubmission(pool, id6);
        check('failed SUPPLIER_NOT_FOUND before any model call', r6.code === 'SUPPLIER_NOT_FOUND' && row.error_code === 'SUPPLIER_NOT_FOUND' && geminiCalls === 0, { r6, geminiCalls });

        console.log('\nA legacy row with no group_supplier_ids is the group of its own supplier_id');
        const ownOnly = { supplierIds: [supplierId], names: [supplierName] };
        const ownRows = await H.withConn(pool, conn => P.loadGroupLines(conn, container, ownOnly));
        if (!ownRows.rows.length) skip('legacy row', `supplier ${supplierId} (${supplierName}) has no line of its own name on ${ref}`);
        else {
            const k6b = H.newKey();
            await H.putPortalObject(k6b, goodPdf, 'application/pdf');
            const id6b = await H.insertSubmission({ supplier_id: supplierId, supplier_name: supplierName, s3_key: k6b });
            created.push(id6b);
            const r6b = await stubbed(pool, H.extractedFrom(ownRows.rows, { invoiceNumber: `LEGACY-${Date.now()}`, containerNumbers: [ref] })).run({ submissionId: id6b });
            row = await H.readSubmission(pool, id6b);
            const other = new Set(rows.filter(r => !ownRows.rows.some(o => Number(o.id) === Number(r.id))).map(r => P.baseJf(r.jf_code)).filter(c => !ownRows.rows.some(o => P.baseJf(o.jf_code) === c)));
            check('check → succeeded, the column stays NULL (shipping never writes it), the suggestion is the container', r6b.outcome === 'succeeded' && row.group_supplier_ids === null
                && row.suggested_container_number === ref, { r6b, group: row.group_supplier_ids, suggested: row.suggested_container_number, confidence: row.match_confidence });
            check('…and its result holds only the lines under its own name', ![...other].some(c => (json(row.result_json)?.lines || []).some(l => P.baseJf(l.jfCode) === c)),
                { other: [...other], lines: (json(row.result_json)?.lines || []).map(l => l.jfCode) });
        }

        console.log('\nduplicateOf: the same bytes already submitted for the supplier');
        const k7 = H.newKey();
        const etag7 = await H.putPortalObject(k7, goodPdf, 'application/pdf');
        const id7 = await H.insertSubmission({
            ...base, s3_key: k7, s3_etag: etag7, file_size: goodPdf.length, check_status: 'succeeded', check_attempt: 1,
            status: 'submitted', submitted_at: new Date().toISOString().slice(0, 19).replace('T', ' '),
        });
        created.push(id7);
        const k8 = H.newKey();
        await H.putPortalObject(k8, goodPdf, 'application/pdf');
        const id8 = await H.insertSubmission({ ...base, s3_key: k8 });
        created.push(id8);
        const r8 = await stubbed(pool, extracted).run({ submissionId: id8 });
        row = await H.readSubmission(pool, id8);
        check('the checker sets result_json.duplicateOf', r8.outcome === 'succeeded' && json(row.result_json)?.duplicateOf === id7, { r8, duplicateOf: json(row.result_json)?.duplicateOf });

        console.log('\nThe printed box number of another shipment is a conflict');
        const boxes = (await pool.query(
            `SELECT DISTINCT UPPER(TRIM(external_container_number)) AS box FROM orders
              WHERE deleted_at IS NULL AND container_number <> ? AND external_container_number IS NOT NULL AND TRIM(external_container_number) <> ''
              LIMIT 50`, [ref]
        ))[0].map(b => b.box).filter(iso6346Valid);
        if (!boxes.length) skip('foreign box → unsure (conflict)', 'no other booked container with a valid box number');
        else {
            const k9 = H.newKey();
            await H.putPortalObject(k9, goodPdf, 'application/pdf');
            const id9 = await H.insertSubmission({ ...base, s3_key: k9 });
            created.push(id9);
            const r9 = await stubbed(pool, { ...extracted, containerNumbers: [boxes[0]] }).run({ submissionId: id9 });
            row = await H.readSubmission(pool, id9);
            const c9 = json(row.candidates_json) || {};
            check('foreign box → unsure (conflict), foreign_box in the stored top-level conflicts', r9.outcome === 'succeeded' && row.match_confidence === 'unsure' && row.match_reason === 'conflict'
                && row.result_state === 'unmatched' && (c9.conflicts || []).some(c => c.code === 'foreign_box'), { r9, confidence: row.match_confidence, reason: row.match_reason, conflicts: c9.conflicts });
        }

        console.log('\nLoader equivalence: the candidate’s lines are loadGroupLines’ (by the group’s names)');
        await H.withConn(pool, async (conn) => {
            const loaded = await P.loadSupplierCandidates(conn, group, { productCodes: [], poNumbers: [], printedNumbers: [] });
            const mine = loaded.candidates.find(c => c.key === `booked:${ref}`);
            const expected = await P.loadGroupLines(conn, container, group);
            const ids = rows => (rows || []).map(r => Number(r.id)).sort((a, b) => a - b);
            check('loadSupplierCandidates lines == loadGroupLines rows', mine && JSON.stringify(ids(mine.lines)) === JSON.stringify(ids(expected.rows)), { candidate: ids(mine?.lines).length, expected: ids(expected.rows).length, keys: loaded.candidates.map(c => c.key).slice(0, 10) });
            check('…every line is under one of the group’s names, and nothing of the group is left out of the container’s lines', expected.rows.every(r => P.lineInGroup(group, r))
                && (await P.loadContainerLines(conn, container)).rows.filter(r => P.lineInGroup(group, r)).length === expected.rows.length);
            const pg = await PL.portalGroup(conn, { supplier_id: supplierId, group_supplier_ids: JSON.stringify(group.supplierIds) });
            check('portalGroup resolves the portal’s ids to their live names (no portal_code)', JSON.stringify(pg.supplierIds) === JSON.stringify(group.supplierIds)
                && JSON.stringify([...pg.names].sort()) === JSON.stringify([...group.names].sort()), pg);
        });

        console.log('\nIsolation at check time: a container shared by two companies, A’s group excluding B');
        const shared = await H.pickContainer(pool, { minSuppliers: 2 });
        if (!shared) skip('isolation', 'no booked container with live lines of two registered suppliers');
        else {
            const other = shared.others[0];
            check('(setup) B is outside A’s group', !other.names.some(n => P.lineInGroup(shared.group, { supplier: n })), { group: shared.group.names, other: other.names });
            // B's lines and filing by the STAFF grouping: a staff upload for B.
            const otherRows = await H.withConn(pool, conn => P.loadExpectedLines(conn, shared.container, other.supplierKey));
            const otherExtracted = H.extractedFrom(otherRows.rows, { invoiceNumber: `OTHER-${Date.now()}`, supplierName: other.supplierName });
            const staffId = await H.insertStaffPackingList(pool, { container: shared.container, supplierKey: other.supplierKey, supplierName: other.supplierName, extracted: otherExtracted, filename: `other-${Date.now()}.pdf` });
            staffPls.push(staffId);
            const mineExtracted = H.extractedFrom(shared.rows, { invoiceNumber: `MINE-${Date.now()}`, containerNumbers: [shared.ref] });
            const k10 = H.newKey();
            await H.putPortalObject(k10, goodPdf, 'application/pdf');
            const id10 = await H.insertSubmission({ supplier_id: shared.supplierId, supplier_name: shared.supplierName, group_supplier_ids: shared.group.supplierIds, s3_key: k10 });
            created.push(id10);
            const r10 = await stubbed(pool, mineExtracted).run({ submissionId: id10 });
            row = await H.readSubmission(pool, id10);
            const res10 = json(row.result_json) || {};
            const text = JSON.stringify(res10);
            const otherCodes = new Set(otherRows.rows.map(r => P.baseJf(r.jf_code)).filter(c => !shared.rows.some(m => P.baseJf(m.jf_code) === c)));
            check('check → succeeded for the shared container', r10.outcome === 'succeeded', r10);
            check('A’s result has none of B’s lines, invoice or document', ![...otherCodes].some(c => (res10.lines || []).some(l => P.baseJf(l.jfCode) === c))
                && !text.includes(otherExtracted.invoiceNumber) && !(res10.documents || []).some(d => d.filename && d.filename.startsWith('other-')), { lines: (res10.lines || []).map(l => l.jfCode), documents: res10.documents });
        }
    } catch (e) {
        failures++;
        console.error('ERROR', e);
    } finally {
        await H.cleanupSubmissions(pool, created, { packingListIds: staffPls }).catch(e => console.error('cleanup failed', e.message));
        await H.closeFixtures();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
