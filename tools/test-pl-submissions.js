#!/usr/bin/env node
'use strict';

// HTTP suite for /api/v1/pl-submissions and the supplier-result hooks around
// it. Drives the real Express app in-process against the TEST database and
// the portal's TEST bucket, like tools/test-pi-submissions.js.
//
//   node tools/test-pl-submissions.js
//
// Locally every request is local@dev; the role comes from LOCAL_USER_TYPE on
// each request. Submissions are inserted as jportal would leave them
// (submitted, with a check written as plCheck writes it, the extraction built
// from the container's own lines; group_supplier_ids = the supplier ids whose
// names are on the container's lines, supplier_id the lowest, supplier_name
// its live name — pickContainer) through the fixture login
// (tools/pl-test-helpers.js). No Gemini call is made: GEMINI_API_KEY is
// cleared, so an import that reaches for the model fails NOT_CONFIGURED —
// which is how the suite tells "reused the stored extraction" from "read the
// file again". Everything it creates is removed again.

process.env.NODE_ENV = 'development';
process.env.LOCAL_USER_TYPE = 'standard';
process.env.PI_UPLOADS_BUCKET = process.env.PI_UPLOADS_BUCKET || 'jportal-pi-uploads-test';
process.env.GEMINI_API_KEY = '';

const http = require('http');
const H = require('./pl-test-helpers');
const { getPool, closePool } = require('../src/db');
const { app } = require('../src/handlers/orders');
const P = require('../src/services/packing-list-check');
const PL = require('../src/services/pl-results');
const { jportalSchema } = require('../src/lib/jportal');

const STAMP = Date.now();
let base;
let failures = 0;
let passes = 0;
const check = (label, ok, detail) => {
    if (ok) passes++; else failures++;
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${ok ? '' : `  -> ${JSON.stringify(detail ?? null).slice(0, 700)}`}`);
    return ok;
};
const skip = (label, why) => console.log(`SKIP  ${label}  (${why})`);
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
const json = v => (typeof v === 'string' ? JSON.parse(v) : v);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const nowSql = () => new Date().toISOString().slice(0, 19).replace('T', ' ');

(async () => {
    const env = H.refuseUnlessTest();
    const pool = getPool();
    const server = http.createServer(app).listen(0);
    await new Promise(r => server.once('listening', r));
    base = `http://127.0.0.1:${server.address().port}`;
    console.log(`TEST database ${env.host}/${env.db} · bucket ${env.bucket} · run ${STAMP}`);

    const created = [];
    const staffPls = [];
    const scratchOrders = [];
    const scratchDrafts = [];
    try {
        const picked = await H.pickContainer(pool);
        if (!picked) throw new Error('No booked container with live lines of a registered supplier on the test database.');
        const { container, ref, supplierId, supplierName, portalKey, group, staffKey, rows } = picked;
        console.log(`Using container ${ref}, supplier ${supplierId} (${supplierName}), group ${JSON.stringify(group.supplierIds)} = ${JSON.stringify(group.names)} (staff key ${staffKey}), ${rows.length} line(s)`);
        const pdf = await H.makePdf(['PACKING LIST', `Container ${ref}`, `run ${STAMP}`]);
        const extractedOf = (over = {}) => H.extractedFrom(rows, { invoiceNumber: `PLT-${STAMP}-${Math.random().toString(36).slice(2, 6)}`, containerNumbers: [ref], ...over });
        // What the portal writes at create: the granted ids, the lowest as supplier_id, its live name.
        const groupIds = group.supplierIds;
        const asPortal = { supplier_id: supplierId, supplier_name: supplierName, group_supplier_ids: groupIds };
        const sameGroup = row => JSON.stringify((json(row.group_supplier_ids) || []).map(Number)) === JSON.stringify(groupIds);

        // A submission as plCheck + the portal leave it: submitted, check
        // succeeded on `extracted`, a confident suggestion of `ref`, the
        // object in the bucket, ETag pinned. `status`/`check` override.
        const submit = async ({ extracted = extractedOf(), status = 'submitted', checkStatus = 'succeeded', errorCode = null, confidence = 'confident',
            replaces = null, withObject = true, supplier = asPortal, acknowledged = 1 } = {}) => {
            const key = H.newKey();
            // No object but a pinned ETag: the row looks checked, so Import reaches S3 and finds the file gone.
            const etag = withObject ? await H.putPortalObject(key, pdf, 'application/pdf') : '"0123456789abcdef0123456789abcdef"';
            const succeeded = checkStatus === 'succeeded';
            const id = await H.insertSubmission({
                ...supplier, s3_key: key, s3_etag: etag, file_size: pdf.length, filename: `PL ${STAMP}.pdf`,
                check_status: checkStatus, check_attempt: 1, error_code: errorCode, checked_at: nowSql(), model_used: 'fixture',
                extract_json: extracted, row_count: extracted.lines.length, invoice_number: extracted.invoiceNumber,
                candidates_json: succeeded ? { top: [{ key: `booked:${ref}`, kind: 'booked', label: ref, containerNumber: ref, draftContainerId: null, boxNumbers: [], coverage: 1, exact: 1, recall: 1, lineHit: 1, poShare: 1, boxHit: false, refHit: true, replacesHit: false, nearBox: [], conflicts: [], score: 1.1 }], conflicts: [], foreignBox: [] } : null,
                match_confidence: succeeded ? confidence : null, match_reason: succeeded ? (confidence === 'confident' ? 'lines' : null) : null,
                suggested_kind: succeeded && confidence === 'confident' ? 'booked' : null, suggested_container_number: succeeded && confidence === 'confident' ? ref : null,
                suggested_label: succeeded && confidence === 'confident' ? ref : null,
                verdict: succeeded ? 'match' : null, discrepancy_count: succeeded ? 0 : null,
                result_json: succeeded ? { v: 1, state: confidence === 'confident' ? 'provisional' : 'unmatched', container: confidence === 'confident' ? { kind: 'booked', label: ref, boxNumber: null } : null, extraction: { rows: extracted.lines.length }, lines: [] } : null,
                result_state: succeeded ? (confidence === 'confident' ? 'provisional' : 'unmatched') : null, result_at: succeeded ? nowSql() : null,
                status, supplier_note: status === 'submitted' ? 'Please see attached' : null, warnings_acknowledged: acknowledged,
                submitted_at: status === 'submitted' ? nowSql() : null, replaces_submission_id: replaces, row_version: 3,
            });
            created.push(id);
            return id;
        };
        const detail = async id => (await call('GET', `/api/v1/pl-submissions/${id}`)).body;
        const decide = async (id, path, body) => {
            const d = await detail(id);
            return call('POST', `/api/v1/pl-submissions/${id}/${path}`, { rowVersion: d.submission.rowVersion, ...body });
        };
        const auditActions = async (type, id) => (await sql(`SELECT action FROM audit_log WHERE entity_type = ? AND entity_id = ? ORDER BY id`, [type, id])).map(r => r.action);
        const plRow = async id => (await sql(`SELECT * FROM packing_lists WHERE id = ?`, [id]))[0] || null;

        console.log('\nGrants: one UPDATE per column group under the shipping login');
        const g0 = await submit();
        const grants = await H.grantsProbe(pool, g0);
        for (const [group, ok] of Object.entries(grants)) check(`UPDATE on the ${group} group`, ok === true, ok);

        console.log('\nRoles');
        as('readonly');
        for (const [path, body] of [['import', { rowVersion: 3, containerNumber: ref }], ['reject', { rowVersion: 3, note: 'x' }]]) {
            const r = await call('POST', `/api/v1/pl-submissions/${g0}/${path}`, body);
            check(`readonly → ${path} 403 CANNOT_DECIDE`, r.status === 403 && r.body.code === 'CANNOT_DECIDE', r);
        }
        const ro = await call('GET', `/api/v1/pl-submissions/${g0}`);
        check('readonly can still look', ro.status === 200 && ro.body.submission?.id === g0, ro);
        as('standard');

        console.log('\nList, suppliers and detail');
        const pending = await call('GET', `/api/v1/pl-submissions?status=pending&supplierKey=${encodeURIComponent(portalKey)}`);
        check('list pending by supplierKey (s:<supplier_id>) → { data, hasMore } with the row, flat suggested_* fields, blockers []', pending.status === 200 && Array.isArray(pending.body.data) && typeof pending.body.hasMore === 'boolean'
            && pending.body.data.some(s => s.id === g0 && s.supplierKey === portalKey && s.supplierId === supplierId && s.suggestedLabel === ref && s.suggestedKind === 'booked' && Array.isArray(s.blockers) && !('extraction' in s)), pending.body);
        const history = await call('GET', `/api/v1/pl-submissions?status=history&supplierId=${supplierId}`);
        check('list history excludes a pending row', history.status === 200 && !history.body.data.some(s => s.id === g0), history.body);
        const otherKey = await call('GET', `/api/v1/pl-submissions?status=pending&supplierKey=s:999999999`);
        check('another supplier id lists nothing of ours', otherKey.status === 200 && !otherKey.body.data.some(s => s.id === g0), otherKey.body);
        const badKey = await call('GET', '/api/v1/pl-submissions?supplierKey=bogus');
        check('a bad supplierKey → 400 BAD_FIELD', badKey.status === 400 && badKey.body.code === 'BAD_FIELD', badKey);
        const sup = await call('GET', '/api/v1/pl-submissions/suppliers');
        const mine = sup.body.data?.find(s => s.supplierKey === portalKey);
        check('suppliers → { supplierKey, supplierId, supplierName, supplierIds, pending, total, oldestSubmittedAt }, keyed by supplier_id', sup.status === 200 && mine && mine.supplierId === supplierId
            && mine.pending >= 1 && mine.total >= mine.pending && typeof mine.supplierName === 'string' && 'oldestSubmittedAt' in mine
            && JSON.stringify(mine.supplierIds) === JSON.stringify(groupIds), sup.body);
        const d0 = await detail(g0);
        check('detail → submission, extraction, acknowledged, candidates { stored, live }, fileUrl, canImport', d0.submission?.id === g0 && d0.extraction?.lines?.length === rows.length
            && d0.acknowledged?.sawNoResult === false && d0.acknowledged.suggestedLabel === ref && d0.candidates?.stored?.length === 1 && d0.candidates.stored[0].container?.label === ref
            && Array.isArray(d0.candidates.live) && Array.isArray(d0.candidates.conflicts) && typeof d0.fileUrl === 'string' && d0.canImport === true && d0.packingList === null, d0);
        check('live candidates carry the container, the ratios and the hits', d0.candidates.live.some(c => c.container?.kind === 'booked' && c.container.containerNumber === ref && typeof c.coverage === 'number' && typeof c.boxHit === 'boolean' && Array.isArray(c.conflicts)), d0.candidates.live);
        const unack = await submit({ acknowledged: 0, extracted: extractedOf() });
        await H.fixtureUpdate(`discrepancy_count = 2, verdict = 'differences'`, 'id = ?', [unack]);
        const dUnack = await detail(unack);
        check('a confident row with differences sent unacknowledged → acknowledged { sawNoResult: true }', dUnack.acknowledged?.sawNoResult === true, dUnack.acknowledged);

        console.log('\nPreview');
        const pv = await call('GET', `/api/v1/pl-submissions/${g0}/preview?containerNumber=${encodeURIComponent(ref)}`);
        check('preview → { container, comparison, packingLists, duplicateInvoices, onBoard }', pv.status === 200 && pv.body.container?.kind === 'booked' && pv.body.comparison?.summary && Array.isArray(pv.body.packingLists)
            && Array.isArray(pv.body.duplicateInvoices) && pv.body.onBoard === true, pv.body);
        check('…the comparison is the staff one (lineKey, matchMethod, elsewhere)', pv.body.comparison?.lines?.every(l => 'lineKey' in l && 'matchMethod' in l), pv.body.comparison?.lines?.[0]);
        const nf = await call('GET', `/api/v1/pl-submissions/${g0}/preview?containerNumber=NOPE-${STAMP}`);
        check('an unknown container → 404 CONTAINER_NOT_FOUND', nf.status === 404 && nf.body.code === 'CONTAINER_NOT_FOUND', nf);
        const noArg = await call('GET', `/api/v1/pl-submissions/${g0}/preview`);
        check('no container → 400 BAD_FIELD', noArg.status === 400 && noArg.body.code === 'BAD_FIELD', noArg);
        const ambiguous = await sql(
            `SELECT dca.draft_container_name AS name FROM draft_container_allocations dca
               JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
               LEFT JOIN draft_containers dc ON dc.name = dca.draft_container_name
              WHERE (dc.id IS NULL OR dc.closed_reason IS NULL) AND (o.container_number IS NULL OR TRIM(o.container_number) = '')
              GROUP BY dca.draft_container_name`
        );
        const hints = new Map();
        for (const d of ambiguous) {
            const m = /-\s*(\d{2,4})\b/.exec(d.name);
            if (m) hints.set(m[1], (hints.get(m[1]) || 0) + 1);
        }
        const dupHint = [...hints.entries()].find(([, n]) => n >= 2)?.[0];
        if (!dupHint) skip('AMBIGUOUS_CONTAINER', 'no two open drafts reserve the same number');
        else {
            const amb = await call('GET', `/api/v1/pl-submissions/${g0}/preview?containerNumber=${dupHint}`);
            check('two drafts reserving the number → 409 AMBIGUOUS_CONTAINER { candidates }', (amb.status === 409 && amb.body.code === 'AMBIGUOUS_CONTAINER' && Array.isArray(amb.body.candidates))
                || amb.status === 200 /* a booked container wins over the drafts */, amb);
        }
        const converted = (await sql(`SELECT id, name, container_number FROM draft_containers WHERE closed_reason = 'converted' AND container_number IS NOT NULL ORDER BY id DESC LIMIT 1`))[0];
        if (!converted) skip('a suggestion for a draft since booked', 'no converted draft on the test database');
        else {
            const fol = await call('GET', `/api/v1/pl-submissions/${g0}/preview?draftContainerId=${converted.id}`);
            check('a draft since booked previews against the booked container (followedFrom)', (fol.status === 200 && fol.body.container?.kind === 'booked' && fol.body.container.followedFrom)
                || (fol.status === 404 && fol.body.code === 'CONTAINER_NOT_FOUND') /* the booked container has no live orders any more */, fol);
        }

        console.log('\nImport');
        const stale = await call('POST', `/api/v1/pl-submissions/${g0}/import`, { rowVersion: 99, containerNumber: ref });
        check('a stale rowVersion → 409 STALE', stale.status === 409 && stale.body.code === 'STALE', stale);
        const noContainer = await decide(g0, 'import', {});
        check('no container → 400 BAD_FIELD', noContainer.status === 400 && noContainer.body.code === 'BAD_FIELD', noContainer);
        const imp = await decide(g0, 'import', { containerNumber: ref, note: 'Thanks' });
        check('import → 200 { submission, packingList }', imp.status === 200 && imp.body.submission?.status === 'imported' && imp.body.submission.confirmedLabel === ref
            && imp.body.packingList?.source === 'portal' && imp.body.packingList.portalSubmissionId === g0, imp);
        const pl0 = imp.body.packingList?.id;
        const plRow0 = await plRow(pl0);
        check('the packing list reused the stored extraction: succeeded, no Gemini call', plRow0?.status === 'succeeded' && plRow0.model_used === 'fixture' && Number(plRow0.row_count) === rows.length, plRow0 && { status: plRow0.status, model: plRow0.model_used, error: plRow0.error_message });
        check('…filed under s:<supplier_id> (the lowest granted id), with the row’s supplier_name', plRow0?.supplier_key === portalKey && plRow0.supplier_name === supplierName,
            plRow0 && { supplier_key: plRow0.supplier_key, portalKey, supplier_name: plRow0.supplier_name, supplierName });
        // The staff check groups by portal_code; it sees the group's lines only
        // where its key for the company is the same 's:<id>' (accepted for now).
        if (staffKey !== portalKey) skip('the staff check on the filing', `staff key ${staffKey} differs from ${portalKey}: the staff check files it under another entry`);
        else check('…and the staff check (same key here) sees every line of the group: no discrepancy on a document built from them', Number(plRow0?.discrepancy_count ?? -1) === 0 && plRow0?.verdict === 'match',
            plRow0 && { verdict: plRow0.verdict, count: plRow0.discrepancy_count });
        let row0 = await H.readSubmission(pool, g0);
        check('the row: imported, packing_list_id, confirmed_*, group_supplier_ids untouched, result confirmed', row0.status === 'imported' && Number(row0.packing_list_id) === pl0 && row0.confirmed_kind === 'booked'
            && row0.confirmed_container_number === ref && sameGroup(row0) && row0.result_state === 'confirmed' && json(row0.result_json)?.state === 'confirmed', row0);
        const res0 = json(row0.result_json);
        check('…the supplier’s result expects exactly the group’s units', Number(res0?.summary?.expectedUnits) === rows.reduce((s, r) => s + Number(r.quantity), 0), { summary: res0?.summary, groupUnits: rows.reduce((s, r) => s + Number(r.quantity), 0) });
        check('result_json contains only allowed keys', H.resultLeaks(json(row0.result_json)).length === 0, H.resultLeaks(json(row0.result_json)));
        check('audited import (pl_submission) and create (packing_list)', (await auditActions('pl_submission', g0)).includes('import') && (await auditActions('packing_list', pl0)).includes('create'));
        const dImp = await detail(g0);
        check('detail of an imported row → packingList, fileUrl from the PO bucket copy', dImp.packingList?.id === pl0 && typeof dImp.fileUrl === 'string' && dImp.fileUrl.includes(plRow0.s3_key.split('/')[1]), { packingList: dImp.packingList?.id, fileUrl: dImp.fileUrl });
        const again = await decide(g0, 'import', { containerNumber: ref });
        check('importing twice → 409 NOT_SUBMITTED with the packing list', again.status === 409 && again.body.code === 'NOT_SUBMITTED' && again.body.packingListId === pl0, again);
        const listed = await call('GET', `/api/v1/packing-lists?containerNumber=${encodeURIComponent(ref)}&supplierKey=${encodeURIComponent(portalKey)}`);
        check('GET /packing-lists shows it with source portal', listed.status === 200 && listed.body.data.some(p => p.id === pl0 && p.source === 'portal' && p.portalSubmissionId === g0), listed.body?.data?.slice(0, 3));

        console.log('\nTwo imports at once');
        const race = await submit();
        const dr = await detail(race);
        const [a, b] = await Promise.all([
            call('POST', `/api/v1/pl-submissions/${race}/import`, { rowVersion: dr.submission.rowVersion, containerNumber: ref }),
            call('POST', `/api/v1/pl-submissions/${race}/import`, { rowVersion: dr.submission.rowVersion, containerNumber: ref }),
        ]);
        const statuses = [a.status, b.status].sort();
        check('one 200, one 409', statuses[0] === 200 && statuses[1] === 409, { a, b });
        const plCount = await sql(`SELECT COUNT(*) AS n FROM packing_lists WHERE portal_submission_id = ?`, [race]);
        check('…and one packing list', Number(plCount[0].n) === 1, plCount);

        console.log('\nSUPPLIER_NOT_ON_CONTAINER and force');
        // The first live booked container holding no line of the group's.
        // (liveContainers before withConn: the pool has ONE connection.)
        const liveRefs = await H.liveContainers(pool, { limit: 40 });
        const foreign = await H.withConn(pool, async (conn) => {
            for (const c of liveRefs) {
                if (c.ref === ref) continue;
                const r = await P.resolveContainer(conn, { containerNumber: c.ref });
                if (!r || r.ambiguous || r.container.kind !== 'booked') continue;
                if (!(await P.loadGroupLines(conn, r.container, group)).rows.length) return { ref: c.ref, container: r.container };
            }
            return null;
        });
        if (!foreign) skip('SUPPLIER_NOT_ON_CONTAINER', 'no other booked container without a line of this group');
        else {
            const f1 = await submit();
            const refused = await decide(f1, 'import', { containerNumber: foreign.ref });
            check('a container without the supplier → 422 SUPPLIER_NOT_ON_CONTAINER { suppliers }', refused.status === 422 && refused.body.code === 'SUPPLIER_NOT_ON_CONTAINER' && Array.isArray(refused.body.suppliers)
                && refused.body.suppliers.every(s => typeof s.supplierKey === 'string' && typeof s.supplierName === 'string' && Array.isArray(s.names) && typeof s.units === 'number'), refused);
            const pvf = await call('GET', `/api/v1/pl-submissions/${f1}/preview?containerNumber=${encodeURIComponent(foreign.ref)}`);
            check('…its preview says onBoard false', pvf.status === 200 && pvf.body.onBoard === false, pvf.body?.onBoard);
            const forced = await decide(f1, 'import', { containerNumber: foreign.ref, force: true });
            const rowF = await H.readSubmission(pool, f1);
            const resF = json(rowF.result_json);
            check('force → 200; the supplier’s result is confirmed with container null and no lines', forced.status === 200 && rowF.status === 'imported' && resF?.state === 'confirmed' && resF.container === null && !resF.lines, { status: forced.status, result: resF });
            const plF = await plRow(forced.body.packingList?.id);
            check('…filed under s:<supplier_id> all the same', plF && plF.supplier_key === portalKey && plF.supplier_name === supplierName, plF && { supplier_key: plF.supplier_key, supplier_name: plF.supplier_name });
            check('…audited with force', (await sql(`SELECT after_json FROM audit_log WHERE entity_type = 'pl_submission' AND entity_id = ? AND action = 'import'`, [f1]))
                .some(r => json(r.after_json)?.force === true));
        }

        console.log('\nA corrected version: replacesId, then delete and re-import');
        const v2 = await submit({ replaces: g0, extracted: extractedOf({ vary: l => ({ ...l, quantity: l.quantity + 1 }) }) });
        const d2 = await detail(v2);
        check('detail pre-selects the replaced filing: defaultReplacesId and its container', d2.defaultReplacesId === pl0 && d2.defaultContainer?.kind === 'booked' && d2.defaultContainer.containerNumber === ref, { defaultReplacesId: d2.defaultReplacesId, defaultContainer: d2.defaultContainer });
        const pvr = await call('GET', `/api/v1/pl-submissions/${v2}/preview?containerNumber=${encodeURIComponent(ref)}&replacesId=${pl0}`);
        check('preview with replacesId pools without the replaced document', pvr.status === 200 && !(pvr.body.comparison?.lines || []).some(l => (l.packed?.rows || []).some(r => r.packingListId === pl0)), pvr.body?.comparison?.lines?.[0]);
        const badReplace = await decide(v2, 'import', { containerNumber: ref, replacesId: 999999999 });
        check('an unknown replacesId → 404 REPLACES_NOT_FOUND (rolled back: still submitted)', badReplace.status === 404 && badReplace.body.code === 'REPLACES_NOT_FOUND' && (await H.readSubmission(pool, v2)).status === 'submitted', badReplace);
        const imp2 = await decide(v2, 'import', { containerNumber: ref, replacesId: pl0 });
        const pl2 = imp2.body.packingList?.id;
        check('import v2 with replacesId → 200, version 2', imp2.status === 200 && imp2.body.packingList?.replacesId === pl0 && imp2.body.packingList.version === 2, imp2);
        row0 = await H.readSubmission(pool, g0);
        check('v1’s submission is replaced immediately', row0.result_state === 'replaced' && json(row0.result_json)?.state === 'replaced', { state: row0.result_state });
        const row2 = await H.readSubmission(pool, v2);
        check('v2 is confirmed', row2.status === 'imported' && row2.result_state === 'confirmed', { status: row2.status, state: row2.result_state });

        const del = await call('DELETE', `/api/v1/packing-lists/${pl2}`);
        check('DELETE v2’s packing list → 204', del.status === 204, del);
        const row2b = await H.readSubmission(pool, v2);
        check('v2 returns to Pending: submitted, in_review, decision cleared, packing_list_id kept', row2b.status === 'submitted' && row2b.result_state === 'in_review' && row2b.decided_by === null && row2b.confirmed_label === null
            && Number(row2b.packing_list_id) === pl2 && Number(row2b.row_version) > Number(row2.row_version), row2b);
        check('…audited returned_to_pending', (await auditActions('pl_submission', v2)).includes('returned_to_pending'));
        row0 = await H.readSubmission(pool, g0);
        check('v1 is confirmed again', row0.result_state === 'confirmed', { state: row0.result_state });
        const d2b = await detail(v2);
        check('detail → previousPackingList (deleted), no packingList, blockers []', d2b.previousPackingList?.id === pl2 && d2b.previousPackingList.deleted === true && d2b.packingList === null && d2b.canImport === true, d2b);
        const imp2b = await decide(v2, 'import', { containerNumber: ref, replacesId: pl0 });
        check('re-import after delete → 200 (the unique key is on the LIVE filing)', imp2b.status === 200 && imp2b.body.packingList?.id !== pl2 && imp2b.body.submission?.status === 'imported', imp2b);
        check('…no refiled audit (the previous filing was already deleted)', !(await auditActions('packing_list', pl2)).includes('refiled'));

        console.log('\nReject');
        const rj = await submit();
        const noReason = await decide(rj, 'reject', {});
        check('reject without a note → 400 BAD_FIELD', noReason.status === 400 && noReason.body.code === 'BAD_FIELD', noReason);
        const rej = await decide(rj, 'reject', { note: 'Lot numbers do not match the PO; please reissue.' });
        check('reject → 200 { submission } rejected with the note', rej.status === 200 && rej.body.submission?.status === 'rejected' && rej.body.submission.decidedNote === 'Lot numbers do not match the PO; please reissue.', rej);
        check('…audited reject', (await auditActions('pl_submission', rj)).includes('reject'));
        const dRej = await detail(rj);
        check('…listed in history, not importable', dRej.submission.status === 'rejected' && dRej.canImport === false, dRej.submission);

        console.log('\nBlockers');
        const gone = await submit({ withObject: false });
        const goneImp = await decide(gone, 'import', { containerNumber: ref });
        check('the file gone from the bucket → 422 NOT_IMPORTABLE file_gone', goneImp.status === 422 && goneImp.body.code === 'NOT_IMPORTABLE' && goneImp.body.blockers?.includes('file_gone'), goneImp);
        const noEtag = await submit();
        await H.fixtureUpdate('s3_etag = NULL', 'id = ?', [noEtag]);
        const noEtagImp = await decide(noEtag, 'import', { containerNumber: ref });
        check('no ETag → 422 NOT_IMPORTABLE no_etag', noEtagImp.status === 422 && noEtagImp.body.blockers?.includes('no_etag'), noEtagImp);
        const deadSupplier = await submit({ supplier: { supplier_id: 999999999, supplier_name: 'Nobody Ltd', group_supplier_ids: [999999999] } });
        const deadImp = await decide(deadSupplier, 'import', { containerNumber: ref });
        check('a group with no live supplier → 422 SUPPLIER_NOT_FOUND', deadImp.status === 422 && deadImp.body.code === 'SUPPLIER_NOT_FOUND', deadImp);
        const deadPv = await call('GET', `/api/v1/pl-submissions/${deadSupplier}/preview?containerNumber=${encodeURIComponent(ref)}`);
        check('…and its preview', deadPv.status === 422 && deadPv.body.code === 'SUPPLIER_NOT_FOUND', deadPv);

        console.log('\nA legacy row with no group_supplier_ids: the group of its own supplier_id, the column left NULL');
        const legacy = await submit({ supplier: { supplier_id: supplierId, supplier_name: supplierName } });
        const legacyImp = await decide(legacy, 'import', { containerNumber: ref, force: true });
        const rowL = await H.readSubmission(pool, legacy);
        check('import → 200, imported and confirmed, group_supplier_ids still NULL', legacyImp.status === 200 && rowL.status === 'imported' && rowL.result_state === 'confirmed' && rowL.group_supplier_ids === null,
            { status: legacyImp.status, code: legacyImp.body.code, group: rowL.group_supplier_ids, state: rowL.result_state });

        console.log('\nNO_ROWS: sendable, and Import falls back to a fresh read');
        const noRows = await submit({ checkStatus: 'failed', errorCode: 'NO_ROWS', extracted: { ...extractedOf(), lines: [] } });
        const dNo = await detail(noRows);
        check('detail → acknowledged null (the supplier saw the failure), canImport true', dNo.acknowledged === null && dNo.canImport === true && dNo.candidates.live === null, { acknowledged: dNo.acknowledged, canImport: dNo.canImport, liveError: dNo.candidates?.liveError });
        const pvNo = await call('GET', `/api/v1/pl-submissions/${noRows}/preview?containerNumber=${encodeURIComponent(ref)}`);
        check('preview without an extraction → 200, extractionUsable false', pvNo.status === 200 && pvNo.body.extractionUsable === false, pvNo.body);
        const impNo = await decide(noRows, 'import', { containerNumber: ref });
        check('import → 200, the packing list processing', impNo.status === 200 && impNo.body.packingList?.status === 'processing', { status: impNo.status, code: impNo.body.code, packingList: impNo.body.packingList });
        await sleep(4000);
        const plNo = await plRow(impNo.body.packingList?.id);
        check('…the fallback read ran (and failed NOT_CONFIGURED: no key in this suite)', plNo?.status === 'failed' && /NOT_CONFIGURED/.test(plNo.error_message || ''), plNo && { status: plNo.status, error: plNo.error_message });
        const rowNo = await H.readSubmission(pool, noRows);
        check('…group_supplier_ids is the portal’s, untouched by the import', sameGroup(rowNo) && rowNo.status === 'imported', rowNo.group_supplier_ids);

        console.log('\nRefresh guards (pl-results.js on a checked draft)');
        const draft = await submit({ status: 'draft', confidence: 'none', acknowledged: 0 });
        await H.fixtureUpdate(`suggested_kind = NULL, suggested_container_number = NULL, suggested_label = NULL, match_reason = NULL, result_state = 'unmatched'`, 'id = ?', [draft]);
        const before = await H.readSubmission(pool, draft);
        const rf1 = await PL.refreshSubmission(pool, draft);
        const afterRf = await H.readSubmission(pool, draft);
        check('a draft that gains a confident suggestion: written, changed, row_version bumped', rf1.written && rf1.changed && afterRf.match_confidence === 'confident' && afterRf.suggested_container_number === ref
            && afterRf.result_state === 'provisional' && Number(afterRf.row_version) === Number(before.row_version) + 1, { rf1, confidence: afterRf.match_confidence, rv: [before.row_version, afterRf.row_version] });
        const rf2 = await PL.refreshSubmission(pool, draft);
        check('the same again: written, unchanged, no bump', rf2.written && !rf2.changed && Number((await H.readSubmission(pool, draft)).row_version) === Number(afterRf.row_version), rf2);
        // The reverse order: a refresh that changes the row makes a submit
        // carrying the old rowVersion miss (what the portal answers STALE to).
        const portalSubmit = async (id, rowVersion) => H.fixtureUpdate(`status = 'submitted', submitted_at = UTC_TIMESTAMP(), row_version = row_version + 1`, 'id = ? AND status = \'draft\' AND row_version = ?', [id, rowVersion]);
        check('a submit with the pre-refresh rowVersion → 0 rows (STALE)', (await portalSubmit(draft, Number(before.row_version))) === 0);
        // A submit that lands mid-refresh: the refresh read the row, then the
        // portal submitted; the guarded write must write 0 rows.
        const readMid = await H.withConn(pool, conn => PL.loadSubmission(conn, draft));
        const computedMid = await H.withConn(pool, conn => PL.computeSupplierResult(conn, readMid));
        await H.fixtureUpdate(`extract_json = JSON_SET(extract_json, '$.invoiceNumber', 'CHANGED-${STAMP}')`, 'id = ?', [draft]);
        check('…(setup) the portal submits in between', (await portalSubmit(draft, Number(readMid.row_version))) === 1);
        const wMid = await PL.writeSupplierResult(pool, readMid, { ...computedMid, result: { ...computedMid.result, extraction: { ...computedMid.result.extraction, rows: 999 } } });
        check('a submit mid-refresh → the refresh writes 0 rows', wMid.written === false, wMid);
        const rf3 = await PL.refreshSubmission(pool, draft);
        check('refresh on a submitted row is a no-op (frozen)', rf3.kind === 'noop' && rf3.reason === 'frozen', rf3);

        console.log('\nContainer gone: a scratch draft deleted under an imported packing list');
        const line = rows[0];
        const order = await call('POST', '/api/v1/orders', {
            status: 'READY', jfCode: line.jf_code, productName: line.product_name || line.jf_code, quantity: 60, supplier: supplierName,
            poNumber: `PLTEST-PO-${STAMP}`, lotNumber: `PLT${STAMP % 100000}`, unitsPerCarton: 10,
        });
        if (order.status !== 201) skip('container gone', `could not create a scratch order: ${order.status} ${JSON.stringify(order.body).slice(0, 200)}`);
        else {
            scratchOrders.push(order.body.id);
            const draftName = `DRAFT-SEA-990101-${String(STAMP % 1000000).padStart(6, '0')} - PLTEST ${STAMP}`;
            const alloc = await call('POST', '/api/v1/draft-containers', { orderId: order.body.id, draftContainerName: draftName, allocated: 60 });
            check('(setup) a scratch draft with the supplier’s line', alloc.status === 201, alloc);
            scratchDrafts.push(draftName);
            const draftRow = (await sql(`SELECT id FROM draft_containers WHERE name = ?`, [draftName]))[0];
            const scratchRows = await H.withConn(pool, async (conn) => {
                const r = await P.resolveContainer(conn, { draftContainerId: draftRow?.id });
                return r && !r.ambiguous ? (await P.loadGroupLines(conn, r.container, group)).rows : [];
            });
            check('(setup) the draft resolves with the line', scratchRows.length === 1, scratchRows.length);
            const sd = await submit({ extracted: H.extractedFrom(scratchRows, { invoiceNumber: `DRAFT-${STAMP}` }) });
            const impD = await decide(sd, 'import', { draftContainerId: draftRow?.id });
            check('import into the draft → 200, confirmed', impD.status === 200 && impD.body.packingList?.containerKind === 'draft' && (await H.readSubmission(pool, sd)).result_state === 'confirmed', impD);
            const plD = impD.body.packingList?.id;
            const close = await call('POST', '/api/v1/draft-containers/close', { name: draftName, reason: 'deleted' });
            check('close the draft (deleted) → 200', close.status === 200 && close.body.ok === true, close);
            const rowD = await H.readSubmission(pool, sd);
            check('the hook returns the submission to Pending (in_review), packing_list_id kept', rowD.status === 'submitted' && rowD.result_state === 'in_review' && Number(rowD.packing_list_id) === plD, rowD);
            const impD2 = await decide(sd, 'import', { containerNumber: ref, force: true });
            check('re-import into the booked container → 200', impD2.status === 200 && impD2.body.packingList?.id !== plD, impD2);
            check('…the orphaned packing list is retired (refiled, deleted_at set)', (await auditActions('packing_list', plD)).includes('refiled') && (await plRow(plD))?.deleted_at != null);
            const dDel = await call('DELETE', `/api/v1/orders/${order.body.id}`);
            check('(cleanup) the scratch order is soft-deleted', dDel.status === 200, dDel);
        }

        console.log('\nIsolation at import and refresh: a container shared by two companies, A’s group excluding B');
        const shared = await H.pickContainer(pool, { minSuppliers: 2 });
        if (!shared) skip('isolation', 'no booked container with live lines of two registered suppliers');
        else {
            const other = shared.others[0];
            check('(setup) B is outside A’s group', !other.names.some(n => P.lineInGroup(shared.group, { supplier: n })), { group: shared.group.names, other: other.names });
            // B's lines and filing by the STAFF grouping: a staff upload for B.
            const otherRows = await H.withConn(pool, conn => P.loadExpectedLines(conn, shared.container, other.supplierKey));
            const otherExtracted = H.extractedFrom(otherRows.rows, { invoiceNumber: `OTHER-${STAMP}`, supplierName: other.supplierName });
            staffPls.push(await H.insertStaffPackingList(pool, { container: shared.container, supplierKey: other.supplierKey, supplierName: other.supplierName, extracted: otherExtracted, filename: `other-${STAMP}.pdf` }));
            const mineExtracted = H.extractedFrom(shared.rows, { invoiceNumber: `MINE-${STAMP}`, containerNumbers: [shared.ref] });
            const sA = await submit({ extracted: mineExtracted, supplier: { supplier_id: shared.supplierId, supplier_name: shared.supplierName, group_supplier_ids: shared.group.supplierIds } });
            const pvA = await call('GET', `/api/v1/pl-submissions/${sA}/preview?containerNumber=${encodeURIComponent(shared.ref)}`);
            check('the staff preview pools only A’s documents, onBoard true', pvA.status === 200 && pvA.body.onBoard === true && !(pvA.body.packingLists || []).some(p => p.filename === `other-${STAMP}.pdf`), pvA.body?.packingLists);
            const otherCodesPv = new Set(otherRows.rows.map(r => P.baseJf(r.jf_code)).filter(c => !shared.rows.some(m => P.baseJf(m.jf_code) === c)));
            check('…and expects only A’s lines', !(pvA.body.comparison?.lines || []).some(l => l.status !== 'unexpected' && otherCodesPv.has(P.baseJf(l.jfCode))), (pvA.body.comparison?.lines || []).map(l => [l.jfCode, l.status]));
            const impA = await decide(sA, 'import', { containerNumber: shared.ref });
            check('import A → 200', impA.status === 200, impA);
            const otherCodes = new Set(otherRows.rows.map(r => P.baseJf(r.jf_code)).filter(c => !shared.rows.some(m => P.baseJf(m.jf_code) === c)));
            const leaks = (res) => {
                const text = JSON.stringify(res || {});
                return [...otherCodes].some(c => (res?.lines || []).some(l => P.baseJf(l.jfCode) === c)) || text.includes(`OTHER-${STAMP}`) || (res?.documents || []).some(d => (d.filename || '').startsWith('other-'));
            };
            const rowA = await H.readSubmission(pool, sA);
            check('A’s result at import has none of B’s lines, invoice or document', !leaks(json(rowA.result_json)), json(rowA.result_json));
            await PL.refreshSubmission(pool, sA);
            check('…and after a refresh', !leaks(json((await H.readSubmission(pool, sA)).result_json)));
        }
    } catch (e) {
        failures++;
        console.error('ERROR', e);
    } finally {
        for (const name of scratchDrafts) await call('POST', '/api/v1/draft-containers/close', { name, reason: 'deleted' }).catch(() => {});
        for (const id of scratchOrders) await sql(`UPDATE orders SET deleted_at = COALESCE(deleted_at, NOW()) WHERE id = ?`, [id]).catch(() => {});
        await H.cleanupSubmissions(getPool(), created, { packingListIds: staffPls }).catch(e => console.error('cleanup failed', e.message));
        await H.closeFixtures();
        server.close();
        await closePool();
        console.log(`\n${passes} passed, ${failures} failed`);
        process.exit(failures ? 1 : 0);
    }
})();
