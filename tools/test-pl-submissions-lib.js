'use strict';

// Unit tests for the supplier-portal packing-list submission rules
// (src/lib/pl-submissions.js) and the guarded result write
// (src/services/pl-results.js writeSupplierResult, against a fake
// connection). No database, no AWS, no Gemini.
//   node --test tools/test-pl-submissions-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/pl-submissions');
const PI = require('../src/lib/pi-submissions');
const PL = require('../src/services/pl-results');
const { compareDocuments, applySignOffs } = require('../src/services/packing-list-check');

const KEY = 'pl/0a1b2c3d-1111-4222-8333-444455556666.pdf';
const BUCKET = 'jportal-pi-uploads-test';
const submitted = (over = {}) => ({
    id: 11, status: 'submitted', s3_key: KEY, content_type: 'application/pdf', file_size: 2048, s3_etag: '"abc"',
    supplier_id: 3, supplier_name: 'Acme Ltd', filename: 'PL-324.pdf', submitted_by_email: 'sales@acme.example',
    row_version: 4, check_status: 'succeeded', check_attempt: 1, ...over,
});

// ── Shared with PI ─────────────────────────────────────────────────────────
test('re-exports PI’s claim rules, limits and helpers unchanged', () => {
    for (const k of ['planEvent', 'claimWhere', 'sniffType', 'safeFilename', 'canDecide', 'parseJson', 'positiveInt', 'parseRowVersion', 'parseNote']) {
        assert.equal(L[k], PI[k], k);
    }
    for (const k of ['STALE_CHECK_SECONDS', 'MAX_CHECK_ATTEMPTS', 'RETRYABLE_CODES', 'MAX_FILE_BYTES', 'ALLOWED_TYPES', 'VISIBLE_STATUSES', 'DECIDED_STATUSES', 'DECIDER_ROLES']) {
        assert.equal(L[k], PI[k], k);
    }
    assert.equal(L.canDecide('admin'), true);
    assert.equal(L.canDecide('readonly'), false);
});

// ── Keys and triggers ──────────────────────────────────────────────────────
test('PL_KEY_RE / destKey', () => {
    assert.equal(L.isPlKey(KEY), true);
    assert.equal(L.isPlKey('pi/0a1b2c3d-1111-4222-8333-444455556666.pdf'), false, 'a PI key is not a PL key');
    assert.equal(L.isPlKey('pl/0a1b2c3d-1111-4222-8333-444455556666.xlsx'), false);
    assert.equal(L.isPlKey('pl/../evil.pdf'), false);
    assert.equal(L.destKey({ submissionId: 11, filename: 'PL 324 (final).PDF', contentType: 'application/pdf', token: 'T' }), 'packing-lists/T/PL_324_final.pdf');
    assert.equal(L.destKey({ submissionId: 11, filename: '装箱单.pdf', contentType: 'application/pdf', token: 'T' }), 'packing-lists/T/pl-11.pdf');
    assert.equal(L.destKey({ submissionId: 11, filename: 'scan.jpeg', contentType: 'image/jpeg', token: 'T' }), 'packing-lists/T/scan.jpg');
});

test('parseTrigger: the bucket event', () => {
    const ev = (key, bucket = BUCKET) => ({ source: 'aws.s3', 'detail-type': 'Object Created', detail: { bucket: { name: bucket }, object: { key } } });
    assert.deepEqual(L.parseTrigger(ev(KEY), { bucket: BUCKET }), { source: 'event', key: KEY });
    assert.deepEqual(L.parseTrigger(ev(KEY.replace('/', '%2F')), { bucket: BUCKET }), { source: 'event', key: KEY }, 'URL-encoded keys are decoded');
    assert.deepEqual(L.parseTrigger(ev(KEY, 'other-bucket'), { bucket: BUCKET }), { ignore: 'other_bucket' });
    assert.deepEqual(L.parseTrigger(ev('pi/0a1b2c3d-1111-4222-8333-444455556666.pdf'), { bucket: BUCKET }), { ignore: 'not_a_pl_key' }, 'PI uploads are not ours');
    assert.deepEqual(L.parseTrigger(ev('pl/notes.txt'), { bucket: BUCKET }), { ignore: 'not_a_pl_key' });
    assert.deepEqual(L.parseTrigger(ev(KEY)), { source: 'event', key: KEY }, 'no bucket configured: any bucket');
});

test('parseTrigger: direct invokes carry an action', () => {
    assert.deepEqual(L.parseTrigger({ submissionId: 7 }), { source: 'direct', submissionId: 7, action: 'check' });
    assert.deepEqual(L.parseTrigger({ submissionId: '7', action: 'check' }), { source: 'direct', submissionId: 7, action: 'check' });
    assert.deepEqual(L.parseTrigger({ submissionId: 7, action: 'refresh' }), { source: 'direct', submissionId: 7, action: 'refresh' });
    assert.deepEqual(L.parseTrigger({ submissionId: 7, action: 'delete' }), { ignore: 'unknown_action' });
    assert.deepEqual(L.parseTrigger({ submissionId: 0 }), { ignore: 'unrecognised_event' });
    assert.deepEqual(L.parseTrigger({ submissionId: -1 }), { ignore: 'unrecognised_event' });
    assert.deepEqual(L.parseTrigger(null), { ignore: 'unrecognised_event' });
    assert.deepEqual(L.parseTrigger('x'), { ignore: 'unrecognised_event' });
});

// ── Blockers ───────────────────────────────────────────────────────────────
test('importBlockers: a clean submitted row has none; each reason', () => {
    assert.deepEqual(L.importBlockers({ row: submitted(), supplier: { exists: true } }), []);
    assert.deepEqual(L.importBlockers({ row: submitted(), supplier: { exists: true }, file: { gone: false, changed: false } }), []);
    assert.deepEqual(L.importBlockers({ row: submitted({ status: 'draft' }) }), ['not_submitted']);
    assert.deepEqual(L.importBlockers({ row: submitted({ status: 'imported' }) }), ['not_submitted']);
    assert.deepEqual(L.importBlockers({ row: submitted(), supplier: { exists: false } }), ['supplier_not_found']);
    assert.deepEqual(L.importBlockers({ row: submitted({ s3_key: 'pi/0a1b2c3d-1111-4222-8333-444455556666.pdf' }) }), ['bad_key']);
    assert.deepEqual(L.importBlockers({ row: submitted({ content_type: 'text/html' }) }), ['bad_type']);
    assert.deepEqual(L.importBlockers({ row: submitted({ content_type: 'image/png' }) }), ['bad_type'], 'type must match the key’s extension');
    assert.deepEqual(L.importBlockers({ row: submitted({ file_size: L.MAX_FILE_BYTES + 1 }) }), ['too_large']);
    assert.deepEqual(L.importBlockers({ row: submitted({ s3_etag: null }) }), ['no_etag']);
    assert.deepEqual(L.importBlockers({ row: submitted(), file: { gone: true } }), ['file_gone']);
    assert.deepEqual(L.importBlockers({ row: submitted(), file: { gone: false, changed: true } }), ['file_changed']);
    assert.deepEqual(L.importBlockers({ row: null }), ['not_submitted']);
    for (const code of L.importBlockers({ row: submitted({ status: 'rejected', s3_etag: null, content_type: 'x' }) })) assert.ok(L.BLOCKERS[code], code);
});

// ── Request parsing ────────────────────────────────────────────────────────
test('parseListQuery', () => {
    assert.deepEqual(L.parseListQuery({}), { value: { statuses: ['submitted'], supplierId: null, limit: L.LIST_LIMIT_DEFAULT, offset: 0 } });
    assert.deepEqual(L.parseListQuery({ status: 'all' }).value.statuses, L.VISIBLE_STATUSES);
    assert.deepEqual(L.parseListQuery({ status: 'decided' }).value.statuses, ['imported', 'rejected', 'withdrawn']);
    assert.deepEqual(L.parseListQuery({ status: 'imported,rejected', supplierId: '3', limit: '9999', offset: '20' }).value,
        { statuses: ['imported', 'rejected'], supplierId: 3, limit: L.LIST_LIMIT_MAX, offset: 20 });
    assert.ok(L.parseListQuery({ status: 'draft' }).error, 'drafts are the supplier’s own');
    assert.ok(L.parseListQuery({ supplierId: 'x' }).error);
    assert.ok(L.parseListQuery({ limit: '0' }).error);
    assert.ok(L.parseListQuery({ offset: '-1' }).error);
});

test('parseImportBody: rowVersion plus a container, optional replacesId / force / note', () => {
    assert.deepEqual(L.parseImportBody({ rowVersion: 4, containerNumber: ' 324 ' }).value,
        { rowVersion: 4, containerNumber: '324', draftContainerId: null, replacesId: null, force: false, note: null });
    assert.deepEqual(L.parseImportBody({ rowVersion: '4', draftContainerId: '129', replacesId: 7, force: true, note: ' ok ' }).value,
        { rowVersion: 4, containerNumber: null, draftContainerId: 129, replacesId: 7, force: true, note: 'ok' });
    assert.ok(/rowVersion/.test(L.parseImportBody({ containerNumber: '324' }).error));
    assert.ok(/containerNumber or draftContainerId/.test(L.parseImportBody({ rowVersion: 4 }).error));
    assert.ok(/draftContainerId/.test(L.parseImportBody({ rowVersion: 4, draftContainerId: 'x' }).error));
    assert.ok(/replacesId/.test(L.parseImportBody({ rowVersion: 4, containerNumber: '324', replacesId: 0 }).error));
    assert.ok(/force/.test(L.parseImportBody({ rowVersion: 4, containerNumber: '324', force: 'yes' }).error));
    assert.ok(/note/.test(L.parseImportBody({ rowVersion: 4, containerNumber: '324', note: 'x'.repeat(L.NOTE_MAX + 1) }).error));
    assert.ok(L.parseImportBody(null).error);
});

test('parseRejectBody: the note is required', () => {
    assert.deepEqual(L.parseRejectBody({ rowVersion: 4, note: 'Wrong lot numbers' }).value, { rowVersion: 4, note: 'Wrong lot numbers' });
    assert.ok(/note/.test(L.parseRejectBody({ rowVersion: 4 }).error));
    assert.ok(/note/.test(L.parseRejectBody({ rowVersion: 4, note: '   ' }).error));
    assert.ok(/rowVersion/.test(L.parseRejectBody({ note: 'x' }).error));
});

// ── Supplier-safe views ────────────────────────────────────────────────────
const order = (o) => ({
    id: 1141, jf_code: 'JF0001', product_name: 'Thing', po_number: 'PO_00001J', lot_number: '0001001', quantity: 340,
    units_per_carton: 10, carton_weight: null, carton_height: null, carton_width: null, carton_depth: null,
    mfg_date: '2026-07-01', exp_date: '2031-06-01', status: 'CONSOLIDATED', supplier: 'Acme', container_number: null,
    order_quantity: 500, po_supplier: 'Acme', allocation_id: 123, ...o,
});
const row = (r) => ({
    jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: '0001001', mfgDate: '2026-07', expDate: '2031-06', description: 'Thing',
    cartons: 50, quantity: 500, unit: 'PC', grossWeightPerCarton: 10, netWeightPerCarton: 9, cartonDimensionsCm: '40X20X30', ...r,
});
const extracted = (lines, over = {}) => ({
    invoiceNumber: 'INV-9', documentDate: '2026-09-01', supplierName: 'Acme', containerNumbers: ['MRKU4645188'],
    totalCartons: 50, totalQuantity: 500, totalGrossWeightKg: 500, totalNetWeightKg: 450, totalCbm: 1.2, lines, ...over,
});

// A staff comparison with everything the supplier must not see.
function staffComparison() {
    const c = compareDocuments(
        [order(), order({ id: 1142, jf_code: 'JF0002', po_number: 'PO_00002J', quantity: 100, allocation_id: 124 })],
        [{ packingListId: 7, filename: 'PL-324.pdf', extracted: extracted([row(), row({ jfCode: 'JF0003', poNumber: 'PO_00003J', quantity: 20, cartons: 2 })]) }]
    );
    applySignOffs(c, [{ id: 5, lineKey: c.lines.find(l => l.jfCode === 'JF0002').lineKey, fingerprint: c.lines.find(l => l.jfCode === 'JF0002').fingerprint,
        signedByEmail: 'ops@jf.example', signedAt: '2026-09-02T10:00:00Z', note: 'fine' }]);
    c.container = { kind: 'booked', containerNumber: '324', draftContainerId: null, draftName: null, label: '324', followedFrom: { kind: 'draft', draftId: 129, draftName: 'DRAFT-SEA-260917-173825 - 328' } };
    c.skippedLines = [{ orderId: 9, jfCode: 'JF0009', poNumber: 'PO_00009J', allocated: 5, bookedIn: '323' }];
    c.duplicateInvoices = [{ invoiceNumber: 'INV-9', packingListIds: [7, 8] }];
    const mismatch = c.lines.find(l => l.jfCode === 'JF0001');
    mismatch.elsewhere = [
        { kind: 'draft', where: 'DRAFT-AIR-260916-154300 - 123. Airfreight urgent', orderId: 1141, jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: '0001001', status: 'READY', quantity: 160 },
        { kind: 'booked', where: '322', orderId: 2000, jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: null, status: 'ON_SEA', quantity: 50 },   // another supplier's order
        { kind: 'planned', where: 'PLANNED-SEA-261001-120000 - Sunmed November', orderId: 1141, jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: null, status: 'READY', quantity: 30 },
        { kind: 'unallocated', where: null, orderId: 1141, jfCode: 'JF0001', poNumber: 'PO_00001J', lotNumber: null, status: 'READY', quantity: 10 },
    ];
    return c;
}

test('toSupplierComparison: leaks no ids, sign-off emails, fingerprints or the staff-only container details', () => {
    const c = staffComparison();
    const out = L.toSupplierComparison(c, { groupOrderIds: new Set([1141, 1142]) });
    const text = JSON.stringify(out);
    for (const forbidden of ['orderId', 'allocationId', 'fingerprint', 'lineKey', 'matchMethod', 'signOff', 'signedBy', 'ops@jf.example',
        'orderQuantity', 'skippedLines', 'packingListId', 'draftContainerId', 'followedFrom', 'DRAFT-SEA-260917', '"status":"CONSOLIDATED"', '"orders":', '"rows":[', 'PLANNED-SEA', '1141', '1142', '2000']) {
        assert.equal(text.includes(forbidden), false, `must not contain ${forbidden}`);
    }
    assert.equal(out.summary.verdict, 'differences');
    assert.equal(out.summary.signedOff, 1);
    assert.equal(out.extractionCheck.ok, false, 'the rows add up to 520, the document prints 500');
    assert.deepEqual(out.documents, [{ filename: 'PL-324.pdf', invoiceNumber: 'INV-9', rows: 2, printedTotalQuantity: 500, sumOfRowsQuantity: 520, printedTotalCartons: 50, sumOfRowsCartons: 52, ok: false }]);
    assert.deepEqual(out.duplicateInvoices, [{ invoiceNumber: 'INV-9', count: 2 }]);
    assert.equal(out.linesTruncated, false);
    const accepted = out.lines.find(l => l.jfCode === 'JF0002');
    assert.equal(accepted.status, 'missing');
    assert.equal(accepted.accepted, true, 'a sign-off shows only as accepted');
    assert.deepEqual(Object.keys(accepted).sort(), ['accepted', 'differences', 'elsewhere', 'expected', 'jfCode', 'packed', 'poNumbers', 'productName', 'severity', 'status']);
    const mismatch = out.lines.find(l => l.jfCode === 'JF0001');
    assert.deepEqual(mismatch.expected, { quantity: 340, cartons: 34, lots: ['0001001'], dates: { mfg: ['2026-07'], exp: ['2031-06'] } });
    assert.deepEqual(mismatch.packed, { quantity: 500, cartons: 50, unit: 'PC', lots: ['0001001'], dates: { mfg: ['2026-07'], exp: ['2031-06'] } });
    assert.deepEqual(mismatch.differences.map(d => d.field), ['quantity', 'cartons']);
    for (const d of mismatch.differences) assert.deepEqual(Object.keys(d).sort(), ['actual', 'expected', 'field', 'message', 'severity']);
    const unexpected = out.lines.find(l => l.status === 'unexpected');
    assert.equal(unexpected.expected, null);
    assert.equal(unexpected.accepted, false);
});

test('toSupplierComparison: elsewhere keeps only the group’s placements, as { label, quantity }, planned and unallocated unnamed', () => {
    const c = staffComparison();
    const out = L.toSupplierComparison(c, { groupOrderIds: new Set([1141]) });
    const mismatch = out.lines.find(l => l.jfCode === 'JF0001');
    assert.deepEqual(mismatch.elsewhere, [
        { label: 'DRAFT-AIR-260916-154300 - 123. Airfreight urgent', quantity: 160 },
        { label: null, quantity: 30 },
        { label: null, quantity: 10 },
    ]);
    assert.equal(JSON.stringify(out).includes('322'), false, 'the other supplier’s container never appears');
    // No group: nothing elsewhere at all.
    const none = L.toSupplierComparison(c);
    assert.deepEqual(none.lines.find(l => l.jfCode === 'JF0001').elsewhere, []);
    // An array works as well as a Set.
    assert.equal(L.toSupplierComparison(c, { groupOrderIds: [1141] }).lines.find(l => l.jfCode === 'JF0001').elsewhere.length, 3);
});

test('toSupplierComparison: the line cap, a JSON string, and junk', () => {
    const lines = [];
    for (let i = 0; i < L.RESULT_LINE_LIMIT + 5; i++) lines.push(order({ id: i + 1, jf_code: `JF${String(i).padStart(4, '0')}` }));
    const c = compareDocuments(lines, [{ extracted: extracted([]) }]);
    const out = L.toSupplierComparison(JSON.stringify(c));
    assert.equal(out.lines.length, L.RESULT_LINE_LIMIT);
    assert.equal(out.linesTruncated, true);
    assert.equal(L.toSupplierComparison(null), null);
    assert.equal(L.toSupplierComparison('not json'), null);
});

test('toSupplierExtraction: the rows, the printed totals against their sum, the line cap', () => {
    const x = L.toSupplierExtraction(extracted([row(), row({ jfCode: 'JF0003', poNumber: 'PO_00003J', quantity: 20, cartons: 2, grossWeightPerCarton: 99 })]));
    assert.equal(x.rows, 2);
    assert.equal(x.invoiceNumber, 'INV-9');
    assert.deepEqual(x.containerNumbers, ['MRKU4645188']);
    assert.deepEqual(x.printed, { totalQuantity: 500, totalCartons: 50, totalGrossWeightKg: 500, totalNetWeightKg: 450, totalCbm: 1.2 });
    assert.deepEqual(x.sumOfRows, { quantity: 520, cartons: 52 });
    assert.equal(x.totalsOk, false);
    assert.deepEqual(Object.keys(x.lines[0]).sort(), ['cartons', 'description', 'expDate', 'jfCode', 'lotNumber', 'mfgDate', 'poNumber', 'quantity', 'unit']);
    assert.equal(x.linesTruncated, false);
    assert.equal(L.toSupplierExtraction(JSON.stringify(extracted([row()]))).totalsOk, true);
    assert.equal(L.toSupplierExtraction(extracted([row()], { totalQuantity: null, totalCartons: null })).totalsOk, true, 'nothing printed: nothing to disagree with');
    const many = L.toSupplierExtraction(extracted(Array.from({ length: L.EXTRACTION_LINE_LIMIT + 1 }, () => row())));
    assert.equal(many.lines.length, L.EXTRACTION_LINE_LIMIT);
    assert.equal(many.linesTruncated, true);
    assert.equal(L.toSupplierExtraction(null), null);
});

// ── JSON ───────────────────────────────────────────────────────────────────
test('submissionToJson / containerFromColumns / checkToJson', () => {
    const r = submitted({
        replaces_submission_id: 5, error_code: null, row_count: 12, invoice_number: 'INV-9', match_confidence: 'confident', match_reason: 'box_number',
        suggested_kind: 'booked', suggested_container_number: '324', suggested_draft_id: null, suggested_label: '324',
        confirmed_kind: null, confirmed_container_number: null, confirmed_draft_id: null, confirmed_label: null,
        verdict: 'differences', discrepancy_count: 2, result_state: 'provisional', result_at_iso: '2026-10-21T10:00:00.000Z',
        supplier_note: 'note', warnings_acknowledged: 1, submitted_at_iso: '2026-10-21T09:00:00.000Z', decided_by: null, decided_at_iso: null,
        decided_note: null, packing_list_id: null, created_at_iso: '2026-10-21T08:00:00.000Z',
    });
    const j = L.submissionToJson(r, { blockers: ['no_etag'] });
    assert.equal(j.id, 11);
    assert.equal(j.supplierId, 3);
    assert.equal(j.replacesSubmissionId, 5);
    assert.deepEqual(j.suggested, { kind: 'booked', containerNumber: '324', draftContainerId: null, label: '324' });
    assert.equal(j.confirmed, null);
    assert.equal(j.warningsAcknowledged, true);
    assert.equal(j.resultState, 'provisional');
    assert.equal(j.resultAt, '2026-10-21T10:00:00.000Z');
    assert.equal(j.rowVersion, 4);
    assert.deepEqual(j.blockers, ['no_etag']);
    assert.equal(JSON.stringify(j).includes('extract'), false, 'the extraction is the detail’s, not the list’s');
    assert.deepEqual(L.containerFromColumns({ confirmed_kind: 'draft', confirmed_container_number: '328', confirmed_draft_id: '129', confirmed_label: 'DRAFT - 328' }, 'confirmed'),
        { kind: 'draft', containerNumber: '328', draftContainerId: 129, label: 'DRAFT - 328' });
    const c = L.checkToJson(submitted({ model_used: 'gemini-3.8-flash', input_tokens: '10', checked_at_iso: '2026-10-21T08:30:00.000Z', error_code: null, row_count: 3 }));
    assert.equal(c.status, 'succeeded');
    assert.equal(c.inputTokens, 10);
    assert.equal(c.attempts, 1);
    assert.equal(c.rowCount, 3);
});

test('parseGroupSupplierIds: the portal’s ids sorted and de-duplicated; a legacy row is its own supplier', () => {
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: 2, group_supplier_ids: '[87, 2, 58, 58]' }), [2, 58, 87]);
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: 2, group_supplier_ids: [87, '58', 2] }), [2, 58, 87]);
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: 2, group_supplier_ids: null }), [2]);
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: '2', group_supplier_ids: '[]' }), [2]);
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: 2, group_supplier_ids: '[0, -1, "x"]' }), [2]);
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: 2, group_supplier_ids: 'junk' }), [2]);
    assert.deepEqual(L.parseGroupSupplierIds({ supplier_id: null, group_supplier_ids: null }), []);
    assert.deepEqual(L.parseGroupSupplierIds(null), []);
});

test('lineInGroup / groupLineFilter: by nameKey on the order’s or the PO’s supplier; no name, no line', () => {
    const P = require('../src/services/packing-list-check');
    const group = { supplierIds: [2, 58], names: ['Suzhou Sunmed Co., Ltd', 'SUNMED'] };
    assert.equal(P.lineInGroup(group, { supplier: 'suzhou  sunmed co., ltd', po_supplier: 'Other' }), true);
    assert.equal(P.lineInGroup(group, { supplier: 'Other', po_supplier: 'Sunmed ' }), true);
    assert.equal(P.lineInGroup(group, { supplier: 'Other', po_supplier: 'Other Ltd' }), false);
    assert.equal(P.lineInGroup(group, { supplier: null, po_supplier: null }), false);
    assert.equal(P.lineInGroup({ supplierIds: [2], names: [] }, { supplier: 'Sunmed', po_supplier: null }), false);
    assert.equal(P.lineInGroup(null, { supplier: 'Sunmed' }), false);
    assert.deepEqual([{ supplier: 'SUNMED' }, { supplier: 'x' }].filter(P.groupLineFilter(group)), [{ supplier: 'SUNMED' }]);
});

// ── The guarded write (pl-results.js) ──────────────────────────────────────
function fakeConn() {
    const state = { statements: [], audits: [], affected: 1 };
    return {
        state,
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            if (/^INSERT INTO audit_log/.test(s)) { state.audits.push({ action: params[2], before: params[3] && JSON.parse(params[3]), after: params[4] && JSON.parse(params[4]) }); return [{ insertId: 1 }]; }
            state.statements.push({ s, params });
            if (/^UPDATE `jportal`\.pl_submissions/.test(s)) return [{ affectedRows: state.affected }];
            throw new Error(`unexpected SQL: ${s.slice(0, 80)}`);
        },
    };
}
const result = (state = 'provisional') => ({ v: 1, state, container: { kind: 'booked', label: '324', boxNumber: null }, extraction: { rows: 1 }, lines: [] });
const match = () => ({
    candidates: [{ key: 'booked:324', score: 1, coverage: 1 }], confidence: 'confident', reason: 'box_number',
    suggested: { kind: 'booked', containerNumber: '324', draftId: null, label: '324' },
});
// A draft row as the refresh read it, already holding `computed`'s result (as
// MySQL would hand it back: keys reordered, JSON columns as strings).
const draftRow = (computed, over = {}) => ({
    id: 11, status: 'draft', check_status: 'succeeded', check_attempt: 2, row_version: 7, packing_list_id: null,
    result_json: JSON.stringify(Object.fromEntries(Object.entries(computed.result).reverse())),
    result_state: computed.state, verdict: computed.verdict, discrepancy_count: computed.discrepancyCount,
    candidates_json: JSON.stringify(computed.match.candidates), match_confidence: computed.match.confidence, match_reason: computed.match.reason,
    suggested_kind: 'booked', suggested_container_number: '324', suggested_draft_id: null, suggested_label: '324', group_supplier_ids: '[3]',
    ...over,
});

test('canonical / hashOf: key order and JSON round trips do not change the hash', () => {
    assert.equal(PL.hashOf({ a: 1, b: [1, { c: 2, d: null }] }), PL.hashOf({ b: [1, { d: null, c: 2 }], a: 1 }));
    assert.notEqual(PL.hashOf({ a: 1 }), PL.hashOf({ a: 2 }));
    assert.equal(PL.canonical({ b: undefined, a: 1 }), '{"a":1}');
});

test('writeSupplierResult: an unchanged draft result writes with no row_version bump and no audit', async () => {
    const computed = { kind: 'result', state: 'provisional', result: result(), verdict: 'differences', discrepancyCount: 2, match: match() };
    const conn = fakeConn();
    const out = await PL.writeSupplierResult(conn, draftRow(computed), computed);
    assert.deepEqual(out, { written: true, changed: false, kind: 'result', state: 'provisional' });
    assert.equal(conn.state.statements.length, 1);
    const { s, params } = conn.state.statements[0];
    assert.match(s, /WHERE id = \? AND status = 'draft' AND check_status = 'succeeded' AND row_version = \? AND check_attempt = \?$/);
    assert.deepEqual(params.slice(-4), [0, 11, 7, 2], 'bump 0, then the guard as read');
    assert.match(s, /candidates_json = \?, match_confidence = \?/, 'a draft writes the match columns too');
    assert.doesNotMatch(s, /group_supplier_ids/, 'the portal’s column is never written');
    assert.deepEqual(conn.state.audits, []);
});

test('writeSupplierResult: a changed draft result bumps row_version; a state change is audited', async () => {
    const before = { kind: 'result', state: 'unmatched', result: { v: 1, state: 'unmatched', container: null, extraction: { rows: 1 } }, verdict: null, discrepancyCount: null, match: { ...match(), confidence: 'none', reason: null, candidates: [] } };
    const computed = { kind: 'result', state: 'provisional', result: result(), verdict: 'differences', discrepancyCount: 2, match: match() };
    const conn = fakeConn();
    const out = await PL.writeSupplierResult(conn, draftRow(before, { suggested_kind: null, suggested_container_number: null, suggested_label: null }), computed);
    assert.deepEqual(out, { written: true, changed: true, kind: 'result', state: 'provisional' });
    assert.deepEqual(conn.state.statements[0].params.slice(-4), [1, 11, 7, 2]);
    assert.equal(conn.state.audits.length, 1);
    assert.equal(conn.state.audits[0].action, 'result_refreshed');
    assert.deepEqual(conn.state.audits[0].before, { resultState: 'unmatched' });
    assert.equal(conn.state.audits[0].after.resultState, 'provisional');
    // The same change, but the guard misses (a submit landed mid-refresh): 0 rows, nothing audited.
    const lost = fakeConn();
    lost.state.affected = 0;
    const miss = await PL.writeSupplierResult(lost, draftRow(before), computed);
    assert.deepEqual(miss, { written: false, changed: false, kind: 'result', state: 'provisional' });
    assert.deepEqual(lost.state.audits, []);
});

test('writeSupplierResult: an imported row writes only the result group, guarded on packing_list_id', async () => {
    const computed = { kind: 'result', state: 'confirmed', result: result('confirmed'), verdict: 'match', discrepancyCount: 0 };
    const conn = fakeConn();
    const row = { id: 11, status: 'imported', packing_list_id: 70, result_json: null, result_state: 'provisional', verdict: null, discrepancy_count: null };
    const out = await PL.writeSupplierResult(conn, row, computed);
    assert.deepEqual(out, { written: true, changed: true, kind: 'result', state: 'confirmed' });
    const { s, params } = conn.state.statements[0];
    assert.match(s, /WHERE id = \? AND status = 'imported' AND packing_list_id <=> \?$/);
    assert.doesNotMatch(s, /candidates_json|suggested_|group_supplier_ids/);
    assert.deepEqual(params, ['match', 0, JSON.stringify(computed.result), 'confirmed', 1, 11, 70]);
    assert.equal(conn.state.audits[0].action, 'result_refreshed');
});

test('writeSupplierResult: return to Pending clears the decision, keeps packing_list_id, audits', async () => {
    const computed = { kind: 'pending', reason: 'container_gone', result: { v: 1, state: 'in_review', container: null, extraction: { rows: 1 } } };
    const conn = fakeConn();
    const row = { id: 11, status: 'imported', packing_list_id: 70, confirmed_label: 'DRAFT - 328' };
    const out = await PL.writeSupplierResult(conn, row, computed);
    assert.deepEqual(out, { written: true, changed: true, kind: 'pending', reason: 'container_gone' });
    const { s, params } = conn.state.statements[0];
    assert.match(s, /SET status = 'submitted', confirmed_kind = NULL, confirmed_container_number = NULL, confirmed_draft_id = NULL, confirmed_label = NULL, decided_by = NULL, decided_at = NULL, decided_note = NULL/);
    assert.match(s, /result_state = 'in_review'/);
    assert.match(s, /row_version = row_version \+ 1/);
    assert.doesNotMatch(s, /packing_list_id = /, 'the previous filing is kept');
    assert.match(s, /WHERE id = \? AND status = 'imported' AND packing_list_id <=> \?$/);
    assert.deepEqual(params, [JSON.stringify(computed.result), 11, 70]);
    assert.equal(conn.state.audits[0].action, 'returned_to_pending');
    assert.deepEqual(conn.state.audits[0].after, { reason: 'container_gone', previousPackingListId: 70, confirmedLabel: 'DRAFT - 328' });
});

test('writeSupplierResult: a no-op and a frozen row write nothing', async () => {
    const conn = fakeConn();
    assert.deepEqual(await PL.writeSupplierResult(conn, { id: 11, status: 'submitted' }, { kind: 'noop', reason: 'frozen' }), { written: false, changed: false, kind: 'noop', reason: 'frozen' });
    assert.deepEqual(await PL.writeSupplierResult(conn, { id: 11, status: 'submitted', row_version: 1 }, { kind: 'result', state: 'provisional', result: result() }), { written: false, changed: false, kind: 'result', reason: 'frozen' });
    assert.deepEqual(conn.state.statements, []);
});

test('computeSupplierResult: frozen statuses and an unchecked draft are no-ops without a query', async () => {
    const conn = fakeConn();
    for (const status of ['submitted', 'rejected', 'withdrawn', 'discarded']) {
        assert.deepEqual(await PL.computeSupplierResult(conn, { id: 1, status }), { kind: 'noop', reason: 'frozen' }, status);
    }
    assert.deepEqual(await PL.computeSupplierResult(conn, { id: 1, status: 'draft', check_status: 'failed' }), { kind: 'noop', reason: 'check_not_succeeded' });
    assert.deepEqual(await PL.computeSupplierResult(conn, { id: 1, status: 'draft', check_status: 'succeeded', extract_json: null }), { kind: 'noop', reason: 'no_extraction' });
    assert.deepEqual(await PL.computeSupplierResult(conn, null), { kind: 'noop', reason: 'not_found' });
    assert.deepEqual(conn.state.statements, []);
});
