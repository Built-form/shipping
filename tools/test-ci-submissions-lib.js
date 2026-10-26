'use strict';

// Unit tests for the supplier-portal commercial-invoice rules
// (src/lib/ci-submissions.js), the candidate evidence (src/services/ci-check.js
// candidateEvidence, over the real assessFit), the reader's portal prompt, and
// the checker against an in-memory row, a fake S3 and a fake model. No
// database, no AWS, no model call.
//   node --test tools/test-ci-submissions-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/ci-submissions');
const PI = require('../src/lib/pi-submissions');
const PLS = require('../src/lib/pl-submissions');
const C = require('../src/services/ci-check');
const E = require('../src/services/shipment-payment-extract');

const KEY = 'ci/0a1b2c3d-1111-4222-8333-444455556666.pdf';
const BUCKET = 'jportal-pi-uploads-test';
const NOW = Date.parse('2026-10-22T12:00:00Z');
const s3Event = (key, bucket = BUCKET) => ({
    source: 'aws.s3', 'detail-type': 'Object Created', detail: { bucket: { name: bucket }, object: { key } },
});

// ── Shared with PI and PL, and with the portal ─────────────────────────────
test('re-exports PI’s limits and helpers unchanged', () => {
    for (const k of ['sniffType', 'safeFilename', 'canDecide', 'parseJson', 'positiveInt', 'parseRowVersion', 'parseNote']) {
        assert.equal(L[k], PI[k], k);
    }
    for (const k of ['STALE_CHECK_SECONDS', 'MAX_CHECK_ATTEMPTS', 'RETRYABLE_CODES', 'MAX_FILE_BYTES', 'ALLOWED_TYPES', 'VISIBLE_STATUSES', 'DECIDED_STATUSES']) {
        assert.equal(L[k], PI[k], k);
    }
    for (const k of ['CHECKABLE_SQL', 'parseGroupSupplierIds', 'parseListQuery', 'parseRejectBody']) assert.equal(L[k], PLS[k], k);
});

test('the constants the portal’s src/lib/ciSubmissions.js holds too', () => {
    assert.equal(L.CI_KEY_RE.source, '^ci\\/[0-9a-f-]{36}\\.(pdf|png|jpg)$');
    assert.deepEqual(L.STATUSES, ['draft', 'submitted', 'imported', 'rejected', 'withdrawn', 'discarded']);
    assert.equal(L.MAX_FILE_BYTES, 10 * 1024 * 1024);
    assert.deepEqual(Object.keys(L.ALLOWED_TYPES), ['application/pdf', 'image/png', 'image/jpeg']);
});

// ── Keys and triggers ──────────────────────────────────────────────────────
test('CI_KEY_RE / destKey', () => {
    assert.equal(L.isCiKey(KEY), true);
    assert.equal(L.isCiKey(KEY.replace('ci/', 'pl/')), false, 'a PL key is not a CI key');
    assert.equal(L.isCiKey('ci/0a1b2c3d-1111-4222-8333-444455556666.xlsx'), false);
    assert.equal(L.isCiKey('ci/../evil.pdf'), false);
    assert.equal(L.destKey({ submissionId: 9, filename: 'CI 324 (final).PDF', contentType: 'application/pdf', token: 'T' }), 'shipment-payments/T/CI_324_final.pdf');
    assert.equal(L.destKey({ submissionId: 9, filename: '商业发票.pdf', contentType: 'application/pdf', token: 'T' }), 'shipment-payments/T/ci-9.pdf');
});

test('parseTrigger', () => {
    assert.deepEqual(L.parseTrigger(s3Event(KEY), { bucket: BUCKET }), { source: 'event', key: KEY });
    assert.deepEqual(L.parseTrigger(s3Event(KEY.replace('ci/', 'pl/')), { bucket: BUCKET }), { ignore: 'not_a_ci_key' });
    assert.deepEqual(L.parseTrigger(s3Event(KEY, 'other'), { bucket: BUCKET }), { ignore: 'other_bucket' });
    assert.deepEqual(L.parseTrigger(s3Event(encodeURIComponent(KEY)), { bucket: BUCKET }), { source: 'event', key: KEY });
    assert.deepEqual(L.parseTrigger({ submissionId: 12 }), { source: 'direct', submissionId: 12 });
    assert.deepEqual(L.parseTrigger({ submissionId: 12, staff: true }), { source: 'direct', submissionId: 12 }, 'no payload asks for the staff claim');
    assert.deepEqual(L.parseTrigger({}), { ignore: 'unrecognised_event' });
});

test('the claim: PL’s for the portal’s triggers; the staff re-read', () => {
    const waiting = { status: 'submitted', check_status: 'awaiting_upload', check_attempt: 0 };
    for (const source of ['event', 'direct']) {
        assert.deepEqual(L.planEvent(source, waiting, NOW), PLS.planEvent(source, waiting, NOW), source);
        assert.deepEqual(L.claimWhere(source), PLS.claimWhere(source), source);
    }
    const staff = row => L.planEvent('staff', { status: 'submitted', check_attempt: 1, ...row }, NOW);
    assert.equal(staff({ check_status: 'succeeded' }).claim, true, 'a wrong read may be read again');
    assert.equal(staff({ check_status: 'failed', error_code: 'FILE_INVALID' }).claim, true);
    assert.equal(staff({ check_status: 'awaiting_upload' }).claim, true);
    assert.deepEqual(staff({ check_status: 'pending', check_age_seconds: 30 }), { claim: false, reason: 'in_progress' });
    assert.equal(staff({ check_status: 'pending', check_age_seconds: 900 }).claim, true);
    assert.deepEqual(staff({ check_status: 'failed', check_attempt: L.STAFF_MAX_CHECK_ATTEMPTS }), { claim: false, reason: 'attempts_exhausted' });
    for (const status of ['draft', 'imported', 'rejected', 'withdrawn']) {
        assert.deepEqual(staff({ status, check_status: 'failed' }), { claim: false, reason: 'not_submitted' }, status);
    }
    const g = L.claimWhere('staff');
    assert.match(g.sql, /status = 'submitted'/);
    assert.match(g.sql, new RegExp(`check_attempt < ${L.STAFF_MAX_CHECK_ATTEMPTS}`));
    assert.deepEqual(g.params, []);
});

// ── Ranking ────────────────────────────────────────────────────────────────
const ev = (shipmentId, over = {}) => ({
    shipmentId, reference: String(shipmentId), container: null, pos: null, poHits: [], amountRatio: null,
    amountOver: false, currencyMiss: false, ...over,
});

test('rankCandidates: nothing to rank, or nothing tying it anywhere', () => {
    assert.deepEqual(L.rankCandidates([]), { ranked: [], confidence: 'none', reason: null, suggested: null });
    const r = L.rankCandidates([ev(1), ev(2)]);
    assert.equal(r.confidence, 'none');
    assert.equal(r.suggested, null);
    assert.deepEqual(r.ranked.map(e => e.shipmentId), [1, 2], 'ties keep the recency order');
});

test('rankCandidates: a container number on the invoice', () => {
    const r = L.rankCandidates([ev(1, { pos: true, poHits: ['PO_1'] }), ev(2, { container: true })]);
    assert.equal(r.suggested.shipmentId, 2);
    assert.deepEqual([r.confidence, r.reason], ['confident', 'container']);
    // A box reused across voyages: the POs tell them apart, or staff choose.
    const reused = L.rankCandidates([ev(1, { container: true }), ev(2, { container: true, poHits: ['PO_2'] })]);
    assert.deepEqual([reused.suggested.shipmentId, reused.confidence, reused.reason], [2, 'confident', 'container']);
    const tie = L.rankCandidates([ev(1, { container: true }), ev(2, { container: true })]);
    assert.deepEqual([tie.suggested.shipmentId, tie.confidence, tie.reason], [1, 'unsure', 'conflict']);
    const contradicted = L.rankCandidates([ev(1, { container: true, pos: false })]);
    assert.deepEqual([contradicted.confidence, contradicted.reason], ['unsure', 'conflict']);
});

test('rankCandidates: purchase orders, and the goods value when a PO is split', () => {
    const one = L.rankCandidates([ev(1, { poHits: ['PO_1'] }), ev(2, { poHits: ['PO_1', 'PO_2'], pos: true })]);
    assert.deepEqual([one.suggested.shipmentId, one.confidence, one.reason], [2, 'confident', 'po_numbers']);
    const split = L.rankCandidates([ev(1, { poHits: ['PO_1'], amountRatio: 1.6 }), ev(2, { poHits: ['PO_1'], amountRatio: 1.004 })]);
    assert.deepEqual([split.suggested.shipmentId, split.confidence, split.reason], [2, 'confident', 'amount']);
    const both = L.rankCandidates([ev(1, { poHits: ['PO_1'], amountRatio: 1.02 }), ev(2, { poHits: ['PO_1'], amountRatio: 0.99 })]);
    assert.deepEqual([both.confidence, both.reason], ['unsure', 'conflict']);
    const over = L.rankCandidates([ev(1, { poHits: ['PO_1'], amountOver: true })]);
    assert.deepEqual([over.confidence, over.reason], ['unsure', 'po_numbers']);
    // A candidate whose box contradicts the invoice is no rival.
    const rivalElsewhere = L.rankCandidates([ev(1, { poHits: ['PO_1'] }), ev(2, { poHits: ['PO_1'], container: false })]);
    assert.deepEqual([rivalElsewhere.suggested.shipmentId, rivalElsewhere.confidence], [1, 'confident']);
});

test('rankCandidates: the only booked shipment', () => {
    assert.deepEqual(pick(L.rankCandidates([ev(1, { amountRatio: 0.97 })])), [1, 'confident', 'single']);
    assert.deepEqual(pick(L.rankCandidates([ev(1)])), [1, 'unsure', 'single']);
    assert.deepEqual(pick(L.rankCandidates([ev(1, { container: false })])), [null, 'none', null]);
    assert.deepEqual(pick(L.rankCandidates([ev(1, { currencyMiss: true })])), [null, 'none', null]);
});
function pick(r) { return [r.suggested ? r.suggested.shipmentId : null, r.confidence, r.reason]; }

test('scoreEvidence orders by strength', () => {
    const s = L.scoreEvidence;
    assert.ok(s(ev(1, { container: true })) > s(ev(1, { poHits: ['A', 'B', 'C'] })));
    assert.ok(s(ev(1, { poHits: ['A', 'B'] })) > s(ev(1, { poHits: ['A'] })));
    assert.ok(s(ev(1, { amountRatio: 1 })) > s(ev(1, { amountRatio: 1.04 })));
    assert.ok(s(ev(1, { amountRatio: 1.04 })) > s(ev(1, { amountRatio: 1.3 })));
    assert.ok(s(ev(1, { container: false })) < 0);
    assert.equal(s(ev(1, { poHits: ['A', 'B', 'C', 'D', 'E', 'F'] })), 100, 'four POs at most');
});

test('storedCandidates / parseStoredCandidates / candidateToJson', () => {
    const ranked = L.rankCandidates([1, 2, 3, 4, 5, 6, 7].map(n => ev(n))).ranked;
    const stored = L.storedCandidates({ ranked, considered: 7, groupNames: ['Acme Ltd'] });
    assert.equal(stored.top.length, L.STORED_CANDIDATE_LIMIT);
    const back = L.parseStoredCandidates(JSON.stringify(stored));
    assert.equal(back.considered, 7);
    assert.deepEqual(back.groupNames, ['Acme Ltd']);
    assert.equal(back.top.length, 5);
    assert.deepEqual(L.parseStoredCandidates(null), { considered: 0, groupNames: [], top: [] });
    const j = L.candidateToJson(back.top[0]);
    assert.equal(j.shipmentId, 1);
    assert.equal(j.rank, 1);
    assert.deepEqual(j.poHits, []);
});

// ── Import and requests ────────────────────────────────────────────────────
const submitted = (over = {}) => ({
    id: 9, status: 'submitted', s3_key: KEY, content_type: 'application/pdf', file_size: 2048, s3_etag: '"abc"',
    supplier_id: 3, supplier_name: 'Acme Ltd', group_supplier_ids: '[3, 8]', filename: 'CI-324.pdf',
    submitted_by_email: 'sales@acme.example', row_version: 2, check_status: 'succeeded', check_attempt: 1, ...over,
});

test('importBlockers', () => {
    assert.deepEqual(L.importBlockers({ row: submitted(), supplier: { exists: true } }), []);
    assert.deepEqual(L.importBlockers({ row: submitted({ status: 'imported' }) }), ['not_submitted']);
    assert.deepEqual(L.importBlockers({ row: submitted({ s3_etag: null }) }), ['no_etag']);
    assert.deepEqual(L.importBlockers({ row: submitted({ s3_key: KEY.replace('ci/', 'pl/') }) }), ['bad_key']);
    assert.deepEqual(L.importBlockers({ row: submitted({ content_type: 'image/png' }) }), ['bad_type']);
    assert.deepEqual(L.importBlockers({ row: submitted(), supplier: { exists: false }, file: { gone: true } }), ['supplier_not_found', 'file_gone']);
    assert.deepEqual(L.importBlockers({ row: null }), ['not_submitted']);
});

test('parseImportBody / parseShipmentChoice', () => {
    assert.deepEqual(L.parseImportBody({ rowVersion: 2, shipmentId: '61' }).value,
        { rowVersion: 2, shipmentId: 61, shipmentReference: null, supplierName: null, force: false, note: null });
    assert.deepEqual(L.parseImportBody({ rowVersion: 0, shipmentReference: ' 324 ', supplierName: ' Acme ', force: true, note: 'Filed on 324' }).value,
        { rowVersion: 0, shipmentId: null, shipmentReference: '324', supplierName: 'Acme', force: true, note: 'Filed on 324' });
    assert.match(L.parseImportBody({ shipmentId: 1 }).error, /rowVersion/);
    assert.match(L.parseImportBody({ rowVersion: 1 }).error, /shipmentId or shipmentReference/);
    assert.match(L.parseImportBody({ rowVersion: 1, shipmentId: -2 }).error, /positive integer/);
    assert.match(L.parseImportBody({ rowVersion: 1, shipmentId: 2, force: 'yes' }).error, /force/);
    assert.match(L.parseImportBody({ rowVersion: 1, shipmentId: 2, supplierName: 7 }).error, /supplierName/);
    assert.match(L.parseRejectBody({ rowVersion: 1 }).error, /note is required/);
});

test('submissionToJson', () => {
    const j = L.submissionToJson({
        ...submitted({ match_confidence: 'confident', match_reason: 'container', suggested_shipment_id: 61, suggested_shipment_reference: '324', suggested_supplier_name: 'Acme Ltd' }),
        submitted_at_iso: '2026-10-22T10:00:00Z', created_at_iso: '2026-10-22T09:59:00Z', supplier_exists: 1,
    }, { blockers: [] });
    assert.equal(j.supplierKey, 's:3');
    assert.deepEqual(j.groupSupplierIds, [3, 8]);
    assert.deepEqual(j.suggested, { shipmentId: 61, shipmentReference: '324', supplierName: 'Acme Ltd' });
    assert.equal(j.confirmed, null);
    assert.equal(j.supplierExists, true);
    assert.equal(j.submittedAt, '2026-10-22T10:00:00Z');
    const imported = L.submissionToJson(submitted({ status: 'imported', document_id: 77, confirmed_shipment_id: 62, confirmed_shipment_reference: '325', filed_supplier_name: 'Medoffice' }));
    assert.deepEqual(imported.confirmed, { shipmentId: 62, shipmentReference: '325', supplierName: 'Medoffice' });
    assert.equal(imported.documentId, 77);
});

// ── The evidence, over the real assessFit ─────────────────────────────────
const SHIP_324 = { id: 61, reference: '324', name: null, stage: 'IN_TRANSIT', mode: 'SEA', tracking_ref: 'MSKU1234565', bl_number: 'BL-77', booking_ref: null, etd: '2026-10-01', eta: '2026-11-05' };
const SHIP_325 = { id: 62, reference: '325', name: null, stage: 'BOOKED', mode: 'SEA', tracking_ref: 'TGHU7654321', bl_number: null, booking_ref: null, etd: '2026-10-12', eta: null };
const po = (id, poNumber, supplier, value, currency = 'USD') => ({
    id, poNumber, supplier, supplierKey: supplier.trim().toLowerCase(), currency, lineCount: 1, unitsInShipment: 100, valueInShipment: value,
});
const POS_324 = [po(299, 'PO_00299J', 'Acme Ltd', 9758.62), po(300, 'PO_00300J', 'Other Co', 5000)];
const POS_325 = [po(301, 'PO_00301J', 'Acme Ltd', 4000), po(299, 'PO_00299J', 'Acme Ltd', 2000)];
const extractOf = (over = {}) => C.normaliseExtract({
    documentKind: 'balance_invoice', supplierName: 'ACME LTD.', currency: 'USD', totalAmount: 9758.62, amountDueNow: null,
    containerRefs: [], blRefs: [], poRefs: [], lines: [], otherCharges: [], ...over,
});

test('candidateEvidence: the container, the POs, the goods value', () => {
    const e = C.candidateEvidence({
        extract: extractOf({ containerRefs: ['MSKU 123456-5'], poRefs: ['PO 00299J', 'PO_00300J'] }),
        shipment: SHIP_324, memberPos: POS_324, groupNames: ['Acme Ltd'],
    });
    assert.equal(e.shipmentId, 61);
    assert.equal(e.supplierName, 'Acme Ltd', 'filed under the group’s spelling on the PO');
    assert.equal(e.container, true);
    assert.deepEqual(e.poHits, ['PO_00299J'], 'another supplier’s PO is not a hit');
    assert.equal(e.onBoard, 9758.62);
    assert.equal(e.amountRatio, 1);
    assert.equal(e.trackingRef, 'MSKU1234565');
    assert.ok(Array.isArray(e.checks) && e.checks.length >= 3);

    const elsewhere = C.candidateEvidence({ extract: extractOf({ containerRefs: ['MSKU1234565'] }), shipment: SHIP_325, memberPos: POS_325, groupNames: ['Acme Ltd'] });
    assert.equal(elsewhere.container, false, 'names a box we hold another number for');
    assert.equal(elsewhere.onBoard, 6000);

    const ranked = L.rankCandidates([elsewhere, e]);
    assert.deepEqual([ranked.suggested.shipmentId, ranked.confidence, ranked.reason], [61, 'confident', 'container']);
});

test('candidateEvidence: a PO on two boxes, told apart by the goods value', () => {
    const extract = extractOf({ totalAmount: 2000, poRefs: ['PO00299J'], lines: [{ poRef: 'PO00299J', amount: 2000, qty: 100, unitPrice: 20 }] });
    const a = C.candidateEvidence({ extract, shipment: SHIP_324, memberPos: POS_324, groupNames: ['Acme Ltd'] });
    const b = C.candidateEvidence({ extract, shipment: SHIP_325, memberPos: POS_325, groupNames: ['Acme Ltd'] });
    assert.deepEqual(a.poHits, ['PO_00299J']);
    assert.deepEqual(b.poHits, ['PO_00299J']);
    // 325 holds 6,000 of Acme's goods; 2,000 is a third. 324 holds 9,758.62.
    assert.ok(Math.abs(b.amountRatio - 0.3333) < 0.001);
    const r = L.rankCandidates([a, b]);
    assert.deepEqual([r.confidence, r.reason], ['unsure', 'conflict'], 'neither box is the invoice’s value');
});

test('goodsOf: lines first, else the total less charges', () => {
    assert.equal(C.goodsOf({ lines: [{ amount: 10 }, { qty: 2, unitPrice: 2.5 }, { amount: -3 }] }), 15);
    assert.equal(C.goodsOf({ lines: [], totalAmount: 120, otherCharges: [{ amount: 20, kind: 'freight' }] }), 100);
    assert.equal(C.goodsOf({ lines: [] }), null);
});

// ── The reader's prompt ────────────────────────────────────────────────────
test('promptFor: the portal context, and the existing ones unchanged', () => {
    const portal = E.promptFor({ shipment: null, purchaseOrders: [], portalUpload: true, qcLines: [] });
    assert.match(portal, /supplier portal/);
    assert.match(portal, /container number, bill-of-lading number and purchase-order reference/);
    assert.doesNotMatch(portal, /QC sample units on this supplier/);
    const withQc = E.promptFor({ shipment: null, purchaseOrders: [], portalUpload: true, qcLines: [{ poId: 1, poNumber: 'PO_1', jfCode: 'JF1_FQC', productName: 'Gloves', quantity: 10, unitPrice: 1 }] });
    assert.match(withQc, /QC sample units on this supplier/);
    assert.match(E.promptFor({ shipment: null, purchaseOrders: [] }), /remittance advice/);
    assert.match(E.promptFor({ shipment: SHIP_324, purchaseOrders: [] }), /reference: 324/);
});

// ── The runner, against fakes ──────────────────────────────────────────────
const PDF = Buffer.concat([Buffer.from('%PDF-1.4\n'), Buffer.alloc(200, 0x20)]);
const READ = { parsed: { documentKind: 'balance_invoice', supplierName: 'Acme Ltd', invoiceNumber: 'INV-9', currency: 'USD', totalAmount: 9758.62, amountDueNow: null, containerRefs: ['MSKU1234565'], poRefs: [] }, modelUsed: 'gemini-test', usage: { totalTokenCount: 10 } };
const GROUP = { supplierIds: [3], names: ['Acme Ltd'], suppliers: [{ id: 3, name: 'Acme Ltd' }] };

// A single ci_submissions row in memory, answering the runner's statements.
function fakeWorld({ dbRow, object = PDF, etag = '"abc"', group = GROUP, read = async () => READ, decideDuringRead = null } = {}) {
    const state = { row: { ...dbRow }, audits: [], objectExists: object !== null, getCalls: 0, headCalls: 0, reads: 0 };
    const checkable = r => r.status === 'draft' || r.status === 'submitted';
    const conn = {
        async query(sql, params = []) {
            const r = state.row;
            const s = sql.replace(/\s+/g, ' ');
            if (/^SELECT id, status/.test(s)) {
                const match = /WHERE s3_key = \?/.test(s) ? r.s3_key === params[0] : Number(r.id) === Number(params[0]);
                return [match ? [{ ...r, check_age_seconds: r.check_age_seconds ?? null }] : []];
            }
            if (/SET check_status = 'pending'/.test(s)) {
                const source = /check_attempt < 5/.test(s) ? 'staff' : params.length > 1 ? 'direct' : 'event';
                const ok = L.planEvent(source, r, NOW).claim;
                if (ok) Object.assign(r, { check_status: 'pending', check_attempt: r.check_attempt + 1, check_age_seconds: 0, error_code: null, row_version: r.row_version + 1 });
                return [{ affectedRows: ok ? 1 : 0 }];
            }
            if (/^SELECT check_attempt/.test(s)) return [[{ check_attempt: r.check_attempt }]];
            const guarded = () => r.check_attempt === params[params.length - 1] && r.check_status === 'pending' && checkable(r);
            if (/SET s3_etag = \?/.test(s)) {
                if (!guarded()) return [{ affectedRows: 0 }];
                Object.assign(r, { s3_etag: params[0], file_size: params[1] });
                return [{ affectedRows: 1 }];
            }
            if (/SET check_status = 'awaiting_upload'/.test(s)) {
                if (!guarded()) return [{ affectedRows: 0 }];
                Object.assign(r, { check_status: 'awaiting_upload', check_attempt: r.check_attempt - 1 });
                return [{ affectedRows: 1 }];
            }
            if (/SET check_status = 'failed'/.test(s)) {
                if (!guarded()) return [{ affectedRows: 0 }];
                Object.assign(r, { check_status: 'failed', error_code: params[0], error_message: params[1] });
                return [{ affectedRows: 1 }];
            }
            if (/SET check_status = 'succeeded'/.test(s)) {
                if (!guarded()) return [{ affectedRows: 0 }];
                Object.assign(r, {
                    check_status: 'succeeded', model_used: params[0], extract_json: params[4], invoice_number: params[5],
                    candidates_json: params[6], match_confidence: params[7], match_reason: params[8],
                    suggested_shipment_id: params[9], suggested_shipment_reference: params[10], suggested_supplier_name: params[11],
                });
                return [{ affectedRows: 1 }];
            }
            if (/INSERT INTO audit_log/.test(s)) { state.audits.push(params[2]); return [{ insertId: 1 }]; }
            throw new Error(`unexpected SQL: ${s.slice(0, 80)}`);
        },
        release() {},
    };
    const missing = () => Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey', $metadata: { httpStatusCode: 404 } });
    const s3 = {
        async send(cmd) {
            const name = cmd.constructor.name;
            if (name === 'HeadObjectCommand') { state.headCalls += 1; if (!state.objectExists) throw missing(); return { ETag: etag }; }
            if (name === 'GetObjectCommand') {
                state.getCalls += 1;
                if (!state.objectExists) throw missing();
                return { ETag: etag, ContentLength: object.length, Body: { transformToByteArray: async () => new Uint8Array(object) } };
            }
            throw new Error(`unexpected S3 ${name}`);
        },
    };
    const checker = C.createCiChecker({
        pool: { getConnection: async () => conn }, bucket: BUCKET, s3, now: () => NOW,
        readDocument: async (args) => {
            state.reads += 1;
            state.lastContext = args.context;
            state.lastBytes = args.bytes;
            if (decideDuringRead) Object.assign(state.row, decideDuringRead);
            return read(args);
        },
        portalGroup: async () => group,
        loadQc: async () => [],
        // The real ranking over two candidates, without the database.
        suggest: async (_conn, { extract }) => {
            const evidence = [
                C.candidateEvidence({ extract, shipment: SHIP_325, memberPos: POS_325, groupNames: group.names }),
                C.candidateEvidence({ extract, shipment: SHIP_324, memberPos: POS_324, groupNames: group.names }),
            ];
            return { ...L.rankCandidates(evidence), considered: 2, groupNames: group.names };
        },
    });
    return { state, checker };
}

const dbRow = (over = {}) => ({
    id: 9, status: 'draft', check_status: 'awaiting_upload', check_attempt: 0, error_code: null, s3_key: KEY, s3_etag: null,
    content_type: 'application/pdf', file_size: 1, supplier_id: 3, supplier_name: 'Acme Ltd', group_supplier_ids: '[3]', row_version: 0, ...over,
});

test('runner: the event path reads, suggests and writes once; a duplicate event is a no-op', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow() });
    const out = await checker.run(s3Event(KEY));
    assert.equal(out.outcome, 'succeeded');
    assert.equal(out.confidence, 'confident');
    assert.equal(out.reason, 'container');
    assert.equal(out.suggestedReference, '324');
    const r = state.row;
    assert.equal(r.check_status, 'succeeded');
    assert.equal(r.s3_etag, '"abc"');
    assert.equal(r.file_size, PDF.length);
    assert.equal(r.invoice_number, 'INV-9');
    assert.equal(r.suggested_shipment_id, 61);
    assert.equal(r.suggested_supplier_name, 'Acme Ltd');
    assert.equal(JSON.parse(r.candidates_json).top[0].shipmentId, 61);
    assert.equal(state.lastContext.portalUpload, true, 'the reader is told it is a portal upload');
    assert.equal(state.lastContext.shipment, null);
    assert.ok(Buffer.isBuffer(state.lastBytes), 'the bytes are handed over, not fetched from the PO bucket');
    assert.deepEqual(state.audits, ['check_succeeded']);
    const again = await checker.run(s3Event(KEY));
    assert.equal(again.outcome, 'skipped');
    assert.equal(state.reads, 1);
});

test('runner: a submitted row (sent on arrival) is checked by the direct invoke', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow({ status: 'submitted', s3_etag: '"abc"' }) });
    const out = await checker.run({ submissionId: 9 });
    assert.equal(out.outcome, 'succeeded');
    assert.equal(state.headCalls, 1);
});

test('runner: a missing object on the direct path uses no attempt', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), object: null });
    assert.deepEqual(await checker.run({ submissionId: 9 }), { outcome: 'skipped', submissionId: 9, reason: 'file_missing' });
    assert.equal(state.row.check_attempt, 0);
    assert.equal(state.getCalls, 0);
});

test('runner: bytes that are not a PDF fail FILE_INVALID before any read', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), object: Buffer.from('not a pdf at all') });
    const out = await checker.run(s3Event(KEY));
    assert.equal(out.code, 'FILE_INVALID');
    assert.equal(state.row.check_status, 'failed');
    assert.equal(state.reads, 0);
});

test('runner: a supplier group with no live supplier fails before Gemini', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), group: { supplierIds: [3], names: [], suppliers: [] } });
    const out = await checker.run(s3Event(KEY));
    assert.equal(out.code, 'SUPPLIER_NOT_FOUND');
    assert.equal(state.reads, 0);
    assert.deepEqual(state.audits, ['check_failed']);
});

test('runner: a model error is classified; the direct path may claim a retryable failure again', async () => {
    const sdkErr = Object.assign(new Error('overloaded'), { status: 503 });
    const { state, checker } = fakeWorld({ dbRow: dbRow(), read: async () => { throw sdkErr; } });
    const out = await checker.run(s3Event(KEY));
    assert.equal(out.code, 'MODEL_UNAVAILABLE');
    assert.equal(state.row.error_code, 'MODEL_UNAVAILABLE');
    const retry = await checker.run({ submissionId: 9 });
    assert.equal(retry.code, 'MODEL_UNAVAILABLE');
    assert.equal(state.row.check_attempt, 2);
});

test('runner: staff decide during the read — nothing is written over the decision', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow({ status: 'submitted' }), decideDuringRead: { status: 'imported' } });
    const out = await checker.run({ submissionId: 9 });
    assert.deepEqual(out, { outcome: 'skipped', submissionId: 9, reason: 'claim_lost' });
    assert.equal(state.row.match_confidence, undefined);
});

test('runner: the staff re-read claims a succeeded row; no payload can', async () => {
    const row = dbRow({ status: 'submitted', check_status: 'succeeded', check_attempt: 3, s3_etag: '"abc"' });
    const { state, checker } = fakeWorld({ dbRow: row });
    assert.equal((await checker.run({ submissionId: 9 })).reason, 'attempts_exhausted', 'the portal’s direct invoke may not');
    const out = await checker.run({ submissionId: 9 }, { staff: true, userEmail: 'ops@example.com' });
    assert.equal(out.outcome, 'succeeded');
    assert.equal(state.row.check_attempt, 4);
    assert.deepEqual(state.audits, ['recheck', 'check_succeeded']);
});

// ── The staff import route, against a fake connection and S3 ───────────────
const { registerCiSubmissionRoutes } = require('../src/services/ci-submission-routes');

function routeWorld({ row = submitted(), shipment = { ...SHIP_324, merged_into_id: null, deleted_at: null }, memberPos = POS_324, duplicate = null, object = PDF, etag = '"abc"' } = {}) {
    const state = { row: { ...row }, audits: [], puts: [], inserted: null, extractions: [], tx: [] };
    const conn = {
        async beginTransaction() { state.tx.push('begin'); },
        async commit() { state.tx.push('commit'); },
        async rollback() { state.tx.push('rollback'); },
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ');
            if (/FROM `jportal`\.ci_submissions r LEFT JOIN suppliers/.test(s)) {
                return [Number(params[0]) === state.row.id && L.VISIBLE_STATUSES.includes(state.row.status) ? [{ ...state.row, supplier_exists: 1 }] : []];
            }
            if (/FROM `jportal`\.ci_submissions WHERE id = \? FOR UPDATE/.test(s)) return [[{ ...state.row }]];
            if (/SELECT id, name FROM suppliers WHERE id IN/.test(s)) return [[{ id: 3, name: 'Acme Ltd' }]];
            if (/FROM shipments WHERE id = \?/.test(s)) return [Number(params[0]) === shipment.id ? [shipment] : []];
            if (/FROM shipments s WHERE s\.id = \?/.test(s)) return [[shipment]];
            if (/FROM orders o JOIN purchase_orders po/.test(s)) {
                return [memberPos.map(p => ({ shipment_id: shipment.id, id: p.id, po_number: p.poNumber, supplier: p.supplier, currency: p.currency, line_count: 1, units_in_shipment: 100, value_in_shipment: p.valueInShipment }))];
            }
            if (/FROM shipment_payment_documents WHERE deleted_at IS NULL AND filename/.test(s)) return [duplicate ? [{ id: duplicate }] : []];
            if (/INSERT INTO shipment_payment_documents/.test(s)) { state.inserted = params; return [{ insertId: 77 }]; }
            if (/INSERT INTO audit_log/.test(s)) { state.audits.push(`${params[0]}:${params[2]}`); return [{ insertId: 1 }]; }
            if (/SET status = 'imported'/.test(s)) {
                const ok = state.row.status === 'submitted' && state.row.row_version === params[params.length - 1];
                if (ok) Object.assign(state.row, { status: 'imported', decided_by: params[0], decided_note: params[1], document_id: params[2], confirmed_shipment_id: params[3], confirmed_shipment_reference: params[4], row_version: state.row.row_version + 1 });
                return [{ affectedRows: ok ? 1 : 0 }];
            }
            if (/SELECT \* FROM shipment_payment_documents WHERE id = \?/.test(s)) return [[{ id: 77, s3_key: state.inserted?.[5], filename: state.inserted?.[4], deleted_at: null }]];
            throw new Error(`unexpected SQL: ${s.slice(0, 90)}`);
        },
    };
    const s3 = {
        async send(cmd) {
            const name = cmd.constructor.name;
            if (name === 'GetObjectCommand') {
                if (cmd.input.IfMatch !== etag) throw Object.assign(new Error('PreconditionFailed'), { name: 'PreconditionFailed', $metadata: { httpStatusCode: 412 } });
                return { ETag: etag, ContentLength: object.length, Body: { transformToByteArray: async () => new Uint8Array(object) } };
            }
            if (name === 'PutObjectCommand') { state.puts.push(cmd.input); return {}; }
            throw new Error(`unexpected S3 ${name}`);
        },
    };
    const handlers = {};
    const app = { get: (p, h) => { handlers[`GET ${p}`] = h; }, post: (p, h) => { handlers[`POST ${p}`] = h; } };
    registerCiSubmissionRoutes(app, {
        withConnection: fn => fn(conn), pool: {}, ready: async () => {}, s3, poBucket: 'shipping-purchase-orders-test',
        publicS3Url: k => `https://po/${k}`, shipmentPaymentDocumentRowToJson: r => ({ id: r.id, filename: r.filename }),
        runShipmentPaymentExtraction: async (_pool, args) => { state.extractions.push(args); return { ok: true }; },
        log: { error() {}, warn() {}, info() {} },
    });
    const call = async (key, { params = {}, body = {}, query = {}, userType = 'standard' } = {}) => {
        const res = {
            statusCode: 200, body: null, headersSent: false,
            status(c) { this.statusCode = c; return this; },
            json(b) { this.body = b; this.headersSent = true; return this; },
        };
        await handlers[key]({ params, body, query, userType, userEmail: 'ops@example.com' }, res);
        return res;
    };
    return { state, call };
}

const IMPORT = 'POST /api/v1/ci-submissions/:id(\\d+)/import';
const importIt = (world, body = {}, extra = {}) => world.call(IMPORT, { params: { id: '9' }, body: { rowVersion: 2, shipmentId: 61, ...body }, ...extra });

test('import: files the invoice on the shipment, then starts the usual read', async () => {
    process.env.PI_UPLOADS_BUCKET = BUCKET;
    const { state, call } = routeWorld();
    const res = await call(IMPORT, { params: { id: '9' }, body: { rowVersion: 2, shipmentId: 61, note: 'Thanks' } });
    assert.equal(res.statusCode, 200, JSON.stringify(res.body));
    assert.equal(res.body.submission.status, 'imported');
    assert.deepEqual(res.body.submission.confirmed, { shipmentId: 61, shipmentReference: '324', supplierName: null });
    assert.equal(res.body.submission.documentId, 77);
    const [shipmentId, reference, supplierName, supplierKey, filename, s3Key, publicUrl, contentType, size, notes, uploadedBy] = state.inserted;
    assert.deepEqual([shipmentId, reference, supplierName, supplierKey, filename], [61, '324', 'Acme Ltd', 'acme ltd', 'CI-324.pdf']);
    assert.match(s3Key, /^shipment-payments\/[0-9a-f-]{36}\/CI-324\.pdf$/);
    assert.equal(publicUrl, `https://po/${s3Key}`);
    assert.deepEqual([contentType, size, uploadedBy], ['application/pdf', PDF.length, 'sales@acme.example']);
    assert.match(notes, /supplier portal \(invoice upload 9\)/);
    assert.equal(state.puts[0].Bucket, 'shipping-purchase-orders-test');
    assert.deepEqual(state.tx, ['begin', 'commit']);
    assert.deepEqual(state.audits, ['shipment_payment_document:create', 'ci_submission:import']);
    assert.equal(state.row.decided_note, 'Thanks');
    await new Promise(r => setImmediate(r));
    assert.deepEqual(state.extractions, [{ documentId: 77, userEmail: 'ops@example.com' }]);
});

test('import: the supplier it is filed under', async () => {
    process.env.PI_UPLOADS_BUCKET = BUCKET;
    const notOnBoard = routeWorld({ memberPos: [po(300, 'PO_00300J', 'Other Co', 5000)] });
    const refused = await importIt(notOnBoard);
    assert.deepEqual([refused.statusCode, refused.body.code], [422, 'SUPPLIER_NOT_ON_SHIPMENT']);
    assert.deepEqual(refused.body.suppliers.map(s => [s.supplierName, s.inGroup]), [['Other Co', false]]);
    assert.equal(notOnBoard.state.puts.length, 0, 'refused before anything is copied');
    const forced = await importIt(notOnBoard, { force: true });
    assert.equal(forced.statusCode, 200);
    assert.equal(notOnBoard.state.inserted[2], 'Acme Ltd', 'forced: filed under the supplier it came from');

    const other = routeWorld();
    assert.equal((await importIt(other, { supplierName: 'other co' })).statusCode, 200, 'another supplier on the box, chosen by staff');
    assert.equal(other.state.inserted[2], 'Other Co', 'the PO’s own spelling');
    assert.equal((await importIt(routeWorld(), { supplierName: 'Nobody Ltd' })).body.code, 'SUPPLIER_NOT_ON_SHIPMENT');
});

test('import: refusals', async () => {
    process.env.PI_UPLOADS_BUCKET = BUCKET;
    const dup = await importIt(routeWorld({ duplicate: 41 }));
    assert.deepEqual([dup.statusCode, dup.body.code, dup.body.existingId], [409, 'DUPLICATE_DOCUMENT', 41]);
    assert.equal((await importIt(routeWorld({ duplicate: 41 }), { force: true })).statusCode, 200);
    const stale = await importIt(routeWorld(), { rowVersion: 1 });
    assert.deepEqual([stale.statusCode, stale.body.code], [409, 'STALE']);
    const decided = await importIt(routeWorld({ row: submitted({ status: 'rejected' }) }));
    assert.deepEqual([decided.statusCode, decided.body.code], [409, 'NOT_SUBMITTED']);
    const merged = await importIt(routeWorld({ shipment: { ...SHIP_324, merged_into_id: 70, deleted_at: null } }));
    assert.deepEqual([merged.statusCode, merged.body.code, merged.body.mergedIntoId], [409, 'MERGED', 70]);
    const draftShip = await importIt(routeWorld({ shipment: { ...SHIP_324, stage: 'DRAFT', merged_into_id: null, deleted_at: null } }));
    assert.deepEqual([draftShip.statusCode, draftShip.body.code], [422, 'NOT_BOOKED']);
    const missingShip = await importIt(routeWorld(), { shipmentId: 999 });
    assert.deepEqual([missingShip.statusCode, missingShip.body.code], [404, 'SHIPMENT_NOT_FOUND']);
    const changed = await importIt(routeWorld({ etag: '"other"' }));
    assert.deepEqual(changed.body.blockers, ['file_changed']);
    const notPdf = await importIt(routeWorld({ object: Buffer.from('not a pdf at all') }));
    assert.deepEqual(notPdf.body.blockers, ['bad_type']);
    const readonly = await importIt(routeWorld(), {}, { userType: 'readonly' });
    assert.deepEqual([readonly.statusCode, readonly.body.code], [403, 'CANNOT_DECIDE']);
    const bad = await importIt(routeWorld(), { shipmentId: null });
    assert.deepEqual([bad.statusCode, bad.body.code], [400, 'BAD_FIELD']);
});
