'use strict';

// Unit tests for the supplier-portal PI check: the trigger rules
// (src/lib/pi-submissions.js parseTrigger / planEvent / claimWhere), the
// Gemini error classification and the retry deadline (src/lib/gemini-retry.js),
// and the runner (src/services/pi-check.js) against an in-memory row, a fake
// S3 and a fake model. No database, no AWS, no model call.
//   node --test tools/test-pi-check-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/pi-submissions');
const { classifyGeminiError, withGeminiRetry, isTransientGeminiError, DEADLINE_RESERVE_MS } = require('../src/lib/gemini-retry');
const { createPiChecker } = require('../src/services/pi-check');
const poCheck = require('../src/services/po-invoice-check');
const emailCheck = require('../src/services/supplier-email-check');

const KEY = 'pi/0a1b2c3d-1111-4222-8333-444455556666.pdf';
const NOW = Date.UTC(2026, 9, 20, 12, 0, 0);
const row = (over = {}) => ({
    id: 7, status: 'draft', check_status: 'awaiting_upload', check_attempt: 0, error_code: null,
    check_started_at: null, s3_key: KEY, content_type: 'application/pdf', ...over,
});

// ── parseTrigger ────────────────────────────────────────────────────────────
const s3Event = (key, bucket = 'jportal-pi-uploads-test') => ({
    source: 'aws.s3', 'detail-type': 'Object Created', detail: { bucket: { name: bucket }, object: { key, size: 10 } },
});

test('parseTrigger: an Object Created event for a PI key', () => {
    assert.deepEqual(L.parseTrigger(s3Event(KEY), { bucket: 'jportal-pi-uploads-test' }), { source: 'event', key: KEY });
});

test('parseTrigger: other keys, other buckets and junk are ignored', () => {
    assert.equal(L.parseTrigger(s3Event('pi/../etc.pdf'), { bucket: 'jportal-pi-uploads-test' }).ignore, 'not_a_pi_key');
    assert.equal(L.parseTrigger(s3Event('pi/0a1b2c3d-1111-4222-8333-444455556666.exe')).ignore, 'not_a_pi_key');
    assert.equal(L.parseTrigger(s3Event(KEY, 'someone-else'), { bucket: 'jportal-pi-uploads-test' }).ignore, 'other_bucket');
    assert.equal(L.parseTrigger({}).ignore, 'unrecognised_event');
    assert.equal(L.parseTrigger({ submissionId: 'x' }).ignore, 'unrecognised_event');
    assert.equal(L.parseTrigger(null).ignore, 'unrecognised_event');
});

test('parseTrigger: a direct invoke with a submission id', () => {
    assert.deepEqual(L.parseTrigger({ submissionId: 42 }), { source: 'direct', submissionId: 42 });
    assert.deepEqual(L.parseTrigger({ submissionId: '42' }), { source: 'direct', submissionId: 42 });
});

test('isPiKey: the shared key pattern', () => {
    assert.equal(L.isPiKey(KEY), true);
    assert.equal(L.isPiKey('pi/0a1b2c3d-1111-4222-8333-444455556666.png'), true);
    assert.equal(L.isPiKey('pi/0a1b2c3d-1111-4222-8333-444455556666.jpeg'), false);
    assert.equal(L.isPiKey('pi/0A1B2C3D-1111-4222-8333-444455556666.pdf'), false);
    assert.equal(L.isPiKey('invoices/x.pdf'), false);
    assert.equal(L.isPiKey(null), false);
});

// ── planEvent ───────────────────────────────────────────────────────────────
test('planEvent: the event path claims only awaiting_upload', () => {
    assert.equal(L.planEvent('event', row(), NOW).claim, true);
    assert.deepEqual(L.planEvent('event', row({ check_status: 'pending', check_started_at: new Date(NOW - 3600e3) }), NOW), { claim: false, reason: 'already_claimed' });
    assert.equal(L.planEvent('event', row({ check_status: 'failed', error_code: 'MODEL_UNAVAILABLE' }), NOW).claim, false);
});

test('planEvent: the direct path also claims stale pending and retryable failures', () => {
    assert.equal(L.planEvent('direct', row(), NOW).claim, true);
    const stale = row({ check_status: 'pending', check_started_at: new Date(NOW - (L.STALE_CHECK_SECONDS + 1) * 1000) });
    assert.deepEqual(L.planEvent('direct', stale, NOW), { claim: true, reason: 'stale_pending' });
    const fresh = row({ check_status: 'pending', check_started_at: new Date(NOW - 30 * 1000) });
    assert.deepEqual(L.planEvent('direct', fresh, NOW), { claim: false, reason: 'in_progress' });
    for (const code of L.RETRYABLE_CODES) {
        assert.equal(L.planEvent('direct', row({ check_status: 'failed', error_code: code }), NOW).claim, true, code);
    }
    for (const code of ['FILE_INVALID', 'DOCUMENT_UNPROCESSABLE', 'PO_NOT_FOUND', 'PO_NO_LINES']) {
        assert.deepEqual(L.planEvent('direct', row({ check_status: 'failed', error_code: code }), NOW), { claim: false, reason: 'not_retryable' }, code);
    }
    assert.equal(L.planEvent('direct', row({ check_status: 'succeeded' }), NOW).claim, false);
});

test('planEvent: SQL-computed age wins over the driver-read timestamp', () => {
    const r = row({ check_status: 'pending', check_started_at: new Date(NOW - 3600e3), check_age_seconds: 30 });
    assert.equal(L.planEvent('direct', r, NOW).claim, false);
    assert.equal(L.planEvent('direct', { ...r, check_age_seconds: 601 }, NOW).claim, true);
});

test('planEvent: never past 3 attempts, never on a submitted or missing row', () => {
    assert.deepEqual(L.planEvent('direct', row({ check_attempt: L.MAX_CHECK_ATTEMPTS }), NOW), { claim: false, reason: 'attempts_exhausted' });
    assert.deepEqual(L.planEvent('direct', row({ status: 'submitted' }), NOW), { claim: false, reason: 'not_draft' });
    assert.deepEqual(L.planEvent('event', null, NOW), { claim: false, reason: 'not_found' });
});

test('claimWhere: the event guard is narrower than the direct one', () => {
    const ev = L.claimWhere('event');
    assert.match(ev.sql, /status = 'draft' AND check_attempt < 3/);
    assert.doesNotMatch(ev.sql, /pending/);
    assert.deepEqual(ev.params, []);
    const d = L.claimWhere('direct');
    assert.match(d.sql, /INTERVAL 600 SECOND/);
    assert.deepEqual(d.params, L.RETRYABLE_CODES);
});

test('shared constants', () => {
    assert.equal(L.STALE_CHECK_SECONDS, 600);
    assert.equal(L.MAX_CHECK_ATTEMPTS, 3);
    assert.deepEqual(L.RETRYABLE_CODES, ['MODEL_UNAVAILABLE', 'NOT_CONFIGURED', 'MODEL_NO_JSON', 'MODEL_BAD_OUTPUT']);
    assert.equal(L.MAX_FILE_BYTES, 10 * 1024 * 1024);
    assert.equal(L.PI_KEY_RE.source, '^pi\\/[0-9a-f-]{36}\\.(pdf|png|jpg)$');
});

// ── classifyGeminiError ─────────────────────────────────────────────────────
const sdkErr = (status, message) => Object.assign(new Error(message || `got status: ${status}`), { status });
const wrapped = cause => Object.assign(new Error(`Gemini call failed: ${cause.message}`), { code: 'MODEL_CALL_FAILED', cause });

test('classifyGeminiError: the file is the problem', () => {
    assert.deepEqual(classifyGeminiError(wrapped(sdkErr(400, 'INVALID_ARGUMENT: bad document'))), { code: 'DOCUMENT_UNPROCESSABLE', retryable: false });
    assert.deepEqual(classifyGeminiError(wrapped(sdkErr(413))), { code: 'DOCUMENT_UNPROCESSABLE', retryable: false });
    assert.equal(classifyGeminiError(wrapped(new Error('The document has too many pages'))).code, 'DOCUMENT_UNPROCESSABLE');
});

test('classifyGeminiError: the model is unavailable (retryable)', () => {
    for (const e of [sdkErr(429), sdkErr(500), sdkErr(503, 'UNAVAILABLE: overloaded'), new Error('RESOURCE_EXHAUSTED'), new Error('The operation was aborted due to timeout'), new Error('fetch failed')]) {
        assert.deepEqual(classifyGeminiError(wrapped(e)), { code: 'MODEL_UNAVAILABLE', retryable: true }, e.message);
    }
});

test('classifyGeminiError: the key is the problem', () => {
    assert.deepEqual(classifyGeminiError(wrapped(sdkErr(401))), { code: 'NOT_CONFIGURED', retryable: false });
    assert.deepEqual(classifyGeminiError(wrapped(sdkErr(403, 'PERMISSION_DENIED'))), { code: 'NOT_CONFIGURED', retryable: false });
    assert.deepEqual(classifyGeminiError(Object.assign(new Error('no key'), { code: 'NOT_CONFIGURED' })), { code: 'NOT_CONFIGURED', retryable: false });
});

test('classifyGeminiError: codes already on the error pass through', () => {
    assert.equal(classifyGeminiError(Object.assign(new Error('x'), { code: 'MODEL_NO_JSON' })).code, 'MODEL_NO_JSON');
    assert.equal(classifyGeminiError(Object.assign(new Error('x'), { code: 'MODEL_BAD_OUTPUT' })).code, 'MODEL_BAD_OUTPUT');
    // Unknown: still something the supplier may check again.
    assert.deepEqual(classifyGeminiError(new Error('weird')), { code: 'MODEL_UNAVAILABLE', retryable: false });
});

// ── withGeminiRetry ─────────────────────────────────────────────────────────
test('withGeminiRetry: without a deadline it behaves as before (retries transient errors)', async () => {
    let calls = 0;
    const slept = [];
    const out = await withGeminiRetry('t', async () => { calls += 1; if (calls < 3) throw sdkErr(503); return 'ok'; }, 5, { sleep: async ms => { slept.push(ms); } });
    assert.equal(out, 'ok');
    assert.equal(calls, 3);
    assert.equal(slept.length, 2);
});

test('withGeminiRetry: a non-transient error is thrown at once', async () => {
    let calls = 0;
    await assert.rejects(withGeminiRetry('t', async () => { calls += 1; throw sdkErr(400); }, 5, { sleep: async () => {} }), /400/);
    assert.equal(calls, 1);
});

test('withGeminiRetry: stops retrying when less than 90 s would remain', async () => {
    let t = 0;
    let calls = 0;
    const deadline = DEADLINE_RESERVE_MS + 5000; // room for a retry or two (backoff 1.5 s, 3 s, + jitter)
    await assert.rejects(withGeminiRetry('t', async () => { calls += 1; throw sdkErr(503); }, 5, {
        deadline, now: () => t, sleep: async (ms) => { t += ms; },
    }), /503/);
    assert.ok(calls >= 2 && calls < 5, `calls=${calls}`);
    assert.ok(deadline - t >= DEADLINE_RESERVE_MS, 'never slept into the reserve');
    calls = 0;
    t = 0;
    await assert.rejects(withGeminiRetry('t', async () => { calls += 1; throw sdkErr(503); }, 5, {
        deadline: DEADLINE_RESERVE_MS - 1, now: () => t, sleep: async (ms) => { t += ms; },
    }));
    assert.equal(calls, 1, 'no retry at all when already inside the reserve');
});

test('supplier-email-check still exports the moved retry helpers', () => {
    assert.equal(emailCheck.withGeminiRetry, withGeminiRetry);
    assert.equal(emailCheck.isTransientGeminiError, isTransientGeminiError);
});

test('the supplier-portal addendum names the PO', () => {
    const s = poCheck.supplierPortalInstructions('PO_00412J');
    assert.match(s, /^The supplier uploaded this for PO PO_00412J\./);
    assert.match(s, /type `other`/);
    assert.match(s, /major_discrepancies/);
    assert.ok(poCheck.SUPPLIER_PORTAL_ADDENDUM.includes('{poNumber}'));
});

// ── The runner, against fakes ──────────────────────────────────────────────
const PDF = Buffer.concat([Buffer.from('%PDF-1.4\n'), Buffer.alloc(200, 0x20)]);
const PO = { poNumber: 'PO_1', supplier: 'Acme', currency: 'USD', lines: [{ sku: 'JF1', description: 'A', quantity: 1, unitPrice: 2, lineTotal: 2 }], subtotal: 2, shippingTotal: 0, grandTotal: 2 };
const RESULT = {
    summary: { matchedLines: 1, mismatches: 0, missingOnInvoice: 0, extraOnInvoice: 0 },
    header: { supplierMatches: true, supplierNotes: '', poNumberReferenced: true, currencyMatches: true, paymentTermsMatch: true, paymentTermsNotes: '' },
    totals: { poGrandTotal: 2, piGrandTotal: 2, delta: 0, notes: '' },
    discrepancies: [], paymentTerms: null, otherCharges: [], overallVerdict: 'pass', verdictExplanation: 'ok',
};

// A single pi_submissions row in memory, answering the runner's statements.
function fakeWorld({ dbRow, object = PDF, etag = '"abc"', po = PO, failWrite = false, check = async () => ({ result: RESULT, usage: { totalTokenCount: 10 }, model: 'm' }) } = {}) {
    const state = { row: { ...dbRow }, audits: [], objectExists: object !== null, getCalls: 0, headCalls: 0 };
    const conn = {
        async query(sql, params = []) {
            const r = state.row;
            const s = sql.replace(/\s+/g, ' ');
            if (/^SELECT id, status/.test(s)) {
                const match = /WHERE s3_key = \?/.test(s) ? r.s3_key === params[0] : Number(r.id) === Number(params[0]);
                return [match ? [{ ...r, check_age_seconds: r.check_age_seconds ?? null }] : []];
            }
            if (/SET check_status = 'pending'/.test(s)) {
                const direct = params.length > 1;
                const ok = r.status === 'draft' && r.check_attempt < 3 && (r.check_status === 'awaiting_upload'
                    || (direct && r.check_status === 'pending' && (r.check_age_seconds ?? 0) > 600)
                    || (direct && r.check_status === 'failed' && L.RETRYABLE_CODES.includes(r.error_code)));
                if (ok) Object.assign(r, { check_status: 'pending', check_attempt: r.check_attempt + 1, check_age_seconds: 0, error_code: null, row_version: r.row_version + 1 });
                return [{ affectedRows: ok ? 1 : 0 }];
            }
            if (/^SELECT check_attempt/.test(s)) return [[{ check_attempt: r.check_attempt }]];
            const guarded = () => r.check_attempt === params[params.length - 1] && r.check_status === 'pending' && r.status === 'draft';
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
                if (failWrite) throw new Error('db down');
                if (!guarded()) return [{ affectedRows: 0 }];
                Object.assign(r, { check_status: 'failed', error_code: params[0], error_message: params[1] });
                if (params[2]) r.po_snapshot_json = params[2];
                return [{ affectedRows: 1 }];
            }
            if (/SET check_status = 'succeeded'/.test(s)) {
                if (!guarded()) return [{ affectedRows: 0 }];
                Object.assign(r, { check_status: 'succeeded', verdict: params[0], discrepancy_count: params[1], result_json: params[2], po_snapshot_json: params[3] });
                return [{ affectedRows: 1 }];
            }
            if (/FROM purchase_orders po/.test(s)) return [po ? [{ id: r.purchase_order_id }] : []];
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
    const checker = createPiChecker({
        pool: { getConnection: async () => conn }, bucket: 'jportal-pi-uploads-test', s3,
        checkDocument: check, loadPoForCheck: async () => po, now: () => NOW,
    });
    return { state, checker };
}

const dbRow = (over = {}) => ({
    id: 7, status: 'draft', check_status: 'awaiting_upload', check_attempt: 0, error_code: null, s3_key: KEY, s3_etag: null,
    content_type: 'application/pdf', file_size: 1, purchase_order_id: 5, po_number: 'PO_1', supplier_name: 'Acme', row_version: 0, ...over,
});

test('runner: the event path claims once; a duplicate event is a no-op', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow() });
    const first = await checker.run(s3Event(KEY));
    assert.equal(first.outcome, 'succeeded');
    assert.equal(state.row.check_status, 'succeeded');
    assert.equal(state.row.check_attempt, 1);
    assert.equal(state.row.s3_etag, '"abc"');
    assert.equal(state.row.file_size, PDF.length);
    assert.deepEqual(JSON.parse(state.row.po_snapshot_json), PO);
    assert.deepEqual(state.audits, ['check_succeeded']);
    const again = await checker.run(s3Event(KEY));
    assert.equal(again.outcome, 'skipped');
    assert.equal(state.row.check_attempt, 1);
});

test('runner: the direct path with a missing object does not claim or use an attempt', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), object: null });
    const out = await checker.run({ submissionId: 7 });
    assert.deepEqual(out, { outcome: 'skipped', submissionId: 7, reason: 'file_missing' });
    assert.equal(state.row.check_attempt, 0);
    assert.equal(state.row.check_status, 'awaiting_upload');
    assert.equal(state.getCalls, 0);
});

test('runner: an object gone after the claim gives the attempt back', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), object: null });
    // The event path does not HEAD first, so it claims and then finds nothing.
    const out = await checker.run(s3Event(KEY));
    assert.equal(out.outcome, 'released');
    assert.equal(state.row.check_status, 'awaiting_upload');
    assert.equal(state.row.check_attempt, 0);
});

test('runner: a .txt sent as a PDF fails FILE_INVALID before the model', async () => {
    let called = false;
    const { state, checker } = fakeWorld({ dbRow: dbRow(), object: Buffer.from('hello, not a pdf'), check: async () => { called = true; } });
    const out = await checker.run(s3Event(KEY));
    assert.equal(out.code, 'FILE_INVALID');
    assert.equal(state.row.error_code, 'FILE_INVALID');
    assert.equal(called, false);
    assert.equal(state.row.s3_etag, null, 'an invalid file is never pinned');
});

test('runner: a PO no longer the supplier’s fails PO_NOT_FOUND', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), po: null });
    assert.equal((await checker.run({ submissionId: 7 })).code, 'PO_NOT_FOUND');
    assert.equal(state.row.check_status, 'failed');
});

test('runner: model errors are classified; a retryable failure can be checked again directly', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), check: async () => { throw wrapped(sdkErr(400, 'INVALID_ARGUMENT')); } });
    assert.equal((await checker.run(s3Event(KEY))).code, 'DOCUMENT_UNPROCESSABLE');
    assert.equal((await checker.run({ submissionId: 7 })).reason, 'not_retryable');

    const w2 = fakeWorld({ dbRow: dbRow(), check: async () => { throw wrapped(sdkErr(503)); } });
    assert.equal((await w2.checker.run(s3Event(KEY))).code, 'MODEL_UNAVAILABLE');
    assert.equal(w2.state.row.check_attempt, 1);
    assert.equal((await w2.checker.run({ submissionId: 7 })).code, 'MODEL_UNAVAILABLE');
    assert.equal(w2.state.row.check_attempt, 2);
    assert.deepEqual(w2.state.audits, ['check_failed', 'check_failed']);
});

test('runner: output that fails the shape check is MODEL_BAD_OUTPUT', async () => {
    const { state, checker } = fakeWorld({ dbRow: dbRow(), check: async () => ({ result: { overallVerdict: 'fine' }, usage: null, model: 'm' }) });
    assert.equal((await checker.run(s3Event(KEY))).code, 'MODEL_BAD_OUTPUT');
    assert.equal(state.row.error_code, 'MODEL_BAD_OUTPUT');
});

test('runner: the supplier-portal addendum and the deadline reach the model', async () => {
    let seen = null;
    const { checker } = fakeWorld({ dbRow: dbRow(), check: async (args) => { seen = args; return { result: RESULT, usage: null, model: 'm' }; } });
    await checker.run(s3Event(KEY), { deadline: NOW + 200000 });
    assert.match(seen.extraInstructions, /for PO PO_1\./);
    assert.equal(seen.deadline, NOW + 200000);
    assert.equal(seen.mimeType, 'application/pdf');
});

test('runner: a third attempt is the last', async () => {
    const { checker } = fakeWorld({ dbRow: dbRow({ check_status: 'failed', error_code: 'MODEL_UNAVAILABLE', check_attempt: 3 }) });
    assert.equal((await checker.run({ submissionId: 7 })).reason, 'attempts_exhausted');
});

test('runner: a write that fails while recording the failure throws', async () => {
    const { checker } = fakeWorld({ dbRow: dbRow(), failWrite: true, check: async () => { throw wrapped(sdkErr(503)); } });
    await assert.rejects(checker.run(s3Event(KEY)), /db down/);
});
