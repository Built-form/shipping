'use strict';

// Unit tests for the supplier-portal PI review rules (src/lib/pi-submissions.js),
// bank verification (src/lib/bank-verification.js) and the payment-row upsert
// for portal PIs (upsertInvoicePaymentTerms in src/services/po-invoice-check.js,
// against a fake connection). No database, no AWS.
//   node --test tools/test-pi-submissions-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/pi-submissions');
const B = require('../src/lib/bank-verification');
const { upsertInvoicePaymentTerms } = require('../src/services/po-invoice-check');
const { DEFAULT_EMAIL_TEMPLATES, SUPPLIER_PI_TEMPLATE_KEYS } = require('../src/lib/email-template-defaults');

const KEY = 'pi/0a1b2c3d-1111-4222-8333-444455556666.pdf';
const submitted = (over = {}) => ({
    id: 11, status: 'submitted', s3_key: KEY, content_type: 'application/pdf', file_size: 2048,
    s3_etag: '"abc"', purchase_order_id: 5, po_number: 'PO_00412J', supplier_id: 3, supplier_name: 'Acme Ltd',
    filename: 'PI-412.pdf', submitted_by_email: 'sales@acme.example', row_version: 4, ...over,
});

// ── Files ──────────────────────────────────────────────────────────────────
test('sniffType: magic bytes, not the name', () => {
    assert.equal(L.sniffType(Buffer.from('%PDF-1.7\n...')), 'application/pdf');
    assert.equal(L.sniffType(Buffer.concat([Buffer.alloc(500, 0x20), Buffer.from('%PDF-1.4')])), 'application/pdf', 'within the first 1024 bytes');
    assert.equal(L.sniffType(Buffer.concat([Buffer.alloc(1030, 0x20), Buffer.from('%PDF-1.4')])), null, 'but not later');
    assert.equal(L.sniffType(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0])), 'image/png');
    assert.equal(L.sniffType(new Uint8Array([0xff, 0xd8, 0xff, 0xe0])), 'image/jpeg');
    assert.equal(L.sniffType(Buffer.from('<html>')), null);
    assert.equal(L.sniffType(Buffer.alloc(0)), null);
});

test('safeFilename / destKey: ASCII names keep their stem; an all-Chinese name falls back', () => {
    assert.equal(L.safeFilename('PI 412 (final).PDF', 'application/pdf'), 'PI_412_final.pdf');
    assert.equal(L.safeFilename('scan.jpeg', 'image/jpeg'), 'scan.jpg');
    assert.equal(L.safeFilename('形式发票.pdf', 'application/pdf'), '');
    assert.equal(L.destKey({ submissionId: 11, filename: '形式发票.pdf', contentType: 'application/pdf', token: 'T' }), 'invoices/T/pi-11.pdf');
    assert.equal(L.destKey({ submissionId: 11, filename: '___.png', contentType: 'image/png', token: 'T' }), 'invoices/T/pi-11.png');
    assert.equal(L.destKey({ submissionId: 11, filename: 'PI-412.pdf', contentType: 'application/pdf', token: 'T' }), 'invoices/T/PI-412.pdf');
    assert.equal(L.destKey({ submissionId: 11, filename: '../../evil.html', contentType: 'application/pdf', token: 'T' }), 'invoices/T/evil.pdf');
});

// ── Blockers ───────────────────────────────────────────────────────────────
test('importBlockers: a clean submitted row has none', () => {
    assert.deepEqual(L.importBlockers({ row: submitted(), po: { exists: true, owned: true } }), []);
    assert.deepEqual(L.importBlockers({ row: submitted(), po: { exists: true, owned: true }, file: { gone: false, changed: false } }), []);
});

test('importBlockers: each reason', () => {
    const po = { exists: true, owned: true };
    assert.deepEqual(L.importBlockers({ row: submitted({ status: 'withdrawn' }), po }), ['not_submitted']);
    assert.deepEqual(L.importBlockers({ row: submitted(), po: { exists: false, owned: false } }), ['po_deleted']);
    assert.deepEqual(L.importBlockers({ row: submitted(), po: { exists: true, owned: false } }), ['po_supplier_changed']);
    assert.deepEqual(L.importBlockers({ row: submitted({ s3_key: 'pi/elsewhere.pdf' }), po }), ['bad_key']);
    assert.deepEqual(L.importBlockers({ row: submitted({ content_type: 'text/html' }), po }), ['bad_type']);
    assert.deepEqual(L.importBlockers({ row: submitted({ content_type: 'image/png' }), po }), ['bad_type'], 'type must match the key’s extension');
    assert.deepEqual(L.importBlockers({ row: submitted({ file_size: L.MAX_FILE_BYTES + 1 }), po }), ['too_large']);
    assert.deepEqual(L.importBlockers({ row: submitted({ s3_etag: null }), po }), ['no_etag']);
    assert.deepEqual(L.importBlockers({ row: submitted(), po, file: { gone: true } }), ['file_gone']);
    assert.deepEqual(L.importBlockers({ row: submitted(), po, file: { changed: true } }), ['file_changed']);
    for (const code of Object.keys(L.BLOCKERS)) assert.equal(typeof L.BLOCKERS[code], 'string');
});

test('canDecide: admin and standard only', () => {
    assert.equal(L.canDecide('admin'), true);
    assert.equal(L.canDecide('standard'), true);
    assert.equal(L.canDecide('readonly'), false);
    assert.equal(L.canDecide('accountant'), false);
    assert.equal(L.canDecide(undefined), false);
});

// ── Check result ───────────────────────────────────────────────────────────
const RESULT = {
    summary: { matchedLines: 1, mismatches: 1, missingOnInvoice: 0, extraOnInvoice: 0 },
    header: { supplierMatches: true }, totals: { poGrandTotal: 10, piGrandTotal: 11 },
    discrepancies: [{ type: 'unit_price_mismatch', sku: 'JF1', description: 'A', po: 1, pi: 1.1 }],
    paymentTerms: { beneficiaryName: 'Acme Ltd', iban: 'GB29 NWBK 6016 1331 9268 19' },
    otherCharges: [], overallVerdict: 'minor_discrepancies', verdictExplanation: '',
};

test('validateCheckResult: recomputes verdict and count; refuses malformed output', () => {
    assert.deepEqual(L.validateCheckResult(RESULT), { ok: true, verdict: 'minor_discrepancies', discrepancyCount: 1 });
    assert.deepEqual(L.validateCheckResult(JSON.stringify(RESULT)), { ok: true, verdict: 'minor_discrepancies', discrepancyCount: 1 });
    assert.equal(L.validateCheckResult({ ...RESULT, overallVerdict: 'ok' }).ok, false);
    assert.equal(L.validateCheckResult({ ...RESULT, discrepancies: [{ type: 'nope' }] }).ok, false);
    assert.equal(L.validateCheckResult({ ...RESULT, totals: null }).ok, false);
    assert.equal(L.validateCheckResult('not json').ok, false);
    assert.equal(L.validateCheckResult(null).ok, false);
});

test('checkUsability: only a succeeded check whose stored verdict matches', () => {
    const ok = { check_status: 'succeeded', verdict: 'minor_discrepancies', discrepancy_count: 1, result_json: RESULT };
    assert.equal(L.checkUsability(ok).usable, true);
    assert.deepEqual(L.checkUsability({ ...ok, check_status: 'failed' }), { usable: false, reason: 'check_failed', result: null });
    assert.equal(L.checkUsability({ ...ok, check_status: 'pending' }).reason, 'check_not_finished');
    assert.equal(L.checkUsability({ ...ok, verdict: 'pass' }).reason, 'verdict_mismatch');
    assert.equal(L.checkUsability({ ...ok, discrepancy_count: 3 }).reason, 'verdict_mismatch');
    assert.match(L.checkUsability({ ...ok, result_json: '{}' }).reason, /^bad_result:/);
});

// ── PO diff ────────────────────────────────────────────────────────────────
const PO = {
    poNumber: 'PO_00412J', supplier: 'Acme Ltd', buyer: 'JFA', currency: 'USD', orderDate: '2026-10-01T09:00:00.000Z', notes: null,
    lines: [
        { sku: 'JF1', description: 'Hanger', quantity: 100, unitPrice: 1.25, lineTotal: 125 },
        { sku: 'JF2', description: 'Clip', quantity: 50, unitPrice: 0.5, lineTotal: 25 },
    ],
    subtotal: 150, shippingTotal: 0, grandTotal: 150,
};

test('diffPo: an unchanged PO (a Date now vs the stored string) is not changed', () => {
    const now = { ...PO, orderDate: new Date('2026-10-01T09:00:00.000Z') };
    const d = L.diffPo(JSON.stringify(PO), now);
    assert.equal(d.comparable, true);
    assert.equal(d.changed, false);
});

test('diffPo: header, changed, added and removed lines', () => {
    const now = {
        ...PO, currency: 'EUR',
        lines: [
            { sku: 'JF1', description: 'Hanger', quantity: 120, unitPrice: 1.25, lineTotal: 150 },
            { sku: 'JF3', description: 'Hook', quantity: 1, unitPrice: 2, lineTotal: 2 },
        ],
    };
    const d = L.diffPo(PO, now);
    assert.equal(d.changed, true);
    assert.deepEqual(d.header, [{ field: 'currency', then: 'USD', now: 'EUR' }]);
    assert.deepEqual(d.lines.changed, [
        { key: 'JF1', field: 'quantity', then: 100, now: 120 },
        { key: 'JF1', field: 'lineTotal', then: 125, now: 150 },
    ]);
    assert.deepEqual(d.lines.removed.map(l => l.sku), ['JF2']);
    assert.deepEqual(d.lines.added.map(l => l.sku), ['JF3']);
});

test('diffPo: a SKU at two prices pairs up by occurrence; a missing side is not comparable', () => {
    const two = { ...PO, lines: [PO.lines[0], { ...PO.lines[0], unitPrice: 1.3, lineTotal: 130 }] };
    assert.equal(L.diffPo(two, two).changed, false);
    assert.deepEqual(L.diffPo(null, PO), { changed: false, comparable: false, header: [], lines: { added: [], removed: [], changed: [] } });
    assert.equal(L.diffPo(PO, null).comparable, false);
});

// ── Bank verification ──────────────────────────────────────────────────────
const BANK = { beneficiaryName: 'ACME Co., Ltd', bankName: 'HSBC', bankAddress: '1 Queen’s Rd', accountNumber: '123-456-789', iban: null, swiftBic: 'HSBCHKHH', intermediaryBank: null };

test('normaliseBank / bankFingerprint: formatting does not matter, digits do', () => {
    const tidy = { ...BANK, beneficiaryName: 'acme co ltd', accountNumber: '123456789', bankAddress: 'somewhere else' };
    assert.equal(B.bankFingerprint(BANK), B.bankFingerprint(tidy));
    assert.notEqual(B.bankFingerprint(BANK), B.bankFingerprint({ ...BANK, accountNumber: '123456780' }));
    assert.match(B.bankFingerprint(BANK), /^[0-9a-f]{64}$/);
    assert.equal(B.bankFingerprint({}), '');
    assert.equal(B.bankFingerprint(null), '');
    assert.equal(B.bankFingerprint({ bankAddress: 'only an address' }), '', 'the address alone identifies nothing');
    assert.deepEqual(B.normaliseBank({ iban: ' gb29 nwbk-6016 ' }).iban, 'GB29NWBK6016');
});

test('compareBankDetails: the four states', () => {
    assert.deepEqual(B.compareBankDetails(BANK, null), { state: 'not_extracted', fields: [] });
    assert.deepEqual(B.compareBankDetails(BANK, { bankAddress: 'x' }).state, 'not_extracted');
    const nh = B.compareBankDetails(null, BANK);
    assert.equal(nh.state, 'no_history');
    assert.deepEqual(nh.fields.map(f => f.field), ['beneficiaryName', 'bankName', 'accountNumber', 'swiftBic']);
    assert.equal(B.compareBankDetails({}, BANK).state, 'no_history');
    assert.deepEqual(B.compareBankDetails(BANK, { ...BANK, beneficiaryName: 'Acme Co Ltd' }), { state: 'unchanged', fields: [] });
    const ch = B.compareBankDetails(BANK, { ...BANK, accountNumber: '999', swiftBic: 'CHASUS33' });
    assert.equal(ch.state, 'changed');
    assert.deepEqual(ch.fields, [
        { field: 'accountNumber', previous: '123-456-789', now: '999' },
        { field: 'swiftBic', previous: 'HSBCHKHH', now: 'CHASUS33' },
    ]);
});

test('bestBankComparison: any matching reference is a match; changed reports the closest', () => {
    // No references at all, or none with details: no_history.
    assert.equal(B.bestBankComparison([], BANK).state, 'no_history');
    assert.equal(B.bestBankComparison([{ bankAddress: 'only an address' }], BANK).state, 'no_history');
    // Nothing read off the PI: not_extracted, whatever the references.
    assert.deepEqual(B.bestBankComparison([BANK], null), { state: 'not_extracted', fields: [], referenceIndex: null });
    // A second JFPro account (another currency) matching is a match, and says which.
    const other = { ...BANK, accountNumber: '111-222', swiftBic: 'CHASUS33' };
    const m = B.bestBankComparison([other, BANK], BANK);
    assert.deepEqual(m, { state: 'unchanged', fields: [], referenceIndex: 1 });
    // Nothing matches: changed, against the reference with the FEWEST differing
    // fields — one wrong digit, not a wall of fields from the wrong account.
    const oneOff = { ...BANK, accountNumber: '123-456-780' };
    const ch = B.bestBankComparison([other, BANK], oneOff);
    assert.equal(ch.state, 'changed');
    assert.equal(ch.referenceIndex, 1);
    assert.deepEqual(ch.fields, [{ field: 'accountNumber', previous: '123-456-789', now: '123-456-780' }]);
});

test('isPayable: portal PIs need a verified payment row; others are not affected', () => {
    assert.equal(B.isPayable({ invoice: { pi_submission_id: null }, payment: null }), true);
    assert.equal(B.isPayable({ invoice: { pi_submission_id: null }, payment: { bank_verification_status: 'needs_verification' } }), true);
    assert.equal(B.isPayable({ invoice: { pi_submission_id: 9 }, payment: null }), false, 'no payment row: not payable');
    assert.equal(B.isPayable({ invoice: { pi_submission_id: 9 }, payment: { bank_verification_status: null } }), false);
    assert.equal(B.isPayable({ invoice: { pi_submission_id: 9 }, payment: { bank_verification_status: 'needs_verification' } }), false);
    assert.equal(B.isPayable({ invoice: { pi_submission_id: 9 }, payment: { bank_verification_status: 'verified' } }), true);
    assert.equal(B.isPayable({ invoice: { piSubmissionId: 9 }, payment: { bankVerificationStatus: 'verified' } }), true);
});

test('parseVerification: methods, the note for "other", typed details', () => {
    assert.deepEqual(B.parseVerification({ method: 'phone_known_number' }), { value: { method: 'phone_known_number', note: null, bankDetails: null } });
    assert.match(B.parseVerification({ method: 'other' }).error, /note is required/);
    assert.equal(B.parseVerification({ method: 'other', note: 'Met them' }).value.note, 'Met them');
    assert.match(B.parseVerification({ method: 'matches_previous' }).error, /method must be/);
    assert.match(B.parseVerification(null).error, /must be an object/);
    assert.match(B.parseVerification({ method: 'existing_email_thread', bankDetails: { iban: 'GB1' } }, { allowDetails: false }).error, /cannot be sent/);
    const v = B.parseVerification({ method: 'existing_email_thread', bankDetails: { iban: ' GB29 NWBK ' } });
    assert.equal(v.value.bankDetails.iban, 'GB29 NWBK');
    assert.equal(v.value.bankDetails.bankName, null);
    assert.match(B.parseVerification({ method: 'existing_email_thread', bankDetails: { bankAddress: 'x' } }).error, /no account details/);
    assert.match(B.parseVerification({ method: 'existing_email_thread', bankDetails: { iban: 5 } }).error, /must be a string/);
});

test('verificationToJson: the payment JSON fields', () => {
    assert.deepEqual(B.verificationToJson({}), {
        bankVerificationStatus: null, bankVerified: null, bankVerifiedBy: null, bankVerifiedAt: null,
        bankVerificationMethod: null, bankVerificationNote: null,
    });
    const j = B.verificationToJson({
        bank_verification_status: 'verified', bank_verified_json: JSON.stringify(BANK), bank_verified_by: 'a@b.c',
        bank_verified_at: new Date('2026-10-20T10:00:00Z'), bank_verification_method: 'other', bank_verification_note: 'n',
    });
    assert.equal(j.bankVerified.swiftBic, 'HSBCHKHH');
    assert.equal(j.bankVerifiedAt, '2026-10-20T10:00:00.000Z');
});

// ── upsertInvoicePaymentTerms on a portal PI (fake connection) ─────────────
// A tiny store for one invoice's payment row: enough to run the upsert's SQL
// shape and check the status it computes and the audit rows it writes.
function fakeConn({ piSubmissionId, existing = null }) {
    const state = { row: existing ? { ...existing } : null, audits: [], statements: [] };
    return {
        state,
        async query(sql, params = []) {
            const s = sql.replace(/\s+/g, ' ').trim();
            state.statements.push(s);
            if (/^SELECT pi_submission_id FROM purchase_order_invoices/.test(s)) return [[{ pi_submission_id: piSubmissionId }]];
            if (/^SELECT \* FROM purchase_order_invoice_payments/.test(s)) return [state.row ? [{ ...state.row }] : []];
            if (/^SELECT bank_verification_status FROM purchase_order_invoice_payments/.test(s)) return [state.row ? [{ bank_verification_status: state.row.bank_verification_status }] : []];
            if (/^INSERT INTO purchase_order_invoice_payments/.test(s)) {
                const cols = /\(([^)]*)\) VALUES/.exec(s)[1].split(',').map(c => c.trim());
                const vals = {};
                cols.forEach((c, i) => { if (i < params.length) vals[c] = params[i]; });
                if (!state.row) {
                    state.row = { ...vals };
                } else {
                    // ON DUPLICATE KEY UPDATE, as MySQL applies it (left to right).
                    for (const [c, v] of Object.entries(vals)) {
                        if (['purchase_order_invoice_id', 'purchase_order_id', 'bank_verification_status'].includes(c)) continue;
                        state.row[c] = v;
                    }
                    const fp = state.row.bank_fingerprint;
                    state.row.bank_verification_status = fp && fp === state.row.bank_verified_fingerprint ? 'verified' : 'needs_verification';
                }
                return [{ affectedRows: 1 }];
            }
            if (/^INSERT INTO audit_log/.test(s)) { state.audits.push({ action: params[2], after: JSON.parse(params[4]) }); return [{ insertId: 1 }]; }
            throw new Error(`unexpected SQL: ${s.slice(0, 80)}`);
        },
    };
}

test('upsert: a legacy PI is written as before (no verification columns, nothing without terms)', async () => {
    const conn = fakeConn({ piSubmissionId: null });
    await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, checkId: 2, paymentTerms: null, modelUsed: 'm' });
    assert.equal(conn.state.row, null);
    await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, checkId: 2, paymentTerms: { bankName: 'X' }, modelUsed: 'm' });
    const insert = conn.state.statements.find(s => s.startsWith('INSERT INTO purchase_order_invoice_payments'));
    assert.doesNotMatch(insert, /bank_fingerprint|bank_verification_status/);
});

test('upsert: a portal PI with no usable check gets an empty, unverified row', async () => {
    const conn = fakeConn({ piSubmissionId: 9 });
    const out = await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, checkId: null, paymentTerms: null });
    assert.equal(out.status, 'needs_verification');
    assert.equal(conn.state.row.bank_fingerprint, '');
    assert.equal(conn.state.row.beneficiary_name, null);
    // Called again with no terms (it already exists): left alone.
    const again = await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, paymentTerms: null });
    assert.equal(again.created, false);
});

test('upsert: verified at import, then a re-check with other details flips it and audits once', async () => {
    const conn = fakeConn({ piSubmissionId: 9 });
    const fp = B.bankFingerprint(BANK);
    const out = await upsertInvoicePaymentTerms(conn, {
        invoiceId: 1, purchaseOrderId: 5, checkId: 2, paymentTerms: BANK, modelUsed: 'm',
        verification: { fingerprint: fp, details: BANK, method: 'phone_known_number', note: null, by: 'staff@x' }, userEmail: 'staff@x',
    });
    assert.equal(out.status, 'verified');
    assert.equal(conn.state.row.bank_verified_fingerprint, fp);
    assert.equal(conn.state.audits.length, 0, 'no transient needs_verification, no audit row');
    const insert = conn.state.statements.find(s => s.startsWith('INSERT INTO purchase_order_invoice_payments'));
    assert.match(insert, /bank_verified_at = NOW\(\)/);
    assert.match(insert, /VALUES\(bank_fingerprint\) <> '' AND VALUES\(bank_fingerprint\) = bank_verified_fingerprint/);

    // The same details re-read (formatted differently): stays verified.
    const same = await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, checkId: 3, paymentTerms: { ...BANK, accountNumber: '123456789' } });
    assert.equal(same.status, 'verified');
    assert.equal(conn.state.audits.length, 0);

    const flipped = await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, checkId: 4, paymentTerms: { ...BANK, accountNumber: '000' }, userEmail: 'staff@x' });
    assert.equal(flipped.status, 'needs_verification');
    assert.equal(flipped.previousStatus, 'verified');
    assert.deepEqual(conn.state.audits.map(a => a.action), ['bank_details_unverified']);
    assert.equal(conn.state.audits[0].after.bank.accountNumber, '000');
});

test('upsert: not_extracted at import starts needs_verification', async () => {
    const conn = fakeConn({ piSubmissionId: 9 });
    const out = await upsertInvoicePaymentTerms(conn, { invoiceId: 1, purchaseOrderId: 5, checkId: 2, paymentTerms: { paymentType: 'deposit', amountDueNow: 30 } });
    assert.equal(out.status, 'needs_verification');
    assert.equal(conn.state.row.payment_type, 'deposit');
    assert.equal(conn.state.row.bank_fingerprint, '');
});

// ── Requests ───────────────────────────────────────────────────────────────
test('parseListQuery', () => {
    assert.deepEqual(L.parseListQuery({}).value, { statuses: ['submitted'], supplierId: null, purchaseOrderId: null, limit: 100, offset: 0 });
    assert.deepEqual(L.parseListQuery({ status: 'all' }).value.statuses, L.VISIBLE_STATUSES);
    assert.deepEqual(L.parseListQuery({ status: 'imported,rejected', supplierId: '3', limit: '9999', offset: '20' }).value,
        { statuses: ['imported', 'rejected'], supplierId: 3, purchaseOrderId: null, limit: 500, offset: 20 });
    assert.match(L.parseListQuery({ status: 'draft' }).error, /status must be/);
    assert.match(L.parseListQuery({ supplierId: 'x' }).error, /supplierId/);
    assert.match(L.parseListQuery({ offset: '-1' }).error, /offset/);
});

test('parseImportBody / parseRejectBody / parseNotifyBody', () => {
    assert.deepEqual(L.parseImportBody({ rowVersion: 4 }).value, { rowVersion: 4, note: null, recheck: false, bankVerification: null });
    assert.deepEqual(L.parseImportBody({ rowVersion: '4', note: ' ok ', recheck: true, bankVerification: { method: 'other' } }).value,
        { rowVersion: 4, note: 'ok', recheck: true, bankVerification: { method: 'other' } });
    assert.match(L.parseImportBody({}).error, /rowVersion is required/);
    assert.match(L.parseImportBody({ rowVersion: 1, recheck: 'yes' }).error, /recheck/);
    assert.match(L.parseImportBody({ rowVersion: 1, note: 'x'.repeat(1001) }).error, /1000/);
    assert.match(L.parseRejectBody({ rowVersion: 1 }).error, /note is required/);
    assert.match(L.parseRejectBody({ rowVersion: 1, note: '   ' }).error, /note is required/);
    assert.equal(L.parseRejectBody({ rowVersion: 1, note: 'Wrong currency' }).value.note, 'Wrong currency');
    assert.deepEqual(L.parseNotifyBody(undefined).value, { force: false });
    assert.match(L.parseNotifyBody({ force: 1 }).error, /force/);
});

test('submissionToJson: the list item shape', () => {
    const j = L.submissionToJson({
        ...submitted(), file_size: '2048', warnings_acknowledged: 1, check_status: 'succeeded', verdict: 'pass', discrepancy_count: 0,
        submitted_at_iso: '2026-10-20T09:00:00Z', imported_invoice_id: null, row_version: 4, supplier_note: 'see note',
    }, { poChanged: true, bankState: 'changed', blockers: ['no_etag'] });
    assert.deepEqual(Object.keys(j).sort(), [
        'bankCheck', 'blockers', 'checkStatus', 'contentType', 'decidedAt', 'decidedBy', 'decidedNote', 'discrepancyCount',
        'fileSize', 'filename', 'id', 'importedInvoiceId', 'notifiedAt', 'notifyError', 'poChanged', 'poNumber',
        'purchaseOrderId', 'rowVersion', 'status', 'submittedAt', 'submittedByEmail', 'supplierId', 'supplierName',
        'supplierNote', 'verdict', 'warningsAcknowledged',
    ]);
    assert.equal(j.fileSize, 2048);
    assert.equal(j.warningsAcknowledged, true);
    assert.deepEqual(j.bankCheck, { state: 'changed' });
    assert.equal(j.submittedAt, '2026-10-20T09:00:00Z');
});

// ── Email ──────────────────────────────────────────────────────────────────
test('buildEmailVars: escaped body tokens, plain subject, the right link', () => {
    const row = submitted({ filename: '<b>pi</b>.pdf', po_number: 'PO_1\r\nBcc: x' });
    const rej = L.buildEmailVars({ row, status: 'rejected', note: 'Price <wrong>\nplease fix', portalUrl: 'https://portal.example/' });
    assert.equal(rej.subject.poNumber, 'PO_1 Bcc: x', 'no header injection through the subject');
    assert.equal(rej.body.filename, '&lt;b&gt;pi&lt;/b&gt;.pdf');
    assert.equal(rej.body.note, 'Price &lt;wrong&gt;\nplease fix');
    assert.match(rej.body.noteHtml, /Price &lt;wrong&gt;<br>please fix/);
    assert.match(rej.body.portalLinkHtml, /href="https:\/\/portal\.example\/orders\/5\/proforma"/);
    const imp = L.buildEmailVars({ row, status: 'imported', note: null, portalUrl: 'https://portal.example' });
    assert.equal(imp.body.noteHtml, '');
    assert.match(imp.body.portalLinkHtml, /\/proforma-invoices\/11"/);
    assert.equal(L.buildEmailVars({ row, status: 'imported', note: null, portalUrl: '' }).body.portalLinkHtml, '');
    assert.equal(L.escapeHtml(`"'&`), '&quot;&#39;&amp;');
});

test('templateKeyFor and the seeded templates', () => {
    assert.equal(L.templateKeyFor('imported'), 'supplier_pi_imported');
    assert.equal(L.templateKeyFor('rejected'), 'supplier_pi_rejected');
    assert.equal(L.templateKeyFor('withdrawn'), null);
    for (const key of SUPPLIER_PI_TEMPLATE_KEYS) {
        const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === key);
        assert.ok(t, key);
        assert.equal(t.category, 'Supplier portal');
        for (const token of ['{poNumber}', '{filename}', '{noteHtml}', '{portalLinkHtml}']) assert.ok(t.bodyHtml.includes(token), `${key} ${token}`);
        assert.ok(/[一-鿿]/.test(t.bodyHtml), `${key} has Chinese text`);
    }
    assert.equal(DEFAULT_EMAIL_TEMPLATES.find(t => t.key === 'supplier_pi_imported').subject, 'PI accepted – {poNumber}');
    assert.equal(DEFAULT_EMAIL_TEMPLATES.find(t => t.key === 'supplier_pi_rejected').subject, 'PI not accepted – {poNumber}');
});
