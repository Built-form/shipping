'use strict';

// Bank-detail verification on a PI's payment row
// (purchase_order_invoice_payments.bank_*; migration
// src/db/migrate/2026-10-20_30_invoice_bank_verification.sql). Pure: no
// database, no clock.
//
// A PI that came through the supplier portal (purchase_order_invoices
// .pi_submission_id set) is payable ONLY when its payment row exists and its
// bank_verification_status is 'verified': a supplier, or someone who got into
// their portal account, could otherwise slip new bank details in with a PI.
// Verified means a person confirmed the details (phone call to a known number,
// an existing email thread, …) or they match the supplier's last verified PI.
// Staff-uploaded and legacy PIs have a NULL status ("not applicable") and are
// not gated.
//
// The status follows the details: the payment row keeps a fingerprint of the
// details now on it (bank_fingerprint) and of the details someone verified
// (bank_verified_fingerprint). Any write of new details recomputes
//   status = fingerprint non-empty AND fingerprint = verified fingerprint
//            ? 'verified' : 'needs_verification'
// so a re-check that reads different details sends the PI back to unverified.

const crypto = require('crypto');

const VERIFIED = 'verified';
const NEEDS_VERIFICATION = 'needs_verification';
const VERIFICATION_STATUSES = [VERIFIED, NEEDS_VERIFICATION];

// The methods a person can record. 'matches_previous' is written by the import
// itself when the details equal the supplier's last verified ones.
const VERIFY_METHODS = ['phone_known_number', 'existing_email_thread', 'other'];
const MATCHES_PREVIOUS = 'matches_previous';

// BankDetails as the API carries them (camelCase), with the payment-row column
// each lives in and the column's width.
const BANK_FIELDS = [
    { key: 'beneficiaryName', col: 'beneficiary_name', max: 255 },
    { key: 'bankName', col: 'bank_name', max: 255 },
    { key: 'bankAddress', col: 'bank_address', max: 500 },
    { key: 'accountNumber', col: 'account_number', max: 100 },
    { key: 'iban', col: 'iban', max: 64 },
    { key: 'swiftBic', col: 'swift_bic', max: 32 },
    { key: 'intermediaryBank', col: 'intermediary_bank', max: 255 },
];
// The fields that identify where money goes. The bank's address is shown but
// not compared: it is free text the model reformats from one read to the next.
const COMPARED_FIELDS = ['beneficiaryName', 'bankName', 'iban', 'accountNumber', 'swiftBic', 'intermediaryBank'];

const NOTE_MAX = 1000;

// A display value: trimmed text cut to the column, or null.
function clean(v, max) {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s ? s.slice(0, max) : null;
}

// BankDetails from anything carrying the fields: the model's paymentTerms, an
// API body, or (with fromRow) a payment row's columns. Every key is present.
function bankDetailsOf(source, { fromRow = false } = {}) {
    const src = source && typeof source === 'object' ? source : {};
    const out = {};
    for (const f of BANK_FIELDS) out[f.key] = clean(fromRow ? src[f.col] : src[f.key], f.max);
    return out;
}

const hasBankDetails = details => BANK_FIELDS.some(f => details && clean(details[f.key], f.max) !== null);

// One field normalised for comparing: Unicode-folded, upper case, letters and
// digits only (so "ACME Co., Ltd" = "acme co ltd" and "GB29 NWBK…" = "gb29nwbk…").
function normField(v) {
    if (v === null || v === undefined) return null;
    const s = String(v).normalize('NFKC').toUpperCase().replace(/[^\p{L}\p{N}]+/gu, '');
    return s || null;
}

// The compared fields, normalised. Accepts BankDetails or the model's paymentTerms.
function normaliseBank(terms) {
    const src = terms && typeof terms === 'object' ? terms : {};
    const out = {};
    for (const key of COMPARED_FIELDS) out[key] = normField(src[key]);
    return out;
}

// sha256 (hex) of the normalised compared fields, or '' when there are none.
function bankFingerprint(terms) {
    const n = normaliseBank(terms);
    if (COMPARED_FIELDS.every(k => n[k] === null)) return '';
    return crypto.createHash('sha256').update(JSON.stringify(COMPARED_FIELDS.map(k => n[k]))).digest('hex');
}

/**
 * The details on a PI against the supplier's previous ones.
 *   previous: BankDetails of the supplier's last verified (or legacy) PI, or null.
 *   current:  BankDetails read from this PI, or null.
 * → { state, fields } — state:
 *   not_extracted  nothing was read from this PI (whatever the history)
 *   no_history     nothing to compare with
 *   unchanged      the compared fields are equal once normalised
 *   changed        they differ; fields lists each one that does
 * fields: [{ field, previous, now }] (display values) for `changed`, and every
 * field read for `no_history`.
 * (A check that is not usable is reported `not_checked` by the routes, which
 * never call this then.)
 */
function compareBankDetails(previous, current) {
    const cur = bankDetailsOf(current);
    if (!bankFingerprint(cur)) return { state: 'not_extracted', fields: [] };
    const prev = previous ? bankDetailsOf(previous) : null;
    if (!prev || !bankFingerprint(prev)) {
        return {
            state: 'no_history',
            fields: COMPARED_FIELDS.filter(k => cur[k] !== null).map(k => ({ field: k, previous: null, now: cur[k] })),
        };
    }
    const fields = COMPARED_FIELDS
        .filter(k => normField(prev[k]) !== normField(cur[k]))
        .map(k => ({ field: k, previous: prev[k], now: cur[k] }));
    return { state: fields.length ? 'changed' : 'unchanged', fields };
}

/**
 * The details on a PI against SEVERAL references (a supplier can hold more
 * than one account in JFPro — one per currency, say). Same states as
 * compareBankDetails: `unchanged` when ANY reference matches (referenceIndex
 * says which); `changed` reports the closest reference's differing fields,
 * so one wrong digit is one changed field, not a wall of them.
 *   references: BankDetails[] (empty / detail-less entries are skipped)
 *   current:    BankDetails or the model's paymentTerms
 * → { state, fields, referenceIndex: index into `references` or null }
 */
function bestBankComparison(references, current) {
    const cur = bankDetailsOf(current);
    if (!bankFingerprint(cur)) return { state: 'not_extracted', fields: [], referenceIndex: null };
    const refs = (Array.isArray(references) ? references : [])
        .map((r, i) => ({ details: bankDetailsOf(r), i }))
        .filter((r) => bankFingerprint(r.details));
    if (!refs.length) {
        return {
            state: 'no_history',
            fields: COMPARED_FIELDS.filter(k => cur[k] !== null).map(k => ({ field: k, previous: null, now: cur[k] })),
            referenceIndex: null,
        };
    }
    let best = null;
    for (const r of refs) {
        const cmp = compareBankDetails(r.details, cur);
        if (cmp.state === 'unchanged') return { state: 'unchanged', fields: [], referenceIndex: r.i };
        if (!best || cmp.fields.length < best.cmp.fields.length) best = { cmp, i: r.i };
    }
    return { state: 'changed', fields: best.cmp.fields, referenceIndex: best.i };
}

// The status a payment row ends with, given the fingerprint of the details on
// it and the fingerprint someone verified.
function statusFor(fingerprint, verifiedFingerprint) {
    return fingerprint && fingerprint === verifiedFingerprint ? VERIFIED : NEEDS_VERIFICATION;
}

/**
 * Whether money may be sent against a PI.
 *   invoice: { pi_submission_id } (or { piSubmissionId })
 *   payment: its purchase_order_invoice_payments row ({ bank_verification_status }), or null.
 * A portal PI is payable only with a payment row that is verified; any other
 * PI is not affected.
 */
function isPayable({ invoice, payment }) {
    const sid = invoice ? (invoice.pi_submission_id ?? invoice.piSubmissionId ?? null) : null;
    if (sid === null || sid === undefined) return true;
    if (!payment) return false;
    return (payment.bank_verification_status ?? payment.bankVerificationStatus) === VERIFIED;
}

// The JSON column bank_verified_json, as mysql2 hands it back (object, or a
// string on some servers), as BankDetails or null.
function parseVerifiedJson(value) {
    let v = value;
    if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch { return null; }
    }
    return v && typeof v === 'object' && !Array.isArray(v) ? bankDetailsOf(v) : null;
}

/**
 * The body of POST /purchase-order-invoices/:id/verify-bank, or of the
 * import's bankVerification: { method, note?, bankDetails? }.
 * → { value: { method, note, bankDetails|null } } or { error }.
 * `allowDetails` is false for the import (the details come from the check).
 */
function parseVerification(raw, { allowDetails = true } = {}) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return { error: 'bank verification must be an object { method, note? }.' };
    if (!VERIFY_METHODS.includes(raw.method)) return { error: `method must be one of ${VERIFY_METHODS.join(', ')}.` };
    let note = null;
    if (raw.note !== undefined && raw.note !== null && raw.note !== '') {
        if (typeof raw.note !== 'string') return { error: 'note must be a string.' };
        note = raw.note.trim() || null;
        if (note && note.length > NOTE_MAX) return { error: `note must be at most ${NOTE_MAX} characters.` };
    }
    if (raw.method === 'other' && !note) return { error: 'A note is required when the method is "other": say how the details were confirmed.' };
    let bankDetails = null;
    if (raw.bankDetails !== undefined && raw.bankDetails !== null) {
        if (!allowDetails) return { error: 'bankDetails cannot be sent here.' };
        if (typeof raw.bankDetails !== 'object' || Array.isArray(raw.bankDetails)) return { error: 'bankDetails must be an object.' };
        for (const f of BANK_FIELDS) {
            const v = raw.bankDetails[f.key];
            if (v !== undefined && v !== null && typeof v !== 'string') return { error: `bankDetails.${f.key} must be a string or null.` };
            if (typeof v === 'string' && v.trim().length > f.max) return { error: `bankDetails.${f.key} must be at most ${f.max} characters.` };
        }
        bankDetails = bankDetailsOf(raw.bankDetails);
        if (!bankFingerprint(bankDetails)) return { error: 'bankDetails holds no account details (beneficiary, bank, IBAN, account number, SWIFT or intermediary bank).' };
    }
    return { value: { method: raw.method, note, bankDetails } };
}

// The verification fields of a payment row for the API.
function verificationToJson(r) {
    if (!r) return {};
    const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));
    return {
        bankVerificationStatus: r.bank_verification_status || null,
        bankVerified: parseVerifiedJson(r.bank_verified_json),
        bankVerifiedBy: r.bank_verified_by || null,
        bankVerifiedAt: iso(r.bank_verified_at),
        bankVerificationMethod: r.bank_verification_method || null,
        bankVerificationNote: r.bank_verification_note || null,
    };
}

module.exports = {
    VERIFIED,
    NEEDS_VERIFICATION,
    VERIFICATION_STATUSES,
    VERIFY_METHODS,
    MATCHES_PREVIOUS,
    BANK_FIELDS,
    COMPARED_FIELDS,
    bankDetailsOf,
    hasBankDetails,
    normaliseBank,
    bankFingerprint,
    compareBankDetails,
    bestBankComparison,
    statusFor,
    isPayable,
    parseVerifiedJson,
    parseVerification,
    verificationToJson,
};
