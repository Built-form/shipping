'use strict';

// Unit tests for src/lib/payment-reviews.js — payment keys, request bodies,
// who may review / assign, and the review and assignment decisions behind
// /api/v1/payment-reviews and /api/v1/payment-assignees. No database.
//   node --test tools/test-payment-reviews-lib.js

const test = require('node:test');
const assert = require('node:assert/strict');
const L = require('../src/lib/payment-reviews');

test('two people must sign off', () => {
    assert.equal(L.REQUIRED_REVIEWS, 2);
});

test('deposit key: deposit:<purchase order id>', () => {
    assert.deepEqual(L.parsePaymentKey('deposit:306'), { key: 'deposit:306', kind: 'deposit', currency: null, containerRef: null });
    assert.deepEqual(L.parsePaymentKey('  deposit:306 '), { key: 'deposit:306', kind: 'deposit', currency: null, containerRef: null });
});

test('balance key: balance:<currency>:<container>|<supplier>, "-" = no container', () => {
    assert.deepEqual(
        L.parsePaymentKey('balance:USD:TIIU9074673|suzhou sunmed co.,ltd.'),
        { key: 'balance:USD:TIIU9074673|suzhou sunmed co.,ltd.', kind: 'balance', currency: 'USD', containerRef: 'TIIU9074673' },
    );
    assert.deepEqual(
        L.parsePaymentKey('balance:EUR:-|foreu'),
        { key: 'balance:EUR:-|foreu', kind: 'balance', currency: 'EUR', containerRef: null },
    );
    // A container number with spaces ("104. Air Freight") is a container number.
    assert.equal(L.parsePaymentKey('balance:USD:104. AIR FREIGHT|kingphar').containerRef, '104. AIR FREIGHT');
});

test('malformed keys are refused', () => {
    for (const bad of [
        null, undefined, '', 42, 'deposit:', 'deposit:0', 'deposit:-3', 'deposit:3.5', 'deposit:abc',
        'balance:usd:324|sunmed', 'balance:USD:324|', 'balance:USD:|sunmed', 'balance:USD:324', 'balance:US:324|x',
        'refund:12', `deposit:${'9'.repeat(300)}`,
    ]) {
        assert.ok(L.parsePaymentKey(bad).error, `expected an error for ${JSON.stringify(bad)}`);
    }
    // 'balance:USD:324|' is 16 characters.
    assert.equal(L.parsePaymentKey(`balance:USD:324|${'x'.repeat(239)}`).error, undefined); // 255 characters
    assert.ok(L.parsePaymentKey(`balance:USD:324|${'x'.repeat(240)}`).error);                // 256
});

test('review body: trimmed, amount to the cent, currency upper-cased', () => {
    const r = L.parseReviewBody({
        paymentKey: 'deposit:306', currency: 'usd', amount: '1234.567', supplierName: '  Suzhou Sunmed  ',
        poNumbers: ['PO_00306J', ' PO_00307J '], dueDate: '2026-10-01',
    });
    assert.equal(r.error, undefined);
    assert.deepEqual(r.value, {
        paymentKey: 'deposit:306', kind: 'deposit', currency: 'USD', amount: 1234.57,
        supplierName: 'Suzhou Sunmed', poNumbers: 'PO_00306J, PO_00307J', containerRef: null, dueDate: '2026-10-01',
    });
});

test('review body: a balance takes its container and currency from the key', () => {
    const r = L.parseReviewBody({ paymentKey: 'balance:USD:324|sunmed', currency: 'USD', amount: 2296 });
    assert.equal(r.error, undefined);
    assert.equal(r.value.kind, 'balance');
    assert.equal(r.value.containerRef, '324');
    assert.equal(r.value.supplierName, null);
    assert.equal(r.value.poNumbers, null);
    assert.equal(r.value.dueDate, null);
    assert.match(L.parseReviewBody({ paymentKey: 'balance:USD:324|sunmed', currency: 'EUR', amount: 2296 }).error, /currency/);
});

test('review body: refusals', () => {
    const ok = { paymentKey: 'deposit:306', currency: 'USD', amount: 100 };
    assert.match(L.parseReviewBody(null).error, /paymentKey/);
    assert.match(L.parseReviewBody({ ...ok, paymentKey: 'deposit:x' }).error, /paymentKey/);
    assert.match(L.parseReviewBody({ ...ok, amount: 0 }).error, /amount/);
    assert.match(L.parseReviewBody({ ...ok, amount: -5 }).error, /amount/);
    assert.match(L.parseReviewBody({ ...ok, amount: 'abc' }).error, /amount/);
    assert.match(L.parseReviewBody({ ...ok, amount: 0.004 }).error, /amount/);
    assert.match(L.parseReviewBody({ ...ok, currency: 'dollars' }).error, /currency/);
    assert.match(L.parseReviewBody({ ...ok, currency: undefined }).error, /currency/);
    assert.match(L.parseReviewBody({ ...ok, dueDate: '01/10/2026' }).error, /dueDate/);
    assert.match(L.parseReviewBody({ ...ok, poNumbers: 'PO_1' }).error, /poNumbers/);
    assert.match(L.parseReviewBody({ ...ok, poNumbers: [1] }).error, /poNumbers/);
});

test('who may review, who may assign, who can be assigned', () => {
    assert.equal(L.canReview('standard'), true);
    assert.equal(L.canReview('admin'), true);
    assert.equal(L.canReview('accountant'), false);
    assert.equal(L.canReview('exec'), false);
    assert.equal(L.canReview('warehouse'), false);
    assert.equal(L.canReview(undefined), false);
    assert.equal(L.canAssign('standard'), true);
    assert.equal(L.canAssign('admin'), true);
    assert.equal(L.canAssign('accountant'), true);
    assert.equal(L.canAssign('warehouse'), false);
    assert.equal(L.ASSIGNEE_ROLE, 'accountant');
});

const active = (id, email, amount, currency = 'USD') => ({ id, reviewed_by_email: email, amount: String(amount), currency });

test('review decision: a first review is created', () => {
    assert.deepEqual(
        L.decideReview({ active: [], userEmail: 'ann@x.com', assigneeEmail: null, amount: 100, currency: 'USD' }),
        { action: 'create', supersede: [] },
    );
});

test('review decision: somebody else\'s review does not stand in the way', () => {
    assert.deepEqual(
        L.decideReview({ active: [active(7, 'bob@x.com', 100)], userEmail: 'ann@x.com', assigneeEmail: null, amount: 100, currency: 'USD' }),
        { action: 'create', supersede: [] },
    );
});

test('review decision: the same person reviewing the same figure twice is refused', () => {
    const d = L.decideReview({ active: [active(7, 'Ann@X.com', '100.00')], userEmail: 'ann@x.com', assigneeEmail: null, amount: 100.004, currency: 'USD' });
    assert.equal(d.refuse.status, 409);
    assert.equal(d.refuse.code, 'ALREADY_REVIEWED');
});

test('review decision: a changed figure replaces the reviewer\'s old review', () => {
    assert.deepEqual(
        L.decideReview({ active: [active(7, 'ann@x.com', 100), active(9, 'bob@x.com', 100)], userEmail: 'ann@x.com', assigneeEmail: null, amount: 140, currency: 'USD' }),
        { action: 'create', supersede: [7] },
    );
    // One cent is a different figure.
    assert.deepEqual(
        L.decideReview({ active: [active(7, 'ann@x.com', '100.00')], userEmail: 'ann@x.com', assigneeEmail: null, amount: 100.01, currency: 'USD' }),
        { action: 'create', supersede: [7] },
    );
    // Same figure, other currency, is a different figure.
    assert.deepEqual(
        L.decideReview({ active: [active(7, 'ann@x.com', 100, 'EUR')], userEmail: 'ann@x.com', assigneeEmail: null, amount: 100, currency: 'USD' }),
        { action: 'create', supersede: [7] },
    );
});

test('review decision: the assignee is never one of the reviewers', () => {
    const d = L.decideReview({ active: [], userEmail: 'acc@x.com', assigneeEmail: 'ACC@x.com', amount: 100, currency: 'USD' });
    assert.equal(d.refuse.status, 409);
    assert.equal(d.refuse.code, 'ASSIGNEE_CANNOT_REVIEW');
});

test('refusals are plain text: no mangled characters reach the page', () => {
    assert.equal(
        L.decideReview({ active: [], userEmail: 'acc@x.com', assigneeEmail: 'acc@x.com', amount: 100, currency: 'USD' }).refuse.error,
        'You are this payment’s assignee — two other people sign it off.',
    );
    assert.equal(
        L.decideAssignment({ email: 'nobody@x.com', candidate: null, activeReviews: [] }).refuse.error,
        'nobody@x.com is not an accountant on ShipLine — only accountants can be assigned a payment.',
    );
    assert.equal(L.parseReviewBody({ paymentKey: 'deposit:1', currency: 'USD', amount: 0 }).error, 'amount must be more than 0 — the figure being signed off.');
    // UTF-8 read back as Latin-1 — how the damage looked (Ã¢â‚¬â€ for —).
    const source = require('node:fs').readFileSync(require.resolve('../src/lib/payment-reviews'), 'utf8');
    assert.doesNotMatch(source, /Ã|â€/);
});

test('withdrawing: your own review, or anyone\'s as an admin', () => {
    const row = { reviewed_by_email: 'ann@x.com' };
    assert.equal(L.canWithdraw({ row, userEmail: 'ANN@x.com', userType: 'standard' }), true);
    assert.equal(L.canWithdraw({ row, userEmail: 'bob@x.com', userType: 'standard' }), false);
    assert.equal(L.canWithdraw({ row, userEmail: 'bob@x.com', userType: 'admin' }), true);
    assert.equal(L.canWithdraw({ row, userEmail: 'acc@x.com', userType: 'accountant' }), false);
});

test('assignment decision: clear, set, or refuse', () => {
    assert.deepEqual(L.decideAssignment({ email: null, candidate: null, activeReviews: [] }), { action: 'clear' });
    assert.deepEqual(
        L.decideAssignment({ email: 'acc@x.com', candidate: { email: 'acc@x.com', type: 'accountant' }, activeReviews: [active(7, 'ann@x.com', 100)] }),
        { action: 'set', email: 'acc@x.com' },
    );
    const missing = L.decideAssignment({ email: 'nobody@x.com', candidate: null, activeReviews: [] });
    assert.equal(missing.refuse.status, 422);
    assert.equal(missing.refuse.code, 'NOT_ACCOUNTANT');
    const standard = L.decideAssignment({ email: 'ann@x.com', candidate: { email: 'ann@x.com', type: 'standard' }, activeReviews: [] });
    assert.equal(standard.refuse.status, 422);
    assert.equal(standard.refuse.code, 'NOT_ACCOUNTANT');
    // Someone who has already signed this payment off cannot also be its assignee.
    const reviewed = L.decideAssignment({ email: 'acc@x.com', candidate: { email: 'acc@x.com', type: 'accountant' }, activeReviews: [active(7, 'ACC@x.com', 100)] });
    assert.equal(reviewed.refuse.status, 409);
    assert.equal(reviewed.refuse.code, 'ASSIGNEE_HAS_REVIEWED');
});

test('assignment body: an email or null', () => {
    assert.deepEqual(L.parseAssigneeBody({ paymentKey: 'deposit:306', assigneeEmail: ' Acc@X.com ' }), { value: { paymentKey: 'deposit:306', email: 'acc@x.com' } });
    assert.deepEqual(L.parseAssigneeBody({ paymentKey: 'deposit:306', assigneeEmail: null }), { value: { paymentKey: 'deposit:306', email: null } });
    assert.deepEqual(L.parseAssigneeBody({ paymentKey: 'deposit:306', assigneeEmail: '' }), { value: { paymentKey: 'deposit:306', email: null } });
    assert.match(L.parseAssigneeBody({ paymentKey: 'deposit:306' }).error, /assigneeEmail/);
    assert.match(L.parseAssigneeBody({ paymentKey: 'deposit:306', assigneeEmail: 'not an email' }).error, /assigneeEmail/);
    assert.match(L.parseAssigneeBody({ paymentKey: 'nope', assigneeEmail: null }).error, /paymentKey/);
});

test('rows serialise with the reviewer\'s name when the allowlist has one', () => {
    const at = new Date('2026-09-28T13:05:00Z');
    assert.deepEqual(L.reviewRowToJson({
        id: 7, payment_key: 'deposit:306', kind: 'deposit', supplier_name: 'Sunmed', po_numbers: 'PO_00306J', container_ref: null,
        currency: 'USD', amount: '1234.50', due_date: '2026-10-01', reviewed_by_email: 'ann@x.com', reviewer_name: 'Ann Smith',
        reviewed_at: at, revoked_at: null, revoked_by_email: null, revoked_reason: null,
    }), {
        id: 7, paymentKey: 'deposit:306', kind: 'deposit', supplierName: 'Sunmed', poNumbers: 'PO_00306J', containerRef: null,
        currency: 'USD', amount: 1234.5, dueDate: '2026-10-01', reviewedByEmail: 'ann@x.com', reviewedByName: 'Ann Smith',
        reviewedAt: '2026-09-28T13:05:00.000Z', revokedAt: null, revokedByEmail: null, revokedReason: null,
    });
    assert.deepEqual(L.assigneeRowToJson({
        payment_key: 'balance:USD:324|sunmed', assignee_email: 'acc@x.com', assignee_name: null, assigned_by_email: 'ann@x.com', updated_at: at,
    }), {
        paymentKey: 'balance:USD:324|sunmed', assigneeEmail: 'acc@x.com', assigneeName: null, assignedByEmail: 'ann@x.com', assignedAt: '2026-09-28T13:05:00.000Z',
    });
});
