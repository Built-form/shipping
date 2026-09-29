'use strict';

// Payment sign-offs — pure rules behind /api/v1/payment-reviews and
// /api/v1/payment-assignees (routes: src/services/payment-review-routes.js).
//
// ShipLine's Payments flow page works out what is owed on the client, from the
// terms and the goods on board, so a "payment" has no row of its own. A
// sign-off is therefore pinned to a KEY the page builds, plus the figure the
// reviewer saw:
//   deposit:<purchase order id>                     every deposit due on that PO
//   balance:<currency>:<container>|<supplier key>   a supplier's balance in one
//                                                   container ('-' = no container)
// Whether a review still stands is the page's call: only it knows today's
// figure, and a review of any other figure does not count. The server owns
// who reviewed, when, and what they saw.
//
// Rules (user, 2026-09-28): two different people sign each payment off; an
// accountant is the assignee (one per payment) and never one of the two;
// accountants assign and record payments but do not review.

const REQUIRED_REVIEWS = 2;
const REVIEWER_ROLES = ['admin', 'standard'];
const ASSIGNER_ROLES = ['admin', 'standard', 'accountant'];
const ASSIGNEE_ROLE = 'accountant';
const KEY_MAX_LEN = 255;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

const canReview = userType => REVIEWER_ROLES.includes(String(userType));
const canAssign = userType => ASSIGNER_ROLES.includes(String(userType));
const sameEmail = (a, b) => !!a && !!b && String(a).trim().toLowerCase() === String(b).trim().toLowerCase();
// Figures are compared to the cent.
const sameFigure = (a, b) => Math.abs(Number(a) - Number(b)) < 0.005;

// { key, kind, currency, containerRef } or { error }.
function parsePaymentKey(raw) {
    if (typeof raw !== 'string') return { error: 'paymentKey is required.' };
    const key = raw.trim();
    if (!key) return { error: 'paymentKey is required.' };
    if (key.length > KEY_MAX_LEN) return { error: `paymentKey cannot exceed ${KEY_MAX_LEN} characters.` };
    const dep = /^deposit:([1-9]\d{0,9})$/.exec(key);
    if (dep) return { key, kind: 'deposit', currency: null, containerRef: null };
    const bal = /^balance:([A-Z]{3}):([^|]+)\|(.+)$/.exec(key);
    if (bal && bal[2].trim() && bal[3].trim()) {
        return { key, kind: 'balance', currency: bal[1], containerRef: bal[2] === '-' ? null : bal[2] };
    }
    return { error: 'paymentKey must be "deposit:<purchase order id>" or "balance:<currency>:<container>|<supplier>".' };
}

// { value } or { error } — a sign-off as the page sends it.
function parseReviewBody(body) {
    const b = body || {};
    const k = parsePaymentKey(b.paymentKey);
    if (k.error) return { error: k.error };
    const currency = typeof b.currency === 'string' ? b.currency.trim().toUpperCase() : '';
    if (!/^[A-Z]{3}$/.test(currency)) return { error: 'currency must be a three-letter code.' };
    if (k.currency && k.currency !== currency) return { error: `currency ${currency} does not match the payment (${k.currency}).` };
    const amount = Math.round(Number(b.amount) * 100) / 100;
    if (!Number.isFinite(amount) || amount <= 0) return { error: 'amount must be more than 0 — the figure being signed off.' };
    let dueDate = null;
    if (b.dueDate != null && b.dueDate !== '') {
        if (typeof b.dueDate !== 'string' || !DATE_RE.test(b.dueDate)) return { error: 'dueDate must be YYYY-MM-DD.' };
        dueDate = b.dueDate;
    }
    let poNumbers = null;
    if (b.poNumbers != null) {
        if (!Array.isArray(b.poNumbers) || b.poNumbers.length > 50 || b.poNumbers.some(p => typeof p !== 'string')) {
            return { error: 'poNumbers must be a list of purchase order numbers.' };
        }
        const joined = b.poNumbers.map(p => p.trim()).filter(Boolean).join(', ');
        poNumbers = joined ? joined.slice(0, 1000) : null;
    }
    const supplierName = typeof b.supplierName === 'string' && b.supplierName.trim() ? b.supplierName.trim().slice(0, 255) : null;
    return {
        value: {
            paymentKey: k.key, kind: k.kind, currency, amount,
            supplierName, poNumbers, containerRef: k.containerRef, dueDate,
        },
    };
}

// { value: { paymentKey, email|null } } or { error }.
function parseAssigneeBody(body) {
    const b = body || {};
    const k = parsePaymentKey(b.paymentKey);
    if (k.error) return { error: k.error };
    if (!('assigneeEmail' in b)) return { error: 'assigneeEmail is required (null to unassign).' };
    const raw = b.assigneeEmail;
    if (raw == null || raw === '') return { value: { paymentKey: k.key, email: null } };
    const email = typeof raw === 'string' ? raw.trim().toLowerCase() : '';
    if (!EMAIL_RE.test(email) || email.length > 255) return { error: 'assigneeEmail must be an email address, or null.' };
    return { value: { paymentKey: k.key, email } };
}

/**
 * Whether a reviewer's sign-off is written, given the key's ACTIVE reviews.
 * → { action: 'create', supersede: [ids of the reviewer's older reviews] }
 *   or { refuse: { status, code, error } }.
 * The assignee pays; two other people sign off. A reviewer has one active
 * review per payment: reviewing a changed figure replaces their old one.
 */
function decideReview({ active, userEmail, assigneeEmail, amount, currency }) {
    if (sameEmail(userEmail, assigneeEmail)) {
        return { refuse: { status: 409, code: 'ASSIGNEE_CANNOT_REVIEW', error: 'You are this payment’s assignee — two other people sign it off.' } };
    }
    const mine = (active || []).filter(r => sameEmail(r.reviewed_by_email, userEmail));
    if (mine.some(r => r.currency === currency && sameFigure(r.amount, amount))) {
        return { refuse: { status: 409, code: 'ALREADY_REVIEWED', error: 'You have already signed off this figure.' } };
    }
    return { action: 'create', supersede: mine.map(r => r.id) };
}

/**
 * Setting a payment's assignee. `candidate` is the allowlist row for `email`
 * (null when not on it); `activeReviews` are the key's active reviews.
 * → { action: 'clear' } | { action: 'set', email } | { refuse }.
 */
function decideAssignment({ email, candidate, activeReviews }) {
    if (email == null) return { action: 'clear' };
    if (!candidate || String(candidate.type) !== ASSIGNEE_ROLE) {
        return { refuse: { status: 422, code: 'NOT_ACCOUNTANT', error: `${email} is not an accountant on ShipLine — only accountants can be assigned a payment.` } };
    }
    if ((activeReviews || []).some(r => sameEmail(r.reviewed_by_email, email))) {
        return { refuse: { status: 409, code: 'ASSIGNEE_HAS_REVIEWED', error: `${email} has signed this payment off, so cannot also be its assignee.` } };
    }
    return { action: 'set', email };
}

const canWithdraw = ({ row, userEmail, userType }) =>
    sameEmail(row && row.reviewed_by_email, userEmail) || userType === 'admin';

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));
const ymd = v => (v == null ? null : v instanceof Date ? v.toISOString().slice(0, 10) : String(v).slice(0, 10));

function reviewRowToJson(r) {
    return {
        id: r.id,
        paymentKey: r.payment_key,
        kind: r.kind,
        supplierName: r.supplier_name || null,
        poNumbers: r.po_numbers || null,
        containerRef: r.container_ref || null,
        currency: r.currency,
        amount: Number(r.amount),
        dueDate: ymd(r.due_date),
        reviewedByEmail: r.reviewed_by_email,
        reviewedByName: r.reviewer_name || null,
        reviewedAt: iso(r.reviewed_at),
        revokedAt: iso(r.revoked_at),
        revokedByEmail: r.revoked_by_email || null,
        revokedReason: r.revoked_reason || null,
    };
}

function assigneeRowToJson(r) {
    return {
        paymentKey: r.payment_key,
        assigneeEmail: r.assignee_email,
        assigneeName: r.assignee_name || null,
        assignedByEmail: r.assigned_by_email || null,
        assignedAt: iso(r.updated_at),
    };
}

module.exports = {
    REQUIRED_REVIEWS,
    REVIEWER_ROLES,
    ASSIGNER_ROLES,
    ASSIGNEE_ROLE,
    canReview,
    canAssign,
    canWithdraw,
    parsePaymentKey,
    parseReviewBody,
    parseAssigneeBody,
    decideReview,
    decideAssignment,
    reviewRowToJson,
    assigneeRowToJson,
};
