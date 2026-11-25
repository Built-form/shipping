'use strict';

// Sending a supplier the proof of a payment (docs/payment-proof-send.md): the
// pure rules. A recorded transfer (supplier_payments + its lines) is told to the
// supplier as one or two NOTICES, one email each:
//   deposit   asks for the estimated ready dates;
//   balance   (balances, QC units, repackaged lines) asks for the telex release.
// The email links to the supplier portal's Payments page, which reads the
// payment live through shipping's portal_payment*_v views. ONLY that way: the
// proof is never attached to the email (the user, 2026-11-17). Where a link
// cannot work the payment cannot be sent, and the dialog says what to fix.
//
// Nothing here is a copy of the payment for later: the portal never reads what
// an email said (user, 2026-11-16). What is built here is what the send dialog
// shows and what the email names.
//
// The SQL is in src/services/payment-notices.js, the routes in
// src/services/payment-notice-routes.js.

const crypto = require('crypto');
const { supplierRef } = require('./request-to-send-portal');
const P = require('./po-portal-email');
const { FONT, INK, MUTED, BORDER, CODE_BG, escapeHtml, button, linkFallback } = require('./email-layout');

const NOTICE_KINDS = ['deposit', 'balance'];
// A forwarder's transfer has one part of its own, sent to the forwarder
// ("A forwarder's payment" in docs/payment-proof-send.md). Never one of a
// supplier's parts: NOTICE_KINDS stays the supplier's two.
const FORWARDER_KIND = 'forwarder';
const SEND_KINDS = [...NOTICE_KINDS, FORWARDER_KIND];
const EMAIL_TYPE = 'payment_proof';
const SEND_TABLE = 'supplier_payment_sends';
const TEMPLATE_CATEGORY = 'Payment proof';
// The stored wording per notice. The keys still say "_portal": they date from
// when a second pair, "_attached", existed (retired by 2026-11-17_20).
const TEMPLATE_KEYS = {
    deposit: 'payment_proof_deposit_portal',
    balance: 'payment_proof_balance_portal',
    forwarder: 'payment_proof_forwarder_portal',
};
// The one way a payment goes out. A send row from before 2026-11-17 may still
// say 'attached': history, read and shown, never written again.
const DELIVERY = 'portal';
// What a send request may name as its delivery. 'attached' is accepted only to
// be answered (409 DELIVERY_CHANGED): a dialog opened before the change.
const DELIVERIES = ['portal', 'attached'];
const SEND_ROLES = ['admin', 'standard', 'accountant'];

// A send is given this long from the moment its row is committed. Front is not
// called unless the call can end inside it (its own timeout plus a margin), so
// once the deadline has passed nothing can still be going out and a person may
// say what happened. The function's own time limit (900 s) is far beyond it,
// which is why the clock is checked rather than the limit trusted.
const DEADLINE_SECONDS = 60;
const FRONT_CALL_SECONDS = 15;   // src/services/email-send.js SEND_TIMEOUT_MS (the SES call)
const FRONT_MARGIN_SECONDS = 5;

const toCents = v => Math.round(Number(v) * 100);
const fromCents = c => c / 100;
const hasPaidWith = l => Array.isArray(l.paidWith) && l.paidWith.length > 0;

/**
 * The notice a line belongs to: 'deposit', 'balance', 'forwarder' (a cost of
 * the shipment, paid to its own payee: not a supplier's payment) or 'credit'
 * (an extra on the supplier's account, which rides with the others: see
 * buildNotices). `l` needs kind, paymentType, ridesWith and paidWith, as both
 * the notice loader and the API's own line JSON carry them.
 */
function noticeKindOfLine(l) {
    switch (l.kind) {
        case 'po_deposit': return 'deposit';
        case 'pi':
            if (l.paymentType === 'deposit') return 'deposit';
            // A 100% PI paid before anything shipped is a deposit; one whose
            // money was ticked against containers pays for goods on board.
            if (l.paymentType === 'full') return hasPaidWith(l) ? 'balance' : 'deposit';
            return 'balance';
        case 'extra':
            if (l.ridesWith === 'deposit') return 'deposit';
            if (l.ridesWith === 'shipment') return 'forwarder';
            if (l.ridesWith === 'account') return 'credit';
            return 'balance';
        default: return 'balance';   // balance, qc, repackaged
    }
}

/**
 * The supplier's notices a transfer has, in order, from its lines. None for a
 * transfer with a forwarder's charge in it: that one has the forwarder's part
 * (forwarderNoticeOf) or, mixed, nothing to send at all. Kept as it was so
 * that a ShipLine from before "Send to forwarder" shows no button on one.
 */
function noticeKindsOf(lines) {
    const kinds = new Set((lines || []).map(noticeKindOfLine));
    if (kinds.has('forwarder')) return [];
    return NOTICE_KINDS.filter(k => kinds.has(k));
}

const isForwarderLine = l => l.kind === 'extra' && l.ridesWith === 'shipment';

/**
 * Whose a transfer is when it pays a forwarder: null when no line is a
 * forwarder's charge (a supplier's payment), else { forwarderId, blocked }:
 * the one forwarder every line is linked to (payment_extras.forwarder_id, a
 * row of the Forwarders list), or what is in the way:
 *   MIXED_PAYMENT         a line that is not a forwarder's charge rides along
 *                         (goods, a deposit, a credit on the account);
 *   FORWARDER_NOT_LINKED  a charge whose payee is typed text, not one of the list;
 *   FORWARDERS_DIFFER     charges of two forwarders.
 * The payee's NAME is never asked: a name is not who someone is. `lines` need
 * kind, ridesWith and forwarderId (the API's line JSON carries them).
 */
function forwarderOfLines(lines) {
    const all = lines || [];
    const mine = all.filter(isForwarderLine);
    if (!mine.length) return null;
    const cannot = blocked => ({ forwarderId: null, blocked });
    if (mine.length !== all.length) return cannot('MIXED_PAYMENT');
    const ids = mine.map(l => (l.forwarderId == null ? null : Number(l.forwarderId)));
    if (ids.some(id => id == null)) return cannot('FORWARDER_NOT_LINKED');
    if (ids.some(id => id !== ids[0])) return cannot('FORWARDERS_DIFFER');
    return { forwarderId: ids[0], blocked: null };
}

const REFUSALS = {
    CANNOT_SEND: [403, 'Your role cannot send a payment to a supplier: admin, standard and accountant users do.', []],
    NO_SUCH_NOTICE: [404, 'This payment has no such part to send.', ['reload']],
    NOTHING_SENT: [409, 'No money was sent with this payment (a credit covered it), so there is no proof to send.', []],
    // A forwarder's payment (the words of the last three are written per case by forwarderRefusal).
    MIXED_PAYMENT: [409, 'This payment pays a forwarder and something else in one transfer, so it cannot be sent to either of them. Record the forwarder\'s charges as a payment of their own, then send.', ['edit_payment']],
    FORWARDER_NOT_LINKED: [409, 'A charge in this payment names its forwarder as typed text, not as one of the Forwarders list, so the portal has nobody to show it to. Open the forwarder payment on its container, pick the forwarder from the list, then send.', []],
    FORWARDERS_DIFFER: [409, 'The charges in this payment belong to more than one forwarder. Record one payment for each forwarder, then send.', ['edit_payment']],
    FORWARDER_REMOVED: [409, 'This payment\'s forwarder has been taken off the Forwarders list, so the portal has nobody to show it to.', []],
    FORWARDER_NO_CONTACT: [409, 'This payment\'s forwarder has no contact on the Forwarders list, so there is nobody to send it to.', []],
    NOT_A_CONTACT: [400, 'A forwarder\'s payment can be sent only to that forwarder\'s contacts on the Forwarders list. Close the dialog and open it again.', ['reload']],
    NO_LINES: [409, 'This payment is not applied to anything yet. Edit it first, then send.', ['edit_payment']],
    LINE_TARGET_MISSING: [409, 'Something this payment was applied to has been deleted since. Edit the payment first, then send.', ['edit_payment']],
    LINES_EXCEED_TRANSFER: [409, 'The lines of this payment add up to more than the amount sent. Edit the payment first, then send.', ['edit_payment']],
    NO_PROOF: [409, 'There is no proof of payment on this payment. Attach one, then send.', ['attach_proof']],
    PROOF_NOT_SENDABLE: [409, 'A file on this payment is not a proof of payment filed under this supplier. Refile or remove it, then send.', ['fix_proof']],
    // Their words are written per case by portalRefusal.
    PORTAL_NOT_LIVE: [409, 'Payments are sent to suppliers through the supplier portal only, and it cannot show them one on this server yet.', []],
    SUPPLIER_NOT_ON_PORTAL: [409, 'No supplier record has this payment\'s exact supplier name, so the supplier portal has nobody to show it to.', ['edit_payment']],
    NOTICE_NOT_POSITIVE: [409, 'This part of the payment comes to nothing once its credits are taken off: there is nothing to tell the supplier.', []],
    PAYMENT_CHANGED: [409, 'This payment was changed after the email was written. Check the figures and send again.', ['reload']],
    UNRESOLVED_SEND: [409, 'An earlier send of this email may or may not have gone out. Check Front, then say which.', ['resolve_sent', 'resolve_not_sent']],
    SEND_IN_PROGRESS: [409, 'This email is being sent right now.', ['wait']],
    SEND_KEY_REUSED: [409, 'This send was already made with other content. Close the dialog and open it again.', ['reload']],
    NOT_UNKNOWN: [409, 'Only a send whose outcome is unknown can be resolved.', ['reload']],
    FRONT_REFUSED: [502, 'The email service (SES) refused the email. Nothing was sent.', ['retry']],
    SEND_FAILED: [502, 'The email could not be prepared. Nothing was sent.', ['retry']],
    SEND_OUTCOME_UNKNOWN: [502, 'The email may or may not have gone out: Front did not answer. Check Front, then say which. It is not sent again by itself.', ['resolve_sent', 'resolve_not_sent']],
    PAYMENT_NOT_CURRENT: [409, 'This payment has been deleted, or is no longer this supplier\'s. Send it again from the payment instead of reminding.', ['send_again']],
};

/** A refusal's body: { status, code, error, allowedNext, ...extra }. */
function refusal(code, extra = {}) {
    const [status, error, allowedNext] = REFUSALS[code] || [500, 'An internal error occurred.', []];
    return { status, code, error, allowedNext, ...extra };
}

const maySend = userType => SEND_ROLES.includes(userType);

// A reference is told only when the record it comes from is filed under
// exactly the transfer's supplier name. === is a code-point comparison, as the
// portal's utf8mb4_0900_bin is: case, punctuation and trailing spaces count.
const sameName = (a, b) => typeof a === 'string' && typeof b === 'string' && a !== '' && a === b;

function lineType(l, kind) {
    const t = l.target || {};
    switch (l.kind) {
        case 'po_deposit': return 'deposit';
        case 'pi': return t.paymentType === 'deposit' ? 'pi_deposit' : (t.paymentType === 'full' ? 'pi_full' : 'pi_balance');
        case 'balance': return t.topUp ? 'top_up' : 'balance';
        case 'qc': return 'qc';
        case 'repackaged': return 'repackaged';
        case 'extra': return kind === 'credit' ? 'credit' : 'extra';
        default: return 'balance';
    }
}

// One line as a supplier may be told it. `named` false: the money is said and
// nothing else (the portal reads "Other payment").
function supplierLine(l, kind, supplierName) {
    const t = l.target || {};
    const out = {
        type: lineType(l, kind), amount: fromCents(toCents(l.amount)), named: false,
        poId: null, poNumber: null, container: null, shipmentId: null,
        invoiceNumber: null, jfCode: null, extraKind: null, containers: null,
    };
    const poMine = sameName(t.poSupplier, supplierName) && t.poId != null;
    if (l.kind === 'extra') {
        // The extra's own supplier decides whether its kind is said; a PO it
        // points at is said only when that PO is this supplier's too.
        if (!sameName(t.extraSupplier, supplierName)) return out;
        out.named = true;
        out.extraKind = kind === 'credit' ? null : (t.extraKind || null);
        if (poMine) { out.poId = t.poId; out.poNumber = t.poNumber || null; }
        if (t.shipmentId != null) { out.shipmentId = t.shipmentId; out.container = supplierRef(t.shipmentReference); }
        return out;
    }
    if (l.kind === 'balance') {
        if (!sameName(t.shipmentSupplier, supplierName)) return out;
        out.named = true;
        out.shipmentId = t.shipmentId ?? null;
        out.container = supplierRef(t.shipmentReference);
        out.invoiceNumber = t.invoiceNumber || null;
        return out;
    }
    // po_deposit, pi, qc, repackaged: everything said hangs on the PO.
    if (!poMine) return out;
    out.named = true;
    out.poId = t.poId;
    out.poNumber = t.poNumber || null;
    if (l.kind === 'qc' || l.kind === 'repackaged') out.jfCode = t.jfCode || null;
    if (l.kind === 'pi' && hasPaidWith(l)) {
        out.containers = l.paidWith.map(e => ({ container: supplierRef(e.shipmentReference), amount: fromCents(toCents(e.amount)) }));
    }
    return out;
}

// One charge as its forwarder is told it: the container, what kind of charge,
// the amount. Never the description or the note typed on it, a PO, an invoice
// number or a supplier (the user, 2026-11-20). The shape is a supplier line's,
// so one preview token and one dialog serve both.
function forwarderLine(l) {
    const t = l.target || {};
    return {
        type: FORWARDER_KIND, amount: fromCents(toCents(l.amount)), named: true,
        poId: null, poNumber: null, container: supplierRef(t.shipmentReference), shipmentId: t.shipmentId ?? null,
        invoiceNumber: null, jfCode: null, extraKind: t.extraKind || null, containers: null,
    };
}

/**
 * What a transfer is told as: `source` is the notice loader's answer
 * ({ payment, lines, documents }).
 * → { sendable, blocked, audience, forwarderId, transfer, proofs, notices }
 *   audience  'forwarder' when a line is a forwarder's charge, else 'supplier';
 *   forwarderId  the forwarder every line is linked to, when it is a
 *             forwarder's and nothing about that is in the way;
 *   blocked   null, or the refusal that stops the whole transfer;
 *   transfer  { amount, currency, paidOn, linesTotal, notItemised, roundingDifference };
 *   proofs    [{ id, filename, url }], empty while any linked file is not a proof;
 *   notices   one per kind present, deposit first: { kind, amount, lines,
 *             paidFor, readyDatePos, unnamed, blocked }.
 * Money is added in minor units. What no line explains is never spread over
 * the notices: it is `notItemised`. Lines one cent above the transfer are a
 * rounding difference; more than that cannot be sent.
 */
function buildNotices(source) {
    const pay = source.payment;
    const lines = source.lines || [];
    const docs = source.documents || [];
    const raw = lines.map(l => ({ l, kind: l.target ? noticeKindOfLine({ ...l, paymentType: l.target.paymentType, ridesWith: l.target.ridesWith }) : 'missing' }));
    // Whose it is, from the lines that still point at something: a deleted
    // target is said as that (LINE_TARGET_MISSING), not as a mixed payment.
    const owner = forwarderOfLines(raw.filter(r => r.kind !== 'missing')
        .map(r => ({ kind: r.l.kind, ridesWith: r.l.target.ridesWith, forwarderId: r.l.target.forwarderId })));
    const mixed = Boolean(owner) && owner.blocked === 'MIXED_PAYMENT';
    // A forwarder's transfer is one email; a mixed one is nobody's.
    const present = owner ? (mixed ? [] : [FORWARDER_KIND]) : NOTICE_KINDS.filter(k => raw.some(r => r.kind === k));
    // A credit rides with the only kind there is, and with the balance when both are.
    const creditKind = present.length === 1 ? present[0] : 'balance';

    const transferCents = toCents(pay.amount);
    const linesCents = lines.reduce((a, l) => a + toCents(l.amount), 0);
    const diff = transferCents - linesCents;
    const transfer = {
        amount: fromCents(transferCents), currency: pay.currency, paidOn: pay.paidOn,
        linesTotal: fromCents(linesCents),
        notItemised: diff > 0 ? fromCents(diff) : 0,
        roundingDifference: diff === -1 ? -0.01 : 0,
    };

    const badProofs = docs.filter(d => d.docKind !== 'remittance' || !sameName(d.supplierName, pay.supplierName));
    const proofs = badProofs.length ? [] : docs.map(d => ({ id: d.id, filename: d.filename, url: d.url || null }));

    let blocked = null;
    if (mixed) blocked = refusal('MIXED_PAYMENT');
    else if (!lines.length) blocked = refusal('NO_LINES');
    else if (transferCents === 0) blocked = refusal('NOTHING_SENT');
    else if (raw.some(r => r.kind === 'missing')) blocked = refusal('LINE_TARGET_MISSING');
    else if (owner && owner.blocked) blocked = refusal(owner.blocked);
    else if (diff < -1) blocked = refusal('LINES_EXCEED_TRANSFER');
    else if (badProofs.length) blocked = refusal('PROOF_NOT_SENDABLE', { files: badProofs.map(d => d.filename) });
    else if (!docs.length) blocked = refusal('NO_PROOF');

    const notices = present.map(kind => {
        const mine = raw.filter(r => r.kind === kind || (r.kind === 'credit' && creditKind === kind));
        const told = mine.map(r => (kind === FORWARDER_KIND ? forwarderLine(r.l) : supplierLine(r.l, r.kind, pay.supplierName)));
        const cents = mine.reduce((a, r) => a + toCents(r.l.amount), 0);
        const paidFor = [];
        const readyDatePos = [];
        const say = v => { if (v && !paidFor.includes(v)) paidFor.push(v); };
        for (const t of told) {
            say(t.poNumber);
            say(t.container);
            for (const c of t.containers || []) say(c.container);
            if (kind === 'deposit' && t.poId != null && !readyDatePos.some(p => p.id === t.poId)) readyDatePos.push({ id: t.poId, poNumber: t.poNumber });
        }
        return {
            kind, amount: fromCents(cents), lines: told, paidFor, readyDatePos,
            unnamed: told.filter(t => !t.named).length,
            blocked: cents <= 0 ? refusal('NOTICE_NOT_POSITIVE') : null,
        };
    });

    return {
        sendable: !blocked, blocked, supplierName: pay.supplierName,
        audience: owner ? FORWARDER_KIND : 'supplier',
        forwarderId: owner && !owner.blocked ? owner.forwarderId : null,
        transfer, proofs, notices,
    };
}

const money = cents => {
    const sign = cents < 0 ? '-' : '';
    const abs = Math.abs(cents);
    return `${sign}${String(Math.floor(abs / 100)).replace(/\B(?=(\d{3})+(?!\d))/g, ',')}.${String(abs % 100).padStart(2, '0')}`;
};

/** What a template's {amount} {currency} {paidOn} {paidFor} stand for, for one notice. */
function tokensFor(built, notice) {
    return {
        amount: money(toCents(notice.amount)),
        currency: built.transfer.currency,
        paidOn: built.transfer.paidOn,
        paidFor: notice.paidFor.length ? notice.paidFor.join(', ') : '—',
    };
}

// ── Links ─────────────────────────────────────────────────────────────────

// The payment on the portal's Payments page (portal: web/src/screens/PaymentsScreen.tsx).
const paymentPortalUrl = (base, paymentId) => `${base}/payments?payment=${encodeURIComponent(paymentId)}`;
// The order's own page, where its lines' estimated ready dates are entered.
// Not P.poPortalUrl: that one opens the Emailed PDFs tab.
const readyDatesUrl = (base, purchaseOrderId) => `${base}/orders/${encodeURIComponent(purchaseOrderId)}`;
// A forwarder's payment on the portal's Freight payments page (portal:
// web/src/screens/ForwarderPaymentsScreen.tsx). Its own page: the supplier's
// Payments page is shown by supplier name, this one by forwarder id.
const forwarderPaymentPortalUrl = (base, paymentId) => `${base}/freight-payments?payment=${encodeURIComponent(paymentId)}`;

// ── Delivery ──────────────────────────────────────────────────────────────

// There is no switch (the user, 2026-11-17, "dont want the switch"): a payment
// can be sent wherever the portal can show it, which is once the portal is
// deployed on that stage (its payment_views table is there). Until then nothing
// can be sent from here: there is no other way a payment goes out.

/**
 * Whether a notice can go out, which is as a link to the portal or not at all:
 * { delivery: 'portal', reason: null }, or { delivery: null, reason } with why not:
 *   portal_url_unset    SUPPLIER_PORTAL_URL is not configured on this server
 *   portal_unavailable  the portal's database (its payment_views table) is not
 *                       here: the portal is not deployed on this stage yet
 *   no_supplier         no live supplier has the transfer's exact supplier name
 */
function deliveryFor({ portalBase, portalAvailable, supplierMatched }) {
    const cannot = reason => ({ delivery: null, reason });
    if (!portalBase) return cannot('portal_url_unset');
    if (!portalAvailable) return cannot('portal_unavailable');
    if (!supplierMatched) return cannot('no_supplier');
    return { delivery: DELIVERY, reason: null };
}

const REASON_TEXT = {
    portal_url_unset: 'the supplier portal address is not configured',
    portal_unavailable: 'the supplier portal is not available here',
};

/**
 * null when the payment can go out as a portal link; otherwise the 409 that
 * stops it, said for the sender: what is in the way and, where they can, what
 * to do about it. `delivery` is deliveryFor's answer. Nothing is sent attached
 * instead (the user, 2026-11-17).
 */
function portalRefusal(delivery, supplierName) {
    if (delivery && delivery.delivery === DELIVERY) return null;
    const reason = (delivery && delivery.reason) || 'portal_unavailable';
    if (reason === 'no_supplier') {
        return refusal('SUPPLIER_NOT_ON_PORTAL', {
            reason,
            error: `No supplier record is named exactly "${supplierName ?? ''}", so the supplier portal has nobody to show this payment to. Make the payment's supplier name and the supplier record match exactly, then send. The proof is not sent attached instead.`,
        });
    }
    return refusal('PORTAL_NOT_LIVE', {
        reason,
        error: `A payment is sent to a supplier as a link to the supplier portal only, never with the proof attached. That cannot be done on this server yet: ${REASON_TEXT[reason] || reason}. Nothing can be sent until it is.`,
    });
}

/**
 * Whether a forwarder's payment can go out, which is as a link to the portal
 * or not at all: { delivery: 'portal', reason: null }, or { delivery: null,
 * reason }:
 *   portal_url_unset    SUPPLIER_PORTAL_URL is not configured on this server
 *   portal_unavailable  the portal's forwarder_payment_events table is not
 *                       here: the portal's Freight payments page is not
 *                       deployed on this stage yet
 *   forwarder_removed   the forwarder is off the Forwarders list (or gone)
 *   no_contact          the forwarder has no contact: nobody can sign in to see it
 * `forwarder` is { id, name, removed } or null; `contacts` its live contacts.
 */
function forwarderDelivery({ portalBase, portalAvailable, forwarder, contacts }) {
    const cannot = reason => ({ delivery: null, reason });
    if (!portalBase) return cannot('portal_url_unset');
    if (!portalAvailable) return cannot('portal_unavailable');
    if (!forwarder || forwarder.removed) return cannot('forwarder_removed');
    if (!(contacts || []).length) return cannot('no_contact');
    return { delivery: DELIVERY, reason: null };
}

/** forwarderDelivery's answer as the 409 that stops the send, or null when it can go. */
function forwarderRefusal(delivery, forwarderName) {
    if (delivery && delivery.delivery === DELIVERY) return null;
    const reason = (delivery && delivery.reason) || 'portal_unavailable';
    const name = forwarderName ?? '';
    if (reason === 'forwarder_removed') {
        return refusal('FORWARDER_REMOVED', {
            reason,
            error: `This payment's forwarder${name ? ` ("${name}")` : ''} has been taken off the Forwarders list, so the portal has nobody to show the payment to. Put the forwarder back on the Freight page, then send.`,
        });
    }
    if (reason === 'no_contact') {
        return refusal('FORWARDER_NO_CONTACT', {
            reason,
            error: `"${name}" has no contact on the Forwarders list, so there is nobody to send this payment to. Add a contact on the Freight page, then send.`,
        });
    }
    return refusal('PORTAL_NOT_LIVE', {
        reason,
        error: `A payment is sent to a forwarder as a link to the portal only, never with the proof attached. That cannot be done on this server yet: ${reason === 'portal_url_unset' ? 'the portal address is not configured' : 'the portal\'s Freight payments page is not available here'}. Nothing can be sent until it is.`,
    });
}

/**
 * The addresses a forwarder's payment may go to, of the ones asked for:
 * { to, notContacts }. Only the forwarder's own live contacts: the portal
 * shows the payment to whoever signs in with a contact's address, so anyone
 * else would get an email about something they cannot open. Trimmed, any
 * case, each once, in the order asked.
 */
function recipientsAreContacts(to, contacts) {
    const known = new Set((contacts || []).map(c => String(c.email || '').trim().toLowerCase()).filter(Boolean));
    const out = { to: [], notContacts: [] };
    for (const a of to || []) {
        const email = String(a || '').trim().toLowerCase();
        if (!email) continue;
        const list = known.has(email) ? out.to : out.notContacts;
        if (!list.includes(email)) list.push(email);
    }
    return out;
}

/**
 * null when the send asked for the way payments go out; otherwise the 409 for
 * a dialog that still showed "proof attached" (opened before 2026-11-17).
 */
function deliveryConflict(requested) {
    if (requested === DELIVERY) return null;
    return {
        status: 409, code: 'DELIVERY_CHANGED', delivery: DELIVERY, reason: null, allowedNext: [DELIVERY],
        error: 'A proof of payment is no longer sent attached: this payment goes out as a link to the supplier portal. Check the wording and send again.',
    };
}

// ── What the dialog showed, and what was asked ────────────────────────────

const sha256 = text => crypto.createHash('sha256').update(text).digest('hex');

/**
 * A hash of everything the send dialog showed for a transfer: the figures, each
 * line as the supplier is told it, the proofs and the way it goes out. Sent
 * back with the send: a difference means the payment was changed in between.
 * Not the order lines are stored in, their row ids, or anything internal.
 */
function previewToken(built, delivery) {
    const notices = built.notices.map(n => ({
        kind: n.kind, cents: toCents(n.amount),
        lines: n.lines.map(l => JSON.stringify([l.type, toCents(l.amount), l.named, l.poId, l.poNumber, l.container, l.shipmentId, l.invoiceNumber, l.jfCode, l.extraKind,
            (l.containers || []).map(c => [c.container, toCents(c.amount)])])).sort(),
    }));
    return sha256(JSON.stringify({
        delivery,
        transfer: [toCents(built.transfer.amount), built.transfer.currency, built.transfer.paidOn],
        supplier: built.supplierName ?? null,
        // A forwarder's only: a supplier's token is what it was before this key existed.
        ...(built.audience === FORWARDER_KIND ? { forwarder: built.forwarderId ?? null } : {}),
        blocked: built.blocked ? built.blocked.code : null,
        proofs: built.proofs.map(p => p.id).sort((a, b) => a - b),
        notices,
    }));
}

/** What one send asked for: binds a sendKey to its content. */
function requestHash({ to, subject, body, delivery, previewToken: token }) {
    const people = [...new Set((to || []).map(a => String(a).trim().toLowerCase()).filter(Boolean))].sort();
    return sha256(JSON.stringify([people, String(subject || ''), String(body || ''), delivery, token]));
}

/** Whether Front may still be called for a send with this deadline. */
function mayCallFront(deadlineAt, now = new Date()) {
    return new Date(deadlineAt).getTime() - now.getTime() > (FRONT_CALL_SECONDS + FRONT_MARGIN_SECONDS) * 1000;
}

// ── The state of a notice ─────────────────────────────────────────────────

/** A send's status as it stands now: one left 'sending' past its deadline is 'unknown'. */
function effectiveStatus(send, now = new Date()) {
    if (send.status === 'sending' && new Date(send.deadlineAt).getTime() < now.getTime()) return 'unknown';
    return send.status;
}

/**
 * Each notice's state from the transfer's sends: [{ kind, state, lastSentAt, delivery }].
 *   unknown    a send's outcome is not known: a person must say (CHECK SEND);
 *   sending    one is going out right now;
 *   sent       one went out to the transfer's CURRENT supplier name (the test
 *              the portal's view makes, so the two never disagree);
 *   not_sent   otherwise: never sent, only failed, or sent to a supplier the
 *              transfer has since been moved from.
 */
function noticeStates(kinds, sends, supplierName, now = new Date()) {
    return kinds.map(kind => stateOf(kind, sends, s => sameName(s.supplierName, supplierName), now));
}

// One part's state from its sends; `wentTo` says whether a send that went out
// went to whoever the transfer belongs to now.
function stateOf(kind, sends, wentTo, now) {
    const mine = (sends || []).filter(s => s.noticeKind === kind);
    const status = mine.map(s => effectiveStatus(s, now));
    const went = mine.filter((s, i) => status[i] === 'sent' && wentTo(s))
        .sort((a, b) => String(b.sentAt).localeCompare(String(a.sentAt)));
    let state = 'not_sent';
    if (status.includes('unknown')) state = 'unknown';
    else if (status.includes('sending')) state = 'sending';
    else if (went.length) state = 'sent';
    return { kind, state, lastSentAt: went.length ? went[0].sentAt : null, delivery: went.length ? went[0].delivery : null };
}

/**
 * The forwarder part of a transfer as the payments list says it:
 * { kind: 'forwarder', state, lastSentAt, delivery }, or null for a transfer
 * that is not a forwarder's alone (a supplier's, a mixed one, one with no
 * lines). `lines` are the API's own line JSON. A transfer whose charges are
 * not linked to a forwarder yet still has the part (never 'sent'), so the
 * send dialog opens and says what to do. 'sent' means sent to the forwarder
 * the transfer belongs to NOW, by id: the test the portal's view makes.
 */
function forwarderNoticeOf(lines, sends, now = new Date()) {
    const owner = forwarderOfLines(lines);
    if (!owner || owner.blocked === 'MIXED_PAYMENT') return null;
    const id = owner.blocked ? null : owner.forwarderId;
    return stateOf(FORWARDER_KIND, sends, s => id != null && s.forwarderId != null && Number(s.forwarderId) === id, now);
}

/**
 * What a forwarder did with a payment on the portal, from the portal's own
 * forwarder_payment_events: { opened, acknowledgement }. Only events of the
 * forwarder the transfer belongs to now (`forwarderId`): what a forwarder it
 * used to belong to did is never shown as this one's.
 *   opened           [{ email, name, firstAt, lastAt, count }], first opener first;
 *   acknowledgement  { byName, byEmail, at, current } or null. `current` when
 *                    it was made of the payment as it stands (`revision`, the
 *                    portal view's own hash of what the forwarder is shown);
 *                    otherwise it is of an earlier figure, container or proof.
 * `events` are [{ forwarderId, event, userEmail, userName, revision, at }].
 */
function forwarderEvidence(events, { forwarderId, revision }) {
    const mine = forwarderId == null ? [] : (events || []).filter(e => e.forwarderId != null && Number(e.forwarderId) === Number(forwarderId));
    const byUser = new Map();
    for (const e of mine.filter(x => x.event === 'opened')) {
        const o = byUser.get(e.userEmail) || { email: e.userEmail, name: e.userName || null, firstAt: e.at, lastAt: e.at, count: 0 };
        if (String(e.at) < String(o.firstAt)) o.firstAt = e.at;
        if (String(e.at) > String(o.lastAt)) o.lastAt = e.at;
        o.count += 1;
        byUser.set(e.userEmail, o);
    }
    const opened = [...byUser.values()].sort((a, b) => String(a.firstAt).localeCompare(String(b.firstAt)) || String(a.email).localeCompare(String(b.email)));
    const acks = mine.filter(e => e.event === 'acknowledged').sort((a, b) => String(b.at).localeCompare(String(a.at)));
    const ofNow = revision ? acks.find(e => e.revision === revision) : null;
    const shown = ofNow || acks[0] || null;
    return {
        opened,
        acknowledgement: shown ? { byName: shown.userName || null, byEmail: shown.userEmail, at: shown.at, current: Boolean(ofNow) } : null,
    };
}

// ── The fixed blocks under the wording ────────────────────────────────────

const BOX_OPEN = `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:18px 0;"><tr><td style="padding:16px 18px 6px;border:1px solid ${BORDER};border-radius:8px;background:${CODE_BG};">`;
const BOX_CLOSE = '</td></tr></table>';
const lead = html => `<p style="margin:0 0 12px;font-family:${FONT};font-size:15px;line-height:1.6;color:${INK};">${html}</p>`;
const foot = html => `<p style="margin:0 0 10px;font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">${html}</p>`;

/**
 * Under the email's wording: the button to the payment on the portal and how
 * to get an account (the PO emails' frame), and then what the payment asks
 * for in return, with the way to it:
 *   a deposit   each order's estimated ready dates (`readyDates`: [{ poNumber,
 *               url }], only orders the recipient can open);
 *   a balance   the telex release of each container it paid (`telex`:
 *               telexContainersOf), uploaded on the payment's own row.
 * A payment that asks for neither gets the first box alone.
 */
function portalBlockHtml({ kind, url, registerUrl, preapproved, readyDates = [], telex = [] }) {
    const block = P.blockHtml({
        leadHtml: 'The <strong>proof of payment</strong> is on the supplier portal, with what the payment covers. Sign in to open it.<br><strong>付款凭证</strong>及付款明细已发布在供应商门户，请登录查看。',
        url, regUrl: registerUrl, preapproved,
        footHtml: 'No file is attached to this email: the proof is kept on the portal. / 本邮件不附带文件：付款凭证保存在门户中。',
        seeEn: 'this payment', seeZh: '此付款',
    });
    if (kind === 'deposit' && readyDates.length) {
        return `${block}${BOX_OPEN}${lead('Please enter the <strong>estimated ready date</strong> of each item on the order\'s page.<br>请在订单页面填写各产品的<strong>预计完成日期</strong>。')}${orderButtons(readyDates)}${BOX_CLOSE}`;
    }
    if (kind === 'balance' && telex.length) {
        // Named only when every one has a number: a list with a gap in it helps nobody.
        const numbers = telex.every(Boolean) ? telex.map(escapeHtml) : [];
        const en = numbers.length ? ` (${numbers.length === 1 ? 'container' : 'containers'} ${numbers.join(', ')})` : '';
        const zh = numbers.length ? `（货柜 ${numbers.join('、')}）` : '';
        return `${block}${BOX_OPEN}${lead(`Please upload the <strong>telex release</strong> of each container this payment covers${en} on the portal, on this payment's own row.<br>请在门户中该笔付款所在行，上传本次付款所涉各货柜${zh}的<strong>电放提单</strong>。`)}${button(url, 'Upload the telex release / 上传电放提单')}${linkFallback(url)}${BOX_CLOSE}`;
    }
    return block;
}

/**
 * Under the wording of a forwarder's email: the button to the payment on the
 * portal and how to get an account. Nothing is asked for in return; the page
 * has an Acknowledge button, which is theirs to press or not.
 */
function forwarderBlockHtml({ url, registerUrl, preapproved }) {
    return P.blockHtml({
        leadHtml: 'The <strong>proof of payment</strong> is on our portal, with the containers it covers. Sign in to open it, and press <strong>Acknowledge</strong> there to let us know you have it.<br><strong>付款凭证</strong>及其对应的货柜已发布在门户，请登录查看，并点击<strong>确认收到</strong>告知我们。',
        url, regUrl: registerUrl, preapproved,
        footHtml: 'No file is attached to this email: the proof is kept on the portal. / 本邮件不附带文件：付款凭证保存在门户中。',
        seeEn: 'this payment', seeZh: '此付款',
    });
}

// A button to each order's own page, where its lines' estimated ready dates
// are entered. button() treats its label as plain text and escapes it.
const orderButtons = readyDates => readyDates.map(r => [
    button(r.url, `Enter ready dates: ${r.poNumber} / 填写预计完成日期`),
    linkFallback(r.url),
].join('')).join('');

/**
 * Where a deposit's estimated ready dates are entered: each of its orders'
 * own page on the portal. The ready dates are ALWAYS asked for there, never by
 * a reply to the email (the user, 2026-11-17). One link per order, where the
 * portal can open the order for this supplier: it has an address here, and a
 * supplier of exactly the transfer's name is on it. `delivery` is
 * resolveNoticeDelivery's answer. A balance has none (`readyDatePos` is a
 * deposit's).
 */
function readyDatesFor(notice, delivery) {
    const orders = (notice && notice.readyDatePos) || [];
    if (!delivery || !delivery.portalBase || !(delivery.suppliers || []).length) return [];
    return orders.map(p => ({ poNumber: p.poNumber, url: readyDatesUrl(delivery.portalBase, p.id) }));
}

/**
 * The containers a balance asks the telex release of, each once, by the number
 * the supplier knows (null for one with none): the ones the portal lists on the
 * payment's row with an upload button. The portal's own rule (jportal
 * src/lib/telex.js): a balance or top-up line filed under exactly the
 * payment's supplier, with its shipment. Not a PI paid through containers, not
 * QC units, not a charge. A deposit has none.
 */
function telexContainersOf(notice) {
    if (!notice) return [];
    const seen = new Set();
    const out = [];
    for (const l of notice.lines || []) {
        if ((l.type !== 'balance' && l.type !== 'top_up') || !l.named || l.shipmentId == null || seen.has(l.shipmentId)) continue;
        seen.add(l.shipmentId);
        out.push(l.container || null);
    }
    return out;
}

/** The note pre-approval leaves on a portal user it creates or approves. */
const preapprovalNote = ({ paymentId, kind, actor }) =>
    `Pre-approved from ShipLine: proof of payment ${paymentId} (${kind}) emailed by ${actor || 'shipline'}`;

module.exports = {
    NOTICE_KINDS, FORWARDER_KIND, SEND_KINDS, EMAIL_TYPE, SEND_TABLE, TEMPLATE_CATEGORY, TEMPLATE_KEYS, DELIVERY, DELIVERIES, SEND_ROLES,
    DEADLINE_SECONDS,
    toCents, noticeKindOfLine, noticeKindsOf, forwarderOfLines, buildNotices, tokensFor,
    paymentPortalUrl, forwarderPaymentPortalUrl, readyDatesUrl, readyDatesFor, telexContainersOf,
    deliveryFor, portalRefusal, deliveryConflict,
    forwarderDelivery, forwarderRefusal, recipientsAreContacts,
    previewToken, requestHash, mayCallFront,
    effectiveStatus, noticeStates, forwarderNoticeOf, forwarderEvidence,
    portalBlockHtml, forwarderBlockHtml, preapprovalNote,
    refusal, maySend,
};
