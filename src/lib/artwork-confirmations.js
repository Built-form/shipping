'use strict';

// Artwork confirmation by the supplier, per PO line (docs/artwork-confirmations.md).
// The pure rules only, no database: the link's token and address, which lines
// can still be confirmed, the state of a request, the email's item list and
// fixed block, the deposit gate on the move into production, and what applying
// a supplier's confirmations writes to orders.
// The SQL is in src/services/artwork-confirmation-routes.js (the staff routes)
// and src/handlers/artwork-confirmed.js (the apply Lambda). The portal mirrors
// TOKEN_RE, TICKABLE_STATUSES and requestState in its src/lib/artworkConfirmations.js.

const crypto = require('crypto');
const { isFqcOrder } = require('./order-transitions');
const { FONT, INK, MUTED, BORDER, CODE_BG, escapeHtml, button, linkFallback, small } = require('./email-layout');

/** 48 lower-case hex characters: unguessable, and the link's whole permission. */
const TOKEN_RE = /^[a-f0-9]{48}$/;
const newToken = () => crypto.randomBytes(24).toString('hex');
const isToken = v => typeof v === 'string' && TOKEN_RE.test(v);

/** How long a link stays open. */
const EXPIRY_DAYS = 60;

/** The page on the supplier portal (no sign-in). `base` has no trailing slash. */
const artworkUrl = (base, token) => `${base}/artwork/${encodeURIComponent(token)}`;

// The stages in which artwork can still be confirmed by the supplier: from the
// PO being sent until the goods leave the factory (the same window the portal
// lets a supplier edit ready dates in). Past them the line is history; a
// SCHEDULED line has no PO sent yet and cannot be confirmed.
const TICKABLE_STATUSES = ['PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY'];

/** The status a tick moves a line out of, and into — when the deposit gate is clear. */
const MOVES_FROM = 'PO_SENT';
const MOVES_TO = 'IN_PRODUCTION';

/**
 * May a tick move the PO's lines into production by itself? Only when the
 * supplier requires no deposit, or the deposit has been paid. `depositPct` is
 * the deposit % in force (src/lib/payment-terms.js; null = nothing says),
 * `depositPaid` whether a deposit payment is on record.
 *   0 %            → clear, 'no_deposit'
 *   paid           → clear, 'deposit_paid'
 *   > 0 %, unpaid  → held,  'deposit_unpaid'
 *   unknown, unpaid→ held,  'terms_unknown' (we cannot say none is required)
 */
function depositGate({ depositPct, depositPaid }) {
    const pct = depositPct == null ? null : Number(depositPct);
    const paid = depositPaid === true;
    if (pct === 0) return { depositPct: 0, depositPaid: paid, clear: true, reason: 'no_deposit' };
    if (paid) return { depositPct: pct, depositPaid: true, clear: true, reason: 'deposit_paid' };
    return { depositPct: pct, depositPaid: false, clear: false, reason: pct == null ? 'terms_unknown' : 'deposit_unpaid' };
}

const HELD_NOTE = {
    deposit_unpaid: 'not moved to production: the deposit has not been paid',
    terms_unknown: 'not moved to production: no deposit terms on record and no deposit paid',
};

const REVOKE_REASONS = ['staff', 'send_failed'];

/** A DATE column ('YYYY-MM-DD' from the pool, or a Date) as 'YYYY-MM-DD', else null. */
function toYmd(value) {
    if (value === null || value === undefined || value === '') return null;
    if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
    const text = String(value).slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

// "Confirmed" here always means confirmed BY THE SUPPLIER: the line's
// artwork_confirmed_by_supplier, a field of its own. artwork_confirmed_date is
// what staff record in ShipLine and is neither read nor written by any of this.
const isConfirmed = line => toYmd(line && (line.artworkConfirmedBySupplier ?? line.artwork_confirmed_by_supplier)) !== null;

/**
 * An _FQC sample line (jf_code or asin ends in _FQC): the units inspected and
 * destroyed. It has no artwork of its own, so it is left out of everything
 * here: never asked about, never listed, never counted. Two things reach it,
 * both through its product (fqcSamplesOf): the staff date set on send, when
 * its product is in that email (artwork-confirmation-routes.js), and the move
 * into production, when its product's tick makes that move (planSampleMoves).
 */
const isFqcLine = line => isFqcOrder(line);

const codeOf = v => String(v ?? '').trim().toUpperCase();
const FQC_SUFFIX = /_FQC$/;

/**
 * The _FQC sample lines that belong to `parents` (the lines an email covers):
 * the same PO's lines whose code is a parent's code plus _FQC. JF0787 in the
 * email matches JF0787_FQC; the comparison is field for field (jf_code with
 * jf_code, asin with asin), ignoring case. A sample whose product is not among
 * the parents is not returned.
 */
function fqcSamplesOf(parents, allLines) {
    const jf = new Set(), asin = new Set();
    for (const p of parents || []) {
        if (isFqcLine(p)) continue;
        if (codeOf(p.jf_code)) jf.add(codeOf(p.jf_code));
        if (codeOf(p.asin)) asin.add(codeOf(p.asin));
    }
    return (allLines || []).filter((l) => {
        if (!isFqcLine(l)) return false;
        const j = codeOf(l.jf_code), a = codeOf(l.asin);
        return (FQC_SUFFIX.test(j) && jf.has(j.replace(FQC_SUFFIX, '')))
            || (FQC_SUFFIX.test(a) && asin.has(a.replace(FQC_SUFFIX, '')));
    });
}

/** Whether a supplier may still confirm this (live) line's artwork. */
function isTickable(line) {
    if (!line || isFqcLine(line)) return false;
    return TICKABLE_STATUSES.includes(String(line.status)) && !isConfirmed(line);
}

/** A JSON column holding order ids (parsed by the driver, or still text) as a clean number[]. */
function parseOrderIds(value) {
    let list = value;
    if (typeof value === 'string') {
        try { list = JSON.parse(value); } catch { list = []; }
    }
    if (!Array.isArray(list)) return [];
    return [...new Set(list.map(Number).filter(n => Number.isSafeInteger(n) && n > 0))];
}

/**
 * The lines a request will ask about. `requested` (the send dialog's ticks) is
 * optional: omitted means every tickable line. Returns { orderIds } or
 * { error, code, field? }.
 */
function chooseLines(lines, requested) {
    const tickable = (lines || []).filter(isTickable);
    if (!tickable.length) {
        return { code: 'NOTHING_TO_CONFIRM', error: 'Every line of this PO already has its artwork confirmed, or is past the stage where it can be.' };
    }
    if (requested === undefined || requested === null) return { orderIds: tickable.map(l => Number(l.id)) };
    if (!Array.isArray(requested) || !requested.length) {
        return { code: 'BAD_FIELD', field: 'orderIds', error: 'orderIds must be a non-empty array of line ids.' };
    }
    const allowed = new Set(tickable.map(l => Number(l.id)));
    const chosen = [];
    for (const raw of requested) {
        const id = Number(raw);
        if (!Number.isSafeInteger(id) || !allowed.has(id)) {
            return { code: 'BAD_FIELD', field: 'orderIds', error: `Line ${String(raw)} cannot have its artwork confirmed (not a line of this PO, already confirmed, or past production).` };
        }
        if (!chosen.includes(id)) chosen.push(id);
    }
    return { orderIds: chosen };
}

/**
 * The state of a request, one precedence shared with the portal and ShipLine:
 * revoked > expired > done > open. `done` = every line it asks about that is
 * still live is confirmed (or none is left). Only `open` takes ticks.
 * `row` = { revoked_at, expired }, `expired` being the database's own answer
 * to `expires_at <= UTC_TIMESTAMP()`; `lines` = the request's live lines.
 */
function requestState(row, lines) {
    if (row.revoked_at) return 'revoked';
    if (row.expired === true || Number(row.expired) === 1) return 'expired';
    const live = lines || [];
    return live.every(l => l.confirmed === true || isConfirmed(l)) ? 'done' : 'open';
}

const lineLabel = l => [l.jfCode ?? l.jf_code, l.productName ?? l.product_name].filter(Boolean).join(' · ') || `Line ${l.id}`;

/** The {lines} token: the items asked about, as an HTML list. */
function linesHtml(lines) {
    const items = (lines || []).map((l) => {
        return `<li style="margin:0 0 4px;">${escapeHtml(lineLabel(l))}</li>`;
    });
    return `<ul style="margin:0 0 14px;padding-left:20px;font-family:${FONT};font-size:14px;line-height:1.5;color:${INK};">${items.join('')}</ul>`;
}

/**
 * The fixed block under the template body: we have approved the final artwork
 * and ask the supplier to confirm they have all of it and will proceed; then
 * the items, the button, the link as text and when it stops working. English and Chinese, as
 * the portal is. `expiresOn` is 'YYYY-MM-DD'.
 */
function artworkBlockHtml({ poNumber, url, lines, expiresOn }) {
    const po = escapeHtml(poNumber);
    const until = escapeHtml(expiresOn || '');
    return [
        `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:18px 0;"><tr><td style="padding:16px 18px 6px;border:1px solid ${BORDER};border-radius:8px;background:${CODE_BG};">`,
        `<p style="margin:0 0 12px;font-family:${FONT};font-size:15px;line-height:1.6;color:${INK};"><strong>Final artwork approved for PO ${po}: please confirm you have it</strong><br>采购订单 <strong>${po}</strong> 的最终图稿已获我方批准，请确认收到</p>`,
        linesHtml(lines),
        button(url, 'Confirm artwork received / 确认已收到图稿'),
        linkFallback(url),
        small('Open the page and tick each item to confirm you have received all of its final artwork and are happy to proceed, then press Confirm. No sign-in is needed. A confirmation cannot be undone, so tick an item only once you have everything for it.'),
        small('打开页面后，请勾选每个产品，确认您已收到其全部最终图稿并同意继续，然后点击“确认”。无需登录。确认后无法撤销，请仅在已收到该产品的全部图稿后勾选。'),
        until ? `<p style="margin:0 0 10px;font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">This link works until ${until}. / 此链接有效期至 ${until}。</p>` : '',
        '</td></tr></table>',
    ].join('');
}

/** Who a confirmation came from, for display and the audit trail. */
function confirmerOf(confirmation, sentTo) {
    const email = String(confirmation.confirmed_by_email || '').trim();
    if (email) return email;
    const first = (Array.isArray(sentTo) ? sentTo : []).map(a => String(a || '').trim()).find(Boolean);
    const name = String(confirmation.confirmed_by_name || '').trim();
    if (first) return name ? `${first} (${name})` : first;
    return name || 'email link';
}

/**
 * What applying a PO's waiting confirmations writes. `confirmations` are the
 * unapplied rows ({ id, order_id, confirmed_on, … }, oldest first), `orders`
 * the PO's live lines among them ({ id, status, artwork_confirmed_by_supplier }),
 * `sentToByRequest` a Map(request id -> recipients).
 *
 * A line the supplier has not confirmed gets the tick's date in
 * artwork_confirmed_by_supplier. From PO_SENT it also moves into production,
 * but only when `gate.clear` (depositGate: no deposit required, or it is
 * paid); otherwise the status stays and the audit note says why. A line
 * already confirmed, or gone, is left alone. Every confirmation is marked
 * applied either way, so it is handled exactly once.
 * → { updates: [{ orderId, confirmedOn, status|null, before, after, userEmail }], appliedIds }
 */
function planApply(confirmations, orders, sentToByRequest = new Map(), gate = { clear: false, reason: 'terms_unknown' }) {
    const byId = new Map((orders || []).map(o => [Number(o.id), o]));
    const updates = [];
    const appliedIds = [];
    const done = new Set();
    for (const c of confirmations || []) {
        appliedIds.push(Number(c.id));
        const orderId = Number(c.order_id);
        const order = byId.get(orderId);
        const date = toYmd(c.confirmed_on);
        if (!order || !date || done.has(orderId) || isConfirmed(order)) continue;
        done.add(orderId);
        const atStart = String(order.status) === MOVES_FROM;
        const moves = atStart && gate.clear === true;
        const held = atStart && !moves ? HELD_NOTE[gate.reason] || HELD_NOTE.terms_unknown : null;
        const before = { artworkConfirmedBySupplier: null };
        const after = {
            artworkConfirmedBySupplier: date,
            note: `artwork confirmed by supplier (supplier portal)${held ? `; ${held}` : ''}`,
        };
        if (moves) { before.status = MOVES_FROM; after.status = MOVES_TO; }
        updates.push({
            orderId,
            confirmedOn: date,
            status: moves ? MOVES_TO : null,
            before,
            after,
            userEmail: `supplier-portal:${confirmerOf(c, sentToByRequest.get(Number(c.request_id)))}`,
        });
    }
    return { updates, appliedIds };
}

/**
 * The _FQC sample lines that follow their product into production. `updates`
 * is planApply's; `orders` the lines it was planned from, with jf_code and
 * asin; `samples` the PO's live _FQC lines ({ id, status, jf_code, asin }).
 *
 * A sample moves only with its own product (fqcSamplesOf: JF0787 takes
 * JF0787_FQC), only when that product's tick moved it PO_SENT → IN_PRODUCTION
 * in this run, and only from PO_SENT itself. So the deposit gate that held
 * the product holds the sample too, and a sample already further on is left
 * alone. The sample gets no supplier date: it was never asked about.
 * → [{ orderId, parentOrderId, status, before, after, userEmail }]
 */
function planSampleMoves(updates, orders, samples) {
    const byId = new Map((orders || []).map(o => [Number(o.id), o]));
    const moves = [];
    const taken = new Set();
    for (const u of updates || []) {
        if (u.status !== MOVES_TO) continue;
        const parent = byId.get(Number(u.orderId));
        if (!parent) continue;
        const code = codeOf(parent.jf_code) || codeOf(parent.asin) || `line ${u.orderId}`;
        for (const s of fqcSamplesOf([parent], samples)) {
            const id = Number(s.id);
            if (String(s.status) !== MOVES_FROM || taken.has(id)) continue;
            taken.add(id);
            moves.push({
                orderId: id,
                parentOrderId: Number(u.orderId),
                status: MOVES_TO,
                before: { status: MOVES_FROM },
                after: { status: MOVES_TO, note: `moved to production with ${code}, whose artwork the supplier confirmed (supplier portal)` },
                userEmail: u.userEmail,
            });
        }
    }
    return moves;
}

module.exports = {
    TOKEN_RE, newToken, isToken, EXPIRY_DAYS, artworkUrl,
    TICKABLE_STATUSES, MOVES_FROM, MOVES_TO, REVOKE_REASONS, depositGate,
    toYmd, isConfirmed, isFqcLine, fqcSamplesOf, isTickable, parseOrderIds, chooseLines, requestState,
    linesHtml, artworkBlockHtml, confirmerOf, planApply, planSampleMoves,
};
