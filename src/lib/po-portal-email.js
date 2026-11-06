'use strict';

// The PO "send" email now points the supplier at the supplier portal instead of
// attaching the PDF (docs/po-portal-send.md). The pure rules only, no
// database: the portal links, which recipients may be pre-approved as portal
// users, and the fixed portal block appended under the editable template body.
// The SQL is in src/services/portal-access.js; the route is
// POST /api/v1/purchase-order-documents/:id/email in src/handlers/orders.js.

// The portal's base URL from SUPPLIER_PORTAL_URL, '' when unset.
const portalBaseOf = (env = process.env) =>
    String((env && env.SUPPLIER_PORTAL_URL) || '').trim().replace(/\/+$/, '');

// The order's page on the portal (never the PDF's own address), opened on its
// Emailed PDFs tab, where the supplier downloads it (portal:
// web/src/screens/OrderDetailScreen.tsx). A signed-out visitor is sent to sign
// in and brought back here (portal: web/src/lib/returnTo.ts).
const poPortalUrl = (base, purchaseOrderId) =>
    `${base}/orders/${encodeURIComponent(purchaseOrderId)}?tab=pdfs`;

const registerUrl = base => `${base}/register`;

// The portal matches a user row by the trimmed, lower-cased email claim.
const normalizeEmail = e => String(e == null ? '' : e).trim().toLowerCase();

// One plain address: no spaces, brackets, quotes or separators, one @, a dot in
// the domain. Stricter than the old check, which let "<a@b.com>",
// "mailto:a@b.com" and "a@b.com" + a zero-width space through to Front AND to
// the portal, where the supplier (signing in as a@b.com) never matched the row.
const PLAIN_ADDRESS = /^[^\s@<>()[\]\\,;:"]+@[^\s@<>()[\]\\,;:"]+\.[^\s@<>()[\]\\,;:"]+$/;

// Invisible format characters (zero-width space / joiner, direction marks, BOM)
// that ride along when an address is copied from a web page, PDF or chat app.
const INVISIBLE = /\p{Cf}/gu;

/**
 * The send dialog's recipient boxes as typed or pasted, into addresses.
 * Each box may hold several addresses separated by "," or ";" (or new lines);
 * each may be wrapped as `Name <a@b.com>` or `<a@b.com>`, or start with
 * `mailto:`. NFKC turns full-width characters a Chinese IME types (＠ ．) into
 * ASCII. Duplicates (ignoring case) collapse to the first spelling.
 * Returns { addresses, invalid }: `invalid` holds the pieces that are still
 * not one plain address, as typed.
 */
function parseRecipients(to) {
    const addresses = [];
    const invalid = [];
    const seen = new Set();
    for (const box of (Array.isArray(to) ? to : [to])) {
        if (typeof box !== 'string') continue;
        for (const piece of box.normalize('NFKC').replace(INVISIBLE, '').split(/[,;\r\n]+/)) {
            let s = piece.trim();
            if (!s) continue;
            const wrapped = s.match(/<([^<>]*)>$/);
            if (wrapped) s = wrapped[1].trim();
            s = s.replace(/^mailto:/i, '').trim();
            if (!PLAIN_ADDRESS.test(s)) { invalid.push(piece.trim()); continue; }
            const key = s.toLowerCase();
            if (seen.has(key)) continue;
            seen.add(key);
            addresses.push(s);
        }
    }
    return { addresses, invalid };
}

/**
 * The addresses a send pre-approves on the portal: EVERY recipient, staff
 * included (a colleague sent a PO is set up to see it as the supplier does;
 * until 2026-11-04 ShipLine users and their company domain were skipped, which
 * left them "waiting for approval" when they registered). Normalised as the
 * portal matches them; duplicates collapse to one.
 */
function provisionList(recipients) {
    return [...new Set((recipients || []).map(normalizeEmail).filter(Boolean))];
}

// What pre-approval did with one address, from the row it found. pending ->
// active is staff vouching for a mailbox whose owner has already registered;
// rejected and disabled are an admin's decision in JFPRO and are never undone
// from here.
const OUTCOME_FOR_STATUS = {
    pending: 'approved',
    active: 'already_active',
    rejected: 'blocked',
    disabled: 'blocked',
};

/** 'invited' (no row yet), 'approved', 'already_active' or 'blocked'. Unknown statuses block. */
const outcomeFor = row => (row ? (OUTCOME_FOR_STATUS[row.status] || 'blocked') : 'invited');

// Outcomes after which the address can see the PO on the portal once signed in.
const GRANTED_OUTCOMES = new Set(['invited', 'approved', 'already_active']);

const { FONT, INK, MUTED, BORDER, CODE_BG, escapeHtml, button, linkFallback, small } = require('./email-layout');

/**
 * The fixed portal block under the template body: the button to the PO, and
 * how to get an account. English and Chinese, as the portal is. `preapproved`
 * = every supplier recipient was granted access to this PO's supplier, so the
 * email may say there is no approval to wait for; otherwise it says our team
 * will approve them.
 */
function portalBlockHtml({ poNumber, version, poUrl, registerUrl: regUrl, preapproved }) {
    const po = escapeHtml(poNumber);
    const v = version != null ? ` v${escapeHtml(version)}` : '';
    const vZh = version != null ? `（第 ${escapeHtml(version)} 版）` : '';
    const reg = escapeHtml(regUrl);
    const regLink = `<a href="${reg}" style="color:#1f5fbf;word-break:break-all;">${reg}</a>`;
    const account = preapproved
        ? [
            small(`<strong style="color:${INK};">New to the portal?</strong> Register at ${regLink} with the email address this message was sent to. That address is already approved for your company, so you will see this order as soon as you sign in. There is no approval to wait for.`),
            small(`<strong style="color:${INK};">首次使用门户？</strong>请使用接收本邮件的邮箱地址在 ${regLink} 注册。该邮箱已获批准，登录后即可查看此订单，无需等待审核。`),
        ]
        : [
            small(`<strong style="color:${INK};">New to the portal?</strong> Register at ${regLink} with the email address this message was sent to, and our team will approve your access.`),
            small(`<strong style="color:${INK};">首次使用门户？</strong>请使用接收本邮件的邮箱地址在 ${regLink} 注册，我们的团队将审核并开通您的访问权限。`),
        ];
    return [
        `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:18px 0;"><tr><td style="padding:16px 18px 6px;border:1px solid ${BORDER};border-radius:8px;background:${CODE_BG};">`,
        `<p style="margin:0 0 12px;font-family:${FONT};font-size:15px;line-height:1.6;color:${INK};"><strong>PO ${po}${v}</strong> is on the supplier portal. Sign in, and download the PDF from the order's page.<br>采购订单 <strong>${po}</strong>${vZh}已发布在供应商门户。请登录门户，在该订单页面下载 PDF。</p>`,
        button(poUrl, 'Go to the supplier portal / 前往供应商门户'),
        linkFallback(poUrl),
        ...account,
        `<p style="margin:0 0 10px;font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">No PDF is attached to this email: the PO is kept on the portal, so you always download the current version. / 本邮件不附带 PDF：采购订单保存在门户中，您下载的始终是最新版本。</p>`,
        '</td></tr></table>',
    ].join('');
}

module.exports = {
    portalBaseOf,
    poPortalUrl,
    registerUrl,
    normalizeEmail,
    parseRecipients,
    provisionList,
    outcomeFor,
    GRANTED_OUTCOMES,
    portalBlockHtml,
};
