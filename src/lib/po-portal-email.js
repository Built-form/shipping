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

const domainOf = e => {
    const at = e.lastIndexOf('@');
    return at < 0 ? '' : e.slice(at + 1);
};

// Mailbox providers anyone can sign up to. A staff address on one of these
// marks only itself as ours, never its whole domain: suppliers use qq.com and
// 163.com as often as we use gmail.com.
const PUBLIC_MAILBOX_DOMAINS = new Set([
    'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'hotmail.co.uk', 'live.com',
    'live.co.uk', 'msn.com', 'yahoo.com', 'yahoo.co.uk', 'icloud.com', 'me.com', 'aol.com',
    'proton.me', 'protonmail.com', 'qq.com', 'foxmail.com', '163.com', '126.com', 'yeah.net',
    'sina.com', 'sina.cn', 'sohu.com', 'aliyun.com', '139.com', '189.cn', 'tom.com',
]);

/**
 * Splits a send's recipients into the addresses to pre-approve on the portal
 * and the staff addresses to leave alone. A recipient is staff when it is a
 * ShipLine user (shipping_allowed_emails) or on the same company domain as
 * one; a colleague copied on a PO must not become a supplier user. Duplicates
 * (after normalising) collapse to one.
 */
function splitRecipients(recipients, staffEmails) {
    const staff = new Set((staffEmails || []).map(normalizeEmail).filter(Boolean));
    const staffDomains = new Set(
        [...staff].map(domainOf).filter(d => d && !PUBLIC_MAILBOX_DOMAINS.has(d))
    );
    const provision = [];
    const skipped = [];
    const seen = new Set();
    for (const raw of recipients || []) {
        const email = normalizeEmail(raw);
        if (!email || seen.has(email)) continue;
        seen.add(email);
        if (staff.has(email) || staffDomains.has(domainOf(email))) skipped.push(email);
        else provision.push(email);
    }
    return { provision, skipped };
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

const escapeHtml = s => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

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
    const href = escapeHtml(poUrl);
    const reg = escapeHtml(regUrl);
    const P = 'margin:0 0 10px;';
    const SMALL = 'margin:0 0 8px;font-size:13px;color:#444;';
    const account = preapproved
        ? [
            `<p style="${SMALL}"><strong>New to the portal?</strong> Register at <a href="${reg}">${reg}</a> with the email address this message was sent to. That address is already approved for your company, so you will see this order as soon as you sign in. There is no approval to wait for.</p>`,
            `<p style="${SMALL}"><strong>首次使用门户？</strong>请使用接收本邮件的邮箱地址在 <a href="${reg}">${reg}</a> 注册。该邮箱已获批准，登录后即可查看此订单，无需等待审核。</p>`,
        ]
        : [
            `<p style="${SMALL}"><strong>New to the portal?</strong> Register at <a href="${reg}">${reg}</a> with the email address this message was sent to, and our team will approve your access.</p>`,
            `<p style="${SMALL}"><strong>首次使用门户？</strong>请使用接收本邮件的邮箱地址在 <a href="${reg}">${reg}</a> 注册，我们的团队将审核并开通您的访问权限。</p>`,
        ];
    return [
        '<div style="margin:18px 0;padding:14px 16px;border:1px solid #d6dbe1;border-radius:4px;background:#f7f9fb;font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
        `<p style="${P}"><strong>PO ${po}${v}</strong> is on the supplier portal. Sign in, and download the PDF from the order's page. / 采购订单 <strong>${po}</strong>${vZh}已发布在供应商门户。请登录门户，在该订单页面下载 PDF。</p>`,
        `<p style="margin:0 0 12px;"><a href="${href}" style="display:inline-block;padding:9px 16px;background:#1f5fbf;color:#ffffff;text-decoration:none;border-radius:3px;font-weight:600;">Go to the supplier portal / 前往供应商门户</a></p>`,
        `<p style="${SMALL}">Or copy this address into your browser / 或将此地址复制到浏览器：<a href="${href}">${href}</a></p>`,
        ...account,
        `<p style="margin:0;font-size:12px;color:#666;">No PDF is attached to this email: the PO is kept on the portal, so you always download the current version. / 本邮件不附带 PDF：采购订单保存在门户中，您下载的始终是最新版本。</p>`,
        '</div>',
    ].join('');
}

module.exports = {
    portalBaseOf,
    poPortalUrl,
    registerUrl,
    normalizeEmail,
    splitRecipients,
    outcomeFor,
    GRANTED_OUTCOMES,
    PUBLIC_MAILBOX_DOMAINS,
    portalBlockHtml,
};
