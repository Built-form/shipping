'use strict';

// One look for every email the supplier portal sends or points at: a light
// page, a white card with a "JFA Medical · Supplier Portal" header, the body,
// and a footer. Table-based with every style inline, as email clients (QQ,
// 163, Outlook, Gmail) need. Nothing is editable by ops here: the templates
// in email_templates are the INSIDE of the card; the senders of the portal
// emails wrap them with wrapEmail at send time (pi-submission-routes.js,
// file-transfer-routes.js, the PO portal send in orders.js), so an ops edit to
// the wording keeps the frame.
//
// The same helper exists in JFPRO (api/email-layout.js) for the emails that
// app sends about the portal; keep the two in step when changing the look.

const FONT = "'Helvetica Neue',Helvetica,Arial,'PingFang SC','Microsoft YaHei',sans-serif";
const BLUE = '#1f5fbf';
const INK = '#1f2933';
const MUTED = '#6b7a8c';
const BORDER = '#e1e6ec';
const PAGE_BG = '#f3f5f8';
const CODE_BG = '#f4f7fb';
const CODE_BORDER = '#b9c6d6';
const NOTE_BG = '#fff8e6';
const NOTE_BORDER = '#e3a008';

const BRAND = 'JFA Medical';
const PRODUCT = 'Supplier Portal · 供应商门户';

const escapeHtml = s => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

const P = `margin:0 0 14px;font-family:${FONT};font-size:15px;line-height:1.6;color:${INK};`;
const SMALL = `margin:0 0 10px;font-family:${FONT};font-size:13px;line-height:1.5;color:${MUTED};`;

/** A paragraph of (already HTML) content in the body style. */
const para = html => `<p style="${P}">${html}</p>`;

/** A quieter paragraph: explanations, fallbacks. */
const small = html => `<p style="${SMALL}">${html}</p>`;

/** A section heading, English with the Chinese beside it. */
const heading = (en, zh) =>
    `<h2 style="margin:0 0 12px;font-family:${FONT};font-size:18px;line-height:1.35;font-weight:700;color:${INK};">${escapeHtml(en)}${zh ? ` <span style="font-weight:500;color:${MUTED};">/ ${escapeHtml(zh)}</span>` : ''}</h2>`;

/** The primary action: a solid button. `label` is plain text. */
const button = (href, label) =>
    `<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:6px 0 16px;"><tr><td style="border-radius:6px;background:${BLUE};">`
    + `<a href="${escapeHtml(href)}" style="display:inline-block;padding:12px 22px;font-family:${FONT};font-size:15px;font-weight:700;line-height:1.2;color:#ffffff;text-decoration:none;border-radius:6px;">${escapeHtml(label)}</a>`
    + '</td></tr></table>';

/** The address under a button, for clients that drop buttons or links. */
const linkFallback = href =>
    small(`Or copy this address into your browser / 或将此地址复制到浏览器：<br><a href="${escapeHtml(href)}" style="color:${BLUE};word-break:break-all;">${escapeHtml(href)}</a>`);

/**
 * A code or password, large and easy to read and copy: a dashed box with the
 * value in monospace. `value` is plain text (or a Cognito placeholder).
 */
const codeBox = (label, value) =>
    `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:6px 0 18px;"><tr><td align="center" style="background:${CODE_BG};border:1px dashed ${CODE_BORDER};border-radius:8px;padding:16px 20px;">`
    + `<div style="font-family:${FONT};font-size:12px;line-height:1.4;letter-spacing:1px;text-transform:uppercase;color:${MUTED};margin-bottom:6px;">${escapeHtml(label)}</div>`
    + `<div style="font-family:Menlo,Consolas,'Courier New',monospace;font-size:30px;line-height:1.3;font-weight:700;letter-spacing:4px;color:${INK};">${value}</div>`
    + '</td></tr></table>';

/** A note from our team, set apart. `bodyHtml` is already escaped. */
const noteBox = (label, bodyHtml) =>
    `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:6px 0 18px;"><tr><td style="background:${NOTE_BG};border-left:4px solid ${NOTE_BORDER};border-radius:4px;padding:12px 16px;">`
    + `<div style="font-family:${FONT};font-size:13px;line-height:1.4;font-weight:700;color:${INK};margin-bottom:4px;">${escapeHtml(label)}</div>`
    + `<div style="font-family:${FONT};font-size:14px;line-height:1.55;color:${INK};">${bodyHtml}</div>`
    + '</td></tr></table>';

/** A two-column facts list: [[label, valueHtml], …]. */
const facts = rows =>
    `<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 16px;border-collapse:collapse;">`
    + rows.map(([k, v]) => `<tr><td style="padding:4px 16px 4px 0;font-family:${FONT};font-size:13px;color:${MUTED};white-space:nowrap;vertical-align:top;">${escapeHtml(k)}</td><td style="padding:4px 0;font-family:${FONT};font-size:14px;color:${INK};">${v}</td></tr>`).join('')
    + '</table>';

/** The sign-off every portal email ends with. */
const signoff = () => para(`Kind Regards,<br>Operations Team<br>${BRAND} Ltd.`);

/**
 * The whole email around `bodyHtml`: page, card, header and footer. `title`
 * (plain text) is the card's heading line under the brand, usually the
 * email's subject in short. A full HTML document; Front, SES and Cognito all
 * take one.
 */
function wrapEmail({ title = '', bodyHtml = '', footerHtml = '' } = {}) {
    const t = escapeHtml(title);
    return [
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">',
        t ? `<title>${t}</title>` : '',
        '</head>',
        `<body style="margin:0;padding:0;background:${PAGE_BG};">`,
        `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:${PAGE_BG};"><tr><td align="center" style="padding:24px 12px;">`,
        `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:600px;background:#ffffff;border:1px solid ${BORDER};border-radius:8px;">`,
        // Header
        `<tr><td style="padding:18px 28px;border-bottom:1px solid ${BORDER};">`,
        `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%"><tr>`,
        `<td style="font-family:${FONT};font-size:16px;font-weight:700;color:${INK};">${BRAND}</td>`,
        `<td align="right" style="font-family:${FONT};font-size:13px;color:${MUTED};">${PRODUCT}</td>`,
        '</tr></table></td></tr>',
        // Body
        `<tr><td style="padding:26px 28px 8px;">`,
        t ? heading(title) : '',
        bodyHtml,
        '</td></tr>',
        // Footer
        `<tr><td style="padding:14px 28px 18px;border-top:1px solid ${BORDER};font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">`,
        footerHtml || `${BRAND} Ltd · This message was sent by our supplier portal. / 此邮件由我们的供应商门户发送。`,
        '</td></tr>',
        '</table></td></tr></table></body></html>',
    ].join('');
}

module.exports = {
    FONT, BLUE, INK, MUTED, BORDER, PAGE_BG, CODE_BG, CODE_BORDER, NOTE_BG, NOTE_BORDER, BRAND, PRODUCT,
    escapeHtml, para, small, heading, button, linkFallback, codeBox, noteBox, facts, signoff, wrapEmail,
};
