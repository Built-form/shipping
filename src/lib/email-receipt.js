// ── Email receipt confirmation ──────────────────────────────────────────────
// Every outbound supplier/forwarder email (PO, delivery-quote request, QA
// sheet, signed PI) carries a "Confirm receipt" link. The link points at a
// PUBLIC, no-login endpoint (served by the dedicated emailReceiptApi Lambda,
// src/handlers/email-receipt.js) keyed by an unguessable token. Opening it shows
// a Confirm button; pressing it records email_receipts.confirmed_at and then
// redirects to a static confirmation page (or renders a built-in one if unset).
//
// Flow:
//   1. send handler → createEmailReceipt() mints a token + a pending row
//   2. appendReceiptLink() embeds the link in the email body before it's sent
//   3. after the *_sends row is written, linkReceiptToSend() ties them together
//   4. supplier opens link → GET markReceiptOpened() → renderConfirmButtonHtml()
//   5. supplier clicks the button → POST markReceiptConfirmed() → redirect (or
//      renderThankYouHtml() when no static page is configured)
// Two-state on purpose: an "open" is weak (link scanners fetch GETs), a
// "confirm" is strong (a form POST a bot won't submit). Ops reads per-email
// status via GET /api/v1/email-receipts (authed, orders.js).
const crypto = require('crypto');

// Path (on the shared httpApi domain) of the public confirm endpoint. Kept in
// sync with the route registered in serverless.yml + the emailReceiptApi
// handler (src/handlers/email-receipt.js).
const CONFIRM_PATH = '/api/v1/confirm-receipt';

// Idempotent table create. Takes a live connection so both the orders Lambda
// (cold-start migration) and the supplier-portal Lambda can call it without
// depending on which warmed first.
async function ensureEmailReceiptsSchema(conn) {
    await conn.query(`
        CREATE TABLE IF NOT EXISTS email_receipts (
            id INT NOT NULL AUTO_INCREMENT,
            token VARCHAR(64) NOT NULL,
            email_type VARCHAR(48) NOT NULL,
            send_table VARCHAR(64) NULL,
            send_id INT NULL,
            sent_to TEXT NULL,
            subject VARCHAR(500) NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            opened_at DATETIME NULL,
            open_count INT NOT NULL DEFAULT 0,
            confirmed_at DATETIME NULL,
            confirmed_ip VARCHAR(64) NULL,
            confirmed_user_agent VARCHAR(500) NULL,
            reminder_count INT NOT NULL DEFAULT 0,
            last_reminder_at DATETIME NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uk_token (token),
            KEY idx_send (send_table, send_id)
        )
    `);
    // Additive migration for any table created before the two-state model
    // (opened = weak GET signal, confirmed = strong POST-button signal). Each
    // ALTER is a no-op if the column already exists (errno 1060 = duplicate).
    const addColumns = [
        `ALTER TABLE email_receipts ADD COLUMN opened_at DATETIME NULL`,
        `ALTER TABLE email_receipts ADD COLUMN open_count INT NOT NULL DEFAULT 0`,
        `ALTER TABLE email_receipts ADD COLUMN confirmed_at DATETIME NULL`,
        `ALTER TABLE email_receipts ADD COLUMN confirmed_ip VARCHAR(64) NULL`,
        `ALTER TABLE email_receipts ADD COLUMN confirmed_user_agent VARCHAR(500) NULL`,
        `ALTER TABLE email_receipts ADD COLUMN reminder_count INT NOT NULL DEFAULT 0`,
        `ALTER TABLE email_receipts ADD COLUMN last_reminder_at DATETIME NULL`,
    ];
    for (const sql of addColumns) {
        try { await conn.query(sql); }
        catch (e) { if (!e || e.errno !== 1060) throw e; }
    }
    // Ledger of individual follow-up (reminder) sends, one row per send. The
    // parent's reminder_count/last_reminder_at is the summary; this is the
    // detail the UI lists. sent_by_email is NULL for the automated sweep, the
    // operator's email for a manual "resend" click.
    await conn.query(`
        CREATE TABLE IF NOT EXISTS email_receipt_reminders (
            id INT NOT NULL AUTO_INCREMENT,
            email_receipt_id INT NOT NULL,
            reminder_number INT NOT NULL,
            sent_to TEXT NULL,
            subject VARCHAR(500) NULL,
            front_message_uid VARCHAR(255) NULL,
            front_conversation_id VARCHAR(255) NULL,
            sent_by_email VARCHAR(255) NULL,
            created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY idx_receipt (email_receipt_id, id)
        )
    `);
}

// Absolute base URL (scheme + host) for building the confirm link. Prefers an
// explicit PUBLIC_API_BASE_URL override (e.g. a custom domain); otherwise
// derives it from the inbound request — the confirm endpoint lives on the SAME
// httpApi domain as the /email routes, so the request's own host is correct.
function apiBaseUrlFromReq(req) {
    const envBase = (process.env.PUBLIC_API_BASE_URL || '').replace(/\/+$/, '');
    if (envBase) return envBase;
    const proto = String(req.headers['x-forwarded-proto'] || 'https').split(',')[0].trim();
    const host = req.headers['x-forwarded-host'] || req.headers.host;
    return host ? `${proto}://${host}` : '';
}

function newToken() {
    return crypto.randomBytes(24).toString('hex'); // 48 hex chars — unguessable
}

// Insert a pending receipt row and return its token. Called just before the
// email body is assembled so the token can be embedded in the link.
async function createEmailReceipt(conn, { emailType, sentTo, subject }) {
    const token = newToken();
    await conn.query(
        `INSERT INTO email_receipts (token, email_type, sent_to, subject)
         VALUES (?, ?, ?, ?)`,
        [
            token,
            emailType,
            Array.isArray(sentTo) ? JSON.stringify(sentTo) : (sentTo || null),
            subject || null,
        ]
    );
    return token;
}

// After the send row is written, point the receipt at it so a click can be
// traced back to a specific send. Best-effort — a failure here must never fail
// the send itself.
async function linkReceiptToSend(conn, token, sendTable, sendId) {
    if (!token || !sendId) return;
    try {
        await conn.query(
            `UPDATE email_receipts SET send_table = ?, send_id = ? WHERE token = ?`,
            [sendTable, sendId, token]
        );
    } catch (e) { /* linkage is non-essential */ }
}

// Load the receipt a resend needs: recipients, subject, the original token
// (reused so the reminder's confirm link updates the SAME row), and whether
// it's already confirmed. Returns null for an unknown id.
async function getReceiptForResend(conn, id) {
    if (!Number.isFinite(Number(id))) return null;
    const [rows] = await conn.query(
        `SELECT id, token, email_type, sent_to, subject, confirmed_at, reminder_count
           FROM email_receipts WHERE id = ?`,
        [Number(id)]
    );
    return rows.length ? rows[0] : null;
}

// Record a reminder that just went out: append a ledger row (the per-send
// detail the UI lists) AND bump the parent's reminder_count/last_reminder_at
// (the summary). `meta` carries what was sent + Front provenance; sentByEmail
// is null for the automated sweep, the operator for a manual resend.
async function recordReminderSend(conn, receiptId, meta = {}) {
    const id = Number(receiptId);
    await conn.query(
        `INSERT INTO email_receipt_reminders
            (email_receipt_id, reminder_number, sent_to, subject,
             front_message_uid, front_conversation_id, sent_by_email)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
            id,
            Number(meta.reminderNumber) || null,
            Array.isArray(meta.sentTo) ? JSON.stringify(meta.sentTo) : (meta.sentTo || null),
            meta.subject ? String(meta.subject).slice(0, 500) : null,
            meta.frontMessageUid || null,
            meta.frontConversationId || null,
            meta.sentByEmail || null,
        ]
    );
    await conn.query(
        `UPDATE email_receipts
            SET reminder_count = reminder_count + 1,
                last_reminder_at = NOW()
          WHERE id = ?`,
        [id]
    );
}

// List the follow-up (reminder) sends for a receipt, oldest first.
async function listReminders(conn, receiptId) {
    if (!Number.isFinite(Number(receiptId))) return [];
    const [rows] = await conn.query(
        `SELECT id, email_receipt_id, reminder_number, sent_to, subject,
                front_message_uid, front_conversation_id, sent_by_email, created_at
           FROM email_receipt_reminders
          WHERE email_receipt_id = ?
          ORDER BY id ASC`,
        [Number(receiptId)]
    );
    return rows;
}

// Recipients are stored on the receipt row as a JSON array (multi-recipient
// sends) or a bare string (single). Normalise either shape to a clean string[].
function parseRecipients(sentTo) {
    if (!sentTo) return [];
    let list;
    try {
        const parsed = JSON.parse(sentTo);
        list = Array.isArray(parsed) ? parsed : [parsed];
    } catch { list = [sentTo]; }
    return list.map((a) => String(a || '').trim()).filter(Boolean);
}

// Body of a standalone reminder email (bilingual). The confirm button + opt-out
// note are added afterwards by appendReceiptLink() with the ORIGINAL token, so a
// click still lands on the same email_receipts row. `subject` is the original
// email's subject, shown so the recipient knows which email to acknowledge.
// `reminderNumber` (1-based) is shown so both sides can see how many times we've
// followed up.
function renderReminderEmailHtml(subject, reminderNumber) {
    const ref = subject ? esc(subject) : null;
    const n = Number(reminderNumber) > 0 ? Math.floor(Number(reminderNumber)) : null;
    return [
        '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#333;line-height:1.6;">',
        n ? `<p style="margin:0 0 12px;font-size:12px;color:#999;">Reminder #${n} · 第 ${n} 次提醒</p>` : '',
        '<p style="margin:0 0 12px;">Hello,</p>',
        '<p style="margin:0 0 12px;">This is a friendly reminder about our earlier email' ,
        ref ? ` regarding <strong>${ref}</strong>` : '',
        '. We would be grateful if you could confirm you have received it.</p>',
        '<p style="margin:0 0 4px;color:#555;">您好，</p>',
        '<p style="margin:0;color:#555;">这是关于我们之前' ,
        ref ? ` <strong>${ref}</strong> ` : '',
        '邮件的友情提醒。如您已收到，敬请确认，谢谢。</p>',
        '</div>',
    ].join('');
}

// Append the confirm-receipt link block to an HTML email body. If no token
// (shouldn't happen) or no base URL, returns the body unchanged rather than
// embedding a broken link.
function appendReceiptLink(htmlBody, token, baseUrl) {
    const base = (baseUrl || '').replace(/\/+$/, '');
    if (!token || !base) return htmlBody || '';
    const url = `${base}${CONFIRM_PATH}/${token}`;
    const block = [
        '<div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;color:#555;line-height:1.5;margin-top:28px;padding-top:16px;border-top:1px solid #e5e7eb;">',
        '<p style="margin:0 0 12px;">Kindly confirm you have received this email:<br><span style="color:#777;">请确认您已收到此邮件：</span></p>',
        `<p style="margin:0 0 12px;"><a href="${url}" style="display:inline-block;background:#0b6bcb;color:#ffffff;text-decoration:none;padding:11px 20px;border-radius:6px;font-weight:600;">I've read it · 看好了</a></p>`,
        '<p style="margin:0;font-size:12px;color:#888;line-height:1.5;">To opt out of automated follow-up reminders for this email, please confirm receipt above.<br>如需停止接收关于此邮件的自动跟进提醒，请在上方确认收到。</p>',
        '</div>',
    ].join('');
    return `${htmlBody || ''}${block}`;
}

const TOKEN_RE = /^[a-f0-9]{16,128}$/;

// Record a landing-page view (the GET). WEAK signal: automated email security
// scanners (Microsoft Safe Links, Mimecast, Proofpoint…) fetch links too, so an
// "open" may be a bot rather than the recipient. Stamps opened_at once and bumps
// open_count. Returns the receipt row (with `alreadyConfirmed`) so the landing
// page can show context, or null if the token is unknown/malformed.
async function markReceiptOpened(conn, token) {
    if (typeof token !== 'string' || !TOKEN_RE.test(token)) return null;
    const [rows] = await conn.query(
        `SELECT id, token, email_type, subject, sent_to, opened_at, open_count, confirmed_at
           FROM email_receipts WHERE token = ?`,
        [token]
    );
    if (!rows.length) return null;
    const row = rows[0];
    await conn.query(
        `UPDATE email_receipts
            SET opened_at = COALESCE(opened_at, NOW()),
                open_count = open_count + 1
          WHERE id = ?`,
        [row.id]
    );
    return { ...row, alreadyConfirmed: !!row.confirmed_at };
}

// Record a confirmation (the POST from the button press). STRONG signal — a
// scanner issues GETs, not form POSTs, so this can't be tripped by link
// pre-fetching. Idempotent: the first confirmation stamps confirmed_at + IP/UA;
// later ones leave those untouched. Returns the receipt row (with
// `alreadyConfirmed`), or null if the token is unknown/malformed.
async function markReceiptConfirmed(conn, token, { userAgent, ip } = {}) {
    if (typeof token !== 'string' || !TOKEN_RE.test(token)) return null;
    const [rows] = await conn.query(
        `SELECT id, token, email_type, subject, sent_to, confirmed_at
           FROM email_receipts WHERE token = ?`,
        [token]
    );
    if (!rows.length) return null;
    const row = rows[0];
    const alreadyConfirmed = !!row.confirmed_at;
    await conn.query(
        `UPDATE email_receipts
            SET confirmed_at = COALESCE(confirmed_at, NOW()),
                confirmed_user_agent = COALESCE(confirmed_user_agent, ?),
                confirmed_ip = COALESCE(confirmed_ip, ?)
          WHERE id = ?`,
        [
            userAgent ? String(userAgent).slice(0, 500) : null,
            ip ? String(ip).slice(0, 64) : null,
            row.id,
        ]
    );
    return { ...row, alreadyConfirmed };
}

function esc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

// Full server-rendered confirmation page. `receipt` may be null (unknown token).
function renderThankYouHtml(receipt) {
    const known = !!receipt;
    const subject = receipt && receipt.subject ? esc(receipt.subject) : null;
    const title = known ? 'Thank you for confirming' : 'Link not recognised';
    const heading = known ? 'Thank you for confirming' : 'This link is not valid';
    const zhHeading = known ? '感谢您的确认' : '此链接无效';
    const message = known
        ? 'We have recorded that you received this email. There is nothing more you need to do — you can close this window.'
        : 'This confirmation link is invalid or has expired. You can close this window.';
    const zhMessage = known
        ? '我们已记录您收到此邮件。您无需再做任何操作，可以关闭此窗口。'
        : '此确认链接无效或已过期。您可以关闭此窗口。';
    const icon = known ? '&#10003;' : '&#33;'; // check mark / exclamation
    const accent = known ? '#16a34a' : '#b45309';
    return [
        '<!doctype html>',
        '<html lang="en"><head>',
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        `<title>${esc(title)}</title>`,
        '</head>',
        '<body style="margin:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;color:#111827;">',
        '<div style="max-width:520px;margin:0 auto;padding:48px 20px;">',
        '<div style="background:#ffffff;border-radius:14px;box-shadow:0 1px 3px rgba(0,0,0,0.08);padding:40px 32px;text-align:center;">',
        `<div style="width:64px;height:64px;border-radius:50%;background:${accent};color:#ffffff;font-size:34px;line-height:64px;margin:0 auto 20px;">${icon}</div>`,
        `<h1 style="margin:0 0 4px;font-size:22px;">${esc(heading)}</h1>`,
        `<div style="margin:0 0 12px;font-size:17px;color:#374151;">${esc(zhHeading)}</div>`,
        `<p style="margin:0;font-size:15px;color:#4b5563;line-height:1.6;">${esc(message)}</p>`,
        `<p style="margin:8px 0 0;font-size:14px;color:#6b7280;line-height:1.7;">${esc(zhMessage)}</p>`,
        subject ? `<p style="margin:20px 0 0;font-size:13px;color:#9ca3af;">Re: ${subject}</p>` : '',
        '</div>',
        '<p style="text-align:center;margin:24px 0 0;font-size:12px;color:#9ca3af;">JFA Medical Ltd.</p>',
        '</div>',
        '</body></html>',
    ].join('');
}

// Landing page rendered on GET: a single "Confirm receipt" button that POSTs
// back to `actionUrl` (the same /confirm-receipt/{token} path). Requiring the
// POST is what makes confirmation trustworthy — scanners follow the GET link
// but don't submit the form. Unknown token → neutral "not recognised" page;
// already-confirmed → the thank-you page (no second confirm needed).
function renderConfirmButtonHtml(receipt, actionUrl) {
    if (!receipt) return renderThankYouHtml(null);
    if (receipt.alreadyConfirmed) return renderThankYouHtml(receipt);
    const subject = receipt.subject ? esc(receipt.subject) : null;
    return [
        '<!doctype html>',
        '<html lang="en"><head>',
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        '<title>Confirm receipt · 确认收到</title>',
        '</head>',
        '<body style="margin:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;color:#111827;">',
        '<div style="max-width:520px;margin:0 auto;padding:48px 20px;">',
        '<div style="background:#ffffff;border-radius:14px;box-shadow:0 1px 3px rgba(0,0,0,0.08);padding:40px 32px;text-align:center;">',
        '<h1 style="margin:0 0 4px;font-size:22px;">Confirm receipt</h1>',
        '<div style="margin:0 0 12px;font-size:17px;color:#374151;">确认收到</div>',
        '<p style="margin:0 0 6px;font-size:15px;color:#4b5563;line-height:1.6;">Please confirm you have received this email by clicking the button below.</p>',
        '<p style="margin:0 0 24px;font-size:14px;color:#6b7280;line-height:1.7;">请点击下方按钮确认您已收到此邮件。</p>',
        `<form method="POST" action="${esc(actionUrl)}">`,
        '<button type="submit" style="background:#0b6bcb;color:#ffffff;border:none;cursor:pointer;padding:13px 26px;border-radius:6px;font-weight:600;font-size:15px;">I\'ve read it · 看好了</button>',
        '</form>',
        subject ? `<p style="margin:24px 0 0;font-size:13px;color:#9ca3af;">Re: ${subject}</p>` : '',
        '</div>',
        '<p style="text-align:center;margin:24px 0 0;font-size:12px;color:#9ca3af;">JFA Medical Ltd.</p>',
        '</div>',
        '</body></html>',
    ].join('');
}

module.exports = {
    CONFIRM_PATH,
    ensureEmailReceiptsSchema,
    apiBaseUrlFromReq,
    createEmailReceipt,
    linkReceiptToSend,
    appendReceiptLink,
    getReceiptForResend,
    recordReminderSend,
    listReminders,
    renderReminderEmailHtml,
    parseRecipients,
    markReceiptOpened,
    markReceiptConfirmed,
    renderConfirmButtonHtml,
    renderThankYouHtml,
};
