// ── Email receipts: the log of every email sent, and how each is confirmed ──
// email_receipts holds one row per email sent to a supplier, forwarder or
// inspector; ShipLine's Email Receipts page lists them
// (GET /api/v1/email-receipts, orders.js). confirm_via says how an email gets
// confirmed (2026-11-12_10):
//   'email'   it carries the "Confirm receipt" button: the delivery-quote
//             request, the QA sheet, and a QC visit email with no link of its
//             own. The two-step flow below. (So did the signed PI while it
//             went out as an attachment; those rows keep their button.)
//   'portal'  it carries a link into the supplier portal or a no-login magic
//             link, and NO confirm button, so the recipient is asked to do one
//             thing, not two: the PO, the signed PI, the artwork request, the
//             "a file is waiting" notice, a QC visit email with an
//             availability or schedule link, JFPRO's quote request. What the
//             recipient does through the link sets confirmed_at
//             (syncPortalReceipts).
//   'none'    a notice; nothing is asked: a PI decision, a portal approval.
// Only an 'email' row, or a 'portal' row with an action_url (the PO, the signed
// PI), is chased with reminders (src/services/receipt-reminders.js).
//
// The confirm button's link points at a PUBLIC, no-login endpoint (served by
// the dedicated emailReceiptApi Lambda, src/handlers/email-receipt.js) keyed by
// an unguessable token. Opening it shows a Confirm button; pressing it records
// email_receipts.confirmed_at and then redirects to a static confirmation page
// (or renders a built-in one if unset).
//
// Flow of an 'email' receipt:
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
const log = require('./logger');
const { jportalSchema, isSourceMissing } = require('./jportal');

const CONFIRM_VIA = ['email', 'portal', 'none'];

/** A row's confirm_via; a row from before the column existed is 'email'. */
const confirmViaOf = row => (row && CONFIRM_VIA.includes(row.confirm_via) ? row.confirm_via : 'email');

/**
 * Whether a reminder can be sent for this row: it has the confirm button, or it
 * is a portal email whose link a reminder may repeat (action_url).
 */
const canRemind = row => confirmViaOf(row) === 'email' || (confirmViaOf(row) === 'portal' && Boolean(row.action_url));

// Path (on the shared httpApi domain) of the public confirm endpoint. Kept in
// sync with the route registered in serverless.yml + the emailReceiptApi
// handler (src/handlers/email-receipt.js).
const CONFIRM_PATH = '/api/v1/confirm-receipt';

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

// Insert a receipt row and return its token. For an 'email' receipt this is
// called just before the email body is assembled, so the token can be embedded
// in the link (and discardEmailReceipt drops the row if Front then refuses the
// email). sendTable / sendId may be given here when the send row already exists.
async function createEmailReceipt(conn, { emailType, sentTo, subject, confirmVia = 'email', actionUrl = null, sendTable = null, sendId = null }) {
    const token = newToken();
    await conn.query(
        `INSERT INTO email_receipts (token, email_type, send_table, send_id, sent_to, subject, confirm_via, action_url)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            token,
            emailType,
            sendTable || null,
            sendId || null,
            Array.isArray(sentTo) ? JSON.stringify(sentTo) : (sentTo || null),
            subject ? String(subject).slice(0, 500) : null,
            CONFIRM_VIA.includes(confirmVia) ? confirmVia : 'email',
            actionUrl ? String(actionUrl).slice(0, 1000) : null,
        ]
    );
    return token;
}

// Record an email that has ALREADY gone out with no confirm button
// (confirmVia 'portal' or 'none'), so it shows on the Email Receipts page.
// The email is sent, so a failure here is logged and never thrown.
// → the row's token, or null.
async function logSentEmail(conn, fields) {
    try {
        return await createEmailReceipt(conn, { confirmVia: 'none', ...fields });
    } catch (e) {
        log.warn('[email-receipt] a sent email was not recorded', { emailType: fields && fields.emailType, error: e.message });
        return null;
    }
}

// Front refused the email: drop the receipt minted for it, so the page lists
// only emails that went out and the reminder sweep never chases one that did
// not. Only a row no send was linked to. Never throws.
async function discardEmailReceipt(conn, token) {
    if (!token) return;
    try {
        await conn.query(
            `DELETE FROM email_receipts
              WHERE token = ? AND send_id IS NULL AND opened_at IS NULL AND confirmed_at IS NULL AND reminder_count = 0`,
            [token]
        );
    } catch (e) {
        log.warn('[email-receipt] an unsent email\'s receipt was not removed', { error: e.message });
    }
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
        `SELECT id, token, email_type, sent_to, subject, confirmed_at, reminder_count, confirm_via, action_url
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
// followed up. `portal`: the reminder of a portal email, which asks the
// recipient to open it on the portal (appendPortalLink adds the button) and
// never to confirm receipt.
function renderReminderEmailHtml(subject, reminderNumber, { portal = false } = {}) {
    const ref = subject ? esc(subject) : null;
    const n = Number(reminderNumber) > 0 ? Math.floor(Number(reminderNumber)) : null;
    return [
        '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#333;line-height:1.6;">',
        n ? `<p style="margin:0 0 12px;font-size:12px;color:#999;">Reminder #${n} · 第 ${n} 次提醒</p>` : '',
        '<p style="margin:0 0 12px;">Hello,</p>',
        '<p style="margin:0 0 12px;">This is a friendly reminder about our earlier email' ,
        ref ? ` regarding <strong>${ref}</strong>` : '',
        portal
            ? '. It is waiting for you on our supplier portal.</p>'
            : '. We would be grateful if you could confirm you have received it.</p>',
        '<p style="margin:0 0 4px;color:#555;">您好，</p>',
        '<p style="margin:0;color:#555;">这是关于我们之前' ,
        ref ? ` <strong>${ref}</strong> ` : '',
        portal
            ? '邮件的友情提醒。相关内容已发布在供应商门户，请前往门户查看，谢谢。</p>'
            : '邮件的友情提醒。如您已收到，敬请确认，谢谢。</p>',
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
    const { FONT, INK, MUTED, BORDER, button } = require('./email-layout');
    const block = [
        `<div style="font-family:${FONT};font-size:13px;color:${INK};line-height:1.5;margin-top:28px;padding-top:16px;border-top:1px solid ${BORDER};">`,
        `<p style="margin:0 0 4px;">Kindly confirm you have received this email:<br><span style="color:${MUTED};">请确认您已收到此邮件：</span></p>`,
        button(url, "I've read it · 看好了"),
        `<p style="margin:0;font-size:12px;color:${MUTED};line-height:1.5;">To opt out of automated follow-up reminders for this email, please confirm receipt above.<br>如需停止接收关于此邮件的自动跟进提醒，请在上方确认收到。</p>`,
        '</div>',
    ].join('');
    return `${htmlBody || ''}${block}`;
}

// The block under a PORTAL email's reminder: the button to the portal page the
// first email pointed at, in place of the confirm button. No url → the body
// unchanged.
function appendPortalLink(htmlBody, url) {
    if (!url) return htmlBody || '';
    const { FONT, INK, MUTED, BORDER, button, linkFallback } = require('./email-layout');
    const block = [
        `<div style="font-family:${FONT};font-size:13px;color:${INK};line-height:1.5;margin-top:28px;padding-top:16px;border-top:1px solid ${BORDER};">`,
        `<p style="margin:0 0 4px;">Please open it on the supplier portal:<br><span style="color:${MUTED};">请在供应商门户中查看：</span></p>`,
        button(url, 'Go to the supplier portal / 前往供应商门户'),
        linkFallback(url),
        `<p style="margin:0;font-size:12px;color:${MUTED};line-height:1.5;">These automated reminders stop once it has been opened on the portal.<br>在门户中查看后，将不再发送关于此邮件的自动提醒。</p>`,
        '</div>',
    ].join('');
    return `${htmlBody || ''}${block}`;
}

// ── What the recipient did through a portal email's link ────────────────────
// A 'portal' email has no confirm button. Its row is confirmed (or, for a link
// a scanner could fetch, opened) from what the portal recorded AFTER the email
// went out. Shipping is not told when that happens (the portal writes its own
// tables), so the rows are brought up to date when they are read: before the
// Email Receipts page lists them, before a reminder is sent, and at the start
// of the reminder sweep. Also here: the two supplier emails JFPRO sends about
// the portal are copied in from its audit log, so the page lists them too.
//
// Each step is its own statement and its own failure: a schema that is not on
// this server (jportal, jfpro) is skipped quietly, anything else is logged.
// → { skipped } or { failed: [step names] }. Never throws.
const PORTAL_SYNC_MS = 20 * 1000;
let lastPortalSync = 0;

const JFPRO_AUDIT = 'jfpro.audit_log';
// As JFPRO's api/portal-approval-email.js sends it.
const PORTAL_APPROVAL_SUBJECT = 'Your supplier portal account is approved / 您的供应商门户账户已获批准';

const PENDING_PORTAL = `r.confirm_via = 'portal' AND r.confirmed_at IS NULL`;

// The portal emails that are chased (they store an action_url), by the send
// table their receipt names, with the sync step that confirms each. A step that
// failed this run leaves its emails unchased (receipt-reminders.js).
const CHASED_PORTAL_STEPS = {
    purchase_order_document_sends: 'po_viewed',
    purchase_order_signed_pi_sends: 'signed_pi_viewed',
};

function portalSyncSteps(jp) {
    // The emailed version, or a later version of the same PO: a supplier who
    // downloaded v3 is not chased about the email for v2.
    const poView = `${jp}.po_document_views v
                 WHERE v.purchase_order_id = d.purchase_order_id
                   AND v.purchase_order_document_id >= d.id
                   AND v.last_viewed_at >= r.created_at`;
    // The emailed signed PI, or a later one of the same PO, for the same reason.
    const signedPiView = `${jp}.signed_pi_views v
                 WHERE v.purchase_order_id = d.purchase_order_id
                   AND v.purchase_order_signed_pi_id >= d.id
                   AND v.last_viewed_at >= r.created_at`;
    const artworkTick = `artwork_confirmations c
                 WHERE c.purchase_order_id = q.purchase_order_id
                   AND c.created_at >= r.created_at
                   AND (c.request_id = q.id OR JSON_CONTAINS(q.order_ids, CAST(c.order_id AS JSON)))`;
    const fileView = `${jp}.file_transfer_views v
                 WHERE v.transfer_id = r.send_id AND v.viewer_kind = 'supplier'`;
    const availabilityAnswer = `${jp}.qc_availability_requests a
                 WHERE a.visit_id = s.visit_id AND a.party = s.audience
                   AND a.answered_at >= r.created_at`;
    return [
        // The PO email: a supplier's user opened that PDF on the portal.
        ['po_viewed', `
            UPDATE email_receipts r
              JOIN purchase_order_document_sends s ON s.id = r.send_id
              JOIN purchase_order_documents d ON d.id = s.purchase_order_document_id
               SET r.confirmed_at = (SELECT MIN(IF(v.first_viewed_at >= r.created_at, v.first_viewed_at, v.last_viewed_at))
                                       FROM ${poView})
             WHERE ${PENDING_PORTAL} AND r.send_table = 'purchase_order_document_sends'
               AND EXISTS (SELECT 1 FROM ${poView})`],
        // The signed PI email: a supplier's user opened that file on the portal.
        ['signed_pi_viewed', `
            UPDATE email_receipts r
              JOIN purchase_order_signed_pi_sends s ON s.id = r.send_id
              JOIN purchase_order_signed_pis d ON d.id = s.purchase_order_signed_pi_id
               SET r.confirmed_at = (SELECT MIN(IF(v.first_viewed_at >= r.created_at, v.first_viewed_at, v.last_viewed_at))
                                       FROM ${signedPiView})
             WHERE ${PENDING_PORTAL} AND r.send_table = 'purchase_order_signed_pi_sends'
               AND EXISTS (SELECT 1 FROM ${signedPiView})`],
        // The artwork request: the supplier ticked one of its items, through
        // the link or signed in.
        ['artwork_ticked', `
            UPDATE email_receipts r
              JOIN artwork_confirmation_requests q ON q.id = r.send_id
               SET r.confirmed_at = (SELECT MIN(c.created_at) FROM ${artworkTick})
             WHERE ${PENDING_PORTAL} AND r.send_table = 'artwork_confirmation_requests'
               AND EXISTS (SELECT 1 FROM ${artworkTick})`],
        // The "a file is waiting" notice: a supplier's user opened the file.
        ['file_opened', `
            UPDATE email_receipts r
               SET r.confirmed_at = (SELECT MIN(v.first_viewed_at) FROM ${fileView})
             WHERE ${PENDING_PORTAL} AND r.send_table = 'file_transfers'
               AND EXISTS (SELECT 1 FROM ${fileView})`],
        // A QC visit email with a link: the party answered the availability
        // page (a form they submit: confirmed) ...
        ['qc_availability_answered', `
            UPDATE email_receipts r
              JOIN qc_visit_sends s ON s.id = r.send_id
               SET r.confirmed_at = (SELECT MIN(a.answered_at) FROM ${availabilityAnswer})
             WHERE ${PENDING_PORTAL} AND r.send_table = 'qc_visit_sends'
               AND EXISTS (SELECT 1 FROM ${availabilityAnswer})`],
        // ... or the schedule page was fetched (a GET a mail scanner makes
        // too: opened, never confirmed).
        ['qc_schedule_opened', `
            UPDATE email_receipts r
              JOIN qc_visit_sends s ON s.id = r.send_id
              JOIN qc_visit_links l ON l.id = s.schedule_link_id
               SET r.opened_at = IF(l.first_opened_at >= r.created_at, l.first_opened_at, l.last_opened_at),
                   r.open_count = GREATEST(r.open_count, 1)
             WHERE ${PENDING_PORTAL} AND r.send_table = 'qc_visit_sends' AND r.opened_at IS NULL
               AND l.last_opened_at >= r.created_at`],
        // JFPRO's quote request: the supplier answered it on the portal.
        ['quote_answered', `
            UPDATE email_receipts r
              JOIN ${JFPRO_AUDIT} a ON a.id = r.send_id
              JOIN ${jp}.quote_requests q ON q.id = a.entity_id
               SET r.confirmed_at = q.answered_at
             WHERE ${PENDING_PORTAL} AND r.send_table = '${JFPRO_AUDIT}' AND r.email_type = 'quote_request'
               AND q.answered_at IS NOT NULL`],
    ];
}

// JFPRO's supplier emails about the portal, from its audit log: the approval
// email ('approval_email_sent', stored cut to the column's 16 characters) and
// the quote-request email ('email_sent' on a quote_request). One receipt per
// audit row: the token is derived from the row's id, so a second import of the
// same row is refused by the unique key. Such a token opens nothing: the
// confirm endpoint answers 'email' receipts only.
async function importJfproEmails(conn) {
    const [[{ last }]] = await conn.query(
        `SELECT COALESCE(MAX(send_id), 0) AS last FROM email_receipts WHERE send_table = ?`,
        [JFPRO_AUDIT]
    );
    await conn.query(
        `INSERT IGNORE INTO email_receipts
                (token, email_type, send_table, send_id, sent_to, subject, confirm_via, created_at)
         SELECT SHA2(CONCAT('jfpro-audit:', a.id), 256),
                IF(a.entity_type = 'portal_user', 'portal_approval', 'quote_request'),
                ?, a.id,
                JSON_UNQUOTE(JSON_EXTRACT(a.after_json, '$.to')),
                LEFT(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(a.after_json, '$.subject')),
                              IF(a.entity_type = 'portal_user', ?, NULL)), 500),
                IF(a.entity_type = 'portal_user', 'none', 'portal'),
                a.created_at
           FROM ${JFPRO_AUDIT} a
          WHERE a.id > ?
            AND ((a.entity_type = 'portal_user' AND a.action LIKE 'approval_email_s%')
              OR (a.entity_type = 'quote_request' AND a.action = 'email_sent'))
          ORDER BY a.id`,
        [JFPRO_AUDIT, PORTAL_APPROVAL_SUBJECT, Number(last) || 0]
    );
}

async function syncPortalReceipts(conn, { maxAgeMs = PORTAL_SYNC_MS } = {}) {
    if (maxAgeMs > 0 && Date.now() - lastPortalSync < maxAgeMs) return { skipped: true, failed: [] };
    lastPortalSync = Date.now();
    const failed = [];
    const run = async (name, fn) => {
        try {
            await fn();
        } catch (e) {
            failed.push(name);
            if (!isSourceMissing(e)) log.warn('[email-receipt] portal sync step failed', { step: name, error: e.message, errno: e.errno });
        }
    };
    await run('jfpro_emails', () => importJfproEmails(conn));
    let steps = [];
    try {
        steps = portalSyncSteps(jportalSchema());
    } catch (e) {
        // JPORTAL_DB_SCHEMA is not a plain identifier: nothing can be read.
        log.warn('[email-receipt] portal sync skipped', { error: e.message });
        return { failed: ['po_viewed', 'signed_pi_viewed', 'artwork_ticked', 'file_opened', 'qc_availability_answered', 'qc_schedule_opened', 'quote_answered', ...failed] };
    }
    for (const [name, sql] of steps) await run(name, () => conn.query(sql));
    return { failed };
}

const TOKEN_RE = /^[a-f0-9]{16,128}$/;

// Only an 'email' receipt has a confirm link: a token of any other row (never
// emailed, or derived from a JFPRO audit id) is answered as unknown.
//
// Record a landing-page view (the GET). WEAK signal: automated email security
// scanners (Microsoft Safe Links, Mimecast, Proofpoint…) fetch links too, so an
// "open" may be a bot rather than the recipient. Stamps opened_at once and bumps
// open_count. Returns the receipt row (with `alreadyConfirmed`) so the landing
// page can show context, or null if the token is unknown/malformed.
async function markReceiptOpened(conn, token) {
    if (typeof token !== 'string' || !TOKEN_RE.test(token)) return null;
    const [rows] = await conn.query(
        `SELECT id, token, email_type, subject, sent_to, opened_at, open_count, confirmed_at
           FROM email_receipts WHERE token = ? AND confirm_via = 'email'`,
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
           FROM email_receipts WHERE token = ? AND confirm_via = 'email'`,
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
    CONFIRM_VIA,
    confirmViaOf,
    canRemind,
    apiBaseUrlFromReq,
    createEmailReceipt,
    logSentEmail,
    discardEmailReceipt,
    linkReceiptToSend,
    appendReceiptLink,
    appendPortalLink,
    CHASED_PORTAL_STEPS,
    portalSyncSteps,
    syncPortalReceipts,
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
