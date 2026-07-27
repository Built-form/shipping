// ── Email-receipt auto-reminders ────────────────────────────────────────────
// Every outbound supplier/forwarder email carries a "Confirm receipt" link
// (src/lib/email-receipt.js). When a supplier hasn't confirmed, we chase them
// automatically: this service sends a short bilingual reminder that reuses the
// ORIGINAL confirm token (so a click still lands on the same email_receipts
// row) and shows which reminder number it is.
//
// Two entry points share ONE send implementation (sendReminderForReceipt):
//   • runReceiptReminders(pool) — the scheduled sweep (src/handlers/send-receipt-reminders.js).
//     Picks every unconfirmed receipt whose last touch (last reminder, else the
//     original send) is ≥ REMINDER_INTERVAL_HOURS old, up to REMINDER_MAX times.
//   • the manual "resend" button — POST /api/v1/email-receipts/:id/resend (orders.js).
const log = require('../lib/logger');
const {
    ensureEmailReceiptsSchema, appendReceiptLink,
    renderReminderEmailHtml, recordReminderSend, parseRecipients,
} = require('../lib/email-receipt');

// How long to wait between reminders for a given email, and the cap on how many
// reminders we'll ever send before giving up. Env-overridable.
const REMINDER_INTERVAL_HOURS = Math.max(1, Number(process.env.RECEIPT_REMINDER_INTERVAL_HOURS) || 48);
const REMINDER_MAX = Math.max(0, Number(process.env.RECEIPT_REMINDER_MAX ?? 5));

// Send a single reminder for a receipt row via Front. Pure send — the caller
// owns the DB bump so the sweep and the endpoint can each control their own
// transaction/response. Reuses receipt.token (the original capability) and
// shows reminder #(reminder_count + 1). Returns a discriminated result:
//   { ok: true, recipients, subject, reminderNumber, frontMessageUid, frontConversationId }
//   { ok: false, reason: 'no_recipients' | 'front_not_configured' | 'front_error', status?, detail? }
async function sendReminderForReceipt(receipt, baseUrl) {
    const recipients = parseRecipients(receipt.sent_to);
    if (!recipients.length) return { ok: false, reason: 'no_recipients' };
    if (!process.env.FRONT_CHANNEL_ID || !process.env.FRONT_API_TOKEN) {
        return { ok: false, reason: 'front_not_configured' };
    }

    const reminderNumber = Number(receipt.reminder_count || 0) + 1;
    const label = `Reminder #${reminderNumber} / 第 ${reminderNumber} 次提醒`;
    const subject = receipt.subject
        ? `${label}: ${receipt.subject}`
        : `${label}: please confirm receipt`;
    // Reuse the ORIGINAL token so the reminder's Confirm button records against
    // the same email_receipts row.
    const body = appendReceiptLink(
        renderReminderEmailHtml(receipt.subject, reminderNumber),
        receipt.token,
        baseUrl
    );

    const form = new FormData();
    for (const addr of recipients) form.append('to[]', addr);
    form.append('subject', subject);
    form.append('body', body);
    form.append('body_format', 'html');
    form.append('options[archive]', 'false');

    const frontUrl = `https://api2.frontapp.com/channels/${encodeURIComponent(process.env.FRONT_CHANNEL_ID)}/messages`;
    const resp = await fetch(frontUrl, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`,
            Accept: 'application/json',
        },
        body: form,
    });
    const respText = await resp.text();
    if (!resp.ok) {
        return { ok: false, reason: 'front_error', status: resp.status, detail: respText.slice(0, 500) };
    }
    // Capture Front provenance (same shape the document-send handlers store) so
    // the reminder ledger can link back to the conversation.
    let parsed = null;
    try { parsed = JSON.parse(respText); } catch { /* 202 with empty body is fine */ }
    const conversationUrl = parsed?._links?.related?.conversation || '';
    const frontConversationId = conversationUrl ? conversationUrl.split('/').pop() : null;
    const frontMessageUid = parsed?.message_uid || parsed?.id || null;

    return { ok: true, recipients, subject, reminderNumber, frontMessageUid, frontConversationId };
}

// Scheduled sweep. Finds unconfirmed receipts due for a reminder and sends one
// each, bumping reminder_count / last_reminder_at on success. Idempotent by
// design: the 48h gate on COALESCE(last_reminder_at, created_at) means a re-run
// (retry, extra tick) won't double-send. Requires PUBLIC_API_BASE_URL so the
// confirm link is absolute (no inbound request to derive the host from).
async function runReceiptReminders(pool, opts = {}) {
    const intervalHours = Math.max(1, Number(opts.intervalHours) || REMINDER_INTERVAL_HOURS);
    const maxReminders = opts.maxReminders != null ? Math.max(0, Number(opts.maxReminders)) : REMINDER_MAX;
    const limit = Math.min(Math.max(Number(opts.limit) || 200, 1), 1000);
    const baseUrl = (opts.baseUrl || process.env.PUBLIC_API_BASE_URL || '').replace(/\/+$/, '');
    const summary = { due: 0, sent: 0, failed: 0, skipped: 0 };

    if (!baseUrl) {
        log.warn('[receipt-reminders] PUBLIC_API_BASE_URL unset — confirm links would be broken; skipping run');
        return { ...summary, aborted: 'no_base_url' };
    }
    if (maxReminders <= 0) {
        return { ...summary, aborted: 'reminders_disabled' };
    }

    const conn = await pool.getConnection();
    try {
        await ensureEmailReceiptsSchema(conn);
        // intervalHours/limit are sanitized ints → safe to inline (INTERVAL and
        // LIMIT can't be bound params in MySQL). maxReminders is bound.
        const [rows] = await conn.query(
            `SELECT id, token, email_type, sent_to, subject, reminder_count
               FROM email_receipts
              WHERE confirmed_at IS NULL
                AND sent_to IS NOT NULL
                AND reminder_count < ?
                AND COALESCE(last_reminder_at, created_at) <= (NOW() - INTERVAL ${intervalHours} HOUR)
              ORDER BY id ASC
              LIMIT ${limit}`,
            [maxReminders]
        );
        summary.due = rows.length;

        for (const receipt of rows) {
            const result = await sendReminderForReceipt(receipt, baseUrl);
            if (result.ok) {
                await recordReminderSend(conn, receipt.id, {
                    reminderNumber: result.reminderNumber,
                    sentTo: result.recipients,
                    subject: result.subject,
                    frontMessageUid: result.frontMessageUid,
                    frontConversationId: result.frontConversationId,
                    sentByEmail: null, // automated sweep
                });
                summary.sent++;
            } else if (result.reason === 'no_recipients') {
                summary.skipped++;
            } else {
                summary.failed++;
                log.error('[receipt-reminders] send failed', {
                    id: receipt.id, reason: result.reason, status: result.status,
                });
                // Config problem affects every row — stop rather than hammer Front.
                if (result.reason === 'front_not_configured') break;
            }
        }
        log.info('[receipt-reminders] sweep complete', summary);
        return summary;
    } finally {
        conn.release();
    }
}

module.exports = {
    REMINDER_INTERVAL_HOURS,
    REMINDER_MAX,
    sendReminderForReceipt,
    runReceiptReminders,
};
