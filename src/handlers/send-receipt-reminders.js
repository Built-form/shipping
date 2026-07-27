require('dotenv').config();
const log = require('../lib/logger');
const { getPool, closePool } = require('../db');
const { runReceiptReminders } = require('../services/receipt-reminders');

// ── Email-receipt auto-reminder sweep ───────────────────────────────────────
// Scheduled (serverless.yml: sendReceiptReminders). Chases suppliers who
// haven't confirmed receipt of an email: sends a bilingual reminder (reusing
// the original confirm token, showing the reminder count) for every unconfirmed
// receipt whose last touch is ≥48h old, capped at RECEIPT_REMINDER_MAX.
//
// The 48h-per-email cadence lives in SQL, so the Lambda can tick more often
// (every few hours) without over-sending — a re-run just re-selects whatever is
// now due. Optional event payload: { intervalHours, maxReminders, baseUrl, limit }.
exports.handler = async (event) => {
    try {
        log.info('[send-receipt-reminders] starting');
        const pool = getPool();
        const opts = {};
        if (Number(event?.intervalHours) > 0) opts.intervalHours = Number(event.intervalHours);
        if (event?.maxReminders != null) opts.maxReminders = Number(event.maxReminders);
        if (event?.baseUrl) opts.baseUrl = event.baseUrl;
        if (Number(event?.limit) > 0) opts.limit = Number(event.limit);
        const summary = await runReceiptReminders(pool, opts);
        log.info('[send-receipt-reminders] done', summary);
        return { statusCode: 200, body: JSON.stringify({ message: 'Receipt reminders processed', summary }) };
    } catch (err) {
        log.error('[send-receipt-reminders] failed', err);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

// Local runner: `node src/handlers/send-receipt-reminders.js`
if (require.main === module) {
    exports.handler({})
        .then((result) => console.log(result.body))
        .catch((err) => { console.error('Fatal:', err); process.exit(1); })
        .finally(() => closePool());
}
