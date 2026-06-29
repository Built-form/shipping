require('dotenv').config();
const log = require('../lib/logger');
const { getPool, closePool } = require('../db');
const { generateDailyAlerts } = require('../services/daily-alerts');

// ── Nightly daily-alerts generator ──────────────────────────────────────────
// Scheduled (serverless.yml: generateDailyAlerts, ~04:00 UTC) so it runs
// overnight UK time. Aggregates the day's container ETAs + supplier ready dates
// into the `daily_alerts` table for the Shipsline slide-out alert window.
// Idempotent: safe to re-run (manual invoke, retry, or a second nightly tick)
// — INSERT IGNORE on a stable dedup_key means no duplicates and no resurrected
// acknowledgements. Optional event payload: { lookbackDays }.
exports.handler = async (event) => {
    try {
        log.info('[generate-daily-alerts] starting');
        const pool = getPool();
        const lookbackDays = Number(event?.lookbackDays) > 0 ? Number(event.lookbackDays) : undefined;
        const summary = await generateDailyAlerts(pool, lookbackDays ? { lookbackDays } : {});
        log.info('[generate-daily-alerts] done', summary);
        return { statusCode: 200, body: JSON.stringify({ message: 'Daily alerts generated', summary }) };
    } catch (err) {
        log.error('[generate-daily-alerts] failed', err);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

// Local runner: `node src/handlers/generate-daily-alerts.js`
if (require.main === module) {
    exports.handler({})
        .then((result) => console.log(result.body))
        .catch((err) => { console.error('Fatal:', err); process.exit(1); })
        .finally(() => closePool());
}
