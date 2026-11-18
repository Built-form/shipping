require('dotenv').config();
const log = require('../lib/logger');
const { getPool, closePool } = require('../db');
const { runReadyDateReminders } = require('../services/ready-date-reminders');

// ── Ready-date reminder sweep ───────────────────────────────────────────────
// Scheduled on working days (serverless.yml: sendReadyDateReminders). Emails a
// supplier a link to the supplier portal every working day while they owe us a
// ready date: the estimated date, from a week after staff confirm a PO line's
// final artwork and once the PO's deposit is paid or not required; the actual
// date, once the estimated one has passed. One email per supplier, covering
// all of their purchase orders
// (docs/ready-date-reminders.md, src/services/ready-date-reminders.js).
//
// A supplier is emailed at most once in 20 hours, so an extra run sends
// nothing twice. Optional event payload:
//   { dryRun: true }  report who would be emailed about what; send and write nothing
//   { force: true }   run at a weekend too
//   { today: 'YYYY-MM-DD' }  the day to run as (default: today, Europe/London)
exports.handler = async (event) => {
    try {
        log.info('[send-ready-date-reminders] starting');
        const opts = {};
        if (event?.dryRun === true) opts.dryRun = true;
        if (event?.force === true) opts.force = true;
        if (typeof event?.today === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(event.today)) opts.today = event.today;
        const summary = await runReadyDateReminders(getPool(), opts);
        log.info('[send-ready-date-reminders] done', { ...summary, emails: undefined });
        return { statusCode: 200, body: JSON.stringify({ message: 'Ready date reminders processed', summary }) };
    } catch (err) {
        log.error('[send-ready-date-reminders] failed', err);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    }
};

// Local runner: `node src/handlers/send-ready-date-reminders.js --dry-run`
// (--force runs at a weekend too). Without --dry-run it SENDS, so it must be
// told which database and which portal it is about to use, as tools/migrate.js
// is for a write:
//   --confirm-host <DB_HOST> --confirm-portal <SUPPLIER_PORTAL_URL>
// .env can pair the live database with the test portal's address. On
// 2026-11-18 a local run did exactly that, and twelve suppliers were emailed
// links to the test portal.
if (require.main === module) {
    const args = process.argv.slice(2);
    const valueOf = flag => (args.includes(flag) ? String(args[args.indexOf(flag) + 1] || '') : '');
    const trim = v => String(v || '').trim().replace(/\/+$/, '');
    const dryRun = args.includes('--dry-run');
    const host = trim(process.env.DB_HOST);
    const portal = trim(process.env.SUPPLIER_PORTAL_URL);
    if (!dryRun && (trim(valueOf('--confirm-host')) !== host || trim(valueOf('--confirm-portal')) !== portal || !host || !portal)) {
        console.error('Refused: this would SEND emails to suppliers. It is set to use');
        console.error(`  database  ${host || '(DB_HOST unset)'}`);
        console.error(`  portal    ${portal || '(SUPPLIER_PORTAL_URL unset)'}   <- every link in the emails`);
        console.error('If both are right, run it again with');
        console.error(`  --confirm-host ${host} --confirm-portal ${portal}`);
        console.error('or add --dry-run to see who would be emailed.');
        process.exit(2);
    }
    exports.handler({ dryRun, force: args.includes('--force') })
        .then((result) => console.log(JSON.stringify(JSON.parse(result.body), null, 2)))
        .catch((err) => { console.error('Fatal:', err); process.exit(1); })
        .finally(() => closePool());
}
