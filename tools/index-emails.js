'use strict';

// Manual runner / backfill for the Front email→order indexer
// (src/services/front-email-index.js). The hourly Lambda runs incrementally;
// this is for the first-time backfill and ad-hoc re-scans.
//
//   node tools/index-emails.js                      # incremental (since watermark)
//   node tools/index-emails.js --since 180          # full backfill (six-month ceiling)
//   node tools/index-emails.js --since 180 --dry    # match + report, write nothing
//   node tools/index-emails.js --since 30 --force   # re-scan even already-seen threads
//   node tools/index-emails.js --no-gemini          # deterministic only (no Flash)
//   node tools/index-emails.js --since 90 --max-gemini 100   # raise the Flash cap
//   node tools/index-emails.js --verbose            # per-conversation log lines
//
// NOTE: a hard ceiling (EMAIL_INDEX_MAX_IMPORT_DAYS, default 180) clamps how far
// back any run reaches — `--since 365` is silently capped to six months.
// Requires FRONT_API_TOKEN (and GEMINI_API_KEY for the fallback) in .env.

require('dotenv').config();
const { getPool } = require('../src/db');
const { indexEmailsFromFront } = require('../src/services/front-email-index');

function flagValue(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1] : null;
}

(async () => {
    const since = Number(flagValue('--since'));
    const maxGemini = Number(flagValue('--max-gemini'));
    const opts = {
        sinceDays: Number.isFinite(since) && since > 0 ? since : null,
        dryRun: process.argv.includes('--dry') || process.argv.includes('--dry-run'),
        force: process.argv.includes('--force'),
        verbose: process.argv.includes('--verbose'),
        useGemini: !process.argv.includes('--no-gemini'),
        ...(Number.isFinite(maxGemini) && maxGemini >= 0 ? { maxGemini } : {}),
    };

    console.log('[index-emails] options:', opts);
    const conn = await getPool().getConnection();
    try {
        const res = await indexEmailsFromFront(conn, opts);
        console.log('\n[index-emails] result:');
        console.log(JSON.stringify(res, null, 2));
        console.log(
            `\nscanned=${res.scanned}  matched=${res.matched}  links=${res.linksUpserted}` +
            `  (strong=${res.byTier.strong} batch=${res.byTier.batch} product=${res.byTier.product})` +
            `  gemini[calls=${res.geminiCalls} matched=${res.geminiMatched} budgetSkipped=${res.geminiBudgetSkipped}]` +
            `  ambiguous=${res.ambiguous}  failed=${res.failed}` +
            `  ${res.watermarkAdvanced ? 'watermark advanced' : 'watermark held'}`
        );
    } finally {
        conn.release();
        process.exit(0);
    }
})().catch(e => { console.error('FAIL:', e.message, e.stack); process.exit(1); });
