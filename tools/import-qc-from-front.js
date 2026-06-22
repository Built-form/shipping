'use strict';

// Pull supplier QC inspection reports out of Front and into the QC pipeline.
//
// Searches the purchasing inbox for inspection-agent emails (from @aqiservice.com
// by default), fetches each report PDF — a direct Front attachment, or a
// jianguoyun share folder linked in the body — de-dupes against anything already
// imported, stores it in the same S3 bucket the manual QC upload uses, then runs
// the existing Gemini extraction + order matching on each new report.
//
// Usage (via the PowerShell tool, per the node-on-Windows note):
//   node tools/import-qc-from-front.js [--days 14] [--dry-run] [--no-analyze] [--model <id>]
//
//   --days N      look back N days (default 14)
//   --dry-run     discover + de-dupe only; fetch/store/analyze nothing
//   --no-analyze  import (fetch + store + row) but skip Gemini analysis
//   --model <id>  override the primary Gemini model

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { importQcReportsFromFront, analyzeAndPersist, resetFrontImports } = require('../src/services/front-qc-import');

function arg(name, fallback) {
    const i = process.argv.indexOf(name);
    if (i === -1) return fallback;
    const v = process.argv[i + 1];
    return v && !v.startsWith('--') ? v : true;
}

const DAYS = Number(arg('--days', 14)) || 14;
const DRY_RUN = process.argv.includes('--dry-run');
const NO_ANALYZE = process.argv.includes('--no-analyze');
const RESET = process.argv.includes('--reset');
const MODEL = typeof arg('--model', null) === 'string' ? arg('--model', null) : undefined;

function short(s, n = 48) { return String(s || '').replace(/\s+/g, ' ').slice(0, n); }

(async () => {
    for (const k of ['FRONT_API_TOKEN', 'PO_DOCS_BUCKET']) {
        if (!process.env[k]) { console.error(`Missing ${k} in .env`); process.exit(1); }
    }
    if (!DRY_RUN && !NO_ANALYZE && !process.env.GEMINI_API_KEY) {
        console.error('Missing GEMINI_API_KEY (needed to analyze). Use --no-analyze to import only.');
        process.exit(1);
    }

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        if (RESET && !DRY_RUN) {
            const r = await resetFrontImports(conn);
            console.log(`Reset: hard-deleted ${r.deleted} prior front-import report(s)${r.ids.length ? ' [' + r.ids.join(', ') + ']' : ''}`);
        }

        console.log(`\nSearching Front: last ${DAYS} days${DRY_RUN ? '  [DRY RUN]' : ''}`);
        const res = await importQcReportsFromFront(conn, { sinceDays: DAYS, dryRun: DRY_RUN });
        console.log(`Query: ${res.query}\n`);

        console.log(`Report sources found: ${res.found.length}`);
        for (const f of res.found) {
            console.log(`  • ${f.date.slice(0, 10)}  ${f.kind.padEnd(16)} ${short(f.subject, 42).padEnd(44)} ${f.link ? short(f.link, 46) : ''}`);
        }

        if (res.skipped.length) {
            console.log(`\nSkipped: ${res.skipped.length}`);
            for (const s of res.skipped) {
                const why = s.reason === 'already-imported' ? `already imported (qc_report ${s.existingId}, ${s.status})` : s.reason;
                console.log(`  • ${short(s.subject, 42).padEnd(44)} ${why}`);
            }
        }

        if (res.failed.length) {
            console.log(`\nFailed to fetch/store: ${res.failed.length}`);
            for (const f of res.failed) console.log(`  ✗ ${f.kind} ${short(f.subject, 36)} — ${f.error}`);
        }

        console.log(`\nImported: ${res.imported.length}`);
        for (const i of res.imported) {
            if (i.dryRun) console.log(`  ~ would import  ${i.kind.padEnd(16)} ${short(i.subject, 42)}`);
            else console.log(`  + qc_report ${String(i.id).padEnd(5)} ${i.kind.padEnd(16)} ${i.filename}  (${(i.fileSize / 1e6).toFixed(1)} MB)`);
        }

        if (!DRY_RUN && !NO_ANALYZE) {
            // Analyze every imported-but-unanalyzed report (status 'uploaded'),
            // not just this run's new ones — so a prior interrupted run self-heals.
            const [pending] = await conn.query(
                `SELECT * FROM qc_reports WHERE status = 'uploaded' AND deleted_at IS NULL ORDER BY id`
            );
            if (pending.length) {
                console.log(`\nAnalyzing ${pending.length} report(s) with Gemini…`);
                for (const row of pending) {
                    process.stdout.write(`  qc_report ${row.id} (${row.filename}) … `);
                    try {
                        const out = await analyzeAndPersist(conn, row, { model: MODEL });
                        const verdicts = out.matched.map(m => `${m.jfCode}/${m.lotNumber}:${m.qcResult}`).join(', ');
                        console.log(`${out.items.length} item(s), ${out.matchedItemCount} matched${verdicts ? ` [${verdicts}]` : ''}${out.unmatched.length ? `, ${out.unmatched.length} unmatched` : ''} (${out.modelUsed})`);
                    } catch (e) {
                        console.log(`FAILED — ${e.message}`);
                    }
                }
            }
        }

        console.log('\nDone.');
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('\nFatal:', e.message); process.exit(1); });
