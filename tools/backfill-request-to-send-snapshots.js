'use strict';

// Backfill draft_container_documents.lines_json for Requests to send (type
// 'supplier-quote') generated before the column existed (migration
// 2026-10-26_20). Only each supplier's LATEST Request to send per container is
// filled (the only baseline the app compares against), only where lines_json
// IS NULL, so a real 'generate' snapshot is never touched and re-running is
// safe. The lines come from the CSV stored beside each PDF: built from the same
// lines in the same request, so it is exactly what was printed. Written with
// source 'csv-backfill'. Rules: src/lib/request-to-send-drift.js.
//
// DRY-RUN BY DEFAULT: prints the report and writes nothing.
//
//   node tools/backfill-request-to-send-snapshots.js --stage test                 # dry-run report
//   node tools/backfill-request-to-send-snapshots.js --stage test --json r.json   # + machine-readable copy
//   node tools/backfill-request-to-send-snapshots.js --stage test --apply         # write
//   node tools/backfill-request-to-send-snapshots.js --stage dev ...              # PRODUCTION (stage "dev")
//
// --stage picks the database exactly as tools/migrate.js does: secret
// shipping/<prod|test> read with the AWS CLI, direct DB_HOST, and a refusal if
// test does not look like TEST or dev (production) does. Without --stage it
// uses .env (normally TEST), and writing then needs --confirm-host <DB_HOST>.
//
// Skipped (left NULL, so that supplier shows no flag until regenerated):
//   - no CSV (Requests to send from before CSV export, 2026-06-25);
//   - the CSV could not be fetched or is not a Request to send CSV.
// The report shows what each filled document would flag right now ("stale")
// for containers still live (DRAFT to ARRIVED).

const fs = require('fs');
const { execFileSync } = require('child_process');
const { getPool, closePool } = require('../src/db');
const R = require('../src/lib/request-to-send-drift');
const { LIVE_STAGES, latestPerSupplier, requestDriftForShipment } = require('../src/services/request-to-send-status');

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);

const apply = flag('--apply');
const stage = arg('--stage');
const confirmHost = arg('--confirm-host');
const jsonOut = arg('--json');

// serverless.yml custom.envNames: the production stack is stage "dev".
const ENV_NAMES = { dev: 'prod', test: 'test' };
const refuse = msg => { console.error(msg); process.exit(2); };

// src/db reads DB_* from process.env on first use, so set them before getPool().
if (stage) {
    const secretId = `shipping/${ENV_NAMES[stage] || stage}`;
    let secret;
    try {
        secret = JSON.parse(execFileSync('aws', [
            'secretsmanager', 'get-secret-value',
            '--region', process.env.AWS_REGION || 'eu-north-1',
            '--secret-id', secretId,
            '--query', 'SecretString', '--output', 'text',
        ], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }));
    } catch (e) {
        refuse(`could not read secret ${secretId} with the AWS CLI: ${String(e.stderr || e.message).trim()}`);
    }
    Object.assign(process.env, {
        DB_HOST: secret.DB_HOST, DB_USER: secret.DB_USER, DB_PASSWORD: secret.DB_PASSWORD,
        DB_NAME: secret.DB_NAME, DB_PORT: String(secret.DB_PORT || 3306),
    });
} else {
    require('dotenv').config();
}
const host = process.env.DB_HOST || '(DB_HOST unset)';
const looksTest = /test/i.test(host) || /test/i.test(process.env.DB_NAME || '');
if (stage === 'test' && !looksTest) refuse(`stage test resolved to ${host}, which does not look like the TEST instance`);
if (stage === 'dev' && looksTest) refuse(`stage dev (production) resolved to ${host}, which looks like TEST`);

const lc = s => String(s || '').trim().toLowerCase();
const iso = d => (d instanceof Date ? d.toISOString() : d);
const fmtChange = c => {
    const lot = c.lot ? ` lot ${c.lot}` : '';
    if (c.kind === 'added') return `+${c.item}${lot} ${c.unitsNow}`;
    if (c.kind === 'removed') return `-${c.item}${lot} ${c.unitsWas}`;
    if (c.kind === 'lot') return `${c.item} lot ${c.lotWas || '-'}->${c.lot || '-'}${c.unitsWas != null ? ` ${c.unitsWas}->${c.unitsNow}` : ''}`;
    return `${c.item}${lot} ${c.unitsWas}->${c.unitsNow}`;
};

(async () => {
    if (apply && !stage && confirmHost !== process.env.DB_HOST) {
        refuse(`--apply on the .env database needs --confirm-host <DB_HOST> (or use --stage): this run is connected to ${host}.`);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        // Without the column a dry-run still reports (every document counts as
        // unfilled); writing needs it.
        const [col] = await conn.query(`SHOW COLUMNS FROM draft_container_documents LIKE 'lines_json'`);
        const hasColumn = col.length > 0;
        if (apply && !hasColumn) refuse('draft_container_documents.lines_json is missing: run the migration (deploy) first.');

        const [docs] = await conn.query(
            `SELECT * FROM draft_container_documents
              WHERE type = 'supplier-quote' AND deleted_at IS NULL
              ORDER BY generated_at DESC, id DESC`);
        const [ships] = await conn.query(
            `SELECT * FROM shipments WHERE deleted_at IS NULL AND merged_into_id IS NULL AND stage IN (?)`, [LIVE_STAGES]);
        const shipById = new Map(ships.map(s => [Number(s.id), s]));
        const draftByName = new Map(ships.filter(s => s.stage === 'DRAFT' && s.name).map(s => [lc(s.name), s]));

        // Owner = the shipment a document belongs to, else its draft name.
        const byOwner = new Map();
        for (const d of docs) {
            const owner = d.shipment_id != null ? `s:${d.shipment_id}` : `n:${lc(d.draft_container_name)}`;
            if (!byOwner.has(owner)) byOwner.set(owner, []);
            byOwner.get(owner).push(d);
        }
        const candidates = [];
        for (const [owner, rows] of byOwner) {
            for (const d of latestPerSupplier(rows)) {
                if (hasColumn && d.lines_json != null) continue;
                const ship = owner.startsWith('s:') ? shipById.get(Number(owner.slice(2))) : draftByName.get(owner.slice(2));
                candidates.push({ doc: d, ship: ship || null });
            }
        }

        console.log(`DB_HOST ${host}  ${apply ? 'APPLY' : 'dry-run'}${hasColumn ? '' : '  (lines_json column not migrated yet)'}`);
        console.log(`${candidates.length} latest Request(s) to send without a line snapshot\n`);

        const report = [];
        for (const { doc, ship } of candidates) {
            const row = {
                documentId: doc.id, container: doc.draft_container_name, supplier: doc.supplier, version: doc.version,
                generatedAt: iso(doc.generated_at), live: !!ship, url: doc.public_url,
            };
            if (!doc.csv_public_url) { report.push({ ...row, action: 'skip', reason: 'no CSV' }); continue; }
            let csv;
            try {
                const res = await fetch(doc.csv_public_url);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                csv = await res.text();
            } catch (e) {
                report.push({ ...row, action: 'skip', reason: `CSV fetch failed (${e.message})` });
                continue;
            }
            const lines = R.csvRequestLines(csv);
            if (!lines || !lines.length) { report.push({ ...row, action: 'skip', reason: 'not a Request to send CSV' }); continue; }
            const snapshot = { v: R.SNAPSHOT_VERSION, source: 'csv-backfill', lines };
            let drift = null;
            if (ship) {
                const fake = { ...doc, lines_json: snapshot };
                drift = (await requestDriftForShipment(conn, ship, [fake])).get(doc.id) || null;
            }
            report.push({ ...row, action: 'fill', snapshot, stale: drift ? drift.stale : null, changes: drift ? drift.changes : null });
        }

        const fill = report.filter(r => r.action === 'fill');
        for (const r of report) {
            const head = `${r.container} / ${r.supplier} v${r.version} ${r.generatedAt}${r.live ? '' : ' (not live)'}`;
            if (r.action === 'skip') { console.log(`  SKIP  ${head}: ${r.reason}`); continue; }
            console.log(`  FILL  ${head}${r.stale ? `  STALE: ${r.changes.map(fmtChange).join('; ')}` : ''}`);
        }
        console.log(`\n${fill.length} to fill (${fill.filter(r => r.stale).length} would flag now), ${report.length - fill.length} skipped`);
        if (jsonOut) {
            fs.writeFileSync(jsonOut, JSON.stringify({ host, report }, null, 2));
            console.log(`report written to ${jsonOut}`);
        }

        if (!apply) {
            console.log(`Dry run. Re-run with ${stage ? `--stage ${stage} --apply` : '--apply --confirm-host <DB_HOST>'} to write.`);
            return;
        }
        let written = 0;
        for (const r of fill) {
            const [res] = await conn.query(
                'UPDATE draft_container_documents SET lines_json = ? WHERE id = ? AND lines_json IS NULL',
                [JSON.stringify(r.snapshot), r.documentId]
            );
            written += res.affectedRows;
        }
        console.log(`wrote ${written} snapshot(s)`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('ERR', e.code || '', e.message); process.exit(1); });
