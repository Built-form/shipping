'use strict';

// Backfill purchase_order_documents.lines_json for PO PDFs generated before
// the column existed (migration 2026-10-26_10). Only the NEWEST document of
// each live PO is filled (the only baseline ShipLine compares against), only
// where lines_json IS NULL, so a real 'generate' snapshot is never touched and
// re-running is safe. Lines are rebuilt from audit_log as they stood at the
// document's generated_at: reconstructPoLinesAt in src/lib/po-line-snapshot.js.
// Written with source 'audit-rebuild'; ShipLine says so on the chip.
//
// DRY-RUN BY DEFAULT: prints the report and writes nothing.
//
//   node tools/backfill-po-line-snapshots.js --stage test                 # dry-run report
//   node tools/backfill-po-line-snapshots.js --stage test --po 307        # one PO, with its lines
//   node tools/backfill-po-line-snapshots.js --stage test --json r.json   # + machine-readable copy
//   node tools/backfill-po-line-snapshots.js --stage test --apply         # write
//   node tools/backfill-po-line-snapshots.js --stage dev ...              # PRODUCTION (stage "dev")
//
// --stage picks the database exactly as tools/migrate.js does: secret
// shipping/<prod|test> read with the AWS CLI, direct DB_HOST, and a refusal if
// test does not look like TEST or dev (production) does. Without --stage it
// uses .env (normally TEST), and writing then needs --confirm-host <DB_HOST>.
//
// Skipped (left NULL, so the PO shows no flags):
//   - generated before the history floor: the first order audit row, or the
//     oldest current orders row (orders was reloaded once, ids restarted, so
//     audit rows from before that point name different orders);
//   - the rebuild has no lines.
// Before --apply, open a few `url`s from the report (some with drift > 0) and
// check the PDF's lines against `rebuilt`.

const fs = require('fs');
const { execFileSync } = require('child_process');
const { getPool, closePool } = require('../src/db');
const { SNAPSHOT_VERSION, reconstructPoLinesAt, countDrift } = require('../src/lib/po-line-snapshot');

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);

const apply = flag('--apply');
const stage = arg('--stage');
const confirmHost = arg('--confirm-host');
const onlyPo = arg('--po') ? Number(arg('--po')) : null;
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

const parse = v => (typeof v === 'string' ? (() => { try { return JSON.parse(v); } catch { return null; } })() : v);
const iso = d => (d instanceof Date ? d.toISOString() : d);
const fmtLines = lines => lines.map(l => `${l.jfCode || l.asin || '?'} x${l.quantity} @${l.unitPrice ?? '-'}`).join(', ');

async function loadPo(conn, doc) {
    const P = Number(doc.purchase_order_id);
    const [poEventRows] = await conn.query(
        `SELECT id, entity_id, action, created_at, before_json, after_json
           FROM audit_log
          WHERE entity_type = 'purchase_order' AND entity_id = ?
            AND action IN ('order_attached', 'order_detached')`,
        [P]
    );
    const poEvents = poEventRows.map(r => {
        const payload = parse(r.action === 'order_attached' ? r.after_json : r.before_json) || {};
        return { id: r.id, poId: P, action: r.action, orderId: payload.orderId, createdAt: r.created_at };
    }).filter(e => e.orderId != null);

    // Orders that ever carried this PO id in an order audit diff.
    const [movedRows] = await conn.query(
        `SELECT DISTINCT entity_id FROM audit_log
          WHERE entity_type = 'order'
            AND (CAST(JSON_UNQUOTE(JSON_EXTRACT(before_json, '$.purchaseOrderId')) AS UNSIGNED) = ?
              OR CAST(JSON_UNQUOTE(JSON_EXTRACT(after_json, '$.purchaseOrderId')) AS UNSIGNED) = ?)`,
        [P, P]
    );
    const [linkedRows] = await conn.query('SELECT id FROM orders WHERE purchase_order_id = ?', [P]);
    const ids = [...new Set([
        ...linkedRows.map(r => Number(r.id)),
        ...movedRows.map(r => Number(r.entity_id)),
        ...poEvents.map(e => Number(e.orderId)),
    ])];
    if (!ids.length) return { orders: [], orderEvents: [], poEvents };

    const ph = ids.map(() => '?').join(',');
    const [orderRows] = await conn.query(
        `SELECT id, jf_code, asin, quantity, unit_price, purchase_order_id, created_at, deleted_at
           FROM orders WHERE id IN (${ph})`,
        ids
    );
    const orders = orderRows.map(r => ({
        id: r.id,
        jfCode: r.jf_code || null,
        asin: r.asin || null,
        quantity: Number(r.quantity || 0),
        unitPrice: r.unit_price != null ? Number(r.unit_price) : null,
        purchaseOrderId: r.purchase_order_id,
        createdAt: r.created_at,
        deletedAt: r.deleted_at,
    }));
    const [eventRows] = await conn.query(
        `SELECT id, entity_id, action, created_at, before_json
           FROM audit_log
          WHERE entity_type = 'order' AND entity_id IN (${ph}) AND created_at > ?`,
        [...ids, doc.generated_at]
    );
    const orderEvents = eventRows.map(r => ({
        id: r.id, orderId: r.entity_id, action: r.action, createdAt: r.created_at, before: parse(r.before_json),
    }));
    return { orders, orderEvents, poEvents };
}

(async () => {
    if (apply && !stage && confirmHost !== process.env.DB_HOST) {
        refuse(`--apply on the .env database needs --confirm-host <DB_HOST> (or use --stage): this run is connected to ${host}.`);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        // Without the column a dry-run still reports (every document counts
        // as unfilled); writing needs it.
        const [col] = await conn.query(`SHOW COLUMNS FROM purchase_order_documents LIKE 'lines_json'`);
        const hasColumn = col.length > 0;
        if (apply && !hasColumn) {
            console.error('purchase_order_documents.lines_json is missing: run `npm run migrate` first.');
            process.exit(2);
        }

        const [[{ auditFloor }]] = await conn.query(
            `SELECT MIN(created_at) AS auditFloor FROM audit_log WHERE entity_type = 'order'`);
        const [[{ ordersFloor }]] = await conn.query(`SELECT MIN(created_at) AS ordersFloor FROM orders`);
        const floor = [auditFloor, ordersFloor].filter(Boolean).reduce((a, b) => (a > b ? a : b), null);

        // Newest document per live PO, still without a snapshot.
        const [docs] = await conn.query(
            `SELECT d.id, d.purchase_order_id, d.version, d.generated_at, d.public_url, po.po_number
               FROM purchase_order_documents d
               JOIN purchase_orders po ON po.id = d.purchase_order_id AND po.deleted_at IS NULL
              WHERE ${hasColumn ? 'd.lines_json IS NULL' : 'TRUE'}
                AND d.version = (SELECT MAX(d2.version) FROM purchase_order_documents d2
                                  WHERE d2.purchase_order_id = d.purchase_order_id)
                ${onlyPo ? 'AND d.purchase_order_id = ?' : ''}
              ORDER BY d.purchase_order_id`,
            onlyPo ? [onlyPo] : []
        );

        console.log(`DB_HOST ${host}  ${apply ? 'APPLY' : 'dry-run'}${hasColumn ? '' : '  (lines_json column not migrated yet)'}`);
        console.log(`history floor ${iso(floor)} (order audit from ${iso(auditFloor)}, oldest orders row ${iso(ordersFloor)})`);
        console.log(`${docs.length} newest PO document(s) without a line snapshot\n`);

        const report = [];
        for (const doc of docs) {
            const row = {
                poId: doc.purchase_order_id, poNumber: doc.po_number, documentId: doc.id, version: doc.version,
                generatedAt: iso(doc.generated_at), url: doc.public_url,
            };
            if (!floor || doc.generated_at < floor) {
                report.push({ ...row, action: 'skip', reason: 'generated before the history floor' });
                continue;
            }
            const data = await loadPo(conn, doc);
            const rebuilt = reconstructPoLinesAt({ poId: doc.purchase_order_id, at: doc.generated_at, ...data });
            const current = reconstructPoLinesAt({ poId: doc.purchase_order_id, at: new Date(), orders: data.orders });
            if (!rebuilt.length) {
                report.push({ ...row, action: 'skip', reason: 'rebuild has no lines', current });
                continue;
            }
            report.push({ ...row, action: 'fill', drift: countDrift(rebuilt, current), rebuilt, current });
        }

        const fill = report.filter(r => r.action === 'fill');
        for (const r of report) {
            const head = `PO ${r.poNumber} (#${r.poId}) v${r.version} ${r.generatedAt}`;
            if (r.action === 'skip') console.log(`  SKIP  ${head}: ${r.reason}`);
            else console.log(`  FILL  ${head}  drift ${r.drift}`);
            if (onlyPo || (r.action === 'fill' && r.drift > 0)) {
                if (r.rebuilt) console.log(`        rebuilt: ${fmtLines(r.rebuilt)}`);
                if (r.current) console.log(`        current: ${fmtLines(r.current)}`);
                console.log(`        ${r.url}`);
            }
        }
        console.log(`\n${fill.length} to fill (${fill.filter(r => r.drift > 0).length} would flag), ${report.length - fill.length} skipped`);
        if (jsonOut) {
            fs.writeFileSync(jsonOut, JSON.stringify({ host, floor: iso(floor), report }, null, 2));
            console.log(`report written to ${jsonOut}`);
        }

        if (!apply) {
            console.log(`Dry run. Re-run with ${stage ? `--stage ${stage} --apply` : '--apply --confirm-host <DB_HOST>'} to write.`);
            return;
        }
        let written = 0;
        for (const r of fill) {
            const [res] = await conn.query(
                'UPDATE purchase_order_documents SET lines_json = ? WHERE id = ? AND lines_json IS NULL',
                [JSON.stringify({ v: SNAPSHOT_VERSION, source: 'audit-rebuild', lines: r.rebuilt }), r.documentId]
            );
            written += res.affectedRows;
        }
        console.log(`wrote ${written} snapshot(s)`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('ERR', e.code || '', e.message); process.exit(1); });
