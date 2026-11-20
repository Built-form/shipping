'use strict';
// Forwarder links backfill (2026-11-20): link the forwarder names typed before
// ShipLine had a Forwarders list — shipments.forwarder and the payee of a
// forwarder payment (payment_extras.supplier_name, rides_with 'shipment') — to
// a row of `forwarders`, by writing forwarder_id. It never changes a name:
// sign-off keys and transfer matching stay as they were.
// Rules: matchForBackfill / revalidateLink / revalidateRepair in
// src/lib/forwarder-links.js.
//
// DRY-RUN BY DEFAULT: prints what it would link and writes nothing to the
// database. Writing takes a reviewed manifest and --confirm-host.
//
//   node tools/backfill-forwarder-ids.js --out manifest.json
//        # dry run: lists linked / ambiguous / collision / unmatched, and writes
//        # the "linked" rows to manifest.json for review (delete any line you do
//        # not want written)
//   node tools/backfill-forwarder-ids.js --apply --manifest manifest.json --confirm-host <DB_HOST>
//        # writes ONLY the manifest's rows, each re-checked first (still
//        # unlinked, same name; the forwarder still live and still so named),
//        # one transaction per row with an audit_log row (forwarder_linked)
//   node tools/backfill-forwarder-ids.js --check --out check.json
//        # linked rows whose name no longer matches their forwarder's: a rename
//        # (fine) or old code edited the row after a rollback (to repair)
//   node tools/backfill-forwarder-ids.js --repair --manifest check.json --confirm-host <DB_HOST>
//        # for the rows left in check.json (delete the renames you accept):
//        # forwarder_id = the exact live match of the row's name now, or NULL;
//        # audit_log forwarder_relinked
//
// .env normally points at TEST. A prod run is the user's call.
require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const { recordAudit } = require('../src/lib/audit');
const FL = require('../src/lib/forwarder-links');
const { sameSupplier } = require('../src/services/shipment-payment-extract');

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);
const apply = flag('--apply');
const check = flag('--check');
const repair = flag('--repair');
const manifestPath = arg('--manifest');
const out = arg('--out');
const confirmHost = arg('--confirm-host');
const host = process.env.DB_HOST || '(DB_HOST unset)';
const WHO = 'backfill-forwarder-ids@tools';

// Where each kind of row keeps its forwarder name.
const TABLES = {
    shipments: { col: 'forwarder', entityType: 'shipment',
        candidates: `SELECT id, forwarder AS value FROM shipments
                      WHERE forwarder_id IS NULL AND forwarder IS NOT NULL AND TRIM(forwarder) <> '' AND deleted_at IS NULL ORDER BY id`,
        linked: `SELECT t.id, t.forwarder AS value, t.forwarder_id, f.name AS forwarder_name, f.deleted_at AS forwarder_deleted_at
                   FROM shipments t LEFT JOIN forwarders f ON f.id = t.forwarder_id
                  WHERE t.forwarder_id IS NOT NULL AND t.deleted_at IS NULL ORDER BY t.id` },
    payment_extras: { col: 'supplier_name', entityType: 'payment_extra',
        candidates: `SELECT id, supplier_name AS value FROM payment_extras
                      WHERE rides_with = 'shipment' AND forwarder_id IS NULL AND deleted_at IS NULL ORDER BY id`,
        linked: `SELECT t.id, t.supplier_name AS value, t.forwarder_id, f.name AS forwarder_name, f.deleted_at AS forwarder_deleted_at
                   FROM payment_extras t LEFT JOIN forwarders f ON f.id = t.forwarder_id
                  WHERE t.forwarder_id IS NOT NULL AND t.deleted_at IS NULL ORDER BY t.id` },
};

function readManifest() {
    if (!manifestPath) { console.error('--manifest <file> is required.'); process.exit(2); }
    const rows = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    if (!Array.isArray(rows) || rows.some(r => !TABLES[r.table] || !Number.isSafeInteger(r.id))) {
        console.error('The manifest is not a list of { table, id, … } rows this tool wrote.');
        process.exit(2);
    }
    return rows;
}

async function dryRun(conn) {
    const [forwarders] = await conn.query('SELECT id, name, deleted_at FROM forwarders');
    const report = { linked: [], ambiguous: [], collision: [], unmatched: [] };
    for (const [table, t] of Object.entries(TABLES)) {
        const [rows] = await conn.query(t.candidates);
        for (const r of rows) {
            const m = FL.matchForBackfill(r.value, forwarders, sameSupplier);
            if (m.result === 'linked') report.linked.push({ table, id: Number(r.id), value: r.value, forwarderId: m.id, forwarderName: m.name, how: m.how });
            else report[m.result].push({ table, id: Number(r.id), value: r.value, ...(m.ids ? { ids: m.ids } : {}), ...(m.deletedIds ? { deletedIds: m.deletedIds } : {}) });
        }
    }
    for (const k of Object.keys(report)) {
        console.log(`\n${k}: ${report[k].length}`);
        for (const r of report[k]) console.log(`    ${JSON.stringify(r)}`);
    }
    if (out) {
        fs.writeFileSync(out, JSON.stringify(report.linked, null, 2));
        console.log(`\nManifest (the ${report.linked.length} linked row(s)) written to ${out}. Review it, then --apply --manifest ${out}.`);
    }
}

async function applyManifest(conn, rows) {
    const tally = { linked: 0, skipped: 0 };
    for (const entry of rows) {
        const t = TABLES[entry.table];
        await conn.beginTransaction();
        try {
            const [[row]] = await conn.query(`SELECT id, forwarder_id, ${t.col} AS value FROM ${entry.table} WHERE id = ? AND deleted_at IS NULL FOR UPDATE`, [entry.id]);
            const [[f]] = await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE id = ?', [entry.forwarderId]);
            const why = FL.revalidateLink({ entry, row: row || null, forwarder: f || null });
            if (why) {
                await conn.rollback();
                tally.skipped++;
                console.log(`    skipped ${entry.table} ${entry.id}: ${why}`);
                continue;
            }
            const [upd] = await conn.query(
                `UPDATE ${entry.table} SET forwarder_id = ? WHERE id = ? AND forwarder_id IS NULL AND ${t.col} = ?`,
                [entry.forwarderId, entry.id, entry.value]
            );
            if (upd.affectedRows !== 1) { await conn.rollback(); tally.skipped++; console.log(`    skipped ${entry.table} ${entry.id}: changed while writing`); continue; }
            await recordAudit(conn, {
                entityType: t.entityType, entityId: entry.id, action: 'forwarder_linked',
                before: { forwarderId: null, name: entry.value }, after: { forwarderId: entry.forwarderId, forwarderName: entry.forwarderName }, userEmail: WHO,
            });
            await conn.commit();
            tally.linked++;
        } catch (e) {
            await conn.rollback().catch(() => {});
            throw e;
        }
    }
    console.log(`\nlinked ${tally.linked}, skipped ${tally.skipped} of ${rows.length}.`);
}

async function checkLinks(conn) {
    const found = [];
    for (const [table, t] of Object.entries(TABLES)) {
        const [rows] = await conn.query(t.linked);
        for (const r of rows) {
            const reason = r.forwarder_name == null ? 'forwarder gone'
                : FL.nameKey(r.value) !== FL.nameKey(r.forwarder_name) ? (r.forwarder_deleted_at ? 'forwarder removed, and named otherwise' : 'named otherwise')
                    : null;
            if (reason) found.push({ table, id: Number(r.id), value: r.value, forwarderId: Number(r.forwarder_id), forwarderName: r.forwarder_name, reason });
        }
    }
    console.log(`\nlinked rows whose name is not their forwarder's: ${found.length}`);
    for (const r of found) console.log(`    ${JSON.stringify(r)}`);
    if (out) {
        fs.writeFileSync(out, JSON.stringify(found, null, 2));
        console.log(`\nWritten to ${out}. Delete the renames you accept, then --repair --manifest ${out}.`);
    }
}

async function repairManifest(conn, rows) {
    const [live] = await conn.query('SELECT id, name, deleted_at FROM forwarders WHERE deleted_at IS NULL');
    const tally = { relinked: 0, skipped: 0 };
    for (const entry of rows) {
        const t = TABLES[entry.table];
        await conn.beginTransaction();
        try {
            const [[row]] = await conn.query(`SELECT id, forwarder_id, ${t.col} AS value FROM ${entry.table} WHERE id = ? AND deleted_at IS NULL FOR UPDATE`, [entry.id]);
            const why = FL.revalidateRepair({ entry, row: row || null });
            if (why) { await conn.rollback(); tally.skipped++; console.log(`    skipped ${entry.table} ${entry.id}: ${why}`); continue; }
            const next = FL.pairFromText(row.value, live).id;
            const [upd] = await conn.query(
                `UPDATE ${entry.table} SET forwarder_id = ? WHERE id = ? AND forwarder_id = ? AND ${t.col} = ?`,
                [next, entry.id, entry.forwarderId, entry.value]
            );
            if (upd.affectedRows !== 1) { await conn.rollback(); tally.skipped++; console.log(`    skipped ${entry.table} ${entry.id}: changed while writing`); continue; }
            await recordAudit(conn, {
                entityType: t.entityType, entityId: entry.id, action: 'forwarder_relinked',
                before: { forwarderId: entry.forwarderId, name: entry.value }, after: { forwarderId: next }, userEmail: WHO,
            });
            await conn.commit();
            tally.relinked++;
        } catch (e) {
            await conn.rollback().catch(() => {});
            throw e;
        }
    }
    console.log(`\nrelinked ${tally.relinked}, skipped ${tally.skipped} of ${rows.length}.`);
}

(async () => {
    console.log(`Database: ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}`);
    const writes = apply || repair;
    console.log(apply ? 'Mode: APPLY the manifest' : repair ? 'Mode: REPAIR from the manifest' : check ? 'Mode: check linked rows (read only)' : 'Mode: dry run (read only)');
    if (writes && confirmHost !== host) {
        console.error(`Refusing to write: --confirm-host must be exactly "${host}".`);
        process.exit(2);
    }
    const rows = writes ? readManifest() : null;
    const pool = getPool();
    const conn = await pool.getConnection();
    let exitCode = 0;
    try {
        if (apply) await applyManifest(conn, rows);
        else if (repair) await repairManifest(conn, rows);
        else if (check) await checkLinks(conn);
        else await dryRun(conn);
    } catch (err) {
        console.error('FAILED:', err);
        exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
    process.exit(exitCode);
})();
