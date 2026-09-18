'use strict';

// Shipments drift check: re-derives what the shadow should say from the legacy
// tables and diffs it (verifyAll in src/services/shipment-sync.js). Exit code 0
// means no hard finding — the gate for arming (D2a), for every later deploy,
// and for rewiring anything onto shipments in rollout step 4.
//
//   node tools/verify-shipments.js                    # report, exit 1 on hard drift
//   node tools/verify-shipments.js --json out.json
//   node tools/verify-shipments.js --fix --apply --confirm-host <DB_HOST>
//
// --fix --apply re-runs the sync primitives for the drifted keys only, verifies
// again, and when that is clean marks the open shipment_sync_failures resolved.
//
// Hard: every live order carrying a number points at that number's live
// shipment and vice versa; open lines == the legacy allocation rows; booked
// lines == the live member orders; released keys; every stamped document
// points at a shipment that is (or was) linked to its registry row.
// Informational: a stored stage ahead of its orders with no recorded
// transition, stored copies that differ from the orders, memberless booked
// shipments, registry pointers, open sync failures, rows flagged for review.

require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const sync = require('../src/services/shipment-sync');

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);
const fix = flag('--fix');
const apply = flag('--apply');
const confirmHost = arg('--confirm-host');
const jsonOut = arg('--json');
const host = process.env.DB_HOST || '(DB_HOST unset)';

function show(title, rows, max = 25) {
    const n = rows.length;
    console.log(`  ${n ? '!!' : 'ok'}  ${title}: ${n}`);
    for (const r of rows.slice(0, max)) console.log(`        ${JSON.stringify(r)}`);
    if (n > max) console.log(`        … ${n - max} more`);
}

(async () => {
    console.log(`Database: ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}`);
    if (fix && !apply) console.log('--fix without --apply only reports; pass --apply --confirm-host <DB_HOST> to repair.');
    if (fix && apply && confirmHost !== host) {
        console.error(`Refusing to write: --confirm-host must be exactly "${host}".`);
        process.exit(2);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    let exitCode = 0;
    try {
        const result = await sync.verifyAll(conn, { fix: fix && apply, userEmail: 'verify@tools' });
        console.log('\nHard checks');
        for (const [k, rows] of Object.entries(result.hard)) show(k, rows);
        console.log('\nInformational');
        for (const [k, rows] of Object.entries(result.info)) {
            console.log(`  ..  ${k}: ${rows.length}`);
            for (const r of rows.slice(0, 15)) console.log(`        ${JSON.stringify(r)}`);
            if (rows.length > 15) console.log(`        … ${rows.length - 15} more`);
        }
        if (result.fixed) console.log(`\nFixed: ${JSON.stringify(result.fixed)}${result.failuresResolved != null ? `, failures resolved: ${result.failuresResolved}` : ''}`);
        console.log(`\n${result.ok ? 'OK' : 'DRIFT'}: ${result.hardCount} hard finding(s).`);
        if (jsonOut) fs.writeFileSync(jsonOut, JSON.stringify(result, null, 2));
        exitCode = result.ok ? 0 : 1;
    } catch (err) {
        console.error('FAILED:', err);
        exitCode = 2;
    } finally {
        conn.release();
        await closePool();
    }
    process.exit(exitCode);
})();
