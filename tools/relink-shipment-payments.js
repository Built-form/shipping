'use strict';
// Repair the shipment pointers on shipment balance payments (and their
// documents) after the shipments shadow was re-seeded or two shipments merged.
//
// Every record stores both `shipment_id` and `shipment_reference` (the string
// orders carry as container_number) precisely because the entity is still a
// rebuildable shadow: a re-seed renumbers it and a merge NULLs the loser's
// reference while moving its orders to the survivor. This walks every live
// record and repairs the pointer:
//
//   merged           -> the surviving shipment (id and reference)
//   missing/deleted  -> the live shipment carrying the same reference
//   reference drift  -> the shipment's current reference (a booked rename
//                       rewrites orders.container_number too)
//
// Anything that still cannot be resolved is listed, never guessed at. The same
// work is exposed to admins as POST /api/v1/shipment-payments/relink, so an
// operator can fix a re-seed without shell access; this tool exists for the
// deploy-time case and prints more detail.
//
// Safe by default: prints what it would change and exits. Every applied change
// writes an audit row (entity_type 'shipment_payment' / '…_document', action
// 'relink').
//
// Usage:
//   node tools/relink-shipment-payments.js                     # dry-run
//   node tools/relink-shipment-payments.js --apply             # repair
//   node tools/relink-shipment-payments.js --apply --user me@example.com

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { recordAudit } = require('../src/lib/audit');
const { relinkShipmentPayments } = require('../src/services/shipment-payments');

function parseArgs(argv) {
    const args = { apply: false, userEmail: 'tools/relink-shipment-payments' };
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a === '--apply') args.apply = true;
        else if (a === '--user') args.userEmail = String(argv[++i] || '').trim() || args.userEmail;
        else { console.error(`Unknown argument: ${a}`); process.exit(1); }
    }
    return args;
}

(async () => {
    const args = parseArgs(process.argv.slice(2));
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const out = await relinkShipmentPayments(conn, {
            apply: args.apply,
            recordAudit,
            userEmail: args.userEmail,
        });
        console.log(`Checked ${out.checked} record(s).`);
        if (!out.relinked.length && !out.unresolved.length) {
            console.log('Every pointer resolves. Nothing to do.');
            return;
        }
        for (const r of out.relinked) {
            console.log(
                `  ${args.apply ? 'relinked' : 'would relink'} ${r.entity} ${r.id} (${r.reason}): ` +
                `shipment ${r.from.shipmentId} "${r.from.shipmentReference}" -> ${r.to.shipmentId} "${r.to.shipmentReference}"`
            );
        }
        for (const u of out.unresolved) {
            console.log(`  UNRESOLVED ${u.entity} ${u.id} (${u.reason}): shipment ${u.shipmentId} "${u.reference}"`);
        }
        console.log(
            `\n${out.relinked.length} to relink, ${out.unresolved.length} unresolved.` +
            (args.apply ? ' Applied.' : ' Dry run — pass --apply to write.')
        );
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => {
    console.error(err);
    process.exit(1);
});
