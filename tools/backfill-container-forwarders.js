'use strict';
// A forwarder's container page (docs/forwarder-containers.md): gives a container
// its forwarder from the payments already linked to one. OPTIONAL, and yours to
// run: nothing else needs it.
//
//   node tools/backfill-container-forwarders.js                                  dry run: says what it would set
//   node tools/backfill-container-forwarders.js --apply --confirm-host <DB_HOST>   sets it
//
// A forwarder sees a container on the supplier portal in full once it is booked
// with them through the Freight page, or once our team pick them on it. Older
// containers have neither: they know their forwarder only through its payments.
// For each REAL container (booked or later) with no forwarder, no typed forwarder,
// no live freight booking, that has NOT arrived, whose live forwarder charges all
// name ONE forwarder from the Forwarders list, this sets that forwarder on the
// container (src/lib/forwarder-sweep.js forwarderToGive). A container with a
// charge that is not linked to a forwarder, or charges to two forwarders, is
// listed and left alone.
//
// What it changes when applied: shipments.forwarder_id and .forwarder (the list's
// name), one audit row per container. THE CONSEQUENCE: that forwarder's portal
// contacts then see the container's page and can send its number, dates and files.
// It is undone by clearing the forwarder on the Shipments page.
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { recordAuditStrict } = require('../src/lib/audit');
const { forwarderToGive } = require('../src/lib/forwarder-sweep');

const argv = process.argv.slice(2);
const arg = (name) => { const i = argv.indexOf(name); return i === -1 ? undefined : argv[i + 1]; };
const apply = argv.includes('--apply');
const host = process.env.DB_HOST || '';

const CANDIDATES = `
    SELECT s.id, s.reference, s.stage, s.deleted_at, s.merged_into_id, s.forwarder_id, s.forwarder,
           (s.ata IS NOT NULL OR EXISTS (SELECT 1 FROM orders o WHERE o.shipment_id = s.id AND o.deleted_at IS NULL AND o.arrived_date IS NOT NULL)) AS arrived,
           EXISTS (SELECT 1 FROM freight_bookings fb WHERE fb.shipment_id = s.id AND fb.status IN ('requested', 'confirmed')) AS live_booking
      FROM shipments s
     WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL AND s.reference IS NOT NULL
       AND s.stage IN ('BOOKED', 'IN_TRANSIT', 'ARRIVED', 'CLOSED') AND s.forwarder_id IS NULL
     ORDER BY s.id`;
// Every live forwarder charge on the container: its forwarder, or NULL for one not linked.
// A forwarder's charge is one paid to its own payee (rides_with 'shipment'), the rule
// tools/backfill-forwarder-ids.js and the payment views use; never told by its kind.
const CHARGES = `
    SELECT ex.forwarder_id
      FROM payment_extras ex
     WHERE ex.deleted_at IS NULL AND (ex.shipment_id = ? OR (ex.shipment_id IS NULL AND ex.shipment_reference = ?))
       AND (ex.forwarder_id IS NOT NULL OR ex.rides_with = 'shipment')`;

const WHY = {
    has_typed_forwarder: 'a forwarder is typed on it (link that one on the Shipments page)',
    has_booking: 'a freight booking holds it',
    arrived: 'it has arrived',
    no_payment: 'no forwarder charge on it',
    unlinked_payment: 'a forwarder charge on it is not linked to the Forwarders list',
    several_forwarders: 'its charges name more than one forwarder',
};

(async () => {
    if (apply && arg('--confirm-host') !== host) { console.error(`To apply, say which database this is: --confirm-host ${host || '<DB_HOST>'}`); process.exit(2); }
    const pool = getPool();
    const conn = await pool.getConnection();
    console.log(`Database ${process.env.DB_USER || '?'}@${host}/${process.env.DB_NAME || '?'}; ${apply ? 'APPLYING' : 'dry run'}`);
    const counts = { set: 0, would: 0 };
    const skipped = {};
    try {
        const [forwarders] = await conn.query('SELECT id, name FROM forwarders WHERE deleted_at IS NULL');
        const nameOf = new Map(forwarders.map(f => [Number(f.id), f.name]));
        const [rows] = await conn.query(CANDIDATES);
        for (const c of rows) {
            const [charges] = await conn.query(CHARGES, [c.id, c.reference]);
            const choice = forwarderToGive({ container: { ...c, arrived: Number(c.arrived) === 1 }, liveBooking: Number(c.live_booking) === 1, paidForwarderIds: charges.map(x => x.forwarder_id) });
            if (choice.forwarderId == null) {
                skipped[choice.why] = (skipped[choice.why] || 0) + 1;
                if (choice.why !== 'no_payment') console.log(`  container ${c.reference}: left alone: ${WHY[choice.why] || choice.why}`);
                continue;
            }
            const name = nameOf.get(choice.forwarderId);
            if (!name) { skipped.forwarder_removed = (skipped.forwarder_removed || 0) + 1; console.log(`  container ${c.reference}: left alone: its forwarder (${choice.forwarderId}) is no longer on the list`); continue; }
            if (!apply) { counts.would += 1; console.log(`  container ${c.reference}: would be given ${name}`); continue; }
            await conn.beginTransaction();
            try {
                // Only a container that still has none: a forwarder picked meanwhile stands.
                const [done] = await conn.query(
                    `UPDATE shipments SET forwarder_id = ?, forwarder = ? WHERE id = ? AND forwarder_id IS NULL AND (forwarder IS NULL OR forwarder = '') AND deleted_at IS NULL`,
                    [choice.forwarderId, name, c.id]
                );
                if (done.affectedRows === 1) {
                    await recordAuditStrict(conn, {
                        entityType: 'shipment', entityId: c.id, action: 'update',
                        before: { forwarderId: null, forwarder: null }, after: { forwarderId: choice.forwarderId, forwarder: name, source: 'backfill-container-forwarders' },
                        userEmail: 'backfill-container-forwarders@tools',
                    });
                    counts.set += 1;
                    console.log(`  container ${c.reference}: given ${name}`);
                } else {
                    console.log(`  container ${c.reference}: changed meanwhile; LEFT ALONE`);
                }
                await conn.commit();
            } catch (e) {
                await conn.rollback().catch(() => {});
                throw e;
            }
        }
        const left = Object.entries(skipped).map(([why, n]) => `${n} ${WHY[why] ? `(${WHY[why]})` : why}`).join('; ');
        console.log(apply ? `${counts.set} container(s) given a forwarder.` : `${counts.would} container(s) would be given a forwarder (run with --apply --confirm-host to set them).`);
        if (left) console.log(`Left alone: ${left}.`);
    } catch (e) {
        console.error('STOPPED:', e.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
