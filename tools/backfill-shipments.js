'use strict';

// Shipments backfill (rollout step 1): seed `shipments` / `shipment_lines` and
// the shipment_id columns from the legacy tables, then arm the dual-write hooks.
// Logic: backfillAll in src/services/shipment-sync.js.
//
// DRY-RUN BY DEFAULT: prints the report and writes nothing.
//
//   node tools/backfill-shipments.js                        # dry-run report
//   node tools/backfill-shipments.js --json report.json     # + machine-readable copy
//   node tools/backfill-shipments.js --apply --confirm-host <DB_HOST> [--link-by-name-hint]
//        [--exclude-hint-ids 88,92] [--rearm]
//
// .env normally points at TEST. Writing requires --confirm-host with exactly
// the DB_HOST this run is connected to, so a prod run is always deliberate.
//   --link-by-name-hint  link documents-only historical drafts to the booked
//                        shipment their name's number names ('... - 316 ETD')
//                        — only after reviewing the "name-hint links" list
//   --exclude-hint-ids   registry ids to leave out of that linking
//   --rearm              also remove the shipments_shadow_off kill switch
// Idempotent: re-running skips what exists. The rollout runs --apply, waits
// more than 60 s (warm Lambdas notice the marker), runs --apply again, then
// tools/verify-shipments.js must exit 0.

require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');
const sync = require('../src/services/shipment-sync');

function arg(name) {
    const i = process.argv.indexOf(name);
    return i >= 0 ? process.argv[i + 1] : null;
}
const flag = name => process.argv.includes(name);

const apply = flag('--apply');
const confirmHost = arg('--confirm-host');
const linkByNameHint = flag('--link-by-name-hint');
const rearm = flag('--rearm');
const excludeHintIds = (arg('--exclude-hint-ids') || '').split(',').map(s => Number(s.trim())).filter(Boolean);
const jsonOut = arg('--json');

const host = process.env.DB_HOST || '(DB_HOST unset)';
const dbName = process.env.DB_NAME || '(DB_NAME unset)';

function table(rows, cols) {
    if (!rows.length) { console.log('    (none)'); return; }
    const widths = cols.map(c => Math.max(c.label.length, ...rows.map(r => String(c.get(r) ?? '').length)));
    const line = vals => '    ' + vals.map((v, i) => String(v ?? '').padEnd(widths[i])).join('  ');
    console.log(line(cols.map(c => c.label)));
    console.log('    ' + widths.map(w => '-'.repeat(w)).join('  '));
    for (const r of rows) console.log(line(cols.map(c => c.get(r))));
}

function section(title) {
    console.log(`\n== ${title} ${'='.repeat(Math.max(0, 74 - title.length))}`);
}

function mix(m) {
    return Object.entries(m).map(([k, v]) => `${k}:${v}`).join(' ');
}

function printReport(r) {
    const refs = r.references;
    section('Totals');
    const byClass = c => refs.filter(x => x.class === c).length;
    console.log(`    references: ${refs.length}  (sea ${byClass('sea')}, air ${byClass('air')}, quarantined ${byClass('junk')})`);
    console.log(`    to create: ${refs.filter(x => x.action === 'create').length}, already exist: ${refs.filter(x => x.action === 'exists').length}`);
    console.log(`    member orders: ${refs.reduce((n, x) => n + x.orders, 0)}, units: ${refs.reduce((n, x) => n + x.units, 0)}`);
    console.log(`    registry rows: ${r.drafts.filter(d => d.registryId).length}, planned names: ${r.planned.length}`);

    section('Booked shipments, one per reference');
    table(refs, [
        { label: 'reference', get: x => x.reference },
        { label: 'class', get: x => x.class },
        { label: 'mode', get: x => x.mode ? `${x.mode} (${x.modeSource})` : '?' },
        { label: 'stage', get: x => x.derivedStage },
        { label: 'orders', get: x => x.orders },
        { label: 'units', get: x => x.units },
        { label: 'tracking_ref', get: x => x.trackingRef ? `${x.trackingRef} [${x.trackingSource === 'awb_number' ? 'awb' : 'ext'}]` : '' },
        { label: 'status mix', get: x => mix(x.statusMix) },
        { label: 'review', get: x => x.reviewNote || '' },
        { label: 'action', get: x => x.action === 'exists' ? `exists #${x.shipmentId}` : 'create' },
    ]);

    section('Quarantined references (kept verbatim, needs_review)');
    console.log(`    ${r.anomalies.junk.length ? r.anomalies.junk.join(', ') : '(none)'}`);
    section('Mode unknown (needs_review)');
    console.log(`    ${r.anomalies.unknownMode.length ? r.anomalies.unknownMode.join(', ') : '(none)'}`);
    section('Sequence outliers (a SEA number inside the AIR range)');
    console.log(`    ${r.anomalies.sequenceOutliers.length ? r.anomalies.sequenceOutliers.join(', ') : '(none)'}`);
    section('Mixed-status references');
    table(r.anomalies.mixedStatus, [
        { label: 'reference', get: x => x.reference }, { label: 'status mix', get: x => mix(x.statusMix) },
    ]);
    section('References with more than one carrier ref (expect none)');
    table(r.anomalies.multipleCarrierRefs, [
        { label: 'reference', get: x => x.reference }, { label: 'carrier refs', get: x => x.candidates.join(', ') },
    ]);
    section('Carrier refs on more than one reference (expect none)');
    table(r.anomalies.carrierRefOnManyReferences, [
        { label: 'carrier ref', get: x => x.carrier }, { label: 'references', get: x => x.references.join(', ') },
    ]);
    section('external_container_number vs awb_number conflicts');
    table(r.anomalies.externalVsAwb.flatMap(x => x.conflicts.map(c => ({ reference: x.reference, ...c }))), [
        { label: 'reference', get: x => x.reference }, { label: 'order', get: x => x.orderId },
        { label: 'external', get: x => x.external }, { label: 'awb', get: x => x.awb },
    ]);

    section('Drafts: per registry row');
    table(r.drafts, [
        { label: 'id', get: x => x.registryId ?? '-' },
        { label: 'name', get: x => x.name },
        { label: 'alloc', get: x => x.allocationRows ?? '' },
        { label: 'live', get: x => x.liveLines ?? '' },
        { label: 'docs', get: x => x.documents ?? '' },
        { label: 'qa', get: x => x.qaDocuments ?? '' },
        { label: 'hint', get: x => x.hint || '' },
        { label: 'action', get: x => x.action },
        { label: 'target', get: x => x.target ?? '' },
        { label: 'note', get: x => x.note || '' },
    ]);
    const hintLinks = r.drafts.filter(d => d.action === 'link-hint' || d.action === 'pending-hint');
    section(`Name-hint links to review (${hintLinks.length}) — apply with --link-by-name-hint`);
    table(hintLinks, [
        { label: 'registry id', get: x => x.registryId },
        { label: 'draft name', get: x => x.name },
        { label: '-> reference', get: x => x.target },
        { label: 'mode', get: x => x.hintMode || '(no stamp)' },
        { label: 'docs', get: x => (x.documents || 0) + (x.qaDocuments || 0) },
    ]);
    const sharedHints = {};
    for (const d of hintLinks) (sharedHints[d.target] ||= []).push(d.registryId);
    const shared = Object.entries(sharedHints).filter(([, ids]) => ids.length > 1);
    if (shared.length) {
        console.log('    several drafts point at one reference (renamed drafts leave their old names behind):');
        for (const [ref, ids] of shared) console.log(`      ${ref}: registry ids ${ids.join(', ')}`);
    }

    section('Planned containers');
    table(r.planned, [
        { label: 'name', get: x => x.name }, { label: 'lines', get: x => x.lines },
        { label: 'dead', get: x => x.deadLines }, { label: 'hint', get: x => x.hint || '' },
    ]);
    section('Allocation lines on deleted orders (mirrored, hidden like legacy)');
    table(r.anomalies.linesOnDeletedOrders, [
        { label: 'kind', get: x => x.kind }, { label: 'name', get: x => x.name },
        { label: 'order', get: x => x.order_id }, { label: 'allocated', get: x => x.allocated },
    ]);
    section('Over-allocations (allocated > order quantity; legacy allows it)');
    table(r.anomalies.overAllocations, [
        { label: 'kind', get: x => x.kind }, { label: 'name', get: x => x.name }, { label: 'order', get: x => x.order_id },
        { label: 'allocated', get: x => x.allocated }, { label: 'order qty', get: x => x.quantity },
    ]);
    section('Documents to stamp');
    for (const [k, v] of Object.entries(r.documents)) console.log(`    ${k}: ${v}`);
    section('Next reference the allocator would hand out');
    for (const m of ['SEA', 'AIR']) {
        const n = r.next[m];
        if (!n) continue;
        const reserved = n.reservedBy.slice(0, 5).map(x => `${x.seq} (${x.name})`).join('; ');
        console.log(`    ${m}: ${n.reference}   (highest carried by a live order: ${n.maxBooked}, highest reserved by an open name: ${n.maxReserved}${reserved ? ` — ${reserved}` : ''})`);
    }
    section(r.apply ? 'Writes' : 'Writes (dry-run: nothing written)');
    for (const [k, v] of Object.entries(r.writes)) console.log(`    ${k}: ${v}`);
    console.log(`    marker ${sync.BACKFILL_MARKER}: ${r.armed ? 'present (hooks armed)' : 'absent (hooks inert)'}`);
    console.log(`    kill switch ${sync.KILL_SWITCH}: ${r.killSwitch ? 'ON' : 'off'}${r.rearmed ? ' (removed by --rearm)' : ''}`);
}

(async () => {
    console.log(`Database: ${process.env.DB_USER || '?'}@${host}/${dbName}`);
    console.log(apply ? 'Mode: APPLY' : 'Mode: dry-run (pass --apply --confirm-host <DB_HOST> to write)');
    if (apply && confirmHost !== host) {
        console.error(`Refusing to write: --confirm-host must be exactly "${host}".`);
        process.exit(2);
    }
    const pool = getPool();
    const conn = await pool.getConnection();
    let exitCode = 0;
    try {
        const [t] = await conn.query(`SHOW TABLES LIKE 'shipments'`);
        const [c] = await conn.query(`SHOW COLUMNS FROM orders LIKE 'shipment_id'`);
        if (!t.length || !c.length) {
            console.error('The shipments schema is not applied here. Hand-apply src/db/migrations/2026-09-18_*.sql first.');
            process.exit(2);
        }
        const started = Date.now();
        const report = await sync.backfillAll(conn, {
            apply, linkByNameHint, excludeHintIds, rearm, userEmail: 'backfill@tools',
        });
        printReport(report);
        console.log(`\nDone in ${((Date.now() - started) / 1000).toFixed(1)} s.`);
        if (apply) console.log('Next: wait > 60 s, run --apply again, then node tools/verify-shipments.js (must exit 0).');
        if (jsonOut) {
            fs.writeFileSync(jsonOut, JSON.stringify(report, null, 2));
            console.log(`JSON report written to ${jsonOut}`);
        }
    } catch (err) {
        console.error('FAILED:', err);
        exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
    process.exit(exitCode);
})();
