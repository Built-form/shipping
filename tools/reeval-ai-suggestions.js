'use strict';

// One-off + re-runnable: re-evaluate every PENDING `status_suggestion` alert
// against the current ShipLine non-admin movement rules
// (src/lib/order-transitions.js) and RESOLVE the ones that are no longer a
// rules-valid move from the order's CURRENT status — e.g. pre-rework suggestions
// that proposed a disallowed backward move or a multi-stage skip, or suggestions
// whose order has since advanced.
//
// Safe by default (analysis only). Pass --apply to write. Resolving is
// reversible (PATCH /api/v1/alerts/:id/restore). Valid suggestions are left
// untouched; the importer regenerates fresh, fully-shaped suggestions on its
// next run anyway.
//
//   node tools/reeval-ai-suggestions.js            # dry run (report only)
//   node tools/reeval-ai-suggestions.js --apply    # resolve invalid ones

require('dotenv').config();
const { getPool } = require('../src/db');
const { recordAudit } = require('../src/lib/audit');
const T = require('../src/lib/order-transitions');

const APPLY = process.argv.includes('--apply');

function parseMeta(raw) {
    if (raw == null) return {};
    if (typeof raw === 'string') { try { return JSON.parse(raw); } catch { return {}; } }
    return raw;
}

(async () => {
    const conn = await getPool().getConnection();
    const keep = [];
    const resolve = [];
    try {
        // Pending = not dismissed, not resolved, not currently snoozed.
        const [rows] = await conn.query(
            `SELECT id, dedup_key, title, meta
               FROM daily_alerts
              WHERE type = 'status_suggestion'
                AND dismissed_at IS NULL AND resolved_at IS NULL
                AND (snoozed_until IS NULL OR snoozed_until <= NOW())`
        );
        console.log(`Pending status_suggestion alerts: ${rows.length}\n`);

        for (const r of rows) {
            const meta = parseMeta(r.meta);
            const orderId = Number(meta.orderId);
            const suggested = meta.suggestedStatus;
            const [ords] = await conn.query(
                `SELECT id, status, jf_code, asin FROM orders WHERE id = ? AND deleted_at IS NULL`, [orderId]
            );
            let reason = null;
            if (!ords.length) {
                reason = 'order missing/deleted';
            } else {
                const order = ords[0];
                const isFqc = T.isFqcOrder(order);
                if (!suggested) reason = 'no suggestedStatus in meta';
                else if (order.status === suggested) reason = `order already ${order.status} (no-op)`;
                else if (!T.canTransition(order.status, suggested, { isFqc })) {
                    reason = `not a valid non-admin move: ${order.status} -> ${suggested}${isFqc ? ' (FQC)' : ''}`;
                }
                if (reason) reason += `  [order now ${order.status}]`;
            }
            if (reason) resolve.push({ id: r.id, title: r.title, reason });
            else keep.push({ id: r.id, title: r.title });
        }

        console.log(`KEEP (still rules-valid): ${keep.length}`);
        for (const k of keep) console.log(`  ✓ #${k.id}  ${k.title}`);
        console.log(`\nRESOLVE (no longer rules-valid): ${resolve.length}`);
        for (const x of resolve) console.log(`  ✗ #${x.id}  ${x.title}\n        → ${x.reason}`);

        if (!APPLY) {
            console.log(`\n(DRY RUN — nothing written. Re-run with --apply to resolve the ${resolve.length} invalid ones.)`);
            return;
        }
        for (const x of resolve) {
            const [[before]] = await conn.query(`SELECT * FROM daily_alerts WHERE id = ?`, [x.id]);
            // DISMISS (not resolve): in the AI flow only an approve sets
            // resolved_at, so the UI reads resolved == approved. These invalid
            // suggestions are rejected, so dismiss them — and last_action='dismiss'
            // keeps the UI label correct ("Dismissed", never "Approved").
            await conn.query(
                `UPDATE daily_alerts
                    SET dismissed_at = NOW(), resolved_at = NULL, snoozed_until = NULL,
                        last_action = 'dismiss', action_note = ?,
                        acknowledged_at = NOW(), acknowledged_by = 'system:rules-reeval'
                  WHERE id = ?`,
                [String(x.reason).slice(0, 1000), x.id]
            );
            const [[after]] = await conn.query(`SELECT * FROM daily_alerts WHERE id = ?`, [x.id]);
            await recordAudit(conn, {
                entityType: 'daily_alert', entityId: x.id, action: 'rules_invalidated',
                before, after, userEmail: 'system:rules-reeval',
            });
        }
        console.log(`\nAPPLIED: resolved ${resolve.length} rule-invalid pending suggestions (reversible via /restore).`);
    } finally {
        conn.release();
        process.exit(0);
    }
})().catch(e => { console.error('FAIL:', e.message); process.exit(1); });
