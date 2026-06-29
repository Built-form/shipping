'use strict';

// Re-run every PENDING (un-actioned) AI status suggestion through the NEW
// pipeline — Pass 1 (triage) → Pass 2 (order-aware refine) → the non-admin
// movement rules — and show how each one changes. Useful after a model/prompt
// change (e.g. enabling Pass-2): does the new code still propose the same thing?
//
// For each pending `status_suggestion` alert it:
//   1. reloads the order fresh (skips if gone/terminal → WITHDRAW the alert),
//   2. recovers the source email (stored meta.source.bodyFull, else re-fetch the
//      thread from Front),
//   3. re-extracts (Flash triage) + refines (Pro, full order context),
//   4. recomputes the rules-valid suggestion + field updates,
//   5. compares to the stored alert and reports CHANGED / SAME / WITHDRAW.
//
// Safe by default (analysis only). Pass --apply to write: refreshed suggestions
// are upserted; ones the new code no longer proposes are dismissed (reversible
// via PATCH /api/v1/alerts/:id/restore). Optional: --limit N, --order ID.
//
//   node tools/reeval-pending-suggestions.js              # dry run (report)
//   node tools/reeval-pending-suggestions.js --apply      # write changes
//   node tools/reeval-pending-suggestions.js --order 602  # one order only

require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { recordAudit } = require('../src/lib/audit');
const { extractStatusFromEmail } = require('../src/services/supplier-email-check');
const { refineSuggestionForOrder } = require('../src/services/suggestion-refine');
const {
    loadOrderForSuggestion, buildSuggestionAlert, computeFieldUpdates,
} = require('../src/services/front-status-import');
const { upsertSuggestionAlert, londonToday } = require('../src/services/daily-alerts');
const { listMessages, messageText, messageFromEmail, frontWebUrl } = require('../src/services/front-qc-import');
const T = require('../src/lib/order-transitions');

const APPLY = process.argv.includes('--apply');
const argVal = (flag) => { const i = process.argv.indexOf(flag); return i >= 0 ? process.argv[i + 1] : null; };
const LIMIT = Number(argVal('--limit')) || 0;
const ONLY_ORDER = Number(argVal('--order')) || 0;
// Pass-1 is only triage here; use Flash to keep the re-eval cheap. Pass-2 (the
// real decision) uses the Pro default.
const PASS1_MODEL = 'gemini-3-flash-preview';

function parseMeta(raw) {
    if (raw == null) return {};
    if (typeof raw === 'string') { try { return JSON.parse(raw); } catch { return {}; } }
    return raw;
}

// Recover the email that drove the suggestion: the stored full body if present
// (alerts created since the bodyFull change), else re-fetch the thread.
async function recoverEmail(meta) {
    const src = meta.source || {};
    if (src.bodyFull) {
        return { text: src.bodyFull, subject: src.subject || null, fromEmail: src.fromEmail || null, receivedAtIso: src.receivedAt || null };
    }
    if (src.conversationId) {
        try {
            const msgs = await listMessages(src.conversationId);
            let m = msgs.find(x => x.id === src.messageId);
            if (!m) for (const x of msgs) if (x.is_inbound && (!m || (x.created_at || 0) > (m.created_at || 0))) m = x;
            if (m) return {
                text: messageText(m), subject: src.subject || null,
                fromEmail: messageFromEmail(m) || src.fromEmail || null,
                receivedAtIso: m.created_at ? new Date(m.created_at * 1000).toISOString() : (src.receivedAt || null),
            };
        } catch (e) { /* fall through */ }
    }
    return null;
}

// Replicates the importer's per-order decision (suggestionFor → field list →
// computeFieldUpdates), returning { declined } or the alert to upsert.
async function decideForOrder(conn, order, ex, ctx, today) {
    const isFqc = T.isFqcOrder(order);
    const sug = T.suggestionFor(order.status, ex.milestone, { isFqc, qcFailed: ex.qcFailed });
    let target, category, remaining, fieldNames;
    if (sug.target) {
        target = sug.target; category = sug.category; remaining = sug.remaining;
        const gateF = category === 'backward_qc_failed' ? [] : T.applicableFieldsFor(target);
        fieldNames = [...new Set([...gateF, ...T.applicableDataFields(target)])];
    } else {
        target = order.status; category = 'data_update'; remaining = [];
        fieldNames = T.applicableDataFields(order.status);
    }
    const fieldUpdates = await computeFieldUpdates(conn, order, fieldNames, ex.fields, today);
    if (!sug.target && fieldUpdates.apply.length === 0) {
        return { declined: true, reason: sug.category === 'blocked_fqc' ? 'blocked_fqc' : 'no rules-valid move and no data change' };
    }
    const appliedMap = {};
    for (const a of fieldUpdates.apply) appliedMap[a.field] = a.to;
    const missing = sug.target
        ? T.missingGateFields(order, target, order.status, { applied: appliedMap, hasQcReport: !!order.has_qc_report })
        : [];
    const alert = buildSuggestionAlert(order, ex, { target, category, remaining }, fieldUpdates, missing, ctx, today);
    return { declined: false, target, category, fieldUpdates, alert };
}

async function withdraw(conn, alertId, note) {
    const [[before]] = await conn.query(`SELECT * FROM daily_alerts WHERE id = ?`, [alertId]);
    await conn.query(
        `UPDATE daily_alerts
            SET dismissed_at = NOW(), resolved_at = NULL, snoozed_until = NULL,
                last_action = 'dismiss', action_note = ?,
                acknowledged_at = NOW(), acknowledged_by = 'system:pass2-reeval'
          WHERE id = ?`,
        [String(note).slice(0, 1000), alertId]
    );
    const [[after]] = await conn.query(`SELECT * FROM daily_alerts WHERE id = ?`, [alertId]);
    await recordAudit(conn, {
        entityType: 'daily_alert', entityId: alertId, action: 'pass2_withdrawn',
        before, after, userEmail: 'system:pass2-reeval',
    });
}

const fuStr = (arr) => (Array.isArray(arr) && arr.length)
    ? arr.map(f => `${f.field}→${f.to}`).join(', ') : '∅';

(async () => {
    const conn = await getPool().getConnection();
    const today = londonToday();
    const stats = { total: 0, same: 0, changed: 0, withdrawn: 0, skipped: 0, failed: 0 };
    try {
        let sql = `SELECT id, dedup_key, title, meta FROM daily_alerts
                    WHERE type = 'status_suggestion'
                      AND dismissed_at IS NULL AND resolved_at IS NULL
                      AND (snoozed_until IS NULL OR snoozed_until <= NOW())`;
        if (ONLY_ORDER) sql += ` AND JSON_EXTRACT(meta,'$.orderId') = ${ONLY_ORDER}`;
        sql += ` ORDER BY id ASC`;
        if (LIMIT) sql += ` LIMIT ${LIMIT}`;
        const [rows] = await conn.query(sql);
        console.log(`Pending status_suggestion alerts: ${rows.length}${APPLY ? '  (APPLY)' : '  (DRY RUN)'}\n`);

        for (const r of rows) {
            stats.total += 1;
            const meta = parseMeta(r.meta);
            const orderId = Number(meta.orderId);
            const oldSuggested = meta.suggestedStatus;
            const oldFields = meta.fieldUpdates || [];
            const head = `#${r.id} order ${orderId} [${meta.poNumber || '—'}] ${meta.currentStatus}→${oldSuggested} (${fuStr(oldFields)})`;

            const order = await loadOrderForSuggestion(conn, orderId);
            if (!order) {
                stats.withdrawn += 1;
                console.log(`WITHDRAW ${head}\n   → order missing/terminal (advanced or deleted)`);
                if (APPLY) await withdraw(conn, r.id, 'Pass-2 re-eval: order missing or past a terminal status.');
                continue;
            }

            const email = await recoverEmail(meta);
            if (!email || !email.text) {
                stats.skipped += 1;
                console.log(`SKIP     ${head}\n   → no stored body and thread not refetchable`);
                continue;
            }

            let ex;
            try {
                const pass1 = await extractStatusFromEmail(email.text, {
                    subject: email.subject, fromEmail: email.fromEmail, emailDate: email.receivedAtIso, model: PASS1_MODEL,
                });
                ex = await refineSuggestionForOrder(conn, { order, pass1, email });
            } catch (e) {
                stats.failed += 1;
                console.log(`FAIL     ${head}\n   → ${e.message}`);
                continue;
            }

            const ctx = {
                messageId: meta.source?.messageId, conversationId: meta.source?.conversationId,
                fromEmail: email.fromEmail, subject: email.subject,
                frontUrl: meta.source?.conversationId ? frontWebUrl(meta.source.conversationId) : null,
                receivedAtIso: email.receivedAtIso, sourceRef: meta.sourceRef,
                bodyFull: email.text, highlights: ex.highlights || [],
            };
            const decision = await decideForOrder(conn, order, ex, ctx, today);

            if (decision.declined) {
                stats.withdrawn += 1;
                console.log(`WITHDRAW ${head}\n   → Pass-2 no longer proposes a change (${decision.reason})\n   reason: ${ex.reasoning || '—'}`);
                if (APPLY) await withdraw(conn, r.id, `Pass-2 re-eval: ${decision.reason}. ${ex.reasoning || ''}`.slice(0, 1000));
                continue;
            }

            const newFields = decision.fieldUpdates.apply;
            const sameTarget = decision.alert.dedupKey === r.dedup_key;
            const sameFields = fuStr(newFields) === fuStr(oldFields);
            if (sameTarget && sameFields) {
                stats.same += 1;
                console.log(`SAME     ${head}`);
                if (APPLY) await upsertSuggestionAlert(conn, decision.alert); // refresh reasoning/highlights anyway
                continue;
            }

            stats.changed += 1;
            console.log(`CHANGED  ${head}\n   → ${order.status}→${decision.target} (${fuStr(newFields)})\n   reason: ${ex.reasoning || '—'}`);
            if (APPLY) {
                // If the target (dedup_key) changed, the old alert is now wrong →
                // withdraw it; the new one is inserted under its own key.
                if (!sameTarget) await withdraw(conn, r.id, `Pass-2 re-eval: superseded by ${order.status}→${decision.target}.`);
                await upsertSuggestionAlert(conn, decision.alert);
            }
        }

        console.log(`\nSUMMARY  total=${stats.total}  same=${stats.same}  changed=${stats.changed}  withdrawn=${stats.withdrawn}  skipped=${stats.skipped}  failed=${stats.failed}`);
        if (!APPLY) console.log(`(DRY RUN — nothing written. Re-run with --apply to write.)`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('FAIL:', e.message, e.stack); process.exitCode = 1; });
