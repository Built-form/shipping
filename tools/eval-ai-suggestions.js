'use strict';

// Read-only eval / observability report for the AI status-suggestion pipeline.
// Answers "is the AI reading emails well, and where is it wrong or missing?" by
// mining data that already accumulates — it writes NOTHING and adds no schema.
//
// SCORING MODEL — operators now either ACCEPT a suggestion (applied as-is) or
// ACKNOWLEDGE it with a comment (they handled it their own way). 'deny' is
// retired and only appears in historical rows. So the headline is the
// DIRECT-ACCEPT RATE = accept / (accept + acknowledge + deny[legacy]). An
// acknowledge isn't always "the AI was wrong" — sometimes the operator just
// actioned it manually — but it IS "the suggestion wasn't used as-is", and its
// comment is the richest signal we have for what the model got wrong.
//
// Three sections:
//   1. ACCURACY & CALIBRATION — direct-accept rate overall and by confidence
//      decile, category, suggested status, supplier, and whether Pass-2 ran.
//      The confidence-decile view tests whether STATUS_IMPORT_CONF_MIN=0.6 sits
//      in the right place.
//   2. RECALL BLIND SPOTS — emails the indexer linked to a KNOWN order
//      (front_email_index.order_ids) that never produced a surfaced suggestion
//      (no 'alerted' row in front_status_imports — skipped, or never processed
//      at all because the sender wasn't on the allow-list).
//   3. ACKNOWLEDGE / CORRECTION MINING — where operators keep handling things
//      themselves, grouped by supplier x suggested status, WITH their comments.
//      Evidence for whether a supplier-level learning loop is worth building.
//
// Usage (run via PowerShell — node has PATH issues under Bash on this box):
//   node tools/eval-ai-suggestions.js                 # last 90 days, tables
//   node tools/eval-ai-suggestions.js --since 30
//   node tools/eval-ai-suggestions.js --supplier "ACME"
//   node tools/eval-ai-suggestions.js --json          # machine-readable

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const argVal = (flag) => { const i = process.argv.indexOf(flag); return i >= 0 ? process.argv[i + 1] : null; };
const JSON_OUT = process.argv.includes('--json');
// Validate to a positive integer so it can be safely inlined into SQL (no bind
// into INTERVAL needed), mirroring how the sibling reeval tools inline ints.
const SINCE = Math.max(1, Math.floor(Number(argVal('--since')) || 90));
const SUPPLIER = (argVal('--supplier') || '').trim().toLowerCase();
// Below this per-cell sample size a rate is too noisy to read into.
const MIN_N = 10;
const SAMPLE_CAP = 15;

function parseMeta(raw) {
    if (raw == null) return {};
    if (typeof raw === 'object') return raw;
    try { return JSON.parse(raw); } catch { return {}; }
}

const pct = (num, den) => (den > 0 ? `${((num / den) * 100).toFixed(0)}%` : '—');
const pctNum = (num, den) => (den > 0 ? Math.round((num / den) * 1000) / 10 : null);

// ── grouping ─────────────────────────────────────────────────────────────────
// Each bucket counts accept (used as-is), acknowledge (handled manually), and
// legacy deny. The direct-accept rate = accept / (accept + acknowledge + deny).
function tally() { return { accept: 0, acknowledge: 0, deny: 0 }; }
function add(bucket, action) {
    const k = action === 'approve' ? 'accept' : action; // approve is stored; we label it accept
    bucket[k] = (bucket[k] || 0) + 1;
}
function denom(b) { return b.accept + b.acknowledge + b.deny; }
function groupBy(rows, keyFn) {
    const m = new Map();
    for (const r of rows) {
        const k = keyFn(r);
        if (k == null) continue;
        if (!m.has(k)) m.set(k, tally());
        add(m.get(k), r.action);
    }
    return m;
}
function confDecile(c) {
    if (typeof c !== 'number' || !Number.isFinite(c)) return null;
    const b = Math.min(9, Math.max(0, Math.floor(c * 10)));
    return `${(b / 10).toFixed(1)}–${((b + 1) / 10).toFixed(1)}`;
}

// Render a grouped Map as a padded table; rows sorted by n desc unless overridden.
function tableFromGroups(map, label, { sortKey } = {}) {
    const entries = [...map.entries()].map(([key, b]) => ({
        key, ...b, n: denom(b), rate: pctNum(b.accept, denom(b)),
    }));
    entries.sort(sortKey || ((a, z) => z.n - a.n || String(a.key).localeCompare(String(z.key))));
    const rows = entries.map(e => ([
        String(e.key),
        String(e.n) + (e.n < MIN_N ? ' *' : ''),
        pct(e.accept, e.n),
        String(e.accept), String(e.acknowledge), String(e.deny),
    ]));
    printTable(label, [label, 'n', 'accept%', 'acc', 'ack', 'deny'], rows);
    return entries;
}

function printTable(title, headers, rows) {
    const widths = headers.map((h, i) => Math.max(h.length, ...rows.map(r => (r[i] || '').length), 0));
    const fmt = (cells) => cells.map((c, i) => String(c).padEnd(widths[i])).join('  ');
    console.log(`\n  ${title}`);
    console.log('  ' + fmt(headers));
    console.log('  ' + widths.map(w => '─'.repeat(w)).join('  '));
    for (const r of rows) console.log('  ' + fmt(r));
}

// ── 1. accuracy & calibration ─────────────────────────────────────────────────
async function loadActioned(conn) {
    // Human accept/acknowledge (+ legacy deny) only — exclude the system re-eval
    // tool's 'dismiss' writes (acknowledged_by LIKE 'system:%') and snooze/restore.
    const [rows] = await conn.query(
        `SELECT id, title, meta, last_action, action_note, acknowledged_by, acknowledged_at
           FROM daily_alerts
          WHERE type = 'status_suggestion'
            AND last_action IN ('approve','deny','acknowledge')
            AND (acknowledged_by IS NULL OR acknowledged_by NOT LIKE 'system:%')
            AND acknowledged_at >= (NOW() - INTERVAL ${SINCE} DAY)
          ORDER BY acknowledged_at DESC`
    );
    return rows.map(r => {
        const meta = parseMeta(r.meta);
        return {
            id: r.id, action: r.last_action, note: r.action_note,
            actionedBy: r.acknowledged_by, actionedAt: r.acknowledged_at,
            confidence: typeof meta.confidence === 'number' ? meta.confidence : null,
            refined: meta.refined === true ? 'refined' : (meta.refined === false ? 'pass1-only' : 'unknown'),
            supplier: meta.supplier || '(unknown)',
            suggestedStatus: meta.suggestedStatus || '(none)',
            category: meta.category || '(none)',
            fieldUpdates: Array.isArray(meta.fieldUpdates) ? meta.fieldUpdates : [],
        };
    }).filter(r => !SUPPLIER || String(r.supplier).toLowerCase().includes(SUPPLIER));
}

function accuracySection(rows) {
    const overall = tally();
    for (const r of rows) add(overall, r.action);
    const n = denom(overall);
    const headline = {
        actioned: rows.length,
        accept: overall.accept, acknowledge: overall.acknowledge, deny: overall.deny,
        n, acceptRate: pctNum(overall.accept, n),
    };
    const groups = {
        confidence: groupBy(rows, r => confDecile(r.confidence)),
        category: groupBy(rows, r => r.category),
        suggestedStatus: groupBy(rows, r => r.suggestedStatus),
        supplier: groupBy(rows, r => r.supplier),
        refined: groupBy(rows, r => r.refined),
    };
    return { headline, groups };
}

// ── 2. recall blind spots ─────────────────────────────────────────────────────
// Outcomes ranked by how informative they are when reporting a thread that never
// alerted (most actionable first).
const OUTCOME_PRIORITY = [
    'skipped_lowconf', 'skipped_ambiguous_jf', 'skipped_nomatch', 'skipped_nopo', 'failed',
    'skipped_fqc', 'skipped_nochange', 'skipped_chatter', 'pending',
];
function pickReason(outcomes) {
    if (!outcomes || !outcomes.size) return 'never_processed';
    for (const o of OUTCOME_PRIORITY) if (outcomes.has(o)) return o;
    return [...outcomes][0] || 'never_processed';
}

async function recallSection(conn) {
    // Conversations the indexer tied to a known order, recently active.
    const [linked] = await conn.query(
        `SELECT conversation_id, subject, order_ids, last_message_at
           FROM front_email_index
          WHERE order_ids IS NOT NULL AND JSON_LENGTH(order_ids) > 0
            AND last_message_at >= (NOW() - INTERVAL ${SINCE} DAY)
          ORDER BY last_message_at DESC`
    );
    if (!linked.length) {
        return { totalLinked: 0, covered: 0, byReason: {}, samples: [], note: 'No order-linked emails indexed in window.' };
    }
    const ids = linked.map(r => r.conversation_id);
    // Pull every importer outcome for those conversations (one row per message).
    const outcomesByConvo = new Map();
    const CHUNK = 500;
    for (let i = 0; i < ids.length; i += CHUNK) {
        const slice = ids.slice(i, i + CHUNK);
        const [imps] = await conn.query(
            `SELECT conversation_id, outcome FROM front_status_imports
              WHERE conversation_id IN (${slice.map(() => '?').join(',')})`,
            slice
        );
        for (const im of imps) {
            if (!outcomesByConvo.has(im.conversation_id)) outcomesByConvo.set(im.conversation_id, new Set());
            outcomesByConvo.get(im.conversation_id).add(im.outcome);
        }
    }

    let covered = 0;
    const byReason = {};
    const samples = [];
    for (const r of linked) {
        const outcomes = outcomesByConvo.get(r.conversation_id);
        if (outcomes && outcomes.has('alerted')) { covered += 1; continue; }
        const reason = pickReason(outcomes);
        byReason[reason] = (byReason[reason] || 0) + 1;
        if (samples.length < SAMPLE_CAP) {
            let orderIds = r.order_ids;
            try { orderIds = typeof r.order_ids === 'string' ? JSON.parse(r.order_ids) : r.order_ids; } catch { /* keep */ }
            samples.push({
                conversationId: r.conversation_id,
                subject: (r.subject || '(no subject)').slice(0, 80),
                orders: Array.isArray(orderIds) ? orderIds.slice(0, 5) : orderIds,
                reason,
            });
        }
    }
    return { totalLinked: linked.length, covered, byReason, samples };
}

// ── 3. acknowledge / correction mining ─────────────────────────────────────────
function correctionSection(rows) {
    // Where the operator did NOT accept as-is. acknowledge is the live signal;
    // deny is legacy but folded in for historical windows.
    const corrections = rows.filter(r => r.action === 'acknowledge' || r.action === 'deny');
    const hotspots = new Map();   // supplier → suggestedStatus
    const fieldHits = new Map();  // fields present on not-accepted suggestions
    for (const r of corrections) {
        const key = `${r.supplier} → ${r.suggestedStatus}`;
        if (!hotspots.has(key)) hotspots.set(key, { count: 0, notes: [] });
        const h = hotspots.get(key);
        h.count += 1;
        if (r.note && h.notes.length < 4) h.notes.push(String(r.note).replace(/\s+/g, ' ').slice(0, 140));
        for (const fu of r.fieldUpdates) {
            if (fu && fu.field) fieldHits.set(fu.field, (fieldHits.get(fu.field) || 0) + 1);
        }
    }
    const topHotspots = [...hotspots.entries()]
        .map(([key, v]) => ({ key, ...v }))
        .sort((a, z) => z.count - a.count)
        .slice(0, SAMPLE_CAP);
    const topFields = [...fieldHits.entries()]
        .map(([field, count]) => ({ field, count }))
        .sort((a, z) => z.count - a.count)
        .slice(0, 12);
    return { totalCorrections: corrections.length, topHotspots, topFields };
}

// ── main ───────────────────────────────────────────────────────────────────────
(async () => {
    const conn = await getPool().getConnection();
    try {
        const actioned = await loadActioned(conn);
        const accuracy = accuracySection(actioned);
        const recall = await recallSection(conn);
        const corrections = correctionSection(actioned);

        if (JSON_OUT) {
            const dump = {
                window: { sinceDays: SINCE, supplier: SUPPLIER || null },
                accuracy: {
                    headline: accuracy.headline,
                    groups: Object.fromEntries(Object.entries(accuracy.groups).map(([k, m]) => [
                        k, [...m.entries()].map(([key, b]) => ({ key, ...b, n: denom(b), acceptRate: pctNum(b.accept, denom(b)) })),
                    ])),
                },
                recall,
                corrections,
                caveats: [
                    'acceptRate = accept / (accept + acknowledge + deny); acknowledge means "handled manually", not always "AI wrong" — read the §3 comments',
                    'deny is retired; appears only in historical rows',
                    `cells with n<${MIN_N} are statistically thin`,
                    'meta.confidence is the post-refine stored value',
                ],
            };
            console.log(JSON.stringify(dump, null, 2));
            return;
        }

        const supLabel = SUPPLIER ? ` · supplier~"${SUPPLIER}"` : '';
        console.log(`\n══ AI suggestion eval · last ${SINCE} days${supLabel} ══`);

        // Section 1
        const h = accuracy.headline;
        console.log(`\n[1] ACCURACY & CALIBRATION`);
        console.log(`  actioned=${h.actioned}  accept=${h.accept}  acknowledge=${h.acknowledge}  deny(legacy)=${h.deny}`);
        console.log(`  direct-accept rate (accept / all actioned, n=${h.n}): ${pct(h.accept, h.n)}`);
        if (!h.actioned) {
            console.log('  (no human-actioned suggestions in window — nothing to score yet)');
        } else {
            tableFromGroups(accuracy.groups.confidence, 'by confidence', {
                sortKey: (a, z) => String(a.key).localeCompare(String(z.key)),
            });
            tableFromGroups(accuracy.groups.refined, 'by Pass-2 refine');
            tableFromGroups(accuracy.groups.category, 'by category');
            tableFromGroups(accuracy.groups.suggestedStatus, 'by suggested status');
            tableFromGroups(accuracy.groups.supplier, 'by supplier');
            console.log(`\n  * n < ${MIN_N}: too few to read into. ack = operator handled it their own way (see §3 comments), counted as not-directly-accepted.`);
        }

        // Section 2
        console.log(`\n[2] RECALL BLIND SPOTS  (emails linked to a known order but never surfaced a suggestion)`);
        const uncovered = recall.totalLinked - recall.covered;
        console.log(`  order-linked emails in window: ${recall.totalLinked}  ·  surfaced (alerted): ${recall.covered}  ·  not surfaced: ${uncovered}`);
        if (recall.note) console.log(`  ${recall.note}`);
        const reasonRows = Object.entries(recall.byReason).sort((a, z) => z[1] - a[1]).map(([k, v]) => [k, String(v)]);
        if (reasonRows.length) printTable('not-surfaced by reason', ['reason', 'count'], reasonRows);
        if (recall.samples.length) {
            console.log(`\n  sample (up to ${SAMPLE_CAP}) — review in Front:`);
            for (const s of recall.samples) {
                console.log(`   · [${s.reason}] order ${JSON.stringify(s.orders)} — "${s.subject}"  (${s.conversationId})`);
            }
        }
        console.log(`  note: many product-tier links are genuine chatter; magnitude + samples guide where recall is actually lost.`);

        // Section 3
        console.log(`\n[3] ACKNOWLEDGE / CORRECTION MINING  (where operators handled it themselves — the comment says what really happened)`);
        console.log(`  total not-accepted (acknowledge + legacy deny): ${corrections.totalCorrections}`);
        if (corrections.topHotspots.length) {
            printTable('supplier → suggested status hotspots', ['supplier → status', 'count'],
                corrections.topHotspots.map(hs => [hs.key, String(hs.count)]));
            console.log(`\n  comments:`);
            for (const hs of corrections.topHotspots) {
                if (hs.notes.length) console.log(`   · ${hs.key}: ${hs.notes.map(n => `"${n}"`).join('  ')}`);
            }
        }
        if (corrections.topFields.length) {
            printTable('fields most-present in not-accepted suggestions', ['field', 'count'],
                corrections.topFields.map(f => [f.field, String(f.count)]));
        }
        console.log('');
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('FAIL:', e.message, e.stack); process.exitCode = 1; });
