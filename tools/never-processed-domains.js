'use strict';

// Read-only: list sender/participant DOMAINS on order-linked emails that the
// status importer NEVER processed (no row in front_status_imports for the
// conversation) — i.e. recall blind spots whose sender almost certainly isn't on
// the allow-list. Output is candidate domains to add to STATUS_IMPORT_DOMAINS
// (or to suppliers/supplier_emails).
//
// "never_processed" = a front_email_index conversation tied to a known order
// (order_ids non-empty) with NO matching front_status_imports row at all.
// Domains come from the latest sender (from_email) AND every thread participant
// (so a thread where WE replied last still surfaces the supplier's domain).
//
// Usage (PowerShell):
//   node tools/never-processed-domains.js                 # last 365 days
//   node tools/never-processed-domains.js --since 180
//   node tools/never-processed-domains.js --json
//   node tools/never-processed-domains.js --all           # include internal + freemail in the paste line

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const argVal = (flag) => { const i = process.argv.indexOf(flag); return i >= 0 ? process.argv[i + 1] : null; };
const JSON_OUT = process.argv.includes('--json');
const INCLUDE_ALL = process.argv.includes('--all');
const SINCE = Math.max(1, Math.floor(Number(argVal('--since')) || 365));

// Our own domain(s) — never allow-list these (they're us, on every thread).
// jfamedical.co.uk = the purchasing company itself (CC'd on 100% of threads).
const INTERNAL = new Set(['built-form.co.uk', 'jfamedical.co.uk']);
// Freemail domains: real suppliers use these, but you'd allow-list the specific
// ADDRESS, not the whole domain. Flagged, kept out of the paste line.
const FREEMAIL = new Set([
    'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'live.com',
    'yahoo.com', 'yahoo.co.uk', 'aol.com', 'icloud.com', 'me.com',
    'qq.com', '163.com', '126.com', '139.com', 'sina.com', 'sina.cn',
    'foxmail.com', 'yeah.net',
]);
// Marketplace / file-transfer NOTIFICATION senders — linked to orders by ASIN
// but never the supplier. Allow-listing them would pull marketing into Gemini.
const NOTIFICATION = new Set([
    'amazon.com', 'amazon.co.uk', 'sell.amazon.com', 'marketplace.amazon.com',
    'wetransfer.com', 'wensend.com',
]);

const SAMPLE_CAP = 3;

function domainOf(email) {
    if (email == null) return null;
    const m = String(email).toLowerCase().trim().match(/@([a-z0-9.-]+\.[a-z]{2,})/);
    return m ? m[1] : null;
}
function parseArr(raw) {
    if (raw == null) return [];
    if (Array.isArray(raw)) return raw;
    try { const v = JSON.parse(raw); return Array.isArray(v) ? v : []; } catch { return []; }
}

(async () => {
    const conn = await getPool().getConnection();
    try {
        // Order-linked conversations with NO importer row at all.
        const [rows] = await conn.query(
            `SELECT fei.conversation_id, fei.subject, fei.from_email
               FROM front_email_index fei
              WHERE fei.order_ids IS NOT NULL AND JSON_LENGTH(fei.order_ids) > 0
                AND fei.last_message_at >= (NOW() - INTERVAL ${SINCE} DAY)
                AND NOT EXISTS (
                    SELECT 1 FROM front_status_imports fsi
                     WHERE fsi.conversation_id = fei.conversation_id)
              ORDER BY fei.last_message_at DESC`
        );

        // Enrich with thread participants (lives on order_emails).
        const ids = rows.map(r => r.conversation_id);
        const participantsByConvo = new Map();
        const CHUNK = 500;
        for (let i = 0; i < ids.length; i += CHUNK) {
            const slice = ids.slice(i, i + CHUNK);
            if (!slice.length) break;
            try {
                const [oe] = await conn.query(
                    `SELECT conversation_id, participants FROM order_emails
                      WHERE conversation_id IN (${slice.map(() => '?').join(',')})`,
                    slice
                );
                for (const r of oe) {
                    const set = participantsByConvo.get(r.conversation_id) || new Set();
                    for (const p of parseArr(r.participants)) set.add(p);
                    participantsByConvo.set(r.conversation_id, set);
                }
            } catch (e) { /* participants optional — fall back to from_email only */ }
        }

        // Aggregate: domain -> { convos:Set, samples:[], fromSender, fromParticipant }
        const domains = new Map();
        const touch = (d, convoId, subject, viaSender) => {
            if (!d) return;
            if (!domains.has(d)) domains.set(d, { convos: new Set(), samples: [], asSender: false, asParticipant: false });
            const e = domains.get(d);
            e.convos.add(convoId);
            if (viaSender) e.asSender = true; else e.asParticipant = true;
            if (viaSender && subject && e.samples.length < SAMPLE_CAP && !e.samples.includes(subject)) e.samples.push(subject);
        };
        for (const r of rows) {
            touch(domainOf(r.from_email), r.conversation_id, (r.subject || '').slice(0, 70), true);
            const ps = participantsByConvo.get(r.conversation_id);
            if (ps) for (const p of ps) touch(domainOf(p), r.conversation_id, null, false);
        }

        const list = [...domains.entries()].map(([domain, e]) => ({
            domain,
            conversations: e.convos.size,
            internal: INTERNAL.has(domain),
            freemail: FREEMAIL.has(domain),
            notification: NOTIFICATION.has(domain),
            asSender: e.asSender,
            asParticipant: e.asParticipant,
            samples: e.samples,
        })).sort((a, z) => z.conversations - a.conversations || a.domain.localeCompare(z.domain));

        const candidates = list.filter(d => !d.internal && (INCLUDE_ALL || (!d.freemail && !d.notification)));
        const pasteLine = candidates.map(d => d.domain).join(',');

        if (JSON_OUT) {
            console.log(JSON.stringify({
                window: { sinceDays: SINCE }, neverProcessedConversations: rows.length,
                distinctDomains: list.length, domains: list, pasteLine,
            }, null, 2));
            return;
        }

        console.log(`\nnever_processed order-linked emails (last ${SINCE}d): ${rows.length}  ·  distinct domains: ${list.length}\n`);
        const widths = { d: Math.max(6, ...list.map(x => x.domain.length)) };
        console.log('  ' + 'domain'.padEnd(widths.d) + '  convos  where        sample subject(s)');
        console.log('  ' + '─'.repeat(widths.d) + '  ──────  ───────────  ─────────────────');
        for (const d of list) {
            const tag = d.internal ? 'INTERNAL' : (d.notification ? 'notify' : (d.freemail ? 'freemail' : ''));
            const where = `${d.asSender ? 'sender' : ''}${d.asSender && d.asParticipant ? '+' : ''}${d.asParticipant ? 'cc' : ''}`;
            const sample = d.samples.length ? `"${d.samples[0]}"` : '';
            console.log('  ' + d.domain.padEnd(widths.d) + `  ${String(d.conversations).padStart(6)}  ${(where || tag).padEnd(11)}  ${tag && where ? `[${tag}] ` : ''}${sample}`);
        }
        console.log(`\n── paste-ready for STATUS_IMPORT_DOMAINS (excludes internal${INCLUDE_ALL ? '' : ' + freemail'}; ${candidates.length} domains) ──`);
        console.log(pasteLine || '(none)');
        console.log(`\nINTERNAL (us), freemail, and notify (Amazon/WeTransfer) domains are excluded from the paste line — allow-list freemail by specific address (supplier_emails), and don't allow-list notify/internal at all. Re-run with --all to include freemail+notify.`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error('FAIL:', e.message, e.stack); process.exitCode = 1; });
