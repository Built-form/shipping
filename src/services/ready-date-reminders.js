'use strict';

// ── Ready-date reminders: the sweep ─────────────────────────────────────────
// docs/ready-date-reminders.md. Run once every working day by the
// sendReadyDateReminders Lambda (src/handlers/send-ready-date-reminders.js).
//
// A supplier is emailed a link to the supplier portal every working day while
// they owe us a ready date, for either of two reasons:
//   - a week after staff confirm a PO line's final artwork, and once the PO's
//     deposit is paid or not required, until the line has an ESTIMATED ready
//     date;
//   - once a line's estimated ready date has passed, until it has an ACTUAL
//     ready date.
// ONE email per supplier per day covers all of their purchase orders and both
// reasons, each in its own section.
// The rules (which lines, the grouping, the email's block) are in
// src/lib/ready-date-reminders.js; this file reads, sends through Front and
// records.
//
// Each supplier's run of emails is one row of ready_date_reminders (a "chase")
// and one row on ShipLine's Email Receipts page: the first email is the
// email_receipts row, every later one a reminder under it
// (email_receipt_reminders). The chase closes, and the receipt shows
// confirmed, once none of the lines it asked about is waiting.
//
// Nothing is sent at a weekend. A supplier is claimed before their email goes
// (ready_date_reminders.last_sent_at, a 20 hour minimum gap), so a second run
// on the same day, or two runs at once, cannot email anyone twice.

const defaultLog = require('../lib/logger');
const R = require('../lib/ready-date-reminders');
const poPortalEmail = require('../lib/po-portal-email');
const { escapeHtml, wrapEmail } = require('../lib/email-layout');
const { DEFAULT_EMAIL_TEMPLATES, READY_DATE_REMINDER_TEMPLATE_KEY } = require('../lib/email-template-defaults');
const { createEmailReceipt, recordReminderSend } = require('../lib/email-receipt');
const { jportalSchema, isSourceMissing } = require('../lib/jportal');
const { sendFrontMessage: defaultSend } = require('./front-send');
const { loadDepositGate: defaultLoadGate } = require('./deposit-gate');
const { londonToday } = require('./daily-alerts');

const TABLE_MISSING = 1146;
const DUPLICATE_KEY = 1062;

const EMAIL_TYPE = 'ready_date_reminder';
const SEND_TABLE = 'ready_date_reminders';

const jsonArray = (value) => {
    if (Array.isArray(value)) return value;
    try { const parsed = JSON.parse(value); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
};

// The stored template (ops may have edited it), else the in-code default.
async function loadTemplate(conn, log) {
    const fallback = DEFAULT_EMAIL_TEMPLATES.find(t => t.key === READY_DATE_REMINDER_TEMPLATE_KEY);
    try {
        const [rows] = await conn.query(
            'SELECT subject, body_html FROM email_templates WHERE template_key = ? AND deleted_at IS NULL',
            [READY_DATE_REMINDER_TEMPLATE_KEY]
        );
        if (rows.length) return { subject: rows[0].subject, bodyHtml: rows[0].body_html };
    } catch (e) {
        log.warn('[ready-date-reminders] template lookup failed, using the in-code default', { error: e.message });
    }
    return { subject: fallback.subject, bodyHtml: fallback.bodyHtml };
}

// The lines whose date the supplier already sent through the portal, where it
// waits for staff to approve it: they have done their part and are not chased.
// `lines` carry their `kind`: an estimated date answers an 'estimated' line, an
// actual date an 'actual' one. A server with no portal schema has no such
// requests; any other failure is thrown, so nobody is chased blind.
async function answeredOnPortal(conn, lines) {
    if (!lines.length) return new Set();
    const kindById = new Map(lines.map(l => [Number(l.id), l.kind]));
    try {
        const [rows] = await conn.query(
            `SELECT order_id, requested_json FROM ${jportalSchema()}.po_change_requests
              WHERE status = 'pending' AND order_id IN (?)`,
            [[...kindById.keys()]]
        );
        return new Set(rows
            .filter(r => R.answersKind(r.requested_json, kindById.get(Number(r.order_id))))
            .map(r => Number(r.order_id)));
    } catch (e) {
        if (isSourceMissing(e)) return new Set();
        throw e;
    }
}

// Who to email for these purchase orders: the addresses each PO was last
// emailed to from ShipLine (they were pre-approved on the portal by that send,
// docs/po-portal-send.md), else the addresses of its latest artwork request.
// Duplicates across the supplier's POs go out once.
async function recipientsFor(conn, poIds) {
    const byPo = new Map();
    const [sends] = await conn.query(
        `SELECT d.purchase_order_id, s.sent_to
           FROM purchase_order_document_sends s
           JOIN purchase_order_documents d ON d.id = s.purchase_order_document_id
          WHERE d.purchase_order_id IN (?)
          ORDER BY s.id DESC`,
        [poIds]
    );
    for (const row of sends) {
        const id = Number(row.purchase_order_id);
        if (!byPo.has(id)) byPo.set(id, jsonArray(row.sent_to));
    }
    const without = poIds.filter(id => !(byPo.get(id) || []).length);
    if (without.length) {
        try {
            const [requests] = await conn.query(
                `SELECT purchase_order_id, sent_to FROM artwork_confirmation_requests
                  WHERE purchase_order_id IN (?) AND (revoke_reason IS NULL OR revoke_reason <> 'send_failed')
                  ORDER BY id DESC`,
                [without]
            );
            for (const row of requests) {
                const id = Number(row.purchase_order_id);
                if (!(byPo.get(id) || []).length) byPo.set(id, jsonArray(row.sent_to));
            }
        } catch (e) {
            if (e.errno !== TABLE_MISSING) throw e;
        }
    }
    const all = poIds.flatMap(id => byPo.get(id) || []).filter(a => typeof a === 'string');
    return poPortalEmail.parseRecipients(all).addresses;
}

async function readChase(conn, key, { tolerateMissing = false } = {}) {
    try {
        const [rows] = await conn.query(
            `SELECT id, order_ids, send_count, last_sent_at,
                    (last_sent_at > UTC_TIMESTAMP() - INTERVAL ${R.MIN_GAP_HOURS} HOUR) AS recent
               FROM ready_date_reminders
              WHERE supplier_key = ? AND closed_at IS NULL
              ORDER BY id DESC LIMIT 1`,
            [key]
        );
        return rows.length ? rows[0] : null;
    } catch (e) {
        if (tolerateMissing && e.errno === TABLE_MISSING) return null;
        throw e;
    }
}

// Takes the supplier for this run, so no other run emails them today: a new
// chase row (the unique open key refuses a second), or the open chase's
// last_sent_at moved forward under the minimum gap. → the claim, or null when
// the supplier was already emailed.
async function claim(conn, group) {
    const chase = await readChase(conn, group.key);
    if (!chase) {
        try {
            const [ins] = await conn.query(
                `INSERT INTO ready_date_reminders (supplier_key, supplier_name, purchase_order_ids, order_ids, last_sent_at)
                 VALUES (?, ?, JSON_ARRAY(), JSON_ARRAY(), UTC_TIMESTAMP())`,
                [group.key, group.supplierName]
            );
            return { id: Number(ins.insertId), isNew: true, sendCount: 0, orderIds: [], previous: null };
        } catch (e) {
            if (e.errno === DUPLICATE_KEY) return null;
            throw e;
        }
    }
    const [upd] = await conn.query(
        `UPDATE ready_date_reminders
            SET last_sent_at = UTC_TIMESTAMP()
          WHERE id = ? AND closed_at IS NULL
            AND last_sent_at <= UTC_TIMESTAMP() - INTERVAL ${R.MIN_GAP_HOURS} HOUR`,
        [chase.id]
    );
    if (!upd.affectedRows) return null;
    return {
        id: Number(chase.id), isNew: false, sendCount: Number(chase.send_count) || 0,
        orderIds: R.parseIds(chase.order_ids), previous: chase.last_sent_at,
    };
}

// The email did not go: give the supplier back, so the next run tries again.
async function release(conn, taken, log) {
    try {
        if (taken.isNew) await conn.query('DELETE FROM ready_date_reminders WHERE id = ? AND send_count = 0', [taken.id]);
        else await conn.query('UPDATE ready_date_reminders SET last_sent_at = ? WHERE id = ?', [taken.previous, taken.id]);
    } catch (e) {
        log.warn('[ready-date-reminders] a claim was not released; that supplier is skipped until tomorrow', { chaseId: taken.id, error: e.message });
    }
}

// The email went: the chase row, and the Email Receipts page (the first email
// is the receipt row, a later one a reminder under it).
async function record(conn, taken, { poIds, orderIds, addresses, subject, sent }) {
    await conn.query(
        `UPDATE ready_date_reminders
            SET purchase_order_ids = ?, order_ids = ?, sent_to = ?, subject = ?,
                send_count = send_count + 1, first_sent_at = COALESCE(first_sent_at, UTC_TIMESTAMP()),
                last_sent_at = UTC_TIMESTAMP(), front_message_uid = ?, front_conversation_id = ?
          WHERE id = ?`,
        [
            JSON.stringify(poIds), JSON.stringify([...new Set([...taken.orderIds, ...orderIds])]),
            JSON.stringify(addresses), subject, sent.frontMessageUid || null, sent.frontConversationId || null, taken.id,
        ]
    );
    const [[receipt]] = await conn.query(
        `SELECT id, reminder_count FROM email_receipts
          WHERE send_table = ? AND send_id = ? AND email_type = ?
          ORDER BY id DESC LIMIT 1`,
        [SEND_TABLE, taken.id, EMAIL_TYPE]
    );
    if (receipt) {
        await recordReminderSend(conn, receipt.id, {
            reminderNumber: Number(receipt.reminder_count || 0) + 1, sentTo: addresses, subject,
            frontMessageUid: sent.frontMessageUid, frontConversationId: sent.frontConversationId,
            sentByEmail: null, // automatic
        });
    } else {
        // No action_url: the generic 48h sweep (receipt-reminders.js) must
        // leave this row alone. This sweep is what repeats it.
        await createEmailReceipt(conn, {
            emailType: EMAIL_TYPE, sentTo: addresses, subject,
            confirmVia: 'portal', sendTable: SEND_TABLE, sendId: taken.id,
        });
    }
}

/**
 * One run. Options:
 *   today    'YYYY-MM-DD', the day it is (default: today in Europe/London)
 *   dryRun   read and report who would be emailed about what; send and write nothing
 *   force    run at a weekend too
 * → { today, lines: { estimated, actual }, suppliers, sent, failed, unrecorded,
 *     skipped: { answeredOnPortal, depositHeld, noRecipients, alreadySent },
 *     emails? (dry run), aborted? }
 */
async function runReadyDateReminders(pool, opts = {}) {
    const {
        today = londonToday(), dryRun = false, force = false,
        sendFrontMessage = defaultSend, loadDepositGate = defaultLoadGate,
        log = defaultLog, env = process.env,
    } = opts;
    const summary = {
        today, lines: { estimated: 0, actual: 0 }, suppliers: 0, sent: 0, failed: 0, unrecorded: 0,
        skipped: { answeredOnPortal: 0, depositHeld: 0, noRecipients: 0, alreadySent: 0 },
    };
    if (dryRun) summary.emails = [];

    if (R.isWeekend(today) && !force) return { ...summary, aborted: 'weekend' };
    const portalBase = poPortalEmail.portalBaseOf(env);
    if (!portalBase) {
        log.warn('[ready-date-reminders] SUPPLIER_PORTAL_URL unset: there is no link to send; skipping run');
        return { ...summary, aborted: 'no_portal_url' };
    }

    const conn = await pool.getConnection();
    try {
        // Chases whose lines all have their date (or have moved on) end here,
        // so a supplier who is chased again later starts a new one.
        if (!dryRun) {
            await conn.query(R.closeChasesSql());
            await conn.query(R.confirmReceiptsSql());
        }

        const [found] = await conn.query(
            `SELECT o.id, o.purchase_order_id, o.jf_code, o.product_name, o.status, o.estimated_ready_date,
                    po.po_number, po.supplier AS po_supplier
               FROM orders o
               JOIN purchase_orders po ON po.id = o.purchase_order_id AND po.deleted_at IS NULL
              WHERE ${R.waitingSql('o', '?')}
              ORDER BY o.purchase_order_id ASC, o.jf_code ASC, o.id ASC`,
            Array(R.WAITING_TODAYS).fill(today)
        );
        const rows = found.map(r => ({ ...r, kind: R.kindOf(r) }));
        const answered = await answeredOnPortal(conn, rows);
        summary.skipped.answeredOnPortal = answered.size;
        const waiting = rows.filter(r => !answered.has(Number(r.id)));
        for (const kind of R.KINDS) summary.lines[kind] = waiting.filter(r => r.kind === kind).length;
        if (!waiting.length) return summary;

        const tpl = await loadTemplate(conn, log);
        const groups = R.groupBySupplier(waiting);
        summary.suppliers = groups.length;

        for (const group of groups) {
            // The deposit gate, per purchase order, on the lines that need an
            // ESTIMATED date: they are asked for only when the deposit is paid
            // or none is required. A gate that cannot be read holds them, as an
            // unpaid deposit does. Lines whose estimated date has passed are
            // asked for their actual date whatever the deposit.
            const pos = [];
            for (const po of group.pos) {
                let lines = po.lines;
                if (lines.some(l => l.kind === 'estimated')) {
                    let clear = false;
                    try {
                        clear = (await loadDepositGate(conn, { id: po.id, supplier: po.supplier })).clear === true;
                    } catch (e) {
                        log.warn('[ready-date-reminders] deposit gate could not be read; the PO is held', { purchaseOrderId: po.id, error: e.message });
                    }
                    if (!clear) {
                        summary.skipped.depositHeld++;
                        lines = lines.filter(l => l.kind !== 'estimated');
                    }
                }
                if (lines.length) pos.push({ ...po, lines });
            }
            if (!pos.length) continue;

            const poIds = pos.map(p => p.id);
            const orderIds = pos.flatMap(p => p.lines.map(l => Number(l.id)));
            const addresses = await recipientsFor(conn, poIds);
            if (!addresses.length) {
                summary.skipped.noRecipients++;
                log.warn('[ready-date-reminders] nobody to email: the PO was never emailed from ShipLine', { supplier: group.supplierName, purchaseOrderIds: poIds });
                continue;
            }

            let taken = null;
            if (dryRun) {
                const chase = await readChase(conn, group.key, { tolerateMissing: true });
                if (chase && Number(chase.recent) === 1) { summary.skipped.alreadySent++; continue; }
                taken = { sendCount: chase ? Number(chase.send_count) || 0 : 0 };
            } else {
                taken = await claim(conn, group);
                if (!taken) { summary.skipped.alreadySent++; continue; }
            }

            const vars = {
                supplier: group.supplierName || '',
                poNumbers: pos.map(p => p.poNumber).join(', '),
                count: String(orderIds.length),
            };
            const subject = (R.fill(tpl.subject, vars) || `Ready dates needed – ${vars.poNumbers}`)
                .replace(/[\r\n]+/g, ' ').slice(0, 500);
            const escaped = Object.fromEntries(Object.entries(vars).map(([k, v]) => [k, escapeHtml(v)]));
            const html = wrapEmail({
                title: subject,
                bodyHtml: R.fill(tpl.bodyHtml, escaped) + R.reminderBlockHtml({
                    pos, portalBase, registerUrl: poPortalEmail.registerUrl(portalBase), reminderNumber: taken.sendCount,
                }),
            });

            if (dryRun) {
                summary.emails.push({
                    supplier: group.supplierName, to: addresses, subject, emailNumber: taken.sendCount + 1,
                    purchaseOrders: pos.map(p => ({
                        id: p.id, poNumber: p.poNumber,
                        ...Object.fromEntries(R.KINDS.map(kind => [kind, p.lines.filter(l => l.kind === kind).map(l => l.jf_code || `line ${l.id}`)])),
                    })),
                });
                continue;
            }

            const sent = await sendFrontMessage({ to: addresses, subject, html });
            if (!sent.ok) {
                await release(conn, taken, log);
                summary.failed++;
                log.error('[ready-date-reminders] send failed', { supplier: group.supplierName, reason: sent.reason, status: sent.status });
                // A configuration problem affects every supplier: stop.
                if (sent.reason === 'front_not_configured') break;
                continue;
            }
            summary.sent++;
            try {
                await record(conn, taken, { poIds, orderIds, addresses, subject, sent });
            } catch (e) {
                // The email is out and the claim stands, so nobody is emailed
                // twice today; only the Email Receipts page may be missing it.
                summary.unrecorded++;
                log.error('[ready-date-reminders] a sent email was not recorded', { supplier: group.supplierName, chaseId: taken.id, error: e.message });
            }
        }
        log.info('[ready-date-reminders] sweep complete', { ...summary, emails: undefined });
        return summary;
    } finally {
        conn.release();
    }
}

module.exports = { runReadyDateReminders, EMAIL_TYPE, SEND_TABLE };
