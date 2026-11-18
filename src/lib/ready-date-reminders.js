'use strict';

// Ready-date reminders (docs/ready-date-reminders.md). The pure rules only, no
// database and no clock: which lines are waiting for a ready date, how they are
// grouped into one email per supplier, and the fixed block under the template
// body. The sweep that reads, sends and records is
// src/services/ready-date-reminders.js.
//
// A line is in the supplier's daily email for one of two reasons (its `kind`):
//
//   'estimated'  it has no estimated ready date, staff confirmed its final
//                artwork (orders.artwork_confirmed_date, the date "by us", not
//                the supplier's own tick) at least WAIT_DAYS days ago, and its
//                purchase order's deposit is paid or none is required (the
//                deposit gate of src/services/deposit-gate.js, read by the
//                sweep);
//   'actual'     its estimated ready date has passed and it has no actual
//                ready date. Nothing else is asked of it: no artwork date, no
//                deposit.
//
// Either way the line has no actual ready date, its status is one of
// CHASED_STATUSES, it is not an _FQC sample line (a sample follows its
// product), and the supplier has not already sent that date through the
// portal, where it waits for staff to approve it (read by the sweep).

const { FONT, INK, MUTED, BORDER, CODE_BG, BLUE, escapeHtml, button, linkFallback, small } = require('./email-layout');

/** Days after staff's Artwork Confirmed date before the estimated date is asked for. */
const WAIT_DAYS = 7;

/** At most one email per supplier in this many hours (the sweep runs once a working day). */
const MIN_GAP_HOURS = 20;

// ShipLine's board columns 01 to 06: Scheduled, Purchase Order Sent, Under
// Production, Ready for QC, Ready at Factory, Consolidated. A line further on
// has left the factory and is never chased.
const CHASED_STATUSES = ['SCHEDULED', 'PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY', 'CONSOLIDATED'];

const KINDS = ['estimated', 'actual'];

/**
 * The condition "this orders row is waiting for a ready date", as SQL over
 * `alias`. `today` is the SQL for today's date: CURDATE(), or a `?` the caller
 * binds; it appears WAITING_TODAYS times. The deposit gate and what the
 * supplier already sent through the portal are not SQL here: the sweep reads
 * them.
 */
function waitingSql(alias = 'o', today = 'CURDATE()') {
    const a = alias;
    return `${a}.deleted_at IS NULL
        AND ${a}.actual_ready_date IS NULL
        AND ${a}.status IN (${CHASED_STATUSES.map(s => `'${s}'`).join(', ')})
        AND COALESCE(${a}.jf_code, '') NOT LIKE '%\\_FQC'
        AND COALESCE(${a}.asin, '') NOT LIKE '%\\_FQC'
        AND ((${a}.estimated_ready_date IS NULL
              AND ${a}.artwork_confirmed_date IS NOT NULL
              AND ${a}.artwork_confirmed_date <= ${today} - INTERVAL ${WAIT_DAYS} DAY)
          OR (${a}.estimated_ready_date IS NOT NULL
              AND ${a}.estimated_ready_date < ${today}))`;
}

/** How many times `today` appears in waitingSql (the number of `?` to bind). */
const WAITING_TODAYS = 2;

/** Which date a waiting row is asked for: no estimated date yet, or one that has passed. */
const kindOf = row => (row && row.estimated_ready_date ? 'actual' : 'estimated');

const NOTHING_WAITING = `NOT EXISTS (SELECT 1 FROM orders o
                          WHERE JSON_CONTAINS(c.order_ids, CAST(o.id AS JSON))
                            AND ${waitingSql('o')})`;

/** Closes every open chase none of whose lines is waiting any more. */
const closeChasesSql = () => `
    UPDATE ready_date_reminders c
       SET c.closed_at = UTC_TIMESTAMP()
     WHERE c.closed_at IS NULL AND ${NOTHING_WAITING}`;

/**
 * Marks a chase's email confirmed on the Email Receipts page once the chase is
 * over, closing the chase if it was still open. One of the steps of
 * syncPortalReceipts (src/lib/email-receipt.js), so the page is right without
 * waiting for the next sweep.
 */
const confirmReceiptsSql = () => `
    UPDATE email_receipts r
      JOIN ready_date_reminders c ON c.id = r.send_id
       SET c.closed_at = COALESCE(c.closed_at, UTC_TIMESTAMP()),
           r.confirmed_at = NOW()
     WHERE r.confirm_via = 'portal' AND r.confirmed_at IS NULL
       AND r.send_table = 'ready_date_reminders'
       AND (c.closed_at IS NOT NULL OR ${NOTHING_WAITING})`;

/** Saturday or Sunday. `ymd` is 'YYYY-MM-DD'. */
function isWeekend(ymd) {
    const day = new Date(`${String(ymd).slice(0, 10)}T00:00:00Z`).getUTCDay();
    return day === 0 || day === 6;
}

/**
 * One key per supplier company: the name as letters and digits only, lower
 * case, so "Suzhou Sunmed Co., Ltd" and "Suzhou Sunmed Co.,Ltd." are one
 * supplier and get one email. '' when there is no name.
 */
const supplierKey = name => String(name == null ? '' : name).normalize('NFKC').toLowerCase().replace(/[^\p{L}\p{N}]/gu, '');

/**
 * The waiting lines grouped into one email per supplier, each with its
 * purchase orders in PO-number order. `rows` = [{ id, purchase_order_id,
 * po_number, po_supplier, jf_code, product_name, estimated_ready_date }]; each
 * line comes back with its `kind`. A PO with no supplier name is a group of
 * its own.
 * → [{ key, supplierName, pos: [{ id, poNumber, supplier, lines: [row + kind] }] }]
 */
function groupBySupplier(rows) {
    const groups = new Map();
    for (const row of rows || []) {
        const poId = Number(row.purchase_order_id);
        const key = (supplierKey(row.po_supplier) || `po:${poId}`).slice(0, 255);
        if (!groups.has(key)) groups.set(key, { key, supplierName: String(row.po_supplier || '').trim() || null, pos: new Map() });
        const group = groups.get(key);
        if (!group.pos.has(poId)) group.pos.set(poId, { id: poId, poNumber: row.po_number || `#${poId}`, supplier: row.po_supplier || null, lines: [] });
        group.pos.get(poId).lines.push({ ...row, kind: row.kind || kindOf(row) });
    }
    return [...groups.values()].map(g => ({
        ...g,
        pos: [...g.pos.values()].sort((a, b) => String(a.poNumber).localeCompare(String(b.poNumber)) || a.id - b.id),
    }));
}

/** A JSON column holding ids (parsed by the driver, or still text) as a clean number[]. */
function parseIds(value) {
    let list = value;
    if (typeof value === 'string') {
        try { list = JSON.parse(value); } catch { list = []; }
    }
    if (!Array.isArray(list)) return [];
    return [...new Set(list.map(Number).filter(n => Number.isSafeInteger(n) && n > 0))];
}

/**
 * Whether a pending portal change request (its requested_json) already carries
 * the date a line of this `kind` is asked for: the supplier has done their
 * part, and staff have still to approve it.
 */
function answersKind(requestedJson, kind) {
    let v = requestedJson;
    if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch { return false; }
    }
    if (!v || typeof v !== 'object') return false;
    const date = kind === 'actual' ? v.actualReadyDate : v.estimatedReadyDate;
    return typeof date === 'string' && date !== '';
}

/** The order's page on the portal, opened on its lines (where the dates are entered). */
const orderUrl = (base, purchaseOrderId) => `${base}/orders/${encodeURIComponent(purchaseOrderId)}`;

/** The portal's list of orders. */
const ordersUrl = base => `${base}/orders`;

/** Replace {token} placeholders; an unknown token is left as typed (as orders.js renderTemplate). */
const fill = (str, vars) => (str == null ? str : String(str).replace(/\{(\w+)\}/g, (m, k) => (vars[k] != null ? String(vars[k]) : m)));

const ymd = v => (v instanceof Date ? v.toISOString().slice(0, 10) : String(v || '').slice(0, 10));

const SECTIONS = {
    estimated: {
        title: '<strong>Estimated ready date needed</strong><br>请填写预计完成日期',
        lead: 'The final artwork for these items was confirmed more than a week ago. Please enter the estimated ready date for each one. / 以下产品的最终图稿已确认超过一周，请为每个产品填写预计完成日期。',
    },
    actual: {
        title: '<strong>Actual ready date needed</strong><br>请填写实际完成日期',
        lead: 'The estimated ready date of these items has passed. If they are ready, please enter the actual ready date; if not, please update the estimated ready date. / 以下产品的预计完成日期已过。如已完成，请填写实际完成日期；如尚未完成，请更新预计完成日期。',
    },
};

// One section: its heading, then each purchase order that has lines of that
// kind, with a link to its page and those lines. '' when no line is of the kind.
function sectionHtml(kind, pos, portalBase) {
    const withLines = (pos || []).map(po => ({ po, lines: po.lines.filter(l => l.kind === kind) })).filter(x => x.lines.length);
    if (!withLines.length) return '';
    const s = SECTIONS[kind];
    return [
        `<p style="margin:0 0 6px;font-family:${FONT};font-size:15px;line-height:1.6;color:${INK};">${s.title}</p>`,
        small(s.lead),
        ...withLines.map(({ po, lines }) => {
            const url = escapeHtml(orderUrl(portalBase, po.id));
            const items = lines.map((l) => {
                const label = [l.jf_code, l.product_name].filter(Boolean).join(' · ') || `Line ${l.id}`;
                const was = kind === 'actual' ? ` <span style="color:${MUTED};">· estimated / 预计 ${escapeHtml(ymd(l.estimated_ready_date))}</span>` : '';
                return `<li style="margin:0 0 4px;">${escapeHtml(label)}${was}</li>`;
            });
            return `<p style="margin:0 0 6px;font-family:${FONT};font-size:14px;line-height:1.5;color:${INK};"><strong>${escapeHtml(po.poNumber)}</strong> · <a href="${url}" style="color:${BLUE};">open on the portal / 在门户中打开</a></p>`
                + `<ul style="margin:0 0 14px;padding-left:20px;font-family:${FONT};font-size:14px;line-height:1.5;color:${INK};">${items.join('')}</ul>`;
        }),
    ].join('');
}

/**
 * The fixed block under the template body: the items that need an estimated
 * ready date, then the items whose estimated date has passed and need the
 * actual one (each section only when it has items), by purchase order with a
 * link to its page; then the button to the portal, how to enter the dates and
 * how to get an account. English and Chinese, as the portal is.
 * `pos` = [{ id, poNumber, lines: [{ kind, … }] }]; `reminderNumber` is how
 * many emails of this chase went before this one (0 = the first).
 */
function reminderBlockHtml({ pos, portalBase, registerUrl, reminderNumber = 0 }) {
    const n = Number(reminderNumber) > 0 ? Math.floor(Number(reminderNumber)) : 0;
    const list = pos || [];
    const target = list.length === 1 ? orderUrl(portalBase, list[0].id) : ordersUrl(portalBase);
    const reg = escapeHtml(registerUrl);
    const regLink = `<a href="${reg}" style="color:${BLUE};word-break:break-all;">${reg}</a>`;
    return [
        `<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:18px 0;"><tr><td style="padding:16px 18px 6px;border:1px solid ${BORDER};border-radius:8px;background:${CODE_BG};">`,
        n ? `<p style="margin:0 0 8px;font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">Reminder #${n} · 第 ${n} 次提醒</p>` : '',
        ...KINDS.map(kind => sectionHtml(kind, list, portalBase)),
        button(target, 'Go to the supplier portal / 前往供应商门户'),
        linkFallback(target),
        small('Sign in, open the order, enter the date for each item and send your changes. Our team approve them, and these reminders stop once every item above has its date.'),
        small('登录后打开订单，为每个产品填写日期并提交。我们的团队审核通过后，以上产品均已填写日期时，将不再发送此提醒。'),
        small(`<strong style="color:${INK};">New to the portal?</strong> Register at ${regLink} with the email address this message was sent to. / <strong style="color:${INK};">首次使用门户？</strong>请使用接收本邮件的邮箱地址在 ${regLink} 注册。`),
        `<p style="margin:0 0 10px;font-family:${FONT};font-size:12px;line-height:1.5;color:${MUTED};">This reminder is sent every working day until the dates are entered. / 在填写日期之前，我们将在每个工作日发送此提醒。</p>`,
        '</td></tr></table>',
    ].join('');
}

module.exports = {
    WAIT_DAYS, MIN_GAP_HOURS, CHASED_STATUSES, KINDS,
    waitingSql, WAITING_TODAYS, kindOf, closeChasesSql, confirmReceiptsSql,
    isWeekend, supplierKey, groupBySupplier, parseIds, answersKind,
    orderUrl, ordersUrl, fill, reminderBlockHtml,
};
