'use strict';

// ── QC visit daily alerts (pure) ────────────────────────────────────────────
//
// buildQcVisitAlerts turns the open QC visits, their lines and the candidate
// orders into daily_alerts rows of type 'qc_visit'. No DB and no clock: the
// loader in src/services/daily-alerts.js reads the rows and passes `today`
// (the China calendar day, chinaToday()), so this is fully unit-tested by
// tools/test-qc-alerts-lib.js.
//
// Kinds (meta.kind) and their dedup keys (`qc_visit:<kind>:<entityId>`):
//   unconfirmed      PROPOSED, proposed more than 3 days ago        today → overdue after 7 days
//   due_soon         CONFIRMED, visit date today … today+7           today
//   completion_due   CONFIRMED, visit date before today              overdue
//   po_shrank        open visit with a line whose PO quantity fell
//                    below its snapshot or below the qty to inspect  today
//   line_left_stage  open visit with a line whose order is no longer
//                    IN_PRODUCTION / READY_FOR_QC (or was deleted)   today
//   unvisited_ready  an order in READY_FOR_QC on no open visit       today   (entityType 'order')
// Every visit kind uses entityType 'qc_visit', entityId String(visitId).
// COMPLETED / CANCELLED visits never alert; their lines do not count as "on a
// visit" for unvisited_ready.
//
// Input rows are the raw DB rows (snake_case) the loader selects:
//   visits: { id, ref, status, supplier_name, factory_label, factory_city, inspector_name,
//             visit_date 'YYYY-MM-DD'|null, window_start 'HH:MM[:SS]'|null, window_end,
//             proposed_at Date|string|null }
//   lines:  { visit_id, order_id, quantity_to_inspect, po_quantity_snapshot, sample_method }
//   orders: { id, jf_code, product_name, po_number, purchase_order_id, supplier, status,
//             quantity, deleted_at|deleted }   — every order a line points at, plus every
//             order in a candidate status (for unvisited_ready)

const {
    OPEN_STATUSES,
    CANDIDATE_ORDER_STATUSES,
    chinaToday,
    normJfCode,
    isFqcCode,
    formatRef,
} = require('./qc-visits');

const TYPE = 'qc_visit';
const UNCONFIRMED_AFTER_DAYS = 3;
const UNCONFIRMED_OVERDUE_AFTER_DAYS = 7;
const DUE_SOON_DAYS = 7;
const READY_STATUS = 'READY_FOR_QC';

// ── Small date helpers (calendar days, no time zone involved) ────────────────
function ymd(val) {
    if (!val) return null;
    if (typeof val === 'string') return /^\d{4}-\d{2}-\d{2}/.test(val) ? val.slice(0, 10) : null;
    if (val instanceof Date && !Number.isNaN(val.getTime())) return val.toISOString().slice(0, 10);
    return null;
}

function dayNumber(d) {
    return Math.floor(Date.parse(`${d}T00:00:00Z`) / 86400000);
}

function addDays(d, n) {
    return new Date((dayNumber(d) + n) * 86400000).toISOString().slice(0, 10);
}

// Whole calendar days from `a` to `b` (b - a).
function daysBetween(a, b) {
    return dayNumber(b) - dayNumber(a);
}

// The China calendar day of a timestamp (proposed_at is a UTC TIMESTAMP).
function chinaDayOf(ts) {
    if (!ts) return null;
    const d = ts instanceof Date ? ts : new Date(typeof ts === 'string' && !/[zZ]|[+-]\d\d:?\d\d$/.test(ts) ? `${ts.replace(' ', 'T')}Z` : ts);
    if (Number.isNaN(d.getTime())) return null;
    return chinaToday(d);
}

function hm(val) {
    return typeof val === 'string' && /^\d{2}:\d{2}/.test(val) ? val.slice(0, 5) : null;
}

// daily_alerts.title is VARCHAR(255) and body VARCHAR(1000); MySQL's strict
// mode rejects a longer value and the whole nightly run would fail. Line
// lists (po_shrank, line_left_stage) and product names are unbounded.
const TITLE_MAX = 255;
const BODY_MAX = 1000;
function clip(str, max) {
    if (str == null) return null;
    const t = String(str);
    return t.length > max ? `${t.slice(0, max - 1)}…` : t;
}

function plural(n, word) {
    return `${n} ${word}${n === 1 ? '' : 's'}`;
}

function sku(o, orderId) {
    if (!o) return `order ${orderId}`;
    return o.jf_code || o.product_name || `order ${o.id}`;
}

function isDeleted(o) {
    return !o || !!o.deleted_at || o.deleted === true || o.deleted === 1;
}

function visitRef(v) {
    return v.ref || formatRef(v.id);
}

function whereText(v) {
    return [v.supplier_name, v.factory_label, v.factory_city].filter(Boolean).join(' · ');
}

function whenText(v) {
    const d = ymd(v.visit_date);
    if (!d) return null;
    const s = hm(v.window_start);
    const e = hm(v.window_end);
    return s ? `${d} ${s}${e ? `–${e}` : ''} China time` : d;
}

function visitAlert(v, kind, { severity, eventDate, title, body, orderIds }) {
    const entityId = String(v.id);
    const meta = { kind, visitId: v.id, ref: visitRef(v), visitDate: ymd(v.visit_date) };
    if (orderIds) meta.orderIds = orderIds;
    return {
        dedupKey: `${TYPE}:${kind}:${entityId}`,
        type: TYPE,
        severity,
        eventDate,
        title: clip(title, TITLE_MAX),
        body: clip(body || null, BODY_MAX),
        entityType: 'qc_visit',
        entityId,
        meta,
    };
}

function buildQcVisitAlerts({ visits = [], lines = [], orders = [], today } = {}) {
    const day = ymd(today) || chinaToday();
    const open = new Set(OPEN_STATUSES);
    const candidate = new Set(CANDIDATE_ORDER_STATUSES);
    const ordersById = new Map(orders.map((o) => [Number(o.id), o]));

    const openVisits = visits.filter((v) => open.has(v.status));
    const openIds = new Set(openVisits.map((v) => Number(v.id)));
    const linesByVisit = new Map();
    const ordersOnOpenVisits = new Set();
    for (const l of lines) {
        const vid = Number(l.visit_id);
        if (!openIds.has(vid)) continue;
        if (!linesByVisit.has(vid)) linesByVisit.set(vid, []);
        linesByVisit.get(vid).push(l);
        ordersOnOpenVisits.add(Number(l.order_id));
    }

    const alerts = [];
    const sorted = [...openVisits].sort((a, b) => Number(a.id) - Number(b.id));
    for (const v of sorted) {
        const ref = visitRef(v);
        const visitDate = ymd(v.visit_date);
        const eventDate = visitDate || day;

        // unconfirmed — proposed and still waiting on a confirmation.
        if (v.status === 'PROPOSED') {
            const proposedDay = chinaDayOf(v.proposed_at);
            const age = proposedDay ? daysBetween(proposedDay, day) : null;
            if (age != null && age > UNCONFIRMED_AFTER_DAYS) {
                const waiting = [];
                if (!v.factory_confirmed_at) waiting.push('factory');
                if (!v.inspector_confirmed_at) waiting.push('inspector');
                alerts.push(visitAlert(v, 'unconfirmed', {
                    severity: age > UNCONFIRMED_OVERDUE_AFTER_DAYS ? 'overdue' : 'today',
                    eventDate,
                    title: `QC visit ${ref} unconfirmed for ${plural(age, 'day')}`,
                    body: [whereText(v), whenText(v), waiting.length ? `waiting on ${waiting.join(' and ')}` : null]
                        .filter(Boolean).join(' · '),
                }));
            }
        }

        if (v.status === 'CONFIRMED' && visitDate) {
            const until = daysBetween(day, visitDate);
            if (until >= 0 && until <= DUE_SOON_DAYS) {
                alerts.push(visitAlert(v, 'due_soon', {
                    severity: 'today',
                    eventDate: visitDate,
                    title: until === 0 ? `QC visit ${ref} today` : `QC visit ${ref} in ${plural(until, 'day')}`,
                    body: [whereText(v), whenText(v), v.inspector_name ? `inspector ${v.inspector_name}` : null]
                        .filter(Boolean).join(' · '),
                }));
            } else if (until < 0) {
                alerts.push(visitAlert(v, 'completion_due', {
                    severity: 'overdue',
                    eventDate: visitDate,
                    title: `QC visit ${ref} took place ${visitDate} — mark it completed`,
                    body: [whereText(v), v.inspector_name ? `inspector ${v.inspector_name}` : null]
                        .filter(Boolean).join(' · '),
                }));
            }
        }

        const vLines = linesByVisit.get(Number(v.id)) || [];

        // po_shrank — the PO was reduced after the line was booked.
        const shrank = [];
        for (const l of vLines) {
            const o = ordersById.get(Number(l.order_id));
            if (isDeleted(o)) continue; // reported by line_left_stage
            const qty = Number(o.quantity || 0);
            const snap = l.po_quantity_snapshot == null ? null : Number(l.po_quantity_snapshot);
            const toInspect = Number(l.quantity_to_inspect || 0);
            if ((snap != null && qty < snap) || qty < toInspect) {
                shrank.push({ orderId: Number(l.order_id), text: `${sku(o, l.order_id)} ${snap != null ? `${snap}→` : ''}${qty}` });
            }
        }
        if (shrank.length) {
            alerts.push(visitAlert(v, 'po_shrank', {
                severity: 'today',
                eventDate,
                title: `QC visit ${ref}: PO quantity reduced on ${plural(shrank.length, 'line')}`,
                body: [shrank.map((s) => s.text).join(', '), whereText(v)].filter(Boolean).join(' · '),
                orderIds: shrank.map((s) => s.orderId).sort((a, b) => a - b),
            }));
        }

        // line_left_stage — the order moved on (or was deleted) while booked.
        const left = [];
        for (const l of vLines) {
            const o = ordersById.get(Number(l.order_id));
            if (isDeleted(o)) {
                left.push({ orderId: Number(l.order_id), text: `${sku(o, l.order_id)} deleted` });
                continue;
            }
            if (candidate.has(o.status)) continue;
            // A destructive (_FQC) sample that has been destroyed is expected.
            if (l.sample_method === 'destructive' && o.status === 'DESTROYED') continue;
            left.push({ orderId: Number(l.order_id), text: `${sku(o, l.order_id)} ${o.status}` });
        }
        if (left.length) {
            alerts.push(visitAlert(v, 'line_left_stage', {
                severity: 'today',
                eventDate,
                title: `QC visit ${ref}: ${plural(left.length, 'line')} no longer in production or ready for QC`,
                body: [left.map((s) => s.text).join(', '), whereText(v)].filter(Boolean).join(' · '),
                orderIds: left.map((s) => s.orderId).sort((a, b) => a - b),
            }));
        }
    }

    // unvisited_ready — ready for QC and on no open visit. An _FQC sample whose
    // parent line (same PO, same base JF code) is itself a live candidate is
    // skipped: it rides with the parent, so the parent's alert covers it.
    const parentKeys = new Set();
    for (const o of orders) {
        if (isDeleted(o) || !candidate.has(o.status) || !o.jf_code || isFqcCode(o.jf_code)) continue;
        parentKeys.add(`${o.purchase_order_id ?? o.po_number ?? ''}|${normJfCode(o.jf_code)}`);
    }
    const ready = orders
        .filter((o) => !isDeleted(o) && o.status === READY_STATUS && !ordersOnOpenVisits.has(Number(o.id)))
        .filter((o) => !(o.jf_code && isFqcCode(o.jf_code)
            && parentKeys.has(`${o.purchase_order_id ?? o.po_number ?? ''}|${normJfCode(o.jf_code)}`)))
        .sort((a, b) => Number(a.id) - Number(b.id));
    for (const o of ready) {
        const entityId = String(o.id);
        alerts.push({
            dedupKey: `${TYPE}:unvisited_ready:${entityId}`,
            type: TYPE,
            severity: 'today',
            eventDate: day,
            title: clip(`Ready for QC, no visit arranged: ${sku(o, o.id)}`, TITLE_MAX),
            body: clip([o.supplier, o.po_number, o.quantity != null ? `${Number(o.quantity)} units` : null]
                .filter(Boolean).join(' · '), BODY_MAX),
            entityType: 'order',
            entityId,
            meta: { kind: 'unvisited_ready', orderId: Number(o.id) },
        });
    }

    return alerts;
}

module.exports = {
    buildQcVisitAlerts,
    UNCONFIRMED_AFTER_DAYS,
    UNCONFIRMED_OVERDUE_AFTER_DAYS,
    DUE_SOON_DAYS,
    // exported for tests
    addDays,
    daysBetween,
};
