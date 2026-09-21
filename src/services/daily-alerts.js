'use strict';

// ── Daily alerts service ────────────────────────────────────────────────────
//
// Backs the slide-out "alert window" in Shipsline. A nightly Lambda
// (src/handlers/generate-daily-alerts.js) calls generateDailyAlerts() to
// aggregate the day's key shipping events into the `daily_alerts` table; the
// authed API (src/handlers/orders.js) exposes the list/history/action routes.
// Kept out of the 7000-line orders.js handler so the nightly job reuses the
// exact same generation + dedup logic without pulling in the Express app.
//
// Event types (generated nightly):
//   • container_eta      — an order's ETA (orders.eta), grouped by container
//   • missing_data       — an in-transit order missing LOT / MFG / EXP
//   • arrived_unreceived — arrived at warehouse, units not booked into Mintsoft
//   • shipment_late      — delivery slot passed or carrier ETA slipped ≥ 2 days
// Retired (kept only so old rows auto-resolve): supplier_ready_date, qc_pending.
//
// LIFECYCLE (the important part). Each logical condition maps to ONE alert via
// a stable, DATE-FREE dedup_key. The nightly job upserts every condition that
// is currently true and refreshes its display fields in place — so a shifting
// ETA or an estimated→actual ready date updates the same alert instead of
// spawning a duplicate. A user can:
//   • snooze it N days  → hidden until then, then re-surfaces IF still true
//   • dismiss it        → "never remind", permanent
//   • restore it        → undo
// And it AUTO-RESOLVES: any alert whose condition is no longer present in a
// nightly run (order left its factory stage, ETA cleared, goods received, or
// the ready date was pushed into the future) is marked resolved and stops
// nagging — "remind me until the order is updated" with no extra clicks. There
// is deliberately NO lookback floor: a genuinely-stuck order keeps re-nagging
// indefinitely until its state actually changes.

const { formatDate } = require('../lib/order-shape');
const { recordAudit } = require('../lib/audit');
const log = require('./../lib/logger');

const ALERT_STATUSES = new Set(['pending', 'snoozed', 'dismissed', 'resolved', 'all']);
const ACTIONS = new Set(['snooze', 'dismiss', 'restore']);
const DEFAULT_SNOOZE_DAYS = 2;

// Real order statuses observed in the data (the canonical pipeline):
//   SCHEDULED · PO_SENT · IN_PRODUCTION · READY_FOR_QC · READY ·
//   CONSOLIDATED · ON_SEA · ON_AIR · ARRIVED_AT_WAREHOUSE · RECEIVED · DESTROYED
// (Legacy names IN_WAREHOUSE / MINTSOFT / PARTIALLY_RECEIVED don't occur but are
// kept in exclusion lists for forward-compat.)

// Container-ETA condition is "live" until the goods reach the warehouse / are
// received. Exclude the arrived/terminal statuses (an order here is no longer
// "arriving").
const ETA_EXCLUDED_STATUSES = ['ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'DESTROYED', 'IN_WAREHOUSE', 'MINTSOFT', 'PARTIALLY_RECEIVED'];

// In-transit stages used by the missing-data and shipment-late builders.
const IN_TRANSIT_STATUSES = ['CONSOLIDATED', 'ON_SEA', 'ON_AIR'];

// Alert types the nightly generator ACTIVELY produces this run — recomputed from
// the DB each run, so the global auto-resolve sweep ("not seen this run ->
// resolve") applies to them. Keep this list in sync with the builders in
// generateDailyAlerts.
const GENERATED_TYPES = [
    'container_eta', 'missing_data', 'arrived_unreceived', 'shipment_late',
];

// Retired types: no longer generated, but STILL owned by the auto-resolve sweep
// so any existing pending rows resolve (disappear) on the next nightly run.
//   • supplier_ready_date — an order sat at its ready date while still in a
//     factory stage
//   • qc_pending          — an order sat in READY_FOR_QC awaiting a QC pass
// Removed 2026-07-14 as low-value nags: an order legitimately sits at "ready" /
// "ready for QC" for a long time, so the alert kept re-surfacing after every
// snooze and was never useful to action.
const RETIRED_TYPES = ['supplier_ready_date', 'qc_pending'];

// Every type the nightly auto-resolve sweep owns (generated + retired). Types
// written by OTHER producers (e.g. 'status_suggestion' from the Front status
// importer, src/services/front-status-import.js) are NOT recomputed nightly and
// MUST be excluded from that sweep, or they'd be wiped every night.
const MANAGED_TYPES = [...GENERATED_TYPES, ...RETIRED_TYPES];

// Outstanding (un-received) qty for an order, as a SQL fragment.
const OUTSTANDING_SQL = `(orders.quantity - COALESCE((SELECT SUM(r.quantity) FROM order_receipts r WHERE r.order_id = orders.id AND r.type IN ('received','not_received')), 0))`;

function severityFor(date, today) {
    return date && date < today ? 'overdue' : 'today';
}

// ── Date helpers (Europe/London) ────────────────────────────────────────────
// "Today" is the UK business day, not the UTC/host day. en-CA formats as
// YYYY-MM-DD, which compares correctly against the DATE columns (mysql2 returns
// those as YYYY-MM-DD strings too).
function londonToday() {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/London' }).format(new Date());
}

function toDateStr(val) {
    if (!val) return null;
    return typeof val === 'string' ? val.slice(0, 10) : formatDate(val);
}

function toIso(val) {
    if (!val) return null;
    return val.toISOString?.() ?? val;
}

function parseJson(raw) {
    if (raw == null) return null;
    if (typeof raw === 'string') {
        try { return JSON.parse(raw); } catch { return null; }
    }
    return raw;
}

function clampLimit(limit, fallback = 200) {
    const n = parseInt(limit, 10);
    if (!Number.isFinite(n)) return fallback;
    return Math.min(Math.max(n, 1), 1000);
}

// SELECT with a computed is_snoozed flag (evaluated in SQL against the DB clock,
// so the snooze boundary never depends on the Node host's timezone). All reads
// go through this so rowToAlert always has is_snoozed.
const ALERT_SELECT = `
    SELECT da.*,
           (da.snoozed_until IS NOT NULL AND da.snoozed_until > NOW()) AS is_snoozed
      FROM daily_alerts da
`;

function rowToAlert(row) {
    const status = row.dismissed_at ? 'dismissed'
        : row.resolved_at ? 'resolved'
            : (row.is_snoozed ? 'snoozed' : 'pending');
    return {
        id: row.id,
        type: row.type,
        severity: row.severity,
        status,
        eventDate: toDateStr(row.event_date),
        title: row.title,
        body: row.body || null,
        entityType: row.entity_type || null,
        entityId: row.entity_id || null,
        meta: parseJson(row.meta),
        snoozedUntil: toIso(row.snoozed_until),
        dismissedAt: toIso(row.dismissed_at),
        resolvedAt: toIso(row.resolved_at),
        lastAction: row.last_action || null,
        actionNote: row.action_note || null,
        actionedBy: row.acknowledged_by || null,
        actionedAt: toIso(row.acknowledged_at),
        createdAt: toIso(row.created_at),
    };
}

// ── Alert builders (current live conditions) ─────────────────────────────────
async function buildContainerEtaAlerts(conn, today) {
    const placeholders = ETA_EXCLUDED_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `SELECT id, jf_code, product_name, supplier, status, quantity, eta,
                container_number, external_container_number, po_number
           FROM orders
          WHERE deleted_at IS NULL
            AND eta IS NOT NULL
            AND eta <= ?
            AND status NOT IN (${placeholders})
          ORDER BY eta ASC, container_number ASC, id ASC`,
        [today, ...ETA_EXCLUDED_STATUSES]
    );

    // Group by container (so one container = one alert listing its SKUs),
    // falling back to per-order when an order has no container number. The
    // dedup_key is date-free, so a shifting ETA refreshes this same alert.
    const groups = new Map();
    for (const r of rows) {
        const container = (r.container_number || r.external_container_number || '').trim();
        const groupKey = container ? `c:${container}` : `o:${r.id}`;
        if (!groups.has(groupKey)) groups.set(groupKey, { container, rows: [] });
        groups.get(groupKey).rows.push(r);
    }

    const alerts = [];
    for (const g of groups.values()) {
        // Earliest ETA across the container's lines is the event we surface.
        const eta = g.rows.map((r) => toDateStr(r.eta)).filter(Boolean).sort()[0];
        if (!eta) continue;
        const suppliers = [...new Set(g.rows.map((r) => r.supplier).filter(Boolean))];
        const skus = g.rows.map((r) => r.jf_code || r.product_name).filter(Boolean);
        const totalQty = g.rows.reduce((s, r) => s + Number(r.quantity || 0), 0);
        const severity = eta === today ? 'today' : 'overdue';
        const lines = g.rows.length;
        const title = g.container
            ? `Container ${g.container} — ETA ${eta}`
            : `ETA ${eta}: ${skus[0] || `order ${g.rows[0].id}`}${lines > 1 ? ` +${lines - 1}` : ''}`;
        const bodyParts = [];
        if (suppliers.length) bodyParts.push(suppliers.join(', '));
        bodyParts.push(`${lines} line${lines === 1 ? '' : 's'}`);
        if (totalQty) bodyParts.push(`${totalQty} units`);
        alerts.push({
            dedupKey: g.container ? `container_eta:c:${g.container}` : `container_eta:o:${g.rows[0].id}`,
            type: 'container_eta',
            severity,
            eventDate: eta,
            title,
            body: bodyParts.join(' · '),
            entityType: g.container ? 'container' : 'order',
            entityId: g.container || String(g.rows[0].id),
            meta: {
                containerNumber: g.container || null,
                eta,
                suppliers,
                poNumbers: [...new Set(g.rows.map((r) => r.po_number).filter(Boolean))],
                totalQuantity: totalQty,
                orders: g.rows.map((r) => ({
                    id: r.id,
                    jfCode: r.jf_code || null,
                    productName: r.product_name || null,
                    supplier: r.supplier || null,
                    quantity: Number(r.quantity || 0),
                    status: r.status,
                })),
            },
        });
    }
    return alerts;
}

// ── Missing receiving data (LOT / MFG / EXP) ────────────────────────────────
// In-transit orders missing the lot / mfg / expiry that Mintsoft receiving
// needs — fix it before the goods land. Resolves when the data is filled in or
// the order ships past / is received.
async function buildMissingDataAlerts(conn, today) {
    const ph = IN_TRANSIT_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `SELECT id, jf_code, product_name, supplier, status, po_number,
                lot_number, mfg_date, exp_date, eta, delivery_date,
                container_number, external_container_number
           FROM orders
          WHERE deleted_at IS NULL
            AND status IN (${ph})
            AND (lot_number IS NULL OR lot_number = '' OR mfg_date IS NULL OR exp_date IS NULL)
            -- Overdue ETAs still nag (no lower bound); cap the lookahead so a
            -- shipment months out isn't badged 'today' / sorted oddly.
            AND (eta IS NULL OR eta <= DATE_ADD(?, INTERVAL 21 DAY))
          ORDER BY eta ASC, id ASC`,
        [...IN_TRANSIT_STATUSES, today]
    );
    const alerts = [];
    for (const r of rows) {
        const missing = [];
        if (!r.lot_number) missing.push('LOT');
        if (!r.mfg_date) missing.push('MFG');
        if (!r.exp_date) missing.push('EXP');
        if (!missing.length) continue;
        const eta = toDateStr(r.eta);
        const date = eta || toDateStr(r.delivery_date) || today;
        const sku = r.jf_code || r.product_name || `order ${r.id}`;
        const container = (r.container_number || r.external_container_number || '').trim();
        alerts.push({
            dedupKey: `missing_data:o:${r.id}`,
            type: 'missing_data',
            severity: severityFor(date, today),
            eventDate: date,
            title: `Missing ${missing.join('/')}: ${sku}`,
            body: [r.supplier, r.status, container ? `container ${container}` : null, eta ? `ETA ${eta}` : null].filter(Boolean).join(' · '),
            entityType: 'order',
            entityId: String(r.id),
            meta: {
                orderId: r.id, jfCode: r.jf_code || null, productName: r.product_name || null,
                supplier: r.supplier || null, poNumber: r.po_number || null, status: r.status,
                missing, containerNumber: container || null, eta,
            },
        });
    }
    return alerts;
}

// ── Arrived at warehouse but not received ───────────────────────────────────
// Stock physically at the warehouse (ARRIVED_AT_WAREHOUSE) with units still not
// booked into Mintsoft. Resolves when fully received (status → RECEIVED / no
// outstanding qty).
async function buildArrivedUnreceivedAlerts(conn, today) {
    const [rows] = await conn.query(
        `SELECT id, jf_code, product_name, supplier, po_number, quantity,
                arrived_date, delivery_date, container_number, external_container_number,
                ${OUTSTANDING_SQL} AS outstanding
           FROM orders
          WHERE deleted_at IS NULL
            AND status = 'ARRIVED_AT_WAREHOUSE'
            AND ${OUTSTANDING_SQL} > 0
          ORDER BY arrived_date ASC, id ASC`
    );
    const alerts = [];
    for (const r of rows) {
        const arrived = toDateStr(r.arrived_date);
        const date = arrived || toDateStr(r.delivery_date) || today;
        const sku = r.jf_code || r.product_name || `order ${r.id}`;
        const outstanding = Number(r.outstanding || 0);
        const container = (r.container_number || r.external_container_number || '').trim();
        alerts.push({
            dedupKey: `arrived_unreceived:o:${r.id}`,
            type: 'arrived_unreceived',
            severity: severityFor(date, today),
            eventDate: date,
            title: `Arrived, not received: ${sku}`,
            body: [`${outstanding} of ${Number(r.quantity || 0)} units`, r.supplier, arrived ? `arrived ${arrived}` : null].filter(Boolean).join(' · '),
            entityType: 'order',
            entityId: String(r.id),
            meta: {
                orderId: r.id, jfCode: r.jf_code || null, productName: r.product_name || null,
                supplier: r.supplier || null, poNumber: r.po_number || null,
                quantity: Number(r.quantity || 0), outstanding,
                containerNumber: container || null, arrivedDate: arrived,
            },
        });
    }
    return alerts;
}

// ── Shipment running late ───────────────────────────────────────────────────
// An in-transit order whose booked delivery slot has passed, or whose carrier
// ETA has been pushed back >= 2 days vs the original (ShipsGo eta vs
// eta_initial). Resolves when it arrives / is received.
async function buildShipmentLateAlerts(conn, today) {
    const ph = IN_TRANSIT_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `SELECT o.id, o.jf_code, o.product_name, o.supplier, o.status, o.po_number,
                o.delivery_date, o.eta, o.container_number, o.external_container_number,
                DATEDIFF(c.eta, c.eta_initial) AS slip
           FROM orders o
           LEFT JOIN containers c ON c.container_number = TRIM(o.external_container_number)
          WHERE o.deleted_at IS NULL
            AND o.status IN (${ph})
            AND (
                  (o.delivery_date IS NOT NULL AND o.delivery_date < ?)
               OR (c.eta IS NOT NULL AND c.eta_initial IS NOT NULL AND DATEDIFF(c.eta, c.eta_initial) >= 2)
            )
          ORDER BY o.id ASC`,
        [...IN_TRANSIT_STATUSES, today]
    );
    const alerts = [];
    for (const r of rows) {
        const dd = toDateStr(r.delivery_date);
        const slip = Number(r.slip || 0);
        const reasons = [];
        if (dd && dd < today) reasons.push(`delivery due ${dd}`);
        if (slip >= 2) reasons.push(`ETA slipped +${slip}d`);
        if (!reasons.length) continue;
        const date = dd || toDateStr(r.eta) || today;
        const sku = r.jf_code || r.product_name || `order ${r.id}`;
        const container = (r.container_number || r.external_container_number || '').trim();
        alerts.push({
            dedupKey: `shipment_late:o:${r.id}`,
            type: 'shipment_late',
            severity: severityFor(date, today),
            eventDate: date,
            title: `Shipment late: ${sku}`,
            body: [r.supplier, container ? `container ${container}` : null, ...reasons].filter(Boolean).join(' · '),
            entityType: 'order',
            entityId: String(r.id),
            meta: {
                orderId: r.id, jfCode: r.jf_code || null, productName: r.product_name || null,
                supplier: r.supplier || null, poNumber: r.po_number || null, status: r.status,
                containerNumber: container || null, deliveryDate: dd, etaSlipDays: slip || null, reasons,
            },
        });
    }
    return alerts;
}

// Upsert one alert per condition. INSERT ... ON DUPLICATE KEY UPDATE refreshes
// the display fields of the existing row (so the surfaced ETA / ready date stays
// current) and stamps last_seen_at = this run, WITHOUT touching the user's
// snooze/dismiss state. A dismissed alert keeps its dismissed snapshot; a live
// condition clears any stale resolved_at so it counts as active again.
async function upsertAlerts(conn, alerts, runTs) {
    let created = 0;
    let refreshed = 0;
    for (const a of alerts) {
        const [res] = await conn.query(
            `INSERT INTO daily_alerts
                (dedup_key, type, severity, event_date, title, body, entity_type, entity_id, meta, last_seen_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                last_seen_at = VALUES(last_seen_at),
                resolved_at  = NULL,
                severity     = IF(dismissed_at IS NULL, VALUES(severity), severity),
                event_date   = IF(dismissed_at IS NULL, VALUES(event_date), event_date),
                title        = IF(dismissed_at IS NULL, VALUES(title), title),
                body         = IF(dismissed_at IS NULL, VALUES(body), body),
                meta         = IF(dismissed_at IS NULL, VALUES(meta), meta)`,
            [
                a.dedupKey, a.type, a.severity, a.eventDate, a.title, a.body || null,
                a.entityType || null, a.entityId || null, a.meta ? JSON.stringify(a.meta) : null, runTs,
            ]
        );
        // affectedRows: 1 = inserted, 2 = updated existing, 0 = unchanged.
        if (res.affectedRows === 1) created += 1;
        else if (res.affectedRows === 2) refreshed += 1;
    }
    return { created, refreshed };
}

// ── Generation (called by the nightly Lambda) ───────────────────────────────
async function generateDailyAlerts(pool, { today = londonToday() } = {}) {
    const conn = await pool.getConnection();
    try {
        // Single DB-clock timestamp marks this run; rows not stamped with it are
        // the ones whose condition is no longer live → auto-resolve below.
        const [[{ run_ts: runTs }]] = await conn.query(
            `SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS run_ts`
        );

        const containerAlerts = await buildContainerEtaAlerts(conn, today);
        const missingAlerts = await buildMissingDataAlerts(conn, today);
        const arrivedAlerts = await buildArrivedUnreceivedAlerts(conn, today);
        const lateAlerts = await buildShipmentLateAlerts(conn, today);
        const all = [
            ...containerAlerts, ...missingAlerts,
            ...arrivedAlerts, ...lateAlerts,
        ];
        const { created, refreshed } = await upsertAlerts(conn, all, runTs);

        // Safety net: the auto-resolve sweep below is global (liveness = "seen
        // this run"), so a builder that silently regresses to [] would wipe its
        // whole category. Warn when a type still has pending rows but produced 0
        // candidates this run, so a silent mass-resolve is at least visible.
        const candidatesByType = {
            container_eta: containerAlerts.length,
            missing_data: missingAlerts.length,
            arrived_unreceived: arrivedAlerts.length,
            shipment_late: lateAlerts.length,
        };
        const managedPh = MANAGED_TYPES.map(() => '?').join(', ');
        const [pendingByType] = await conn.query(
            `SELECT type, COUNT(*) AS n FROM daily_alerts
              WHERE resolved_at IS NULL AND dismissed_at IS NULL
                AND (snoozed_until IS NULL OR snoozed_until <= NOW())
                AND type IN (${managedPh})
              GROUP BY type`,
            MANAGED_TYPES
        );
        for (const row of pendingByType) {
            // Retired types are EXPECTED to produce 0 candidates and mass-resolve
            // their remaining rows — that's the point, not a builder regression.
            if (RETIRED_TYPES.includes(row.type)) continue;
            if ((candidatesByType[row.type] || 0) === 0 && row.n > 0) {
                log.warn(`[daily-alerts] type '${row.type}' produced 0 candidates but has ${row.n} pending — they will auto-resolve this run (possible builder regression)`);
            }
        }

        // Auto-resolve: any still-pending (not dismissed, not already resolved)
        // alert whose condition was NOT live this run — the order progressed,
        // was received, or its ready date moved into the future. This is what
        // makes alerts stop nagging "once the order is updated".
        const [resolved] = await conn.query(
            `UPDATE daily_alerts
                SET resolved_at = NOW()
              WHERE resolved_at IS NULL
                AND dismissed_at IS NULL
                AND type IN (${managedPh})
                AND (last_seen_at IS NULL OR last_seen_at < ?)`,
            [...MANAGED_TYPES, runTs]
        );

        return {
            today,
            candidates: {
                containerEta: containerAlerts.length,
                missingData: missingAlerts.length,
                arrivedUnreceived: arrivedAlerts.length,
                shipmentLate: lateAlerts.length,
            },
            created,
            refreshed,
            resolved: resolved.affectedRows || 0,
        };
    } finally {
        conn.release();
    }
}

// ── Reads (called by the authed API) ────────────────────────────────────────
async function listAlerts(conn, { status = 'pending', type, limit } = {}) {
    let s = typeof status === 'string' ? status : 'pending';
    if (!ALERT_STATUSES.has(s)) s = 'pending'; // unknown → safe default
    const where = [];
    const params = [];
    if (s === 'pending') {
        where.push('da.dismissed_at IS NULL AND da.resolved_at IS NULL AND (da.snoozed_until IS NULL OR da.snoozed_until <= NOW())');
    } else if (s === 'snoozed') {
        where.push('da.dismissed_at IS NULL AND da.resolved_at IS NULL AND da.snoozed_until IS NOT NULL AND da.snoozed_until > NOW()');
    } else if (s === 'dismissed') {
        where.push('da.dismissed_at IS NOT NULL');
    } else if (s === 'resolved') {
        where.push('da.resolved_at IS NOT NULL AND da.dismissed_at IS NULL');
    } // 'all' → no lifecycle filter
    if (typeof type === 'string' && type) { where.push('da.type = ?'); params.push(type); }
    const lim = clampLimit(limit);
    const [rows] = await conn.query(
        `${ALERT_SELECT}
         ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
         ORDER BY da.event_date ASC, da.id ASC
         LIMIT ${lim}`,
        params
    );
    return rows.map(rowToAlert);
}

// The most recent lifecycle transition on a row, across user actions AND the
// auto-resolve sweep (which stamps only resolved_at). Ordering/filtering on
// COALESCE(acknowledged_at, resolved_at) would use the stale snooze time for a
// snoozed-then-auto-resolved row and bury / day-filter it out.
const LAST_EVENT_AT = `GREATEST(
    COALESCE(da.acknowledged_at, '1000-01-01'),
    COALESCE(da.resolved_at,     '1000-01-01'),
    COALESCE(da.dismissed_at,    '1000-01-01'))`;

async function listHistory(conn, { limit, days } = {}) {
    const where = ['(da.acknowledged_at IS NOT NULL OR da.dismissed_at IS NOT NULL OR da.resolved_at IS NOT NULL)'];
    const params = [];
    if (days) {
        const d = parseInt(days, 10);
        if (Number.isFinite(d) && d > 0) {
            where.push(`${LAST_EVENT_AT} >= (NOW() - INTERVAL ? DAY)`);
            params.push(d);
        }
    }
    const lim = clampLimit(limit);
    const [rows] = await conn.query(
        `${ALERT_SELECT}
          WHERE ${where.join(' AND ')}
          ORDER BY ${LAST_EVENT_AT} DESC, da.id DESC
          LIMIT ${lim}`,
        params
    );
    return rows.map(rowToAlert);
}

// ── Action (snooze / dismiss / restore) ──────────────────────────────────────
async function actOnAlert(conn, { id, userEmail, action, days }) {
    if (!ACTIONS.has(action)) return { invalidAction: true };
    const [existing] = await conn.query(`${ALERT_SELECT} WHERE da.id = ?`, [id]);
    if (!existing.length) return { notFound: true };
    const before = rowToAlert(existing[0]);

    if (action === 'snooze') {
        const n = Number.isFinite(Number(days)) && Number(days) > 0
            ? Math.min(Math.floor(Number(days)), 365)
            : DEFAULT_SNOOZE_DAYS;
        // Snooze does NOT clear dismissed_at/resolved_at: it only acts on a live
        // alert. (Restore is the explicit un-dismiss / revive path.) Snoozing a
        // dismissed or resolved alert is therefore a no-op on its visibility.
        await conn.query(
            `UPDATE daily_alerts
                SET snoozed_until = (NOW() + INTERVAL ? DAY),
                    last_action = 'snooze',
                    acknowledged_at = NOW(),
                    acknowledged_by = ?
              WHERE id = ?`,
            [n, userEmail || null, id]
        );
    } else if (action === 'dismiss') {
        await conn.query(
            `UPDATE daily_alerts
                SET dismissed_at = NOW(),
                    snoozed_until = NULL,
                    last_action = 'dismiss',
                    acknowledged_at = NOW(),
                    acknowledged_by = ?
              WHERE id = ?`,
            [userEmail || null, id]
        );
    } else { // restore
        await conn.query(
            `UPDATE daily_alerts
                SET snoozed_until = NULL,
                    dismissed_at = NULL,
                    resolved_at = NULL,
                    last_action = 'restore',
                    acknowledged_at = NOW(),
                    acknowledged_by = ?
              WHERE id = ?`,
            [userEmail || null, id]
        );
    }

    const [after] = await conn.query(`${ALERT_SELECT} WHERE da.id = ?`, [id]);
    const alert = rowToAlert(after[0]);
    // Provenance: who snoozed/dismissed/restored, with before/after snapshots.
    // recordAudit never throws — a missing audit row must not fail the action.
    await recordAudit(conn, { entityType: 'daily_alert', entityId: id, action, before, after: alert, userEmail });
    return { alert };
}

// ── AI status suggestions (event-driven, written by the Front importer) ──────
//
// A 'status_suggestion' alert proposes an order status change Gemini inferred
// from a supplier email (src/services/front-status-import.js). It rides the
// same daily_alerts table + lifecycle, but is NOT a MANAGED_TYPE, so the nightly
// auto-resolve never touches it. Approve/Deny is wired in the authed API
// (PATCH /api/v1/alerts/:id/approve|deny in src/handlers/orders.js): approve
// applies the change to orders.status (+ audit), deny dismisses it permanently.

const SUGGESTION_TYPE = 'status_suggestion';
// approve  → apply the order status change + resolve (handled in orders.js)
// acknowledge → resolve WITHOUT changing the order; operator notes what they did instead
// deny     → dismiss (reject the suggestion)
const SUGGESTION_ACTIONS = new Set(['approve', 'acknowledge', 'deny']);

// Upsert ONE suggestion alert. Refreshes display fields in place on a repeated
// dedup_key (e.g. a second email re-asserting the same change with new
// confidence), but — unlike the nightly upsert — NEVER revives a dismissed
// (denied) or resolved (approved) row: once a human has actioned a suggestion,
// a later identical extraction must not re-surface it. Returns the row id.
async function upsertSuggestionAlert(conn, a) {
    await conn.query(
        `INSERT INTO daily_alerts
            (dedup_key, type, severity, event_date, title, body, entity_type, entity_id, meta, last_seen_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE
            last_seen_at = NOW(),
            severity   = IF(dismissed_at IS NULL AND resolved_at IS NULL, VALUES(severity), severity),
            event_date = IF(dismissed_at IS NULL AND resolved_at IS NULL, VALUES(event_date), event_date),
            title      = IF(dismissed_at IS NULL AND resolved_at IS NULL, VALUES(title), title),
            body       = IF(dismissed_at IS NULL AND resolved_at IS NULL, VALUES(body), body),
            meta       = IF(dismissed_at IS NULL AND resolved_at IS NULL, VALUES(meta), meta)`,
        [
            a.dedupKey, SUGGESTION_TYPE, a.severity || 'info', a.eventDate, a.title, a.body || null,
            a.entityType || 'order', a.entityId || null, a.meta ? JSON.stringify(a.meta) : null,
        ]
    );
    // ON DUPLICATE KEY UPDATE doesn't reliably set insertId on the update path,
    // so resolve the id by its (unique) dedup_key.
    const [[row]] = await conn.query(`SELECT id FROM daily_alerts WHERE dedup_key = ?`, [a.dedupKey]);
    return { id: row ? row.id : null };
}

// Fetch a single alert (any type) by id, shaped like the list/history reads.
async function getAlert(conn, id) {
    const [rows] = await conn.query(`${ALERT_SELECT} WHERE da.id = ?`, [id]);
    return rows.length ? rowToAlert(rows[0]) : null;
}

// Lock-and-fetch a status_suggestion by id (SELECT ... FOR UPDATE). MUST be
// called inside a transaction: the row lock is held until commit/rollback, so a
// concurrent approve of the same suggestion blocks here and — once the first
// approve commits the alert as resolved — sees 'resolved' and aborts, instead of
// mutating the order a second time. Returns the alert or null. The FOR UPDATE is
// on `daily_alerts` only; the trailing computed `is_snoozed` expression doesn't
// affect lockability.
async function getSuggestionForUpdate(conn, id) {
    const [rows] = await conn.query(
        `${ALERT_SELECT} WHERE da.id = ? AND da.type = ? FOR UPDATE`, [id, SUGGESTION_TYPE]
    );
    return rows.length ? rowToAlert(rows[0]) : null;
}

// Approve / deny a status_suggestion. Approve marks it RESOLVED (the order
// mutation itself is done by the caller in orders.js before this), deny marks it
// DISMISSED ("never remind"). Returns { notFound } for a missing/non-suggestion
// alert, { alreadyActioned, alert } if it was already approved/denied, else
// { alert }. Records a daily_alert audit row (who/when) — recordAudit never throws.
async function actOnSuggestion(conn, { id, userEmail, action, note }) {
    if (!SUGGESTION_ACTIONS.has(action)) return { invalidAction: true };
    const [existing] = await conn.query(
        `${ALERT_SELECT} WHERE da.id = ? AND da.type = ?`, [id, SUGGESTION_TYPE]
    );
    if (!existing.length) return { notFound: true };
    const before = rowToAlert(existing[0]);
    if (before.status === 'dismissed' || before.status === 'resolved') {
        return { alreadyActioned: true, alert: before };
    }
    const noteVal = note != null && String(note).trim() !== '' ? String(note).trim().slice(0, 1000) : null;

    if (action === 'deny') {
        // Reject the suggestion → dismissed (permanent).
        await conn.query(
            `UPDATE daily_alerts
                SET dismissed_at = NOW(), snoozed_until = NULL,
                    last_action = 'deny', action_note = ?, acknowledged_at = NOW(), acknowledged_by = ?
              WHERE id = ?`,
            [noteVal, userEmail || null, id]
        );
    } else {
        // approve | acknowledge → resolved (off the queue). 'approve' applies the
        // order change (done by the caller); 'acknowledge' does NOT touch the
        // order — the note records what the operator did instead.
        await conn.query(
            `UPDATE daily_alerts
                SET resolved_at = NOW(), snoozed_until = NULL,
                    last_action = ?, action_note = ?, acknowledged_at = NOW(), acknowledged_by = ?
              WHERE id = ?`,
            [action, noteVal, userEmail || null, id]
        );
    }

    const [after] = await conn.query(`${ALERT_SELECT} WHERE da.id = ?`, [id]);
    const alert = rowToAlert(after[0]);
    await recordAudit(conn, { entityType: 'daily_alert', entityId: id, action, before, after: alert, userEmail });
    return { alert };
}

module.exports = {
    generateDailyAlerts,
    listAlerts,
    listHistory,
    actOnAlert,
    rowToAlert,
    londonToday,
    getAlert,
    getSuggestionForUpdate,
    actOnSuggestion,
    upsertSuggestionAlert,
    ALERT_STATUSES,
    ACTIONS,
    MANAGED_TYPES,
    SUGGESTION_TYPE,
    DEFAULT_SNOOZE_DAYS,
};
