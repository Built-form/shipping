'use strict';

// ── Front -> Gemini -> human-approval status importer ────────────────────────
//
// Scheduled sibling of front-qc-import.js. Pulls recent INBOUND supplier emails
// out of Front, asks Gemini (src/services/supplier-email-check.js) whether each
// one announces an order-status milestone, matches the extracted PO number to
// live `orders`, and queues a PENDING `status_suggestion` alert in `daily_alerts`
// for a human to APPROVE (applies orders.status + audits) or DENY (dismiss).
//
// AI never mutates an order — it only proposes. The approval lives behind the
// authed API (PATCH /api/v1/alerts/:id/approve|deny in src/handlers/orders.js).
//
// COST / RATE CONTROL — Gemini is called only after a cascade of cheap gates:
//   1. Front search is scoped to supplier addresses/domains derived from the
//      `suppliers` + `supplier_emails` tables (+ optional STATUS_IMPORT_DOMAINS).
//   2. Only inbound messages from those senders, inside the look-back window.
//   3. Dedup on front_status_imports.source_ref BEFORE any Gemini call — a
//      re-run over already-processed messages costs one indexed SELECT each.
//   4. A keyword pre-filter drops pure-chatter messages without calling Gemini.
//   5. A per-run cap (STATUS_IMPORT_MAX_GEMINI) bounds spend; the overflow stays
//      undeduped and is picked up next hour.
//
// Idempotent: source_ref dedup + the order-status no-op check mean re-runs don't
// duplicate alerts, and a suggestion whose order already reached the inferred
// status is never created.

const log = require('../lib/logger');
const {
    searchConversations,
    listMessages,
    messageFromEmail,
    emailInDomain,
    messageText,
    messageBodySnippet,
    cleanSubject,
    frontWebUrl,
} = require('./front-qc-import');
const { extractStatusFromEmail, INFERRABLE_MILESTONES } = require('./supplier-email-check');
const { refineMatchedOrders } = require('./suggestion-refine');
const { upsertSuggestionAlert, londonToday } = require('./daily-alerts');
const T = require('../lib/order-transitions');

const DEFAULT_SINCE_DAYS = 3;        // first-ever run (no watermark yet)
const DEFAULT_CONF_MIN = 0.6;
const DEFAULT_MAX_GEMINI = 40;
// Pass-2 (refine) is one Pro call PER MATCHED ORDER and is NOT bounded by the
// Pass-1 cap — a single consolidated-container email can match 100+ orders. So
// bound it independently with BOTH a per-run call cap and a wall-clock budget
// (the Lambda timeout is 900s); when either is hit, remaining orders fall back
// to the Pass-1 extraction (a valid, if less context-aware, suggestion).
const DEFAULT_MAX_REFINE = 40;
const DEFAULT_REFINE_BUDGET_MS = 600000; // 10 min — leaves headroom under the 900s Lambda timeout
// Incremental look-back: normally scan only since the last fully-drained run,
// minus this overlap (catches a late-delivered / late-Front-indexed email), and
// never further back than the cap (a long outage can't trigger a giant scan).
const DEFAULT_OVERLAP_HOURS = 6;
const DEFAULT_MAX_LOOKBACK_DAYS = 7;
// Hard ceiling on conversations walked per run, so a wide window can't blow the
// Lambda timeout on listMessages calls. The Gemini cap is the real cost guard.
const MAX_CONVERSATIONS = 800;
// Full email text stored for provenance + on the alert (so the reviewer can read
// the whole message and see the highlighted evidence in context). Capped to the
// same window Gemini actually reads, so a quote can always be located in it.
const BODY_FULL_CAP = 20000;
// Gentle spacing between the ~per-domain Front searches so a run of many target
// domains doesn't burst the search route's rate limit (frontGetJson also
// retries on 429, but spacing avoids most of them).
const SEARCH_THROTTLE_MS = Number(process.env.STATUS_IMPORT_SEARCH_THROTTLE_MS) || 350;

// Orders whose CURRENT status is received/terminal/warehouse — a supplier email
// about production or shipping no longer advances them, so never candidates.
// (Live DB values; see src/lib/order-transitions.js CANDIDATE_EXCLUDED_STATUSES.)
const TERMINAL_STATUSES = [...T.CANDIDATE_EXCLUDED_STATUSES];

// Freight forwarders — not in the suppliers tables, but they drive the in-transit
// milestones (consolidated / sailed / ETD-ETA / arrived) and tracking-date updates.
// Always scanned, on top of supplier domains. Extend via STATUS_IMPORT_DOMAINS too.
const FORWARDER_DOMAINS = [
    'dcglogistics.com',
    'savinodelbene.com',
];

// Freemail / generic domains: a supplier may legitimately use one, but we must
// NOT broaden the Front search to `from:gmail.com` (matches the whole world).
// Such suppliers are searched by their EXACT address instead.
const FREEMAIL_DOMAINS = new Set([
    'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'live.com',
    'yahoo.com', 'yahoo.com.cn', 'ymail.com', 'foxmail.com', 'qq.com',
    '163.com', '126.com', 'sina.com', 'aliyun.com', 'icloud.com', 'me.com',
]);

// Cheap pre-filter: a message must contain at least one status-suggestive token
// before it's worth a Gemini call. This is a COST gate, not a correctness gate
// (Gemini makes the real judgement), so it errs broad.
const STATUS_HINT_RE = /produc|manufactur|ready|finish|complet|ship|vessel|sail|depart|\betd\b|\beta\b|load|dispatch|deliver|consign|container|booking|inspect|\bqc\b|airway|\bawb\b|bill of lading|\bb\/?l\b|cargo|forwarder|pick.?up|collect/i;

function envNum(name, fallback) {
    const n = Number(process.env[name]);
    return Number.isFinite(n) && n > 0 ? n : fallback;
}

function normalizePo(s) {
    return String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}

// ── Schema (idempotent) ──────────────────────────────────────────────────────
// Provenance + dedup ledger: one row per Front MESSAGE processed (UNIQUE
// source_ref), recording what Gemini saw, what we matched, and the outcome.
async function ensureSchema(conn) {
    await conn.query(`
        CREATE TABLE IF NOT EXISTS front_status_imports (
            id BIGINT NOT NULL AUTO_INCREMENT,
            source_ref          VARCHAR(255) NOT NULL,
            conversation_id     VARCHAR(100) NULL,
            message_id          VARCHAR(100) NULL,
            from_email          VARCHAR(255) NULL,
            subject             VARCHAR(512) NULL,
            received_at         DATETIME NULL,
            extracted_po        VARCHAR(100) NULL,
            po_found            TINYINT(1) NULL,
            inferred_status     VARCHAR(32) NULL,
            status_stated       TINYINT(1) NULL,
            confidence          DECIMAL(4,3) NULL,
            rationale           VARCHAR(1000) NULL,
            matched_order_count INT NOT NULL DEFAULT 0,
            order_ids           JSON NULL,
            alert_ids           JSON NULL,
            model_used          VARCHAR(64) NULL,
            usage_json          JSON NULL,
            outcome             VARCHAR(24) NOT NULL DEFAULT 'pending',
            error_message       VARCHAR(1000) NULL,
            source_meta         JSON NULL,
            created_at          TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uk_source_ref (source_ref),
            KEY idx_outcome (outcome),
            KEY idx_created (created_at)
        )
    `);
    // Single-row watermark for incremental look-back. Stored as epoch SECONDS to
    // sidestep mysql2 DATETIME/timezone conversion entirely.
    await conn.query(`
        CREATE TABLE IF NOT EXISTS front_status_import_state (
            id TINYINT NOT NULL,
            last_run_ts BIGINT NULL,
            updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        )
    `);
}

// Epoch ms of the last fully-drained run's start, or null if never run.
async function getLastRunMs(conn) {
    const [rows] = await conn.query(`SELECT last_run_ts FROM front_status_import_state WHERE id = 1`);
    return rows.length && rows[0].last_run_ts ? Number(rows[0].last_run_ts) * 1000 : null;
}

async function setLastRunTs(conn, sec) {
    await conn.query(
        `INSERT INTO front_status_import_state (id, last_run_ts) VALUES (1, ?)
         ON DUPLICATE KEY UPDATE last_run_ts = VALUES(last_run_ts)`,
        [sec]
    );
}

// ── Front targets (which senders to scan) ────────────────────────────────────
// Derives the supplier address set from the suppliers tables + an optional
// STATUS_IMPORT_DOMAINS env override (comma-separated domains and/or full
// addresses). Returns the Front search queries to run plus an accept() filter.
async function buildFrontTargets(conn, afterTs) {
    const domains = new Set();      // corporate domains -> from:<domain>
    const exactEmails = new Set();  // freemail-based supplier addrs -> from:<email>
    const allAddrs = new Set();     // every known supplier address, for accept()

    // Strict shapes: an address/domain is interpolated into the Front search
    // query (from:<x>), so a value containing query syntax (spaces, colons,
    // quotes) could inject extra operators. Reject anything that isn't a clean
    // email/domain. accept() is still the authoritative gate on results, but we
    // keep malformed values out of the query entirely.
    const EMAIL_RE = /^[a-z0-9._%+'-]+@[a-z0-9.-]+\.[a-z]{2,}$/;
    const DOMAIN_RE = /^[a-z0-9.-]+\.[a-z]{2,}$/;
    const addAddr = (raw) => {
        const a = String(raw || '').trim().toLowerCase();
        if (!EMAIL_RE.test(a)) return;
        allAddrs.add(a);
        const dom = a.split('@').pop();
        if (FREEMAIL_DOMAINS.has(dom)) exactEmails.add(a);
        else domains.add(dom);
    };
    const addDomain = (raw) => {
        const d = String(raw || '').trim().toLowerCase().replace(/^@/, '');
        if (DOMAIN_RE.test(d) && !FREEMAIL_DOMAINS.has(d)) domains.add(d);
    };

    // Env override: each item is a full address (has @) or a bare domain.
    for (const item of String(process.env.STATUS_IMPORT_DOMAINS || '').split(',')) {
        const v = item.trim();
        if (!v) continue;
        if (v.includes('@')) addAddr(v); else addDomain(v);
    }

    // Onboarded suppliers (primary contact + the supplier_emails list).
    try {
        const [rows] = await conn.query(`
            SELECT email FROM supplier_emails
             WHERE deleted_at IS NULL AND email IS NOT NULL AND email <> ''
            UNION
            SELECT contact_email AS email FROM suppliers
             WHERE deleted_at IS NULL AND contact_email IS NOT NULL AND contact_email <> ''
        `);
        for (const r of rows) addAddr(r.email);
    } catch (e) {
        log.warn('[front-status-import] supplier address lookup failed', { error: e.message });
    }

    // Freight forwarders (always included).
    for (const d of FORWARDER_DOMAINS) addDomain(d);

    const queries = [
        ...[...domains].map(d => `from:${d} after:${afterTs}`),
        ...[...exactEmails].map(e => `from:${e} after:${afterTs}`),
    ];
    const domainList = [...domains];
    const accept = (email) =>
        !!email && (allAddrs.has(String(email).toLowerCase()) || domainList.some(d => emailInDomain(email, d)));

    return { queries, accept, counts: { domains: domains.size, exactEmails: exactEmails.size, addrs: allAddrs.size } };
}

// ── PO -> order matching ─────────────────────────────────────────────────────
// Every column the transition gates + field-apply need, plus a computed
// has_qc_report flag (the §3a hard gate for READY). Shared by both matchers.
const ORDER_MATCH_SELECT = `
    SELECT o.id, o.status, o.po_number, o.po_date, o.jf_code, o.asin, o.product_name,
           o.supplier, o.unit_price, o.qc_status, o.qc_date, o.qc_invoice_number,
           o.lot_number, o.mfg_date, o.exp_date, o.estimated_ready_date, o.actual_ready_date,
           o.artwork_confirmed_date, o.container_number, o.external_container_number,
           o.vessel_name, o.eta, o.estimated_departure_date,
           o.shipped_date, o.delivery_date, o.arrived_date,
           EXISTS(SELECT 1 FROM order_qc_reports oqr WHERE oqr.order_id = o.id) AS has_qc_report
      FROM orders o`;

// Load a single order in the same shape matchOrdersByPo returns (incl.
// has_qc_report), for the pending-suggestion re-evaluator. Returns null if the
// order is gone or has reached a terminal/received status (no longer a candidate).
async function loadOrderForSuggestion(conn, orderId) {
    const placeholders = TERMINAL_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `${ORDER_MATCH_SELECT}
          WHERE o.id = ? AND o.deleted_at IS NULL
            AND o.status NOT IN (${placeholders})`,
        [orderId, ...TERMINAL_STATUSES]
    );
    return rows.length ? rows[0] : null;
}

// orders.po_number is free-text VARCHAR (no FK), so match on the exact string OR
// on the alphanumeric-normalised value ("PO# 12345" == "PO12345" == "po12345").
async function matchOrdersByPo(conn, poRaw) {
    const norm = normalizePo(poRaw);
    if (!norm) return [];
    const placeholders = TERMINAL_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `${ORDER_MATCH_SELECT}
          WHERE o.deleted_at IS NULL
            AND o.po_number IS NOT NULL AND o.po_number <> ''
            AND o.status NOT IN (${placeholders})
            AND (o.po_number = ? OR UPPER(REGEXP_REPLACE(o.po_number, '[^A-Za-z0-9]', '')) = ?)
          ORDER BY o.id ASC`,
        [...TERMINAL_STATUSES, poRaw, norm]
    );
    return rows;
}

// Forwarder emails usually cite a container/booking number, not the importer's
// PO. Match it against orders.container_number OR external_container_number
// (alphanumeric-normalised, exact).
async function matchOrdersByContainer(conn, raw) {
    const norm = normalizePo(raw); // reuse: uppercase + strip non-alphanumerics
    if (norm.length < 6) return []; // avoid spurious short matches
    const placeholders = TERMINAL_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `${ORDER_MATCH_SELECT}
          WHERE o.deleted_at IS NULL
            AND o.status NOT IN (${placeholders})
            AND ( (o.container_number IS NOT NULL AND o.container_number <> ''
                   AND UPPER(REGEXP_REPLACE(o.container_number, '[^A-Za-z0-9]', '')) = ?)
               OR (o.external_container_number IS NOT NULL AND o.external_container_number <> ''
                   AND UPPER(REGEXP_REPLACE(o.external_container_number, '[^A-Za-z0-9]', '')) = ?) )
          ORDER BY o.id ASC`,
        [...TERMINAL_STATUSES, norm, norm]
    );
    return rows;
}

// Suppliers frequently reference an order by its PRODUCT code ("JF0230") instead
// of the PO. A JF code lives in orders.jf_code and — unlike a PO — is a SKU
// shared across many order rows (one per batch/PO), so a match can be ambiguous.
// This returns ALL live (non-terminal) orders for the code; the caller only
// TRUSTS a single-order result and treats a wider match as ambiguous. Match is
// alphanumeric-normalised + exact (so "JF0230" != the FQC variant "JF5011_FQC").
async function matchOrdersByJfCode(conn, raw) {
    const norm = normalizePo(raw);
    if (norm.length < 4) return []; // avoid spurious short matches
    const placeholders = TERMINAL_STATUSES.map(() => '?').join(', ');
    const [rows] = await conn.query(
        `${ORDER_MATCH_SELECT}
          WHERE o.deleted_at IS NULL
            AND o.jf_code IS NOT NULL AND o.jf_code <> ''
            AND o.status NOT IN (${placeholders})
            AND UPPER(REGEXP_REPLACE(o.jf_code, '[^A-Za-z0-9]', '')) = ?
          ORDER BY o.id ASC`,
        [...TERMINAL_STATUSES, norm]
    );
    return rows;
}

// A single PO can span MULTIPLE live order rows when it was /split (see
// POST /orders/:id/split): same po_number + jf_code, but only PART of the
// quantity went into a given container — the split clone carries that
// container_number (status CONSOLIDATED) while the remainder keeps producing.
// Anything an email says about a SPECIFIC container therefore applies ONLY to
// the row(s) physically in THAT container — never the sibling still in the
// factory. That covers both a container/transit MILESTONE (consolidated /
// sailed / arrived) AND a pure DATA update that carries container data (a
// forwarder pushing ETA/vessel onto its shipment). This narrows a multi-row PO
// match to the order(s) whose recorded container matches the email's container.
// It is a no-op unless ALL of these hold, so a normal (unsplit) PO and any
// FACTORY-stage milestone (which advances the whole PO) are untouched:
//   • more than one row matched, AND
//   • the milestone is NOT a factory stage (transit milestone, or none at all
//     for a data-only forwarder update), AND
//   • the email cited a usable container, AND
//   • at least one matched row already carries that container.
// When no matched row carries the container yet (e.g. a first-ever mention that
// predates the split) we can't disambiguate deterministically, so keep them all
// and let Pass 2 + the human reviewer decide.
const FACTORY_MILESTONES = new Set(['PO_SENT', 'IN_PRODUCTION', 'READY_FOR_QC', 'READY']);
function narrowSplitByContainer(matched, containerRef, milestone) {
    if (matched.length <= 1 || FACTORY_MILESTONES.has(milestone)) return matched;
    const norm = normalizePo(containerRef);
    if (norm.length < 6) return matched; // no usable container ref to split on
    const inContainer = matched.filter(o =>
        normalizePo(o.container_number) === norm ||
        normalizePo(o.external_container_number) === norm);
    return inContainer.length ? inContainer : matched;
}

function confidencePct(c) {
    return `${Math.round((Number(c) || 0) * 100)}%`;
}

// mysql2 returns DATE as 'YYYY-MM-DD' strings (pool dateStrings) but DATETIME as
// Date objects; normalise either to a comparable 'YYYY-MM-DD'.
function asDateStr(v) {
    if (v == null) return null;
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    return String(v).slice(0, 10);
}

// Mirrors the frontend lotExpiryConflictError (§5): a JF+lot identifies one
// physical batch, so it may carry exactly one expiry. Returns the conflicting
// expiry another order already uses (YYYY-MM-DD), or null if none.
async function lotExpiryConflict(conn, { orderId, jfCode, lotNumber, expDate }) {
    if (!jfCode || !lotNumber || !expDate) return null;
    const [rows] = await conn.query(
        `SELECT DISTINCT DATE_FORMAT(exp_date, '%Y-%m-%d') AS d
           FROM orders
          WHERE deleted_at IS NULL AND id <> ?
            AND UPPER(TRIM(jf_code)) = UPPER(TRIM(?))
            AND UPPER(TRIM(lot_number)) = UPPER(TRIM(?))
            AND exp_date IS NOT NULL`,
        [orderId, jfCode, lotNumber]
    );
    const want = asDateStr(expDate);
    return rows.map(r => r.d).find(d => d && d !== want) || null;
}

// Which email-provided fields to apply for this move: only those pertinent to
// the target (§3 gate + that milestone's data), that are non-null, valid, and
// actually CHANGE the order. Non-admin rules: mfg/exp snap to 1st-of-month;
// expiry can't be in the past; a JF+lot can't take a second expiry (§5). Each
// rejected field is returned in `blocked` with a reason so the human sees why.
async function computeFieldUpdates(conn, order, fieldNames, fields, today) {
    const apply = [];
    const blocked = [];
    const isDate = (f) => f.endsWith('Date') || f === 'eta';
    for (const f of fieldNames) {
        let to = fields[f];
        if (to == null) continue;
        if (f === 'mfgDate' || f === 'expDate') to = T.snapMonthStart(to);
        if (f === 'expDate' && asDateStr(to) < today) { blocked.push({ field: f, value: to, reason: 'expiry_in_past' }); continue; }
        const cur = order[T.FIELD_COLUMN[f]];
        const curCmp = isDate(f) ? asDateStr(cur) : (cur == null ? null : String(cur));
        const toCmp = isDate(f) ? asDateStr(to) : String(to);
        if (curCmp != null && curCmp === toCmp) continue; // unchanged
        apply.push({ field: f, from: curCmp, to });
    }
    // Lot↔expiry uniqueness: a JF+lot carries exactly one expiry. Re-check
    // whenever EITHER the expiry OR the lot would change (changing the lot alone
    // can collide an unchanged expiry with another order's lot).
    const expChange = apply.find(a => a.field === 'expDate');
    const lotChange = apply.find(a => a.field === 'lotNumber');
    if (expChange || lotChange) {
        const lot = lotChange ? lotChange.to : order.lot_number;
        const exp = expChange ? expChange.to : asDateStr(order.exp_date);
        if (lot && exp) {
            const conflict = await lotExpiryConflict(conn, { orderId: order.id, jfCode: order.jf_code, lotNumber: lot, expDate: exp });
            if (conflict) {
                const drop = expChange || lotChange; // prefer dropping the changed expiry; else the lot change
                apply.splice(apply.indexOf(drop), 1);
                blocked.push({ field: drop.field, value: drop.to, reason: `lot_expiry_conflict:${conflict}` });
            }
        }
    }
    return { apply, blocked };
}

const CATEGORY_LABEL = {
    forward: '', multi_step: 'multi-step', lateral: 'sea/air', backward_qc_failed: 'QC failed',
    data_update: 'data',
};

// One alert per affected order line. dedup_key is date-free and includes the
// *target* status, so a re-asserted same suggestion refreshes one row. `sug` is
// { target, category, remaining }; `fieldUpdates`/`missing` come from the importer.
// A 'data_update' suggestion has target === current status (no move) and just
// carries field changes (e.g. an estimated ready date).
function buildSuggestionAlert(order, extract, sug, fieldUpdates, missing, ctx, today) {
    const sku = order.jf_code || order.product_name || `order ${order.id}`;
    const who = order.supplier || ctx.fromEmail || 'Supplier';
    const isData = sug.category === 'data_update';
    const tag = CATEGORY_LABEL[sug.category] ? ` [${CATEGORY_LABEL[sug.category]}]` : '';
    const fieldList = fieldUpdates.apply.map(a => a.field).join(', ');
    const bodyParts = [
        who,
        order.po_number ? `PO ${order.po_number}` : null,
        `AI ${confidencePct(extract.confidence)}`,
        fieldUpdates.apply.length ? (isData ? fieldList : `+${fieldUpdates.apply.length} field${fieldUpdates.apply.length === 1 ? '' : 's'}`) : null,
        missing.length ? `needs: ${missing.join(', ')}` : null,
        extract.rationale ? `"${extract.rationale}"` : null,
    ];
    return {
        dedupKey: `status_suggestion:o:${order.id}:${sug.target}`,
        severity: 'info',
        eventDate: today,
        title: isData
            ? `Update ${sku}: ${fieldList || 'data'}`
            : `Suggested: ${order.status} → ${sug.target}${tag} — ${sku}`,
        body: bodyParts.filter(Boolean).join(' · ').slice(0, 1000),
        entityType: 'order',
        entityId: String(order.id),
        meta: {
            orderId: order.id,
            poNumber: order.po_number || null,
            extractedPo: extract.poNumber,
            currentStatus: order.status,
            suggestedStatus: sug.target,
            milestone: extract.milestone,          // the stage the email evidenced
            category: sug.category,                // forward | multi_step | lateral | backward_qc_failed
            remainingPath: sug.remaining,          // stages still to traverse (multi_step)
            isFqc: T.isFqcOrder(order),
            hasQcReport: !!order.has_qc_report,
            fieldUpdates: fieldUpdates.apply,      // [{ field, from, to }] Approve will apply
            blockedFields: fieldUpdates.blocked,   // [{ field, value, reason }] not applied
            gate: { required: T.gateFieldsFor(sug.target, order.status), missing },
            confidence: extract.confidence,
            rationale: extract.rationale,
            model: extract.modelUsed,
            // Pass-2 (order-aware refine) provenance: the justification that used
            // the order's context (current values / audit / human feedback / prior
            // emails), and the model that produced it. Null when refine was off or
            // fell back to Pass 1. Surfaced on the card's "why" panel.
            reasoning: extract.reasoning || null,
            refined: !!extract.refineModel,
            refineModel: extract.refineModel || null,
            jfCode: order.jf_code || null,
            productName: order.product_name || null,
            supplier: order.supplier || null,
            source: {
                messageId: ctx.messageId,
                conversationId: ctx.conversationId,
                fromEmail: ctx.fromEmail,
                subject: ctx.subject,
                frontUrl: ctx.frontUrl,
                receivedAt: ctx.receivedAtIso,
                // Full email + verbatim evidence quotes for the card to render the
                // message with the important bits highlighted in place.
                bodyFull: ctx.bodyFull || null,
                highlights: ctx.highlights || [],
            },
            sourceRef: ctx.sourceRef,
        },
    };
}

async function recordImport(conn, row) {
    await conn.query(
        `INSERT INTO front_status_imports
            (source_ref, conversation_id, message_id, from_email, subject, received_at,
             extracted_po, po_found, inferred_status, status_stated, confidence, rationale,
             matched_order_count, order_ids, alert_ids, model_used, usage_json, outcome, error_message, source_meta)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            outcome = VALUES(outcome), matched_order_count = VALUES(matched_order_count),
            order_ids = VALUES(order_ids), alert_ids = VALUES(alert_ids),
            inferred_status = VALUES(inferred_status), confidence = VALUES(confidence),
            error_message = VALUES(error_message),
            -- Refresh extraction-derived provenance on a force re-process, so a
            -- re-run (e.g. after a model/prompt change) updates the stored email
            -- text, highlights, rationale and model rather than keeping stale data.
            source_meta = VALUES(source_meta), rationale = VALUES(rationale),
            model_used = VALUES(model_used), usage_json = VALUES(usage_json),
            po_found = VALUES(po_found), status_stated = VALUES(status_stated)`,
        [
            row.sourceRef, row.conversationId, row.messageId, row.fromEmail,
            row.subject ? row.subject.slice(0, 512) : null, row.receivedAt,
            row.extractedPo, row.poFound == null ? null : (row.poFound ? 1 : 0),
            row.inferredStatus, row.statusStated == null ? null : (row.statusStated ? 1 : 0),
            // confidence column is DECIMAL(4,3); round to 3dp so storage is
            // explicit rather than relying on silent MySQL truncation.
            row.confidence == null ? null : Math.round(row.confidence * 1000) / 1000,
            row.rationale ? row.rationale.slice(0, 1000) : null,
            row.matchedOrderCount || 0,
            row.orderIds ? JSON.stringify(row.orderIds) : null,
            row.alertIds ? JSON.stringify(row.alertIds) : null,
            row.modelUsed, row.usage ? JSON.stringify(row.usage) : null,
            row.outcome, row.errorMessage ? row.errorMessage.slice(0, 1000) : null,
            row.sourceMeta ? JSON.stringify(row.sourceMeta) : null,
        ]
    );
}

// ── Orchestrator ─────────────────────────────────────────────────────────────
async function importStatusUpdatesFromFront(conn, {
    sinceDays = null, // null => incremental (since last run); a number forces a fixed window
    dryRun = false,
    model,
    confMin = envNum('STATUS_IMPORT_CONF_MIN', DEFAULT_CONF_MIN),
    maxGemini = envNum('STATUS_IMPORT_MAX_GEMINI', DEFAULT_MAX_GEMINI),
    verbose = false, // per-message progress lines (subject/sender) — handy for manual runs
    force = false,   // re-examine messages even if already in the dedup ledger (re-process history)
    refine = true,   // Pass 2: re-decide per matched order with full order context (Pro)
    refineModel,     // override the Pass-2 model (defaults to the Pro default)
} = {}) {
    if (!process.env.FRONT_API_TOKEN) {
        const e = new Error('FRONT_API_TOKEN is not configured.');
        e.code = 'NOT_CONFIGURED';
        throw e;
    }
    await ensureSchema(conn);

    const today = londonToday();
    const overlapHours = envNum('STATUS_IMPORT_OVERLAP_HOURS', DEFAULT_OVERLAP_HOURS);
    const maxLookbackDays = envNum('STATUS_IMPORT_MAX_LOOKBACK_DAYS', DEFAULT_MAX_LOOKBACK_DAYS);
    const maxRefine = envNum('STATUS_IMPORT_MAX_REFINE', DEFAULT_MAX_REFINE);
    const refineBudgetMs = envNum('STATUS_IMPORT_REFINE_BUDGET_MS', DEFAULT_REFINE_BUDGET_MS);
    let refineCalls = 0;
    const runStartMs = Date.now();

    // Look-back window. An explicit sinceDays (manual backfill) wins. Otherwise go
    // INCREMENTAL: scan only since the last fully-drained run, minus an overlap so
    // a late-delivered / late-indexed email isn't missed, floored at maxLookbackDays
    // so a long outage can't trigger a giant scan. Re-scanned messages still
    // dedup-skip on source_ref, so the overlap costs only Front reads, no Gemini.
    let cutoffMs;
    let windowMode;
    if (sinceDays != null && Number(sinceDays) > 0) {
        cutoffMs = runStartMs - Number(sinceDays) * 86400000;
        windowMode = `sinceDays=${sinceDays}`;
    } else {
        const lastRunMs = await getLastRunMs(conn);
        const floorMs = runStartMs - maxLookbackDays * 86400000;
        if (lastRunMs) {
            cutoffMs = Math.max(lastRunMs - overlapHours * 3600000, floorMs);
            windowMode = `incremental(-${overlapHours}h overlap)`;
        } else {
            cutoffMs = runStartMs - DEFAULT_SINCE_DAYS * 86400000;
            windowMode = `first-run(${DEFAULT_SINCE_DAYS}d)`;
        }
    }
    const afterTs = Math.floor(cutoffMs / 1000);

    const targets = await buildFrontTargets(conn, afterTs);
    const summary = {
        window: windowMode, windowFrom: new Date(cutoffMs).toISOString().slice(0, 16),
        sinceDays, confMin, maxGemini, today,
        targets: targets.counts,
        scanned: 0, geminiCalls: 0, capped: false,
        alerted: 0, alertsCreated: 0,
        refine, maxRefine, refineCalls: 0, refineSkipped: 0,
        skipped: { chatter: 0, nopo: 0, nomatch: 0, nochange: 0, lowconf: 0, fqc: 0, ambiguousJf: 0 },
        failed: 0,
        suggestions: [],
    };

    if (!targets.queries.length) {
        log.warn('[front-status-import] no supplier addresses/domains configured — nothing to scan');
        return summary;
    }

    log.info('[front-status-import] start', {
        window: windowMode, windowFrom: new Date(cutoffMs).toISOString().slice(0, 16),
        queries: targets.queries.length, domains: targets.counts.domains,
        exactEmails: targets.counts.exactEmails, maxGemini, confMin, dryRun,
    });

    const seenMsg = new Set();
    const seenConvo = new Set();
    let convosWalked = 0;
    let queryIdx = 0;

    for (const query of targets.queries) {
        queryIdx += 1;
        if (queryIdx > 1 && SEARCH_THROTTLE_MS > 0) await new Promise(r => setTimeout(r, SEARCH_THROTTLE_MS));
        // Heartbeat so an all-quiet window still shows the scan is progressing.
        if (queryIdx === 1 || queryIdx % 20 === 0 || queryIdx === targets.queries.length) {
            log.info('[front-status-import] progress', {
                query: `${queryIdx}/${targets.queries.length}`, convos: convosWalked,
                scanned: summary.scanned, gemini: summary.geminiCalls, suggestions: summary.suggestions.length,
            });
        }
        for await (const convo of searchConversations(query)) {
            if (seenConvo.has(convo.id)) continue;
            seenConvo.add(convo.id);
            if (++convosWalked > MAX_CONVERSATIONS) {
                summary.capped = true;
                log.warn('[front-status-import] conversation cap hit', { cap: MAX_CONVERSATIONS });
                break;
            }

            const messages = await listMessages(convo.id);
            // ONE suggestion per CONVERSATION. Pick the most recent inbound message
            // from an accepted sender: it already quotes the whole thread, so a
            // single Gemini call sees the full history while anchoring extraction
            // on the CURRENT message. Processing every sibling message instead
            // caused (a) a last-write race where whichever message landed last
            // won the alert, and (b) stale values (e.g. an old ETA buried in a
            // long quoted chain) resurfacing from a message that wasn't the latest.
            let latestInbound = null;
            for (const msg of messages) {
                if (!msg.is_inbound) continue;
                if (!targets.accept(messageFromEmail(msg))) continue;
                if (!latestInbound || (msg.created_at || 0) > (latestInbound.created_at || 0)) latestInbound = msg;
            }
            for (const m of (latestInbound ? [latestInbound] : [])) {
                if (!m.is_inbound) continue;
                if ((m.created_at || 0) * 1000 < cutoffMs) continue;
                const from = messageFromEmail(m);
                if (!targets.accept(from)) continue;
                if (seenMsg.has(m.id)) continue;
                seenMsg.add(m.id);

                const sourceRef = `front:${m.id}`;
                if (!force) {
                    const [existing] = await conn.query(
                        `SELECT id FROM front_status_imports WHERE source_ref = ? LIMIT 1`, [sourceRef]
                    );
                    if (existing.length) continue; // already processed this message
                }

                summary.scanned += 1;
                const subjectRaw = convo.subject || '';
                const subject = cleanSubject(subjectRaw);
                const text = messageText(m);
                const receivedAt = m.created_at ? new Date(m.created_at * 1000) : null;
                const receivedAtIso = receivedAt ? receivedAt.toISOString() : null;
                const bodyFull = String(text || '').slice(0, BODY_FULL_CAP);
                const baseRow = {
                    sourceRef, conversationId: convo.id, messageId: m.id, fromEmail: from,
                    subject, receivedAt,
                    sourceMeta: {
                        from, subject: subjectRaw, frontUrl: frontWebUrl(convo.id),
                        receivedAt: receivedAtIso, body: messageBodySnippet(m),
                        // Full message text Gemini read; highlights filled post-extraction.
                        bodyFull, highlights: [],
                    },
                };

                // Gate 4: keyword pre-filter — chatter never reaches Gemini.
                if (!STATUS_HINT_RE.test(`${subjectRaw}\n${text}`)) {
                    summary.skipped.chatter += 1;
                    if (!dryRun) await recordImport(conn, { ...baseRow, outcome: 'skipped_chatter' });
                    continue;
                }

                // Gate 5: per-run Gemini cap. Overflow stays undeduped -> next run.
                if (summary.geminiCalls >= maxGemini) {
                    summary.capped = true;
                    seenMsg.delete(m.id);
                    break;
                }

                let extract;
                try {
                    summary.geminiCalls += 1;
                    extract = await extractStatusFromEmail(text, { model, subject: subjectRaw, fromEmail: from, emailDate: receivedAtIso });
                } catch (e) {
                    summary.failed += 1;
                    log.error('[front-status-import] gemini failed', { messageId: m.id, error: e.message });
                    if (!dryRun) await recordImport(conn, { ...baseRow, outcome: 'failed', errorMessage: e.message });
                    continue;
                }

                // Verbatim evidence quotes — store on the shared sourceMeta so both
                // the provenance row and (via ctx) the alert carry them.
                baseRow.sourceMeta.highlights = extract.highlights || [];
                const enrich = {
                    ...baseRow,
                    extractedPo: extract.poNumber, poFound: extract.poNumberFound,
                    inferredStatus: extract.milestone, statusStated: extract.milestoneStated || extract.qcFailed,
                    confidence: extract.confidence, rationale: extract.rationale,
                    modelUsed: extract.modelUsed, usage: extract.usage,
                };
                // Per-message detail (subject/sender) — opt-in so routine hourly
                // CloudWatch logs stay light on PII; invaluable on a manual run to
                // see exactly what Gemini decided for each email.
                if (verbose) {
                    log.info('[front-status-import] examined', {
                        from, subject: subject.slice(0, 70),
                        po: extract.poNumber, poFound: extract.poNumberFound,
                        milestone: extract.milestone, qcFailed: extract.qcFailed,
                        conf: Math.round(extract.confidence * 100) / 100,
                        fields: Object.values(extract.fields).filter(v => v != null).length,
                        rationale: (extract.rationale || '').slice(0, 90),
                    });
                }

                // Anti-hallucination backstop: even when the model claims a PO,
                // verify it ACTUALLY appears in the email (normalised on both
                // sides). A PO the model invented won't be present in the text.
                const poInText = extract.poNumberFound
                    && normalizePo(extract.poNumber).length >= 3
                    && normalizePo(`${subjectRaw}\n${text}`).includes(normalizePo(extract.poNumber));
                // Forwarders cite a container/booking number instead of a PO.
                const containerRef = extract.fields.containerNumber || null;
                // Pure-data emails (e.g. a forwarder ETA update) carry no milestone
                // but still have something to apply.
                const hasData = Object.values(extract.fields).some(v => v != null);

                // Need a way to MATCH (PO or container) AND something to say
                // (a milestone, a QC-fail, or a data value).
                if (!poInText && !containerRef) {
                    summary.skipped.nopo += 1;
                    if (!dryRun) await recordImport(conn, { ...enrich, outcome: 'skipped_nopo' });
                    continue;
                }
                if (!extract.milestoneStated && !extract.qcFailed && !hasData) {
                    summary.skipped.nochange += 1;
                    if (!dryRun) await recordImport(conn, { ...enrich, outcome: 'skipped_nochange' });
                    continue;
                }
                // Confidence gates STATUS MOVES (milestone/QC-fail); pure data
                // updates are lower-stakes field changes and aren't gated on it.
                if ((extract.milestoneStated || extract.qcFailed) && extract.confidence < confMin) {
                    summary.skipped.lowconf += 1;
                    if (!dryRun) await recordImport(conn, { ...enrich, outcome: 'skipped_lowconf' });
                    continue;
                }

                // Match by PO first, then the container/booking number, then the
                // product (JF) code — suppliers often reference an order by its
                // SKU ("JF0230") rather than the PO (the extractor puts such a code
                // in poNumber, and poInText has confirmed it's really in the email).
                // A JF code can span many order rows, so matchOrdersByJfCode is only
                // TRUSTED when it resolves to exactly ONE live order; a wider match
                // is recorded as ambiguous rather than guessed across order lines.
                let matched = poInText ? await matchOrdersByPo(conn, extract.poNumber) : [];
                if (!matched.length && containerRef) matched = await matchOrdersByContainer(conn, containerRef);
                if (!matched.length && poInText && /^[A-Z]{2,4}\d{2,}/.test(normalizePo(extract.poNumber))) {
                    const byJf = await matchOrdersByJfCode(conn, extract.poNumber);
                    if (byJf.length === 1) {
                        matched = byJf;
                        if (verbose) log.info('[front-status-import] matched by JF code', { jf: extract.poNumber, order: byJf[0].id });
                    } else if (byJf.length > 1) {
                        summary.skipped.ambiguousJf += 1;
                        log.info('[front-status-import] ambiguous JF — skipped', { jf: extract.poNumber, liveOrders: byJf.length, from });
                        if (!dryRun) await recordImport(conn, {
                            ...enrich, outcome: 'skipped_ambiguous_jf',
                            matchedOrderCount: 0, orderIds: byJf.map(o => o.id),
                        });
                        continue;
                    }
                }
                if (!matched.length) {
                    summary.skipped.nomatch += 1;
                    if (!dryRun) await recordImport(conn, { ...enrich, outcome: 'skipped_nomatch' });
                    continue;
                }

                // Split PO: when an email cites a container (a transit milestone
                // OR a data-only forwarder update), keep only the part physically
                // in it — don't touch the sibling half still in the factory (see
                // narrowSplitByContainer).
                if (matched.length > 1 && containerRef) {
                    const narrowed = narrowSplitByContainer(matched, containerRef, extract.milestone);
                    if (narrowed.length < matched.length) {
                        log.info('[front-status-import] split PO narrowed to container', {
                            po: extract.poNumber || null, container: containerRef,
                            milestone: extract.milestone, from: matched.length, to: narrowed.length,
                        });
                        matched = narrowed;
                    }
                }

                const ctx = {
                    messageId: m.id, conversationId: convo.id, fromEmail: from,
                    subject: subjectRaw, frontUrl: frontWebUrl(convo.id),
                    receivedAtIso, sourceRef,
                    bodyFull, highlights: extract.highlights || [],
                };
                // For each matched order, turn the email's milestone into a
                // RULES-VALID non-admin move (exactly one step forward / a permitted
                // lateral or backward / blocked for FQC). Only valid moves become
                // alerts; we attach the fields the email provides for that move.
                // Pass 2: refine ALL matched orders for this email together —
                // batched when one email matches many orders (a consolidated
                // container) and run with bounded CONCURRENCY. Capped by the per-run
                // call budget + a wall-clock deadline so it can't run past the Lambda
                // timeout; any order not refined (over budget / failed / omitted by
                // the model) falls back to the Pass-1 `extract`.
                let refinedByOrder = new Map();
                if (refine) {
                    const remaining = maxRefine - refineCalls;
                    if (remaining > 0 && (Date.now() - runStartMs) < refineBudgetMs) {
                        try {
                            const r = await refineMatchedOrders(conn, {
                                orders: matched, pass1: extract, model: refineModel,
                                email: { text: bodyFull, subject: subjectRaw, fromEmail: from, receivedAtIso },
                                maxCalls: remaining, deadlineMs: runStartMs + refineBudgetMs,
                            });
                            refinedByOrder = r.refined;
                            refineCalls += r.calls;
                            summary.refineSkipped += r.skipped;
                            if (verbose) log.info('[front-status-import] refined', {
                                matched: matched.length, refined: r.refined.size, calls: r.calls, skipped: r.skipped,
                            });
                        } catch (e) {
                            log.warn('[front-status-import] refine batch failed — Pass 1 for all matched', { error: e.message });
                        }
                    } else {
                        if (summary.refineSkipped === 0) {
                            log.warn('[front-status-import] refine budget reached — remaining orders use Pass-1 only', { maxRefine, refineBudgetMs, refineCalls });
                        }
                        summary.refineSkipped += matched.length;
                    }
                }

                const alertIds = [];
                let builtCount = 0, fqcBlocked = 0, noChange = 0;
                for (const order of matched) {
                    const isFqc = T.isFqcOrder(order);
                    const ex = refinedByOrder.get(order.id) || extract;
                    const sug = T.suggestionFor(order.status, ex.milestone, { isFqc, qcFailed: ex.qcFailed });

                    // Daisy must not auto-suggest a move INTO READY_FOR_QC while the
                    // estimated ready date is still in the future: production isn't
                    // finished, so there's nothing to inspect yet. Downgrade to a no-move
                    // data_update (the ready date the email carries still gets captured)
                    // rather than propose a premature QC move. Prefer the date this email
                    // provides, else the order's current one.
                    if (sug.target === 'READY_FOR_QC') {
                        const erd = asDateStr((ex.fields && ex.fields.estimatedReadyDate) ?? order.estimated_ready_date);
                        if (erd && erd > today) { sug.target = null; sug.category = null; sug.remaining = null; }
                    }

                    // Field list: for a status MOVE, the target's gate/milestone
                    // fields PLUS the stage data fields; for NO move, just the stage
                    // data fields (e.g. an estimated ready date) — which become a
                    // 'data_update' suggestion if any actually change.
                    let target, category, remaining, fieldNames;
                    if (sug.target) {
                        target = sug.target; category = sug.category; remaining = sug.remaining;
                        // A backward QC-failed rework is not gated and carries no
                        // gate fields (the production fields already exist).
                        const gateF = category === 'backward_qc_failed' ? [] : T.applicableFieldsFor(target);
                        fieldNames = [...new Set([...gateF, ...T.applicableDataFields(target)])];
                    } else {
                        target = order.status; category = 'data_update'; remaining = [];
                        fieldNames = T.applicableDataFields(order.status);
                    }
                    const fieldUpdates = await computeFieldUpdates(conn, order, fieldNames, ex.fields, today);

                    // Nothing to do: no rules-valid move AND no data change.
                    if (!sug.target && fieldUpdates.apply.length === 0) {
                        if (sug.category === 'blocked_fqc') fqcBlocked += 1; else noChange += 1;
                        continue;
                    }

                    const appliedMap = {};
                    for (const a of fieldUpdates.apply) appliedMap[a.field] = a.to;
                    const missing = sug.target
                        ? T.missingGateFields(order, target, order.status, { applied: appliedMap, hasQcReport: !!order.has_qc_report })
                        : [];
                    const alert = buildSuggestionAlert(order, ex, { target, category, remaining }, fieldUpdates, missing, ctx, today);
                    if (!dryRun) {
                        const { id } = await upsertSuggestionAlert(conn, alert);
                        if (id) alertIds.push(id);
                    }
                    builtCount += 1;
                    summary.suggestions.push({
                        orderId: order.id, poNumber: order.po_number,
                        from: order.status, to: target, category,
                        fields: fieldUpdates.apply.length, missing: missing.length,
                        confidence: ex.confidence,
                    });
                    log.info('[front-status-import] SUGGESTION', {
                        order: order.id, po: order.po_number,
                        change: sug.target ? `${order.status} -> ${target}` : `data_update @ ${order.status}`,
                        category, fields: fieldUpdates.apply.length, needs: missing, dryRun,
                    });
                }

                if (builtCount > 0) {
                    summary.alerted += 1;
                    summary.alertsCreated += dryRun ? 0 : alertIds.length;
                    if (!dryRun) {
                        await recordImport(conn, {
                            ...enrich, outcome: 'alerted',
                            matchedOrderCount: builtCount, orderIds: matched.map(o => o.id), alertIds,
                        });
                    }
                } else {
                    // No rules-valid move for any matched order.
                    const fqcOnly = fqcBlocked > 0 && noChange === 0;
                    if (fqcOnly) summary.skipped.fqc += 1; else summary.skipped.nochange += 1;
                    if (!dryRun) {
                        await recordImport(conn, {
                            ...enrich, outcome: fqcOnly ? 'skipped_fqc' : 'skipped_nochange',
                            matchedOrderCount: 0, orderIds: matched.map(o => o.id),
                        });
                    }
                }
            }
            if (summary.capped) break;
        }
        if (summary.capped) break;
    }

    // Advance the incremental watermark ONLY when we fully drained the window
    // (not capped) and actually persisted (not dryRun). A capped run leaves the
    // watermark where it was, so the next run re-scans the same window and works
    // through the leftover tail (those messages have no source_ref row yet, so
    // advancing past them would skip them forever).
    summary.watermarkAdvanced = !dryRun && !summary.capped;
    if (summary.watermarkAdvanced) {
        await setLastRunTs(conn, Math.floor(runStartMs / 1000));
    }
    summary.refineCalls = refineCalls;
    summary.elapsedMs = Date.now() - runStartMs;
    return summary;
}

module.exports = {
    importStatusUpdatesFromFront,
    ensureSchema,
    matchOrdersByPo,
    matchOrdersByContainer,
    matchOrdersByJfCode,
    narrowSplitByContainer,
    loadOrderForSuggestion,
    buildSuggestionAlert,
    normalizePo,
    buildFrontTargets,
    getLastRunMs,
    setLastRunTs,
    lotExpiryConflict,
    computeFieldUpdates,
    INFERRABLE_MILESTONES,
};
