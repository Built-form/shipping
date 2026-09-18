const serverless = require('serverless-http');
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const compression = require('compression');
const axios = require('axios');
const { getPool } = require('../db');
const log = require('../lib/logger');
const { ensureSupplierPortalCodes } = require('../lib/portal-code');
const { getProductsByJfCode, createAsn, receiveAsnItems } = require('../services/mintsoft');
const { snapshotJfCode } = require('./mintsoft-snapshot');
const {
    buildDraftContainerPdf, buildForwarderQuotePdf, buildSupplierQuotePdf, buildQualityAssurancePdf,
    buildDraftContainerCsv, buildForwarderQuoteCsv, buildSupplierQuoteCsv, buildQualityAssuranceCsv,
} = require('../services/draft-container-pdf');
// Order-shaping helpers, audit, and the receive transaction live in shared
// modules so the public carton-scan Lambda reuses the exact same projection +
// Mintsoft receiving path (see src/services/order-receive.js).
const { ORDER_SELECT, parseDates, formatDate, formatDateTime, normalizeExpiry, receiptToJson, rowToOrder } = require('../lib/order-shape');
const { recordAudit } = require('../lib/audit');
// Draft-container registry + event writer: every draft mutation lands in
// audit_log as entity_type 'draft_container' against a stable registry id, so
// a draft's history outlives renames, conversion and deletion.
const draftAudit = require('../lib/draft-audit');
const {
    ensureEmailReceiptsSchema, apiBaseUrlFromReq, createEmailReceipt,
    linkReceiptToSend, appendReceiptLink,
    getReceiptForResend, recordReminderSend, listReminders,
} = require('../lib/email-receipt');
const { sendReminderForReceipt } = require('../services/receipt-reminders');
const { receiveOrderStock, ReceiveError } = require('../services/order-receive');
const { listWarehousesWithLocations, listLocationsForWarehouse } = require('../services/mintsoft-locations');
// Daily alerts (slide-out alert window). Generation logic lives in the service
// so the nightly Lambda reuses it; here we only expose the read/ack routes.
const { ensureDailyAlertsSchema, listAlerts, listHistory, actOnAlert, getSuggestionForUpdate, actOnSuggestion, londonToday } = require('../services/daily-alerts');
// This app's own user allowlist: the table the auth middleware below checks,
// plus the /api/v1/users CRUD that manages it.
const { ensureAllowedEmailsSchema, lookupUserType, registerUserRoutes } = require('../lib/allowed-emails');
const T = require('../lib/order-transitions');
// Shipments: one row per physical movement of goods. Through rollout step 3
// it is a derived shadow of the legacy container columns and the draft /
// planned tables: the routes that write those call one fail-soft hook
// (shipmentSync.shadow) that is inert until the backfill arms it. See
// src/services/shipment-sync.js and the /api/v1/shipments routes registered at
// the bottom of this file.
const shipmentSync = require('../services/shipment-sync');
const { registerShipmentRoutes } = require('../services/shipment-routes');
const { makeSplitOrder } = require('../services/order-split');
const shipmentsLib = require('../lib/shipments');

const app = express();

// ── Middleware ────────────────────────────────────────────────────────────
app.use(compression());
app.use(cors({ exposedHeaders: ['X-User-Type'] }));
// 12mb covers invoice/payment uploads (base64 PDFs/XLSX). The per-route
// express.json({limit:'12mb'}) overrides further down are now redundant —
// they're dead code anyway because this app-wide parser claims the body
// first and any limit set here is the one that matters. API Gateway's own
// payload cap (6mb HTTP API / 10mb REST) is the real ceiling.
app.use(express.json({ limit: '12mb' }));

// Database pool — declared before middleware that depends on it
const pool = getPool();

// Lazily migrate order_receipts to support idempotent retries. The unique
// (order_id, idempotency_key) index is what enforces dedupe; NULL keys are
// treated as distinct by MySQL so legacy rows without a key still coexist.
const orderReceiptsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        const migrations = [
            `ALTER TABLE order_receipts ADD COLUMN idempotency_key VARCHAR(64) NULL AFTER asn_item_id`,
            `ALTER TABLE order_receipts ADD UNIQUE KEY uk_order_idempotency (order_id, idempotency_key)`,
            `ALTER TABLE order_receipts ADD COLUMN type VARCHAR(16) NOT NULL DEFAULT 'received'`,
            // Did these units go into Mintsoft quarantine rather than normal
            // sellable stock? Reflects what Mintsoft actually did.
            `ALTER TABLE order_receipts ADD COLUMN quarantined TINYINT(1) NOT NULL DEFAULT 0`,
        ];
        for (const sql of migrations) {
            try { await conn.query(sql); } catch (e) {
                const msg = e.message || '';
                if (!msg.includes('Duplicate column') && !msg.includes('Duplicate key name')) throw e;
            }
        }
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] order_receipts schema migration failed', err));

// Give stock_snapshots a real "last refreshed" timestamp (bumped on every
// upsert) so the stock-sum views can gate order receipts against the cached
// Mintsoft number — netting a receipt only once the snapshot reflects it.
const stockSnapshotsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(
            `ALTER TABLE stock_snapshots ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP`
        );
    } catch (e) {
        if (!(e.message || '').includes('Duplicate column')) throw e;
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] stock_snapshots schema migration failed', err));

// Manually adjusted sales figures — an ops override of the sales number used
// for an ASIN in a given marketplace, held at the same (asin, country) grain as
// amazon_stock_country_snapshots. Purely a store: nothing in this API consumes
// the figure, it's read back through the /adjusted-sales routes.
//
// One live row per (asin, country); country 'ALL' is the reserved cross-market
// entry. The unique key covers soft-deleted rows too, so re-creating a deleted
// key revives that row rather than inserting a second one (see POST below).
const adjustedSalesSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS adjusted_sales (
                id INT NOT NULL AUTO_INCREMENT,
                asin VARCHAR(20) NOT NULL,
                country VARCHAR(8) NOT NULL,
                adjusted_sales DECIMAL(12,2) NOT NULL,
                note VARCHAR(500) NULL,
                created_by_email VARCHAR(255) NULL,
                updated_by_email VARCHAR(255) NULL,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uk_asin_country (asin, country),
                KEY idx_asin (asin)
            )
        `);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] adjusted_sales schema migration failed', err));

// Lazy-create the daily_alerts table (shared with the nightly generator). Each
// alert route awaits this before touching the table.
const dailyAlertsSchemaReady = ensureDailyAlertsSchema(pool)
    .catch(err => log.error('[orders] daily_alerts schema migration failed', err));

// Lazy-create the audit_log table. Captures before/after JSON for every
// mutation on orders and purchase_orders performed through this API. The
// Asana importer is intentionally NOT audited (TRUNCATE + bulk insert every
// 10 min would spam the table).
const auditLogSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS audit_log (
                id BIGINT NOT NULL AUTO_INCREMENT,
                entity_type VARCHAR(32) NOT NULL,
                entity_id INT NOT NULL,
                action VARCHAR(16) NOT NULL,
                before_json JSON NULL,
                after_json JSON NULL,
                user_email VARCHAR(255) NULL,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_entity (entity_type, entity_id, created_at)
            )
        `);
        // `action` started life as VARCHAR(16), which silently truncated longer
        // action names in non-strict MySQL (e.g. 'qc_report_downloaded' was
        // stored as 'qc_report_downlo', so ?action= filtering never matched).
        // Widen it — data-preserving and idempotent. Existing already-truncated
        // rows stay as-is; new writes keep the full name.
        try { await conn.query(`ALTER TABLE audit_log MODIFY COLUMN action VARCHAR(64) NOT NULL`); }
        catch (e) { log.warn('[orders] audit_log action widen skipped', e.message); }
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] audit_log schema migration failed', err));

// Join table linking orders to user-named "draft containers" — a planning
// step before a real container is booked. One order can sit in many drafts
// with different allocated quantities; (order_id, draft_container_name) is
// unique so the same order can't appear twice in the same draft.
const draftContainerAllocationsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS draft_container_allocations (
                id INT NOT NULL AUTO_INCREMENT,
                order_id INT NOT NULL,
                draft_container_name VARCHAR(100) NOT NULL,
                allocated INT NOT NULL DEFAULT 0,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_order_draft (order_id, draft_container_name),
                KEY idx_draft_name (draft_container_name),
                KEY idx_order_id (order_id)
            )
        `);
        // Versioned PDFs of a draft container, used to request delivery
        // quotes from freight forwarders. Mirrors purchase_order_documents.
        // The draft is identified by name (no parent table — drafts are
        // implicit, defined by the existence of allocations).
        await conn.query(`
            CREATE TABLE IF NOT EXISTS draft_container_documents (
                id INT NOT NULL AUTO_INCREMENT,
                draft_container_name VARCHAR(100) NOT NULL,
                version INT NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                file_size INT NULL,
                generated_by_email VARCHAR(255) NULL,
                generated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_draft_version (draft_container_name, version)
            )
        `);
        // One row per send attempt (same as purchase_order_document_sends).
        await conn.query(`
            CREATE TABLE IF NOT EXISTS draft_container_document_sends (
                id INT NOT NULL AUTO_INCREMENT,
                draft_container_document_id INT NOT NULL,
                sent_to JSON NOT NULL,
                subject VARCHAR(255) NULL,
                front_message_uid VARCHAR(128) NULL,
                front_conversation_id VARCHAR(64) NULL,
                sent_by_email VARCHAR(255) NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_document_id (draft_container_document_id),
                KEY idx_sent_at (sent_at)
            )
        `);
        // PDF flavour: 'quote' (default delivery quote request),
        // 'forwarder-quote' (detailed carton-level forwarder quote) or
        // 'supplier-quote' (forwarder layout filtered to a single supplier).
        // Versions are sequenced per (draft, type, supplier).
        try {
            await conn.query(
                `ALTER TABLE draft_container_documents ADD COLUMN type VARCHAR(32) NOT NULL DEFAULT 'quote'`
            );
        } catch (e) {
            if (!String(e.message || '').includes('Duplicate column')) throw e;
        }
        // supplier is NULL for 'quote'/'forwarder-quote'; the supplier name for
        // 'supplier-quote' (orders store supplier as a free-text string).
        try {
            await conn.query(
                `ALTER TABLE draft_container_documents ADD COLUMN supplier VARCHAR(255) NULL`
            );
        } catch (e) {
            if (!String(e.message || '').includes('Duplicate column')) throw e;
        }
        // batch_id groups the documents produced by a single generate call (a
        // forwarder-quote plus its per-supplier supplier-quotes), so emailing the
        // forwarder-quote can attach the whole set. NULL for standalone docs.
        try {
            await conn.query(
                `ALTER TABLE draft_container_documents ADD COLUMN batch_id VARCHAR(64) NULL`
            );
        } catch (e) {
            if (!String(e.message || '').includes('Duplicate column')) throw e;
        }
        try {
            await conn.query(
                `ALTER TABLE draft_container_documents ADD KEY idx_batch_id (batch_id)`
            );
        } catch (e) {
            if (!String(e.message || '').includes('Duplicate key')) throw e;
        }
        // CSV companion of the PDF — generated, stored in S3 and served at its own
        // public URL exactly like the PDF. NULL on documents generated before this
        // was added (their email send rebuilds the CSV on the fly as a fallback).
        const draftDocCsvMigrations = [
            `ALTER TABLE draft_container_documents ADD COLUMN csv_s3_key VARCHAR(500) NULL`,
            `ALTER TABLE draft_container_documents ADD COLUMN csv_public_url VARCHAR(1000) NULL`,
            `ALTER TABLE draft_container_documents ADD COLUMN csv_file_size INT NULL`,
        ];
        for (const sql of draftDocCsvMigrations) {
            try { await conn.query(sql); } catch (e) {
                if (!String(e.message || '').includes('Duplicate column')) throw e;
            }
        }
        // Registry of every draft-container name that has ever existed. Drafts
        // are otherwise implicit (defined by their allocation rows), so this is
        // what gives audit_log a stable integer entity_id per draft: renames
        // keep the id, and a row is never deleted — closing (conversion into a
        // real container, or deletion) only stamps closed_* so the history stays
        // reachable after the draft is gone. See src/lib/draft-audit.js.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS draft_containers (
                id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                created_by_email VARCHAR(255) NULL,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                last_activity_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                closed_reason VARCHAR(16) NULL,
                closed_at DATETIME NULL,
                closed_by_email VARCHAR(255) NULL,
                container_number VARCHAR(100) NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uk_name (name),
                KEY idx_last_activity (last_activity_at)
            )
        `);
        // One-shot data migrations claim a row here (INSERT IGNORE inside the
        // migration's own transaction) so they run exactly once across
        // concurrent cold starts.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS app_migrations (
                name VARCHAR(64) NOT NULL,
                applied_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (name)
            )
        `);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] draft_container_allocations schema migration failed', err));

// Join table linking orders to user-named "planned containers" — the same
// planning shape as draft containers (see above) but a separate, independent
// stream, so an order can be planned and drafted at the same time without the
// two lists interfering. Allocations only: planned containers have no document
// or email side, by design. One order can sit in many planned containers with
// different allocated quantities; (order_id, planned_container_name) is unique
// so the same order can't appear twice in the same planned container.
const plannedContainerAllocationsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS planned_container_allocations (
                id INT NOT NULL AUTO_INCREMENT,
                order_id INT NOT NULL,
                planned_container_name VARCHAR(100) NOT NULL,
                allocated INT NOT NULL DEFAULT 0,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_order_planned (order_id, planned_container_name),
                KEY idx_planned_name (planned_container_name),
                KEY idx_order_id (order_id)
            )
        `);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] planned_container_allocations schema migration failed', err));

// Quality Assurance documents — a QC inspection sheet built from an explicit
// set of order ids (not tied to a draft container). Mirrors the draft/PO
// document + sends pattern: versioned PDFs in S3, emailed to suppliers via
// Front. Versions are sequenced per `order_ids_key` (the sorted id set) so
// regenerating the same selection bumps the version.
const qualityAssuranceSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS quality_assurance_documents (
                id INT NOT NULL AUTO_INCREMENT,
                ref VARCHAR(100) NULL,
                version INT NOT NULL,
                draft_container_name VARCHAR(100) NULL,
                order_ids JSON NOT NULL,
                order_ids_key VARCHAR(255) NOT NULL,
                qc_units JSON NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                file_size INT NULL,
                comments TEXT NULL,
                generated_by_email VARCHAR(255) NULL,
                generated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_order_ids_key (order_ids_key, version)
            )
        `);
        await conn.query(`
            CREATE TABLE IF NOT EXISTS quality_assurance_document_sends (
                id INT NOT NULL AUTO_INCREMENT,
                quality_assurance_document_id INT NOT NULL,
                sent_to JSON NOT NULL,
                subject VARCHAR(255) NULL,
                front_message_uid VARCHAR(128) NULL,
                front_conversation_id VARCHAR(64) NULL,
                sent_by_email VARCHAR(255) NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_qa_document_id (quality_assurance_document_id),
                KEY idx_sent_at (sent_at)
            )
        `);
        // draft_container_name was added after the initial release — backfill
        // the column + index on already-created tables. Optional tag set at
        // generate time so the list can be scoped to a draft container.
        const migrations = [
            `ALTER TABLE quality_assurance_documents ADD COLUMN draft_container_name VARCHAR(100) NULL`,
            `ALTER TABLE quality_assurance_documents ADD KEY idx_draft_container_name (draft_container_name)`,
            // CSV companion of the PDF — stored in S3 and served at its own public
            // URL exactly like the PDF. NULL on documents predating this column.
            `ALTER TABLE quality_assurance_documents ADD COLUMN csv_s3_key VARCHAR(500) NULL`,
            `ALTER TABLE quality_assurance_documents ADD COLUMN csv_public_url VARCHAR(1000) NULL`,
            `ALTER TABLE quality_assurance_documents ADD COLUMN csv_file_size INT NULL`,
        ];
        for (const sql of migrations) {
            try { await conn.query(sql); } catch (e) {
                const msg = e.message || '';
                if (!msg.includes('Duplicate column') && !msg.includes('Duplicate key name')) throw e;
            }
        }
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] quality_assurance schema migration failed', err));

// Draft registry + its one-time backfill. The backfill reads allocations,
// draft documents AND quality_assurance_documents, so it waits for both
// schemas; every draft route awaits this instead of the allocations schema so
// a legacy draft is registered (with its reconstructed history) before any new
// event is written against it.
const draftRegistryReady = (async () => {
    await draftContainerAllocationsSchemaReady;
    await qualityAssuranceSchemaReady;
    await auditLogSchemaReady;
    const conn = await pool.getConnection();
    try {
        const { ran } = await draftAudit.backfillDraftRegistry(conn);
        if (ran) log.info('[orders] draft_containers registry backfilled from existing drafts');
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] draft_containers registry backfill failed', err));

// deepEqual / diffSnapshots / recordAudit moved to src/lib/audit.js (imported
// above) so the receive service can write identical before/after audit rows.

// Mirror an order's PO assignment change as PO-level audit events so the
// PO's audit trail shows when lines come and go. The order's own row in
// audit_log already captures the field-level diff; this helper writes the
// sibling purchase_order entries so GET /audit-log filtered by a PO id
// surfaces attachments without scanning every order's history.
function poLineSnapshot(orderLike) {
    if (!orderLike) return null;
    return {
        orderId: orderLike.id,
        jfCode: orderLike.jfCode || null,
        asin: orderLike.asin || null,
        productName: orderLike.productName || null,
        quantity: orderLike.quantity ?? null,
        poNumber: orderLike.poNumber || null,
        supplier: orderLike.supplier || null,
    };
}

async function recordPoAttachmentChange(conn, { before, after, userEmail }) {
    try {
        const beforePo = before && before.purchaseOrderId ? Number(before.purchaseOrderId) : null;
        const afterPo = after && after.purchaseOrderId ? Number(after.purchaseOrderId) : null;
        if (beforePo === afterPo) return;

        const payload = poLineSnapshot(after || before);
        const payloadJson = JSON.stringify(payload);

        if (beforePo) {
            await conn.query(
                `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                ['purchase_order', beforePo, 'order_detached', payloadJson, null, userEmail || null]
            );
        }
        if (afterPo) {
            await conn.query(
                `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                ['purchase_order', afterPo, 'order_attached', null, payloadJson, userEmail || null]
            );
        }
    } catch (err) {
        log.warn('[audit] po-attachment insert failed', {
            orderId: (after || before || {}).id,
            error: err.message,
        });
    }
}

// Lazy-create the po_sent_webhooks dedup table. Persistent (not truncated by
// the Asana import) so a (po_number, jf_code) pair fires the Make webhook
// at most once across the order's lifetime, even if the row is recreated.
const poSentWebhooksSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS po_sent_webhooks (
                po_number VARCHAR(100) NOT NULL,
                jf_code VARCHAR(50) NOT NULL,
                fired_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (po_number, jf_code)
            )
        `);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] po_sent_webhooks schema migration failed', err));

const PO_SENT_WEBHOOK_URL = process.env.MAKE_PO_SENT_WEBHOOK_URL
    || 'https://hook.eu1.make.com/ufykbn3v7pxz4yn9wyrdchy7fumuyifr';

// Claims (po_number, jf_code) atomically via INSERT IGNORE; only the caller
// that actually inserts the row fires the webhook. On webhook failure the
// claim is released so the next save retries. No-op when status isn't PO_SENT
// or when po_number/jf_code is missing.
async function firePoSentWebhookIfNeeded(conn, order) {
    if (!order || order.status !== 'PO_SENT') return;
    const poNumber = order.poNumber;
    const jfCode = order.jfCode;
    if (!poNumber || !jfCode) return;

    const [claim] = await conn.query(
        `INSERT IGNORE INTO po_sent_webhooks (po_number, jf_code) VALUES (?, ?)`,
        [poNumber, jfCode]
    );
    if (claim.affectedRows === 0) return;

    const payload = {
        poNumber,
        poDate: order.poDate || new Date().toISOString().slice(0, 10),
        items: [{ code: jfCode, qty: String(order.quantity ?? 0) }],
    };

    try {
        await axios.post(PO_SENT_WEBHOOK_URL, payload, { timeout: 10_000 });
        log.info('[po-sent-webhook] fired', { poNumber, jfCode });
    } catch (err) {
        log.error('[po-sent-webhook] fire failed; releasing dedup claim for retry', {
            poNumber, jfCode, error: err.message,
        });
        try {
            await conn.query(
                `DELETE FROM po_sent_webhooks WHERE po_number = ? AND jf_code = ?`,
                [poNumber, jfCode]
            );
        } catch (delErr) {
            log.error('[po-sent-webhook] failed to release dedup claim', {
                poNumber, jfCode, error: delErr.message,
            });
        }
    }
}

// suppliers and supplier_emails are now VIEWs over the unified JFPro tables
// (jfpro.suppliers / jfpro.supplier_contacts) — see 2026-06-30_suppliers_views.sql.
// There is nothing to create here. We only ensure every live JFPro supplier has a
// portal access code (the helper writes to jfpro.suppliers; it no longer ALTERs
// anything). Idempotent — the portal Lambda runs the same helper on its own cold
// start. The promise name is kept so existing `await supplierEmailsSchemaReady`
// gates are unchanged.
const supplierEmailsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await ensureSupplierPortalCodes(conn);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] supplier portal-code setup failed', err));

// ── Email templates ──────────────────────────────────────────────────────
// Subject/body HTML used when emailing documents to suppliers/forwarders via
// Front. Previously hardcoded in the send handlers; now stored in the
// email_templates table so ops can edit the copy without a deploy. The send
// handlers read by template_key and fall back to these in-code defaults if
// the row is missing. Subjects support {placeholder} tokens that are
// substituted with per-send values (e.g. {poNumber}) at send time.
const DEFAULT_EMAIL_TEMPLATES = [
    {
        key: 'purchase_order',
        name: 'Purchase Order',
        category: 'Purchasing',
        description: 'Emailed to a supplier with a PO PDF attached. Tokens: {poNumber}.',
        subject: '{poNumber}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please see attached PO. Please send back a PI. I will send artwork shortly.</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'draft_container_quote',
        name: 'Delivery Quote Request',
        category: 'Logistics',
        description: 'Emailed to a freight forwarder with a draft container PDF attached. Tokens: {draftContainerName}.',
        subject: 'Delivery Quote Request – {draftContainerName}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please see attached our delivery quote request. Could you please provide a quote for shipping the listed goods to our UK warehouse?</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'purchase_order_signed_pi',
        name: 'Signed Proforma Invoice',
        category: 'Purchasing',
        description: 'Emailed with a signed proforma invoice (PI_signed) attached. Tokens: {poNumber}.',
        subject: 'Signed PI – {poNumber}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please find attached the signed proforma invoice for your records.</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'quality_assurance',
        name: 'Quality Assurance',
        category: 'Quality',
        description: 'Emailed to a QC inspector/supplier with a quality assurance sheet attached. Tokens: {ref}.',
        subject: 'Quality Assurance – {ref}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please see attached our quality assurance sheet. Could you please carry out QC inspection on the listed items per the QC Units indicated?</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
];
const DEFAULT_TEMPLATES_BY_KEY = new Map(DEFAULT_EMAIL_TEMPLATES.map(t => [t.key, t]));

const emailTemplatesSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS email_templates (
                id INT NOT NULL AUTO_INCREMENT,
                template_key VARCHAR(64) NOT NULL,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(64) NULL,
                description TEXT NULL,
                subject VARCHAR(500) NULL,
                body_html MEDIUMTEXT NOT NULL,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uk_template_key (template_key)
            )
        `);
        // Additive migration for tables that predate the category column.
        try { await conn.query(`ALTER TABLE email_templates ADD COLUMN category VARCHAR(64) NULL AFTER name`); }
        catch (e) { if (!e || e.errno !== 1060) throw e; } // 1060 = duplicate column, already migrated
        // Seed the built-in templates. INSERT IGNORE so an ops edit is never
        // clobbered on redeploy — only missing keys get (re)created.
        for (const t of DEFAULT_EMAIL_TEMPLATES) {
            await conn.query(
                `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
            );
        }
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] email_templates schema migration failed', err));

// Lazy-create the email_receipts table (shared with the public supplier-portal
// Lambda, which serves the confirm-receipt click endpoint). Each send handler
// awaits this before minting a receipt token.
const emailReceiptsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await ensureEmailReceiptsSchema(conn);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] email_receipts schema migration failed', err));

// Resolve an email template by key, falling back to the in-code default if
// the row was deleted or the table isn't ready yet. Never throws — a missing
// template must not block a document send.
async function getEmailTemplate(conn, key) {
    const fallback = DEFAULT_TEMPLATES_BY_KEY.get(key) || null;
    try {
        const [rows] = await conn.query(
            `SELECT subject, body_html FROM email_templates WHERE template_key = ? AND deleted_at IS NULL`,
            [key]
        );
        if (rows.length) {
            return { subject: rows[0].subject, bodyHtml: rows[0].body_html };
        }
    } catch (e) {
        log.warn('[email-templates] lookup failed, using in-code default', { key, error: e.message });
    }
    return { subject: fallback ? fallback.subject : null, bodyHtml: fallback ? fallback.bodyHtml : '' };
}

// Replace {token} placeholders with values from vars. Unknown tokens are
// left intact so a typo'd template surfaces visibly rather than silently
// blanking out part of the subject/body.
function renderTemplate(str, vars) {
    if (str == null) return str;
    return String(str).replace(/\{(\w+)\}/g, (m, k) => (vars[k] != null ? String(vars[k]) : m));
}

// Lazy-create the purchase_orders parent table and the nullable
// orders.purchase_order_id column. No FK constraint — orders is TRUNCATEd
// by the Asana importer every 10 min, which InnoDB blocks on a referencing FK.
const purchaseOrdersSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id INT NOT NULL AUTO_INCREMENT,
                po_number VARCHAR(100) NOT NULL,
                supplier VARCHAR(255) NULL,
                notes TEXT NULL,
                currency CHAR(3) NULL DEFAULT 'USD',
                shipping_total DECIMAL(10,2) NULL DEFAULT 0,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uk_po_number (po_number)
            )
        `);
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_documents (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_id INT NOT NULL,
                version INT NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                file_size INT NULL,
                generated_by_email VARCHAR(255) NULL,
                generated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_po_version (purchase_order_id, version)
            )
        `);
        // One row per send attempt (a given PDF version can be emailed many
        // times — to different suppliers, to corrected addresses, etc.).
        // Captures the recipients, Front's IDs (for cross-referencing with
        // the Front conversation), and the user who triggered the send.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_document_sends (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_document_id INT NOT NULL,
                sent_to JSON NOT NULL,
                subject VARCHAR(255) NULL,
                front_message_uid VARCHAR(128) NULL,
                front_conversation_id VARCHAR(64) NULL,
                sent_by_email VARCHAR(255) NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_document_id (purchase_order_document_id),
                KEY idx_sent_at (sent_at)
            )
        `);
        // Supplier-issued invoices attached to a PO. Many-to-one with
        // purchase_orders. Files live next to the generated PO PDFs in the
        // same public S3 bucket (different prefix). Notes are editable; the
        // file itself is immutable once uploaded — re-upload creates a new row.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_invoices (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_id INT NOT NULL,
                filename VARCHAR(255) NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                content_type VARCHAR(128) NULL,
                file_size INT NULL,
                notes TEXT NULL,
                uploaded_by_email VARCHAR(255) NULL,
                uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_purchase_order_id (purchase_order_id),
                KEY idx_uploaded_at (uploaded_at)
            )
        `);
        // One row per check run — auto-fired on upload + manual re-runs.
        // A given invoice can have many check rows (history of attempts,
        // re-runs after model upgrades, retries after a failed run, etc.).
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_invoice_checks (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_invoice_id INT NOT NULL,
                status ENUM('pending', 'succeeded', 'failed') NOT NULL,
                verdict VARCHAR(32) NULL,
                discrepancy_count INT NULL,
                result_json JSON NULL,
                model_used VARCHAR(64) NULL,
                input_tokens INT NULL,
                output_tokens INT NULL,
                total_tokens INT NULL,
                error_code VARCHAR(64) NULL,
                error_message TEXT NULL,
                triggered_by VARCHAR(32) NULL,
                triggered_by_email VARCHAR(255) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_invoice_id (purchase_order_invoice_id),
                KEY idx_created_at (created_at)
            )
        `);
        // Payment instructions extracted from a supplier invoice (PI): how much
        // to pay now (deposit/upfront or full), by when, and the beneficiary
        // bank details — so a downstream job (e.g. WorldFirst) can arrange the
        // transfer. One row per invoice/PI (UNIQUE on the invoice id): a newly
        // uploaded PI gets its own row. Populated by the Gemini invoice check
        // (folded into the same call). `payment_status` is owned by the operator
        // / payment job and is deliberately NOT overwritten when a re-check
        // re-extracts the terms.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_invoice_payments (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_invoice_id INT NOT NULL,
                purchase_order_id INT NOT NULL,
                payment_type VARCHAR(32) NULL,
                amount_due DECIMAL(14,2) NULL,
                currency CHAR(3) NULL,
                deposit_percentage DECIMAL(6,3) NULL,
                invoice_total DECIMAL(14,2) NULL,
                due_date DATE NULL,
                due_terms VARCHAR(500) NULL,
                beneficiary_name VARCHAR(255) NULL,
                bank_name VARCHAR(255) NULL,
                bank_address VARCHAR(500) NULL,
                account_number VARCHAR(100) NULL,
                iban VARCHAR(64) NULL,
                swift_bic VARCHAR(32) NULL,
                intermediary_bank VARCHAR(255) NULL,
                payment_reference VARCHAR(255) NULL,
                raw_terms_text TEXT NULL,
                payment_status ENUM('pending', 'arranged', 'paid', 'skipped') NOT NULL DEFAULT 'pending',
                extracted_from_check_id INT NULL,
                model_used VARCHAR(64) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_invoice (purchase_order_invoice_id),
                KEY idx_purchase_order_id (purchase_order_id),
                KEY idx_payment_status (payment_status)
            )
        `);
        // Payment documents attached to a PO (bank transfer confirmations,
        // SWIFT receipts, wire proofs, etc.). Same shape as invoices —
        // separate table to keep concerns clean and audit trails distinct.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_payments (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_id INT NOT NULL,
                filename VARCHAR(255) NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                content_type VARCHAR(128) NULL,
                file_size INT NULL,
                notes TEXT NULL,
                uploaded_by_email VARCHAR(255) NULL,
                uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_purchase_order_id (purchase_order_id),
                KEY idx_uploaded_at (uploaded_at)
            )
        `);
        // Signed proforma invoices attached to a PO ("PI_signed"). Same shape
        // as purchase_order_invoices — a supplier-/buyer-signed copy of the PI,
        // uploaded as a file, separate table to keep its audit trail distinct.
        // No Gemini PO-vs-PI check (it's a signed copy, not for verification).
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_signed_pis (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_id INT NOT NULL,
                filename VARCHAR(255) NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                content_type VARCHAR(128) NULL,
                file_size INT NULL,
                notes TEXT NULL,
                uploaded_by_email VARCHAR(255) NULL,
                uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_purchase_order_id (purchase_order_id),
                KEY idx_uploaded_at (uploaded_at)
            )
        `);
        // One row per send attempt for a signed PI (same as the document sends
        // tables). A given signed PI can be emailed many times.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS purchase_order_signed_pi_sends (
                id INT NOT NULL AUTO_INCREMENT,
                purchase_order_signed_pi_id INT NOT NULL,
                sent_to JSON NOT NULL,
                subject VARCHAR(255) NULL,
                front_message_uid VARCHAR(128) NULL,
                front_conversation_id VARCHAR(64) NULL,
                sent_by_email VARCHAR(255) NULL,
                sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_signed_pi_id (purchase_order_signed_pi_id),
                KEY idx_sent_at (sent_at)
            )
        `);
        // The buyer-side company a PO is issued by/for. Customer Details block
        // on the PDF reads from here when the PO has a company_id set.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS companies (
                id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(255) NOT NULL,
                address_lines JSON NULL,
                country VARCHAR(100) NULL,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_name (name)
            )
        `);
        // Seed JFA Medical + Hangerworld so the dropdown is non-empty on first
        // run. Legal names + addresses sourced from Companies House. Both
        // entities share the Cornford Road premises.
        // INSERT IGNORE → re-running is harmless once names exist; existing
        // rows aren't overwritten so manual UI edits are preserved.
        await conn.query(
            `INSERT IGNORE INTO companies (name, address_lines, country) VALUES
                ('JFA Medical Ltd', ?, 'UNITED KINGDOM'),
                ('Hangerworld Ltd', ?, 'UNITED KINGDOM')`,
            [
                JSON.stringify(['Unit B, Prestige House', 'Cornford Road', 'Blackpool', 'Lancashire', 'FY4 4QQ']),
                JSON.stringify(['Unit B, Prestige House', 'Cornford Road', 'Blackpool', 'Lancashire', 'FY4 4QQ']),
            ]
        );
        const migrations = [
            `ALTER TABLE orders ADD COLUMN purchase_order_id INT NULL`,
            `ALTER TABLE orders ADD KEY idx_purchase_order_id (purchase_order_id)`,
            `ALTER TABLE orders ADD COLUMN unit_price DECIMAL(10,4) NULL`,
            `ALTER TABLE orders ADD COLUMN actual_ready_date DATE NULL`,
            `ALTER TABLE orders ADD COLUMN estimated_departure_date DATE NULL`,
            // Promoted from dates JSON to flat scalar columns. Convention:
            // flat = the date value itself; dates JSON = status-transition
            // timestamps (manufacturing, ready, consolidated, delivered, etc).
            `ALTER TABLE orders ADD COLUMN shipped_date DATE NULL`,
            `ALTER TABLE orders ADD COLUMN ordered_date DATE NULL`,
            `ALTER TABLE orders ADD COLUMN estimated_ready_date DATE NULL`,
            `ALTER TABLE orders ADD COLUMN artwork_confirmed_date DATE NULL`,
            // AWB (Air Waybill) number, frontend-entered. Air analog of
            // external_container_number; joined to air_shipments by shipsgo-air.
            `ALTER TABLE orders ADD COLUMN awb_number VARCHAR(50) NULL`,
            `ALTER TABLE purchase_orders ADD COLUMN currency CHAR(3) NULL DEFAULT 'USD'`,
            `ALTER TABLE purchase_orders ADD COLUMN shipping_total DECIMAL(10,2) NULL DEFAULT 0`,
            `ALTER TABLE purchase_orders ADD COLUMN company_id INT NULL`,
            `ALTER TABLE purchase_orders ADD KEY idx_company_id (company_id)`,
            `ALTER TABLE purchase_orders ADD COLUMN deleted_at DATETIME NULL`,
            `ALTER TABLE purchase_order_documents ADD COLUMN public_url VARCHAR(1000) NULL`,
            // NOTE: `allowed_emails` (joshdex's shared table) is deliberately no
            // longer migrated from here — this app owns shipping_allowed_emails
            // instead. See src/lib/allowed-emails.js.
            // Widen the check status enum to include 'pending' — a row is
            // inserted at upload time and updated in the background once
            // Gemini settles. MODIFY is idempotent across cold starts.
            `ALTER TABLE purchase_order_invoice_checks MODIFY COLUMN status ENUM('pending', 'succeeded', 'failed') NOT NULL`,
            // Widen delivery_date DATE → DATETIME so it carries a time-of-day.
            // MODIFY is idempotent; existing date-only values become midnight.
            `ALTER TABLE orders MODIFY COLUMN delivery_date DATETIME NULL`,
        ];
        for (const sql of migrations) {
            try { await conn.query(sql); } catch (e) {
                const msg = e.message || '';
                if (!msg.includes('Duplicate column') && !msg.includes('Duplicate key name')) throw e;
            }
        }
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] purchase_orders schema migration failed', err));

// Shipments tables + the shipment_id columns on orders, draft_containers,
// draft_container_documents and quality_assurance_documents. The migration
// files (src/db/migrations/2026-09-18_*) are hand-applied before each deploy;
// this is the fallback for a fresh environment. The prerequisite schemas are
// awaited BEFORE taking a connection: with connectionLimit 1, holding it while
// waiting would hang every later request in this warm Lambda.
const shipmentsSchemaReady = (async () => {
    await draftContainerAllocationsSchemaReady;
    await qualityAssuranceSchemaReady;
    await purchaseOrdersSchemaReady;
    const conn = await pool.getConnection();
    try {
        await shipmentSync.ensureShipmentsSchema(conn);
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] shipments schema migration failed', err));

// Failure-row key for an order-membership hook: the shipment reference when the
// orders carry one (so a bulk operation failing repeatedly is one row with a
// counter), else the order ids.
function membershipKey(orders, reference = null) {
    const ref = shipmentsLib.clean(reference);
    if (ref) return { keyKind: 'reference', keyValue: ref };
    const refs = [...new Set((orders || []).map(o => shipmentsLib.clean(o && o.containerNumber)).filter(Boolean))].sort();
    if (refs.length) return { keyKind: 'reference', keyValue: refs.join(',') };
    return {
        keyKind: 'order',
        keyValue: (orders || []).map(o => o && o.id).filter(Boolean).sort((a, b) => a - b).join(','),
    };
}

// Put the shipmentId a membership hook settled on into the response orders.
// Called only after the route's audit rows and webhook, so it never reaches
// either.
function patchShipmentIds(orders, byOrderId) {
    if (!byOrderId) return;
    for (const o of orders) {
        if (o && Object.prototype.hasOwnProperty.call(byOrderId, o.id)) o.shipmentId = byOrderId[o.id];
    }
}

// ── Email whitelist middleware ────────────────────────────────────────────
// Checks `shipping_allowed_emails` — THIS app's own table, managed through
// /api/v1/users. It used to read `allowed_emails`, which joshdex's API creates
// and manages in the same schema; that list is no longer consulted here beyond
// the one-time seed in src/lib/allowed-emails.js.
const IS_LOCAL = process.env.IS_OFFLINE || process.env.NODE_ENV === 'development';

// Create + seed the table eagerly rather than on the first request, so a cold
// start doesn't pay for it inside an auth check.
const allowedEmailsSchemaReady = ensureAllowedEmailsSchema(pool)
    .catch(err => log.error('[orders] shipping_allowed_emails schema migration failed', err));

app.use(async (req, res, next) => {
    if (IS_LOCAL) {
        req.userEmail = 'local@dev';
        // Local-only escape hatch so dev can exercise the admin-gated routes.
        // Defaults to 'standard'; this branch never runs in production.
        req.userType = process.env.LOCAL_USER_TYPE || 'standard';
        res.set('X-User-Type', req.userType);
        return next();
    }

    try {
        const email = req.requestContext?.authorizer?.jwt?.claims?.email;
        if (!email) {
            return res.status(401).json({ error: 'Unauthorized: no email in token.' });
        }

        await allowedEmailsSchemaReady;
        const userType = await lookupUserType(pool, email);
        if (userType === null) {
            return res.status(401).json({ error: 'Unauthorized: email not in allowlist.' });
        }

        req.userEmail = email.toLowerCase();
        req.userType = userType;
        res.set('X-User-Type', userType);
        next();
    } catch (err) {
        log.error('[auth-middleware]', err);
        res.status(500).json({ error: 'Internal authentication error.' });
    }
});

// Cache-Control — internal admin tool, freshness > network savings. The HTTP
// cache only spares the browser a round-trip; the server still runs every
// query, so there's no backend cost to disabling it. If a future read becomes
// expensive enough to warrant caching, do it server-side (LRU keyed by the
// query's natural cache key), not via Cache-Control.
app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store');
    next();
});

// ── User allowlist CRUD (/api/v1/users) ──────────────────────────────────
// Manages shipping_allowed_emails — the same table the middleware above just
// checked, so a grant or revoke lands on the affected user's next request.
// Admin-only apart from GET /api/v1/users/me. See src/lib/allowed-emails.js.
registerUserRoutes(app, pool);

// ── Constants ────────────────────────────────────────────────────────────

const STATUS_DATE_KEY = {
    PLANNING: 'planned',
    PO_RAISED: 'ordered',
    MANUFACTURING: 'manufacturing',
    READY: 'ready',
    READY_FOR_QC: 'ready_for_qc',
    CONSOLIDATED: 'consolidated',
    IN_TRANSIT: 'shipped',
    DELIVERED: 'delivered',
    COMPLETED: 'completed',
    IN_WAREHOUSE: 'received_by_warehouse',
    RECEIVED: 'received',
    PARTIALLY_RECEIVED: 'received',
};

const ALL_STATUSES = [
    'SCHEDULED', 'PO_SENT', 'IN_PRODUCTION', 'READY', 'READY_FOR_QC',
    'CONSOLIDATED', 'ON_SEA', 'ON_AIR', 'IN_WAREHOUSE', 'MINTSOFT',
    'PARTIALLY_RECEIVED', 'RECEIVED',
];

const ASIN_PATTERN = /^[A-Z0-9]{10}$/;
const QUERY_TIMEOUT_MS = 10_000;

const UPDATABLE_FIELDS = {
    jfCode: 'jf_code',
    asin: 'asin', productName: 'product_name', quantity: 'quantity',
    poNumber: 'po_number', supplier: 'supplier', containerNumber: 'container_number',
    vesselName: 'vessel_name', eta: 'eta', cbmPerUnit: 'cbm_per_unit', orderCbm: 'order_cbm',
    cartonCbm: 'carton_cbm',
    unitsPerCarton: 'units_per_carton',
    cartonWeight: 'carton_weight',
    cartonHeight: 'carton_height',
    cartonWidth: 'carton_width',
    cartonDepth: 'carton_depth',
    packSize: 'pack_size',
    scheduledDate: 'scheduled_date',
    poDate: 'po_date',
    qcStatus: 'qc_status',
    qcDate: 'qc_date',
    qcInvoiceNumber: 'qc_invoice_number',
    notes: 'notes',
    port: 'port',
    deliveryDate: 'delivery_date',
    lotNumber: 'lot_number',
    mfgDate: 'mfg_date',
    expDate: 'exp_date',
    deliveryTime: 'delivery_time',
    containerStatus: 'container_status',
    bookingStatus: 'booking_status',
    arrivedDate: 'arrived_date',
    externalContainerNumber: 'external_container_number',
    awbNumber: 'awb_number',
    purchaseOrderId: 'purchase_order_id',
    unitPrice: 'unit_price',
    actualReadyDate: 'actual_ready_date',
    estimatedDepartureDate: 'estimated_departure_date',
    shippedDate: 'shipped_date',
    orderedDate: 'ordered_date',
    estimatedReadyDate: 'estimated_ready_date',
    artworkConfirmedDate: 'artwork_confirmed_date',
};

// INSERT shape derived from UPDATABLE_FIELDS so every CRUD path covers every
// settable column. id is auto-assigned by MySQL; status/dates set by handler.
const REQUIRED_INSERT_COLS = ['status', 'dates'];
const ORDER_INSERT_COLS_LIST = [...REQUIRED_INSERT_COLS, ...Object.values(UPDATABLE_FIELDS)];
const ORDER_INSERT_COLS = `(${ORDER_INSERT_COLS_LIST.join(', ')})`;
const ORDER_INSERT_PLACEHOLDERS = `(${ORDER_INSERT_COLS_LIST.map(() => '?').join(', ')})`;

// ORDER_SELECT + the order/receipt mappers (parseDates, formatDate,
// formatDateTime, normalizeExpiry, receiptToJson, rowToOrder) moved to
// src/lib/order-shape.js (imported above) so the receive/lookup service shapes
// orders identically. setDateKey stays here — it's only used by this handler.

// ── Helpers ──────────────────────────────────────────────────────────────

function setDateKey(dates, status) {
    const key = STATUS_DATE_KEY[status];
    if (key) dates[key] = new Date().toISOString();
    return dates;
}

// Builds INSERT values from a DB row (snake_case keys), copying every
// UPDATABLE_FIELDS column. Used by split / pack to clone an existing order.
// `quantity` overrides the source row's quantity when provided.
function orderInsertValues(order, status, dates, quantity) {
    const values = [status, JSON.stringify(dates)];
    for (const col of Object.values(UPDATABLE_FIELDS)) {
        if (col === 'quantity') {
            values.push(quantity ?? order.quantity ?? 0);
        } else {
            values.push(order[col] ?? null);
        }
    }
    return values;
}

// Builds INSERT values from a request body (camelCase keys via UPDATABLE_FIELDS).
function orderInsertValuesFromBody(body, status, dates) {
    const values = [status, JSON.stringify(dates)];
    for (const [key, col] of Object.entries(UPDATABLE_FIELDS)) {
        if (col === 'quantity') {
            values.push(body[key] ?? 0);
        } else {
            values.push(body[key] ?? null);
        }
    }
    return values;
}

// Carton spec frozen onto each order row at create time so historical quote
// PDFs (forwarder/supplier/QA) stay reproducible even as jfpro.products later
// changes. camelCase body key -> product_carton_sizes (view) column.
const CARTON_SNAPSHOT_FIELDS = {
    cartonWeight: 'carton_weight',
    cartonHeight: 'carton_height',
    cartonWidth: 'carton_width',
    cartonDepth: 'carton_depth',
    cartonCbm: 'carton_cbm',
    unitsPerCarton: 'carton_qty',   // the view exposes units-per-carton as carton_qty
};

// A carton field counts as client-supplied only when it carries a real positive
// number. null / undefined / '' / 0 / non-numeric are treated as "not given" so
// a left-blank create form falls back to the catalogue rather than saving 0.
function cartonValueProvided(val) {
    if (val == null || val === '') return false;
    const n = Number(val);
    return Number.isFinite(n) && n > 0;
}

// Resolves the carton spec for a new order on `body`. Per field, a client-
// supplied value (the create form) wins; otherwise weight/dims/units freeze from
// the catalogue (product_carton_sizes, matched by jf_code) so historical quote
// PDFs stay reproducible. CBM is DERIVED from the resolved H×W×D — so it always
// agrees with the dimensions on the row and with what the quote PDFs recompute —
// unless the client sent an explicit CBM. Mutates + returns `body`.
async function applyCartonSnapshot(conn, body) {
    // 1. Normalise client input: keep provided positive numbers, null the rest
    //    (so '' never coerces to 0 on INSERT).
    const provided = {};
    for (const key of Object.keys(CARTON_SNAPSHOT_FIELDS)) {
        provided[key] = cartonValueProvided(body[key]);
        body[key] = provided[key] ? Number(body[key]) : null;
    }

    // 2. Pull the catalogue row once if anything still needs filling.
    const jfCode = typeof body.jfCode === 'string' ? body.jfCode.trim() : '';
    let v = null;
    if (jfCode && Object.keys(CARTON_SNAPSHOT_FIELDS).some(k => body[k] == null)) {
        const [rows] = await conn.query(
            `SELECT carton_weight, carton_height, carton_width, carton_depth, carton_cbm, carton_qty
               FROM product_carton_sizes WHERE jf_code = ? LIMIT 1`,
            [jfCode]
        );
        v = rows[0] || null;
    }
    const snap = (col) => {
        if (!v) return null;
        const n = Number(v[col]);
        return Number.isFinite(n) && n > 0 ? n : null;
    };

    // 3. Fill weight / dims / units from the catalogue where the client omitted them.
    for (const key of ['cartonWeight', 'cartonHeight', 'cartonWidth', 'cartonDepth', 'unitsPerCarton']) {
        if (body[key] == null) body[key] = snap(CARTON_SNAPSHOT_FIELDS[key]);
    }

    // 4. CBM precedence: an explicit client-supplied CBM wins and is kept as-is
    //    (the quote PDFs now prefer the stored carton_cbm, so it actually flows
    //    through). Otherwise derive it from the resolved dimensions; failing
    //    that (dims incomplete), fall back to the catalogue's stored CBM.
    if (!provided.cartonCbm) {
        const h = body.cartonHeight, w = body.cartonWidth, d = body.cartonDepth;
        body.cartonCbm = (h > 0 && w > 0 && d > 0)
            ? Number((h * w * d / 1_000_000).toFixed(6))
            : snap('carton_cbm');
    }
    return body;
}

async function withConnection(fn) {
    const connection = await pool.getConnection();
    try {
        return await fn(connection);
    } finally {
        connection.release();
    }
}

function withTimeout(promise, ms, label = 'Query') {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function parseAsin(raw) {
    if (!raw) return null;
    const cleaned = String(raw).trim().toUpperCase();
    return ASIN_PATTERN.test(cleaned) ? cleaned : null;
}

function aggregateAmazonStock(rows) {
    const byCountry = {};
    const totals = {
        amazon_fulfillable: 0, amazon_inbound_working: 0,
        amazon_inbound_shipped: 0, amazon_inbound_receiving: 0, amazon_reserved: 0,
    };
    for (const row of rows) {
        const country = row.country?.trim().toUpperCase();
        if (!country) continue;
        const entry = {
            fulfillable: Number(row.amazon_fulfillable || 0),
            inbound_working: Number(row.amazon_inbound_working || 0),
            inbound_shipped: Number(row.amazon_inbound_shipped || 0),
            inbound_receiving: Number(row.amazon_inbound_receiving || 0),
            reserved: Number(row.amazon_reserved || 0),
        };
        byCountry[country] = entry;
        totals.amazon_fulfillable       += entry.fulfillable;
        totals.amazon_inbound_working   += entry.inbound_working;
        totals.amazon_inbound_shipped   += entry.inbound_shipped;
        totals.amazon_inbound_receiving += entry.inbound_receiving;
        totals.amazon_reserved          += entry.reserved;
    }
    totals.amazon_total = totals.amazon_fulfillable + totals.amazon_inbound_working
        + totals.amazon_inbound_shipped + totals.amazon_inbound_receiving + totals.amazon_reserved;
    return { byCountry, totals };
}

// Amazon history from (date_ran, country) rows, ordered by date. Returns
//   total:     [ {date, fulfillable, …}, … ]            — one row per day, all
//              countries summed (the pre-existing history.amazon shape)
//   byCountry: { UK: [ {date, …}, … ], US: [ … ] }      — one series per country
// The total is summed in code from the same rows rather than fetched by a
// second GROUP BY date_ran query, so the two can never disagree. Rows with no
// country (shouldn't happen — the collector always stamps one) still count
// towards the total but get no per-country series.
const AMAZON_HISTORY_FIELDS = ['fulfillable', 'inbound_working', 'inbound_shipped', 'inbound_receiving', 'reserved'];
function buildAmazonHistory(rows) {
    const totalByDate = new Map();
    const byCountry = {};
    for (const row of rows) {
        const date = row.date_ran;
        const entry = { date };
        for (const f of AMAZON_HISTORY_FIELDS) entry[f] = Number(row[f] || 0);

        let total = totalByDate.get(date);
        if (!total) {
            total = { date };
            for (const f of AMAZON_HISTORY_FIELDS) total[f] = 0;
            totalByDate.set(date, total);
        }
        for (const f of AMAZON_HISTORY_FIELDS) total[f] += entry[f];

        const country = row.country?.trim().toUpperCase();
        if (!country) continue;
        (byCountry[country] ||= []).push(entry);
    }
    return { total: [...totalByDate.values()], byCountry };
}

// Splits the aggregate mintsoft_* figures back out per child SKU. A JF code's
// stock is the SUM of its whitelisted children (bare code + _TR trade, _QC,
// _IFU, _LABELLED, … — see SKU_SUFFIXES in services/mintsoft.js), so the
// aggregate alone can't answer "where are those units?": 4,000 units reads very
// differently when 3,500 of them sit in _TR.
//
// Rows arrive at (sku, warehouse) grain — the stock_snapshots grain. Each entry
// sums its warehouses and keeps the per-warehouse split nested, so callers that
// only care about the SKU label ignore `warehouses` and callers splitting by
// warehouse don't need a second query. Bare SKU first, then suffix A-Z.
function buildMintsoftSkuBreakdown(rows) {
    const bySku = new Map();
    for (const row of rows) {
        const sku = (row.sku || '').trim();
        if (!sku) continue;
        const jfCode = (row.jf_code || '').trim();
        // The suffix is whatever the SKU carries beyond its JF code ('' for the
        // bare code). Rows whose sku doesn't start with the jf_code (legacy /
        // hand-entered) report a null suffix rather than a bogus slice.
        const startsWithCode = jfCode && sku.toUpperCase().startsWith(jfCode.toUpperCase());
        const suffix = startsWithCode ? sku.slice(jfCode.length) : null;

        if (!bySku.has(sku)) {
            bySku.set(sku, {
                sku,
                jf_code: jfCode || null,
                suffix: suffix || null,          // null for the bare SKU
                is_base: startsWithCode ? suffix === '' : null,
                product_id: row.product_id ?? null,
                stock_level: 0, available: 0, allocated: 0, quarantine: 0,
                warehouses: [],
            });
        }

        const entry = bySku.get(sku);
        const figures = {
            stock_level: Number(row.stock_level || 0),
            available: Number(row.available || 0),
            allocated: Number(row.allocated || 0),
            quarantine: Number(row.quarantine || 0),
        };
        entry.stock_level += figures.stock_level;
        entry.available   += figures.available;
        entry.allocated   += figures.allocated;
        entry.quarantine  += figures.quarantine;
        entry.warehouses.push({ warehouse_id: Number(row.warehouse_id || 0), ...figures });
    }

    return [...bySku.values()].sort((a, b) =>
        Number(Boolean(a.suffix)) - Number(Boolean(b.suffix)) || a.sku.localeCompare(b.sku)
    );
}

function mapGoodsOnSeaRow(r) {
    const dates = parseDates(r.dates);
    return {
        id: r.id,
        asin: r.asin || null,
        productName: r.product_name || null,
        quantity: Number(r.quantity),
        eta: formatDate(r.eta),
        containerNumber: r.container_number || null,
        port: r.port || null,
        supplier: r.supplier || null,
        poNumber: r.po_number || null,
        orderCbm: r.order_cbm != null ? Number(r.order_cbm) : null,
        lotNumber: r.lot_number || null,
        mfgDate: formatDate(r.mfg_date),
        expDate: formatDate(r.exp_date),
        deliveryDate: formatDateTime(r.delivery_date),
        deliveryTime: r.delivery_time || null,
        containerStatus: r.container_status || null,
        bookingStatus: r.booking_status || null,
        arrivedDate: formatDate(r.arrived_date),
        externalContainerNumber: r.external_container_number || null,
        shippedDate: formatDate(r.shipped_date),
        orderedDate: formatDate(r.ordered_date) || formatDate(r.po_created_at),
        estimatedReadyDate: formatDate(r.estimated_ready_date),
        artworkConfirmedDate: formatDate(r.artwork_confirmed_date),
        actualReadyDate: formatDate(r.actual_ready_date),
        estimatedDepartureDate: formatDate(r.estimated_departure_date),
        dates,
    };
}

function mapGoodsOnAirRow(r) {
    return {
        id: r.id,
        productName: r.product_name,
        quantity: Number(r.quantity),
        eta: formatDate(r.eta),
        containerNumber: r.container_number || null,
        awbNumber: r.awb_number || null,
        supplier: r.supplier || null,
        poNumber: r.po_number || null,
        orderCbm: r.order_cbm != null ? Number(r.order_cbm) : null,
        deliveryDate: formatDateTime(r.delivery_date),
        shippedDate: formatDate(r.shipped_date),
        orderedDate: formatDate(r.ordered_date) || formatDate(r.po_created_at),
        estimatedReadyDate: formatDate(r.estimated_ready_date),
        artworkConfirmedDate: formatDate(r.artwork_confirmed_date),
        actualReadyDate: formatDate(r.actual_ready_date),
        estimatedDepartureDate: formatDate(r.estimated_departure_date),
        dates: parseDates(r.dates),
    };
}

function mapOrderBreakdownRow(r) {
    return {
        id: r.id,
        productName: r.product_name,
        quantity: Number(r.quantity),
        status: r.status,
        poNumber: r.po_number || null,
        port: r.port || null,
        supplier: r.supplier || null,
        deliveryDate: formatDateTime(r.delivery_date),
        scheduledDate: formatDate(r.scheduled_date),
        orderedDate: formatDate(r.ordered_date) || formatDate(r.po_created_at),
        estimatedReadyDate: formatDate(r.estimated_ready_date),
        actualReadyDate: formatDate(r.actual_ready_date),
        artworkConfirmedDate: formatDate(r.artwork_confirmed_date),
        shippedDate: formatDate(r.shipped_date),
        estimatedDepartureDate: formatDate(r.estimated_departure_date),
        dates: parseDates(r.dates),
        // Consolidated orders sit in a container — surface its number so the
        // caller can group/track them. Other statuses don't carry one yet.
        ...(r.status === 'CONSOLIDATED' ? { containerNumber: r.container_number || null } : {}),
    };
}

function buildOrderStatusMap(rows) {
    const orders = Object.fromEntries(ALL_STATUSES.map(s => [s, 0]));
    for (const row of rows) {
        orders[row.status] = Number(row.total_quantity || 0);
    }
    return orders;
}

// Like buildOrderStatusMap but from RAW per-order rows ({ id, asin, status,
// quantity }). ONLY the ARRIVED_AT_WAREHOUSE bucket is netted: that's the
// column that double-counts against mintsoft_stock_level, because a partial
// receive moves units into Mintsoft but leaves the order sitting in
// ARRIVED_AT_WAREHOUSE at its full ordered qty. Every other status reports its
// raw ordered total. Used by /all-asins, where the snapshot-freshness threshold
// differs per ASIN and so can't be gated in a single grouped SQL sum.
function buildNettedStatusMap(rows, reflectedSettledFor) {
    const orders = Object.fromEntries(ALL_STATUSES.map(s => [s, 0]));
    for (const row of rows) {
        const ordered = Number(row.quantity || 0);
        const qty = row.status === 'ARRIVED_AT_WAREHOUSE'
            ? Math.max(ordered - reflectedSettledFor(row), 0)
            : ordered;
        orders[row.status] = (orders[row.status] || 0) + qty;
    }
    return orders;
}

// ── 1. GET /api/v1/orders ────────────────────────────────────────────────
app.get('/api/v1/orders', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { orders, purchaseOrders, shipments } = await withConnection(async (conn) => {
            const [rows] = await conn.query(`${ORDER_SELECT} ORDER BY orders.created_at DESC`);
            const orders = rows.map(rowToOrder);
            // Deduplicated side-map: every PO referenced by these orders
            // (via orders.purchase_order_id) with its full document/invoice/
            // signed-PI/payment bundle, keyed by PO id. Many order lines share
            // one PO, so we attach it once here rather than per-line.
            const purchaseOrders = await loadPurchaseOrdersForOrders(conn, orders);
            // Same pattern for the shipment each order travels in (by
            // orders.shipment_id). Never fails the list: a missing table or
            // column just yields {}.
            const shipments = await shipmentSync.loadShipmentsForOrders(conn, orders);
            return { orders, purchaseOrders, shipments };
        });
        res.status(200).json({ data: orders, purchaseOrders, shipments });
    } catch (error) {
        log.error('[GET /orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 2. POST /api/v1/orders ───────────────────────────────────────────────
app.post('/api/v1/orders', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await poSentWebhooksSchemaReady;
        await shipmentsSchemaReady;
        const b = req.body || {};
        const status = typeof b.status === 'string' ? b.status.trim() : '';
        if (!status) return res.status(400).json({ error: 'status is required.' });

        const order = await withConnection(async (conn) => {
            const dates = setDateKey(b.dates && typeof b.dates === 'object' ? { ...b.dates } : {}, status);

            // Freeze the carton spec from the catalogue onto this order so its
            // future quote PDFs don't drift when jfpro.products changes.
            await applyCartonSnapshot(conn, b);

            const [insertResult] = await conn.query(
                `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
                orderInsertValuesFromBody(b, status, dates)
            );

            const [rows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [insertResult.insertId]);
            const created = rowToOrder(rows[0]);
            await recordAudit(conn, {
                entityType: 'order', entityId: created.id, action: 'create',
                before: null, after: shipmentsLib.auditSnapshot(created), userEmail: req.userEmail,
            });
            await recordPoAttachmentChange(conn, {
                before: null, after: created, userEmail: req.userEmail,
            });
            await firePoSentWebhookIfNeeded(conn, created);
            const linked = await shipmentSync.shadow(conn, {
                site: 'POST /orders', inTx: false, ...membershipKey([created]),
            }, c => shipmentSync.syncOrderMembership(c, [created.id], { userEmail: req.userEmail }));
            patchShipmentIds([created], linked);
            return created;
        });
        res.status(201).json(order);
    } catch (error) {
        log.error('[POST /orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 3. PUT /api/v1/orders/:id ────────────────────────────────────────────
app.put('/api/v1/orders/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await poSentWebhooksSchemaReady;
        await shipmentsSchemaReady;
        const result = await withConnection(async (conn) => {
            const { id } = req.params;
            const b = req.body || {};

            const [existing] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            if (!existing.length) return { notFound: id };
            const beforeOrder = rowToOrder(existing[0]);

            // If quantity is being changed and the order has receipts,
            // refuse to drop below the already-received total. Auto-flip to
            // RECEIVED when the new quantity exactly matches the sum.
            let dates = parseDates(existing[0].dates);
            let datesTouched = false;
            let nextStatus = null;
            if (b.quantity !== undefined) {
                const newQty = Number(b.quantity);
                if (!Number.isFinite(newQty) || newQty < 0) {
                    return { badQuantity: 'quantity must be a non-negative number.' };
                }
                const [sumRows] = await conn.query(
                    `SELECT COALESCE(SUM(quantity), 0) AS received FROM order_receipts WHERE order_id = ?`,
                    [id]
                );
                const receivedQty = Number(sumRows[0].received);
                if (newQty < receivedQty) {
                    return {
                        badQuantity: `quantity (${newQty}) is less than already-received total (${receivedQty}).`,
                    };
                }
                if (receivedQty > 0 && newQty === receivedQty) {
                    nextStatus = 'RECEIVED';
                    if (!dates.received) {
                        dates.received = new Date().toISOString();
                        datesTouched = true;
                    }
                }
            }

            const fields = [];
            const values = [];

            for (const [key, col] of Object.entries(UPDATABLE_FIELDS)) {
                if (b[key] !== undefined) {
                    fields.push(`${col} = ?`);
                    values.push(b[key]);
                }
            }

            // `status` is deliberately not in UPDATABLE_FIELDS (it also lives in
            // REQUIRED_INSERT_COLS, so listing it there would duplicate the
            // column in the INSERT). Handle it explicitly here so a PUT that
            // carries a status persists it AND stamps the matching `dates` key,
            // exactly like PATCH /:id/status — otherwise the status field in a
            // full-order PUT body is silently dropped. An explicit status wins
            // over the quantity-driven auto-flip to RECEIVED below.
            const explicitStatus = typeof b.status === 'string' ? b.status.trim() : '';
            if (explicitStatus) {
                fields.push('status = ?');
                values.push(explicitStatus);
                if (STATUS_DATE_KEY[explicitStatus]) {
                    setDateKey(dates, explicitStatus);
                    datesTouched = true;
                }
            } else if (nextStatus) {
                fields.push('status = ?');
                values.push(nextStatus);
            }

            if (b.dates !== undefined) {
                fields.push('dates = ?');
                values.push(JSON.stringify(b.dates));
            } else if (datesTouched) {
                fields.push('dates = ?');
                values.push(JSON.stringify(dates));
            }

            if (fields.length === 0) return { noFields: true };

            values.push(id);
            await conn.query(`UPDATE orders SET ${fields.join(', ')} WHERE id = ?`, values);

            const [rows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            const updated = rowToOrder(rows[0]);
            await recordAudit(conn, {
                entityType: 'order', entityId: updated.id, action: 'update',
                before: beforeOrder, after: updated, userEmail: req.userEmail,
            });
            await recordPoAttachmentChange(conn, {
                before: beforeOrder, after: updated, userEmail: req.userEmail,
            });
            await firePoSentWebhookIfNeeded(conn, updated);
            const linked = await shipmentSync.shadow(conn, {
                site: 'PUT /orders/:id', inTx: false, ...membershipKey([updated, beforeOrder]),
            }, c => shipmentSync.syncOrderMembership(c, [updated.id], { userEmail: req.userEmail }));
            patchShipmentIds([updated], linked);
            return { order: updated };
        });

        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.badQuantity) return res.status(400).json({ error: result.badQuantity });
        if (result.noFields) return res.status(400).json({ error: 'No fields to update.' });
        res.json(result.order);
    } catch (error) {
        log.error('[PUT /orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4. PATCH /api/v1/orders/:id/status ───────────────────────────────────
app.patch('/api/v1/orders/:id/status', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await poSentWebhooksSchemaReady;
        await shipmentsSchemaReady;
        const { status } = req.body || {};
        if (!status) return res.status(400).json({ error: 'status is required.' });

        const result = await withConnection(async (conn) => {
            const { id } = req.params;

            const [existing] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            if (!existing.length) return { notFound: id };
            const beforeOrder = rowToOrder(existing[0]);

            const dates = parseDates(existing[0].dates);
            setDateKey(dates, status);

            await conn.query(
                'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
                [status, JSON.stringify(dates), id]
            );

            const [rows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            const updated = rowToOrder(rows[0]);
            await recordAudit(conn, {
                entityType: 'order', entityId: updated.id, action: 'update',
                before: beforeOrder, after: updated, userEmail: req.userEmail,
            });
            await firePoSentWebhookIfNeeded(conn, updated);
            const linked = await shipmentSync.shadow(conn, {
                site: 'PATCH /orders/:id/status', inTx: false, ...membershipKey([updated]),
            }, c => shipmentSync.syncOrderMembership(c, [updated.id], { userEmail: req.userEmail }));
            patchShipmentIds([updated], linked);
            return { order: updated };
        });

        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        res.json(result.order);
    } catch (error) {
        log.error('[PATCH /orders/:id/status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4a. DELETE /api/v1/orders/:id ────────────────────────────────────────
// Soft delete: stamps deleted_at; row is hidden from all reads.
// Refuses if the order already has receipts — deleting would orphan rows
// in order_receipts that reference real Mintsoft ASNs.
app.delete('/api/v1/orders/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await shipmentsSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            if (!existing.length) return { notFound: id };
            const beforeOrder = rowToOrder(existing[0]);
            const [sumRows] = await conn.query(
                `SELECT COALESCE(SUM(quantity), 0) AS received FROM order_receipts WHERE order_id = ?`,
                [id]
            );
            if (Number(sumRows[0].received) > 0) {
                return { hasReceipts: Number(sumRows[0].received) };
            }
            await conn.query(
                'UPDATE orders SET deleted_at = NOW() WHERE id = ? AND deleted_at IS NULL',
                [id]
            );
            await recordAudit(conn, {
                entityType: 'order', entityId: Number(id), action: 'delete',
                before: shipmentsLib.auditSnapshot(beforeOrder), after: null, userEmail: req.userEmail,
            });
            // A soft-deleted order is effectively detached from its PO from
            // every read path's perspective — surface that on the PO timeline.
            await recordPoAttachmentChange(conn, {
                before: beforeOrder, after: null, userEmail: req.userEmail,
            });
            // It leaves its booked manifest; draft / planned lines stay, as in legacy.
            await shipmentSync.shadow(conn, {
                site: 'DELETE /orders/:id', inTx: false, ...membershipKey([beforeOrder]),
            }, c => shipmentSync.syncOrderMembership(c, [Number(id)], { userEmail: req.userEmail }));
            return { ok: true };
        });
        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.hasReceipts) {
            return res.status(409).json({
                error: `Cannot delete order ${id}: it has ${result.hasReceipts} unit(s) already received.`,
            });
        }
        res.json({ ok: true, id: Number(id) });
    } catch (error) {
        log.error('[DELETE /orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4b. POST /api/v1/orders/:id/receive ──────────────────────────────────
// Receives a portion of an order into a Mintsoft location.
// Each call creates one ASN, ships it into one Location, and appends a row
// to `order_receipts`. The order's status only flips to RECEIVED when the
// running sum of receipts equals the order's quantity — partial receipts
// leave `status` unchanged. On Mintsoft failure nothing is persisted.
//
// Concurrency: the orders row is locked FOR UPDATE for the duration of the
// validation + Mintsoft call + insert, so two concurrent receives can't both
// pass the "remaining" check and over-receive.
//
// Idempotency: clients may pass `idempotencyKey` (string, ≤64 chars). If a
// receipt with the same (order_id, idempotency_key) already exists we skip
// Mintsoft entirely and return the prior result with `idempotent: true`.
app.post('/api/v1/orders/:id/receive', async (req, res) => {
    try {
        await orderReceiptsSchemaReady;
        await auditLogSchemaReady;

        // `quarantine: true` books the units into Mintsoft quarantine instead of
        // normal sellable stock (omit or false = normal receive, unchanged).
        const { locationId, warehouseId, quantity, goodsInType, idempotencyKey, lotNumber, expiryDate, quarantine } = req.body || {};

        // The whole receive transaction (validation, idempotency, FOR UPDATE
        // lock, capacity check, Mintsoft ASN create/confirm/receive, receipt
        // persistence, lot/expiry override, status + audit) lives in the shared
        // service so the public carton-scan Lambda runs the exact same path.
        const conn = await pool.getConnection();
        let result;
        try {
            result = await receiveOrderStock(conn, {
                orderId: req.params.id,
                quantity, locationId, warehouseId, goodsInType,
                lotNumber, expiryDate, idempotencyKey, quarantine,
                actorEmail: req.userEmail,
            });
        } finally {
            conn.release();
        }

        res.json({
            order: result.order,
            asnId: result.asnId,
            receipts: result.receipts,
            // Present whenever quarantine was asked for. `applied: false` means
            // the stock IS booked in but as normal stock — the caller must not
            // read a 200 as "quarantined".
            ...(result.quarantine?.requested ? { quarantine: result.quarantine } : {}),
            ...(result.idempotent ? { idempotent: true } : {}),
        });

        // Refresh stock_snapshots for this JF code so UIs reflect the receive.
        // Fire-and-forget with a delay so Mintsoft's stock aggregates have time
        // to settle after /Items/Receive — failure here must not affect the
        // response. Skipped on an idempotent replay (result.jfCode is null).
        if (result.jfCode) {
            const { jfCode, asin } = result;
            setTimeout(async () => {
                const c = await pool.getConnection();
                try {
                    await snapshotJfCode(c, jfCode, asin || '');
                } catch (err) {
                    log.warn('[POST /orders/:id/receive] post-receive snapshot failed', { jfCode, error: err.message });
                } finally {
                    c.release();
                }
            }, 10000);
        }
    } catch (error) {
        if (error instanceof ReceiveError) {
            return res.status(error.status).json({ error: error.message, ...(error.payload || {}) });
        }
        log.error('[POST /orders/:id/receive]', error);
        if (!res.headersSent) {
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    }
});

// ── 4c. POST /api/v1/orders/:id/not-received ─────────────────────────────
// Records a shortfall against an order line — the units that didn't physically
// turn up. No Mintsoft side effect; this is purely a reconciliation marker so
// the line can settle without `received` summing to the full ordered qty.
//
// Same locking + idempotency model as /receive. `quantity` is bounded by
// `orderQty - sum(any-type receipts)` so the order can't be over-reconciled.
// When the running total of all receipts hits the ordered qty the status
// flips to PARTIALLY_RECEIVED (any shortfall → not fully received).
app.post('/api/v1/orders/:id/not-received', async (req, res) => {
    try {
        await orderReceiptsSchemaReady;
        await auditLogSchemaReady;

        const { quantity, idempotencyKey } = req.body || {};
        if (!quantity || Number(quantity) <= 0) {
            return res.status(400).json({ error: 'quantity must be a positive number.' });
        }
        const qty = Number(quantity);

        let idemKey = null;
        if (idempotencyKey !== undefined && idempotencyKey !== null && idempotencyKey !== '') {
            if (typeof idempotencyKey !== 'string' || idempotencyKey.length > 64) {
                return res.status(400).json({ error: 'idempotencyKey must be a string of ≤64 characters.' });
            }
            idemKey = idempotencyKey;
        }

        const { id } = req.params;

        const conn = await pool.getConnection();
        let committed = false;
        try {
            await conn.beginTransaction();

            if (idemKey) {
                const [dup] = await conn.query(
                    `SELECT * FROM order_receipts WHERE order_id = ? AND idempotency_key = ? LIMIT 1`,
                    [id, idemKey]
                );
                if (dup.length) {
                    const [orderRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
                    const [allReceipts] = await conn.query(
                        'SELECT * FROM order_receipts WHERE order_id = ? ORDER BY received_at DESC',
                        [id]
                    );
                    await conn.commit();
                    committed = true;
                    return res.json({
                        order: rowToOrder(orderRows[0]),
                        receipts: allReceipts.map(receiptToJson),
                        idempotent: true,
                    });
                }
            }

            const [lockRows] = await conn.query(
                `SELECT * FROM orders WHERE id = ? AND deleted_at IS NULL FOR UPDATE`,
                [id]
            );
            if (!lockRows.length) {
                await conn.rollback();
                committed = true;
                return res.status(404).json({ error: `Order ${id} not found.` });
            }
            const orderRow = lockRows[0];

            const [sumRows] = await conn.query(
                `SELECT
                    COALESCE(SUM(CASE WHEN type = 'received' THEN quantity END), 0) AS received,
                    COALESCE(SUM(CASE WHEN type = 'not_received' THEN quantity END), 0) AS not_received,
                    COALESCE(SUM(quantity), 0) AS settled
                 FROM order_receipts WHERE order_id = ?`,
                [id]
            );
            const receivedQty = Number(sumRows[0].received);
            const notReceivedQty = Number(sumRows[0].not_received);
            const settledQty = Number(sumRows[0].settled);
            const orderQty = Number(orderRow.quantity);
            const remaining = orderQty - settledQty;
            const beforeOrder = rowToOrder({
                ...orderRow,
                received_quantity: receivedQty,
                not_received_quantity: notReceivedQty,
            });

            if (remaining <= 0) {
                await conn.rollback();
                committed = true;
                const [orderRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
                return res.status(409).json({ error: 'Order is already fully reconciled.', order: rowToOrder(orderRows[0]) });
            }
            if (qty > remaining) {
                await conn.rollback();
                committed = true;
                return res.status(400).json({ error: `quantity (${qty}) exceeds remaining (${remaining}).` });
            }

            await conn.query(
                `INSERT INTO order_receipts
                    (order_id, jf_code, quantity, idempotency_key, type)
                 VALUES (?, ?, ?, ?, 'not_received')`,
                [id, orderRow.jf_code || null, qty, idemKey]
            );

            const newSettled = settledQty + qty;
            const dates = parseDates(orderRow.dates);
            if (!dates.received_by_warehouse) {
                dates.received_by_warehouse = new Date().toISOString();
            }
            // This is a /not-received call so the new not_received total is
            // notReceivedQty + qty (>0). When outstanding hits zero, the
            // order has at least one shortfall → PARTIALLY_RECEIVED, never
            // RECEIVED.
            let nextStatus = orderRow.status;
            if (newSettled >= orderQty) {
                nextStatus = 'PARTIALLY_RECEIVED';
                dates.received = new Date().toISOString();
            }

            await conn.query(
                'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
                [nextStatus, JSON.stringify(dates), id]
            );

            const [finalRows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [id]);
            const updatedOrder = rowToOrder(finalRows[0]);
            await recordAudit(conn, {
                entityType: 'order', entityId: updatedOrder.id, action: 'update',
                before: beforeOrder, after: updatedOrder, userEmail: req.userEmail,
            });

            await conn.commit();
            committed = true;

            const [receipts] = await conn.query(
                'SELECT * FROM order_receipts WHERE order_id = ? ORDER BY received_at DESC',
                [id]
            );

            res.json({
                order: updatedOrder,
                receipts: receipts.map(receiptToJson),
            });
        } catch (err) {
            if (!committed) {
                try { await conn.rollback(); } catch (_) { /* ignore */ }
            }
            throw err;
        } finally {
            conn.release();
        }
    } catch (error) {
        log.error('[POST /orders/:id/not-received]', error);
        if (!res.headersSent) {
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    }
});

// ── 4d. GET /api/v1/orders/:id/receipts ──────────────────────────────────
app.get('/api/v1/orders/:id/receipts', async (req, res) => {
    try {
        const { id } = req.params;
        const receipts = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                'SELECT * FROM order_receipts WHERE order_id = ? ORDER BY received_at DESC',
                [id]
            );
            return rows.map(receiptToJson);
        });
        res.json({ data: receipts });
    } catch (error) {
        log.error('[GET /orders/:id/receipts]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4d. GET /api/v1/mintsoft/warehouses ──────────────────────────────────
// Reads from the local mintsoft_locations cache (synced nightly).
// Returns every warehouse with its locations nested.
app.get('/api/v1/mintsoft/warehouses', async (req, res) => {
    try {
        const data = await withConnection((conn) => listWarehousesWithLocations(conn));
        res.json({ data });
    } catch (error) {
        log.error('[GET /mintsoft/warehouses]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4g. GET /api/v1/mintsoft/carton-sizes ────────────────────────────────
// Lists every row in product_carton_sizes (a jfa view over jfpro.products,
// keyed by jf_code) with both unit and carton dimensions, plus computed CBMs.
// Source dimensions are in cm; CBM is in m³ (cm³ → m³ via /1,000,000).
// NOTE: unit-level weight/height/width/depth and product_id have no source in
// jfpro.products and come back as 0/null; carton weight is the real
// grossCartonWeightKg (falls back to netCartonWeightKg).
//
// Optional filters:
//   ?asin=B0...      (exact)
//   ?jfCode=JF1408   (exact)
app.get('/api/v1/mintsoft/carton-sizes', async (req, res) => {
    try {
        const where = [];
        const params = [];
        if (req.query.asin) { where.push('asin = ?'); params.push(String(req.query.asin).trim()); }
        if (req.query.jfCode) { where.push('jf_code = ?'); params.push(String(req.query.jfCode).trim()); }
        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT id, jf_code, asin, sku, product_id,
                        weight, height, width, depth,
                        carton_weight, carton_height, carton_width, carton_depth,
                        carton_qty, created_at, updated_at
                   FROM product_carton_sizes
                   ${whereSql}
                  ORDER BY jf_code ASC, sku ASC`,
                params
            );
            return r;
        });

        const num = (v) => v != null ? Number(v) : 0;
        const cbm = (h, w, d) => {
            const v = num(h) * num(w) * num(d);
            return v > 0 ? v / 1_000_000 : 0;
        };

        const data = rows.map(r => ({
            id: r.id,
            jfCode: r.jf_code,
            asin: r.asin || null,
            sku: r.sku,
            productId: r.product_id,
            weight: num(r.weight),
            height: num(r.height),
            width: num(r.width),
            depth: num(r.depth),
            unitCbm: cbm(r.height, r.width, r.depth),
            // Fall back to unit weight × units-per-carton when carton_weight is
            // empty (0 or null) in the source data.
            cartonWeight: num(r.carton_weight) || num(r.weight) * num(r.carton_qty),
            cartonHeight: num(r.carton_height),
            cartonWidth: num(r.carton_width),
            cartonDepth: num(r.carton_depth),
            cartonCbm: cbm(r.carton_height, r.carton_width, r.carton_depth),
            cartonQty: num(r.carton_qty),
            createdAt: r.created_at?.toISOString?.() ?? r.created_at,
            updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
        }));

        res.json({ data });
    } catch (error) {
        log.error('[GET /mintsoft/carton-sizes]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── GET /api/v1/suppliers ────────────────────────────────────────────────
// Lists all active suppliers from the local DB with their email addresses
// embedded. The legacy `suppliers.contact_email` is still surfaced as a
// scalar for backwards compatibility; the authoritative list is `emails`.
app.get('/api/v1/suppliers', async (req, res) => {
    try {
        await supplierEmailsSchemaReady;
        const { rows, emailsBySupplierId } = await withConnection(async (conn) => {
            const [supRows] = await conn.query(`
                SELECT id, name, code, portal_code, country, default_currency, default_incoterms,
                       default_payment_terms, contact_name, contact_email, created_at, updated_at
                  FROM suppliers
                 WHERE deleted_at IS NULL
                 ORDER BY name ASC
            `);
            const ids = supRows.map(r => r.id);
            const emailRows = ids.length
                ? (await conn.query(
                    `SELECT id, supplier_id, email, label, is_primary, created_at, updated_at
                       FROM supplier_emails
                      WHERE supplier_id IN (${ids.map(() => '?').join(',')}) AND deleted_at IS NULL
                      ORDER BY is_primary DESC, email ASC`,
                    ids
                ))[0]
                : [];
            const byId = new Map();
            for (const e of emailRows) {
                if (!byId.has(e.supplier_id)) byId.set(e.supplier_id, []);
                byId.get(e.supplier_id).push(e);
            }
            return { rows: supRows, emailsBySupplierId: byId };
        });

        res.json({
            data: rows.map(r => ({
                id: r.id,
                name: r.name,
                code: r.code || null,
                portalCode: r.portal_code || null,
                country: r.country || null,
                defaultCurrency: r.default_currency || null,
                defaultIncoterms: r.default_incoterms || null,
                defaultPaymentTerms: r.default_payment_terms || null,
                contactName: r.contact_name || null,
                contactEmail: r.contact_email || null,
                createdAt: r.created_at?.toISOString?.() ?? r.created_at,
                updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
                emails: (emailsBySupplierId.get(r.id) || []).map(e => ({
                    id: e.id,
                    email: e.email,
                    label: e.label || null,
                    isPrimary: e.is_primary === 1,
                    createdAt: e.created_at?.toISOString?.() ?? e.created_at,
                    updatedAt: e.updated_at?.toISOString?.() ?? e.updated_at,
                })),
            })),
        });
    } catch (error) {
        log.error('[GET /suppliers]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 4e. GET /api/v1/mintsoft/warehouses/:warehouseId/locations ───────────
app.get('/api/v1/mintsoft/warehouses/:warehouseId/locations', async (req, res) => {
    try {
        const warehouseId = Number(req.params.warehouseId);
        if (!warehouseId) return res.status(400).json({ error: 'warehouseId must be a number.' });
        const data = await withConnection((conn) => listLocationsForWarehouse(conn, warehouseId));
        res.json({ data });
    } catch (error) {
        log.error('[GET /mintsoft/warehouses/:warehouseId/locations]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 5. POST /api/v1/orders/:id/split ─────────────────────────────────────
app.post('/api/v1/orders/:id/split', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await shipmentsSchemaReady;
        const { splitQuantity, containerNumber } = req.body || {};

        if (!splitQuantity || splitQuantity <= 0) {
            return res.status(400).json({ error: 'splitQuantity must be a positive number.' });
        }
        if (!containerNumber) {
            return res.status(400).json({ error: 'containerNumber is required.' });
        }

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const [existing] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [req.params.id]);
                if (!existing.length) {
                    await conn.rollback();
                    return { notFound: req.params.id };
                }

                const original = existing[0];
                const beforeOriginal = rowToOrder(original);
                if (splitQuantity >= original.quantity) {
                    await conn.rollback();
                    return { badQuantity: true };
                }

                // Splitting an order with receipts would leave order_receipts
                // pointing at the wrong (shrunken) original. We don't currently
                // support reassigning receipts to a fragment, so reject.
                const [sumRows] = await conn.query(
                    `SELECT COALESCE(SUM(quantity), 0) AS received FROM order_receipts WHERE order_id = ?`,
                    [req.params.id]
                );
                if (Number(sumRows[0].received) > 0) {
                    await conn.rollback();
                    return { hasReceipts: Number(sumRows[0].received) };
                }

                await conn.query('UPDATE orders SET quantity = ? WHERE id = ?',
                    [original.quantity - splitQuantity, req.params.id]);

                const newDates = setDateKey({ ...parseDates(original.dates) }, 'CONSOLIDATED');

                const [insertResult] = await conn.query(
                    `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
                    orderInsertValues({ ...original, container_number: containerNumber }, 'CONSOLIDATED', newDates, splitQuantity)
                );

                const [updatedOrig] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [req.params.id]);
                const [newOrder] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [insertResult.insertId]);
                const updatedOriginal = rowToOrder(updatedOrig[0]);
                const createdOrder = rowToOrder(newOrder[0]);
                await recordAudit(conn, {
                    entityType: 'order', entityId: updatedOriginal.id, action: 'update',
                    before: beforeOriginal, after: updatedOriginal, userEmail: req.userEmail,
                });
                await recordAudit(conn, {
                    entityType: 'order', entityId: createdOrder.id, action: 'create',
                    before: null, after: shipmentsLib.auditSnapshot(createdOrder), userEmail: req.userEmail,
                });
                // The clone inherits purchase_order_id from the original via
                // orderInsertValues, so the PO gains a new line in audit terms.
                await recordPoAttachmentChange(conn, {
                    before: null, after: createdOrder, userEmail: req.userEmail,
                });
                const linked = await shipmentSync.shadow(conn, {
                    site: 'POST /orders/:id/split', inTx: true, ...membershipKey([createdOrder], containerNumber),
                }, c => shipmentSync.syncOrderMembership(c, [updatedOriginal.id, createdOrder.id], { userEmail: req.userEmail }));

                await conn.commit();
                patchShipmentIds([updatedOriginal, createdOrder], linked);

                return { originalOrder: updatedOriginal, newOrder: createdOrder };
            } catch (err) {
                await conn.rollback();
                throw err;
            }
        });

        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.badQuantity) return res.status(400).json({ error: 'splitQuantity must be less than the original order quantity.' });
        if (result.hasReceipts) {
            return res.status(409).json({
                error: `Cannot split order ${req.params.id}: it has ${result.hasReceipts} unit(s) already received.`,
            });
        }
        res.status(201).json(result);
    } catch (error) {
        log.error('[POST /orders/:id/split]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 6. POST /api/v1/containers/pack ──────────────────────────────────────
app.post('/api/v1/containers/pack', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await shipmentsSchemaReady;
        const { containerNumber, vesselName, eta, packs } = req.body || {};

        if (!containerNumber) {
            return res.status(400).json({ error: 'containerNumber is required.' });
        }
        if (!packs || !Array.isArray(packs) || packs.length === 0) {
            return res.status(400).json({ error: 'packs array is required and must not be empty.' });
        }

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const affected = [];
                const auditEntries = [];

                for (const pack of packs) {
                    const { orderId, qty } = pack;
                    if (!orderId || !qty || qty <= 0) {
                        await conn.rollback();
                        return { badPack: true };
                    }

                    const [existing] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [orderId]);
                    if (!existing.length) {
                        await conn.rollback();
                        return { notFound: orderId };
                    }

                    const order = existing[0];
                    const beforeOrder = rowToOrder(order);

                    if (qty === order.quantity) {
                        // Full pack — update in place
                        const dates = setDateKey(parseDates(order.dates), 'CONSOLIDATED');
                        await conn.query(
                            `UPDATE orders SET status = 'CONSOLIDATED', container_number = ?, vessel_name = ?, eta = ?, dates = ? WHERE id = ?`,
                            [containerNumber, vesselName || null, eta || null, JSON.stringify(dates), orderId]
                        );
                        affected.push(orderId);
                        auditEntries.push({ id: orderId, before: beforeOrder, action: 'update' });
                    } else if (qty < order.quantity) {
                        // Partial pack — split
                        await conn.query('UPDATE orders SET quantity = ? WHERE id = ?',
                            [order.quantity - qty, orderId]);
                        affected.push(orderId);
                        auditEntries.push({ id: orderId, before: beforeOrder, action: 'update' });

                        const newDates = setDateKey({ ...parseDates(order.dates) }, 'CONSOLIDATED');

                        const [insertResult] = await conn.query(
                            `INSERT INTO orders ${ORDER_INSERT_COLS} VALUES ${ORDER_INSERT_PLACEHOLDERS}`,
                            orderInsertValues(
                                { ...order, container_number: containerNumber, vessel_name: vesselName || null, eta: eta || null },
                                'CONSOLIDATED', newDates, qty
                            )
                        );
                        affected.push(insertResult.insertId);
                        auditEntries.push({ id: insertResult.insertId, before: null, action: 'create' });
                    } else {
                        await conn.rollback();
                        return { qtyExceeds: { qty, orderId, orderQty: order.quantity } };
                    }
                }

                const ph = affected.map(() => '?').join(',');
                const [rows] = await conn.query(
                    `${ORDER_SELECT} AND orders.id IN (${ph}) ORDER BY orders.created_at DESC`, affected
                );
                const orders = rows.map(rowToOrder);
                const ordersById = new Map(orders.map(o => [o.id, o]));
                for (const entry of auditEntries) {
                    const after = ordersById.get(entry.id) || null;
                    await recordAudit(conn, {
                        entityType: 'order', entityId: entry.id, action: entry.action,
                        before: shipmentsLib.auditSnapshot(entry.before), after: shipmentsLib.auditSnapshot(after),
                        userEmail: req.userEmail,
                    });
                    // Full-pack rows keep their PO (no-op in the helper), but
                    // partial-pack clones are fresh creates that inherit
                    // purchase_order_id from their source row and need an
                    // 'order_attached' on the PO timeline.
                    await recordPoAttachmentChange(conn, {
                        before: entry.before, after,
                        userEmail: req.userEmail,
                    });
                }
                const linked = await shipmentSync.shadow(conn, {
                    site: 'POST /containers/pack', inTx: true, ...membershipKey(orders, containerNumber),
                }, c => shipmentSync.syncOrderMembership(c, affected, { userEmail: req.userEmail }));

                await conn.commit();
                patchShipmentIds(orders, linked);
                return { data: orders };
            } catch (err) {
                await conn.rollback();
                throw err;
            }
        });

        if (result.badPack) return res.status(400).json({ error: 'Invalid pack entry: orderId and qty > 0 required.' });
        if (result.notFound) return res.status(404).json({ error: `Order ${result.notFound} not found.` });
        if (result.qtyExceeds) {
            const { qty, orderId, orderQty } = result.qtyExceeds;
            return res.status(400).json({ error: `qty (${qty}) exceeds order ${orderId} quantity (${orderQty}).` });
        }
        res.json(result);
    } catch (error) {
        log.error('[POST /containers/pack]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 7. PATCH /api/v1/containers/:containerNumber/status ──────────────────
app.patch('/api/v1/containers/:containerNumber/status', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await shipmentsSchemaReady;
        const { status } = req.body || {};
        if (!status) return res.status(400).json({ error: 'status is required.' });

        const result = await withConnection(async (conn) => {
            const { containerNumber } = req.params;
            const [orders] = await conn.query(
                `${ORDER_SELECT} AND container_number = ?`, [containerNumber]
            );
            if (!orders.length) return { notFound: containerNumber };
            const beforeById = new Map(orders.map(o => [o.id, rowToOrder(o)]));

            for (const order of orders) {
                const dates = setDateKey(parseDates(order.dates), status);
                await conn.query(
                    'UPDATE orders SET status = ?, dates = ? WHERE id = ?',
                    [status, JSON.stringify(dates), order.id]
                );
            }

            const [updated] = await conn.query(
                `${ORDER_SELECT} AND container_number = ?`, [containerNumber]
            );
            const afterOrders = updated.map(rowToOrder);
            for (const o of afterOrders) {
                await recordAudit(conn, {
                    entityType: 'order', entityId: o.id, action: 'update',
                    before: beforeById.get(o.id) || null, after: o,
                    userEmail: req.userEmail,
                });
            }
            // Status never changes membership; the hook keeps the stored
            // copies fresh. The shipment's stage follows at read time.
            const linked = await shipmentSync.shadow(conn, {
                site: 'PATCH /containers/:cn/status', inTx: false, ...membershipKey(afterOrders, containerNumber),
            }, c => shipmentSync.syncOrderMembership(c, afterOrders.map(o => o.id), { userEmail: req.userEmail }));
            patchShipmentIds(afterOrders, linked);
            return { data: afterOrders };
        });

        if (result.notFound) return res.status(404).json({ error: `No orders found for container ${result.notFound}.` });
        res.status(200).json(result);
    } catch (error) {
        log.error('[PATCH /containers/:containerNumber/status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 7b. Containers (read from cached ShipsGo data) ───────────────────────
// Populated by the shipsgo-containers Lambda; pure reads — no live API call.

function parseJsonCol(v) {
    if (v == null) return null;
    if (typeof v === 'string') { try { return JSON.parse(v); } catch { return v; } }
    return v;
}

function rowToContainer(row) {
    const milestones = parseJsonCol(row.milestones);

    // Port-level "current location" derived from milestones:
    //   lastEvent  = most-recent ACT milestone (where the container actually is)
    //   nextEvent  = first upcoming EST milestone (where it's heading)
    let lastEvent = null;
    let nextEvent = null;
    if (Array.isArray(milestones)) {
        for (const m of milestones) {
            if (m.is_actual === true) lastEvent = m;       // keep walking → ends on the latest
            else if (!nextEvent && m.is_actual === false) nextEvent = m;  // first estimated
        }
    }

    return {
        containerNumber: row.container_number,
        blNumber: row.bl_number,
        blType: row.bl_type,
        bookingRef: row.booking_ref,
        sealNumber: row.seal_number,
        containerSizeType: row.container_size_type,
        vessel: {
            imo: row.vessel_imo,
            name: row.vessel_name,
            voyage: row.voyage,
        },
        carrier: {
            name: row.shipping_line,
            scac: row.carrier_scac,
            referenceNumber: row.carrier_ref_number,
            lastUpdated: row.carrier_last_updated,
        },
        serviceName: row.service_name,
        por: { name: row.por_name, locode: row.por_locode },
        pol: { name: row.pol_name, locode: row.pol_locode, country: row.pol_country },
        pod: {
            name: row.pod_name,
            locode: row.pod_locode,
            country: row.pod_country,
            terminal: row.discharge_terminal,
        },
        finalDelivery: { name: row.final_delivery_name, locode: row.final_delivery_locode },
        transshipments: parseJsonCol(row.transshipments),
        currentLocation: {
            // lat/lng come from ShipsGo's GeoJSON endpoint (CURRENT LineString
            // feature). lastEvent/nextEvent are derived port-level fallbacks.
            lat: row.current_lat != null ? Number(row.current_lat) : null,
            lng: row.current_lng != null ? Number(row.current_lng) : null,
            lastEvent,
            nextEvent,
        },
        times: {
            departure: row.departure_date,
            departureIsActual: row.departure_is_actual === 1,
            arrival: row.arrival_date,
            arrivalIsActual: row.arrival_is_actual === 1,
            eta: row.eta,
            ata: row.ata,
            etaInitial: row.eta_initial,
            totalTransitDays: row.total_transit_days,
        },
        milestones,
        holds: parseJsonCol(row.holds),
        freeTime: parseJsonCol(row.free_time_info),
        demurrage: parseJsonCol(row.demurrage_info),
        tags: parseJsonCol(row.tags),
        co2Emissions: row.co2_emissions != null ? Number(row.co2_emissions) : null,
        delayStatus: row.delay_status,
        shipsgoId: row.shipsgo_id,
        routeGeojson: parseJsonCol(row.route_geojson),
        fetchedAt: row.fetched_at,
        updatedAt: row.updated_at,
    };
}

app.get('/api/v1/containers', async (req, res) => {
    try {
        const rows = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                'SELECT * FROM containers ORDER BY COALESCE(eta, arrival_date, departure_date) DESC, container_number ASC'
            );
            return rows;
        });
        res.json({ data: rows.map(rowToContainer) });
    } catch (error) {
        log.error('[GET /containers]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get('/api/v1/containers/:containerNumber', async (req, res) => {
    try {
        const { containerNumber } = req.params;
        const row = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                'SELECT * FROM containers WHERE container_number = ?',
                [containerNumber]
            );
            return rows[0] || null;
        });
        if (!row) return res.status(404).json({ error: `Container ${containerNumber} not tracked.` });
        res.json(rowToContainer(row));
    } catch (error) {
        log.error('[GET /containers/:containerNumber]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 7c. Air shipments (read from cached ShipsGo air data) ────────────────
// Populated by the shipsgo-air Lambda; pure reads — no live API call. Air
// analog of the containers endpoints above (airports/IATA + flights instead
// of ports/vessels).
function rowToAirShipment(row) {
    const milestones = parseJsonCol(row.milestones);

    let lastEvent = null;
    let nextEvent = null;
    if (Array.isArray(milestones)) {
        for (const m of milestones) {
            if (m.is_actual === true) lastEvent = m;
            else if (!nextEvent && m.is_actual === false) nextEvent = m;
        }
    }

    return {
        awbNumber: row.awb_number,
        reference: row.reference,
        status: row.status,
        airline: { name: row.airline_name, iata: row.airline_iata },
        origin: { name: row.origin_name, iata: row.origin_iata, country: row.origin_country },
        destination: { name: row.destination_name, iata: row.destination_iata, country: row.destination_country },
        transshipments: parseJsonCol(row.transshipments),
        currentLocation: {
            lat: row.current_lat != null ? Number(row.current_lat) : null,
            lng: row.current_lng != null ? Number(row.current_lng) : null,
            lastEvent,
            nextEvent,
        },
        times: {
            departure: row.departure_date,
            departureIsActual: row.departure_is_actual === 1,
            departureInitial: row.departure_initial,
            arrival: row.arrival_date,
            arrivalIsActual: row.arrival_is_actual === 1,
            eta: row.eta,
            ata: row.ata,
            etaInitial: row.eta_initial,
            totalTransitTime: row.total_transit_time,
            transitPercentage: row.transit_percentage,
        },
        milestones,
        tags: parseJsonCol(row.tags),
        shipsgoId: row.shipsgo_id,
        routeGeojson: parseJsonCol(row.route_geojson),
        fetchedAt: row.fetched_at,
        updatedAt: row.updated_at,
    };
}

app.get('/api/v1/air-shipments', async (req, res) => {
    try {
        const rows = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                'SELECT * FROM air_shipments ORDER BY COALESCE(eta, arrival_date, departure_date) DESC, awb_number ASC'
            );
            return rows;
        });
        res.json({ data: rows.map(rowToAirShipment) });
    } catch (error) {
        // Table is created on the shipsgo-air Lambda's first run; until then
        // there's simply nothing tracked.
        if (error.code === 'ER_NO_SUCH_TABLE') return res.json({ data: [] });
        log.error('[GET /air-shipments]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get('/api/v1/air-shipments/:awbNumber', async (req, res) => {
    try {
        const { awbNumber } = req.params;
        const row = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                'SELECT * FROM air_shipments WHERE awb_number = ?',
                [awbNumber]
            );
            return rows[0] || null;
        });
        if (!row) return res.status(404).json({ error: `AWB ${awbNumber} not tracked.` });
        res.json(rowToAirShipment(row));
    } catch (error) {
        if (error.code === 'ER_NO_SUCH_TABLE') {
            return res.status(404).json({ error: `AWB ${req.params.awbNumber} not tracked.` });
        }
        log.error('[GET /air-shipments/:awbNumber]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Draft container allocations ───────────────────────────────────────────
// Planning layer: one order can sit in many user-named "draft containers"
// with different allocated quantities. Each row joins back to orders so the
// caller can render jf_code / product_name / quantity without a second hop.
const DRAFT_ALLOC_SELECT = `
    SELECT dca.id, dca.order_id, dca.draft_container_name, dca.allocated,
           dca.created_at, dca.updated_at,
           orders.jf_code, orders.asin, orders.product_name, orders.quantity AS order_quantity,
           orders.status AS order_status, orders.supplier, orders.po_number,
           orders.purchase_order_id, orders.container_number, orders.eta
    FROM draft_container_allocations dca
    INNER JOIN orders ON orders.id = dca.order_id AND orders.deleted_at IS NULL
`;

function rowToDraftAllocation(row) {
    return {
        id: row.id,
        orderId: row.order_id,
        draftContainerName: row.draft_container_name,
        allocated: Number(row.allocated),
        createdAt: row.created_at?.toISOString?.() ?? row.created_at,
        updatedAt: row.updated_at?.toISOString?.() ?? row.updated_at,
        order: {
            id: row.order_id,
            jfCode: row.jf_code || null,
            asin: row.asin || null,
            productName: row.product_name || null,
            quantity: row.order_quantity != null ? Number(row.order_quantity) : null,
            status: row.order_status || null,
            supplier: row.supplier || null,
            poNumber: row.po_number || null,
            purchaseOrderId: row.purchase_order_id ?? null,
            containerNumber: row.container_number || null,
            eta: formatDate(row.eta),
        },
    };
}

// List allocations, optionally filtered by draft_container_name or order_id.
// GET /api/v1/draft-containers              → every row
// GET /api/v1/draft-containers?name=DRAFT1  → one draft's lines
// GET /api/v1/draft-containers?orderId=42   → every draft a given order sits in
app.get('/api/v1/draft-containers', async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        const { name, orderId } = req.query;
        const where = [];
        const params = [];
        if (name) { where.push('dca.draft_container_name = ?'); params.push(String(name)); }
        if (orderId) { where.push('dca.order_id = ?'); params.push(Number(orderId)); }
        const sql = `${DRAFT_ALLOC_SELECT}${where.length ? ' WHERE ' + where.join(' AND ') : ''} ORDER BY dca.draft_container_name ASC, dca.id ASC`;

        const rows = await withConnection(async (conn) => {
            const [rs] = await conn.query(sql, params);
            return rs;
        });
        res.json({ data: rows.map(rowToDraftAllocation) });
    } catch (error) {
        log.error('[GET /draft-containers]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Fetch a single allocation row by its primary key.
app.get('/api/v1/draft-containers/:id', async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        const { id } = req.params;
        const row = await withConnection(async (conn) => {
            const [rs] = await conn.query(`${DRAFT_ALLOC_SELECT} WHERE dca.id = ?`, [id]);
            return rs[0] || null;
        });
        if (!row) return res.status(404).json({ error: `Allocation ${id} not found.` });
        res.json(rowToDraftAllocation(row));
    } catch (error) {
        log.error('[GET /draft-containers/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Create an allocation. Body: { orderId, draftContainerName, allocated }.
// 409 if (orderId, draftContainerName) already exists — callers should PUT
// to change the allocated quantity instead.
app.post('/api/v1/draft-containers', async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        await shipmentsSchemaReady;
        const { orderId, draftContainerName, allocated } = req.body || {};
        const oid = Number(orderId);
        const name = typeof draftContainerName === 'string' ? draftContainerName.trim() : '';
        const qty = Number(allocated);
        if (!Number.isInteger(oid) || oid <= 0) return res.status(400).json({ error: 'orderId must be a positive integer.' });
        if (!name) return res.status(400).json({ error: 'draftContainerName is required.' });
        if (!Number.isFinite(qty) || qty < 0) return res.status(400).json({ error: 'allocated must be a non-negative number.' });

        await draftRegistryReady;
        const result = await withConnection(async (conn) => {
            const [orderRows] = await conn.query('SELECT id FROM orders WHERE id = ? AND deleted_at IS NULL', [oid]);
            if (!orderRows.length) return { orderNotFound: true };
            try {
                const [ins] = await conn.query(
                    `INSERT INTO draft_container_allocations (order_id, draft_container_name, allocated) VALUES (?, ?, ?)`,
                    [oid, name, qty]
                );
                const [rs] = await conn.query(`${DRAFT_ALLOC_SELECT} WHERE dca.id = ?`, [ins.insertId]);
                // First line into a new name registers the draft (its "create"
                // event); every line records itself against the draft's id.
                const reg = await draftAudit.ensureDraftRegistered(conn, name, req.userEmail);
                await draftAudit.recordDraftAudit(conn, {
                    draftId: reg.id, action: 'line_added',
                    after: { draftName: reg.name, ...draftAudit.lineSnapshot(rs[0]) },
                    userEmail: req.userEmail,
                });
                await shipmentSync.shadow(conn, {
                    site: 'POST /draft-containers', inTx: false, keyKind: 'draft', keyValue: name,
                }, c => shipmentSync.syncDraft(c, name, { userEmail: req.userEmail }));
                return { row: rs[0] };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.orderNotFound) return res.status(404).json({ error: `Order ${oid} not found.` });
        if (result.duplicate) return res.status(409).json({ error: `Order ${oid} is already in draft container ${name}.` });
        res.status(201).json(rowToDraftAllocation(result.row));
    } catch (error) {
        log.error('[POST /draft-containers]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Update an allocation. Any of { draftContainerName, allocated } can be sent;
// orderId is intentionally immutable — to move a line to a different order
// the caller should DELETE + POST.
app.put('/api/v1/draft-containers/:id', async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        await shipmentsSchemaReady;
        const { id } = req.params;
        const { draftContainerName, allocated } = req.body || {};
        const fields = [];
        const values = [];
        if (draftContainerName !== undefined) {
            const name = String(draftContainerName).trim();
            if (!name) return res.status(400).json({ error: 'draftContainerName cannot be empty.' });
            fields.push('draft_container_name = ?');
            values.push(name);
        }
        if (allocated !== undefined) {
            const qty = Number(allocated);
            if (!Number.isFinite(qty) || qty < 0) return res.status(400).json({ error: 'allocated must be a non-negative number.' });
            fields.push('allocated = ?');
            values.push(qty);
        }
        if (!fields.length) return res.status(400).json({ error: 'No fields to update.' });

        await draftRegistryReady;
        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(`${DRAFT_ALLOC_SELECT} WHERE dca.id = ?`, [id]);
            if (!existing.length) return { notFound: true };
            const before = existing[0];
            try {
                values.push(id);
                await conn.query(`UPDATE draft_container_allocations SET ${fields.join(', ')} WHERE id = ?`, values);
                const [rs] = await conn.query(`${DRAFT_ALLOC_SELECT} WHERE dca.id = ?`, [id]);
                const after = rs[0];

                // A per-line name change is the line MOVING between drafts (the
                // whole-draft rename is POST /draft-containers/rename): removed
                // from the old draft, added to the new. A quantity change on the
                // same draft is line_updated.
                const beforeSnap = draftAudit.lineSnapshot(before);
                const afterSnap = draftAudit.lineSnapshot(after);
                if (before.draft_container_name !== after.draft_container_name) {
                    const src = await draftAudit.ensureDraftRegistered(conn, before.draft_container_name, req.userEmail);
                    await draftAudit.recordDraftAudit(conn, {
                        draftId: src.id, action: 'line_removed',
                        before: { draftName: src.name, ...beforeSnap, movedTo: after.draft_container_name },
                        userEmail: req.userEmail,
                    });
                    const dst = await draftAudit.ensureDraftRegistered(conn, after.draft_container_name, req.userEmail);
                    await draftAudit.recordDraftAudit(conn, {
                        draftId: dst.id, action: 'line_added',
                        after: { draftName: dst.name, ...afterSnap, movedFrom: before.draft_container_name },
                        userEmail: req.userEmail,
                    });
                } else if (beforeSnap.allocated !== afterSnap.allocated) {
                    const reg = await draftAudit.ensureDraftRegistered(conn, after.draft_container_name, req.userEmail);
                    await draftAudit.recordDraftAudit(conn, {
                        draftId: reg.id, action: 'line_updated',
                        before: { draftName: reg.name, ...beforeSnap },
                        after: { draftName: reg.name, ...afterSnap },
                        userEmail: req.userEmail,
                    });
                }
                // A name change moves the line, so both drafts re-sync.
                await shipmentSync.shadow(conn, {
                    site: 'PUT /draft-containers/:id', inTx: false, keyKind: 'draft', keyValue: after.draft_container_name,
                }, async (c) => {
                    await shipmentSync.syncDraft(c, after.draft_container_name, { userEmail: req.userEmail });
                    if (before.draft_container_name !== after.draft_container_name) {
                        await shipmentSync.syncDraft(c, before.draft_container_name, { userEmail: req.userEmail });
                    }
                });
                return { row: after };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.notFound) return res.status(404).json({ error: `Allocation ${id} not found.` });
        if (result.duplicate) return res.status(409).json({ error: 'This order is already in the target draft container.' });
        res.json(rowToDraftAllocation(result.row));
    } catch (error) {
        log.error('[PUT /draft-containers/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Hard delete of one line. The row itself is planning state, but its removal
// is recorded against the draft's registry id so the draft's history keeps it.
app.delete('/api/v1/draft-containers/:id', async (req, res) => {
    try {
        await draftRegistryReady;
        await shipmentsSchemaReady;
        const { id } = req.params;
        const deleted = await withConnection(async (conn) => {
            const [existing] = await conn.query(`${DRAFT_ALLOC_SELECT} WHERE dca.id = ?`, [id]);
            // The name, read without the live-order join, so the shadow also
            // follows the removal of a line whose order was soft-deleted.
            const [raw] = await conn.query('SELECT draft_container_name FROM draft_container_allocations WHERE id = ?', [id]);
            const [r] = await conn.query('DELETE FROM draft_container_allocations WHERE id = ?', [id]);
            if (r.affectedRows > 0 && existing.length) {
                const row = existing[0];
                const reg = await draftAudit.ensureDraftRegistered(conn, row.draft_container_name, req.userEmail);
                await draftAudit.recordDraftAudit(conn, {
                    draftId: reg.id, action: 'line_removed',
                    before: { draftName: reg.name, ...draftAudit.lineSnapshot(row) },
                    userEmail: req.userEmail,
                });
            }
            if (r.affectedRows > 0 && raw.length) {
                const name = raw[0].draft_container_name;
                await shipmentSync.shadow(conn, {
                    site: 'DELETE /draft-containers/:id', inTx: false, keyKind: 'draft', keyValue: name,
                }, c => shipmentSync.syncDraft(c, name, { userEmail: req.userEmail }));
            }
            return r.affectedRows > 0;
        });
        if (!deleted) return res.status(404).json({ error: `Allocation ${id} not found.` });
        res.json({ ok: true, id: Number(id) });
    } catch (error) {
        log.error('[DELETE /draft-containers/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Draft container lifecycle (whole-draft operations) ────────────────────
// Rename a draft everywhere it is keyed by name — allocations, quote/forwarder
// documents, QA documents — in one transaction, under its existing registry
// id. Replaces the per-line PUT fan-out (which could split a draft in two on a
// partial failure and left the draft's documents behind under the old name).
// Body: { from, to }. 404 unknown draft · 409 target name already in use.
app.post('/api/v1/draft-containers/rename', async (req, res) => {
    try {
        await draftRegistryReady;
        await shipmentsSchemaReady;
        const { from, to } = req.body || {};
        if (typeof from !== 'string' || !from.trim()) return res.status(400).json({ error: 'from is required.' });
        if (typeof to !== 'string' || !to.trim()) return res.status(400).json({ error: 'to is required.' });
        if (to.trim().length > 100) return res.status(400).json({ error: 'to must be 100 characters or fewer.' });

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const r = await draftAudit.renameDraft(conn, { from, to, userEmail: req.userEmail });
                if (r.notFound || r.conflict || r.invalid || r.unchanged) { await conn.rollback(); return r; }
                // The open shipment follows the name (its open_key moves); the
                // old name is then left with nothing to mirror.
                await shipmentSync.shadow(conn, {
                    site: 'POST /draft-containers/rename', inTx: true, keyKind: 'draft', keyValue: r.name,
                }, async (c) => {
                    await shipmentSync.syncDraft(c, r.name, { userEmail: req.userEmail });
                    await shipmentSync.syncDraft(c, from, { userEmail: req.userEmail });
                });
                await conn.commit();
                return r;
            } catch (e) {
                await conn.rollback();
                throw e;
            }
        });
        if (result.invalid) return res.status(400).json({ error: 'from and to are required.' });
        if (result.notFound) return res.status(404).json({ error: `Draft container "${from.trim()}" not found.` });
        if (result.conflict) return res.status(409).json({ error: `A draft container named "${to.trim()}" already exists.` });
        if (result.unchanged) return res.json({ ok: true, name: from.trim(), allocations: 0, documents: 0, qaDocuments: 0, unchanged: true });
        res.json({ ok: true, id: result.id, name: result.name, allocations: result.allocations, documents: result.documents, qaDocuments: result.qaDocuments });
    } catch (error) {
        log.error('[POST /draft-containers/rename]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Close a draft: delete every allocation in one statement and record why.
// Body: { name, reason: 'deleted' | 'converted', containerNumber?,
//         externalContainerNumber?, vesselName?, eta?, etd?, freightType?,
//         port?, awbNumber?, packs? } — the conversion details are stored on
// the 'converted' event so the history says which real container it became.
// Idempotent: closing an already-closed draft is a no-op 200.
app.post('/api/v1/draft-containers/close', async (req, res) => {
    try {
        await draftRegistryReady;
        await shipmentsSchemaReady;
        const body = req.body || {};
        const { name, reason } = body;
        if (typeof name !== 'string' || !name.trim()) return res.status(400).json({ error: 'name is required.' });
        if (reason !== 'deleted' && reason !== 'converted') return res.status(400).json({ error: "reason must be 'deleted' or 'converted'." });
        if (reason === 'converted' && (typeof body.containerNumber !== 'string' || !body.containerNumber.trim())) {
            return res.status(400).json({ error: 'containerNumber is required when reason is converted.' });
        }

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const r = await draftAudit.closeDraft(conn, { name, reason, userEmail: req.userEmail, details: body });
                if (r.notFound || r.invalid) { await conn.rollback(); return r; }
                // A repeat close (alreadyClosed) is a legacy no-op, and one here.
                // 'deleted' cancels the draft's shipment; 'converted' into N
                // merges it with N's holder (shipmentSync.syncDraftClose).
                if (!r.alreadyClosed) {
                    await shipmentSync.shadow(conn, {
                        site: 'POST /draft-containers/close', inTx: true, keyKind: 'draft', keyValue: name.trim(),
                    }, c => shipmentSync.syncDraftClose(c, {
                        name, reason, details: body, closeResult: r, userEmail: req.userEmail,
                    }));
                }
                await conn.commit();
                return r;
            } catch (e) {
                await conn.rollback();
                throw e;
            }
        });
        if (result.invalid) return res.status(400).json({ error: 'name and reason are required.' });
        if (result.notFound) return res.status(404).json({ error: `Draft container "${name.trim()}" not found.` });
        res.json({ ok: true, id: result.id, name: result.name, reason, deleted: result.deleted, alreadyClosed: !!result.alreadyClosed });
    } catch (error) {
        log.error('[POST /draft-containers/close]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Registry of every draft container that has ever existed, with live counts,
// newest activity first. Query: ?name= exact lookup · ?q= substring on name /
// container number · ?status= open | empty | converted | deleted | closed ·
// ?limit= (default 500, max 2000). A draft's history is then
// GET /audit-log?entityType=draft_container&entityId=<id>.
app.get('/api/v1/draft-container-registry', async (req, res) => {
    try {
        await draftRegistryReady;
        const { name, q, status, limit } = req.query;
        const data = await withConnection(conn => draftAudit.listDraftRecords(conn, { name, q, status, limit }));
        res.json({ data });
    } catch (error) {
        log.error('[GET /draft-container-registry]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Planned container allocations ─────────────────────────────────────────
// Independent twin of the draft-container planning layer above: one order can
// sit in many user-named "planned containers" with different allocated
// quantities. Deliberately allocations-only — no PDF/CSV generation and no
// forwarder email, so the routes stop at CRUD.
const PLANNED_ALLOC_SELECT = `
    SELECT pca.id, pca.order_id, pca.planned_container_name, pca.allocated,
           pca.created_at, pca.updated_at,
           orders.jf_code, orders.asin, orders.product_name, orders.quantity AS order_quantity,
           orders.status AS order_status, orders.supplier, orders.po_number,
           orders.purchase_order_id, orders.container_number, orders.eta
    FROM planned_container_allocations pca
    INNER JOIN orders ON orders.id = pca.order_id AND orders.deleted_at IS NULL
`;

function rowToPlannedAllocation(row) {
    return {
        id: row.id,
        orderId: row.order_id,
        plannedContainerName: row.planned_container_name,
        allocated: Number(row.allocated),
        createdAt: row.created_at?.toISOString?.() ?? row.created_at,
        updatedAt: row.updated_at?.toISOString?.() ?? row.updated_at,
        order: {
            id: row.order_id,
            jfCode: row.jf_code || null,
            asin: row.asin || null,
            productName: row.product_name || null,
            quantity: row.order_quantity != null ? Number(row.order_quantity) : null,
            status: row.order_status || null,
            supplier: row.supplier || null,
            poNumber: row.po_number || null,
            purchaseOrderId: row.purchase_order_id ?? null,
            containerNumber: row.container_number || null,
            eta: formatDate(row.eta),
        },
    };
}

// List allocations, optionally filtered by planned_container_name or order_id.
// GET /api/v1/planned-containers                → every row
// GET /api/v1/planned-containers?name=PLANNED1  → one planned container's lines
// GET /api/v1/planned-containers?orderId=42     → every planned container an order sits in
app.get('/api/v1/planned-containers', async (req, res) => {
    try {
        await plannedContainerAllocationsSchemaReady;
        const { name, orderId } = req.query;
        const where = [];
        const params = [];
        if (name) { where.push('pca.planned_container_name = ?'); params.push(String(name)); }
        if (orderId) { where.push('pca.order_id = ?'); params.push(Number(orderId)); }
        const sql = `${PLANNED_ALLOC_SELECT}${where.length ? ' WHERE ' + where.join(' AND ') : ''} ORDER BY pca.planned_container_name ASC, pca.id ASC`;

        const rows = await withConnection(async (conn) => {
            const [rs] = await conn.query(sql, params);
            return rs;
        });
        res.json({ data: rows.map(rowToPlannedAllocation) });
    } catch (error) {
        log.error('[GET /planned-containers]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Fetch a single allocation row by its primary key.
app.get('/api/v1/planned-containers/:id', async (req, res) => {
    try {
        await plannedContainerAllocationsSchemaReady;
        const { id } = req.params;
        const row = await withConnection(async (conn) => {
            const [rs] = await conn.query(`${PLANNED_ALLOC_SELECT} WHERE pca.id = ?`, [id]);
            return rs[0] || null;
        });
        if (!row) return res.status(404).json({ error: `Allocation ${id} not found.` });
        res.json(rowToPlannedAllocation(row));
    } catch (error) {
        log.error('[GET /planned-containers/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Create an allocation. Body: { orderId, plannedContainerName, allocated }.
// 409 if (orderId, plannedContainerName) already exists — callers should PUT
// to change the allocated quantity instead.
app.post('/api/v1/planned-containers', async (req, res) => {
    try {
        await plannedContainerAllocationsSchemaReady;
        await shipmentsSchemaReady;
        const { orderId, plannedContainerName, allocated } = req.body || {};
        const oid = Number(orderId);
        const name = typeof plannedContainerName === 'string' ? plannedContainerName.trim() : '';
        const qty = Number(allocated);
        if (!Number.isInteger(oid) || oid <= 0) return res.status(400).json({ error: 'orderId must be a positive integer.' });
        if (!name) return res.status(400).json({ error: 'plannedContainerName is required.' });
        if (!Number.isFinite(qty) || qty < 0) return res.status(400).json({ error: 'allocated must be a non-negative number.' });

        const result = await withConnection(async (conn) => {
            const [orderRows] = await conn.query('SELECT id FROM orders WHERE id = ? AND deleted_at IS NULL', [oid]);
            if (!orderRows.length) return { orderNotFound: true };
            try {
                const [ins] = await conn.query(
                    `INSERT INTO planned_container_allocations (order_id, planned_container_name, allocated) VALUES (?, ?, ?)`,
                    [oid, name, qty]
                );
                const [rs] = await conn.query(`${PLANNED_ALLOC_SELECT} WHERE pca.id = ?`, [ins.insertId]);
                await shipmentSync.shadow(conn, {
                    site: 'POST /planned-containers', inTx: false, keyKind: 'planned', keyValue: name,
                }, c => shipmentSync.syncPlanned(c, name, { userEmail: req.userEmail }));
                return { row: rs[0] };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.orderNotFound) return res.status(404).json({ error: `Order ${oid} not found.` });
        if (result.duplicate) return res.status(409).json({ error: `Order ${oid} is already in planned container ${name}.` });
        res.status(201).json(rowToPlannedAllocation(result.row));
    } catch (error) {
        log.error('[POST /planned-containers]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Update an allocation. Any of { plannedContainerName, allocated } can be sent;
// orderId is intentionally immutable — to move a line to a different order
// the caller should DELETE + POST.
app.put('/api/v1/planned-containers/:id', async (req, res) => {
    try {
        await plannedContainerAllocationsSchemaReady;
        await shipmentsSchemaReady;
        const { id } = req.params;
        const { plannedContainerName, allocated } = req.body || {};
        const fields = [];
        const values = [];
        if (plannedContainerName !== undefined) {
            const name = String(plannedContainerName).trim();
            if (!name) return res.status(400).json({ error: 'plannedContainerName cannot be empty.' });
            fields.push('planned_container_name = ?');
            values.push(name);
        }
        if (allocated !== undefined) {
            const qty = Number(allocated);
            if (!Number.isFinite(qty) || qty < 0) return res.status(400).json({ error: 'allocated must be a non-negative number.' });
            fields.push('allocated = ?');
            values.push(qty);
        }
        if (!fields.length) return res.status(400).json({ error: 'No fields to update.' });

        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query('SELECT id, planned_container_name FROM planned_container_allocations WHERE id = ?', [id]);
            if (!existing.length) return { notFound: true };
            try {
                values.push(id);
                await conn.query(`UPDATE planned_container_allocations SET ${fields.join(', ')} WHERE id = ?`, values);
                const [rs] = await conn.query(`${PLANNED_ALLOC_SELECT} WHERE pca.id = ?`, [id]);
                // A name change moves the line, so both planned containers re-sync.
                const oldName = existing[0].planned_container_name;
                const [cur] = await conn.query('SELECT planned_container_name FROM planned_container_allocations WHERE id = ?', [id]);
                const newName = cur.length ? cur[0].planned_container_name : oldName;
                await shipmentSync.shadow(conn, {
                    site: 'PUT /planned-containers/:id', inTx: false, keyKind: 'planned', keyValue: newName,
                }, async (c) => {
                    await shipmentSync.syncPlanned(c, newName, { userEmail: req.userEmail });
                    if (newName !== oldName) await shipmentSync.syncPlanned(c, oldName, { userEmail: req.userEmail });
                });
                return { row: rs[0] };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.notFound) return res.status(404).json({ error: `Allocation ${id} not found.` });
        if (result.duplicate) return res.status(409).json({ error: 'This order is already in the target planned container.' });
        res.json(rowToPlannedAllocation(result.row));
    } catch (error) {
        log.error('[PUT /planned-containers/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Hard delete — these rows are pure planning state, no audit history to keep.
app.delete('/api/v1/planned-containers/:id', async (req, res) => {
    try {
        await plannedContainerAllocationsSchemaReady;
        await shipmentsSchemaReady;
        const { id } = req.params;
        const deleted = await withConnection(async (conn) => {
            const [existing] = await conn.query('SELECT planned_container_name FROM planned_container_allocations WHERE id = ?', [id]);
            const [r] = await conn.query('DELETE FROM planned_container_allocations WHERE id = ?', [id]);
            if (r.affectedRows > 0 && existing.length) {
                // Removing the last line ends the planned container, and cancels its shipment.
                const name = existing[0].planned_container_name;
                await shipmentSync.shadow(conn, {
                    site: 'DELETE /planned-containers/:id', inTx: false, keyKind: 'planned', keyValue: name,
                }, c => shipmentSync.syncPlanned(c, name, { userEmail: req.userEmail }));
            }
            return r.affectedRows > 0;
        });
        if (!deleted) return res.status(404).json({ error: `Allocation ${id} not found.` });
        res.json({ ok: true, id: Number(id) });
    } catch (error) {
        log.error('[DELETE /planned-containers/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Draft container PDF generation + send ────────────────────────────────
// Generates a "Delivery Quote Request" PDF for one draft container, stores
// versioned copies in S3 (same bucket as PO docs, draft-containers/ prefix),
// and supports emailing a chosen version to a freight forwarder via Front.
//
// Note: `s3`, `PO_BUCKET`, `publicS3Url`, and `EMAIL_RE` are declared further
// down with the PO PDF block. That's fine — route handlers close over the
// module scope, so by the time a request fires those bindings exist.

// Aggregates allocations for one draft into PDF-ready lines. CBM per line is
// cbm_per_unit × allocated when available; falls back to a proportional
// slice of order_cbm. Cartons are computed from units_per_carton.
async function loadDraftForPdf(conn, draftName) {
    const [rows] = await conn.query(
        `SELECT dca.allocated, dca.draft_container_name,
                o.id AS order_id, o.jf_code, o.asin, o.product_name, o.quantity,
                o.cbm_per_unit, o.order_cbm, o.units_per_carton, o.port, o.lot_number
           FROM draft_container_allocations dca
           INNER JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
          WHERE dca.draft_container_name = ?
          ORDER BY dca.id ASC`,
        [draftName]
    );

    const originPorts = [...new Set(rows.map(r => r.port).filter(Boolean))];
    const lines = rows.map(r => {
        const allocated = Number(r.allocated || 0);
        const orderQty = Number(r.quantity || 0);
        let cbm = null;
        if (r.cbm_per_unit != null) {
            cbm = Number(r.cbm_per_unit) * allocated;
        } else if (r.order_cbm != null && orderQty > 0) {
            cbm = (Number(r.order_cbm) / orderQty) * allocated;
        }
        const cartons = r.units_per_carton != null && Number(r.units_per_carton) > 0
            ? Math.ceil(allocated / Number(r.units_per_carton))
            : null;
        return {
            sku: r.jf_code || r.asin || '',
            name: r.product_name || '',
            quantity: allocated,
            cbm,
            cartons,
            lot: r.lot_number || '',
        };
    });

    return { lines, originPorts };
}

// Richer loader for the 'forwarder-quote' PDF. One row per allocation, enriched
// with carton weight/dimensions. Source precedence: the spec frozen on the
// order row at create time (so a regenerated quote reproduces the original
// numbers even after jfpro.products changes), then the live product_carton_sizes
// view (joined by jf_code), then unit-weight × units-per-carton.
async function loadForwarderQuoteForPdf(conn, draftName, supplierName = null) {
    const [allRows] = await conn.query(
        `SELECT dca.allocated,
                o.jf_code, o.asin, o.product_name, o.supplier, o.port, o.po_number,
                o.units_per_carton, o.lot_number,
                o.carton_weight, o.carton_height, o.carton_width, o.carton_depth, o.carton_cbm
           FROM draft_container_allocations dca
           INNER JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
          WHERE dca.draft_container_name = ?
          ORDER BY dca.id ASC`,
        [draftName]
    );
    // For supplier-quote, keep only rows whose order supplier matches the
    // requested one (orders store supplier as free text; compare trimmed/casefold).
    const norm = s => String(s || '').trim().toLowerCase();
    const rows = supplierName != null
        ? allRows.filter(r => norm(r.supplier) === norm(supplierName))
        : allRows;
    if (!rows.length) return { lines: [], originPorts: [] };

    const jfCodes = [...new Set(rows.map(r => r.jf_code).filter(Boolean))];

    // Carton sizes by jf_code (first row wins if a jf_code has several SKUs).
    const cartonByJf = new Map();
    if (jfCodes.length) {
        const [crows] = await conn.query(
            `SELECT jf_code, weight, carton_weight, carton_qty,
                    carton_height, carton_width, carton_depth
               FROM product_carton_sizes
              WHERE jf_code IN (${jfCodes.map(() => '?').join(',')})`,
            jfCodes
        );
        for (const c of crows) if (!cartonByJf.has(c.jf_code)) cartonByJf.set(c.jf_code, c);
    }

    const num = v => (v != null ? Number(v) : 0);
    const originPorts = [...new Set(rows.map(r => r.port).filter(Boolean))];

    const lines = rows.map(r => {
        const c = cartonByJf.get(r.jf_code) || {};
        const orderedUnits = num(r.allocated);
        // Prefer the carton spec frozen on the order row (snapshot at create);
        // fall back to the live catalogue view, then unit-weight × units-per-carton.
        const cartonWeight = num(r.carton_weight) || num(c.carton_weight) || num(c.weight) * num(c.carton_qty);
        const unitsPerCarton = num(r.units_per_carton) || num(c.carton_qty);
        const cartonH = num(r.carton_height) || num(c.carton_height);
        const cartonL = num(r.carton_depth) || num(c.carton_depth);
        const cartonW = num(r.carton_width) || num(c.carton_width);
        const cartonVol = cartonH * cartonW * cartonL;
        // Prefer the CBM frozen on the order row (honours a user-supplied value);
        // otherwise recompute from the resolved dimensions.
        const cartonCbm = num(r.carton_cbm) || (cartonVol > 0 ? cartonVol / 1_000_000 : 0);
        const noOfCartons = unitsPerCarton > 0 ? Math.ceil(orderedUnits / unitsPerCarton) : 0;
        const totalCbm = cartonCbm * noOfCartons;
        return {
            sku: r.product_name || r.jf_code || r.asin || '',
            jfCode: r.jf_code || '',
            lot: r.lot_number || '',
            orderedUnits,
            cartonWeight,
            unitsPerCarton,
            cartonH,
            cartonL,
            cartonW,
            cartonCbm,
            noOfCartons,
            totalCbm,
            supplier: r.supplier || '',
            port: r.port || '',
            poNumber: r.po_number || '',
        };
    });

    return { lines, originPorts };
}

// Build the CSV companion for a draft-container document, drawing from the same
// loaders the matching PDF was rendered from so the two formats carry identical
// figures. `doc` is a row carrying { type, supplier, draft_container_name }.
// Returns a Buffer, or null when the (filtered) draft has no lines.
async function buildDraftDocumentCsv(conn, doc) {
    const draftName = doc.draft_container_name;
    if (doc.type === 'quote') {
        const { lines } = await loadDraftForPdf(conn, draftName);
        return lines.length ? buildDraftContainerCsv({ name: draftName }, lines) : null;
    }
    // forwarder-quote (supplier null = combined) and supplier-quote both use the
    // forwarder loader, filtered by the doc's supplier (null = all suppliers).
    const { lines } = await loadForwarderQuoteForPdf(conn, draftName, doc.supplier);
    if (!lines.length) return null;
    return doc.type === 'supplier-quote'
        ? buildSupplierQuoteCsv({ name: draftName, supplier: doc.supplier }, lines)
        : buildForwarderQuoteCsv({ name: draftName }, lines);
}

// Distinct supplier names allocated to a draft container (free-text on orders),
// trimmed and de-duplicated case-insensitively so we don't emit two near-identical
// per-supplier PDFs for "Acme" vs "acme". Blank suppliers are skipped — a file
// can't be named after a nameless supplier; those lines still appear in the main
// forwarder PDF.
async function loadDraftSuppliers(conn, draftName) {
    const [rows] = await conn.query(
        `SELECT DISTINCT TRIM(o.supplier) AS supplier
           FROM draft_container_allocations dca
           INNER JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
          WHERE dca.draft_container_name = ?
            AND o.supplier IS NOT NULL AND TRIM(o.supplier) <> ''
          ORDER BY supplier ASC`,
        [draftName]
    );
    const seen = new Set();
    const out = [];
    for (const r of rows) {
        const s = String(r.supplier || '').trim();
        if (!s) continue;
        const key = s.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(s);
    }
    return out;
}

// Build + store ONE draft-container document (quote / forwarder-quote /
// supplier-quote), returning its DB record. Extracted from the generate route so
// the route can produce a main forwarder PDF plus one per-supplier PDF in a single
// request. Returns { noLines: true } when the (filtered) draft has no lines.
async function generateDraftDocument(conn, { name, docType, supplierName, comments, generatedByEmail, batchId = null }) {
    const { lines, originPorts } = docType === 'quote'
        ? await loadDraftForPdf(conn, name)
        : await loadForwarderQuoteForPdf(conn, name, supplierName);
    if (!lines.length) return { noLines: true };

    const draftMeta = { name, date: new Date(), originPorts, comments: comments || '', supplier: supplierName };
    let pdfBuffer, csvBuffer;
    if (docType === 'supplier-quote') {
        pdfBuffer = await buildSupplierQuotePdf(draftMeta, lines);
        csvBuffer = buildSupplierQuoteCsv(draftMeta, lines);
    } else if (docType === 'forwarder-quote') {
        pdfBuffer = await buildForwarderQuotePdf(draftMeta, lines);
        csvBuffer = buildForwarderQuoteCsv(draftMeta, lines);
    } else {
        pdfBuffer = await buildDraftContainerPdf(draftMeta, lines);
        csvBuffer = buildDraftContainerCsv(draftMeta, lines);
    }

    // Versions are sequenced per (draft, type, supplier) so each flavour — and
    // each supplier within supplier-quote — has its own v1, v2, …
    const [versionRows] = await conn.query(
        `SELECT COALESCE(MAX(version), 0) AS max_version
           FROM draft_container_documents
          WHERE draft_container_name = ? AND type = ?
            AND ((supplier IS NULL AND ? IS NULL) OR supplier = ?)
            AND deleted_at IS NULL`,
        [name, docType, supplierName, supplierName]
    );
    const version = Number(versionRows[0].max_version) + 1;
    const safeName = name.replace(/[^A-Za-z0-9._-]/g, '_');
    const token = uuidv4();
    // Any per-supplier doc (supplier-quote, or a per-supplier forwarder-quote
    // copy) carries the supplier name stripped to a–z so it doesn't collide with
    // the combined PDF or the other suppliers' copies.
    const supplierSlug = supplierName ? (supplierName.replace(/[^A-Za-z]/g, '') || 'supplier') : null;
    const fileBase = supplierSlug
        ? `${docType}-${supplierSlug}-${safeName}-v${version}`
        : `${docType}-${safeName}-v${version}`;
    const s3Key = `draft-containers/${token}/${fileBase}.pdf`;
    const publicUrl = publicS3Url(s3Key);
    // CSV sibling, same key prefix so it lives next to the PDF in S3.
    const csvS3Key = `draft-containers/${token}/${fileBase}.csv`;
    const csvPublicUrl = publicS3Url(csvS3Key);

    await s3.send(new PutObjectCommand({
        Bucket: PO_BUCKET,
        Key: s3Key,
        Body: pdfBuffer,
        ContentType: 'application/pdf',
        ContentDisposition: `inline; filename="${fileBase}.pdf"`,
    }));
    await s3.send(new PutObjectCommand({
        Bucket: PO_BUCKET,
        Key: csvS3Key,
        Body: csvBuffer,
        ContentType: 'text/csv; charset=utf-8',
        // attachment (not inline) so a click downloads the .csv with its filename
        // rather than rendering raw text in a browser tab; PDFs stay inline.
        ContentDisposition: `attachment; filename="${fileBase}.csv"`,
    }));

    const [insertResult] = await conn.query(
        `INSERT INTO draft_container_documents
            (draft_container_name, version, type, supplier, s3_key, public_url, file_size,
             csv_s3_key, csv_public_url, csv_file_size, generated_by_email, batch_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [name, version, docType, supplierName, s3Key, publicUrl, pdfBuffer.length,
         csvS3Key, csvPublicUrl, csvBuffer.length, generatedByEmail || null, batchId]
    );
    return {
        documentId: insertResult.insertId,
        draftContainerName: name,
        type: docType,
        supplier: supplierName,
        version,
        fileSize: pdfBuffer.length,
        url: publicUrl,
        csvUrl: csvPublicUrl,
    };
}

// A draft-container name can contain "/" (e.g. "… - HW/JFA Ningbo Container 2")
// and spaces. Behind API Gateway (httpApi), an encoded "%2F" in the path is
// decoded to a REAL "/" before Express routes, which splits the name across
// segments and makes a single-token `:name` param fail to match (→ Express 404,
// "Cannot GET …"). So the name-in-path routes below match the name with a
// greedy regex capture (req.params[0]) instead of `:name`, and this helper
// decodes whatever encoding survives to that point (Express decodes the capture,
// but a stray "%20" left by the gateway is handled defensively).
function draftNameFromPath(raw) {
    let s = String(raw || '');
    if (s.includes('%')) { try { s = decodeURIComponent(s); } catch { /* keep as-is */ } }
    return s.trim();
}

// Build + store one generate run's document set for a draft and record its one
// 'document_generated' event. For a split forwarder quote that is the combined
// PDF plus one per-supplier copy, all sharing a batch id so the forwarder email
// can attach the whole set. Extracted from the generate route below so POST
// /shipments/:id/documents produces exactly the same set. Returns { noLines }
// or { main, batchId, supplierDocuments?, failedSuppliers? }.
async function generateDraftDocumentSet(conn, { name, docType, supplierName, comments, wantSplit, userEmail }) {
    const batchId = wantSplit ? uuidv4() : null;
    const main = await generateDraftDocument(conn, {
        name, docType, supplierName, comments, generatedByEmail: userEmail, batchId,
    });
    if (main.noLines) return { noLines: true };

    let out = { main, batchId };
    if (wantSplit) {
        // One PDF per supplier, best-effort: a single supplier failing (or
        // having no lines after filtering) must not lose the main PDF or the
        // other suppliers. Collect outcomes for the response.
        const suppliers = await loadDraftSuppliers(conn, name);
        const supplierDocuments = [];
        const failedSuppliers = [];
        for (const s of suppliers) {
            try {
                const doc = await generateDraftDocument(conn, {
                    name, docType: 'forwarder-quote', supplierName: s,
                    comments, generatedByEmail: userEmail, batchId,
                });
                if (!doc.noLines) supplierDocuments.push(doc);
            } catch (err) {
                log.error(`[generate] supplier-quote failed for "${s}"`, err);
                failedSuppliers.push(s);
            }
        }
        out = { main, batchId, supplierDocuments, failedSuppliers };
    }

    // One event per generate run (a split run lists its per-supplier copies
    // inside the event rather than becoming N rows).
    const reg = await draftAudit.ensureDraftRegistered(conn, name, userEmail);
    await draftAudit.recordDraftAudit(conn, {
        draftId: reg.id, action: 'document_generated',
        after: {
            draftName: reg.name,
            documentId: main.documentId, type: docType, supplier: supplierName, version: main.version,
            url: main.url, csvUrl: main.csvUrl, batchId,
            comments: comments ? String(comments).slice(0, 500) : null,
            supplierDocuments: (out.supplierDocuments || []).map(d => ({
                documentId: d.documentId, supplier: d.supplier, version: d.version, url: d.url, csvUrl: d.csvUrl,
            })),
            failedSuppliers: out.failedSuppliers || [],
        },
        userEmail,
    });
    return out;
}

app.post(/^\/api\/v1\/draft-containers\/(.+)\/generate\/?$/, async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        const name = draftNameFromPath(req.params[0]);
        if (!name) return res.status(400).json({ error: 'draft container name required in path.' });
        const { comments, type, supplier } = req.body || {};

        // PDF flavour. 'quote' = the standard delivery quote request;
        // 'forwarder-quote' = the detailed carton-level forwarder quote;
        // 'supplier-quote' = the forwarder layout filtered to a single supplier.
        const docType = type === undefined || type === null || type === '' ? 'quote' : String(type);
        if (docType !== 'quote' && docType !== 'forwarder-quote' && docType !== 'supplier-quote') {
            return res.status(400).json({ error: "type must be 'quote', 'forwarder-quote' or 'supplier-quote'." });
        }

        // supplier-quote is scoped to one supplier (orders store supplier as
        // free text, so the caller sends the supplier name string).
        const supplierName = docType === 'supplier-quote' ? String(supplier || '').trim() : null;
        if (docType === 'supplier-quote' && !supplierName) {
            return res.status(400).json({ error: "supplier is required for type 'supplier-quote'." });
        }

        // Per-supplier split: emit one forwarder-quote PDF per distinct supplier in
        // the draft (forwarder layout, filtered to that supplier) alongside the main
        // combined forwarder PDF. Defaults ON for forwarder quotes (they go out as
        // forwarder emails as a collection — the combined quote plus each supplier's
        // copy); pass splitBySupplier:false to suppress. Never applies to a plain
        // 'quote' or an already-single-supplier 'supplier-quote'.
        const splitFlag = req.body ? req.body.splitBySupplier : undefined;
        const wantSplit = docType === 'forwarder-quote' && splitFlag !== false;

        await draftRegistryReady;
        await shipmentsSchemaReady;
        const result = await withConnection(async (conn) => {
            const out = await generateDraftDocumentSet(conn, {
                name, docType, supplierName, comments, wantSplit, userEmail: req.userEmail,
            });
            if (out.noLines) return out;
            // Stamps the new documents with the draft's shipment.
            await shipmentSync.shadow(conn, {
                site: 'POST /draft-containers/:name/generate', inTx: false, keyKind: 'draft', keyValue: name,
            }, c => shipmentSync.syncDraft(c, name, { userEmail: req.userEmail }));
            return out;
        });
        const batchId = result.batchId;

        if (result.noLines) {
            const msg = docType === 'supplier-quote'
                ? `Draft container "${name}" has no allocations for supplier "${supplierName}".`
                : `Draft container "${name}" has no allocations.`;
            return res.status(400).json({ error: msg });
        }
        const { main } = result;
        const payload = {
            documentId: main.documentId,
            draftContainerName: name,
            type: docType,
            supplier: supplierName,
            version: main.version,
            fileSize: main.fileSize,
            url: main.url,
            csvUrl: main.csvUrl,
        };
        if (wantSplit) {
            payload.batchId = batchId;
            payload.supplierDocuments = result.supplierDocuments.map(d => ({
                documentId: d.documentId,
                type: d.type,
                supplier: d.supplier,
                version: d.version,
                fileSize: d.fileSize,
                url: d.url,
                csvUrl: d.csvUrl,
            }));
            if (result.failedSuppliers.length) payload.failedSuppliers = result.failedSuppliers;
        }
        res.status(201).json(payload);
    } catch (error) {
        log.error('[POST /draft-containers/:name/generate]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get(/^\/api\/v1\/draft-containers\/(.+)\/documents\/?$/, async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        const name = draftNameFromPath(req.params[0]);
        if (!name) return res.status(400).json({ error: 'draft container name required in path.' });

        const { docs, sendsByDoc } = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT id, draft_container_name, version, type, supplier, s3_key, public_url, file_size,
                        csv_s3_key, csv_public_url, csv_file_size,
                        batch_id, generated_by_email, generated_at
                   FROM draft_container_documents
                  WHERE draft_container_name = ? AND deleted_at IS NULL
                  ORDER BY type ASC, version DESC`,
                [name]
            );
            const ids = r.map(d => d.id);
            const sendRows = ids.length
                ? (await conn.query(
                    `SELECT id, draft_container_document_id, sent_to, subject,
                            front_message_uid, front_conversation_id, sent_by_email, sent_at
                       FROM draft_container_document_sends
                      WHERE draft_container_document_id IN (${ids.map(() => '?').join(',')})
                      ORDER BY sent_at DESC`,
                    ids
                ))[0]
                : [];
            const byDoc = new Map();
            for (const s of sendRows) {
                if (!byDoc.has(s.draft_container_document_id)) byDoc.set(s.draft_container_document_id, []);
                byDoc.get(s.draft_container_document_id).push(s);
            }
            return { docs: r, sendsByDoc: byDoc };
        });

        res.json({
            data: docs.map(r => ({
                id: r.id,
                draftContainerName: r.draft_container_name,
                version: r.version,
                type: r.type || 'quote',
                supplier: r.supplier || null,
                // Ties together the PDFs produced by one split-generate run. Group
                // by this in the UI to show the exact set a forwarder email sends
                // (combined + per-supplier copies); null for non-split documents.
                batchId: r.batch_id || null,
                fileSize: r.file_size,
                url: r.public_url || publicS3Url(r.s3_key),
                csvUrl: r.csv_public_url || (r.csv_s3_key ? publicS3Url(r.csv_s3_key) : null),
                csvFileSize: r.csv_file_size ?? null,
                generatedByEmail: r.generated_by_email || null,
                generatedAt: r.generated_at?.toISOString?.() ?? r.generated_at,
                sends: (sendsByDoc.get(r.id) || []).map(s => ({
                    id: s.id,
                    sentTo: typeof s.sent_to === 'string' ? JSON.parse(s.sent_to) : s.sent_to,
                    subject: s.subject || null,
                    frontMessageUid: s.front_message_uid || null,
                    frontConversationId: s.front_conversation_id || null,
                    sentByEmail: s.sent_by_email || null,
                    sentAt: s.sent_at?.toISOString?.() ?? s.sent_at,
                })),
            })),
        });
    } catch (error) {
        log.error('[GET /draft-containers/:name/documents]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.post('/api/v1/draft-container-documents/:id/email', async (req, res) => {
    try {
        await draftContainerAllocationsSchemaReady;
        await draftRegistryReady;
        await auditLogSchemaReady;
        await emailTemplatesSchemaReady;
        await emailReceiptsSchemaReady;

        const { id } = req.params;
        // `subject`/`body` are optional per-send overrides. When the caller
        // supplies a body (typically the template fetched via
        // GET /email-templates, then edited in the UI), it's used verbatim;
        // otherwise the stored `draft_container_quote` template is rendered.
        const { to, subject, body } = req.body || {};

        const toAddresses = (Array.isArray(to) ? to : [to])
            .filter(x => typeof x === 'string')
            .map(x => x.trim())
            .filter(Boolean);
        if (toAddresses.length === 0 || toAddresses.some(a => !EMAIL_RE.test(a))) {
            return res.status(400).json({ error: 'A valid `to` email address (or array of addresses) is required.' });
        }
        if (!process.env.FRONT_API_TOKEN || !process.env.FRONT_CHANNEL_ID) {
            return res.status(500).json({ error: 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).' });
        }
        if (!PO_BUCKET) {
            return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        }

        const result = await withConnection(async (conn) => {
            const [docRows] = await conn.query(
                `SELECT id, draft_container_name, version, s3_key, public_url, file_size, csv_s3_key, type, supplier, batch_id
                   FROM draft_container_documents
                  WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!docRows.length) return { notFound: true };
            const doc = docRows[0];

            // Emailing a split forwarder-quote attaches the whole collection of
            // forwarder quotes — the main combined PDF (supplier NULL) first, then
            // one per-supplier forwarder copy A–Z. Restricting to the same `type`
            // guarantees we only ever group forwarder quotes: any other document
            // type that happened to share the batch is never mixed in. Any other
            // document emails just itself.
            let docsToSend = [doc];
            if (doc.type === 'forwarder-quote' && doc.batch_id) {
                const [batchRows] = await conn.query(
                    `SELECT id, draft_container_name, version, s3_key, csv_s3_key, type, supplier
                       FROM draft_container_documents
                      WHERE batch_id = ? AND type = ? AND deleted_at IS NULL
                      ORDER BY (supplier IS NULL) DESC, supplier ASC, id ASC`,
                    [doc.batch_id, doc.type]
                );
                if (batchRows.length) docsToSend = batchRows;
            }

            // Download each PDF + its stored CSV companion. Filenames = the S3
            // objects' basenames, which already encode the supplier (e.g.
            // supplier-quote-Acme-<draft>-v1.pdf / .csv) so the pair reads as a set.
            const safeName = String(doc.draft_container_name).replace(/[^A-Za-z0-9._-]/g, '_');
            const attachments = [];
            for (const d of docsToSend) {
                const obj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: d.s3_key }));
                const bytes = Buffer.from(await obj.Body.transformToByteArray());
                const filename = d.s3_key.split('/').pop() || `draft-${safeName}-v${d.version}.pdf`;
                const item = { doc: d, pdf: { bytes, filename }, csv: null };
                // Attach the CSV stored next to the PDF. For documents generated
                // before CSVs were stored (no csv_s3_key), rebuild it on the fly so
                // the email still carries one. Best-effort: never lose the PDF send.
                try {
                    let csvBytes = null;
                    if (d.csv_s3_key) {
                        const csvObj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: d.csv_s3_key }));
                        csvBytes = Buffer.from(await csvObj.Body.transformToByteArray());
                    } else {
                        csvBytes = await buildDraftDocumentCsv(conn, d);
                    }
                    if (csvBytes && csvBytes.length) {
                        const csvName = d.csv_s3_key
                            ? (d.csv_s3_key.split('/').pop() || filename.replace(/\.pdf$/i, '') + '.csv')
                            : filename.replace(/\.pdf$/i, '') + '.csv';
                        item.csv = { bytes: csvBytes, filename: csvName };
                    }
                } catch (err) {
                    log.error('[draft-container-documents/email] CSV attach failed', { documentId: d.id, err });
                }
                attachments.push(item);
            }

            const tpl = await getEmailTemplate(conn, 'draft_container_quote');
            const vars = { draftContainerName: doc.draft_container_name };
            const finalSubject = (typeof subject === 'string' && subject.trim())
                ? subject.trim()
                : (renderTemplate(tpl.subject, vars) || `Delivery Quote Request – ${doc.draft_container_name}`);
            const htmlBody = (typeof body === 'string' && body.trim())
                ? body
                : renderTemplate(tpl.bodyHtml, vars);

            const receiptToken = await createEmailReceipt(conn, {
                emailType: 'draft_container_quote', sentTo: toAddresses, subject: finalSubject,
            });
            const bodyWithReceipt = appendReceiptLink(htmlBody, receiptToken, apiBaseUrlFromReq(req));

            const form = new FormData();
            for (const addr of toAddresses) form.append('to[]', addr);
            form.append('subject', finalSubject);
            form.append('body', bodyWithReceipt);
            form.append('body_format', 'html');
            form.append('options[archive]', 'false');
            for (const a of attachments) {
                form.append('attachments[]', new Blob([a.pdf.bytes], { type: 'application/pdf' }), a.pdf.filename);
                if (a.csv) {
                    form.append('attachments[]', new Blob([a.csv.bytes], { type: 'text/csv' }), a.csv.filename);
                }
            }

            const frontUrl = `https://api2.frontapp.com/channels/${encodeURIComponent(process.env.FRONT_CHANNEL_ID)}/messages`;
            const resp = await fetch(frontUrl, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`,
                    Accept: 'application/json',
                },
                body: form,
            });
            const respText = await resp.text();
            if (!resp.ok) {
                log.error('[draft-container-documents/email] Front API error', {
                    status: resp.status, body: respText.slice(0, 500),
                });
                return { frontError: { status: resp.status, body: respText.slice(0, 500) } };
            }
            let parsed = null;
            try { parsed = JSON.parse(respText); } catch { /* 202 with empty body is fine */ }

            const conversationUrl = parsed?._links?.related?.conversation || '';
            const frontConversationId = conversationUrl ? conversationUrl.split('/').pop() : null;
            const frontMessageUid = parsed?.message_uid || parsed?.id || null;

            // One send row per attached document (all share the Front message), so
            // each PDF shows this email in its own sends history.
            let primarySendId = null;
            for (const a of attachments) {
                const [ins] = await conn.query(
                    `INSERT INTO draft_container_document_sends
                        (draft_container_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email)
                     VALUES (?, ?, ?, ?, ?, ?)`,
                    [
                        a.doc.id,
                        JSON.stringify(toAddresses),
                        finalSubject,
                        frontMessageUid,
                        frontConversationId,
                        req.userEmail || null,
                    ]
                );
                if (a.doc.id === doc.id) primarySendId = ins.insertId;
            }
            const [sentRow] = await conn.query(
                `SELECT sent_at FROM draft_container_document_sends WHERE id = ?`,
                [primarySendId]
            );
            const sentAt = sentRow[0]?.sent_at?.toISOString?.() ?? sentRow[0]?.sent_at ?? null;
            await linkReceiptToSend(conn, receiptToken, 'draft_container_document_sends', primarySendId);

            // One email = one draft event, listing every PDF it carried.
            const reg = await draftAudit.ensureDraftRegistered(conn, doc.draft_container_name, req.userEmail);
            await draftAudit.recordDraftAudit(conn, {
                draftId: reg.id, action: 'document_sent',
                after: {
                    draftName: reg.name,
                    documentId: doc.id, type: doc.type || 'quote', supplier: doc.supplier || null, version: doc.version,
                    sendId: primarySendId, sentTo: toAddresses, subject: finalSubject,
                    frontMessageUid, frontConversationId, receiptToken,
                    attachments: attachments.map(a => ({
                        documentId: a.doc.id, type: a.doc.type || 'quote', supplier: a.doc.supplier || null,
                        version: a.doc.version, filename: a.pdf.filename, csvFilename: a.csv ? a.csv.filename : null,
                    })),
                },
                userEmail: req.userEmail,
            });

            return {
                ok: true,
                sendId: primarySendId,
                documentId: doc.id,
                draftContainerName: doc.draft_container_name,
                version: doc.version,
                sentTo: toAddresses,
                subject: finalSubject,
                sentAt,
                frontMessageUid,
                frontConversationId,
                attachments: attachments.map(a => ({
                    documentId: a.doc.id,
                    type: a.doc.type,
                    supplier: a.doc.supplier,
                    version: a.doc.version,
                    filename: a.pdf.filename,
                    csvFilename: a.csv ? a.csv.filename : null,
                })),
            };
        });

        if (result.notFound) return res.status(404).json({ error: `Draft container document ${id} not found.` });
        if (result.frontError) {
            return res.status(502).json({
                error: 'Front rejected the message.',
                frontStatus: result.frontError.status,
                frontBody: result.frontError.body,
            });
        }
        res.status(200).json(result);
    } catch (error) {
        log.error('[POST /draft-container-documents/:id/email]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Quality Assurance documents ──────────────────────────────────────────
// Builds a QC inspection sheet from an explicit set of order ids (not a draft
// container). Same carton-level layout as the forwarder quote, enriched with
// LOT (from the order), Supplier Country + Terms (from the suppliers table,
// matched to the order's free-text supplier name), and a per-order QC Units
// column supplied by the caller.
async function loadQualityAssuranceForPdf(conn, orderIds, qcUnitsByOrderId) {
    if (!orderIds.length) return { lines: [] };
    const ph = orderIds.map(() => '?').join(',');
    const [orderRows] = await conn.query(
        `SELECT id, jf_code, asin, product_name, quantity, supplier, port, po_number,
                units_per_carton, lot_number,
                carton_weight, carton_height, carton_width, carton_depth, carton_cbm
           FROM orders
          WHERE id IN (${ph}) AND deleted_at IS NULL`,
        orderIds
    );
    if (!orderRows.length) return { lines: [] };
    const byId = new Map(orderRows.map(o => [o.id, o]));

    // Carton sizes by jf_code (first row wins) — same source/derivation as the
    // forwarder quote.
    const jfCodes = [...new Set(orderRows.map(r => r.jf_code).filter(Boolean))];
    const cartonByJf = new Map();
    if (jfCodes.length) {
        const [crows] = await conn.query(
            `SELECT jf_code, weight, carton_weight, carton_qty,
                    carton_height, carton_width, carton_depth
               FROM product_carton_sizes
              WHERE jf_code IN (${jfCodes.map(() => '?').join(',')})`,
            jfCodes
        );
        for (const c of crows) if (!cartonByJf.has(c.jf_code)) cartonByJf.set(c.jf_code, c);
    }

    // Supplier country + incoterms, matched to the order's free-text supplier
    // name (trimmed/casefold). Blank when there's no matching supplier row.
    const norm = s => String(s || '').trim().toLowerCase();
    const [supRows] = await conn.query(
        `SELECT name, country, default_incoterms FROM suppliers WHERE deleted_at IS NULL`
    );
    const supByName = new Map();
    for (const s of supRows) supByName.set(norm(s.name), s);

    const num = v => (v != null ? Number(v) : 0);

    // Preserve the caller's requested order; ids that didn't resolve to a live
    // order are skipped.
    const lines = [];
    for (const oid of orderIds) {
        const r = byId.get(oid);
        if (!r) continue;
        const c = cartonByJf.get(r.jf_code) || {};
        const sup = supByName.get(norm(r.supplier)) || {};
        const orderedUnits = num(r.quantity);
        // Prefer the carton spec frozen on the order row; fall back to the live
        // catalogue view, then unit-weight × units-per-carton.
        const cartonWeight = num(r.carton_weight) || num(c.carton_weight) || num(c.weight) * num(c.carton_qty);
        const unitsPerCarton = num(r.units_per_carton) || num(c.carton_qty);
        const cartonH = num(r.carton_height) || num(c.carton_height);
        const cartonL = num(r.carton_depth) || num(c.carton_depth);
        const cartonW = num(r.carton_width) || num(c.carton_width);
        const cartonVol = cartonH * cartonW * cartonL;
        // Prefer the CBM frozen on the order row (honours a user-supplied value);
        // otherwise recompute from the resolved dimensions.
        const cartonCbm = num(r.carton_cbm) || (cartonVol > 0 ? cartonVol / 1_000_000 : 0);
        const noOfCartons = unitsPerCarton > 0 ? Math.ceil(orderedUnits / unitsPerCarton) : 0;
        const totalCbm = cartonCbm * noOfCartons;
        const qcRaw = qcUnitsByOrderId[oid] ?? qcUnitsByOrderId[String(oid)];
        lines.push({
            sku: r.product_name || r.jf_code || r.asin || '',
            jfCode: r.jf_code || '',
            orderedUnits,
            lot: r.lot_number || '',
            cartonWeight,
            unitsPerCarton,
            cartonH, cartonL, cartonW,
            cartonCbm,
            noOfCartons,
            totalCbm,
            supplier: r.supplier || '',
            supplierCountry: sup.country || '',
            port: r.port || '',
            terms: sup.default_incoterms || '',
            poNumber: r.po_number || '',
            qcUnits: (qcRaw == null || qcRaw === '') ? null : Number(qcRaw),
        });
    }
    return { lines };
}

app.post('/api/v1/quality-assurance/generate', async (req, res) => {
    try {
        await qualityAssuranceSchemaReady;
        await draftRegistryReady;
        await shipmentsSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        // orderIds: which orders become rows (and the rows shown are limited to
        // these). qcUnits: optional { orderId: number } map printed in the QC
        // Units column per row. comments: free text shown at the foot of the PDF.
        // draftContainerName: optional tag recorded on the doc so the list can
        // be scoped to a draft container (the rows still come from orderIds).
        const { orderIds, qcUnits, comments, draftContainerName } = req.body || {};
        const ids = Array.isArray(orderIds)
            ? [...new Set(orderIds.map(Number).filter(n => Number.isInteger(n) && n > 0))]
            : [];
        if (!ids.length) {
            return res.status(400).json({ error: 'orderIds must be a non-empty array of order ids.' });
        }
        const qcMap = (qcUnits && typeof qcUnits === 'object' && !Array.isArray(qcUnits)) ? qcUnits : {};
        const draftName = typeof draftContainerName === 'string' && draftContainerName.trim()
            ? draftContainerName.trim().slice(0, 100)
            : null;

        const result = await withConnection(async (conn) => {
            const { lines } = await loadQualityAssuranceForPdf(conn, ids, qcMap);
            if (!lines.length) return { noOrders: true };

            await conn.beginTransaction();
            try {
                // Versions sequenced per sorted-id set: regenerating the same
                // selection bumps the version instead of starting a new series.
                const orderIdsKey = [...ids].sort((a, b) => a - b).join(',');
                const [versionRows] = await conn.query(
                    `SELECT COALESCE(MAX(version), 0) AS max_version
                       FROM quality_assurance_documents
                      WHERE order_ids_key = ? AND deleted_at IS NULL`,
                    [orderIdsKey]
                );
                const version = Number(versionRows[0].max_version) + 1;

                // Two-step: claim an id, derive the human ref (QA_00001) from it,
                // build + upload the PDF, then backfill the s3 columns. Mirrors
                // the PO number derivation.
                const token = uuidv4();
                const [ins] = await conn.query(
                    `INSERT INTO quality_assurance_documents
                        (ref, version, draft_container_name, order_ids, order_ids_key, qc_units, s3_key, comments, generated_by_email)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [null, version, draftName, JSON.stringify(ids), orderIdsKey, JSON.stringify(qcMap),
                     `__tmp_${token}`, comments || null, req.userEmail || null]
                );
                const newId = ins.insertId;
                const ref = `QA_${String(newId).padStart(5, '0')}`;

                const meta = { name: ref, date: new Date(), comments: comments || '' };
                const pdfBuffer = await buildQualityAssurancePdf(meta, lines);
                const csvBuffer = buildQualityAssuranceCsv(meta, lines);

                const s3Key = `quality-assurance/${token}/${ref}-v${version}.pdf`;
                const publicUrl = publicS3Url(s3Key);
                // CSV sibling, same key prefix so it lives next to the PDF in S3.
                const csvS3Key = `quality-assurance/${token}/${ref}-v${version}.csv`;
                const csvPublicUrl = publicS3Url(csvS3Key);
                await s3.send(new PutObjectCommand({
                    Bucket: PO_BUCKET,
                    Key: s3Key,
                    Body: pdfBuffer,
                    ContentType: 'application/pdf',
                    ContentDisposition: `inline; filename="${ref}-v${version}.pdf"`,
                }));
                await s3.send(new PutObjectCommand({
                    Bucket: PO_BUCKET,
                    Key: csvS3Key,
                    Body: csvBuffer,
                    ContentType: 'text/csv; charset=utf-8',
                    // attachment (not inline) so a click downloads the .csv with its
                    // filename rather than rendering raw text in a tab; PDF stays inline.
                    ContentDisposition: `attachment; filename="${ref}-v${version}.csv"`,
                }));

                await conn.query(
                    `UPDATE quality_assurance_documents
                        SET ref = ?, s3_key = ?, public_url = ?, file_size = ?,
                            csv_s3_key = ?, csv_public_url = ?, csv_file_size = ?
                      WHERE id = ?`,
                    [ref, s3Key, publicUrl, pdfBuffer.length, csvS3Key, csvPublicUrl, csvBuffer.length, newId]
                );
                // A QA sheet raised from a draft container is part of that
                // draft's story; untagged sheets belong to no draft.
                if (draftName) {
                    const reg = await draftAudit.ensureDraftRegistered(conn, draftName, req.userEmail);
                    await draftAudit.recordDraftAudit(conn, {
                        draftId: reg.id, action: 'qa_document_generated',
                        after: {
                            draftName: reg.name, documentId: newId, ref, version, orderIds: ids, rowCount: lines.length,
                            url: publicUrl, csvUrl: csvPublicUrl, comments: comments ? String(comments).slice(0, 500) : null,
                        },
                        userEmail: req.userEmail,
                    });
                    // Stamps the sheet with the draft's shipment: its open one,
                    // or, for a draft already converted, the booked one.
                    await shipmentSync.shadow(conn, {
                        site: 'POST /quality-assurance/generate', inTx: true, keyKind: 'draft', keyValue: draftName,
                    }, c => shipmentSync.syncDraft(c, draftName, { userEmail: req.userEmail }));
                }
                await conn.commit();
                return { documentId: newId, ref, version, draftContainerName: draftName, publicUrl, csvPublicUrl, fileSize: pdfBuffer.length, csvFileSize: csvBuffer.length, rowCount: lines.length };
            } catch (err) {
                await conn.rollback();
                throw err;
            }
        });

        if (result.noOrders) {
            return res.status(400).json({ error: 'None of the supplied orderIds matched a live order.' });
        }
        res.status(201).json({
            documentId: result.documentId,
            ref: result.ref,
            version: result.version,
            draftContainerName: result.draftContainerName,
            rowCount: result.rowCount,
            fileSize: result.fileSize,
            url: result.publicUrl,
            csvUrl: result.csvPublicUrl,
            csvFileSize: result.csvFileSize,
        });
    } catch (error) {
        log.error('[POST /quality-assurance/generate]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get('/api/v1/quality-assurance/documents', async (req, res) => {
    try {
        await qualityAssuranceSchemaReady;
        // Optional ?draftContainerName=… scopes the list to QA docs generated
        // for that draft container (the name stored on the doc at generate
        // time). Without it the endpoint returns every QA doc, which makes the
        // same docs appear on every draft container's view.
        const draftContainerName = typeof req.query.draftContainerName === 'string'
            ? req.query.draftContainerName.trim()
            : '';

        const { docs, sendsByDoc } = await withConnection(async (conn) => {
            const filterSql = draftContainerName ? ' AND draft_container_name = ?' : '';
            const filterParams = draftContainerName ? [draftContainerName] : [];
            const [r] = await conn.query(
                `SELECT id, ref, version, draft_container_name, order_ids, qc_units, s3_key,
                        public_url, file_size, csv_s3_key, csv_public_url, csv_file_size,
                        comments, generated_by_email, generated_at
                   FROM quality_assurance_documents
                  WHERE deleted_at IS NULL${filterSql}
                  ORDER BY generated_at DESC, id DESC`,
                filterParams
            );

            const ids = r.map(d => d.id);
            const sendRows = ids.length
                ? (await conn.query(
                    `SELECT id, quality_assurance_document_id, sent_to, subject,
                            front_message_uid, front_conversation_id, sent_by_email, sent_at
                       FROM quality_assurance_document_sends
                      WHERE quality_assurance_document_id IN (${ids.map(() => '?').join(',')})
                      ORDER BY sent_at DESC`,
                    ids
                ))[0]
                : [];
            const byDoc = new Map();
            for (const s of sendRows) {
                if (!byDoc.has(s.quality_assurance_document_id)) byDoc.set(s.quality_assurance_document_id, []);
                byDoc.get(s.quality_assurance_document_id).push(s);
            }
            return { docs: r, sendsByDoc: byDoc };
        });
        const parseJson = v => (typeof v === 'string' ? JSON.parse(v) : v);
        res.json({
            data: docs.map(r => {
                const orderIds = parseJson(r.order_ids) || [];
                return {
                    id: r.id,
                    ref: r.ref,
                    version: r.version,
                    draftContainerName: r.draft_container_name || null,
                    orderIds,
                    qcUnits: parseJson(r.qc_units) || {},
                    rowCount: orderIds.length,
                    comments: r.comments || null,
                    fileSize: r.file_size,
                    url: r.public_url || publicS3Url(r.s3_key),
                    csvUrl: r.csv_public_url || (r.csv_s3_key ? publicS3Url(r.csv_s3_key) : null),
                    csvFileSize: r.csv_file_size ?? null,
                    generatedByEmail: r.generated_by_email || null,
                    generatedAt: r.generated_at?.toISOString?.() ?? r.generated_at,
                    sends: (sendsByDoc.get(r.id) || []).map(s => ({
                        id: s.id,
                        sentTo: typeof s.sent_to === 'string' ? JSON.parse(s.sent_to) : s.sent_to,
                        subject: s.subject || null,
                        frontMessageUid: s.front_message_uid || null,
                        frontConversationId: s.front_conversation_id || null,
                        sentByEmail: s.sent_by_email || null,
                        sentAt: s.sent_at?.toISOString?.() ?? s.sent_at,
                    })),
                };
            }),
        });
    } catch (error) {
        log.error('[GET /quality-assurance/documents]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Sends a QA sheet to a recipient via Front with the PDF attached. Same
// delivery primitive as the draft/PO email endpoints; subject/body come from
// the `quality_assurance` email template and can be overridden per-send.
app.post('/api/v1/quality-assurance-documents/:id/email', async (req, res) => {
    try {
        await qualityAssuranceSchemaReady;
        await draftRegistryReady;
        await emailTemplatesSchemaReady;
        await emailReceiptsSchemaReady;

        const { id } = req.params;
        const { to, subject, body } = req.body || {};
        const toAddresses = (Array.isArray(to) ? to : [to])
            .filter(x => typeof x === 'string')
            .map(x => x.trim())
            .filter(Boolean);
        if (toAddresses.length === 0 || toAddresses.some(a => !EMAIL_RE.test(a))) {
            return res.status(400).json({ error: 'A valid `to` email address (or array of addresses) is required.' });
        }
        if (!process.env.FRONT_API_TOKEN || !process.env.FRONT_CHANNEL_ID) {
            return res.status(500).json({ error: 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).' });
        }
        if (!PO_BUCKET) {
            return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        }

        const result = await withConnection(async (conn) => {
            const [docRows] = await conn.query(
                `SELECT id, ref, version, s3_key, csv_s3_key, order_ids, qc_units, draft_container_name
                   FROM quality_assurance_documents
                  WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!docRows.length) return { notFound: true };
            const doc = docRows[0];
            const refLabel = doc.ref || `QA-${doc.id}`;

            const obj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: doc.s3_key }));
            const pdfBytes = Buffer.from(await obj.Body.transformToByteArray());
            const safeRef = String(refLabel).replace(/[^A-Za-z0-9._-]/g, '_');
            const filename = `${safeRef}-v${doc.version}.pdf`;

            // Attach the CSV stored next to the PDF. For documents generated before
            // CSVs were stored (no csv_s3_key), rebuild it from the doc's order ids
            // + QC units via the same loader the PDF used. Best-effort: a CSV
            // failure must not block the PDF send.
            let csvAttachment = null;
            try {
                let csvBytes = null;
                if (doc.csv_s3_key) {
                    const csvObj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: doc.csv_s3_key }));
                    csvBytes = Buffer.from(await csvObj.Body.transformToByteArray());
                } else {
                    const orderIds = ((typeof doc.order_ids === 'string' ? JSON.parse(doc.order_ids) : doc.order_ids) || [])
                        .map(Number).filter(n => Number.isInteger(n) && n > 0);
                    const qcUnits = (typeof doc.qc_units === 'string' ? JSON.parse(doc.qc_units) : doc.qc_units) || {};
                    const { lines } = await loadQualityAssuranceForPdf(conn, orderIds, qcUnits);
                    if (lines.length) csvBytes = buildQualityAssuranceCsv({ name: refLabel }, lines);
                }
                if (csvBytes && csvBytes.length) {
                    const csvName = doc.csv_s3_key
                        ? (doc.csv_s3_key.split('/').pop() || `${safeRef}-v${doc.version}.csv`)
                        : `${safeRef}-v${doc.version}.csv`;
                    csvAttachment = { bytes: csvBytes, filename: csvName };
                }
            } catch (err) {
                log.error('[quality-assurance-documents/email] CSV attach failed', { documentId: doc.id, err });
            }

            const tpl = await getEmailTemplate(conn, 'quality_assurance');
            const vars = { ref: refLabel };
            const finalSubject = (typeof subject === 'string' && subject.trim())
                ? subject.trim()
                : (renderTemplate(tpl.subject, vars) || `Quality Assurance – ${refLabel}`);
            const htmlBody = (typeof body === 'string' && body.trim())
                ? body
                : renderTemplate(tpl.bodyHtml, vars);

            const receiptToken = await createEmailReceipt(conn, {
                emailType: 'quality_assurance', sentTo: toAddresses, subject: finalSubject,
            });
            const bodyWithReceipt = appendReceiptLink(htmlBody, receiptToken, apiBaseUrlFromReq(req));

            const form = new FormData();
            for (const addr of toAddresses) form.append('to[]', addr);
            form.append('subject', finalSubject);
            form.append('body', bodyWithReceipt);
            form.append('body_format', 'html');
            form.append('options[archive]', 'false');
            form.append('attachments[]', new Blob([pdfBytes], { type: 'application/pdf' }), filename);
            if (csvAttachment) {
                form.append('attachments[]', new Blob([csvAttachment.bytes], { type: 'text/csv' }), csvAttachment.filename);
            }

            const frontUrl = `https://api2.frontapp.com/channels/${encodeURIComponent(process.env.FRONT_CHANNEL_ID)}/messages`;
            const resp = await fetch(frontUrl, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`,
                    Accept: 'application/json',
                },
                body: form,
            });
            const respText = await resp.text();
            if (!resp.ok) {
                log.error('[quality-assurance-documents/email] Front API error', {
                    status: resp.status, body: respText.slice(0, 500),
                });
                return { frontError: { status: resp.status, body: respText.slice(0, 500) } };
            }
            let parsed = null;
            try { parsed = JSON.parse(respText); } catch { /* 202 with empty body is fine */ }

            const conversationUrl = parsed?._links?.related?.conversation || '';
            const frontConversationId = conversationUrl ? conversationUrl.split('/').pop() : null;
            const frontMessageUid = parsed?.message_uid || parsed?.id || null;

            const [sendInsert] = await conn.query(
                `INSERT INTO quality_assurance_document_sends
                    (quality_assurance_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [doc.id, JSON.stringify(toAddresses), finalSubject, frontMessageUid, frontConversationId, req.userEmail || null]
            );
            const [sentRow] = await conn.query(
                `SELECT sent_at FROM quality_assurance_document_sends WHERE id = ?`,
                [sendInsert.insertId]
            );
            const sentAt = sentRow[0]?.sent_at?.toISOString?.() ?? sentRow[0]?.sent_at ?? null;
            await linkReceiptToSend(conn, receiptToken, 'quality_assurance_document_sends', sendInsert.insertId);

            if (doc.draft_container_name) {
                const reg = await draftAudit.ensureDraftRegistered(conn, doc.draft_container_name, req.userEmail);
                await draftAudit.recordDraftAudit(conn, {
                    draftId: reg.id, action: 'qa_document_sent',
                    after: {
                        draftName: reg.name, documentId: doc.id, ref: refLabel, version: doc.version,
                        sendId: sendInsert.insertId, sentTo: toAddresses, subject: finalSubject,
                        frontMessageUid, frontConversationId, receiptToken, filename,
                    },
                    userEmail: req.userEmail,
                });
            }

            return {
                ok: true,
                sendId: sendInsert.insertId,
                documentId: doc.id,
                ref: doc.ref,
                version: doc.version,
                sentTo: toAddresses,
                subject: finalSubject,
                sentAt,
                frontMessageUid,
                frontConversationId,
                filename,
                csvFilename: csvAttachment ? csvAttachment.filename : null,
            };
        });

        if (result.notFound) return res.status(404).json({ error: `Quality assurance document ${id} not found.` });
        if (result.frontError) {
            return res.status(502).json({
                error: 'Front rejected the message.',
                frontStatus: result.frontError.status,
                frontBody: result.frontError.body,
            });
        }
        res.status(200).json(result);
    } catch (error) {
        log.error('[POST /quality-assurance-documents/:id/email]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── 8. GET /api/v1/stock-snapshots/sum ───────────────────────────────────
app.get('/api/v1/stock-snapshots/sum', async (req, res) => {
    try {
        await stockSnapshotsSchemaReady;
        const cleanAsin = parseAsin(req.query.asin);
        const jfCodeParam = (req.query.jfCode || '').trim();
        if (!cleanAsin && !jfCodeParam) {
            return res.status(400).json({ error: 'Provide either a valid 10-character asin or a jfCode query parameter.' });
        }
        // company is optional: when supplied it narrows the Amazon snapshots to
        // one seller account; when omitted we sum across every company for the
        // resolved ASIN(s).
        const company = (req.query.company || '').trim();

        // A product is identified by an asin OR a jf_code; the two are mapped in
        // landed_costs. We resolve BOTH sets here because the sources are keyed
        // differently: stock_snapshots / orders / product_carton_sizes by
        // jf_code, amazon_stock_country_snapshots by asin. One jf_code can map
        // to several ASINs (e.g. HW0152 → B01MXXEO0D + B00CCGEKCY) that all
        // share a single stock_snapshots row, so looking up by jf_code is what
        // reliably finds it; looking up by asin alone misses it half the time.
        let jfCodes, asins;
        if (cleanAsin) {
            const [rows] = await pool.query(
                `SELECT DISTINCT jf_code FROM landed_costs WHERE asin = ? AND jf_code IS NOT NULL AND jf_code <> ''`,
                [cleanAsin]
            );
            jfCodes = rows.map(r => r.jf_code);
            asins = [cleanAsin];
        } else {
            jfCodes = [jfCodeParam];
            const [rows] = await pool.query(
                `SELECT DISTINCT asin FROM landed_costs WHERE jf_code = ? AND asin IS NOT NULL AND asin <> ''`,
                [jfCodeParam]
            );
            asins = rows.map(r => r.asin);
        }

        const stockClause = jfCodes.length
            ? `jf_code IN (${jfCodes.map(() => '?').join(',')})`
            : `asin = ?`;
        const stockParams = jfCodes.length ? jfCodes : [cleanAsin];

        // Match orders / product_carton_sizes by the SAME identifier the caller
        // searched on: an asin search keeps its exact previous behaviour
        // (orders.asin = ?), a jfCode search keys off the jf_code(s).
        let idParams, idWhere;
        if (cleanAsin) {
            idParams = [cleanAsin];
            idWhere = (prefix) => `${prefix ? `${prefix}.` : ''}asin = ?`;
        } else {
            idParams = jfCodes;
            idWhere = (prefix) => `${prefix ? `${prefix}.` : ''}jf_code IN (${jfCodes.map(() => '?').join(',')})`;
        }

        // Amazon snapshots are keyed by asin only. With no mapped ASIN there is
        // nothing to look up, so amzWhere collapses to a never-match clause.
        const hasAmazon = asins.length > 0;
        const amzParams = hasAmazon ? [...asins, ...(company ? [company] : [])] : [];
        const amzWhere = (prefix) => {
            if (!hasAmazon) return '1=0';
            const p = prefix ? `${prefix}.` : '';
            let w = `${p}asin IN (${asins.map(() => '?').join(',')})`;
            if (company) w += ` AND ${p}company = ?`;
            return w;
        };

        const [
            [msTodayRows], [amzTodayRows], [msFallbackRows], [amzFallbackRows],
        ] = await withTimeout(
            Promise.all([
                pool.query(`SELECT MAX(date_ran) as latest FROM stock_snapshots WHERE ${stockClause} AND date_ran = CURDATE()`, stockParams),
                pool.query(`SELECT MAX(date_ran) as latest FROM amazon_stock_country_snapshots WHERE ${amzWhere('')} AND date_ran = CURDATE()`, amzParams),
                pool.query(`SELECT MAX(date_ran) as latest FROM stock_snapshots WHERE ${stockClause}`, stockParams),
                pool.query(`SELECT MAX(date_ran) as latest FROM amazon_stock_country_snapshots WHERE ${amzWhere('')}`, amzParams),
            ]),
            QUERY_TIMEOUT_MS, 'Date lookup'
        );

        const msDate = msTodayRows[0]?.latest ?? msFallbackRows[0]?.latest ?? null;
        const amzDate = amzTodayRows[0]?.latest ?? amzFallbackRows[0]?.latest ?? null;
        const days = parseInt(req.query.days, 10) || 30;

        // How fresh is the Mintsoft number we're about to show? = the newest
        // last-refresh time among the snapshot rows that feed mintsoft_stock_level.
        // A received receipt is only netted out of the order figures once it's
        // reflected here (received_at <= this). NULL (no snapshot yet) → nothing
        // received is netted, so we never undercount.
        let snapshotFreshness = null;
        if (msDate) {
            const [freshRows] = await pool.query(
                `SELECT MAX(updated_at) AS fresh FROM stock_snapshots WHERE date_ran = ? AND ${stockClause}`,
                [msDate, ...stockParams]
            );
            snapshotFreshness = freshRows[0]?.fresh ?? null;
        }

        // Per-order rollup of receipt units the cached Mintsoft snapshot ALREADY
        // reflects — netted out of the ARRIVED_AT_WAREHOUSE total below so it
        // doesn't double-count what's now in mintsoft_stock_level. = not_received
        // shortfalls (always reconciled) + received units whose received_at is
        // <= the snapshot's last refresh. The leading `?` binds snapshotFreshness,
        // so it must precede idParams.
        const receiptsJoin = `
                     LEFT JOIN (
                         SELECT order_id,
                             COALESCE(SUM(CASE
                                 WHEN type <> 'received' THEN quantity
                                 WHEN received_at <= ? THEN quantity
                                 ELSE 0 END), 0) AS reflected_settled
                         FROM order_receipts GROUP BY order_id
                     ) rcpt ON rcpt.order_id = orders.id`;
        const orderParams = [snapshotFreshness, ...idParams];

        const [msRows, amzRows, orderRows, onSeaRows, onAirRows, ordersBreakdownRows, msHistoryRows, amzHistoryRows, tagRows, msSkuRows] = await withTimeout(
            Promise.all([
                msDate
                    ? pool.query(
                        `SELECT
                            CAST(COALESCE(SUM(stock_level), 0) AS UNSIGNED)  as total_stock_level,
                            CAST(COALESCE(SUM(available), 0) AS UNSIGNED)    as total_available,
                            CAST(COALESCE(SUM(allocated), 0) AS UNSIGNED)    as total_allocated,
                            CAST(COALESCE(SUM(quarantine), 0) AS UNSIGNED)   as total_quarantine
                         FROM stock_snapshots
                         WHERE date_ran = ? AND ${stockClause}`,
                        [msDate, ...stockParams]
                    ).then(([rows]) => rows)
                    : Promise.resolve([{
                        total_stock_level: 0, total_available: 0,
                        total_allocated: 0, total_quarantine: 0,
                    }]),

                amzDate
                    ? pool.query(
                        `SELECT a.country,
                            CAST(COALESCE(SUM(a.fulfillable), 0) AS UNSIGNED)       as amazon_fulfillable,
                            CAST(COALESCE(SUM(a.inbound_working), 0) AS UNSIGNED)   as amazon_inbound_working,
                            CAST(COALESCE(SUM(a.inbound_shipped), 0) AS UNSIGNED)   as amazon_inbound_shipped,
                            CAST(COALESCE(SUM(a.inbound_receiving), 0) AS UNSIGNED) as amazon_inbound_receiving,
                            CAST(COALESCE(SUM(a.reserved), 0) AS UNSIGNED)          as amazon_reserved
                         FROM amazon_stock_country_snapshots a
                         JOIN (
                             SELECT country, MAX(date_ran) AS latest
                             FROM amazon_stock_country_snapshots
                             WHERE ${amzWhere('')}
                             GROUP BY country
                         ) ld ON ld.country = a.country AND a.date_ran = ld.latest
                         WHERE ${amzWhere('a')}
                         GROUP BY a.country`,
                        [...amzParams, ...amzParams]
                    ).then(([rows]) => rows)
                    : Promise.resolve([]),

                pool.query(
                    // Only ARRIVED_AT_WAREHOUSE is netted — that's the bucket that
                    // double-counts against mintsoft_stock_level (a partial receive
                    // moves units into Mintsoft but leaves the order here at full
                    // qty). Every other status keeps its raw ordered total.
                    `SELECT orders.status,
                            CAST(COALESCE(SUM(
                                CASE WHEN orders.status = 'ARRIVED_AT_WAREHOUSE'
                                     THEN GREATEST(orders.quantity - COALESCE(rcpt.reflected_settled, 0), 0)
                                     ELSE orders.quantity END
                            ), 0) AS UNSIGNED) as total_quantity
                     FROM orders${receiptsJoin}
                     WHERE ${idWhere('orders')} AND orders.deleted_at IS NULL GROUP BY orders.status`,
                    orderParams
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT orders.id, orders.asin, orders.product_name, orders.quantity, orders.eta, orders.container_number, orders.port, orders.supplier, orders.po_number,
                            orders.dates, orders.delivery_date, orders.order_cbm, orders.lot_number, orders.mfg_date, orders.exp_date, orders.delivery_time,
                            orders.container_status, orders.booking_status, orders.arrived_date, orders.external_container_number,
                            orders.shipped_date, orders.ordered_date, orders.estimated_ready_date, orders.artwork_confirmed_date,
                            orders.actual_ready_date, orders.estimated_departure_date,
                            po.created_at AS po_created_at
                     FROM orders LEFT JOIN purchase_orders po ON po.id = orders.purchase_order_id
                     WHERE ${idWhere('orders')} AND orders.status = 'ON_SEA' AND orders.deleted_at IS NULL ORDER BY orders.eta ASC`,
                    idParams
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT orders.id, orders.product_name, orders.quantity, orders.eta, orders.container_number, orders.awb_number, orders.supplier, orders.po_number, orders.dates, orders.order_cbm,
                            orders.delivery_date,
                            orders.shipped_date, orders.ordered_date, orders.estimated_ready_date, orders.artwork_confirmed_date,
                            orders.actual_ready_date, orders.estimated_departure_date,
                            po.created_at AS po_created_at
                     FROM orders LEFT JOIN purchase_orders po ON po.id = orders.purchase_order_id
                     WHERE ${idWhere('orders')} AND orders.status = 'ON_AIR' AND orders.deleted_at IS NULL ORDER BY orders.eta ASC`,
                    idParams
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT orders.id, orders.product_name, orders.quantity, orders.status, orders.po_number, orders.port, orders.supplier, orders.dates, orders.container_number,
                            orders.delivery_date,
                            orders.scheduled_date, orders.ordered_date, orders.estimated_ready_date, orders.actual_ready_date, orders.artwork_confirmed_date,
                            orders.shipped_date, orders.estimated_departure_date,
                            po.created_at AS po_created_at
                     FROM orders LEFT JOIN purchase_orders po ON po.id = orders.purchase_order_id
                     WHERE ${idWhere('orders')} AND orders.status NOT IN ('ON_SEA', 'ON_AIR', 'RECEIVED') AND orders.deleted_at IS NULL ORDER BY orders.created_at DESC`,
                    idParams
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT date_ran,
                        CAST(COALESCE(SUM(stock_level), 0) AS UNSIGNED)  as stock_level,
                        CAST(COALESCE(SUM(available), 0) AS UNSIGNED)    as available,
                        CAST(COALESCE(SUM(allocated), 0) AS UNSIGNED)    as allocated,
                        CAST(COALESCE(SUM(quarantine), 0) AS UNSIGNED)   as quarantine
                     FROM stock_snapshots
                     WHERE ${stockClause} AND date_ran >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                     GROUP BY date_ran ORDER BY date_ran`,
                    [...stockParams, days]
                ).then(([rows]) => rows),

                // One row per (day, country). buildAmazonHistory derives BOTH
                // the per-day total series (history.amazon, shape unchanged)
                // and the per-country series (history.amazon_by_country) from
                // these rows, so one query serves both.
                pool.query(
                    `SELECT date_ran, country,
                        CAST(COALESCE(SUM(fulfillable), 0) AS UNSIGNED)       as fulfillable,
                        CAST(COALESCE(SUM(inbound_working), 0) AS UNSIGNED)   as inbound_working,
                        CAST(COALESCE(SUM(inbound_shipped), 0) AS UNSIGNED)   as inbound_shipped,
                        CAST(COALESCE(SUM(inbound_receiving), 0) AS UNSIGNED) as inbound_receiving,
                        CAST(COALESCE(SUM(reserved), 0) AS UNSIGNED)          as reserved
                     FROM amazon_stock_country_snapshots
                     WHERE ${amzWhere('')} AND date_ran >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
                     GROUP BY date_ran, country ORDER BY date_ran, country`,
                    [...amzParams, days]
                ).then(([rows]) => rows),

                pool.query(
                    `SELECT carton_qty AS units_per_ctn,
                            (carton_height * carton_width * carton_depth) / 1000000 AS carton_cbm
                     FROM product_carton_sizes
                     WHERE ${idWhere('')}
                     LIMIT 1`,
                    idParams
                ).then(([rows]) => rows),

                // Same rows, same date as the aggregate above — just kept at
                // (sku, warehouse) grain instead of collapsed, so the response
                // can show which child SKU (_TR, _QC, …) holds the units.
                msDate
                    ? pool.query(
                        `SELECT jf_code, sku, product_id, warehouse_id,
                            CAST(COALESCE(SUM(stock_level), 0) AS UNSIGNED)  as stock_level,
                            CAST(COALESCE(SUM(available), 0) AS UNSIGNED)    as available,
                            CAST(COALESCE(SUM(allocated), 0) AS UNSIGNED)    as allocated,
                            CAST(COALESCE(SUM(quarantine), 0) AS UNSIGNED)   as quarantine
                         FROM stock_snapshots
                         WHERE date_ran = ? AND ${stockClause}
                         GROUP BY jf_code, sku, product_id, warehouse_id
                         ORDER BY sku, warehouse_id`,
                        [msDate, ...stockParams]
                    ).then(([rows]) => rows)
                    : Promise.resolve([]),
            ]),
            QUERY_TIMEOUT_MS, 'Data fetch'
        );

        const { byCountry: amazon_stock_by_country, totals } = aggregateAmazonStock(amzRows);
        const amazonHistory = buildAmazonHistory(amzHistoryRows);
        const msStats = msRows[0] || {};
        const tagData = tagRows[0] || {};

        res.json({
            data: {
                mintsoft_date: msDate,
                amazon_date: amzDate,
                carton_cbm: tagData.carton_cbm != null ? Number(tagData.carton_cbm) : null,
                units_per_ctn: tagData.units_per_ctn != null ? Number(tagData.units_per_ctn) : null,
                mintsoft_stock_level: Number(msStats.total_stock_level || 0),
                mintsoft_available: Number(msStats.total_available || 0),
                mintsoft_allocated: Number(msStats.total_allocated || 0),
                mintsoft_quarantine: Number(msStats.total_quarantine || 0),
                // Per-child-SKU split of the four mintsoft_* figures above; the
                // entries sum to them exactly.
                mintsoft_by_sku: buildMintsoftSkuBreakdown(msSkuRows),
                ...totals,
                amazon_stock_by_country,
                orders: buildOrderStatusMap(orderRows),
                goods_on_sea: onSeaRows.map(mapGoodsOnSeaRow),
                goods_on_air: onAirRows.map(mapGoodsOnAirRow),
                orders_breakdown: ordersBreakdownRows.map(mapOrderBreakdownRow),
                history: {
                    mintsoft: msHistoryRows.map(r => ({
                        date: r.date_ran,
                        stock_level: Number(r.stock_level || 0),
                        available: Number(r.available || 0),
                        allocated: Number(r.allocated || 0),
                        quarantine: Number(r.quarantine || 0),
                    })),
                    amazon: amazonHistory.total,
                    // { UK: [ {date, fulfillable, …}, … ], US: [ … ] } — same
                    // row shape as `amazon`, one series per country. Countries
                    // start on different days (US collection began after UK),
                    // so a day missing from one series means "no snapshot",
                    // not zero.
                    amazon_by_country: amazonHistory.byCountry,
                },
            },
        });
    } catch (error) {
        log.error('[GET /stock-snapshots/sum]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 9. GET /api/v1/stock-snapshots/sum/all-asins ─────────────────────────
app.get('/api/v1/stock-snapshots/sum/all-asins', async (req, res) => {
    try {
        await stockSnapshotsSchemaReady;
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        const [
            [asinsFromOrders], [asinsFromMintsoft], [asinsFromAmazon],
        ] = await withTimeout(
            Promise.all([
                pool.query(`SELECT DISTINCT asin FROM orders WHERE asin IS NOT NULL AND asin != '' AND deleted_at IS NULL`),
                pool.query(`SELECT DISTINCT asin FROM stock_snapshots WHERE asin IS NOT NULL AND asin != ''`),
                pool.query(`SELECT DISTINCT asin FROM amazon_stock_country_snapshots WHERE asin IS NOT NULL AND asin != '' AND company = ?`, [company]),
            ]),
            QUERY_TIMEOUT_MS, 'ASIN list fetch'
        );

        const allAsins = new Set([
            ...asinsFromOrders.map(r => r.asin),
            ...asinsFromMintsoft.map(r => r.asin),
            ...asinsFromAmazon.map(r => r.asin),
        ]);

        const [
            [msDates], [amzDates], [allStatusOrders], [onSeaOrders], [onAirOrders], [allOrdersRows], [msLatestRows], [amzLatestRows], [tagRows], [receiptRollup], [msSkuRows],
        ] = await withTimeout(
            Promise.all([
                pool.query(`
                    SELECT asin, MAX(date_ran) as latest
                    FROM stock_snapshots WHERE asin IS NOT NULL AND asin != ''
                    GROUP BY asin ORDER BY asin
                `),
                pool.query(`
                    SELECT asin, MAX(date_ran) as latest
                    FROM amazon_stock_country_snapshots WHERE asin IS NOT NULL AND asin != '' AND company = ?
                    GROUP BY asin ORDER BY asin
                `, [company]),
                // Raw per-order rows (not pre-aggregated) so the status map can
                // net each order's reflected receipts using its ASIN's snapshot
                // freshness — see buildNettedStatusMap.
                pool.query(`
                    SELECT id, asin, status, quantity
                    FROM orders WHERE asin IS NOT NULL AND deleted_at IS NULL
                `),
                pool.query(`
                    SELECT orders.asin, orders.id, orders.product_name, orders.quantity, orders.eta, orders.container_number, orders.port, orders.supplier, orders.po_number,
                           orders.dates, orders.delivery_date, orders.order_cbm, orders.lot_number, orders.mfg_date, orders.exp_date, orders.delivery_time,
                           orders.container_status, orders.booking_status, orders.arrived_date, orders.external_container_number,
                           orders.shipped_date, orders.ordered_date, orders.estimated_ready_date, orders.artwork_confirmed_date,
                           orders.actual_ready_date, orders.estimated_departure_date,
                           po.created_at AS po_created_at
                    FROM orders LEFT JOIN purchase_orders po ON po.id = orders.purchase_order_id
                    WHERE orders.status = 'ON_SEA' AND orders.asin IS NOT NULL AND orders.deleted_at IS NULL
                    ORDER BY orders.asin, orders.eta ASC
                `),
                pool.query(`
                    SELECT orders.asin, orders.id, orders.product_name, orders.quantity, orders.eta, orders.container_number, orders.awb_number, orders.supplier, orders.po_number, orders.dates, orders.order_cbm,
                           orders.shipped_date, orders.ordered_date, orders.estimated_ready_date, orders.artwork_confirmed_date,
                           orders.actual_ready_date, orders.estimated_departure_date,
                           po.created_at AS po_created_at
                    FROM orders LEFT JOIN purchase_orders po ON po.id = orders.purchase_order_id
                    WHERE orders.status = 'ON_AIR' AND orders.asin IS NOT NULL AND orders.deleted_at IS NULL
                    ORDER BY orders.asin, orders.eta ASC
                `),
                pool.query(`
                    SELECT orders.asin, orders.id, orders.product_name, orders.quantity, orders.status, orders.po_number, orders.port, orders.supplier, orders.dates, orders.container_number,
                           orders.scheduled_date, orders.ordered_date, orders.estimated_ready_date, orders.actual_ready_date, orders.artwork_confirmed_date,
                           orders.shipped_date, orders.estimated_departure_date,
                           po.created_at AS po_created_at
                    FROM orders LEFT JOIN purchase_orders po ON po.id = orders.purchase_order_id
                    WHERE orders.status NOT IN ('ON_SEA', 'ON_AIR', 'RECEIVED') AND orders.asin IS NOT NULL AND orders.deleted_at IS NULL
                    ORDER BY orders.asin, orders.created_at DESC
                `),
                pool.query(`
                    WITH LatestDates AS (
                        SELECT asin, MAX(date_ran) as latest FROM stock_snapshots
                        WHERE asin IS NOT NULL AND asin != '' GROUP BY asin
                    )
                    SELECT s.asin, s.date_ran,
                           CAST(COALESCE(SUM(s.stock_level), 0) AS UNSIGNED) as total_stock_level,
                           CAST(COALESCE(SUM(s.available), 0) AS UNSIGNED) as total_available,
                           CAST(COALESCE(SUM(s.allocated), 0) AS UNSIGNED) as total_allocated,
                           CAST(COALESCE(SUM(s.quarantine), 0) AS UNSIGNED) as total_quarantine,
                           MAX(s.updated_at) AS fresh
                    FROM stock_snapshots s
                    JOIN LatestDates ld ON s.asin = ld.asin AND s.date_ran = ld.latest
                    GROUP BY s.asin, s.date_ran
                `),
                pool.query(`
                    WITH LatestDates AS (
                        SELECT asin, country, MAX(date_ran) as latest FROM amazon_stock_country_snapshots
                        WHERE asin IS NOT NULL AND asin != '' AND company = ?
                        GROUP BY asin, country
                    )
                    SELECT a.asin, a.country, a.date_ran,
                           CAST(COALESCE(SUM(a.fulfillable), 0) AS UNSIGNED) as amazon_fulfillable,
                           CAST(COALESCE(SUM(a.inbound_working), 0) AS UNSIGNED) as amazon_inbound_working,
                           CAST(COALESCE(SUM(a.inbound_shipped), 0) AS UNSIGNED) as amazon_inbound_shipped,
                           CAST(COALESCE(SUM(a.inbound_receiving), 0) AS UNSIGNED) as amazon_inbound_receiving,
                           CAST(COALESCE(SUM(a.reserved), 0) AS UNSIGNED) as amazon_reserved
                    FROM amazon_stock_country_snapshots a
                    JOIN LatestDates ld ON a.asin = ld.asin AND a.country = ld.country AND a.date_ran = ld.latest
                    WHERE a.company = ?
                    GROUP BY a.asin, a.country, a.date_ran
                `, [company, company]),
                pool.query(`
                    SELECT asin,
                           carton_qty AS units_per_ctn,
                           (carton_height * carton_width * carton_depth) / 1000000 AS carton_cbm
                    FROM product_carton_sizes
                    WHERE asin IS NOT NULL AND asin != ''
                `),
                // Per-order receipt rollup for the reflected-ledger netting.
                pool.query(`
                    SELECT order_id,
                           COALESCE(SUM(CASE WHEN type = 'received' THEN quantity END), 0) AS received_qty,
                           COALESCE(SUM(CASE WHEN type <> 'received' THEN quantity END), 0) AS not_received_qty,
                           MAX(CASE WHEN type = 'received' THEN received_at END) AS last_received_at
                    FROM order_receipts GROUP BY order_id
                `),
                // The same latest-date rows as msLatestRows, left at (sku,
                // warehouse) grain to feed each ASIN's mintsoft_by_sku split.
                pool.query(`
                    WITH LatestDates AS (
                        SELECT asin, MAX(date_ran) as latest FROM stock_snapshots
                        WHERE asin IS NOT NULL AND asin != '' GROUP BY asin
                    )
                    SELECT s.asin, s.jf_code, s.sku, s.product_id, s.warehouse_id,
                           CAST(COALESCE(SUM(s.stock_level), 0) AS UNSIGNED) as stock_level,
                           CAST(COALESCE(SUM(s.available), 0) AS UNSIGNED) as available,
                           CAST(COALESCE(SUM(s.allocated), 0) AS UNSIGNED) as allocated,
                           CAST(COALESCE(SUM(s.quarantine), 0) AS UNSIGNED) as quarantine
                    FROM stock_snapshots s
                    JOIN LatestDates ld ON s.asin = ld.asin AND s.date_ran = ld.latest
                    GROUP BY s.asin, s.jf_code, s.sku, s.product_id, s.warehouse_id
                    ORDER BY s.sku, s.warehouse_id
                `),
            ]),
            QUERY_TIMEOUT_MS, 'Batch data fetch'
        );

        const msDatesMap = Object.fromEntries(msDates.map(r => [r.asin, r.latest]));
        const amzDatesMap = Object.fromEntries(amzDates.map(r => [r.asin, r.latest]));
        const tagMap = Object.fromEntries(tagRows.map(r => [r.asin, r]));

        // Bucketed once up front rather than filtered per ASIN — this is the one
        // list here with several rows per ASIN (a child SKU per warehouse).
        const msSkuByAsin = new Map();
        for (const row of msSkuRows) {
            if (!msSkuByAsin.has(row.asin)) msSkuByAsin.set(row.asin, []);
            msSkuByAsin.get(row.asin).push(row);
        }

        // For the ARRIVED_AT_WAREHOUSE netting in buildNettedStatusMap: snapshot
        // freshness per ASIN + the receipt rollup keyed by order. A received
        // receipt is only netted once its ASIN's snapshot reflects it. Coarser
        // than /sum (which gates each receipt individually): here we net an
        // order's received units only if its MOST RECENT received receipt is
        // reflected — biasing to a brief double-count over an undercount.
        const freshnessMap = Object.fromEntries(msLatestRows.map(r => [r.asin, r.fresh]));
        const receiptsByOrder = new Map(receiptRollup.map(r => [r.order_id, r]));
        const reflectedSettledFor = (r) => {
            const rr = receiptsByOrder.get(r.id);
            if (!rr) return 0;
            const fresh = freshnessMap[r.asin] || null;
            const lastRecv = rr.last_received_at || null;
            const receivedReflected = (lastRecv && fresh && +new Date(lastRecv) <= +new Date(fresh))
                ? Number(rr.received_qty || 0) : 0;
            return Number(rr.not_received_qty || 0) + receivedReflected;
        };

        const results = {};

        for (const asin of allAsins) {
            if (!asin || !parseAsin(asin)) continue;

            const msStats = msLatestRows.find(r => r.asin === asin) || {};
            const amzByCountry = amzLatestRows.filter(r => r.asin === asin);
            const ordersByStatus = allStatusOrders.filter(r => r.asin === asin);
            const seaOrders = onSeaOrders.filter(r => r.asin === asin);
            const airOrders = onAirOrders.filter(r => r.asin === asin);
            const ordersBreakdown = allOrdersRows.filter(r => r.asin === asin);

            const { byCountry: amazon_stock_by_country, totals } = aggregateAmazonStock(amzByCountry);

            const tagData = tagMap[asin] || {};

            results[asin] = {
                mintsoft_date: msDatesMap[asin] || null,
                amazon_date: amzDatesMap[asin] || null,
                carton_cbm: tagData.carton_cbm != null ? Number(tagData.carton_cbm) : null,
                units_per_ctn: tagData.units_per_ctn != null ? Number(tagData.units_per_ctn) : null,
                mintsoft_stock_level: Number(msStats.total_stock_level || 0),
                mintsoft_available: Number(msStats.total_available || 0),
                mintsoft_allocated: Number(msStats.total_allocated || 0),
                mintsoft_quarantine: Number(msStats.total_quarantine || 0),
                mintsoft_by_sku: buildMintsoftSkuBreakdown(msSkuByAsin.get(asin) || []),
                ...totals,
                amazon_stock_by_country,
                orders: buildNettedStatusMap(ordersByStatus, reflectedSettledFor),
                goods_on_sea: seaOrders.map(mapGoodsOnSeaRow),
                goods_on_air: airOrders.map(mapGoodsOnAirRow),
                orders_breakdown: ordersBreakdown.map(mapOrderBreakdownRow),
            };
        }

        res.json({ data: results });
    } catch (error) {
        log.error('[GET /stock-snapshots/sum/all-asins]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 10. GET /api/v1/stock-snapshots/fnskus ──────────────────────────────
app.get('/api/v1/stock-snapshots/fnskus', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        // Authoritative pairs: amazon_active_listings has one row per listed
        // SKU with its FNSKU, no Pan-EU dedup — every marketplace SKU survives.
        // first_seen uses MIN(created_at) (timestamp) not MIN(date_ran) (date)
        // so SKUs in a same-date snapshot still order by insertion time.
        const activeListingsQuery = withTimeout(
            pool.query(
                `SELECT al.country, al.sku, al.fnsku, fs.first_seen
                 FROM amazon_active_listings al
                 JOIN (
                     SELECT sku, MIN(created_at) AS first_seen
                     FROM amazon_active_listings
                     WHERE asin = ? AND company = ?
                     GROUP BY sku
                 ) fs ON fs.sku = al.sku
                 WHERE al.asin = ? AND al.company = ?
                   AND al.date_ran = (SELECT MAX(date_ran) FROM amazon_active_listings
                                      WHERE asin = ? AND company = ?)`,
                [cleanAsin, company, cleanAsin, company, cleanAsin, company]
            ),
            QUERY_TIMEOUT_MS, 'FNSKU lookup (active_listings)'
        );

        // Orphan FNSKUs: physical inventory pools (re-stickered/return units)
        // that exist without an active listing. CSV-packed per country row.
        // Latest row is picked per country so a mid-run refresh (some countries
        // on today, others still on yesterday) doesn't drop the stragglers.
        const countrySnapshotsQuery = withTimeout(
            pool.query(
                `SELECT a.country, a.sku, a.fnsku
                 FROM amazon_stock_country_snapshots a
                 JOIN (
                     SELECT country, MAX(date_ran) AS latest
                     FROM amazon_stock_country_snapshots
                     WHERE asin = ? AND company = ?
                     GROUP BY country
                 ) ld ON ld.country = a.country AND a.date_ran = ld.latest
                 WHERE a.asin = ? AND a.company = ?`,
                [cleanAsin, company, cleanAsin, company]
            ),
            QUERY_TIMEOUT_MS, 'FNSKU lookup (country_snapshots)'
        );

        const [[alRows], [csRows]] = await Promise.all([activeListingsQuery, countrySnapshotsQuery]);

        const splitCsv = v => (v || '').split(',').map(s => s.trim()).filter(Boolean);
        const toMs = d => (d ? new Date(d).getTime() : 0);

        // Per-country fallback FNSKU set from country_snapshots + any non-empty
        // fnskus in active_listings. Used when a listing row has no fnsku of
        // its own (common: all SKUs for an ASIN share one FNSKU pool).
        const fnskusByCountry = new Map();
        const csPositional = new Map(); // `${country}|${sku}` → fnsku (same index in CSV)

        for (const row of csRows) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            if (!fnskusByCountry.has(country)) fnskusByCountry.set(country, new Set());
            const skus = splitCsv(row.sku);
            const fnskus = splitCsv(row.fnsku);
            for (const f of fnskus) fnskusByCountry.get(country).add(f);
            const pairCount = Math.min(skus.length, fnskus.length);
            for (let i = 0; i < pairCount; i++) {
                csPositional.set(`${country}|${skus[i]}`, fnskus[i]);
            }
        }
        for (const row of alRows) {
            const country = row.country?.trim().toUpperCase();
            const fnsku = (row.fnsku || '').trim();
            if (country && fnsku) {
                if (!fnskusByCountry.has(country)) fnskusByCountry.set(country, new Set());
                fnskusByCountry.get(country).add(fnsku);
            }
        }

        const byCountry = {};
        const seenPairs = new Set();
        const pairKey = (c, s, f) => `${c}|${s || ''}|${f || ''}`;

        const sortedAl = [...alRows].sort((a, b) => {
            const diff = toMs(b.first_seen) - toMs(a.first_seen);
            if (diff) return diff;
            const ca = (a.country || '').toUpperCase();
            const cb = (b.country || '').toUpperCase();
            if (ca !== cb) return ca.localeCompare(cb);
            return (a.sku || '').localeCompare(b.sku || '');
        });

        for (const row of sortedAl) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            const sku = (row.sku || '').trim() || null;
            const rowFnsku = (row.fnsku || '').trim();
            if (!byCountry[country]) byCountry[country] = [];

            // Resolve fnsku(s) for this SKU: listing's own → positional from CS
            // → country's unique FNSKU → all country FNSKUs (cartesian) → null.
            const countryFnskus = fnskusByCountry.get(country);
            const resolved = [];
            if (rowFnsku) {
                resolved.push(rowFnsku);
            } else if (csPositional.has(`${country}|${sku}`)) {
                resolved.push(csPositional.get(`${country}|${sku}`));
            } else if (countryFnskus && countryFnskus.size === 1) {
                resolved.push([...countryFnskus][0]);
            } else if (countryFnskus && countryFnskus.size > 1) {
                resolved.push(...countryFnskus);
            } else {
                resolved.push(null);
            }

            for (const fnsku of resolved) {
                const key = pairKey(country, sku, fnsku);
                if (seenPairs.has(key)) continue;
                seenPairs.add(key);
                byCountry[country].push({ sku, fnsku });
            }
        }

        // Orphan FNSKUs with no matching SKU in active_listings — surface them
        // so callers can still see the stock pool.
        for (const [country, fnskus] of fnskusByCountry) {
            for (const fnsku of fnskus) {
                const alreadyPaired = [...seenPairs].some(k => k.startsWith(`${country}|`) && k.endsWith(`|${fnsku}`));
                if (alreadyPaired) continue;
                if (!byCountry[country]) byCountry[country] = [];
                byCountry[country].push({ sku: null, fnsku });
                seenPairs.add(pairKey(country, null, fnsku));
            }
        }

        res.json({ asin: cleanAsin, fnskus_by_country: byCountry });
    } catch (error) {
        log.error('[GET /stock-snapshots/fnskus]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 11. GET /api/v1/stock-snapshots/asana-tasks ─────────────────────────
app.get('/api/v1/stock-snapshots/asana-tasks', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }

        const pat = process.env.ASANA_PAT;
        if (!pat) return res.status(500).json({ error: 'ASANA_PAT not configured.' });

        const headers = { Authorization: `Bearer ${pat}` };
        const ctx = await getAsanaPortfolioContext(pat);

        const { data: { data: tasks } } = await axios.get(
            `${ASANA_BASE}/workspaces/${ctx.workspaceGid}/tasks/search`,
            {
                headers,
                params: {
                    'projects.any': ctx.projectIds.join(','),
                    [`custom_fields.${ctx.asinFieldGid}.value`]: cleanAsin,
                    opt_fields: 'name,completed,created_at,memberships.section.name,memberships.project.gid,memberships.project.name,custom_fields.name,custom_fields.display_value',
                    limit: 100,
                },
            }
        );

        // Search filter on text custom fields is substring-based; defensively
        // require an exact display_value match on the ASIN field.
        const exactAsinMatch = t =>
            (t.custom_fields || []).some(cf => cf.name === 'ASIN' && cf.display_value === cleanAsin);

        const portfolioProjects = new Set(ctx.projectIds);
        const tasksByCountry = {};
        for (const t of (tasks || []).filter(exactAsinMatch)) {
            for (const m of (t.memberships || [])) {
                const projectGid = m.project?.gid;
                if (!portfolioProjects.has(projectGid)) continue;
                // Recent-only projects surface tasks only when created in
                // the last 5 days; skip their older tasks (other projects are
                // included regardless of age).
                if (ASANA_RECENT_ONLY_PROJECT_IDS.has(projectGid) && !isWithinRecentWindow(t.created_at)) continue;
                const section = m.section?.name || null;
                // Fall back to the section name (or "OTHER") when the section
                // doesn't match a known country — silently dropping these
                // hides tasks in regions like EU/PAN-EU.
                const bucket = resolveSectionCountry(section)
                    || (section ? section.trim().toUpperCase() : 'OTHER');

                const listName = m.project?.name || ctx.projectNameById[projectGid] || projectGid;
                if (!tasksByCountry[bucket]) tasksByCountry[bucket] = [];
                tasksByCountry[bucket].push({
                    gid: t.gid,
                    name: `${listName} — ${t.name}`,
                    completed: t.completed,
                    created_at: t.created_at || null,
                    section,
                    list: listName,
                    custom_fields: Object.fromEntries(
                        (t.custom_fields || [])
                            .filter(cf => cf.display_value)
                            .map(cf => [cf.name, cf.display_value])
                    ),
                });
            }
        }

        res.json({ asin: cleanAsin, tasks_by_country: tasksByCountry });
    } catch (error) {
        log.error('[GET /stock-snapshots/asana-tasks]', error);
        const msg = error.response?.data?.errors?.[0]?.message || error.message;
        res.status(error.response?.status || 500).json({ error: msg });
    }
});

// ── 12. GET /api/v1/stock-snapshots/oos-dates ───────────────────────────
app.get('/api/v1/stock-snapshots/oos-dates', async (req, res) => {
    try {
        const { asin } = req.query;
        if (!asin) return res.status(400).json({ error: 'asin query parameter is required.' });
        const company = (req.query.company || '').trim();
        if (!company) return res.status(400).json({ error: 'company query parameter is required.' });

        const [rows] = await pool.query(
            `SELECT country, date_ran AS date
             FROM amazon_stock_raw_snapshots
             WHERE asin = ? AND company = ? AND fulfillable = 0
             ORDER BY country, date_ran`,
            [asin, company]
        );

        // Group by territory
        const byTerritory = {};
        for (const row of rows) {
            const country = row.country;
            if (!byTerritory[country]) byTerritory[country] = [];
            byTerritory[country].push(formatDate(row.date));
        }

        res.json({ asin, oos_dates: byTerritory });
    } catch (error) {
        log.error('[GET /stock-snapshots/oos-dates]', error);
        res.status(500).json({ error: error.message });
    }
});

// ── 13. GET /api/v1/stock-snapshots/active-listings ────────────────────
app.get('/api/v1/stock-snapshots/active-listings', async (req, res) => {
    try {
        const cleanAsin = parseAsin(req.query.asin);
        if (!cleanAsin) {
            return res.status(400).json({ error: 'A valid 10-character alphanumeric ASIN is required.' });
        }
        const company = (req.query.company || '').trim();
        if (!company) {
            return res.status(400).json({ error: 'company query parameter is required.' });
        }

        const [[latestRows], [historyRows]] = await withTimeout(
            Promise.all([
                pool.query(
                    `SELECT a.country, a.sku, a.fnsku, a.product_name, a.status, a.price, a.date_ran
                     FROM amazon_active_listings a
                     JOIN (
                         SELECT country, MAX(date_ran) AS latest
                         FROM amazon_active_listings
                         WHERE asin = ? AND company = ?
                         GROUP BY country
                     ) ld ON ld.country = a.country AND a.date_ran = ld.latest
                     WHERE a.asin = ? AND a.company = ?
                     ORDER BY a.country, a.sku`,
                    [cleanAsin, company, cleanAsin, company]
                ),
                pool.query(
                    `SELECT date_ran, country, sku, fnsku, product_name, status, price
                     FROM amazon_active_listings
                     WHERE asin = ? AND company = ?
                     ORDER BY date_ran DESC, country, sku`,
                    [cleanAsin, company]
                ),
            ]),
            QUERY_TIMEOUT_MS, 'Active listings lookup'
        );

        const currentByCountry = {};
        for (const row of latestRows) {
            const country = row.country?.trim().toUpperCase();
            if (!country) continue;
            if (!currentByCountry[country]) currentByCountry[country] = [];
            currentByCountry[country].push({
                sku: row.sku,
                fnsku: row.fnsku,
                product_name: row.product_name,
                status: row.status,
                price: Number(row.price),
            });
        }

        const history = historyRows.map(row => ({
            date: formatDate(row.date_ran),
            country: row.country,
            sku: row.sku,
            fnsku: row.fnsku,
            product_name: row.product_name,
            status: row.status,
            price: Number(row.price),
        }));

        const latestDateRaw = latestRows.reduce((max, r) => {
            const t = r.date_ran ? new Date(r.date_ran).getTime() : 0;
            return t > max ? t : max;
        }, 0);

        res.json({
            asin: cleanAsin,
            company,
            latest_date: latestDateRaw ? formatDate(new Date(latestDateRaw)) : null,
            current: currentByCountry,
            history,
        });
    } catch (error) {
        log.error('[GET /stock-snapshots/active-listings]', error);
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({ error: 'An internal error occurred.' });
    }
});

// ── 14. POST /api/v1/stock-snapshots/refresh ─────────────────────────────
// On-demand Mintsoft refresh for ONE jf_code instead of waiting for the hourly
// stockSnapshot sweep (which walks ~900 codes and takes 2-11 minutes). Hits
// Mintsoft live and upserts today's stock_snapshots rows for the bare SKU AND
// its whitelisted children (_TR trade units, _QC, _IFU, …) — the same SKU set
// the hourly sweep resolves, so refreshing a code never moves its total on its
// own; it only makes the existing number fresher.
const JF_CODE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$/;
const REFRESH_TIMEOUT_MS = 25_000; // under API Gateway's 30s integration cap

app.post('/api/v1/stock-snapshots/refresh', async (req, res) => {
    const jfCode = String(req.body?.jfCode ?? req.query.jfCode ?? '').trim();
    try {
        await stockSnapshotsSchemaReady;
        if (!JF_CODE_PATTERN.test(jfCode)) {
            return res.status(400).json({ error: 'Provide a valid jfCode (letters, digits, . _ -).' });
        }

        // Populate the row's asin column the same way the batch job does, so a
        // refreshed row is indistinguishable from a swept one. landed_costs is
        // the mapping table; an existing snapshot row is the fallback for codes
        // that never made it into it.
        const [asinRows] = await pool.query(
            `SELECT asin FROM landed_costs WHERE jf_code = ? AND asin IS NOT NULL AND asin <> ''
             UNION
             SELECT asin FROM stock_snapshots WHERE jf_code = ? AND asin IS NOT NULL AND asin <> ''
             LIMIT 1`,
            [jfCode, jfCode]
        );
        const asin = asinRows[0]?.asin || '';

        const conn = await pool.getConnection();
        let stocks;
        try {
            stocks = await withTimeout(
                snapshotJfCode(conn, jfCode, asin),
                REFRESH_TIMEOUT_MS, 'Mintsoft refresh'
            );
        } finally {
            conn.release();
        }

        if (!stocks.length) {
            return res.status(404).json({ error: `No Mintsoft product found for jfCode "${jfCode}".`, jfCode });
        }

        // One entry per SKU/warehouse pair — the same grain as stock_snapshots.
        const skus = stocks.flatMap(({ sku, productId, warehouseStocks }) =>
            warehouseStocks.map(w => ({
                sku, productId, warehouseId: w.warehouseId,
                stockLevel: w.stockLevel, available: w.available,
                allocated: w.allocated, quarantine: w.quarantine,
            }))
        );
        const sum = (key) => skus.reduce((t, s) => t + (s[key] || 0), 0);

        log.info(`[POST /stock-snapshots/refresh] ${jfCode} — ${skus.length} rows (${stocks.map(s => s.sku).join(', ')})`);
        res.json({
            jfCode,
            asin: asin || null,
            refreshedAt: new Date().toISOString(),
            skus,
            // Same numbers as `skus`, grouped into the mintsoft_by_sku shape the
            // /stock-snapshots/sum endpoints return, so a caller renders the
            // breakdown the same way whichever endpoint it read.
            mintsoft_by_sku: buildMintsoftSkuBreakdown(skus.map(s => ({
                jf_code: jfCode, sku: s.sku, product_id: s.productId,
                warehouse_id: s.warehouseId, stock_level: s.stockLevel,
                available: s.available, allocated: s.allocated, quarantine: s.quarantine,
            }))),
            totals: {
                stockLevel: sum('stockLevel'), available: sum('available'),
                allocated: sum('allocated'), quarantine: sum('quarantine'),
            },
        });
    } catch (error) {
        log.error('[POST /stock-snapshots/refresh]', { jfCode, error: error.message });
        const status = error.message?.includes('timed out') ? 504 : 500;
        res.status(status).json({
            error: status === 504
                ? 'Mintsoft did not respond in time — try again.'
                : 'An internal error occurred.',
        });
    }
});

// ── Adjusted sales CRUD ──────────────────────────────────────────────────
// An ops-entered override of the sales figure for an ASIN in one marketplace.
// Grain is (asin, country) — the same grain as amazon_stock_country_snapshots —
// with 'ALL' reserved for a single cross-market figure. One live row per key:
// the figure is a current value, not a series, and every change is written to
// audit_log (entity_type 'adjusted_sales') so the history is recoverable via
// GET /audit-log?entityType=adjusted_sales&entityId=<id>.
//
// Rows are addressable two ways, because both are natural for a UI: by numeric
// id, and by the (asin, country) pair — the latter upserts, so a grid can PUT a
// cell without first knowing whether a row exists.
// The six marketplaces amazon_stock_country_snapshots actually carries, plus
// EU (used by the replenishment regions) and the reserved cross-market 'ALL'.
// Spelled out rather than reusing ASANA_REPLENISHMENT_REGIONS, which is
// declared further down this file and so isn't initialised yet at this point.
const ADJUSTED_SALES_COUNTRIES = new Set(['UK', 'DE', 'FR', 'IT', 'ES', 'US', 'EU', 'ALL']);

function rowToAdjustedSales(r) {
    return {
        id: r.id,
        asin: r.asin,
        country: r.country,
        // DECIMAL comes back from mysql2 as a string — Number() so callers get
        // a JSON number rather than "1234.00".
        adjustedSales: Number(r.adjusted_sales),
        note: r.note || null,
        createdBy: r.created_by_email || null,
        updatedBy: r.updated_by_email || null,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
    };
}

const ADJUSTED_SALES_COLS = `id, asin, country, adjusted_sales, note,
                             created_by_email, updated_by_email, created_at, updated_at`;

// Shared validation. Returns { error } or the cleaned values.
function parseAdjustedSalesKey(asinRaw, countryRaw) {
    const asin = parseAsin(asinRaw);
    if (!asin) return { error: 'asin must be a valid 10-character ASIN.' };
    const country = String(countryRaw ?? '').trim().toUpperCase();
    if (!country) return { error: 'country is required.' };
    if (!ADJUSTED_SALES_COUNTRIES.has(country)) {
        return { error: `country must be one of: ${[...ADJUSTED_SALES_COUNTRIES].join(', ')}.` };
    }
    return { asin, country };
}

// A fetch() with no headers sends text/plain, which express.json() ignores —
// the body would then arrive empty and produce a baffling "adjustedSales is
// required". Capture non-JSON bodies as raw text instead; adjustedSalesInput()
// JSON-parses the string. The matcher deliberately skips application/json so
// this can never clobber express.json's already-parsed object with ''.
const adjustedSalesTextBody = express.text({
    type: (req) => !/application\/json/i.test(req.headers['content-type'] || ''),
    limit: '256kb',
});

// Pull the request payload tolerantly. The stock endpoints in this file return
// snake_case while these routes speak camelCase, so callers legitimately send
// either — accept both rather than 400 on a naming coin-flip. Query params are
// a fallback for the same reason POST /stock-snapshots/refresh accepts them.
// A body that arrived as a JSON *string* (wrong Content-Type, double-encoded
// client) is parsed here too instead of silently reading as empty.
function adjustedSalesInput(req) {
    let body = req.body;
    if (typeof body === 'string') {
        try { body = JSON.parse(body); } catch { body = {}; }
    }
    if (!body || typeof body !== 'object') body = {};
    const src = { ...req.query, ...body };
    const pick = (...names) => {
        for (const n of names) if (src[n] !== undefined) return src[n];
        return undefined;
    };
    return {
        asin: pick('asin', 'ASIN'),
        country: pick('country', 'countryCode', 'country_code'),
        adjustedSales: pick('adjustedSales', 'adjusted_sales'),
        note: pick('note'),
        // Was a note key present at all? Drives keep-vs-clear on PUT.
        noteProvided: 'note' in src,
        receivedKeys: Object.keys(src),
    };
}

// The figure itself. Negative is rejected (a negative sales figure is always a
// mistake); 0 is legitimate — it means "assume this ASIN doesn't sell here".
// The "required" error echoes what actually arrived: this failing on a body
// that looked fine to the caller is otherwise near-impossible to diagnose.
function parseAdjustedSalesValue(raw, receivedKeys) {
    if (raw === undefined || raw === null || raw === '') {
        const got = receivedKeys?.length ? receivedKeys.join(', ') : '(no body received)';
        return {
            error: `adjustedSales is required (accepted as "adjustedSales" or "adjusted_sales", `
                 + `in a JSON body with Content-Type: application/json, or as a query param). Received: ${got}.`,
        };
    }
    const value = Number(raw);
    if (!Number.isFinite(value)) return { error: 'adjustedSales must be a number.' };
    if (value < 0) return { error: 'adjustedSales cannot be negative.' };
    if (value > 99_999_999.99) return { error: 'adjustedSales is out of range.' };
    return { value };
}

function parseAdjustedSalesNote(raw) {
    if (raw === undefined || raw === null) return { note: null };
    const note = String(raw).trim();
    if (!note) return { note: null };
    if (note.length > 500) return { error: 'note cannot exceed 500 characters.' };
    return { note };
}

// GET /api/v1/adjusted-sales?asin=&country= — list, both filters optional.
app.get('/api/v1/adjusted-sales', async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        const where = ['deleted_at IS NULL'];
        const params = [];

        if (req.query.asin) {
            const asin = parseAsin(req.query.asin);
            if (!asin) return res.status(400).json({ error: 'asin must be a valid 10-character ASIN.' });
            where.push('asin = ?'); params.push(asin);
        }
        if (req.query.country) {
            const country = String(req.query.country).trim().toUpperCase();
            if (!ADJUSTED_SALES_COUNTRIES.has(country)) {
                return res.status(400).json({ error: `country must be one of: ${[...ADJUSTED_SALES_COUNTRIES].join(', ')}.` });
            }
            where.push('country = ?'); params.push(country);
        }

        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales
                  WHERE ${where.join(' AND ')} ORDER BY asin ASC, country ASC`,
                params
            );
            return r;
        });
        res.json({ data: rows.map(rowToAdjustedSales) });
    } catch (error) {
        log.error('[GET /adjusted-sales]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/adjusted-sales/:asin — every country's figure for one ASIN.
// `byCountry` is the same rows keyed for direct lookup, so a per-country UI
// doesn't have to scan the array.
app.get('/api/v1/adjusted-sales/:asin', async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        const asin = parseAsin(req.params.asin);
        if (!asin) return res.status(400).json({ error: 'asin must be a valid 10-character ASIN.' });

        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales
                  WHERE asin = ? AND deleted_at IS NULL ORDER BY country ASC`,
                [asin]
            );
            return r;
        });
        const data = rows.map(rowToAdjustedSales);
        res.json({
            asin,
            data,
            byCountry: Object.fromEntries(data.map(d => [d.country, d])),
        });
    } catch (error) {
        log.error('[GET /adjusted-sales/:asin]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// POST /api/v1/adjusted-sales — create. 409 if a live row already holds the
// key (use PUT to change it). A soft-deleted row for the same key is revived
// and overwritten: the unique index spans deleted rows, so inserting alongside
// it isn't possible, and silently failing would be worse.
app.post('/api/v1/adjusted-sales', adjustedSalesTextBody, async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        await auditLogSchemaReady;
        const input = adjustedSalesInput(req);

        const key = parseAdjustedSalesKey(input.asin, input.country);
        if (key.error) return res.status(400).json({ error: key.error });
        const val = parseAdjustedSalesValue(input.adjustedSales, input.receivedKeys);
        if (val.error) return res.status(400).json({ error: val.error });
        const noteParsed = parseAdjustedSalesNote(input.note);
        if (noteParsed.error) return res.status(400).json({ error: noteParsed.error });

        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT ${ADJUSTED_SALES_COLS}, deleted_at FROM adjusted_sales
                  WHERE asin = ? AND country = ?`,
                [key.asin, key.country]
            );

            if (existing.length && !existing[0].deleted_at) {
                return { duplicate: true };
            }

            let id;
            let before = null;
            if (existing.length) {
                id = existing[0].id;
                before = { ...rowToAdjustedSales(existing[0]), deleted: true };
                await conn.query(
                    `UPDATE adjusted_sales
                        SET adjusted_sales = ?, note = ?, updated_by_email = ?, deleted_at = NULL
                      WHERE id = ?`,
                    [val.value, noteParsed.note, req.userEmail || null, id]
                );
            } else {
                const [ins] = await conn.query(
                    `INSERT INTO adjusted_sales
                        (asin, country, adjusted_sales, note, created_by_email, updated_by_email)
                     VALUES (?, ?, ?, ?, ?, ?)`,
                    [key.asin, key.country, val.value, noteParsed.note, req.userEmail || null, req.userEmail || null]
                );
                id = ins.insertId;
            }

            const [rows] = await conn.query(
                `SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales WHERE id = ?`, [id]
            );
            await recordAudit(conn, {
                entityType: 'adjusted_sales',
                entityId: id,
                action: 'create',
                before,
                after: rowToAdjustedSales(rows[0]),
                userEmail: req.userEmail,
            });
            return { row: rows[0] };
        });

        if (result.duplicate) {
            return res.status(409).json({
                error: `An adjusted sales figure already exists for ${key.asin} / ${key.country}. Use PUT to update it.`,
            });
        }
        res.status(201).json(rowToAdjustedSales(result.row));
    } catch (error) {
        log.error('[POST /adjusted-sales]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Shared by both PUT forms. `create` allows the upsert route to insert.
async function updateAdjustedSales(conn, { id, asin, country, value, note, noteProvided, userEmail, allowCreate }) {
    const [existing] = await conn.query(
        id != null
            ? `SELECT ${ADJUSTED_SALES_COLS}, deleted_at FROM adjusted_sales WHERE id = ?`
            : `SELECT ${ADJUSTED_SALES_COLS}, deleted_at FROM adjusted_sales WHERE asin = ? AND country = ?`,
        id != null ? [id] : [asin, country]
    );
    const live = existing.length && !existing[0].deleted_at ? existing[0] : null;

    if (!live && !allowCreate) return { notFound: true };

    if (!live) {
        // Upsert path: insert, or revive the soft-deleted row holding the key.
        if (existing.length) {
            await conn.query(
                `UPDATE adjusted_sales
                    SET adjusted_sales = ?, note = ?, updated_by_email = ?, deleted_at = NULL
                  WHERE id = ?`,
                [value, note, userEmail || null, existing[0].id]
            );
            const [rows] = await conn.query(`SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales WHERE id = ?`, [existing[0].id]);
            await recordAudit(conn, {
                entityType: 'adjusted_sales', entityId: existing[0].id, action: 'create',
                before: { ...rowToAdjustedSales(existing[0]), deleted: true },
                after: rowToAdjustedSales(rows[0]), userEmail,
            });
            return { row: rows[0], created: true };
        }
        const [ins] = await conn.query(
            `INSERT INTO adjusted_sales (asin, country, adjusted_sales, note, created_by_email, updated_by_email)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [asin, country, value, note, userEmail || null, userEmail || null]
        );
        const [rows] = await conn.query(`SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales WHERE id = ?`, [ins.insertId]);
        await recordAudit(conn, {
            entityType: 'adjusted_sales', entityId: ins.insertId, action: 'create',
            before: null, after: rowToAdjustedSales(rows[0]), userEmail,
        });
        return { row: rows[0], created: true };
    }

    const fields = ['adjusted_sales = ?', 'updated_by_email = ?'];
    const values = [value, userEmail || null];
    // Omitting `note` leaves the existing one; sending null/'' clears it.
    if (noteProvided) { fields.push('note = ?'); values.push(note); }
    values.push(live.id);

    await conn.query(`UPDATE adjusted_sales SET ${fields.join(', ')} WHERE id = ?`, values);
    const [rows] = await conn.query(`SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales WHERE id = ?`, [live.id]);
    await recordAudit(conn, {
        entityType: 'adjusted_sales', entityId: live.id, action: 'update',
        before: rowToAdjustedSales(live), after: rowToAdjustedSales(rows[0]), userEmail,
    });
    return { row: rows[0] };
}

// PUT /api/v1/adjusted-sales/:id — update an existing row by numeric id.
// A non-numeric param is an ASIN: fall through to the bulk route below.
app.put('/api/v1/adjusted-sales/:id', adjustedSalesTextBody, async (req, res, next) => {
    try {
        await adjustedSalesSchemaReady;
        await auditLogSchemaReady;
        const { id } = req.params;
        if (!/^\d+$/.test(id)) return next();
        const input = adjustedSalesInput(req);
        const val = parseAdjustedSalesValue(input.adjustedSales, input.receivedKeys);
        if (val.error) return res.status(400).json({ error: val.error });
        const noteParsed = parseAdjustedSalesNote(input.note);
        if (noteParsed.error) return res.status(400).json({ error: noteParsed.error });

        const result = await withConnection((conn) => updateAdjustedSales(conn, {
            id, value: val.value, note: noteParsed.note,
            noteProvided: input.noteProvided,
            userEmail: req.userEmail, allowCreate: false,
        }));
        if (result.notFound) return res.status(404).json({ error: `Adjusted sales figure ${id} not found.` });
        res.json(rowToAdjustedSales(result.row));
    } catch (error) {
        log.error('[PUT /adjusted-sales/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// PUT /api/v1/adjusted-sales/:asin — set every country in one request.
//
// Three body forms, all upserts:
// A DIFFERENT figure per country (any of these — the wrapper is optional and
// the value key may be either casing):
//   { "countries": { "UK": 920, "DE": { "adjustedSales": 300, "note": "..." } } }
//   { "UK": 920, "DE": 300, "FR": 150 }
//   [ { "country": "UK", "adjustedSales": 920 }, { "country": "DE", "adjusted_sales": 300 } ]
//   { "countries": [ { "country": "UK", "adjustedSales": 920 } ] }
// The SAME figure across countries:
//   { "countries": ["UK", "DE"], "adjustedSales": 920 }
//   { "adjustedSales": 920 }                        → every marketplace (as "*")
//
// "*" means the six real marketplaces (UK/DE/FR/IT/ES/US) — NOT the EU or ALL
// pseudo-countries, which have to be named explicitly so a blanket update can't
// silently write a cross-market figure nobody asked for.
//
// Runs in one transaction: either every country lands or none does, so a
// half-applied bulk edit can't leave the ASIN in a state nobody intended.
const ADJUSTED_SALES_MARKETPLACES = ['UK', 'DE', 'FR', 'IT', 'ES', 'US'];

app.put('/api/v1/adjusted-sales/:asin', adjustedSalesTextBody, async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        await auditLogSchemaReady;

        const asin = parseAsin(req.params.asin);
        if (!asin) return res.status(400).json({ error: 'asin must be a valid 10-character ASIN.' });

        const input = adjustedSalesInput(req);
        let body = req.body;
        if (typeof body === 'string') { try { body = JSON.parse(body); } catch { body = {}; } }
        if (!body || typeof body !== 'object') body = {};
        let countriesRaw = body.countries !== undefined ? body.countries : req.query.countries;

        // Unwrapped forms: a bare top-level array of per-country objects, or a
        // bare country→figure map. Recognised only when the body carries no
        // shared-value key, so { "adjustedSales": 920 } keeps meaning "one
        // figure everywhere" and can't be misread as a country map.
        const hasSharedValue = body.adjustedSales !== undefined || body.adjusted_sales !== undefined;
        if (countriesRaw === undefined) {
            if (Array.isArray(body)) {
                countriesRaw = body;
            } else {
                const countryKeys = Object.keys(body)
                    .filter(k => ADJUSTED_SALES_COUNTRIES.has(String(k).trim().toUpperCase()));
                if (countryKeys.length && hasSharedValue) {
                    return res.status(400).json({
                        error: 'Send either per-country figures or a single adjustedSales, not both.',
                    });
                }
                if (countryKeys.length) {
                    countriesRaw = Object.fromEntries(countryKeys.map(k => [k, body[k]]));
                }
            }
        }

        // Normalise every form into one list of { country, value, note,
        // noteProvided }, validating everything BEFORE touching the database —
        // a bad country in the map must not leave earlier ones applied.
        const items = [];
        const addItem = (countryRaw, valueRaw, noteRaw, noteProvided) => {
            const key = parseAdjustedSalesKey(asin, countryRaw);
            if (key.error) return key.error;
            if (items.some(i => i.country === key.country)) {
                return `country ${key.country} appears more than once.`;
            }
            const val = parseAdjustedSalesValue(valueRaw, input.receivedKeys);
            if (val.error) return `${key.country}: ${val.error}`;
            const noteParsed = parseAdjustedSalesNote(noteRaw);
            if (noteParsed.error) return `${key.country}: ${noteParsed.error}`;
            items.push({ country: key.country, value: val.value, note: noteParsed.note, noteProvided });
            return null;
        };

        // One entry of a per-country list: either a bare country code (which
        // takes the shared top-level figure) or an object carrying its own.
        const addEntry = (entry, countryFromKey) => {
            const isObj = entry && typeof entry === 'object' && !Array.isArray(entry);
            if (!isObj) {
                // In a map the key is the country and the value is the figure;
                // in a list a bare element IS the country code.
                return countryFromKey !== undefined
                    ? addItem(countryFromKey, entry, input.note, input.noteProvided)
                    : addItem(entry, input.adjustedSales, input.note, input.noteProvided);
            }
            const country = countryFromKey !== undefined
                ? countryFromKey
                : (entry.country ?? entry.countryCode ?? entry.country_code);
            const value = entry.adjustedSales !== undefined ? entry.adjustedSales
                : (entry.adjusted_sales !== undefined ? entry.adjusted_sales : input.adjustedSales);
            const noteProvided = 'note' in entry;
            return addItem(country, value, noteProvided ? entry.note : input.note,
                noteProvided || input.noteProvided);
        };

        let err = null;
        if (countriesRaw && typeof countriesRaw === 'object' && !Array.isArray(countriesRaw)) {
            // Per-country map: value is either a bare number or an object.
            for (const [countryRaw, entry] of Object.entries(countriesRaw)) {
                err = addEntry(entry, countryRaw);
                if (err) break;
            }
        } else if (Array.isArray(countriesRaw) && countriesRaw.some(e => e && typeof e === 'object')) {
            // List of per-country objects (possibly mixed with bare codes).
            if (!countriesRaw.length) {
                return res.status(400).json({ error: 'countries resolved to an empty list.' });
            }
            for (const entry of countriesRaw) {
                err = addEntry(entry);
                if (err) break;
            }
        } else {
            // Shared value across a list of countries (or every marketplace).
            const list = Array.isArray(countriesRaw)
                ? countriesRaw
                : (countriesRaw === undefined || countriesRaw === '*' || String(countriesRaw).trim() === '*'
                    ? ADJUSTED_SALES_MARKETPLACES
                    : String(countriesRaw).split(',').map(s => s.trim()).filter(Boolean));
            if (!list.length) {
                return res.status(400).json({ error: 'countries resolved to an empty list.' });
            }
            const val = parseAdjustedSalesValue(input.adjustedSales, input.receivedKeys);
            if (val.error) return res.status(400).json({ error: val.error });
            for (const countryRaw of list) {
                err = addItem(countryRaw, input.adjustedSales, input.note, input.noteProvided);
                if (err) break;
            }
        }
        if (err) return res.status(400).json({ error: err });
        if (!items.length) return res.status(400).json({ error: 'No countries to update.' });

        const results = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const out = [];
                for (const item of items) {
                    const r = await updateAdjustedSales(conn, {
                        asin, country: item.country, value: item.value,
                        note: item.note, noteProvided: item.noteProvided,
                        userEmail: req.userEmail, allowCreate: true,
                    });
                    out.push({ ...rowToAdjustedSales(r.row), action: r.created ? 'created' : 'updated' });
                }
                // Final state for the whole ASIN, so a grid can refresh from
                // this one response without a follow-up GET.
                const [rows] = await conn.query(
                    `SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales
                      WHERE asin = ? AND deleted_at IS NULL ORDER BY country ASC`,
                    [asin]
                );
                await conn.commit();
                return { applied: out, all: rows.map(rowToAdjustedSales) };
            } catch (e) {
                await conn.rollback();
                throw e;
            }
        });

        res.json({
            asin,
            created: results.applied.filter(r => r.action === 'created').length,
            updated: results.applied.filter(r => r.action === 'updated').length,
            results: results.applied,
            data: results.all,
            byCountry: Object.fromEntries(results.all.map(d => [d.country, d])),
        });
    } catch (error) {
        log.error('[PUT /adjusted-sales/:asin]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// PUT /api/v1/adjusted-sales/:asin/:country — upsert by natural key. 201 when
// it created the row, 200 when it updated one, so a UI can tell them apart.
app.put('/api/v1/adjusted-sales/:asin/:country', adjustedSalesTextBody, async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        await auditLogSchemaReady;
        const key = parseAdjustedSalesKey(req.params.asin, req.params.country);
        if (key.error) return res.status(400).json({ error: key.error });
        const input = adjustedSalesInput(req);
        const val = parseAdjustedSalesValue(input.adjustedSales, input.receivedKeys);
        if (val.error) return res.status(400).json({ error: val.error });
        const noteParsed = parseAdjustedSalesNote(input.note);
        if (noteParsed.error) return res.status(400).json({ error: noteParsed.error });

        const result = await withConnection((conn) => updateAdjustedSales(conn, {
            asin: key.asin, country: key.country, value: val.value, note: noteParsed.note,
            noteProvided: input.noteProvided,
            userEmail: req.userEmail, allowCreate: true,
        }));
        res.status(result.created ? 201 : 200).json(rowToAdjustedSales(result.row));
    } catch (error) {
        log.error('[PUT /adjusted-sales/:asin/:country]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Soft-delete, matching the rest of the schema. The row keeps its id and its
// audit trail; POSTing the same key again revives it.
async function deleteAdjustedSales(conn, { id, asin, country, userEmail }) {
    const [existing] = await conn.query(
        id != null
            ? `SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales WHERE id = ? AND deleted_at IS NULL`
            : `SELECT ${ADJUSTED_SALES_COLS} FROM adjusted_sales WHERE asin = ? AND country = ? AND deleted_at IS NULL`,
        id != null ? [id] : [asin, country]
    );
    if (!existing.length) return { notFound: true };

    await conn.query('UPDATE adjusted_sales SET deleted_at = NOW(), updated_by_email = ? WHERE id = ?',
        [userEmail || null, existing[0].id]);
    await recordAudit(conn, {
        entityType: 'adjusted_sales', entityId: existing[0].id, action: 'delete',
        before: rowToAdjustedSales(existing[0]), after: null, userEmail,
    });
    return { deleted: true };
}

app.delete('/api/v1/adjusted-sales/:id', async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        await auditLogSchemaReady;
        const { id } = req.params;
        if (!/^\d+$/.test(id)) {
            return res.status(400).json({
                error: 'Expected a numeric id. To address a row by ASIN, use DELETE /adjusted-sales/:asin/:country.',
            });
        }
        const result = await withConnection((conn) => deleteAdjustedSales(conn, { id, userEmail: req.userEmail }));
        if (result.notFound) return res.status(404).json({ error: `Adjusted sales figure ${id} not found.` });
        res.status(204).end();
    } catch (error) {
        log.error('[DELETE /adjusted-sales/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.delete('/api/v1/adjusted-sales/:asin/:country', async (req, res) => {
    try {
        await adjustedSalesSchemaReady;
        await auditLogSchemaReady;
        const key = parseAdjustedSalesKey(req.params.asin, req.params.country);
        if (key.error) return res.status(400).json({ error: key.error });
        const result = await withConnection((conn) => deleteAdjustedSales(conn, {
            asin: key.asin, country: key.country, userEmail: req.userEmail,
        }));
        if (result.notFound) {
            return res.status(404).json({ error: `No adjusted sales figure for ${key.asin} / ${key.country}.` });
        }
        res.status(204).end();
    } catch (error) {
        log.error('[DELETE /adjusted-sales/:asin/:country]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Asana: Add Dispatch Task ─────────────────────────────────────────────
const ASANA_BASE = 'https://app.asana.com/api/1.0';

const ASANA_REPLENISHMENT_PROJECT_ID = '1214003962428764';
const ASANA_REPLENISHMENT_REGIONS = ['UK', 'DE', 'FR', 'IT', 'ES', 'EU', 'US'];

const ASANA_PROJECT_BY_COMPANY = {
    JFA: '1214003962428764',
    HANGERWORLD: '1214307738471512',
};

const COUNTRY_NAME_TO_CODE = {
    UK: 'UK', GB: 'UK', BRITAIN: 'UK', 'UNITED KINGDOM': 'UK', ENGLAND: 'UK',
    DE: 'DE', GERMANY: 'DE', DEUTSCHLAND: 'DE',
    FR: 'FR', FRANCE: 'FR',
    IT: 'IT', ITALY: 'IT', ITALIA: 'IT',
    ES: 'ES', SPAIN: 'ES', ESPANA: 'ES',
    // HW USA sections are named "USA", "US - LTL", "US - FTL", "US - DHL", etc.
    // "US" matches the "US " / "US-" prefixes; "USA" is its own entry because
    // the trailing A breaks the US word boundary.
    US: 'US', USA: 'US', 'UNITED STATES': 'US', AMERICA: 'US',
};

function resolveSectionCountry(section) {
    if (!section) return null;
    const upper = section.toUpperCase();
    for (const [name, code] of Object.entries(COUNTRY_NAME_TO_CODE)) {
        const re = new RegExp(`(^|[^A-Z])${name}([^A-Z]|$)`);
        if (re.test(upper)) return code;
    }
    return null;
}

// Additional Asana projects to surface tasks from, grouped by project name.
const ASANA_ADDITIONAL_SOURCES = [
    { projectId: '1204907541333306' },
    { projectId: '1208741186098190' },
    { projectId: '1214307738471512' }, // Hangerworld replenishment
    { projectId: '1204907500325538', recentOnly: true }, // Completed Works Orders - JFA — last 5 days only
    { projectId: '1204944316569231', recentOnly: true }, // last 5 days only
];

// Archive projects surfaced in the Asana lookups. Unlike the portfolio/company
// projects, only their recently-created tasks are included (created within
// ASANA_RECENT_ONLY_WINDOW_MS) — older tasks there are noise. Applies to both
// the stock-snapshots portfolio search and the /orders/asana-tasks sweep.
const ASANA_RECENT_ONLY_PROJECT_IDS = new Set([
    '1204907500325538', // Completed Works Orders - JFA
    '1204944316569231', // recent-only additional source
]);
const ASANA_RECENT_ONLY_WINDOW_MS = 5 * 24 * 60 * 60 * 1000; // 5 days

function isWithinRecentWindow(createdAt) {
    if (!createdAt) return false;
    const ts = Date.parse(createdAt);
    return Number.isFinite(ts) && (Date.now() - ts) <= ASANA_RECENT_ONLY_WINDOW_MS;
}

// Portfolio of replenishment lists. Used to discover projects + workspace +
// the ASIN custom field GID at runtime, so the ASIN lookup can hit Asana's
// workspace task search API in a single call instead of N parallel project
// fetches. Cached in-process for ASANA_CTX_TTL_MS.
const ASANA_PORTFOLIO_GID = '1207527576522409';
const ASANA_CTX_TTL_MS = 5 * 60 * 1000;
let asanaPortfolioCtx = null;

async function getAsanaPortfolioContext(pat) {
    if (asanaPortfolioCtx && asanaPortfolioCtx.expiresAt > Date.now()) {
        return asanaPortfolioCtx;
    }
    const headers = { Authorization: `Bearer ${pat}` };

    const [meta, items] = await Promise.all([
        axios.get(`${ASANA_BASE}/portfolios/${ASANA_PORTFOLIO_GID}`, {
            headers,
            params: { opt_fields: 'workspace.gid' },
        }),
        axios.get(`${ASANA_BASE}/portfolios/${ASANA_PORTFOLIO_GID}/items`, {
            headers,
            params: { opt_fields: 'gid,name,resource_type' },
        }),
    ]);

    const workspaceGid = meta.data.data?.workspace?.gid;
    if (!workspaceGid) throw new Error('Could not resolve workspace from portfolio.');

    const portfolioProjects = (items.data.data || []).filter(i => i.resource_type === 'project');
    if (portfolioProjects.length === 0) throw new Error('Portfolio contains no projects.');

    // The POST-routing targets (JFA, Hangerworld) live outside the portfolio,
    // so tasks created via /asana-task wouldn't otherwise be searchable. Pull
    // their names and merge them in (deduped).
    const portfolioIdSet = new Set(portfolioProjects.map(p => p.gid));
    const extraCandidates = [
        ...Object.values(ASANA_PROJECT_BY_COMPANY),
        ...ASANA_RECENT_ONLY_PROJECT_IDS,
    ];
    const extraIds = [...new Set(extraCandidates)].filter(id => !portfolioIdSet.has(id));
    const extras = extraIds.length === 0 ? [] : (await Promise.all(
        extraIds.map(id =>
            axios.get(`${ASANA_BASE}/projects/${id}`, {
                headers,
                params: { opt_fields: 'gid,name' },
            })
        )
    )).map(r => r.data.data);

    const allProjects = [...portfolioProjects, ...extras];

    const { data: { data: settings } } = await axios.get(
        `${ASANA_BASE}/projects/${allProjects[0].gid}/custom_field_settings`,
        { headers, params: { opt_fields: 'custom_field.name,custom_field.gid' } }
    );
    const asinSetting = (settings || []).find(s => s.custom_field?.name === 'ASIN');
    if (!asinSetting) throw new Error('ASIN custom field not found in portfolio projects.');

    asanaPortfolioCtx = {
        workspaceGid,
        projectIds: allProjects.map(p => p.gid),
        projectNameById: Object.fromEntries(allProjects.map(p => [p.gid, p.name])),
        asinFieldGid: asinSetting.custom_field.gid,
        expiresAt: Date.now() + ASANA_CTX_TTL_MS,
    };
    return asanaPortfolioCtx;
}

// Body key → Asana custom field display name
const FIELD_KEY_TO_NAME = {
    asin:                  'ASIN',
    publish_date:          'Publish Date',
    jf_hw_code:            'JF / HW Code',
    fnsku:                 'FNSKU',
    qty_required:          'Qty Required',
    order_number:          'Order Number',
    allocations:           'Allocations',
    expiry_date:           'Expiry Date',
    ctns_to_send:          'Ctns. to Send',
    ctn_width:             'Ctn. Width',
    ctn_length:            'Ctn. Length',
    ctn_height:            'Ctn. Height',
    ctn_weight:            'Ctn. Weight',
    units_per_ctn:         'Units/Ctn.',
    drop_area:             'Drop Area',
    packing_dispatch:      'Packing / Dispatch',
    final_dispatch_checks: 'Final Dispatch Checks',
    mintsoft_updated:       'Mintsoft Updated',
    goods_status:          'Goods Status',
};

async function fetchProjectCustomFields(projectId, pat) {
    const { data: { data: settings } } = await axios.get(
        `${ASANA_BASE}/projects/${projectId}/custom_field_settings?opt_fields=custom_field.name,custom_field.gid,custom_field.type,custom_field.enum_options.name,custom_field.enum_options.gid`,
        { headers: { Authorization: `Bearer ${pat}` } }
    );
    // Build a name → field lookup
    const byName = {};
    for (const s of settings) {
        const cf = s.custom_field;
        byName[cf.name] = cf;
    }
    return byName;
}

function buildCustomFields(body, fieldsByName) {
    const custom_fields = {};
    for (const [key, asanaName] of Object.entries(FIELD_KEY_TO_NAME)) {
        const val = body[key];
        if (val === undefined || val === null || val === '') continue;

        const cf = fieldsByName[asanaName];
        if (!cf) continue; // field not on this project, skip

        if (cf.type === 'text') {
            custom_fields[cf.gid] = String(val);
        } else if (cf.type === 'number') {
            custom_fields[cf.gid] = Number(val);
        } else if (cf.type === 'date') {
            custom_fields[cf.gid] = val;
        } else if (cf.type === 'enum') {
            const opt = (cf.enum_options || []).find(o => o.name === val);
            if (!opt) {
                const valid = (cf.enum_options || []).map(o => o.name).join(', ');
                throw new Error(`Invalid value "${val}" for ${key}. Valid: ${valid}`);
            }
            custom_fields[cf.gid] = opt.gid;
        }
    }
    return custom_fields;
}

async function findOrCreateSection(projectId, carrier, pat) {
    const { data: { data: sections } } = await axios.get(
        `${ASANA_BASE}/projects/${projectId}/sections`,
        { headers: { Authorization: `Bearer ${pat}` } }
    );

    const existing = sections.find(s => s.name.toLowerCase() === carrier.toLowerCase());
    if (existing) return existing.gid;

    const { data: { data: newSection } } = await axios.post(
        `${ASANA_BASE}/projects/${projectId}/sections`,
        { data: { name: carrier } },
        { headers: { Authorization: `Bearer ${pat}` } }
    );
    return newSection.gid;
}

app.get('/api/v1/orders/asana-tasks', async (req, res) => {
    try {
        const pat = process.env.ASANA_PAT;
        if (!pat) return res.status(500).json({ error: 'ASANA_PAT not configured.' });

        const headers = { Authorization: `Bearer ${pat}` };
        const TASK_OPT_FIELDS = 'name,completed,created_at,modified_at,memberships.section.name,memberships.section.gid,custom_fields.name,custom_fields.display_value,custom_fields.text_value,custom_fields.number_value,custom_fields.enum_value.name';
        const queryProjectId = (req.query.projectId || '').trim();
        // Recent-only projects are archives (e.g. Completed Works Orders) —
        // paginating them in full is slow and we'd discard all but the last
        // 5 days anyway, so we fetch them via search with created_at.after.
        const sources = queryProjectId
            ? [{ projectId: queryProjectId, recentOnly: ASANA_RECENT_ONLY_PROJECT_IDS.has(queryProjectId) }]
            : [{ projectId: ASANA_REPLENISHMENT_PROJECT_ID }, ...ASANA_ADDITIONAL_SOURCES];
        const projectIds = sources.map(s => s.projectId);

        // Pagination within a project must stay sequential (each page needs the previous offset),
        // but projects are independent and can be fetched in parallel.
        const fetchProjectTasks = async (pid) => {
            const out = [];
            let offset = null;
            do {
                const params = { limit: 100, opt_fields: TASK_OPT_FIELDS };
                if (offset) params.offset = offset;
                const { data } = await axios.get(
                    `${ASANA_BASE}/projects/${pid}/tasks`,
                    { headers, params }
                );
                for (const t of (data.data || [])) out.push({ ...t, _projectId: pid });
                offset = data.next_page?.offset || null;
            } while (offset);
            return out;
        };
        // Server-side created_at filter via workspace search — returns only the
        // recent window (capped at 100, far more than 5 days of one project).
        const fetchRecentProjectTasks = async (pid) => {
            const ctx = await getAsanaPortfolioContext(pat);
            const sinceIso = new Date(Date.now() - ASANA_RECENT_ONLY_WINDOW_MS).toISOString();
            const { data } = await axios.get(
                `${ASANA_BASE}/workspaces/${ctx.workspaceGid}/tasks/search`,
                {
                    headers,
                    params: {
                        'projects.any': pid,
                        'created_at.after': sinceIso,
                        sort_by: 'created_at',
                        sort_ascending: false,
                        limit: 100,
                        opt_fields: TASK_OPT_FIELDS,
                    },
                }
            );
            return (data.data || []).map(t => ({ ...t, _projectId: pid }));
        };
        const tasks = (await Promise.all(
            sources.map(s => s.recentOnly ? fetchRecentProjectTasks(s.projectId) : fetchProjectTasks(s.projectId))
        )).flat();

        const flat = tasks.map(t => {
            const m = (t.memberships || []).find(x => x.section) || {};
            const cfByName = {};
            for (const cf of (t.custom_fields || [])) {
                cfByName[cf.name] = cf.display_value
                    ?? cf.text_value
                    ?? cf.number_value
                    ?? cf.enum_value?.name
                    ?? null;
            }
            return {
                gid: t.gid,
                project_id: t._projectId,
                name: t.name,
                completed: !!t.completed,
                created_at: t.created_at,
                modified_at: t.modified_at,
                section_gid: m.section?.gid || null,
                section_name: m.section?.name || null,
                asin: cfByName['ASIN'] || null,
                fnsku: cfByName['FNSKU'] || null,
                custom_fields: cfByName,
            };
        });

        // Archive projects only surface their recently created tasks (last 5
        // days), mirroring the stock-snapshots endpoint. Other projects are
        // returned in full regardless of age.
        const filtered = flat.filter(t =>
            !ASANA_RECENT_ONLY_PROJECT_IDS.has(t.project_id) || isWithinRecentWindow(t.created_at)
        );

        const bySection = {};
        for (const t of filtered) {
            const key = t.section_name || '(no section)';
            if (!bySection[key]) bySection[key] = [];
            bySection[key].push(t);
        }

        res.json({ projectIds, total: filtered.length, tasks: filtered, by_section: bySection });
    } catch (error) {
        log.error('[GET /api/v1/orders/asana-tasks]', error);
        const msg = error.response?.data?.errors?.[0]?.message || error.message;
        res.status(error.response?.status || 500).json({ error: msg });
    }
});

app.post('/api/v1/orders/asana-task', async (req, res) => {
    try {
        const pat = process.env.ASANA_PAT;
        if (!pat) return res.status(500).json({ error: 'ASANA_PAT not configured.' });

        const { sku, asin, carrier, region, company, ...fields } = req.body;
        if (!sku) return res.status(400).json({ error: 'sku is required.' });

        const companyKey = (company || '').trim().toUpperCase();
        const projectId = ASANA_PROJECT_BY_COMPANY[companyKey];
        if (!projectId) {
            return res.status(400).json({
                error: `Invalid company "${company}". Valid: ${Object.keys(ASANA_PROJECT_BY_COMPANY).join(', ')}`,
            });
        }

        const regionKey = (region || 'UK').toUpperCase();
        if (!ASANA_REPLENISHMENT_REGIONS.includes(regionKey)) {
            return res.status(400).json({
                error: `Invalid region "${regionKey}". Valid: ${ASANA_REPLENISHMENT_REGIONS.join(', ')}`,
            });
        }
        const carrierTrimmed = (carrier || '').trim();
        const sectionName = carrierTrimmed ? `${regionKey} - ${carrierTrimmed}` : regionKey;

        const fieldsByName = await fetchProjectCustomFields(projectId, pat);
        const custom_fields = buildCustomFields({ asin, ...fields }, fieldsByName);

        const sectionGid = await findOrCreateSection(projectId, sectionName, pat);

        // Create the task
        const { data: { data: task } } = await axios.post(
            `${ASANA_BASE}/tasks`,
            {
                data: {
                    name: sku,
                    projects: [projectId],
                    memberships: [{ project: projectId, section: sectionGid }],
                    custom_fields,
                },
            },
            { headers: { Authorization: `Bearer ${pat}` } }
        );

        res.status(201).json({ task_gid: task.gid, sku: task.name, asin: asin || null, company: companyKey, project_id: projectId, region: regionKey, section: sectionName });
    } catch (error) {
        log.error('[POST /api/v1/orders/asana-task]', error);
        const msg = error.response?.data?.errors?.[0]?.message || error.message;
        res.status(error.response?.status || 500).json({ error: msg });
    }
});

// ── Purchase Orders ──────────────────────────────────────────────────────
// Minimal CRUD over the purchase_orders parent table. orders.purchase_order_id
// is a plain int (no FK) — callers attach a line by setting it via
// PUT /api/v1/orders/:id { purchaseOrderId }.

function rowToPurchaseOrder(row) {
    return {
        id: row.id,
        poNumber: row.po_number,
        supplier: row.supplier || null,
        notes: row.notes || null,
        currency: row.currency || 'USD',
        shippingTotal: row.shipping_total != null ? Number(row.shipping_total) : 0,
        companyId: row.company_id ?? null,
        createdAt: row.created_at?.toISOString?.() ?? row.created_at,
        updatedAt: row.updated_at?.toISOString?.() ?? row.updated_at,
    };
}

// Given a list of PO objects (post rowToPurchaseOrder), fetch the relevant
// companies in one query and attach { ...po, company: { ... } | null }.
async function attachCompanies(conn, pos) {
    if (!pos.length) return [];
    const ids = [...new Set(pos.map(p => p.companyId).filter(Boolean))];
    if (!ids.length) return pos.map(p => ({ ...p, company: null }));
    const placeholders = ids.map(() => '?').join(',');
    const [rows] = await conn.query(
        `SELECT * FROM companies WHERE id IN (${placeholders})`, ids
    );
    const byId = new Map(rows.map(r => [r.id, rowToCompany(r)]));
    return pos.map(p => ({
        ...p,
        company: p.companyId ? (byId.get(p.companyId) || null) : null,
    }));
}

function rowToCompany(row) {
    let lines = [];
    if (row.address_lines) {
        try {
            lines = typeof row.address_lines === 'string'
                ? JSON.parse(row.address_lines)
                : row.address_lines;
        } catch { lines = []; }
    }
    return {
        id: row.id,
        name: row.name,
        addressLines: Array.isArray(lines) ? lines : [],
        country: row.country || null,
        createdAt: row.created_at?.toISOString?.() ?? row.created_at,
        updatedAt: row.updated_at?.toISOString?.() ?? row.updated_at,
    };
}

// ── Companies (buyer-side / "my company" picklist for POs) ───────────────
app.get('/api/v1/companies', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query('SELECT * FROM companies ORDER BY name ASC');
            return r;
        });
        res.json({ data: rows.map(rowToCompany) });
    } catch (error) {
        log.error('[GET /companies]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get('/api/v1/companies/:id', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const company = await withConnection(async (conn) => {
            const [r] = await conn.query('SELECT * FROM companies WHERE id = ?', [id]);
            return r[0] ? rowToCompany(r[0]) : null;
        });
        if (!company) return res.status(404).json({ error: `Company ${id} not found.` });
        res.json(company);
    } catch (error) {
        log.error('[GET /companies/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.post('/api/v1/companies', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { name, addressLines, country } = req.body || {};
        if (!name || typeof name !== 'string' || !name.trim()) {
            return res.status(400).json({ error: 'name is required.' });
        }
        if (addressLines !== undefined && !Array.isArray(addressLines)) {
            return res.status(400).json({ error: 'addressLines must be an array of strings.' });
        }
        const result = await withConnection(async (conn) => {
            try {
                const [r] = await conn.query(
                    'INSERT INTO companies (name, address_lines, country) VALUES (?, ?, ?)',
                    [name.trim(), addressLines ? JSON.stringify(addressLines) : null, country || null]
                );
                const [rows] = await conn.query('SELECT * FROM companies WHERE id = ?', [r.insertId]);
                return rowToCompany(rows[0]);
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: name.trim() };
                throw e;
            }
        });
        if (result.duplicate) return res.status(409).json({ error: `Company "${result.duplicate}" already exists.` });
        res.status(201).json(result);
    } catch (error) {
        log.error('[POST /companies]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.put('/api/v1/companies/:id', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const { name, addressLines, country } = req.body || {};
        const fields = [];
        const values = [];
        if (name !== undefined) {
            if (typeof name !== 'string' || !name.trim()) {
                return res.status(400).json({ error: 'name must be a non-empty string.' });
            }
            fields.push('name = ?'); values.push(name.trim());
        }
        if (addressLines !== undefined) {
            if (addressLines !== null && !Array.isArray(addressLines)) {
                return res.status(400).json({ error: 'addressLines must be an array of strings or null.' });
            }
            fields.push('address_lines = ?');
            values.push(addressLines === null ? null : JSON.stringify(addressLines));
        }
        if (country !== undefined) { fields.push('country = ?'); values.push(country || null); }
        if (!fields.length) return res.status(400).json({ error: 'No fields to update.' });

        const result = await withConnection(async (conn) => {
            try {
                values.push(id);
                const [upd] = await conn.query(
                    `UPDATE companies SET ${fields.join(', ')} WHERE id = ?`,
                    values
                );
                if (!upd.affectedRows) return { notFound: true };
                const [rows] = await conn.query('SELECT * FROM companies WHERE id = ?', [id]);
                return { company: rowToCompany(rows[0]) };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.notFound) return res.status(404).json({ error: `Company ${id} not found.` });
        if (result.duplicate) return res.status(409).json({ error: 'name already in use.' });
        res.json(result.company);
    } catch (error) {
        log.error('[PUT /companies/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.delete('/api/v1/companies/:id', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            // Detach POs that point at this company so we don't orphan FKs.
            await conn.query('UPDATE purchase_orders SET company_id = NULL WHERE company_id = ?', [id]);
            const [del] = await conn.query('DELETE FROM companies WHERE id = ?', [id]);
            return { deleted: del.affectedRows > 0 };
        });
        if (!result.deleted) return res.status(404).json({ error: `Company ${id} not found.` });
        res.json({ ok: true, id: Number(id) });
    } catch (error) {
        log.error('[DELETE /companies/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Purchase Orders ──────────────────────────────────────────────────────
app.get('/api/v1/purchase-orders', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const data = await withConnection(async (conn) => {
            const [rows] = await conn.query('SELECT * FROM purchase_orders WHERE deleted_at IS NULL ORDER BY created_at DESC');
            const pos = rows.map(rowToPurchaseOrder);
            return attachCompanies(conn, pos);
        });
        res.json({ data });
    } catch (error) {
        log.error('[GET /purchase-orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get('/api/v1/purchase-orders/:id', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [rows] = await conn.query('SELECT * FROM purchase_orders WHERE id = ? AND deleted_at IS NULL', [id]);
            if (!rows.length) return null;
            const [poWithCo] = await attachCompanies(conn, [rowToPurchaseOrder(rows[0])]);
            const [orders] = await conn.query(`${ORDER_SELECT} AND orders.purchase_order_id = ?`, [id]);
            return { po: poWithCo, orders: orders.map(rowToOrder) };
        });
        if (!result) return res.status(404).json({ error: `Purchase order ${id} not found.` });
        res.json(result);
    } catch (error) {
        log.error('[GET /purchase-orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.post('/api/v1/purchase-orders', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        await auditLogSchemaReady;
        // poNumber from body is intentionally ignored — it's derived from
        // the auto-increment id + the company's leading letter (e.g. JFA → J,
        // Hangerworld → H), giving deterministic PO_00001J / PO_00002H.
        const { supplier, notes, currency, shippingTotal, companyId } = req.body || {};
        if (companyId === undefined || companyId === null || companyId === '') {
            return res.status(400).json({ error: 'companyId is required (used to derive PO number).' });
        }
        const cid = Number(companyId);
        if (!Number.isInteger(cid) || cid <= 0) {
            return res.status(400).json({ error: 'companyId must be a positive integer.' });
        }

        const result = await withConnection(async (conn) => {
            const [cRows] = await conn.query('SELECT name FROM companies WHERE id = ?', [cid]);
            if (!cRows.length) return { companyNotFound: true };
            const letter = (cRows[0].name || '').trim().charAt(0).toUpperCase();
            if (!letter || !/^[A-Z]$/.test(letter)) return { invalidLetter: true };

            // Two-step insert: temp uuid satisfies UNIQUE(po_number) NOT NULL,
            // then UPDATE to PO_<padded-id><letter> once the auto-increment id
            // is known. Wrapped in a txn so a crash between steps doesn't
            // leave a tmp-named row behind.
            await conn.beginTransaction();
            try {
                const tmpPoNumber = `__tmp_${uuidv4()}`;
                const [insertResult] = await conn.query(
                    `INSERT INTO purchase_orders (po_number, supplier, notes, currency, shipping_total, company_id)
                     VALUES (?, ?, ?, ?, ?, ?)`,
                    [tmpPoNumber, supplier || null, notes || null, currency || 'USD', shippingTotal ?? 0, cid]
                );
                const newId = insertResult.insertId;
                const poNumber = `PO_${String(newId).padStart(5, '0')}${letter}`;
                await conn.query('UPDATE purchase_orders SET po_number = ? WHERE id = ?', [poNumber, newId]);

                const [rows] = await conn.query('SELECT * FROM purchase_orders WHERE id = ?', [newId]);
                const created = rowToPurchaseOrder(rows[0]);
                await recordAudit(conn, {
                    entityType: 'purchase_order', entityId: created.id, action: 'create',
                    before: null, after: created, userEmail: req.userEmail,
                });
                await conn.commit();
                const [withCo] = await attachCompanies(conn, [created]);
                return { po: withCo };
            } catch (err) {
                await conn.rollback();
                throw err;
            }
        });

        if (result.companyNotFound) return res.status(400).json({ error: `Company ${cid} not found.` });
        if (result.invalidLetter) return res.status(400).json({ error: 'Cannot derive letter — company name must start with A-Z.' });
        res.status(201).json(result.po);
    } catch (error) {
        log.error('[POST /purchase-orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.put('/api/v1/purchase-orders/:id', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        await auditLogSchemaReady;
        const { id } = req.params;
        // poNumber is derived at creation and immutable — silently ignore here.
        const { supplier, notes, currency, shippingTotal, companyId } = req.body || {};
        const fields = [];
        const values = [];
        if (supplier !== undefined) { fields.push('supplier = ?'); values.push(supplier || null); }
        if (notes !== undefined) { fields.push('notes = ?'); values.push(notes || null); }
        if (currency !== undefined) { fields.push('currency = ?'); values.push(currency || 'USD'); }
        if (shippingTotal !== undefined) { fields.push('shipping_total = ?'); values.push(Number(shippingTotal) || 0); }
        if (companyId !== undefined) { fields.push('company_id = ?'); values.push(companyId === null || companyId === '' ? null : Number(companyId)); }
        if (!fields.length) return res.status(400).json({ error: 'No fields to update.' });

        const result = await withConnection(async (conn) => {
            try {
                const [existingRows] = await conn.query('SELECT * FROM purchase_orders WHERE id = ? AND deleted_at IS NULL', [id]);
                if (!existingRows.length) return { notFound: true };
                const beforePo = rowToPurchaseOrder(existingRows[0]);

                values.push(id);
                await conn.query(
                    `UPDATE purchase_orders SET ${fields.join(', ')} WHERE id = ?`,
                    values
                );
                const [rows] = await conn.query('SELECT * FROM purchase_orders WHERE id = ?', [id]);
                const updated = rowToPurchaseOrder(rows[0]);
                await recordAudit(conn, {
                    entityType: 'purchase_order', entityId: updated.id, action: 'update',
                    before: beforePo, after: updated, userEmail: req.userEmail,
                });
                const [withCo] = await attachCompanies(conn, [updated]);
                return { po: withCo };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.notFound) return res.status(404).json({ error: `Purchase order ${id} not found.` });
        if (result.duplicate) return res.status(409).json({ error: 'poNumber already in use.' });
        res.json(result.po);
    } catch (error) {
        log.error('[PUT /purchase-orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.delete('/api/v1/purchase-orders/:id', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        await auditLogSchemaReady;
        await shipmentsSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [existingRows] = await conn.query('SELECT * FROM purchase_orders WHERE id = ? AND deleted_at IS NULL', [id]);
            if (!existingRows.length) return { deleted: false };
            const beforePo = rowToPurchaseOrder(existingRows[0]);

            // Snapshot the orders about to be cascade-deleted so we can
            // emit per-line audit rows after the bulk UPDATE. Without this,
            // the cascade would silently soft-delete lines and the PO's
            // audit feed would only show the parent 'delete'.
            const [cascadeOrderRows] = await conn.query(
                `${ORDER_SELECT} AND orders.purchase_order_id = ?`,
                [id]
            );
            const cascadeOrders = cascadeOrderRows.map(rowToOrder);

            await conn.query('UPDATE purchase_orders SET deleted_at = NOW() WHERE id = ? AND deleted_at IS NULL', [id]);
            await conn.query('UPDATE orders SET deleted_at = NOW() WHERE purchase_order_id = ? AND deleted_at IS NULL', [id]);
            await conn.query('UPDATE purchase_order_documents SET deleted_at = NOW() WHERE purchase_order_id = ? AND deleted_at IS NULL', [id]);
            await recordAudit(conn, {
                entityType: 'purchase_order', entityId: Number(id), action: 'delete',
                before: beforePo, after: null, userEmail: req.userEmail,
            });
            // Per-line audit for each cascade soft-delete. We deliberately
            // don't call recordPoAttachmentChange here — the PO itself is
            // being deleted, so a separate 'order_detached' would be noise
            // alongside the parent 'delete' event in the timeline.
            for (const o of cascadeOrders) {
                await recordAudit(conn, {
                    entityType: 'order', entityId: o.id, action: 'delete',
                    before: shipmentsLib.auditSnapshot(o), after: null, userEmail: req.userEmail,
                });
            }
            // The cascaded orders leave their booked manifests.
            if (cascadeOrders.length) {
                const key = membershipKey(cascadeOrders);
                await shipmentSync.shadow(conn, {
                    site: 'DELETE /purchase-orders/:id', inTx: false,
                    ...(key.keyKind === 'order' ? { keyKind: 'purchase_order', keyValue: String(id) } : key),
                }, c => shipmentSync.syncOrderMembership(c, cascadeOrders.map(o => o.id), { userEmail: req.userEmail }));
            }
            return { deleted: true };
        });
        if (!result.deleted) return res.status(404).json({ error: `Purchase order ${id} not found.` });
        res.json({ ok: true, id: Number(id) });
    } catch (error) {
        log.error('[DELETE /purchase-orders/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Purchase Order PDF generation ────────────────────────────────────────
// Each generate call:
//   1. Builds a PDF from a sample dataset (TODO: swap for live PO + lines)
//   2. Uploads to S3 at purchase-orders/{poId}/v{n}/{poNumber}-v{n}.pdf
//   3. Inserts a row into purchase_order_documents (idempotent versions)
//   4. Returns the new document metadata + a 10-min presigned download URL.

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const { buildPoPdf } = require('../services/po-pdf');

const s3 = new S3Client({ region: process.env.AWS_REGION || 'eu-north-1' });
const PO_BUCKET = process.env.PO_DOCS_BUCKET;
const PO_BUCKET_REGION = process.env.AWS_REGION || 'eu-north-1';

function publicS3Url(s3Key) {
    return `https://${PO_BUCKET}.s3.${PO_BUCKET_REGION}.amazonaws.com/${s3Key}`;
}

// Loads the live PO header + every order line attached to it via
// orders.purchase_order_id, plus the company (Customer Details on the PDF)
// when company_id is set. Maps into the shape buildPoPdf expects.
//
// Lines are aggregated by (jf_code, asin, product_name, unit_price) so that
// orders split across multiple shipments — e.g. via POST /:id/split or
// /containers/pack, which clone the row with a smaller quantity — appear
// as a single line on the PDF with the combined quantity. Aggregation is
// PDF-only; GET /purchase-orders/:id still returns the raw split rows.
async function loadPoForPdf(conn, po) {
    // The item name on the PO comes from the product master (jfpro_products,
    // keyed by jfcode) so it matches the canonical catalogue name rather than
    // whatever name happened to be stamped on the order line at import time.
    // Fall back to the order's own product_name when the jf_code isn't in the
    // master (or is blank).
    const [lineRows] = await conn.query(
        `SELECT o.jf_code, o.asin, o.unit_price,
                COALESCE(NULLIF(TRIM(p.name), ''), o.product_name) AS product_name,
                CAST(SUM(o.quantity) AS UNSIGNED) AS quantity,
                MIN(o.id) AS first_id
           FROM orders o
           LEFT JOIN jfpro_products p ON p.jfcode = o.jf_code
          WHERE o.purchase_order_id = ? AND o.deleted_at IS NULL
          GROUP BY o.jf_code, o.asin, o.unit_price, COALESCE(NULLIF(TRIM(p.name), ''), o.product_name)
          ORDER BY first_id ASC`,
        [po.id]
    );

    let customer = null;
    if (po.companyId) {
        const [cRows] = await conn.query('SELECT * FROM companies WHERE id = ?', [po.companyId]);
        if (cRows.length) {
            const c = rowToCompany(cRows[0]);
            customer = { name: c.name, addressLines: c.addressLines, country: c.country };
        }
    }

    return {
        po: {
            poNumber: po.poNumber,
            orderDate: po.createdAt || new Date(),
            supplier: po.supplier || '',
            supplierAddress: [], // optional — supplier address not modeled yet
            currency: po.currency || 'USD',
            shippingTotal: po.shippingTotal ?? 0,
            comments: po.notes || '',
            customer,
        },
        lines: lineRows.map(r => ({
            sku: r.jf_code || r.asin || '',
            name: r.product_name || '',
            quantity: Number(r.quantity || 0),
            unitPrice: r.unit_price != null ? Number(r.unit_price) : 0,
        })),
    };
}

app.post('/api/v1/purchase-orders/:id/generate', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        const { id } = req.params;

        const result = await withConnection(async (conn) => {
            const [existingRows] = await conn.query('SELECT * FROM purchase_orders WHERE id = ? AND deleted_at IS NULL', [id]);
            if (!existingRows.length) return { notFound: true };
            const po = rowToPurchaseOrder(existingRows[0]);

            const { po: pdfPo, lines } = await loadPoForPdf(conn, po);
            if (!lines.length) return { noLines: true };

            const pdfBuffer = await buildPoPdf(pdfPo, lines);

            // Compute next version number
            const [versionRows] = await conn.query(
                'SELECT COALESCE(MAX(version), 0) AS max_version FROM purchase_order_documents WHERE purchase_order_id = ?',
                [id]
            );
            const version = Number(versionRows[0].max_version) + 1;
            const safePoNumber = String(po.poNumber).replace(/[^A-Za-z0-9._-]/g, '_');
            // Random UUID prefix → unguessable public URL.
            const token = uuidv4();
            const s3Key = `purchase-orders/${token}/${safePoNumber}-v${version}.pdf`;
            const publicUrl = publicS3Url(s3Key);

            await s3.send(new PutObjectCommand({
                Bucket: PO_BUCKET,
                Key: s3Key,
                Body: pdfBuffer,
                ContentType: 'application/pdf',
                ContentDisposition: `inline; filename="${safePoNumber}-v${version}.pdf"`,
            }));

            const [insertResult] = await conn.query(
                `INSERT INTO purchase_order_documents
                    (purchase_order_id, version, s3_key, public_url, file_size, generated_by_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [id, version, s3Key, publicUrl, pdfBuffer.length, req.userEmail || null]
            );
            return {
                documentId: insertResult.insertId,
                version,
                s3Key,
                publicUrl,
                fileSize: pdfBuffer.length,
            };
        });

        if (result.notFound) return res.status(404).json({ error: `Purchase order ${id} not found.` });
        if (result.noLines) return res.status(400).json({ error: `Purchase order ${id} has no order lines linked.` });

        res.status(201).json({
            documentId: result.documentId,
            purchaseOrderId: Number(id),
            version: result.version,
            fileSize: result.fileSize,
            url: result.publicUrl,
        });
    } catch (error) {
        log.error('[POST /purchase-orders/:id/generate]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Shared JSON shape for a "send" row. purchase_order_document_sends and
// purchase_order_signed_pi_sends are column-identical, so the documents
// endpoint, the signed-PIs endpoint, and the orders bundle all map sends
// through here.
function sendRowToJson(s) {
    return {
        id: s.id,
        sentTo: typeof s.sent_to === 'string' ? JSON.parse(s.sent_to) : s.sent_to,
        subject: s.subject || null,
        frontMessageUid: s.front_message_uid || null,
        frontConversationId: s.front_conversation_id || null,
        sentByEmail: s.sent_by_email || null,
        sentAt: s.sent_at?.toISOString?.() ?? s.sent_at,
    };
}

app.get('/api/v1/purchase-orders/:id/documents', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const { docs, sendsByDoc } = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT id, purchase_order_id, version, s3_key, public_url, file_size,
                        generated_by_email, generated_at
                 FROM purchase_order_documents
                 WHERE purchase_order_id = ?
                 ORDER BY version DESC`,
                [id]
            );
            // Pull every send record for these doc IDs in one query, then
            // group in JS. Newest-first per doc so the UI can show "last
            // sent to X on Y" without re-sorting.
            const ids = r.map(d => d.id);
            const sendRows = ids.length
                ? (await conn.query(
                    `SELECT id, purchase_order_document_id, sent_to, subject,
                            front_message_uid, front_conversation_id, sent_by_email, sent_at
                       FROM purchase_order_document_sends
                      WHERE purchase_order_document_id IN (${ids.map(() => '?').join(',')})
                      ORDER BY sent_at DESC`,
                    ids
                ))[0]
                : [];
            const byDoc = new Map();
            for (const s of sendRows) {
                if (!byDoc.has(s.purchase_order_document_id)) byDoc.set(s.purchase_order_document_id, []);
                byDoc.get(s.purchase_order_document_id).push(s);
            }
            return { docs: r, sendsByDoc: byDoc };
        });
        res.json({
            data: docs.map(r => ({
                id: r.id,
                purchaseOrderId: r.purchase_order_id,
                version: r.version,
                fileSize: r.file_size,
                url: r.public_url || publicS3Url(r.s3_key),
                generatedByEmail: r.generated_by_email || null,
                generatedAt: r.generated_at?.toISOString?.() ?? r.generated_at,
                sends: (sendsByDoc.get(r.id) || []).map(sendRowToJson),
            })),
        });
    } catch (error) {
        log.error('[GET /purchase-orders/:id/documents]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── GET /api/v1/suppliers/:id/emails ─────────────────────────────────────
// All contact emails for a supplier. Backed by the supplier_emails table
// for multi-address support; the legacy `suppliers.contact_email` (synced
// from Mintsoft) is treated as the canonical primary and lazily inserted
// on first read so the endpoint is immediately useful for existing data.
app.get('/api/v1/suppliers/:id/emails', async (req, res) => {
    try {
        await supplierEmailsSchemaReady;
        const supplierId = Number(req.params.id);
        if (!Number.isInteger(supplierId) || supplierId <= 0) {
            return res.status(400).json({ error: 'supplier id must be a positive integer.' });
        }

        const result = await withConnection(async (conn) => {
            const [supRows] = await conn.query(
                `SELECT id, name, contact_email FROM suppliers WHERE id = ? AND deleted_at IS NULL`,
                [supplierId]
            );
            if (!supRows.length) return { notFound: true };
            const sup = supRows[0];

            const fetchEmails = async () => {
                const [rows] = await conn.query(
                    `SELECT id, supplier_id, email, label, is_primary, created_at, updated_at
                       FROM supplier_emails
                      WHERE supplier_id = ? AND deleted_at IS NULL
                      ORDER BY is_primary DESC, email ASC`,
                    [supplierId]
                );
                return rows;
            };

            const rows = await fetchEmails();
            // Contacts are owned by JFPro now (jfpro.supplier_contacts, surfaced
            // through the supplier_emails view), so the legacy contact_email
            // backfill is retired — a view is not insertable.
            return { supplier: sup, rows };
        });

        if (result.notFound) return res.status(404).json({ error: `Supplier ${supplierId} not found.` });
        res.json({
            supplierId: result.supplier.id,
            supplierName: result.supplier.name,
            data: result.rows.map(r => ({
                id: r.id,
                supplierId: r.supplier_id,
                email: r.email,
                label: r.label || null,
                isPrimary: r.is_primary === 1,
                createdAt: r.created_at?.toISOString?.() ?? r.created_at,
                updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
            })),
        });
    } catch (error) {
        log.error('[GET /suppliers/:id/emails]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── POST /api/v1/purchase-order-documents/:id/email ──────────────────────
// Sends a specific PO PDF version to a single recipient via Front, with
// the PDF attached. Purely a delivery primitive: doesn't touch order
// status, doesn't fire the legacy Make webhook. Inputs are the document
// id (which version to send) and the recipient address — the caller picks
// which generated version to deliver. Subject and body come from the stored
// `purchase_order` email template (editable via /email-templates), and can be
// overridden per-send via the request body.
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

app.post('/api/v1/purchase-order-documents/:id/email', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        await emailTemplatesSchemaReady;
        await emailReceiptsSchemaReady;

        const { id } = req.params;
        // `subject`/`body` are optional per-send overrides. When the caller
        // supplies a body (typically the template fetched via
        // GET /email-templates, then edited in the UI), it's used verbatim;
        // otherwise the stored `purchase_order` template is rendered.
        const { to, subject, body } = req.body || {};

        // `to` accepts either a single string or an array of strings. Each
        // address is validated; the whole request fails if any one is bad
        // so partial sends never happen by accident.
        const toAddresses = (Array.isArray(to) ? to : [to])
            .filter(x => typeof x === 'string')
            .map(x => x.trim())
            .filter(Boolean);
        if (toAddresses.length === 0 || toAddresses.some(a => !EMAIL_RE.test(a))) {
            return res.status(400).json({ error: 'A valid `to` email address (or array of addresses) is required.' });
        }
        if (!process.env.FRONT_API_TOKEN || !process.env.FRONT_CHANNEL_ID) {
            return res.status(500).json({ error: 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).' });
        }
        if (!PO_BUCKET) {
            return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        }

        const result = await withConnection(async (conn) => {
            const [docRows] = await conn.query(
                `SELECT d.id, d.purchase_order_id, d.version, d.s3_key, d.public_url, d.file_size,
                        po.po_number
                   FROM purchase_order_documents d
                   JOIN purchase_orders po ON po.id = d.purchase_order_id
                  WHERE d.id = ? AND po.deleted_at IS NULL`,
                [id]
            );
            if (!docRows.length) return { notFound: true };
            const doc = docRows[0];

            const obj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: doc.s3_key }));
            const pdfBytes = Buffer.from(await obj.Body.transformToByteArray());

            const safePoNumber = String(doc.po_number).replace(/[^A-Za-z0-9._-]/g, '_');
            const filename = `${safePoNumber}-v${doc.version}.pdf`;

            const tpl = await getEmailTemplate(conn, 'purchase_order');
            const vars = { poNumber: doc.po_number };
            const finalSubject = (typeof subject === 'string' && subject.trim())
                ? subject.trim()
                : (renderTemplate(tpl.subject, vars) || String(doc.po_number));
            const htmlBody = (typeof body === 'string' && body.trim())
                ? body
                : renderTemplate(tpl.bodyHtml, vars);

            // Mint a receipt token and embed a "Confirm receipt" link so the
            // recipient can one-click confirm they got this email.
            const receiptToken = await createEmailReceipt(conn, {
                emailType: 'purchase_order', sentTo: toAddresses, subject: finalSubject,
            });
            const bodyWithReceipt = appendReceiptLink(htmlBody, receiptToken, apiBaseUrlFromReq(req));

            // Front /channels/{id}/messages — multipart so we can attach the
            // PDF. `to[]` notation makes Front treat the field as an array;
            // we append once per recipient. body_format=html so our HTML
            // styling renders instead of being shown as source.
            const form = new FormData();
            for (const addr of toAddresses) form.append('to[]', addr);
            form.append('subject', finalSubject);
            form.append('body', bodyWithReceipt);
            form.append('body_format', 'html');
            form.append('options[archive]', 'false');
            form.append('attachments[]', new Blob([pdfBytes], { type: 'application/pdf' }), filename);

            const frontUrl = `https://api2.frontapp.com/channels/${encodeURIComponent(process.env.FRONT_CHANNEL_ID)}/messages`;
            const resp = await fetch(frontUrl, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`,
                    Accept: 'application/json',
                },
                body: form,
            });
            const respText = await resp.text();
            if (!resp.ok) {
                log.error('[purchase-order-documents/email] Front API error', {
                    status: resp.status, body: respText.slice(0, 500),
                });
                return { frontError: { status: resp.status, body: respText.slice(0, 500) } };
            }
            let parsed = null;
            try { parsed = JSON.parse(respText); } catch { /* 202 accepted with empty body is fine */ }

            // Pull the Front conversation id out of `_links.related.conversation`,
            // which is a URL like `.../conversations/cnv_xxx`.
            const conversationUrl = parsed?._links?.related?.conversation || '';
            const frontConversationId = conversationUrl
                ? conversationUrl.split('/').pop()
                : null;
            const frontMessageUid = parsed?.message_uid || parsed?.id || null;

            const [sendInsert] = await conn.query(
                `INSERT INTO purchase_order_document_sends
                    (purchase_order_document_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [
                    doc.id,
                    JSON.stringify(toAddresses),
                    finalSubject,
                    frontMessageUid,
                    frontConversationId,
                    req.userEmail || null,
                ]
            );
            const [sentRow] = await conn.query(
                `SELECT sent_at FROM purchase_order_document_sends WHERE id = ?`,
                [sendInsert.insertId]
            );
            const sentAt = sentRow[0]?.sent_at?.toISOString?.() ?? sentRow[0]?.sent_at ?? null;
            await linkReceiptToSend(conn, receiptToken, 'purchase_order_document_sends', sendInsert.insertId);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: doc.purchase_order_id,
                action: 'document_emailed',
                before: null,
                after: {
                    documentId: doc.id,
                    sendId: sendInsert.insertId,
                    version: doc.version,
                    to: toAddresses,
                    subject: finalSubject,
                    frontMessageUid,
                    frontConversationId,
                },
                userEmail: req.userEmail,
            });

            return {
                ok: true,
                sendId: sendInsert.insertId,
                documentId: doc.id,
                purchaseOrderId: doc.purchase_order_id,
                version: doc.version,
                sentTo: toAddresses,
                subject: finalSubject,
                sentAt,
                frontMessageUid,
                frontConversationId,
            };
        });

        if (result.notFound) return res.status(404).json({ error: `Purchase order document ${id} not found.` });
        if (result.frontError) {
            return res.status(502).json({
                error: 'Front rejected the message.',
                frontStatus: result.frontError.status,
                frontBody: result.frontError.body,
            });
        }
        res.status(200).json(result);
    } catch (error) {
        log.error('[POST /purchase-order-documents/:id/email]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Purchase Order Invoices ──────────────────────────────────────────────
// Supplier-issued invoices attached to a PO. Files are uploaded as base64
// JSON (no new multipart deps), stored in the existing public S3 bucket
// under an `invoices/` prefix, and tracked in purchase_order_invoices.
// Preview/download = the public_url (Content-Disposition: inline). Notes
// are editable post-upload; file content is immutable (re-upload = new row).
const INVOICE_UPLOAD_MAX_BYTES = 10 * 1024 * 1024; // 10 MB hard limit
const SAFE_FILENAME_RE = /[^A-Za-z0-9._-]+/g;

function invoiceRowToJson(r) {
    return {
        id: r.id,
        purchaseOrderId: r.purchase_order_id,
        filename: r.filename,
        url: r.public_url || publicS3Url(r.s3_key),
        contentType: r.content_type || null,
        fileSize: r.file_size != null ? Number(r.file_size) : null,
        notes: r.notes || null,
        uploadedByEmail: r.uploaded_by_email || null,
        uploadedAt: r.uploaded_at?.toISOString?.() ?? r.uploaded_at,
    };
}

// POST /api/v1/purchase-orders/:id/invoices
// Body: { filename, contentType?, dataBase64, notes? }
app.post('/api/v1/purchase-orders/:id/invoices', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });

        const { id } = req.params;
        const { filename, contentType, dataBase64, notes } = req.body || {};

        if (!filename || typeof filename !== 'string' || !filename.trim()) {
            return res.status(400).json({ error: 'filename is required.' });
        }
        if (!dataBase64 || typeof dataBase64 !== 'string') {
            return res.status(400).json({ error: 'dataBase64 is required.' });
        }

        // Strip any "data:...;base64," prefix so callers can pass the raw
        // FileReader.readAsDataURL output if they want.
        const rawB64 = dataBase64.replace(/^data:[^;]+;base64,/, '');
        let buffer;
        try { buffer = Buffer.from(rawB64, 'base64'); }
        catch { return res.status(400).json({ error: 'dataBase64 is not valid base64.' }); }
        if (buffer.length === 0) return res.status(400).json({ error: 'dataBase64 decoded to empty buffer.' });
        if (buffer.length > INVOICE_UPLOAD_MAX_BYTES) {
            return res.status(413).json({ error: `File too large (max ${INVOICE_UPLOAD_MAX_BYTES} bytes).` });
        }

        const result = await withConnection(async (conn) => {
            const [poRows] = await conn.query(
                `SELECT id, po_number FROM purchase_orders WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!poRows.length) return { notFound: true };
            const po = poRows[0];

            const trimmedName = filename.trim().slice(0, 200);
            const safeFilename = trimmedName.replace(SAFE_FILENAME_RE, '_');
            const token = uuidv4();
            const s3Key = `invoices/${token}/${safeFilename}`;
            const ct = (typeof contentType === 'string' && contentType.trim()) || 'application/octet-stream';

            await s3.send(new PutObjectCommand({
                Bucket: PO_BUCKET,
                Key: s3Key,
                Body: buffer,
                ContentType: ct,
                ContentDisposition: `inline; filename="${safeFilename}"`,
            }));

            const publicUrl = publicS3Url(s3Key);
            const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 4000) : null;

            const [ins] = await conn.query(
                `INSERT INTO purchase_order_invoices
                    (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                [id, trimmedName, s3Key, publicUrl, ct, buffer.length, trimmedNotes, req.userEmail || null]
            );
            const [readback] = await conn.query(
                `SELECT * FROM purchase_order_invoices WHERE id = ?`,
                [ins.insertId]
            );
            const created = invoiceRowToJson(readback[0]);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: po.id,
                action: 'invoice_uploaded',
                before: null,
                after: { invoiceId: created.id, filename: created.filename, fileSize: created.fileSize },
                userEmail: req.userEmail,
            });

            // Insert the check row in 'pending' state so the upload response
            // carries something the UI can render with a spinner. The
            // background task finalises this same row (UPDATE) once Gemini
            // settles, so the latest-check projection switches from pending
            // → succeeded/failed without a new row appearing.
            const pendingCheck = await createPendingInvoiceCheck(conn, {
                invoiceId: created.id,
                triggeredBy: 'auto_upload',
                triggeredByEmail: req.userEmail,
            });

            return { created, poId: po.id, pendingCheck };
        });

        if (result.notFound) return res.status(404).json({ error: `Purchase order ${id} not found.` });

        // Respond now — UI shows pending and refreshes later for the verdict.
        res.status(201).json({ ...result.created, check: result.pendingCheck });

        // Background: run the Gemini PO-vs-PI check on its own connection
        // and finalise the pending row we just inserted. Lambda would
        // normally freeze the container as soon as the response above is
        // built, killing this promise mid-flight — flipping
        // callbackWaitsForEmptyEventLoop = true keeps it alive until the
        // promise settles. Lambda billed duration extends past the HTTP
        // response, but the user no longer waits. If Lambda is killed
        // before completion the row stays 'pending'; manual /check inserts
        // a fresh row to recover.
        if (result.created && result.pendingCheck) {
            if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
            const invoiceId = result.created.id;
            const userEmail = req.userEmail;
            const poIdForAudit = result.poId;
            const pendingCheckId = result.pendingCheck.id;
            (async () => {
                const bgConn = await pool.getConnection();
                try {
                    const checkOut = await recordInvoiceCheck(bgConn, {
                        purchaseOrderId: id,
                        invoiceId,
                        triggeredBy: 'auto_upload',
                        triggeredByEmail: userEmail,
                        existingCheckId: pendingCheckId,
                    });
                    await recordAudit(bgConn, {
                        entityType: 'purchase_order',
                        entityId: poIdForAudit,
                        action: 'invoice_checked',
                        before: null,
                        after: {
                            invoiceId,
                            checkId: checkOut.row.id,
                            verdict: checkOut.row.verdict,
                            discrepancyCount: checkOut.row.discrepancyCount,
                            modelUsed: checkOut.row.modelUsed,
                            triggeredBy: 'auto_upload',
                        },
                        userEmail,
                    });
                } catch (e) {
                    log.warn('[invoices/upload] background auto-check failed', {
                        invoiceId, code: e.code, error: e.message,
                    });
                } finally {
                    bgConn.release();
                }
            })();
        }
    } catch (error) {
        log.error('[POST /purchase-orders/:id/invoices]', error);
        if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/purchase-orders/:id/invoices
// Lists every invoice on a PO, newest first. Each row carries the public
// `url` for direct preview/download, plus the latest check result so the
// UI can show "verdict + discrepancy count" inline without a follow-up
// fetch. Older checks are still available via /purchase-order-invoices/:id/checks.
app.get('/api/v1/purchase-orders/:id/invoices', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const { rows, latestByInvoiceId, paymentByInvoiceId } = await withConnection(async (conn) => {
            const [invoiceRows] = await conn.query(
                `SELECT id, purchase_order_id, filename, s3_key, public_url, content_type,
                        file_size, notes, uploaded_by_email, uploaded_at
                   FROM purchase_order_invoices
                  WHERE purchase_order_id = ? AND deleted_at IS NULL
                  ORDER BY uploaded_at DESC, id DESC`,
                [id]
            );
            const invoiceIds = invoiceRows.map(r => r.id);
            // Pull every check for these invoices in one query; take the
            // newest per invoice id in JS. Cheaper than a per-row
            // correlated subquery, and works on every MySQL version.
            const latestByInvoiceId = new Map();
            const paymentByInvoiceId = new Map();
            if (invoiceIds.length) {
                const placeholders = invoiceIds.map(() => '?').join(',');
                const [checkRows] = await conn.query(
                    `SELECT * FROM purchase_order_invoice_checks
                      WHERE purchase_order_invoice_id IN (${placeholders})
                      ORDER BY purchase_order_invoice_id, created_at DESC, id DESC`,
                    invoiceIds
                );
                for (const cr of checkRows) {
                    if (!latestByInvoiceId.has(cr.purchase_order_invoice_id)) {
                        latestByInvoiceId.set(cr.purchase_order_invoice_id, cr);
                    }
                }
                // One payment row per invoice (UNIQUE), so no de-dupe needed.
                const [payRows] = await conn.query(
                    `SELECT * FROM purchase_order_invoice_payments
                      WHERE purchase_order_invoice_id IN (${placeholders})`,
                    invoiceIds
                );
                for (const pr of payRows) paymentByInvoiceId.set(pr.purchase_order_invoice_id, pr);
            }
            return { rows: invoiceRows, latestByInvoiceId, paymentByInvoiceId };
        });
        res.json({
            data: rows.map(r => ({
                ...invoiceRowToJson(r),
                latestCheck: invoiceCheckRowToJson(latestByInvoiceId.get(r.id)) || null,
                payment: invoicePaymentRowToJson(paymentByInvoiceId.get(r.id)) || null,
            })),
        });
    } catch (error) {
        log.error('[GET /purchase-orders/:id/invoices]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// PUT /api/v1/purchase-orders/:poId/invoices/:invoiceId/payment-status
// Advance the extracted payment instruction's lifecycle (pending → arranged →
// paid → skipped). This is what the operator / WorldFirst job flips once a
// transfer has been arranged or settled; it never touches the extracted terms.
// Body: { paymentStatus }
const INVOICE_PAYMENT_STATUSES = ['pending', 'arranged', 'paid', 'skipped'];
app.put('/api/v1/purchase-orders/:poId/invoices/:invoiceId/payment-status', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { poId, invoiceId } = req.params;
        const paymentStatus = typeof req.body?.paymentStatus === 'string' ? req.body.paymentStatus.trim() : '';
        if (!INVOICE_PAYMENT_STATUSES.includes(paymentStatus)) {
            return res.status(400).json({ error: `paymentStatus must be one of: ${INVOICE_PAYMENT_STATUSES.join(', ')}.` });
        }

        const result = await withConnection(async (conn) => {
            const [payRows] = await conn.query(
                `SELECT p.* FROM purchase_order_invoice_payments p
                   JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id
                  WHERE p.purchase_order_invoice_id = ? AND p.purchase_order_id = ?
                    AND i.deleted_at IS NULL`,
                [invoiceId, poId]
            );
            if (!payRows.length) return { notFound: true };
            const before = payRows[0].payment_status;
            await conn.query(
                `UPDATE purchase_order_invoice_payments SET payment_status = ?
                  WHERE purchase_order_invoice_id = ?`,
                [paymentStatus, invoiceId]
            );
            const [rb] = await conn.query(
                `SELECT * FROM purchase_order_invoice_payments WHERE purchase_order_invoice_id = ?`,
                [invoiceId]
            );
            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: Number(poId),
                action: 'invoice_payment_status',
                before: { invoiceId: Number(invoiceId), paymentStatus: before },
                after: { invoiceId: Number(invoiceId), paymentStatus },
                userEmail: req.userEmail,
            });
            return { payment: invoicePaymentRowToJson(rb[0]) };
        });

        if (result.notFound) {
            return res.status(404).json({ error: `No payment instruction found for invoice ${invoiceId} on PO ${poId}.` });
        }
        res.json(result.payment);
    } catch (error) {
        log.error('[PUT /purchase-orders/:poId/invoices/:invoiceId/payment-status]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Purchase Order Signed PIs (PI_signed) ────────────────────────────────
// A signed proforma invoice attached to a PO. Same upload mechanics as
// purchase_order_invoices (base64 → public S3 under a `signed-pis/` prefix),
// but no Gemini check, and it can be emailed to a recipient via Front (with
// the file attached), recording each send like the PO document sends.
function signedPiRowToJson(r) {
    return {
        id: r.id,
        purchaseOrderId: r.purchase_order_id,
        filename: r.filename,
        url: r.public_url || publicS3Url(r.s3_key),
        contentType: r.content_type || null,
        fileSize: r.file_size != null ? Number(r.file_size) : null,
        notes: r.notes || null,
        uploadedByEmail: r.uploaded_by_email || null,
        uploadedAt: r.uploaded_at?.toISOString?.() ?? r.uploaded_at,
    };
}

// POST /api/v1/purchase-orders/:id/signed-pis
// Body: { filename, contentType?, dataBase64, notes? }
app.post('/api/v1/purchase-orders/:id/signed-pis', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });

        const { id } = req.params;
        const { filename, contentType, dataBase64, notes } = req.body || {};

        if (!filename || typeof filename !== 'string' || !filename.trim()) {
            return res.status(400).json({ error: 'filename is required.' });
        }
        if (!dataBase64 || typeof dataBase64 !== 'string') {
            return res.status(400).json({ error: 'dataBase64 is required.' });
        }

        const rawB64 = dataBase64.replace(/^data:[^;]+;base64,/, '');
        let buffer;
        try { buffer = Buffer.from(rawB64, 'base64'); }
        catch { return res.status(400).json({ error: 'dataBase64 is not valid base64.' }); }
        if (buffer.length === 0) return res.status(400).json({ error: 'dataBase64 decoded to empty buffer.' });
        if (buffer.length > INVOICE_UPLOAD_MAX_BYTES) {
            return res.status(413).json({ error: `File too large (max ${INVOICE_UPLOAD_MAX_BYTES} bytes).` });
        }

        const result = await withConnection(async (conn) => {
            const [poRows] = await conn.query(
                `SELECT id, po_number FROM purchase_orders WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!poRows.length) return { notFound: true };
            const po = poRows[0];

            const trimmedName = filename.trim().slice(0, 200);
            const safeFilename = trimmedName.replace(SAFE_FILENAME_RE, '_');
            const token = uuidv4();
            const s3Key = `signed-pis/${token}/${safeFilename}`;
            const ct = (typeof contentType === 'string' && contentType.trim()) || 'application/octet-stream';

            await s3.send(new PutObjectCommand({
                Bucket: PO_BUCKET,
                Key: s3Key,
                Body: buffer,
                ContentType: ct,
                ContentDisposition: `inline; filename="${safeFilename}"`,
            }));

            const publicUrl = publicS3Url(s3Key);
            const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 4000) : null;

            const [ins] = await conn.query(
                `INSERT INTO purchase_order_signed_pis
                    (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                [id, trimmedName, s3Key, publicUrl, ct, buffer.length, trimmedNotes, req.userEmail || null]
            );
            const [readback] = await conn.query(
                `SELECT * FROM purchase_order_signed_pis WHERE id = ?`,
                [ins.insertId]
            );
            const created = signedPiRowToJson(readback[0]);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: po.id,
                action: 'signed_pi_uploaded',
                before: null,
                after: { signedPiId: created.id, filename: created.filename, fileSize: created.fileSize },
                userEmail: req.userEmail,
            });

            return { created };
        });

        if (result.notFound) return res.status(404).json({ error: `Purchase order ${id} not found.` });
        res.status(201).json(result.created);
    } catch (error) {
        log.error('[POST /purchase-orders/:id/signed-pis]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/purchase-orders/:id/signed-pis — newest first, each with its send history.
app.get('/api/v1/purchase-orders/:id/signed-pis', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const { rows, sendsByPi } = await withConnection(async (conn) => {
            const [piRows] = await conn.query(
                `SELECT id, purchase_order_id, filename, s3_key, public_url, content_type,
                        file_size, notes, uploaded_by_email, uploaded_at
                   FROM purchase_order_signed_pis
                  WHERE purchase_order_id = ? AND deleted_at IS NULL
                  ORDER BY uploaded_at DESC, id DESC`,
                [id]
            );
            const ids = piRows.map(r => r.id);
            const sendRows = ids.length
                ? (await conn.query(
                    `SELECT id, purchase_order_signed_pi_id, sent_to, subject,
                            front_message_uid, front_conversation_id, sent_by_email, sent_at
                       FROM purchase_order_signed_pi_sends
                      WHERE purchase_order_signed_pi_id IN (${ids.map(() => '?').join(',')})
                      ORDER BY sent_at DESC`,
                    ids
                ))[0]
                : [];
            const sendsByPi = new Map();
            for (const s of sendRows) {
                if (!sendsByPi.has(s.purchase_order_signed_pi_id)) sendsByPi.set(s.purchase_order_signed_pi_id, []);
                sendsByPi.get(s.purchase_order_signed_pi_id).push(s);
            }
            return { rows: piRows, sendsByPi };
        });
        res.json({
            data: rows.map(r => ({
                ...signedPiRowToJson(r),
                sends: (sendsByPi.get(r.id) || []).map(sendRowToJson),
            })),
        });
    } catch (error) {
        log.error('[GET /purchase-orders/:id/signed-pis]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// DELETE /api/v1/purchase-order-signed-pis/:id  (soft delete)
app.delete('/api/v1/purchase-order-signed-pis/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT * FROM purchase_order_signed_pis WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            const before = signedPiRowToJson(existing[0]);

            await conn.query(
                `UPDATE purchase_order_signed_pis SET deleted_at = NOW() WHERE id = ?`,
                [id]
            );
            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: existing[0].purchase_order_id,
                action: 'signed_pi_deleted',
                before: { signedPiId: before.id, filename: before.filename },
                after: null,
                userEmail: req.userEmail,
            });
            return { ok: true };
        });
        if (result.notFound) return res.status(404).json({ error: `Signed PI ${id} not found.` });
        res.status(204).end();
    } catch (error) {
        log.error('[DELETE /purchase-order-signed-pis/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// POST /api/v1/purchase-order-signed-pis/:id/email
// Sends a signed PI to a recipient via Front with the file attached. Subject/
// body come from the `purchase_order_signed_pi` email template ({poNumber}
// token) and can be overridden per-send via the request body.
app.post('/api/v1/purchase-order-signed-pis/:id/email', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        await emailTemplatesSchemaReady;
        await emailReceiptsSchemaReady;

        const { id } = req.params;
        const { to, subject, body } = req.body || {};
        const toAddresses = (Array.isArray(to) ? to : [to])
            .filter(x => typeof x === 'string')
            .map(x => x.trim())
            .filter(Boolean);
        if (toAddresses.length === 0 || toAddresses.some(a => !EMAIL_RE.test(a))) {
            return res.status(400).json({ error: 'A valid `to` email address (or array of addresses) is required.' });
        }
        if (!process.env.FRONT_API_TOKEN || !process.env.FRONT_CHANNEL_ID) {
            return res.status(500).json({ error: 'Front is not configured (FRONT_API_TOKEN, FRONT_CHANNEL_ID).' });
        }
        if (!PO_BUCKET) {
            return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        }

        const result = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT spi.id, spi.purchase_order_id, spi.filename, spi.s3_key, spi.content_type,
                        po.po_number
                   FROM purchase_order_signed_pis spi
                   JOIN purchase_orders po ON po.id = spi.purchase_order_id
                  WHERE spi.id = ? AND spi.deleted_at IS NULL AND po.deleted_at IS NULL`,
                [id]
            );
            if (!rows.length) return { notFound: true };
            const doc = rows[0];

            const obj = await s3.send(new GetObjectCommand({ Bucket: PO_BUCKET, Key: doc.s3_key }));
            const fileBytes = Buffer.from(await obj.Body.transformToByteArray());
            const safeFilename = String(doc.filename || `signed-pi-${doc.id}`).replace(SAFE_FILENAME_RE, '_');
            const ct = doc.content_type || 'application/octet-stream';

            const tpl = await getEmailTemplate(conn, 'purchase_order_signed_pi');
            const vars = { poNumber: doc.po_number };
            const finalSubject = (typeof subject === 'string' && subject.trim())
                ? subject.trim()
                : (renderTemplate(tpl.subject, vars) || `Signed PI – ${doc.po_number}`);
            const htmlBody = (typeof body === 'string' && body.trim())
                ? body
                : renderTemplate(tpl.bodyHtml, vars);

            const receiptToken = await createEmailReceipt(conn, {
                emailType: 'purchase_order_signed_pi', sentTo: toAddresses, subject: finalSubject,
            });
            const bodyWithReceipt = appendReceiptLink(htmlBody, receiptToken, apiBaseUrlFromReq(req));

            const form = new FormData();
            for (const addr of toAddresses) form.append('to[]', addr);
            form.append('subject', finalSubject);
            form.append('body', bodyWithReceipt);
            form.append('body_format', 'html');
            form.append('options[archive]', 'false');
            form.append('attachments[]', new Blob([fileBytes], { type: ct }), safeFilename);

            const frontUrl = `https://api2.frontapp.com/channels/${encodeURIComponent(process.env.FRONT_CHANNEL_ID)}/messages`;
            const resp = await fetch(frontUrl, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${process.env.FRONT_API_TOKEN}`,
                    Accept: 'application/json',
                },
                body: form,
            });
            const respText = await resp.text();
            if (!resp.ok) {
                log.error('[purchase-order-signed-pis/email] Front API error', {
                    status: resp.status, body: respText.slice(0, 500),
                });
                return { frontError: { status: resp.status, body: respText.slice(0, 500) } };
            }
            let parsed = null;
            try { parsed = JSON.parse(respText); } catch { /* 202 with empty body is fine */ }

            const conversationUrl = parsed?._links?.related?.conversation || '';
            const frontConversationId = conversationUrl ? conversationUrl.split('/').pop() : null;
            const frontMessageUid = parsed?.message_uid || parsed?.id || null;

            const [sendInsert] = await conn.query(
                `INSERT INTO purchase_order_signed_pi_sends
                    (purchase_order_signed_pi_id, sent_to, subject, front_message_uid, front_conversation_id, sent_by_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [doc.id, JSON.stringify(toAddresses), finalSubject, frontMessageUid, frontConversationId, req.userEmail || null]
            );
            const [sentRow] = await conn.query(
                `SELECT sent_at FROM purchase_order_signed_pi_sends WHERE id = ?`,
                [sendInsert.insertId]
            );
            const sentAt = sentRow[0]?.sent_at?.toISOString?.() ?? sentRow[0]?.sent_at ?? null;
            await linkReceiptToSend(conn, receiptToken, 'purchase_order_signed_pi_sends', sendInsert.insertId);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: doc.purchase_order_id,
                action: 'signed_pi_emailed',
                before: null,
                after: { signedPiId: doc.id, sendId: sendInsert.insertId, to: toAddresses, subject: finalSubject, frontMessageUid },
                userEmail: req.userEmail,
            });

            return {
                ok: true,
                sendId: sendInsert.insertId,
                signedPiId: doc.id,
                purchaseOrderId: doc.purchase_order_id,
                sentTo: toAddresses,
                subject: finalSubject,
                sentAt,
                frontMessageUid,
                frontConversationId,
            };
        });

        if (result.notFound) return res.status(404).json({ error: `Signed PI ${id} not found.` });
        if (result.frontError) {
            return res.status(502).json({
                error: 'Front rejected the message.',
                frontStatus: result.frontError.status,
                frontBody: result.frontError.body,
            });
        }
        res.status(200).json(result);
    } catch (error) {
        log.error('[POST /purchase-order-signed-pis/:id/email]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// PATCH /api/v1/purchase-order-invoices/:id
// Body: { notes }  — notes are the only mutable field.
app.patch('/api/v1/purchase-order-invoices/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const { notes } = req.body || {};
        if (notes === undefined) return res.status(400).json({ error: 'notes is required (pass empty string to clear).' });

        const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 4000) : null;

        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT * FROM purchase_order_invoices WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            const before = invoiceRowToJson(existing[0]);

            await conn.query(
                `UPDATE purchase_order_invoices SET notes = ? WHERE id = ?`,
                [trimmedNotes || null, id]
            );
            const [readback] = await conn.query(
                `SELECT * FROM purchase_order_invoices WHERE id = ?`,
                [id]
            );
            const after = invoiceRowToJson(readback[0]);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: existing[0].purchase_order_id,
                action: 'invoice_notes_updated',
                before: { invoiceId: before.id, notes: before.notes },
                after: { invoiceId: after.id, notes: after.notes },
                userEmail: req.userEmail,
            });

            return { updated: after };
        });

        if (result.notFound) return res.status(404).json({ error: `Invoice ${id} not found.` });
        res.json(result.updated);
    } catch (error) {
        log.error('[PATCH /purchase-order-invoices/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// DELETE /api/v1/purchase-order-invoices/:id  (soft delete)
app.delete('/api/v1/purchase-order-invoices/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT * FROM purchase_order_invoices WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            const before = invoiceRowToJson(existing[0]);

            await conn.query(
                `UPDATE purchase_order_invoices SET deleted_at = NOW() WHERE id = ?`,
                [id]
            );

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: existing[0].purchase_order_id,
                action: 'invoice_deleted',
                before: { invoiceId: before.id, filename: before.filename },
                after: null,
                userEmail: req.userEmail,
            });
            return { before };
        });

        if (result.notFound) return res.status(404).json({ error: `Invoice ${id} not found.` });
        res.status(204).end();
    } catch (error) {
        log.error('[DELETE /purchase-order-invoices/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Invoice check (PO vs PI) — service + helpers ─────────────────────────
// All comparison logic lives in src/services/po-invoice-check.js; this file
// is the persistence and transport layer. recordInvoiceCheck() wraps the
// service: it calls compare(), then inserts a row into the checks table
// (whether success or failure). Same helper is used for the auto-fire on
// upload and the manual /check endpoint, so both paths produce identical
// rows. A given invoice accumulates a check row per attempt — query the
// table to see the full history.
const { compare: comparePoInvoice, upsertInvoicePaymentTerms } = require('../services/po-invoice-check');

function invoiceCheckRowToJson(r) {
    if (!r) return null;
    return {
        id: r.id,
        invoiceId: r.purchase_order_invoice_id,
        status: r.status,
        verdict: r.verdict || null,
        discrepancyCount: r.discrepancy_count != null ? Number(r.discrepancy_count) : null,
        result: typeof r.result_json === 'string' ? JSON.parse(r.result_json) : (r.result_json || null),
        modelUsed: r.model_used || null,
        inputTokens: r.input_tokens != null ? Number(r.input_tokens) : null,
        outputTokens: r.output_tokens != null ? Number(r.output_tokens) : null,
        totalTokens: r.total_tokens != null ? Number(r.total_tokens) : null,
        errorCode: r.error_code || null,
        errorMessage: r.error_message || null,
        triggeredBy: r.triggered_by || null,
        triggeredByEmail: r.triggered_by_email || null,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
    };
}

function invoicePaymentRowToJson(r) {
    if (!r) return null;
    return {
        id: r.id,
        invoiceId: r.purchase_order_invoice_id,
        purchaseOrderId: r.purchase_order_id,
        paymentType: r.payment_type || null,
        amountDue: r.amount_due != null ? Number(r.amount_due) : null,
        currency: r.currency || null,
        depositPercentage: r.deposit_percentage != null ? Number(r.deposit_percentage) : null,
        invoiceTotal: r.invoice_total != null ? Number(r.invoice_total) : null,
        dueDate: r.due_date || null, // dateStrings:['DATE'] → already 'YYYY-MM-DD'
        dueTerms: r.due_terms || null,
        beneficiaryName: r.beneficiary_name || null,
        bankName: r.bank_name || null,
        bankAddress: r.bank_address || null,
        accountNumber: r.account_number || null,
        iban: r.iban || null,
        swiftBic: r.swift_bic || null,
        intermediaryBank: r.intermediary_bank || null,
        paymentReference: r.payment_reference || null,
        rawTermsText: r.raw_terms_text || null,
        paymentStatus: r.payment_status,
        modelUsed: r.model_used || null,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
    };
}

// Payment-instruction persistence (toDateOnlyOrNull + upsertInvoicePaymentTerms)
// lives in src/services/po-invoice-check.js alongside the extraction schema, so
// the two writers (this handler and tools/backfill-invoice-payments.js) can't
// drift. upsertInvoicePaymentTerms is imported at the top of this file.

// Inserts a row in 'pending' state so the upload response can carry a
// check object the UI can render with a spinner. The background task
// later UPDATEs this row with the model verdict (or failure) via
// recordInvoiceCheck({ existingCheckId }). If Lambda dies before that
// happens, the pending row lingers — manual /check inserts a fresh row,
// and the latest-check projection still picks the newest one up.
async function createPendingInvoiceCheck(conn, { invoiceId, triggeredBy, triggeredByEmail }) {
    const [ins] = await conn.query(
        `INSERT INTO purchase_order_invoice_checks
            (purchase_order_invoice_id, status, triggered_by, triggered_by_email)
         VALUES (?, 'pending', ?, ?)`,
        [invoiceId, triggeredBy || null, triggeredByEmail || null]
    );
    const [rb] = await conn.query(
        `SELECT * FROM purchase_order_invoice_checks WHERE id = ?`,
        [ins.insertId]
    );
    return invoiceCheckRowToJson(rb[0]);
}

// Runs compare() and persists the outcome. If `existingCheckId` is set,
// UPDATEs that row in place (used by the auto-upload flow to finalize a
// pending row). Otherwise INSERTs a new row (manual /check path).
// On success returns { row, serviceResult }. On failure UPDATEs the
// pending row to 'failed' / INSERTs a fresh 'failed' row and throws an
// Error with .checkRow attached.
async function recordInvoiceCheck(conn, { purchaseOrderId, invoiceId, triggeredBy, triggeredByEmail, existingCheckId = null }) {
    try {
        const out = await comparePoInvoice(conn, { purchaseOrderId, invoiceId });
        const verdict = out.check?.overallVerdict || null;
        const discrepancyCount = Array.isArray(out.check?.discrepancies) ? out.check.discrepancies.length : null;
        const resultJson = JSON.stringify(out.check ?? null);
        const modelUsed = out.modelUsed || null;
        const inputTokens = out.usage?.promptTokenCount ?? null;
        const outputTokens = out.usage?.candidatesTokenCount ?? null;
        const totalTokens = out.usage?.totalTokenCount ?? null;

        let rowId;
        if (existingCheckId) {
            await conn.query(
                `UPDATE purchase_order_invoice_checks
                    SET status='succeeded', verdict=?, discrepancy_count=?, result_json=?,
                        model_used=?, input_tokens=?, output_tokens=?, total_tokens=?,
                        error_code=NULL, error_message=NULL
                  WHERE id = ?`,
                [verdict, discrepancyCount, resultJson, modelUsed, inputTokens, outputTokens, totalTokens, existingCheckId]
            );
            rowId = existingCheckId;
        } else {
            const [ins] = await conn.query(
                `INSERT INTO purchase_order_invoice_checks
                    (purchase_order_invoice_id, status, verdict, discrepancy_count, result_json,
                     model_used, input_tokens, output_tokens, total_tokens,
                     triggered_by, triggered_by_email)
                 VALUES (?, 'succeeded', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [
                    invoiceId, verdict, discrepancyCount, resultJson, modelUsed,
                    inputTokens, outputTokens, totalTokens,
                    triggeredBy || null, triggeredByEmail || null,
                ]
            );
            rowId = ins.insertId;
        }
        // Persist the extracted payment instructions (best-effort: a failure
        // here must not turn a successful check into a failed one).
        try {
            await upsertInvoicePaymentTerms(conn, {
                invoiceId,
                purchaseOrderId,
                checkId: rowId,
                paymentTerms: out.check?.paymentTerms,
                modelUsed,
            });
        } catch (e) {
            log.warn('[invoice-check] payment-terms upsert failed', { invoiceId, error: e.message });
        }

        const [rb] = await conn.query(
            `SELECT * FROM purchase_order_invoice_checks WHERE id = ?`,
            [rowId]
        );
        return { row: invoiceCheckRowToJson(rb[0]), serviceResult: out };
    } catch (err) {
        const errCode = err.code || null;
        const errMsg = String(err.message || err).slice(0, 4000);

        let rowId;
        if (existingCheckId) {
            await conn.query(
                `UPDATE purchase_order_invoice_checks
                    SET status='failed', error_code=?, error_message=?
                  WHERE id = ?`,
                [errCode, errMsg, existingCheckId]
            );
            rowId = existingCheckId;
        } else {
            const [ins] = await conn.query(
                `INSERT INTO purchase_order_invoice_checks
                    (purchase_order_invoice_id, status, error_code, error_message,
                     triggered_by, triggered_by_email)
                 VALUES (?, 'failed', ?, ?, ?, ?)`,
                [invoiceId, errCode, errMsg, triggeredBy || null, triggeredByEmail || null]
            );
            rowId = ins.insertId;
        }
        const [rb] = await conn.query(
            `SELECT * FROM purchase_order_invoice_checks WHERE id = ?`,
            [rowId]
        );
        const wrapped = new Error(`Check failed: ${err.message}`);
        wrapped.code = err.code || 'CHECK_FAILED';
        wrapped.cause = err;
        wrapped.checkRow = invoiceCheckRowToJson(rb[0]);
        throw wrapped;
    }
}

// POST /api/v1/purchase-orders/:poId/invoices/:invoiceId/check
// Manual re-run. Inserts a 'pending' row synchronously, returns it, then
// finalises the same row in the background. Gemini-time failures (PO not
// found, model error, etc.) land in the row's error_code/error_message
// — the UI sees them on the next refresh.
app.post('/api/v1/purchase-orders/:poId/invoices/:invoiceId/check', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { poId, invoiceId } = req.params;

        const result = await withConnection(async (conn) => {
            // Verify the invoice exists and belongs to this PO before
            // creating an orphan pending row. Other validation (PO has
            // lines, Gemini configured, etc.) happens inside the background
            // recordInvoiceCheck — failures there populate the pending row
            // as 'failed' rather than returning a 4xx.
            const [invRows] = await conn.query(
                `SELECT id FROM purchase_order_invoices
                  WHERE id = ? AND purchase_order_id = ? AND deleted_at IS NULL`,
                [invoiceId, poId]
            );
            if (!invRows.length) return { notFound: true };

            const pendingCheck = await createPendingInvoiceCheck(conn, {
                invoiceId,
                triggeredBy: 'manual',
                triggeredByEmail: req.userEmail,
            });
            return { pendingCheck };
        });

        if (result.notFound) {
            return res.status(404).json({ error: `Invoice ${invoiceId} not found on PO ${poId}.` });
        }

        // Respond with the pending row — UI shows spinner, refreshes for verdict.
        res.status(202).json({ checkRow: result.pendingCheck });

        // Background: finalise the pending row. Same callbackWaitsForEmptyEventLoop
        // dance as the upload route — keep the container alive until Gemini settles.
        if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const userEmail = req.userEmail;
        const pendingCheckId = result.pendingCheck.id;
        (async () => {
            const bgConn = await pool.getConnection();
            try {
                const checkOut = await recordInvoiceCheck(bgConn, {
                    purchaseOrderId: poId,
                    invoiceId,
                    triggeredBy: 'manual',
                    triggeredByEmail: userEmail,
                    existingCheckId: pendingCheckId,
                });
                await recordAudit(bgConn, {
                    entityType: 'purchase_order',
                    entityId: Number(poId),
                    action: 'invoice_checked',
                    before: null,
                    after: {
                        invoiceId: Number(invoiceId),
                        checkId: checkOut.row.id,
                        verdict: checkOut.row.verdict,
                        discrepancyCount: checkOut.row.discrepancyCount,
                        modelUsed: checkOut.row.modelUsed,
                        triggeredBy: 'manual',
                    },
                    userEmail,
                });
            } catch (e) {
                log.warn('[invoices/check] background recheck failed', {
                    invoiceId, code: e.code, error: e.message,
                });
            } finally {
                bgConn.release();
            }
        })();
    } catch (error) {
        log.error('[POST /purchase-orders/:poId/invoices/:invoiceId/check]', error);
        if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/purchase-order-invoices/:id/checks
// Full check history for an invoice, newest first.
app.get('/api/v1/purchase-order-invoices/:id/checks', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT * FROM purchase_order_invoice_checks
                  WHERE purchase_order_invoice_id = ?
                  ORDER BY created_at DESC, id DESC`,
                [id]
            );
            return r;
        });
        res.json({ data: rows.map(invoiceCheckRowToJson) });
    } catch (error) {
        log.error('[GET /purchase-order-invoices/:id/checks]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Purchase Order Payments ──────────────────────────────────────────────
// Bank-transfer confirmations, SWIFT receipts, wire transfer proofs, etc.
// Same upload mechanism and lifecycle as invoices but kept in a separate
// table so the audit trail and UI affordances stay clean.
function paymentRowToJson(r) {
    return {
        id: r.id,
        purchaseOrderId: r.purchase_order_id,
        filename: r.filename,
        url: r.public_url || publicS3Url(r.s3_key),
        contentType: r.content_type || null,
        fileSize: r.file_size != null ? Number(r.file_size) : null,
        notes: r.notes || null,
        uploadedByEmail: r.uploaded_by_email || null,
        uploadedAt: r.uploaded_at?.toISOString?.() ?? r.uploaded_at,
    };
}

// Bulk-fetch every send row for a set of parent ids and bucket them by parent
// id (newest first). `table`/`fkCol` are hardcoded constants at every call
// site — never user input — so the interpolation is injection-safe.
async function sendsByParent(conn, table, fkCol, ids) {
    const map = new Map();
    if (!ids.length) return map;
    const [rows] = await conn.query(
        `SELECT id, ${fkCol}, sent_to, subject, front_message_uid,
                front_conversation_id, sent_by_email, sent_at
           FROM ${table}
          WHERE ${fkCol} IN (${ids.map(() => '?').join(',')})
          ORDER BY sent_at DESC`,
        ids
    );
    for (const s of rows) {
        if (!map.has(s[fkCol])) map.set(s[fkCol], []);
        map.get(s[fkCol]).push(s);
    }
    return map;
}

// Given mapped order objects (rowToOrder output), build a deduplicated
// { [poId]: bundle } map of every purchase order they reference via
// orders.purchase_order_id — the canonical FK the rest of the app joins on.
// Each bundle is the PO header + company plus all attached documents (with
// email sends), invoices (with their latest check), signed PIs (with email
// sends), and payments. Soft-deleted POs are skipped.
//
// Efficiency: a fixed set of bulk `... WHERE parent_id IN (...)` queries
// regardless of how many orders or POs are involved — never N+1, even when
// hundreds of order lines share a single PO (the bundle is built once here
// and looked up by id on the client, not duplicated per line).
async function loadPurchaseOrdersForOrders(conn, orders) {
    const poIds = [...new Set(orders.map(o => o.purchaseOrderId).filter(Boolean))];
    if (!poIds.length) return {};

    const [poRows] = await conn.query(
        `SELECT * FROM purchase_orders WHERE id IN (${poIds.map(() => '?').join(',')}) AND deleted_at IS NULL`,
        poIds
    );
    if (!poRows.length) return {};
    const pos = await attachCompanies(conn, poRows.map(rowToPurchaseOrder));
    const liveIds = pos.map(p => p.id);
    const ph = liveIds.map(() => '?').join(',');

    // Documents (+ their email sends). This table has no deleted_at column.
    const [docRows] = await conn.query(
        `SELECT id, purchase_order_id, version, s3_key, public_url, file_size,
                generated_by_email, generated_at
           FROM purchase_order_documents
          WHERE purchase_order_id IN (${ph})
          ORDER BY version DESC`,
        liveIds
    );
    const docSends = await sendsByParent(
        conn, 'purchase_order_document_sends', 'purchase_order_document_id', docRows.map(d => d.id)
    );

    // Invoices (+ the latest check per invoice).
    const [invRows] = await conn.query(
        `SELECT id, purchase_order_id, filename, s3_key, public_url, content_type,
                file_size, notes, uploaded_by_email, uploaded_at
           FROM purchase_order_invoices
          WHERE purchase_order_id IN (${ph}) AND deleted_at IS NULL
          ORDER BY uploaded_at DESC, id DESC`,
        liveIds
    );
    const invIds = invRows.map(r => r.id);
    const latestCheckByInvoice = new Map();
    if (invIds.length) {
        const [checkRows] = await conn.query(
            `SELECT * FROM purchase_order_invoice_checks
              WHERE purchase_order_invoice_id IN (${invIds.map(() => '?').join(',')})
              ORDER BY purchase_order_invoice_id, created_at DESC, id DESC`,
            invIds
        );
        for (const cr of checkRows) {
            if (!latestCheckByInvoice.has(cr.purchase_order_invoice_id)) {
                latestCheckByInvoice.set(cr.purchase_order_invoice_id, cr);
            }
        }
    }

    // Signed PIs (+ their email sends).
    const [piRows] = await conn.query(
        `SELECT id, purchase_order_id, filename, s3_key, public_url, content_type,
                file_size, notes, uploaded_by_email, uploaded_at
           FROM purchase_order_signed_pis
          WHERE purchase_order_id IN (${ph}) AND deleted_at IS NULL
          ORDER BY uploaded_at DESC, id DESC`,
        liveIds
    );
    const piSends = await sendsByParent(
        conn, 'purchase_order_signed_pi_sends', 'purchase_order_signed_pi_id', piRows.map(r => r.id)
    );

    // Payments.
    const [payRows] = await conn.query(
        `SELECT id, purchase_order_id, filename, s3_key, public_url, content_type,
                file_size, notes, uploaded_by_email, uploaded_at
           FROM purchase_order_payments
          WHERE purchase_order_id IN (${ph}) AND deleted_at IS NULL
          ORDER BY uploaded_at DESC, id DESC`,
        liveIds
    );

    // Assemble bundles keyed by PO id. The per-PO arrays inherit each query's
    // global ORDER BY since we push rows in fetch order.
    const bundles = {};
    for (const po of pos) {
        bundles[po.id] = { ...po, documents: [], invoices: [], signedPis: [], payments: [] };
    }
    for (const d of docRows) {
        const b = bundles[d.purchase_order_id];
        if (!b) continue;
        b.documents.push({
            id: d.id,
            purchaseOrderId: d.purchase_order_id,
            version: d.version,
            fileSize: d.file_size,
            url: d.public_url || publicS3Url(d.s3_key),
            generatedByEmail: d.generated_by_email || null,
            generatedAt: d.generated_at?.toISOString?.() ?? d.generated_at,
            sends: (docSends.get(d.id) || []).map(sendRowToJson),
        });
    }
    for (const r of invRows) {
        const b = bundles[r.purchase_order_id];
        if (!b) continue;
        b.invoices.push({
            ...invoiceRowToJson(r),
            latestCheck: invoiceCheckRowToJson(latestCheckByInvoice.get(r.id)) || null,
        });
    }
    for (const r of piRows) {
        const b = bundles[r.purchase_order_id];
        if (!b) continue;
        b.signedPis.push({
            ...signedPiRowToJson(r),
            sends: (piSends.get(r.id) || []).map(sendRowToJson),
        });
    }
    for (const r of payRows) {
        const b = bundles[r.purchase_order_id];
        if (!b) continue;
        b.payments.push(paymentRowToJson(r));
    }
    return bundles;
}

// POST /api/v1/purchase-orders/:id/payments
// Body: { filename, contentType?, dataBase64, notes? }
app.post('/api/v1/purchase-orders/:id/payments', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });

        const { id } = req.params;
        const { filename, contentType, dataBase64, notes } = req.body || {};

        if (!filename || typeof filename !== 'string' || !filename.trim()) {
            return res.status(400).json({ error: 'filename is required.' });
        }
        if (!dataBase64 || typeof dataBase64 !== 'string') {
            return res.status(400).json({ error: 'dataBase64 is required.' });
        }

        const rawB64 = dataBase64.replace(/^data:[^;]+;base64,/, '');
        let buffer;
        try { buffer = Buffer.from(rawB64, 'base64'); }
        catch { return res.status(400).json({ error: 'dataBase64 is not valid base64.' }); }
        if (buffer.length === 0) return res.status(400).json({ error: 'dataBase64 decoded to empty buffer.' });
        if (buffer.length > INVOICE_UPLOAD_MAX_BYTES) {
            return res.status(413).json({ error: `File too large (max ${INVOICE_UPLOAD_MAX_BYTES} bytes).` });
        }

        const result = await withConnection(async (conn) => {
            const [poRows] = await conn.query(
                `SELECT id, po_number FROM purchase_orders WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!poRows.length) return { notFound: true };
            const po = poRows[0];

            const trimmedName = filename.trim().slice(0, 200);
            const safeFilename = trimmedName.replace(SAFE_FILENAME_RE, '_');
            const token = uuidv4();
            const s3Key = `payments/${token}/${safeFilename}`;
            const ct = (typeof contentType === 'string' && contentType.trim()) || 'application/octet-stream';

            await s3.send(new PutObjectCommand({
                Bucket: PO_BUCKET,
                Key: s3Key,
                Body: buffer,
                ContentType: ct,
                ContentDisposition: `inline; filename="${safeFilename}"`,
            }));

            const publicUrl = publicS3Url(s3Key);
            const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 4000) : null;

            const [ins] = await conn.query(
                `INSERT INTO purchase_order_payments
                    (purchase_order_id, filename, s3_key, public_url, content_type, file_size, notes, uploaded_by_email)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
                [id, trimmedName, s3Key, publicUrl, ct, buffer.length, trimmedNotes, req.userEmail || null]
            );
            const [readback] = await conn.query(
                `SELECT * FROM purchase_order_payments WHERE id = ?`,
                [ins.insertId]
            );
            const created = paymentRowToJson(readback[0]);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: po.id,
                action: 'payment_uploaded',
                before: null,
                after: { paymentId: created.id, filename: created.filename, fileSize: created.fileSize },
                userEmail: req.userEmail,
            });

            return { created };
        });

        if (result.notFound) return res.status(404).json({ error: `Purchase order ${id} not found.` });
        res.status(201).json(result.created);
    } catch (error) {
        log.error('[POST /purchase-orders/:id/payments]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/purchase-orders/:id/payments
app.get('/api/v1/purchase-orders/:id/payments', async (req, res) => {
    try {
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT id, purchase_order_id, filename, s3_key, public_url, content_type,
                        file_size, notes, uploaded_by_email, uploaded_at
                   FROM purchase_order_payments
                  WHERE purchase_order_id = ? AND deleted_at IS NULL
                  ORDER BY uploaded_at DESC, id DESC`,
                [id]
            );
            return r;
        });
        res.json({ data: rows.map(paymentRowToJson) });
    } catch (error) {
        log.error('[GET /purchase-orders/:id/payments]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// PATCH /api/v1/purchase-order-payments/:id
// Body: { notes }  — notes are the only mutable field.
app.patch('/api/v1/purchase-order-payments/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const { notes } = req.body || {};
        if (notes === undefined) return res.status(400).json({ error: 'notes is required (pass empty string to clear).' });

        const trimmedNotes = typeof notes === 'string' ? notes.trim().slice(0, 4000) : null;

        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT * FROM purchase_order_payments WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            const before = paymentRowToJson(existing[0]);

            await conn.query(
                `UPDATE purchase_order_payments SET notes = ? WHERE id = ?`,
                [trimmedNotes || null, id]
            );
            const [readback] = await conn.query(
                `SELECT * FROM purchase_order_payments WHERE id = ?`,
                [id]
            );
            const after = paymentRowToJson(readback[0]);

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: existing[0].purchase_order_id,
                action: 'payment_notes_updated',
                before: { paymentId: before.id, notes: before.notes },
                after: { paymentId: after.id, notes: after.notes },
                userEmail: req.userEmail,
            });
            return { updated: after };
        });

        if (result.notFound) return res.status(404).json({ error: `Payment ${id} not found.` });
        res.json(result.updated);
    } catch (error) {
        log.error('[PATCH /purchase-order-payments/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// DELETE /api/v1/purchase-order-payments/:id  (soft delete)
app.delete('/api/v1/purchase-order-payments/:id', async (req, res) => {
    try {
        await auditLogSchemaReady;
        await purchaseOrdersSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT * FROM purchase_order_payments WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            const before = paymentRowToJson(existing[0]);

            await conn.query(
                `UPDATE purchase_order_payments SET deleted_at = NOW() WHERE id = ?`,
                [id]
            );

            await recordAudit(conn, {
                entityType: 'purchase_order',
                entityId: existing[0].purchase_order_id,
                action: 'payment_deleted',
                before: { paymentId: before.id, filename: before.filename },
                after: null,
                userEmail: req.userEmail,
            });
            return { before };
        });

        if (result.notFound) return res.status(404).json({ error: `Payment ${id} not found.` });
        res.status(204).end();
    } catch (error) {
        log.error('[DELETE /purchase-order-payments/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Email receipt status (read) ──────────────────────────────────────────
// Per-email confirmation status for the "Confirm receipt" links embedded in
// outbound supplier/forwarder emails (see src/lib/email-receipt.js). One row
// per sent email, with a three-state lifecycle:
//   'sent'      → link never fetched
//   'opened'    → landing page fetched (WEAK: link-scanners fetch it too)
//   'confirmed' → recipient pressed the Confirm button (STRONG: a form POST)
// Filter by a specific send (sendTable+sendId, as recorded on the *_sends row),
// by email_type, or by status ('sent'|'opened'|'confirmed'). Newest first.
//   email_type ∈ purchase_order | purchase_order_signed_pi |
//                draft_container_quote | quality_assurance
// The token is intentionally NOT returned — it is a click capability, not data.
function rowToEmailReceipt(r) {
    let sentTo = null;
    if (r.sent_to) {
        try { sentTo = JSON.parse(r.sent_to); } catch { sentTo = r.sent_to; }
    }
    const status = r.confirmed_at ? 'confirmed' : (r.opened_at ? 'opened' : 'sent');
    return {
        id: r.id,
        emailType: r.email_type,
        sendTable: r.send_table || null,
        sendId: r.send_id || null,
        sentTo,
        subject: r.subject || null,
        status,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        openedAt: r.opened_at?.toISOString?.() ?? r.opened_at,
        openCount: Number(r.open_count || 0),
        confirmedAt: r.confirmed_at?.toISOString?.() ?? r.confirmed_at,
        confirmedIp: r.confirmed_ip || null,
        confirmedUserAgent: r.confirmed_user_agent || null,
        reminderCount: Number(r.reminder_count || 0),
        lastReminderAt: r.last_reminder_at?.toISOString?.() ?? r.last_reminder_at ?? null,
    };
}

app.get('/api/v1/email-receipts', async (req, res) => {
    try {
        await emailReceiptsSchemaReady;
        const { sendTable, emailType, status } = req.query;
        // Clamp paging to sane bounds; both are sanitized ints, safe to inline.
        const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 500);
        const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);

        const where = [];
        const params = [];
        if (sendTable) { where.push('send_table = ?'); params.push(String(sendTable)); }
        if (req.query.sendId !== undefined && Number.isFinite(Number(req.query.sendId))) {
            where.push('send_id = ?'); params.push(Number(req.query.sendId));
        }
        if (emailType) { where.push('email_type = ?'); params.push(String(emailType)); }
        if (status === 'confirmed') where.push('confirmed_at IS NOT NULL');
        else if (status === 'opened') where.push('opened_at IS NOT NULL AND confirmed_at IS NULL');
        else if (status === 'sent' || status === 'pending') where.push('opened_at IS NULL AND confirmed_at IS NULL');
        const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT id, email_type, send_table, send_id, sent_to, subject,
                        created_at, opened_at, open_count, confirmed_at, confirmed_ip, confirmed_user_agent,
                        reminder_count, last_reminder_at
                   FROM email_receipts
                   ${whereSql}
                  ORDER BY created_at DESC, id DESC
                  LIMIT ${limit} OFFSET ${offset}`,
                params
            );
            return r;
        });
        res.json({ data: rows.map(rowToEmailReceipt) });
    } catch (error) {
        log.error('[GET /email-receipts]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── POST /api/v1/email-receipts/:id/resend ───────────────────────────────
// Resend a reminder for a receipt that hasn't been confirmed yet. Sends a
// short standalone email (no attachment) to the ORIGINAL recipients via Front,
// reusing the original confirm token so a click updates the same row. No-op
// (200, resent:false) if the receipt is already confirmed. Bumps
// reminder_count / last_reminder_at.
app.post('/api/v1/email-receipts/:id/resend', async (req, res) => {
    try {
        await emailReceiptsSchemaReady;
        const id = Number(req.params.id);
        if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id.' });

        const receipt = await withConnection((conn) => getReceiptForResend(conn, id));
        if (!receipt) return res.status(404).json({ error: 'Email receipt not found.' });
        if (receipt.confirmed_at) {
            return res.json({ resent: false, reason: 'already_confirmed' });
        }

        // Same send path the auto-reminder sweep uses (reuses the original
        // token, shows the reminder count).
        const result = await sendReminderForReceipt(receipt, apiBaseUrlFromReq(req));
        if (!result.ok) {
            if (result.reason === 'no_recipients') {
                return res.status(422).json({ error: 'No recipients recorded for this email; cannot resend.' });
            }
            if (result.reason === 'front_not_configured') {
                return res.status(503).json({ error: 'Front is not configured.' });
            }
            log.error('[email-receipts/resend] Front API error', {
                status: result.status, body: result.detail,
            });
            return res.status(502).json({ error: 'Failed to send reminder via Front.' });
        }

        const updated = await withConnection(async (conn) => {
            await recordReminderSend(conn, id, {
                reminderNumber: result.reminderNumber,
                sentTo: result.recipients,
                subject: result.subject,
                frontMessageUid: result.frontMessageUid,
                frontConversationId: result.frontConversationId,
                sentByEmail: req.userEmail || null,
            });
            const [rows] = await conn.query(
                `SELECT id, email_type, send_table, send_id, sent_to, subject,
                        created_at, opened_at, open_count, confirmed_at, confirmed_ip, confirmed_user_agent,
                        reminder_count, last_reminder_at
                   FROM email_receipts WHERE id = ?`,
                [id]
            );
            await recordAudit(conn, {
                entityType: 'email_receipt',
                entityId: id,
                action: 'reminder_sent',
                before: null,
                after: { to: result.recipients, subject: result.subject, reminderNumber: result.reminderNumber },
                userEmail: req.userEmail,
            });
            return rows[0];
        });

        res.json({ resent: true, data: updated ? rowToEmailReceipt(updated) : null });
    } catch (error) {
        log.error('[POST /email-receipts/:id/resend]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── GET /api/v1/email-receipts/:id/reminders ─────────────────────────────
// The list of follow-up (reminder) emails sent for a receipt, oldest first.
// Each row is one send — automated sweep (sentBy null) or a manual resend
// (sentBy = operator). The parent receipt's reminderCount/lastReminderAt is the
// summary; this is the detail.
app.get('/api/v1/email-receipts/:id/reminders', async (req, res) => {
    try {
        await emailReceiptsSchemaReady;
        const id = Number(req.params.id);
        if (!Number.isFinite(id)) return res.status(400).json({ error: 'Invalid id.' });
        const rows = await withConnection((conn) => listReminders(conn, id));
        res.json({
            data: rows.map((r) => {
                let sentTo = null;
                if (r.sent_to) {
                    try { sentTo = JSON.parse(r.sent_to); } catch { sentTo = r.sent_to; }
                }
                return {
                    id: r.id,
                    emailReceiptId: r.email_receipt_id,
                    reminderNumber: Number(r.reminder_number || 0),
                    sentTo,
                    subject: r.subject || null,
                    frontMessageUid: r.front_message_uid || null,
                    frontConversationId: r.front_conversation_id || null,
                    sentBy: r.sent_by_email || null,
                    sentAt: r.created_at?.toISOString?.() ?? r.created_at,
                };
            }),
        });
    } catch (error) {
        log.error('[GET /email-receipts/:id/reminders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Email templates CRUD ─────────────────────────────────────────────────
// The subject/body HTML used when emailing documents via Front. The send
// handlers read by template_key; this CRUD lets ops view and edit the copy.
// The UI typically GETs a template, lets the user tweak the body, then posts
// the edited body back as the `body` override on the send endpoint.
function rowToEmailTemplate(r) {
    return {
        id: r.id,
        key: r.template_key,
        name: r.name,
        category: r.category || null,
        description: r.description || null,
        subject: r.subject || null,
        body: r.body_html,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        updatedAt: r.updated_at?.toISOString?.() ?? r.updated_at,
    };
}

const EMAIL_TEMPLATE_COLS = `id, template_key, name, category, description, subject, body_html,
                             created_at, updated_at`;

app.get('/api/v1/email-templates', async (req, res) => {
    try {
        await emailTemplatesSchemaReady;
        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT ${EMAIL_TEMPLATE_COLS} FROM email_templates
                  WHERE deleted_at IS NULL ORDER BY name ASC`
            );
            return r;
        });
        res.json({ data: rows.map(rowToEmailTemplate) });
    } catch (error) {
        log.error('[GET /email-templates]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Look up by numeric id or by template_key, so the UI can fetch the template
// for a given send (e.g. /email-templates/purchase_order) without knowing ids.
app.get('/api/v1/email-templates/:idOrKey', async (req, res) => {
    try {
        await emailTemplatesSchemaReady;
        const { idOrKey } = req.params;
        const byId = /^\d+$/.test(idOrKey);
        const row = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT ${EMAIL_TEMPLATE_COLS} FROM email_templates
                  WHERE ${byId ? 'id' : 'template_key'} = ? AND deleted_at IS NULL`,
                [idOrKey]
            );
            return r[0] || null;
        });
        if (!row) return res.status(404).json({ error: `Email template ${idOrKey} not found.` });
        res.json(rowToEmailTemplate(row));
    } catch (error) {
        log.error('[GET /email-templates/:idOrKey]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.post('/api/v1/email-templates', async (req, res) => {
    try {
        await emailTemplatesSchemaReady;
        const { key, name, category, description, subject, body } = req.body || {};
        if (!key || typeof key !== 'string' || !/^[a-z0-9_]+$/.test(key.trim())) {
            return res.status(400).json({ error: 'key is required (lowercase letters, digits and underscores only).' });
        }
        if (!name || typeof name !== 'string' || !name.trim()) {
            return res.status(400).json({ error: 'name is required.' });
        }
        if (typeof body !== 'string' || !body.trim()) {
            return res.status(400).json({ error: 'body (HTML) is required.' });
        }
        const result = await withConnection(async (conn) => {
            try {
                const [ins] = await conn.query(
                    `INSERT INTO email_templates (template_key, name, category, description, subject, body_html)
                     VALUES (?, ?, ?, ?, ?, ?)`,
                    [key.trim(), name.trim(), category == null ? null : (String(category).trim() || null), description || null, subject || null, body]
                );
                const [rows] = await conn.query(
                    `SELECT ${EMAIL_TEMPLATE_COLS} FROM email_templates WHERE id = ?`,
                    [ins.insertId]
                );
                return { row: rows[0] };
            } catch (e) {
                if (e.code === 'ER_DUP_ENTRY') return { duplicate: true };
                throw e;
            }
        });
        if (result.duplicate) return res.status(409).json({ error: `Template key "${key.trim()}" already exists.` });
        res.status(201).json(rowToEmailTemplate(result.row));
    } catch (error) {
        log.error('[POST /email-templates]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.put('/api/v1/email-templates/:id', async (req, res) => {
    try {
        await emailTemplatesSchemaReady;
        const { id } = req.params;
        // template_key is the stable handle the send handlers rely on, so it's
        // immutable here — name/category/description/subject/body are editable.
        const { name, category, description, subject, body } = req.body || {};
        const fields = [];
        const values = [];
        if (name !== undefined) {
            if (!name || !String(name).trim()) return res.status(400).json({ error: 'name cannot be empty.' });
            fields.push('name = ?'); values.push(String(name).trim());
        }
        if (category !== undefined) { fields.push('category = ?'); values.push(category == null ? null : (String(category).trim() || null)); }
        if (description !== undefined) { fields.push('description = ?'); values.push(description || null); }
        if (subject !== undefined) { fields.push('subject = ?'); values.push(subject || null); }
        if (body !== undefined) {
            if (typeof body !== 'string' || !body.trim()) return res.status(400).json({ error: 'body cannot be empty.' });
            fields.push('body_html = ?'); values.push(body);
        }
        if (!fields.length) return res.status(400).json({ error: 'No fields to update.' });

        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT id FROM email_templates WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            values.push(id);
            await conn.query(`UPDATE email_templates SET ${fields.join(', ')} WHERE id = ?`, values);
            const [rows] = await conn.query(
                `SELECT ${EMAIL_TEMPLATE_COLS} FROM email_templates WHERE id = ?`,
                [id]
            );
            return { row: rows[0] };
        });
        if (result.notFound) return res.status(404).json({ error: `Email template ${id} not found.` });
        res.json(rowToEmailTemplate(result.row));
    } catch (error) {
        log.error('[PUT /email-templates/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.delete('/api/v1/email-templates/:id', async (req, res) => {
    try {
        await emailTemplatesSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [existing] = await conn.query(
                `SELECT id FROM email_templates WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            if (!existing.length) return { notFound: true };
            await conn.query('UPDATE email_templates SET deleted_at = NOW() WHERE id = ?', [id]);
            return { deleted: true };
        });
        if (result.notFound) return res.status(404).json({ error: `Email template ${id} not found.` });
        res.status(204).end();
    } catch (error) {
        log.error('[DELETE /email-templates/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Audit log retrieval ──────────────────────────────────────────────────
// Filters are all optional and AND'd together. Limit caps at 500 to keep
// payloads bounded; default 100. Most-recent first.
app.get('/api/v1/audit-log', async (req, res) => {
    try {
        await auditLogSchemaReady;
        const { entityType, entityId, action, userEmail } = req.query;

        let limit = parseInt(req.query.limit, 10);
        if (!Number.isFinite(limit) || limit <= 0) limit = 100;
        if (limit > 500) limit = 500;

        // When the caller asks for a specific PO's audit trail, also surface
        // the lifecycle of every order line that's been attached to it —
        // currently linked, plus any order ever marked attached/detached
        // for this PO. Opt out with ?includeLines=false.
        const includeLines = String(req.query.includeLines || 'true').toLowerCase() !== 'false';
        const poExpand = (
            entityType === 'purchase_order' &&
            entityId !== undefined && entityId !== '' &&
            includeLines
        );

        let whereSql;
        let params;
        if (poExpand) {
            const poId = Number(entityId);
            if (!Number.isInteger(poId)) return res.status(400).json({ error: 'entityId must be an integer.' });
            const extra = [];
            const extraParams = [];
            if (action) { extra.push('action = ?'); extraParams.push(String(action)); }
            if (userEmail) { extra.push('user_email = ?'); extraParams.push(String(userEmail).toLowerCase()); }
            const extraSql = extra.length ? ` AND ${extra.join(' AND ')}` : '';
            whereSql = `WHERE (
                (entity_type = 'purchase_order' AND entity_id = ?)
                OR (entity_type = 'order' AND entity_id IN (
                    SELECT id FROM orders WHERE purchase_order_id = ?
                    UNION
                    SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(after_json, '$.orderId')) AS UNSIGNED)
                      FROM audit_log
                     WHERE entity_type = 'purchase_order'
                       AND entity_id = ?
                       AND action = 'order_attached'
                    UNION
                    SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(before_json, '$.orderId')) AS UNSIGNED)
                      FROM audit_log
                     WHERE entity_type = 'purchase_order'
                       AND entity_id = ?
                       AND action = 'order_detached'
                ))
            )${extraSql}`;
            params = [poId, poId, poId, poId, ...extraParams];
        } else {
            const where = [];
            params = [];
            if (entityType) { where.push('entity_type = ?'); params.push(String(entityType)); }
            if (entityId) {
                const n = Number(entityId);
                if (!Number.isInteger(n)) return res.status(400).json({ error: 'entityId must be an integer.' });
                where.push('entity_id = ?'); params.push(n);
            }
            if (action) { where.push('action = ?'); params.push(String(action)); }
            if (userEmail) { where.push('user_email = ?'); params.push(String(userEmail).toLowerCase()); }
            whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
        }

        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT id, entity_type, entity_id, action, before_json, after_json, user_email, created_at
                 FROM audit_log ${whereSql}
                 ORDER BY id DESC
                 LIMIT ${limit}`,
                params
            );
            return r;
        });

        res.json({
            data: rows.map(r => ({
                id: r.id,
                entityType: r.entity_type,
                entityId: r.entity_id,
                action: r.action,
                before: parseJsonCol(r.before_json),
                after: parseJsonCol(r.after_json),
                userEmail: r.user_email || null,
                createdAt: r.created_at?.toISOString?.() ?? r.created_at,
            })),
            ...(poExpand ? { expanded: 'lines' } : {}),
        });
    } catch (error) {
        log.error('[GET /audit-log]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── QC Reports ─────────────────────────────────────────────────────────────
// Upload a supplier QC / inspection report (PDF), store it in S3, run Gemini
// extraction, and attach the report to every order whose (jf_code, lot_number)
// the report inspected — each with a pass/fail verdict.
//
// Real reports run 9-40 MB (scanned, photo-heavy), which blows past the 6 MB
// HTTP API request-body cap, so the file is NOT posted to us as base64. Instead
// the client gets a short-lived presigned S3 PUT URL and uploads straight to
// S3, then calls /analyze. Gemini on a 40 MB scan (with the Pro fallback) can
// take 1-2 min, but HTTP API caps an integration at 30 s — so /analyze flips to
// 'processing', responds immediately, runs Gemini in the background (same
// callbackWaitsForEmptyEventLoop trick as the invoice auto-check), and the
// client polls GET /qc-reports/:id for the result.
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { HeadObjectCommand } = require('@aws-sdk/client-s3');
const { analyzeQcReport, countMatchedItems } = require('../services/qc-report-check');

const qcReportsSchemaReady = (async () => {
    const conn = await pool.getConnection();
    try {
        // The uploaded file + its analysis run. status:
        // awaiting_upload -> processing -> succeeded | failed.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS qc_reports (
                id INT NOT NULL AUTO_INCREMENT,
                filename VARCHAR(200) NOT NULL,
                s3_key VARCHAR(500) NOT NULL,
                public_url VARCHAR(1000) NULL,
                content_type VARCHAR(100) NULL,
                file_size INT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'awaiting_upload',
                supplier VARCHAR(255) NULL,
                report_title VARCHAR(255) NULL,
                inspection_date VARCHAR(100) NULL,
                model_used VARCHAR(64) NULL,
                item_count INT NULL,
                matched_count INT NULL,
                result_json JSON NULL,
                error_message TEXT NULL,
                uploaded_by_email VARCHAR(255) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                analyzed_at TIMESTAMP NULL,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                KEY idx_status (status),
                KEY idx_created_at (created_at)
            )
        `);
        // The attachment: one row per (report, matched order) with that order's
        // pass/fail verdict. UNIQUE(report, order) keeps re-analysis idempotent.
        await conn.query(`
            CREATE TABLE IF NOT EXISTS order_qc_reports (
                id INT NOT NULL AUTO_INCREMENT,
                qc_report_id INT NOT NULL,
                order_id INT NOT NULL,
                jf_code VARCHAR(50) NULL,
                lot_number VARCHAR(255) NULL,
                qc_result VARCHAR(16) NOT NULL,
                result_detail VARCHAR(1000) NULL,
                match_method VARCHAR(16) NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_report_order (qc_report_id, order_id),
                KEY idx_order_id (order_id),
                KEY idx_qc_report_id (qc_report_id)
            )
        `);
        // Origin columns for reports auto-imported from Front (the email a report
        // came from, who sent it, etc.) — see src/services/front-qc-import.js.
        // Additive + idempotent so this never depends on the importer running.
        for (const ddl of [
            `ALTER TABLE qc_reports ADD COLUMN source VARCHAR(32) NULL`,
            `ALTER TABLE qc_reports ADD COLUMN source_ref VARCHAR(255) NULL`,
            `ALTER TABLE qc_reports ADD COLUMN source_meta JSON NULL`,
            `ALTER TABLE qc_reports ADD COLUMN source_received_at DATETIME NULL`,
            `ALTER TABLE qc_reports ADD UNIQUE KEY uk_source_ref (source_ref)`,
        ]) {
            try { await conn.query(ddl); }
            catch (e) { if (e.errno !== 1060 && e.errno !== 1061) throw e; } // dup column / dup key
        }
    } finally {
        conn.release();
    }
})().catch(err => log.error('[orders] qc_reports schema migration failed', err));

// mysql2 returns JSON columns already parsed; tolerate a string too.
function parseJsonColumn(v) {
    if (v == null) return null;
    if (typeof v === 'object') return v;
    try { return JSON.parse(v); } catch { return null; }
}

function qcReportRowToJson(r) {
    return {
        id: r.id,
        filename: r.filename,
        url: r.public_url || publicS3Url(r.s3_key),
        // Audited fetch route — front-end should GET this *with the Bearer
        // token* (returns JSON { url } pointing at a short-lived presigned S3
        // URL), then open that url, instead of opening `url`/`documentUrl`
        // directly — so each view/download lands in audit_log. Add
        // `?disposition=attachment` to force a download vs. in-browser preview.
        downloadPath: `/api/v1/qc-reports/${r.id}/download`,
        status: r.status,
        supplier: r.supplier || null,
        reportTitle: r.report_title || null,
        inspectionDate: r.inspection_date || null,
        modelUsed: r.model_used || null,
        itemCount: r.item_count != null ? Number(r.item_count) : null,
        matchedCount: r.matched_count != null ? Number(r.matched_count) : null,
        fileSize: r.file_size != null ? Number(r.file_size) : null,
        error: r.error_message || null,
        uploadedByEmail: r.uploaded_by_email || null,
        source: r.source || null,
        sourceMeta: parseJsonColumn(r.source_meta),
        sourceReceivedAt: r.source_received_at?.toISOString?.() ?? r.source_received_at ?? null,
        createdAt: r.created_at?.toISOString?.() ?? r.created_at,
        analyzedAt: r.analyzed_at?.toISOString?.() ?? r.analyzed_at,
    };
}

// The full report detail (used by GET /:id and the manual attach/detach routes):
// the report + its attached `orders` + the still-`unmatched` extraction items.
// Same key names across both lists so the UI renders them identically; an
// unmatched item's `nearMiss` carries the "exists under another lot/code" hint
// the user can confirm via POST /:id/orders.
function qcReportDetailJson(r, links) {
    const documentUrl = r.public_url || publicS3Url(r.s3_key);
    const unmatchedRaw = parseJsonColumn(r.result_json)?.unmatched || [];
    return {
        ...qcReportRowToJson(r),
        documentUrl,
        orders: links.map(l => ({
            orderId: l.order_id,
            jfCode: l.jf_code,
            lotNumber: l.lot_number,
            qaStatus: l.qc_result,
            detail: l.result_detail,
            matchMethod: l.match_method,
            productName: l.product_name || null,
            poNumber: l.po_number || null,
            orderStatus: l.order_status || null,
            documentUrl,
        })),
        unmatched: unmatchedRaw.map(u => ({
            jfCode: u.jfCode || null,
            lotNumber: u.lotNumber || null,
            qaStatus: u.qcResult || u.qaStatus || null,
            detail: u.resultDetail || u.detail || null,
            nearMiss: u.nearMiss || null,
        })),
    };
}

// POST /api/v1/qc-reports
// Body: { filename, contentType? }
// Creates the report row (awaiting_upload) and returns a presigned S3 PUT URL.
// The client PUTs the file bytes to `uploadUrl` with the given Content-Type,
// then calls POST /qc-reports/:id/analyze.
app.post('/api/v1/qc-reports', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });

        const { filename, contentType } = req.body || {};
        if (!filename || typeof filename !== 'string' || !filename.trim()) {
            return res.status(400).json({ error: 'filename is required.' });
        }
        const trimmedName = filename.trim().slice(0, 200);
        const safeFilename = trimmedName.replace(SAFE_FILENAME_RE, '_');
        const token = uuidv4();
        const s3Key = `qc-reports/${token}/${safeFilename}`;
        const ct = (typeof contentType === 'string' && contentType.trim()) || 'application/pdf';

        const created = await withConnection(async (conn) => {
            const [ins] = await conn.query(
                `INSERT INTO qc_reports (filename, s3_key, public_url, content_type, status, uploaded_by_email)
                 VALUES (?, ?, ?, ?, 'awaiting_upload', ?)`,
                [trimmedName, s3Key, publicS3Url(s3Key), ct, req.userEmail || null]
            );
            const [rows] = await conn.query(`SELECT * FROM qc_reports WHERE id = ?`, [ins.insertId]);
            return rows[0];
        });

        const uploadUrl = await getSignedUrl(
            s3,
            new PutObjectCommand({ Bucket: PO_BUCKET, Key: s3Key, ContentType: ct }),
            { expiresIn: 600 }
        );

        res.status(201).json({
            ...qcReportRowToJson(created),
            uploadUrl,
            s3Key,
            uploadMethod: 'PUT',
            uploadHeaders: { 'Content-Type': ct },
            expiresInSeconds: 600,
        });
    } catch (error) {
        log.error('[POST /qc-reports]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// POST /api/v1/qc-reports/:id/analyze
// Confirms the file landed in S3, flips the report to 'processing', responds
// immediately, then runs Gemini + order matching in the background and writes
// the order_qc_reports links. Re-callable to re-run (replaces prior links).
app.post('/api/v1/qc-reports/:id/analyze', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        await auditLogSchemaReady;
        const { id } = req.params;
        const model = (req.body && typeof req.body.model === 'string' && req.body.model.trim()) || undefined;

        const prep = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT * FROM qc_reports WHERE id = ? AND deleted_at IS NULL`, [id]
            );
            if (!rows.length) return { notFound: true };
            const r = rows[0];
            if (r.status === 'processing') return { already: true, row: r };

            let head;
            try { head = await s3.send(new HeadObjectCommand({ Bucket: PO_BUCKET, Key: r.s3_key })); }
            catch { return { notUploaded: true }; }

            await conn.query(
                `UPDATE qc_reports SET status = 'processing', file_size = ?, error_message = NULL, analyzed_at = NULL WHERE id = ?`,
                [head.ContentLength != null ? Number(head.ContentLength) : null, id]
            );
            const [readback] = await conn.query(`SELECT * FROM qc_reports WHERE id = ?`, [id]);
            return { row: readback[0] };
        });

        if (prep.notFound) return res.status(404).json({ error: `QC report ${id} not found.` });
        if (prep.notUploaded) return res.status(409).json({ error: 'File not found in S3 — PUT it to the uploadUrl before calling /analyze.' });
        if (prep.already) return res.status(202).json(qcReportRowToJson(prep.row));

        // Respond now; the client polls GET /qc-reports/:id for the verdict.
        res.status(202).json(qcReportRowToJson(prep.row));

        if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const userEmail = req.userEmail;
        const reportRow = prep.row;
        (async () => {
            const bgConn = await pool.getConnection();
            try {
                const out = await analyzeQcReport(bgConn, {
                    s3Key: reportRow.s3_key,
                    contentType: reportRow.content_type,
                    model,
                });

                // Replace any prior links so a re-run is idempotent, then write
                // one row per (report, matched order).
                await bgConn.query(`DELETE FROM order_qc_reports WHERE qc_report_id = ?`, [reportRow.id]);
                for (const m of out.matched) {
                    await bgConn.query(
                        `INSERT INTO order_qc_reports
                            (qc_report_id, order_id, jf_code, lot_number, qc_result, result_detail, match_method)
                         VALUES (?, ?, ?, ?, ?, ?, ?)
                         ON DUPLICATE KEY UPDATE
                            jf_code = VALUES(jf_code), lot_number = VALUES(lot_number),
                            qc_result = VALUES(qc_result), result_detail = VALUES(result_detail),
                            match_method = VALUES(match_method)`,
                        [
                            reportRow.id, m.orderId, m.jfCode, m.lotNumber, m.qcResult,
                            m.resultDetail ? String(m.resultDetail).slice(0, 1000) : null,
                            m.matchMethod,
                        ]
                    );
                }

                await bgConn.query(
                    `UPDATE qc_reports
                        SET status = 'succeeded', supplier = ?, report_title = ?, inspection_date = ?,
                            model_used = ?, item_count = ?, matched_count = ?, file_size = ?,
                            result_json = ?, error_message = NULL, analyzed_at = NOW()
                      WHERE id = ?`,
                    [
                        out.report.supplier, out.report.title, out.report.inspectionDate,
                        out.modelUsed, out.items.length, out.matchedItemCount, out.fileSize,
                        JSON.stringify({ matched: out.matched, unmatched: out.unmatched, items: out.items }),
                        reportRow.id,
                    ]
                );

                await recordAudit(bgConn, {
                    entityType: 'qc_report',
                    entityId: reportRow.id,
                    action: 'qc_report_analyzed',
                    before: null,
                    after: {
                        itemCount: out.items.length,
                        matchedOrders: out.matched.length,
                        unmatched: out.unmatched.length,
                        modelUsed: out.modelUsed,
                    },
                    userEmail,
                });
            } catch (e) {
                log.error('[qc-reports/analyze] background failed', { id: reportRow.id, code: e.code, error: e.message });
                try {
                    await bgConn.query(
                        `UPDATE qc_reports SET status = 'failed', error_message = ? WHERE id = ?`,
                        [String(e.message).slice(0, 4000), reportRow.id]
                    );
                } catch (e2) {
                    log.error('[qc-reports/analyze] failed to record failure', { id: reportRow.id, error: e2.message });
                }
            } finally {
                bgConn.release();
            }
        })();
    } catch (error) {
        log.error('[POST /qc-reports/:id/analyze]', error);
        if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/qc-reports — list newest first (no per-order detail).
// Optional ?jfCode=JF0134 filters to reports that cover that product code —
// either attached to an order of that code, OR listed in the report's extracted
// items (so inspections that didn't match an order, e.g. a misread lot, are
// still found). Code match is whitespace/punctuation-insensitive.
app.get('/api/v1/qc-reports', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        const jfCodeRaw = typeof req.query.jfCode === 'string' ? req.query.jfCode.trim() : '';
        const jfNorm = jfCodeRaw.toUpperCase().replace(/[\s\-_.]/g, '');

        const cols = `r.id, r.filename, r.s3_key, r.public_url, r.content_type, r.file_size, r.status,
                      r.supplier, r.report_title, r.inspection_date, r.model_used, r.item_count,
                      r.matched_count, r.uploaded_by_email, r.created_at, r.analyzed_at, r.source, r.source_meta,
                      r.source_received_at`;

        const rows = await withConnection(async (conn) => {
            if (jfNorm) {
                const [r] = await conn.query(
                    `SELECT ${cols}
                       FROM qc_reports r
                      WHERE r.deleted_at IS NULL
                        AND (
                          EXISTS (
                            SELECT 1 FROM order_qc_reports l
                             WHERE l.qc_report_id = r.id
                               AND UPPER(REPLACE(REPLACE(REPLACE(l.jf_code,' ',''),'-',''),'_','')) = ?
                          )
                          OR REPLACE(REPLACE(REPLACE(LOWER(CAST(r.result_json AS CHAR)),' ',''),'-',''),'_','')
                             LIKE CONCAT('%', ?, '%')
                        )
                      ORDER BY COALESCE(r.source_received_at, r.created_at) DESC, r.id DESC
                      LIMIT 200`,
                    [jfNorm, jfNorm.toLowerCase()]
                );
                return r;
            }
            const [r] = await conn.query(
                `SELECT ${cols}
                   FROM qc_reports r
                  WHERE r.deleted_at IS NULL
                  ORDER BY COALESCE(r.source_received_at, r.created_at) DESC, r.id DESC
                  LIMIT 200`
            );
            return r;
        });
        res.json({ data: rows.map(qcReportRowToJson), ...(jfNorm ? { jfCode: jfCodeRaw } : {}) });
    } catch (error) {
        log.error('[GET /qc-reports]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/qc-reports/:id — report + matched orders (each with the document
// URL + pass/fail) + the unmatched items (with near-miss hints).
app.get('/api/v1/qc-reports/:id', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        const { id } = req.params;
        const result = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT * FROM qc_reports WHERE id = ? AND deleted_at IS NULL`, [id]
            );
            if (!rows.length) return { notFound: true };
            const [links] = await conn.query(
                `SELECT l.order_id, l.jf_code, l.lot_number, l.qc_result, l.result_detail, l.match_method,
                        o.product_name, o.po_number, o.status AS order_status
                   FROM order_qc_reports l
                   LEFT JOIN orders o ON o.id = l.order_id
                  WHERE l.qc_report_id = ?
                  ORDER BY l.order_id`,
                [id]
            );
            return { row: rows[0], links };
        });
        if (result.notFound) return res.status(404).json({ error: `QC report ${id} not found.` });

        res.json(qcReportDetailJson(result.row, result.links));
    } catch (error) {
        log.error('[GET /qc-reports/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/qc-reports/:id/download — the audited way to fetch the PDF.
// Writes a `qc_report_downloaded` audit row attributed to the requesting user,
// then returns a short-lived presigned S3 URL as JSON. The client (which must
// send the JWT Bearer token, like every QC route) opens that `url` itself
// (window.open / iframe src) so the bytes flow S3 → browser, never through
// Lambda. It returns JSON rather than a 302 because a redirect can only be used
// via a plain browser navigation, which can't carry the Bearer token this API
// requires — so a 302 here would just 401 at the authorizer and log nothing.
// `GET /api/v1/audit-log?entityType=qc_report&action=qc_report_downloaded` then
// yields the named list of who fetched a given report. `?disposition=inline`
// (default) opens it in-browser (preview); `?disposition=attachment` forces a
// download — the mode is recorded in the audit row so previews and downloads
// stay distinguishable.
app.get('/api/v1/qc-reports/:id/download', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        await auditLogSchemaReady;
        if (!PO_BUCKET) return res.status(500).json({ error: 'PO_DOCS_BUCKET env var not configured.' });
        const { id } = req.params;
        const disposition = req.query.disposition === 'attachment' ? 'attachment' : 'inline';

        const report = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT id, filename, s3_key, content_type FROM qc_reports WHERE id = ? AND deleted_at IS NULL`,
                [id]
            );
            return rows[0] || null;
        });
        if (!report) return res.status(404).json({ error: `QC report ${id} not found.` });

        // Quote the filename for the Content-Disposition header (RFC 6266); a
        // missing object still presigns fine — S3 returns the 404 on fetch.
        const safeName = String(report.filename || `qc-report-${id}.pdf`).replace(/"/g, '');
        const downloadUrl = await getSignedUrl(
            s3,
            new GetObjectCommand({
                Bucket: PO_BUCKET,
                Key: report.s3_key,
                ResponseContentDisposition: `${disposition}; filename="${safeName}"`,
                ...(report.content_type ? { ResponseContentType: report.content_type } : {}),
            }),
            { expiresIn: 300 }
        );

        await withConnection((conn) => recordAudit(conn, {
            entityType: 'qc_report',
            entityId: Number(id),
            action: 'qc_report_downloaded',
            before: null,
            after: { disposition, filename: report.filename || null },
            userEmail: req.userEmail,
        }));

        res.json({ id: Number(id), url: downloadUrl, disposition, expiresInSeconds: 300 });
    } catch (error) {
        log.error('[GET /qc-reports/:id/download]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// DELETE /api/v1/qc-reports/:id — soft-delete the report. Leaves the
// order_qc_reports links (the orders' GET filters them by the report's
// deleted_at via the JOIN below), and leaves the S3 object in place.
app.delete('/api/v1/qc-reports/:id', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        const { id } = req.params;
        const out = await withConnection(async (conn) => {
            const [rows] = await conn.query(
                `SELECT id FROM qc_reports WHERE id = ? AND deleted_at IS NULL`, [id]
            );
            if (!rows.length) return { notFound: true };
            await conn.query(`UPDATE qc_reports SET deleted_at = NOW() WHERE id = ?`, [id]);
            return { ok: true };
        });
        if (out.notFound) return res.status(404).json({ error: `QC report ${id} not found.` });
        res.json({ id: Number(id), deleted: true });
    } catch (error) {
        log.error('[DELETE /qc-reports/:id]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// GET /api/v1/orders/:id/qc-reports — QC reports attached to a single order
// (its QC history), newest analysis first. The "attached to each order" view.
app.get('/api/v1/orders/:id/qc-reports', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        const { id } = req.params;
        const rows = await withConnection(async (conn) => {
            const [r] = await conn.query(
                `SELECT l.qc_report_id, l.qc_result, l.result_detail, l.match_method,
                        l.jf_code, l.lot_number, l.created_at AS linked_at,
                        r.filename, r.s3_key, r.public_url, r.supplier, r.report_title,
                        r.inspection_date, r.status, r.analyzed_at
                   FROM order_qc_reports l
                   JOIN qc_reports r ON r.id = l.qc_report_id
                  WHERE l.order_id = ? AND r.deleted_at IS NULL
                  ORDER BY r.analyzed_at DESC, l.qc_report_id DESC`,
                [id]
            );
            return r;
        });
        res.json({
            data: rows.map(l => ({
                reportId: l.qc_report_id,
                qaStatus: l.qc_result,
                detail: l.result_detail,
                matchMethod: l.match_method,
                jfCode: l.jf_code,
                lotNumber: l.lot_number,
                documentUrl: l.public_url || publicS3Url(l.s3_key),
                downloadPath: `/api/v1/qc-reports/${l.qc_report_id}/download`,
                filename: l.filename,
                supplier: l.supplier || null,
                reportTitle: l.report_title || null,
                inspectionDate: l.inspection_date || null,
                status: l.status,
                analyzedAt: l.analyzed_at?.toISOString?.() ?? l.analyzed_at,
            })),
        });
    } catch (error) {
        log.error('[GET /orders/:id/qc-reports]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// POST /api/v1/qc-reports/:id/orders — manually attach an order to a report,
// e.g. confirming a near-miss the strict matcher won't auto-attach. The link is
// stored with match_method='manual'. If the optional `item` (the unmatched
// extraction being resolved) is given, it's removed from result_json.unmatched
// so it stops showing as unmatched. Returns the refreshed report (GET /:id shape).
// Body: { orderId, qcResult?, resultDetail?, item?: { jfCode, lotNumber } }
app.post('/api/v1/qc-reports/:id/orders', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        const { id } = req.params;
        const body = req.body || {};
        const orderId = Number(body.orderId);
        if (!Number.isInteger(orderId) || orderId <= 0) {
            return res.status(400).json({ error: 'orderId (a positive integer) is required.' });
        }
        const qcResult = typeof body.qcResult === 'string' ? body.qcResult.toLowerCase().trim() : null;
        if (qcResult && !['pass', 'fail', 'unknown'].includes(qcResult)) {
            return res.status(400).json({ error: "qcResult must be 'pass', 'fail', or 'unknown'." });
        }

        const out = await withConnection(async (conn) => {
            const [reps] = await conn.query(`SELECT * FROM qc_reports WHERE id = ? AND deleted_at IS NULL`, [id]);
            if (!reps.length) return { notFound: 'report' };
            const report = reps[0];

            const [ords] = await conn.query(
                `SELECT id, jf_code, lot_number FROM orders WHERE id = ? AND deleted_at IS NULL`, [orderId]
            );
            if (!ords.length) return { notFound: 'order' };
            const order = ords[0];

            // Verdict/detail come from the request, else the unmatched item being confirmed.
            const item = body.item && typeof body.item === 'object' ? body.item : null;
            const verdict = qcResult || (item && (item.qcResult || item.qaStatus)) || 'unknown';
            const detailRaw = body.resultDetail ?? (item && (item.resultDetail ?? item.detail)) ?? null;
            const detail = detailRaw != null ? String(detailRaw).slice(0, 1000) : null;

            const [existing] = await conn.query(
                `SELECT id FROM order_qc_reports WHERE qc_report_id = ? AND order_id = ?`, [report.id, orderId]
            );
            // jf_code/lot on the link are the ORDER's real values; the verdict is
            // what the report found for that item.
            await conn.query(
                `INSERT INTO order_qc_reports (qc_report_id, order_id, jf_code, lot_number, qc_result, result_detail, match_method)
                 VALUES (?, ?, ?, ?, ?, ?, 'manual')
                 ON DUPLICATE KEY UPDATE jf_code=VALUES(jf_code), lot_number=VALUES(lot_number),
                    qc_result=VALUES(qc_result), result_detail=VALUES(result_detail), match_method='manual'`,
                [report.id, orderId, order.jf_code, order.lot_number, verdict, detail]
            );

            // Keep result_json consistent: drop the resolved item from unmatched,
            // record a manual entry under matched.
            const data = parseJsonColumn(report.result_json) || {};
            const matchedArr = Array.isArray(data.matched) ? data.matched : [];
            let unmatchedArr = Array.isArray(data.unmatched) ? data.unmatched : [];
            if (item) {
                const nrm = s => String(s == null ? '' : s).toUpperCase().replace(/[\s\-_.]/g, '');
                const nrmLot = s => nrm(s).replace(/^0+/, '');
                unmatchedArr = unmatchedArr.filter(u =>
                    !(nrm(u.jfCode) === nrm(item.jfCode) && nrmLot(u.lotNumber) === nrmLot(item.lotNumber)));
            }
            if (!matchedArr.some(m => Number(m.orderId) === orderId)) {
                matchedArr.push({ orderId, jfCode: order.jf_code, lotNumber: order.lot_number, qcResult: verdict, resultDetail: detail, matchMethod: 'manual' });
            }
            await conn.query(
                `UPDATE qc_reports SET matched_count = ?, result_json = ? WHERE id = ?`,
                [countMatchedItems(matchedArr), JSON.stringify({ ...data, matched: matchedArr, unmatched: unmatchedArr }), report.id]
            );

            await recordAudit(conn, {
                entityType: 'qc_report', entityId: report.id,
                action: existing.length ? 'qc_order_relinked' : 'qc_order_attached',
                before: null,
                after: { orderId, qcResult: verdict, jfCode: order.jf_code, lotNumber: order.lot_number, method: 'manual' },
                userEmail: req.userEmail,
            });

            const [freshRep] = await conn.query(`SELECT * FROM qc_reports WHERE id = ?`, [report.id]);
            const [links] = await conn.query(
                `SELECT l.order_id, l.jf_code, l.lot_number, l.qc_result, l.result_detail, l.match_method,
                        o.product_name, o.po_number, o.status AS order_status
                   FROM order_qc_reports l LEFT JOIN orders o ON o.id = l.order_id
                  WHERE l.qc_report_id = ? ORDER BY l.order_id`, [report.id]
            );
            return { report: freshRep[0], links };
        });

        if (out.notFound === 'report') return res.status(404).json({ error: `QC report ${id} not found.` });
        if (out.notFound === 'order') return res.status(404).json({ error: `Order ${orderId} not found.` });
        res.json(qcReportDetailJson(out.report, out.links));
    } catch (error) {
        log.error('[POST /qc-reports/:id/orders]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// DELETE /api/v1/qc-reports/:id/orders/:orderId — detach an order from a report
// (undo a manual attach, or remove a wrong auto-match). Returns refreshed report.
app.delete('/api/v1/qc-reports/:id/orders/:orderId', async (req, res) => {
    try {
        await qcReportsSchemaReady;
        const { id, orderId } = req.params;
        const oid = Number(orderId);
        const out = await withConnection(async (conn) => {
            const [reps] = await conn.query(`SELECT * FROM qc_reports WHERE id = ? AND deleted_at IS NULL`, [id]);
            if (!reps.length) return { notFound: true };
            const report = reps[0];
            const [del] = await conn.query(
                `DELETE FROM order_qc_reports WHERE qc_report_id = ? AND order_id = ?`, [report.id, oid]
            );
            if (!del.affectedRows) return { noLink: true };

            const data = parseJsonColumn(report.result_json) || {};
            if (Array.isArray(data.matched)) data.matched = data.matched.filter(m => Number(m.orderId) !== oid);
            await conn.query(`UPDATE qc_reports SET matched_count = ?, result_json = ? WHERE id = ?`,
                [countMatchedItems(data.matched), JSON.stringify(data), report.id]);

            await recordAudit(conn, {
                entityType: 'qc_report', entityId: report.id, action: 'qc_order_detached',
                before: { orderId: oid }, after: null, userEmail: req.userEmail,
            });

            const [freshRep] = await conn.query(`SELECT * FROM qc_reports WHERE id = ?`, [report.id]);
            const [links] = await conn.query(
                `SELECT l.order_id, l.jf_code, l.lot_number, l.qc_result, l.result_detail, l.match_method,
                        o.product_name, o.po_number, o.status AS order_status
                   FROM order_qc_reports l LEFT JOIN orders o ON o.id = l.order_id
                  WHERE l.qc_report_id = ? ORDER BY l.order_id`, [report.id]
            );
            return { report: freshRep[0], links };
        });
        if (out.notFound) return res.status(404).json({ error: `QC report ${id} not found.` });
        if (out.noLink) return res.status(404).json({ error: `Order ${orderId} is not attached to report ${id}.` });
        res.json(qcReportDetailJson(out.report, out.links));
    } catch (error) {
        log.error('[DELETE /qc-reports/:id/orders/:orderId]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Daily alerts (slide-out alert window) ──────────────────────────────────
// Populated overnight by the generateDailyAlerts Lambda. Each alert can be
// snoozed (re-nags in N days if still unresolved), dismissed ("never remind"),
// or restored; actions are attributed and `/alerts/history` lists past alerts
// with who actioned them. `/alerts/history` is a fixed sub-path and the only
// `/alerts/:id` routes are PATCH actions, so no GET route can shadow it.
app.get('/api/v1/alerts', async (req, res) => {
    try {
        await dailyAlertsSchemaReady;
        const { status, type, limit } = req.query;
        const data = await withConnection(conn => listAlerts(conn, { status, type, limit }));
        const pending = data.filter(a => a.status === 'pending').length;
        res.json({ data, counts: { returned: data.length, pending } });
    } catch (error) {
        log.error('[GET /alerts]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.get('/api/v1/alerts/history', async (req, res) => {
    try {
        await dailyAlertsSchemaReady;
        const { limit, days } = req.query;
        const data = await withConnection(conn => listHistory(conn, { limit, days }));
        res.json({ data });
    } catch (error) {
        log.error('[GET /alerts/history]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// One action endpoint per verb (all PATCH, all covered by the
// PATCH /api/v1/alerts/{proxy+} route in serverless.yml):
//   POST-less by design — these mutate an existing alert's lifecycle state.
//   • /snooze   body { days } (default 2) — hide until now+days, re-nags if still live
//   • /dismiss  — permanent "never remind"
//   • /restore  — undo snooze/dismiss, back to pending
const ALERT_ACTION_PATHS = { snooze: 'snooze', dismiss: 'dismiss', restore: 'restore' };
for (const action of Object.values(ALERT_ACTION_PATHS)) {
    app.patch(`/api/v1/alerts/:id/${action}`, async (req, res) => {
        try {
            await dailyAlertsSchemaReady;
            const id = Number(req.params.id);
            if (!Number.isInteger(id) || id <= 0) {
                return res.status(400).json({ error: 'A valid alert id is required.' });
            }
            const days = action === 'snooze' ? req.body?.days : undefined;
            const result = await withConnection(conn =>
                actOnAlert(conn, { id, userEmail: req.userEmail, action, days }));
            if (result.notFound) {
                return res.status(404).json({ error: `Alert ${id} not found.` });
            }
            res.json(result.alert);
        } catch (error) {
            log.error(`[PATCH /alerts/:id/${action}]`, error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });
}

// ── AI status suggestions: approve / deny ──────────────────────────────────
// The Front importer (frontStatusImport) queues type='status_suggestion' alerts
// proposing an order status change inferred from a supplier email. A human
// either APPROVES it — which applies the change to orders.status exactly like
// PATCH /orders/:id/status (audited, PO-sent webhook fired) and resolves the
// alert — or DENIES it, which dismisses it permanently. Both are covered by the
// existing PATCH /api/v1/alerts/{proxy+} route, so serverless.yml needs no change.
//
// No transition validation: per the product decision, the human approver is the
// control. The card shows current -> suggested; an operator may pass an explicit
// { status } in the body to correct the AI's guess before applying. The one
// hard guard is that we refuse to apply a (now-stale) suggestion to an order
// that has since reached a received/warehouse state — those are never live
// candidates for a supplier-email status change.
//
// Runs in a TRANSACTION with a row lock on the suggestion (getSuggestionForUpdate
// = SELECT ... FOR UPDATE) so two concurrent approvals can't both mutate the
// order: the second blocks on the lock, then sees the suggestion resolved and
// aborts. The PO-sent webhook (an external HTTP call) is fired AFTER commit so it
// never holds the lock.
// Already-received/terminal: a stale suggestion must not regress one of these
// (live DB vocabulary — see order-transitions.CANDIDATE_EXCLUDED_STATUSES).
const RECEIVED_TERMINAL_STATUSES = [...T.CANDIDATE_EXCLUDED_STATUSES];
// Statuses an approve may set. The legacy ALL_STATUSES enum predates the live
// pipeline (it has IN_WAREHOUSE/MINTSOFT — 0 rows — and lacks ARRIVED_AT_WAREHOUSE
// / DESTROYED, both of which the rules engine can legitimately target), so union
// it with the real pipeline statuses from order-transitions.
const APPROVE_VALID_STATUSES = new Set([...ALL_STATUSES, ...Object.keys(T.STATUS_LEVEL), 'PARTIALLY_RECEIVED']);
app.patch('/api/v1/alerts/:id/approve', async (req, res) => {
    try {
        await dailyAlertsSchemaReady;
        await auditLogSchemaReady;
        await poSentWebhooksSchemaReady;
        await shipmentsSchemaReady;
        const id = Number(req.params.id);
        if (!Number.isInteger(id) || id <= 0) {
            return res.status(400).json({ error: 'A valid alert id is required.' });
        }

        const result = await withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const alert = await getSuggestionForUpdate(conn, id);
                if (!alert) { await conn.rollback(); return { notFound: true }; }
                if (alert.status === 'dismissed' || alert.status === 'resolved') {
                    await conn.rollback();
                    return { alreadyActioned: true, alert };
                }
                const meta = alert.meta || {};
                const orderId = Number(meta.orderId);
                if (!Number.isInteger(orderId) || orderId <= 0) { await conn.rollback(); return { badMeta: 'orderId' }; }

                const [existing] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [orderId]);
                if (!existing.length) { await conn.rollback(); return { orderNotFound: orderId }; }
                const beforeOrder = rowToOrder(existing[0]);
                if (RECEIVED_TERMINAL_STATUSES.includes(beforeOrder.status)) {
                    await conn.rollback();
                    return { staleTerminal: beforeOrder.status, alert };
                }

                // Resolve the status this approve will set. A 'data_update' suggestion
                // carries NO status move: meta.suggestedStatus is just the order's status
                // frozen at email-processing time (front-status-import sets target =
                // order.status for a no-move). Applying it verbatim would silently
                // re-stamp an order whose status was changed by hand in the interim —
                // e.g. order 930 approved a field-only estimatedReadyDate update and
                // jumped IN_PRODUCTION -> READY_FOR_QC. Pin to the LIVE status; only move
                // for a genuine move suggestion or an explicit body.status override.
                const isDataUpdate = meta.category === 'data_update';
                const status = (req.body && req.body.status)
                    || (isDataUpdate ? beforeOrder.status : meta.suggestedStatus);
                if (!APPROVE_VALID_STATUSES.has(status)) { await conn.rollback(); return { badStatus: status }; }

                // Also apply the field values the email provided for this move
                // (meta.fieldUpdates), or an explicit body.fields override. These
                // are re-validated HERE against the current order (the world may
                // have changed since the suggestion): non-admin rules — mfg/exp
                // snap to the 1st, expiry can't be in the past, and a JF+lot can't
                // take a second expiry (§5). Rejected fields are skipped (the
                // status move still applies) and reported back.
                const proposed = {};
                if (req.body && req.body.fields && typeof req.body.fields === 'object') {
                    Object.assign(proposed, req.body.fields);
                } else if (Array.isArray(meta.fieldUpdates)) {
                    for (const u of meta.fieldUpdates) if (u && u.field) proposed[u.field] = u.to;
                }
                // Scope: only fields pertinent to THIS move/stage may be applied via
                // approve (gate + milestone + stage-data fields). A backward QC-failed
                // rework and a no-move data_update carry only data fields. Anything
                // else (e.g. editing supplier while moving to CONSOLIDATED) belongs on
                // the normal order-edit path, not approve.
                const fromStatus = beforeOrder.status;
                const isMove = status !== fromStatus;
                const isBackwardRework = fromStatus === 'READY_FOR_QC' && status === 'IN_PRODUCTION';
                const gateScope = (isMove && !isBackwardRework) ? T.applicableFieldsFor(status) : [];
                const allowedFields = new Set([...gateScope, ...T.applicableDataFields(status)]);

                const today = londonToday();
                const fieldsToSet = {};
                const appliedFields = [];
                const skippedFields = [];
                let pendingExp = null;
                for (const [field, rawVal] of Object.entries(proposed)) {
                    const col = UPDATABLE_FIELDS[field];
                    if (!col) { skippedFields.push({ field, reason: 'not_updatable' }); continue; }
                    if (!allowedFields.has(field)) { skippedFields.push({ field, reason: 'not_applicable_here' }); continue; }
                    if (rawVal == null || rawVal === '') continue;
                    let val = rawVal;
                    if (field === 'mfgDate' || field === 'expDate') val = T.snapMonthStart(val);
                    if (field === 'expDate') {
                        if (String(val).slice(0, 10) < today) { skippedFields.push({ field, reason: 'expiry_in_past' }); continue; }
                        pendingExp = String(val).slice(0, 10);
                    }
                    fieldsToSet[col] = val;
                    appliedFields.push({ field, value: val });
                }
                // Lot↔expiry uniqueness (§5): re-check when EITHER the lot OR the
                // expiry would change (a lot change can collide an unchanged expiry).
                const lotChanged = fieldsToSet.lot_number !== undefined;
                if (pendingExp != null || lotChanged) {
                    const lot = lotChanged ? fieldsToSet.lot_number : existing[0].lot_number;
                    const exp = pendingExp != null ? pendingExp
                        : (existing[0].exp_date ? String(existing[0].exp_date).slice(0, 10) : null);
                    if (lot && exp) {
                        const [conf] = await conn.query(
                            `SELECT 1 FROM orders WHERE deleted_at IS NULL AND id <> ?
                                AND UPPER(TRIM(jf_code)) = UPPER(TRIM(?)) AND UPPER(TRIM(lot_number)) = UPPER(TRIM(?))
                                AND exp_date IS NOT NULL AND DATE_FORMAT(exp_date, '%Y-%m-%d') <> ? LIMIT 1`,
                            [orderId, existing[0].jf_code, lot, exp]
                        );
                        if (conf.length) {
                            const dropField = pendingExp != null ? 'expDate' : 'lotNumber';
                            const dropCol = pendingExp != null ? 'exp_date' : 'lot_number';
                            delete fieldsToSet[dropCol];
                            const i = appliedFields.findIndex(a => a.field === dropField);
                            if (i >= 0) appliedFields.splice(i, 1);
                            skippedFields.push({ field: dropField, reason: 'lot_expiry_conflict' });
                        }
                    }
                }
                // QC-report HARD gate (§3a): a forward move to READY needs an attached
                // inspection report — un-fillable by a form field, and skipping it
                // leaves a genuinely invalid order. Re-checked here in case the report
                // was removed since the suggestion. Soft field gates stay "human
                // decides" (consistent with the permissive PATCH /orders/:id/status).
                if (isMove && status === 'READY') {
                    const [[qr]] = await conn.query(`SELECT EXISTS(SELECT 1 FROM order_qc_reports WHERE order_id = ?) AS h`, [orderId]);
                    if (!qr || !qr.h) {
                        await conn.rollback();
                        return { gateBlocked: ['qcReport(attachment)'], alert };
                    }
                }
                // Ready-date HARD gate: an order can't move into READY_FOR_QC while its
                // estimated ready date is still in the future — the goods aren't finished,
                // so there's nothing to inspect yet. Use the value being applied by this
                // approve (fieldsToSet) if present, else the order's current one.
                if (isMove && status === 'READY_FOR_QC') {
                    const erd = ('estimated_ready_date' in fieldsToSet)
                        ? String(fieldsToSet.estimated_ready_date).slice(0, 10)
                        : beforeOrder.estimatedReadyDate;
                    if (erd && erd > today) {
                        await conn.rollback();
                        return { readyDateFuture: erd, alert };
                    }
                }

                // Same status-update transaction as PATCH /orders/:id/status, plus
                // any validated field columns. A data_update suggestion sets status
                // to the CURRENT status (no move) — don't re-stamp its date key.
                const dates = parseDates(existing[0].dates);
                if (status !== beforeOrder.status) setDateKey(dates, status);
                const extraCols = Object.keys(fieldsToSet);
                const setClause = ['status = ?', 'dates = ?', ...extraCols.map(c => `${c} = ?`)].join(', ');
                await conn.query(
                    `UPDATE orders SET ${setClause} WHERE id = ?`,
                    [status, JSON.stringify(dates), ...extraCols.map(c => fieldsToSet[c]), orderId]
                );
                const [rows] = await conn.query(`${ORDER_SELECT} AND orders.id = ?`, [orderId]);
                const updated = rowToOrder(rows[0]);
                await recordAudit(conn, {
                    entityType: 'order', entityId: updated.id, action: 'update',
                    before: beforeOrder, after: updated, userEmail: req.userEmail,
                });
                // Resolve the suggestion + audit who approved it (row already locked above).
                const acted = await actOnSuggestion(conn, {
                    id, userEmail: req.userEmail, action: 'approve', note: req.body && req.body.note,
                });
                const linked = await shipmentSync.shadow(conn, {
                    site: 'PATCH /alerts/:id/approve', inTx: true, ...membershipKey([updated]),
                }, c => shipmentSync.syncOrderMembership(c, [orderId], { userEmail: req.userEmail }));
                await conn.commit();
                return { order: updated, alert: acted.alert, appliedStatus: status, appliedFields, skippedFields, linked };
            } catch (e) {
                await conn.rollback();
                throw e;
            }
        });

        if (result.notFound) return res.status(404).json({ error: `Status suggestion ${id} not found.` });
        if (result.alreadyActioned) return res.status(409).json({ error: 'Suggestion already actioned.', alert: result.alert });
        if (result.badMeta) return res.status(422).json({ error: `Suggestion is missing ${result.badMeta}.` });
        if (result.badStatus) return res.status(400).json({ error: `Invalid status '${result.badStatus}'.` });
        if (result.orderNotFound) return res.status(404).json({ error: `Order ${result.orderNotFound} not found.` });
        if (result.staleTerminal) {
            return res.status(409).json({ error: `Order already ${result.staleTerminal}; suggestion is stale.`, alert: result.alert });
        }
        if (result.gateBlocked) {
            return res.status(422).json({ error: 'Cannot move to READY: a QC inspection report must be attached first.', missing: result.gateBlocked, alert: result.alert });
        }
        if (result.readyDateFuture) {
            return res.status(422).json({ error: `Cannot move to READY_FOR_QC: estimated ready date (${result.readyDateFuture}) is in the future.`, alert: result.alert });
        }

        // Webhook fires outside the lock window, only if the approve committed.
        await withConnection(conn => firePoSentWebhookIfNeeded(conn, result.order));
        patchShipmentIds([result.order], result.linked);
        res.json({
            order: result.order, alert: result.alert, appliedStatus: result.appliedStatus,
            appliedFields: result.appliedFields || [], skippedFields: result.skippedFields || [],
        });
    } catch (error) {
        log.error('[PATCH /alerts/:id/approve]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

app.patch('/api/v1/alerts/:id/deny', async (req, res) => {
    try {
        await dailyAlertsSchemaReady;
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        if (!Number.isInteger(id) || id <= 0) {
            return res.status(400).json({ error: 'A valid alert id is required.' });
        }
        const note = req.body && req.body.note;
        const result = await withConnection(conn => actOnSuggestion(conn, { id, userEmail: req.userEmail, action: 'deny', note }));
        if (result.notFound) return res.status(404).json({ error: `Status suggestion ${id} not found.` });
        if (result.alreadyActioned) return res.status(409).json({ error: 'Suggestion already actioned.', alert: result.alert });
        res.json(result.alert);
    } catch (error) {
        log.error('[PATCH /alerts/:id/deny]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// Acknowledge: the operator has SEEN the suggestion and handled it their own way
// (e.g. updated the order manually, or it's already actioned elsewhere) — so it
// leaves the queue WITHOUT applying the AI's status change. The { note } records
// what they did instead and is shown in /alerts/history alongside who did it.
app.patch('/api/v1/alerts/:id/acknowledge', async (req, res) => {
    try {
        await dailyAlertsSchemaReady;
        await auditLogSchemaReady;
        const id = Number(req.params.id);
        if (!Number.isInteger(id) || id <= 0) {
            return res.status(400).json({ error: 'A valid alert id is required.' });
        }
        const note = req.body && req.body.note;
        const result = await withConnection(conn => actOnSuggestion(conn, { id, userEmail: req.userEmail, action: 'acknowledge', note }));
        if (result.notFound) return res.status(404).json({ error: `Status suggestion ${id} not found.` });
        if (result.alreadyActioned) return res.status(409).json({ error: 'Suggestion already actioned.', alert: result.alert });
        res.json(result.alert);
    } catch (error) {
        log.error('[PATCH /alerts/:id/acknowledge]', error);
        res.status(500).json({ error: 'An internal error occurred.' });
    }
});

// ── Shipments (/api/v1/shipments) ────────────────────────────────────────
// Registered last: the handlers close over UPDATABLE_FIELDS, ORDER_INSERT_COLS
// and s3, which only exist once the module has evaluated this far (registering
// beside registerUserRoutes would hit their temporal dead zone). No other route
// overlaps /api/v1/shipments, so the position is otherwise irrelevant.
registerShipmentRoutes(app, {
    withConnection,
    ready: async () => {
        await shipmentsSchemaReady;
        await draftRegistryReady;
        await auditLogSchemaReady;
        await plannedContainerAllocationsSchemaReady;
        await qualityAssuranceSchemaReady;
        await purchaseOrdersSchemaReady;
    },
    ORDER_SELECT, rowToOrder, parseDates, setDateKey, recordAudit, recordPoAttachmentChange, draftAudit,
    splitOrder: makeSplitOrder({ ORDER_INSERT_COLS, ORDER_INSERT_PLACEHOLDERS, orderInsertValues, setDateKey, parseDates }),
    statusLevel: T.statusLevel,
    isFqcOrder: T.isFqcOrder,
    generateDraftDocumentSet,
    publicS3Url,
    poBucket: () => PO_BUCKET,
    rowToContainer,
    rowToAirShipment,
    log,
});

// ── Serverless export ────────────────────────────────────────────────────
const serverlessApp = serverless(app, {
    binary: ['application/json', 'image/*', 'application/javascript'],
    // Expose the Lambda event/context to express route handlers as
    // req.lambdaEvent / req.lambdaContext. Routes that intentionally do
    // fire-and-forget work after responding can flip
    // req.lambdaContext.callbackWaitsForEmptyEventLoop back to true so
    // Lambda keeps the container alive until the background promise
    // settles instead of freezing it on response.
    request: (req, event, context) => {
        req.lambdaEvent = event;
        req.lambdaContext = context;
    },
});

// The bare Express app, so local test scripts (tools/test-*.js) can drive the
// routes in-process instead of through a Lambda event.
module.exports.app = app;

module.exports.handler = async (event, context) => {
    // Default: freeze the container as soon as the HTTP response is built.
    // Routes can opt out per-invocation by setting this back to true.
    context.callbackWaitsForEmptyEventLoop = false;
    return await serverlessApp(event, context);
};

if (require.main === module) {
    const PORT = process.env.ORDERS_PORT || 3001;
    app.listen(PORT, () => {
        log.info(`Orders API running on http://localhost:${PORT}`);
    });
}
