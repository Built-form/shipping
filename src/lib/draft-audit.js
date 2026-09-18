'use strict';

// Draft-container audit trail.
//
// Drafts are implicit — a draft is "the allocation rows sharing a name" — so
// audit_log's integer entity_id can't point at one directly, and a rename or a
// delete would make the history unreachable. The `draft_containers` registry
// gives every name that ever existed a stable integer id; every draft event
// (line added / changed / removed, document generated / sent, rename,
// conversion into a real container, delete) is written to audit_log as
// entity_type 'draft_container' against that id. Renames keep the id, and the
// registry row is never deleted, so a draft's history survives any change to
// it — including the draft itself disappearing.
//
// Event vocabulary (audit_log.action):
//   create, line_added, line_updated, line_removed, renamed,
//   document_generated, document_sent, qa_document_generated, qa_document_sent,
//   converted, deleted, reopened
// Every payload carries `draftName` so a row reads on its own in the global
// feed. Backfilled rows (reconstructed from existing tables at first deploy)
// carry `backfilled: true`.
//
// Every writer here swallows its own errors the same way recordAudit does: a
// missing audit row must never fail the mutation it describes.

const log = require('./logger');

const ENTITY_TYPE = 'draft_container';
const CLOSE_REASONS = new Set(['converted', 'deleted']);
const BACKFILL_MARKER = 'draft_container_audit_backfill_v1';

const DRAFT_LINE_SELECT = `
    SELECT dca.id, dca.order_id, dca.allocated,
           orders.jf_code, orders.asin, orders.product_name, orders.supplier, orders.po_number,
           orders.quantity AS order_quantity
      FROM draft_container_allocations dca
      LEFT JOIN orders ON orders.id = dca.order_id
`;

// Registry row + live counts. `line_count` is what makes 'open' vs 'empty' —
// an open registry row whose lines were all removed one at a time (the
// pre-registry delete path) has nothing left to show but its history.
//
// Lines are counted the way the Draft tab lists them (DRAFT_ALLOC_SELECT in
// orders.js): INNER JOIN to a live order. An allocation left behind under a
// deleted order is invisible in the UI — the old delete path could only remove
// the rows it could see — so counting it kept drafts "open" here that the
// Draft tab no longer showed (9 vs 7 on prod, 2026-09-17).
const DRAFT_REGISTRY_SELECT = `
    SELECT dc.id, dc.name, dc.created_by_email, dc.created_at, dc.last_activity_at,
           dc.closed_reason, dc.closed_at, dc.closed_by_email, dc.container_number,
           (SELECT COUNT(*) FROM draft_container_allocations a
             INNER JOIN orders o ON o.id = a.order_id AND o.deleted_at IS NULL
             WHERE a.draft_container_name = dc.name) AS line_count,
           (SELECT COALESCE(SUM(a.allocated), 0) FROM draft_container_allocations a
             INNER JOIN orders o ON o.id = a.order_id AND o.deleted_at IS NULL
             WHERE a.draft_container_name = dc.name) AS total_units,
           (SELECT COUNT(*) FROM draft_container_documents d WHERE d.draft_container_name = dc.name AND d.deleted_at IS NULL) AS document_count,
           (SELECT COUNT(*) FROM quality_assurance_documents q WHERE q.draft_container_name = dc.name AND q.deleted_at IS NULL) AS qa_document_count,
           (SELECT COUNT(*) FROM audit_log al WHERE al.entity_type = '${ENTITY_TYPE}' AND al.entity_id = dc.id) AS event_count
      FROM draft_containers dc
`;

function iso(v) {
    if (v == null) return null;
    return typeof v.toISOString === 'function' ? v.toISOString() : v;
}

function num(v) {
    if (v == null || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

async function recordDraftAudit(conn, { draftId, action, before = null, after = null, userEmail = null, createdAt = null }) {
    try {
        const beforeJson = before ? JSON.stringify(before) : null;
        const afterJson = after ? JSON.stringify(after) : null;
        if (createdAt) {
            await conn.query(
                `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [ENTITY_TYPE, draftId, action, beforeJson, afterJson, userEmail || null, createdAt]
            );
        } else {
            await conn.query(
                `INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [ENTITY_TYPE, draftId, action, beforeJson, afterJson, userEmail || null]
            );
        }
    } catch (err) {
        log.warn('[audit] draft insert failed', { draftId, action, error: err.message });
    }
}

async function findDraft(conn, name) {
    const [rows] = await conn.query(
        `SELECT id, name, closed_reason, container_number FROM draft_containers WHERE name = ?`,
        [name]
    );
    return rows[0] || null;
}

// Give `name` a registry row (creating it — and its "create" event — on first
// sight), bump last_activity_at, and revive a closed row that is seeing new
// activity. Returns { id, name, created, reopened }.
async function ensureDraftRegistered(conn, name, userEmail) {
    let row = await findDraft(conn, name);
    let created = false;
    if (!row) {
        try {
            await conn.query(
                `INSERT INTO draft_containers (name, created_by_email) VALUES (?, ?)`,
                [name, userEmail || null]
            );
            created = true;
        } catch (e) {
            // Lost a race with another request registering the same name.
            if (e.code !== 'ER_DUP_ENTRY') throw e;
        }
        row = await findDraft(conn, name);
        if (!row) throw new Error(`draft_containers row missing for "${name}"`);
    }

    let reopened = false;
    if (created) {
        await recordDraftAudit(conn, { draftId: row.id, action: 'create', after: { draftName: row.name }, userEmail });
    } else {
        await conn.query(`UPDATE draft_containers SET last_activity_at = CURRENT_TIMESTAMP WHERE id = ?`, [row.id]);
        if (row.closed_reason) {
            reopened = true;
            await conn.query(
                `UPDATE draft_containers
                    SET closed_reason = NULL, closed_at = NULL, closed_by_email = NULL, container_number = NULL
                  WHERE id = ?`,
                [row.id]
            );
            await recordDraftAudit(conn, {
                draftId: row.id, action: 'reopened',
                before: { draftName: row.name, closedReason: row.closed_reason, containerNumber: row.container_number || null },
                after: { draftName: row.name },
                userEmail,
            });
        }
    }
    return { id: row.id, name: row.name, created, reopened };
}

// Event payload for one allocation line, from a DRAFT_LINE_SELECT-shaped row
// (the orders.js DRAFT_ALLOC_SELECT rows carry the same columns).
function lineSnapshot(row) {
    return {
        allocationId: row.id,
        orderId: row.order_id,
        allocated: num(row.allocated) ?? 0,
        jfCode: row.jf_code || null,
        asin: row.asin || null,
        productName: row.product_name || null,
        supplier: row.supplier || null,
        poNumber: row.po_number || null,
        orderQuantity: num(row.order_quantity),
    };
}

async function loadDraftLines(conn, name) {
    const [rows] = await conn.query(`${DRAFT_LINE_SELECT} WHERE dca.draft_container_name = ? ORDER BY dca.id ASC`, [name]);
    return rows.map(lineSnapshot);
}

async function countAllocations(conn, name) {
    const [rows] = await conn.query(
        `SELECT COUNT(*) AS n FROM draft_container_allocations WHERE draft_container_name = ?`,
        [name]
    );
    return Number(rows[0] && rows[0].n) || 0;
}

// Rename a draft everywhere it is keyed by name — allocations, quote/forwarder
// documents, QA documents — under its existing registry id, and record it.
// The caller owns the transaction. Returns one of:
//   { invalid } | { unchanged } | { notFound } | { conflict }
//   | { id, name, allocations, documents, qaDocuments }
async function renameDraft(conn, { from, to, userEmail }) {
    const src = typeof from === 'string' ? from.trim() : '';
    const dst = typeof to === 'string' ? to.trim() : '';
    if (!src || !dst) return { invalid: true };
    if (src === dst) return { unchanged: true };

    const srcRow = await findDraft(conn, src);
    if (!srcRow && (await countAllocations(conn, src)) === 0) return { notFound: true };
    const reg = await ensureDraftRegistered(conn, src, userEmail);

    // A case-only rename resolves to the same registry row (the name key is
    // case-insensitive) and must go through; any other holder of the target
    // name — registered or just allocations — is a conflict.
    const dstRow = await findDraft(conn, dst);
    if (dstRow && dstRow.id !== reg.id) return { conflict: true };
    if (!dstRow && (await countAllocations(conn, dst)) > 0) return { conflict: true };

    const [a] = await conn.query(
        `UPDATE draft_container_allocations SET draft_container_name = ? WHERE draft_container_name = ?`,
        [dst, src]
    );
    const [d] = await conn.query(
        `UPDATE draft_container_documents SET draft_container_name = ? WHERE draft_container_name = ?`,
        [dst, src]
    );
    const [q] = await conn.query(
        `UPDATE quality_assurance_documents SET draft_container_name = ? WHERE draft_container_name = ?`,
        [dst, src]
    );
    await conn.query(
        `UPDATE draft_containers SET name = ?, last_activity_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [dst, reg.id]
    );
    const result = {
        id: reg.id,
        name: dst,
        allocations: a.affectedRows || 0,
        documents: d.affectedRows || 0,
        qaDocuments: q.affectedRows || 0,
    };
    await recordDraftAudit(conn, {
        draftId: reg.id, action: 'renamed',
        before: { draftName: src },
        after: { draftName: dst, allocations: result.allocations, documents: result.documents, qaDocuments: result.qaDocuments },
        userEmail,
    });
    return result;
}

// Close a draft: snapshot its lines, delete every allocation in one statement,
// mark the registry row and record why — 'converted' (with the real
// container's details in `details`) or 'deleted'. The caller owns the
// transaction. Returns one of:
//   { invalid } | { notFound } | { id, name, deleted: 0, alreadyClosed }
//   | { id, name, deleted, lines }
async function closeDraft(conn, { name, reason, userEmail, details }) {
    const draftName = typeof name === 'string' ? name.trim() : '';
    if (!draftName || !CLOSE_REASONS.has(reason)) return { invalid: true };

    const existing = await findDraft(conn, draftName);
    const lines = await loadDraftLines(conn, draftName);
    if (!existing && lines.length === 0) return { notFound: true };
    // A retry of a close that already landed: nothing to delete, nothing to say.
    if (existing && existing.closed_reason && lines.length === 0) {
        return { id: existing.id, name: existing.name, deleted: 0, alreadyClosed: true };
    }

    const reg = await ensureDraftRegistered(conn, draftName, userEmail);
    const [del] = await conn.query(
        `DELETE FROM draft_container_allocations WHERE draft_container_name = ?`,
        [draftName]
    );
    const containerNumber = reason === 'converted' && details && details.containerNumber
        ? String(details.containerNumber).trim().slice(0, 100)
        : null;
    await conn.query(
        `UPDATE draft_containers
            SET closed_reason = ?, closed_at = NOW(), closed_by_email = ?, container_number = ?,
                last_activity_at = CURRENT_TIMESTAMP
          WHERE id = ?`,
        [reason, userEmail || null, containerNumber, reg.id]
    );

    const totalUnits = lines.reduce((s, l) => s + (l.allocated || 0), 0);
    const before = { draftName, lineCount: lines.length, totalUnits, lines };
    const after = reason === 'converted'
        ? { draftName, ...sanitizeConversionDetails(details), containerNumber }
        : null;
    await recordDraftAudit(conn, { draftId: reg.id, action: reason, before, after, userEmail });

    return { id: reg.id, name: reg.name, deleted: del.affectedRows || 0, lines };
}

// Only the fields the conversion form carries — a caller can't smuggle
// arbitrary JSON into the audit row.
function sanitizeConversionDetails(details) {
    const d = details && typeof details === 'object' ? details : {};
    const str = v => (v == null ? null : String(v).trim().slice(0, 200) || null);
    const out = {
        externalContainerNumber: str(d.externalContainerNumber),
        vesselName: str(d.vesselName),
        eta: str(d.eta),
        etd: str(d.etd),
        freightType: str(d.freightType),
        port: str(d.port),
        awbNumber: str(d.awbNumber),
    };
    if (Array.isArray(d.packs)) {
        out.packs = d.packs
            .filter(p => p && typeof p === 'object')
            .slice(0, 500)
            .map(p => ({ orderId: num(p.orderId), qty: num(p.qty) }));
    }
    return out;
}

// API shape of a DRAFT_REGISTRY_SELECT row.
function rowToDraftRecord(row) {
    const lineCount = num(row.line_count) || 0;
    const status = row.closed_reason || (lineCount > 0 ? 'open' : 'empty');
    return {
        id: row.id,
        name: row.name,
        status,
        createdAt: iso(row.created_at),
        createdByEmail: row.created_by_email || null,
        lastActivityAt: iso(row.last_activity_at),
        closedAt: iso(row.closed_at),
        closedByEmail: row.closed_by_email || null,
        closedReason: row.closed_reason || null,
        containerNumber: row.container_number || null,
        lineCount,
        totalUnits: num(row.total_units) || 0,
        documentCount: num(row.document_count) || 0,
        qaDocumentCount: num(row.qa_document_count) || 0,
        eventCount: num(row.event_count) || 0,
    };
}

// Registry listing. `name` = exact lookup; `q` = substring on name / container
// number; `status` = open | empty | converted | deleted | closed (either).
async function listDraftRecords(conn, { name, q, status, limit } = {}) {
    const where = [];
    const params = [];
    if (name) { where.push('dc.name = ?'); params.push(String(name).trim()); }
    if (q) {
        const like = `%${String(q).trim()}%`;
        where.push('(dc.name LIKE ? OR dc.container_number LIKE ?)');
        params.push(like, like);
    }
    if (status === 'converted' || status === 'deleted') { where.push('dc.closed_reason = ?'); params.push(status); }
    else if (status === 'closed') where.push('dc.closed_reason IS NOT NULL');
    else if (status === 'open' || status === 'empty') where.push('dc.closed_reason IS NULL');

    let having = '';
    if (status === 'open') having = 'HAVING line_count > 0';
    else if (status === 'empty') having = 'HAVING line_count = 0';

    let lim = parseInt(limit, 10);
    if (!Number.isFinite(lim) || lim <= 0) lim = 500;
    if (lim > 2000) lim = 2000;

    const [rows] = await conn.query(
        `${DRAFT_REGISTRY_SELECT}
         ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
         ${having}
         ORDER BY dc.last_activity_at DESC, dc.id DESC
         LIMIT ${lim}`,
        params
    );
    return rows.map(rowToDraftRecord);
}

// One-time reconstruction at first deploy. Registers every name that has ever
// left a trace (allocations, quote/forwarder documents, QA documents) and
// writes the events those tables already imply — line_added per allocation,
// document_generated / document_sent per document and send — stamped with
// their original timestamps and marked `backfilled`. Guarded by a marker row
// in app_migrations claimed inside the same transaction, so concurrent cold
// starts run it once and a failure releases the claim for the next start.
async function backfillDraftRegistry(conn) {
    await conn.query('START TRANSACTION');
    try {
        const [claim] = await conn.query(`INSERT IGNORE INTO app_migrations (name) VALUES (?)`, [BACKFILL_MARKER]);
        if (!claim.affectedRows) {
            await conn.query('ROLLBACK');
            return { ran: false };
        }

        await conn.query(`
            INSERT IGNORE INTO draft_containers (name, created_at, last_activity_at)
            SELECT u.name, MIN(u.first_at), MAX(u.last_at)
              FROM (
                    SELECT draft_container_name AS name, created_at AS first_at, updated_at AS last_at
                      FROM draft_container_allocations
                    UNION ALL
                    SELECT draft_container_name, generated_at, generated_at
                      FROM draft_container_documents
                    UNION ALL
                    SELECT draft_container_name, generated_at, generated_at
                      FROM quality_assurance_documents
                     WHERE draft_container_name IS NOT NULL
                   ) u
             WHERE u.name IS NOT NULL AND u.name <> ''
             GROUP BY u.name
        `);

        // Registry rows that exist only because of this backfill get a
        // "create" at their reconstructed created_at.
        await conn.query(`
            INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
            SELECT '${ENTITY_TYPE}', dc.id, 'create', NULL,
                   JSON_OBJECT('draftName', dc.name, 'backfilled', TRUE), NULL, dc.created_at
              FROM draft_containers dc
             WHERE NOT EXISTS (
                   SELECT 1 FROM audit_log al
                    WHERE al.entity_type = '${ENTITY_TYPE}' AND al.entity_id = dc.id AND al.action = 'create')
        `);

        await conn.query(`
            INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
            SELECT '${ENTITY_TYPE}', dc.id, 'line_added', NULL,
                   JSON_OBJECT('draftName', dc.name, 'allocationId', a.id, 'orderId', a.order_id,
                               'allocated', a.allocated, 'jfCode', o.jf_code, 'asin', o.asin,
                               'productName', o.product_name, 'supplier', o.supplier, 'poNumber', o.po_number,
                               'orderQuantity', o.quantity, 'backfilled', TRUE),
                   NULL, a.created_at
              FROM draft_container_allocations a
              JOIN draft_containers dc ON dc.name = a.draft_container_name
              LEFT JOIN orders o ON o.id = a.order_id
        `);

        await conn.query(`
            INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
            SELECT '${ENTITY_TYPE}', dc.id, 'document_generated', NULL,
                   JSON_OBJECT('draftName', dc.name, 'documentId', d.id, 'type', COALESCE(d.type, 'quote'),
                               'supplier', d.supplier, 'version', d.version, 'url', d.public_url,
                               'csvUrl', d.csv_public_url, 'batchId', d.batch_id, 'backfilled', TRUE),
                   d.generated_by_email, d.generated_at
              FROM draft_container_documents d
              JOIN draft_containers dc ON dc.name = d.draft_container_name
        `);

        // One email = one event. A split forwarder run records a send row per
        // attached PDF; the combined PDF's row (supplier NULL) stands for the
        // whole email, so per-supplier copies inside a batch are skipped.
        await conn.query(`
            INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
            SELECT '${ENTITY_TYPE}', dc.id, 'document_sent', NULL,
                   JSON_OBJECT('draftName', dc.name, 'documentId', d.id, 'type', COALESCE(d.type, 'quote'),
                               'supplier', d.supplier, 'version', d.version, 'sendId', s.id,
                               'sentTo', s.sent_to, 'subject', s.subject,
                               'frontMessageUid', s.front_message_uid, 'frontConversationId', s.front_conversation_id,
                               'backfilled', TRUE),
                   s.sent_by_email, s.sent_at
              FROM draft_container_document_sends s
              JOIN draft_container_documents d ON d.id = s.draft_container_document_id
              JOIN draft_containers dc ON dc.name = d.draft_container_name
             WHERE d.batch_id IS NULL OR d.supplier IS NULL
        `);

        await conn.query(`
            INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
            SELECT '${ENTITY_TYPE}', dc.id, 'qa_document_generated', NULL,
                   JSON_OBJECT('draftName', dc.name, 'documentId', q.id, 'ref', q.ref, 'version', q.version,
                               'orderIds', q.order_ids, 'url', q.public_url, 'csvUrl', q.csv_public_url,
                               'backfilled', TRUE),
                   q.generated_by_email, q.generated_at
              FROM quality_assurance_documents q
              JOIN draft_containers dc ON dc.name = q.draft_container_name
        `);

        await conn.query(`
            INSERT INTO audit_log (entity_type, entity_id, action, before_json, after_json, user_email, created_at)
            SELECT '${ENTITY_TYPE}', dc.id, 'qa_document_sent', NULL,
                   JSON_OBJECT('draftName', dc.name, 'documentId', q.id, 'ref', q.ref, 'version', q.version,
                               'sendId', s.id, 'sentTo', s.sent_to, 'subject', s.subject,
                               'frontMessageUid', s.front_message_uid, 'frontConversationId', s.front_conversation_id,
                               'backfilled', TRUE),
                   s.sent_by_email, s.sent_at
              FROM quality_assurance_document_sends s
              JOIN quality_assurance_documents q ON q.id = s.quality_assurance_document_id
              JOIN draft_containers dc ON dc.name = q.draft_container_name
        `);

        await conn.query('COMMIT');
        return { ran: true };
    } catch (err) {
        try { await conn.query('ROLLBACK'); } catch (_) { /* connection already gone */ }
        throw err;
    }
}

module.exports = {
    ENTITY_TYPE,
    BACKFILL_MARKER,
    DRAFT_REGISTRY_SELECT,
    recordDraftAudit,
    findDraft,
    ensureDraftRegistered,
    lineSnapshot,
    loadDraftLines,
    renameDraft,
    closeDraft,
    rowToDraftRecord,
    listDraftRecords,
    backfillDraftRegistry,
};
