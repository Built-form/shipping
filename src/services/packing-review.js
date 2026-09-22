'use strict';

// Packing review — what a container's packing lists say, taken together, and
// the human decisions laid over that.
//
// One supplier may send several packing lists for one container (one per
// invoice), and may send a corrected version of one. So the unit of
// comparison is not a document but the SUPPLIER'S CHECK: every active
// document for that supplier in the container, pooled, against the
// supplier's lines in the container (packing-list-check.js). Active means
// not deleted and not superseded by a newer version.
//
// On top of the check sit two kinds of decision, both kept as rows, both
// audited, neither ever inferred:
//   sign-off  — "this line's difference is fine" (per supplier, per line key).
//               It holds only while the line is exactly as it was signed (its
//               fingerprint); a line that changes afterwards is outstanding
//               again and shows the old sign-off as stale.
//   approval  — "this container is packed to our satisfaction", whatever the
//               checks say. It carries a snapshot of the checks at the time,
//               so a later change to the goods (a quantity, a lot, a new
//               document) shows as "approved, but changed since".
//
// Everything here reads the lines as they are NOW: edit an order or an
// allocation and the next read reflects it. Nothing is cached.

const crypto = require('crypto');
const S = require('../lib/shipments');
const P = require('./packing-list-check');
const { recordAudit } = require('../lib/audit');

const iso = v => v?.toISOString?.() ?? v ?? null;
const parseJson = v => {
    if (v == null) return null;
    if (typeof v === 'object') return v;
    try { return JSON.parse(v); } catch { return null; }
};

// ── Rows → JSON ──────────────────────────────────────────────────────────
function packingListToJson(r) {
    return {
        id: r.id,
        // As uploaded. A draft's lines are followed live (renames, booking).
        containerKind: r.container_kind,
        containerNumber: r.container_number || null,
        draftContainerId: r.draft_container_id != null ? Number(r.draft_container_id) : null,
        containerName: r.container_name || null,
        supplierKey: r.supplier_key,
        supplierName: r.supplier_name || null,
        filename: r.filename,
        downloadPath: `/api/v1/packing-lists/${r.id}/download`,
        contentType: r.content_type || null,
        fileSize: r.file_size != null ? Number(r.file_size) : null,
        status: r.status,
        // The document ALONE, as read (the live check pools every document).
        verdict: r.verdict || null,
        discrepancyCount: r.discrepancy_count != null ? Number(r.discrepancy_count) : null,
        rowCount: r.row_count != null ? Number(r.row_count) : null,
        invoiceNumber: r.invoice_number || null,
        documentDate: r.document_date || null,
        supplierPrinted: r.supplier_printed || null,
        modelUsed: r.model_used || null,
        error: r.error_message || null,
        // Versions: a corrected upload replaces its predecessor.
        version: r.version != null ? Number(r.version) : 1,
        replacesId: r.replaces_id != null ? Number(r.replaces_id) : null,
        supersededById: r.superseded_by_id != null ? Number(r.superseded_by_id) : null,
        supersededAt: iso(r.superseded_at),
        uploadedByEmail: r.uploaded_by_email || null,
        createdAt: iso(r.created_at),
        analyzedAt: iso(r.analyzed_at),
    };
}

function signOffToJson(r) {
    return {
        id: r.id,
        supplierKey: r.supplier_key,
        lineKey: r.line_key,
        jfCode: r.jf_code || null,
        poNumber: r.po_number || null,
        lineStatus: r.line_status || null,
        fingerprint: r.fingerprint,
        differences: parseJson(r.differences_json) || [],
        note: r.note || null,
        signedByEmail: r.signed_by_email || null,
        signedAt: iso(r.signed_at),
    };
}

function approvalToJson(r) {
    return {
        id: r.id,
        containerKind: r.container_kind,
        containerNumber: r.container_number || null,
        draftContainerId: r.draft_container_id != null ? Number(r.draft_container_id) : null,
        containerName: r.container_name || null,
        note: r.note || null,
        approvedByEmail: r.approved_by_email || null,
        approvedAt: iso(r.approved_at),
        withdrawnAt: iso(r.withdrawn_at),
        withdrawnByEmail: r.withdrawn_by_email || null,
        withdrawnNote: r.withdrawn_note || null,
    };
}

// ── Container scope ──────────────────────────────────────────────────────
// Which stored rows (packing lists, sign-offs, approvals) belong to a
// container. A row keeps the container it was uploaded against; a draft's
// rows follow it once it is booked, so a booked container also owns the rows
// of every draft that became it.
async function draftsBehind(conn, bookedRef) {
    const ids = new Set();
    const names = new Set();
    const [conv] = await conn.query(
        `SELECT id, name FROM draft_containers WHERE closed_reason = 'converted' AND container_number = ?`, [bookedRef]
    );
    for (const r of conv) { ids.add(Number(r.id)); names.add(r.name); }
    // Drafts whose lines were packed into this container but never closed.
    const [alloc] = await conn.query(
        `SELECT DISTINCT dca.draft_container_name AS name
           FROM draft_container_allocations dca
           JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
          WHERE o.container_number = ?`,
        [bookedRef]
    );
    for (const r of alloc) names.add(r.name);
    // Drafts that reserved this number and have no open lines any more.
    const [reg] = await conn.query(`SELECT id, name FROM draft_containers WHERE name LIKE ?`, [`%${bookedRef}%`]);
    const candidates = reg.filter(r => S.parseNameHint(r.name)?.reference === bookedRef);
    if (candidates.length) {
        const [open] = await conn.query(
            `SELECT dca.draft_container_name AS name, COUNT(*) AS n
               FROM draft_container_allocations dca
               JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
              WHERE dca.draft_container_name IN (?) AND (o.container_number IS NULL OR TRIM(o.container_number) = '')
              GROUP BY dca.draft_container_name`,
            [candidates.map(r => r.name)]
        );
        const stillOpen = new Set(open.filter(r => Number(r.n) > 0).map(r => r.name));
        for (const r of candidates) if (!stillOpen.has(r.name)) { ids.add(Number(r.id)); names.add(r.name); }
    }
    if (names.size) {
        const [byName] = await conn.query(`SELECT id, name FROM draft_containers WHERE name IN (?)`, [[...names]]);
        for (const r of byName) ids.add(Number(r.id));
    }
    return { ids: [...ids], names: [...names] };
}

/** SQL for rows of table alias `t` that belong to `container`: { sql, params }. */
async function containerScope(conn, container) {
    if (container.kind === 'draft') {
        const ors = [];
        const params = [];
        if (container.draftId != null) { ors.push('t.draft_container_id = ?'); params.push(container.draftId); }
        ors.push(`(t.container_kind = 'draft' AND t.container_name = ?)`);
        params.push(container.draftName);
        return { sql: `(${ors.join(' OR ')})`, params };
    }
    const ref = container.containerNumber;
    const behind = await draftsBehind(conn, ref);
    const ors = [`(t.container_kind = 'booked' AND t.container_number = ?)`];
    const params = [ref];
    if (behind.ids.length) { ors.push('t.draft_container_id IN (?)'); params.push(behind.ids); }
    if (behind.names.length) { ors.push(`(t.container_kind = 'draft' AND t.container_name IN (?))`); params.push(behind.names); }
    return { sql: `(${ors.join(' OR ')})`, params };
}

// The columns a new row takes from its container.
function containerColumns(container) {
    const isDraft = container.kind === 'draft';
    return {
        container_kind: container.kind,
        container_number: container.containerNumber || null,
        draft_container_id: isDraft ? container.draftId : null,
        container_name: isDraft ? container.draftName : null,
    };
}

// ── Loading ──────────────────────────────────────────────────────────────
async function loadPackingLists(conn, container, { supplierKey = null, includeSuperseded = true } = {}) {
    const scope = await containerScope(conn, container);
    const where = ['t.deleted_at IS NULL', scope.sql];
    const params = [...scope.params];
    if (supplierKey) { where.push('t.supplier_key = ?'); params.push(supplierKey); }
    if (!includeSuperseded) where.push('t.superseded_by_id IS NULL');
    const [rows] = await conn.query(
        `SELECT t.* FROM packing_lists t WHERE ${where.join(' AND ')} ORDER BY t.id DESC`, params
    );
    return rows;
}

async function loadSignOffs(conn, container, supplierKey) {
    const scope = await containerScope(conn, container);
    const [rows] = await conn.query(
        `SELECT t.* FROM packing_list_sign_offs t
          WHERE t.revoked_at IS NULL AND t.supplier_key = ? AND ${scope.sql}
          ORDER BY t.id`,
        [supplierKey, ...scope.params]
    );
    return rows.map(signOffToJson);
}

async function loadActiveApproval(conn, container) {
    const scope = await containerScope(conn, container);
    const [rows] = await conn.query(
        `SELECT t.* FROM packing_approvals t
          WHERE t.withdrawn_at IS NULL AND ${scope.sql}
          ORDER BY t.id DESC LIMIT 1`,
        scope.params
    );
    return rows[0] || null;
}

// ── The supplier's check ─────────────────────────────────────────────────
/**
 * Every document the supplier has on file for the container, pooled and
 * compared, with the sign-offs laid over. `rows` may be passed by a caller
 * that already loaded the container's packing lists.
 */
async function buildSupplierCheck(conn, container, supplierKey, { rows = null } = {}) {
    const all = rows ?? await loadPackingLists(conn, container, { supplierKey });
    const mine = all.filter(r => r.supplier_key === supplierKey);
    const active = mine.filter(r => r.superseded_by_id == null);
    const superseded = mine.filter(r => r.superseded_by_id != null);
    const readable = active.filter(r => r.status === 'succeeded' && r.extract_json);
    const documents = readable.map(r => ({ packingListId: r.id, filename: r.filename, extracted: parseJson(r.extract_json) }));
    const signOffs = await loadSignOffs(conn, container, supplierKey);

    let comparison = null;
    if (documents.length) {
        comparison = await P.buildComparison(conn, { container, supplierKey, documents, signOffs });
        // The same invoice twice is almost always a re-upload that should
        // have been a replacement: its rows would be counted twice.
        const seen = new Map();
        for (const r of readable) {
            const inv = (r.invoice_number || '').trim().toLowerCase();
            if (!inv) continue;
            if (seen.has(inv)) comparison.duplicateInvoices = [...(comparison.duplicateInvoices || []), { invoiceNumber: r.invoice_number, packingListIds: [seen.get(inv), r.id] }];
            else seen.set(inv, r.id);
        }
    }
    return {
        supplierKey,
        packingLists: active.map(packingListToJson),
        superseded: superseded.map(packingListToJson),
        processing: active.filter(r => r.status === 'processing').length,
        failed: active.filter(r => r.status === 'failed').length,
        signOffs,
        comparison,
    };
}

// The check as a short, comparable state (what an approval snapshots).
function checkState(check) {
    const c = check.comparison;
    const lines = {};
    if (c) for (const l of c.lines) lines[l.lineKey] = { status: l.status, fingerprint: l.fingerprint, accepted: !!l.accepted };
    return {
        verdict: c ? c.summary.verdict : (check.processing ? 'processing' : check.failed ? 'failed' : 'none'),
        discrepancyCount: c ? c.summary.discrepancyCount : null,
        signedOff: c ? c.summary.signedOff : null,
        outstanding: c ? c.summary.outstanding : null,
        extractionOk: c ? c.extractionCheck.ok : null,
        packingListIds: check.packingLists.map(p => p.id).sort((a, b) => a - b),
        lines,
    };
}

// ── The container's status ───────────────────────────────────────────────
/**
 * One entry per supplier with lines in the container (plus any supplier
 * whose packing list was filed here anyway), each with its live check, and
 * the container's approval with its staleness.
 */
async function buildContainerStatus(conn, container, { withLines = false } = {}) {
    const { status } = await computeStatus(conn, container, { withLines });
    return status;
}

// The status plus each supplier's comparable state (kept aside for approvals).
async function computeStatus(conn, container, { withLines = false } = {}) {
    const suppliersOnBoard = await P.loadContainerSuppliers(conn, container);
    const rows = await loadPackingLists(conn, container);
    const keys = [...new Set([...suppliersOnBoard.map(s => s.supplierKey), ...rows.map(r => r.supplier_key)])];
    const suppliers = [];
    const states = new Map();
    for (const key of keys) {
        const onBoard = suppliersOnBoard.find(s => s.supplierKey === key) || null;
        const check = await buildSupplierCheck(conn, container, key, { rows });
        const state = checkState(check);
        const entry = {
            supplierKey: key,
            supplierName: onBoard?.supplierName ?? check.packingLists[0]?.supplierName ?? check.superseded[0]?.supplierName ?? key,
            names: onBoard?.names ?? [],
            poNumbers: onBoard?.poNumbers ?? [],
            orderCount: onBoard?.orderCount ?? 0,
            units: onBoard?.units ?? 0,
            onBoard: !!onBoard,
            packingLists: check.packingLists,
            superseded: check.superseded,
            processing: check.processing,
            failed: check.failed,
            check: {
                verdict: state.verdict,
                discrepancyCount: state.discrepancyCount,
                signedOff: state.signedOff,
                outstanding: state.outstanding,
                extractionOk: state.extractionOk,
                summary: check.comparison ? check.comparison.summary : null,
                duplicateInvoices: check.comparison?.duplicateInvoices ?? [],
            },
            ...(withLines ? { comparison: check.comparison, signOffs: check.signOffs } : {}),
        };
        suppliers.push(entry);
        states.set(key, state);
    }

    const approvalRow = await loadActiveApproval(conn, container);
    const approval = approvalRow ? approvalState(approvalRow, suppliers, states) : null;
    const overall = overallVerdict(suppliers, approval);
    return { status: { container: P.describeContainer(container), suppliers, approval, overall }, states };
}

function overallVerdict(suppliers, approval) {
    const onBoard = suppliers.filter(s => s.onBoard);
    const withoutList = onBoard.filter(s => !s.packingLists.length).length;
    const failed = onBoard.filter(s => s.packingLists.length && s.check.verdict === 'failed').length;
    const processing = onBoard.filter(s => s.processing > 0 && s.check.verdict === 'processing').length;
    const outstanding = suppliers.reduce((n, s) => n + (s.check.outstanding || 0), 0);
    const unreadable = suppliers.filter(s => s.check.extractionOk === false).length;
    let verdict;
    if (approval) verdict = approval.stale ? 'approved_stale' : 'approved';
    else if (!suppliers.some(s => s.packingLists.length)) verdict = 'none';
    else if (withoutList || failed) verdict = 'incomplete';
    else if (outstanding || unreadable) verdict = 'differences';
    else if (processing) verdict = 'processing';
    else verdict = 'complete';
    return {
        verdict,
        suppliers: onBoard.length,
        suppliersWithoutList: withoutList,
        suppliersProcessing: processing,
        suppliersFailed: failed,
        outstanding,
        signedOff: suppliers.reduce((n, s) => n + (s.check.signedOff || 0), 0),
        mismatched: suppliers.reduce((n, s) => n + (s.check.summary?.mismatched || 0), 0),
        missingProducts: suppliers.reduce((n, s) => n + (s.check.summary?.missingFromPackingList || 0), 0),
        extraProducts: suppliers.reduce((n, s) => n + (s.check.summary?.notExpected || 0), 0),
    };
}

// ── Sign-offs ────────────────────────────────────────────────────────────
class ReviewError extends Error {
    constructor(status, code, message, payload) { super(message); this.status = status; this.code = code; this.payload = payload; }
}

/** Sign off one line of a supplier's check. Idempotent for an unchanged line. */
async function signOffLine(conn, { container, supplierKey, lineKey, note, userEmail }) {
    const check = await buildSupplierCheck(conn, container, supplierKey);
    if (!check.comparison) throw new ReviewError(409, 'NO_CHECK', 'This supplier has no readable packing list in the container yet.');
    const line = check.comparison.lines.find(l => l.lineKey === lineKey);
    if (!line) throw new ReviewError(404, 'LINE_NOT_FOUND', `No line ${lineKey} in this check.`);
    if (line.status === 'match') throw new ReviewError(409, 'LINE_MATCHES', 'That line matches — there is nothing to sign off.');
    if (line.accepted && line.signOff) return { signOff: line.signOff, created: false };

    // A stale sign-off for the same line is retired, never edited.
    const scope = await containerScope(conn, container);
    const [old] = await conn.query(
        `SELECT t.id FROM packing_list_sign_offs t
          WHERE t.revoked_at IS NULL AND t.supplier_key = ? AND t.line_key = ? AND ${scope.sql}`,
        [supplierKey, lineKey, ...scope.params]
    );
    for (const o of old) {
        await conn.query(
            `UPDATE packing_list_sign_offs SET revoked_at = NOW(), revoked_by_email = ?, revoked_reason = 'superseded' WHERE id = ?`,
            [userEmail || null, o.id]
        );
    }
    const cols = containerColumns(container);
    const [ins] = await conn.query(
        `INSERT INTO packing_list_sign_offs
            (container_kind, container_number, draft_container_id, container_name, supplier_key, line_key,
             jf_code, po_number, line_status, fingerprint, differences_json, note, signed_by_email)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            cols.container_kind, cols.container_number, cols.draft_container_id, cols.container_name, supplierKey, lineKey,
            line.jfCode || null, (line.poNumbers || [])[0] || null, line.status, line.fingerprint,
            JSON.stringify(line.differences.filter(d => d.severity !== 'info')),
            note ? String(note).trim().slice(0, 1000) || null : null,
            userEmail || null,
        ]
    );
    await recordAudit(conn, {
        entityType: 'packing_list_sign_off', entityId: ins.insertId, action: 'create', before: null,
        after: { container: P.describeContainer(container), supplierKey, lineKey, lineStatus: line.status, differences: line.differences.filter(d => d.severity !== 'info').map(d => d.message), note: note || null, replaced: old.map(o => o.id) },
        userEmail,
    });
    const [rows] = await conn.query(`SELECT * FROM packing_list_sign_offs WHERE id = ?`, [ins.insertId]);
    return { signOff: signOffToJson(rows[0]), created: true };
}

async function revokeSignOff(conn, { id, reason, userEmail }) {
    const [rows] = await conn.query(`SELECT * FROM packing_list_sign_offs WHERE id = ?`, [id]);
    const row = rows[0];
    if (!row) throw new ReviewError(404, 'NOT_FOUND', `Sign-off ${id} not found.`);
    if (row.revoked_at) return { signOff: signOffToJson(row), already: true };
    await conn.query(
        `UPDATE packing_list_sign_offs SET revoked_at = NOW(), revoked_by_email = ?, revoked_reason = ? WHERE id = ?`,
        [userEmail || null, reason ? String(reason).slice(0, 255) : 'revoked', id]
    );
    await recordAudit(conn, {
        entityType: 'packing_list_sign_off', entityId: id, action: 'revoke',
        before: signOffToJson(row), after: null, userEmail,
    });
    return { signOff: { ...signOffToJson(row), revokedAt: new Date().toISOString() }, already: false };
}

// ── Approval ─────────────────────────────────────────────────────────────
/** Approve the container's packing as it stands, whatever the checks say. */
async function approveContainer(conn, { container, note, userEmail }) {
    const existing = await loadActiveApproval(conn, container);
    if (existing) throw new ReviewError(409, 'ALREADY_APPROVED', 'This container is already approved. Withdraw that approval to approve again.', { approval: approvalToJson(existing) });
    const { status, states } = await computeStatus(conn, container);
    const snapshot = {
        overall: status.overall,
        suppliers: Object.fromEntries(status.suppliers.map(s => [s.supplierKey, { supplierName: s.supplierName, ...snapshotOf(states.get(s.supplierKey)) }])),
    };
    const cols = containerColumns(container);
    const [ins] = await conn.query(
        `INSERT INTO packing_approvals
            (container_kind, container_number, draft_container_id, container_name, note, snapshot_json, approved_by_email)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [cols.container_kind, cols.container_number, cols.draft_container_id, cols.container_name,
         note ? String(note).trim().slice(0, 1000) || null : null, JSON.stringify(snapshot), userEmail || null]
    );
    await recordAudit(conn, {
        entityType: 'packing_approval', entityId: ins.insertId, action: 'create', before: null,
        after: { container: P.describeContainer(container), note: note || null, overall: status.overall },
        userEmail,
    });
    const [rows] = await conn.query(`SELECT * FROM packing_approvals WHERE id = ?`, [ins.insertId]);
    // Fresh: the snapshot IS the current state, so nothing is stale yet.
    return approvalState(rows[0], status.suppliers, states);
}

async function withdrawApproval(conn, { id, note, userEmail }) {
    const [rows] = await conn.query(`SELECT * FROM packing_approvals WHERE id = ?`, [id]);
    const row = rows[0];
    if (!row) throw new ReviewError(404, 'NOT_FOUND', `Approval ${id} not found.`);
    if (row.withdrawn_at) return { approval: approvalToJson(row), already: true };
    await conn.query(
        `UPDATE packing_approvals SET withdrawn_at = NOW(), withdrawn_by_email = ?, withdrawn_note = ? WHERE id = ?`,
        [userEmail || null, note ? String(note).trim().slice(0, 1000) || null : null, id]
    );
    await recordAudit(conn, {
        entityType: 'packing_approval', entityId: id, action: 'withdraw',
        before: approvalToJson(row), after: { note: note || null }, userEmail,
    });
    const [rb] = await conn.query(`SELECT * FROM packing_approvals WHERE id = ?`, [id]);
    return { approval: approvalToJson(rb[0]), already: false };
}

// What an approval remembers per supplier.
function snapshotOf(st) {
    return { verdict: st.verdict, outstanding: st.outstanding, discrepancyCount: st.discrepancyCount, packingListIds: st.packingListIds, lines: st.lines };
}

/**
 * Pure: the approval with `stale` and the changes since. A change is a line
 * whose status or differences moved, a line added or removed, or a document
 * added or replaced — the goods or the paperwork, never a later sign-off.
 * `states` maps supplierKey → the comparable state (checkState) now.
 */
function approvalState(row, suppliersNow, states) {
    const snapshot = parseJson(row.snapshot_json) || { suppliers: {} };
    const changes = [];
    const then = snapshot.suppliers || {};
    const nowByKey = new Map(suppliersNow.map(s => [s.supplierKey, s]));
    const keys = new Set([...Object.keys(then), ...nowByKey.keys()]);
    for (const key of keys) {
        const before = then[key];
        const now = nowByKey.get(key);
        const name = now?.supplierName ?? before?.supplierName ?? key;
        if (!before) { if (now && (now.packingLists.length || now.onBoard)) changes.push({ supplierKey: key, supplierName: name, kind: 'supplier_added' }); continue; }
        if (!now) { changes.push({ supplierKey: key, supplierName: name, kind: 'supplier_removed' }); continue; }
        const st = states.get(key) || { packingListIds: [], lines: {} };
        const beforeIds = (before.packingListIds || []).join(',');
        const nowIds = (st.packingListIds || []).join(',');
        if (beforeIds !== nowIds) changes.push({ supplierKey: key, supplierName: name, kind: 'documents_changed' });
        const bl = before.lines || {};
        const nl = st.lines || {};
        for (const lk of new Set([...Object.keys(bl), ...Object.keys(nl)])) {
            const b = bl[lk];
            const n = nl[lk];
            const [jfCode, pos] = lk.split('|');
            const base = { supplierKey: key, supplierName: name, lineKey: lk, jfCode, poNumbers: pos ? pos.split('+') : [] };
            if (!b) changes.push({ ...base, kind: 'line_added', status: n.status });
            else if (!n) changes.push({ ...base, kind: 'line_removed', status: b.status });
            else if (b.status !== n.status || b.fingerprint !== n.fingerprint) changes.push({ ...base, kind: 'line_changed', from: b.status, to: n.status });
        }
    }
    return {
        ...approvalToJson(row),
        stale: changes.length > 0,
        changesSinceApproval: changes,
        outstandingAtApproval: snapshot.overall?.outstanding ?? null,
        suppliersWithoutListAtApproval: snapshot.overall?.suppliersWithoutList ?? null,
    };
}

async function listActiveApprovals(conn) {
    const [rows] = await conn.query(`SELECT * FROM packing_approvals WHERE withdrawn_at IS NULL ORDER BY id DESC LIMIT 500`);
    return rows.map(approvalToJson);
}

// ── Versions ─────────────────────────────────────────────────────────────
/** Mark `oldId` as replaced by `newId` (same container scope and supplier). */
async function supersede(conn, { container, supplierKey, oldId, newId, userEmail }) {
    const scope = await containerScope(conn, container);
    const [rows] = await conn.query(
        `SELECT t.* FROM packing_lists t WHERE t.id = ? AND t.deleted_at IS NULL AND t.supplier_key = ? AND ${scope.sql}`,
        [oldId, supplierKey, ...scope.params]
    );
    const old = rows[0];
    if (!old) throw new ReviewError(404, 'REPLACES_NOT_FOUND', `Packing list ${oldId} is not one of this supplier's in this container.`);
    if (old.superseded_by_id != null) throw new ReviewError(409, 'ALREADY_REPLACED', `Packing list ${oldId} has already been replaced by ${old.superseded_by_id}.`, { supersededById: old.superseded_by_id });
    await conn.query(`UPDATE packing_lists SET superseded_by_id = ?, superseded_at = NOW() WHERE id = ?`, [newId, oldId]);
    await conn.query(`UPDATE packing_lists SET replaces_id = ?, version = ? WHERE id = ?`, [oldId, (Number(old.version) || 1) + 1, newId]);
    await recordAudit(conn, {
        entityType: 'packing_list', entityId: oldId, action: 'superseded', before: null,
        after: { supersededById: newId, version: (Number(old.version) || 1) + 1 }, userEmail,
    });
    return { version: (Number(old.version) || 1) + 1 };
}

/** Deleting a replacement puts its predecessor back. */
async function restorePredecessor(conn, { deletedRow, userEmail }) {
    if (deletedRow.replaces_id == null) return null;
    const [rows] = await conn.query(
        `SELECT id FROM packing_lists WHERE id = ? AND superseded_by_id = ? AND deleted_at IS NULL`,
        [deletedRow.replaces_id, deletedRow.id]
    );
    if (!rows.length) return null;
    await conn.query(`UPDATE packing_lists SET superseded_by_id = NULL, superseded_at = NULL WHERE id = ?`, [rows[0].id]);
    await recordAudit(conn, {
        entityType: 'packing_list', entityId: rows[0].id, action: 'restored', before: { supersededById: deletedRow.id }, after: null, userEmail,
    });
    return rows[0].id;
}

module.exports = {
    ReviewError,
    packingListToJson,
    signOffToJson,
    approvalToJson,
    containerScope,
    draftsBehind,
    loadPackingLists,
    loadSignOffs,
    loadActiveApproval,
    buildSupplierCheck,
    buildContainerStatus,
    overallVerdict,
    approvalState,
    signOffLine,
    revokeSignOff,
    approveContainer,
    withdrawApproval,
    listActiveApprovals,
    supersede,
    restorePredecessor,
};
