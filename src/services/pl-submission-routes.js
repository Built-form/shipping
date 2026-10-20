'use strict';

// /api/v1/pl-submissions — packing lists suppliers uploaded through the
// supplier portal (jportal), read and matched to a container by the plCheck
// Lambda (src/services/pl-check.js) and submitted for review. Staff see the
// extraction, what the supplier acknowledged and the ranked candidate
// containers with their evidence, preview the full comparison against any
// container, then import (a packing_lists row with source 'portal' that
// reuses the stored extraction — no second Gemini read — into the existing
// packing-list check: sign-offs and approvals work as for a staff upload) or
// reject with a note the supplier sees. The supplier's page is kept current
// by src/services/pl-results.js. Rules: src/lib/pl-submissions.js; matcher:
// src/lib/pl-match.js; frontend guide: docs/pl-submissions-frontend.md.
//
// jportal is another schema on the same MySQL server (JPORTAL_DB_SCHEMA).
// This app's login needs SELECT and the column-level UPDATEs in the frontend
// guide on <jportal>.pl_submissions; nothing here creates or deletes its rows.
// The files live in the portal's private bucket (PI_UPLOADS_BUCKET, pl/*,
// s3:GetObject only); an import copies the bytes into this app's PO bucket.
//
// Import, in order:
//   1. plain reads: the row (status, rowVersion), the supplier group
//      (pl-results.js portalGroup — the ids the portal wrote into
//      group_supplier_ids, with their live names; SUPPLIER_NOT_FOUND when
//      none is live), the blockers, the container (CONTAINER_NOT_FOUND /
//      AMBIGUOUS_CONTAINER / CONTAINER_EMPTY) and the container gate: the
//      group must hold a live line there (packing-list-check.js lineInGroup),
//      or 422 SUPPLIER_NOT_ON_CONTAINER unless `force` (which leaves the
//      supplier's page with container null until lines are allocated). The
//      packing list is filed under the company of the order lines it matches
//      there (pl-results.js filingCompany: a login may hold more than one
//      company), with that company's key — the one the staff container check
//      groups those lines under — and name;
//   2. outside any transaction: GetObject (IfMatch the ETag plCheck pinned),
//      size and magic bytes, PutObject into the PO bucket at
//      packing-lists/<uuid>/<name> (not CopyObject: that needs tagging
//      permissions this app lacks);
//   3. one transaction: lock the row, re-check everything, retire the
//      previous filing if it is still live (a return to Pending kept its id:
//      audit 'refiled'), INSERT the packing list, storeAnalysis with the
//      stored extraction (CONTAINER_EMPTY → 422), supersede the version it
//      replaces, mark the row imported, audit (recordAuditStrict: a failed
//      audit insert fails it);
//   4. after commit: syncPortalResults for the new packing list and the one
//      it superseded (that one's submission becomes 'replaced' at once);
//   5. with no usable extraction (a NO_ROWS or failed check), the background
//      runPackingListCheck reads the file again after the response, on its
//      own connections (the pool has ONE connection, so never inside
//      withConnection), then syncs the portal result.
// A deadlock is retried once, then answered 409 CONFLICT. An import that
// fails after step 2 leaves an unreferenced copy in the PO bucket, as a failed
// staff upload can.
//
// Registered from orders.js; authed through the greedy /api/v1/{proxy+} route.

const { HeadObjectCommand, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const { v4: uuidv4 } = require('uuid');
const L = require('../lib/pl-submissions');
const { jportalSchema } = require('../lib/jportal');
const { recordAuditStrict } = require('../lib/audit');
const P = require('./packing-list-check');
const R = require('./packing-review');
const PL = require('./pl-results');

const FILE_URL_SECONDS = 15 * 60;

const isDeadlock = err => Boolean(err) && (err.code === 'ER_LOCK_DEADLOCK' || err.errno === 1213);
const isLockTimeout = err => Boolean(err) && (err.code === 'ER_LOCK_WAIT_TIMEOUT' || err.errno === 1205);
const isDuplicate = err => Boolean(err) && (err.code === 'ER_DUP_ENTRY' || err.errno === 1062);
const isMissing = e => Boolean(e) && (e.name === 'NoSuchKey' || e.name === 'NotFound' || (e.$metadata && e.$metadata.httpStatusCode === 404));
const isPreconditionFailed = e => Boolean(e) && (e.name === 'PreconditionFailed' || (e.$metadata && e.$metadata.httpStatusCode === 412));

const CONFLICT = { status: 409, code: 'CONFLICT', error: 'Someone changed this submission at the same moment; reload and try again.' };
const CANNOT_DECIDE = userType => ({
    status: 403, code: 'CANNOT_DECIDE',
    error: `Your role (${userType || 'none'}) cannot do this — admin and standard users import and reject supplier packing lists.`,
});
const NOT_IMPORTABLE = blockers => ({
    status: 422, code: 'NOT_IMPORTABLE', error: 'This packing list cannot be imported as it stands.', extra: { blockers },
});
const SUPPLIER_NOT_FOUND = row => ({
    status: 422, code: 'SUPPLIER_NOT_FOUND',
    error: `Supplier ${row.supplier_id} (${row.supplier_name}) no longer exists, so its lines cannot be found.`,
    extra: { blockers: ['supplier_not_found'] },
});
const FILE_UNAVAILABLE = { status: 502, code: 'FILE_UNAVAILABLE', error: 'The file could not be read from the supplier portal’s storage; try again.' };

const iso = v => (v == null ? null : v.toISOString ? v.toISOString() : String(v));
const num = v => (v === null || v === undefined ? null : Number(v));
const parseJson = L.parseJson;

// A resolved or stored container as the API shows it (the candidate shape).
const containerJson = c => (c ? {
    kind: c.kind,
    containerNumber: c.containerNumber ?? null,
    draftContainerId: c.kind === 'draft' ? c.draftId ?? c.draftContainerId ?? null : null,
    label: c.label ?? c.draftName ?? c.containerNumber ?? null,
} : null);

// candidates_json as stored: { top, conflicts, foreignBox } (pl-results.js
// matchColumns), or a bare array from before the conflicts were kept.
function storedCandidates(value) {
    const v = parseJson(value);
    if (Array.isArray(v)) return { top: v, conflicts: [], foreignBox: [] };
    if (!v || typeof v !== 'object') return { top: [], conflicts: [], foreignBox: [] };
    return { top: Array.isArray(v.top) ? v.top : [], conflicts: Array.isArray(v.conflicts) ? v.conflicts : [], foreignBox: Array.isArray(v.foreignBox) ? v.foreignBox : [] };
}

// One scored candidate (pl-match scoreCandidates' entry, stored in
// candidates_json) for the review panel. nearBox is the printed numbers one
// edit away from this container's box, joined; nearBoxes the list.
function candidateToJson(c) {
    const near = Array.isArray(c.nearBox) ? c.nearBox : (c.nearBox ? [String(c.nearBox)] : []);
    return {
        key: c.key ?? null,
        container: { kind: c.kind ?? null, containerNumber: c.containerNumber ?? null, draftContainerId: num(c.draftContainerId), label: c.label ?? null },
        score: num(c.score), coverage: num(c.coverage), exact: num(c.exact), recall: num(c.recall), lineHit: num(c.lineHit), poShare: num(c.poShare),
        packedUnits: num(c.packedUnits), expectedUnits: num(c.expectedUnits), matched: num(c.matched), unexpected: num(c.unexpected), missing: num(c.missing),
        boxHit: c.boxHit === true, boxNumbers: Array.isArray(c.boxNumbers) ? c.boxNumbers : [],
        nearBox: near.length ? near.join(', ') : null, nearBoxes: near,
        refHit: c.refHit === true, replacesHit: c.replacesHit === true,
        conflicts: Array.isArray(c.conflicts) ? c.conflicts : [],
    };
}

/**
 * What the supplier saw when sending. The portal asks for an acknowledgement
 * only for a confident suggestion with differences (or printed totals that do
 * not add up); the one way such a row is sent unacknowledged is the portal's
 * group gate, which showed the supplier no result (the gate's answer is in
 * jportal's own audit, not on the row — this is the inference). Null when the
 * check never succeeded (the supplier saw the failure and wrote a note).
 */
function acknowledgedOf(row) {
    if (row.check_status !== 'succeeded') return null;
    const result = parseJson(row.result_json);
    const confident = row.match_confidence === 'confident';
    const differences = confident && (Number(row.discrepancy_count) > 0 || row.verdict === 'differences');
    const totalsOff = result?.extractionCheck?.ok === false;
    if ((differences || totalsOff) && Number(row.warnings_acknowledged) !== 1) return { sawNoResult: true };
    return { sawNoResult: false, suggestedLabel: row.suggested_label || null, confidence: row.match_confidence || null, reason: row.match_reason || null };
}

// The extraction for staff: the model's reading as stored, plus the
// supplierPrinted alias the review panel reads.
function extractionOf(row) {
    const x = parseJson(row.extract_json);
    if (!x || typeof x !== 'object') return null;
    return { ...x, supplierPrinted: x.supplierName ?? null };
}

// The stored extraction can be reused by Import (no second read) when the
// check succeeded and read at least one row.
function usableExtraction(row) {
    if (row.check_status !== 'succeeded') return null;
    const x = parseJson(row.extract_json);
    return x && Array.isArray(x.lines) && x.lines.length ? x : null;
}

function registerPlSubmissionRoutes(app, deps) {
    const { withConnection, pool, ready, s3, poBucket, log } = deps;

    const refuse = (res, r) => res.status(r.status).json({ error: r.error, code: r.code, ...(r.extra || {}) });
    const sendFail = (res, f) => res.status(f.status).json({ error: f.error, code: f.code, ...(f.payload || {}) });
    const plBucket = () => process.env.PI_UPLOADS_BUCKET || null;

    // Every column of the row, the timestamps as ISO UTC strings, and whether
    // its supplier row is live (supplier_exists). Read on every call: an
    // invalid JPORTAL_DB_SCHEMA is a 500 on these routes, not a failed import.
    const selectSql = () => `SELECT r.*,
                       DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso,
                       DATE_FORMAT(r.decided_at, '%Y-%m-%dT%H:%i:%sZ') AS decided_at_iso,
                       DATE_FORMAT(r.checked_at, '%Y-%m-%dT%H:%i:%sZ') AS checked_at_iso,
                       DATE_FORMAT(r.result_at, '%Y-%m-%dT%H:%i:%sZ') AS result_at_iso,
                       DATE_FORMAT(r.created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at_iso,
                       (s.id IS NOT NULL) AS supplier_exists
                  FROM ${jportalSchema()}.pl_submissions r
                  LEFT JOIN suppliers s ON s.id = r.supplier_id AND s.deleted_at IS NULL`;

    const readRow = async (conn, id) => {
        const [rows] = await conn.query(`${selectSql()} WHERE r.id = ? AND r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})`, [id, ...L.VISIBLE_STATUSES]);
        return rows[0] || null;
    };
    const supplierOf = row => ({ exists: Number(row.supplier_exists) === 1 });

    // The key ShipLine filters by: 's:<supplier_id>' — the row's supplier_id
    // is the lowest id of the group the portal wrote, so it names the group.
    // Derived from the row alone; no portal_code, no lookup.
    const supplierKeyOf = row => `s:${Number(row.supplier_id)}`;
    // The group behind a row (pl-results.js portalGroup).
    const groupOf = (conn, row) => PL.portalGroup(conn, row);

    // Who is on the container, by supplier name (the SUPPLIER_NOT_ON_CONTAINER
    // payload, in the staff picker's shape): one entry per name on its lines
    // (the PO's name, else the order's), keyed 's:<id>' when a live suppliers
    // row carries that name, 'n:<name>' otherwise. Not the staff picker
    // (loadContainerSuppliers), which groups by portal_code.
    const containerSuppliersByName = async (conn, container) => {
        const { rows } = await P.queryContainerLines(conn, container);
        const byName = new Map();
        for (const o of rows) {
            const name = String(o.po_supplier || o.supplier || '').trim();
            const k = P.nameKey(name);
            let g = byName.get(k);
            if (!g) { g = { name, names: new Set(), poNumbers: new Set(), orderCount: 0, units: 0 }; byName.set(k, g); }
            for (const n of [o.po_supplier, o.supplier]) if (n && String(n).trim()) g.names.add(String(n).trim());
            if (o.po_number) g.poNumbers.add(o.po_number);
            g.orderCount += 1;
            g.units += Number(o.quantity) || 0;
        }
        const ids = new Map();
        const names = [...byName.values()].map(g => g.name).filter(Boolean);
        if (names.length) {
            const [live] = await conn.query(`SELECT id, name FROM suppliers WHERE deleted_at IS NULL AND name IN (?) ORDER BY id`, [names]);
            for (const s of live) if (!ids.has(P.nameKey(s.name))) ids.set(P.nameKey(s.name), Number(s.id));
        }
        return [...byName.entries()]
            .map(([k, g]) => ({
                supplierKey: ids.has(k) ? `s:${ids.get(k)}` : `n:${k}`,
                supplierName: g.name || '(no supplier)',
                names: [...g.names].sort(), poNumbers: [...g.poNumbers].sort(), orderCount: g.orderCount, units: g.units,
            }))
            .sort((a, b) => b.units - a.units);
    };

    // The submission for the API: the shared JSON plus the flat suggested_* /
    // confirmed_* columns the review panel reads, and the group key.
    const toJson = (row, { supplierKey = null, blockers = [] } = {}) => {
        const base = L.submissionToJson(row, { blockers });
        return {
            ...base,
            supplierKey,
            supplierExists: Number(row.supplier_exists) === 1,
            suggestedKind: base.suggested?.kind ?? null,
            suggestedContainerNumber: base.suggested?.containerNumber ?? null,
            suggestedDraftId: base.suggested?.draftContainerId ?? null,
            suggestedLabel: base.suggested?.label ?? null,
            confirmedKind: base.confirmed?.kind ?? null,
            confirmedContainerNumber: base.confirmed?.containerNumber ?? null,
            confirmedDraftId: base.confirmed?.draftContainerId ?? null,
            confirmedLabel: base.confirmed?.label ?? null,
        };
    };
    const blockersOf = row => (row.status === 'submitted' ? L.importBlockers({ row, supplier: supplierOf(row) }) : []);

    // A pending (submitted) row loaded by the routes, refused when it cannot be decided.
    const stateRefusal = (row, id, rowVersion) => {
        if (!row || !L.VISIBLE_STATUSES.includes(row.status)) return { status: 404, code: 'NOT_FOUND', error: `Packing-list submission ${id} not found.` };
        if (row.status !== 'submitted') {
            return {
                status: 409, code: 'NOT_SUBMITTED', error: `Packing-list submission ${id} is ${row.status}, not waiting for review.`,
                extra: { status: row.status, packingListId: num(row.packing_list_id) },
            };
        }
        if (Number(row.row_version) !== rowVersion) {
            return { status: 409, code: 'STALE', error: 'The supplier or someone else changed this submission since it was loaded; reload and check it again.' };
        }
        return null;
    };

    // fn(conn) in a transaction; a returned { fail } rolls back. Retried once
    // on a deadlock; a second deadlock or a lock-wait timeout is { fail: CONFLICT }.
    const inTransaction = async (site, fn) => {
        const once = () => withConnection(async (conn) => {
            await conn.beginTransaction();
            try {
                const out = await fn(conn);
                if (out && out.fail) await conn.rollback();
                else await conn.commit();
                return out;
            } catch (e) {
                try {
                    await conn.rollback();
                } catch (rollbackError) {
                    log.warn(`[${site}] rollback failed`, rollbackError.message);
                }
                throw e;
            }
        });
        try {
            return await once();
        } catch (e) {
            if (isLockTimeout(e)) return { fail: CONFLICT };
            if (!isDeadlock(e)) throw e;
        }
        try {
            return await once();
        } catch (e) {
            if (isDeadlock(e) || isLockTimeout(e)) return { fail: CONFLICT };
            throw e;
        }
    };

    // The file link: an imported row's permanent copy in the PO bucket (the
    // packing list's own key), else the portal bucket's object, which the
    // portal's lifecycle removes after 180 days. No connection is held.
    // → { url, expiresAt, state: { gone, changed } }; url null when gone.
    const fileLink = async (row, packingList) => {
        const state = { gone: false, changed: false };
        const name = L.safeFilename(row.filename, row.content_type) || `pl-${row.id}.${L.ALLOWED_TYPES[row.content_type] || 'bin'}`;
        const type = L.ALLOWED_TYPES[row.content_type] ? row.content_type : 'application/octet-stream';
        const sign = (bucket, key) => getSignedUrl(s3, new GetObjectCommand({
            Bucket: bucket, Key: key, ResponseContentType: type, ResponseContentDisposition: `inline; filename="${name}"`,
        }), { expiresIn: FILE_URL_SECONDS });
        const expiresAt = () => new Date(Date.now() + FILE_URL_SECONDS * 1000).toISOString();
        if (packingList && !packingList.deleted_at && poBucket && packingList.s3_key) {
            try {
                return { url: await sign(poBucket, packingList.s3_key), expiresAt: expiresAt(), state };
            } catch (e) {
                log.warn('[pl-submissions] could not sign the imported copy', { id: row.id, error: e.message });
            }
        }
        const bucket = plBucket();
        if (!bucket || !L.isPlKey(row.s3_key)) return { url: null, expiresAt: null, state };
        try {
            const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: row.s3_key }));
            if (row.s3_etag && head.ETag && head.ETag !== row.s3_etag) state.changed = true;
            return { url: await sign(bucket, row.s3_key), expiresAt: expiresAt(), state };
        } catch (e) {
            if (isMissing(e)) state.gone = true;
            else log.warn('[pl-submissions] file lookup failed', { id: row.id, error: e.message });
            return { url: null, expiresAt: null, state };
        }
    };

    // The packing list a row points at (its current filing, or the previous
    // one after a return to Pending), deleted or not.
    const packingListOf = async (conn, row) => {
        const plId = num(row.packing_list_id);
        if (!plId) return null;
        const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [plId]);
        return rows[0] || null;
    };
    const packingListJson = pl => (pl ? { ...R.packingListToJson(pl), deleted: pl.deleted_at != null } : null);

    // Background read of an imported packing list whose stored extraction was
    // not usable, then the portal result. Same callbackWaitsForEmptyEventLoop
    // dance as the staff upload route (packing-list-routes.js startCheck).
    const startCheck = (req, packingListId) => {
        if (req.lambdaContext) req.lambdaContext.callbackWaitsForEmptyEventLoop = true;
        const userEmail = req.userEmail || null;
        (async () => {
            try {
                const out = await P.runPackingListCheck(pool, { id: packingListId, userEmail });
                if (out && out.failed) log.warn('[pl-submissions/import] fallback read failed', { packingListId, code: out.failed });
            } catch (e) {
                log.warn('[pl-submissions/import] fallback read threw', { packingListId, error: e.message });
            }
            try {
                await PL.syncPortalResults(pool, { packingListIds: [packingListId] });
            } catch (e) {
                log.warn('[pl-submissions/import] portal sync after the fallback read failed', { packingListId, error: e.message });
            }
        })();
    };

    // GET /api/v1/pl-submissions?status=&supplierKey=&supplierId=&limit=&offset=
    // status: pending (= submitted, the default), history (= imported, rejected,
    // withdrawn), one or more of submitted / imported / rejected / withdrawn
    // comma-separated, decided or all. Drafts and discarded rows are never
    // listed. supplierKey ('s:<id>' from /suppliers) and supplierId both
    // filter on the row's supplier_id (the lowest id of its group, which the
    // portal writes). Oldest first for submitted alone (the order they came
    // in), newest change first otherwise. The extraction, the candidates and
    // the result are left out (the detail has them).
    // 200 { data: PlSubmission[], hasMore }
    app.get('/api/v1/pl-submissions', async (req, res) => {
        try {
            const q0 = req.query || {};
            const status = q0.status === 'pending' ? 'submitted' : q0.status === 'history' ? 'decided' : q0.status;
            const parsed = L.parseListQuery({ ...q0, status });
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const q = parsed.value;
            const supplierKey = q0.supplierKey !== undefined && q0.supplierKey !== '' ? String(q0.supplierKey).trim() : null;
            const keyMatch = supplierKey ? /^s:(\d+)$/.exec(supplierKey) : null;
            if (supplierKey && !keyMatch) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'supplierKey must be a supplier group key (s:<id>).' });
            const where = [`r.status IN (${q.statuses.map(() => '?').join(', ')})`];
            const params = [...q.statuses];
            if (q.supplierId) { where.push('r.supplier_id = ?'); params.push(q.supplierId); }
            if (keyMatch) { where.push('r.supplier_id = ?'); params.push(Number(keyMatch[1])); }
            const order = q.statuses.length === 1 && q.statuses[0] === 'submitted'
                ? 'r.submitted_at, r.id'
                : 'r.updated_at DESC, r.id DESC';
            const out = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `${selectSql()} WHERE ${where.join(' AND ')} ORDER BY ${order} LIMIT ${q.limit + 1} OFFSET ${q.offset}`,
                    params
                );
                const hasMore = rows.length > q.limit;
                const data = rows.slice(0, q.limit).map(row => toJson(row, { supplierKey: supplierKeyOf(row), blockers: blockersOf(row) }));
                return { data, hasMore };
            });
            res.json(out);
        } catch (error) {
            log.error('[GET /pl-submissions]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/pl-submissions/suppliers — every supplier (keyed by the
    // rows' supplier_id: the lowest id of the group the portal wrote, so one
    // entry per group) with submissions staff can see: how many wait for
    // review, how many in all, and the oldest pending. supplierIds is the
    // union of the rows' group_supplier_ids; supplierName the live name of
    // supplier_id, else the rows' snapshot. Suppliers with pending rows
    // first, oldest first.
    // 200 { data: PlSupplierSummary[] }
    app.get('/api/v1/pl-submissions/suppliers', async (req, res) => {
        try {
            const data = await withConnection(async (conn) => {
                const [rows] = await conn.query(
                    `SELECT r.supplier_id, r.supplier_name, r.group_supplier_ids, r.status,
                            DATE_FORMAT(r.submitted_at, '%Y-%m-%dT%H:%i:%sZ') AS submitted_at_iso
                       FROM ${jportalSchema()}.pl_submissions r
                      WHERE r.status IN (${L.VISIBLE_STATUSES.map(() => '?').join(', ')})
                      ORDER BY r.submitted_at, r.id`,
                    L.VISIBLE_STATUSES
                );
                const groups = new Map();
                for (const r of rows) {
                    const sid = Number(r.supplier_id);
                    const key = supplierKeyOf(r);
                    let g = groups.get(key);
                    if (!g) {
                        g = { supplierKey: key, supplierId: sid, supplierName: r.supplier_name, supplierIds: new Set(), pending: 0, total: 0, oldestSubmittedAt: null };
                        groups.set(key, g);
                    }
                    for (const id of L.parseGroupSupplierIds(r)) g.supplierIds.add(id);
                    g.total += 1;
                    if (r.status === 'submitted') {
                        g.pending += 1;
                        if (r.submitted_at_iso && (!g.oldestSubmittedAt || r.submitted_at_iso < g.oldestSubmittedAt)) g.oldestSubmittedAt = r.submitted_at_iso;
                    }
                }
                const live = new Map();
                if (groups.size) {
                    const [names] = await conn.query(`SELECT id, name FROM suppliers WHERE id IN (?) AND deleted_at IS NULL`, [[...groups.values()].map(g => g.supplierId)]);
                    for (const n of names) live.set(Number(n.id), n.name);
                }
                return [...groups.values()]
                    .map(g => ({ ...g, supplierName: live.get(g.supplierId) || g.supplierName, supplierIds: [...g.supplierIds].sort((a, b) => a - b) }))
                    .sort((a, b) => (b.pending > 0) - (a.pending > 0)
                        || String(a.oldestSubmittedAt || '~').localeCompare(String(b.oldestSubmittedAt || '~'))
                        || String(a.supplierName).localeCompare(String(b.supplierName)));
            });
            res.json({ data });
        } catch (error) {
            log.error('[GET /pl-submissions/suppliers]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/pl-submissions/:id — everything the review panel shows: the
    // row, the extraction, what the supplier acknowledged, the candidates as
    // scored at check time and as they score now (live null with liveError
    // when that load failed), a 15-minute link to the file, the linked or
    // previous packing list, the replace choice to pre-select, the blockers
    // and this submission's audit trail (shipping's audit_log; the supplier's
    // own actions are in jportal's audit_logs).
    // 200 PlSubmissionDetail · 404 NOT_FOUND
    app.get('/api/v1/pl-submissions/:id(\\d+)', async (req, res) => {
        try {
            const id = Number(req.params.id);
            const out = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                if (!row) return null;
                const supplierKey = supplierKeyOf(row);
                const group = await groupOf(conn, row);
                const packingList = await packingListOf(conn, row);
                // Only a check that read rows is worth re-scoring; a NO_ROWS or failed
                // check has nothing to match (same test as Import uses).
                const extracted = usableExtraction(row);

                // The candidates now, with the matcher's top-level conflicts
                // (foreign_box lives only there). The matcher logs nothing
                // here: this is not a check run.
                let live = null;
                let liveConflicts = null;
                let liveError = null;
                if (extracted && !PL.groupGone(group)) {
                    try {
                        const replacesContainer = await PL.replacesContainerFor(conn, row);
                        const s = await PL.suggestContainer(conn, { group, extracted, filename: row.filename, replacesContainer, submissionId: id, quiet: true });
                        live = s.match.candidates.map(candidateToJson);
                        liveConflicts = s.match.conflicts;
                    } catch (e) {
                        liveError = e.message;
                        log.warn('[GET /pl-submissions/:id] live candidates failed', { id, error: e.message });
                    }
                } else {
                    liveError = PL.groupGone(group) ? 'The supplier no longer exists.' : 'The file was not read.';
                }

                // The imported version a corrected upload should replace, and
                // where it is now: pre-selected in the import pop-up.
                let defaultReplacesId = null;
                let defaultContainer = null;
                const replacesId = num(row.replaces_submission_id);
                if (replacesId) {
                    const [prev] = await conn.query(
                        `SELECT p.* FROM ${jportalSchema()}.pl_submissions r
                           JOIN packing_lists p ON p.id = r.packing_list_id
                          WHERE r.id = ? AND r.status = 'imported' AND p.deleted_at IS NULL AND p.superseded_by_id IS NULL`,
                        [replacesId]
                    );
                    if (prev.length) {
                        defaultReplacesId = Number(prev[0].id);
                        try {
                            defaultContainer = containerJson(await P.resolveStoredContainer(conn, prev[0]));
                        } catch (e) {
                            log.warn('[GET /pl-submissions/:id] could not resolve the replaced filing', { id, error: e.message });
                        }
                    }
                }
                // No corrected-upload link: a packing list of this supplier's
                // already on file in the suggested shipment with the same
                // invoice number is almost always this same list sent again.
                // Pre-selected, so Import replaces it rather than filing a
                // second copy (the pooled check counts it once either way).
                if (!defaultReplacesId && row.invoice_number && row.suggested_kind && !PL.groupGone(group)) {
                    try {
                        const r = await P.resolveContainerForRequest(conn, {
                            containerNumber: row.suggested_kind === 'booked' ? row.suggested_container_number : undefined,
                            draftContainerId: row.suggested_kind === 'draft' ? row.suggested_draft_id : undefined,
                        });
                        if (r.container) {
                            const inv = String(row.invoice_number).replace(/\s+/g, '').toLowerCase();
                            const company = await PL.filingCompany(conn, { group, container: r.container, extracted, supplierId: row.supplier_id });
                            const { packingLists } = await PL.groupPool(conn, r.container, company);
                            const same = packingLists.find(p => String(p.invoice_number || '').replace(/\s+/g, '').toLowerCase() === inv);
                            if (same) {
                                defaultReplacesId = Number(same.id);
                                defaultContainer = containerJson(r.container);
                            }
                        }
                    } catch (e) {
                        log.warn('[GET /pl-submissions/:id] could not look for a same-invoice filing', { id, error: e.message });
                    }
                }
                const [auditRows] = await conn.query(
                    `SELECT action, user_email, created_at, after_json FROM audit_log
                      WHERE entity_type = 'pl_submission' AND entity_id = ? ORDER BY created_at, id`,
                    [id]
                );
                return { row, supplierKey, packingList, live, liveConflicts, liveError, defaultReplacesId, defaultContainer, auditRows };
            });
            if (!out) return refuse(res, { status: 404, code: 'NOT_FOUND', error: `Packing-list submission ${id} not found.` });
            const { row, supplierKey, packingList } = out;
            const imported = row.status === 'imported';

            const file = await fileLink(row, imported ? packingList : null);
            const blockers = row.status === 'submitted' ? L.importBlockers({ row, supplier: supplierOf(row), file: file.state }) : [];
            const stored = storedCandidates(row.candidates_json);
            res.json({
                submission: toJson(row, { supplierKey, blockers }),
                check: L.checkToJson(row),
                extraction: extractionOf(row),
                acknowledged: acknowledgedOf(row),
                candidates: {
                    stored: stored.top.map(candidateToJson),
                    live: out.live,
                    liveError: out.liveError,
                    // The matcher's top-level conflicts, live when it ran (the
                    // stored ones when it could not), each { code, message,
                    // candidates: [keys], boxNumbers? (foreign_box) }.
                    conflicts: out.live ? out.liveConflicts : stored.conflicts,
                    storedConflicts: stored.conflicts,
                    foreignBox: out.live ? null : stored.foreignBox,
                },
                fileUrl: file.url,
                fileExpiresAt: file.expiresAt,
                packingList: imported ? packingListJson(packingList) : null,
                previousPackingList: imported ? null : packingListJson(packingList),
                defaultReplacesId: out.defaultReplacesId,
                defaultContainer: out.defaultContainer,
                result: parseJson(row.result_json),
                canImport: row.status === 'submitted' && blockers.length === 0,
                audit: out.auditRows.map(a => ({
                    action: a.action, userEmail: a.user_email || null, createdAt: iso(a.created_at), after: parseJson(a.after_json),
                })),
            });
        } catch (error) {
            log.error('[GET /pl-submissions/:id]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // GET /api/v1/pl-submissions/:id/preview?containerNumber=324 | ?draftContainerId=129 [&replacesId=7]
    // The full staff comparison this document would give in that container:
    // pooled with the supplier's packing lists already on file there (minus
    // the one replacesId names), sign-offs applied, with `elsewhere`. A draft
    // is followed through renames and bookings (container.followedFrom). The
    // supplier is the company Import would file it under there
    // (pl-results.js filingCompany): supplierKey and supplierName.
    // 200 { supplierKey, supplierName, container, comparison, packingLists, duplicateInvoices, onBoard }
    // 400 BAD_FIELD · 404 NOT_FOUND | CONTAINER_NOT_FOUND · 409 AMBIGUOUS_CONTAINER
    // 422 CONTAINER_EMPTY | SUPPLIER_NOT_FOUND
    app.get('/api/v1/pl-submissions/:id(\\d+)/preview', async (req, res) => {
        try {
            const id = Number(req.params.id);
            let replacesId = null;
            if (req.query.replacesId !== undefined && req.query.replacesId !== '') {
                replacesId = L.positiveInt(req.query.replacesId);
                if (!replacesId) return refuse(res, { status: 400, code: 'BAD_FIELD', error: 'replacesId must be a packing list id.' });
            }
            const out = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                if (!row) return { fail: { status: 404, code: 'NOT_FOUND', error: `Packing-list submission ${id} not found.` } };
                const group = await groupOf(conn, row);
                if (PL.groupGone(group)) return { fail: SUPPLIER_NOT_FOUND(row) };
                const r = await P.resolveContainerForRequest(conn, { containerNumber: req.query.containerNumber, draftContainerId: req.query.draftContainerId });
                if (r.fail) return { fail: { status: r.fail.status, code: r.fail.code, error: r.fail.error, extra: r.fail.payload || {} } };
                const container = r.container;
                const extracted = parseJson(row.extract_json);
                const company = await PL.filingCompany(conn, { group, container, extracted, supplierId: row.supplier_id });
                const { comparison, pool } = await PL.pooledComparison(conn, {
                    container, group: company,
                    extraDocument: extracted ? { packingListId: null, filename: row.filename, extracted } : null,
                    excludePackingListIds: replacesId ? [replacesId] : [],
                });
                return {
                    supplierKey: company.key,
                    supplierName: company.name,
                    container: P.describeContainer(container),
                    comparison,
                    packingLists: pool.packingLists.map(R.packingListToJson),
                    duplicateInvoices: comparison?.duplicateInvoices || [],
                    onBoard: pool.expected.rows.length > 0,
                    extractionUsable: Boolean(usableExtraction(row)),
                };
            });
            if (out.fail) return refuse(res, out.fail);
            res.json(out);
        } catch (error) {
            log.error('[GET /pl-submissions/:id/preview]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/pl-submissions/:id/import
    // Body: { rowVersion, containerNumber | draftContainerId, replacesId?, force?, note? }
    // Files the packing list in that container (see the header). replacesId:
    // the supplier's packing list there this one corrects (the versioning of
    // packing-review.js). force: file it although the supplier's group has no
    // live line in the container. note: shown to the supplier.
    // 200 { submission, packingList }
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND | CONTAINER_NOT_FOUND | REPLACES_NOT_FOUND
    // 409 NOT_SUBMITTED | STALE | CONFLICT | AMBIGUOUS_CONTAINER | ALREADY_REPLACED
    // 422 NOT_IMPORTABLE | SUPPLIER_NOT_FOUND | SUPPLIER_NOT_ON_CONTAINER | CONTAINER_EMPTY · 502 FILE_UNAVAILABLE
    app.post('/api/v1/pl-submissions/:id(\\d+)/import', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseImportBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            const bucket = plBucket();
            if (!bucket || !poBucket) return res.status(500).json({ error: 'PI_UPLOADS_BUCKET / PO_DOCS_BUCKET not configured.' });
            await ready();

            // The gates, on plain reads: the row, the supplier group, the
            // container and whether the group is on board. → { fail } or what
            // was found, with the company the packing list is filed under
            // (pl-results.js filingCompany: its supplier_key and name).
            const gates = async (conn, row) => {
                const refusal = stateRefusal(row, id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const group = await groupOf(conn, row);
                if (PL.groupGone(group)) return { fail: SUPPLIER_NOT_FOUND(row) };
                const blockers = L.importBlockers({ row, supplier: { exists: true } });
                if (blockers.length) return { fail: NOT_IMPORTABLE(blockers) };
                const r = await P.resolveContainerForRequest(conn, { containerNumber: body.containerNumber, draftContainerId: body.draftContainerId });
                if (r.fail) return { fail: { status: r.fail.status, code: r.fail.code, error: r.fail.error, extra: r.fail.payload || {} } };
                const container = r.container;
                // Of the group's companies, the one whose lines there it
                // matches; one with lines there whenever the group has any.
                const company = await PL.filingCompany(conn, { group, container, extracted: parseJson(row.extract_json), supplierId: row.supplier_id });
                const { rows: mine } = await P.loadGroupLines(conn, container, company);
                const onBoard = mine.length > 0;
                // A group with nothing in the container would make every row
                // "not expected" — almost always the wrong container.
                if (!onBoard && !body.force) {
                    return { fail: {
                        status: 422, code: 'SUPPLIER_NOT_ON_CONTAINER',
                        error: `That supplier has no lines in ${container.label}. Send force: true to file it there anyway.`,
                        extra: { suppliers: await containerSuppliersByName(conn, container) },
                    } };
                }
                return { group, company, supplierKey: company.key, container, onBoard, supplierName: company.name || row.supplier_name };
            };

            // 1. Plain reads.
            const pre = await withConnection(async (conn) => {
                const row = await readRow(conn, id);
                const g = await gates(conn, row);
                return g.fail ? g : { ...g, row };
            });
            if (pre.fail) return refuse(res, pre.fail);
            const { row } = pre;

            // 2. The bytes, exactly as checked, copied into the PO bucket.
            let bytes;
            try {
                const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: row.s3_key, IfMatch: row.s3_etag }));
                if (Number(obj.ContentLength) > L.MAX_FILE_BYTES) {
                    if (obj.Body && typeof obj.Body.destroy === 'function') obj.Body.destroy();
                    return refuse(res, NOT_IMPORTABLE(['too_large']));
                }
                bytes = Buffer.from(await obj.Body.transformToByteArray());
            } catch (e) {
                if (isMissing(e)) return refuse(res, NOT_IMPORTABLE(['file_gone']));
                if (isPreconditionFailed(e)) return refuse(res, NOT_IMPORTABLE(['file_changed']));
                log.warn('[POST /pl-submissions/:id/import] get failed', { id, error: e.message });
                return refuse(res, FILE_UNAVAILABLE);
            }
            if (bytes.length > L.MAX_FILE_BYTES) return refuse(res, NOT_IMPORTABLE(['too_large']));
            if (L.sniffType(bytes) !== row.content_type) return refuse(res, NOT_IMPORTABLE(['bad_type']));
            const s3Key = L.destKey({ submissionId: id, filename: row.filename, contentType: row.content_type, token: uuidv4() });
            const shownName = s3Key.slice(s3Key.lastIndexOf('/') + 1);
            try {
                await s3.send(new PutObjectCommand({
                    Bucket: poBucket, Key: s3Key, Body: bytes, ContentType: row.content_type,
                    ContentDisposition: `inline; filename="${shownName}"`,
                }));
            } catch (e) {
                log.warn('[POST /pl-submissions/:id/import] put failed', { id, error: e.message });
                return refuse(res, FILE_UNAVAILABLE);
            }

            // 3. One transaction.
            const out = await inTransaction('POST /pl-submissions/:id/import', async (conn) => {
                const jp = jportalSchema();
                const [locked] = await conn.query(`SELECT * FROM ${jp}.pl_submissions WHERE id = ? FOR UPDATE`, [id]);
                const lockedRow = locked[0];
                const g = await gates(conn, lockedRow);
                if (g.fail) return g;
                const { supplierKey, container, supplierName } = g;
                const extracted = usableExtraction(lockedRow);

                // The previous filing (a return to Pending kept its id): retire
                // it, as DELETE /packing-lists/:id would.
                let refiledId = null;
                let restoredId = null;
                const prevId = num(lockedRow.packing_list_id);
                if (prevId) {
                    const [prev] = await conn.query(`SELECT * FROM packing_lists WHERE id = ? AND deleted_at IS NULL`, [prevId]);
                    if (prev.length) {
                        await conn.query(`UPDATE packing_lists SET deleted_at = NOW() WHERE id = ?`, [prevId]);
                        await recordAuditStrict(conn, {
                            entityType: 'packing_list', entityId: prevId, action: 'refiled',
                            before: R.packingListToJson(prev[0]), after: { portalSubmissionId: id, container: P.describeContainer(container) },
                            userEmail: req.userEmail,
                        });
                        restoredId = await R.restorePredecessor(conn, { deletedRow: prev[0], userEmail: req.userEmail });
                        refiledId = prevId;
                    }
                }

                let packingListId;
                try {
                    const c = R.containerColumns(container);
                    const [ins] = await conn.query(
                        `INSERT INTO packing_lists
                            (container_kind, container_number, draft_container_id, container_name,
                             supplier_key, supplier_name, filename, s3_key, content_type, file_size, status, uploaded_by_email,
                             source, portal_submission_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processing', ?, 'portal', ?)`,
                        [
                            c.container_kind, c.container_number, c.draft_container_id, c.container_name,
                            supplierKey, supplierName, String(lockedRow.filename || shownName).trim().slice(0, 200) || shownName,
                            s3Key, lockedRow.content_type, bytes.length, lockedRow.submitted_by_email, id,
                        ]
                    );
                    packingListId = ins.insertId;
                } catch (e) {
                    // The unique key on the live portal_submission_id: imported twice at once.
                    if (isDuplicate(e)) return { fail: CONFLICT };
                    throw e;
                }
                await recordAuditStrict(conn, {
                    entityType: 'packing_list', entityId: packingListId, action: 'create', before: null,
                    after: {
                        container: P.describeContainer(container), supplierKey, supplierName, filename: lockedRow.filename, fileSize: bytes.length,
                        replacesId: body.replacesId, source: 'portal', portalSubmissionId: id, uploadedByEmail: lockedRow.submitted_by_email,
                    },
                    userEmail: req.userEmail,
                });

                // The stored extraction, compared with the container's lines:
                // what runPackingListCheck would do after reading the file.
                let analysed = false;
                if (extracted) {
                    const [plRows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [packingListId]);
                    try {
                        await P.storeAnalysis(conn, {
                            id: packingListId, row: plRows[0], parsed: extracted, modelUsed: lockedRow.model_used || null, userEmail: req.userEmail,
                        });
                        analysed = true;
                    } catch (e) {
                        if (e.code === 'CONTAINER_EMPTY') return { fail: { status: 422, code: 'CONTAINER_EMPTY', error: e.message } };
                        throw e;
                    }
                }

                if (body.replacesId != null) {
                    // The version it replaces may sit under another of the
                    // company's keys (an import from before filings followed
                    // the lines' company): supersede checks it under its own.
                    const [old] = await conn.query(`SELECT supplier_key FROM packing_lists WHERE id = ?`, [body.replacesId]);
                    const oldKey = old.length && PL.groupKeys(g.company).includes(old[0].supplier_key) ? old[0].supplier_key : supplierKey;
                    try {
                        await R.supersede(conn, { container, supplierKey: oldKey, oldId: body.replacesId, newId: packingListId, userEmail: req.userEmail });
                    } catch (e) {
                        if (!(e instanceof R.ReviewError)) throw e;
                        return { fail: { status: e.status, code: e.code, error: e.message, extra: e.payload || {} } };
                    }
                }

                const isDraft = container.kind === 'draft';
                const [done] = await conn.query(
                    `UPDATE ${jp}.pl_submissions
                        SET status = 'imported', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            packing_list_id = ?, confirmed_kind = ?, confirmed_container_number = ?, confirmed_draft_id = ?, confirmed_label = ?,
                            row_version = row_version + 1
                      WHERE id = ? AND status = 'submitted' AND row_version = ?`,
                    [
                        req.userEmail || 'unknown', body.note, packingListId,
                        container.kind, container.containerNumber || null, isDraft ? container.draftId ?? null : null, String(container.label || '').slice(0, 255) || null,
                        id, body.rowVersion,
                    ]
                );
                // The row is locked, so this cannot miss; if it does, nothing is committed.
                if (done.affectedRows !== 1) throw new Error(`pl_submissions ${id} was not submitted at import`);

                await recordAuditStrict(conn, {
                    entityType: 'pl_submission', entityId: id, action: 'import',
                    before: { status: 'submitted', rowVersion: body.rowVersion },
                    after: {
                        status: 'imported', packingListId, container: P.describeContainer(container), onBoard: g.onBoard, force: body.force,
                        supplierKey, supplierName, companySupplierIds: g.company.supplierIds, groupSupplierIds: g.group.supplierIds,
                        replacesId: body.replacesId, refiledPackingListId: refiledId, extractionReused: analysed, note: body.note,
                        suggested: L.containerFromColumns(lockedRow, 'suggested'), matchConfidence: lockedRow.match_confidence || null, matchReason: lockedRow.match_reason || null,
                    },
                    userEmail: req.userEmail,
                });
                return { packingListId, analysed, supersededId: body.replacesId, restoredId };
            });
            if (out.fail) return refuse(res, out.fail);

            // 4. The supplier's page: this submission (confirmed), the one
            // whose packing list was superseded (replaced) and, when the
            // retired filing was itself a replacement, the version it put
            // back (confirmed again, as after DELETE /packing-lists/:id).
            // Best effort.
            try {
                await PL.syncPortalResults(pool, { packingListIds: [out.packingListId, out.supersededId, out.restoredId] });
            } catch (e) {
                log.warn('[POST /pl-submissions/:id/import] portal sync failed', { id, error: e.message });
            }
            const reply = await withConnection(async (conn) => {
                const after = await readRow(conn, id);
                const [pl] = await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [out.packingListId]);
                return { submission: toJson(after, { supplierKey: supplierKeyOf(after) }), packingList: packingListJson(pl[0]) };
            });
            res.json(reply);

            // 5. No usable extraction: read the file again, after the response.
            if (!out.analysed) startCheck(req, out.packingListId);
        } catch (error) {
            log.error('[POST /pl-submissions/:id/import]', error);
            if (!res.headersSent) res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    // POST /api/v1/pl-submissions/:id/reject — body { rowVersion, note }.
    // Request a correction: the note is required and shown to the supplier
    // word for word. Nothing is filed; the supplier uploads a corrected
    // version (which the matcher then suggests the same container for).
    // 200 { submission }
    // 400 BAD_FIELD · 403 CANNOT_DECIDE · 404 NOT_FOUND · 409 NOT_SUBMITTED | STALE | CONFLICT
    app.post('/api/v1/pl-submissions/:id(\\d+)/reject', async (req, res) => {
        try {
            if (!L.canDecide(req.userType)) return refuse(res, CANNOT_DECIDE(req.userType));
            const id = Number(req.params.id);
            const parsed = L.parseRejectBody(req.body);
            if (parsed.error) return refuse(res, { status: 400, code: 'BAD_FIELD', error: parsed.error });
            const body = parsed.value;
            await ready();

            const out = await inTransaction('POST /pl-submissions/:id/reject', async (conn) => {
                const jp = jportalSchema();
                const [locked] = await conn.query(`SELECT * FROM ${jp}.pl_submissions WHERE id = ? FOR UPDATE`, [id]);
                const refusal = stateRefusal(locked[0], id, body.rowVersion);
                if (refusal) return { fail: refusal };
                const [done] = await conn.query(
                    `UPDATE ${jp}.pl_submissions
                        SET status = 'rejected', decided_by = ?, decided_at = UTC_TIMESTAMP(), decided_note = ?,
                            row_version = row_version + 1
                      WHERE id = ? AND status = 'submitted' AND row_version = ?`,
                    [req.userEmail || 'unknown', body.note, id, body.rowVersion]
                );
                if (done.affectedRows !== 1) throw new Error(`pl_submissions ${id} was not submitted at reject`);
                await recordAuditStrict(conn, {
                    entityType: 'pl_submission', entityId: id, action: 'reject',
                    before: { status: 'submitted', rowVersion: body.rowVersion },
                    after: { status: 'rejected', note: body.note, suggested: L.containerFromColumns(locked[0], 'suggested'), matchConfidence: locked[0].match_confidence || null },
                    userEmail: req.userEmail,
                });
                return {};
            });
            if (out.fail) return refuse(res, out.fail);

            const reply = await withConnection(async (conn) => {
                const after = await readRow(conn, id);
                return { submission: toJson(after, { supplierKey: supplierKeyOf(after) }) };
            });
            res.json(reply);
        } catch (error) {
            log.error('[POST /pl-submissions/:id/reject]', error);
            res.status(500).json({ error: 'An internal error occurred.' });
        }
    });

    return { candidateToJson, acknowledgedOf };
}

module.exports = { registerPlSubmissionRoutes, candidateToJson, storedCandidates, acknowledgedOf, containerJson };
