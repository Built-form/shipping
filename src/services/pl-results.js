'use strict';

// The supplier-facing result of a portal packing-list submission
// (jportal.pl_submissions.result_json, v:1), and the suggestion behind it.
// Shared by the plCheck Lambda (the provisional result), the plRefresh Lambda
// (the page-open refresh), the staff routes and the hooks that fire when a
// packing list or the draft it was filed in is deleted. Rules and the pure
// parts: src/lib/pl-submissions.js, src/lib/pl-match.js.
//
// What a row gets, by status (computeSupplierResult):
//   draft, check succeeded — re-scored from extract_json: a confident
//           suggestion gives 'provisional' with the pooled check against the
//           suggested container; anything else 'unmatched'. The match columns
//           (candidates_json, match_*, suggested_*) are written too.
//   imported — the linked packing list is followed (resolveStoredContainer, so
//           a draft is followed into its booked container):
//           · the container holds the group's live lines: 'confirmed' with the
//             pooled check, lines staff signed off marked accepted;
//           · it has lines but none of the group's (a forced import):
//             'confirmed' with container null and no line table;
//           · it is gone (a deleted draft, one closed without a booking to
//             follow, a booked reference with no live orders), or the packing
//             list was deleted: back to Pending (status 'submitted', the
//             confirmed_* and decided_* columns cleared, packing_list_id kept
//             as the previous filing, result 'in_review');
//           · the packing list was superseded: 'replaced'.
//   submitted, rejected, withdrawn, discarded — frozen: what the supplier saw
//           when sending stays until staff decide. A no-op.
// A database or other error never changes the row: it propagates to the
// caller, which logs it, and the last result stays.
//
// Every write is guarded (writeSupplierResult): a draft on status, check
// state, row_version and check_attempt as read; an imported row on status
// and packing_list_id. row_version is bumped only when the stored result
// changed (a canonical-JSON hash), so a page-open refresh that finds nothing
// new is invisible, and a submit that lands mid-refresh makes the refresh
// write 0 rows. Refresh never writes submitted rows, so an open staff Import
// can't go STALE under it.
//
// The supplier group (portalGroup): the set of supplier ids the uploader has
// access to, which the portal writes into group_supplier_ids at create
// (supplier_id = the lowest of them). That column is the authority and is
// never written here; a legacy row with none is the group of its own
// supplier_id. The group's names are the live suppliers.name of those ids,
// and a container line is the group's when its order's or PO's supplier name
// is one of them (packing-list-check.js lineInGroup).
//
// The companies (groupCompanies): one login may hold more than one company
// (Sunmed's three spellings and Medoffice). The group's ids are split the way
// the staff check groups a container's lines — the ids sharing one
// suppliers.portal_code are one company, keyed 's:<lowest live id sharing
// it>' — and a packing list is one company's: the matcher scores each
// company's containers apart and keeps the one whose order lines the document
// fits (suggestContainer), and an import files it under the company whose
// lines in the chosen container it fits (filingCompany), with that key and
// name, so the staff container check finds it beside those lines. Each
// company is group-shaped, and everything below takes one where it takes a
// group. A company's documents are the packing_lists rows under its key or
// 's:<id>' for any of its granted ids (the key portal imports used before).
//
// Supplier isolation: everything here is built from the uploader's group
// only — expected lines are loadGroupLines for the container, pooled
// documents only active packing_lists rows under the group's (or company's)
// keys (plus the row's own filing), sign-offs for those keys, and `elsewhere`
// only placements whose order is the group's by name.
//
// The pool has one connection: callers taking `pool` acquire it per step and
// never nest (syncPortalResults runs after the caller's commit).

const crypto = require('crypto');
const P = require('./packing-list-check');
const R = require('./packing-review');
const PM = require('../lib/pl-match');
const L = require('../lib/pl-submissions');
const { jportalSchema } = require('../lib/jportal');
const { recordAudit } = require('../lib/audit');
const log = require('../lib/logger');

const parseJson = L.parseJson;
const num = v => (v === null || v === undefined ? null : Number(v));
const uniqIds = ids => [...new Set((ids || []).map(Number).filter(n => Number.isInteger(n) && n > 0))];

// Acquire from a pool, or use the connection given.
async function withConn(db, fn) {
    if (db && typeof db.getConnection === 'function') {
        const conn = await db.getConnection();
        try { return await fn(conn); } finally { conn.release(); }
    }
    return fn(db);
}

// ── Rows ─────────────────────────────────────────────────────────────────
// The columns the result needs; the routes' SELECTs add their own.
const RESULT_COLUMNS = `id, status, check_status, check_attempt, row_version, supplier_id, supplier_name, filename,
        extract_json, group_supplier_ids, packing_list_id, replaces_submission_id,
        candidates_json, match_confidence, match_reason, suggested_kind, suggested_container_number, suggested_draft_id, suggested_label,
        confirmed_kind, confirmed_container_number, confirmed_draft_id, confirmed_label,
        verdict, discrepancy_count, result_json, result_state`;

async function loadSubmission(conn, id) {
    const [rows] = await conn.query(`SELECT ${RESULT_COLUMNS} FROM ${jportalSchema()}.pl_submissions WHERE id = ?`, [id]);
    return rows[0] || null;
}

// ── The group ────────────────────────────────────────────────────────────
/**
 * The supplier group behind a row (see the header): its ids as the portal
 * wrote them (group_supplier_ids, else [supplier_id]) and the live name of
 * each. `names` is empty when none of the ids has a live suppliers row — the
 * callers' SUPPLIER_NOT_FOUND. Nothing here reads portal_code.
 * → { supplierIds: number[], names: string[], suppliers: [{ id, name }] }
 */
async function portalGroup(conn, row) {
    const supplierIds = L.parseGroupSupplierIds(row);
    if (!supplierIds.length) return { supplierIds, names: [], suppliers: [] };
    const [rows] = await conn.query(
        `SELECT id, name FROM suppliers WHERE id IN (?) AND deleted_at IS NULL ORDER BY id`, [supplierIds]
    );
    const suppliers = rows.map(r => ({ id: Number(r.id), name: r.name }));
    return { supplierIds, names: [...new Set(suppliers.map(s => s.name).filter(n => n && String(n).trim()))], suppliers };
}

// A group with no live supplier (the callers' SUPPLIER_NOT_FOUND).
const groupGone = group => !group || !group.names.length;

// The packing_lists.supplier_key values of a group: 's:<id>' for each granted
// id, and a company's own key (groupCompanies) first.
const groupKeys = group => [...new Set([
    ...(group?.key ? [group.key] : []),
    ...L.parseGroupSupplierIds({ group_supplier_ids: group?.supplierIds || [] }).map(id => `s:${id}`),
])];

// ── The companies ────────────────────────────────────────────────────────
/**
 * A group split into companies (see the header) by the staff grouping itself
 * (packing-list-check.js resolveSupplierNames, today the ids sharing one
 * suppliers.portal_code, keyed 's:<lowest live id sharing it>'), so the split
 * and the filing key follow the staff check wherever it moves. A name it does
 * not resolve is its own company. Each is group-shaped ({ supplierIds, names,
 * suppliers }: its granted ids) plus { id, key, name }: name is that id's own
 * name when it is granted, else the lowest granted id's. Lowest id first.
 * → [{ id, key, name, supplierIds, names, suppliers }]
 */
async function groupCompanies(conn, group) {
    const suppliers = group?.suppliers || [];
    if (!suppliers.length) return [];
    const resolve = await P.resolveSupplierNames(conn, suppliers.map(s => s.name));
    const byId = new Map();
    for (const s of suppliers) {
        const id = Number(resolve.registered(s.name)?.groupId ?? s.id);
        let c = byId.get(id);
        if (!c) {
            c = { id, key: `s:${id}`, name: null, supplierIds: [], names: [], suppliers: [] };
            byId.set(id, c);
        }
        c.supplierIds.push(Number(s.id));
        c.suppliers.push({ id: Number(s.id), name: s.name });
        if (s.name && String(s.name).trim() && !c.names.includes(s.name)) c.names.push(s.name);
    }
    for (const c of byId.values()) c.name = (c.suppliers.find(s => s.id === c.id) || c.suppliers[0]).name ?? null;
    return [...byId.values()].sort((a, b) => a.id - b.id);
}

// The company a packing list is filed under, by its supplier_key: the
// company's own key, or 's:<id>' of one of its ids. Null when none.
const companyForKey = (companies, key) => companies.find(c => groupKeys(c).includes(key)) || null;

/**
 * The company a packing list is filed under in one container (import,
 * preview): of the group's companies with lines there, the one whose lines
 * the document fits best (the matcher's score, pl-match pickCompany); then
 * the most units there, then the lowest id. With none on board: the one the
 * printed name names, else the one holding `supplierId` (the row's), else the
 * first. Null only for a group with no live supplier.
 */
async function filingCompany(conn, { group, container, extracted = null, supplierId = null }) {
    const companies = await groupCompanies(conn, group);
    if (companies.length <= 1) return companies[0] || null;
    const { rows } = await P.queryContainerLines(conn, container);
    const docs = extracted ? [{ packingListId: null, filename: null, extracted }] : [];
    const onBoard = [];
    for (const c of companies) {
        const lines = rows.filter(P.groupLineFilter(c)).map(r => ({ ...r, quantity: Number(r.quantity) || 0 }));
        if (!lines.length) continue;
        const best = docs.length ? PM.scoreOf({ ...PM.ratiosOf(P.compareDocuments(lines, docs)), boxHit: false, refHit: false, replacesHit: false }) : 0;
        onBoard.push({ id: c.id, names: c.names, best, units: lines.reduce((s, l) => s + l.quantity, 0), company: c });
    }
    const printedName = extracted?.supplierName ?? null;
    if (onBoard.length) {
        const picked = PM.pickCompany({ companies: onBoard, printedName });
        if (picked) return picked.company;
        return [...onBoard].sort((a, b) => b.units - a.units || a.id - b.id)[0].company;
    }
    const named = PM.pickCompany({ companies: companies.map(c => ({ id: c.id, names: c.names, best: 0, company: c })), printedName });
    return named?.company || companies.find(c => c.supplierIds.includes(Number(supplierId))) || companies[0];
}

// The group's lines there and the active packing lists filed under the
// group's keys: what the supplier's result pools. `includePackingListIds`
// are pooled whatever their key (an imported row's own filing);
// `excludePackingListIds` are left out (a replace preview).
async function groupPool(conn, container, group, { includePackingListIds = [], excludePackingListIds = [] } = {}) {
    const expected = await P.loadGroupLines(conn, container, group);
    const keys = groupKeys(group);
    const exclude = new Set(uniqIds(excludePackingListIds));
    const packingLists = [];
    for (const key of keys) packingLists.push(...await R.loadPackingLists(conn, container, { supplierKey: key, includeSuperseded: false }));
    // The row's own filing, when it sits under another key: still the
    // supplier's document.
    const missing = uniqIds(includePackingListIds).filter(id => !packingLists.some(r => Number(r.id) === id));
    if (missing.length) {
        const [own] = await conn.query(`SELECT * FROM packing_lists WHERE id IN (?) AND deleted_at IS NULL AND superseded_by_id IS NULL`, [missing]);
        packingLists.push(...own);
    }
    const kept = packingLists.filter(r => !exclude.has(Number(r.id))).sort((a, b) => Number(b.id) - Number(a.id));
    const signOffKeys = [...new Set([...keys, ...kept.map(r => r.supplier_key).filter(Boolean)])];
    return { expected, keys, signOffKeys, packingLists: kept };
}

// ── The suggestion ───────────────────────────────────────────────────────
// Where the submission this one replaces was filed, as the container is now
// (a draft followed into its booking). Null when it was never imported.
async function replacesContainerFor(conn, row) {
    const replacesId = num(row.replaces_submission_id);
    if (!replacesId) return null;
    const [subs] = await conn.query(`SELECT packing_list_id FROM ${jportalSchema()}.pl_submissions WHERE id = ?`, [replacesId]);
    const plId = num(subs[0]?.packing_list_id);
    if (!plId) return null;
    const [pls] = await conn.query(
        `SELECT container_kind, container_number, draft_container_id, container_name FROM packing_lists WHERE id = ?`, [plId]
    );
    return pls.length ? P.resolveStoredContainer(conn, pls[0]) : null;
}

/**
 * Score the candidate containers for one extraction and decide. With more
 * than one company in the group, each company's containers are scored apart
 * and the company the document fits is kept (pl-match pickCompany); when
 * nothing tells them apart the whole group is scored as one, company null.
 * Logs the structured line CloudWatch Insights groups on (not with `quiet`:
 * the staff detail re-scores live, and that must not count as a check run).
 * → { match (pl-match scoreCandidates), winnerContainer, loaded
 *     (loadSupplierCandidates), company (groupCompanies' entry, or null),
 *     logFields }
 */
async function suggestContainer(conn, { group, extracted, filename = null, replacesContainer = null, submissionId = null, quiet = false }) {
    const evidence = PM.evidenceFromExtraction(extracted);
    const doc = { packingListId: null, filename, extracted };
    const score = async (scope) => {
        const loaded = await P.loadSupplierCandidates(conn, scope, { ...evidence, replacesContainer });
        const candidates = loaded.candidates.map(c => ({
            key: c.key, container: c.container, boxNumbers: c.boxNumbers, reservedRef: c.reservedRef,
            comparison: P.compareDocuments(c.lines, [doc]),
        }));
        const match = PM.scoreCandidates({
            candidates, printedNumbers: evidence.printedNumbers,
            replacesContainerKey: loaded.evidence.replacesContainerKey, foreignBoxes: loaded.knownBoxes,
        });
        return { loaded, match };
    };
    const companies = await groupCompanies(conn, group);
    let company = companies.length === 1 ? companies[0] : null;
    let run = null;
    if (companies.length > 1) {
        const runs = [];
        for (const c of companies) runs.push({ company: c, ...(await score(c)) });
        const picked = PM.pickCompany({
            companies: runs.map(r => ({ id: r.company.id, names: r.company.names, best: r.match.candidates[0]?.score ?? 0, run: r })),
            printedName: extracted?.supplierName ?? null,
        });
        if (picked) {
            run = picked.run;
            company = run.company;
        }
    }
    const { loaded, match } = run || await score(company || group);
    const winnerContainer = match.winner ? loaded.candidates.find(c => c.key === match.winner.key)?.container ?? null : null;
    const logFields = {
        submissionId, ...match.logFields,
        capCut: loaded.capCut, codeFilterFallback: loaded.codeFilterFallback, totalContainers: loaded.totalContainers,
        company: company?.key ?? null, companies: companies.length,
    };
    if (!quiet) log.info('[pl-match] scored', logFields);
    return { match, winnerContainer, loaded, company, logFields };
}

// The match column group from a suggestion, for the UPDATE and candidates_json
// ({ top: the top 5 with their evidence, conflicts: the matcher's top-level
// list — foreign_box is only there, never on a candidate — foreignBox }).
function matchColumns(s) {
    const w = s.winnerContainer;
    return {
        candidates: { top: s.match.candidates, conflicts: s.match.conflicts, foreignBox: s.match.foreignBox || [] },
        confidence: s.match.confidence,
        reason: s.match.reason,
        suggested: {
            kind: w ? w.kind : null,
            containerNumber: w ? w.containerNumber ?? null : null,
            draftId: w && w.kind === 'draft' ? w.draftId ?? null : null,
            label: w ? w.label ?? null : null,
        },
    };
}

// ── The pooled check ─────────────────────────────────────────────────────
// Every active document on file for the container under the group's keys
// (groupPool), plus the one being checked (a draft submission, not yet a
// packing_lists row), pooled and compared with the sign-offs for those keys
// laid over — what buildSupplierCheck does for one staff key. Each invoice
// is counted once (P.onePerInvoice): the supplier's own file — the one being
// checked, or an imported row's own filing — stands in for any other copy on
// file, which is reported in duplicateInvoices rather than counted again.
// excludePackingListIds: the staff preview of an import with
// replacesId drops the version being replaced out of the pool;
// includePackingListIds: an imported row's own filing, whatever its key.
// → { comparison (null with nothing to compare), pool (groupPool) }
async function pooledComparison(conn, { container, group, extraDocument = null, excludePackingListIds = [], includePackingListIds = [] }) {
    const pool = await groupPool(conn, container, group, { includePackingListIds, excludePackingListIds });
    const own = new Set(uniqIds(includePackingListIds));
    const readable = pool.packingLists.filter(r => r.status === 'succeeded' && r.extract_json);
    const onFile = readable.map(r => ({
        packingListId: r.id, filename: r.filename, invoiceNumber: r.invoice_number || null,
        extracted: parseJson(r.extract_json), mine: own.has(Number(r.id)),
    }));
    if (extraDocument) onFile.push({ ...extraDocument, mine: true });
    const { documents, duplicateInvoices } = P.onePerInvoice(onFile);
    if (!documents.length) return { comparison: null, pool };
    const signOffs = [];
    for (const key of pool.signOffKeys) signOffs.push(...await R.loadSignOffs(conn, container, key));
    const comparison = await P.buildComparison(conn, { container, expected: pool.expected, documents, signOffs });
    if (duplicateInvoices.length) comparison.duplicateInvoices = duplicateInvoices;
    return { comparison, pool };
}

// The orders among a comparison's `elsewhere` placements that belong to the
// group, re-checked by name — the supplier-safe view keeps only those.
async function groupOrderIds(conn, comparison, group) {
    const ids = uniqIds((comparison?.lines || []).flatMap(l => (l.elsewhere || []).map(e => e.orderId)));
    if (!ids.length) return new Set();
    const [rows] = await conn.query(
        `SELECT o.id, o.supplier, po.supplier AS po_supplier
           FROM orders o LEFT JOIN purchase_orders po ON po.id = o.purchase_order_id
          WHERE o.id IN (?)`,
        [ids]
    );
    const mine = P.groupLineFilter(group);
    return new Set(rows.filter(mine).map(r => Number(r.id)));
}

// The carrier's box number of a booked container, for the supplier's page.
async function boxNumberFor(conn, container) {
    if (!container || container.kind !== 'booked') return null;
    const [rows] = await conn.query(
        `SELECT TRIM(external_container_number) AS box FROM orders
          WHERE deleted_at IS NULL AND container_number = ?
            AND external_container_number IS NOT NULL AND TRIM(external_container_number) <> ''
          LIMIT 1`,
        [container.containerNumber]
    );
    return rows[0]?.box || null;
}

// The company the supplier's page names: only one that is part of a group
// holding more than one, so a one-company login's result is unchanged.
const shownCompany = (group, company) => (company && company.supplierIds.length < (group?.suppliers || []).length ? company : null);

// result_json (v:1): { v, state, container, supplier?, extraction, ...the
// supplier-safe comparison }. supplier: { name } of the company the packing
// list was matched or filed as (shownCompany), one of the group's own names.
async function buildResult(conn, { state, container, extraction, comparison, group, company = null }) {
    const out = {
        v: L.RESULT_VERSION,
        state,
        container: container ? { kind: container.kind, label: container.label ?? null, boxNumber: await boxNumberFor(conn, container) } : null,
        ...(company?.name ? { supplier: { name: company.name } } : {}),
        extraction,
    };
    if (comparison) {
        const allowed = await groupOrderIds(conn, comparison, group);
        Object.assign(out, L.toSupplierComparison(comparison, { groupOrderIds: allowed }));
    }
    return out;
}

// ── Compute ──────────────────────────────────────────────────────────────
const noop = reason => ({ kind: 'noop', reason });

/**
 * What the row should carry now (see the header for the rules). Reads only.
 * → { kind: 'noop', reason }
 *   | { kind: 'result', state, result, verdict, discrepancyCount, container,
 *       match? (draft only: the match column group), logFields? }
 *   | { kind: 'pending', reason, result }  (an imported row to return to Pending)
 */
async function computeSupplierResult(conn, row) {
    if (!row) return noop('not_found');

    if (row.status === 'draft') {
        if (row.check_status !== 'succeeded') return noop('check_not_succeeded');
        const extracted = parseJson(row.extract_json);
        if (!extracted) return noop('no_extraction');
        const group = await portalGroup(conn, row);
        if (groupGone(group)) return noop('supplier_not_found');
        const replacesContainer = await replacesContainerFor(conn, row);
        const s = await suggestContainer(conn, { group, extracted, filename: row.filename, replacesContainer, submissionId: Number(row.id) });
        const scope = s.company || group;
        const extraction = L.toSupplierExtraction(extracted);
        let state = 'unmatched';
        let container = null;
        let comparison = null;
        if (s.match.confidence === 'confident' && s.winnerContainer) {
            state = 'provisional';
            container = s.winnerContainer;
            ({ comparison } = await pooledComparison(conn, {
                container, group: scope, extraDocument: { packingListId: null, filename: row.filename, extracted },
            }));
        }
        const result = await buildResult(conn, { state, container, extraction, comparison, group: scope, company: shownCompany(group, s.company) });
        return {
            kind: 'result', state, result, container,
            verdict: comparison?.summary?.verdict ?? null,
            discrepancyCount: comparison?.summary?.discrepancyCount ?? null,
            match: matchColumns(s),
            logFields: s.logFields,
        };
    }

    if (row.status === 'imported') {
        const plId = num(row.packing_list_id);
        const [pls] = plId ? await conn.query(`SELECT * FROM packing_lists WHERE id = ?`, [plId]) : [[]];
        const pl = pls[0] || null;
        const extraction = L.toSupplierExtraction(parseJson(pl?.extract_json) ?? parseJson(row.extract_json));
        const pending = reason => ({ kind: 'pending', reason, result: { v: L.RESULT_VERSION, state: 'in_review', container: null, extraction } });
        if (!pl) return pending('packing_list_missing');
        if (pl.deleted_at) return pending('packing_list_deleted');
        const confirmed = L.containerFromColumns(row, 'confirmed');
        if (pl.superseded_by_id != null) {
            const result = { v: L.RESULT_VERSION, state: 'replaced', container: confirmed ? { kind: confirmed.kind, label: confirmed.label, boxNumber: null } : null, extraction };
            return { kind: 'result', state: 'replaced', result, container: null, verdict: null, discrepancyCount: null };
        }
        const group = await portalGroup(conn, row);
        const container = await P.resolveStoredContainer(conn, pl);
        // The company it was filed under (its supplier_key).
        let company = companyForKey(await groupCompanies(conn, group), pl.supplier_key);
        let lines = await P.loadGroupLines(conn, container, company || group);
        if (company && !lines.rows.length && lines.containerLines) {
            // Filed under the group's lowest id before imports looked at the
            // lines' company: the group's lines there, as then.
            const all = await P.loadGroupLines(conn, container, group);
            if (all.rows.length) {
                company = null;
                lines = all;
            }
        }
        const scope = company || group;
        const shown = shownCompany(group, company);
        if (!lines.containerLines) return pending('container_gone');
        if (!lines.rows.length) {
            // A forced import: the group's lines aren't allocated here yet
            // (or the group has no live supplier at all).
            const result = await buildResult(conn, { state: 'confirmed', container: null, extraction, comparison: null, group: scope, company: shown });
            return { kind: 'result', state: 'confirmed', result, container: null, verdict: null, discrepancyCount: null };
        }
        const { comparison } = await pooledComparison(conn, { container, group: scope, includePackingListIds: [pl.id] });
        const result = await buildResult(conn, { state: 'confirmed', container, extraction, comparison, group: scope, company: shown });
        return {
            kind: 'result', state: 'confirmed', result, container,
            verdict: comparison?.summary?.verdict ?? null,
            discrepancyCount: comparison?.summary?.discrepancyCount ?? null,
        };
    }

    return noop('frozen');
}

// ── Write ────────────────────────────────────────────────────────────────
// A stable hash of what the result columns would hold, so an unchanged
// refresh is invisible. Keys are sorted: MySQL reorders JSON object keys.
function canonical(v) {
    if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
    if (v && typeof v === 'object') {
        return `{${Object.keys(v).filter(k => v[k] !== undefined).sort().map(k => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`;
    }
    return JSON.stringify(v === undefined ? null : v);
}
const hashOf = v => crypto.createHash('sha1').update(canonical(v)).digest('hex');

function storedMatch(row) {
    return {
        candidates: parseJson(row.candidates_json),
        confidence: row.match_confidence || null,
        reason: row.match_reason || null,
        suggested: {
            kind: row.suggested_kind || null,
            containerNumber: row.suggested_container_number || null,
            draftId: num(row.suggested_draft_id),
            label: row.suggested_label || null,
        },
    };
}

/**
 * Write what computeSupplierResult returned, guarded on the row as it was
 * read (`row`). Audits a change of state ('result_refreshed') and a return
 * to Pending ('returned_to_pending'), entity_type 'pl_submission'.
 * → { written, changed, kind, reason? }
 */
async function writeSupplierResult(db, row, computed) {
    const jp = jportalSchema();
    const id = Number(row.id);
    if (!computed || computed.kind === 'noop') return { written: false, changed: false, kind: 'noop', reason: computed?.reason ?? null };

    if (computed.kind === 'pending') {
        return withConn(db, async (conn) => {
            const [r] = await conn.query(
                `UPDATE ${jp}.pl_submissions
                    SET status = 'submitted',
                        confirmed_kind = NULL, confirmed_container_number = NULL, confirmed_draft_id = NULL, confirmed_label = NULL,
                        decided_by = NULL, decided_at = NULL, decided_note = NULL,
                        verdict = NULL, discrepancy_count = NULL,
                        result_json = ?, result_state = 'in_review', result_at = UTC_TIMESTAMP(), row_version = row_version + 1
                  WHERE id = ? AND status = 'imported' AND packing_list_id <=> ?`,
                [JSON.stringify(computed.result), id, num(row.packing_list_id)]
            );
            const written = r.affectedRows === 1;
            if (written) {
                await recordAudit(conn, {
                    entityType: 'pl_submission', entityId: id, action: 'returned_to_pending', before: null,
                    after: { reason: computed.reason, previousPackingListId: num(row.packing_list_id), confirmedLabel: row.confirmed_label || null },
                    userEmail: null,
                });
            }
            return { written, changed: written, kind: 'pending', reason: computed.reason };
        });
    }

    const verdict = computed.verdict ?? null;
    const discrepancyCount = computed.discrepancyCount ?? null;
    const newHash = hashOf({ result: computed.result, match: computed.match ?? null, verdict, discrepancyCount });
    const oldHash = hashOf({
        result: parseJson(row.result_json), match: computed.match ? storedMatch(row) : null,
        verdict: row.verdict || null, discrepancyCount: num(row.discrepancy_count),
    });
    const changed = newHash !== oldHash;
    const bump = changed ? 1 : 0;
    const resultJson = JSON.stringify(computed.result);

    return withConn(db, async (conn) => {
        let r;
        if (row.status === 'draft') {
            const m = computed.match;
            [r] = await conn.query(
                `UPDATE ${jp}.pl_submissions
                    SET candidates_json = ?, match_confidence = ?, match_reason = ?,
                        suggested_kind = ?, suggested_container_number = ?, suggested_draft_id = ?, suggested_label = ?,
                        verdict = ?, discrepancy_count = ?, result_json = ?, result_state = ?, result_at = UTC_TIMESTAMP(),
                        row_version = row_version + ?
                  WHERE id = ? AND status = 'draft' AND check_status = 'succeeded' AND row_version = ? AND check_attempt = ?`,
                [
                    m ? JSON.stringify(m.candidates) : null, m?.confidence ?? null, m?.reason ?? null,
                    m?.suggested.kind ?? null, m?.suggested.containerNumber ?? null, m?.suggested.draftId ?? null, m?.suggested.label ?? null,
                    verdict, discrepancyCount, resultJson, computed.state, bump,
                    id, Number(row.row_version), Number(row.check_attempt),
                ]
            );
        } else if (row.status === 'imported') {
            [r] = await conn.query(
                `UPDATE ${jp}.pl_submissions
                    SET verdict = ?, discrepancy_count = ?, result_json = ?, result_state = ?, result_at = UTC_TIMESTAMP(),
                        row_version = row_version + ?
                  WHERE id = ? AND status = 'imported' AND packing_list_id <=> ?`,
                [verdict, discrepancyCount, resultJson, computed.state, bump, id, num(row.packing_list_id)]
            );
        } else {
            return { written: false, changed: false, kind: 'result', reason: 'frozen' };
        }
        const written = r.affectedRows === 1;
        if (written && changed && row.result_state !== computed.state) {
            await recordAudit(conn, {
                entityType: 'pl_submission', entityId: id, action: 'result_refreshed', before: { resultState: row.result_state || null },
                after: { resultState: computed.state, verdict, discrepancyCount, matchConfidence: computed.match?.confidence ?? null, suggestedLabel: computed.match?.suggested.label ?? null },
                userEmail: null,
            });
        }
        return { written, changed: written && changed, kind: 'result', state: computed.state };
    });
}

// ── Entry points ─────────────────────────────────────────────────────────
/** Load, compute and write one submission (the plRefresh Lambda). → { id, ...write } */
async function refreshSubmission(pool, submissionId) {
    const row = await withConn(pool, conn => loadSubmission(conn, submissionId));
    if (!row) return { id: Number(submissionId), written: false, changed: false, kind: 'noop', reason: 'not_found' };
    const computed = await withConn(pool, conn => computeSupplierResult(conn, row));
    const out = await writeSupplierResult(pool, row, computed);
    return { id: Number(row.id), ...out };
}

/**
 * Recompute the portal submissions imported as any of `packingListIds`
 * (a deleted PL, the version it restores, a new import, the one it
 * superseded). Call after the commit; each row is best effort and logged.
 * → [{ id, written, changed, kind, reason? | error }]
 */
async function syncPortalResults(pool, { packingListIds = [] } = {}) {
    const ids = uniqIds(packingListIds);
    if (!ids.length) return [];
    let rows;
    try {
        rows = await withConn(pool, async (conn) => {
            const [r] = await conn.query(
                `SELECT ${RESULT_COLUMNS} FROM ${jportalSchema()}.pl_submissions WHERE packing_list_id IN (?) AND status = 'imported'`, [ids]
            );
            return r;
        });
    } catch (e) {
        log.warn('[pl-results] could not list the submissions to sync', { packingListIds: ids, error: e.message });
        return [];
    }
    const out = [];
    for (const row of rows) {
        try {
            const computed = await withConn(pool, conn => computeSupplierResult(conn, row));
            out.push({ id: Number(row.id), ...(await writeSupplierResult(pool, row, computed)) });
        } catch (e) {
            log.warn('[pl-results] sync failed; the last result stays', { submissionId: row.id, error: e.message });
            out.push({ id: Number(row.id), written: false, changed: false, error: e.message });
        }
    }
    return out;
}

/**
 * The draft-close hook: after a close with reason 'deleted' is committed,
 * the portal submissions imported into that draft go back to Pending. Best
 * effort; the refresh path catches a hook that was missed.
 */
async function syncPortalResultsForDraft(pool, { draftId = null, draftName = null } = {}) {
    const ors = [];
    const params = [];
    if (draftId != null) { ors.push('draft_container_id = ?'); params.push(Number(draftId)); }
    if (draftName) { ors.push('container_name = ?'); params.push(String(draftName).trim()); }
    if (!ors.length) return [];
    let ids;
    try {
        ids = await withConn(pool, async (conn) => {
            const [r] = await conn.query(
                `SELECT id FROM packing_lists WHERE deleted_at IS NULL AND container_kind = 'draft' AND source = 'portal' AND (${ors.join(' OR ')})`, params
            );
            return r.map(x => Number(x.id));
        });
    } catch (e) {
        log.warn('[pl-results] could not list the packing lists of a deleted draft', { draftId, draftName, error: e.message });
        return [];
    }
    return syncPortalResults(pool, { packingListIds: ids });
}

module.exports = {
    RESULT_COLUMNS,
    loadSubmission,
    portalGroup,
    groupGone,
    groupKeys,
    groupCompanies,
    companyForKey,
    filingCompany,
    shownCompany,
    groupPool,
    replacesContainerFor,
    suggestContainer,
    matchColumns,
    pooledComparison,
    groupOrderIds,
    buildResult,
    computeSupplierResult,
    canonical,
    hashOf,
    writeSupplierResult,
    refreshSubmission,
    syncPortalResults,
    syncPortalResultsForDraft,
};
