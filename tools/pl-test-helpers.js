'use strict';

// Shared fixtures for the supplier-portal packing-list suites that run
// against the TEST stage (tools/test-pl-check-flow.js,
// tools/test-pl-submissions.js). Not unit tests: they need the test database,
// the portal's test bucket and AWS credentials. The bucket, the fixture
// login and the PDF maker are the PI suites' (tools/pi-test-helpers.js).
//
// jportal.pl_submissions rows are created by the portal, and this app's login
// may not INSERT or DELETE them (docs/pl-submissions-frontend.md), so the
// fixtures go through FIXTURE_DB_USER / FIXTURE_DB_PASSWORD, falling back to
// DB_USER / DB_PASSWORD.

const crypto = require('crypto');
const PI = require('./pi-test-helpers');
const P = require('../src/services/packing-list-check');
const { jportalSchema } = require('../src/lib/jportal');

const { s3, refuseUnlessTest, fixtures, closeFixtures, makePdf, putPortalObject, deletePortalObject, portalObjectExists } = PI;

// Genuinely valid ISO 6346 numbers (the check digit is verified): a number
// that fails it never scores a box hit.
const VALID_BOXES = ['CSQU3054383', 'MRKU4645188', 'TCLU2855831'];

const newKey = (ext = 'pdf') => `pl/${crypto.randomUUID()}.${ext}`;

async function withConn(pool, fn) {
    const conn = await pool.getConnection();
    try { return await fn(conn); } finally { conn.release(); }
}

// Booked containers on the test database holding LIVE lines of a supplier
// that has a live `suppliers` row named exactly as the order's supplier, most
// lines first. → [{ ref, supplierId, supplierName, lines }]
async function liveContainers(pool, { limit = 20 } = {}) {
    const [rows] = await pool.query(
        `SELECT TRIM(o.container_number) AS ref, s.id AS supplier_id, s.name AS supplier_name, COUNT(*) AS n
           FROM orders o
           JOIN suppliers s ON s.deleted_at IS NULL
                           AND CONVERT(s.name USING utf8mb4) COLLATE utf8mb4_0900_bin = CONVERT(o.supplier USING utf8mb4) COLLATE utf8mb4_0900_bin
          WHERE o.deleted_at IS NULL AND o.status IN (?)
            AND o.container_number IS NOT NULL AND TRIM(o.container_number) <> ''
            AND o.jf_code IS NOT NULL AND TRIM(o.jf_code) <> '' AND o.quantity > 0
          GROUP BY ref, s.id, s.name
          ORDER BY n DESC, ref
          LIMIT ${Number(limit)}`,
        [P.LIVE_STATUSES]
    );
    return rows.map(r => ({ ref: r.ref, supplierId: Number(r.supplier_id), supplierName: r.supplier_name, lines: Number(r.n) }));
}

/**
 * A booked container with LIVE lines of one supplier, resolved as the app
 * sees it, with the portal group the uploader would have and its lines.
 *
 * The group is derived from the data, as the portal derives it from the
 * user's grants (one company, every spelling of it): the live `suppliers`
 * rows of the picked supplier's company — the staff picker entry
 * (loadContainerSuppliers) carrying the supplier's name gives the company's
 * staff key, and every live suppliers row the staff grouping
 * (resolveSupplierNames) puts under that key is in the group. On the test
 * database, container 324 gives the three "Suzhou Sunmed" spellings (ids 2,
 * 58, 87) although only one of them is on 324's own lines. The portal writes
 * that set as group_supplier_ids, supplier_id = its lowest id and
 * supplier_name = that row's live name.
 *   exclude: refs to skip · minSuppliers: 2 for a container shared by two
 *   companies (the isolation tests), with `others` filled: the staff picker
 *   entries of the other companies, none of whose names is in the group.
 * → { container, ref, supplierId, supplierName, portalKey ('s:<supplierId>'),
 *     group ({ supplierIds, names }), staffKey (the staff system's key for the
 *     company: what an import files the packing list under), rows (the
 *     group's lines, loadGroupLines), suppliers, others } or null
 */
async function pickContainer(pool, { exclude = [], minSuppliers = 1 } = {}) {
    const candidates = await liveContainers(pool, { limit: 40 });
    for (const c of candidates) {
        if (exclude.includes(c.ref)) continue;
        const found = await withConn(pool, async (conn) => {
            const r = await P.resolveContainer(conn, { containerNumber: c.ref });
            if (!r || r.ambiguous || r.container.kind !== 'booked') return null;
            const suppliers = await P.loadContainerSuppliers(conn, r.container);
            const company = suppliers.find(s => /^s:\d+$/.test(s.supplierKey) && s.names.some(n => P.nameKey(n) === P.nameKey(c.supplierName)));
            if (!company) return null;
            const [allLive] = await conn.query(`SELECT id, name FROM suppliers WHERE deleted_at IS NULL AND name IS NOT NULL AND TRIM(name) <> '' ORDER BY id`);
            const resolve = await P.resolveSupplierNames(conn, allLive.map(s => s.name));
            const live = allLive.filter(s => resolve.registered(s.name)?.key === company.supplierKey);
            if (!live.length) return null;
            const group = { supplierIds: live.map(s => Number(s.id)), names: [...new Set(live.map(s => s.name))] };
            const mine = P.groupLineFilter(group);
            const others = suppliers.filter(s => s.supplierKey !== company.supplierKey && /^s:\d+$/.test(s.supplierKey)
                && !s.names.some(n => mine({ supplier: n })));
            if (suppliers.length < minSuppliers || (minSuppliers > 1 && !others.length)) return null;
            const { rows } = await P.loadGroupLines(conn, r.container, group);
            if (!rows.length) return null;
            const supplierId = group.supplierIds[0];
            return {
                container: r.container, ref: c.ref, supplierId, supplierName: live[0].name, portalKey: `s:${supplierId}`,
                group, staffKey: company.supplierKey, rows, suppliers, others,
            };
        });
        if (found) return found;
    }
    return null;
}

const month = v => (v ? String(v).slice(0, 7) : null);

/**
 * An extraction in readPackingList's shape (RESPONSE_SCHEMA) built from
 * expected lines (loadExpectedLines rows), so a suite needs no Gemini. `vary`
 * is applied to each row after it is built (quantity changes, say).
 */
function extractedFrom(rows, { invoiceNumber = `INV-${Date.now()}`, containerNumbers = [], supplierName = null, vary = null, documentDate = null } = {}) {
    const lines = rows.map((o, i) => {
        const upc = Number(o.units_per_carton) || 0;
        const line = {
            jfCode: o.jf_code, poNumber: o.po_number || null, lotNumber: o.lot_number || null,
            mfgDate: month(o.mfg_date), expDate: month(o.exp_date), description: o.product_name || o.jf_code,
            cartons: upc > 0 ? Math.ceil(Number(o.quantity) / upc) : null, quantity: Number(o.quantity), unit: 'PC',
            grossWeightPerCarton: null, netWeightPerCarton: null, cartonDimensionsCm: null,
        };
        return vary ? vary(line, i, o) || line : line;
    });
    return {
        invoiceNumber, documentDate, supplierName: supplierName || rows[0]?.supplier || null, containerNumbers,
        totalCartons: lines.reduce((s, l) => s + (Number(l.cartons) || 0), 0) || null,
        totalQuantity: lines.reduce((s, l) => s + (Number(l.quantity) || 0), 0),
        totalGrossWeightKg: null, totalNetWeightKg: null, totalCbm: null,
        lines,
    };
}

// The text of a packing list PDF for `extracted` (extractedFrom output), for
// the Gemini path: a header, one row per line, the printed totals.
function plLines(extracted, { title = 'PACKING LIST' } = {}) {
    const out = [
        title, `Supplier: ${extracted.supplierName || ''}`, `Invoice No: ${extracted.invoiceNumber}`,
        ...(extracted.containerNumbers.length ? [`Container No: ${extracted.containerNumbers.join(', ')}`] : []),
        '', 'Item | PO No. | Lot No. | Mfg | Exp | Cartons | Qty (PC)',
    ];
    for (const l of extracted.lines) {
        out.push(`${l.jfCode} ${l.description || ''} | ${l.poNumber || '-'} | ${l.lotNumber || '-'} | ${l.mfgDate || '-'} | ${l.expDate || '-'} | ${l.cartons ?? '-'} | ${l.quantity}`);
    }
    out.push('', `Total cartons: ${extracted.totalCartons ?? '-'}`, `Total quantity: ${extracted.totalQuantity} PC`);
    return out;
}

// Inserts a pl_submissions row as jportal would (create, then whatever
// state the test needs): the caller passes supplier_id, supplier_name and
// group_supplier_ids as the portal writes them at create (pickContainer's
// supplierId, supplierName, group.supplierIds). JSON columns may be objects. → id
async function insertSubmission(fields) {
    const conn = await fixtures();
    const f = {
        user_id: 999999, submitted_by_email: 'pl-test@example.invalid', filename: 'PL-test.pdf',
        content_type: 'application/pdf', file_size: 1, check_status: 'awaiting_upload', status: 'draft', ...fields,
    };
    for (const k of ['extract_json', 'candidates_json', 'group_supplier_ids', 'result_json']) {
        if (f[k] && typeof f[k] !== 'string') f[k] = JSON.stringify(f[k]);
    }
    const cols = Object.keys(f);
    const [ins] = await conn.query(
        `INSERT INTO ${jportalSchema()}.pl_submissions (${cols.join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`,
        cols.map(c => f[c])
    );
    return ins.insertId;
}

// An UPDATE through the fixture login (what the portal would do: submit,
// discard …). → affectedRows
async function fixtureUpdate(setSql, whereSql, params) {
    const conn = await fixtures();
    const [r] = await conn.query(`UPDATE ${jportalSchema()}.pl_submissions SET ${setSql} WHERE ${whereSql}`, params);
    return r.affectedRows;
}

async function readSubmission(pool, id) {
    const [rows] = await pool.query(`SELECT * FROM ${jportalSchema()}.pl_submissions WHERE id = ?`, [id]);
    return rows[0] || null;
}

// A staff packing list on file (as POST /packing-lists + a finished read
// would leave it), for the pooling and isolation tests. → id
async function insertStaffPackingList(pool, { container, supplierKey, supplierName, extracted, filename = `staff-${Date.now()}.pdf` }) {
    const isDraft = container.kind === 'draft';
    const [ins] = await pool.query(
        `INSERT INTO packing_lists
            (container_kind, container_number, draft_container_id, container_name, supplier_key, supplier_name,
             filename, s3_key, content_type, file_size, status, uploaded_by_email, invoice_number, row_count, extract_json, analyzed_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'application/pdf', 1, 'succeeded', 'local@dev', ?, ?, ?, NOW())`,
        [
            container.kind, container.containerNumber || null, isDraft ? container.draftId : null, isDraft ? container.draftName : null,
            supplierKey, supplierName, filename, `packing-lists/test/${crypto.randomUUID()}/${filename}`,
            extracted.invoiceNumber || null, extracted.lines.length, JSON.stringify(extracted),
        ]
    );
    return ins.insertId;
}

// One UPDATE per column group of the shipping grant, under THIS app's login
// (`pool`), so a missing grant fails on test rather than in production. Each
// column is set to itself. group_supplier_ids is not among them: the portal
// writes it at create and shipping never touches it.
// → { check, match, result, decision }: true, or the error
async function grantsProbe(pool, id) {
    const groups = {
        check: ['check_status', 'check_attempt', 'check_started_at', 'error_code', 'error_message', 'checked_at', 'model_used',
            'input_tokens', 'output_tokens', 'total_tokens', 's3_etag', 'file_size', 'extract_json', 'row_count', 'invoice_number'],
        match: ['candidates_json', 'match_confidence', 'match_reason', 'suggested_kind', 'suggested_container_number', 'suggested_draft_id',
            'suggested_label'],
        result: ['verdict', 'discrepancy_count', 'result_json', 'result_state', 'result_at'],
        decision: ['status', 'decided_by', 'decided_at', 'decided_note', 'packing_list_id', 'confirmed_kind', 'confirmed_container_number',
            'confirmed_draft_id', 'confirmed_label', 'row_version'],
    };
    const out = {};
    for (const [name, cols] of Object.entries(groups)) {
        try {
            await pool.query(`UPDATE ${jportalSchema()}.pl_submissions SET ${cols.map(c => `${c} = ${c}`).join(', ')} WHERE id = ?`, [id]);
            out[name] = true;
        } catch (e) {
            out[name] = `${e.code || ''} ${e.message}`.trim();
        }
    }
    return out;
}

// Removes submissions and what an import made of them (packing lists, their
// audit rows, this app's audit rows for the submissions). Imported files in
// the PO bucket are left (the nightly live→test mirror deletes strays).
async function cleanupSubmissions(pool, ids, { packingListIds = [] } = {}) {
    if (!ids.length && !packingListIds.length) return;
    const conn = await fixtures();
    const subs = ids.length ? (await pool.query(`SELECT id, s3_key FROM ${jportalSchema()}.pl_submissions WHERE id IN (?)`, [ids]))[0] : [];
    const pls = ids.length ? (await pool.query(`SELECT id FROM packing_lists WHERE portal_submission_id IN (?)`, [ids]))[0].map(r => r.id) : [];
    const plIds = [...new Set([...pls, ...packingListIds])];
    if (plIds.length) {
        await pool.query(`UPDATE packing_lists SET superseded_by_id = NULL, replaces_id = NULL WHERE superseded_by_id IN (?) OR replaces_id IN (?)`, [plIds, plIds]);
        await pool.query(`DELETE FROM audit_log WHERE entity_type = 'packing_list' AND entity_id IN (?)`, [plIds]);
        await pool.query(`DELETE FROM packing_lists WHERE id IN (?)`, [plIds]);
    }
    if (ids.length) {
        await pool.query(`DELETE FROM audit_log WHERE entity_type = 'pl_submission' AND entity_id IN (?)`, [ids]);
        await conn.query(`DELETE FROM ${jportalSchema()}.pl_submissions WHERE id IN (?)`, [ids]);
    }
    for (const s of subs) await deletePortalObject(s.s3_key);
}

// result_json (v:1) may carry only these keys (src/lib/pl-submissions.js
// toSupplierComparison + buildResult, plus duplicateOf from the check).
const RESULT_KEYS = ['v', 'state', 'container', 'extraction', 'summary', 'extractionCheck', 'documents', 'duplicateInvoices', 'lines', 'linesTruncated', 'duplicateOf'];
const LINE_KEYS = ['status', 'severity', 'accepted', 'jfCode', 'poNumbers', 'productName', 'expected', 'packed', 'differences', 'elsewhere'];
function resultLeaks(result) {
    if (!result || typeof result !== 'object') return ['not an object'];
    const out = Object.keys(result).filter(k => !RESULT_KEYS.includes(k)).map(k => `top: ${k}`);
    for (const l of result.lines || []) {
        for (const k of Object.keys(l)) if (!LINE_KEYS.includes(k)) out.push(`line: ${k}`);
        for (const e of l.elsewhere || []) for (const k of Object.keys(e)) if (!['label', 'quantity'].includes(k)) out.push(`elsewhere: ${k}`);
    }
    const text = JSON.stringify(result);
    for (const needle of ['orderId', 'allocationId', 'fingerprint', 'signedByEmail', 'matchMethod', 'lineKey', 'draftContainerId']) {
        if (text.includes(`"${needle}"`)) out.push(`key: ${needle}`);
    }
    return out;
}

module.exports = {
    s3, refuseUnlessTest, fixtures, closeFixtures, makePdf, putPortalObject, deletePortalObject, portalObjectExists,
    VALID_BOXES, newKey, withConn, liveContainers, pickContainer, extractedFrom, plLines,
    insertSubmission, fixtureUpdate, readSubmission, insertStaffPackingList, grantsProbe, cleanupSubmissions,
    RESULT_KEYS, LINE_KEYS, resultLeaks,
};
