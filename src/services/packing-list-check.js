'use strict';

// Packing-list check — reads a supplier's packing list for one container with
// Gemini, then compares every line against the orders we expect that supplier
// to have in that container, and reports each difference.
//
// The model is NOT shown what we expect. A verification tool that hands the
// model the answer invites it to "find" it, so extraction is blind and every
// comparison happens here, in code, where it is deterministic and testable
// (tools/test-packing-list-compare.js).
//
// "The container" is the line items IN that container — never the purchase
// order. A PO is often split across containers, so the expected quantity is
// this container's share only:
//   booked — the live orders with container_number = ref. Packing a partial
//            quantity splits the order row, so orders.quantity already IS this
//            container's share (container 324: 24 rows, 53,120 units).
//   draft  — draft_container_allocations for that draft, at `allocated`. The
//            order row still holds the whole line (or the remainder) and one
//            order can sit in two drafts (45,000 in TAM 320 + 114,300 in 321).
// Both read the legacy tables (the source of truth on prod); the shipments
// shadow is not used. Planned containers are not supported: a packing list
// only exists once goods are packed.
//
// "The supplier" is a company, not a spelling: the three Sunmed names ("SUNMED", "Suzhou Sunmed
// Co., Ltd", "Suzhou Sunmed Co.,Ltd.") share one suppliers.portal_code and so
// one packing list. Groups are keyed 's:<lowest supplier id sharing the code>'
// — never the code itself, which is the supplier-portal credential. A name with
// no suppliers row falls back to 'n:<lower-cased name>'.
//
// The pool has a single connection, so the model call happens with none held:
// load, release, call Gemini, reacquire, write (same as shipment-payment-extract).

const crypto = require('crypto');
const { GoogleGenAI } = require('@google/genai');
const log = require('../lib/logger');
const { fetchInvoicePdf } = require('./po-invoice-check');
const { withGeminiRetry } = require('./supplier-email-check');
const { recordAudit } = require('../lib/audit');
const S = require('../lib/shipments');

// gemini-3-flash-preview returned truncated JSON on a 7-page Sunmed list;
// 3.8 Flash is the default, Pro the fallback. Env override for retuning.
const GEMINI_MODEL_DEFAULT = process.env.PACKING_LIST_GEMINI_MODEL || 'gemini-3.8-flash';
const FALLBACK_MODEL = 'gemini-3.1-pro-preview';

// ── Prompt + schema ──────────────────────────────────────────────────────
const SYSTEM_INSTRUCTION = `You read supplier PACKING LISTS for a UK medical-supplies importer and return structured JSON. The document is a PDF or image, often several pages, printed from a spreadsheet, sometimes bilingual (Chinese/English).

Return one entry in "lines" for EVERY packing row on the document, in document order. Do NOT merge rows: if the same product appears on two rows (e.g. "16CTN 8000BOX" and then "1CTN 80BOX" as a part carton), return two lines.

For each line:
- jfCode: the importer's product code — "JF" or "HW" followed by digits, e.g. "JF0799", "HW0168". Usually the first column. Copy it as printed but uppercase with no spaces. If a row prints a short form such as "JF923" next to "JF0923", prefer the 4-digit form. Null only if there is genuinely no code.
- poNumber: the purchase-order reference for THIS row, verbatim — typically "PO_00297J", but suppliers sometimes use their own reference such as "SUNMED-101". It is usually a line inside the description block. Null if the row has none. Never copy one row's PO onto another row.
- lotNumber: the lot / batch number, digits as printed (keep leading zeros), e.g. "0798014". Null if absent.
- mfgDate / expDate: manufacture and expiry dates. Return "YYYY-MM" when only month and year are printed ("EXP DATE 07.2031" -> "2031-07"), "YYYY-MM-DD" when a full date is printed. Null if absent.
- description: the product description, condensed to one line (drop UDI / barcode numbers).
- cartons: integer number of cartons on this row ("25CTN" -> 25). Null if not stated.
- quantity: integer quantity on this row ("2500BOX" -> 2500). This is the TOTAL for the row, not per carton.
- unit: the quantity unit as printed, uppercase: "BOX", "PKT", "PC", "PCS", "ROLL", "SET"...
- grossWeightPerCarton / netWeightPerCarton: kg per carton (a leading "@" means per carton). If the document only gives a row total, divide by cartons. Null if absent.
- cartonDimensionsCm: carton measurement as printed, e.g. "59.5X25X45". Values wrapped across lines ("@59.5X25X4" then "5") belong together -> "59.5X25X45". Null if absent.

Also return the document-level fields: invoiceNumber, documentDate (YYYY-MM-DD), supplierName (the issuing company as printed), containerNumbers (any container / seal / B/L numbers printed), and the printed TOTAL row: totalCartons, totalQuantity, totalGrossWeightKg, totalNetWeightKg, totalCbm. Null any that are not printed — never compute a total yourself.

Copy digits exactly. Better to return null than to guess.`;

const RESPONSE_SCHEMA = {
    type: 'object',
    additionalProperties: false,
    properties: {
        invoiceNumber: { type: ['string', 'null'] },
        documentDate: { type: ['string', 'null'] },
        supplierName: { type: ['string', 'null'] },
        containerNumbers: { type: 'array', items: { type: 'string' } },
        totalCartons: { type: ['integer', 'null'] },
        totalQuantity: { type: ['integer', 'null'] },
        totalGrossWeightKg: { type: ['number', 'null'] },
        totalNetWeightKg: { type: ['number', 'null'] },
        totalCbm: { type: ['number', 'null'] },
        lines: {
            type: 'array',
            items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                    jfCode: { type: ['string', 'null'] },
                    poNumber: { type: ['string', 'null'] },
                    lotNumber: { type: ['string', 'null'] },
                    mfgDate: { type: ['string', 'null'] },
                    expDate: { type: ['string', 'null'] },
                    description: { type: ['string', 'null'] },
                    cartons: { type: ['integer', 'null'] },
                    quantity: { type: ['integer', 'null'] },
                    unit: { type: ['string', 'null'] },
                    grossWeightPerCarton: { type: ['number', 'null'] },
                    netWeightPerCarton: { type: ['number', 'null'] },
                    cartonDimensionsCm: { type: ['string', 'null'] },
                },
                required: [
                    'jfCode', 'poNumber', 'lotNumber', 'mfgDate', 'expDate', 'description', 'cartons',
                    'quantity', 'unit', 'grossWeightPerCarton', 'netWeightPerCarton', 'cartonDimensionsCm',
                ],
            },
        },
    },
    required: [
        'invoiceNumber', 'documentDate', 'supplierName', 'containerNumbers', 'totalCartons',
        'totalQuantity', 'totalGrossWeightKg', 'totalNetWeightKg', 'totalCbm', 'lines',
    ],
};

// ── Normalisers ──────────────────────────────────────────────────────────
const norm = s => (s == null ? '' : String(s).toUpperCase().replace(/[\s\-_.]/g, '').trim());
// Lots are zero-padded inconsistently (00124 vs 124) — same rule as the QC matcher.
const normLot = s => norm(s).replace(/^0+/, '');
// A QC-sample order (JF0197_FQC) is the same physical product as JF0197 and
// usually travels on the same packing-list row, so both group under the base code.
const baseJf = s => norm(String(s ?? '').replace(/_FQC$/i, ''));

// Same normalisation as shipment-payments.supplierKey (payment_rules).
const nameKey = name => String(name ?? '').trim().replace(/\s+/g, ' ').toLowerCase();

// 'YYYY-MM' from 'YYYY-MM', 'YYYY-MM-DD', 'MM.YYYY', 'MM/YYYY', or a Date.
// Packing lists print month precision, so every date compares by month.
function toMonth(v) {
    if (v == null || v === '') return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString().slice(0, 7);
    const t = String(v).trim();
    let m = t.match(/^(\d{4})-(\d{1,2})(?:-\d{1,2})?/);
    if (m) return `${m[1]}-${m[2].padStart(2, '0')}`;
    m = t.match(/^(\d{1,2})[./-](\d{4})$/);
    if (m) return `${m[2]}-${m[1].padStart(2, '0')}`;
    return null;
}

const num = v => (v == null || v === '' ? null : (Number.isFinite(Number(v)) ? Number(v) : null));

// "59.5X25X45" / "59.5 x 25 x 45 cm" → [25, 45, 59.5] (sorted, so a carton
// measured in a different orientation still compares equal).
function parseDims(v) {
    if (v == null) return null;
    const parts = String(v).toUpperCase().replace(/CM/g, '').split(/[X×*]/).map(p => Number(p.trim()));
    if (parts.length !== 3 || parts.some(p => !Number.isFinite(p) || p <= 0)) return null;
    return parts.sort((a, b) => a - b);
}

// ── Supplier groups ──────────────────────────────────────────────────────
// Maps each supplier name to its company group. One query for the lot.
async function resolveSupplierNames(conn, names) {
    const distinct = [...new Set(names.filter(n => n && String(n).trim()).map(n => String(n).trim()))];
    const out = new Map();
    if (distinct.length) {
        const [rows] = await conn.query(
            `SELECT s.name,
                    (SELECT MIN(s2.id) FROM suppliers s2
                      WHERE s2.portal_code = s.portal_code AND s2.deleted_at IS NULL) AS group_id,
                    s.id
               FROM suppliers s
              WHERE s.deleted_at IS NULL AND s.name IN (?)`,
            [distinct]
        );
        for (const r of rows) {
            out.set(nameKey(r.name), { key: `s:${r.group_id ?? r.id}`, groupId: Number(r.group_id ?? r.id) });
        }
    }
    const resolve = name => {
        const k = nameKey(name);
        if (!k) return null;
        return out.get(k) || { key: `n:${k}`, groupId: null };
    };
    // Only a real suppliers row, never the 'n:' fallback.
    resolve.registered = name => {
        const k = nameKey(name);
        return k ? out.get(k) || null : null;
    };
    return resolve;
}

// A line's company group. A registered name wins wherever it is: the PO often
// carries an unregistered short form ('ISO', 'ROOSIN') while the order has the
// full registered name, and preferring the PO blindly split one company into
// two picker entries. Only when neither is registered does the PO's name key it.
function groupFor(resolve, poSupplier, orderSupplier) {
    return resolve.registered(poSupplier) || resolve.registered(orderSupplier)
        || resolve(poSupplier) || resolve(orderSupplier) || { key: 'n:', groupId: null };
}

// The display name for each 's:<id>' group: that supplier row's own name.
async function groupDisplayNames(conn, groupIds) {
    const ids = [...new Set(groupIds.filter(Boolean))];
    if (!ids.length) return new Map();
    const [rows] = await conn.query(`SELECT id, name FROM suppliers WHERE id IN (?)`, [ids]);
    return new Map(rows.map(r => [Number(r.id), r.name]));
}

// Turns a caller's supplier choice (supplierKey from /context, a suppliers.id,
// or a plain name) into a group key.
async function resolveSupplierChoice(conn, { supplierKey, supplierId, supplierName }) {
    // 'n:' alone is the group of lines with no supplier name at all.
    if (typeof supplierKey === 'string' && /^(s:\d+|n:.*)$/.test(supplierKey.trim())) {
        return supplierKey.trim();
    }
    if (supplierId != null && supplierId !== '' && Number.isFinite(Number(supplierId))) {
        const [rows] = await conn.query(
            `SELECT (SELECT MIN(s2.id) FROM suppliers s2
                      WHERE s2.portal_code = s.portal_code AND s2.deleted_at IS NULL) AS group_id, s.id
               FROM suppliers s WHERE s.id = ?`,
            [Number(supplierId)]
        );
        if (!rows.length) return null;
        return `s:${rows[0].group_id ?? rows[0].id}`;
    }
    if (typeof supplierName === 'string' && supplierName.trim()) {
        const resolve = await resolveSupplierNames(conn, [supplierName]);
        return resolve(supplierName).key;
    }
    return null;
}

// ── Container resolution ─────────────────────────────────────────────────
// A resolved container is one of:
//   { kind: 'booked', containerNumber, label }
//   { kind: 'draft', draftId, draftName, containerNumber (the number the name
//     reserves, e.g. '328', or null), lineCount, units, label }
// plus followedFrom when a draft the caller named has since been booked.

// Allocation lines per draft name (soft-deleted orders don't count —
// DRAFT_ALLOC_SELECT hides them too), with the registry id when there is one.
// lineCount/units count only lines still open; a line whose order is already
// booked (packed, close not yet run) is in bookedLines/bookedRefs instead.
const OPEN_LINE = `(o.container_number IS NULL OR TRIM(o.container_number) = '')`;
async function draftsWithLines(conn, whereSql = '', params = []) {
    const [rows] = await conn.query(
        `SELECT dca.draft_container_name AS name, dc.id AS draft_id,
                SUM(${OPEN_LINE}) AS line_count,
                SUM(CASE WHEN ${OPEN_LINE} THEN dca.allocated ELSE 0 END) AS units,
                SUM(NOT ${OPEN_LINE}) AS booked_lines,
                GROUP_CONCAT(DISTINCT CASE WHEN NOT ${OPEN_LINE} THEN TRIM(o.container_number) END SEPARATOR '\\n') AS booked_refs
           FROM draft_container_allocations dca
           JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
           LEFT JOIN draft_containers dc ON dc.name = dca.draft_container_name
          ${whereSql}
          GROUP BY dca.draft_container_name, dc.id`,
        params
    );
    return rows.map(r => ({
        kind: 'draft',
        draftId: r.draft_id != null ? Number(r.draft_id) : null,
        draftName: r.name,
        containerNumber: S.parseNameHint(r.name)?.reference ?? null,
        lineCount: Number(r.line_count) || 0,
        units: Number(r.units) || 0,
        bookedLines: Number(r.booked_lines) || 0,
        bookedRefs: r.booked_refs ? String(r.booked_refs).split('\n').filter(Boolean) : [],
    }));
}

async function bookedRefFor(conn, ref) {
    const r = String(ref ?? '').trim();
    if (!r) return null;
    const [own] = await conn.query(
        `SELECT container_number FROM orders WHERE container_number = ? AND deleted_at IS NULL LIMIT 1`, [r]
    );
    if (own.length) return own[0].container_number.trim();
    // The carrier's box number ('MRKU4645188') for a booked container.
    const [ext] = await conn.query(
        `SELECT container_number FROM orders
          WHERE external_container_number = ? AND deleted_at IS NULL
            AND container_number IS NOT NULL AND container_number <> ''
          LIMIT 1`,
        [r]
    );
    return ext.length ? ext[0].container_number.trim() : null;
}

const bookedContainer = (containerNumber, followedFrom = null) => ({
    kind: 'booked', containerNumber, label: containerNumber, ...(followedFrom ? { followedFrom } : {}),
});
const withLabel = ({ bookedLines, bookedRefs, ...d }) => ({ ...d, label: d.draftName });

// A draft named by the caller, followed to wherever its lines are now. Closing
// a draft deletes its allocations, so a converted draft is found through the
// container the registry recorded, else through the number its name reserved.
// A draft packed in full but never closed still has its allocation rows, all
// pointing at orders already in one container: that container is followed too.
async function followDraft(conn, { draftId = null, draftName }) {
    const [live] = await draftsWithLines(conn, 'WHERE dca.draft_container_name = ?', [draftName]);
    if (live && live.lineCount > 0) return withLabel({ ...live, draftId: live.draftId ?? draftId });

    const from = { kind: 'draft', draftId, draftName };
    if (live && live.bookedRefs.length === 1) {
        const ref = await bookedRefFor(conn, live.bookedRefs[0]);
        if (ref) return bookedContainer(ref, from);
    }
    if (draftId != null) {
        const [reg] = await conn.query(
            `SELECT closed_reason, container_number FROM draft_containers WHERE id = ?`, [draftId]
        );
        const cn = reg[0]?.closed_reason === 'converted' ? reg[0].container_number : null;
        const ref = cn ? await bookedRefFor(conn, cn) : null;
        if (ref) return bookedContainer(ref, from);
    }
    const hint = S.parseNameHint(draftName);
    const ref = hint ? await bookedRefFor(conn, hint.reference) : null;
    if (ref) return bookedContainer(ref, from);
    return withLabel({ kind: 'draft', draftId, draftName, containerNumber: hint?.reference ?? null, lineCount: 0, units: 0 });
}

/**
 * Resolve what the caller typed or picked to one container.
 *   draftContainerId — draft_containers.id (stable across renames).
 *   containerNumber  — free text: a booked reference ('324'), the carrier box
 *                      ('MRKU4645188'), an exact draft name, or the number a
 *                      draft's name reserves ('328').
 * Returns { container } | { ambiguous: [candidates] } | null. A booked
 * container wins over a draft reserving the same number; pass draftContainerId
 * to pick the draft.
 */
async function resolveContainer(conn, { containerNumber, draftContainerId } = {}) {
    if (draftContainerId != null && draftContainerId !== '') {
        const id = Number(draftContainerId);
        if (!Number.isInteger(id) || id <= 0) return null;
        const [reg] = await conn.query(`SELECT id, name FROM draft_containers WHERE id = ?`, [id]);
        if (!reg.length) return null;
        return { container: await followDraft(conn, { draftId: id, draftName: reg[0].name }) };
    }

    const raw = String(containerNumber ?? '').trim();
    if (!raw) return null;

    const ref = await bookedRefFor(conn, raw);
    if (ref) return { container: bookedContainer(ref) };

    // An exact draft name (case-insensitive by collation), open or converted.
    // An empty one is kept as a last resort so the caller hears "empty", not
    // "not found", when no draft with lines reserves the same text.
    let emptyByName = null;
    const [byName] = await conn.query(`SELECT id, name FROM draft_containers WHERE name = ? LIMIT 1`, [raw]);
    if (byName.length) {
        const c = await followDraft(conn, { draftId: Number(byName[0].id), draftName: byName[0].name });
        if (c.kind === 'booked' || c.lineCount > 0) return { container: c };
        emptyByName = c;
    } else {
        // An allocation name with no registry row: only its own lines count,
        // never a number guessed from arbitrary text.
        const [exact] = await draftsWithLines(conn, 'WHERE dca.draft_container_name = ?', [raw]);
        if (exact && exact.lineCount > 0) return { container: withLabel(exact) };
        if (exact && exact.bookedRefs.length === 1) {
            const ref2 = await bookedRefFor(conn, exact.bookedRefs[0]);
            if (ref2) return { container: bookedContainer(ref2, { kind: 'draft', draftId: null, draftName: exact.draftName }) };
        }
    }
    const fallback = emptyByName ? { container: emptyByName } : null;

    // The number an open draft's name reserves. Drafts with no open lines are
    // ignored — the registry is full of them and they would make every number
    // ambiguous.
    const want = parseTypedReference(raw);
    if (!want) return fallback;
    const matches = (await draftsWithLines(conn)).filter(d => {
        if (!d.lineCount) return false;
        const hint = S.parseNameHint(d.draftName);
        if (!hint) return false;
        // Compare mode + number, never raw text ('123. air freight' is AIR 123).
        // A bare number also finds an AIR draft: '123' → '… - 123. Airfreight urgent'.
        return hint.seq === want.seq && (hint.mode === want.mode || want.bare);
    });
    if (matches.length === 1) return { container: withLabel(matches[0]) };
    if (matches.length > 1) return { ambiguous: matches.map(withLabel) };
    return fallback;
}

// A typed reference as { seq, mode, bare }: '328' (bare, SEA), '123. Air
// Freight' / '123. airfreight' / '123 air freight' (AIR). Null for anything else.
function parseTypedReference(raw) {
    const t = String(raw ?? '').trim();
    let m = t.match(/^(\d{1,4})$/);
    if (m) return { seq: Number(m[1]), mode: 'SEA', bare: true };
    m = t.match(/^(\d{1,4})\.?\s*air\s*freight$/i);
    if (m) return { seq: Number(m[1]), mode: 'AIR', bare: false };
    return null;
}

// Re-resolve a stored packing list's container, for the live comparison and
// the background check. A draft follows its registry id through renames, and
// on to its booked container once converted.
async function resolveStoredContainer(conn, row) {
    if (row.container_kind !== 'draft') return bookedContainer(row.container_number);
    const draftId = row.draft_container_id != null ? Number(row.draft_container_id) : null;
    let name = row.container_name;
    if (draftId != null) {
        const [reg] = await conn.query(`SELECT name FROM draft_containers WHERE id = ?`, [draftId]);
        if (reg.length) name = reg[0].name;
    }
    return followDraft(conn, { draftId, draftName: name });
}

// ── Expected lines ───────────────────────────────────────────────────────
const LINE_COLS = `
           o.id, o.jf_code, o.product_name, o.po_number, o.lot_number,
           o.units_per_carton, o.carton_weight, o.carton_height, o.carton_width, o.carton_depth,
           o.mfg_date, o.exp_date, o.status, o.supplier, o.container_number,
           o.quantity AS order_quantity, po.supplier AS po_supplier`;

// Every line in the container, each tagged with its supplier group (groupFor),
// with `quantity` = what THIS container should hold (see the header).
// Returns { rows, skipped }: skipped are draft lines whose order is already
// booked into a container (the gap between the legacy pack and close calls, or
// a close that never ran) — counting them too would double-count those units.
async function loadContainerLines(conn, container) {
    let rows;
    const skipped = [];
    if (container.kind === 'booked') {
        [rows] = await conn.query(
            `SELECT ${LINE_COLS}, o.quantity AS quantity, NULL AS allocation_id
               FROM orders o
               LEFT JOIN purchase_orders po ON po.id = o.purchase_order_id
              WHERE o.deleted_at IS NULL AND o.container_number = ?
              ORDER BY o.jf_code, o.po_number, o.id`,
            [container.containerNumber]
        );
    } else {
        const [all] = await conn.query(
            `SELECT ${LINE_COLS}, dca.allocated AS quantity, dca.id AS allocation_id
               FROM draft_container_allocations dca
               JOIN orders o ON o.id = dca.order_id AND o.deleted_at IS NULL
               LEFT JOIN purchase_orders po ON po.id = o.purchase_order_id
              WHERE dca.draft_container_name = ?
              ORDER BY o.jf_code, o.po_number, o.id`,
            [container.draftName]
        );
        rows = [];
        for (const r of all) {
            const cn = r.container_number && String(r.container_number).trim();
            (cn ? skipped : rows).push(r);
        }
    }
    const resolve = await resolveSupplierNames(conn, [...rows, ...skipped].flatMap(r => [r.po_supplier, r.supplier]));
    const tag = r => {
        const g = groupFor(resolve, r.po_supplier, r.supplier);
        return { ...r, quantity: Number(r.quantity) || 0, supplier_group_key: g.key, supplier_group_id: g.groupId };
    };
    return { rows: rows.map(tag), skipped: skipped.map(tag) };
}

// The supplier picker for one container: one entry per company on board.
async function loadContainerSuppliers(conn, container) {
    const { rows } = await loadContainerLines(conn, container);
    const groups = new Map();
    for (const o of rows) {
        let g = groups.get(o.supplier_group_key);
        if (!g) {
            g = { supplierKey: o.supplier_group_key, groupId: o.supplier_group_id, names: new Set(), poNumbers: new Set(), orderCount: 0, units: 0 };
            groups.set(o.supplier_group_key, g);
        }
        for (const n of [o.po_supplier, o.supplier]) if (n && n.trim()) g.names.add(n.trim());
        if (o.po_number) g.poNumbers.add(o.po_number);
        g.orderCount += 1;
        g.units += o.quantity;
    }
    const display = await groupDisplayNames(conn, [...groups.values()].map(g => g.groupId));
    return [...groups.values()]
        .map(g => ({
            supplierKey: g.supplierKey,
            supplierName: (g.groupId && display.get(g.groupId)) || [...g.names][0] || '(no supplier)',
            names: [...g.names].sort(),
            poNumbers: [...g.poNumbers].sort(),
            orderCount: g.orderCount,
            units: g.units,
        }))
        .sort((a, b) => b.units - a.units);
}

// This supplier's lines in the container, plus its skipped (already booked) ones.
async function loadExpectedLines(conn, container, supplierKey) {
    const { rows, skipped } = await loadContainerLines(conn, container);
    return {
        rows: rows.filter(o => o.supplier_group_key === supplierKey),
        skipped: skipped.filter(o => o.supplier_group_key === supplierKey),
    };
}

// ── Where the rest is ────────────────────────────────────────────────────
// Every other place the same product (and PO, when known) sits: other booked
// containers, other drafts, planned containers, and what is still unallocated
// at the factory. Answers "packed 2,000 but we expected 1,720 — where is the
// rest of that PO line?" and "this row isn't expected here — where is it?".
async function findPlacements(conn, { jfCode, poNumbers = [], container }) {
    const base = baseJf(jfCode);
    if (!base) return [];
    const [orders] = await conn.query(
        `SELECT o.id, o.jf_code, o.po_number, o.lot_number, o.quantity, o.status, o.container_number
           FROM orders o
          WHERE o.deleted_at IS NULL
            AND UPPER(REPLACE(REPLACE(REPLACE(o.jf_code,' ',''),'-',''),'_','')) IN (?, ?)
          ORDER BY o.id DESC
          LIMIT 200`,
        [base, `${base}FQC`]
    );
    const wantPos = new Set(poNumbers.map(norm).filter(Boolean));
    const relevant = orders.filter(o => !wantPos.size || wantPos.has(norm(o.po_number)));
    if (!relevant.length) return [];

    const ids = relevant.map(o => o.id);
    const [drafts] = await conn.query(
        `SELECT order_id, draft_container_name AS name, allocated FROM draft_container_allocations WHERE order_id IN (?)`, [ids]
    );
    let planned = [];
    try {
        [planned] = await conn.query(
            `SELECT order_id, planned_container_name AS name, allocated FROM planned_container_allocations WHERE order_id IN (?)`, [ids]
        );
    } catch (e) {
        if (e.errno !== 1146) throw e;   // no planned table on an old schema
    }

    const thisDraft = container.kind === 'draft' ? container.draftName : null;
    const thisRef = container.kind === 'booked' ? container.containerNumber : null;
    const out = [];
    const about = o => ({ orderId: o.id, jfCode: o.jf_code, poNumber: o.po_number || null, lotNumber: o.lot_number || null, status: o.status });
    for (const o of relevant) {
        const cn = o.container_number && String(o.container_number).trim();
        if (cn) {
            if (cn !== thisRef) out.push({ kind: 'booked', where: cn, quantity: Number(o.quantity) || 0, ...about(o) });
            continue;
        }
        let placed = 0;
        for (const a of drafts.filter(d => d.order_id === o.id)) {
            placed += Number(a.allocated) || 0;
            if (a.name !== thisDraft) out.push({ kind: 'draft', where: a.name, quantity: Number(a.allocated) || 0, ...about(o) });
        }
        for (const a of planned.filter(p => p.order_id === o.id)) {
            placed += Number(a.allocated) || 0;
            out.push({ kind: 'planned', where: a.name, quantity: Number(a.allocated) || 0, ...about(o) });
        }
        // Received or destroyed stock (a spent QC sample) isn't waiting anywhere.
        const rest = (Number(o.quantity) || 0) - placed;
        if (rest > 0 && !S.TERMINAL_STATUSES.has(o.status)) out.push({ kind: 'unallocated', where: null, quantity: rest, ...about(o) });
    }
    return out.slice(0, 20);
}

// ── The comparison (pure) ────────────────────────────────────────────────
const SEVERITY_RANK = { error: 3, warning: 2, info: 1 };
const WEIGHT_TOLERANCE = kg => Math.max(0.25, kg * 0.05);   // kg per carton
const DIM_TOLERANCE_CM = 1;

function expectedCartons(o) {
    const q = Number(o.quantity) || 0;
    const upc = Number(o.units_per_carton) || 0;
    return upc > 0 ? Math.ceil(q / upc) : null;
}

const uniq = arr => [...new Set(arr.filter(v => v != null && v !== ''))];

function sameSet(a, b) {
    if (a.length !== b.length) return false;
    const s = new Set(a);
    return b.every(v => s.has(v));
}

// Compares one matched group of order rows against one group of packing rows.
function compareGroup(orders, packed, matchMethod) {
    const differences = [];
    const add = (field, severity, expected, actual, message) => differences.push({ field, severity, expected, actual, message });

    const expQty = orders.reduce((s, o) => s + (Number(o.quantity) || 0), 0);
    const packedQtyKnown = packed.every(p => p.quantity != null);
    const packQty = packed.reduce((s, p) => s + (Number(p.quantity) || 0), 0);
    if (!packedQtyKnown) {
        add('quantity', 'warning', expQty, null, 'The packing list does not state a quantity for every row.');
    } else if (packQty !== expQty) {
        const diff = packQty - expQty;
        add('quantity', 'error', expQty, packQty,
            `Packed ${packQty.toLocaleString('en-GB')}, expected ${expQty.toLocaleString('en-GB')} (${diff > 0 ? '+' : ''}${diff.toLocaleString('en-GB')}).`);
    }

    // Lots — by the zero-insensitive form, displayed as printed.
    const expLots = uniq(orders.map(o => o.lot_number));
    const packLots = uniq(packed.map(p => p.lotNumber));
    if (packLots.length && expLots.length && !sameSet(expLots.map(normLot), packLots.map(normLot))) {
        add('lot', 'error', expLots, packLots, `Lot ${packLots.join(', ')} on the packing list; we have ${expLots.join(', ')}.`);
    } else if (packLots.length && !expLots.length) {
        add('lot', 'warning', [], packLots, `Lot ${packLots.join(', ')} on the packing list; no lot recorded on the order.`);
    } else if (!packLots.length && expLots.length) {
        add('lot', 'warning', expLots, [], `No lot on the packing list; we have ${expLots.join(', ')}.`);
    }

    // Dates — month precision; a date the packing list does not print is not a difference.
    for (const [field, label, col, key, severity] of [
        ['expDate', 'Expiry', 'exp_date', 'expDate', 'error'],
        ['mfgDate', 'Manufacture date', 'mfg_date', 'mfgDate', 'warning'],
    ]) {
        const packM = uniq(packed.map(p => toMonth(p[key])));
        if (!packM.length) continue;
        const expM = uniq(orders.map(o => toMonth(o[col])));
        if (!expM.length) {
            add(field, 'info', [], packM, `${label} ${packM.join(', ')} on the packing list; none recorded on the order.`);
        } else if (!sameSet(expM, packM)) {
            add(field, severity, expM, packM, `${label} ${packM.join(', ')} on the packing list; we have ${expM.join(', ')}.`);
        }
    }

    // Cartons — only when every order has a units-per-carton to derive them
    // from. Rounded up once per carton size, not per order row: split siblings
    // (500 + 5) or a line plus its FQC sample share their part carton.
    const packCtnKnown = packed.every(p => p.cartons != null);
    const packCtn = packed.reduce((s, p) => s + (Number(p.cartons) || 0), 0);
    let expCtn = null;
    if (orders.every(o => Number(o.units_per_carton) > 0)) {
        const byUpc = new Map();
        for (const o of orders) {
            const upc = Number(o.units_per_carton);
            byUpc.set(upc, (byUpc.get(upc) || 0) + (Number(o.quantity) || 0));
        }
        expCtn = [...byUpc].reduce((s, [upc, q]) => s + Math.ceil(q / upc), 0);
    }
    if (expCtn != null && packCtnKnown && packCtn !== expCtn) {
        add('cartons', 'warning', expCtn, packCtn, `${packCtn} cartons on the packing list; ${expCtn} expected from units per carton.`);
    }

    // Carton weight and size against what the order carries — informational.
    const refOrder = orders.find(o => num(o.carton_weight) != null || num(o.carton_height) != null) || null;
    if (refOrder) {
        const expW = num(refOrder.carton_weight);
        const packW = uniq(packed.map(p => num(p.grossWeightPerCarton)));
        if (expW != null && packW.length && packW.some(w => Math.abs(w - expW) > WEIGHT_TOLERANCE(expW))) {
            add('cartonWeight', 'info', expW, packW.length === 1 ? packW[0] : packW,
                `Carton gross weight ${packW.join(', ')} kg on the packing list; ${expW} kg on the order.`);
        }
        const expD = [num(refOrder.carton_height), num(refOrder.carton_width), num(refOrder.carton_depth)];
        if (expD.every(v => v != null && v > 0)) {
            const expSorted = [...expD].sort((a, b) => a - b);
            const packD = packed.map(p => parseDims(p.cartonDimensionsCm)).filter(Boolean);
            const off = packD.find(d => d.some((v, i) => Math.abs(v - expSorted[i]) > DIM_TOLERANCE_CM));
            if (off) {
                add('cartonDimensions', 'info', expSorted.join('x'), off.join('x'),
                    `Carton ${off.join(' x ')} cm on the packing list; ${expSorted.join(' x ')} cm on the order.`);
            }
        }
    }

    if (matchMethod === 'jf_only') {
        const expPos = uniq(orders.map(o => o.po_number));
        const packPos = uniq(packed.map(p => p.poNumber));
        add('poNumber', 'warning', expPos, packPos, packPos.length
            ? `PO ${packPos.join(', ')} on the packing list; the order${expPos.length > 1 ? 's are' : ' is'} on ${expPos.join(', ')}.`
            : `No PO on the packing list row; matched on product code to ${expPos.join(', ')}.`);
    }
    if (matchMethod === 'no_code') {
        const jf = baseJf(orders[0].jf_code) || orders[0].jf_code;
        const byPo = packed.some(p => norm(p.poNumber) && orders.some(o => norm(o.po_number) === norm(p.poNumber)));
        const desc = packed[0].description ? ` ("${String(packed[0].description).slice(0, 60)}")` : '';
        add('productCode', 'warning', jf, null,
            `No product code on the packing-list row${desc}; matched to ${jf} by ${byPo ? 'PO' : 'quantity'} — check it is the same product.`);
    }

    return { expQty, packQty, expCtn, packCtn, differences };
}

function orderSummary(o) {
    return {
        orderId: o.id,
        jfCode: o.jf_code,
        poNumber: o.po_number || null,
        lotNumber: o.lot_number || null,
        // This container's share; orderQuantity is the whole order row (they
        // differ for a draft line split with another container).
        quantity: Number(o.quantity) || 0,
        orderQuantity: o.order_quantity != null ? Number(o.order_quantity) : Number(o.quantity) || 0,
        allocationId: o.allocation_id != null ? Number(o.allocation_id) : null,
        cartons: expectedCartons(o),
        unitsPerCarton: o.units_per_carton != null ? Number(o.units_per_carton) : null,
        mfgDate: toMonth(o.mfg_date),
        expDate: toMonth(o.exp_date),
        status: o.status,
        productName: o.product_name || null,
    };
}

function packedSummary(p) {
    return {
        index: p.index,
        packingListId: p.packingListId ?? null,
        jfCode: p.jfCode || null,
        poNumber: p.poNumber || null,
        lotNumber: p.lotNumber || null,
        quantity: p.quantity != null ? Number(p.quantity) : null,
        unit: p.unit || null,
        cartons: p.cartons != null ? Number(p.cartons) : null,
        mfgDate: toMonth(p.mfgDate),
        expDate: toMonth(p.expDate),
        grossWeightPerCarton: num(p.grossWeightPerCarton),
        cartonDimensionsCm: p.cartonDimensionsCm || null,
        description: p.description || null,
    };
}

function worstSeverity(diffs) {
    let worst = null;
    for (const d of diffs) if (!worst || SEVERITY_RANK[d.severity] > SEVERITY_RANK[worst]) worst = d.severity;
    return worst;
}

// A line's identity across recomputes: base product code + the PO(s) it was
// matched under. Sign-offs are keyed by it, so they survive a re-read, a
// replaced packing list and a changed quantity alike.
const lineKeyFor = (jf, pos) => `${baseJf(jf)}|${uniq(pos.map(norm)).sort().join('+')}`;

// What a sign-off vouches for: the line's status and every difference that
// isn't informational. If any of that changes, the sign-off no longer applies.
function lineFingerprint(line) {
    const parts = (line.differences || [])
        .filter(d => d.severity !== 'info')
        .map(d => `${d.field}=${JSON.stringify(d.expected ?? null)}>${JSON.stringify(d.actual ?? null)}`)
        .sort();
    return crypto.createHash('sha1').update(`${line.status}|${parts.join('|')}`).digest('hex');
}

/**
 * Pure: expected order rows (LINE_COLS shape) × the documents on file for the
 * supplier → { summary, lines, documents, extractionCheck }.
 *
 * `documents` is [{ packingListId, filename, extracted }]. A supplier who ships
 * one container under two invoices sends two packing lists; their rows are
 * pooled and compared together, so neither shows the other's lines as
 * missing. Each packed row remembers which document it came from.
 *
 * Grouping is by (base product code, PO). A row that finds no group with its
 * PO falls back to the same product code on any PO still unmatched, with a PO
 * warning — so a packing list that omits or misprints PO numbers still lines up.
 */
function compareDocuments(expectedOrders, documents) {
    const docs = (documents || []).map((d, docIndex) => ({ ...d, docIndex }));
    const packedRows = docs.flatMap(d =>
        ((d.extracted && d.extracted.lines) || []).map((l, index) => ({
            ...l, index, packingListId: d.packingListId ?? null, filename: d.filename ?? null, docIndex: d.docIndex,
        }))
    );

    const groupBy = (items, keyFn) => {
        const m = new Map();
        for (const it of items) {
            const k = keyFn(it);
            if (!m.has(k)) m.set(k, []);
            m.get(k).push(it);
        }
        return m;
    };
    const orderGroups = groupBy(expectedOrders, o => `${baseJf(o.jf_code)}|${norm(o.po_number)}`);
    const packGroups = groupBy(packedRows, p => `${baseJf(p.jfCode)}|${norm(p.poNumber)}`);

    const lines = [];
    const pushMatched = (orders, packed, matchMethod) => {
        const c = compareGroup(orders, packed, matchMethod);
        const severity = worstSeverity(c.differences);
        lines.push({
            lineKey: lineKeyFor(orders[0].jf_code, orders.map(o => o.po_number)),
            status: c.differences.some(d => d.severity !== 'info') ? 'mismatch' : 'match',
            severity,
            matchMethod,
            jfCode: baseJf(orders[0].jf_code) || orders[0].jf_code,
            poNumbers: uniq(orders.map(o => o.po_number)),
            productName: orders[0].product_name || packed[0].description || null,
            expected: { quantity: c.expQty, cartons: c.expCtn, orders: orders.map(orderSummary) },
            packed: { quantity: c.packQty, cartons: c.packCtn, unit: uniq(packed.map(p => p.unit)).join('/') || null, rows: packed.map(packedSummary) },
            differences: c.differences,
        });
    };

    // Pass 1 — product code + PO.
    for (const [k, packed] of [...packGroups]) {
        const orders = orderGroups.get(k);
        if (!orders || !baseJf(packed[0].jfCode)) continue;
        pushMatched(orders, packed, 'jf_po');
        orderGroups.delete(k);
        packGroups.delete(k);
    }

    // Pass 2 — product code alone, across whatever is left of that code.
    const leftoverPackByJf = groupBy([...packGroups.values()].flat().filter(p => baseJf(p.jfCode)), p => baseJf(p.jfCode));
    const leftoverOrdersByJf = groupBy([...orderGroups.values()].flat(), o => baseJf(o.jf_code));
    for (const [jf, packed] of leftoverPackByJf) {
        const orders = leftoverOrdersByJf.get(jf);
        if (!orders) continue;
        pushMatched(orders, packed, 'jf_only');
        leftoverOrdersByJf.delete(jf);
        for (const p of packed) {
            const k = `${baseJf(p.jfCode)}|${norm(p.poNumber)}`;
            const rest = (packGroups.get(k) || []).filter(x => x !== p);
            if (rest.length) packGroups.set(k, rest); else packGroups.delete(k);
        }
    }

    // Pass 3 — rows with no product code. A supplier that prints only its own
    // description ("Tape", 10,000 SET) still lines up when the row's PO, or
    // failing that its quantity, points at exactly one expected line.
    const dropPacked = (p) => {
        const k = `${baseJf(p.jfCode)}|${norm(p.poNumber)}`;
        const rest = (packGroups.get(k) || []).filter(x => x !== p);
        if (rest.length) packGroups.set(k, rest); else packGroups.delete(k);
    };
    for (const p of [...packGroups.values()].flat().filter(x => !baseJf(x.jfCode))) {
        const left = [...leftoverOrdersByJf.entries()];
        const po = norm(p.poNumber);
        let candidates = po ? left.filter(([, orders]) => orders.some(o => norm(o.po_number) === po)) : [];
        if (candidates.length !== 1) {
            const q = Number(p.quantity);
            candidates = q > 0 ? left.filter(([, orders]) => orders.reduce((s, o) => s + (Number(o.quantity) || 0), 0) === q) : [];
        }
        if (candidates.length !== 1) continue;
        const [jf, orders] = candidates[0];
        pushMatched(orders, [p], 'no_code');
        leftoverOrdersByJf.delete(jf);
        dropPacked(p);
    }

    // Expected but not on any packing list.
    for (const orders of leftoverOrdersByJf.values()) {
        for (const group of groupBy(orders, o => norm(o.po_number)).values()) {
            const qty = group.reduce((s, o) => s + (Number(o.quantity) || 0), 0);
            lines.push({
                lineKey: lineKeyFor(group[0].jf_code, [group[0].po_number]),
                status: 'missing',
                severity: 'error',
                matchMethod: null,
                jfCode: baseJf(group[0].jf_code) || group[0].jf_code,
                poNumbers: uniq(group.map(o => o.po_number)),
                productName: group[0].product_name || null,
                expected: { quantity: qty, cartons: null, orders: group.map(orderSummary) },
                packed: null,
                differences: [{
                    field: 'line', severity: 'error', expected: qty, actual: 0,
                    message: `Expected ${qty.toLocaleString('en-GB')} in this container; not on the packing list.`,
                }],
            });
        }
    }

    // On a packing list but not expected (from this supplier, in this container).
    for (const packed of packGroups.values()) {
        const qty = packed.reduce((s, p) => s + (Number(p.quantity) || 0), 0);
        const ctn = packed.reduce((s, p) => s + (Number(p.cartons) || 0), 0);
        lines.push({
            lineKey: lineKeyFor(packed[0].jfCode || '', [packed[0].poNumber]),
            status: 'unexpected',
            severity: 'error',
            matchMethod: null,
            jfCode: baseJf(packed[0].jfCode) || packed[0].jfCode || null,
            poNumbers: uniq(packed.map(p => p.poNumber)),
            productName: packed[0].description || null,
            expected: null,
            packed: { quantity: qty, cartons: ctn, unit: uniq(packed.map(p => p.unit)).join('/') || null, rows: packed.map(packedSummary) },
            differences: [{
                field: 'line', severity: 'error', expected: 0, actual: qty,
                message: packed[0].jfCode
                    ? `${qty.toLocaleString('en-GB')} packed; not expected from this supplier in this container.`
                    : 'A packing-list row with no product code.',
            }],
            elsewhere: [],
        });
    }

    const ORDER = { missing: 0, unexpected: 1, mismatch: 2, match: 3 };
    lines.sort((a, b) => ORDER[a.status] - ORDER[b.status] || String(a.jfCode).localeCompare(String(b.jfCode)));
    for (const l of lines) l.fingerprint = lineFingerprint(l);

    // Does the model's reading of each document add up to that document's own
    // printed totals? If not, a row was probably missed or misread — say so
    // before anyone trusts the differences.
    const docChecks = docs.map(d => {
        const rows = packedRows.filter(p => p.docIndex === d.docIndex);
        const sumQty = rows.reduce((s, p) => s + (Number(p.quantity) || 0), 0);
        const sumCtn = rows.reduce((s, p) => s + (Number(p.cartons) || 0), 0);
        const printedQty = d.extracted?.totalQuantity ?? null;
        const printedCtn = d.extracted?.totalCartons ?? null;
        return {
            packingListId: d.packingListId ?? null,
            filename: d.filename ?? null,
            invoiceNumber: d.extracted?.invoiceNumber ?? null,
            rows: rows.length,
            printedTotalQuantity: printedQty,
            sumOfRowsQuantity: sumQty,
            printedTotalCartons: printedCtn,
            sumOfRowsCartons: sumCtn,
            ok: (printedQty == null || printedQty === sumQty) && (printedCtn == null || printedCtn === sumCtn),
        };
    });
    const sumQty = packedRows.reduce((s, p) => s + (Number(p.quantity) || 0), 0);
    const sumCtn = packedRows.reduce((s, p) => s + (Number(p.cartons) || 0), 0);
    const printedOr = (k, sumK) => (docChecks.every(c => c[k] == null) ? null : docChecks.reduce((s, c) => s + (c[k] ?? c[sumK]), 0));
    const extractionCheck = {
        printedTotalQuantity: printedOr('printedTotalQuantity', 'sumOfRowsQuantity'),
        sumOfRowsQuantity: sumQty,
        printedTotalCartons: printedOr('printedTotalCartons', 'sumOfRowsCartons'),
        sumOfRowsCartons: sumCtn,
        ok: docChecks.every(c => c.ok),
    };

    const count = s => lines.filter(l => l.status === s).length;
    const expectedUnits = expectedOrders.reduce((s, o) => s + (Number(o.quantity) || 0), 0);
    const discrepancyCount = lines.filter(l => l.status !== 'match').length;
    const summary = {
        verdict: discrepancyCount > 0 || !extractionCheck.ok ? 'differences' : 'match',
        lines: lines.length,
        matched: count('match'),
        mismatched: count('mismatch'),
        missingFromPackingList: count('missing'),
        notExpected: count('unexpected'),
        discrepancyCount,
        signedOff: 0,
        outstanding: discrepancyCount,
        expectedUnits,
        packedUnits: sumQty,
        expectedOrders: expectedOrders.length,
        packingListRows: packedRows.length,
        documents: docs.length,
    };

    return { summary, lines, documents: docChecks, extractionCheck };
}

/** One document (the original shape); see compareDocuments. */
function comparePackingList(expectedOrders, extracted) {
    return compareDocuments(expectedOrders, [{ packingListId: null, filename: null, extracted }]);
}

/**
 * Pure: lay the sign-offs over a comparison. A sign-off is a person saying
 * "this difference is fine" for one line — it holds only while the line's
 * status and differences are exactly what was signed (its fingerprint). A
 * line that has since changed keeps its sign-off as `stale`, and counts as
 * outstanding again. The verdict becomes 'accepted' once every difference is
 * signed off (the reading check must be clean too).
 */
function applySignOffs(comparison, signOffs) {
    const byKey = new Map();
    for (const s of signOffs || []) if (s && s.lineKey) byKey.set(s.lineKey, s);
    let signedOff = 0;
    for (const line of comparison.lines) {
        const s = byKey.get(line.lineKey);
        if (line.status === 'match') {
            // Nothing to accept; a leftover sign-off is history, not a state.
            if (s) line.signOff = { ...s, stale: false, moot: true };
            continue;
        }
        if (!s) continue;
        const stale = s.fingerprint !== line.fingerprint;
        line.signOff = { ...s, stale };
        if (!stale) { line.accepted = true; signedOff += 1; }
    }
    const summary = comparison.summary;
    summary.signedOff = signedOff;
    summary.outstanding = summary.discrepancyCount - signedOff;
    summary.verdict = summary.outstanding > 0 || !comparison.extractionCheck.ok
        ? (summary.discrepancyCount > 0 || !comparison.extractionCheck.ok ? 'differences' : 'match')
        : (summary.discrepancyCount > 0 ? 'accepted' : 'match');
    return comparison;
}

// Adds `elsewhere` — where else that product/PO sits — to every line whose
// quantity is off or that isn't expected here, so a split PO line reads as
// "packed 2,000, expected 1,720; the other 2,800 is in 122. Air Freight"
// rather than a bare shortfall. DB reads only.
async function annotateElsewhere(conn, comparison, container) {
    for (const line of comparison.lines) {
        const qtyOff = line.differences.some(d => d.field === 'quantity' && d.severity === 'error');
        if (!line.jfCode || !(line.status === 'unexpected' || qtyOff)) continue;
        // A product-code-only match expected one PO but was packed under
        // another: look for both.
        const poNumbers = line.matchMethod === 'jf_only'
            ? uniq([...line.poNumbers, ...(line.packed?.rows || []).map(r => r.poNumber)])
            : line.poNumbers;
        line.elsewhere = await findPlacements(conn, { jfCode: line.jfCode, poNumbers, container });
    }
    return comparison;
}

// A plain description of the container for the payload.
function describeContainer(c) {
    return {
        kind: c.kind,
        containerNumber: c.containerNumber ?? null,
        draftContainerId: c.kind === 'draft' ? c.draftId ?? null : null,
        draftName: c.kind === 'draft' ? c.draftName : null,
        label: c.label,
        ...(c.followedFrom ? { followedFrom: c.followedFrom } : {}),
    };
}

/**
 * Compare the supplier's documents ([{ packingListId, filename, extracted }],
 * or one `extracted`) with its lines in the container. Sign-offs, if given,
 * are laid over the result (see applySignOffs).
 */
async function buildComparison(conn, { container, supplierKey, documents, extracted, signOffs }) {
    const { rows, skipped } = await loadExpectedLines(conn, container, supplierKey);
    const docs = documents || [{ packingListId: null, filename: null, extracted }];
    const comparison = compareDocuments(rows, docs);
    applySignOffs(comparison, signOffs || []);
    comparison.container = describeContainer(container);
    // Draft lines whose order is already booked elsewhere were left out rather
    // than double-counted; say so, since the packing list may well include them.
    comparison.skippedLines = skipped.map(s => ({
        orderId: s.id, jfCode: s.jf_code, poNumber: s.po_number || null,
        allocated: s.quantity, bookedIn: String(s.container_number).trim(),
    }));
    return annotateElsewhere(conn, comparison, container);
}

// ── The model call ───────────────────────────────────────────────────────
/** Reads one packing list. Returns { parsed, modelUsed, usage }. */
async function readPackingList({ s3Key, bytes, contentType, model }) {
    if (!process.env.GEMINI_API_KEY) {
        const err = new Error('GEMINI_API_KEY is not configured.');
        err.code = 'NOT_CONFIGURED';
        throw err;
    }
    if (!bytes) {
        try { bytes = await fetchInvoicePdf(s3Key); }
        catch (e) {
            const err = new Error(`Could not read the uploaded packing list: ${e.message}`);
            err.code = 'PDF_FETCH_FAILED';
            throw err;
        }
    }
    const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });
    const models = [...new Set([model || GEMINI_MODEL_DEFAULT, FALLBACK_MODEL])];
    let lastErr;
    for (const modelId of models) {
        try {
            const response = await withGeminiRetry(`packing-list(${modelId})`, () => ai.models.generateContent({
                model: modelId,
                contents: [
                    { text: 'Extract every packing row and the printed totals from this packing list per the schema.' },
                    { inlineData: { mimeType: contentType || 'application/pdf', data: bytes.toString('base64') } },
                ],
                config: {
                    temperature: 0,
                    // ~40 rows × 12 fields: leave room so the JSON is never cut off mid-row.
                    maxOutputTokens: 32768,
                    systemInstruction: SYSTEM_INSTRUCTION,
                    responseMimeType: 'application/json',
                    responseSchema: RESPONSE_SCHEMA,
                },
            }));
            let parsed;
            try { parsed = JSON.parse(response?.text || ''); }
            catch {
                log.warn('[packing-list-check] unparseable JSON', {
                    modelId, finishReason: response?.candidates?.[0]?.finishReason, chars: (response?.text || '').length,
                });
                const err = new Error('The model did not return parseable JSON.');
                err.code = 'MODEL_NO_JSON';
                throw err;
            }
            if (!Array.isArray(parsed.lines)) parsed.lines = [];
            return { parsed, modelUsed: modelId, usage: response?.usageMetadata ?? null };
        } catch (e) {
            lastErr = e;
            log.warn('[packing-list-check] model failed, trying the next', { modelId, error: String(e.message || '').slice(0, 160) });
        }
    }
    const err = new Error(`Gemini call failed: ${lastErr?.message ?? 'unknown error'}`);
    err.code = lastErr?.code === 'MODEL_NO_JSON' ? 'MODEL_NO_JSON' : 'MODEL_CALL_FAILED';
    throw err;
}

// ── Orchestration ────────────────────────────────────────────────────────
/** Read one uploaded packing list and store what it says plus the comparison.
 *  Owns its own connections; never throws — a failure lands on the row. */
async function runPackingListCheck(pool, { id, userEmail = null, model = null } = {}) {
    let row;
    {
        const conn = await pool.getConnection();
        try {
            const [rows] = await conn.query(`SELECT * FROM packing_lists WHERE id = ? AND deleted_at IS NULL`, [id]);
            row = rows[0];
        } finally {
            conn.release();
        }
    }
    if (!row) return { skipped: 'not_found' };

    const fail = async (code, message) => {
        const conn = await pool.getConnection();
        try {
            await conn.query(
                `UPDATE packing_lists SET status = 'failed', error_message = ?, analyzed_at = NOW() WHERE id = ?`,
                [`${code}: ${String(message).slice(0, 3900)}`, id]
            );
        } catch (e) {
            log.error('[packing-list-check] could not record failure', { id, error: e.message });
        } finally {
            conn.release();
        }
        return { failed: code };
    };

    let read;
    try {
        read = await readPackingList({ s3Key: row.s3_key, contentType: row.content_type, model });
    } catch (e) {
        log.warn('[packing-list-check] read failed', { id, code: e.code, error: e.message });
        return fail(e.code || 'MODEL_CALL_FAILED', e.message);
    }

    let writeError = null;
    const conn = await pool.getConnection();
    try {
        const container = await resolveStoredContainer(conn, row);
        if (container.kind === 'draft' && !container.lineCount) {
            throw Object.assign(new Error(`Draft ${container.draftName} has no lines and no booked container was found for it.`), { code: 'CONTAINER_EMPTY' });
        }
        // The row's snapshot is what THIS document alone says at reading time;
        // the live view (packing-review.js) pools every active document for
        // the supplier and lays the sign-offs over it.
        const comparison = await buildComparison(conn, {
            container, supplierKey: row.supplier_key,
            documents: [{ packingListId: id, filename: row.filename, extracted: read.parsed }],
        });
        const p = read.parsed;
        await conn.query(
            `UPDATE packing_lists
                SET status = 'succeeded', error_message = NULL, analyzed_at = NOW(),
                    model_used = ?, invoice_number = ?, document_date = ?, supplier_printed = ?,
                    row_count = ?, discrepancy_count = ?, verdict = ?, extract_json = ?, comparison_json = ?
              WHERE id = ?`,
            [
                read.modelUsed,
                p.invoiceNumber ? String(p.invoiceNumber).slice(0, 100) : null,
                p.documentDate ? String(p.documentDate).slice(0, 32) : null,
                p.supplierName ? String(p.supplierName).slice(0, 255) : null,
                p.lines.length, comparison.summary.discrepancyCount, comparison.summary.verdict,
                JSON.stringify(p), JSON.stringify(comparison),
                id,
            ]
        );
        await recordAudit(conn, {
            entityType: 'packing_list', entityId: id, action: 'packing_list_analyzed', before: null,
            after: { modelUsed: read.modelUsed, rows: p.lines.length, ...comparison.summary, extractionOk: comparison.extractionCheck.ok },
            userEmail,
        });
        return { ok: true, summary: comparison.summary };
    } catch (e) {
        log.error('[packing-list-check] compare/write failed', { id, error: e.message });
        writeError = e;
    } finally {
        conn.release();
    }
    // Recorded after the release: fail() needs the pool's only connection.
    return fail(writeError.code === 'CONTAINER_EMPTY' ? 'CONTAINER_EMPTY' : 'COMPARE_FAILED', writeError.message);
}

module.exports = {
    SYSTEM_INSTRUCTION,
    RESPONSE_SCHEMA,
    toMonth,
    parseDims,
    baseJf,
    norm,
    comparePackingList,
    compareDocuments,
    applySignOffs,
    lineFingerprint,
    buildComparison,
    describeContainer,
    parseTypedReference,
    resolveContainer,
    resolveStoredContainer,
    resolveSupplierChoice,
    loadContainerSuppliers,
    loadExpectedLines,
    findPlacements,
    readPackingList,
    runPackingListCheck,
};
