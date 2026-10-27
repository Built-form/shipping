'use strict';

// Request to send staleness: has a supplier's part of a container changed since
// that supplier's latest Request to send PDF (draft_container_documents type
// 'supplier-quote') was generated?
//
// The lines a Request to send was printed from are saved on the document
// (draft_container_documents.lines_json):
//   { v: 1, source: 'generate' | 'csv-backfill', lines: [{ jfCode, sku, lot, units, supplier }] }
//   'generate'     — written by generateDraftDocument from the exact PDF lines.
//   'csv-backfill' — written once by tools/backfill-request-to-send-snapshots.js
//                    from the CSV stored beside an older PDF (same lines).
//
// Only what the supplier acts on counts (user rule, 2026-10-26): a line added
// or removed, its units, its LOT. Carton spec, cartons, CBM, weight, port, PO
// number and product name are ignored. Item = the PDF's JF code, else its SKU
// cell (product name, else JF code, else ASIN); item and lot compared trimmed
// and upper-cased; units summed per item + lot, so split rows and the new
// order id a partial pack creates are not changes.
//
// Pure: no database, tested by tools/test-request-to-send-drift-lib.js.

const { parse } = require('csv-parse/sync');

const SNAPSHOT_VERSION = 1;

const text = v => (v == null ? '' : String(v)).trim();
const orNull = v => text(v) || null;
const upper = v => text(v).toUpperCase();
const units = v => Number(v || 0);

// Free-text orders.supplier, matched the way loadForwarderQuoteForPdf filters.
const sameSupplier = (a, b) => text(a).toLowerCase() === text(b).toLowerCase();

// A lot of "-" means none: the PDF prints it, but the stored CSV blanks every
// "-" cell (csvCell in draft-container-pdf.js), so a CSV baseline reads it empty.
const lotOf = v => (text(v) === '-' ? null : orNull(v));

const line = (jfCode, sku, lot, n, supplier) => ({
    jfCode: orNull(jfCode), sku: orNull(sku), lot: lotOf(lot), units: units(n), supplier: orNull(supplier),
});

// From the lines generateDraftDocument hands the PDF (loadForwarderQuoteForPdf).
function requestLinesSnapshot(pdfLines, source) {
    return {
        v: SNAPSHOT_VERSION,
        source,
        lines: (pdfLines || []).map(l => line(l.jfCode, l.sku, l.lot, l.orderedUnits, l.supplier)),
    };
}

// From quoteLineSource rows (jf_code, asin, product_name, lot_number,
// allocated, supplier), with the PDF's SKU rule.
function currentRequestLines(rows) {
    return (rows || []).map(r =>
        line(r.jf_code, r.product_name || r.jf_code || r.asin, r.lot_number, r.allocated, r.supplier));
}

// lines_json as read back (mysql2 may return the JSON parsed or as a string).
// Unusable → null, which means "no baseline": never flagged.
function parseRequestLines(value) {
    let v = value;
    if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch { return null; }
    }
    if (!v || typeof v !== 'object' || !Array.isArray(v.lines)) return null;
    return {
        source: typeof v.source === 'string' ? v.source : null,
        lines: v.lines.map(l => line(l && l.jfCode, l && l.sku, l && l.lot, l && l.units, l && l.supplier)),
    };
}

// The CSV stored beside a Request to send PDF (buildSupplierQuoteCsv: SQ_COLS
// headers, one row per printed line, no totals row). Not that CSV → null.
const CSV_HEADERS = ['SKU', 'HW/JF Code', 'LOT', 'Ordered Units', 'Supplier'];
function csvRequestLines(csvText) {
    let records;
    try {
        records = parse(String(csvText || ''), { bom: true, columns: true, skip_empty_lines: true, relax_column_count: true });
    } catch {
        return null;
    }
    if (!records.length || !CSV_HEADERS.every(h => h in records[0])) return null;
    return records.map(r => line(r['HW/JF Code'], r.SKU, r.LOT, r['Ordered Units'], r.Supplier));
}

// item → lot → { units, item label, lot label }, in first-seen order.
function bucket(lines) {
    const items = new Map();
    for (const l of lines || []) {
        const itemKey = upper(l.jfCode) || upper(l.sku);
        if (!items.has(itemKey)) items.set(itemKey, { label: text(l.jfCode) || text(l.sku), lots: new Map() });
        const lots = items.get(itemKey).lots;
        const lot = lotOf(l.lot);
        const lotKey = upper(lot);
        const b = lots.get(lotKey);
        if (b) b.units += units(l.units);
        else lots.set(lotKey, { units: units(l.units), lot });
    }
    return items;
}

// Snapshot lines vs current lines for one supplier. null snapshot → null.
// One lot each side reads as a lot change (with units if those moved too);
// an item in several lots is compared lot by lot.
function requestDrift(thenLines, nowLines) {
    if (!thenLines) return null;
    const then = bucket(thenLines);
    const now = bucket(nowLines);
    const changes = [];
    const removed = (item, b) => changes.push({ kind: 'removed', item, lot: b.lot, unitsWas: b.units });
    const added = (item, b) => changes.push({ kind: 'added', item, lot: b.lot, unitsNow: b.units });

    for (const k of new Set([...then.keys(), ...now.keys()])) {
        const t = then.get(k);
        const n = now.get(k);
        if (!n) { for (const b of t.lots.values()) removed(t.label, b); continue; }
        if (!t) { for (const b of n.lots.values()) added(n.label, b); continue; }

        if (t.lots.size === 1 && n.lots.size === 1) {
            const [[tk, tb]] = [...t.lots];
            const [[nk, nb]] = [...n.lots];
            if (tk !== nk) {
                const c = { kind: 'lot', item: n.label, lotWas: tb.lot, lot: nb.lot };
                if (tb.units !== nb.units) Object.assign(c, { unitsWas: tb.units, unitsNow: nb.units });
                changes.push(c);
            } else if (tb.units !== nb.units) {
                changes.push({ kind: 'units', item: n.label, lot: nb.lot, unitsWas: tb.units, unitsNow: nb.units });
            }
            continue;
        }
        for (const lk of new Set([...t.lots.keys(), ...n.lots.keys()])) {
            const tb = t.lots.get(lk);
            const nb = n.lots.get(lk);
            if (!nb) removed(t.label, tb);
            else if (!tb) added(n.label, nb);
            else if (tb.units !== nb.units) {
                changes.push({ kind: 'units', item: n.label, lot: nb.lot, unitsWas: tb.units, unitsNow: nb.units });
            }
        }
    }
    return { stale: changes.length > 0, changes };
}

module.exports = {
    SNAPSHOT_VERSION, sameSupplier, requestLinesSnapshot, currentRequestLines, parseRequestLines,
    csvRequestLines, requestDrift,
};
