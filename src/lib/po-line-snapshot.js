'use strict';

// PO PDF line snapshots: the lines a purchase_order_documents version was
// printed from, saved in purchase_order_documents.lines_json so ShipLine can
// flag lines whose qty, unit price or item changed since the PDF was generated
// (src/components/poLineDrift.ts there).
//
// Stored shape: { v: 1, source: 'generate' | 'audit-rebuild', lines: [{ jfCode, asin, quantity, unitPrice }] }
//   'generate'      — written by POST /purchase-orders/:id/generate from the
//                     exact rows loadPoForPdf printed.
//   'audit-rebuild' — written once by tools/backfill-po-line-snapshots.js for
//                     documents generated before the column existed, rebuilt
//                     from audit_log by reconstructPoLinesAt below.
// Product name is deliberately not kept: the PDF prints the jfpro_products
// name, so a catalogue rename would read as a change.
//
// Pure: no database, tested by tools/test-po-line-snapshot-lib.js.

const SNAPSHOT_VERSION = 1;

const code = v => (v == null || String(v) === '' ? null : String(v));
const num = v => Number(v || 0);
const price = v => (v == null || v === '' ? null : Number(v));

// From loadPoForPdf's grouped rows (jf_code, asin, unit_price, quantity).
function pdfLineSnapshot(lineRows, source) {
    return {
        v: SNAPSHOT_VERSION,
        source,
        lines: (lineRows || []).map(r => ({
            jfCode: code(r.jf_code),
            asin: code(r.asin),
            quantity: num(r.quantity),
            unitPrice: price(r.unit_price),
        })),
    };
}

// lines_json as read back: mysql2 returns a JSON column parsed, but a string
// is handled too. Anything unusable → null, which means "no baseline".
function parseLinesJson(value) {
    let v = value;
    if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch { return null; }
    }
    if (!v || typeof v !== 'object' || !Array.isArray(v.lines)) return null;
    return {
        source: typeof v.source === 'string' ? v.source : null,
        lines: v.lines.map(l => ({
            jfCode: code(l && l.jfCode),
            asin: code(l && l.asin),
            quantity: num(l && l.quantity),
            unitPrice: price(l && l.unitPrice),
        })),
    };
}

// ── Audit-log rebuild ─────────────────────────────────────────────────────
// The order fields a PDF line is made of, with the snake_case spelling some
// audit writers could use.
const FIELDS = [
    ['jfCode', 'jf_code'],
    ['asin', 'asin'],
    ['quantity', 'quantity'],
    ['unitPrice', 'unit_price'],
];

const ms = v => (v instanceof Date ? v.getTime() : Date.parse(v));
const has = (o, k) => !!o && Object.prototype.hasOwnProperty.call(o, k);
const oldestFirst = (a, b) => (ms(a.createdAt) - ms(b.createdAt)) || (Number(a.id) - Number(b.id));
const poIdOf = v => (v == null || v === '' ? null : Number(v));

// The PDF lines a PO had at time `at` (a document's generated_at).
//   orders      — every candidate order row in its CURRENT state, soft-deleted
//                 included: { id, jfCode, asin, quantity, unitPrice,
//                 purchaseOrderId, createdAt, deletedAt }
//   orderEvents — audit_log rows for those orders (entity_type 'order'):
//                 { id, orderId, action, createdAt, before }
//   poEvents    — PO-level audit rows: { id, poId, action
//                 'order_attached' | 'order_detached', orderId, createdAt }
//
// Not a replay. Each value at `at` is the `before` of the OLDEST event after
// `at` that carries it (a `before` snapshot is the truth just before its
// event), else the current value. Existence uses the row's own created_at /
// deleted_at, so a failed audit insert can't hide a create or a delete.
// Strictly after: an event in the same second as generation counts as before
// it (both columns are second-precision). Grouped like loadPoForPdf, minus
// the name.
function reconstructPoLinesAt({ poId, at, orders, orderEvents, poEvents }) {
    const t = ms(at);
    const P = Number(poId);
    const after = e => ms(e.createdAt) > t;

    const updatesByOrder = new Map();
    for (const e of orderEvents || []) {
        if (e.action !== 'update' || !after(e)) continue;
        const k = Number(e.orderId);
        if (!updatesByOrder.has(k)) updatesByOrder.set(k, []);
        updatesByOrder.get(k).push(e);
    }
    for (const list of updatesByOrder.values()) list.sort(oldestFirst);

    // Membership evidence per order: { id, createdAt, wasOn }.
    const membership = new Map();
    const addEvidence = (orderId, ev) => {
        const k = Number(orderId);
        if (!membership.has(k)) membership.set(k, []);
        membership.get(k).push(ev);
    };
    for (const [orderId, list] of updatesByOrder) {
        for (const e of list) {
            const b = e.before;
            const key = has(b, 'purchaseOrderId') ? 'purchaseOrderId' : has(b, 'purchase_order_id') ? 'purchase_order_id' : null;
            if (key) addEvidence(orderId, { id: e.id, createdAt: e.createdAt, wasOn: poIdOf(b[key]) === P });
        }
    }
    for (const e of poEvents || []) {
        if (Number(e.poId) !== P || !after(e)) continue;
        if (e.action === 'order_attached') addEvidence(e.orderId, { id: e.id, createdAt: e.createdAt, wasOn: false });
        else if (e.action === 'order_detached') addEvidence(e.orderId, { id: e.id, createdAt: e.createdAt, wasOn: true });
    }

    const live = [];
    for (const o of orders || []) {
        if (ms(o.createdAt) > t) continue;
        if (o.deletedAt != null && ms(o.deletedAt) <= t) continue;

        const evidence = (membership.get(Number(o.id)) || []).sort(oldestFirst);
        const onPo = evidence.length ? evidence[0].wasOn : poIdOf(o.purchaseOrderId) === P;
        if (!onPo) continue;

        const state = { id: Number(o.id), jfCode: o.jfCode, asin: o.asin, quantity: o.quantity, unitPrice: o.unitPrice };
        const updates = updatesByOrder.get(Number(o.id)) || [];
        for (const [camel, snake] of FIELDS) {
            const e = updates.find(u => has(u.before, camel) || has(u.before, snake));
            if (e) state[camel] = has(e.before, camel) ? e.before[camel] : e.before[snake];
        }
        live.push(state);
    }

    const groups = new Map();
    for (const s of live) {
        const line = { jfCode: code(s.jfCode), asin: code(s.asin), quantity: num(s.quantity), unitPrice: price(s.unitPrice) };
        const key = JSON.stringify([line.jfCode, line.asin, line.unitPrice]);
        const g = groups.get(key);
        if (g) {
            g.line.quantity += line.quantity;
            g.firstId = Math.min(g.firstId, s.id);
        } else {
            groups.set(key, { line, firstId: s.id });
        }
    }
    return [...groups.values()].sort((a, b) => a.firstId - b.firstId).map(g => g.line);
}

// ── Drift count (backfill report only; ShipLine has the per-line version) ─
// Item = jf code, else ASIN, trimmed and upper-cased; price at 4 dp with null
// as 0 (the PDF prints a missing price as 0); qty summed per item + price.
const itemKey = l => (String(l.jfCode || '').trim() || String(l.asin || '').trim()).toUpperCase();
const priceKey = p => Math.round(Number(p || 0) * 10000);

function bucketLines(lines) {
    const items = new Map();
    for (const l of lines || []) {
        const k = itemKey(l);
        if (!items.has(k)) items.set(k, new Map());
        const prices = items.get(k);
        const p = priceKey(l.unitPrice);
        prices.set(p, (prices.get(p) || 0) + num(l.quantity));
    }
    return items;
}

// Number of items whose qty-per-price differs between two line lists
// (an item on one side only counts once).
function countDrift(then, now) {
    const a = bucketLines(then);
    const b = bucketLines(now);
    let n = 0;
    for (const k of new Set([...a.keys(), ...b.keys()])) {
        const pa = a.get(k);
        const pb = b.get(k);
        if (!pa || !pb) { n++; continue; }
        const same = pa.size === pb.size && [...pa].every(([p, q]) => pb.get(p) === q);
        if (!same) n++;
    }
    return n;
}

module.exports = { SNAPSHOT_VERSION, pdfLineSnapshot, parseLinesJson, reconstructPoLinesAt, countDrift };
