'use strict';

// Delivered air freight for the Record payment search. Air goods are released
// before their balance is paid, so the balance goes with a later transfer —
// and air delivered before the Payments page's start date is not in its
// forecast at all. This finds it again: a supplier's air shipments that have
// landed, with what each of its POs has on board. Pure; the route in
// src/handlers/orders.js runs the queries.

// Line statuses that mean the goods are in.
const LANDED_STATUSES = new Set(['ARRIVED_AT_WAREHOUSE', 'RECEIVED', 'PARTIALLY_RECEIVED', 'IN_WAREHOUSE', 'MINTSOFT']);
const LANDED_STAGES = new Set(['ARRIVED', 'CLOSED']);

const pad = (n) => String(n).padStart(2, '0');
// DATE columns arrive as strings (dateStrings); DATETIME ones as Dates in local time.
function ymdOf(v) {
    if (!v) return null;
    if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : `${v.getFullYear()}-${pad(v.getMonth() + 1)}-${pad(v.getDate())}`;
    const s = String(v);
    return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : null;
}
const round2 = (n) => Math.round(n * 100) / 100;

// rows: one per order line on an air shipment (see the route's query).
// isSupplier(poSupplier) picks the supplier's POs; currency (optional) the PO currency.
// → [{ shipmentId, reference, stage, deliveredOn, awbNumbers, purchaseOrders: [...] }], newest delivery first.
function groupDeliveredAir(rows, { isSupplier, currency }) {
    const cur = currency ? String(currency).toUpperCase() : null;
    const byShipment = new Map();
    for (const r of rows) {
        if (!isSupplier(r.po_supplier)) continue;
        if (cur && String(r.currency || 'USD').toUpperCase() !== cur) continue;
        if (!byShipment.has(r.shipment_id)) {
            byShipment.set(r.shipment_id, { first: r, lines: [] });
        }
        byShipment.get(r.shipment_id).lines.push(r);
    }
    const out = [];
    for (const [shipmentId, { first, lines }] of byShipment) {
        // Landed by stage, or — the stage often lags — every one of this supplier's lines is in.
        const delivered = LANDED_STAGES.has(String(first.stage || '').toUpperCase())
            || lines.every(l => LANDED_STATUSES.has(String(l.status || '').toUpperCase()));
        if (!delivered) continue;
        const dates = lines.map(l => ymdOf(l.arrived_date) ?? ymdOf(l.delivery_date)).filter(Boolean).sort();
        const pos = new Map();
        const awbs = new Set();
        for (const l of lines) {
            if (l.awb_number && String(l.awb_number).trim()) awbs.add(String(l.awb_number).trim());
            if (!pos.has(l.purchase_order_id)) {
                pos.set(l.purchase_order_id, {
                    purchaseOrderId: l.purchase_order_id, poNumber: l.po_number, supplier: l.po_supplier,
                    currency: String(l.currency || 'USD').toUpperCase(),
                    lines: 0, units: 0, value: 0, unpricedLines: 0, jfCodes: [],
                });
            }
            const p = pos.get(l.purchase_order_id);
            const qty = Number(l.quantity) || 0;
            const price = l.unit_price != null ? Number(l.unit_price) : null;
            p.lines++;
            p.units += qty;
            if (price != null && price > 0) p.value += qty * price;
            else p.unpricedLines++;
            if (l.jf_code && !p.jfCodes.includes(l.jf_code)) p.jfCodes.push(l.jf_code);
        }
        out.push({
            shipmentId,
            reference: first.reference,
            stage: first.stage,
            deliveredOn: dates[0] ?? ymdOf(first.shipment_arrived_at),
            awbNumbers: [...awbs],
            purchaseOrders: [...pos.values()]
                .map(p => ({ ...p, value: round2(p.value) }))
                .sort((a, b) => String(a.poNumber).localeCompare(String(b.poNumber), undefined, { numeric: true })),
        });
    }
    return out.sort((a, b) => (b.deliveredOn ?? '').localeCompare(a.deliveredOn ?? '') || String(a.reference).localeCompare(String(b.reference), undefined, { numeric: true }));
}

// balanceRows: this supplier's balance records on those shipments, one row
// per allocation (purchase_order_id null = recorded with no split, so it
// covers the whole box). Sets purchaseOrders[].balance — the record that
// covers that PO, a paid one winning over an open one — or null.
function attachBalances(shipments, balanceRows) {
    const rank = (s) => (s === 'paid' ? 2 : 1);
    for (const s of shipments) {
        for (const p of s.purchaseOrders) {
            let best = null;
            for (const b of balanceRows) {
                if (b.shipment_id !== s.shipmentId || b.status === 'skipped') continue;
                if (b.purchase_order_id != null && b.purchase_order_id !== p.purchaseOrderId) continue;
                if (!best || rank(b.status) > rank(best.status)) best = b;
            }
            p.balance = best ? { id: best.id, status: best.status, paidOn: ymdOf(best.paid_on), amount: round2(Number(best.amount) || 0) } : null;
        }
    }
    return shipments;
}

// paidRows: paid balance records for one shipment x supplier, one row per
// allocation. The first that already pays any of poIds (or pays the whole
// box, having no split) — recording another balance for it would pay twice.
function findPaidClash(paidRows, poIds) {
    const want = new Set(poIds);
    return paidRows.find(r => r.purchase_order_id == null || want.has(r.purchase_order_id)) ?? null;
}

module.exports = { LANDED_STATUSES, groupDeliveredAir, attachBalances, findPaidClash };
