'use strict';

// Shipment balance payments — the SQL half of "what we actually owe this
// supplier for the goods in this box".
//
// A deposit is a fact about a purchase order and lives on the PO (its PI, its
// payment status, its proof files). A BALANCE is not: it is settled per
// shipment, for the lines that actually travelled in it. One PO can span a
// dozen containers and one container can hold eight suppliers, so the only
// honest key is shipment x supplier, with a per-PO split underneath it.
//
// This module owns the queries that both the routes (src/handlers/orders.js),
// the re-link tool (tools/relink-shipment-payments.js) and the extractor
// (src/services/shipment-payment-extract.js) need. It takes a connection and
// never touches the pool, so a tool can use it without booting the handler
// module (which would start the pool and run every migration).
//
// It only READS the shipments entity (src/services/shipment-sync.js owns it,
// and it is still a rebuildable shadow of the legacy columns): a re-seed
// changes ids, so every record also stores the shipment's reference (the
// string orders carry as container_number) and `relinkShipmentPayments`
// repairs the pointers afterwards.

const S = require('../lib/shipments');

// How deep a merge chain is followed before we call it unresolved. Mirrors the
// depth cap in the shipments routes' own mergedInto helper.
const MERGE_HOPS = 5;

// Supplier names differ per PO for the same company ("SUNMED" vs "Suzhou
// Sunmed Co., Ltd" on the same shipment), so records key on a normalised form
// and the display name is kept verbatim. Same normalisation as payment_rules.
function supplierKey(name) {
    return String(name ?? '').trim().replace(/\s+/g, ' ').toLowerCase();
}

// Money as integer pence/cents, so a split of 34,400.02 across four POs sums
// back to 34,400.02 exactly.
function toCents(v) {
    const n = Number(v);
    return Number.isFinite(n) ? Math.round(n * 100) : NaN;
}

function fromCents(c) {
    return Math.round(c) / 100;
}

// ── Shipment resolution ──────────────────────────────────────────────────
// Returns { notFound: true } | { merged: true, mergedIntoId, row } | { row }.
// `row` is the live shipment the caller should write against. A merge loser
// keeps its history but its orders (and therefore its balances) moved to the
// survivor, so writes are refused with the survivor's id rather than silently
// redirected.
async function resolveShipment(conn, { id = null, reference = null } = {}) {
    let row = null;
    if (id != null && Number.isFinite(Number(id))) {
        const [rows] = await conn.query(
            `SELECT id, reference, name, stage, mode, merged_into_id, deleted_at FROM shipments WHERE id = ?`,
            [Number(id)]
        );
        row = rows[0] || null;
    } else {
        const ref = S.clean(reference);
        if (!ref) return { notFound: true };
        const [rows] = await conn.query(
            `SELECT id, reference, name, stage, mode, merged_into_id, deleted_at
               FROM shipments
              WHERE reference = ? AND deleted_at IS NULL AND merged_into_id IS NULL
              ORDER BY id DESC LIMIT 1`,
            [ref]
        );
        row = rows[0] || null;
    }
    if (!row) return { notFound: true };
    if (row.merged_into_id) {
        const survivor = await followMerge(conn, row.merged_into_id);
        return { merged: true, mergedIntoId: survivor ? survivor.id : row.merged_into_id, row: survivor };
    }
    if (row.deleted_at) return { notFound: true };
    return { row };
}

// Walk merged_into_id to the last live row (or null if the chain dead-ends).
async function followMerge(conn, startId) {
    let id = startId;
    for (let hop = 0; hop < MERGE_HOPS && id != null; hop++) {
        const [rows] = await conn.query(
            `SELECT id, reference, name, stage, mode, merged_into_id, deleted_at FROM shipments WHERE id = ?`,
            [id]
        );
        const row = rows[0];
        if (!row) return null;
        if (!row.merged_into_id) return row.deleted_at ? null : row;
        id = row.merged_into_id;
    }
    return null;
}

// ── Membership and the share basis ───────────────────────────────────────
// The POs with lines in this shipment, with the value each has ON BOARD — the
// basis both the UI's default split and `shareAllocations` use. Merge losers
// need no special case: merging re-points the loser's orders at the survivor.
async function loadMemberPurchaseOrders(conn, shipmentId) {
    const [rows] = await conn.query(
        `SELECT po.id, po.po_number, po.supplier, po.currency,
                COUNT(*) AS line_count,
                SUM(o.quantity) AS units_in_shipment,
                ROUND(SUM(o.quantity * COALESCE(o.unit_price, 0)), 2) AS value_in_shipment
           FROM orders o
           JOIN purchase_orders po ON po.id = o.purchase_order_id AND po.deleted_at IS NULL
          WHERE o.deleted_at IS NULL AND o.shipment_id = ?
          GROUP BY po.id, po.po_number, po.supplier, po.currency
          ORDER BY value_in_shipment DESC, po.id`,
        [shipmentId]
    );
    return rows.map(r => ({
        id: r.id,
        poNumber: r.po_number,
        supplier: r.supplier || null,
        supplierKey: supplierKey(r.supplier),
        currency: (r.currency || 'USD').toUpperCase(),
        lineCount: Number(r.line_count) || 0,
        unitsInShipment: Number(r.units_in_shipment) || 0,
        valueInShipment: r.value_in_shipment != null ? Number(r.value_in_shipment) : 0,
    }));
}

// The newest deposit instruction per PO, so the operator sees "deposit 30%
// already paid" while recording the balance. Deleted invoices drop out.
async function loadDepositsFor(conn, poIds) {
    const out = new Map();
    if (!poIds.length) return out;
    const ph = poIds.map(() => '?').join(',');
    const [rows] = await conn.query(
        `SELECT p.purchase_order_id, p.amount_due, p.currency, p.payment_status, p.due_date, p.deposit_percentage
           FROM purchase_order_invoice_payments p
           JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
          WHERE p.purchase_order_id IN (${ph}) AND p.payment_type IN ('deposit', 'full')
          ORDER BY p.purchase_order_id, p.id DESC`,
        poIds
    );
    for (const r of rows) {
        if (out.has(r.purchase_order_id)) continue;
        out.set(r.purchase_order_id, {
            amountDue: r.amount_due != null ? Number(r.amount_due) : null,
            currency: r.currency || null,
            paymentStatus: r.payment_status || null,
            dueDate: r.due_date || null,
            depositPercentage: r.deposit_percentage != null ? Number(r.deposit_percentage) : null,
        });
    }
    return out;
}

// Everything the document reader needs to map a supplier's own references
// onto our purchase orders: the shipment's own identifiers, every PO on
// board with its line items, and the deposit already recorded against each.
async function loadShipmentContext(conn, shipmentId) {
    const [rows] = await conn.query(
        `SELECT id, reference, name, mode, stage, tracking_ref, bl_number, booking_ref, vessel_name, etd, eta
           FROM shipments WHERE id = ?`,
        [shipmentId]
    );
    const shipment = rows[0];
    if (!shipment) return null;
    const pos = await loadMemberPurchaseOrders(conn, shipmentId);
    const deposits = await loadDepositsFor(conn, pos.map(p => p.id));
    const linesByPo = new Map();
    if (pos.length) {
        const ph = pos.map(() => '?').join(',');
        const [lines] = await conn.query(
            `SELECT purchase_order_id, jf_code, product_name, quantity, unit_price
               FROM orders
              WHERE deleted_at IS NULL AND shipment_id = ? AND purchase_order_id IN (${ph})
              ORDER BY purchase_order_id, id`,
            [shipmentId, ...pos.map(p => p.id)]
        );
        for (const l of lines) {
            if (!linesByPo.has(l.purchase_order_id)) linesByPo.set(l.purchase_order_id, []);
            linesByPo.get(l.purchase_order_id).push({
                jfCode: l.jf_code || null,
                productName: l.product_name || null,
                quantity: l.quantity != null ? Number(l.quantity) : null,
                unitPrice: l.unit_price != null ? Number(l.unit_price) : null,
            });
        }
    }
    return {
        shipment,
        // The currency most of the goods on board are priced in — the default
        // when a document does not say.
        shipmentCurrency: pos[0]?.currency ?? null,
        purchaseOrders: pos.map(p => ({ ...p, deposit: deposits.get(p.id) || null, lines: linesByPo.get(p.id) || [] })),
    };
}

// What the record form needs: the shipment, its member POs with the share
// basis, and the deposit already on file for each.
async function loadRecordContext(conn, shipmentRow) {
    const pos = await loadMemberPurchaseOrders(conn, shipmentRow.id);
    const deposits = await loadDepositsFor(conn, pos.map(p => p.id));
    return pos.map(p => ({ ...p, deposit: deposits.get(p.id) || null }));
}

// ── Allocation by value share ────────────────────────────────────────────
// Split `amount` across `pos` in proportion to what each has on board, in
// whole cents, so the parts always sum back to `amount`. Falls back to line
// count when nothing on board is priced.
function shareAllocations(amount, pos, { source = 'share' } = {}) {
    const cents = toCents(amount);
    if (!pos.length || !Number.isFinite(cents) || cents <= 0) return [];
    let basis = pos.map(p => Math.max(0, Number(p.valueInShipment) || 0));
    let total = basis.reduce((a, b) => a + b, 0);
    if (!(total > 0)) {
        basis = pos.map(p => Math.max(1, Number(p.lineCount) || 1));
        total = basis.reduce((a, b) => a + b, 0);
    }
    const parts = basis.map(w => Math.floor((cents * w) / total));
    let left = cents - parts.reduce((a, b) => a + b, 0);
    // Spread the rounding remainder a cent at a time, biggest share first.
    const order = basis.map((_, i) => i).sort((a, b) => basis[b] - basis[a] || a - b);
    for (let i = 0; left > 0; i = (i + 1) % order.length) { parts[order[i]] += 1; left -= 1; }
    return pos
        .map((p, i) => ({ purchaseOrderId: p.id, poRef: p.poNumber, amount: fromCents(parts[i]), source }))
        .filter(a => a.amount > 0);
}

// ── Re-link after a shadow re-seed ───────────────────────────────────────
// Every record stores both the shipment id and its reference. When the
// shipments shadow is rebuilt the ids change; when two shipments merge the
// loser's reference is NULLed and its orders move. This walks every live
// record and repairs the pointer: merged -> the survivor, missing -> the live
// shipment carrying the same reference. Dry by default.
async function relinkShipmentPayments(conn, { apply = false, recordAudit = null, userEmail = null, tables = null } = {}) {
    const targets = tables || [
        { table: 'shipment_payments', entityType: 'shipment_payment' },
        { table: 'shipment_payment_documents', entityType: 'shipment_payment_document' },
    ];
    const out = { checked: 0, relinked: [], unresolved: [] };
    for (const target of targets) {
        let rows;
        try {
            [rows] = await conn.query(
                `SELECT id, shipment_id, shipment_reference FROM ${target.table} WHERE deleted_at IS NULL ORDER BY id`
            );
        } catch (e) {
            // Phase 1 databases have no documents table yet.
            if (e.errno === 1146) continue;
            throw e;
        }
        for (const row of rows) {
            out.checked++;
            const resolution = await resolveRelink(conn, row);
            if (resolution.ok) continue;
            if (!resolution.shipment) {
                out.unresolved.push({
                    entity: target.entityType, id: row.id,
                    shipmentId: row.shipment_id, reference: row.shipment_reference, reason: resolution.reason,
                });
                continue;
            }
            const before = { shipmentId: row.shipment_id, shipmentReference: row.shipment_reference };
            const after = { shipmentId: resolution.shipment.id, shipmentReference: resolution.shipment.reference || row.shipment_reference };
            out.relinked.push({ entity: target.entityType, id: row.id, from: before, to: after, reason: resolution.reason });
            if (!apply) continue;
            await conn.query(
                `UPDATE ${target.table} SET shipment_id = ?, shipment_reference = ? WHERE id = ?`,
                [after.shipmentId, after.shipmentReference, row.id]
            );
            if (recordAudit) {
                await recordAudit(conn, {
                    entityType: target.entityType, entityId: row.id, action: 'relink',
                    before, after, userEmail,
                });
            }
        }
    }
    return out;
}

async function resolveRelink(conn, row) {
    const [found] = await conn.query(
        `SELECT id, reference, merged_into_id, deleted_at FROM shipments WHERE id = ?`,
        [row.shipment_id]
    );
    const live = found[0];
    if (live && !live.merged_into_id && !live.deleted_at) {
        // Reference drift alone is repaired silently (a rename on a booked
        // shipment rewrites orders.container_number too).
        if (S.clean(live.reference) === S.clean(row.shipment_reference)) return { ok: true };
        return { shipment: live, reason: 'reference_drift' };
    }
    if (live && live.merged_into_id) {
        const survivor = await followMerge(conn, live.merged_into_id);
        return survivor ? { shipment: survivor, reason: 'merged' } : { shipment: null, reason: 'merged_dead_end' };
    }
    const ref = S.clean(row.shipment_reference);
    if (ref) {
        const [byRef] = await conn.query(
            `SELECT id, reference FROM shipments
              WHERE reference = ? AND deleted_at IS NULL AND merged_into_id IS NULL
              ORDER BY id DESC LIMIT 1`,
            [ref]
        );
        if (byRef[0]) return { shipment: byRef[0], reason: live ? 'deleted' : 'missing' };
    }
    return { shipment: null, reason: live ? 'deleted_no_reference_match' : 'missing_no_reference_match' };
}

module.exports = {
    MERGE_HOPS,
    supplierKey,
    toCents,
    fromCents,
    resolveShipment,
    followMerge,
    loadMemberPurchaseOrders,
    loadDepositsFor,
    loadShipmentContext,
    loadRecordContext,
    shareAllocations,
    relinkShipmentPayments,
};
