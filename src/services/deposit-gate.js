'use strict';

// Whether a purchase order's lines may move into production by themselves when
// the supplier confirms artwork (docs/artwork-confirmations.md): only when the
// supplier requires no deposit, or the deposit has been paid. The SQL half; the
// rule itself is depositGate in src/lib/artwork-confirmations.js, and the
// reading of payment terms is src/lib/payment-terms.js (a port of ShipLine's).
//
// The deposit % in force, as ShipLine's Payments flow page works it out: the
// company's payment rule for the supplier (else the default rule), else the
// newest proforma invoice's own terms, else the supplier's JFPRO payment terms.
//
// The deposit counts as paid when a supplier payment was applied to it — a
// 'po_deposit' line on the PO, or a 'pi' line on a deposit / full PI of the PO —
// or such a PI is marked paid, or the supplier ticked "deposit received" on the
// portal (po_deposit_receipts). The first three are the rule the portal's own
// orders list uses (portal src/handlers/purchaseOrders.js loadDeposits).

const A = require('../lib/artwork-confirmations');
const { depositPctInForce } = require('../lib/payment-terms');

const TABLE_MISSING = 1146;

async function loadDepositPct(conn, po) {
    const [lineRows] = await conn.query(
        `SELECT DISTINCT supplier FROM orders
          WHERE purchase_order_id = ? AND deleted_at IS NULL AND supplier IS NOT NULL AND supplier <> ''`,
        [po.id]
    );
    const supplierNames = [...new Set([po.supplier, ...lineRows.map(r => r.supplier)].filter(Boolean))];

    const [rules] = await conn.query('SELECT scope, supplier_name, deposit_pct FROM payment_rules');

    // The newest live PI's payment row: its terms text and extracted deposit %.
    const [piRows] = await conn.query(
        `SELECT p.due_terms, p.raw_terms_text, p.deposit_percentage
           FROM purchase_order_invoice_payments p
           JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
          WHERE p.purchase_order_id = ?
          ORDER BY i.uploaded_at DESC, p.id DESC
          LIMIT 1`,
        [po.id]
    );
    const newestPi = piRows.length ? {
        dueTerms: piRows[0].due_terms,
        rawTermsText: piRows[0].raw_terms_text,
        depositPercentage: piRows[0].deposit_percentage != null ? Number(piRows[0].deposit_percentage) : null,
    } : null;

    // The supplier's JFPRO default terms (suppliers is a view over jfpro.suppliers).
    let supplierTerms = null;
    if (po.supplier) {
        const [supRows] = await conn.query(
            `SELECT default_payment_terms FROM suppliers
              WHERE name = ? AND deleted_at IS NULL AND default_payment_terms IS NOT NULL AND default_payment_terms <> ''
              LIMIT 1`,
            [po.supplier]
        );
        supplierTerms = supRows.length ? supRows[0].default_payment_terms : null;
    }

    return depositPctInForce({ rules, supplierNames, newestPi, supplierTerms });
}

async function loadDepositPaid(conn, poId) {
    const [paid] = await conn.query(
        `SELECT 1 FROM supplier_payment_lines l
           JOIN supplier_payments sp ON sp.id = l.payment_id AND sp.deleted_at IS NULL
          WHERE l.target_kind = 'po_deposit' AND l.target_id = ?
         UNION ALL
         SELECT 1 FROM purchase_order_invoice_payments p
           JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
           JOIN supplier_payment_lines l ON l.target_kind = 'pi' AND l.target_id = p.id
           JOIN supplier_payments sp ON sp.id = l.payment_id AND sp.deleted_at IS NULL
          WHERE p.purchase_order_id = ? AND p.payment_type IN ('deposit', 'full')
         UNION ALL
         SELECT 1 FROM purchase_order_invoice_payments p
           JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
          WHERE p.purchase_order_id = ? AND p.payment_type IN ('deposit', 'full') AND p.payment_status = 'paid'
         LIMIT 1`,
        [poId, poId, poId]
    );
    if (paid.length) return true;
    try {
        const [receipts] = await conn.query(
            'SELECT 1 FROM po_deposit_receipts WHERE purchase_order_id = ? AND received_on IS NOT NULL LIMIT 1',
            [poId]
        );
        return receipts.length > 0;
    } catch (e) {
        if (e.errno !== TABLE_MISSING) throw e;
        return false;
    }
}

/**
 * The gate for one purchase order: { depositPct, depositPaid, clear, reason }
 * (depositGate). `po` = { id, supplier }.
 */
async function loadDepositGate(conn, po) {
    const depositPct = await loadDepositPct(conn, po);
    // No deposit required: nothing to look up.
    const depositPaid = depositPct === 0 ? false : await loadDepositPaid(conn, po.id);
    return A.depositGate({ depositPct, depositPaid });
}

module.exports = { loadDepositGate, loadDepositPct, loadDepositPaid };
