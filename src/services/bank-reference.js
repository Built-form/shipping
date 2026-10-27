'use strict';

// The reference bank details for a supplier, for checking what a portal PI
// claims (src/services/pi-check.js at check time; the PI submission routes at
// review time). Two sources, in order:
//
//   1. JFPro — jfpro.supplier_bank_accounts / _versions, the details staff
//      maintain as the source of truth. Every live (not deleted, not retired)
//      account counts, newest version each: a supplier can hold one account
//      per currency, and a PI matching ANY of them is a match.
//   2. The supplier's last PO — the newest purchase_order_invoice_payments row
//      of another PO of this supplier that carries bank details, preferring a
//      verified one (the same row src/services/pi-submission-routes.js used
//      before JFPro existed). Only consulted when JFPro has nothing: staff
//      keeping JFPro current must not be out-voted by an old invoice.
//
// Returns { source: 'jfpro'|'last_po', label, verifiedAt, details, candidates }
// — `details` is the primary candidate for display, `candidates` every
// BankDetails to compare against — or null when neither source has anything.
// A missing jfpro schema (or no grant on it) falls through to the last PO, so
// an environment without JFPro degrades instead of failing the check.

const bank = require('../lib/bank-verification');
const { jportalSchema, PO_SUPPLIER_IS } = require('../lib/jportal');
const log = require('../lib/logger');

// Schema or table absent, or no grant: this source has nothing to say.
const SOURCE_MISSING = new Set([1049, 1142, 1143, 1146]);

const iso = (v) => (v instanceof Date ? v.toISOString() : v ? String(v) : null);

// Every live JFPro account's newest version, primary first, detail-less rows dropped.
async function jfproCandidates(conn, supplierId) {
    try {
        const [rows] = await conn.query(
            `SELECT v.currency, v.beneficiary_name, v.bank_name, v.bank_address, v.account_number,
                    v.iban, v.swift_bic, v.intermediary_bank, v.created_at, a.is_primary
               FROM jfpro.supplier_bank_accounts a
               JOIN jfpro.supplier_bank_account_versions v
                 ON v.id = (SELECT MAX(v2.id) FROM jfpro.supplier_bank_account_versions v2 WHERE v2.account_id = a.id)
              WHERE a.supplier_id = ? AND a.deleted_at IS NULL AND a.retired_at IS NULL
              ORDER BY a.is_primary DESC, a.id DESC`,
            [supplierId],
        );
        return rows
            .map((r) => ({
                details: bank.bankDetailsOf(r, { fromRow: true }),
                label: `JFPro${r.currency ? ` ${String(r.currency).toUpperCase()}` : ''}${Number(r.is_primary) === 1 ? ' (primary)' : ''}`,
                verifiedAt: iso(r.created_at),
            }))
            .filter((c) => bank.hasBankDetails(c.details));
    } catch (err) {
        if (!SOURCE_MISSING.has(err.errno)) throw err;
        log.warn('[bank-reference] jfpro bank tables unavailable; falling back to the last PO', { errno: err.errno });
        return [];
    }
}

/**
 * The reference for one supplier. `supplierId` is the jfa/jfpro suppliers id
 * (the same id space); `fallbackName` names the supplier when the live row is
 * gone; `excludeInvoiceId` keeps a re-check from comparing a PI with itself.
 */
async function loadBankReference(conn, { supplierId, fallbackName = null, excludeInvoiceId = null }) {
    const fromJfpro = await jfproCandidates(conn, supplierId);
    if (fromJfpro.length) {
        return {
            source: 'jfpro',
            label: fromJfpro[0].label,
            verifiedAt: fromJfpro[0].verifiedAt,
            details: fromJfpro[0].details,
            candidates: fromJfpro,
        };
    }

    // The last PO: the routes' previous-bank rule, word for word (verified
    // portal PIs by supplier id; staff/legacy PIs by the PO's supplier name).
    const exclude = excludeInvoiceId != null ? Number(excludeInvoiceId) : 0;
    const [names] = await conn.query('SELECT name FROM suppliers WHERE id = ? AND deleted_at IS NULL LIMIT 1', [supplierId]);
    const supplierName = names.length ? names[0].name : fallbackName;
    const [rows] = await conn.query(
        `SELECT p.*, po.po_number
           FROM purchase_order_invoice_payments p
           JOIN purchase_order_invoices i ON i.id = p.purchase_order_invoice_id AND i.deleted_at IS NULL
           JOIN purchase_orders po ON po.id = p.purchase_order_id AND po.deleted_at IS NULL
           LEFT JOIN ${jportalSchema()}.pi_submissions ps ON ps.id = i.pi_submission_id
          WHERE p.purchase_order_invoice_id <> ?
            AND ((i.pi_submission_id IS NOT NULL AND ps.supplier_id = ? AND p.bank_verification_status = 'verified')
              OR (i.pi_submission_id IS NULL AND ${PO_SUPPLIER_IS}
                  AND (p.bank_verification_status = 'verified'
                       OR COALESCE(NULLIF(p.beneficiary_name, ''), NULLIF(p.bank_name, ''), NULLIF(p.account_number, ''),
                                   NULLIF(p.iban, ''), NULLIF(p.swift_bic, ''), NULLIF(p.intermediary_bank, '')) IS NOT NULL)))
          ORDER BY COALESCE(p.bank_verification_status = 'verified', 0) DESC,
                   COALESCE(p.bank_verified_at, p.updated_at) DESC, p.id DESC
          LIMIT 1`,
        [exclude, supplierId, supplierName],
    );
    if (!rows.length) return null;
    const p = rows[0];
    const verified = p.bank_verification_status === bank.VERIFIED;
    const details = (verified && bank.parseVerifiedJson(p.bank_verified_json)) || bank.bankDetailsOf(p, { fromRow: true });
    return {
        source: 'last_po',
        label: p.po_number || `invoice ${p.purchase_order_invoice_id}`,
        verifiedAt: verified ? iso(p.bank_verified_at) : null,
        invoiceId: Number(p.purchase_order_invoice_id),
        poNumber: p.po_number || null,
        details,
        candidates: [{ details, label: p.po_number || null, verifiedAt: verified ? iso(p.bank_verified_at) : null }],
    };
}

module.exports = { loadBankReference };
