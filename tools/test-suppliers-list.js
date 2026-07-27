'use strict';

// Mirrors the GET /api/v1/suppliers logic.
//
//   node tools/test-suppliers-list.js          # human-readable summary
//   node tools/test-suppliers-list.js --json    # canonical JSON keyed by name
//
// The --json mode is the supplier-merge parity check: capture it BEFORE the
// cutover and AFTER, then diff. ids and timestamps are intentionally excluded
// (ids change from jfa -> jfpro), so the diff surfaces only real field changes.
// Assert: no pre-existing supplier name disappears, and every name that had a
// portalCode still has one. (Both `suppliers` and `supplier_emails` resolve via
// the views after cutover, so the same queries work before and after.)

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const JSON_MODE = process.argv.includes('--json');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [supRows] = await conn.query(`
            SELECT id, name, code, portal_code, country, default_currency, default_incoterms,
                   default_payment_terms, contact_name, contact_email, created_at
              FROM suppliers
             WHERE deleted_at IS NULL
             ORDER BY name ASC
        `);
        const ids = supRows.map(r => r.id);
        const emailRows = ids.length
            ? (await conn.query(
                `SELECT id, supplier_id, email, label, is_primary
                   FROM supplier_emails
                  WHERE supplier_id IN (${ids.map(() => '?').join(',')}) AND deleted_at IS NULL
                  ORDER BY is_primary DESC, email ASC`,
                ids
            ))[0]
            : [];
        const byId = new Map();
        for (const e of emailRows) {
            if (!byId.has(e.supplier_id)) byId.set(e.supplier_id, []);
            byId.get(e.supplier_id).push({ email: e.email, label: e.label || null, isPrimary: e.is_primary === 1 });
        }

        if (JSON_MODE) {
            // Canonical, id/timestamp-free, sorted-by-name snapshot for diffing.
            const out = {};
            for (const r of supRows) {
                const key = String(r.name == null ? '' : r.name).trim().toLowerCase();
                out[key] = {
                    name: r.name,
                    code: r.code || null,
                    hasPortalCode: !!(r.portal_code && String(r.portal_code).trim()),
                    country: r.country || null,
                    defaultCurrency: r.default_currency || null,
                    defaultIncoterms: r.default_incoterms || null,
                    defaultPaymentTerms: r.default_payment_terms || null,
                    contactName: r.contact_name || null,
                    contactEmail: r.contact_email || null,
                    emails: (byId.get(r.id) || []).slice().sort((a, b) => a.email.localeCompare(b.email)),
                };
            }
            const ordered = {};
            for (const k of Object.keys(out).sort()) ordered[k] = out[k];
            console.log(JSON.stringify({ count: supRows.length, suppliers: ordered }, null, 2));
            return;
        }

        const withEmails = supRows.filter(r => (byId.get(r.id) || []).length > 0);
        const withCode = supRows.filter(r => r.portal_code && String(r.portal_code).trim());
        console.log(`Total suppliers: ${supRows.length}; with emails: ${withEmails.length}; with portal code: ${withCode.length}\n`);
        console.log('First 5 suppliers with emails:');
        for (const r of withEmails.slice(0, 5)) {
            console.log(`\n- ${r.name} (id=${r.id})`);
            console.log('  emails:', JSON.stringify(byId.get(r.id) || []));
        }
        console.log('\nFirst 3 suppliers with NO emails:');
        for (const r of supRows.filter(x => !byId.has(x.id)).slice(0, 3)) {
            console.log(`\n- ${r.name} (id=${r.id})`);
        }
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
