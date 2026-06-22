'use strict';

// Mirrors the GET /api/v1/suppliers logic and prints a sample.

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [supRows] = await conn.query(`
            SELECT id, name, code, country, contact_email, active, created_at
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
            byId.get(e.supplier_id).push({ email: e.email, label: e.label, isPrimary: e.is_primary === 1 });
        }

        const withEmails = supRows.filter(r => (byId.get(r.id) || []).length > 0);
        console.log(`Total suppliers: ${supRows.length}; with emails: ${withEmails.length}\n`);
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
