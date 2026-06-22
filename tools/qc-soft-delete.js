'use strict';
// Soft-delete QC reports by id (sets deleted_at) — for removing superseded
// manual duplicates. Recoverable: clear deleted_at to restore.
//
// Usage: node tools/qc-soft-delete.js <id> [id ...]

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const ids = process.argv.slice(2).map(Number).filter(n => Number.isInteger(n) && n > 0);

(async () => {
    if (!ids.length) { console.error('Usage: node tools/qc-soft-delete.js <id> [id ...]'); process.exit(1); }
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [before] = await conn.query(
            `SELECT id, filename, supplier FROM qc_reports WHERE id IN (?) AND deleted_at IS NULL`, [ids]
        );
        const [res] = await conn.query(
            `UPDATE qc_reports SET deleted_at = NOW() WHERE id IN (?) AND deleted_at IS NULL`, [ids]
        );
        for (const r of before) console.log(`  - soft-deleted ${r.id}  ${r.supplier || ''}  ${r.filename || ''}`);
        console.log(`Soft-deleted ${res.affectedRows} of ${ids.length} requested.`);
    } finally { conn.release(); await closePool(); }
})().catch(e => { console.error(e.message); process.exit(1); });
