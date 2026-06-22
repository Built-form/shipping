'use strict';

// Idempotent migration for supplier_emails:
//   1. Creates the table if it doesn't exist.
//   2. Backfills every supplier with a non-empty suppliers.contact_email
//      into supplier_emails as the primary row.
//
// Safe to re-run — INSERT IGNORE skips suppliers that already have a row
// for their primary address. Soft-deleted suppliers are excluded.

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        console.log('Creating supplier_emails table (idempotent)…');
        await conn.execute(`
            CREATE TABLE IF NOT EXISTS supplier_emails (
                id INT NOT NULL AUTO_INCREMENT,
                supplier_id INT NOT NULL,
                email VARCHAR(255) NOT NULL,
                label VARCHAR(64) NULL,
                is_primary TINYINT(1) NOT NULL DEFAULT 0,
                created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uk_supplier_email (supplier_id, email),
                KEY idx_supplier_id (supplier_id)
            )
        `);

        // Single INSERT IGNORE…SELECT does the entire backfill atomically.
        // Unique key (supplier_id, email) handles dedup; any supplier whose
        // primary email is already present is skipped silently.
        console.log('Backfilling primary emails from suppliers.contact_email…');
        const [result] = await conn.execute(`
            INSERT IGNORE INTO supplier_emails (supplier_id, email, label, is_primary)
            SELECT id, TRIM(contact_email), 'Primary', 1
              FROM suppliers
             WHERE contact_email IS NOT NULL
               AND TRIM(contact_email) <> ''
               AND deleted_at IS NULL
        `);
        console.log(`  Inserted ${result.affectedRows} row(s).`);

        // Stats: how many suppliers / how many emails do we have now?
        const [stats] = await conn.query(`
            SELECT
                (SELECT COUNT(*) FROM suppliers WHERE deleted_at IS NULL) AS total_suppliers,
                (SELECT COUNT(*) FROM suppliers WHERE deleted_at IS NULL AND contact_email IS NOT NULL AND TRIM(contact_email) <> '') AS suppliers_with_primary,
                (SELECT COUNT(*) FROM supplier_emails WHERE deleted_at IS NULL) AS supplier_emails_rows,
                (SELECT COUNT(DISTINCT supplier_id) FROM supplier_emails WHERE deleted_at IS NULL) AS suppliers_with_emails
        `);
        console.log('\nDB state after migration:');
        console.table(stats);
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
