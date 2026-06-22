'use strict';

// Exercises the GET /suppliers/:id/emails logic directly against the DB.
// Picks a supplier with a contact_email, runs the lazy backfill, prints
// the resulting supplier_emails rows. Optional CLI arg: supplier id.

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

const supplierIdArg = process.argv[2] ? Number(process.argv[2]) : null;

(async () => {
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        // Make sure the table exists (orders.js creates it on first hit
        // via supplierEmailsSchemaReady; we re-create idempotently here).
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

        let supplierId = supplierIdArg;
        if (!supplierId) {
            const [pick] = await conn.query(`
                SELECT id, name, contact_email FROM suppliers
                 WHERE contact_email IS NOT NULL AND contact_email <> ''
                   AND deleted_at IS NULL
                 ORDER BY id ASC LIMIT 1
            `);
            if (!pick.length) {
                console.error('No supplier with a contact_email found.');
                process.exit(1);
            }
            supplierId = pick[0].id;
            console.log(`Picked supplier ${pick[0].id} "${pick[0].name}" (contact_email=${pick[0].contact_email})`);
        }

        const [supRows] = await conn.query(
            `SELECT id, name, contact_email FROM suppliers WHERE id = ? AND deleted_at IS NULL`,
            [supplierId]
        );
        if (!supRows.length) {
            console.error(`Supplier ${supplierId} not found.`);
            process.exit(1);
        }
        const sup = supRows[0];

        const fetchEmails = async () => {
            const [r] = await conn.query(
                `SELECT id, supplier_id, email, label, is_primary, created_at
                   FROM supplier_emails
                  WHERE supplier_id = ? AND deleted_at IS NULL
                  ORDER BY is_primary DESC, email ASC`,
                [supplierId]
            );
            return r;
        };

        let rows = await fetchEmails();
        if (rows.length === 0 && sup.contact_email) {
            console.log('No supplier_emails rows yet — backfilling from suppliers.contact_email…');
            await conn.execute(
                `INSERT IGNORE INTO supplier_emails (supplier_id, email, label, is_primary)
                 VALUES (?, ?, 'Primary', 1)`,
                [supplierId, sup.contact_email.trim()]
            );
            rows = await fetchEmails();
        }

        console.log(`\nSupplier ${sup.id} "${sup.name}" emails:`);
        console.table(rows.map(r => ({
            id: r.id,
            email: r.email,
            label: r.label,
            isPrimary: r.is_primary,
            createdAt: r.created_at,
        })));
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
