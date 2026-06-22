'use strict';

// Bulk-import additional supplier emails from supplier-emails-import.csv.
//   - id column = suppliers.id (must already exist)
//   - cols 1..4 = additional email addresses; non-empty ones are inserted
//   - INSERT IGNORE on UNIQUE (supplier_id, email) → duplicates are skipped
//   - All imported as is_primary=0; the backfill already set primaries
//     for suppliers with a Mintsoft contact_email
//
// Reports: total inserted vs skipped, missing suppliers, name mismatches.

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { getPool, closePool } = require('../src/db');

// Minimal RFC4180 parser: handles quoted fields, escaped quotes, embedded commas
function parseCsv(text) {
    const rows = [];
    let cur = [];
    let field = '';
    let inQuotes = false;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (inQuotes) {
            if (ch === '"' && text[i + 1] === '"') { field += '"'; i++; }
            else if (ch === '"') inQuotes = false;
            else field += ch;
        } else {
            if (ch === '"') inQuotes = true;
            else if (ch === ',') { cur.push(field); field = ''; }
            else if (ch === '\n') { cur.push(field); rows.push(cur); cur = []; field = ''; }
            else if (ch !== '\r') field += ch;
        }
    }
    if (field.length || cur.length) { cur.push(field); rows.push(cur); }
    return rows;
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const norm = s => (s || '').toLowerCase().replace(/[^a-z0-9]/g, '');

(async () => {
    const csvPath = path.join(__dirname, 'supplier-emails-import.csv');
    const text = fs.readFileSync(csvPath, 'utf8');
    const rows = parseCsv(text).filter(r => r.length > 0 && r.some(c => c !== ''));
    const header = rows.shift();
    console.log(`CSV header: ${header.join(' | ')}`);
    console.log(`Data rows: ${rows.length}\n`);

    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        const [before] = await conn.query(`SELECT COUNT(*) AS n FROM supplier_emails`);
        console.log(`supplier_emails before: ${before[0].n}`);

        let attempted = 0;
        let inserted = 0;
        let invalidEmails = 0;
        const missingSuppliers = [];
        const nameMismatches = [];

        for (const row of rows) {
            const supplierId = Number(row[0]);
            const csvName = (row[1] || '').trim();
            if (!Number.isInteger(supplierId) || supplierId <= 0) continue;

            const [supRows] = await conn.query(
                `SELECT id, name FROM suppliers WHERE id = ? AND deleted_at IS NULL`,
                [supplierId]
            );
            if (!supRows.length) {
                missingSuppliers.push({ id: supplierId, csvName });
                continue;
            }
            const dbName = supRows[0].name;
            if (csvName && dbName && norm(csvName) !== norm(dbName)) {
                nameMismatches.push({ id: supplierId, csvName, dbName });
            }

            for (let col = 2; col <= 5; col++) {
                const raw = (row[col] || '').trim();
                if (!raw) continue;
                if (!EMAIL_RE.test(raw)) {
                    console.warn(`  supplier ${supplierId} col ${col - 1}: invalid email "${raw}"`);
                    invalidEmails++;
                    continue;
                }
                attempted++;
                const [result] = await conn.execute(
                    `INSERT IGNORE INTO supplier_emails (supplier_id, email, label, is_primary)
                     VALUES (?, ?, NULL, 0)`,
                    [supplierId, raw]
                );
                if (result.affectedRows > 0) inserted++;
            }
        }

        const [after] = await conn.query(`SELECT COUNT(*) AS n FROM supplier_emails`);
        console.log(`supplier_emails after:  ${after[0].n}`);
        console.log(`\nResults:`);
        console.log(`  attempted:       ${attempted}`);
        console.log(`  inserted:        ${inserted}`);
        console.log(`  skipped (dup):   ${attempted - inserted}`);
        console.log(`  invalid emails:  ${invalidEmails}`);

        if (missingSuppliers.length) {
            console.warn(`\n${missingSuppliers.length} supplier id(s) not found in DB:`);
            console.table(missingSuppliers);
        }
        if (nameMismatches.length) {
            console.log(`\n${nameMismatches.length} name mismatch(es) (still imported — first 10):`);
            console.table(nameMismatches.slice(0, 10));
        }
    } catch (err) {
        console.error('Fatal:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
