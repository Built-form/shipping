'use strict';
// Manage per-supplier access codes for the public "ready date" portal.
//
//   node tools/portal-codes.js                 # list every supplier + code
//   node tools/portal-codes.js --all           # regenerate EVERY supplier's code
//   node tools/portal-codes.js <supplierId>    # rotate one supplier's code
//   node tools/portal-codes.js <supplierId> <CODE>  # set an explicit code
//
// Run via the PowerShell tool on this box (Bash has PATH issues for node).
require('dotenv').config();
const { getPool, closePool } = require('../src/db');
const { generatePortalCode, normalizeCode, ensureSupplierPortalCodes } = require('../src/lib/portal-code');

const idArg = process.argv[2];
const codeArg = process.argv[3];

(async () => {
    const conn = await getPool().getConnection();
    try {
        // Make sure the column exists and every supplier has a code before we
        // list/rotate (no-op once it's been run).
        await ensureSupplierPortalCodes(conn);

        if (!idArg) {
            const [rows] = await conn.query(
                `SELECT id, name, portal_code FROM suppliers
                  WHERE deleted_at IS NULL ORDER BY name ASC`
            );
            console.log(`Supplier portal codes (${rows.length}):`);
            for (const r of rows) {
                console.log(`  [${String(r.id).padStart(4)}] ${r.portal_code || '(none)'}  ${r.name}`);
            }
            return;
        }

        // Regenerate every active supplier's code with the current alphabet.
        // Invalidates any previously shared code — only safe before codes are
        // distributed (or when deliberately rotating everyone).
        if (idArg === '--all' || idArg === 'all') {
            const [rows] = await conn.query(
                `SELECT id, name FROM suppliers WHERE deleted_at IS NULL ORDER BY name ASC`
            );
            for (const r of rows) {
                const code = generatePortalCode();
                await conn.query('UPDATE jfpro.suppliers SET portal_code = ? WHERE id = ?', [code, r.id]);
                console.log(`  [${String(r.id).padStart(4)}] ${code}  ${r.name}`);
            }
            console.log(`Regenerated portal codes for ${rows.length} suppliers.`);
            return;
        }

        const id = Number(idArg);
        if (!Number.isInteger(id) || id <= 0) {
            console.error(`Invalid supplier id: ${idArg}`);
            process.exit(1);
        }
        const [supRows] = await conn.query(
            'SELECT id, name FROM suppliers WHERE id = ? AND deleted_at IS NULL',
            [id]
        );
        if (!supRows[0]) {
            console.error(`No active supplier with id ${id}.`);
            process.exit(1);
        }

        const newCode = codeArg ? normalizeCode(codeArg) : generatePortalCode();
        if (!newCode) {
            console.error('Explicit code is empty after normalisation (letters/digits only).');
            process.exit(1);
        }
        await conn.query('UPDATE jfpro.suppliers SET portal_code = ? WHERE id = ?', [newCode, id]);
        console.log(`Supplier [${id}] ${supRows[0].name}`);
        console.log(`New portal code: ${newCode}`);
    } finally {
        conn.release();
        await closePool();
    }
})().catch(e => { console.error(e); process.exit(1); });
