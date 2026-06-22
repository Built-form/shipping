'use strict';

// Dumps the raw ShipsGo payload for a container_number (or all of them)
// so we can see the real response shape and tighten the parser paths.
//
// Usage:
//   node tools/inspect-container.js                     # list all
//   node tools/inspect-container.js MSMU6128850         # one row, raw JSON
//   node tools/inspect-container.js MSMU6128850 keys    # just top-level keys

require('dotenv').config();
const { getPool, closePool } = require('../src/db');

async function main() {
    const [, , cn, mode] = process.argv;
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        if (!cn) {
            const [rows] = await conn.query(
                'SELECT container_number, fetched_at FROM containers ORDER BY fetched_at DESC'
            );
            console.log(`${rows.length} containers:`);
            for (const r of rows) console.log(`  ${r.container_number}  (fetched ${r.fetched_at})`);
            return;
        }

        const [rows] = await conn.query(
            'SELECT raw FROM containers WHERE container_number = ?',
            [cn]
        );
        if (rows.length === 0) {
            console.error(`No row for container ${cn}`);
            process.exit(1);
        }

        let raw = rows[0].raw;
        if (typeof raw === 'string') {
            try { raw = JSON.parse(raw); } catch { /* keep as string */ }
        }

        if (mode === 'keys') {
            const top = raw?.shipment || raw || {};
            console.log('Top-level keys:');
            for (const k of Object.keys(top).sort()) {
                const v = top[k];
                const t = v == null ? 'null'
                    : Array.isArray(v) ? `array[${v.length}]`
                    : typeof v === 'object' ? `object{${Object.keys(v).join(',')}}`
                    : `${typeof v}: ${JSON.stringify(v).slice(0, 60)}`;
                console.log(`  ${k}: ${t}`);
            }
            return;
        }

        console.log(JSON.stringify(raw, null, 2));
    } finally {
        conn.release();
        await closePool();
    }
}

main().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});
