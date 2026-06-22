'use strict';

// Run an ad-hoc SQL query against the configured DB.
//
// Usage:
//   node tools/sql.js "SELECT 1"
//   node tools/sql.js -f path/to/query.sql
//   echo "SELECT 1" | node tools/sql.js -

require('dotenv').config();
const fs = require('fs');
const { getPool, closePool } = require('../src/db');

function readSql() {
    const args = process.argv.slice(2);
    if (args.length === 0) {
        console.error('usage: node tools/sql.js "<sql>" | -f <file> | -');
        process.exit(1);
    }
    if (args[0] === '-f') return fs.readFileSync(args[1], 'utf8');
    if (args[0] === '-')  return fs.readFileSync(0, 'utf8');
    return args.join(' ');
}

function splitStatements(sql) {
    return sql
        .split(/;\s*/)
        .map(s => s.trim())
        .filter(Boolean);
}

function printRows(rows, fields) {
    if (!Array.isArray(rows) || rows.length === 0) {
        console.log('(0 rows)');
        return;
    }
    const cols = fields ? fields.map(f => f.name) : Object.keys(rows[0]);
    const widths = cols.map(c =>
        Math.max(c.length, ...rows.map(r => String(r[c] ?? '').length))
    );
    const fmt = (vals) => vals.map((v, i) => String(v ?? '').padEnd(widths[i])).join(' | ');
    console.log(fmt(cols));
    console.log(widths.map(w => '-'.repeat(w)).join('-+-'));
    for (const row of rows) console.log(fmt(cols.map(c => row[c])));
    console.log(`(${rows.length} row${rows.length === 1 ? '' : 's'})`);
}

(async () => {
    const sql = readSql();
    const statements = splitStatements(sql);
    const pool = getPool();
    const conn = await pool.getConnection();
    try {
        for (const stmt of statements) {
            console.log(`\n> ${stmt.split('\n')[0]}${stmt.includes('\n') ? ' …' : ''}`);
            const [result, fields] = await conn.query(stmt);
            if (Array.isArray(result)) {
                printRows(result, fields);
            } else {
                console.log(`affectedRows=${result.affectedRows} changedRows=${result.changedRows ?? '-'}`);
            }
        }
    } catch (err) {
        console.error('ERROR:', err.message);
        process.exitCode = 1;
    } finally {
        conn.release();
        await closePool();
    }
})();
