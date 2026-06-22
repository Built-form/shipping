'use strict';
require('dotenv').config();
const { getPool, closePool } = require('../src/db');

(async () => {
    const conn = await getPool().getConnection();
    try {
        const [rows] = await conn.query(`SHOW COLUMNS FROM allowed_emails`);
        for (const r of rows) {
            console.log(`  ${String(r.Field).padEnd(16)} ${String(r.Type).padEnd(20)} null=${r.Null}  default=${r.Default}`);
        }
    } finally {
        conn.release();
        await closePool();
    }
})().catch(err => { console.error(err); process.exit(1); });
