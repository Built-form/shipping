const mysql = require('mysql2/promise');

let pool;

function getPool() {
    if (!pool) {
        pool = mysql.createPool({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME,
            port: process.env.DB_PORT || 3306,
            ssl: { rejectUnauthorized: false },
            waitForConnections: true,
            connectionLimit: 1,
            // Return DATE columns as 'YYYY-MM-DD' strings rather than Date
            // objects. Otherwise mysql2 builds the Date at Node's local-tz
            // midnight, which on a BST client shifts a UTC DATE back by a day
            // when rendered via .toISOString(). DATETIME/TIMESTAMP still come
            // back as Date objects.
            dateStrings: ['DATE'],
        });
        pool.on('connection', (conn) => {
            conn.query('SET SESSION wait_timeout = 80, SESSION interactive_timeout = 80');
        });
    }
    return pool;
}

async function closePool() {
    if (pool) {
        await pool.end();
        pool = null;
    }
}

module.exports = { getPool, closePool };
