'use strict';

// Let the supplier portal read a freight quote request's stock movements
// (2026-11-16_30, docs/freight-quotes.md): the forwarder's quote page lists
// them. One more column on the SELECT grant of 2026-11-16_11; the portal's
// db/grants/jportal_app.sql carries the same statement. Only where the
// portal's login exists on this server (a GRANT to a missing account is error
// 1133). GRANT is idempotent, so a re-run changes nothing.
//
// The portal does not read freight_delivery_options: a request carries the
// text of the options chosen for it.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

exports.up = async (conn, { log }) => {
    const [users] = await conn.query(
        'SELECT 1 FROM mysql.user WHERE user = ? AND host = ?',
        [PORTAL_USER, PORTAL_HOST]
    );
    if (!users.length) {
        log(`no '${PORTAL_USER}'@'${PORTAL_HOST}' on this server; no grants to apply`);
        return;
    }
    const [[{ schema }]] = await conn.query('SELECT DATABASE() AS `schema`');
    const to = `'${PORTAL_USER}'@'${PORTAL_HOST}'`;
    await conn.query(`GRANT SELECT (stock_movements) ON \`${schema}\`.freight_quote_requests TO ${to}`);
    log(`${to} granted freight_quote_requests.stock_movements on ${schema}`);
};
