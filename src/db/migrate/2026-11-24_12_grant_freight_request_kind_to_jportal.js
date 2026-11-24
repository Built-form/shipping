'use strict';

// Let the supplier portal read a quote request's KIND (2026-11-24_10,
// docs/freight-quotes.md "A delivery request"): freight, or delivery (a UK
// haulier, port to warehouse). The portal shows a delivery request with its own
// words (collection, days to deliver) and judges its options by its own rule,
// so it has to know which it is. One more column on the SELECT grant of
// 2026-11-16_11; the portal's db/grants/jportal_app.sql carries the same
// statement. Approved by the user on 2026-11-24.
//
// Only where the portal's login exists on this server (a GRANT to a missing
// account is error 1133). GRANT is idempotent, so a re-run changes nothing.
//
// Nothing else is granted: the portal writes no new column (a delivery option
// goes into the columns its INSERT grant already names).

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
    await conn.query(`GRANT SELECT (kind) ON \`${schema}\`.freight_quote_requests TO ${to}`);
    log(`${to} granted freight_quote_requests.kind on ${schema}`);
};
