'use strict';

// Let the supplier portal read artwork confirmation requests and write
// confirmations (2026-11-11_10, docs/artwork-confirmations.md). The portal's
// db/grants/jportal_app.sql carries the same statements; this applies them on
// deploy so the tables and their writer land together. Only where the portal's
// login exists on this server (a GRANT to a missing account is error 1133).
// GRANT is idempotent, so a re-run changes nothing.
//
// The portal never reads who a request was emailed to or who sent it, never
// writes applied_at, and reads one more column of orders:
// artwork_confirmed_by_supplier (2026-11-11_05). It never writes orders.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const REQUEST_COLUMNS = 'id, token, purchase_order_id, po_number, supplier_name, order_ids, created_at, expires_at, revoked_at, revoke_reason';
const CONFIRMATION_INSERT_COLUMNS = 'order_id, purchase_order_id, request_id, source, supplier_id, user_id, confirmed_by_email, confirmed_by_name, confirmed_on';

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
    await conn.query(`GRANT SELECT (${REQUEST_COLUMNS}) ON \`${schema}\`.artwork_confirmation_requests TO ${to}`);
    await conn.query(`GRANT SELECT ON \`${schema}\`.artwork_confirmations TO ${to}`);
    await conn.query(`GRANT INSERT (${CONFIRMATION_INSERT_COLUMNS}) ON \`${schema}\`.artwork_confirmations TO ${to}`);
    await conn.query(`GRANT SELECT (artwork_confirmed_by_supplier) ON \`${schema}\`.orders TO ${to}`);
    log(`${to} granted the artwork confirmation columns on ${schema}`);
};
