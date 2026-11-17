'use strict';

// Let the supplier portal read a request's stock movements and save the
// forwarder's price for each (2026-11-17_20, docs/freight-quotes.md). The
// portal's db/grants/jportal_app.sql carries the same statements. Only where
// the portal's login exists on this server (a GRANT to a missing account is
// error 1133). GRANT is idempotent, so a re-run changes nothing.
//
// The portal reads what its page shows of a movement and whether it was
// removed: NOT who added it, who removed it, the address or IP a price came
// from. It writes the price columns and row_version only: never the factory,
// the volume, the weight or deleted_at.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const MOVEMENT_SELECT = 'id, request_id, line_no, factory, cbm, weight_kg, created_at, deleted_at, '
    + 'price, currency, price_note, priced_at, priced_by_name, price_source, row_version';
const MOVEMENT_UPDATE = 'price, currency, price_note, priced_at, priced_by_name, priced_by_email, '
    + 'price_source, price_ip, row_version';

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
    await conn.query(`GRANT SELECT (${MOVEMENT_SELECT}) ON \`${schema}\`.freight_stock_movements TO ${to}`);
    await conn.query(`GRANT UPDATE (${MOVEMENT_UPDATE}) ON \`${schema}\`.freight_stock_movements TO ${to}`);
    log(`${to} granted the freight_stock_movements columns on ${schema}`);
};
