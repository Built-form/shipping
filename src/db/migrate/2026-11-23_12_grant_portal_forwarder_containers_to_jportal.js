'use strict';

// Let the portal read the containers of a freight forwarder
// (docs/forwarder-containers.md): SELECT on the view of 2026-11-23_11, and on
// nothing under it. The portal's login gets no new right on shipments, orders,
// freight_bookings or a payment table. The user agreed to this grant on
// 2026-11-23. (portal_forwarder_payment_lines_v, re-created with one more
// column in the same migration, was granted by 2026-11-20_22: a view replaced
// with CREATE OR REPLACE keeps its grants.)
//
// The portal's db/grants/jportal_app.sql carries the same statement, and its
// own deploy applies that file. Only where the portal's login exists (a GRANT
// to a missing account is error 1133). GRANT is idempotent, so a re-run changes
// nothing.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const VIEWS = ['portal_forwarder_containers_v'];

exports.VIEWS = VIEWS;

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
    for (const view of VIEWS) {
        await conn.query(`GRANT SELECT ON \`${schema}\`.${view} TO ${to}`);
    }
    log(`${to} granted the portal forwarder containers view on ${schema}`);
};
