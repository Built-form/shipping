'use strict';

// Let the supplier portal say when and from where a Request to send's shipment
// leaves, read live (docs/request-to-send-portal.md, "The day and port on the
// portal"): of an order its supplier, port and estimated departure day (a
// booked container's day and port are kept on its orders, and `supplier` tells
// one supplier's orders from another's in the same container), and of the
// shipment's own row its day and port of origin. The user asked for these on
// 2026-11-16, in place of what the email said (request_etd / request_port on
// the send, which the portal no longer shows once it can read these).
// NOT when it arrives, the vessel, the forwarder, the B/L or any price.
//
// The portal's db/grants/jportal_app.sql carries the same statements, and its
// own deploy applies that file. Only where the portal's login exists (a GRANT
// to a missing account is error 1133). GRANT is idempotent, so a re-run changes
// nothing.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const GRANTS = [
    ['shipments', 'etd, origin_port'],
    ['orders', 'supplier, port, estimated_departure_date'],
];

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
    for (const [table, columns] of GRANTS) {
        await conn.query(`GRANT SELECT (${columns}) ON \`${schema}\`.${table} TO ${to}`);
    }
    log(`${to} granted a shipment's departure day and port on ${schema}`);
};
