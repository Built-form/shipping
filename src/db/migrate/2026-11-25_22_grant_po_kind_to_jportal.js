'use strict';

// Let the supplier portal read a purchase order's KIND (2026-11-25_20,
// docs/sample-orders.md "The portal"): stock, or sample. A sample order has its
// own page on the portal (no ready dates, no artwork, one "Mark as sent"
// action), so the portal has to know which a PO is. One more column on the
// SELECT grant of purchase_orders; the portal's db/grants/jportal_app.sql
// carries the same statement. Approved by the user on 2026-11-25, this one
// column and no other.
//
// Only where the portal's login exists on this server (a GRANT to a missing
// account is error 1133). GRANT is idempotent, so a re-run changes nothing.
//
// Nothing else is granted. The portal writes no column of this schema for a
// sample: "Mark as sent" is staged in the portal's own po_change_requests and
// applied by staff in ShipLine. A line's state comes from orders.status, which
// the portal already reads.

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
    await conn.query(`GRANT SELECT (kind) ON \`${schema}\`.purchase_orders TO ${to}`);
    log(`${to} granted purchase_orders.kind on ${schema}`);
};
