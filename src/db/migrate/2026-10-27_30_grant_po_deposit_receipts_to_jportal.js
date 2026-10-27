'use strict';

// Let the supplier portal write po_deposit_receipts (2026-10-27_10): it is the
// one jfa table the portal writes, under column grants. The portal's
// db/grants/jportal_app.sql is the source of truth for its grants and carries
// the same three statements; this applies them on deploy so the table and its
// writer land together. Only where the portal's login exists on this server —
// production has no portal, and a GRANT to a missing account is an error
// (1133), not a no-op. GRANT is idempotent, so a re-run changes nothing.

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
    const table = `\`${schema}\`.po_deposit_receipts`;
    const to = `'${PORTAL_USER}'@'${PORTAL_HOST}'`;
    await conn.query(`GRANT SELECT ON ${table} TO ${to}`);
    await conn.query(
        `GRANT INSERT (purchase_order_id, supplier_id, user_id, confirmed_by_email, received_on) ON ${table} TO ${to}`
    );
    await conn.query(
        `GRANT UPDATE (supplier_id, user_id, confirmed_by_email, received_on) ON ${table} TO ${to}`
    );
    log(`${to} granted SELECT, INSERT and UPDATE columns on ${table}`);
};
