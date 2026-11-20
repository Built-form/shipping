'use strict';

// Let the portal read a payment SENT TO A FORWARDER live
// (docs/payment-proof-send.md, "A forwarder's payment"): SELECT on the three
// views of 2026-11-20_21, and on nothing under them. The portal's login gets
// no new right on supplier_payments, its lines, payment_extras or the payment
// documents. The user agreed to this grant on 2026-11-20.
//
// The portal's db/grants/jportal_app.sql carries the same statements, and its
// own deploy applies that file. Only where the portal's login exists (a GRANT
// to a missing account is error 1133). GRANT is idempotent, so a re-run changes
// nothing.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const VIEWS = ['portal_forwarder_payments_v', 'portal_forwarder_payment_lines_v', 'portal_forwarder_payment_proofs_v'];

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
    log(`${to} granted the three portal forwarder payment views on ${schema}`);
};
