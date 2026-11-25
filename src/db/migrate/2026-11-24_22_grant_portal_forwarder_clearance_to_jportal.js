'use strict';

// Let the supplier portal read the clearance instructions sent to a forwarder
// (2026-11-24_21, docs/clearance-instructions.md): SELECT on the one view,
// portal_forwarder_clearance_v. The portal's db/grants/jportal_app.sql carries
// the same statement. Approved by the user on 2026-11-24 (they chose "email +
// portal", told it needs one new read permission for the portal's login).
//
// Only where the portal's login exists on this server (a GRANT to a missing
// account is error 1133). GRANT is idempotent, so a re-run changes nothing.
//
// Nothing else is granted: not the table under the view, not the duty rates,
// and no write. The portal has no way to change a sheet.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const VIEWS = ['portal_forwarder_clearance_v'];

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
    log(`${to} granted the portal forwarder clearance view on ${schema}`);
};
