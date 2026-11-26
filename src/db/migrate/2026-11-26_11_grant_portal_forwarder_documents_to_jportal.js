'use strict';

// Let the supplier portal read a container's papers (2026-11-26_10,
// docs/forwarder-containers.md "A container's documents"): SELECT on the one
// view, portal_forwarder_documents_v. The portal's db/grants/jportal_app.sql
// carries the same statement. Approved by the user on 2026-11-26 (the plan
// they told to be built names this grant).
//
// Only where the portal's login exists on this server (a GRANT to a missing
// account is error 1133). GRANT is idempotent, so a re-run changes nothing.
//
// Nothing else is granted: not the tables under the view (the packing lists,
// the invoices, the telex releases, the forwarders' documents), and no write.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const VIEWS = ['portal_forwarder_documents_v'];

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
    log(`${to} granted the portal forwarder documents view on ${schema}`);
};
