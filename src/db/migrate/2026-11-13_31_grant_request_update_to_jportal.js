'use strict';

// Let the supplier portal read what an update's email said (2026-11-13_30:
// draft_container_document_sends.request_update), so its Requests to send page
// says the same beside the PDF: which version it replaces and what changed
// (docs/request-to-send-portal.md, "Updates"). The portal's
// db/grants/jportal_app.sql carries the same statement. Only where the
// portal's login exists (a GRANT to a missing account is error 1133). GRANT is
// idempotent, so a re-run changes nothing.

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
    await conn.query(`GRANT SELECT (request_update) ON \`${schema}\`.draft_container_document_sends TO ${to}`);
    log(`${to} granted request_update on ${schema}`);
};
