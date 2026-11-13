'use strict';

// Let the supplier portal read what its Requests to send page shows beside each
// emailed packing list (docs/request-to-send-portal.md): the CSV's address, and
// the ETD and port of departure the email gave (2026-11-13_10). The portal's
// db/grants/jportal_app.sql carries the same statements; this applies them on
// deploy so the columns and their reader land together. Only where the
// portal's login exists on this server (a GRANT to a missing account is error
// 1133). GRANT is idempotent, so a re-run changes nothing.
//
// Column grants add to the ones the portal already holds on these two tables
// (id, draft_container_name, version, type, supplier, public_url, deleted_at;
// draft_container_document_id, sent_at). Still NOT granted: sent_to, subject,
// sent_by_email and delivery: who an email went to, who sent it and how are
// staff information.

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
    await conn.query(`GRANT SELECT (csv_public_url) ON \`${schema}\`.draft_container_documents TO ${to}`);
    await conn.query(`GRANT SELECT (request_etd, request_port) ON \`${schema}\`.draft_container_document_sends TO ${to}`);
    log(`${to} granted the Request to send columns on ${schema}`);
};
