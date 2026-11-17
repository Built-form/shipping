'use strict';

// Let the supplier portal show a forwarder the documents the contents email
// attached (2026-11-17_10, docs/freight-quotes.md): when the contents were
// sent, and which draft_container_documents rows went with them. Two more
// columns on the SELECT grant of 2026-11-16_11; the portal's
// db/grants/jportal_app.sql carries the same statement. The documents
// themselves (id, type, supplier, version, the two addresses, deleted_at) are
// already granted, for the Requests to send page. Only where the portal's
// login exists on this server (a GRANT to a missing account is error 1133).
// GRANT is idempotent, so a re-run changes nothing.
//
// Still not granted: contents_sent_to (who it went to), contents_json (the
// lines) and the attached container.

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
    await conn.query(`GRANT SELECT (contents_sent_at, contents_document_ids) ON \`${schema}\`.freight_bookings TO ${to}`);
    log(`${to} granted freight_bookings.contents_sent_at and contents_document_ids on ${schema}`);
};
