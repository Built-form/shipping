'use strict';

// Let the supplier portal read freight quote requests, save a forwarder's
// options and answer a booking (2026-11-16_10, docs/freight-quotes.md). The
// portal's db/grants/jportal_app.sql carries the same statements; this applies
// them on deploy so the tables and their writer land together. Only where the
// portal's login exists on this server (a GRANT to a missing account is error
// 1133). GRANT is idempotent, so a re-run changes nothing.
//
// The portal never reads who a request or a booking was emailed to, who sent
// it, or the address an answer came from; it never writes the revoke columns,
// a booking's cancellation or its contents columns; and of the forwarder list
// it reads only which address belongs to which forwarder.

const PORTAL_USER = 'jportal_app';
const PORTAL_HOST = '%';

const REQUEST_SELECT = 'id, token, forwarder_id, forwarder_name, window_start_date, window_end_date, origin, '
    + 'destination, container_size, services, staff_note, created_at, expires_at, revoked_at, revoke_reason, '
    + 'answer_count, answered_at, answered_by_name, answer_note, declined, row_version';
const REQUEST_UPDATE = 'answer_count, answered_at, answered_by_name, answered_by_email, answer_source, '
    + 'answer_note, declined, answer_ip, row_version';
const OPTION_INSERT = 'request_id, answer_no, line_no, loading_date, sailing_date, transit_days, port_from, '
    + 'port_to, service, container_size, price, currency, carrier, valid_until, note';
const BOOKING_SELECT = 'id, request_id, option_id, status, requested_at, request_note, booking_ref, '
    + 'response_note, responded_at, responded_by_name, response_source, cancelled_at, row_version';
const BOOKING_UPDATE = 'status, booking_ref, response_note, responded_at, responded_by_name, '
    + 'responded_by_email, response_source, response_ip, row_version';
const CONTACT_SELECT = 'forwarder_id, email, deleted_at';

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
    const on = table => `\`${schema}\`.${table}`;
    await conn.query(`GRANT SELECT (${REQUEST_SELECT}) ON ${on('freight_quote_requests')} TO ${to}`);
    await conn.query(`GRANT UPDATE (${REQUEST_UPDATE}) ON ${on('freight_quote_requests')} TO ${to}`);
    await conn.query(`GRANT SELECT ON ${on('freight_quote_options')} TO ${to}`);
    await conn.query(`GRANT INSERT (${OPTION_INSERT}) ON ${on('freight_quote_options')} TO ${to}`);
    await conn.query(`GRANT SELECT (${BOOKING_SELECT}) ON ${on('freight_bookings')} TO ${to}`);
    await conn.query(`GRANT UPDATE (${BOOKING_UPDATE}) ON ${on('freight_bookings')} TO ${to}`);
    await conn.query(`GRANT SELECT (${CONTACT_SELECT}) ON ${on('forwarder_contacts')} TO ${to}`);
    log(`${to} granted the freight quote columns on ${schema}`);
};
