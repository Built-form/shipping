'use strict';

// Seed request_to_send_update_portal (category 'Supplier Quote'): the email for
// a NEW VERSION of a Request to send the supplier was already sent
// (docs/request-to-send-portal.md, "Updates"). INSERT IGNORE, as
// 2026-11-13_20: an existing row is never overwritten.

const { REQUEST_TO_SEND_UPDATE_PORTAL_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
