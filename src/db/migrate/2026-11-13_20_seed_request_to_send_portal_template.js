'use strict';

// Seed request_to_send_portal (category 'Supplier Quote'): the Request to send
// email for a supplier who is on the supplier portal, which links to the
// packing list there instead of attaching it (docs/request-to-send-portal.md).
// INSERT IGNORE, as 2026-11-03_20_seed_po_portal_template.js: an existing row
// is never overwritten. The templates ops wrote for the attached email
// ("Please see the attached packing list ...") are left as they are: they are
// still what a request goes out with when the portal cannot show it.

const { REQUEST_TO_SEND_PORTAL_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
