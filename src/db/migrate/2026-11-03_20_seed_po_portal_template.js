'use strict';

// Seed purchase_order_portal (category 'PO'): the PO send's email now links to
// the PDF on the supplier portal instead of attaching it
// (docs/po-portal-send.md). INSERT IGNORE, as
// 2026-09-21_11_seed_email_templates.js: an existing row is never overwritten.
// The old purchase_order template ("Please see attached PO") is left as it is.

const { DEFAULT_EMAIL_TEMPLATES, PO_PORTAL_TEMPLATE_KEY } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === PO_PORTAL_TEMPLATE_KEY);
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
