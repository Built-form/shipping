'use strict';

// Seed sample_order_portal (category 'PO'): the email a sample order's send
// starts from (docs/sample-orders.md, "Sending it"). A sample order goes out by
// the PO send (POST /api/v1/purchase-order-documents/:id/email), which until
// now had only the production PO's words: "send back a PI", "I will send
// artwork shortly". INSERT IGNORE, as 2026-11-03_20_seed_po_portal_template.js:
// an existing row is never overwritten. The send works without this row (it
// falls back to the in-code default); the row is what lists the template in
// ShipLine, where it can be edited and picked.

const { DEFAULT_EMAIL_TEMPLATES, SAMPLE_ORDER_PORTAL_TEMPLATE_KEY } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === SAMPLE_ORDER_PORTAL_TEMPLATE_KEY);
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
