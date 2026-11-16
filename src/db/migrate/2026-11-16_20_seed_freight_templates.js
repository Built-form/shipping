'use strict';

// Seed the three 'Freight' templates: the quote request, the booking request
// and the container contents email, sent to a freight forwarder from ShipLine's
// FREIGHT page (src/services/freight-quote-routes.js, docs/freight-quotes.md).
// INSERT IGNORE, as 2026-11-11_20_seed_artwork_confirmation_template.js: an
// existing row is never overwritten.

const { FREIGHT_TEMPLATES } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    for (const t of FREIGHT_TEMPLATES) {
        const [r] = await conn.query(
            `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
        );
        log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
    }
};
