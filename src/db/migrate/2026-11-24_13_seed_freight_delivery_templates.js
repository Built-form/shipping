'use strict';

// Seed the two templates of a DELIVERY request (2026-11-24, docs/freight-quotes.md
// "A delivery request"): the quote request and the booking request sent to a UK
// haulier, whose words are a collection and a delivery, not a sailing. INSERT
// IGNORE, as 2026-11-16_20_seed_freight_templates.js: an existing row is never
// overwritten.

const { FREIGHT_DELIVERY_TEMPLATES } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    for (const t of FREIGHT_DELIVERY_TEMPLATES) {
        const [r] = await conn.query(
            `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
        );
        log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
    }
};
