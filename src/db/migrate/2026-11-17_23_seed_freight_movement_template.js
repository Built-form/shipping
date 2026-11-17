'use strict';

// Seed the fourth 'Freight' template: the email that asks a forwarder to price
// stock movements added to a request after it was sent
// (POST /api/v1/freight-quotes/:id/stock-movements, docs/freight-quotes.md).
// The same loop as 2026-11-16_20: INSERT IGNORE, so the three templates that
// are already there, and any staff have edited, are never overwritten.

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
