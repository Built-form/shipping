'use strict';

// Seed artwork_confirmation (category 'Artwork'): the email asking a supplier
// to confirm final artwork per PO line, sent from a PO in ShipLine
// (src/services/artwork-confirmation-routes.js, docs/artwork-confirmations.md).
// INSERT IGNORE, as 2026-11-04_20_seed_file_transfer_template.js: an existing
// row is never overwritten.

const { ARTWORK_CONFIRMATION_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
