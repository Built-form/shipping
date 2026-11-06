'use strict';

// Seed file_transfer_portal (category 'Supplier portal'): the email a supplier's
// portal users get when our team send them a file from ShipLine's SUPPLIER
// PORTAL > Files (src/services/file-transfer-routes.js,
// docs/file-transfers-frontend.md). INSERT IGNORE, as
// 2026-11-03_20_seed_po_portal_template.js: an existing row is never overwritten.

const { FILE_TRANSFER_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
