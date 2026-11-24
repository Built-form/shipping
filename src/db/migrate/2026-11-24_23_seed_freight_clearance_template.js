'use strict';

// Seed the email that sends a container's clearance instructions to the
// forwarder handling its customs (2026-11-24, docs/clearance-instructions.md):
// freight_clearance_instructions ("Freight clearance instructions", category
// Freight). INSERT IGNORE, as 2026-11-16_20_seed_freight_templates.js: an
// existing row (one our team have reworded) is never overwritten.

const { FREIGHT_CLEARANCE_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
