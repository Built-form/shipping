'use strict';

// Seed the built-in email templates (was emailTemplatesSchemaReady in
// src/handlers/orders.js). INSERT IGNORE: only missing keys are created, an
// ops edit to an existing template is never overwritten.

const { DEFAULT_EMAIL_TEMPLATES } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    let created = 0;
    for (const t of DEFAULT_EMAIL_TEMPLATES) {
        const [r] = await conn.query(
            `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
        );
        created += r.affectedRows;
    }
    log(`${created} of ${DEFAULT_EMAIL_TEMPLATES.length} template(s) created, the rest already existed`);
};
