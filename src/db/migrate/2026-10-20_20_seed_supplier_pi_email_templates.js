'use strict';

// Seed the two supplier-portal PI decision emails (supplier_pi_imported,
// supplier_pi_rejected; category 'Supplier portal', sent by
// src/services/pi-submission-routes.js). INSERT IGNORE, as
// 2026-09-21_11_seed_email_templates.js: only missing keys are created, an ops
// edit to an existing template is never overwritten.

const { DEFAULT_EMAIL_TEMPLATES, SUPPLIER_PI_TEMPLATE_KEYS } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const templates = DEFAULT_EMAIL_TEMPLATES.filter(t => SUPPLIER_PI_TEMPLATE_KEYS.includes(t.key));
    let created = 0;
    for (const t of templates) {
        const [r] = await conn.query(
            `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
        );
        created += r.affectedRows;
    }
    log(`${created} of ${templates.length} template(s) created, the rest already existed`);
};
