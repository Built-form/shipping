'use strict';

// Seed ready_date_reminder (category 'Supplier portal'): the email that asks a
// supplier for the ready dates they owe us, every working day: the estimated
// date still missing a week after the final artwork was confirmed, and the
// actual date once the estimated one has passed (src/services/ready-date-reminders.js,
// docs/ready-date-reminders.md). INSERT IGNORE, as
// 2026-11-11_20_seed_artwork_confirmation_template.js: an existing row is never
// overwritten.

const { READY_DATE_REMINDER_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
