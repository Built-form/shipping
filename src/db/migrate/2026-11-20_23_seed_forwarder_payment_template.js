'use strict';

// Seed payment_proof_forwarder_portal ("Forwarder paid", category 'Payment
// proof'): the email that tells a freight forwarder's contacts a payment to
// the forwarder has been made, with a link to it on the portal
// (docs/payment-proof-send.md, "A forwarder's payment"; sent from ShipLine's
// "Send to forwarder"). INSERT IGNORE, as
// 2026-11-18_20_seed_ready_date_reminder_template.js: an existing row is never
// overwritten.

const { FORWARDER_PAYMENT_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
};
