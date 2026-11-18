'use strict';

// Seed the four "Send to supplier" templates of a recorded payment (category
// 'Payment proof'; docs/payment-proof-send.md): a deposit and a balance, each
// as a link to the supplier portal and with the proof attached. INSERT IGNORE,
// as the other seeds: a row someone has edited is never overwritten, and the
// send falls back to the in-code default if a row is missing.

const { PAYMENT_PROOF_TEMPLATES } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    for (const t of PAYMENT_PROOF_TEMPLATES) {
        const [r] = await conn.query(
            `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
        );
        log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);
    }
};
