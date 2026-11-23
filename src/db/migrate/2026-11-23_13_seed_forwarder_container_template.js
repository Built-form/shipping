'use strict';

// Seeds for a forwarder's container page (docs/forwarder-containers.md):
//
// - freight_container_details ("Freight container details", category
//   'Freight'): the email staff send from a container's FORWARDER tab, asking
//   the forwarder for its number, dates and papers, with a button to the
//   container's page on the portal. INSERT IGNORE, as
//   2026-11-20_23_seed_forwarder_payment_template.js: an existing row, and any
//   staff have edited, is never overwritten.
//
// - the photo description "Proof of loading": what an accepted loading photo
//   from a forwarder is filed under unless staff pick another. Revived if it
//   was removed, as re-adding a label by hand does; never duplicated
//   (uk_label).

const { FREIGHT_CONTAINER_TEMPLATE: t } = require('../../lib/email-template-defaults');

const PRESET = 'Proof of loading';

exports.PRESET = PRESET;

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [t.key, t.name, t.category || null, t.description || null, t.subject || null, t.bodyHtml]
    );
    log(r.affectedRows ? `${t.key} created` : `${t.key} already existed`);

    const [p] = await conn.query(
        `INSERT INTO container_photo_presets (label, sort_order) VALUES (?, 45)
         ON DUPLICATE KEY UPDATE deleted_at = NULL`,
        [PRESET]
    );
    log(p.affectedRows ? `photo description "${PRESET}" is in the list` : `photo description "${PRESET}" was already in the list`);
};
