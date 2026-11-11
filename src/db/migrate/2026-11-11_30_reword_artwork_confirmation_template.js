'use strict';

// Reword artwork_confirmation (seeded earlier today by 2026-11-11_20): the email
// now says the final artwork has been reviewed and approved and asks the
// supplier to acknowledge it, with the carton note (glue, not staples), in
// English and in Chinese to match.
//
// Unlike the seeds (INSERT IGNORE), this OVERWRITES the stored body and
// description on purpose: the new wording was written by ops, and the row in
// each database is a few hours old. The subject, name and category are left as
// they are. Any later edit in ShipLine's template editor is kept: this file
// runs once.

const { ARTWORK_CONFIRMATION_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        'UPDATE email_templates SET body_html = ?, description = ? WHERE template_key = ? AND deleted_at IS NULL',
        [t.bodyHtml, t.description || null, t.key]
    );
    log(r.affectedRows ? `${t.key} reworded` : `${t.key} not found; nothing to reword`);
};
