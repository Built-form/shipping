'use strict';

// artwork_confirmation, second rewording of the day (after 2026-11-11_30): the
// email is about RECEIPT. We have approved the final artwork; the supplier
// confirms they have all of it and are happy to proceed. The subject says so
// now, and the body's last sentence asks for that instead of "tick each item
// whose artwork you have confirmed".
//
// Overwrites the stored subject, body and description on purpose, as _30 did:
// the wording comes from ops and the row is hours old. Runs once, so a later
// edit in ShipLine's template editor is kept.

const { ARTWORK_CONFIRMATION_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        'UPDATE email_templates SET subject = ?, body_html = ?, description = ? WHERE template_key = ? AND deleted_at IS NULL',
        [t.subject, t.bodyHtml, t.description || null, t.key]
    );
    log(r.affectedRows ? `${t.key} reworded` : `${t.key} not found; nothing to reword`);
};
