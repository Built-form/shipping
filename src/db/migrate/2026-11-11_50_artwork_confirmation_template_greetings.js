'use strict';

// artwork_confirmation, third rewording of the day (after _30 and _40): the
// body ops supplied. "Greetings," / the final artwork for the selected items
// is approved / proceed, cartons with glue rather than staples / click the
// button, view the approved items and tick each one to acknowledge receipt.
// Chinese to match. The subject and the fixed block under the body are as
// _40 left them.
//
// Overwrites the stored body on purpose, as _30 and _40 did: the wording comes
// from ops and the row is a day old. Runs once, so a later edit in ShipLine's
// template editor is kept.

const { ARTWORK_CONFIRMATION_TEMPLATE: t } = require('../../lib/email-template-defaults');

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        'UPDATE email_templates SET body_html = ? WHERE template_key = ? AND deleted_at IS NULL',
        [t.bodyHtml, t.key]
    );
    log(r.affectedRows ? `${t.key} body reworded` : `${t.key} not found; nothing to reword`);
};
