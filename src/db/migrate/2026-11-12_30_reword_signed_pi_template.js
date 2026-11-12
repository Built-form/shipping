'use strict';

// Reword purchase_order_signed_pi: the signed-PI send no longer attaches the
// file. Its email now links to the order on the supplier portal, where the
// signed PI is downloaded (docs/po-portal-send.md, "Signed PIs"), so "Please
// find attached ..." would promise an attachment that is not there.
//
// Only a row that still holds the wording this app seeded is touched: the body
// is replaced where it is exactly OLD_BODY, and the subject where it is exactly
// OLD_SUBJECT. A template ops have edited is left as it is and named in the
// log, to be reworded by hand in ShipLine's template editor. Runs once.

const { DEFAULT_EMAIL_TEMPLATES, SIGNED_PI_TEMPLATE_KEY } = require('../../lib/email-template-defaults');

const OLD_SUBJECT = 'Signed PI – {poNumber}';
const OLD_BODY = [
    '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
    '<p>Greetings,</p>',
    '<p>Please find attached the signed proforma invoice for your records.</p>',
    '<p>Thank you.</p>',
    '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
    '</div>',
].join('');

exports.up = async (conn, { log }) => {
    const t = DEFAULT_EMAIL_TEMPLATES.find(x => x.key === SIGNED_PI_TEMPLATE_KEY);
    const [r] = await conn.query(
        `UPDATE email_templates
            SET subject = IF(subject = ?, ?, subject), body_html = ?, description = ?
          WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?`,
        [OLD_SUBJECT, t.subject, t.bodyHtml, t.description || null, t.key, OLD_BODY]
    );
    if (r.affectedRows) {
        log(`${t.key} reworded`);
        return;
    }
    const [rows] = await conn.query(
        'SELECT 1 FROM email_templates WHERE template_key = ? AND deleted_at IS NULL',
        [t.key]
    );
    log(rows.length
        ? `${t.key} has been edited since it was seeded and was left as it is: check it no longer says the file is attached`
        : `${t.key} not found; nothing to reword`);
};
