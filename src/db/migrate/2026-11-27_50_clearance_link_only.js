'use strict';

// Clearance instructions are sent to the customs forwarder as a link to the container's
// page on the supplier portal ONLY, never with the file attached (the user, 2026-11-27:
// "Don't send clearance document via email attachment, just a link to the portal";
// docs/clearance-instructions.md). The send no longer attaches the file; this rewords
// the stored template, which said "Please find attached".
//
// Only a row that still holds the wording 2026-11-24_23 seeded is touched: the body is
// replaced where it is exactly SEEDED's. A template someone has edited is left as it is
// and named in the log, to be checked by hand. Name and subject stay.
//
// Runs once. A database seeded after this change already holds the new wording.

const { FREIGHT_CLEARANCE_TEMPLATE: t } = require('../../lib/email-template-defaults');

const KEY = 'freight_clearance_instructions';
// What 2026-11-24_23 seeded, word for word.
const SEEDED = {
    bodyHtml: "<div style=\"font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;\"><p>Greetings,</p><p>Please find attached our clearance instructions for container <strong>{containerName}</strong>. The same file is on the container’s page on our supplier portal.</p><p>您好，</p><p>随函附上集装箱 <strong>{containerName}</strong> 的清关资料。该文件也可在供应商门户的集装箱页面下载。</p><p>Kind regards, / 此致敬礼<br>Operations Team.<br>JFA Medical Ltd.</p></div>",
};

exports.KEY = KEY;
exports.SEEDED = SEEDED;

exports.up = async (conn, { log }) => {
    const [r] = await conn.query(
        `UPDATE email_templates
            SET description = ?, body_html = ?
          WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?`,
        [t.description || null, t.bodyHtml, KEY, SEEDED.bodyHtml]
    );
    if (r.affectedRows) {
        log(`${KEY} reworded`);
        return;
    }
    const [rows] = await conn.query(
        'SELECT body_html FROM email_templates WHERE template_key = ? AND deleted_at IS NULL',
        [KEY]
    );
    if (!rows.length) log(`${KEY} not found; nothing to reword`);
    else if (rows[0].body_html === t.bodyHtml) log(`${KEY} already holds the new wording`);
    else log(`${KEY} has been edited since it was seeded and was left as it is: check it does not say the file is attached`);
};
