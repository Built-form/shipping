'use strict';

// Reword payment_proof_deposit_attached: a deposit email whose proof goes out
// attached asked the supplier to "reply to this email with the estimated ready
// date of each item". The ready dates are entered on the supplier portal,
// never by reply (the user, 2026-11-17, on the first one sent): the wording
// now says so, and the send appends a button to each order's page
// (docs/payment-proof-send.md).
//
// Only a row that still holds the wording this app seeded
// (2026-11-16_30_seed_payment_proof_templates.js) is touched: the body is
// replaced where it is exactly OLD_BODY. A template someone has edited is left
// as it is and named in the log, to be reworded by hand in ShipLine's template
// editor. A database seeded after this change already holds the new wording,
// and is left alone the same way. Runs once.

const { PAYMENT_PROOF_TEMPLATES } = require('../../lib/email-template-defaults');

const KEY = 'payment_proof_deposit_attached';
const OLD_BODY = [
    '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
    '<p>Hi,</p>',
    '<p>We have sent a <strong>deposit</strong> payment of <strong>{currency} {amount}</strong> on {paidOn}, for: {paidFor}.</p>',
    '<p>The proof of payment is attached. Please confirm you have received the payment, and reply to this email with the estimated ready date of each item.</p>',
    '<p>您好，</p>',
    '<p>我们已于 {paidOn} 支付<strong>定金</strong> <strong>{currency} {amount}</strong>，对应：{paidFor}。</p>',
    '<p>付款凭证见附件。请确认收款，并回复本邮件告知各产品的预计完成日期。</p>',
    '<p>Kind regards<br>JFA Medical LTD</p>',
    '</div>',
].join('');

exports.OLD_BODY = OLD_BODY;

exports.up = async (conn, { log }) => {
    const t = PAYMENT_PROOF_TEMPLATES.find(x => x.key === KEY);
    const [r] = await conn.query(
        `UPDATE email_templates
            SET body_html = ?, description = ?
          WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?`,
        [t.bodyHtml, t.description || null, KEY, OLD_BODY]
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
    else log(`${KEY} has been edited since it was seeded and was left as it is: check it asks for the ready dates on the portal, not by reply`);
};
