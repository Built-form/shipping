'use strict';

// A payment is sent to a supplier as a link to the supplier portal ONLY, never with the
// proof attached (the user, 2026-11-17: "dont want to send proof attached, only via
// portal"; docs/payment-proof-send.md). For the stored templates that means:
//
//   1. the two that stay (payment_proof_deposit_portal, payment_proof_balance_portal) are
//      reworded and renamed. They said where the proof is and what we ask for, which the
//      blocks appended under the body say again with their buttons; they now say what was
//      paid and point below. Only a row that still holds the wording this app seeded
//      (2026-11-16_30) is touched: the body is replaced where it is exactly SEEDED's. A
//      template someone has edited is left as it is and named in the log;
//   2. the two "proof attached" ones are retired (soft-deleted, as the template editor
//      deletes one): nothing sends them any more. Their rows stay in the table.
//
// Runs once. A database seeded after this change already holds the new wording.

const { PAYMENT_PROOF_TEMPLATES } = require('../../lib/email-template-defaults');

// What 2026-11-16_30 seeded for the two that stay, word for word.
const SEEDED = {
    payment_proof_deposit_portal: { name: "Deposit paid (supplier portal)", bodyHtml: "<div style=\"font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;\"><p>Hi,</p><p>We have sent a <strong>deposit</strong> payment of <strong>{currency} {amount}</strong> on {paidOn}, for: {paidFor}.</p><p>The proof of payment is on our supplier portal, with what the payment covers. It is not attached to this email. Please confirm you have received the payment, and enter the estimated ready date of each item on the order's page of the portal.</p><p>您好，</p><p>我们已于 {paidOn} 支付<strong>定金</strong> <strong>{currency} {amount}</strong>，对应：{paidFor}。</p><p>付款凭证及付款明细已发布在我们的供应商门户，本邮件不附带该文件。请确认收款，并在门户的订单页面填写各产品的预计完成日期。</p><p>Kind regards<br>JFA Medical LTD</p></div>" },
    payment_proof_balance_portal: { name: "Balance paid (supplier portal)", bodyHtml: "<div style=\"font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;\"><p>Hi,</p><p>We have sent a <strong>balance</strong> payment of <strong>{currency} {amount}</strong> on {paidOn}, for: {paidFor}.</p><p>The proof of payment is on our supplier portal, with what the payment covers. It is not attached to this email. Please arrange the telex release and send us the telex-released bill of lading.</p><p>您好，</p><p>我们已于 {paidOn} 支付<strong>尾款</strong> <strong>{currency} {amount}</strong>，对应：{paidFor}。</p><p>付款凭证及付款明细已发布在我们的供应商门户，本邮件不附带该文件。请安排电放，并将电放提单发给我们。</p><p>Kind regards<br>JFA Medical LTD</p></div>" },
};
const RETIRED = ['payment_proof_deposit_attached', 'payment_proof_balance_attached'];

exports.SEEDED = SEEDED;
exports.RETIRED = RETIRED;

exports.up = async (conn, { log }) => {
    for (const key of Object.keys(SEEDED)) {
        const t = PAYMENT_PROOF_TEMPLATES.find(x => x.key === key);
        const [r] = await conn.query(
            `UPDATE email_templates
                SET name = ?, description = ?, body_html = ?
              WHERE template_key = ? AND deleted_at IS NULL AND body_html = ?`,
            [t.name, t.description || null, t.bodyHtml, key, SEEDED[key].bodyHtml]
        );
        if (r.affectedRows) {
            log(`${key} reworded`);
            continue;
        }
        const [rows] = await conn.query(
            'SELECT body_html FROM email_templates WHERE template_key = ? AND deleted_at IS NULL',
            [key]
        );
        if (!rows.length) log(`${key} not found; nothing to reword`);
        else if (rows[0].body_html === t.bodyHtml) log(`${key} already holds the new wording`);
        else log(`${key} has been edited since it was seeded and was left as it is: check it does not say the proof is attached`);
    }
    const [gone] = await conn.query(
        `UPDATE email_templates
            SET deleted_at = NOW()
          WHERE template_key IN (?, ?) AND deleted_at IS NULL`,
        RETIRED
    );
    log(`${gone.affectedRows} "proof attached" template(s) retired`);
};
