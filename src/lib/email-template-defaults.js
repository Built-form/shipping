'use strict';

// Built-in email templates: subject/body HTML used when emailing documents to
// suppliers/forwarders via Front. Stored in the email_templates table so ops
// can edit the copy without a deploy; migration
// 2026-09-21_11_seed_email_templates.js seeds any missing key (INSERT IGNORE,
// so an ops edit is never clobbered), and the send handlers in
// src/handlers/orders.js fall back to these if a row is missing. Subjects
// support {placeholder} tokens substituted per send (e.g. {poNumber}).
//
// The 'Supplier portal' templates are sent by the system, not from a send
// dialog: the decision email for a PI a supplier uploaded through the supplier
// portal (src/services/pi-submission-routes.js), looked up by key. Seeded by
// 2026-10-20_20_seed_supplier_pi_email_templates.js. Their body tokens are
// HTML-escaped by the sender: {poNumber} {filename} {note} {noteHtml}
// (the staff note as a paragraph, or nothing) {portalLinkHtml} (a link into
// the portal, or nothing).

const DEFAULT_EMAIL_TEMPLATES = [
    {
        key: 'purchase_order',
        name: 'Purchase Order',
        category: 'Purchasing',
        description: 'Emailed to a supplier with a PO PDF attached. Tokens: {poNumber}.',
        subject: '{poNumber}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please see attached PO. Please send back a PI. I will send artwork shortly.</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'draft_container_quote',
        name: 'Delivery Quote Request',
        category: 'Logistics',
        description: 'Emailed to a freight forwarder with a draft container PDF attached. Tokens: {draftContainerName}.',
        subject: 'Delivery Quote Request – {draftContainerName}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please see attached our delivery quote request. Could you please provide a quote for shipping the listed goods to our UK warehouse?</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'purchase_order_signed_pi',
        name: 'Signed Proforma Invoice',
        category: 'Purchasing',
        description: 'Emailed with a signed proforma invoice (PI_signed) attached. Tokens: {poNumber}.',
        subject: 'Signed PI – {poNumber}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please find attached the signed proforma invoice for your records.</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'quality_assurance',
        name: 'Quality Assurance',
        category: 'Quality',
        description: 'Emailed to a QC inspector/supplier with a quality assurance sheet attached. Tokens: {ref}.',
        subject: 'Quality Assurance – {ref}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Please see attached our quality assurance sheet. Could you please carry out QC inspection on the listed items per the QC Units indicated?</p>',
            '<p>Thank you.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'supplier_pi_imported',
        name: 'Supplier PI accepted',
        category: 'Supplier portal',
        description: 'Sent automatically to the supplier when a PI they uploaded in the supplier portal is accepted. Tokens: {poNumber} {filename} {note} {noteHtml} {portalLinkHtml}.',
        subject: 'PI accepted – {poNumber}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>Thank you. We have accepted your proforma invoice <strong>{filename}</strong> for {poNumber}.</p>',
            '<p>您好，</p>',
            '<p>谢谢。我们已接受您为 {poNumber} 上传的形式发票 <strong>{filename}</strong>。</p>',
            '{noteHtml}',
            '{portalLinkHtml}',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'supplier_pi_rejected',
        name: 'Supplier PI not accepted',
        category: 'Supplier portal',
        description: 'Sent automatically to the supplier when a PI they uploaded in the supplier portal is not accepted, with the reason and a link to upload a corrected one. Tokens: {poNumber} {filename} {note} {noteHtml} {portalLinkHtml}.',
        subject: 'PI not accepted – {poNumber}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Greetings,</p>',
            '<p>We could not accept your proforma invoice <strong>{filename}</strong> for {poNumber}. Please see the reason below, and upload a corrected PI in the supplier portal.</p>',
            '<p>您好，</p>',
            '<p>很抱歉，我们未能接受您为 {poNumber} 上传的形式发票 <strong>{filename}</strong>。请查看以下原因，并在供应商门户中上传更正后的形式发票。</p>',
            '{noteHtml}',
            '{portalLinkHtml}',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
];

// The templates the supplier-portal PI decision email uses (seeded by their own migration).
const SUPPLIER_PI_TEMPLATE_KEYS = ['supplier_pi_imported', 'supplier_pi_rejected'];

module.exports = { DEFAULT_EMAIL_TEMPLATES, SUPPLIER_PI_TEMPLATE_KEYS };
