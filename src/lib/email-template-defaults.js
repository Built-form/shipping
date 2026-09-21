'use strict';

// Built-in email templates: subject/body HTML used when emailing documents to
// suppliers/forwarders via Front. Stored in the email_templates table so ops
// can edit the copy without a deploy; migration
// 2026-09-21_11_seed_email_templates.js seeds any missing key (INSERT IGNORE,
// so an ops edit is never clobbered), and the send handlers in
// src/handlers/orders.js fall back to these if a row is missing. Subjects
// support {placeholder} tokens substituted per send (e.g. {poNumber}).

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
];

module.exports = { DEFAULT_EMAIL_TEMPLATES };
