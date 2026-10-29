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
//
// The 'QC Visit' templates are sent from a QC visit's Schedule tab
// (src/services/qc-visit-comms-routes.js, POST /api/v1/qc-visits/:id/email):
// qc_visit_factory proposes the date to the factory, qc_visit_inspector
// assigns the inspector, qc_visit_cancel tells either party the visit is off.
// Seeded by 2026-10-28_31_seed_qc_visit_templates.js. Body tokens are
// HTML-escaped by the sender ({lines} is already an HTML list): {ref}
// {supplier} {factory} {factoryAddress} {factoryContact} {visitDate} {window}
// {windowUk} {inspector} {inspectorCompany} {lines} {productionState}
// {reportDeadline} {escalation} {availabilityLink}. {availabilityLink} is a
// whole paragraph (the "tick the days you could attend" lead-in and the
// supplier-portal link, docs/qc-availability.md) when the proposal is sent
// with "Include availability link", and nothing otherwise; a template without
// the token still gets the paragraph appended, so stored templates seeded
// before it existed keep working.

const QC_VISIT_TOKENS = '{ref} {supplier} {factory} {factoryAddress} {factoryContact} {visitDate} {window} {windowUk} {inspector} {inspectorCompany} {lines} {productionState} {reportDeadline} {escalation} {availabilityLink}';

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
    {
        key: 'qc_visit_factory',
        name: 'QC visit – proposal to factory',
        category: 'QC Visit',
        description: `Emailed to the factory to propose a QC inspection date, with the calendar invite attached. Tokens: ${QC_VISIT_TOKENS}.`,
        subject: 'QC inspection {ref} – proposed for {visitDate}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Hello,</p>',
            '<p>We would like to arrange a quality inspection at your factory. The proposed details are below.</p>',
            '<p><strong>Reference:</strong> {ref}<br>',
            '<strong>Date:</strong> {visitDate}<br>',
            '<strong>Time:</strong> {window}<br>',
            '<strong>Factory:</strong> {factory}<br>',
            '<strong>Address:</strong> {factoryAddress}<br>',
            '<strong>Inspector:</strong> {inspector}</p>',
            '<p><strong>Products to be inspected:</strong></p>',
            '{lines}',
            '<p><strong>Production state expected on the day:</strong> {productionState}. Please have the goods ready and accessible, with someone available to open cartons and help the inspector.</p>',
            '{availabilityLink}',
            '<p>Please confirm this date by replying to this email, or in the supplier portal using your PO number and portal code. If the date does not suit you, reply with a date that does.</p>',
            '<p>您好，</p>',
            '<p>我们希望安排到贵厂进行验货（编号 {ref}），详情如上：日期 {visitDate}，时间 {window}。</p>',
            '<p>验货当天货物应达到的状态：{productionState}。请提前备好货物，并安排人员协助开箱。</p>',
            '<p>请回复此邮件确认日期，或使用 PO 号和门户代码在供应商门户中确认。如该日期不合适，请回复可行的日期。</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'qc_visit_inspector',
        name: 'QC visit – inspector assignment',
        category: 'QC Visit',
        description: `Emailed to the inspector with the calendar invite and the products' inspection kit documents attached: each product's current China factory QC documents in JFPRO (QA documents, stage "China Agent — factory QC"). Needs no manual fields. Tokens: ${QC_VISIT_TOKENS}.`,
        subject: 'QC inspection assignment {ref} – {factory}, {visitDate}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Hello {inspector},</p>',
            '<p>Please carry out the inspection below for us. The inspection documents for these products are attached: please follow them.</p>',
            '<p><strong>Reference:</strong> {ref}<br>',
            '<strong>Date:</strong> {visitDate}<br>',
            '<strong>Time:</strong> {window}<br>',
            '<strong>Supplier:</strong> {supplier}<br>',
            '<strong>Factory:</strong> {factory}<br>',
            '<strong>Address:</strong> {factoryAddress}<br>',
            '<strong>Factory contact:</strong> {factoryContact}</p>',
            '<p><strong>Products to inspect:</strong></p>',
            '{lines}',
            '<p><strong>Production state expected:</strong> {productionState}</p>',
            '<p><strong>Report:</strong> please send your full report, with photos, within {reportDeadline} of finishing the inspection.</p>',
            '<p><strong>Escalation:</strong> {escalation}</p>',
            '{availabilityLink}',
            '<p>Please reply to confirm you can attend on this date.</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
    {
        key: 'qc_visit_cancel',
        name: 'QC visit – cancellation',
        category: 'QC Visit',
        description: `Emailed to the factory or the inspector when a QC visit is cancelled, with a calendar cancellation attached. Tokens: ${QC_VISIT_TOKENS}.`,
        subject: 'Cancelled: QC inspection {ref} on {visitDate}',
        bodyHtml: [
            '<div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;color:#222;line-height:1.5;">',
            '<p>Hello,</p>',
            '<p>The quality inspection <strong>{ref}</strong> planned for {visitDate} ({window}) at {factory} has been cancelled. Please disregard our earlier email about this visit and remove it from your calendar.</p>',
            '<p>We will contact you again if a new date is needed. Sorry for any inconvenience.</p>',
            '<p>您好，</p>',
            '<p>原定于 {visitDate}（{window}）在 {factory} 进行的验货 <strong>{ref}</strong> 已取消。请忽略我们之前关于此次验货的邮件，并将其从日历中删除。</p>',
            '<p>如需重新安排，我们会再与您联系。给您带来不便，敬请谅解。</p>',
            '<p>Kind Regards,<br>Operations Team.<br>JFA Medical Ltd.</p>',
            '</div>',
        ].join(''),
    },
];

// The templates the supplier-portal PI decision email uses (seeded by their own migration).
const SUPPLIER_PI_TEMPLATE_KEYS = ['supplier_pi_imported', 'supplier_pi_rejected'];

// The QC visit emails (seeded by 2026-10-28_31_seed_qc_visit_templates.js).
const QC_VISIT_TEMPLATE_KEYS = ['qc_visit_factory', 'qc_visit_inspector', 'qc_visit_cancel'];

module.exports = { DEFAULT_EMAIL_TEMPLATES, SUPPLIER_PI_TEMPLATE_KEYS, QC_VISIT_TEMPLATE_KEYS };
