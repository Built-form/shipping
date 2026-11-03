'use strict';

// QC visit schedule links (docs/qc-schedule-link.md), the email side:
//
// 1. Seed qc_visit_update ("products changed since our last email"),
//    INSERT IGNORE as 2026-10-28_31: an existing row is never overwritten.
// 2. The stored qc_visit_inspector body said "The inspection documents for
//    these products are attached", no longer true when the email carries the
//    schedule link instead. It is replaced by the new default ONLY when it is
//    still exactly the 2026-10-28 seed: a template ops have edited is left
//    alone (the sender appends the link paragraph to it anyway).

const { DEFAULT_EMAIL_TEMPLATES } = require('../../lib/email-template-defaults');

// qc_visit_inspector as seeded by 2026-10-28_31.
const OLD_INSPECTOR_BODY = [
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
].join('');

exports.up = async (conn, { log }) => {
    const byKey = new Map(DEFAULT_EMAIL_TEMPLATES.map(t => [t.key, t]));

    const u = byKey.get('qc_visit_update');
    const [ins] = await conn.query(
        `INSERT IGNORE INTO email_templates (template_key, name, category, description, subject, body_html)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [u.key, u.name, u.category || null, u.description || null, u.subject || null, u.bodyHtml]
    );
    log(ins.affectedRows ? 'qc_visit_update created' : 'qc_visit_update already existed');

    // Databases seeded before {availabilityLink} existed hold the body without it.
    const oldBodies = [OLD_INSPECTOR_BODY, OLD_INSPECTOR_BODY.replace('{availabilityLink}', '')];
    const i = byKey.get('qc_visit_inspector');
    const [upd] = await conn.query(
        `UPDATE email_templates SET body_html = ?, description = ?
          WHERE template_key = 'qc_visit_inspector' AND body_html IN (?)`,
        [i.bodyHtml, i.description || null, oldBodies]
    );
    log(upd.affectedRows
        ? 'qc_visit_inspector body updated (it was the unedited seed)'
        : 'qc_visit_inspector left as it is (edited by ops, or already updated)');
};
