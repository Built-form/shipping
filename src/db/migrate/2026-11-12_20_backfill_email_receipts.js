'use strict';

// The emails ShipLine sent before email_receipts logged every email
// (2026-11-12_10): the artwork confirmation requests, the "a file is waiting"
// notices and the PI decision notices. None of them carried a "Confirm receipt"
// button, so none had a receipt row and none showed on the Email Receipts page.
//
// Artwork requests come from their own table, with the subject. The other two
// are read from the audit log (action 'notified'), which kept who the email
// went to but not its subject, so their subject stays NULL.
//
// confirm_via is 'portal' or 'none' and action_url NULL on every row written
// here, so the reminder sweep never chases one of them. The token is random and
// was never emailed. Each statement skips a send that already has a row, and
// the ledger runs this file once.

const INSERT = `INSERT INTO email_receipts
        (token, email_type, send_table, send_id, sent_to, subject, confirm_via, created_at)`;

exports.up = async (conn, { log }) => {
    const [artwork] = await conn.query(
        `${INSERT}
         SELECT LEFT(SHA2(CONCAT(UUID(), '-artwork-', q.id), 256), 48),
                'artwork_confirmation', 'artwork_confirmation_requests', q.id,
                CAST(q.sent_to AS CHAR), q.subject, 'portal', q.created_at
           FROM artwork_confirmation_requests q
           LEFT JOIN email_receipts r
                  ON r.send_table = 'artwork_confirmation_requests' AND r.send_id = q.id
          WHERE COALESCE(q.revoke_reason, '') <> 'send_failed'
            AND r.id IS NULL`
    );

    const [files] = await conn.query(
        `${INSERT}
         SELECT LEFT(SHA2(CONCAT(UUID(), '-audit-', a.id), 256), 48),
                'file_transfer', 'file_transfers', a.entity_id,
                JSON_UNQUOTE(JSON_EXTRACT(a.after_json, '$.to')), NULL, 'portal', a.created_at
           FROM audit_log a
           LEFT JOIN email_receipts r
                  ON r.send_table = 'file_transfers' AND r.send_id = a.entity_id
          WHERE a.entity_type = 'file_transfer' AND a.action = 'notified'
            AND r.id IS NULL`
    );

    const [decisions] = await conn.query(
        `${INSERT}
         SELECT LEFT(SHA2(CONCAT(UUID(), '-audit-', a.id), 256), 48),
                'pi_decision', 'pi_submissions', a.entity_id,
                JSON_UNQUOTE(JSON_EXTRACT(a.after_json, '$.to')), NULL, 'none', a.created_at
           FROM audit_log a
           LEFT JOIN email_receipts r
                  ON r.send_table = 'pi_submissions' AND r.send_id = a.entity_id
          WHERE a.entity_type = 'pi_submission' AND a.action = 'notified'
            AND r.id IS NULL`
    );

    log(`email receipts backfilled: ${artwork.affectedRows} artwork request(s), ${files.affectedRows} file notice(s), ${decisions.affectedRows} PI decision(s)`);
};
