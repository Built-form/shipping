'use strict';

// Fill freight_bookings.contents_document_ids (2026-11-17_10) for the contents
// emails sent before the column existed, so the forwarder's page lists their
// documents too.
//
// Such an email left no id on the booking, but each document it attached got a
// draft_container_document_sends row (delivery 'attached') in the same call,
// and was filed under the booking's container. So a booking's documents are
// the live forwarder quotes of its container with an attached send within two
// minutes of contents_sent_at. A resend generated a new set and moved
// contents_sent_at, so only the last email's documents match. The two clocks
// are compared as seconds since 1970 (sent_at is a TIMESTAMP, contents_sent_at
// a UTC DATETIME), so the session's time zone does not matter.
//
// Only bookings whose column is still NULL are touched, so a re-run, or a run
// after the new code has sent an email, changes nothing it should not.

exports.up = async (conn, { log }) => {
    const [rows] = await conn.query(
        `SELECT b.id AS booking_id, d.id AS document_id
           FROM freight_bookings b
           JOIN draft_container_documents d
             ON d.shipment_id = b.shipment_id AND d.type = 'forwarder-quote' AND d.deleted_at IS NULL
          WHERE b.contents_sent_at IS NOT NULL
            AND b.contents_document_ids IS NULL
            AND b.shipment_id IS NOT NULL
            AND EXISTS (SELECT 1 FROM draft_container_document_sends s
                         WHERE s.draft_container_document_id = d.id
                           AND s.delivery = 'attached'
                           AND ABS(UNIX_TIMESTAMP(s.sent_at)
                                   - TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', b.contents_sent_at)) <= 120)
          ORDER BY b.id ASC, (d.supplier IS NULL) DESC, d.supplier ASC, d.id ASC`
    );
    const byBooking = new Map();
    for (const r of rows) {
        const id = Number(r.booking_id);
        if (!byBooking.has(id)) byBooking.set(id, []);
        byBooking.get(id).push(Number(r.document_id));
    }
    for (const [bookingId, documentIds] of byBooking) {
        // No row_version bump: nothing a page could have been shown changes.
        await conn.query(
            'UPDATE freight_bookings SET contents_document_ids = ?, updated_at = updated_at WHERE id = ? AND contents_document_ids IS NULL',
            [JSON.stringify(documentIds), bookingId]
        );
    }
    log(`contents_document_ids filled on ${byBooking.size} booking(s)`);
};
