-- email_receipts becomes the log of EVERY email sent to a supplier, forwarder
-- or inspector, not only the ones that carry a "Confirm receipt" button
-- (src/lib/email-receipt.js; ShipLine's Email Receipts page reads it through
-- GET /api/v1/email-receipts).
--
-- confirm_via: how the email gets confirmed.
--   'email'   it carries the "Confirm receipt" button (the two-step flow). Every
--             row written before this column existed is one of these.
--   'portal'  it carries a link into the supplier portal, or a no-login magic
--             link, and NO confirm button: the recipient is asked to do one
--             thing, not two. What they do through the link (open the PO, tick
--             the artwork, open the file, answer the availability or the quote
--             request) is what sets confirmed_at (syncPortalReceipts).
--   'none'    a notice (a PI decision, a portal approval): nothing is asked.
-- action_url: for a 'portal' email that is chased with reminders (today the PO
--   email only), the address the reminder's button opens. NULL = never chased.
--   Never a secret link: a magic link's token stays in its own table.
--
-- tools/migrate.js tolerates "column exists" (1060), so this is safe to re-run.

ALTER TABLE email_receipts ADD COLUMN confirm_via VARCHAR(16) NOT NULL DEFAULT 'email' AFTER subject;

ALTER TABLE email_receipts ADD COLUMN action_url VARCHAR(1000) NULL AFTER confirm_via;
