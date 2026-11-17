-- Freight quotes (docs/freight-quotes.md): reminders sent by staff.
--
-- POST /api/v1/freight-quotes/:id/remind emails a forwarder the same link
-- again: for a quote that has not come, or for stock movements that have no
-- price yet. Nothing sends one automatically.
--   reminded_at     when the last reminder went (NULL = none yet)
--   reminder_count  how many have gone. Also the route's retry guard: the call
--                   carries the count it last read and claims the next one
--                   under the row lock, so a retried call cannot send twice.
-- Written by shipping only; the portal's login is granted neither column and
-- reads neither.
--
-- tools/migrate.js tolerates "column exists" (1060), so this is safe to re-run.

ALTER TABLE freight_quote_requests
    ADD COLUMN reminded_at DATETIME NULL AFTER sent_at,
    ADD COLUMN reminder_count INT NOT NULL DEFAULT 0 AFTER reminded_at;
