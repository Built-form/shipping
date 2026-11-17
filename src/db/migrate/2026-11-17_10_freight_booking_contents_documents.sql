-- Freight quotes (docs/freight-quotes.md): which documents the contents email
-- attached.
--
-- The contents email attaches a freshly generated forwarder quote: the combined
-- PDF and CSV plus one pair per supplier, each a draft_container_documents row.
-- Until now nothing on the booking said which rows, so the supplier portal
-- could not list them on the forwarder's page. `contents_document_ids` keeps
-- their ids, as a JSON array in the order they were attached (the combined
-- document first):
--   [698, 699]
-- NULL = no contents email yet, or one sent without documents. Written with
-- contents_sent_at, replaced by a resend, cleared when another container is
-- attached. 2026-11-17_11 grants the portal's login the column and
-- contents_sent_at; 2026-11-17_12 fills it for the emails already sent.
--
-- tools/migrate.js tolerates "column exists" (1060), so this is safe to re-run.

ALTER TABLE freight_bookings ADD COLUMN contents_document_ids JSON NULL AFTER contents_json;
