-- Request to send line snapshots (src/lib/request-to-send-drift.js).
--
-- lines_json: the lines a Request to send PDF (type 'supplier-quote') was
-- printed from, { v: 1, source: 'generate' | 'csv-backfill', lines: [{ jfCode,
-- sku, lot, units, supplier }] }. Written by generateDraftDocument (orders.js)
-- for supplier-quote only; read by GET /shipments/:id/documents (a `drift` on
-- each supplier's latest) and GET /request-to-send/status (the container
-- cards) to flag a Request to send whose supplier's lines, units or lots
-- changed since. NULL = no baseline, never flagged: every document generated
-- before this column, until tools/backfill-request-to-send-snapshots.js fills
-- each supplier's latest from the CSV stored beside it (source 'csv-backfill').
-- Instant column add; the reads use SELECT * and the INSERT falls back, so the
-- API keeps working whichever lands first.

ALTER TABLE draft_container_documents ADD COLUMN lines_json JSON NULL;
