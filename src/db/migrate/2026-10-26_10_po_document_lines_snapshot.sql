-- PO PDF line snapshots (src/lib/po-line-snapshot.js).
--
-- lines_json: the lines a generated PO PDF was printed from,
-- { v: 1, source: 'generate' | 'audit-rebuild', lines: [{ jfCode, asin, quantity, unitPrice }] }.
-- Written by POST /api/v1/purchase-orders/:id/generate; read back by the
-- documents endpoint and the GET /orders purchaseOrders bundle (newest
-- version only) so ShipLine can flag PO lines whose qty, price or item
-- changed since. NULL for every document generated before this column, until
-- tools/backfill-po-line-snapshots.js rebuilds the newest one per PO from
-- audit_log (source 'audit-rebuild'). NULL means "no baseline": no flags.
-- Instant column add; the reads use SELECT * and the INSERT falls back, so
-- the API keeps working whichever lands first.

ALTER TABLE purchase_order_documents ADD COLUMN lines_json JSON NULL;
