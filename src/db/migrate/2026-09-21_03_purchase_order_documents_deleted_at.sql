-- DELETE /purchase-orders/:id soft-deletes a PO's documents with it. The
-- column was never created, so that route 500ed after it had already
-- soft-deleted the PO and its orders (was cold-start DDL in orders.js, and
-- src/db/migrations/2026-09-18_purchase_order_documents_deleted_at.sql).

ALTER TABLE purchase_order_documents ADD COLUMN deleted_at DATETIME NULL;
