-- purchase_order_documents.deleted_at
--
-- DELETE /purchase-orders/:id soft-deletes the PO, its orders and its
-- documents. The documents column was never created, so the route answered
-- 500 after it had already soft-deleted the PO and its orders, and before its
-- audit rows. orders.js adds the column at cold start (purchaseOrdersSchemaReady),
-- and this file lets it be applied ahead of a deploy. Fails with Duplicate
-- column if already applied, which is harmless.

SET SESSION lock_wait_timeout = 5;

ALTER TABLE purchase_order_documents ADD COLUMN deleted_at DATETIME NULL;
