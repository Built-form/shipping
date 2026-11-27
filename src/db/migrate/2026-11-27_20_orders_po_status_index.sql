-- A PO's lines by status, in one index (2026-11-27 query review). The supplier portal's PO
-- list counts each PO's live lines, its editable ones and its open samples
-- (portal src/handlers/purchaseOrders.js listPurchaseOrders). With only idx_purchase_order_id
-- and idx_status to choose from, MySQL took idx_status and read every ordered or shipped line
-- in the company for each PO, so the list grew with POs times open lines. This index answers
-- each count from the PO's own lines, and covers it (deleted_at is in it).
--
-- Additive. Online (InnoDB ADD KEY is in place, no lock on writes).

ALTER TABLE orders ADD KEY idx_orders_po_status (purchase_order_id, status, deleted_at);
