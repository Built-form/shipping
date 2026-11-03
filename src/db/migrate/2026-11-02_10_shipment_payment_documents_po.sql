-- The balance invoice for a PO's combined / repackaged goods (user,
-- 2026-11-02): those goods never ship, so their invoice is uploaded on their
-- balance in the Payments flow with no shipment, naming the PO instead.
-- POST /api/v1/shipment-payment-documents takes purchaseOrderId and files it
-- here; the reader checks it against that PO's REPACKAGED lines. A balance
-- invoice with no shipment and no PO stays a QC invoice ("Upload QC invoice").
--
-- Additive, nullable, no foreign key. One clause per ALTER, so a re-run after a
-- lock-wait timeout skips what landed and finishes the rest.

ALTER TABLE shipment_payment_documents ADD COLUMN purchase_order_id INT NULL;
ALTER TABLE shipment_payment_documents ADD KEY idx_purchase_order (purchase_order_id);
