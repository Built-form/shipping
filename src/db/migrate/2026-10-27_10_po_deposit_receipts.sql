-- The supplier's "deposit payment received" tick, one row per purchase order
-- (docs/po-deposit-receipts.md).
--
-- Written by the supplier portal (jportal, POST /api/supplier/purchase-orders/
-- {id}/deposit-received) under its own login, into THIS schema: the portal
-- owns every column, and its grants on this table live in jportal's
-- db/grants/jportal_app.sql. Read here by GET /purchase-orders and
-- GET /purchase-orders/:id/payments (orders.js, ShipLine's RECEIVED tick and
-- the Payments section) and by jportal-deposit-received-<env>
-- (deposit-received.js, the move into production).
--
-- It replaces jportal.po_deposit_receipts, which only the portal's own server
-- could see: production has no jportal schema at all, so the cross-schema
-- read came back "Unknown database" there. Keeping the row beside the PO
-- means every reader is on jfa whatever server the portal runs on.
-- 2026-10-27_20_copy_po_deposit_receipts.js brings the jportal rows over
-- where that schema exists.
--
-- received_on is the UTC date the supplier ticked the box; unticking sets it
-- back to NULL and the row stays, so who last touched it survives.
-- supplier_id and user_id are the portal's ids (jportal.suppliers,
-- jportal.supplier_users), snapshots for its audit trail; nothing here joins
-- on them. No foreign keys, no ENUMs, UTC timestamps, as the other tables.

CREATE TABLE IF NOT EXISTS po_deposit_receipts (
    id                 INT          NOT NULL AUTO_INCREMENT,
    purchase_order_id  INT          NOT NULL,             -- purchase_orders.id
    supplier_id        INT          NOT NULL,             -- jportal.suppliers.id when last touched
    user_id            INT          NOT NULL,             -- jportal.supplier_users.id of the last change
    confirmed_by_email VARCHAR(255) NOT NULL,             -- that user's email, a snapshot
    received_on        DATE         NULL,                 -- UTC date of the tick; NULL = cleared
    created_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_po_deposit_receipts_po (purchase_order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
