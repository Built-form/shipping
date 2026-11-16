-- What the supplier portal may read of a payment (docs/payment-proof-send.md,
-- "What the portal reads"): three views, and nothing of the tables under them.
--
-- Why views and not column grants: a column grant on supplier_payments covers
-- every row of it, so the portal's login could read every supplier's amounts,
-- sent or not, and a grant on shipment_payment_documents (filename,
-- public_url) would hand it every invoice's file link. These views hold SENT
-- payments only, and of a payment only what a supplier is told. The portal's
-- login gets SELECT on the three views (2026-11-16_22) and no new right on a
-- payment table. Which supplier sees which row is still the portal's own SQL
-- (its supplierNameMatch on supplier_name / po_supplier / shipment_supplier /
-- extra_supplier).
--
-- Live: an edit to a payment shows at once, a deleted payment is gone. The
-- user asked for this in place of a copy of what the email said (2026-11-16).
--
-- SQL SECURITY DEFINER (unlike qc_kit_documents, an INVOKER view the app
-- itself reads): the reader is another login, which must NOT hold rights on
-- the tables underneath. The definer is whoever runs this migration, the same
-- account that owns the tables. CREATE OR REPLACE makes a re-run safe.
--
-- Names are compared with utf8mb4_0900_bin, as the portal compares them: case,
-- punctuation and trailing spaces all count.
--
-- Rollback (once no deployed portal reads them): DROP VIEW portal_payment_proofs_v,
-- portal_payment_lines_v, portal_payments_v

-- A live transfer that was sent to the supplier it now belongs to: at least
-- one send that went out ('sent') carries the transfer's current supplier_name.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_payments_v AS
SELECT
  sp.id             AS payment_id,
  sp.supplier_name  AS supplier_name,
  sp.amount         AS amount,
  sp.currency       AS currency,
  sp.paid_on        AS paid_on,
  (SELECT MAX(s.sent_at) FROM supplier_payment_sends s
    WHERE s.supplier_payment_id = sp.id AND s.status = 'sent'
      AND CONVERT(s.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
        = CONVERT(sp.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin) AS last_sent_at
FROM supplier_payments sp
WHERE sp.deleted_at IS NULL
  AND EXISTS (SELECT 1 FROM supplier_payment_sends s
               WHERE s.supplier_payment_id = sp.id AND s.status = 'sent'
                 AND CONVERT(s.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
                   = CONVERT(sp.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin);

-- The lines of those transfers, and what each points at. Every reference comes
-- with the supplier it is filed under, so the portal can test each one on its
-- own: a PO by the PO's own supplier (whatever line names it, an extra's
-- included), a container by the record that ties it to a supplier (the balance
-- record, or the extra). A target that is deleted or not there leaves its
-- columns NULL: the line is then told as its amount alone.
--   po_deposit            target_id -> purchase_orders
--   pi                    target_id -> purchase_order_invoice_payments -> its PO
--                         (the invoice must still be on file)
--   balance               target_id -> shipment_payments -> its shipment
--   qc, repackaged        target_id -> orders -> its PO
--   extra                 target_id -> payment_extras (-> its PO, -> its shipment)
-- NOT here: an extra's description or note, a balance record's own amount or
-- status, a PI's terms or bank details, unit prices.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_payment_lines_v AS
SELECT
  l.id                 AS line_id,
  l.payment_id         AS payment_id,
  l.target_kind        AS kind,
  l.amount             AS amount,
  l.paid_with_json     AS paid_with_json,
  pip.payment_type     AS payment_type,
  po.id                AS po_id,
  po.po_number         AS po_number,
  po.supplier          AS po_supplier,
  o.jf_code            AS jf_code,
  COALESCE(bal.shipment_id, ex.shipment_id)                                  AS shipment_id,
  COALESCE(sh.reference, bal.shipment_reference, ex.shipment_reference)      AS shipment_reference,
  CASE WHEN bal.id IS NOT NULL THEN bal.supplier_name
       WHEN ex.shipment_id IS NOT NULL THEN ex.supplier_name END             AS shipment_supplier,
  bal.invoice_number   AS invoice_number,
  (bal.source = 'top_up') AS top_up,
  ex.kind              AS extra_kind,
  ex.rides_with        AS extra_rides_with,
  ex.supplier_name     AS extra_supplier
FROM supplier_payment_lines l
JOIN portal_payments_v p ON p.payment_id = l.payment_id
LEFT JOIN purchase_order_invoice_payments pip
       ON l.target_kind = 'pi' AND pip.id = l.target_id
      AND EXISTS (SELECT 1 FROM purchase_order_invoices i
                   WHERE i.id = pip.purchase_order_invoice_id AND i.deleted_at IS NULL)
LEFT JOIN orders o
       ON l.target_kind IN ('qc', 'repackaged') AND o.id = l.target_id AND o.deleted_at IS NULL
LEFT JOIN shipment_payments bal
       ON l.target_kind = 'balance' AND bal.id = l.target_id AND bal.deleted_at IS NULL
LEFT JOIN payment_extras ex
       ON l.target_kind = 'extra' AND ex.id = l.target_id AND ex.deleted_at IS NULL
LEFT JOIN purchase_orders po
       ON po.deleted_at IS NULL
      AND po.id = CASE l.target_kind
                    WHEN 'po_deposit' THEN l.target_id
                    WHEN 'pi'         THEN pip.purchase_order_id
                    WHEN 'qc'         THEN o.purchase_order_id
                    WHEN 'repackaged' THEN o.purchase_order_id
                    WHEN 'extra'      THEN ex.purchase_order_id
                  END
LEFT JOIN shipments sh ON sh.id = COALESCE(bal.shipment_id, ex.shipment_id);

-- The proofs of those transfers: a live document linked to the transfer, a
-- remittance, filed under exactly the transfer's supplier name. The same rule
-- the send applies before an email goes (src/lib/payment-notices.js), so the
-- email never promises a proof this view does not hold. No invoice, and no
-- other supplier's file, is ever a row here.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_payment_proofs_v AS
SELECT
  d.id                   AS document_id,
  d.supplier_payment_id  AS payment_id,
  d.filename             AS filename,
  d.public_url           AS public_url
FROM shipment_payment_documents d
JOIN portal_payments_v p ON p.payment_id = d.supplier_payment_id
WHERE d.deleted_at IS NULL
  AND d.doc_kind = 'remittance'
  AND CONVERT(d.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin
    = CONVERT(p.supplier_name USING utf8mb4) COLLATE utf8mb4_0900_bin;
