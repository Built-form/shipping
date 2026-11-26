-- The papers of a container that a forwarder working on it may download from
-- the supplier portal (docs/forwarder-containers.md, "A container's
-- documents"; the user, 2026-11-26): its bill of lading, the suppliers'
-- packing lists and their commercial invoices. One row per file our team HOLD:
-- accepted, imported or filed here. An upload still waiting for them is not in
-- it. The clearance instructions have their own view (2026-11-24_21).
--
--   bill_of_lading      a bill of lading accepted from a forwarder's upload
--                       (shipment_forwarder_documents). Never a customs draft
--                       or a customs entry: those are their sender's alone.
--   telex_release       a supplier's telex-released bill of lading, accepted
--                       from the portal or filed by our team
--   packing_list        a live packing list of the container, the newest
--                       version of each: filed on the container's number, or
--                       on the draft that was converted into it. (A draft that
--                       was never closed is not followed: packing-review.js
--                       does that in code, and it cannot be said in a view.)
--   commercial_invoice  a balance invoice on the shipment, filed under a
--                       supplier WITH GOODS ON IT. Not a forwarder's invoice
--                       (forwarder_id), not one filed by hand under a
--                       forwarder's name (no goods on board under that name),
--                       not a proof of payment, not a QC invoice (no shipment).
--
-- THIS VIEW HAS NO FORWARDER: it says what a container's papers are, not who
-- may have them. The portal gives a row only to a contact of a forwarder that
-- holds a duty on that container (its own join on
-- portal_forwarder_containers_v, link_source 'booking', 'container' or
-- 'duty'), and never to one that knows the container through a payment only.
-- The user chose that on 2026-11-26: every attached forwarder gets all of
-- them, the suppliers' names and our prices included.
--
-- The file is named by its key in our document bucket, never by an address:
-- the portal signs a short-lived download for it and hands out nothing that
-- lasts. Nothing here says who uploaded or accepted a file.
--
-- Every `kind` is wrapped in CONVERT(… USING utf8mb4), in every branch
-- (src/lib/view-collations.js): a caller compares it with a literal.
--
-- SQL SECURITY DEFINER and CREATE OR REPLACE, as 2026-11-16_21: the portal's
-- login reads the view and gains no right on the tables under it. No grant
-- until 2026-11-26_11.
--
-- Rollback, once no deployed portal reads it: DROP VIEW
-- portal_forwarder_documents_v. Nothing else refers to it.

CREATE OR REPLACE SQL SECURITY DEFINER VIEW portal_forwarder_documents_v AS
SELECT
  d.shipment_id                              AS shipment_id,
  CONVERT('bill_of_lading' USING utf8mb4)    AS kind,
  d.id                                       AS document_id,
  NULL                                       AS supplier_name,
  d.filename                                 AS filename,
  d.s3_key                                   AS s3_key,
  d.content_type                             AS content_type,
  d.file_size                                AS file_size,
  d.created_at                               AS filed_at
FROM shipment_forwarder_documents d
WHERE d.deleted_at IS NULL
  AND d.kind = 'bill_of_lading'

UNION ALL

SELECT
  t.shipment_id,
  CONVERT('telex_release' USING utf8mb4),
  t.id,
  t.supplier_name,
  t.filename,
  t.s3_key,
  t.content_type,
  t.file_size,
  t.created_at
FROM shipment_telex_releases t
WHERE t.deleted_at IS NULL

UNION ALL

SELECT
  s.id,
  CONVERT('packing_list' USING utf8mb4),
  pl.id,
  pl.supplier_name,
  pl.filename,
  pl.s3_key,
  pl.content_type,
  pl.file_size,
  pl.created_at
FROM shipments s
JOIN packing_lists pl ON pl.container_kind = 'booked' AND pl.container_number = s.reference
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND pl.deleted_at IS NULL AND pl.superseded_by_id IS NULL

UNION ALL

SELECT
  s.id,
  CONVERT('packing_list' USING utf8mb4),
  pl.id,
  pl.supplier_name,
  pl.filename,
  pl.s3_key,
  pl.content_type,
  pl.file_size,
  pl.created_at
FROM shipments s
JOIN draft_containers dc ON dc.closed_reason = 'converted' AND dc.container_number = s.reference
JOIN packing_lists pl ON pl.container_kind = 'draft' AND pl.draft_container_id = dc.id
WHERE s.deleted_at IS NULL AND s.merged_into_id IS NULL
  AND NULLIF(TRIM(s.reference), '') IS NOT NULL
  AND pl.deleted_at IS NULL AND pl.superseded_by_id IS NULL

UNION ALL

SELECT
  i.shipment_id,
  CONVERT('commercial_invoice' USING utf8mb4),
  i.id,
  i.supplier_name,
  i.filename,
  i.s3_key,
  i.content_type,
  i.file_size,
  i.created_at
FROM shipment_payment_documents i
WHERE i.deleted_at IS NULL
  AND i.doc_kind = 'balance_invoice'
  AND i.shipment_id IS NOT NULL
  AND i.forwarder_id IS NULL
  AND EXISTS (
        SELECT 1
          FROM orders o
         WHERE o.deleted_at IS NULL
           AND (o.shipment_id = i.shipment_id
                OR o.id IN (SELECT sl.order_id FROM shipment_lines sl WHERE sl.shipment_id = i.shipment_id))
           AND TRIM(o.supplier) = TRIM(i.supplier_name));
