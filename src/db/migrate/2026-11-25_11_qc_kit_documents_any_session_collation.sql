-- qc_kit_documents of 2026-10-28_20, unchanged in what it answers, with one
-- column made safe under either session collation (src/lib/view-collations.js,
-- and 2026-11-25_10 for the failure that led here).
--
-- attached_via was a bare literal ('product' / 'product_group'). A bare
-- literal in a view keeps the collation of the session that created the view
-- (this runner: utf8mb4_unicode_ci). Compared with a literal in a query run
-- through the RDS Proxy (utf8mb4_0900_ai_ci) it fails with "Illegal mix of
-- collations". NOTHING DOES THAT TODAY: the app only selects attached_via and
-- compares it in JavaScript (src/lib/qc-kit-docs.js). This closes the trap
-- before a WHERE attached_via = '…' is ever written.
--
-- CONVERT(… USING utf8mb4) answers utf8mb4_0900_ai_ci in every session and
-- behaves like a real column. The values are the same text.
--
-- Nothing else differs from 2026-10-28_20: tools/test-view-collations-lib.js
-- rebuilds this statement from that file and compares them. Same SQL SECURITY
-- INVOKER, no DEFINER clause, same six jfpro tables.
--
-- Rollback: none needed.

CREATE OR REPLACE SQL SECURITY INVOKER VIEW qc_kit_documents AS
SELECT
  UPPER(TRIM(p.jfcode))  AS jf_code,
  p.id                   AS product_id,
  p.name                 AS product_name,
  d.id                   AS document_id,
  d.name                 AS document_name,
  v.id                   AS version_id,
  v.version_number       AS version_number,
  v.s3_key               AS s3_key,
  v.filename             AS filename,
  v.mime_type            AS mime_type,
  v.size_bytes           AS size_bytes,
  v.uploaded_by          AS uploaded_by,
  v.uploaded_at          AS uploaded_at,
  CONVERT('product' USING utf8mb4)       AS attached_via,
  d.updated_at           AS document_updated_at
FROM jfpro.documents d
JOIN jfpro.document_versions v      ON v.id = d.current_version_id AND v.document_id = d.id
JOIN jfpro.document_attachments da  ON da.document_id = d.id AND da.entity_type = 'product'
JOIN jfpro.products p               ON p.id = da.entity_id
LEFT JOIN jfpro.document_tags t     ON t.document_id = d.id AND t.tag_key = 'qa_type'
WHERE d.is_deleted = FALSE
  AND d.category = 'qa'
  AND (t.document_id IS NULL OR COALESCE(t.tag_value, '') IN ('', 'poa_china'))
  AND TRIM(COALESCE(p.jfcode, '')) <> ''

UNION ALL

SELECT
  UPPER(TRIM(p.jfcode))  AS jf_code,
  p.id                   AS product_id,
  p.name                 AS product_name,
  d.id                   AS document_id,
  d.name                 AS document_name,
  v.id                   AS version_id,
  v.version_number       AS version_number,
  v.s3_key               AS s3_key,
  v.filename             AS filename,
  v.mime_type            AS mime_type,
  v.size_bytes           AS size_bytes,
  v.uploaded_by          AS uploaded_by,
  v.uploaded_at          AS uploaded_at,
  CONVERT('product_group' USING utf8mb4) AS attached_via,
  d.updated_at           AS document_updated_at
FROM jfpro.documents d
JOIN jfpro.document_versions v       ON v.id = d.current_version_id AND v.document_id = d.id
JOIN jfpro.document_attachments da   ON da.document_id = d.id AND da.entity_type = 'product_group'
JOIN jfpro.product_group_members gm  ON gm.group_id = da.entity_id
JOIN jfpro.products p                ON p.id = gm.product_id
LEFT JOIN jfpro.document_tags t      ON t.document_id = d.id AND t.tag_key = 'qa_type'
WHERE d.is_deleted = FALSE
  AND d.category = 'qa'
  AND (t.document_id IS NULL OR COALESCE(t.tag_value, '') IN ('', 'poa_china'))
  AND TRIM(COALESCE(p.jfcode, '')) <> '';
