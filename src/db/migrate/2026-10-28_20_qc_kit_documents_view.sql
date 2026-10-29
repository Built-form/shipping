-- QC inspection kits from JFPRO (docs/qc-visits.md): a product's kit is its
-- JFPRO documents with category 'qa' whose qa_type tag is 'poa_china' (the
-- Plan of Action stage "China Agent — factory QC"). A QA document with no
-- qa_type tag, or an empty one, counts too: JFPRO's UploadModal treats a
-- missing tag as 'poa_china'. Documents attached to the product directly
-- (document_attachments.entity_type 'product') and to any product group the
-- product belongs to ('product_group' via product_group_members) both count.
--
-- One row per (product, attachment path, document): the document's CURRENT
-- version only. A document reachable both directly and through a group (or
-- through two groups) appears once per path. The app de-duplicates
-- (src/lib/qc-kit-docs.js, 'product' wins). Read by GET /api/v1/qc-kits
-- (src/services/qc-kit-routes.js), the kit_missing warning
-- (src/services/qc-visit-routes.js) and the inspector email's kit attachments
-- (src/services/qc-visit-comms-routes.js).
--
-- jf_code is UPPER(TRIM(products.jfcode)): the app looks codes up normalised
-- (src/lib/qc-visits.js normJfCode). s3_key is a key in the JFPRO documents
-- bucket. The app uses its own JFPRO_DOCS_BUCKET (jfpro-documents-<stage>),
-- never document_versions.s3_bucket (the test database is a nightly copy of
-- production, the test bucket a mirror of the production one).
--
-- A migration, unlike the older hand-applied DEFINER views over jfpro
-- (src/db/migrations/README.md): no DEFINER clause and SQL SECURITY INVOKER,
-- so it is created by, and read with the privileges of, the app's own
-- database user, which already reads jfpro tables directly (jfpro.products,
-- jfpro.suppliers, jfpro.supplier_bank_accounts). Creating it needs SELECT on
-- the six jfpro tables below. If that user lacks it, this file fails and
-- deploy.sh stops before any code ships. Unqualified name: it lands in the
-- stage's own database. CREATE OR REPLACE makes a re-run safe.
--
-- Rollback (only once no deployed code reads it): DROP VIEW qc_kit_documents

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
  'product'              AS attached_via,
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
  'product_group'        AS attached_via,
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
