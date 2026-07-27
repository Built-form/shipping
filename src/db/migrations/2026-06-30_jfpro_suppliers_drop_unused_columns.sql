-- Remediation: drop four shipping-added columns from jfpro.suppliers that turned
-- out to be unused — contact_phone, address, notes, active. They were created by
-- 2026-06-30_jfpro_suppliers_add_columns.sql (now edited to no longer add them)
-- and populated by the merge, but nothing reads them: the GET /api/v1/suppliers
-- endpoint and the jfa.suppliers view no longer reference them.
--
-- Order matters: recreate the jfa.suppliers VIEW *without* these columns FIRST,
-- so it never points at a column that is about to disappear, THEN drop them.
--
-- RUN ONCE, as admin, ONLY on an environment where the ORIGINAL add-columns
-- migration (which still included these four) was applied — i.e. the current
-- live DB. On a fresh install the current add-columns migration never creates
-- them, so SKIP this file there (the DROP COLUMN would error that the column
-- does not exist).
--   node tools/sql.js -f src/db/migrations/2026-06-30_jfpro_suppliers_drop_unused_columns.sql
-- REQUIRES JFPro-team sign-off (it mutates a JFPro-owned table), same as the add.
--
-- Rollback: re-run 2026-06-30_jfpro_suppliers_add_columns.sql's ADD COLUMN lines
-- for the four columns (data is not recoverable — back up first if it matters).

CREATE OR REPLACE DEFINER = 'admin'@'%' SQL SECURITY DEFINER VIEW jfa.suppliers AS
SELECT
  s.id                                                       AS id,
  s.name                                                     AS name,
  s.code                                                     AS code,
  s.portal_code                                              AS portal_code,
  s.country_code                                             AS country,
  s.default_currency                                         AS default_currency,
  s.shippingTerms                                            AS default_incoterms,
  s.paymentTerms                                             AS default_payment_terms,
  s.contactPerson                                            AS contact_name,
  s.email                                                    AS contact_email,
  s.created_at                                               AS created_at,
  s.updated_at                                               AS updated_at,
  CASE WHEN s.is_deleted = 1 THEN s.updated_at ELSE NULL END AS deleted_at,
  -- camelCase originals preserved for safety / future use
  s.contactPerson                                            AS contactPerson,
  s.email                                                    AS email,
  s.wechat                                                   AS wechat,
  s.countryOfOrigin                                          AS countryOfOrigin,
  s.port                                                     AS port,
  s.paymentTerms                                             AS paymentTerms,
  s.shippingTerms                                            AS shippingTerms,
  s.is_deleted                                               AS is_deleted,
  s.customFields                                             AS customFields
FROM jfpro.suppliers s;

ALTER TABLE jfpro.suppliers
  DROP COLUMN contact_phone,
  DROP COLUMN address,
  DROP COLUMN notes,
  DROP COLUMN active;
