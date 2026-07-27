-- Phase 3 of the supplier unification: cut shipping over to read JFPro live.
-- Replaces the jfa.suppliers and jfa.supplier_emails TABLES with VIEWs over
-- jfpro.suppliers / jfpro.supplier_contacts, re-exposing the exact column names
-- the shipping handlers already SELECT (so their queries are a drop-in — no
-- FROM repoint). Mirrors the DEFINER='admin'@'%' SQL SECURITY DEFINER pattern of
-- jfa.product_carton_sizes so the cross-DB read always runs with admin rights.
--
-- RUN ONCE, as admin, AFTER the merge (tools/merge-suppliers-into-jfpro.js --apply)
-- and ATOMICALLY WITH the Phase 4 code deploy (portal-code.js no longer ALTERs a
-- view, it writes portal_code straight to jfpro.suppliers):
--   node tools/sql.js -f src/db/migrations/2026-06-30_suppliers_views.sql
--
-- Rollback (each line is two separate statements):
--   DROP VIEW jfa.suppliers         then RENAME TABLE jfa.suppliers_legacy TO jfa.suppliers
--   DROP VIEW jfa.supplier_emails   then RENAME TABLE jfa.supplier_emails_legacy TO jfa.supplier_emails

RENAME TABLE jfa.suppliers TO jfa.suppliers_legacy;

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

RENAME TABLE jfa.supplier_emails TO jfa.supplier_emails_legacy;

CREATE OR REPLACE DEFINER = 'admin'@'%' SQL SECURITY DEFINER VIEW jfa.supplier_emails AS
SELECT
  c.id          AS id,
  c.supplier_id AS supplier_id,
  c.email       AS email,
  c.type        AS label,
  c.is_primary  AS is_primary,
  c.created_at  AS created_at,
  c.updated_at  AS updated_at,
  NULL          AS deleted_at
FROM jfpro.supplier_contacts c;
