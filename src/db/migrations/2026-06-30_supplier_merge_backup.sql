-- Phase 0 of the supplier unification (JFPro becomes the single source of truth).
-- Snapshots EVERYTHING the merge touches before any write, and creates the
-- id-map that drives the rekey + rollback. Purely additive and idempotent
-- (CREATE TABLE IF NOT EXISTS ... SELECT) so it is safe to re-run.
--
-- Restore points created here:
--   jfa.suppliers_backup_20260630          full pre-merge shipping roster (incl. ids/portal_code)
--   jfa.supplier_emails_backup_20260630    full pre-merge shipping contacts
--   jfpro.suppliers_backup_20260630        full pre-merge JFPro roster (for the 9 merged rows)
--   jfpro.supplier_contacts_backup_20260630
--   jfpro.tags_backup_20260630 / jfpro.supplier_tags_backup_20260630 (the shipsline-legacy tag)
--   jfa.{po,orders,dcd}_supplier_name_backup_20260630   free-text supplier-name columns (canonicalization rollback)
--
-- Run as admin:  node tools/sql.js -f src/db/migrations/2026-06-30_supplier_merge_backup.sql

CREATE TABLE IF NOT EXISTS jfa.suppliers_backup_20260630 AS SELECT * FROM jfa.suppliers;

CREATE TABLE IF NOT EXISTS jfa.supplier_emails_backup_20260630 AS SELECT * FROM jfa.supplier_emails;

CREATE TABLE IF NOT EXISTS jfpro.suppliers_backup_20260630 AS SELECT * FROM jfpro.suppliers;

CREATE TABLE IF NOT EXISTS jfpro.supplier_contacts_backup_20260630 AS SELECT * FROM jfpro.supplier_contacts;

CREATE TABLE IF NOT EXISTS jfpro.tags_backup_20260630 AS SELECT * FROM jfpro.tags;

CREATE TABLE IF NOT EXISTS jfpro.supplier_tags_backup_20260630 AS SELECT * FROM jfpro.supplier_tags;

CREATE TABLE IF NOT EXISTS jfa.po_supplier_name_backup_20260630 AS SELECT id, supplier FROM jfa.purchase_orders;

CREATE TABLE IF NOT EXISTS jfa.orders_supplier_name_backup_20260630 AS SELECT id, supplier FROM jfa.orders;

CREATE TABLE IF NOT EXISTS jfa.dcd_supplier_name_backup_20260630 AS SELECT id, supplier FROM jfa.draft_container_documents;

-- Drives the rekey of jfa.supplier_emails -> jfpro ids (via the contacts copy),
-- name canonicalization, and rollback. One row per processed jfa supplier.
CREATE TABLE IF NOT EXISTS jfa.supplier_id_map (
    jfa_id     INT PRIMARY KEY,
    jfpro_id   INT NULL,
    action     VARCHAR(16) NOT NULL DEFAULT 'pending',   -- pending | inserted | merged
    name_jfa   VARCHAR(255) NULL,
    name_jfpro VARCHAR(255) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Belt-and-braces rollback marker: rows above this id created by the merge are
-- ours (the authoritative key is customFields.$.source = 'shipsline').
SELECT MAX(id) AS jfpro_max_supplier_id_before_merge FROM jfpro.suppliers;
