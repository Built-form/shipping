-- JFPRO's sample lots, as shipping reads them (2026-11-25, docs/sample-orders.md).
--
-- A sample order's line carries the sample's own code as its lot. That code is
-- issued and owned by JFPRO: a row of jfpro.lots with type = 'sample', linked
-- to its product through jfpro.lot_products. This view is the one place
-- shipping asks "is this lot a sample of that product?", the way jfa.suppliers
-- already reads jfpro.suppliers (same DEFINER pattern, so the cross-schema read
-- always runs with admin rights).
--
--   · a sample line may only take a lot found here for its JF code;
--   · a stock line may NOT take a lot found here (it would walk the sample into
--     Mintsoft through the ordinary receive).
--
-- One row per sample lot and linked product. A sample with no product link has
-- no row, so it cannot be ordered: link it in JFPRO first.
--
-- NEEDS JFPRO's "samples are lots" schema on this server (jfpro.lots.type and
-- jfpro.lot_products). Where that is missing this statement fails, on purpose:
-- sample orders cannot work there yet.
--
-- Collations (src/lib/view-collations.js): every column selected is a real
-- column of a jfpro table, and the one literal ('sample') is compared with a
-- real column, which wins over a literal in any session. After applying:
--   node tools/migrate.js --stage test --check-views

CREATE OR REPLACE DEFINER = 'admin'@'%' SQL SECURITY DEFINER VIEW sample_lots_v AS
SELECT
  l.id          AS lot_id,
  l.lot_number  AS lot_number,
  p.id          AS product_id,
  p.jfcode      AS jf_code
FROM jfpro.lots l
INNER JOIN jfpro.lot_products lp ON lp.lot_id = l.id
INNER JOIN jfpro.products p ON p.id = lp.product_id
WHERE l.type = 'sample';
