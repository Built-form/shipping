-- Freeze the carton spec (weight + dimensions) onto each order row so historical
-- forwarder/supplier/QA quote PDFs stay reproducible even as jfpro.products
-- (via the product_carton_sizes view) later changes.
--
-- orders already snapshots units_per_carton + carton_cbm; this adds the missing
-- weight + three carton dimensions. At create time the app fills these from
-- product_carton_sizes by jf_code (see applyCartonSnapshot in handlers/orders.js);
-- the PDF loaders prefer the order-row values and fall back to the live view.
--
-- Run order: apply AFTER 2026-06-26_product_carton_sizes_view.sql (the backfill
-- reads that view).

ALTER TABLE jfa.orders ADD COLUMN carton_weight DECIMAL(10,3) NULL;
ALTER TABLE jfa.orders ADD COLUMN carton_height DECIMAL(10,3) NULL;
ALTER TABLE jfa.orders ADD COLUMN carton_width  DECIMAL(10,3) NULL;
ALTER TABLE jfa.orders ADD COLUMN carton_depth  DECIMAL(10,3) NULL;

-- One-time backfill: freeze current catalogue values onto live orders. Only
-- fills columns that are NULL (never clobbers an existing snapshot) and ignores
-- zero/empty view values (0 = missing -> left NULL so the loader falls back).
UPDATE jfa.orders o
  JOIN jfa.product_carton_sizes v ON v.jf_code = o.jf_code
   SET o.carton_weight    = COALESCE(o.carton_weight,    NULLIF(v.carton_weight, 0)),
       o.carton_height    = COALESCE(o.carton_height,    NULLIF(v.carton_height, 0)),
       o.carton_width     = COALESCE(o.carton_width,     NULLIF(v.carton_width, 0)),
       o.carton_depth     = COALESCE(o.carton_depth,     NULLIF(v.carton_depth, 0)),
       o.carton_cbm       = COALESCE(o.carton_cbm,       NULLIF(v.carton_cbm, 0)),
       o.units_per_carton = COALESCE(o.units_per_carton, NULLIF(v.carton_qty, 0))
 WHERE o.deleted_at IS NULL;
