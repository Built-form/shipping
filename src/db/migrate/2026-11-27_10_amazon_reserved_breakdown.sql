-- amazon_stock_country_snapshots: Amazon's reserved sub-categories, stored
-- beside the existing `reserved` total (which is unchanged, as is everything
-- that reads it).
--
--   reserved_customer_order   units held for customer orders not yet shipped
--   reserved_fc_transfer      units moving between fulfilment centres
--   reserved_fc_processing    units held at an FC for processing (excl. staging)
--   reserved_staging          units staged at an FC ahead of distribution
--
-- NULL = not captured: every row written before this shipped, which only ever
-- had the total. Filled by writeSnapshots (src/services/amazon-stock-shared.js)
-- and served by GET /api/v1/stock-snapshots/reserved.
--
-- One clause per ALTER; tools/migrate.js tolerates "column exists" (1060), so
-- this is safe to re-run.

ALTER TABLE amazon_stock_country_snapshots ADD COLUMN reserved_customer_order INT NULL AFTER reserved;
ALTER TABLE amazon_stock_country_snapshots ADD COLUMN reserved_fc_transfer INT NULL AFTER reserved_customer_order;
ALTER TABLE amazon_stock_country_snapshots ADD COLUMN reserved_fc_processing INT NULL AFTER reserved_fc_transfer;
ALTER TABLE amazon_stock_country_snapshots ADD COLUMN reserved_staging INT NULL AFTER reserved_fc_processing;
