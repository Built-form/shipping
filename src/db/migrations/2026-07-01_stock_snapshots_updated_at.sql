-- Add a real "last refreshed" timestamp to stock_snapshots.
--
-- date_ran is day-granular (YYYY-MM-DD) and created_at is only stamped on the
-- first insert of a day's (date_ran, sku, warehouse) row, so neither can tell
-- us WHEN a given jf_code's cached Mintsoft number was last refreshed. The
-- stock-sum views need that to decide whether an order receipt is already
-- reflected in mintsoft_stock_level before they net it out (otherwise the total
-- reads low between the receive and the next snapshot).
--
-- ON UPDATE CURRENT_TIMESTAMP bumps it on every upsert — both the hourly
-- snapshot job and the live post-receive refresh — whenever the row's values
-- actually change (a receive always changes stock_level, so the next snapshot
-- for that code moves updated_at past the receipt's received_at). Existing rows
-- are stamped with now() on add, which is correct: the current snapshot already
-- reflects every receipt older than this migration.
ALTER TABLE stock_snapshots
    ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
