-- ===================================================================
-- UNIVERSAL TEARDOWN CLEANUP (CLEARS QUERY LOW OR CAPACITY HIGH)
-- ===================================================================

-- 1. Dissolve the Join Group dictionary tracking first
DROP INMEMORY JOIN GROUP "JG_LISTING_ID";

-- 2. Completely strip In-Memory storage segments to unlock columns
ALTER TABLE listings NO INMEMORY;

ALTER TABLE calendar NO INMEMORY;

ALTER TABLE reviews NO INMEMORY;

ALTER TABLE listing_scrape_snapshot NO INMEMORY;

-- 3. Drop the experimental generated virtual columns cleanly
ALTER TABLE calendar DROP COLUMN availability_flag;

ALTER TABLE listing_scrape_snapshot DROP COLUMN proj_monthly_profit;