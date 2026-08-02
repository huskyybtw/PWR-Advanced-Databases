-- ===================================================================
-- SCENARIO 2: MEMCOMPRESS FOR CAPACITY HIGH (MAX COMPRESSION)
-- ===================================================================

-- ===================================================================
-- FORCE FOREGROUND IN-MEMORY POPULATION (JUMPSTART THE PYTHON LOOP)
-- ===================================================================
SELECT /*+ FULL(l) */ COUNT(*)
FROM listings l;

SELECT /*+ FULL(c) */ COUNT(*)
FROM calendar c;

SELECT /*+ FULL(r) */ COUNT(*)
FROM reviews r;

SELECT /*+ FULL(s) */ COUNT(*)
FROM listing_scrape_snapshot s;

ALTER TABLE listings INMEMORY MEMCOMPRESS FOR CAPACITY HIGH PRIORITY CRITICAL;

ALTER TABLE calendar INMEMORY MEMCOMPRESS FOR CAPACITY HIGH PRIORITY CRITICAL;

ALTER TABLE reviews INMEMORY MEMCOMPRESS FOR CAPACITY HIGH PRIORITY CRITICAL;

ALTER TABLE listing_scrape_snapshot INMEMORY MEMCOMPRESS FOR CAPACITY HIGH PRIORITY CRITICAL;

ALTER TABLE reviews NO INMEMORY (comments);

ALTER TABLE calendar
ADD (
    availability_flag NUMBER AS (
        CASE
            WHEN available = 1 THEN 1
            ELSE 0
        END
    )
);

ALTER TABLE calendar INMEMORY (availability_flag);

ALTER TABLE listing_scrape_snapshot
ADD (
    proj_monthly_profit NUMBER AS ((price * 0.15) * 30)
);

ALTER TABLE listing_scrape_snapshot INMEMORY (proj_monthly_profit);

CREATE INMEMORY
JOIN GROUP jg_listing_id (
    listings (listing_id),
    calendar (listing_id),
    reviews (listing_id)
);