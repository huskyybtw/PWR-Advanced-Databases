BEGIN TRANSACTION;
-- Massive Snapshot Generation
INSERT INTO listing_scrape_snapshot (
    listing_id,
    scrape_id,
    scraped_at,
    price,
    minimum_nights,
    number_of_reviews,
    last_review
)
SELECT
    l.listing_id,
    1001 AS scrape_id,
    CURRENT_DATE AS scraped_at,
    ROUND(AVG(c.price) * 1.05, 2) AS price,
    MIN(c.minimum_nights) AS minimum_nights,
    COUNT(r.review_id) AS number_of_reviews,
    MAX(r."date") AS last_review
FROM listings l
JOIN calendar c ON l.listing_id = c.listing_id
LEFT JOIN reviews r ON l.listing_id = r.listing_id
WHERE c."date" BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
  AND c.available = 1
  AND l.room_type IN ('Entire home/apt', 'Private room')
  AND l.instant_bookable = 1
GROUP BY l.listing_id
HAVING COUNT(r.review_id) >= 10
   AND AVG(c.price) > 50
   AND MIN(c.minimum_nights) <= 3;

ROLLBACK;