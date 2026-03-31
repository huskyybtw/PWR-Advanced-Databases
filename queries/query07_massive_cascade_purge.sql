BEGIN TRANSACTION;
-- Massive Cascade-Style Purge
DELETE FROM calendar c
WHERE c."date" < CURRENT_DATE - 365
  AND c.listing_id IN (
      SELECT l.listing_id
      FROM listings l
      JOIN hosts h ON l.host_id = h.host_id
      LEFT JOIN reviews r ON l.listing_id = r.listing_id
        AND r."date" >= CURRENT_DATE - 365
      LEFT JOIN calendar c_recent ON l.listing_id = c_recent.listing_id
        AND c_recent."date" BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
        AND c_recent.available = 0
      LEFT JOIN listing_scrape_snapshot snap ON l.listing_id = snap.listing_id
        AND snap.scraped_at >= CURRENT_DATE - 365
      WHERE h.host_is_superhost = 0
      GROUP BY l.listing_id
      HAVING COUNT(r.review_id) = 0
         AND COUNT(c_recent.calendar_id) = 0
         AND MAX(snap.price) = MIN(snap.price)
  );

ROLLBACK;