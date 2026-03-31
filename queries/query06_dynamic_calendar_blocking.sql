BEGIN TRANSACTION;
-- Dynamic Calendar Blocking
UPDATE calendar c
SET c.available = 0
WHERE c."date" BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
  AND c.listing_id IN (
      SELECT lst.listing_id
      FROM listings lst
      JOIN listing_scrape_snapshot snap ON lst.listing_id = snap.listing_id
      WHERE snap.price < 20
        AND NOT EXISTS (
            SELECT 1
            FROM reviews r
            WHERE r.listing_id = lst.listing_id
              AND r."date" >= CURRENT_DATE - 365
        )
  );

ROLLBACK;