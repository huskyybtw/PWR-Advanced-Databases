BEGIN TRANSACTION;
-- Historical Price Fluctuation
SELECT
    lst.room_type,
    COUNT(DISTINCT lst.listing_id) AS properties_count,
    AVG(snap.price) AS historical_avg_price,
    AVG(c.price) AS current_calendar_avg_price,
    (AVG(c.price) - AVG(snap.price)) AS price_difference
FROM listings lst
JOIN listing_scrape_snapshot snap ON lst.listing_id = snap.listing_id
JOIN calendar c ON lst.listing_id = c.listing_id
WHERE c."date" BETWEEN CURRENT_DATE AND CURRENT_DATE + 60
  AND lst.room_type IN ('Entire home/apt', 'Private room', 'Hotel room')
  AND lst.listing_id IN (
      SELECT listing_id
      FROM calendar
      WHERE "date" BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
        AND available = 1
        AND price > 0
      GROUP BY listing_id
      HAVING COUNT("date") = 30
  )
GROUP BY lst.room_type
HAVING AVG(snap.price) IS NOT NULL
   AND (AVG(c.price) - AVG(snap.price)) > 15
   AND COUNT(DISTINCT lst.listing_id) > 5
ORDER BY price_difference DESC;

ROLLBACK;