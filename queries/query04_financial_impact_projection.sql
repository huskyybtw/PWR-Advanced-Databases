BEGIN TRANSACTION;
-- Financial Impact Projection
SELECT
    n.neighbourhood,
    COUNT(l.listing_id) AS premium_properties_count,
    ROUND(AVG(snap.price), 2) AS current_avg_price,
    ROUND(AVG(snap.price * 1.15), 2) AS projected_avg_price,
    SUM(snap.price * 30) AS current_max_monthly_revenue,
    SUM((snap.price * 1.15) * 30) AS projected_max_monthly_revenue,
    SUM((snap.price * 0.15) * 30) AS neighbourhood_monthly_profit_gain
FROM listings l
JOIN neighbourhoods n ON l.neighbourhood_id = n.neighbourhood_id
JOIN listing_scrape_snapshot snap ON l.listing_id = snap.listing_id
WHERE l.room_type = 'Entire home/apt'
  AND l.listing_id IN (
      SELECT r.listing_id
      FROM reviews r
      WHERE r."date" >= CURRENT_DATE - 365
      GROUP BY r.listing_id
      HAVING COUNT(r.review_id) > 20
  )
GROUP BY n.neighbourhood
ORDER BY neighbourhood_monthly_profit_gain DESC;

ROLLBACK;