BEGIN TRANSACTION;
-- Potential Monthly Revenue
SELECT
    l.location_name,
    n.neighbourhood,
    COUNT(DISTINCT lst.listing_id) AS total_listings,
    SUM(c.price) AS potential_monthly_revenue
FROM locations l
JOIN neighbourhoods n ON l.location_id = n.location_id
JOIN listings lst ON n.neighbourhood_id = lst.neighbourhood_id
JOIN calendar c ON lst.listing_id = c.listing_id
WHERE c.available = 1
  AND c."date" BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
  AND c.price BETWEEN 10 AND 5000
  AND lst.accommodates >= 2
GROUP BY
    l.location_id,
    l.location_name,
    n.neighbourhood_id,
    n.neighbourhood
HAVING AVG(c.price) > (
           SELECT AVG(c2.price)
           FROM listings lst2
           JOIN neighbourhoods n2 ON lst2.neighbourhood_id = n2.neighbourhood_id
           JOIN calendar c2 ON lst2.listing_id = c2.listing_id
           WHERE n2.location_id = l.location_id
             AND c2.available = 1
       )
   AND SUM(c.price) > 50000
ORDER BY potential_monthly_revenue DESC;

ROLLBACK;