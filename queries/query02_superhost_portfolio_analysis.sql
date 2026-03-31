BEGIN TRANSACTION;
-- Superhost Portfolio Analysis
SELECT
    h.host_name,
    h.host_total_listings_count,
    COUNT(DISTINCT lst.neighbourhood_id) AS neighbourhoods_covered,
    COUNT(r.review_id) AS total_reviews_received,
    MAX(r."date") AS last_review_date
FROM hosts h
JOIN listings lst ON h.host_id = lst.host_id
LEFT JOIN reviews r ON lst.listing_id = r.listing_id
WHERE h.host_is_superhost = 1
  AND h.host_response_time IN ('within an hour', 'within a few hours')
  AND lst.instant_bookable = 1
  AND h.host_id IN (
      SELECT host_id
      FROM listings
      WHERE property_type NOT LIKE '%Shared%'
      GROUP BY host_id
      HAVING COUNT(DISTINCT neighbourhood_id) >= 2
         AND COUNT(listing_id) >= 3
  )
GROUP BY h.host_id, h.host_name, h.host_total_listings_count
HAVING COUNT(r.review_id) > 50
ORDER BY total_reviews_received DESC;

ROLLBACK;