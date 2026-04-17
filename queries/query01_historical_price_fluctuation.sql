BEGIN TRANSACTION;
-- Historical Price Fluctuation
WITH
    calendar_stats AS (
        SELECT
            c.listing_id,
            AVG(c.price) AS avg_price_dec,
            SUM(
                CASE
                    WHEN c.available = 1 THEN 1
                    ELSE 0
                END
            ) AS available_days_dec,
            COUNT(*) AS total_days_dec
        FROM calendar c
        WHERE
            c."date" >= DATE '2025-12-01'
            AND c."date" < DATE '2026-01-01'
        GROUP BY
            c.listing_id
    ),
    review_stats AS (
        SELECT
            r.listing_id,
            COUNT(*) AS review_count,
            MAX(r."date") AS last_review_date
        FROM reviews r
        WHERE
            r."date" >= ADD_MONTHS(DATE '2025-12-31', -12)
        GROUP BY
            r.listing_id
    ),
    city_price_avg AS (
        SELECT loc.location_id, AVG(cs.avg_price_dec) AS city_avg_price
        FROM
            calendar_stats cs
            JOIN listings l ON l.listing_id = cs.listing_id
            JOIN neighbourhoods n ON n.neighbourhood_id = l.neighbourhood_id
            JOIN locations loc ON loc.location_id = n.location_id
        GROUP BY
            loc.location_id
    )
SELECT loc.location_name, n.neighbourhood, l.listing_id, l.name, h.host_name, cs.avg_price_dec, cs.available_days_dec, rs.review_count, rs.last_review_date
FROM
    listings l
    JOIN hosts h ON h.host_id = l.host_id
    JOIN neighbourhoods n ON n.neighbourhood_id = l.neighbourhood_id
    JOIN locations loc ON loc.location_id = n.location_id
    JOIN calendar_stats cs ON cs.listing_id = l.listing_id
    LEFT JOIN review_stats rs ON rs.listing_id = l.listing_id
    JOIN city_price_avg cpa ON cpa.location_id = loc.location_id
WHERE
    cs.avg_price_dec > cpa.city_avg_price
    AND cs.available_days_dec >= 20
    AND NVL(rs.review_count, 0) <= 2
ORDER BY cs.avg_price_dec DESC, cs.available_days_dec DESC;

ROLLBACK;