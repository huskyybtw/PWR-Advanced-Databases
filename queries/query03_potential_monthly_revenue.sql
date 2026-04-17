BEGIN TRANSACTION;
-- Potential Monthly Revenue
WITH
    host_listing_stats AS (
        SELECT
            l.host_id,
            COUNT(DISTINCT l.listing_id) AS listings_cnt,
            COUNT(DISTINCT l.neighbourhood_id) AS neighbourhoods_cnt
        FROM listings l
        GROUP BY
            l.host_id
    ),
    host_review_stats AS (
        SELECT l.host_id, COUNT(r.review_id) AS total_reviews
        FROM listings l
            JOIN reviews r ON r.listing_id = l.listing_id
        GROUP BY
            l.host_id
    ),
    host_price_stats AS (
        SELECT l.host_id, AVG(c.price) AS avg_price
        FROM listings l
            JOIN calendar c ON c.listing_id = l.listing_id
        WHERE
            c."date" >= DATE '2025-12-01'
            AND c."date" < DATE '2026-01-01'
        GROUP BY
            l.host_id
    )
SELECT
    h.host_id,
    h.host_name,
    h.host_since,
    h.host_is_superhost,
    hls.listings_cnt,
    hls.neighbourhoods_cnt,
    hrs.total_reviews,
    hps.avg_price,
    DENSE_RANK() OVER (
        ORDER BY hrs.total_reviews DESC, hps.avg_price DESC
    ) AS host_rank
FROM
    hosts h
    JOIN host_listing_stats hls ON hls.host_id = h.host_id
    JOIN host_review_stats hrs ON hrs.host_id = h.host_id
    JOIN host_price_stats hps ON hps.host_id = h.host_id
WHERE
    hls.listings_cnt >= 3
    AND hls.neighbourhoods_cnt >= 2
    AND hrs.total_reviews > (
        SELECT AVG(total_reviews)
        FROM host_review_stats
    )
ORDER BY host_rank;

ROLLBACK;