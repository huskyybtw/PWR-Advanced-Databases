BEGIN TRANSACTION;

INSERT INTO
    listing_scrape_snapshot (
        listing_id,
        scrape_id,
        scraped_at,
        price,
        minimum_nights,
        number_of_reviews,
        last_review,
        reviews_per_month,
        availability_365,
        number_of_reviews_ltm
    )
WITH
    cal AS (
        SELECT
            c.listing_id,
            AVG(c.price) AS avg_price,
            AVG(c.minimum_nights) AS avg_min_nights,
            SUM(
                CASE
                    WHEN c.available = 1 THEN 1
                    ELSE 0
                END
            ) AS free_days
        FROM calendar c
        WHERE
            c."date" >= DATE '2025-12-01'
            AND c."date" < DATE '2026-01-01'
        GROUP BY
            c.listing_id
    ),
    rev AS (
        SELECT
            r.listing_id,
            COUNT(*) AS reviews_cnt,
            MAX(r."date") AS last_review,
            COUNT(
                CASE
                    WHEN r."date" >= ADD_MONTHS(DATE '2025-12-31', -12) THEN 1
                END
            ) AS reviews_ltm
        FROM reviews r
        GROUP BY
            r.listing_id
    ),
    ranked AS (
        SELECT
            l.listing_id,
            n.location_id,
            cal.avg_price,
            cal.avg_min_nights,
            NVL(rev.reviews_cnt, 0) AS reviews_cnt,
            rev.last_review,
            NVL(rev.reviews_ltm, 0) AS reviews_ltm,
            cal.free_days,
            DENSE_RANK() OVER (
                PARTITION BY
                    n.location_id
                ORDER BY NVL(rev.reviews_cnt, 0) DESC, cal.avg_price DESC
            ) AS rnk
        FROM
            listings l
            JOIN neighbourhoods n ON n.neighbourhood_id = l.neighbourhood_id
            JOIN cal ON cal.listing_id = l.listing_id
            LEFT JOIN rev ON rev.listing_id = l.listing_id
    )
SELECT
    listing_id,
    900000 + ROW_NUMBER() OVER (
        ORDER BY location_id, listing_id
    ) AS scrape_id,
    SYSDATE,
    avg_price,
    avg_min_nights,
    reviews_cnt,
    last_review,
    ROUND(reviews_ltm / 12, 2),
    free_days,
    reviews_ltm
FROM ranked
WHERE
    rnk <= 20;

ROLLBACK;