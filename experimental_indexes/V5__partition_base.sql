ALTER TABLE calendar RENAME TO calendar_base_v5;

CREATE TABLE calendar (
    calendar_id NUMBER(19) NOT NULL,
    listing_id NUMBER(19) NOT NULL,
    "date" DATE NOT NULL,
    available NUMBER(1) NOT NULL,
    price BINARY_DOUBLE,
    minimum_nights NUMBER(10) NOT NULL,
    maximum_nights NUMBER(10) NOT NULL,
    CONSTRAINT calendar_v5_pk PRIMARY KEY (calendar_id),
    CONSTRAINT calendar_v5_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
)
PARTITION BY RANGE ("date")
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION p_cal_min VALUES LESS THAN (DATE '2020-01-01')
);

INSERT INTO calendar (
    calendar_id,
    listing_id,
    "date",
    available,
    price,
    minimum_nights,
    maximum_nights
)
SELECT
    calendar_id,
    listing_id,
    "date",
    available,
    price,
    minimum_nights,
    maximum_nights
FROM calendar_base_v5;

CREATE INDEX idx_calendar_trunc_date_fbi_v5 ON calendar (TRUNC ("date"));
CREATE INDEX idx_calendar_covering_v5 ON calendar (listing_id, "date", available);

ALTER TABLE reviews RENAME TO reviews_base_v5;

CREATE TABLE reviews (
    review_id NUMBER(19) NOT NULL,
    listing_id NUMBER(19) NOT NULL,
    "date" DATE NOT NULL,
    reviewer_id NUMBER(19) NOT NULL,
    reviewer_name VARCHAR2(255) NOT NULL,
    comments CLOB,
    CONSTRAINT reviews_v5_pk PRIMARY KEY (review_id),
    CONSTRAINT reviews_v5_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
)
PARTITION BY RANGE ("date")
INTERVAL (NUMTOYMINTERVAL(1, 'YEAR'))
(
    PARTITION p_rev_min VALUES LESS THAN (DATE '2020-01-01')
);

INSERT INTO reviews (
    review_id,
    listing_id,
    "date",
    reviewer_id,
    reviewer_name,
    comments
)
SELECT
    review_id,
    listing_id,
    "date",
    reviewer_id,
    reviewer_name,
    comments
FROM reviews_base_v5;

CREATE INDEX idx_reviews_listing_date_v5 ON reviews (listing_id, "date");

ALTER TABLE listing_scrape_snapshot RENAME TO listing_scrape_snapshot_base_v5;

CREATE TABLE listing_scrape_snapshot (
    listing_id NUMBER(19) NOT NULL,
    scrape_id NUMBER(19) NOT NULL,
    scraped_at DATE NOT NULL,
    price BINARY_DOUBLE,
    minimum_nights NUMBER(10),
    number_of_reviews NUMBER(10),
    last_review DATE,
    reviews_per_month BINARY_DOUBLE,
    availability_365 NUMBER(10),
    number_of_reviews_ltm NUMBER(10),
    CONSTRAINT snapshot_v5_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
)
PARTITION BY RANGE (scraped_at)
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(
    PARTITION p_snap_min VALUES LESS THAN (DATE '2020-01-01')
);

INSERT INTO listing_scrape_snapshot (
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
SELECT
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
FROM listing_scrape_snapshot_base_v5;