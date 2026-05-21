ALTER TABLE calendar RENAME TO calendar_base_v7;

CREATE TABLE calendar (
    calendar_id NUMBER(19) NOT NULL,
    listing_id NUMBER(19) NOT NULL,
    "date" DATE NOT NULL,
    available NUMBER(1) NOT NULL,
    price BINARY_DOUBLE,
    minimum_nights NUMBER(10) NOT NULL,
    maximum_nights NUMBER(10) NOT NULL,
    year_val NUMBER(4) GENERATED ALWAYS AS (EXTRACT(YEAR FROM "date")) VIRTUAL,
    CONSTRAINT calendar_v7_pk PRIMARY KEY (calendar_id),
    CONSTRAINT calendar_v7_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
)
PARTITION BY LIST (year_val)
(
    PARTITION p_cal_2020 VALUES (2020),
    PARTITION p_cal_2021 VALUES (2021),
    PARTITION p_cal_2022 VALUES (2022),
    PARTITION p_cal_2023 VALUES (2023),
    PARTITION p_cal_2024 VALUES (2024),
    PARTITION p_cal_2025 VALUES (2025),
    PARTITION p_cal_2026 VALUES (2026),
    PARTITION p_cal_2027 VALUES (2027),
    PARTITION p_cal_2028 VALUES (2028),
    PARTITION p_cal_2029 VALUES (2029),
    PARTITION p_cal_2030 VALUES (2030),
    PARTITION p_cal_default VALUES (DEFAULT)
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
FROM calendar_base_v7;

CREATE INDEX idx_calendar_trunc_date_fbi_v7 ON calendar (TRUNC ("date"));
CREATE INDEX idx_calendar_covering_v7 ON calendar (listing_id, "date", available);