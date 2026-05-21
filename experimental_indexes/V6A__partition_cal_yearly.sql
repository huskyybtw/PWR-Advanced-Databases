ALTER TABLE calendar RENAME TO calendar_base_v6a;

CREATE TABLE calendar (
    calendar_id NUMBER(19) NOT NULL,
    listing_id NUMBER(19) NOT NULL,
    "date" DATE NOT NULL,
    available NUMBER(1) NOT NULL,
    price BINARY_DOUBLE,
    minimum_nights NUMBER(10) NOT NULL,
    maximum_nights NUMBER(10) NOT NULL,
    CONSTRAINT calendar_v6a_pk PRIMARY KEY (calendar_id),
    CONSTRAINT calendar_v6a_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
)
PARTITION BY RANGE ("date")
INTERVAL (NUMTOYMINTERVAL(1, 'YEAR'))
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
FROM calendar_base_v6a;

CREATE INDEX idx_calendar_trunc_date_fbi_v6a ON calendar (TRUNC ("date"));
CREATE INDEX idx_calendar_covering_v6a ON calendar (listing_id, "date", available);