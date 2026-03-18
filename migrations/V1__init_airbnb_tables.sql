-- Inside AirBnB Schema Migration for Oracle Database

CREATE TABLE locations (
    location_id NUMBER(19) NOT NULL,
    location_name VARCHAR2(255) NOT NULL,
    country VARCHAR2(255) NOT NULL,
    state_code VARCHAR2(255) NOT NULL,
    CONSTRAINT locations_pk PRIMARY KEY (location_id)
);

CREATE TABLE neighbourhoods (
    neighbourhood_id NUMBER(19) NOT NULL,
    location_id NUMBER(19) NOT NULL,
    neighbourhood_group VARCHAR2(255),
    neighbourhood VARCHAR2(255) NOT NULL,
    CONSTRAINT neighbourhoods_pk PRIMARY KEY (neighbourhood_id),
    CONSTRAINT neighbourhoods_loc_fk FOREIGN KEY (location_id) REFERENCES locations (location_id)
);

CREATE TABLE hosts (
    host_id NUMBER(19) NOT NULL,
    host_name VARCHAR2(255) NOT NULL,
    host_since DATE NOT NULL,
    host_location VARCHAR2(255) NOT NULL,
    host_about CLOB,
    host_response_time VARCHAR2(255),
    host_response_rate VARCHAR2(255),
    host_acceptance_rate VARCHAR2(255),
    host_is_superhost NUMBER(1) NOT NULL, -- Oracle uses NUMBER(1) for BOOLEAN
    host_total_listings_count NUMBER(10) NOT NULL,
    CONSTRAINT hosts_pk PRIMARY KEY (host_id)
);

CREATE TABLE listings (
    listing_id NUMBER(19) NOT NULL,
    host_id NUMBER(19) NOT NULL,
    listing_url VARCHAR2(1024),
    name VARCHAR2(255) NOT NULL,
    description CLOB,
    picture_url VARCHAR2(1024),
    neighbourhood_id NUMBER(19) NOT NULL,
    latitude BINARY_DOUBLE NOT NULL,
    longitude BINARY_DOUBLE NOT NULL,
    property_type VARCHAR2(255),
    room_type VARCHAR2(255) NOT NULL,
    accommodates NUMBER(10),
    bathrooms_text VARCHAR2(255),
    bedrooms NUMBER(10),
    beds NUMBER(10),
    license VARCHAR2(255),
    instant_bookable NUMBER(1), -- Oracle uses NUMBER(1) for BOOLEAN
    CONSTRAINT listings_pk PRIMARY KEY (listing_id),
    CONSTRAINT listings_host_fk FOREIGN KEY (host_id) REFERENCES hosts (host_id),
    CONSTRAINT listings_neigh_fk FOREIGN KEY (neighbourhood_id) REFERENCES neighbourhoods (neighbourhood_id)
);

CREATE TABLE calendar (
    calendar_id NUMBER(19) NOT NULL,
    listing_id NUMBER(19) NOT NULL,
    "date" DATE NOT NULL,
    available NUMBER(1) NOT NULL, -- Oracle uses NUMBER(1) for BOOLEAN
    price BINARY_DOUBLE,
    minimum_nights NUMBER(10) NOT NULL,
    maximum_nights NUMBER(10) NOT NULL,
    CONSTRAINT calendar_pk PRIMARY KEY (calendar_id),
    CONSTRAINT calendar_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
);

CREATE TABLE reviews (
    review_id NUMBER(19) NOT NULL,
    listing_id NUMBER(19) NOT NULL,
    "date" DATE NOT NULL,
    reviewer_id NUMBER(19) NOT NULL,
    reviewer_name VARCHAR2(255) NOT NULL,
    comments CLOB,
    CONSTRAINT reviews_pk PRIMARY KEY (review_id),
    CONSTRAINT reviews_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
);

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
    CONSTRAINT snapshot_listing_fk FOREIGN KEY (listing_id) REFERENCES listings (listing_id)
);