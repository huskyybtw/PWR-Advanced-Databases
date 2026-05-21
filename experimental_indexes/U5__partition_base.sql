DROP TABLE calendar PURGE;
ALTER TABLE calendar_base_v5 RENAME TO calendar;

DROP TABLE reviews PURGE;
ALTER TABLE reviews_base_v5 RENAME TO reviews;

DROP TABLE listing_scrape_snapshot PURGE;
ALTER TABLE listing_scrape_snapshot_base_v5 RENAME TO listing_scrape_snapshot;