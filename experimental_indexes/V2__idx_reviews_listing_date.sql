-- 1.1 Proposed Indexes
-- 1. B-Tree Index (Composite Index)
-- Name: idx_reviews_listing_date
-- Target: reviews (listing_id, "date")
-- Purpose: Most analytical queries (Q4, Q5, Q6, Q7) filter historical reviews checking activity over the last year.

CREATE INDEX idx_reviews_listing_date ON reviews (listing_id, "date");