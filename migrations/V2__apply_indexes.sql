-- Apply the most optimal indexes discovered during the index experimentation phase
-- These indexes optimize the query plans significantly avoiding Full Table Scans.

-- 1. Composite B-Tree Index for reviews
-- Purpose: Speeds up historical lookups filtering by listing and date
CREATE
INDEX idx_reviews_listing_date ON reviews (listing_id, "date");

-- 2. Bitmap Index for superhosts
-- Purpose: Extremely efficient for low-cardinality boolean filtering (host_is_superhost)
CREATE BITMAP
INDEX idx_hosts_superhost_bmp ON hosts (host_is_superhost);

-- 3. Function-Based Index for calendar
-- Purpose: Optimizes date-bounded queries and aggregations
CREATE INDEX idx_calendar_trunc_date_fbi ON calendar (TRUNC ("date"));

-- 4. Covering Index for calendar
-- Purpose: Eliminates table accesses by providing necessary columns directly in the index (listing_id, date, availability)
CREATE
INDEX idx_calendar_covering ON calendar (listing_id, "date", available);