-- 1.2 Comparative Experiments
-- Single-Column vs. Composite (Covering) Index - Part 1
-- Name: idx_calendar_listing_id
-- Target: calendar (listing_id)
-- Purpose: To act as a baseline for the Single-Column vs Composite Index experiment.

CREATE INDEX idx_calendar_listing_id ON calendar (listing_id);