-- 1.2 Comparative Experiments
-- Single-Column vs. Composite (Covering) Index - Part 2
-- Name: idx_calendar_covering
-- Target: calendar (listing_id, "date", available)
-- Purpose: To act as the covering index for the Single-Column vs Composite Index experiment. It covers Q1 implicitly.

CREATE INDEX idx_calendar_covering ON calendar (listing_id, "date", available);