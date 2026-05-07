-- Undo migration: Revert Single-Column index on calendar
DROP INDEX idx_calendar_listing_id;