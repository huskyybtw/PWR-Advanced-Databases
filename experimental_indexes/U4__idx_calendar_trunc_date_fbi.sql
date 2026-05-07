-- Undo migration: Revert Function-Based Index on calendar
DROP INDEX idx_calendar_trunc_date_fbi;