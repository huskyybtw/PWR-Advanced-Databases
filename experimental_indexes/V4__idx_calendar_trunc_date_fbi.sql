-- 1.1 Proposed Indexes
-- 3. Function-Based Index (FBI)
-- Name: idx_calendar_trunc_date_fbi
-- Target: calendar (TRUNC("date"))
-- Purpose: Optimize analytical queries (Q1, Q4, Q6) that group/filter date-bounded lookups.

CREATE INDEX idx_calendar_trunc_date_fbi ON calendar (TRUNC("date"));