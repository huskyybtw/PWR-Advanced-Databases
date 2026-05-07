-- 1.1 Proposed Indexes
-- 2. Bitmap Index
-- Name: idx_hosts_superhost_bmp
-- Target: hosts (host_is_superhost)
-- Purpose: Target queries Q2 and Q7, which filter strictly on h.host_is_superhost = TRUE (or 0).

CREATE BITMAP INDEX idx_hosts_superhost_bmp ON hosts (host_is_superhost);