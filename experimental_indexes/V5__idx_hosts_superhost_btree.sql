-- 1.2 Comparative Experiments
-- B-Tree vs. Bitmap for Low-Cardinality Filters
-- Name: idx_hosts_superhost_btree
-- Target: hosts (host_is_superhost)
-- Purpose: To compare with Bitmap index (idx_hosts_superhost_bmp) and verify if the Oracle Optimizer defaults back to FTS.

CREATE INDEX idx_hosts_superhost_btree ON hosts (host_is_superhost);