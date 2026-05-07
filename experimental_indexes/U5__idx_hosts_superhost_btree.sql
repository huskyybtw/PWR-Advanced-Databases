-- Undo migration: Revert B-Tree index on hosts
DROP INDEX idx_hosts_superhost_btree;