-- Undo migration: Revert Bitmap index on hosts
DROP INDEX idx_hosts_superhost_bmp;