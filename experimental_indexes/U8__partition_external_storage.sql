-- Move the partition back to the default tablespace first!
-- Otherwise we cannot drop the external tablespace without data loss.
ALTER TABLE calendar MOVE PARTITION p_cal_min TABLESPACE USERS ONLINE
UPDATE INDEXES;

-- Now drop the tablespace and delete the physical file
DROP TABLESPACE external_archive_ts INCLUDING CONTENTS AND DATAFILES;