-- Create external tablespace on the external docker volume
CREATE TABLESPACE external_archive_ts 
DATAFILE '/opt/oracle/external_disk/archive_data01.dbf' 
SIZE 100M AUTOEXTEND ON NEXT 10M;

-- Move the old partition to the external tablespace
-- Note: Requires the calendar table to be partitioned first. This sits on top of V5!
ALTER TABLE calendar MOVE PARTITION p_cal_min TABLESPACE external_archive_ts ONLINE
UPDATE INDEXES;