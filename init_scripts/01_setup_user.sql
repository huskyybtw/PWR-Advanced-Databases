-- This script runs on every container startup.
-- We must switch to the pluggable database (XEPDB1) where users are allowed to be created.
ALTER SESSION SET CONTAINER = XEPDB1;

-- Idempotent PL/SQL block to create the user only if it doesn't already exist
DECLARE users_count NUMBER;

BEGIN
SELECT COUNT(*) INTO users_count
FROM DBA_USERS
WHERE
    USERNAME = 'AIRBNB';

IF users_count = 0 THEN
EXECUTE IMMEDIATE 'CREATE USER airbnb IDENTIFIED BY "Oracle123!"';

EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE TO airbnb';

EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO airbnb';

EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO airbnb';

DBMS_OUTPUT.PUT_LINE (
    'User airbnb created successfully.'
);

ELSE DBMS_OUTPUT.PUT_LINE (
    'User airbnb already exists. Skipping creation.'
);

END IF;

END;

/