import oracledb

# Centralized connection settings
PASSWORD = "Oracle123!"
DSN = "localhost:1521/XEPDB1"
USER = "airbnb"


def get_connection():
    """
    Creates and returns a new connection to the Oracle database.
    Remember to close the connection in your scripts after use!
    """
    try:
        connection = oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)
        return connection
    except Exception as e:
        print(f"[!] Failed to connect to DB: {e}")
        raise
