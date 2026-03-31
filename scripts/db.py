import oracledb
from pathlib import Path

# Centralized connection settings
PASSWORD = "Oracle123!"
DSN = "localhost:1521/XEPDB1"
USER = "airbnb"

BASE_DIR = Path(__file__).resolve().parent.parent
QUERIES_DIR = BASE_DIR / "queries"


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


def load_query(filename):
    """Load a SQL file from queries/ while stripping guard statements."""
    query_path = QUERIES_DIR / filename
    if not query_path.exists():
        raise FileNotFoundError(f"SQL file not found: {query_path}")

    guard_markers = {"BEGIN TRANSACTION;", "ROLLBACK;"}
    skip_prefixes = ("SAVEPOINT ", "ROLLBACK TO ")
    lines = []

    with query_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            upper_line = stripped.upper()

            if not stripped or stripped.startswith("--"):
                continue
            if upper_line in guard_markers:
                continue
            if any(upper_line.startswith(prefix) for prefix in skip_prefixes):
                continue

            lines.append(raw_line.rstrip())

    sql = "\n".join(lines).strip()
    if sql.endswith(";"):
        sql = sql[:-1]

    if not sql:
        raise ValueError(f"SQL file {filename} is empty after stripping guards")

    return sql
