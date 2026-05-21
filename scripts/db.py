import oracledb
from pathlib import Path

# Centralized connection settings
PASSWORD = "Oracle123!"
DSN = "localhost:1521/XEPDB1"
USER = "airbnb"
ADMIN_USER = "system"
ADMIN_PASSWORD = PASSWORD

BASE_DIR = Path(__file__).resolve().parent.parent
QUERIES_DIR = BASE_DIR / "queries"
EXPERIMENTAL_INDEXES_DIR = BASE_DIR / "experimental_indexes"


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


def get_admin_connection():
    """Create a privileged connection for maintenance operations."""
    try:
        connection = oracledb.connect(user=ADMIN_USER, password=ADMIN_PASSWORD, dsn=DSN)
        return connection
    except Exception as e:
        print(f"[!] Failed to connect to DB as admin: {e}")
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


def get_experimental_index_files(tag):
    """
    Finds the V and U SQL files in experimental_indexes/ that match the given tag.
    Returns (v_file_path, u_file_path) or (None, None) if not found.
    """
    if tag == "baseline" or not tag:
        return None, None

    v_files = list(EXPERIMENTAL_INDEXES_DIR.glob(f"V*__*{tag}*.sql"))
    u_files = list(EXPERIMENTAL_INDEXES_DIR.glob(f"U*__*{tag}*.sql"))

    v_file = v_files[0] if v_files else None
    u_file = u_files[0] if u_files else None

    return v_file, u_file


def execute_ddl_script(filepath):
    """
    Executes a DDL configuration script (like CREATE INDEX or DROP INDEX).
    """
    if not filepath or not filepath.exists():
        return

    lines = []
    with filepath.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            lines.append(stripped)

    sql_text = "\n".join(lines).strip()
    if sql_text.endswith(";"):
        sql_text = sql_text[:-1]

    if not sql_text:
        return

    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        conn.commit()
        print(f"[DB] Successfully executed {filepath.name}")
    except Exception as e:
        print(f"[!] Error executing {filepath.name}:\n{e}")
        raise
    finally:
        conn.close()
