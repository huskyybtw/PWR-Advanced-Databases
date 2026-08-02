import oracledb
import shlex
import subprocess
import time
from datetime import datetime
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
BACKUPS_DIR = BASE_DIR / "backups"
ORACLE_CONTAINER_NAME = "oracle-xe-21c"
ORACLE_PDB_DSN = "localhost:1521/XEPDB1"
ORACLE_BACKUP_DIR = "/opt/oracle/backup"


def wait_for_inmemory_population():
    """
    Blocks execution until all Oracle In-Memory segments have finished loading.
    """
    print("\n[DB] Checking In-Memory column store population status...")

    # Since 'airbnb' user has SELECT ANY DICTIONARY privileges, standard connection works
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            while True:
                # Count segments that aren't fully populated yet
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM v$im_segments 
                    WHERE populate_status != 'COMPLETED'
                """)
                remaining = cursor.fetchone()[0]

                # Verify that segments have actually registered in the tracking view
                cursor.execute("SELECT COUNT(*) FROM v$im_segments")
                total_segments = cursor.fetchone()[0]

                if remaining == 0 and total_segments > 0:
                    print(
                        f"[DB] Success! All {total_segments} In-Memory segments are 100% populated.\n"
                    )
                    break
                elif total_segments == 0:
                    print(
                        "[DB] Waiting for Oracle background processes to register In-Memory segments..."
                    )
                else:
                    print(
                        f"[DB] Population in progress... {remaining} segment(s) remaining."
                    )

                time.sleep(3)  # Poll every 3 seconds
    except Exception as e:
        print(f"[!] Error querying In-Memory status: {e}")
        raise
    finally:
        conn.close()


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

    statements = [
        statement.strip() for statement in sql_text.split(";") if statement.strip()
    ]

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


def _run_docker_exec(shell_command):
    subprocess.run(
        ["docker", "exec", ORACLE_CONTAINER_NAME, "bash", "-lc", shell_command],
        check=True,
    )


def _ensure_backup_directory():
    sql = """
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = XEPDB1;
BEGIN
    EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY BACKUP_DIR AS ''/opt/oracle/backup''';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
END;
/
GRANT READ, WRITE ON DIRECTORY BACKUP_DIR TO airbnb;
EXIT;
""".strip()

    _run_docker_exec(
        "cat <<'SQL' | sqlplus -s system/$ORACLE_PWD@localhost:1521/XEPDB1\n"
        + sql
        + "\nSQL"
    )


def create_schema_backup(prefix="pre_benchmark"):
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    _ensure_backup_directory()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dumpfile = f"{prefix}_{timestamp}.dmp"
    logfile = f"{prefix}_{timestamp}.log"

    _run_docker_exec(
        "expdp airbnb/$ORACLE_PWD@localhost:1521/XEPDB1 "
        + f"schemas=airbnb directory=BACKUP_DIR dumpfile={shlex.quote(dumpfile)} "
        + f"logfile={shlex.quote(logfile)} metrics=y"
    )

    container_dump = f"{ORACLE_CONTAINER_NAME}:{ORACLE_BACKUP_DIR}/{dumpfile}"
    container_log = f"{ORACLE_CONTAINER_NAME}:{ORACLE_BACKUP_DIR}/{logfile}"
    local_dump = BACKUPS_DIR / dumpfile
    local_log = BACKUPS_DIR / logfile

    subprocess.run(["docker", "cp", container_dump, str(local_dump)], check=True)
    subprocess.run(["docker", "cp", container_log, str(local_log)], check=True)

    return local_dump


def restore_schema_backup(backup_file):
    backup_file = Path(backup_file)
    if not backup_file.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_file}")

    _ensure_backup_directory()
    subprocess.run(
        [
            "docker",
            "cp",
            str(backup_file),
            f"{ORACLE_CONTAINER_NAME}:{ORACLE_BACKUP_DIR}/{backup_file.name}",
        ],
        check=True,
    )

    sql = """
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = XEPDB1;
BEGIN
    EXECUTE IMMEDIATE 'DROP USER airbnb CASCADE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -1918 THEN
            RAISE;
        END IF;
END;
/
CREATE USER airbnb IDENTIFIED BY "Oracle123!";
GRANT CONNECT, RESOURCE TO airbnb;
GRANT UNLIMITED TABLESPACE TO airbnb;
GRANT SELECT ANY DICTIONARY TO airbnb;
GRANT READ, WRITE ON DIRECTORY BACKUP_DIR TO airbnb;
EXIT;
""".strip()

    _run_docker_exec(
        "cat <<'SQL' | sqlplus -s system/$ORACLE_PWD@localhost:1521/XEPDB1\n"
        + sql
        + "\nSQL"
    )

    _run_docker_exec(
        "impdp airbnb/$ORACLE_PWD@localhost:1521/XEPDB1 "
        + f"schemas=airbnb directory=BACKUP_DIR dumpfile={shlex.quote(backup_file.name)} "
        + f"logfile={shlex.quote(backup_file.stem + '_restore.log')} metrics=y"
    )
