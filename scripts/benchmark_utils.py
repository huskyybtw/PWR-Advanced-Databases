import datetime
import time

from scripts.db import BASE_DIR, get_admin_connection, get_connection, load_query

RESULTS_DIR = BASE_DIR / "benchmark_results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def _collect_plan(cursor, sql):
    cursor.execute("EXPLAIN PLAN FOR\n" + sql)
    plan_rows = cursor.execute(
        "SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY())"
    ).fetchall()
    return "\n".join(row[0] for row in plan_rows if row and row[0])


def _count_rows(cursor, batch_size=1000):
    total = 0
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        total += len(batch)
    return total


def _flush_database_memory():
    try:
        conn = get_admin_connection()
    except Exception:
        return

    try:
        cursor = conn.cursor()
        for statement in (
            "ALTER SYSTEM FLUSH SHARED_POOL",
            "ALTER SYSTEM FLUSH BUFFER_CACHE",
        ):
            try:
                cursor.execute(statement)
            except Exception as exc:
                print(f"[!] Skipping {statement}: {exc}")
    finally:
        conn.close()


def run_benchmark(
    query_name,
    sql_file,
    statement_type="select",
    tag="baseline",
    run_index=None,
    timestamp_override=None,
):
    sql = load_query(sql_file)
    statement = statement_type.lower()

    _flush_database_memory()

    conn = get_connection()
    try:
        cursor = conn.cursor()
        plan_text = _collect_plan(cursor, sql)

        started_at = datetime.datetime.now()
        execute_start = time.perf_counter()
        cursor.execute(sql)
        execute_duration = time.perf_counter() - execute_start

        fetch_duration = 0.0
        if statement == "select":
            fetch_start = time.perf_counter()
            rowcount = _count_rows(cursor)
            fetch_duration = time.perf_counter() - fetch_start
        else:
            rowcount = cursor.rowcount

        if rowcount is None:
            rowcount = -1

        duration = execute_duration + fetch_duration
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()
        _flush_database_memory()

    output_lines = [
        f"Query Name: {query_name}",
        f"Tag: {tag}",
        f"Run Index: {run_index if run_index is not None else 'N/A'}",
        f"SQL File: {sql_file}",
        f"Statement Type: {statement_type.upper()}",
        f"Started At: {started_at.isoformat()}",
        f"Execution Duration (s): {execute_duration:.4f}",
        f"Fetch Duration (s): {fetch_duration:.4f}",
        f"Total Duration (s): {duration:.4f}",
        f"Rows Impacted: {rowcount}",
        "-- SQL --",
        sql,
        "-- Estimated Execution Plan --",
        plan_text,
    ]

    timestamp = timestamp_override or started_at.strftime("%Y%m%d_%H%M%S")
    suffix = f"_{tag}_run{run_index}" if run_index is not None else f"_{tag}"
    output_path = RESULTS_DIR / f"{query_name}{suffix}_{timestamp}.txt"
    output_path.write_text("\n".join(output_lines), encoding="utf-8")

    return output_path
