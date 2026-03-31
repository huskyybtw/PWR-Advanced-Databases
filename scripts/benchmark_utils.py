import datetime
import time

from scripts.db import BASE_DIR, get_connection, load_query

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


def run_benchmark(query_name, sql_file, statement_type="select"):
    sql = load_query(sql_file)
    statement = statement_type.lower()

    conn = get_connection()
    try:
        cursor = conn.cursor()
        plan_text = _collect_plan(cursor, sql)

        started_at = datetime.datetime.now()
        timer_start = time.perf_counter()
        cursor.execute(sql)

        if statement == "select":
            rowcount = _count_rows(cursor)
        else:
            rowcount = cursor.rowcount

        if rowcount is None:
            rowcount = -1

        duration = time.perf_counter() - timer_start
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()

    output_lines = [
        f"Query Name: {query_name}",
        f"SQL File: {sql_file}",
        f"Statement Type: {statement_type.upper()}",
        f"Started At: {started_at.isoformat()}",
        f"Duration (s): {duration:.4f}",
        f"Rows Impacted: {rowcount}",
        "-- SQL --",
        sql,
        "-- Execution Plan --",
        plan_text,
    ]

    timestamp = started_at.strftime("%Y%m%d_%H%M%S")
    output_path = RESULTS_DIR / f"{query_name}_{timestamp}.txt"
    output_path.write_text("\n".join(output_lines), encoding="utf-8")

    return output_path
