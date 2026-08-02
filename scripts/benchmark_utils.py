import datetime
import hashlib
import json
import time
from decimal import Decimal

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


def _normalize_value(value):
    if value is None:
        return "<NULL>"
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat(sep=" ")
        except TypeError:
            return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _fingerprint_result_set(cursor, batch_size=1000):
    hasher = hashlib.sha256()
    row_count = 0

    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break

        for row in batch:
            serialized_row = "\x1f".join(_normalize_value(value) for value in row)
            hasher.update(serialized_row.encode("utf-8"))
            hasher.update(b"\x1e")
            row_count += 1

    return row_count, hasher.hexdigest()


def _flush_database_memory():
    return
    # try:
    #     conn = get_admin_connection()
    # except Exception:
    #     return

    # try:
    #     cursor = conn.cursor()
    #     for statement in (
    #         "ALTER SYSTEM FLUSH SHARED_POOL",
    #         "ALTER SYSTEM FLUSH BUFFER_CACHE",
    #     ):
    #         try:
    #             cursor.execute(statement)
    #         except Exception as exc:
    #             print(f"[!] Skipping {statement}: {exc}")
    # finally:
    #     conn.close()


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
        cursor.execute("SAVEPOINT benchmark_start")
        plan_text = _collect_plan(cursor, sql)

        started_at = datetime.datetime.now()
        execute_start = time.perf_counter()
        cursor.execute(sql)
        execute_duration = time.perf_counter() - execute_start

        fetch_duration = 0.0
        if statement == "select":
            fetch_start = time.perf_counter()
            rowcount, result_signature = _fingerprint_result_set(cursor)
            fetch_duration = time.perf_counter() - fetch_start
        else:
            rowcount = cursor.rowcount
            result_signature = f"ROWS:{rowcount if rowcount is not None else -1}"

        if rowcount is None:
            rowcount = -1

        duration = execute_duration + fetch_duration
    finally:
        try:
            try:
                conn.cursor().execute("ROLLBACK TO benchmark_start")
            except Exception:
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
        f"Result Signature: {result_signature}",
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
