from scripts.benchmark_utils import run_benchmark


QUERY_NAME = "query07_massive_cascade_purge"
SQL_FILE = "query07_massive_cascade_purge.sql"


def run():
    """Run Massive Cascade-Style Purge benchmark."""
    return run_benchmark(QUERY_NAME, SQL_FILE, statement_type="delete")
