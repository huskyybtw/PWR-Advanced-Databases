from scripts.benchmark_utils import run_benchmark


QUERY_NAME = "query05_massive_snapshot_generation"
SQL_FILE = "query05_massive_snapshot_generation.sql"


def run():
    """Run Massive Snapshot Generation benchmark."""
    return run_benchmark(QUERY_NAME, SQL_FILE, statement_type="insert")
