from scripts.benchmark_utils import run_benchmark

QUERY_NAME = "query05_massive_snapshot_generation"
SQL_FILE = "query05_massive_snapshot_generation.sql"


def run(tag="baseline", run_index=None, timestamp_override=None):
    """Run Massive Snapshot Generation benchmark."""
    return run_benchmark(
        QUERY_NAME,
        SQL_FILE,
        statement_type="insert",
        tag=tag,
        run_index=run_index,
        timestamp_override=timestamp_override,
    )
