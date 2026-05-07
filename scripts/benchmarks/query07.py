from scripts.benchmark_utils import run_benchmark

QUERY_NAME = "query07_massive_cascade_purge"
SQL_FILE = "query07_massive_cascade_purge.sql"


def run(tag="baseline", run_index=None, timestamp_override=None):
    """Run Massive Cascade-Style Purge benchmark."""
    return run_benchmark(
        QUERY_NAME,
        SQL_FILE,
        statement_type="delete",
        tag=tag,
        run_index=run_index,
        timestamp_override=timestamp_override,
    )
