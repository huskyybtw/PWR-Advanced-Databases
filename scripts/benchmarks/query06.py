from scripts.benchmark_utils import run_benchmark

QUERY_NAME = "query06_dynamic_calendar_blocking"
SQL_FILE = "query06_dynamic_calendar_blocking.sql"


def run(tag="baseline", run_index=None, timestamp_override=None):
    """Run Dynamic Calendar Blocking benchmark."""
    return run_benchmark(
        QUERY_NAME,
        SQL_FILE,
        statement_type="update",
        tag=tag,
        run_index=run_index,
        timestamp_override=timestamp_override,
    )
