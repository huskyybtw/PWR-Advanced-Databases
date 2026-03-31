from scripts.benchmark_utils import run_benchmark


QUERY_NAME = "query06_dynamic_calendar_blocking"
SQL_FILE = "query06_dynamic_calendar_blocking.sql"


def run():
    """Run Dynamic Calendar Blocking benchmark."""
    return run_benchmark(QUERY_NAME, SQL_FILE, statement_type="update")
