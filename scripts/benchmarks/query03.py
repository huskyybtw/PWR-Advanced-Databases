from scripts.benchmark_utils import run_benchmark

QUERY_NAME = "query03_potential_monthly_revenue"
SQL_FILE = "query03_potential_monthly_revenue.sql"


def run(tag="baseline", run_index=None, timestamp_override=None):
    """Run Potential Monthly Revenue benchmark."""
    return run_benchmark(
        QUERY_NAME,
        SQL_FILE,
        statement_type="select",
        tag=tag,
        run_index=run_index,
        timestamp_override=timestamp_override,
    )
