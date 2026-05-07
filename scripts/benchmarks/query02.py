from scripts.benchmark_utils import run_benchmark

QUERY_NAME = "query02_superhost_portfolio_analysis"
SQL_FILE = "query02_superhost_portfolio_analysis.sql"


def run(tag="baseline", run_index=None, timestamp_override=None):
    """Run Superhost Portfolio Analysis benchmark."""
    return run_benchmark(
        QUERY_NAME,
        SQL_FILE,
        statement_type="select",
        tag=tag,
        run_index=run_index,
        timestamp_override=timestamp_override,
    )
