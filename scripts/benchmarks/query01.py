from scripts.benchmark_utils import run_benchmark

QUERY_NAME = "query01_historical_price_fluctuation"
SQL_FILE = "query01_historical_price_fluctuation.sql"


def run(tag="baseline", run_index=None, timestamp_override=None):
    """Run Historical Price Fluctuation benchmark."""
    return run_benchmark(
        QUERY_NAME,
        SQL_FILE,
        statement_type="select",
        tag=tag,
        run_index=run_index,
        timestamp_override=timestamp_override,
    )
