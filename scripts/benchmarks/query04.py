from scripts.benchmark_utils import run_benchmark


QUERY_NAME = "query04_financial_impact_projection"
SQL_FILE = "query04_financial_impact_projection.sql"


def run():
    """Run Financial Impact Projection benchmark."""
    return run_benchmark(QUERY_NAME, SQL_FILE, statement_type="select")
