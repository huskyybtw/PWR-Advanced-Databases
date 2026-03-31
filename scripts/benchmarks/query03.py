from scripts.benchmark_utils import run_benchmark


QUERY_NAME = "query03_potential_monthly_revenue"
SQL_FILE = "query03_potential_monthly_revenue.sql"


def run():
    """Run Potential Monthly Revenue benchmark."""
    return run_benchmark(QUERY_NAME, SQL_FILE, statement_type="select")
