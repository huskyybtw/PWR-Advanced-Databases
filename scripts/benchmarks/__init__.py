from scripts.benchmarks import query01, query02, query03, query04, query05, query06, query07

QUERY_RUNNERS = {
    "query01": query01.run,
    "query02": query02.run,
    "query03": query03.run,
    "query04": query04.run,
    "query05": query05.run,
    "query06": query06.run,
    "query07": query07.run,
}

__all__ = ["QUERY_RUNNERS"]
