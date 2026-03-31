from scripts.benchmarks import QUERY_RUNNERS


def run_query(name):
    normalized = name.lower()
    if normalized not in QUERY_RUNNERS:
        raise ValueError(f"Unknown query benchmark: {name}")
    runner = QUERY_RUNNERS[normalized]
    return runner()


def run_all():
    results = []
    for name in sorted(QUERY_RUNNERS.keys()):
        output_path = QUERY_RUNNERS[name]()
        results.append((name, output_path))
    return results
