from scripts.benchmarks import QUERY_RUNNERS


import datetime


def run_query(name, populations=1, tag="baseline", timestamp_override=None):
    normalized = name.lower()
    if normalized not in QUERY_RUNNERS:
        raise ValueError(f"Unknown query benchmark: {name}")
    runner = QUERY_RUNNERS[normalized]

    if timestamp_override is None:
        timestamp_override = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    results = []
    for i in range(1, populations + 1):
        output_path = runner(
            tag=tag, run_index=i, timestamp_override=timestamp_override
        )
        results.append(output_path)

    from scripts.report_utils import create_aggregate_report

    report_path = create_aggregate_report(
        name, tag, populations, results, timestamp_override
    )
    results.append(report_path)
    return results


def run_all(populations=1, tag="baseline"):
    results = []
    timestamp_override = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    for name in sorted(QUERY_RUNNERS.keys()):
        outputs = run_query(
            name,
            populations=populations,
            tag=tag,
            timestamp_override=timestamp_override,
        )
        results.append((name, outputs))
    return results
