import argparse
from scripts.seed import run_seed
from scripts.benchmarks.runner import run_all as run_all_benchmarks, run_query

BENCHMARK_CHOICES = [
    "query01",
    "query02",
    "query03",
    "query04",
    "query05",
    "query06",
    "query07",
]


def main():
    parser = argparse.ArgumentParser(
        description="Advanced Databases Test Suite orchestration."
    )
    parser.add_argument(
        "--seed", action="store_true", help="Run the database seeder (scripts/seed.py)"
    )
    parser.add_argument(
        "--benchmark",
        choices=BENCHMARK_CHOICES,
        help="Run a single benchmark query (see queries/ directory)",
    )
    parser.add_argument(
        "--benchmark-all",
        action="store_true",
        help="Run every benchmark query and collect execution plans",
    )

    args = parser.parse_args()

    if args.seed:
        run_seed()
    elif args.benchmark:
        output_path = run_query(args.benchmark)
        print(f"Benchmark {args.benchmark} complete. Results saved to {output_path}.")
    elif args.benchmark_all:
        results = run_all_benchmarks()
        for name, output in results:
            print(f"Benchmark {name} complete. Results saved to {output}.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
