import argparse
from scripts.seed import run_seed
from scripts.benchmarks.runner import run_all as run_all_benchmarks, run_query
from scripts.db import get_experimental_index_files, execute_ddl_script

"""
=== EXPERIMENTAL INDEXING PIPELINE ===
This module automates the execution of Oracle SQL queries to calculate execution averages,
variance, and caching performance. 

* Indexing Isolation:
Flyway applies SQL modifications sequentially. To bypass this limitations for pure index experiments,
the experimental index scripts (e.g., `V2`, `V3` etc.) have been moved to `experimental_indexes/`.

* How it works with --tag:
If you supply a `--tag` that matches part of an experimental index file name (e.g. `idx_calendar_covering`), 
this script will automatically:
  1. Hunt for the matching `V...__<tag>.sql` file & execute it (CREATE INDEX)
  2. Run the benchmarks 
  3. Hunt for the matching `U...__<tag>.sql` file & execute it (DROP INDEX)
  
This ensures the DB remains clean (at baseline) after each experiment finishes!
"""

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
    parser.add_argument(
        "--populations",
        type=int,
        default=1,
        help="Number of iterations to run each benchmark query to calculate averages.",
    )
    parser.add_argument(
        "--tag",
        type=str,
        default="baseline",
        help="Tag name for the current test (e.g., baseline, idx_calendar_covering). "
        "If the tag matches an isolated index script in experimental_indexes/, it will be applied automatically before benchmarking and dropped when finished.",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("TAG1", "TAG2"),
        help="Compare two tags and display an ASCII table of averages",
    )

    args = parser.parse_args()

    if args.compare:
        from scripts.report_utils import generate_comparison_table

        generate_comparison_table(args.compare[0], args.compare[1])
        return

    if args.seed:
        run_seed()
    elif args.benchmark or args.benchmark_all:
        v_file, u_file = get_experimental_index_files(args.tag)
        if v_file:
            print(f"Applying experimental index for tag '{args.tag}'...")
            execute_ddl_script(v_file)

        try:
            if args.benchmark:
                output_paths = run_query(
                    args.benchmark, populations=args.populations, tag=args.tag
                )
                for p in output_paths:
                    print(f"Benchmark {args.benchmark} complete. Results saved to {p}.")
            else:
                results = run_all_benchmarks(populations=args.populations, tag=args.tag)
                for name, outputs in results:
                    for p in outputs:
                        print(f"Benchmark {name} complete. Results saved to {p}.")
        finally:
            if u_file:
                print(f"Reverting experimental index for tag '{args.tag}'...")
                execute_ddl_script(u_file)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
