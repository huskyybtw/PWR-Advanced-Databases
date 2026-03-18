import argparse
from scripts.seed import run_seed


def main():
    parser = argparse.ArgumentParser(
        description="Advanced Databases Test Suite orchestration."
    )
    parser.add_argument(
        "--seed", action="store_true", help="Run the database seeder (scripts/seed.py)"
    )

    args = parser.parse_args()

    if args.seed:
        run_seed()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
