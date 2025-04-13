import argparse
from rapidq.master import main_process


def parse_args():
    """
    Parse command line arguments.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="RapidQ - A simplified task processing library for Python."
    )
    parser.add_argument(
        "--module",
        type=str,
        help="Module to import for the application to work.",
        # required=True,
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        help="The number of worker processes to use.",
    )
    args = parser.parse_args()
    return args


def main():
    """
    Main entry point for RapidQ.
    """
    args = parse_args()
    print("Welcome to RapidQ!")
    main_process(workers=args.workers, module_name=args.module)
    return 0
