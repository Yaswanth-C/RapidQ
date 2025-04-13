import argparse
import sys
from rapidq.master import main_process
from rapidq.broker import get_broker_class


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
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Flush the broker and exit",
    )
    args = parser.parse_args()
    return args


def main():
    """
    Main entry point for RapidQ.
    """
    args = parse_args()
    if args.flush:
        broker_class = get_broker_class()
        broker = broker_class()
        broker.flush()
        print("Tasks flushed.")
        sys.exit(0)

    print("Welcome to RapidQ!")
    main_process(workers=args.workers, module_name=args.module)
    return 0
