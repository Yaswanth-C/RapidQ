import argparse
import os
import sys
from argparse import Namespace

from rapidq import __version__
from rapidq.broker import Broker, get_broker
from rapidq.constants import CPU_COUNT
from rapidq.master import main_process
from rapidq.utils import import_module


def parse_args() -> Namespace:
    """
    Parse command line arguments.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="RapidQ - A simplified task processing library for Python."
    )
    parser.add_argument(
        "module",
        type=str,
        help="Module to import for the application to work.",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=CPU_COUNT,
        help="The number of worker processes to use.",
    )

    args = parser.parse_args()
    return args


def main():
    """
    Main entry point for RapidQ.
    """
    args = parse_args()
    import_module(args.module)
    sys.stdout.write(f"\nWelcome to RapidQ! ({__version__})\n\n")
    main_process(workers=args.workers, module_name=args.module)
    return 0


def flush_queue() -> None:
    broker = get_broker()
    broker.flush()
    print("Tasks flushed.")
    sys.exit(0)
