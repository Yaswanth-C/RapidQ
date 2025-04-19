import argparse
import os
import sys
from multiprocessing import cpu_count
from rapidq.master import main_process
from rapidq.broker import get_broker
from rapidq.utils import import_module

CPU_COUNT = min(4, cpu_count())


def parse_args():
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
    print("Welcome to RapidQ!")
    main_process(workers=args.workers, module_name=args.module)
    return 0


def flush_queue():
    broker = get_broker()
    broker.flush()
    print("Tasks flushed.")
    sys.exit(0)
