import argparse
import os
import sys
from importlib import import_module

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
        "-w", "--workers", type=int, default=1, help="The number of worker processes to use."
    )
    args = parser.parse_args()
    return args


def _import_module(module_name):
    current_path = os.getcwd()
    if current_path not in sys.path:
        sys.path.append(current_path)
    _module = import_module(module_name)
    return _module


def main():
    """
    Main entry point for RapidQ.
    """
    args = parse_args()
    print("Welcome to RapidQ!")
    module = None
    if args.module:
        module = _import_module(args.module)
    main_process(workers=args.workers)
    return 0
