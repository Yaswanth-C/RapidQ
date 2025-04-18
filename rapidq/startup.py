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
        # required=True,
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=CPU_COUNT,
        help="The number of worker processes to use.",
    )
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Flush the broker and exit",
    )
    args = parser.parse_args()
    return args


def read_config_from_module(module_path: str):
    module = import_module(module_path)

    configurable_keys = (
        "RAPIDQ_BROKER_SERIALIZER",
        "RAPIDQ_BROKER_URL",
    )
    for key in configurable_keys:
        if not getattr(module, key, None):
            continue
        os.environ[key] = str(getattr(module, key))


def main():
    """
    Main entry point for RapidQ.
    """
    args = parse_args()

    if args.module:
        import_module(args.module)

    if args.flush:
        broker = get_broker()
        broker.flush()
        print("Tasks flushed.")
        sys.exit(0)

    print("Welcome to RapidQ!")
    main_process(workers=args.workers, module_name=args.module)
    return 0
