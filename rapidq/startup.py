import argparse
import os
import sys
from multiprocessing import cpu_count
from rapidq.master import main_process
from rapidq.broker import get_broker_class

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
        "--broker-serializer",
        type=str,
        default="json",
        help="The type of serialization to use for broker communication. (json/pickle)",
    )
    parser.add_argument(
        "--broker-url",
        type=str,
        default="redis://localhost:6379",
        help="URL to use for connecting to the broker. eg: redis://localhost:6379",
    )
    parser.add_argument(
        "--flush",
        action="store_true",
        help="Flush the broker and exit",
    )
    args = parser.parse_args()
    return args


def configure(configuration: dict):
    configurable_keys = (
        "RAPIDQ_BROKER_SERIALIZER",
        "RAPIDQ_BROKER_URL",
    )
    for key in configurable_keys:
        if not configuration.get(key):
            continue
        os.environ[key] = str(configuration[key])


def main():
    """
    Main entry point for RapidQ.
    """
    args = parse_args()
    configure(
        {
            "RAPIDQ_BROKER_SERIALIZER": args.broker_serializer,
            "RAPIDQ_BROKER_URL": args.broker_url,
        }
    )

    if args.flush:
        broker_class = get_broker_class()
        broker = broker_class()
        broker.flush()
        print("Tasks flushed.")
        sys.exit(0)

    print("Welcome to RapidQ!")
    main_process(workers=args.workers, module_name=args.module)
    return 0
