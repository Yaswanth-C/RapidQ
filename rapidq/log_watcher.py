from __future__ import annotations

import logging
from threading import Thread
from typing import TYPE_CHECKING, Any, ContextManager, Optional, TextIO

if TYPE_CHECKING:
    from rapidq.worker.process_worker import Worker

import sys
import time
from contextlib import nullcontext
from logging import Logger
from logging.handlers import QueueHandler
from multiprocessing.connection import Connection, PipeConnection
from multiprocessing.connection import wait as connection_wait
from multiprocessing.synchronize import Event

from rapidq.constants import LOGGING_FMT, LOGGING_FMT_TIME


def get_log_writable(filename: Optional[str]) -> ContextManager[TextIO]:
    """
    Returns a writable stream. Either file or stderr.
    """
    if filename is not None:
        return open(filename, "a", encoding="utf-8")
    return nullcontext(sys.stderr)


class LogWatcher(Thread):
    def __init__(
        self,
        *,
        log_pipe: Connection | PipeConnection,
        workers: dict[str, Worker],
        stop_event: Event,
        logging_file: Optional[str] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.log_pipe = log_pipe
        self.workers = workers
        self.stop_event = stop_event
        self.logger = logging.getLogger("rapidq.Master")
        self.logging_file = logging_file

    def log(self, pipe: Connection | PipeConnection, stream: TextIO):
        while pipe.poll():
            message = pipe.recv_bytes()
            if not message:
                continue

            try:
                stream.write(message.decode(encoding="utf-8", errors="replace"))
                stream.flush()
            except AttributeError:
                self.logger.info(message)

    def run(self) -> None:
        self.logger.info("logging thread started...")
        with get_log_writable(self.logging_file) as stream:
            while not self.stop_event.is_set():
                active_pipes = []
                if self.log_pipe and not self.log_pipe.closed:
                    active_pipes.append(self.log_pipe)

                active_pipes.extend(
                    [
                        worker.log_read_pipe
                        for worker in self.workers.values()
                        if worker.log_read_pipe and not worker.log_read_pipe.closed
                    ]
                )

                if not active_pipes:
                    time.sleep(0.2)
                    continue

                try:
                    ready_connections = connection_wait(active_pipes, timeout=0.5)
                    for pipe in ready_connections:
                        if not isinstance(pipe, (Connection, PipeConnection)):
                            continue
                        try:
                            self.log(pipe=pipe, stream=stream)
                        except (BrokenPipeError, EOFError, ConnectionResetError):
                            pipe.close()
                            continue

                except (EOFError, OSError) as error:
                    self.logger.debug(f"Logging Connection error: {error}")
                    time.sleep(0.1)


class PipeHandler(logging.Handler):
    """
    Logging handler that writes to Pipe.
    """

    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def emit(self, record):
        try:
            self.conn.send_bytes(self.prepare_record(record))
        except Exception as e:
            self.handleError(record)

    def prepare_record(self, record):
        log_str = self.format(record)
        return log_str.encode(encoding="utf-8", errors="replace")


class LogFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        # struct_time has no microsecond format string, so get it this way.
        micro_second = f"{int((record.created % 1) * 1000_000):06d}"
        converted_time = self.converter(record.created)
        fmt = datefmt or LOGGING_FMT_TIME
        # replace the custom string with microseconds.
        fmt = fmt.replace("%6f", micro_second)
        return time.strftime(fmt, converted_time)


def configure_logger(
    name: str, logging_pipe: Connection | PipeConnection, format_: str = LOGGING_FMT
) -> Logger:
    pipe_handler = PipeHandler(logging_pipe)
    formatter = LogFormatter(
        fmt=format_,
        datefmt=LOGGING_FMT_TIME,
    )
    pipe_handler.setFormatter(formatter)
    logging.basicConfig(
        handlers=[pipe_handler],
    )
    logger = logging.getLogger(f"rapidq.{name}")
    logger.setLevel(logging.DEBUG)
    return logger
