from __future__ import annotations

import logging
from threading import Thread
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rapidq.worker.process_worker import Worker

import sys
import time
from logging import Logger
from logging.handlers import QueueHandler
from multiprocessing.connection import Connection, PipeConnection
from multiprocessing.connection import wait as connection_wait
from multiprocessing.synchronize import Event


class LogWatcher(Thread):
    def __init__(
        self,
        *,
        log_pipe: Connection | PipeConnection,
        workers: dict[str, Worker],
        stop_event: Event,
    ) -> None:
        super().__init__(daemon=True)
        self.log_pipe = log_pipe
        self.workers = workers
        self.stop_event = stop_event

    def run(self) -> None:
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
                        while pipe.poll():
                            message = pipe.recv_bytes()
                            if not message:
                                continue

                            try:
                                # FIXME: change to file logger
                                print(
                                    message.decode(encoding="utf-8", errors="replace")
                                )
                                # getattr(self.logger, level)(msg)
                            except AttributeError:
                                self.logger.info(msg)
                    except (BrokenPipeError, EOFError):
                        pipe.close()
                        continue

            except (EOFError, OSError, BrokenPipeError) as error:
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


def configure_logger(name, logging_pipe, format=None) -> Logger:
    logging.basicConfig(
        format="[%(asctime)s] [PID %(process)d] [%(name)s] [%(levelname)s] %(message)s",
        handlers=[PipeHandler(logging_pipe)],
    )
    logger = logging.getLogger(f"rapidq.{name}")
    logger.setLevel(logging.DEBUG)
    return logger
