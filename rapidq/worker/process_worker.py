import os
import time
from multiprocessing import Process, Queue, Value
from multiprocessing.sharedctypes import Synchronized
from multiprocessing.synchronize import Event as SyncEvent
from queue import Empty
from typing import Any, Callable, Optional

from rapidq.constants import DEFAULT_IDLE_TIME, WorkerState
from rapidq.message import Message
from rapidq.registry import TaskRegistry
from rapidq.utils import import_module


class Worker:
    """
    Class that handles a single worker process.
    """

    def __init__(
        self,
        queue: Queue,
        name: str,
        shutdown_event: SyncEvent,
        process_counter: Synchronized,
        state: Synchronized,
        module_name: str,
    ):
        self.process: Optional[Process] = None
        self.pid: Optional[int] = None

        self.name: str = name
        self.task_queue: Queue[bytes] = queue
        self.shutdown_event: SyncEvent = shutdown_event
        self.counter: Synchronized = process_counter
        self.state: Synchronized = state
        # TODO: module_name has to be specified some other way,
        # or has to be removed completely
        self.module_name: str = module_name

    def __call__(self):
        """Start the worker"""
        try:
            self.start()
        except Exception as error:
            self.stop()
            self.logger("Startup failed!")
            self.logger(str(error))

    def update_state(self, state: int):
        """Updates a worker state"""
        with self.state.get_lock():
            self.state.value = state

    def logger(self, message: str):
        """For logging messages."""
        print(f"{self.name} [PID: {self.pid}]: {message}")

    def start(self):
        """Start the worker."""
        self.update_state(WorkerState.BOOTING)
        if self.module_name:
            import_module(self.module_name)

        self.pid = os.getpid()
        self.logger(f"starting with PID: {self.pid}")
        # increment the worker counter
        with self.counter.get_lock():
            self.counter.value += 1
        return self.run()

    def flush_tasks(self):
        """
        Removes all the assigned tasks from the worker's task queue.
        """
        while not self.task_queue.empty():
            try:
                self.task_queue.get(block=False)
            except Empty:
                pass

    def join(self, timeout: int | None = None):
        """Wait for the worker process to exit."""
        if self.process:
            self.process.join(timeout=timeout)

    def stop(self):
        """
        Prepare to stop the worker process.
        Flush task queue and sets `shutdown_event`.
        """
        if not self.shutdown_event.is_set():
            self.shutdown_event.set()
            self.flush_tasks()

    def process_task(self, raw_message: bytes):
        """Process the given message. This is where the registered callables are executed."""
        message = Message.deserialize(raw_message)
        self.update_state(WorkerState.BUSY)
        task_callable = TaskRegistry.fetch(message.task_name)
        if not task_callable:
            self.logger(f"Got unregistered task `{message.task_name}`")
            return 1

        try:
            self.logger(f"[{message.message_id}] [{message.task_name}]: Received.")
            _task_result = task_callable(*message.args, **message.kwargs)
        except Exception as error:
            # TODO: change logger
            self.logger(str(error))
            self.logger(f"[{message.message_id}] [{message.task_name}]: Error.")
        else:
            self.logger(f"[{message.message_id}] [{message.task_name}]: Finished.")
        return 0

    def run(self):
        """Implements a worker's execution logic."""
        self.logger(f"worker {self.name} started with pid: {self.pid}")

        # Run the loop until this event is set by master or the worker itself.
        while not self.shutdown_event.is_set():
            try:
                # task will be a message in bytes.
                task = self.task_queue.get(block=False)
            except Empty:
                task = None
            if task:
                self.process_task(task)

            try:
                if not task:
                    time.sleep(DEFAULT_IDLE_TIME)
            except KeyboardInterrupt:
                self.stop()
                self.update_state(WorkerState.SHUTDOWN)
            self.update_state(WorkerState.IDLE)
