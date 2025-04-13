import importlib
import os
import sys
import time

from multiprocessing import Process, Queue
from multiprocessing.synchronize import Event as SyncEvent
from multiprocessing.sharedctypes import Synchronized
from queue import Empty
from rapidq.registry import TaskRegistry

from .state import WorkerState


def import_module(module_name):
    current_path = os.getcwd()
    if current_path not in sys.path:
        sys.path.append(current_path)
    _module = importlib.import_module(module_name)
    return _module


class Worker:
    """
    Class that handles a single worker process.
    """

    def __init__(
        self,
        queue: Queue,
        event: SyncEvent,
        name: str,
        shutdown_event: SyncEvent,
        process_counter: Synchronized,
        worker_state: dict,
        module_name: str,
    ):
        self.process: Process = None
        self.worker_pid: int = None

        self.name: str = name
        self.assigned_tasks: Queue = queue
        self.event: SyncEvent = event
        self.shutdown_event: SyncEvent = shutdown_event
        self.counter = process_counter
        self.worker_state = worker_state
        self.module_name = module_name

    def __call__(self):
        """
        Start the worker
        """
        self.start()

    def update_state(self, state: str):
        self.worker_state[self.name] = state

    def logger(self, message: str):
        """
        For logging messages.
        """
        print(f"{self.name} : {message}")

    def start(self):
        """
        Start the worker.
        """
        if self.module_name:
            import_module(self.module_name)
        self.event.set()
        self.worker_pid = os.getpid()
        self.update_state(WorkerState.BOOTING)

        self.logger(f"starting with PID - {self.worker_pid}")
        # increment the worker counter
        self.counter.value += 1
        return self.run()

    def flush_tasks(self):
        """
        Removes all the assigned tasks from the worker's task queue.
        """
        while not self.assigned_tasks.empty():
            try:
                self.assigned_tasks.get(block=False)
            except Empty:
                pass

    def join(self, timeout: int = None):
        """
        Wait for the worker process to exit.
        """
        self.process.join(timeout=timeout)

    def stop(self):
        """
        Prepare to stop the worker process.
        Flush task queue and sets `shutdown_event`.
        """
        if not self.shutdown_event.is_set():
            self.logger(
                f"worker {self.name} {self.process.pid} is preparing to shutdown"
            )
            self.shutdown_event.set()
            self.flush_tasks()

    def process_task(self, task):
        """
        Processes the given task.
        This method processes the task given
        """
        # TODO: handle exceptions also
        self.update_state(WorkerState.BUSY)
        task_callable = TaskRegistry.fetch(task.task_name)
        if not task_callable:
            self.logger(f"Got unregistered task `{task.task_name}`")
            return 1
        self.logger(task_callable)
        task_result = task_callable(*task.args, **task.kwargs)
        return 0

    def run(self):
        """
        Implements a worker's execution logic.
        """
        return_code = None
        self.logger(f"worker {self.name} started with pid: {self.worker_pid}")

        # Run the loop until this event is set by master.
        while not self.shutdown_event.is_set():
            try:
                task = self.assigned_tasks.get(block=False)
            except Empty:
                task = None
            if task:
                return_code = self.process_task(task)

            try:
                if not task:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.stop()
                self.update_state(WorkerState.SHUTDOWN)
            self.update_state(WorkerState.IDLE)

        return return_code
