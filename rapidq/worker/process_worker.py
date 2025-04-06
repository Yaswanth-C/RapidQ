import os
import time
from multiprocessing import Process, Queue
from multiprocessing.synchronize import Event as SyncEvent
from multiprocessing.sharedctypes import Synchronized

from queue import Empty


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
    ):
        self.process: Process = None
        self.worker_pid: int = None

        self.name: str = name
        self.assigned_tasks: Queue = queue
        self.event: SyncEvent = event
        self.shutdown_event: SyncEvent = shutdown_event
        self.counter = process_counter

    def __call__(self):
        """
        Start the worker
        """
        self.start()

    def logger(self, message: str):
        """
        For logging messages.
        """
        print(f"{self.name} : {message}")

    def start(self):
        """
        Start the worker.
        """
        self.event.set()
        self.worker_pid = os.getpid()
        self.logger(f"inside {self.name} - {id(self)}")
        self.logger(f"starting - {self.worker_pid}")
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
        This method executes the task (function) given
        """
        # TODO: handle exceptions also
        self.logger("processing task")
        time.sleep(5)  # FIXME: remove
        print(task)
        return 0

    def run(self):
        """
        Implements a worker's execution logic.
        """
        return_code = None
        self.logger(f"worker {self.name} started with pid: {self.worker_pid}")

        # Run the loop until this event is set by master.
        while not self.shutdown_event.is_set():
            self.logger(f"worker {self.name} running ...")
            try:
                task = self.assigned_tasks.get(block=False)
            except Empty:
                task = None
            if task:
                return_code = self.process_task(task)

            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.stop()

            self.logger(f"return code={return_code}")
            # print(f"worker {self.name} idle ...")
            if return_code in (None, 1):
                break

        return return_code
