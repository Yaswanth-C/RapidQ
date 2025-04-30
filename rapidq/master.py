import time
import sys
import os
from typing import Dict
from multiprocessing import Process, Queue, Event, Value

from rapidq.broker import get_broker, Broker
from rapidq.decorators import background_task as task_decorator
from rapidq.utils import import_module
from rapidq.worker.process_worker import Worker
from rapidq.worker.state import WorkerState, DEFAULT_IDLE_TIME


class RapidQ:
    """
    Master application.
    Handles the workers, broker and allocates tasks to workers
    based on their availability.
    """

    def __init__(
        self, workers: int = None, module_name: str = None, init_as_app: bool = False
    ):
        self.no_of_workers = workers
        self.module_name: str = module_name
        self.boot_complete: bool = False
        if init_as_app:
            if not all([self.no_of_workers, self.module_name]):
                raise RuntimeError("Arguments are improper unable to start RapidQ.")
            self.initialize()

    def initialize(self):
        self.process_counter = Value("i", 0)
        self.workers: Dict[str, Worker] = {}
        self.pid: int = os.getpid()
        self.broker: Broker = get_broker()

    def config_from_module(self, module_path: str):
        module = import_module(module_path)

        configurable_keys = (
            "RAPIDQ_BROKER_SERIALIZER",
            "RAPIDQ_BROKER_URL",
        )
        for key in configurable_keys:
            if not getattr(module, key, None):
                continue
            os.environ[key] = str(getattr(module, key))

    def background_task(self, name: str):
        """Decorator for callables to be registered as task."""
        return task_decorator(name)

    def start_workers(self):
        for _worker in self.workers.values():
            _worker.process.start()

    def logger(self, message: str):
        print(f"Master: [PID: {self.pid}] {message}")

    def _create_worker(self, worker_num: int):
        """Create and return a single Worker instance."""
        worker_queue = Queue()
        shutdown_event = Event()
        worker_state = Value("i", 0)
        process_name = f"Worker-{worker_num}"
        worker = Worker(
            queue=worker_queue,
            name=process_name,
            shutdown_event=shutdown_event,
            process_counter=self.process_counter,
            state=worker_state,
            module_name=self.module_name,
        )

        # NOTE: I am well aware of the state duplication when the process is started
        # That's why most of the instance variables in Worker use shared memory resources.
        process = Process(
            target=worker,
            name=process_name,
            daemon=False,
        )
        worker.process = process
        return worker

    def create_workers(self):
        """Creates the worker processes."""
        for worker_num in range(self.no_of_workers):
            worker = self._create_worker(worker_num)
            self.add_worker(worker=worker)

    def add_worker(self, worker: Worker):
        # we cant get the pid before it is started, so use name.
        self.workers[worker.name] = worker

    @property
    def queued_tasks(self):
        """Returns the queued messages"""
        return self.broker.fetch_queued()

    @property
    def idle_workers(self):
        """Returns the workers in idle state."""
        if not self.boot_complete:
            return []

        return filter(
            lambda _worker: _worker.state.value == WorkerState.IDLE,
            self.workers.values(),
        )

    def shutdown(self):
        self.logger("Preparing to shutdown ...")
        for worker in self.workers.values():
            try:
                self.logger(
                    f"Waiting for {worker.process.name} - PID: {worker.process.pid} to exit!"
                )
                worker.stop()
                worker.join(timeout=5)

                if worker.process.is_alive():
                    self.logger(
                        f"Worker still alive, forcefully killing. PID: {worker.process.pid}"
                    )
                    worker.process.terminate()
                    worker.join(timeout=1)
            except Exception as error:
                self.logger(
                    f"Error while shutting down worker {worker.process.name}: {error}"
                )

        self.logger("Shutting down master")


def main_process(workers: int, module_name: str):
    master = RapidQ(workers=workers, module_name=module_name, init_as_app=True)
    if not master.broker.is_alive():
        master.logger("Error: unable to access broker, shutting down.")
        master.shutdown()
        sys.exit(1)

    master.create_workers()
    master.start_workers()

    while True:
        try:
            # wait for all the workers to boot up.
            if master.process_counter.value == workers:
                break
            master.logger("waiting for workers to boot up...")
            time.sleep(2)

            # check for any abnormal shutdown event.
            if list(master.workers.values())[0].shutdown_event.is_set():
                # worker didn't boot, there is something wrong with the setup.
                master.shutdown()
                sys.exit(1)
        except KeyboardInterrupt:
            master.shutdown()
            sys.exit(1)

    master.boot_complete = True

    while True:
        # loop through idle workers and assign tasks.
        try:
            for worker in master.idle_workers:
                pending_message_ids = master.queued_tasks
                if not pending_message_ids:
                    break

                try:
                    message_id = pending_message_ids.pop(0).decode()
                    message = master.broker.dequeue_message(message_id=message_id)
                except UnicodeDecodeError as error:
                    master.logger(
                        f"Unable to decode message! message_id: {message_id} \n"
                        "You have configured different serialization strategies"
                        " for RapidQ and your project.\nCheck configuration."
                        "You will have to restart the workers with correct serialization. Or flush the broker."
                    )
                    raise error

                # assign the task to the idle worker
                master.logger(
                    f"assigning [{message_id}] [{message.task_name}] to {worker.name}"
                )
                worker.task_queue.put(message)
            time.sleep(DEFAULT_IDLE_TIME)
        except (KeyboardInterrupt, Exception) as error:
            print(error)
            master.shutdown()
            sys.exit(1)
