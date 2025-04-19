import time
import sys
import os
from typing import Dict
from multiprocessing import Process, Queue, Event, Value, Manager
from multiprocessing.managers import SyncManager, DictProxy

from rapidq.broker import get_broker, Broker
from rapidq.worker.process_worker import Worker
from rapidq.worker.state import WorkerState, DEFAULT_IDLE_TIME


class MasterProcess:
    """
    Handles the workers and allocates tasks to them.
    """

    def __init__(self, workers: int, module_name: str):
        self.no_of_workers = workers
        self.process_counter = Value("i", 0)
        self.workers: Dict[str, Worker] = {}
        self.pid: int = os.getpid()
        self.broker: Broker = get_broker()
        self.module_name: str = module_name

        manager: SyncManager = Manager()
        # maps worker_name -> state
        self.worker_state: DictProxy[str, str] = manager.dict()
        self.boot_complete: bool = False

    def start_workers(self):
        for _worker in self.workers.values():
            _worker.process.start()

    def logger(self, message: str):
        print(f"Master: [PID: {self.pid}] {message}")

    def _create_worker(self, worker_num: int):
        """
        Create and return a single worker process.
        """
        worker_queue = Queue()
        shutdown_event = Event()
        process_name = f"Worker-{worker_num}"
        worker = Worker(
            queue=worker_queue,
            name=process_name,
            shutdown_event=shutdown_event,
            process_counter=self.process_counter,
            worker_state=self.worker_state,
            module_name=self.module_name,
        )

        # NOTE: I am well aware of the state duplication when the process is started
        process = Process(
            target=worker,
            name=process_name,
            daemon=False,
        )
        worker.process = process
        return worker

    def create_workers(self):
        """
        Creates the worker processes.
        """
        for worker_num in range(self.no_of_workers):
            worker = self._create_worker(worker_num)
            self.add_worker(worker=worker)

    def add_worker(self, worker: Worker):
        # we cant get the pid before it is started, so use name.
        self.workers[worker.name] = worker

    @property
    def queued_tasks(self):
        """
        Returns the queued messages
        """
        return self.broker.fetch_queued()

    @property
    def idle_workers(self):
        """
        Returns the workers in idle state.
        """
        if not self.boot_complete:
            return []

        # [(worker_name, state), ...]
        return filter(
            lambda item: item[1] == WorkerState.IDLE, self.worker_state.items()
        )

    def shutdown(self):
        self.logger("Preparing to shutdown ...")
        for worker in self.workers.values():
            self.logger(
                f"waiting for {worker.process.name} - PID: {worker.process.pid} to exit!"
            )
            worker.stop()
            if worker.process.is_alive():
                self.logger(f"worker alive, killing. PID: {worker.process.pid}")
                worker.process.terminate()
            worker.join(1)
        self.logger("shutting down master")


def main_process(workers: int, module_name: str):
    master = MasterProcess(workers=workers, module_name=module_name)
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
            master.logger("waiting for workers to boot up")
            time.sleep(1)

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
            for worker_name, _state in master.idle_workers:
                pending_message_ids = master.queued_tasks
                if not pending_message_ids:
                    break

                worker = master.workers[worker_name]
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
