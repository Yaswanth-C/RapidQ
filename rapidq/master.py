import time
from typing import Dict
from multiprocessing import Process, Queue, Event, Value, Manager

from rapidq.broker import get_broker_class
from rapidq.worker.process_worker import Worker
from rapidq.worker.state import WorkerState


class MasterProcess:
    """
    Handles the workers and allocates tasks to them.
    """

    def __init__(self, workers: int, module_name: str):
        self.no_of_workers = workers
        self.process_counter = Value("i", 0)
        self.workers: Dict[str, Worker] = {}

        broker_class = get_broker_class()
        self.broker = broker_class()

        self.module_name = module_name

        manager = Manager()
        # maps worker_pid -> state
        self.worker_state = manager.dict()
        self.boot_complete = False

    def start_workers(self):
        for _worker in self.workers.values():
            _worker.process.start()

    def logger(self, message: str):
        print(f"Master-process: {message}")

    def _create_worker(self, worker_num: int):
        """
        Create and return a single worker process.
        """
        worker_queue = Queue()
        worker_event = Event()
        shutdown_event = Event()
        process_name = f"Worker Process-{worker_num}"
        worker = Worker(
            queue=worker_queue,
            event=worker_event,
            name=process_name,
            shutdown_event=shutdown_event,
            process_counter=self.process_counter,
            worker_state=self.worker_state,
            module_name=self.module_name,
        )
        self.logger(f"{worker.name} {id(worker)}")
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
        for worker in self.workers.values():
            self.logger(
                f"waiting for {worker.process.name} - {worker.process.pid} to exit !"
            )
            worker.stop()
            if worker.process.is_alive():
                worker.logger(f"alive, killing {worker.process.pid} - {worker.name}")
                worker.process.terminate()
            worker.join(1)
        print("shutting down master")


def main_process(workers: int, module_name: str):
    master = MasterProcess(workers=workers, module_name=module_name)
    master.create_workers()
    master.start_workers()

    while True:
        try:
            # wait for all the workers to boot up.
            if master.process_counter.value == workers:
                break
            master.logger("waiting for workers to boot up")
            time.sleep(1)
        except KeyboardInterrupt:
            master.shutdown()

    master.boot_complete = True

    while True:
        try:
            master.logger(master.worker_state)

            for worker_name, _state in master.idle_workers:
                pending_message_ids = master.queued_tasks
                if not pending_message_ids:
                    break

                worker = master.workers[worker_name]
                # assign the task to the idle worker
                worker.assigned_tasks.put(
                    master.broker.dequeue_message(
                        message_id=pending_message_ids.pop(0).decode()
                    )
                )
            time.sleep(1)
        except (KeyboardInterrupt, Exception) as error:
            print(error)
            master.shutdown()
            time.sleep(2)
            break
