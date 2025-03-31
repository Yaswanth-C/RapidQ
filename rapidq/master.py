import time
from typing import List
from multiprocessing import Process, Queue, Event, Value

from rapidq.worker.base import Worker


class MasterProcess:
    """
    Handles the workers and allocates tasks to them.
    """

    def __init__(self, workers: int):
        self.no_of_workers = workers
        self.process_counter = Value("i", 0)
        self.workers: List[Worker] = []

    def start_workers(self):
        for _worker in self.workers:
            self.logger(f"start ============================= {_worker.name}")
            self.logger(_worker.assigned_tasks)
            _worker.process.start()

    def logger(self, message: str):
        print(f"Master-process: {message}")

    def create_workers(self):
        for worker_num in range(self.no_of_workers):
            worker_queue = Queue()
            worker_event = Event()
            shutdown_event = Event()
            worker_queue.put(1)
            process_name = f"Worker Process-{worker_num}"
            worker = Worker(
                queue=worker_queue,
                event=worker_event,
                name=process_name,
                shutdown_event=shutdown_event,
                process_counter=self.process_counter,
            )
            self.logger(f"{worker.name} {id(worker)}")
            # NOTE: I am well aware that there will be state duplication when the process is started
            process = Process(
                target=worker,
                name=process_name,
                daemon=False,
            )
            worker.process = process
            self.add_worker(worker=worker)

    def add_worker(self, worker: Worker):
        # TODO: add some other logic to map worker details
        self.workers.append(worker)

    def shutdown(self):
        for worker in self.workers:
            self.logger(
                f"waiting for {worker.process.name} - {worker.process.pid} to exit !"
            )
            worker.stop()
            if worker.process.is_alive():
                worker.logger(f"alive, killing {worker.process.pid} - {worker.name}")
                worker.process.terminate()
            worker.join(1)
        print("shutting down master")


def main_process(workers: int):
    master = MasterProcess(workers=workers)
    master.create_workers()
    master.start_workers()
    print("counter")
    print(master.process_counter.value)

    while True:
        try:
            # wait for all the workers to boot up.
            if master.process_counter.value == workers:
                break
            master.logger("waiting for workers to boot up")
            time.sleep(1)
        except KeyboardInterrupt:
            master.shutdown()

    # FIXME: modify the loop to make the master check for worker idle conditions and assigning tasks to workers.
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print("keyboard interrupt received ...")
            master.shutdown()
            time.sleep(3)
            break
