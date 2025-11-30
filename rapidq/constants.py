from multiprocessing import cpu_count


class Serialization:
    PICKLE: str = "pickle"
    JSON: str = "json"


class WorkerState:
    BOOTING = 0
    IDLE = 1
    BUSY = 2
    SHUTDOWN = 3


DEFAULT_SERIALIZATION: str = Serialization.PICKLE
DEFAULT_QUEUE_NAME = "default"
DEFAULT_IDLE_TIME = 0.5  # 500ms

CPU_COUNT: int = min(4, cpu_count())
