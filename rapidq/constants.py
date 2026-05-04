from multiprocessing import cpu_count


class Serialization:
    PICKLE: str = "pickle"
    JSON: str = "json"


class WorkerState:
    BOOTING: int = 0
    IDLE: int = 1
    BUSY: int = 2
    SHUTDOWN: int = 3


DEFAULT_SERIALIZATION: str = Serialization.PICKLE
DEFAULT_QUEUE_NAME: str = "default"
DEFAULT_IDLE_TIME: float = 0.5  # 500ms
DEFAULT_AUTO_DISCOVER_MODULES: tuple = ("tasks",)

LOGGING_FMT = (
    "%(asctime)s | %(levelname)-8s | %(name)-16s [PID:%(process)-5d] -> %(message)s\n"
)
LOGGING_FMT_TIME = "%a %d %b %Y %H:%M:%S.%6f"

CPU_COUNT: int = min(4, cpu_count())
