class Serialization:
    PICKLE = "pickle"
    JSON = "json"


DEFAULT_SERIALIZATION = Serialization.PICKLE


class WorkerState:
    BOOTING = 0
    IDLE = 1
    BUSY = 2
    SHUTDOWN = 3


DEFAULT_IDLE_TIME = 0.5  # 500ms
