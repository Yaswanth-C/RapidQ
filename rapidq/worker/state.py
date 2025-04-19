class WorkerState:
    """
    Simple class for handling worker state.
    """

    BOOTING = "booting"
    IDLE = "idle"
    BUSY = "busy"
    SHUTDOWN = "shutdown"


# TODO: make this configurable between 0.2 and 2.0
DEFAULT_IDLE_TIME = 0.5  # 500ms
