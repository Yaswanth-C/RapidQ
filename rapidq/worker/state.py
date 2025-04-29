class WorkerState:
    """
    Simple class for handling worker state.
    """

    BOOTING = 0
    IDLE = 1
    BUSY = 2
    SHUTDOWN = 3


# TODO: make this configurable between 0.2 and 2.0
DEFAULT_IDLE_TIME = 0.5  # 500ms
