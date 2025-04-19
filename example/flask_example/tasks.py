import time
from .rapidq_app import rapidq_app


@rapidq_app.background_task("example-task")
def test_task():
    print("test task is running")
    # do something computationally intensive
    time.sleep(5)
    print("task complete")
