from functools import wraps
from typing import Callable

from rapidq.broker import get_broker, Broker
from rapidq.message import Message
from rapidq.registry import TaskRegistry

DEFAULT_QUEUE_NAME = "default"


class BackGroundTask:
    def __init__(
        self, func: Callable, args: tuple, kwargs: dict, name: str, broker: Broker
    ):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.name = name or f"{func.__module__}.{func.__name__}"
        self.broker = broker
        # registers the task for calling later via name.
        TaskRegistry.register(self)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def in_background(self, *args, **kwargs):
        """Queue the task for processing later."""
        message = Message(
            task_name=self.name,
            queue_name=DEFAULT_QUEUE_NAME,
            args=args,
            kwargs=kwargs,
        )
        self.broker.enqueue_message(message)
        return message


def background_task(name: str):
    """Decorator for callables to be registered as task."""

    def decorator(func):
        if not name:
            raise RuntimeError(
                f"You must provide a valid name for the task: {func.__module__}.{func.__name__}"
            )

        @wraps(func)
        def wrapped_func(*args, **kwargs):
            broker = get_broker()
            return BackGroundTask(
                func=func,
                args=args,
                kwargs=kwargs,
                name=name,
                broker=broker,
            )

        return wrapped_func(func)

    return decorator
