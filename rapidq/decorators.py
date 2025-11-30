from functools import wraps
from typing import Any, Callable

from rapidq.broker import Broker, get_broker
from rapidq.constants import DEFAULT_QUEUE_NAME
from rapidq.message import Message
from rapidq.registry import TaskRegistry


class BackGroundTask:
    def __init__(
        self,
        func: Callable[..., Any],
        args: tuple,
        kwargs: dict[str, Any],
        name: str,
        broker: Broker,
    ) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        func_name = getattr(func, "__name__", None)
        self.name = name or f"{func.__module__}.{func_name}"
        self.broker = broker
        # registers the task for calling later via name.
        TaskRegistry.register(self)

    def __call__(self, *args, **kwargs) -> Any:
        return self.func(*args, **kwargs)

    def in_background(self, *args, **kwargs) -> Message:
        """Queue the task for processing later."""
        message = Message(
            task_name=self.name,
            queue_name=DEFAULT_QUEUE_NAME,
            args=args,
            kwargs=kwargs,
        )
        self.broker.enqueue_message(message)
        return message


def background_task(name: str) -> Callable[[Callable[..., Any]], BackGroundTask]:
    """Decorator for callables to be registered as task."""

    def decorator(func) -> BackGroundTask:
        if not name:
            raise RuntimeError(
                f"You must provide a valid name for the task: {func.__module__}.{func.__name__}"
            )

        @wraps(func)
        def wrapped_func(*args, **kwargs) -> BackGroundTask:
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
