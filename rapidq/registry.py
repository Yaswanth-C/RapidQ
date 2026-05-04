from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from rapidq.decorators import BackGroundTask

FRAMEWORK_LOADERS: set[Callable[..., Any]] = set()

PRE_EXECUTION_HOOKS: set[Callable[..., Any]] = set()
POST_EXECUTION_HOOKS: set[Callable[..., Any]] = set()


class TaskRegistry:
    """
    Class for registering tasks with name.
    """

    @classmethod
    def register(cls, task: BackGroundTask) -> None:
        if "tasks" not in cls.__dict__:
            cls.tasks: dict[str, Callable[..., Any]] = {}
        if task.name in cls.tasks:
            raise RuntimeError(
                f"The name `{task.name}` has already registered for a different callable.\n"
                f"check `{task.func.__module__}.{task.func.__name__}`"
            )
        cls.tasks[task.name] = task.func

    @classmethod
    def fetch(cls, name: str) -> Callable[..., Any] | None:
        tasks: dict[str, Callable[..., Any]] = cls.__dict__.get("tasks", {})
        return tasks.get(name)


def framework_loader(
    loader_callable: Callable[..., Any],
) -> Callable[..., Any]:
    """
    Registers a callable for initializing web frameworks.
    """
    global FRAMEWORK_LOADERS
    FRAMEWORK_LOADERS.add(loader_callable)
    return loader_callable


def pre_execution_hook(
    function: Callable[..., Any],
) -> Callable[..., Any]:
    """
    Registers a pre-execution hook.
    The hooks can be any callable object that takes 3 arguments - `message`,
    `task_name`, and the `worker` itself. Or it should accept **kwargs.
    """
    global PRE_EXECUTION_HOOKS
    PRE_EXECUTION_HOOKS.add(function)
    return function


def post_execution_hook(
    function: Callable[..., Any],
) -> Callable[..., Any]:
    """
    Registers a post-execution hook.
    The hooks can be any callable object that takes 4 arguments - `message`,
    `task_name`, `result` and the `worker` itself. Or it should accept **kwargs.
    """
    global POST_EXECUTION_HOOKS
    POST_EXECUTION_HOOKS.add(function)
    return function
