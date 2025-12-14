from typing import Any, Callable

FRAMEWORK_LOADERS: set[Callable] = set()


class TaskRegistry:
    """
    Class for registering tasks with name.
    """

    @classmethod
    def register(cls, task) -> None:
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


def framework_loader(loader_callable: Callable[..., None]):
    """
    Registers a callable for initializing web frameworks.
    """
    global FRAMEWORK_LOADERS
    if loader_callable not in FRAMEWORK_LOADERS:
        FRAMEWORK_LOADERS.add(loader_callable)
    return loader_callable
