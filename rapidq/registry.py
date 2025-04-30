class TaskRegistry:
    """
    Class for registering tasks with name.
    """

    @classmethod
    def register(cls, task):
        if "tasks" not in cls.__dict__:
            cls.tasks = {}
        if task.name in cls.tasks:
            raise RuntimeError(
                f"The name `{task.name}` has already registered for a different callable.\n"
                f"check `{task.func.__module__}.{task.func.__name__}`"
            )
        cls.tasks[task.name] = task.func

    @classmethod
    def fetch(cls, name: str):
        tasks = cls.__dict__.get("tasks", {})
        return tasks.get(name)
