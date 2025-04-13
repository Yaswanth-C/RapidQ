class TaskRegistry:
    """
    Class for registering tasks with name.
    """

    @classmethod
    def register(cls, task):
        if "tasks" not in cls.__dict__:
            cls.tasks = {}
        cls.tasks[task.name] = task.func

    @classmethod
    def fetch(cls, name: str):
        tasks = cls.__dict__.get("tasks", {})
        return tasks.get(name)
