import rapidq.startup
from rapidq.decorators import background_task

# define the custom configuration.
rapidq.startup.configure(
    {
        "RAPIDQ_BROKER_SERIALIZER": "pickle",
        "RAPIDQ_BROKER_URL": "redis://localhost:6379",
    }
)


@background_task(name="simple-task")
def test_func(msg):
    print("simple task is running")
    print(msg)


if __name__ == "__main__":
    test_func.in_background(msg="Hello, I'm running")
