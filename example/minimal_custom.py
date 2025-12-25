from rapidq import RapidQ

app = RapidQ()

# define the custom configuration. Below line can be omitted if configuration is not needed.
app.config_from_module("example.config_example")


@app.task(name="simple-task")
def test_func(msg):
    print("simple task is running")
    print(msg)


if __name__ == "__main__":
    test_func.enqueue(msg="Hello, I'm running")
