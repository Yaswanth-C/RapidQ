from rapidq import RapidQ

app = RapidQ()


@app.background_task(name="simple-task")
def test_func(msg):
    # of course this could do more than just print.
    print("simple task is running")
    print(msg)


if __name__ == "__main__":
    test_func.in_background(msg="Hello, I'm running in background")
    # Line below will be printed directly and will not go to worker.
    test_func(msg="Hello, I'm running in the same process!")
