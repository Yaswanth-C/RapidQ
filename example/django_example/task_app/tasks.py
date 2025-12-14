from test_project import app


@app.task(name="simple_task")
def simple_task():
    print("This is a simple task executed by RapidQ.")
