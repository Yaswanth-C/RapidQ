from .app import app
from .tasks import test_task


@app.route("/")
def hello_world():
    test_task.delay()
    return "<p>Hello, World!</p>"
