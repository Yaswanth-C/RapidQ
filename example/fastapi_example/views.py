from .app import app
from .tasks import test_task


@app.get("/")
async def root():
    test_task.enqueue()
    return {"message": "Hello World"}
