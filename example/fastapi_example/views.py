from .app import app
from .tasks import test_task


@app.get("/")
async def root():
    test_task.delay()
    return {"message": "Hello World"}
