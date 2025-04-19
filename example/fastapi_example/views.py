from .app import app

from .tasks import test_task


@app.get("/")
async def root():
    test_task.in_background()
    return {"message": "Hello World"}
