"""
Ensure you have a redis instance running and configured.
Run rapidq in a terminal. Adjust the module depending on your current path.
Adjust workers based on requirement
`rapidq flask_example.main -w 2`

Run the flask app in another terminal.
`flask --app main run`
"""

from . import views
from . import tasks
from .app import app

if __name__ == "__main__":
    app.run()
