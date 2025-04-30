"""
Ensure you have a redis instance running and configured.
Run rapidq in a terminal. Adjust the module depending on your current path.
Adjust workers based on requirement
`rapidq fastapi_example.main -w 2`

Run the fastapi app in another terminal.
`fastapi dev main.py`
"""

from .app import app
from . import views  # for fastapi, to get views.
from . import tasks  # for RapidQ, to register tasks.
