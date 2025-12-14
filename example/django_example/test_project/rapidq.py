import os

from rapidq import RapidQ

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "test_project.settings")
app = RapidQ()
