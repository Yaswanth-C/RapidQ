from rapidq import RapidQ

rapidq_app = RapidQ()
# below line can be omitted if custom config is not needed.
rapidq_app.config_from_module("fastapi_example.rapidq_config")
