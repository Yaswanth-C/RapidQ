import os
from dataclasses import dataclass

from rapidq.constants import DEFAULT_SERIALIZATION


@dataclass(slots=True)
class QueueConfig:
    broker_url: str = os.environ.get("RAPIDQ_BROKER_URL", "redis://localhost:6379/0")
    broker_serializer: str = os.environ.get(
        "RAPIDQ_BROKER_SERIALIZER", DEFAULT_SERIALIZATION
    )


settings = QueueConfig()


def load_config_from_module(module) -> None:
    if hasattr(module, "RAPIDQ_BROKER_URL"):
        url = str(getattr(module, "RAPIDQ_BROKER_URL"))
        settings.broker_url = url

    if hasattr(module, "RAPIDQ_BROKER_SERIALIZER"):
        serializer = str(getattr(module, "RAPIDQ_BROKER_SERIALIZER"))
        settings.broker_serializer = serializer
