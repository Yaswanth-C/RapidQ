import json
import os
import pickle
import uuid
from typing import Any

from rapidq.constants import DEFAULT_SERIALIZATION, Serialization


class Message:
    """
    A class for handling messages.
    """

    def __init__(
        self,
        task_name: str,
        queue_name: str,
        args: tuple,
        kwargs: dict[str, Any],
        message_id: str | None = None,
    ) -> None:
        self.task_name: str = task_name
        self.queue_name: str = queue_name
        self.args: list[Any] = list(args)
        self.kwargs: dict[str, Any] = kwargs
        self.message_id: str = message_id or str(uuid.uuid4())

    def dict(self) -> dict[str, Any]:
        return {
            "task_name": self.task_name,
            "queue_name": self.queue_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "message_id": self.message_id,
        }

    def json(self) -> str:
        return json.dumps(self.dict())

    def pickle(self) -> bytes:
        return pickle.dumps(self.dict())

    @classmethod
    def from_json(cls, json_data: bytes) -> "Message":
        return cls(**json.loads(json_data))

    @classmethod
    def from_pickle_bytes(cls, pickle_data: bytes) -> "Message":
        return cls(**pickle.loads(pickle_data))

    @classmethod
    def get_message_from_raw_data(cls, raw_data: bytes) -> "Message":
        serialization = os.environ.get(
            "RAPIDQ_BROKER_SERIALIZER", DEFAULT_SERIALIZATION
        )
        deserializer_callable = DE_SERIALIZER_MAP[serialization]
        return deserializer_callable(raw_data)


SERIALIZER_MAP = {
    Serialization.JSON: Message.json,
    Serialization.PICKLE: Message.pickle,
}

DE_SERIALIZER_MAP = {
    Serialization.JSON: Message.from_json,
    Serialization.PICKLE: Message.from_pickle_bytes,
}
