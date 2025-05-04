import json
import uuid
import pickle
import os
from rapidq.constants import Serialization, DEFAULT_SERIALIZATION


class Message:
    """
    A class for handling messages.
    """

    def __init__(
        self,
        task_name: str,
        queue_name: str,
        args: tuple,
        kwargs: dict,
        message_id: str = None,
    ):
        self.task_name = task_name
        self.queue_name = queue_name
        self.args = list(args)
        self.kwargs = kwargs
        self.message_id = message_id or str(uuid.uuid4())

    def dict(self):
        return {
            "task_name": self.task_name,
            "queue_name": self.queue_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "message_id": self.message_id,
        }

    def json(self):
        return json.dumps(self.dict())

    def pickle(self):
        return pickle.dumps(self.dict())

    @classmethod
    def from_json(cls, json_str) -> "Message":
        return cls(**json.loads(json_str))

    @classmethod
    def from_pickle_bytes(cls, pickle_bytes) -> "Message":
        return cls(**pickle.loads(pickle_bytes))

    @classmethod
    def get_message_from_raw_data(cls, raw_data) -> "Message":
        serialization = os.environ.get(
            "RAPIDQ_BROKER_SERIALIZER", DEFAULT_SERIALIZATION
        )
        if serialization == Serialization.PICKLE:
            return cls.from_pickle_bytes(raw_data)
        return cls.from_json(raw_data)
