import os
import redis
from rapidq.message import Message
from rapidq.constants import Serialization, DEFAULT_SERIALIZATION
from .base import Broker


class RedisBroker(Broker):
    """
    A Broker that uses Redis.
    """

    MESSAGE_PREFIX = "rapidq.message|"
    TASK_KEY = "rapidq.queued_tasks"
    DEFAULT_URL = "redis://localhost:6379/0"
    BATCH_SIZE = 100

    def __init__(self, connection_params: dict = None):
        if not connection_params:
            connection_params = {}

        serialization = os.environ.get(
            "RAPIDQ_BROKER_SERIALIZER", DEFAULT_SERIALIZATION
        )
        if serialization not in (Serialization.PICKLE, Serialization.JSON):
            raise RuntimeError(
                f"serialization must be in ({Serialization.PICKLE, Serialization.JSON})"
            )
        self.serialization = serialization

        connection_params.setdefault(
            "url", os.environ.get("RAPIDQ_BROKER_URL", self.DEFAULT_URL)
        )
        self.client = redis.Redis.from_url(**connection_params)

    def is_alive(self) -> bool:
        try:
            self.client.ping()
            return True
        except redis.ConnectionError:
            return False

    def generate_message_key(self, message_id: str) -> str:
        return f"{self.MESSAGE_PREFIX}{message_id}"

    def enqueue_message(self, message: Message) -> None:
        key = self.generate_message_key(message.message_id)
        _data = (
            message.pickle()
            if self.serialization == Serialization.PICKLE
            else message.json()
        )
        self.client.set(key, _data)
        # This below Redis set will be monitored by master.
        self.client.rpush(self.TASK_KEY, message.message_id)

    def fetch_queued(self) -> list:
        return list(self.client.lrange(self.TASK_KEY, 0, self.BATCH_SIZE))

    def fetch_message(self, message_id: str) -> bytes | str:
        key = self.generate_message_key(message_id)
        return self.client.get(key)

    def dequeue_message(self, message_id: str) -> bytes | str:
        key = self.generate_message_key(message_id)
        message = self.fetch_message(message_id)
        self.client.delete(key)
        self.client.lrem(self.TASK_KEY, 0, message_id)
        return message

    def flush(self) -> None:
        pattern = "rapidq*"
        pipe = self.client.pipeline()
        for key in self.client.scan_iter(match=pattern):
            pipe.delete(key)
        pipe.execute()
