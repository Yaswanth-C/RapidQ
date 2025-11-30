import os
from typing import Any, cast

from redis import ConnectionError, Redis

from rapidq.broker.base import Broker
from rapidq.constants import DEFAULT_SERIALIZATION
from rapidq.message import SERIALIZER_MAP, Message


class RedisBroker(Broker):
    """
    A Broker that uses Redis.
    """

    MESSAGE_PREFIX = "rapidq.message|"
    TASK_KEY = "rapidq.queued_tasks"
    DEFAULT_URL = "redis://localhost:6379/0"
    BATCH_SIZE = 100

    def __init__(self, connection_params: dict[str, Any] | None = None) -> None:
        if not connection_params:
            connection_params: dict[str, Any] = {}

        serialization = os.environ.get(
            "RAPIDQ_BROKER_SERIALIZER", DEFAULT_SERIALIZATION
        )
        if serialization not in SERIALIZER_MAP:
            raise RuntimeError(f"serialization must be in {list(SERIALIZER_MAP)}")
        self.serialization = serialization

        connection_params.setdefault(
            "url", os.environ.get("RAPIDQ_BROKER_URL", self.DEFAULT_URL)
        )
        self.client = Redis.from_url(**connection_params)

    def is_alive(self) -> bool:
        try:
            self.client.ping()
            return True
        except ConnectionError:
            return False

    def generate_message_key(self, message_id: str) -> str:
        return f"{self.MESSAGE_PREFIX}{message_id}"

    def enqueue_message(self, message: Message) -> None:
        key = self.generate_message_key(message.message_id)
        serializer_callable = SERIALIZER_MAP[self.serialization]
        data = serializer_callable(message)
        self.client.set(key, data)
        # This below Redis set will be monitored by master.
        self.client.rpush(self.TASK_KEY, message.message_id)

    def fetch_queued(self) -> list[bytes]:
        return cast(list[bytes], self.client.lrange(self.TASK_KEY, 0, self.BATCH_SIZE))

    def fetch_message(self, message_id: str) -> bytes:
        key = self.generate_message_key(message_id)
        return cast(bytes, self.client.get(key))

    def dequeue_message(self, message_id: str) -> bytes:
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
