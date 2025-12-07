import json
import os
import pickle
import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar, Type, TypeVar

from rapidq.constants import DEFAULT_SERIALIZATION, Serialization

MsgRegistryT = TypeVar("MsgRegistryT", bound="MessageTypeRegistry")
MsgT = TypeVar("MsgT", bound="Message")


class MessageType(ABC):
    msg_type: ClassVar[str]

    @staticmethod
    @abstractmethod
    def serialize(message: "Message") -> bytes | str: ...

    @staticmethod
    @abstractmethod
    def deserialize(message_data: bytes) -> "Message": ...


class MessageTypeRegistry:

    message_types: ClassVar[dict[str, Type[MessageType]]] = {}

    @classmethod
    def register(
        cls: Type[MsgRegistryT], msg_class: Type[MessageType]
    ) -> Type[MessageType]:
        cls.message_types[msg_class.msg_type] = msg_class
        return msg_class

    @classmethod
    def fetch(cls: Type[MsgRegistryT], msg_type: str) -> Type[MessageType]:
        return cls.message_types[msg_type]


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

    @classmethod
    def _get_serializer(cls: Type[MsgT]) -> Type[MessageType]:
        """Get the configured serializer based on environment settings."""
        serialization = os.environ.get(
            "RAPIDQ_BROKER_SERIALIZER", DEFAULT_SERIALIZATION
        )
        return MessageTypeRegistry.fetch(msg_type=serialization)

    @classmethod
    def serialize(cls: Type[MsgT], message: "Message") -> bytes | str:
        return cls._get_serializer().serialize(message)

    @classmethod
    def deserialize(cls: Type[MsgT], message_data: bytes) -> "Message":
        return cls._get_serializer().deserialize(message_data)
