import json
import pickle

from rapidq.constants import Serialization
from rapidq.message import Message, MessageType, MessageTypeRegistry


@MessageTypeRegistry.register
class JsonMessage(MessageType):
    msg_type: str = Serialization.JSON

    @staticmethod
    def serialize(message: Message) -> str:
        return json.dumps(message.dict())

    @staticmethod
    def deserialize(message_data: bytes) -> Message:
        return Message(**json.loads(message_data))


@MessageTypeRegistry.register
class PickleMessage(MessageType):
    msg_type: str = Serialization.PICKLE

    @staticmethod
    def serialize(message: Message) -> bytes:
        return pickle.dumps(message.dict())

    @staticmethod
    def deserialize(message_data: bytes) -> Message:
        return Message(**pickle.loads(message_data))
