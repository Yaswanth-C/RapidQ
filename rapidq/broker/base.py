from abc import ABC, abstractmethod

from rapidq.message import Message


class Broker(ABC):

    @abstractmethod
    def is_alive(self) -> bool:
        """Test if broker is alive."""

    @abstractmethod
    def enqueue_message(self, message: Message) -> None:
        """Adds a message into the broker client."""

    @abstractmethod
    def fetch_queued(self) -> list[bytes]:
        """Return the list of pending queued tasks (message ids)."""

    @abstractmethod
    def fetch_message(self, message_id: str) -> bytes:
        """
        Fetch the raw message from the broker using message id.
        Returned data will not be a Message instance.
        Use `Message.deserialize` for de-serializing.
        """

    @abstractmethod
    def dequeue_message(self, message_id: str) -> bytes:
        """
        Remove a message from broker using `message_id` and return it.
        Returned data will not be a Message instance.
        """

    @abstractmethod
    def flush(self) -> None:
        """Flush the broker."""
