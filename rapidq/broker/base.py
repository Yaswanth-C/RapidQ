from rapidq.message import Message


class Broker:

    def is_alive(self):
        """Test if broker is alive."""

    def enqueue_message(self, message: Message):
        """Adds a message into the broker client."""

    def fetch_queued(self) -> list:
        """Return the list of pending queued tasks."""

    def fetch_message(self, message_id: str) -> Message:
        """
        fetch the message from the broker using message id.
        """

    def dequeue_message(self, message_id: str) -> Message:
        """
        Remove a message from broker using message id.
        Returns the message being removed.
        """

    def flush(self) -> None:
        """Flush the broker."""
