from rapidq.message import Message


class Broker:

    def is_alive(self):
        """Test if broker is alive."""

    def enqueue_message(self, message: Message):
        """Adds a message into the broker client."""

    def fetch_queued(self) -> list:
        """Return the list of pending queued tasks."""

    def fetch_message(self, message_id: str) -> bytes | str:
        """
        fetch the message from the broker using message id.
        Returned data will not be a Message instance.
        Use Message.get_message_from_raw_data for de-serializing.
        """

    def dequeue_message(self, message_id: str) -> bytes | str:
        """
        Remove a message from broker using message_id and return it.
        Returned data will not be a Message instance.
        Use Message.get_message_from_raw_data for de-serializing.
        """

    def flush(self) -> None:
        """Flush the broker."""
