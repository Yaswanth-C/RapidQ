from typing import Type
from .base import Broker
from .redis_broker import RedisBroker


def get_broker_class() -> Type[Broker]:
    # TODO: update when extra brokers are added.
    return RedisBroker


broker_instance = None


def get_broker() -> Broker:
    global broker_instance
    if broker_instance is None:
        broker_class = get_broker_class()
        broker_instance = broker_class()
    return broker_instance
