from typing import Any

from .._base import Event


class BroadcastBackend:
    def __init__(self, url: str) -> None:
        raise NotImplementedError()

    async def connect(self) -> None:
        raise NotImplementedError()

    async def disconnect(self) -> None:
        raise NotImplementedError()

    async def subscribe(self, channel: str) -> None:
        raise NotImplementedError()

    async def unsubscribe(self, channel: str) -> None:
        raise NotImplementedError()

    async def publish(self, channel: str, message: Any) -> None:
        raise NotImplementedError()

    async def next_published(self) -> Event:
        raise NotImplementedError()
