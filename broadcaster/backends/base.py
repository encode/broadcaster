from __future__ import annotations

from typing import Any, AsyncGenerator

from .._event import Event


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


class BroadcastCacheBackend(BroadcastBackend):
    async def get_current_channel_id(self, channel: str):
        raise NotImplementedError()

    async def get_history_messages(
        self,
        channel: str,
        msg_id: int | bytes | str | memoryview,
        count: int | None = None,
    ) -> AsyncGenerator[Event, None]:
        raise NotImplementedError()
