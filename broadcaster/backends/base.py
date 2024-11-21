from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from .._event import Event


class BroadcastBackend(ABC):
    @abstractmethod
    def __init__(self, url: str) -> None: ...

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def subscribe(self, channel: str) -> None: ...

    @abstractmethod
    async def unsubscribe(self, channel: str) -> None: ...

    @abstractmethod
    async def publish(self, channel: str, message: Any) -> None: ...

    @abstractmethod
    async def next_published(self) -> Event: ...


class BroadcastCacheBackend(BroadcastBackend):
    _ready: asyncio.Event

    @abstractmethod
    async def get_current_channel_id(self, channel: str) -> str | bytes | memoryview | int: ...

    @abstractmethod
    async def get_history_messages(
        self,
        channel: str,
        msg_id: int | bytes | str | memoryview,
        count: int | None = None,
    ) -> list[Event]: ...
