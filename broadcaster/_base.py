from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, AsyncIterator, cast
from urllib.parse import urlparse

from broadcaster.backends.base import BroadcastCacheBackend

from ._event import Event

if TYPE_CHECKING:  # pragma: no cover
    from broadcaster.backends.base import BroadcastBackend


class Unsubscribed(Exception):
    pass


class Broadcast:
    def __init__(self, url: str | None = None, *, backend: BroadcastBackend | None = None) -> None:
        assert url or backend, "Either `url` or `backend` must be provided."
        self._backend = backend or self._create_backend(cast(str, url))
        self._subscribers: dict[str, set[asyncio.Queue[Event | None]]] = {}

    def _create_backend(self, url: str) -> BroadcastBackend:
        parsed_url = urlparse(url)
        if parsed_url.scheme in ("redis", "rediss"):
            from broadcaster.backends.redis import RedisBackend

            return RedisBackend(url)

        elif parsed_url.scheme == "redis-stream":
            from broadcaster.backends.redis import RedisStreamBackend

            return RedisStreamBackend(url)

        elif parsed_url.scheme == "redis-stream-cached":
            from broadcaster.backends.redis import RedisStreamCachedBackend

            return RedisStreamCachedBackend(url)

        elif parsed_url.scheme in ("postgres", "postgresql"):
            from broadcaster.backends.postgres import PostgresBackend

            return PostgresBackend(url)

        if parsed_url.scheme == "kafka":
            from broadcaster.backends.kafka import KafkaBackend

            return KafkaBackend(url)

        elif parsed_url.scheme == "memory":
            from broadcaster.backends.memory import MemoryBackend

            return MemoryBackend(url)
        raise ValueError(f"Unsupported backend: {parsed_url.scheme}")

    async def __aenter__(self) -> Broadcast:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        await self._backend.connect()
        self._listener_task = asyncio.create_task(self._listener())

    async def disconnect(self) -> None:
        if self._listener_task.done():
            self._listener_task.result()
        else:
            self._listener_task.cancel()
        await self._backend.disconnect()

    async def _listener(self) -> None:
        while True:
            event = await self._backend.next_published()
            for queue in list(self._subscribers.get(event.channel, [])):
                await queue.put(event)

    async def publish(self, channel: str, message: Any) -> None:
        await self._backend.publish(channel, message)

    @asynccontextmanager
    async def subscribe(self, channel: str, history: int | None = None) -> AsyncIterator[Subscriber]:
        queue: asyncio.Queue[Event | None] = asyncio.Queue()

        try:
            if not self._subscribers.get(channel):
                await self._backend.subscribe(channel)
                self._subscribers[channel] = {queue}
            else:
                if isinstance(self._backend, BroadcastCacheBackend):
                    try:
                        current_id = await self._backend.get_current_channel_id(channel)
                        self._backend._ready.clear()
                        for message in await self._backend.get_history_messages(channel, current_id, history):
                            queue.put_nowait(message)
                        self._subscribers[channel].add(queue)
                    finally:
                        # wake up the listener after inqueue history messages
                        # for sorted messages by publish time
                        self._backend._ready.set()
                else:
                    self._subscribers[channel].add(queue)

            yield Subscriber(queue)
        finally:
            self._subscribers[channel].remove(queue)
            if not self._subscribers.get(channel):
                del self._subscribers[channel]
                await self._backend.unsubscribe(channel)
            await queue.put(None)


class Subscriber:
    def __init__(self, queue: asyncio.Queue[Event | None]) -> None:
        self._queue = queue

    async def __aiter__(self) -> AsyncGenerator[Event | None, None]:
        try:
            while True:
                yield await self.get()
        except Unsubscribed:
            pass

    async def get(self) -> Event:
        item = await self._queue.get()
        if item is None:
            raise Unsubscribed()
        return item
