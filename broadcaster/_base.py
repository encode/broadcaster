import asyncio
import typing
from contextlib import asynccontextmanager
from urllib.parse import urlparse


class Event:
    def __init__(self, channel, message):
        self.channel = channel
        self.message = message

    def __eq__(self, other):
        return (
            isinstance(other, Event)
            and self.channel == other.channel
            and self.message == other.message
        )

    def __repr__(self):
        return f'Event(channel={self.channel!r}, message={self.message!r})'


class Broadcast:
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._subscribers = {}
        if parsed_url.scheme == "redis":
            from ._backends.redis import RedisBackend

            self._backend = RedisBackend(url)

        elif parsed_url.scheme in ("postgres", "postgresql"):
            from ._backends.postgres import PostgresBackend

            self._backend = PostgresBackend(url)

        elif parsed_url.scheme == "memory":
            from ._backends.memory import MemoryBackend

            self._backend = MemoryBackend(url)

    async def __aenter__(self) -> "Broadcast":
        await self.connect()
        return self

    async def __aexit__(self, *args, **kwargs) -> None:
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
            for subscriber in list(self._subscribers.get(event.channel, [])):
                await subscriber.put(event)

    async def _queue_listener(self, queue, callback, args=(), kwargs={}):
        while True:
            event = await queue.get()
            if event is None:
                break
            await callback(event, *args, **kwargs)

    async def publish(self, event: Event) -> None:
        await self._backend.publish(event)

    @asynccontextmanager
    async def subscribe(
        self, channel: str, callback: typing.Callable = None, args=(), kwargs={}
    ) -> None:
        queue = asyncio.Queue()
        task = asyncio.create_task(self._queue_listener(queue, callback, args, kwargs))

        try:
            if not self._subscribers.get(channel):
                await self._backend.subscribe(channel)
                self._subscribers[channel] = set([queue])
            else:
                self._subscribers[channel].add(queue)

            yield

            self._subscribers[channel].remove(queue)
            if not self._subscribers.get(channel):
                del self._subscribers[channel]
                await self._backend.unsubscribe(channel)
        finally:
            await queue.put(None)
            await task
            task.result()
