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


class Unsubscribed(Exception):
    pass


class Broadcast:
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._subscribers = {}
        if parsed_url.scheme in ('redis', 'rediss'):
            from ._backends.redis import RedisBackend

            self._backend = RedisBackend(url)

        elif parsed_url.scheme in ('postgres', 'postgresql'):
            from ._backends.postgres import PostgresBackend

            self._backend = PostgresBackend(url)

        if parsed_url.scheme == 'kafka':
            from ._backends.kafka import KafkaBackend

            self._backend = KafkaBackend(url)

        elif parsed_url.scheme == 'memory':
            from ._backends.memory import MemoryBackend

            self._backend = MemoryBackend(url)

    async def __aenter__(self) -> 'Broadcast':
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
            for queue in list(self._subscribers.get(event.channel, [])):
                await queue.put(event)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._backend.publish(channel, message)

    @asynccontextmanager
    async def subscribe(self, channel: str) -> 'Subscriber':
        queue: asyncio.Queue = asyncio.Queue()

        try:
            if not self._subscribers.get(channel):
                await self._backend.subscribe(channel)
                self._subscribers[channel] = set([queue])
            else:
                self._subscribers[channel].add(queue)

            yield Subscriber(queue)

            self._subscribers[channel].remove(queue)
            if not self._subscribers.get(channel):
                del self._subscribers[channel]
                await self._backend.unsubscribe(channel)
        finally:
            await queue.put(None)


class Subscriber:
    def __init__(self, queue):
        self._queue = queue

    async def __aiter__(self):
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
