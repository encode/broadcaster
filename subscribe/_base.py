import asyncio
import typing
from contextlib import asynccontextmanager
from urllib.parse import urlparse


class Broadcast:
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._subscribers = {}
        if parsed_url.scheme == 'redis':
            from ._backends.redis import RedisBackend
            self._backend = RedisBackend(url)

        elif parsed_url.scheme in ('postgres', 'postgresql'):
            from ._backends.postgres import PostgresBackend
            self._backend = PostgresBackend(url)

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
            group, message = await self._backend.next_published()
            for subscriber in list(self._subscribers.get(group, [])):
                await subscriber.put((group, message))

    async def _queue_listener(self, queue, callback, args=(), kwargs={}):
        while True:
            event = await queue.get()
            if event is None:
                break
            await callback(event, *args, **kwargs)

    async def publish(self, group: str, message: typing.Any) -> None:
        await self._backend.publish(group, message)

    @asynccontextmanager
    async def subscribe(self, group: str, callback: typing.Callable = None, args=(), kwargs={}) -> asyncio.Queue:
        queue = asyncio.Queue()

        task = asyncio.create_task(self._queue_listener(queue, callback, args, kwargs))

        try:
            if not self._subscribers.get(group):
                await self._backend.subscribe(group)
                self._subscribers[group] = set([queue])
            else:
                self._subscribers[group].add(queue)

            yield queue

            await queue.put(None)
            self._subscribers[group].remove(queue)
            if not self._subscribers.get(group):
                del self._subscribers[group]
                await self._backend.unsubscribe(group)
        finally:
            await task
            task.result()
