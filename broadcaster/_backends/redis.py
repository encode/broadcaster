import asyncio
import typing

from redis import asyncio as redis

from .._base import Event
from .base import BroadcastBackend


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._conn = redis.Redis.from_url(url)
        self._pubsub = self._conn.pubsub()
        self._ready = asyncio.Event()
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._listener = asyncio.create_task(self._pubsub_listener())

    async def connect(self) -> None:
        await self._pubsub.connect()

    async def disconnect(self) -> None:
        await self._pubsub.aclose()
        await self._conn.aclose()
        self._listener.cancel()

    async def subscribe(self, channel: str) -> None:
        self._ready.set()
        await self._pubsub.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self._pubsub.unsubscribe(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._conn.publish(channel, message)

    async def next_published(self) -> Event:
        return await self._queue.get()

    async def _pubsub_listener(self) -> None:
        # redis-py does not listen to the pubsub connection if there are no channels subscribed
        # so we need to wait until the first channel is subscribed to start listening
        await self._ready.wait()
        async for message in self._pubsub.listen():
            if message["type"] == "message":
                event = Event(
                    channel=message["channel"].decode(),
                    message=message["data"].decode(),
                )
                await self._queue.put(event)
