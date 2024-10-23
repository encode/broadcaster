from __future__ import annotations

import asyncio
import typing

from redis import asyncio as redis

from .._base import Event
from .base import BroadcastBackend


class RedisBackend(BroadcastBackend):
    _conn: redis.Redis

    def __init__(self, url: str | None = None, *, conn: redis.Redis | None = None):
        if url is None:
            assert conn is not None, "conn must be provided if url is not"
            self._conn = conn
        else:
            self._conn = redis.Redis.from_url(url)

        self._pubsub = self._conn.pubsub()
        self._ready = asyncio.Event()
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._listener: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        self._listener = asyncio.create_task(self._pubsub_listener())
        await self._pubsub.connect()  # type: ignore[no-untyped-call]

    async def disconnect(self) -> None:
        await self._pubsub.aclose()  # type: ignore[no-untyped-call]
        await self._conn.aclose()
        if self._listener is not None:
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
        while True:
            await self._ready.wait()
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    event = Event(
                        channel=message["channel"].decode(),
                        message=message["data"].decode(),
                    )
                    await self._queue.put(event)

            # when no channel subscribed, clear the event.
            # And then in next loop, event will blocked again until
            # the new channel subscribed.Now asyncio.Task will not exit again.
            self._ready.clear()


StreamMessageType = typing.Tuple[bytes, typing.Tuple[typing.Tuple[bytes, typing.Dict[bytes, bytes]]]]


class RedisStreamBackend(BroadcastBackend):
    def __init__(self, url: str):
        url = url.replace("redis-stream", "redis", 1)
        self.streams: dict[bytes | str | memoryview, int | bytes | str | memoryview] = {}
        self._ready = asyncio.Event()
        self._producer = redis.Redis.from_url(url)
        self._consumer = redis.Redis.from_url(url)

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        await self._producer.aclose()
        await self._consumer.aclose()

    async def subscribe(self, channel: str) -> None:
        try:
            info = await self._consumer.xinfo_stream(channel)
            last_id = info["last-generated-id"]
        except redis.ResponseError:
            last_id = "0"
        self.streams[channel] = last_id
        self._ready.set()

    async def unsubscribe(self, channel: str) -> None:
        self.streams.pop(channel, None)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.xadd(channel, {"message": message})

    async def wait_for_messages(self) -> list[StreamMessageType]:
        await self._ready.wait()
        messages = None
        while not messages:
            messages = await self._consumer.xread(self.streams, count=1, block=100)
        return messages

    async def next_published(self) -> Event:
        messages = await self.wait_for_messages()
        stream, events = messages[0]
        _msg_id, message = events[0]
        self.streams[stream.decode("utf-8")] = _msg_id.decode("utf-8")
        return Event(
            channel=stream.decode("utf-8"),
            message=message.get(b"message", b"").decode("utf-8"),
        )
