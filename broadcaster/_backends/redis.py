import typing
from urllib.parse import urlparse

import asyncio_redis

from .._base import Event
from .base import BroadcastBackend


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "localhost"
        self._port = parsed_url.port or 6379
        self._password = parsed_url.password or None

    async def connect(self) -> None:
        kwargs = {"host": self._host, "port": self._port, "password": self._password}
        self._pub_conn = await asyncio_redis.Connection.create(**kwargs)
        self._sub_conn = await asyncio_redis.Connection.create(**kwargs)
        self._subscriber = await self._sub_conn.start_subscribe()

    async def disconnect(self) -> None:
        self._pub_conn.close()
        self._sub_conn.close()

    async def subscribe(self, channel: str) -> None:
        await self._subscriber.subscribe([channel])

    async def unsubscribe(self, channel: str) -> None:
        await self._subscriber.unsubscribe([channel])

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._pub_conn.publish(channel, message)

    async def next_published(self) -> Event:
        message = await self._subscriber.next_published()
        return Event(channel=message.channel, message=message.value)
