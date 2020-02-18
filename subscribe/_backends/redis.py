import asyncio_redis
import typing
from urllib.parse import urlparse
from .base import BroadcastBackend


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "localhost"
        self._port = parsed_url.port or 6379

    async def connect(self) -> None:
        self._pub_conn = await asyncio_redis.Connection.create(self._host, self._port)
        self._sub_conn = await asyncio_redis.Connection.create(self._host, self._port)
        self._subscriber = await self._sub_conn.start_subscribe()

    async def disconnect(self) -> None:
        self._pub_conn.close()
        self._sub_conn.close()

    async def subscribe(self, group: str) -> None:
        await self._subscriber.subscribe([group])

    async def unsubscribe(self, group: str) -> None:
        await self._subscriber.unsubscribe([group])

    async def publish(self, group: str, message: typing.Any) -> None:
        await self._pub_conn.publish(group, message)

    async def next_published(self) -> typing.Tuple[str, typing.Any]:
        message = await self._subscriber.next_published()
        return (message.channel, message.value)
