import aioredis
import asyncio
import typing

from .base import BroadcastBackend
from .._base import Event


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        self.conn_url = url
        self.channel = None

    async def connect(self) -> None:
        loop = asyncio.get_event_loop()
        self._pub_conn = await aioredis.create_redis(self.conn_url, loop=loop)
        self._sub_conn = await aioredis.create_redis(self.conn_url, loop=loop)

    async def disconnect(self) -> None:
        self._pub_conn.close()
        self._sub_conn.close()

    async def subscribe(self, channel: str) -> None:
        channel = await self._sub_conn.subscribe(channel)
        self.channel = channel[0]

    async def unsubscribe(self, channel: str) -> None:
        await self._sub_conn.unsubscribe(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._pub_conn.publish_json(channel, message)

    async def next_published(self) -> Event:
        while (await self.channel.wait_message()):
            message = await self.channel.get_json()
            return Event(channel=self.channel.name.decode("utf8"), message=message)
