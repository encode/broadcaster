import typing

import aioredis
from aioredis.pubsub import Receiver

from .base import BroadcastBackend
from .._base import Event


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._url = url

    async def connect(self) -> None:
        self._pub_conn = await aioredis.create_redis(self._url)
        self._sub_conn = await aioredis.create_redis(self._url)
        self._receiver = Receiver()

    async def disconnect(self) -> None:
        self._receiver.stop()
        self._pub_conn.close()
        self._sub_conn.close()
        await self._pub_conn.wait_closed()
        await self._sub_conn.wait_closed()

    async def subscribe(self, channel: str) -> None:
        await self._sub_conn.subscribe(self._receiver.channel(channel))

    async def unsubscribe(self, channel: str) -> None:
        await self._sub_conn.unsubscribe(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._pub_conn.publish(channel, message)

    async def next_published(self) -> Event:
        message = await self._receiver.get(encoding="utf8")
        if message is None:
            raise aioredis.ChannelClosedError()
        channel, message = message
        return Event(channel=channel.name.decode("utf8"), message=message)
