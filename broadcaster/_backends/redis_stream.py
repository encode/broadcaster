import aioredis
import asyncio
import typing

from .base import BroadcastBackend
from .._base import Event


class RedisStreamBackend(BroadcastBackend):
    def __init__(self, url: str):
        self.conn_url = url.replace('redis-stream', 'redis', 1)
        self.streams: typing.Set = set()

    async def connect(self) -> None:
        loop = asyncio.get_event_loop()
        self._producer = await aioredis.create_redis(self.conn_url, loop=loop)
        self._consumer = await aioredis.create_redis(self.conn_url, loop=loop)

    async def disconnect(self) -> None:
        self._producer.close()
        self._consumer.close()

    async def subscribe(self, channel: str) -> None:
        self.streams.add(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self.streams.discard(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.xadd(channel, {'message': message})

    async def _wait_for_streams(self) -> None:
        while not self.streams:
            await asyncio.sleep(1)

    async def next_published(self) -> Event:
        await self._wait_for_streams()
        messages = await self._consumer.xread(list(self.streams))
        stream, _msg_id, message = messages[0]
        return Event(
            channel=stream.decode('utf-8'),
            message=message.get(b'message').decode('utf-8'),
        )
