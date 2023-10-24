import asyncio
import typing
from urllib.parse import urlparse

import asyncio_redis
import aioredis

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


class RedisStreamBackend(BroadcastBackend):
    def __init__(self, url: str):
        self.conn_url = url.replace('redis-stream', 'redis', 1)
        self.streams: typing.Dict = dict()

    async def connect(self) -> None:
        self._producer = await aioredis.from_url(self.conn_url)
        self._consumer = await aioredis.from_url(self.conn_url)

    async def disconnect(self) -> None:
        await self._producer.close()
        await self._consumer.close()

    async def subscribe(self, channel: str) -> None:
        try:
            info = await self._consumer.xinfo_stream(channel)
            last_id = info['last-generated-id']
        except aioredis.exceptions.ResponseError:
            last_id = '0'
        self.streams[channel] = last_id

    async def unsubscribe(self, channel: str) -> None:
        self.streams.pop(channel, None)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.xadd(channel, {'message': message})

    async def wait_for_messages(self) -> typing.List:
        messages = None
        while not messages:
            while not self.streams:
                await asyncio.sleep(1)
            messages = await self._consumer.xread(self.streams, count=1, block=1000)
        return messages

    async def next_published(self) -> Event:
        messages = await self.wait_for_messages()
        stream, events = messages[0]
        _msg_id, message = events[0]
        self.streams[stream.decode('utf-8')] = _msg_id.decode('utf-8')
        return Event(
            channel=stream.decode('utf-8'),
            message=message.get(b'message').decode('utf-8'),
        )
