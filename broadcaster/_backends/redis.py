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
        self.parse_query_db_password(parsed_url.query)

    async def connect(self) -> None:
        self._pub_conn = await asyncio_redis.Connection.create(
            host=self._host, port=self._port, db=self._db, password=self._password)
        self._sub_conn = await asyncio_redis.Connection.create(
            host=self._host, port=self._port, db=self._db, password=self._password)
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

    def parse_query_db_password(self, query: str) -> None:
        db = None
        password = None
        for query_param in query.split("&"):
            if query_param.count("=") != 1:
                continue
            query_key, query_value = query_param.split("=")
            if query_key == "password":
                password = query_value
            elif query_key == "db":
                try:
                    db = int(query_value)
                except ValueError:
                    db = 0
        self._db = db or 0
        self._password = password
