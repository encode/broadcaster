from typing import Any
from urllib.parse import urlparse

import redis.asyncio as redis
from redis.asyncio.client import PubSub

from .._base import Event
from .base import BroadcastBackend
import asyncio


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "localhost"
        self._port = parsed_url.port or 6379
        self._password = parsed_url.password or None
        self._ssl = parsed_url.scheme == "rediss"
        self.kwargs = {
            "host": self._host,
            "port": self._port,
            "password": self._password,
            "ssl": self._ssl,
        }

        self._sub_conn: PubSub | None = None
        self._pub_conn: PubSub | None = None

    async def connect(self) -> None:
        self._pub_conn = redis.Redis(**self.kwargs).pubsub()
        self._sub_conn = redis.Redis(**self.kwargs).pubsub()

    async def disconnect(self) -> None:
        await self._pub_conn.close()
        await self._sub_conn.close()

    async def subscribe(self, channel: str) -> None:
        await self._sub_conn.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self._sub_conn.unsubscribe(channel)

    async def publish(self, channel: str, message: Any) -> None:
        try:
            await self._pub_conn.execute_command("PUBLISH", channel, message)
        except (redis.ConnectionError, redis.TimeoutError):
            await asyncio.sleep(1)
            self._pub_conn = redis.Redis(**self.kwargs).pubsub()
            await self.publish(channel, message)

    async def next_published(self) -> Event:
        message = None
        while not message:
            message = await self._sub_conn.get_message(
                ignore_subscribe_messages=True, timeout=None
            )
        event = Event(
            channel=message["channel"].decode(),
            message=message["data"].decode(),
        )
        return event
