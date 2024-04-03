import asyncio
from typing import Any

import asyncpg

from .._base import Event
from .base import BroadcastBackend


class PostgresBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._url = url

    async def connect(self) -> None:
        self._conn = await asyncpg.connect(self._url)
        self._listen_queue: asyncio.Queue = asyncio.Queue()
        self._conn.add_termination_listener(self._termination_listener)

    async def disconnect(self) -> None:
        await self._conn.close()

    async def subscribe(self, channel: str) -> None:
        await self._conn.add_listener(channel, self._listener)

    async def unsubscribe(self, channel: str) -> None:
        await self._conn.remove_listener(channel, self._listener)

    async def publish(self, channel: str, message: str) -> None:
        await self._conn.execute("SELECT pg_notify($1, $2);", channel, message)

    async def _listener(self, *args: Any) -> None:
        connection, pid, channel, payload = args
        event = Event(channel=channel, message=payload)
        await self._listen_queue.put(event)

    async def _termination_listener(self, *args: Any) -> None:
        await self._listen_queue.put(None)

    async def next_published(self) -> Event:
        return await self._listen_queue.get()
