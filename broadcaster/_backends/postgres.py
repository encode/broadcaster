import asyncio
from typing import Any

import asyncpg
import os

from .._base import Event
from .base import BroadcastBackend

try:
    POOL_MAX_SIZE = int(os.getenv("BROADCASTER_PG_MAX_POOL_SIZE"))
except TypeError:
    POOL_MAX_SIZE = 10

class PostgresBackend(BroadcastBackend):
    _pools = {}
    _pools_lock = asyncio.Lock()

    def __init__(self, url: str):
        self._url = url

    async def _get_pool(self):
        async with self.__class__._pools_lock:
            if self._url not in self.__class__._pools:
                self.__class__._pools[self._url] = await asyncpg.create_pool(self._url, max_size=POOL_MAX_SIZE)
            return self.__class__._pools[self._url]

    async def connect(self) -> None:
        self._conn = await (await self._get_pool()).acquire()
        self._listen_queue: asyncio.Queue = asyncio.Queue()
        self._conn.add_termination_listener(self._termination_listener)

    async def disconnect(self) -> None:
        try:
            self._conn.remove_termination_listener(self._termination_listener)
        except Exception:
            # Best effort, would fail if conn already closed (thus released)
            pass

        await (await self._get_pool()).release(self._conn)
        self._conn = None

    async def subscribe(self, channel: str) -> None:
        await self._conn.add_listener(channel, self._listener)

    async def unsubscribe(self, channel: str) -> None:
        try:
            await self._conn.remove_listener(channel, self._listener)
        except Exception:
            # Best effort, would fail if conn already closed (thus released)
            pass

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
