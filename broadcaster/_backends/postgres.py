import asyncio
import asyncpg
from .base import BroadcastBackend
from .._base import Event


class PostgresBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._url = url
        self._listen_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> None:
        self._conn = await asyncpg.connect(self._url)

    async def disconnect(self) -> None:
        await self._conn.close()

    async def subscribe(self, channel: str) -> None:
        await self._conn.add_listener(channel, self._listener)

    async def unsubscribe(self, channel: str) -> None:
        await self._conn.remove_listener(channel, self._listener)

    async def publish(self, channel: str, message: str) -> None:
        await self._conn.execute("SELECT pg_notify($1, $2);", channel, message)

    def _listener(self, *args) -> None:
        connection, pid, channel, payload = args
        event = Event(channel=channel, message=payload)
        self._listen_queue.put_nowait(event)

    async def next_published(self) -> Event:
        return await self._listen_queue.get()
