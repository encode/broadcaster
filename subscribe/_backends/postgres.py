import asyncio
import asyncpg
import typing
from urllib.parse import ParseResult
from .base import BroadcastBackend


class PostgresBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._url = url
        self._listen_queue = asyncio.Queue()

    async def connect(self) -> None:
        self._conn = await asyncpg.connect(self._url)

    async def disconnect(self) -> None:
        await self._conn.close()

    async def subscribe(self, group: str) -> None:
        await self._conn.add_listener(group, self._listener)

    async def unsubscribe(self, group: str) -> None:
        await self._conn.remove_listener(group, self._listener)

    async def publish(self, group: str, message: str) -> None:
        await self._conn.execute("SELECT pg_notify($1, $2);", group, message)

    def _listener(self, *args) -> None:
        connection, pid, channel, payload = args
        self._listen_queue.put_nowait((channel, payload))

    async def next_published(self) -> typing.Tuple[str, str]:
        return await self._listen_queue.get()
