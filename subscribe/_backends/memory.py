import asyncio
import typing
from .base import BroadcastBackend


class MemoryBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._subscribed = set()
        self._published = asyncio.Queue()

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def subscribe(self, group: str) -> None:
        self._subscribed.add(group)

    async def unsubscribe(self, group: str) -> None:
        self._subscribed.remove(group)

    async def publish(self, group: str, message: typing.Any) -> None:
        await self._published.put((group, message))

    async def next_published(self) -> typing.Tuple[str, typing.Any]:
        while True:
            group, payload = await self._published.get()
            if group in self._subscribed:
                return (group, payload)
