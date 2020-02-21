import asyncio
import typing
from .base import BroadcastBackend
from .._base import Event


class MemoryBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._subscribed: typing.Set = set()
        self._published: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def subscribe(self, channel: str) -> None:
        self._subscribed.add(channel)

    async def unsubscribe(self, channel: str) -> None:
        self._subscribed.remove(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        event = Event(channel=channel, message=message)
        await self._published.put(event)

    async def next_published(self) -> Event:
        while True:
            event = await self._published.get()
            if event.channel in self._subscribed:
                return event
