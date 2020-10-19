import asyncio
import logging
import typing
from .base import BroadcastBackend
from .._base import Event

logger = logging.getLogger("broadcaster.memory")


class MemoryBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._subscribed: typing.Set = set()
        self._published: typing.Optional[asyncio.Queue] = None

    async def connect(self) -> None:
        if self._published is not None:
            logger.warning("already connected, cannot connect again!")
            return

        self._published = asyncio.Queue()

    async def disconnect(self) -> None:
        self._published = None

    async def subscribe(self, channel: str) -> None:
        self._subscribed.add(channel)

    async def unsubscribe(self, channel: str) -> None:
        self._subscribed.remove(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        if self._published is None:
            logger.warning("not connected, unable to publish message")
            return

        event = Event(channel=channel, message=message)
        await self._published.put(event)

    async def next_published(self) -> Event:
        while True:
            if self._published is None:
                logger.warning("not connected, unable to retrieve next published message")
                continue

            event = await self._published.get()
            if event.channel in self._subscribed:
                return event
