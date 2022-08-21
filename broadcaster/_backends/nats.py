import asyncio

from nats.aio.client import Client

from .base import BroadcastBackend
from .._base import Event


class NATSBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._nc = Client()
        self._url = url
        self._listen_queue: asyncio.Queue = asyncio.Queue()
        self._channel_ssids = {}

    async def connect(self) -> None:
        await self._nc.connect(self._url)

    async def disconnect(self) -> None:
        await self._nc.drain()

    async def subscribe(self, channel: str) -> None:
        ssid = await self._nc.subscribe(channel, cb=self.message_handler)
        self._channel_ssids[channel] = ssid

    async def unsubscribe(self, channel: str) -> None:
        ssid = self._channel_ssids.get(channel)
        if ssid is not None:
            await self._nc.unsubscribe(ssid)

    async def publish(self, channel: str, message: str) -> None:
        await self._nc.publish(channel, message.encode())

    async def message_handler(self, message) -> None:
        subject = message.subject
        data = message.data.decode()
        event = Event(channel=subject, message=data)
        self._listen_queue.put_nowait(event)

    async def next_published(self) -> Event:
        return await self._listen_queue.get()
