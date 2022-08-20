import typing
from contextlib import AsyncExitStack
from urllib.parse import urlparse

import asyncio_mqtt

from .._base import Event
from .base import BroadcastBackend



class MqttBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "localhost"
        self._port = 8883 if parsed_url.scheme == "mqtts" else 1883
        self._port = parsed_url.port or self._port

    async def connect(self) -> None:
        self.stack = AsyncExitStack()

        self.client = asyncio_mqtt.Client(self._host, port=self._port)
        self.messages = self.client.filtered_messages("#")

        self.client = await self.stack.enter_async_context(self.client)
        self.messages = await self.stack.enter_async_context(self.messages)

    async def disconnect(self) -> None:
        await self.stack.aclose()

    async def subscribe(self, channel: str) -> None:
        await self.client.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self.client.unsubscribe(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self.client.publish(channel, message, retain=False)

    async def next_published(self) -> Event:
        async for message in self.messages:
            return Event(channel=message.topic, message=message.payload)
