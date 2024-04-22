import asyncio
import typing
from urllib.parse import urlparse

import aiomqtt

from .._base import Event
from .base import BroadcastBackend


class MqttBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "localhost"
        self._port = 8883 if parsed_url.scheme == "mqtts" else 1883
        self._port = parsed_url.port or self._port
        self._client = aiomqtt.Client(self._host, port=self._port)
        self._queue: asyncio.Queue[aiomqtt.Message] = asyncio.Queue()
        self._listener_task = asyncio.create_task(self._listener())

    async def connect(self) -> None:
        await self._client.__aenter__()

    async def disconnect(self) -> None:
        self._listener_task.cancel()
        try:
            await self._listener_task
        except asyncio.CancelledError:
            pass

        await self._client.__aexit__(None, None, None)

    async def subscribe(self, channel: str) -> None:
        await self._client.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        await self._client.unsubscribe(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._client.publish(channel, message, retain=False)

    async def next_published(self) -> Event:
        message = await self._queue.get()

        # Event.message is string, not bytes
        # this is a limiting factor and we need to make sure
        # that the payload is bytes in order to properly decode it
        assert isinstance(message.payload, bytes), "Payload must be bytes."

        return Event(channel=message.topic.value, message=message.payload.decode())

    async def _listener(self) -> None:
        async for message in self._client.messages:
            await self._queue.put(message)
