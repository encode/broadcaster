from __future__ import annotations

import asyncio
import typing
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .._base import Event
from .base import BroadcastBackend


class KafkaBackend(BroadcastBackend):
    def __init__(self, urls: str | list[str]) -> None:
        urls = [urls] if isinstance(urls, str) else urls
        self._servers = [urlparse(url).netloc for url in urls]
        self._consumer_channels: set[str] = set()
        self._ready = asyncio.Event()

    async def connect(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._servers)  # pyright: ignore
        self._consumer = AIOKafkaConsumer(bootstrap_servers=self._servers)  # pyright: ignore
        await self._producer.start()
        await self._consumer.start()

    async def disconnect(self) -> None:
        await self._producer.stop()
        await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        self._consumer_channels.add(channel)
        self._consumer.subscribe(topics=self._consumer_channels)
        await self._wait_for_assignment()

    async def unsubscribe(self, channel: str) -> None:
        self._consumer.unsubscribe()

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.send_and_wait(channel, message.encode("utf8"))

    async def next_published(self) -> Event:
        await self._ready.wait()
        message = await self._consumer.getone()
        value = message.value

        # for type compatibility:
        # we declare Event.message as str, so convert None to empty string
        if value is None:
            value = b""
        return Event(channel=message.topic, message=value.decode("utf8"))

    async def _wait_for_assignment(self) -> None:
        """Wait for the consumer to be assigned to the partition."""
        while not self._consumer.assignment():
            await asyncio.sleep(0.001)

        self._ready.set()
