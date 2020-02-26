import asyncio
import typing
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .._base import Event
from .base import BroadcastBackend


class KafkaBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._servers = [urlparse(url).netloc]
        self._consumer_channels: typing.Set = set()

    async def connect(self) -> None:
        loop = asyncio.get_event_loop()
        self._producer = AIOKafkaProducer(loop=loop, bootstrap_servers=self._servers)
        self._consumer = AIOKafkaConsumer(loop=loop, bootstrap_servers=self._servers)
        await self._producer.start()
        await self._consumer.start()

    async def disconnect(self) -> None:
        await self._producer.stop()
        await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        self._consumer_channels.add(channel)
        self._consumer.subscribe(topics=self._consumer_channels)

    async def unsubscribe(self, channel: str) -> None:
        await self._consumer.unsubscribe()

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.send_and_wait(channel, message.encode("utf8"))

    async def next_published(self) -> Event:
        message = await self._consumer.getone()
        return Event(channel=message.topic, message=message.value.decode("utf8"))
