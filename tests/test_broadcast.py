from __future__ import annotations

import asyncio
import typing

import pytest

from broadcaster import Broadcast, BroadcastBackend, Event


class CustomBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._subscribed: set[str] = set()

    async def connect(self) -> None:
        self._published: asyncio.Queue[Event] = asyncio.Queue()

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


@pytest.mark.asyncio
async def test_memory():
    async with Broadcast("memory://") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis():
    async with Broadcast("redis://localhost:6379") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_postgres():
    async with Broadcast("postgres://postgres:postgres@localhost:5432/broadcaster") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.skip("Deadlock on `next_published`")
@pytest.mark.asyncio
async def test_kafka():
    async with Broadcast("kafka://localhost:9092") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_custom():
    backend = CustomBackend("")
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_unknown_backend():
    with pytest.raises(ValueError, match="Unsupported backend"):
        async with Broadcast(url="unknown://"):
            pass


@pytest.mark.asyncio
async def test_needs_url_or_backend():
    with pytest.raises(AssertionError, match="Either `url` or `backend` must be provided."):
        Broadcast()
