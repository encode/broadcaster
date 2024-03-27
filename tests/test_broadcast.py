import pytest

from broadcaster import Broadcast
from broadcaster._backends.memory import MemoryBackend


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
    async with Broadcast(
        "postgres://postgres:postgres@localhost:5432/broadcaster"
    ) as broadcast:
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
    backend = MemoryBackend("")
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
    with pytest.raises(
        AssertionError, match="Either `url` or `backend` must be provided."
    ):
        Broadcast()
