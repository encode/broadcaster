import asyncio
import pytest
from broadcaster import Broadcast


@pytest.mark.asyncio
async def test_memory():
    async with Broadcast('memory://') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_redis():
    async with Broadcast('redis://localhost:6379') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_redis_stream():
    async with Broadcast('redis-stream://localhost:6379') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_postgres():
    async with Broadcast('postgres://localhost:5432/hostedapi') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'
