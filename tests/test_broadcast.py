import pytest
from broadcaster import Broadcast


@pytest.mark.asyncio
async def test_memory():
    async with Broadcast('memory://') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('foo', 'bar')
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_redis():
    async with Broadcast('redis://localhost:6379') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('foo', 'bar')
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_redis_complex():
    async with Broadcast('redis://:123@localhost:6377/4') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_postgres():
    async with Broadcast('postgres://postgres:postgres@localhost:5432/broadcaster') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_kafka():
    async with Broadcast('kafka://localhost:9092') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'
