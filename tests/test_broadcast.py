import asyncio
import subscribe
import pytest


@pytest.mark.asyncio
async def test_memory():
    events = []

    async def handler(event):
        events.append(event)

    async with subscribe.Broadcast('memory://') as broadcast:
        async with broadcast.subscribe('chatroom', callback=handler):
            await broadcast.publish('chatroom', 'hello')
            await asyncio.sleep(0.1)

    assert events == [('chatroom', 'hello')]


@pytest.mark.asyncio
async def test_redis():
    events = []

    async def handler(event):
        events.append(event)

    async with subscribe.Broadcast('redis://localhost:6379') as broadcast:
        async with broadcast.subscribe('chatroom', callback=handler):
            await broadcast.publish('chatroom', 'hello')
            await asyncio.sleep(0.1)

    assert events == [('chatroom', 'hello')]


@pytest.mark.asyncio
async def test_postgres():
    events = []

    async def handler(event):
        events.append(event)

    async with subscribe.Broadcast('postgres://localhost:5432/hostedapi') as broadcast:
        async with broadcast.subscribe('chatroom', callback=handler):
            await broadcast.publish('chatroom', 'hello')
            await asyncio.sleep(0.1)

    assert events == [('chatroom', 'hello')]
