from uuid import uuid4

import pytest

URLS = [
    ("memory://",),
    ("redis://localhost:6379",),
    ("postgres://postgres:postgres@localhost:5432/broadcaster",),
    ("kafka://localhost:9092",),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(["setup_broadcast"], URLS, indirect=True)
async def test_broadcast(setup_broadcast):
    uid = uuid4()
    channel = f"chatroom-{uid}"
    msg = f"hello {uid}"
    async with setup_broadcast.subscribe(channel) as subscriber:
        await setup_broadcast.publish(channel, msg)
        event = await subscriber.get()
        assert event.channel == channel
        assert event.message == msg
