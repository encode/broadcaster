"""Check for #22"""
import asyncio
from uuid import uuid4

import pytest

MESSAGES = ["hello", "goodbye"]

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
    msgs = [f"{msg} {uid}" for msg in MESSAGES]
    async with setup_broadcast.subscribe(channel) as subscriber:
        to_publish = [setup_broadcast.publish(channel, msg) for msg in msgs]

        await asyncio.gather(*to_publish)
        for msg in msgs:
            event = await subscriber.get()
            assert event.channel == channel
            assert event.message == msg
