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


@pytest.mark.asyncio
@pytest.mark.parametrize(["setup_broadcast"], URLS, indirect=True)
async def test_sub(setup_broadcast):
    uid = uuid4()
    channel1 = f"chatroom-{uid}1"
    channel2 = f"chatroom-{uid}2"

    to_sub = [
        setup_broadcast._backend.subscribe(channel1),
        setup_broadcast._backend.subscribe(channel2),
    ]
    await asyncio.gather(*to_sub)


@pytest.mark.asyncio
@pytest.mark.parametrize(["setup_broadcast"], URLS, indirect=True)
async def test_unsub(setup_broadcast):
    uid = uuid4()
    channel1 = f"chatroom-{uid}1"
    channel2 = f"chatroom-{uid}2"

    await setup_broadcast._backend.subscribe(channel1)
    await setup_broadcast._backend.subscribe(channel2)

    to_unsub = [
        setup_broadcast._backend.unsubscribe(channel1),
        setup_broadcast._backend.unsubscribe(channel2),
    ]

    await asyncio.gather(*to_unsub)
