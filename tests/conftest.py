"""Check for #22"""

import asyncio
import functools

import pytest_asyncio

from broadcaster import Broadcast
from broadcaster._backends.kafka import KafkaBackend


async def __has_topic_now(client, topic):
    if await client.force_metadata_update():
        if topic in client.cluster.topics():
            print(f'Topic "{topic}" exists')
            return True
    return False


async def __wait_has_topic(client, topic, *, timeout_sec=5):
    poll_time_sec = 1 / 10000
    from datetime import datetime

    pre = datetime.now()
    while True:
        if (datetime.now() - pre).total_seconds() >= timeout_sec:
            raise ValueError(f'No topic "{topic}" exists')
        if await __has_topic_now(client, topic):
            return
        await asyncio.sleep(poll_time_sec)


def kafka_backend_setup(kafka_backend):
    """Block until consumer client contains the topic"""
    subscribe_impl = kafka_backend.subscribe

    @functools.wraps(subscribe_impl)
    async def subscribe(channel: str) -> None:
        await subscribe_impl(channel)
        await __wait_has_topic(kafka_backend._consumer._client, channel)

    kafka_backend.subscribe = subscribe


BROADCASTS_SETUP = {
    KafkaBackend: kafka_backend_setup,
}


@pytest_asyncio.fixture(scope="function")
async def setup_broadcast(request):
    url = request.param
    async with Broadcast(url) as broadcast:
        backend = broadcast._backend
        for klass, setup in BROADCASTS_SETUP.items():
            if isinstance(backend, klass):
                setup(backend)
                break
        yield broadcast
