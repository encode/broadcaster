import os
import pytest
from broadcaster import Broadcast
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists as AlreadyExistsError


@pytest.fixture
def pubsub_init():
    os.environ['PUBSUB_EMULATOR_HOST'] = 'localhost:8086'
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    project_id = 'broadcaster-local'

    for name in ['chatroom', 'chatroom2']:
        topic_path = publisher.topic_path(project_id, name)
        subscription_path = subscriber.subscription_path(project_id, name)

        try:
            publisher.create_topic(topic_path)
        except AlreadyExistsError:
            ...

        try:
            subscriber.create_subscription(subscription_path, topic_path)
        except AlreadyExistsError:
            ...


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


@pytest.mark.asyncio
async def test_gcloud_pubsub(pubsub_init):
    async with Broadcast('gcloud-pubsub://broadcaster-local') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_gcloud_pubsub_two_channels(pubsub_init):
    async with Broadcast('gcloud-pubsub://broadcaster-local') as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            async with broadcast.subscribe('chatroom2') as subscriber2:
                await broadcast.publish('chatroom', 'hello')
                await broadcast.publish('chatroom2', 'hello2')
                event = await subscriber.get()
                event2 = await subscriber2.get()
                assert event.channel == 'chatroom'
                assert event.message == 'hello'
                assert event2.channel == 'chatroom2'
                assert event2.message == 'hello2'


@pytest.mark.asyncio
async def test_gcloud_pubsub_consumer_wait_time_option(pubsub_init):
    url = 'gcloud-pubsub://broadcaster-local?consumer_wait_time=0.1'
    async with Broadcast(url) as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_gcloud_pubsub_consumer_ack_consumed_messages_option(pubsub_init):
    url = 'gcloud-pubsub://broadcaster-local/?ack_consumed_messages=0'
    async with Broadcast(url) as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            event.context['ack_func']()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'


@pytest.mark.asyncio
async def test_gcloud_pubsub_consumer_wait_time_and_ack_consumed_messages_options(pubsub_init):
    url = 'gcloud-pubsub://broadcaster-local?consumer_wait_time=0.1&ack_consumed_messages=0'
    async with Broadcast(url) as broadcast:
        async with broadcast.subscribe('chatroom') as subscriber:
            await broadcast.publish('chatroom', 'hello')
            event = await subscriber.get()
            event.context['ack_func']()
            assert event.channel == 'chatroom'
            assert event.message == 'hello'
