import asyncio
import typing
from urllib.parse import urlparse

from google.cloud import pubsub_v1

from .._base import Event
from .base import BroadcastBackend


class GCloudPubSubBackend(BroadcastBackend):    
    def __init__(self, url: str):
        url_parsed = urlparse(url)
        self._project = url_parsed.path
        self._consumer_channels: typing.Dict = {}
        self._producer_channels: typing.Dict = {}
        self._channel_index = 0

    async def connect(self) -> None:
        self._producer = pubsub_v1.PublisherClient()
        self._consumer = pubsub_v1.SubscriberClient()

    async def disconnect(self) -> None:
        self._producer = None
        self._consumer = None

    async def subscribe(self, channel: str) -> None:
        pubsub_channel = self._consumer.subscription_path(
            self._project, channel
        )
        self._consumer_channels[channel] = pubsub_channel

    async def unsubscribe(self, channel: str) -> None:
        self._consumer_channels.pop(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        producer_channel = self._producer_channels.get(channel)

        if not producer_channel:
            producer_channel = self._producer.topic_path(
                self._project, channel
            )
            self._producer_channels[channel] = producer_channel

        self._producer.publish(producer_channel, message.encode())

    async def next_published(self) -> Event:
        has_message = False
        channel_index = self._channel_index

        while not has_message:
            channels = list(self._consumer_channels.items())

            if not channels:
                await asyncio.sleep(1)
                continue

            channel_id, pubsub_channel = channels[channel_index]

            response = self._consumer.pull(
                pubsub_channel, max_messages=1, return_immediately=True
            )

            if not response.received_messages:
                await asyncio.sleep(1)            
                if self._channel_index >= len(channels) - 1:
                    channel_index = self._channel_index = 0
                else:
                    self._channel_index += 1
                    channel_index = self._channel_index

            else:
                pubsub_response = response.received_messages[0]
                event = Event(channel_id, pubsub_response.message.data.decode())
                self._consumer.acknowledge(
                    pubsub_channel, [pubsub_response.ack_id]
                )
                return event
