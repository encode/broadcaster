import asyncio
import typing
import functools
from urllib.parse import urlparse

from google.cloud import pubsub_v1

from .._base import Event
from .base import BroadcastBackend


class GCloudPubSubBackend(BroadcastBackend):
    def __init__(self, url: str):
        url_parsed = urlparse(url, scheme='gcloud-pubsub')
        self._project = url_parsed.netloc
        self._consumer_channels: typing.Dict = {}
        self._producer_channels: typing.Dict = {}
        self._channel_index = 0
        self._should_stop = False
        self._set_consumer_config(url_parsed.query)

    async def connect(self) -> None:
        self._producer = pubsub_v1.PublisherClient()
        self._consumer = pubsub_v1.SubscriberClient()

    async def disconnect(self) -> None:
        self._should_stop = True
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
        channel_index = self._channel_index

        while not self._should_stop:
            channels = list(self._consumer_channels.items())

            if not channels:
                await asyncio.sleep(self._consumer_wait_time)
                continue

            channel_id, pubsub_channel = channels[channel_index]

            response = self._consumer.pull(
                pubsub_channel, max_messages=1, return_immediately=True
            )

            if not response.received_messages:
                await asyncio.sleep(self._consumer_wait_time)
                if self._channel_index >= len(channels) - 1:
                    channel_index = self._channel_index = 0
                else:
                    self._channel_index += 1
                    channel_index = self._channel_index

            else:
                pubsub_response = response.received_messages[0]
                event = Event(channel_id, pubsub_response.message.data.decode())

                if self._ack_consumed_messages:
                    self._consumer.acknowledge(
                        pubsub_channel, [pubsub_response.ack_id]
                    )
                else:
                    event.context['ack_func'] = functools.partial(
                        self._consumer.acknowledge,
                        pubsub_channel,
                        [pubsub_response.ack_id],
                    )

                return event

    def _set_consumer_config(self, query: str) -> None:
        consumer_wait_time = 1
        ack_consumed_messages = True
        splitted_query = query.split('&')

        if splitted_query[0]:
            for arg in splitted_query:
                arg_splitted = arg.split('=')
                if len(arg_splitted) == 2:
                    arg_name, arg_value = arg_splitted

                    if arg_name == 'consumer_wait_time':
                        consumer_wait_time = float(arg_value)

                    elif arg_name == 'ack_consumed_messages':
                        ack_consumed_messages = (
                            arg_value in ('1', 'true', 't', 'True', 'y', 'yes')
                        )

        self._consumer_wait_time = consumer_wait_time
        self._ack_consumed_messages = ack_consumed_messages
