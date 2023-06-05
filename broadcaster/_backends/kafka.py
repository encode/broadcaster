import logging
import os
import typing
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

from .._base import Event
from .base import BroadcastBackend


class KafkaBackend(BroadcastBackend):
    def __init__(self, url: str):
        parsed_url = urlparse(url).netloc
        self._servers = parsed_url.split(",")
        self._consumer_channels: typing.Set = set()
        self._security_protocol = (
            os.environ.get("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT"
        )
        self._sasl_mechanism = os.environ.get("KAFKA_SASL_MECHANISM") or "PLAIN"
        self._sasl_plain_username = os.environ.get("KAFKA_PLAIN_USERNAME")
        self._sasl_plain_password = os.environ.get("KAFKA_PLAIN_PASSWORD")
        self._ssl_keyfile = os.environ.get("KAFKA_SSL_KEYFILE")
        self._ssl_keypassword = os.environ.get("KAFKA_SSL_KEY_PASSWORD")
        self._ssl_cafile = os.environ.get("KAFKA_SSL_CAFILE")
        self._ssl_capath = os.environ.get("KAFKA_SSL_CAPATH")
        self._ssl_certfile = os.environ.get("KAFKA_SSL_CERTFILE")
        self._ssl_context = (
            create_ssl_context(
                cafile=self._ssl_cafile,
                capath=self._ssl_capath,
                certfile=self._ssl_certfile,
                keyfile=self._ssl_keyfile,
                password=self._ssl_keypassword,
            )
            if self._security_protocol in ["SSL", "SASL_SSL"]
            else None
        )

    async def connect(self) -> None:
        logging.info(f"connecting to brokers: {self._servers}")
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._servers,
                security_protocol=self._security_protocol,
                ssl_context=self._ssl_context,
                sasl_mechanism=self._sasl_mechanism,
                sasl_plain_username=self._sasl_plain_username,
                sasl_plain_password=self._sasl_plain_password,
            )
            self._consumer = AIOKafkaConsumer(
                bootstrap_servers=self._servers,
                security_protocol=self._security_protocol,
                ssl_context=self._ssl_context,
                sasl_mechanism=self._sasl_mechanism,
                sasl_plain_username=self._sasl_plain_username,
                sasl_plain_password=self._sasl_plain_password,
            )
            await self._producer.start()
            await self._consumer.start()
        except Exception as e:
            logging.error(e)
            raise e

    async def disconnect(self) -> None:
        await self._producer.stop()
        await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        self._consumer_channels.add(channel)
        self._consumer.subscribe(topics=self._consumer_channels)

    async def unsubscribe(self, channel: str) -> None:
        self._consumer.unsubscribe()

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self._producer.send_and_wait(channel, message.encode("utf8"))

    async def next_published(self) -> Event:
        message = await self._consumer.getone()
        return Event(channel=message.topic, message=message.value.decode("utf8"))
