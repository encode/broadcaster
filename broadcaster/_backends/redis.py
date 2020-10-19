import aioredis
from aioredis.abc import AbcChannel
from aioredis.pubsub import Receiver
import asyncio
import json
import logging
import typing
from .base import BroadcastBackend
from .._base import Event

logger = logging.getLogger("broadcaster.redis")


class RedisBackend(BroadcastBackend):
    def __init__(self, url: str):
        self.conn_url = url

        self._pub_conn: typing.Optional[aioredis.Redis] = None
        self._sub_conn: typing.Optional[aioredis.Redis] = None

        self._msg_queue: typing.Optional[asyncio.Queue] = None
        self._reader_task: typing.Optional[asyncio.Task] = None
        self._mpsc: typing.Optional[Receiver] = None

    async def connect(self) -> None:
        if self._pub_conn or self._sub_conn or self._msg_queue:
            logger.warning("connections are already setup but connect called again; not doing anything")
            return

        self._pub_conn = await aioredis.create_redis(self.conn_url)
        self._sub_conn = await aioredis.create_redis(self.conn_url)
        self._msg_queue = asyncio.Queue()  # must be created here, to get proper event loop
        self._mpsc = Receiver()
        self._reader_task = asyncio.create_task(self._reader())

    async def disconnect(self) -> None:
        if self._pub_conn and self._sub_conn:
            self._pub_conn.close()
            self._sub_conn.close()
        else:
            logger.warning("connections are not setup, invalid call to disconnect")

        self._pub_conn = None
        self._sub_conn = None
        self._msg_queue = None

        if self._mpsc:
            self._mpsc.stop()
        else:
            logger.warning("redis mpsc receiver is not set, cannot stop it")

        if self._reader_task:
            if self._reader_task.done():
                self._reader_task.result()
            else:
                logger.debug("cancelling reader task")
                self._reader_task.cancel()
                self._reader_task = None

    async def subscribe(self, channel: str) -> None:
        if not self._sub_conn:
            logger.error(f"not connected, cannot subscribe to channel {channel!r}")
            return

        await self._sub_conn.subscribe(self._mpsc.channel(channel))

    async def unsubscribe(self, channel: str) -> None:
        if not self._sub_conn:
            logger.error(f"not connected, cannot unsubscribe from channel {channel!r}")
            return

        await self._sub_conn.unsubscribe(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        if not self._pub_conn:
            logger.error(f"not connected, cannot publish to channel {channel!r}")
            return

        await self._pub_conn.publish_json(channel, message)

    async def next_published(self) -> Event:
        if not self._msg_queue:
            raise RuntimeError("unable to get next_published event, RedisBackend is not connected")

        return await self._msg_queue.get()

    async def _reader(self) -> None:
        async for channel, msg in self._mpsc.iter(encoding="utf8", decoder=json.loads):
            if not isinstance(channel, AbcChannel):
                logger.error(f"invalid channel returned from Receiver().iter() - {channel!r}")
                continue

            channel_name = channel.name.decode("utf8")

            if not self._msg_queue:
                logger.error(f"unable to put new message from {channel_name} into queue, not connected")
                continue

            await self._msg_queue.put(Event(channel=channel_name, message=msg))
