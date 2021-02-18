# Broadcaster

**Forked from original [broadcaster](https://github.com/encode/broadcaster) to get patch fixes available for re-use**

Broadcaster helps you develop realtime streaming functionality by providing
a simple broadcast API onto a number of different backend services.

It currently supports [Redis PUB/SUB](https://redis.io/topics/pubsub), [Apache Kafka](https://kafka.apache.org/), and [Postgres LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html), plus a simple in-memory backend, that you can use for local development or during testing.

<img src="https://raw.githubusercontent.com/encode/broadcaster/master/docs/demo.gif" alt='WebSockets Demo'>

Here's a complete example of the backend code for a simple websocket chat app:

**app.py**

```python
# Requires: `starlette`, `uvicorn`, `jinja2`
# Run with `uvicorn example:app`
from broadcaster import Broadcast
from starlette.applications import Starlette
from starlette.concurrency import run_until_first_complete
from starlette.routing import Route, WebSocketRoute
from starlette.templating import Jinja2Templates


broadcast = Broadcast("redis://localhost:6379")
templates = Jinja2Templates("templates")


async def homepage(request):
    template = "index.html"
    context = {"request": request}
    return templates.TemplateResponse(template, context)


async def chatroom_ws(websocket):
    await websocket.accept()
    await run_until_first_complete(
        (chatroom_ws_receiver, {"websocket": websocket}),
        (chatroom_ws_sender, {"websocket": websocket}),
    )


async def chatroom_ws_receiver(websocket):
    async for message in websocket.iter_text():
        await broadcast.publish(channel="chatroom", message=message)


async def chatroom_ws_sender(websocket):
    async with broadcast.subscribe(channel="chatroom") as subscriber:
        async for event in subscriber:
            await websocket.send_text(event.message)


routes = [
    Route("/", homepage),
    WebSocketRoute("/", chatroom_ws, name='chatroom_ws'),
]


app = Starlette(
    routes=routes, on_startup=[broadcast.connect], on_shutdown=[broadcast.disconnect],
)
```

The HTML template for the front end [is available here](https://github.com/encode/broadcaster/blob/master/example/templates/index.html), and is adapted from [Pieter Noordhuis's PUB/SUB demo](https://gist.github.com/pietern/348262).

## Requirements

Python 3.7+

## Installation

* `pip install broadcaster`
* `pip install broadcaster[redis]`
* `pip install broadcaster[postgres]`
* `pip install broadcaster[kafka]`

## Available backends

* `Broadcast('memory://')`
* `Broadcast("redis://localhost:6379")`
* `Broadcast("postgres://localhost:5432/broadcaster")`
* `Broadcast("kafka://localhost:9092")`

## Where next?

At the moment `broadcaster` is in Alpha, and should be considered a working design document.

The API should be considered subject to change. If you *do* want to use Broadcaster in its current
state, make sure to strictly pin your requirements to `broadcaster==0.2.0`.

To be more capable we'd really want to add some additional backends, provide API support for reading recent event history from persistent stores, and provide a serialization/deserialization API...

* Serialization / deserialization to support broadcasting structured data.
* Backends for Redis Streams, Apache Kafka, and RabbitMQ.
* Add support for `subscribe('chatroom', history=100)` for backends which provide persistence. (Redis Streams, Apache Kafka) This will allow applications to subscribe to channel updates, while also being given an initial window onto the most recent events. We *might* also want to support some basic paging operations, to allow applications to scan back in the event history.
* Support for pattern subscribes in backends that support it.
