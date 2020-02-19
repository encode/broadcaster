# Broadcaster

Broadcaster provides a simple API for broadcast notifications.

*You'll need this if you want to implement realtime streaming services such
as WebSocket based chat rooms.*

The broadcaster package provides a consistent interface to a number of
different broadcast services...

* Local memory - `broadcaster.Broadcast('memory://')`
* Redis PUB/SUB - `broadcaster.Broadcast('redis://localhost:6379')`
* Postgres LISTEN/NOTIFY - `broadcaster.Broadcast('postgres://localhost:5432/database')`
* Apache Kafka - `broadcaster.Broadcast('kafka://localhost:9092')`

Some rules of thumb for choosing a backend:

* Consider using a local memory backend for test and development environments.
This backend won't communicate across multiple app instances, so isn't appropriate for
production, but is useful for lightweight testing.
* Redis PUB/SUB is a great all round option.
* Postgres LISTEN/NOTIFY is useful if you're already using a Postgres database,
and want to minimise the number of different services.
* Apache Kafka is the most capable and consistent option for large-scale services.

**app.py**

```python
from broadcaster import Broadcast, Event
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.templating import Jinja2Templates


broadcast = Broadcast('redis://localhost:6379')
templates = Jinja2Templates('templates')


async def homepage(request):
    template = 'chatroom.html'
    context = {'request': request}
    return templates.TemplateResponse(template, context)


async def chatroom_ws(websocket):
    async with broadcast.subscribe(
        channel='chatroom',
        callback=handle_chat_event,
        args=(websocket,)
    ):
        async for message in websocket.iter_text():
            event = Event(channel='chatroom', message=message)
            await broadcast.publish(event)


async def handle_chat_event(event, websocket):
    await websocket.send_text(event.message)


routes = [
    Route("/", homepage),
    WebSocketRoute("/", chatroom_ws),
]


app = Starlette(
    routes=routes,
    on_startup=[broadcast.connect],
    on_shutdown=[broadcast.disconnect],
)
```
