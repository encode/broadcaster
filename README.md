# Broadcaster

Broadcaster provides an API around Redis PUB/SUB and Postgres LISTEN/NOTIFY to
make developing realtime broadcast

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
    async with broadcast.subscribe(channel='chatroom', callback=handle_chat_event, args=(websocket,)):
        async for message in websocket.iter_text()
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
