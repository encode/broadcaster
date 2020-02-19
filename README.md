# Broadcaster

Broadcaster provides an API around Redis PUB/SUB and Postgres LISTEN/NOTIFY to
make developing realtime broadcast

**app.py**

```python
import subscribe
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.templating import Jinja2Templates
from starlette.websockets import WebSocketDisconnect


broadcast = subscribe.Broadcast('redis://localhost:6379')
templates = Jinja2Templates('templates')


async def homepage(request):
    template = 'chatroom.html'
    context = {'request': request}
    return templates.TemplateResponse(template, context)


async def chatroom_ws(websocket):
    async with broadcast.subscribe(group='chatroom', callback=handle_chat_event, args=(websocket,)):
        async for message in websocket.iter_text()
            await broadcast.publish(group='chatroom', message=message)


async def handle_chat_event(event, websocket):
    channel, message = event
    await websocket.send_text(message)


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
