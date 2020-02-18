# Broadcaster

Broadcaster provides an API around Redis PUB/SUB and Postgres LISTEN/NOTIFY to
make developing realtime broadcast

**app.py**

```python
broadcast = broadcast.Broadcast("memory://")
templates = Jinja2Templates(templates='templates')


async def homepage(request):
    return templates.TemplateResponse(...)


async def chat_websocket(websocket):
    async with broadcast.subscribe(channel='chat', callback=handle_chat_event, args=(websocket,)):
        async for message in websocket.iter_text():
            event = broadcast.Event(channel='chat', message=message)
            await broadcast.publish(event)


async def handle_chat_event(event, websocket):
    await websocket.send_text(event.message)


routes = [
    Route('/', homepage),
    WebSocketRoute('/', chat_websocket)
]

app = Starlette(routes=routes, on_startup=[broadcast.connect], on_shutdown=[broadcast.disconnect])
```
