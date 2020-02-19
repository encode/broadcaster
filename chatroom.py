from broadcaster import Broadcast, Event
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.templating import Jinja2Templates
from starlette.websockets import WebSocketDisconnect


broadcast = Broadcast("redis://localhost:6379")
templates = Jinja2Templates("templates")


async def homepage(request):
    template = "chatroom.html"
    context = {"request": request}
    return templates.TemplateResponse(template, context)


async def chatroom_ws(websocket):
    await websocket.accept()

    async with broadcast.subscribe(
        channel="chatroom", callback=handle_chat_event, args=(websocket,)
    ):
        try:
            while True:
                message = await websocket.receive_text()
                event = Event(channel="chatroom", message=message)
                await broadcast.publish(event)
        except WebSocketDisconnect:
            await websocket.close()


async def handle_chat_event(event, websocket):
    await websocket.send_text(event.message)


routes = [
    Route("/", homepage),
    WebSocketRoute("/", chatroom_ws),
]


app = Starlette(
    routes=routes, on_startup=[broadcast.connect], on_shutdown=[broadcast.disconnect],
)
