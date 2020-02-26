import os

import uvicorn
from starlette.applications import Starlette
from starlette.concurrency import run_until_first_complete
from starlette.routing import Route, WebSocketRoute
from starlette.templating import Jinja2Templates

from broadcaster import Broadcast

BROADCAST_URL = os.environ.get("BROADCAST_URL", "memory://")

broadcast = Broadcast(BROADCAST_URL)
templates = Jinja2Templates("example/templates")


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
    WebSocketRoute("/", chatroom_ws, name="chatroom_ws"),
]


app = Starlette(
    routes=routes, on_startup=[broadcast.connect], on_shutdown=[broadcast.disconnect],
)
