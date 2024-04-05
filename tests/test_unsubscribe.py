import pytest

from broadcaster import Broadcast


@pytest.mark.asyncio
async def test_unsubscribe():
    """The queue should be removed when the context manager is left."""
    async with Broadcast("memory://") as broadcast:
        async with broadcast.subscribe("chatroom"):
            pass

        assert "chatroom" not in broadcast._subscribers


@pytest.mark.asyncio
async def test_unsubscribe_w_exception():
    """In case an exception is raised inside the context manager, the queue should be removed."""
    async with Broadcast("memory://") as broadcast:
        try:
            async with broadcast.subscribe("chatroom"):
                raise RuntimeError("MyException")
        except RuntimeError:
            pass

        assert "chatroom" not in broadcast._subscribers
