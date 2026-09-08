import asyncio
import json
import pytest
from unittest.mock import AsyncMock
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_dispatch_ping(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({"method": "ping", "params": {}})
    assert result == {"pong": True}


@pytest.mark.asyncio
async def test_dispatch_send_message(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "send_message",
        "params": {"chat_id": 100, "topic_id": 205, "text": "hello"},
    })
    assert result["message_id"] == 999
    mock_telegram_client.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_inbox_peek(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    await inbox.handle(make_event(100, 205, 1, "test"))
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "inbox_peek",
        "params": {"chat_id": 100, "topic_id": 205},
    })
    assert len(result["messages"]) == 1


@pytest.mark.asyncio
async def test_dispatch_inbox_ack(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    for i in range(1, 4):
        await inbox.handle(make_event(100, 205, i, f"m{i}"))
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({
        "method": "inbox_ack",
        "params": {"chat_id": 100, "topic_id": 205, "last_id": 2},
    })
    assert result["dropped"] == 2


@pytest.mark.asyncio
async def test_dispatch_unknown_method(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    with pytest.raises(ValueError, match="Unknown method"):
        await srv._dispatch({"method": "nonexistent", "params": {}})


@pytest.mark.asyncio
async def test_concurrent_dispatch_not_blocked(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer

    async def slow_send(*args, **kwargs):
        await asyncio.sleep(0.3)
        from unittest.mock import MagicMock
        return MagicMock(id=1)

    mock_telegram_client.send_message = slow_send
    srv = IPCServer(inbox, mock_telegram_client)

    t_start = asyncio.get_event_loop().time()

    send_task = asyncio.create_task(srv._dispatch({
        "method": "send_message",
        "params": {"chat_id": 100, "topic_id": 205, "text": "slow"},
    }))
    ping_task = asyncio.create_task(srv._dispatch({
        "method": "ping", "params": {},
    }))

    ping_result = await ping_task
    t_ping = asyncio.get_event_loop().time() - t_start

    assert ping_result == {"pong": True}
    assert t_ping < 0.1

    await send_task


@pytest.mark.asyncio
async def test_dispatch_health_returns_status(mock_telegram_client, inbox):
    from src.mcp_telegram.ipc_server import IPCServer
    mock_telegram_client.is_connected = lambda: True
    srv = IPCServer(inbox, mock_telegram_client)
    result = await srv._dispatch({"method": "health", "params": {}})
    assert result["status"] == "ok"
    assert result["telegram"] is True
    assert isinstance(result["store"], dict)
    assert isinstance(result["ram_buffers"], dict)
