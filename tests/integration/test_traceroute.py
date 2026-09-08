import asyncio
import json
import os
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from tests.conftest import make_event

TG_CHAT_ID = 100
TG_TOPIC_ID = 205


@pytest_asyncio.fixture
async def ipc_stack(tmp_sock_path, inbox):
    mock_client = AsyncMock()
    mock_client.send_message = AsyncMock(return_value=MagicMock(id=42))

    from src.mcp_telegram.ipc_server import IPCServer
    from src.mcp_telegram.ipc_client import IPCClient

    server = IPCServer(inbox, mock_client)
    server_task = asyncio.create_task(server.start(tmp_sock_path))
    await asyncio.sleep(0.05)

    client = IPCClient(tmp_sock_path)
    await client.connect()

    yield server, client, mock_client

    await client.close()
    server_task.cancel()


def _reload_proxy():
    os.environ.setdefault("TG_TOPIC_ID", str(TG_TOPIC_ID))
    os.environ.setdefault("TG_CHAT_ID", str(TG_CHAT_ID))
    import src.mcp_telegram.proxy as m
    return m


@pytest.mark.asyncio
async def test_traceroute_send_message(ipc_stack):
    _, client, mock_client = ipc_stack
    proxy_mod = _reload_proxy()

    with patch.object(proxy_mod, "get_ipc", return_value=client):
        result = await proxy_mod.call_tool("send_message", {"text": "traceroute"})

    assert "42" in result[0].text
    args = mock_client.send_message.await_args.args
    assert "traceroute" in args[1]
    assert "**Agent**" in args[1]


@pytest.mark.asyncio
async def test_traceroute_inbox_read(ipc_stack):
    server, client, _ = ipc_stack

    await server.inbox.handle(make_event(TG_CHAT_ID, TG_TOPIC_ID, 1, "traceroute msg"))

    proxy_mod = _reload_proxy()
    with patch.object(proxy_mod, "get_ipc", return_value=client):
        result = await proxy_mod.call_tool("inbox_read", {})

    msgs = json.loads(result[0].text)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "traceroute msg"

    peek = await client.call("inbox_peek", {
        "chat_id": TG_CHAT_ID, "topic_id": TG_TOPIC_ID
    })
    assert peek["messages"] == []


@pytest.mark.asyncio
async def test_traceroute_send_message_ipc_error(ipc_stack):
    _, client, mock_client = ipc_stack

    mock_client.send_message = AsyncMock(
        side_effect=RuntimeError("PEER_FLOOD")
    )

    proxy_mod = _reload_proxy()
    with patch.object(proxy_mod, "get_ipc", return_value=client):
        with pytest.raises(RuntimeError, match="PEER_FLOOD"):
            await proxy_mod.call_tool("send_message", {"text": "boom"})


@pytest.mark.asyncio
async def test_traceroute_inbox_empty(ipc_stack):
    _, client, _ = ipc_stack

    proxy_mod = _reload_proxy()
    with patch.object(proxy_mod, "get_ipc", return_value=client):
        result = await proxy_mod.call_tool("inbox_read", {})

    assert result[0].text == "[]"
