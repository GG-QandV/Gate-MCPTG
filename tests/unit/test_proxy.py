import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, patch


def _reload_proxy():
    """Import proxy module with env vars set."""
    os.environ.setdefault("TG_TOPIC_ID", "205")
    os.environ.setdefault("TG_CHAT_ID", "100")
    import src.mcp_telegram.proxy as m
    return m


@pytest.mark.asyncio
async def test_inbox_read_peek_then_ack():
    proxy_mod = _reload_proxy()

    calls = []

    async def mock_ipc_call(method, params):
        calls.append(method)
        if method == "inbox_peek":
            return {"messages": [{"id": 5, "text": "hi", "from": "1", "ts": 0}]}
        if method == "inbox_ack":
            assert params["last_id"] == 5
            return {"dropped": 1}
        return {}

    mock_ipc = AsyncMock()
    mock_ipc.call = mock_ipc_call

    with patch.object(proxy_mod, "get_ipc", return_value=mock_ipc):
        result = await proxy_mod.call_tool("inbox_read", {})
        assert calls == ["inbox_peek", "inbox_ack"]
        msgs = json.loads(result[0].text)
        assert msgs[0]["text"] == "hi"


@pytest.mark.asyncio
async def test_inbox_read_empty_no_ack():
    proxy_mod = _reload_proxy()

    calls = []

    async def mock_ipc_call(method, params):
        calls.append(method)
        if method == "inbox_peek":
            return {"messages": []}
        return {}

    mock_ipc = AsyncMock()
    mock_ipc.call = mock_ipc_call

    with patch.object(proxy_mod, "get_ipc", return_value=mock_ipc):
        result = await proxy_mod.call_tool("inbox_read", {})
        assert calls == ["inbox_peek"]
        assert result[0].text == "[]"


@pytest.mark.asyncio
async def test_send_message_tool():
    proxy_mod = _reload_proxy()

    async def mock_ipc_call(method, params):
        assert method == "send_message"
        assert "test text" in params["text"]
        assert "**Agent**" in params["text"]
        assert params["topic_id"] == 205
        return {"message_id": 777}

    mock_ipc = AsyncMock()
    mock_ipc.call = mock_ipc_call

    with patch.object(proxy_mod, "get_ipc", return_value=mock_ipc):
        result = await proxy_mod.call_tool("send_message", {"text": "test text"})
        assert "777" in result[0].text
