import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from tests.conftest import make_event


@pytest_asyncio.fixture
async def running_server(tmp_sock_path, inbox, mock_telegram_client):
    from src.mcp_telegram.ipc_server import IPCServer
    srv = IPCServer(inbox, mock_telegram_client)
    task = asyncio.create_task(srv.start(tmp_sock_path))
    await asyncio.sleep(0.05)
    yield tmp_sock_path
    task.cancel()


@pytest.mark.asyncio
async def test_roundtrip_ping(running_server):
    from src.mcp_telegram.ipc_client import IPCClient
    client = IPCClient(running_server)
    await client.connect()
    result = await client.call("ping", {})
    assert result == {"pong": True}
    await client.close()


@pytest.mark.asyncio
async def test_roundtrip_peek_ack(running_server, inbox):
    from src.mcp_telegram.ipc_client import IPCClient

    await inbox.handle(make_event(100, 205, 1, "round_trip"))

    client = IPCClient(running_server)
    await client.connect()

    peek = await client.call("inbox_peek", {"chat_id": 100, "topic_id": 205})
    assert len(peek["messages"]) == 1

    ack = await client.call("inbox_ack", {
        "chat_id": 100, "topic_id": 205, "last_id": 1,
    })
    assert ack["dropped"] == 1

    peek2 = await client.call("inbox_peek", {"chat_id": 100, "topic_id": 205})
    assert peek2["messages"] == []

    await client.close()


@pytest.mark.asyncio
async def test_concurrent_clients(running_server, inbox):
    from src.mcp_telegram.ipc_client import IPCClient

    async def one_client():
        c = IPCClient(running_server)
        await c.connect()
        r = await c.call("ping", {})
        await c.close()
        return r

    results = await asyncio.gather(*[one_client() for _ in range(5)])
    assert all(r == {"pong": True} for r in results)
