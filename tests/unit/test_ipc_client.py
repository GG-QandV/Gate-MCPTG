import asyncio
import json
import pytest


@pytest.mark.asyncio
async def test_req_id_increments(tmp_sock_path):
    from src.mcp_telegram.ipc_client import IPCClient

    async def fake_server(reader, writer):
        while True:
            line = await reader.readline()
            if not line:
                break
            req = json.loads(line)
            resp = {"result": {"pong": True}, "id": req["id"]}
            writer.write(json.dumps(resp).encode() + b"\n")
            await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(fake_server, path=tmp_sock_path)
    client = IPCClient(tmp_sock_path)
    await client.connect()

    assert client._req_id == 0
    await client.call("ping", {})
    assert client._req_id == 1
    await client.call("ping", {})
    assert client._req_id == 2

    await client.close()
    server.close()


@pytest.mark.asyncio
async def test_retry_on_connection_refused(tmp_sock_path):
    from src.mcp_telegram.ipc_client import IPCClient
    client = IPCClient(tmp_sock_path)

    with pytest.raises(RuntimeError, match="unavailable after 3 attempts"):
        await client.call("ping", {})


@pytest.mark.asyncio
async def test_reconnect_after_daemon_restart(tmp_sock_path):
    from src.mcp_telegram.ipc_client import IPCClient

    responses = [{"pong": True}]

    async def server_handler(reader, writer):
        line = await reader.readline()
        req = json.loads(line)
        resp = {"result": responses[0], "id": req["id"]}
        writer.write(json.dumps(resp).encode() + b"\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(server_handler, path=tmp_sock_path)
    client = IPCClient(tmp_sock_path)
    await client.connect()

    try:
        await client.call("ping", {})
    except Exception:
        pass

    server2 = await asyncio.start_unix_server(server_handler, path=tmp_sock_path)
    result = await client.call("ping", {})
    assert result == {"pong": True}

    await client.close()
    server.close()
    server2.close()


@pytest.mark.asyncio
async def test_connect_waits_for_daemon(tmp_sock_path):
    from src.mcp_telegram.ipc_client import IPCClient

    async def start_server_delayed():
        await asyncio.sleep(0.8)
        async def handler(r, w):
            w.close()
        return await asyncio.start_unix_server(handler, path=tmp_sock_path)

    server_task = asyncio.create_task(start_server_delayed())
    client = IPCClient(tmp_sock_path)

    await client.connect()
    await client.close()
    (await server_task).close()
