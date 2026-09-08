"""Socket activation (v0.9.3): fd adoption, no unlink in activation mode,
fallback probe semantics, client behaviour with dead service."""
from __future__ import annotations

import fcntl
import asyncio
import json
import os
import socket
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_telegram.daemon import (
    SD_LISTEN_FDS_START,
    _check_stale_socket,
    sd_listen_fds,
    validate_activation_socket,
)
from mcp_telegram.ipc_client import IPCClient
from mcp_telegram.ipc_server import IPCServer


@pytest.fixture
def listen_sock(tmp_path):
    path = str(tmp_path / "tgmcpd.sock")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(path)
    s.listen(8)
    yield s, path
    s.close()


def _dup_as_start_fd(s: socket.socket, monkeypatch) -> int:
    """Duplicate socket fd and point SD_LISTEN_FDS_START at it (pytest owns fd 3)."""
    fd = os.dup(s.fileno())
    monkeypatch.setattr("mcp_telegram.daemon.SD_LISTEN_FDS_START", fd)
    return fd


def test_sd_listen_fds_parses_own_pid(monkeypatch, listen_sock):
    s, path = listen_sock
    fd = _dup_as_start_fd(s, monkeypatch)
    monkeypatch.setenv("LISTEN_PID", str(os.getpid()))
    monkeypatch.setenv("LISTEN_FDS", "1")

    got = sd_listen_fds()
    assert len(got) == 1
    assert got[0].getsockname() == path

    # env consumed exactly once
    assert sd_listen_fds() == []
    got[0].close()  # socket(fileno=) owns the dup — this closes fd


def test_sd_listen_fds_pid_mismatch_fallback(monkeypatch, listen_sock):
    s, path = listen_sock
    fd = _dup_as_start_fd(s, monkeypatch)
    monkeypatch.setenv("LISTEN_PID", "999999")
    monkeypatch.setenv("LISTEN_FDS", "1")

    assert sd_listen_fds() == []
    os.close(fd)


def test_sd_listen_fds_absent_fallback(monkeypatch):
    monkeypatch.delenv("LISTEN_PID", raising=False)
    monkeypatch.delenv("LISTEN_FDS", raising=False)
    assert sd_listen_fds() == []


def test_sd_listen_fds_multiple_uses_first(monkeypatch):
    s1 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    # deterministic contiguous fd block far away from pytest's fds
    os.dup2(s1.fileno(), 1000)
    os.dup2(s2.fileno(), 1001)
    monkeypatch.setattr("mcp_telegram.daemon.SD_LISTEN_FDS_START", 1000)
    monkeypatch.setenv("LISTEN_PID", str(os.getpid()))
    monkeypatch.setenv("LISTEN_FDS", "2")

    try:
        got = sd_listen_fds()
        assert len(got) == 1
        assert got[0].fileno() == 1000
        # the wrapper around the extra fd (1001) was closed by sd_listen_fds
        with pytest.raises(OSError):
            fcntl.fcntl(1001, fcntl.F_GETFD)
        got[0].close()  # owns fd 1000
    finally:
        s1.close()
        s2.close()


def test_activation_fd_invalid_exit69(tmp_path):
    # bound but NOT listening
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    p = str(tmp_path / "sock")
    s.bind(p)
    with pytest.raises(SystemExit) as ei:
        validate_activation_socket(s, str(tmp_path / "other.sock"))
    assert ei.value.code == 69

    # listening but wrong path
    s.listen(1)
    with pytest.raises(SystemExit) as ei:
        validate_activation_socket(s, p + ".x")
    assert ei.value.code == 69
    s.close()


@pytest.mark.asyncio
async def test_ipc_server_bind_requires_exactly_one_arg():
    ipc = IPCServer(inbox=MagicMock(), client=MagicMock())
    with pytest.raises(ValueError):
        await ipc.bind()
    with pytest.raises(ValueError):
        await ipc.bind("/tmp/x.sock", sock=MagicMock())


@pytest.mark.asyncio
async def test_activation_mode_does_not_unlink_or_chmod(monkeypatch, listen_sock):
    s, path = listen_sock
    ipc = IPCServer(inbox=MagicMock(), client=MagicMock())

    with (
        patch("mcp_telegram.ipc_server.os.chmod") as m_chmod,
        patch.object(Path, "unlink", autospec=True) as m_unlink,
    ):
        server = await ipc.bind(sock=s)
        task = asyncio.create_task(ipc.serve(server))
        await asyncio.sleep(0.05)

        r, w = await asyncio.open_unix_connection(path)
        w.write(json.dumps({"method": "ping", "params": {}, "id": 1}).encode() + b"\n")
        await w.drain()
        line = await asyncio.wait_for(r.readline(), 2)
        assert json.loads(line)["result"]["pong"] is True
        w.close()

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        m_chmod.assert_not_called()
        m_unlink.assert_not_called()

    assert Path(path).exists()  # systemd owns the file — untouched
    server.close()


@pytest.mark.asyncio
async def test_fallback_probe_live_socket_exit69(listen_sock):
    s, path = listen_sock  # file exists with a live listener (systemd simulation)
    with pytest.raises(SystemExit) as ei:
        await _check_stale_socket(path)
    assert ei.value.code == 69
    assert Path(path).exists()  # file untouched


@pytest.mark.asyncio
async def test_ipc_client_refused_when_service_dead(tmp_path, monkeypatch):
    p = str(tmp_path / "tgmcpd.sock")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(p)
    s.close()  # file remains, no listener — service dead, socket unit alive

    monkeypatch.setattr("mcp_telegram.ipc_client.CONNECT_DELAY", 0)
    with pytest.raises(ConnectionRefusedError):
        await IPCClient(p).connect()
