import asyncio
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_stale_socket_removed_on_start(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket

    Path(tmp_sock_path).touch()
    assert Path(tmp_sock_path).exists()

    await _check_stale_socket(tmp_sock_path)

    assert not Path(tmp_sock_path).exists()


@pytest.mark.asyncio
async def test_live_socket_causes_exit(tmp_sock_path):
    # v0.9.3: live-instance detection moved from connect-probe to flock.
    # While instance 1 holds the lock (socket live), instance 2 exits 69
    # BEFORE touching the socket file — the unlink race is impossible.
    from src.mcp_telegram.daemon import acquire_instance_lock

    async def handler(r, w):
        pass
    server = await asyncio.start_unix_server(handler, path=tmp_sock_path)

    acquire_instance_lock(tmp_sock_path)  # instance 1: live
    with pytest.raises(SystemExit) as excinfo:
        acquire_instance_lock(tmp_sock_path)  # instance 2: must die, not unlink
    assert excinfo.value.code == 69
    assert Path(tmp_sock_path).exists()  # socket untouched

    server.close()


@pytest.mark.asyncio
async def test_no_socket_no_error(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket
    await _check_stale_socket(tmp_sock_path)


def test_store_dir_in_settings(tmp_path):
    from src.mcp_telegram.telegram import TelegramSettings
    settings = TelegramSettings(
        api_id="1", api_hash="2",
        store_dir=str(tmp_path / "custom_store"),
    )
    assert settings.store_dir == str(tmp_path / "custom_store")


@pytest.mark.asyncio
async def test_restore_called_on_start(tmp_path):
    store_dir = str(tmp_path / "inbox_store")

    with (
        patch("src.mcp_telegram.daemon.TelegramSettings") as mock_settings,
        patch("src.mcp_telegram.daemon._check_stale_socket"),
        patch("src.mcp_telegram.daemon.acquire_instance_lock"),
        patch("src.mcp_telegram.daemon.sd_listen_fds", return_value=[]),
        patch("src.mcp_telegram.daemon.TelegramClient") as mock_client_class,
        patch("src.mcp_telegram.daemon.InboxEngine") as mock_inbox_class,
        patch("src.mcp_telegram.daemon.IPCServer") as mock_ipc_class,
    ):
        mock_settings.return_value.store_dir = store_dir
        mock_settings.return_value.session_path = str(tmp_path / "session")
        mock_settings.return_value.bot_token = None
        mock_settings.return_value.api_id = "123"
        mock_settings.return_value.api_hash = "abc"

        client_instance = mock_client_class.return_value
        client_instance.start = AsyncMock()
        client_instance.run_until_disconnected = AsyncMock(
            side_effect=asyncio.CancelledError()
        )

        ipc_instance = mock_ipc_class.return_value
        ipc_instance.bind = AsyncMock(return_value=MagicMock())
        ipc_instance.serve = AsyncMock()

        mock_inbox = mock_inbox_class.return_value
        mock_inbox.restore_from_store = AsyncMock(return_value=3)

        from src.mcp_telegram.daemon import main as daemon_main

        with pytest.raises(asyncio.CancelledError):
            await daemon_main()

        mock_inbox.restore_from_store.assert_awaited_once()
        assert mock_inbox_class.call_args[1]["store"] is not None


@pytest.mark.asyncio
async def test_bridge_created_and_started(tmp_path):
    from src.mcp_telegram.daemon import main as daemon_main

    with (
        patch("src.mcp_telegram.daemon.TelegramSettings") as mock_settings,
        patch("src.mcp_telegram.daemon._check_stale_socket"),
        patch("src.mcp_telegram.daemon.acquire_instance_lock"),
        patch("src.mcp_telegram.daemon.sd_listen_fds", return_value=[]),
        patch("src.mcp_telegram.daemon.TelegramClient") as mock_client_class,
        patch("src.mcp_telegram.daemon.InboxEngine") as mock_inbox_class,
        patch("src.mcp_telegram.daemon.IPCServer") as mock_ipc_class,
        patch("src.mcp_telegram.daemon.InboxBridge") as mock_bridge_class,
    ):
        mock_settings.return_value.store_dir = str(tmp_path / "store")
        mock_settings.return_value.session_path = str(tmp_path / "session")
        mock_settings.return_value.bot_token = None
        mock_settings.return_value.api_id = "123"
        mock_settings.return_value.api_hash = "abc"
        mock_settings.return_value.topic_map = [(1, 2, "agent:opencode"), (3, 4, "agent2:opencode")]

        client_instance = mock_client_class.return_value
        client_instance.start = AsyncMock()
        client_instance.run_until_disconnected = AsyncMock(
            side_effect=asyncio.CancelledError()
        )

        ipc_instance = mock_ipc_class.return_value
        ipc_instance.bind = AsyncMock(return_value=MagicMock())
        ipc_instance.serve = AsyncMock()

        mock_inbox = mock_inbox_class.return_value
        mock_inbox.restore_from_store = AsyncMock(return_value=0)

        bridge_instance = mock_bridge_class.return_value
        bridge_instance.start = AsyncMock()

        with pytest.raises(asyncio.CancelledError):
            await daemon_main()

        mock_bridge_class.assert_called_once_with(
            inbox=mock_inbox,
            topic_map=[(1, 2, "agent:opencode"), (3, 4, "agent2:opencode")],
        )
        bridge_instance.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_activation_uses_fd_no_unlink(tmp_path):
    """v0.9.3: with systemd-passed fd, main binds via sock= and never unlinks."""
    from src.mcp_telegram.daemon import main as daemon_main

    sock_path = str(tmp_path / "tgmcpd.sock")
    mock_fd = MagicMock()
    mock_fd.getsockname.return_value = sock_path
    mock_fd.getsockopt.return_value = 1  # SO_ACCEPTCONN: listening

    with (
        patch("src.mcp_telegram.daemon.TelegramSettings") as mock_settings,
        patch("src.mcp_telegram.daemon.acquire_instance_lock"),
        patch("src.mcp_telegram.daemon.get_sock_path", return_value=sock_path),
        patch("src.mcp_telegram.daemon.sd_listen_fds", return_value=[mock_fd]),
        patch("src.mcp_telegram.daemon.TelegramClient") as mock_client_class,
        patch("src.mcp_telegram.daemon.InboxEngine") as mock_inbox_class,
        patch("src.mcp_telegram.daemon.IPCServer") as mock_ipc_class,
        patch("src.mcp_telegram.daemon.InboxBridge") as mock_bridge_class,
        patch("src.mcp_telegram.daemon.Path.unlink", autospec=True) as m_unlink,
    ):
        mock_settings.return_value.store_dir = str(tmp_path / "store")
        mock_settings.return_value.session_path = str(tmp_path / "session")
        mock_settings.return_value.bot_token = None
        mock_settings.return_value.api_id = "123"
        mock_settings.return_value.api_hash = "abc"

        client_instance = mock_client_class.return_value
        client_instance.start = AsyncMock()
        client_instance.run_until_disconnected = AsyncMock(
            side_effect=asyncio.CancelledError()
        )

        ipc_instance = mock_ipc_class.return_value
        ipc_instance.bind = AsyncMock(return_value=MagicMock())
        ipc_instance.serve = AsyncMock()

        mock_inbox_class.return_value.restore_from_store = AsyncMock(return_value=0)
        mock_bridge_class.return_value.start = AsyncMock()

        with pytest.raises(asyncio.CancelledError):
            await daemon_main()

        ipc_instance.bind.assert_awaited_once_with(sock=mock_fd)
        ipc_instance.serve.assert_awaited_once()
        m_unlink.assert_not_called()  # activation mode: file owned by systemd
