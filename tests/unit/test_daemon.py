import asyncio
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch


@pytest.mark.asyncio
async def test_stale_socket_removed_on_start(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket

    Path(tmp_sock_path).touch()
    assert Path(tmp_sock_path).exists()

    await _check_stale_socket(tmp_sock_path)

    assert not Path(tmp_sock_path).exists()


@pytest.mark.asyncio
async def test_live_socket_causes_exit(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket

    async def handler(r, w):
        pass
    server = await asyncio.start_unix_server(handler, path=tmp_sock_path)

    with pytest.raises(SystemExit) as excinfo:
        await _check_stale_socket(tmp_sock_path)
    assert excinfo.value.code == 69

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
        ipc_instance.start = AsyncMock()

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
        ipc_instance.start = AsyncMock()

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
