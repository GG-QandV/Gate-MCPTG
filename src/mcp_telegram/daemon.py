from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from telethon import TelegramClient, events

from .inbox import InboxEngine
from .inbox_bridge import InboxBridge
from .inbox_store import InboxStore
from .ipc_server import IPCServer, get_sock_path
from .telegram import TelegramSettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def _check_stale_socket(sock_path: str) -> None:
    p = Path(sock_path)
    if not p.exists():
        return
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(sock_path), timeout=1.0
        )
        writer.close()
        logger.error("Another tgmcpd instance is already running")
        sys.exit(69)
    except (ConnectionRefusedError, OSError):
        logger.warning("Removing stale socket %s", sock_path)
        p.unlink(missing_ok=True)


async def main() -> None:
    cfg = TelegramSettings()
    sock_path = get_sock_path()

    await _check_stale_socket(sock_path)

    session_path = (
        str(Path(cfg.session_path).parent / "bot_session")
        if cfg.bot_token
        else cfg.session_path
    )

    session_dir = Path(session_path).parent
    session_dir.mkdir(parents=True, exist_ok=True)
    for f in session_dir.glob("*.session-journal"):
        try:
            f.unlink()
        except OSError:
            pass

    store = InboxStore(store_dir=cfg.store_dir)

    client = TelegramClient(session_path, cfg.api_id, cfg.api_hash)
    if cfg.bot_token:
        await client.start(bot_token=cfg.bot_token)
    else:
        await client.start()
    logger.info("Telegram connected")

    inbox = InboxEngine(store=store)
    restored = await inbox.restore_from_store()
    if restored:
        logger.warning("Restored %d unread messages from disk after restart", restored)

    client.add_event_handler(inbox.handle, events.NewMessage)

    ipc = IPCServer(inbox, client)

    bridge = InboxBridge(
        inbox=inbox,
        topic_map=cfg.topic_map,
    )

    try:
        await asyncio.gather(
            ipc.start(sock_path),
            client.run_until_disconnected(),
            bridge.start(),
        )
    finally:
        Path(sock_path).unlink(missing_ok=True)
        logger.info("tgmcpd stopped, socket removed")


if __name__ == "__main__":
    asyncio.run(main())
