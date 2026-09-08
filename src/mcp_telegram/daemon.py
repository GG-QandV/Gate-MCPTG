from __future__ import annotations

import asyncio
import fcntl
import logging
import os
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

_instance_lock_fp = None


def acquire_instance_lock(sock_path: str) -> None:
    """Exclusive single-instance lock (flock).

    Held for the whole process lifetime: while we own it, no other tgmcpd
    can run, so unlinking a leftover socket file is always safe. A second
    instance exits 69 before touching anything.
    """
    global _instance_lock_fp
    lock_path = str(Path(sock_path).with_suffix(".lock"))
    fp = open(lock_path, "w")
    try:
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        fp.close()
        logger.error("Another tgmcpd instance is already running (lock %s)", lock_path)
        raise SystemExit(69)
    fp.write(str(os.getpid()))
    fp.flush()
    os.fsync(fp.fileno())
    _instance_lock_fp = fp  # keep open until process exit


async def _check_stale_socket(sock_path: str) -> None:
    """Remove a leftover socket file.

    Safe only because the instance lock is already held: the previous
    owner is dead, so nothing can be listening on this path.
    """
    p = Path(sock_path)
    if not p.exists():
        return
    logger.warning("Removing stale socket %s", sock_path)
    p.unlink(missing_ok=True)


async def main() -> None:
    cfg = TelegramSettings()
    sock_path = get_sock_path()

    acquire_instance_lock(sock_path)
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
