from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import socket
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

SD_LISTEN_FDS_START = 3


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


def sd_listen_fds() -> list[socket.socket]:
    """Parse LISTEN_PID/LISTEN_FDS per sd_listen_fds(3).

    Env is consumed exactly once (cleared on call). Returned sockets are
    non-inheritable (PEP 446). Extra fds beyond the first are closed.
    """
    pid = os.environ.pop("LISTEN_PID", None)
    n = os.environ.pop("LISTEN_FDS", None)
    os.environ.pop("LISTEN_FDNAMES", None)
    if pid != str(os.getpid()):
        return []
    try:
        count = int(n or "0")
    except ValueError:
        return []
    if count <= 0:
        return []
    socks = [socket.socket(fileno=SD_LISTEN_FDS_START + i) for i in range(count)]
    for extra in socks[1:]:
        logger.warning("tgmcpd: %d listen fds passed, keeping only the first", count)
        extra.close()
    return socks[:1]


def validate_activation_socket(s: socket.socket, expected_path: str) -> None:
    """Exit 69 if systemd passed anything other than the expected listening socket."""
    name = s.getsockname()
    listening = s.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) == 1
    if name != expected_path or not listening:
        logger.error(
            "tgmcpd.socket passed fd for %r (listening=%s), expected %r",
            name, listening, expected_path,
        )
        raise SystemExit(69)


async def _check_stale_socket(sock_path: str) -> None:
    """Fallback mode only (instance lock already held).

    If the file exists and something listens on it, the socket is owned by
    tgmcpd.socket (systemd) — exit 69 without touching the file. Otherwise
    it is a stale leftover from a dead fallback process — unlink.
    """
    p = Path(sock_path)
    if not p.exists():
        return
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(sock_path), timeout=0.5
        )
        writer.close()
        logger.error(
            "Socket %s is owned by tgmcpd.socket — use: systemctl --user start tgmcpd",
            sock_path,
        )
        raise SystemExit(69)
    except (ConnectionRefusedError, OSError, asyncio.TimeoutError):
        logger.warning("Removing stale socket %s", sock_path)
        p.unlink(missing_ok=True)


async def main() -> None:
    cfg = TelegramSettings()
    sock_path = get_sock_path()

    acquire_instance_lock(sock_path)

    fds = sd_listen_fds()
    from_activation = bool(fds)
    if from_activation:
        validate_activation_socket(fds[0], sock_path)
        logger.info("tgmcpd: activation mode — socket owned by tgmcpd.socket")
    else:
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
        if from_activation:
            server = await ipc.bind(sock=fds[0])
        else:
            server = await ipc.bind(sock_path=sock_path)
    except OSError as e:
        logger.error(
            "Socket %s in use — owned by tgmcpd.socket? Use: systemctl --user start tgmcpd",
            sock_path,
        )
        raise SystemExit(69) from e

    try:
        await asyncio.gather(
            ipc.serve(server),
            client.run_until_disconnected(),
            bridge.start(),
        )
    finally:
        if not from_activation:
            Path(sock_path).unlink(missing_ok=True)
        logger.info(
            "tgmcpd stopped (socket owned by systemd)"
            if from_activation
            else "tgmcpd stopped, socket removed"
        )

if __name__ == "__main__":
    asyncio.run(main())
