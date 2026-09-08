from __future__ import annotations

import asyncio
import json
import logging
import os

logger = logging.getLogger(__name__)

CONNECT_RETRIES = 10
CONNECT_DELAY = 0.5
CALL_TIMEOUT = 120.0


def get_sock_path() -> str:
    explicit = os.environ.get("TGMCPD_SOCK")
    if explicit:
        return explicit
    runtime = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    return f"{runtime}/tgmcpd.sock"


class IPCClient:
    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._req_id = 0
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        for attempt in range(CONNECT_RETRIES):
            try:
                self._reader, self._writer = await asyncio.open_unix_connection(
                    self.sock_path
                )
                logger.info("IPC connected to %s", self.sock_path)
                return
            except OSError:
                logger.debug(
                    "IPC connect attempt %d/%d failed, retry in %.1fs",
                    attempt + 1, CONNECT_RETRIES, CONNECT_DELAY,
                )
                await asyncio.sleep(CONNECT_DELAY)
        raise ConnectionRefusedError(f"Cannot connect to tgmcpd at {self.sock_path}")

    async def _ensure_connected(self) -> None:
        if self._writer and not self._writer.is_closing():
            return
        logger.info("IPC reconnecting...")
        await self.connect()

    async def call(self, method: str, params: dict) -> dict:
        async with self._lock:
            for attempt in range(3):
                try:
                    await self._ensure_connected()

                    self._req_id += 1
                    req = {
                        "method": method,
                        "params": params,
                        "id": self._req_id,
                    }

                    self._writer.write(json.dumps(req).encode() + b"\n")
                    await self._writer.drain()

                    line = await asyncio.wait_for(
                        self._reader.readline(), timeout=CALL_TIMEOUT
                    )
                    if not line:
                        raise ConnectionResetError("daemon closed connection")

                    resp = json.loads(line)
                    if "error" in resp:
                        raise RuntimeError(resp["error"]["message"])
                    return resp["result"]

                except (ConnectionResetError, BrokenPipeError, OSError) as e:
                    logger.warning(
                        "IPC call failed attempt %d/3: %s", attempt + 1, e
                    )
                    self._writer = None
                    await asyncio.sleep(0.5 * attempt)

            raise RuntimeError("tgmcpd unavailable after 3 attempts")

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
