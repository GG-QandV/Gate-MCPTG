from __future__ import annotations

import asyncio
import json
import logging

logger = logging.getLogger(__name__)


def _fmt_alert(msgs: list[dict]) -> str:
    previews = " | ".join(
        f"[{m.get('from','?')}]: {m.get('text','')[:2000].replace(chr(10),' ')}"
        for m in msgs
    )
    return f"⚡ INBOX [{len(msgs)}] · {previews} · inbox_read"


class InboxBridge:
    """Доставляет TG-сообщения агентам через два транспорта:

    - tmux  → paste-buffer -p (opencode, любой TUI без HTTP API)
    - ws    → WebSocket JSON-RPC prompt.submit (Hermes gateway)

    topic_map: [(chat_id, topic_id, transport, target), ...]
      transport="tmux", target="session:window"
      transport="ws",   target="8771"  (port as str)
    """

    def __init__(
        self,
        inbox: "InboxEngine",
        topic_map: list[tuple[int, int, str, str]],
        retry_max: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        self.inbox = inbox
        self.topic_map = topic_map
        self.retry_max = retry_max
        self.retry_delay = retry_delay

    async def start(self) -> None:
        if not self.topic_map:
            logger.warning("topic_map is empty, bridge has no topics to watch")
            return
        tasks = [self._watch(*entry) for entry in self.topic_map]
        await asyncio.gather(*tasks)

    async def _watch(
        self, chat_id: int, topic_id: int, transport: str, target: str
    ) -> None:
        while True:
            try:
                msgs = await self.inbox.wait(chat_id, topic_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("inbox.wait crashed: %s", e)
                await asyncio.sleep(5)
                continue

            if not msgs:
                continue

            if transport == "tmux":
                success = await self._push_tmux(target, msgs)
            elif transport == "tui":
                success = await self._push_tui(target, msgs)
            elif transport == "uds":
                success = await self._push_uds(target, msgs)
            else:
                logger.error("Unknown transport %r, dropping msgs", transport)
                success = False

            if success:
                logger.info("pushed %d msgs → %s %s", len(msgs), transport, target)
            else:
                logger.error(
                    "push failed chat=%d topic=%d, msgs stay in store",
                    chat_id, topic_id,
                )
            await self.inbox.clear_ram_buffer(chat_id, topic_id)

    # ── tmux transport ────────────────────────────────────────────────────────

    async def _push_tmux(self, pane: str, msgs: list[dict]) -> bool:
        if not await self._pane_exists(pane):
            logger.warning("tmux pane %s not found", pane)
            return False
        text = _fmt_alert(msgs)
        for attempt in range(self.retry_max):
            try:
                if await self._do_paste(pane, text):
                    logger.info("pushed %d msgs → tmux %s", len(msgs), pane)
                    return True
            except asyncio.TimeoutError:
                logger.warning("paste timeout attempt %d/%d pane=%s", attempt + 1, self.retry_max, pane)
            except Exception as e:
                logger.warning("paste error attempt %d/%d pane=%s: %s", attempt + 1, self.retry_max, pane, e)
            if attempt < self.retry_max - 1:
                await asyncio.sleep(self.retry_delay)
        return False

    async def _pane_exists(self, pane: str) -> bool:
        try:
            p = await asyncio.create_subprocess_exec(
                "tmux", "has-session", "-t", pane,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(p.wait(), timeout=3.0)
            return p.returncode == 0
        except Exception:
            return False

    async def _do_paste(self, pane: str, text: str) -> bool:
        p1 = await asyncio.create_subprocess_exec(
            "tmux", "load-buffer", "-",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err1 = await asyncio.wait_for(
            p1.communicate(input=text.encode("utf-8")), timeout=5.0
        )
        if p1.returncode != 0:
            logger.warning("load-buffer failed: %s", err1.decode().strip())
            return False

        # -p = bracketed paste: bubbletea буферизирует весь текст как единый ввод
        p2 = await asyncio.create_subprocess_exec(
            "tmux", "paste-buffer", "-p", "-t", pane,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err2 = await asyncio.wait_for(p2.communicate(), timeout=5.0)
        if p2.returncode != 0:
            logger.warning("paste-buffer failed: %s", err2.decode().strip())
            return False

        p3 = await asyncio.create_subprocess_exec(
            "tmux", "send-keys", "-t", pane, "Enter",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(p3.wait(), timeout=3.0)
        return True

    # ── TUI transport (Hermes tui_gateway inject server) ─────────────────────

    async def _push_tui(self, sock_path: str, msgs: list[dict]) -> bool:
        text = _fmt_alert(msgs)
        for attempt in range(self.retry_max):
            try:
                if await self._do_tui_push(sock_path, text):
                    logger.info("pushed %d msgs → hermes tui:%s", len(msgs), sock_path)
                    return True
                logger.warning("tui push attempt %d/%d failed", attempt + 1, self.retry_max)
            except Exception as e:
                logger.warning("tui push error attempt %d/%d: %s", attempt + 1, self.retry_max, e)
            if attempt < self.retry_max - 1:
                await asyncio.sleep(self.retry_delay)
        return False

    async def _do_tui_push(self, sock_path: str, text: str) -> bool:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(sock_path), timeout=3.0
            )
        except (FileNotFoundError, ConnectionRefusedError) as e:
            logger.warning("hermes tui inject sock unavailable: %s", e)
            return False
        try:
            writer.write(text.encode("utf-8"))
            await asyncio.wait_for(writer.drain(), timeout=3.0)
            writer.write_eof()
            # Wait for ack {"result":"queued"} or {"error":...}
            resp_raw = await asyncio.wait_for(reader.read(256), timeout=5.0)
            resp_text = resp_raw.decode("utf-8", errors="replace").strip()
            if '"error"' in resp_text:
                logger.warning("tui inject server returned error: %s", resp_text)
                return False
            return True
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # ── UDS transport (Hermes mirror_listen) ─────────────────────────────────

    async def _push_uds(self, sock_path: str, msgs: list[dict]) -> bool:
        text = _fmt_alert(msgs)
        for attempt in range(self.retry_max):
            try:
                if await self._do_uds_push(sock_path, text):
                    logger.info("pushed %d msgs → hermes uds:%s", len(msgs), sock_path)
                    return True
                logger.warning("uds push attempt %d/%d failed", attempt + 1, self.retry_max)
            except Exception as e:
                logger.warning("uds push error attempt %d/%d: %s", attempt + 1, self.retry_max, e)
            if attempt < self.retry_max - 1:
                await asyncio.sleep(self.retry_delay)
        return False

    async def _do_uds_push(self, sock_path: str, text: str) -> bool:
        # Формат совместим с tg_mirror.py → mirror_listen.py → FIFO → Hermes stdin
        line = f"[TG→CLI] uid=0 tgmcpd: {text}\n"
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(sock_path), timeout=3.0
            )
        except (FileNotFoundError, ConnectionRefusedError) as e:
            logger.warning("hermes mirror sock unavailable: %s", e)
            return False
        try:
            writer.write(line.encode("utf-8"))
            await asyncio.wait_for(writer.drain(), timeout=3.0)
            return True
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
