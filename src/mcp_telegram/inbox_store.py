from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class InboxStore:
    def __init__(self, store_dir: str):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    def _path(self, chat_id: int, topic_id: int) -> Path:
        return self.store_dir / f"inbox_{chat_id}_{topic_id}.jsonl"

    async def append(self, chat_id: int, topic_id: int, msg: dict) -> None:
        path = self._path(chat_id, topic_id)
        async with self._lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(msg, ensure_ascii=False) + "\n")
                f.flush()

    async def read_all(self, chat_id: int, topic_id: int) -> list[dict]:
        path = self._path(chat_id, topic_id)
        async with self._lock:
            if not path.exists():
                return []
            msgs = []
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    msgs.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Corrupt line in %s, skipping", path)
            return msgs

    async def ack(self, chat_id: int, topic_id: int, last_id: int) -> int:
        path = self._path(chat_id, topic_id)
        async with self._lock:
            if not path.exists():
                return 0
            lines = path.read_text(encoding="utf-8").splitlines()
            kept = []
            dropped = 0
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                    if msg.get("id", 0) <= last_id:
                        dropped += 1
                    else:
                        kept.append(line)
                except json.JSONDecodeError:
                    logger.warning("Corrupt line in %s dropped during ack", path)
                    dropped += 1
            content = "\n".join(kept) + ("\n" if kept else "")
            tmp = path.with_suffix(".tmp")
            tmp.write_text(content, encoding="utf-8")
            tmp.replace(path)
            return dropped

    async def health_check(self) -> dict:
        result = {}
        async with self._lock:
            for f in sorted(self.store_dir.glob("inbox_*.jsonl")):
                lines = f.read_text(encoding="utf-8").splitlines()
                good = 0
                corrupt = 0
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        json.loads(line)
                        good += 1
                    except json.JSONDecodeError:
                        corrupt += 1
                result[f.name] = {"total": len(lines), "good": good, "corrupt": corrupt}
        return result
