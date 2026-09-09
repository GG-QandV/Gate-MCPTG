from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

STORE_MAX_RECORDS = 10_000
HEALTH_CACHE_TTL = 5.0


def _append_sync(path: Path, line: str, cap: int) -> int:
    """Serialize one record; enforce retention cap. Returns records dropped."""
    dropped = 0
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())
    if cap > 0:
        lines = path.read_text(encoding="utf-8").splitlines()
        good = [ln for ln in lines if ln.strip()]
        if len(good) > cap:
            dropped = len(good) - cap
            kept = good[-cap:]
            _rewrite(path, kept)
            logger.warning(
                "retention: %s capped at %d records, dropped %d oldest",
                path.name, cap, dropped,
            )
    return dropped


def _read_all_sync(path: Path, cap: int) -> list[dict]:
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
    if cap > 0 and len(msgs) > cap:
        msgs = msgs[-cap:]
    return msgs


def _rewrite(path: Path, kept_lines: list[str]) -> None:
    tmp = path.parent / (path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for ln in kept_lines:
            f.write(ln + "\n")
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def _ack_sync(path: Path, last_id: int) -> tuple[int, int]:
    """Filter records <= last_id. Returns (dropped, corrupt_quarantined)."""
    if not path.exists():
        return 0, 0
    kept: list[str] = []
    dropped = 0
    corrupt: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
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
            corrupt.append(line)
            dropped += 1
    if corrupt:
        qpath = path.parent / (path.name + ".corrupt")
        with qpath.open("a", encoding="utf-8") as q:
            for ln in corrupt:
                q.write(ln + "\n")
    _rewrite(path, kept)
    return dropped, len(corrupt)


def _health_sync(store_dir: Path) -> dict:
    result = {}
    for f in sorted(store_dir.glob("inbox_*.jsonl")):
        total = good = corrupt = 0
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                total += 1
                try:
                    json.loads(line)
                    good += 1
                except json.JSONDecodeError:
                    corrupt += 1
        except OSError as e:
            logger.warning("health: cannot read %s: %s", f.name, e)
            continue
        result[f.name] = {"total": total, "good": good, "corrupt": corrupt}
    return result


class InboxStore:
    def __init__(self, store_dir: str, max_records: int | None = None):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self.max_records = max_records if max_records is not None else STORE_MAX_RECORDS
        self.retention_dropped = 0
        self._health_cache: tuple[float, dict] | None = None

    def _path(self, chat_id: int, topic_id: int) -> Path:
        return self.store_dir / f"inbox_{chat_id}_{topic_id}.jsonl"

    async def append(self, chat_id: int, topic_id: int, msg: dict) -> None:
        path = self._path(chat_id, topic_id)
        line = json.dumps(msg, ensure_ascii=False)
        async with self._lock:
            dropped = await asyncio.to_thread(_append_sync, path, line, self.max_records)
            self.retention_dropped += dropped
            self._health_cache = None

    async def read_all(self, chat_id: int, topic_id: int) -> list[dict]:
        path = self._path(chat_id, topic_id)
        async with self._lock:
            return await asyncio.to_thread(_read_all_sync, path, self.max_records)

    async def ack(self, chat_id: int, topic_id: int, last_id: int) -> int:
        path = self._path(chat_id, topic_id)
        async with self._lock:
            dropped, _corrupt = await asyncio.to_thread(_ack_sync, path, last_id)
            self._health_cache = None
            return dropped

    async def health_check(self) -> dict:
        now = asyncio.get_event_loop().time()
        if self._health_cache is not None:
            ts, cached = self._health_cache
            if now - ts < HEALTH_CACHE_TTL:
                return cached
        async with self._lock:
            if self._health_cache is not None:
                ts, cached = self._health_cache
                if now - ts < HEALTH_CACHE_TTL:
                    return cached
            result = await asyncio.to_thread(_health_sync, self.store_dir)
            self._health_cache = (now, result)
            return result
