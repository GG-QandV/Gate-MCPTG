"""Регресс: политика подъёма после рестарта (2026-11-11).

- RAM: только свежие (<=4ч) и с маркером-исключением.
- Store: старше 2 суток без маркера — удаляются.
- Середина (4ч–2 суток): остаётся на диске, в RAM не поднимается.
"""

import time

import pytest


def _entry(msg_id: int, ts: float, text: str = "m"):
    return {"id": msg_id, "text": text, "from": "1",
            "ts": int(ts), "topic": 205}


@pytest.mark.asyncio
async def test_restore_policy_buckets(inbox, inbox_store):
    now = time.time()
    await inbox_store.append(100, 205, _entry(1, now - 100, "fresh"))          # RAM+store
    await inbox_store.append(100, 205, _entry(2, now - 5 * 3600, "middle"))   # store only
    await inbox_store.append(100, 205, _entry(3, now - 3 * 86400, "old"))     # deleted
    await inbox_store.append(100, 205, _entry(4, now - 3 * 86400, "не удалять!"))  # RAM+store
    await inbox_store.append(100, 205, _entry(5, 0, "no-ts"))                  # ts=0 → RAM+store

    lifted = await inbox.restore_from_store(now=now)

    assert lifted == 3
    assert [m["id"] for m in await inbox.peek(100, 205)] == [1, 4, 5]
    assert [m["id"] for m in await inbox_store.read_all(100, 205)] == [1, 2, 4, 5]


@pytest.mark.asyncio
async def test_restore_empty_store(inbox):
    assert await inbox.restore_from_store() == 0
    assert await inbox.peek(100, 205) == []
