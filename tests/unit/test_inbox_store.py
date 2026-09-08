import pytest
from pathlib import Path


@pytest.fixture
def store(tmp_path):
    from src.mcp_telegram.inbox_store import InboxStore
    return InboxStore(str(tmp_path / "store"))


@pytest.mark.asyncio
async def test_append_persists_to_disk(store):
    await store.append(-100, 205, {"id": 1, "text": "hello"})
    path = store._path(-100, 205)
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert "hello" in content
    assert '"id": 1' in content


@pytest.mark.asyncio
async def test_read_all_returns_all(store):
    await store.append(-100, 205, {"id": 1, "text": "a"})
    await store.append(-100, 205, {"id": 2, "text": "b"})
    msgs = await store.read_all(-100, 205)
    assert len(msgs) == 2
    assert msgs[0]["text"] == "a"
    assert msgs[1]["text"] == "b"


@pytest.mark.asyncio
async def test_ack_removes_up_to_last_id(store):
    for i in range(5):
        await store.append(-100, 205, {"id": i, "text": str(i)})
    dropped = await store.ack(-100, 205, last_id=2)
    assert dropped == 3
    remaining = await store.read_all(-100, 205)
    assert [m["id"] for m in remaining] == [3, 4]


@pytest.mark.asyncio
async def test_ack_atomic_no_data_loss(store):
    await store.append(-100, 205, {"id": 1, "text": "keep"})
    await store.append(-100, 205, {"id": 2, "text": "keep"})
    path = store._path(-100, 205)
    before = path.stat().st_ino

    await store.ack(-100, 205, last_id=1)

    assert path.exists()
    after = path.stat().st_ino
    assert after != before
    remaining = await store.read_all(-100, 205)
    assert len(remaining) == 1
    assert remaining[0]["id"] == 2


@pytest.mark.asyncio
async def test_corrupt_line_skipped(store):
    path = store._path(-100, 205)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"id": 1, "text": "good"}\nnot json\n{"id": 2, "text": "good"}\n', encoding="utf-8")
    msgs = await store.read_all(-100, 205)
    assert len(msgs) == 2
    assert msgs[0]["id"] == 1
    assert msgs[1]["id"] == 2


@pytest.mark.asyncio
async def test_health_check_detects_corrupt(store):
    path = store._path(-100, 205)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"id": 1}\nbroken\n', encoding="utf-8")
    health = await store.health_check()
    fname = path.name
    assert fname in health
    assert health[fname]["good"] == 1
    assert health[fname]["corrupt"] == 1


@pytest.mark.asyncio
async def test_multiple_topics_separate_files(store):
    await store.append(-100, 205, {"id": 1, "text": "topic205"})
    await store.append(-100, 310, {"id": 1, "text": "topic310"})
    msgs205 = await store.read_all(-100, 205)
    msgs310 = await store.read_all(-100, 310)
    assert msgs205[0]["text"] == "topic205"
    assert msgs310[0]["text"] == "topic310"
    assert store._path(-100, 205).name != store._path(-100, 310).name
