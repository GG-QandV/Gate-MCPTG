import asyncio
import logging
import pytest
from unittest.mock import AsyncMock, MagicMock
from tests.conftest import make_event


@pytest.mark.asyncio
async def test_handle_stores_message(inbox):
    event = make_event(chat_id=100, topic_id=205, msg_id=1, text="hello")
    await inbox.handle(event)
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "hello"
    assert msgs[0]["id"] == 1


@pytest.mark.asyncio
async def test_peek_does_not_drain(inbox):
    event = make_event(100, 205, 1, "hello")
    await inbox.handle(event)
    await inbox.peek(100, 205)
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_ack_removes_up_to_last_id(inbox):
    for i in range(1, 6):
        await inbox.handle(make_event(100, 205, i, f"msg{i}"))

    dropped = await inbox.ack(100, 205, last_id=3)
    assert dropped == 3
    remaining = await inbox.peek(100, 205)
    assert [m["id"] for m in remaining] == [4, 5]


@pytest.mark.asyncio
async def test_ack_all(inbox):
    for i in range(1, 4):
        await inbox.handle(make_event(100, 205, i, f"msg{i}"))
    await inbox.ack(100, 205, last_id=999)
    assert await inbox.peek(100, 205) == []


@pytest.mark.asyncio
async def test_ack_empty_buffer(inbox):
    dropped = await inbox.ack(100, 205, last_id=1)
    assert dropped == 0


@pytest.mark.asyncio
async def test_peek_empty(inbox):
    assert await inbox.peek(999, 999) == []


@pytest.mark.asyncio
async def test_different_chats_isolated(inbox):
    await inbox.handle(make_event(chat_id=100, topic_id=0, msg_id=1, text="chat_A"))
    await inbox.handle(make_event(chat_id=200, topic_id=0, msg_id=2, text="chat_B"))

    msgs_a = await inbox.peek(100, 0)
    msgs_b = await inbox.peek(200, 0)

    assert len(msgs_a) == 1 and msgs_a[0]["text"] == "chat_A"
    assert len(msgs_b) == 1 and msgs_b[0]["text"] == "chat_B"


@pytest.mark.asyncio
async def test_different_topics_isolated(inbox):
    await inbox.handle(make_event(100, 205, 1, "topic_205"))
    await inbox.handle(make_event(100, 310, 2, "topic_310"))

    assert len(await inbox.peek(100, 205)) == 1
    assert len(await inbox.peek(100, 310)) == 1


@pytest.mark.asyncio
async def test_topic_id_zero_no_collision(inbox):
    await inbox.handle(make_event(100, 0, 1, "from_chat_100"))
    await inbox.handle(make_event(200, 0, 2, "from_chat_200"))

    assert await inbox.peek(100, 0) != await inbox.peek(200, 0)


@pytest.mark.asyncio
async def test_overflow_drops_oldest(inbox):
    for i in range(15):
        await inbox.handle(make_event(100, 205, i, f"msg{i}"))
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 10
    assert msgs[0]["id"] == 5


@pytest.mark.asyncio
async def test_overflow_logs_warning(inbox, caplog):
    with caplog.at_level(logging.WARNING, logger="src.mcp_telegram.inbox"):
        for i in range(12):
            await inbox.handle(make_event(100, 205, i, f"msg{i}"))
    assert any("overflow" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_handle_reply_to_none(inbox):
    event = make_event(100, 0, 1, "no_thread")
    event.message.reply_to = None
    await inbox.handle(event)
    msgs = await inbox.peek(100, 0)
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_handle_persists_to_store(inbox, inbox_store):
    event = make_event(100, 205, 1, "persist me")
    await inbox.handle(event)
    stored = await inbox_store.read_all(100, 205)
    assert len(stored) == 1
    assert stored[0]["text"] == "persist me"


@pytest.mark.asyncio
async def test_restore_loads_unread_on_start(inbox_store):
    await inbox_store.append(100, 205, {"id": 1, "text": "survivor"})
    inbox = __import__("src.mcp_telegram.inbox", fromlist=[""]).InboxEngine(
        store=inbox_store, maxlen=10
    )
    restored = await inbox.restore_from_store()
    assert restored == 1
    msgs = await inbox.peek(100, 205)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "survivor"


@pytest.mark.asyncio
async def test_restore_fires_event_if_unread(inbox_store):
    await inbox_store.append(100, 205, {"id": 1, "text": "trigger"})
    inbox = __import__("src.mcp_telegram.inbox", fromlist=[""]).InboxEngine(
        store=inbox_store, maxlen=10
    )
    await inbox.restore_from_store()
    msgs = await inbox.wait(100, 205)
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_wait_wakes_on_event(inbox):
    async def delayed_handle():
        await asyncio.sleep(0.1)
        await inbox.handle(make_event(100, 205, 99, "wake up"))

    asyncio.create_task(delayed_handle())
    msgs = await inbox.wait(100, 205)
    assert len(msgs) == 1
    assert msgs[0]["id"] == 99


@pytest.mark.asyncio
async def test_wait_returns_existing_immediately(inbox):
    await inbox.handle(make_event(100, 205, 1, "already here"))
    msgs = await inbox.wait(100, 205)
    assert len(msgs) == 1
    assert msgs[0]["id"] == 1


@pytest.mark.asyncio
async def test_wait_no_race_message_before_wait(inbox):
    await inbox.handle(make_event(100, 205, 1, "before"))
    ev = inbox._events[(100, 205)]
    ev.clear()
    msgs = await inbox.wait(100, 205)
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_wait_no_race_message_during_clear(inbox):
    original_clear = asyncio.Event.clear

    async def risky_clear():
        ev = inbox._events[(100, 205)]
        ev.clear()
        await asyncio.sleep(0)
        await inbox.handle(make_event(100, 205, 1, "during clear"))

    asyncio.create_task(risky_clear())
    msgs = await inbox.wait(100, 205)
    assert len(msgs) == 1


@pytest.mark.asyncio
async def test_ack_syncs_store_and_buffer(inbox, inbox_store):
    await inbox.handle(make_event(100, 205, 1, "one"))
    await inbox.handle(make_event(100, 205, 2, "two"))
    dropped = await inbox.ack(100, 205, last_id=1)
    assert dropped == 1
    remaining = await inbox.peek(100, 205)
    assert len(remaining) == 1
    assert remaining[0]["id"] == 2
    stored = await inbox_store.read_all(100, 205)
    assert len(stored) == 1
    assert stored[0]["id"] == 2


@pytest.mark.asyncio
async def test_wait_is_cancellable(inbox):
    """wait() on an empty inbox blocks indefinitely — no polling.
    The only way to stop it is CancelledError (e.g. bridge shutdown).
    """
    task = asyncio.create_task(inbox.wait(999, 999))
    await asyncio.sleep(0.05)  # confirm it is still running
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_wait_clears_event_on_entry(inbox):
    """wait() should clear the event on entry to avoid spin loop on empty buffer."""
    key = (999, 999)
    ev = inbox._events[key]
    ev.set()  # Взводим событие вручную
    
    # С пустым буфером вызов wait() должен сбросить событие и заблокироваться
    task = asyncio.create_task(inbox.wait(999, 999))
    await asyncio.sleep(0.05)
    
    # Если событие сброшено, задача все еще висит
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
