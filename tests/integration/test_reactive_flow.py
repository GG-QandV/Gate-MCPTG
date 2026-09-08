import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from tests.conftest import make_event


# ── bridge → tmux push ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_bridge_pushes_message_via_tmux(inbox):
    """Full flow: TG message → InboxEngine → InboxBridge → tmux paste-buffer."""
    from src.mcp_telegram.inbox_bridge import InboxBridge

    pane = "agent:opencode"
    bridge = InboxBridge(inbox=inbox, topic_map=[(100, 205, "tmux", pane)], retry_max=1, retry_delay=0.05)

    # Patch subprocess так, чтобы has-session(0) + load-buffer(0) + paste-buffer(0) + send-keys(0)
    def _proc(rc=0):
        p = AsyncMock()
        p.returncode = rc
        p.communicate = AsyncMock(return_value=(b"", b""))
        p.wait = AsyncMock(return_value=rc)
        return p

    exec_calls = []

    async def fake_exec(*args, **kwargs):
        exec_calls.append(args)
        return _proc(0)

    bridge_task = asyncio.create_task(bridge.start())

    with patch("asyncio.create_subprocess_exec", side_effect=fake_exec):
        await inbox.handle(make_event(100, 205, 1, "hello from bridge"))
        await asyncio.sleep(0.3)

    bridge_task.cancel()
    try:
        await bridge_task
    except asyncio.CancelledError:
        pass

    # Убеждаемся что tmux has-session и paste-buffer были вызваны
    cmds = [" ".join(str(a) for a in call) for call in exec_calls]
    assert any("has-session" in c for c in cmds), f"has-session not called: {cmds}"
    assert any("paste-buffer" in c for c in cmds), f"paste-buffer not called: {cmds}"
    assert any(pane in c for c in cmds), f"pane {pane} not in any call: {cmds}"


@pytest.mark.asyncio
async def test_bridge_pane_missing_msgs_stay_in_store(inbox_store, tmp_path):
    """УМ-Л + УМ-И: pane недоступен → msgs не теряются, store не опустошается."""
    from src.mcp_telegram.inbox_bridge import InboxBridge
    from src.mcp_telegram.inbox import InboxEngine

    real_inbox = InboxEngine(store=inbox_store, maxlen=10)
    await real_inbox.handle(make_event(100, 205, 1, "stay in store"))

    pane = "agent:opencode"
    bridge = InboxBridge(inbox=real_inbox, topic_map=[(100, 205, "tmux", pane)], retry_max=1, retry_delay=0.01)
    bridge._pane_exists = AsyncMock(return_value=False)
    bridge._do_paste = AsyncMock()

    # Запускаем _watch — он должен вернуть уже стоящие сообщения, обнаружить отсутствие pane
    # и уйти в сон (после clear_ram_buffer event сброшен, wait подвиснет)
    watch_task = asyncio.create_task(bridge._watch(100, 205, "tmux", pane))
    await asyncio.sleep(0.1)
    watch_task.cancel()
    try:
        await watch_task
    except asyncio.CancelledError:
        pass

    # paste не был вызван
    bridge._do_paste.assert_not_called()
    # ack не был вызван → сообщения остались в store
    remaining = await inbox_store.read_all(100, 205)
    assert len(remaining) == 1
    assert remaining[0]["text"] == "stay in store"


# ── store persistence (bridge-independent) ────────────────────────────────────

@pytest.mark.asyncio
async def test_message_survives_daemon_restart(tmp_path, mock_telegram_client):
    from src.mcp_telegram.inbox_store import InboxStore
    from src.mcp_telegram.inbox import InboxEngine

    store = InboxStore(str(tmp_path / "store"))
    inbox1 = InboxEngine(store=store, maxlen=10)

    await inbox1.handle(make_event(200, 310, 5, "survive restart"))

    inbox2 = InboxEngine(store=store, maxlen=10)
    restored = await inbox2.restore_from_store()
    assert restored == 1

    msgs = await inbox2.wait(200, 310)
    assert len(msgs) == 1
    assert msgs[0]["text"] == "survive restart"
    assert msgs[0]["id"] == 5


@pytest.mark.asyncio
async def test_ack_clears_store_and_buffer(inbox, inbox_store):
    await inbox.handle(make_event(100, 205, 1, "msg1"))
    await inbox.handle(make_event(100, 205, 2, "msg2"))

    assert len(await inbox.peek(100, 205)) == 2
    assert len(await inbox_store.read_all(100, 205)) == 2

    dropped = await inbox.ack(100, 205, 2)
    assert dropped == 2

    assert await inbox.peek(100, 205) == []
    assert await inbox_store.read_all(100, 205) == []
