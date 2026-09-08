import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_telegram_client():
    client = AsyncMock()
    client.send_message = AsyncMock(return_value=MagicMock(id=999))
    client.run_until_disconnected = AsyncMock()
    return client


@pytest.fixture
def tmp_sock_path(tmp_path):
    return str(tmp_path / "test_tgmcpd.sock")


@pytest.fixture
def inbox_store(tmp_path):
    from src.mcp_telegram.inbox_store import InboxStore
    return InboxStore(str(tmp_path / "store"))


@pytest.fixture
def inbox(inbox_store):
    from src.mcp_telegram.inbox import InboxEngine
    return InboxEngine(store=inbox_store, maxlen=10)


def make_event(chat_id: int, topic_id: int, msg_id: int, text: str):
    reply_to = MagicMock()
    reply_to.reply_to_top_id = topic_id
    reply_to.reply_to_msg_id = 0

    msg = MagicMock()
    msg.chat_id = chat_id
    msg.id = msg_id
    msg.text = text
    msg.sender_id = 42
    msg.date.timestamp.return_value = 1234567890.0
    msg.reply_to = reply_to

    event = MagicMock()
    event.chat_id = chat_id
    event.message = msg
    return event
