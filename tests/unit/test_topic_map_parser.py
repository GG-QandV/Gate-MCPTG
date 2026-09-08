"""Tests for TelegramSettings.topic_map parser — all transport types."""
import pytest
from src.mcp_telegram.telegram import TelegramSettings


@pytest.fixture(autouse=True)
def _telegram_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_API_ID", "12345")
    monkeypatch.setenv("TELEGRAM_API_HASH", "testhash")


# ── tmux ──────────────────────────────────────────────────────────────────────

def test_parse_tmux_entry(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "-1003998609906:205:agent:opencode")
    s = TelegramSettings()
    assert s.topic_map == [(-1003998609906, 205, "tmux", "agent:opencode")]


def test_parse_multiple_tmux(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "-100:1:sess1:win1,-100:2:sess2:win2")
    s = TelegramSettings()
    assert len(s.topic_map) == 2
    assert s.topic_map[0] == (-100, 1, "tmux", "sess1:win1")
    assert s.topic_map[1] == (-100, 2, "tmux", "sess2:win2")


# ── tui ───────────────────────────────────────────────────────────────────────

def test_parse_tui_entry(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "-1003998609906:3:tui:/tmp/hrm-tui-inject.sock")
    s = TelegramSettings()
    assert s.topic_map == [(-1003998609906, 3, "tui", "/tmp/hrm-tui-inject.sock")]


def test_parse_tui_transport_not_tmux(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "-100:3:tui:/tmp/hrm-tui-inject.sock")
    s = TelegramSettings()
    chat_id, topic_id, transport, target = s.topic_map[0]
    assert transport == "tui"
    assert target == "/tmp/hrm-tui-inject.sock"


# ── uds (legacy) ──────────────────────────────────────────────────────────────

def test_parse_uds_entry(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "-100:3:uds:/tmp/hrm-mirror.sock")
    s = TelegramSettings()
    assert s.topic_map == [(-100, 3, "uds", "/tmp/hrm-mirror.sock")]


# ── mixed ─────────────────────────────────────────────────────────────────────

def test_parse_mixed_tmux_and_tui(monkeypatch):
    raw = "-1003998609906:205:agent:opencode,-1003998609906:3:tui:/tmp/hrm-tui-inject.sock"
    monkeypatch.setenv("TG_TOPIC_MAP", raw)
    s = TelegramSettings()
    assert len(s.topic_map) == 2
    assert s.topic_map[0] == (-1003998609906, 205, "tmux", "agent:opencode")
    assert s.topic_map[1] == (-1003998609906, 3, "tui", "/tmp/hrm-tui-inject.sock")


# ── edge cases ────────────────────────────────────────────────────────────────

def test_empty_topic_map(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "")
    s = TelegramSettings()
    assert s.topic_map == []


def test_invalid_entry_skipped(monkeypatch, caplog):
    monkeypatch.setenv("TG_TOPIC_MAP", "bad_entry,-100:1:agent:opencode")
    with caplog.at_level("WARNING"):
        s = TelegramSettings()
    result = s.topic_map
    assert len(result) == 1
    assert result[0][2] == "tmux"
    assert "must have 4 fields" in caplog.text


def test_whitespace_trimmed(monkeypatch):
    monkeypatch.setenv("TG_TOPIC_MAP", "  -100:1:agent:opencode  ")
    s = TelegramSettings()
    assert len(s.topic_map) == 1
