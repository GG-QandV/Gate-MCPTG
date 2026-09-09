# MCP-TG — Telegram MCP Server with Reactive Daemon

**v0.9.4** · Standalone Telegram MCP server — daemon + proxy for multi-agent isolation.

[![Version 0.9.4](https://img.shields.io/badge/version-0.9.4-blue)](src/mcp_telegram/_version.py)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue)](setup.py)
[![MCP](https://img.shields.io/badge/MCP-stdio-green)](#)
[![License: MIT](LICENSE)](LICENSE)

Standalone MCP server for Telegram — 50+ tools over Model Context Protocol. One persistent MTProto connection (`tgmcpd` daemon, `Telethon`) serves N isolated agents via Unix-socket IPC proxies — each agent bound to its own Telegram topic.

## Overview

* **tgmcpd daemon** — one `Telethon` client, persistent MTProto session, `InboxEngine` with per-topic buffers + disk persistence (`~/.local/state/tgmcpd/inbox_store`).
* **tg-mcp-proxy** — thin stateless stdio↔IPC bridge per agent (`TG_TOPIC_ID` isolates topics).
* **Reactive inbox v0.9.2** — `asyncio.Event` wake-up + JSONL store + priority envelope, no polling.

## Architecture

### v0.9.2 — Reactive Inbox

```
Telegram push → Telethon event → InboxEngine.handle()
  → InboxStore.append(JSONL) → buffer[(chat,topic)].append()
  → asyncio.Event[(chat,topic)].set()
  → proxy inbox_wait() wakes → IPC → MCP inbox_read → envelope + ack
```

* `InboxStore` — per `(chat_id, topic_id)` JSONL in `~/.local/state/tgmcpd/inbox_store` (`TGMCPD_STORE_DIR`), `ack` via `tmp→replace` (atomic), survives restarts.
* `InboxEngine` — `defaultdict(deque)` buffers + `defaultdict(asyncio.Event)` per topic, `restore_from_store()` on daemon start.
* `InboxBridge` — push via `tmux`/`tui`/`uds` (`TG_TOPIC_MAP`), `clear_ram_buffer` after push (agent drains via `inbox_read`).

See `docs/tg-mcpd Architecture v0.9.2 — Reactive Inbox.md`.

## Tools — 50 total (MCP stdio, Telethon/MTProto)

**Chats** — `GetAllChats`, `GetChats`, `GetChatInfo`, `GetChatMembers`, `GetChatAdmins`, `GetChatOnlineCount`, `SearchChats`, `GetChatInviteLink`, `CheckChatInvite`, `JoinChatByInvite`, `LeaveChat`, `GetFolders`, `GetChatsFromFolder`, `GetForumTopics`, `AddChatMember`, `BanChatMember`, `UnbanChatMember`, `KickChatMember`, `PromoteToAdmin`

**Messages** — `SendMessage`, `ReplyToMessage`, `EditMessage`, `DeleteMessage`, `ForwardMessage`, `GetChatHistory`, `SearchMessages`, `MarkAsRead`, `PinMessage`, `UnpinMessage`, `SendFile`, `DownloadMedia`

**Contacts** — `GetContacts`, `AddContact`, `DeleteContact`, `SearchContacts`, `BlockUser`, `UnblockUser`, `GetBlockedUsers`, `SearchGlobal`

**Users** — `GetMe`, `GetUserInfo`, `GetUserStatus`, `SearchUsers`, `ResolveUsername`, `UpdateProfile`

**Groups/Channels** — `CreateGroup`, `CreateChannel`, `EditChatTitle`

**Inbox** — `InboxPeek`, `InboxRead` (via proxy `inbox_read` → `inbox_peek`/`inbox_wait` + `inbox_ack`)

Full table: [TOOLS_RU.md](TOOLS_RU.md).

## Requirements

* Python 3.9+
* Telegram `api_id` / `api_hash` (https://my.telegram.org)
* Linux/macOS (systemd for daemon), Windows via WSL

## Quick Start

```bash
pip install -e .
cp .env.example .env  # fill TELEGRAM_API_ID, TELEGRAM_API_HASH
python -m mcp_telegram.qr_auth  # or: mcp-tg sign-in
```

### Run daemon (systemd)

```bash
sudo cp scripts/tgmcpd.user.service ~/.config/systemd/user/tgmcpd.service
systemctl --user daemon-reload
systemctl --user enable --now tgmcpd.socket
systemctl --user enable --now tgmcpd
systemctl --user status tgmcpd
```

Daemon socket: `/run/user/1000/tgmcpd.sock` (`$XDG_RUNTIME_DIR/tgmcpd.sock`), store: `~/.local/state/tgmcpd/inbox_store` (`$TGMCPD_STORE_DIR`).

### Run proxy per topic (opencode)

```bash
TG_CHAT_ID=-1003998609906 TG_TOPIC_ID=205 tg-mcp-proxy
```

`opencode.json` example:

```json
{
  "mcpServers": {
    "tg-mcp-205": { "command": "tg-mcp-proxy", "env": { "TG_CHAT_ID": "-1003998609906", "TG_TOPIC_ID": "205" } }
  }
}
```

Add more proxies with different `TG_TOPIC_ID` for multi-agent isolation.

### Docker (optional)

```bash
docker build -t mcp-tg .
docker-compose up -d
```

## Configuration

Env / `.env`:

```
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_SESSION_PATH=~/.config/mcp-tg/session.session
TGMCPD_SOCK=/run/tgmcpd/tgmcpd.sock
TGMCPD_STORE_DIR=~/tgmcpd/inbox_store
TG_CHAT_ID=-1003998609906
TG_TOPIC_ID=205
```

Entry points (`setup.py`): `mcp-tg`, `tgmcpd`, `tg-mcp-proxy`.

## Project Structure

```
src/mcp_telegram/
  daemon.py       # tgmcpd entry, Telethon + IPC
  inbox.py        # InboxEngine (buffers + Events)
  inbox_store.py  # JSONL persistence
  inbox_bridge.py # InboxBridge push (tmux/tui/uds)
  ipc_server.py   # Unix socket JSON-line server
  ipc_client.py   # proxy side client
  proxy.py        # MCP stdio ↔ IPC
  telegram.py     # TelegramSettings, client helpers
  tools.py        # core 50 tools
  server.py       # legacy single-process server
docs/
  tg-mcpd Architecture v0.9.2 — Reactive Inbox.md
scripts/
  tgmcpd.user.service
  tgmcp-proxy-wrapper.py / tgmcp-proxy-watchdog.py
```

## Development

```bash
pytest
pytest tests/unit/test_inbox_bridge.py
black src && ruff check src && mypy src
```

Inbox protocol for agents: see [AGENTS.md](AGENTS.md) — on `⚡ INBOX ALERT` call `inbox_read`, then `send_message` with ack.

## Notes

Local project — no GitHub origin.

## License

MIT — see [LICENSE](LICENSE).
