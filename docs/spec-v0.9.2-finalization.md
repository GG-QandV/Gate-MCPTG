# Spec: Finalization of v0.9.2 Reactive Inbox — MCP-TG

**Status:** Draft for review  
**Date:** 2026-09-08  
**Branches:** `main` ( stable, `9480753`) → `v0.9.2-reactive-inbox` (+12, `e4adc4d`) → `main`  
**Supersedes:** `tg-mcpd Architecture v0.9.2 — Reactive Inbox.md` (pull-model) + `tg-mcpd Architecture v0.9.2 add some 3 connects.md` (HTTP push) + `developer-guide.md` (v0.9.2) + `spec-db-lock-resolution.md`  
**Goal:** Freeze target architecture, close the 2 failing tests and 3 dirty files, merge v0.9.2 to main as standalone.

---

## 1. Context

`mcp-tg` (upstream `66e31ac`) is a single-process stdio MCP server.  
`MCP-TG ` (`docs/architecture-.md`) solved scaling: one `Telethon` client → N `tg-mcp-proxy` via Unix-socket IPC (`/run/tgmcpd/tgmcpd.sock`), per-topic buffers.

Problem  left:
- RAM-only inbox — loss on restart.
- Polling (`inbox_poll` / `inbox_read`) — cost, latency.
- No priority rules, no delivery guarantee.
- `database is locked` when `daemon` and `run_server.sh` race on the same `session.session`.

v0.9.2 exists as branch `v0.9.2-reactive-inbox` (12 commits ahead). It implements reactive delivery but docs diverged: original v0.9.2 pull (`proxy inbox_subscribe` + `inbox_wait`) vs. later push (`InboxBridge` → `opencode` HTTP/tmux/tui/uds). Code reality is the second: `InboxBridge` with 3 transports.

## 2. Target Architecture (final)

```
Telegram ─MTProto push──▶ Telethon ──▶ InboxEngine.handle()
                                         ├─ InboxStore.append(JSONL)  ← disk
                                         ├─ buffer[(chat,topic)].append() ← RAM
                                         └─ asyncio.Event[(chat,topic)].set()

InboxBridge (one task per topic_map entry) — same event loop as daemon:
  while True:
    msgs = await inbox.wait(chat,topic)   # Event-driven, no polling, no timeout
    if !msgs: continue
    success = await push(transport, target, msgs)   # tmux / tui / uds
    inbox.clear_ram_buffer(chat,topic)              # always, even on failure (УМ-И)
    # ack is via clear_ram_buffer + store remains until agent's inbox_read (peek → read_all fallback)

Agent side:
  tmux ──paste──▶ opencode TUI  |  tui inject UDS  |  uds mirror FIFO → Hermes stdin
  Agent on start calls inbox_read (peek → store.read_all fallback) to drain disk backlog.
```

**IPC server** (`ipc_server.py`) exposes: `send_message`, `send_file`, `download_file`, `inbox_peek`, `health`. `inbox_wait` is no longer exposed — `InboxBridge` calls `InboxEngine.wait()` directly inside the daemon.

**Proxy** (`proxy.py`) stays only for `send_*` / `download_*` / `inbox_peek` (stateless stdio↔IPC). `inbox_subscribe` is deleted.

**Config** (`telegram.py:TelegramSettings`):
```
TG_TOPIC_MAP="chat:topic:transport:target,..."
  tmux: "-1003998609906:205:agent:opencode"
  tui:  "-1003998609906:3:tui:/tmp/hrm-tui-inject.sock"   # Hermes TUI inject
  uds:  "-1003998609906:3:uds:/tmp/hrm-mirror.sock"       # deprecated, keep for compat
TGMCPD_STORE_DIR=~/tgmcpd/inbox_store
TGMCPD_SOCK=/run/tgmcpd/tgmcpd.sock
PrivateTmp=false  (developer-guide.md:4 — otherwise socket invisible)
```

**No HTTP push variant.** The `openmodel`/`openrouter` HTTP `prompt_async` idea from `add some 3 connects.md` is rejected — current `InboxBridge` is `tmux`/`tui`/`uds` only (code `inbox_bridge.py:18`). HTTP would add auth/session discovery complexity with no gain over `tmux paste -p` and `tui` UDS inject.

## 3. Component Contracts (must not drift)

| Component | Contract |
|---|---|
| `InboxStore` | One `asyncio.Lock` per instance (УМ-1). `append` before `buffer`. `ack` via `tmp→replace`, empty → `""` not deletion (УМ-2). `health_check` counts corrupt lines. |
| `InboxEngine` | `wait` order: `event.clear() → peek → if exists return → await event.wait()` (УМ-3, `spec-db-lock-resolution.md`). `restore_from_store()` uses `rfind("_")` for negative `chat_id` (УМ-4). `handle` tolerates `reply_to=None` → `topic 0` (УМ-6). `defaultdict(asyncio.Event)` without `()` (УМ-7). `clear_ram_buffer()` + `event.clear()` after every push attempt (УМ-И), even on failure to avoid 100% CPU spin. `peek()` falls back to `store.read_all()` if RAM empty. |
| `InboxBridge` | `retry_max=3, retry_delay=2.0`. `_watch` catches all, never crashes daemon (УМ-Е). `_push_*` returns `bool`; only on `True` log success, but `clear_ram_buffer` always (current code `inbox_bridge.py:81`). No `ack` here — agent's `inbox_read` does `store.ack` via `inbox_peek` path. `_pane_exists` checks `tmux has-session -t`. `_do_paste` uses `load-buffer -` + `paste-buffer -p` + `send-keys Enter`. Missing `TG_TOPIC_MAP` → warning, continue (УМ-Ж). |
| `daemon.py` | `_check_stale_socket` → `sys.exit(69)` on live socket, `RestartPreventExitStatus=69` (spec-db-lock). Single `TelegramClient`, `store = InboxStore(cfg.store_dir)`, `await inbox.restore_from_store()`. `asyncio.gather(ipc.start, client.run_until_disconnected, bridge.start)`. |
| `telegram.py` | `topic_map_raw` parses `chat:topic:transport:target` where transport `tui`/`uds` has `target=sock`, else `tmux` with `target=session:window`. |

## 4. Critical Details (unified УМ)

| # | Where | Requirement |
|---|---|---|
| УМ-1 | `InboxStore` | single `asyncio.Lock` |
| УМ-2 | `ack` | `tmp.with_suffix(".tmp").replace(path)` |
| УМ-3 | `wait` | `clear → peek → wait` |
| УМ-4 | `restore` | `rfind("_")` for `-100...` |
| УМ-6 | `handle` | `getattr(reply_to, "reply_to_top_id") or "reply_to_msg_id" or 0` |
| УМ-7 | `__init__` | `defaultdict(asyncio.Event)` |
| УМ-И | `InboxBridge` | `clear_ram_buffer` on every `wait` return, success or fail; `peek` fallback to disk |
| УМ-Бridge | `InboxBridge` | 3 transports, `watch` per topic via `asyncio.gather`, isolated `try/except` |

(УМ-5 triple timeout, УМ-8 ack-before-return, УМ-10 from old pull spec are obsolete — deleted with `inbox_subscribe`.)

## 5. Gaps to Close Before Merge (verified 2026-09-08)

Current `v0.9.2-reactive-inbox` status (`git status -sb`, `PYTHONPATH=src:. pytest -q`):

- **Failing tests 2/23** in `tests/unit/test_inbox_bridge.py`: `test_watch_ack_only_after_successful_push` and `test_watch_routes_tui_transport` — both `assert inbox.ack awaited 1, got 0`. Root: branch changed `inbox.py:48` to `if not msg: return` + `has_media` fields and `telegram.py` to `tui` transport, and added `test_inbox_bridge.py:+118` for `tui`, but `InboxBridge._watch` no longer calls `inbox.ack` at all — it now calls `clear_ram_buffer`. Tests expect `ack`, code does `clear_ram_buffer`. **Fix:** update those 2 tests to assert `clear_ram_buffer` instead of `ack`, or restore `ack` path if intended. Decision: tests are wrong — update tests (ack belongs to agent `inbox_read`, not bridge).
- **Dirty files:** `M inbox.py`, `M telegram.py`, `M test_inbox_bridge.py` + `?? 6` untracked (`agent-os-architecture.html`, `tg-mcpd Architecture v0.9.2 add some 3 connects.md`, `tgmcp-proxy-*.py`, `tgmcpd.user.service`, `inbox.py.bak`, `test_topic_map_parser.py`). **Fix:** commit `inbox.py` media fields + `telegram.py tui` as one commit; either delete or add `test_topic_map_parser.py` (currently `from tests.conftest import make_event` vs `import src` — inconsistent `PYTHONPATH`, needs `pythonpath=src` in `pytest.ini`).
- **Pytest invocation:** without `PYTHONPATH=src:.` 7 collection errors (`ModuleNotFoundError: tests` / `src`). `developer-guide.md:4` says `PYTHONPATH=. python -m pytest tests/` — actually needs `PYTHONPATH=src:.` or `pytest.ini` `pythonpath = src .`. **Fix:** add `pythonpath = src` to `pytest.ini`.
- **DB lock:** `spec-db-lock-resolution.md` diffs not yet applied on branch (check `daemon.py` still `sys.exit(1)`, `run_server.sh` still `mcp-tg`, service still `ExecStartPre rm -f`). **Fix:** apply those 4 diffs before merge (they are independent of inbox).

## 6. Acceptance Criteria

- `PYTHONPATH=src:. pytest -q` → 23 passed, 0 failed (after fixing the 2).
- `git diff` clean (or only intended untracked docs ignored).
- `docs/spec-v0.9.2-finalization.md` (this file) reviewed.
- `systemd` `PrivateTmp=false`, `RestartPreventExitStatus=69` verified.
- Manual e2e: start `tgmcpd`, start one `opencode` on `TG_TOPIC_MAP="-100...:205:tmux:agent:opencode"`, send Telegram message → `store` JSONL written, bridge `paste-buffer -p`, agent receives `⚡ INBOX [...] inbox_read`, `inbox_read` drains store. Restart daemon → `restore_from_store` recovers unread.

## 7. Merge Plan

1. Fix the 2 tests + `pytest.ini` (`pythonpath = src`), commit.
2. Apply `spec-db-lock-resolution.md` diffs (`daemon.py`, `tgmcpd.service`, `run_server.sh`), commit.
3. Commit or drop the 3 `M` files (media fields + tui transport) as above.
4. `git checkout main && git merge v0.9.2-reactive-inbox --no-ff -m "merge v0.9.2 reactive inbox (final)"` (now `main` is `c513964` docs, `v0.9.2` is `e4adc4d` docs — same README, will need `ours` for README).
5. `git push origin main` (standalone).
6. Tag `v0.9.2.0.0`, archive `mcp-tg` fork reference in README.

---

**References:** `architecture-.md`, `tg-mcpd Architecture v0.9.2 — Reactive Inbox.md` (L1-L4, phases 0-8, total 31+31 tests), `add some 3 connects.md` (push variant, rejected), `developer-guide.md` (UМ-А…И, `PrivateTmp`), `spec-db-lock-resolution.md` (4 diffs).
