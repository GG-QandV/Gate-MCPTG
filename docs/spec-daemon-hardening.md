# Spec: Daemon Hardening — продуктовая оптимизация tgmcpd

**Status:** Draft → реализация
**Date:** 2026-09-09
**Version target:** 0.9.4
**Baseline:** v0.9.3 + socket activation (32bb23e, 105 тестов зелёных)
**Scope:** `inbox_store.py`, `inbox.py`, `ipc_server.py`, `daemon.py` + тесты. Клиенты и протокол IPC — **без изменений** (обратная совместимость: старый прокси работает с новым демоном и наоборот).

---

## 1. Найденные проблемы (аудит 2026-09-09)

| # | Где | Проблема | Класс |
|---|---|---|---|
| P1 | `inbox_store.py:23,33,48,66` | **Блокирующий файловый I/O в event loop** — `open/read_text/write_text` выполняются в главном loop; большой `read_all`/`ack` замораживает ВСЕХ агентов и Telethon | product-critical |
| P2 | `inbox_store.py:24-25,66-67` | **Нет fsync**: `flush()` сбрасывает в OS, но не на диск — потеря хвоста inbox при падении питания/OOM-kill | product-critical |
| P3 | `daemon.py` | **SIGTERM = мгновенная смерть**: handler не установлен, `finally` не выполняется (в journal ни разу нет «tgmcpd stopped»), Telethon не disconnect, хвост store не сброшен | product-critical |
| P4 | `inbox.py:62-69` | **Тестовые костыли в проде**: `_is_mock()` / проверки MagicMock в `handle()` — тест-прессинг протёк в production-код | product (чистота) |
| P5 | `ipc_server.py:62` | **`readline()` unbounded** → локальный клиент может подать строку любого размера; сейчас длинная строка валит соединение через `ValueError` без внятного ответа | robustness |
| P6 | `inbox_store.py:70-86` | **`health_check` читает ВСЕ файлы целиком на каждый `health`** — O(весь inbox) каждые 5 мин + на каждый ping монитора | performance |
| P7 | `inbox_store.py` | **Неограниченный рост**: пока агенты не читают — файл растёт вечно; `read_all` отдаёт всё без потолка | robustness |
| P8 | `ipc_server.py:170` | Предсказуемое имя `/tmp/tg_dl_...` — коллизии между параллельными вызовами разных агентов | correctness |
| **P9** | `tests/unit/test_daemon.py` + `daemon.py` finally | **НАЙДЕН КОРЕНЬ ВСЕХ «сокет not found»**: main()-тесты звали `main()` с реальным `get_sock_path()` → finally делал реальный unlink живого файла сокета. Каждый прогон pytest убивал сокет демона (орфан-инод). Фикс: тесты на tmp-путь + `_unlink_if_owned()` (unlink только при совпадении inode bound-сокета и файла на диске) | root-cause |

Не делаем (контроль оверхеда): aiofiles (хватит `asyncio.to_thread`), sqlite-миграция (JSONL адекватен объёмам), HTTP-push (отвергнут ранее), метрик-стек (лёгкие счётчики в health вместо Prometheus).

## 2. Решения

### 2.1. `inbox_store.py` — thread-offload + durability + retention

Все файловые операции выносятся из loop в тред (`asyncio.to_thread`) под тем же `asyncio.Lock`. Sync-внутренности — чистые функции `_append_sync`, `_read_all_sync`, `_ack_sync`, `_health_sync`:

```
append:   to_thread(_append_sync)   → open("a") + write + flush + fsync
read_all: to_thread(_read_all_sync) → read + parse (corrupt → quarantine-лог)
ack:      to_thread(_ack_sync)      → filter → tmp write + flush + fsync → replace
health:   TTL-кэш 5s поверх to_thread(_health_sync)
```

**Retention** (`STORE_MAX_RECORDS = 10_000`, env `TGMCPD_STORE_MAX_RECORDS`): в `_append_sync` после записи, если записей в файле > cap — oldest обрезаются (превентивно в момент append, не отдельным воркером), warning + счётчик `retention_dropped`. `read_all` тоже отдаёт не больше cap последних записей (защита агента от монстро-дампа).

**Кварантин:** corrupt-строки при `ack`/trim не молча удаляются — дописываются в `inbox_*.jsonl.corrupt` рядом (диагностика без потери данных).

### 2.2. `inbox.py` — чистая сериализация + счётчики

- Новый модульный pure-функционал `message_to_entry(msg) -> dict`: защитная конвертация (getattr с дефолтами, `isinstance` для str/int) — **без `_is_mock`**. Тесты переводятся на `SimpleNamespace`-фейки вместо MagicMock.
- Счётчики `self.stats = {messages_in, acked, restored, dropped_overflow, retention_dropped}` — инкремент в `handle/ack/restore_from_store`, отдаются в `health`.

### 2.3. `ipc_server.py` — границы ввода + обогащённый health

- `readline()` обёрнут: `ValueError` (stream limit 64KB по умолчанию, поднимем до 1MB на соединение) → ответ `{"error": {"code": -32600, "message": "line too long"}}` + разрыв соединения (не тихий).
- `health` → добавляются: `"uptime_s"`, `"version"` (из `_version.py`), `"stats"` (из engine), `"store_stats"`.
- `download_file`: дефолтный dest → `tg_dl_{chat}_{msg}_{uuid4().hex[:8]}_{name}` — уникальность.

### 2.4. `daemon.py` — graceful shutdown

```
loop.add_signal_handler(SIGTERM/SIGINT → main_task.cancel())
finally: await client.disconnect()  # Telethon корректно закрывает сессию
        лог "tgmcpd stopped gracefully"
```

Проверка E2E: `systemctl --user stop tgmcpd` → journal содержит «stopped gracefully», время останова < 10s.

### 2.5. Монитор — без изменений (парсит `status`/`telegram`, новые поля игнорирует).

## 3. Новые тесты (9 шт., `tests/unit/test_hardening.py`)

1. **`test_message_to_entry_pure`** — `SimpleNamespace`-сообщение (текст, media, file size/name) → корректный entry; сообщение без атрибутов → дефолты, без исключений. В prod-коде grep `_is_mock` → 0.
2. **`test_store_append_offloaded`** — monkeypatch `asyncio.to_thread`: append уходит в тред, файл содержит строку.
3. **`test_store_fsync_on_append_and_ack`** — monkeypatch `os.fsync`: вызван ≥1 раза на append и на ack.
4. **`test_store_retention_cap`** — cap=5 (env-override в fixture): append 8 → в файле 5 последних, `retention_dropped` ≥ 3, corrupt-квоты нет.
5. **`test_readline_too_long_clean_reject`** — клиент шлёт 2MB-строку → получает JSON-ошибку «line too long», соединение закрыто, демон жив (следующий ping OK).
6. **`test_health_contains_uptime_version_stats`** — после `handle` + `ack`: health имеет `uptime_s ≥ 0`, `version`, `stats.messages_in ≥ 1`, `stats.acked ≥ 1`.
7. **`test_download_unique_paths`** — два `download_file` без output_path → разные dest (uuid-часть).
8. **`test_graceful_sigterm`** — unit: `main_task.cancel()` триггерит finally → `client.disconnect` awaited, лог «stopped gracefully» (subprocess-E2E — ручной чек-лист).

9. **`test_no_mock_awareness_in_src`** — grep-guard: `_is_mock` отсутствует во всех `src/**/*.py` (защита от регрессии костыля).

E2E чек-лист (ручной): `systemctl --user stop tgmcpd` → journal «stopped gracefully»; рестарт → рестор 8 сообщений; monitor OK; `pytest` → 114 passed.

## 4. Acceptance criteria

1. `pytest tests/ -q` → **114 passed** (105 + 9), 0 warnings.
2. `grep -rn "_is_mock" src/` → пусто; `grep -rn "to_thread" src/mcp_telegram/inbox_store.py` → все 4 файловые операции.
3. E2E: graceful stop в journal; health отдаёт `uptime_s`/`version`/`stats`.
4. Прокси/агенты перезапускать не нужно (протокол IPC не менялся) — новый демон совместим со старым прокси.
5. Версия 0.9.3 → 0.9.4 (`setup.py`, `_version.py`, README).

## 5. Риски

| Риск | Митигация |
|---|---|
| `to_thread` + fsync замедлит append | объёмы малы (десятки msg/час); fsync ~1-5ms на tmpfs/NVMe; lock serializes — приемлемо |
| retention молча режет непрочитанное | warning-лог + счётчик `retention_dropped` в health; cap 10000 ≫ maxlen RAM 200 |
| graceful shutdown зависнет на disconnect | `asyncio.wait_for(client.disconnect(), 5)` с таймаутом |

## 6. План

1. `inbox_store.py` (to_thread, fsync, retention, lazy health) → 2. `inbox.py` (message_to_entry, stats) → 3. `ipc_server.py` (readline limit, health, uuid) → 4. `daemon.py` (signals) → 5. тесты → 6. версия 0.9.4 + E2E.
