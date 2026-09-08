# tg-mcpd Architecture v0.9.2 — Reactive Inbox

---

## Ответы на ключевые вопросы

## А) Один inbox или персональный?

**Персональный inbox на каждый топик** — правильнее:

text

`Один InboxEngine с буферами по (chat_id, topic_id) Каждый proxy имеет доступ ТОЛЬКО к своему (chat_id, topic_id) Сигнал идёт только тому proxy у которого есть сообщение`

Один общий inbox с правами — это лишняя сложность без выгоды. Изоляция по ключу `(chat_id, topic_id)` уже есть в  — это и есть персональный inbox.

## Б) Инжект правил приоритетности

Правила едут **вместе с каждым сообщением** в структурированном конверте — агент не может их проигнорировать.

## В) Надёжность

Сообщения персистентны на диске — переживают рестарт всего.

---

## Новые компоненты

text

`src/mcp_telegram/   daemon.py          # без изменений  ipc_server.py      # +inbox_wait dispatch  ipc_client.py      # без изменений  inbox.py           # +asyncio.Event + persist  inbox_store.py     # ← НОВЫЙ: персистентность на диске  proxy.py           # +inbox_subscribe tool + envelope  telegram.py        # без изменений`

---

## inbox_store.py — персистентный архив

Сообщения хранятся на диске в JSONL файле. Переживают рестарт daemon, системы, агента.

python

`import asyncio import json import logging from pathlib import Path logger = logging.getLogger(__name__) class InboxStore:     """    Персистентный архив сообщений.    Каждый (chat_id, topic_id) → отдельный JSONL файл.    Сообщение удаляется только после явного ack от агента.    """     def __init__(self, store_dir: str):        self.store_dir = Path(store_dir)        self.store_dir.mkdir(parents=True, exist_ok=True)        self._lock = asyncio.Lock()     def _path(self, chat_id: int, topic_id: int) -> Path:        return self.store_dir / f"inbox_{chat_id}_{topic_id}.jsonl"     async def append(self, chat_id: int, topic_id: int, msg: dict) -> None:        path = self._path(chat_id, topic_id)        async with self._lock:            with path.open("a", encoding="utf-8") as f:                f.write(json.dumps(msg, ensure_ascii=False) + "\n")                f.flush()  # гарантируем запись на диск     async def read_all(self, chat_id: int, topic_id: int) -> list[dict]:        path = self._path(chat_id, topic_id)        async with self._lock:            if not path.exists():                return []            msgs = []            for line in path.read_text(encoding="utf-8").splitlines():                line = line.strip()                if line:                    try:                        msgs.append(json.loads(line))                    except json.JSONDecodeError:                        logger.warning("Corrupt line in %s, skipping", path)            return msgs     async def ack(self, chat_id: int, topic_id: int, last_id: int) -> int:        """Удалить сообщения с id <= last_id. Остальные переписать."""        path = self._path(chat_id, topic_id)        async with self._lock:            if not path.exists():                return 0            lines = path.read_text(encoding="utf-8").splitlines()            kept = []            dropped = 0            for line in lines:                line = line.strip()                if not line:                    continue                try:                    msg = json.loads(line)                    if msg["id"] <= last_id:                        dropped += 1                    else:                        kept.append(line)                except json.JSONDecodeError:                    pass  # битые строки дропаем            # атомарная перезапись через tmp файл            tmp = path.with_suffix(".tmp")            tmp.write_text("\n".join(kept) + ("\n" if kept else ""),                           encoding="utf-8")            tmp.replace(path)  # атомарно на Linux            return dropped     async def health_check(self) -> dict:        """Проверить целостность всех store файлов."""        results = {}        for f in self.store_dir.glob("inbox_*.jsonl"):            corrupt = 0            total = 0            for line in f.read_text(encoding="utf-8").splitlines():                line = line.strip()                if not line:                    continue                total += 1                try:                    json.loads(line)                except json.JSONDecodeError:                    corrupt += 1            results[f.name] = {"total": total, "corrupt": corrupt}        return results`

---

## inbox.py — обновлённый с Event + Store

python

`import asyncio import logging from collections import defaultdict, deque from .inbox_store import InboxStore logger = logging.getLogger(__name__) INBOX_MAXLEN = 200 class InboxEngine:     def __init__(self, store: InboxStore, maxlen: int = INBOX_MAXLEN):        self.store   = store        self.maxlen  = maxlen        # RAM буфер для быстрого доступа        self._buffers: dict[tuple, deque] = defaultdict(            lambda: deque(maxlen=self.maxlen)        )        # asyncio.Event на каждый топик для сигнализации        self._events: dict[tuple, asyncio.Event] = defaultdict(asyncio.Event)        self._lock = asyncio.Lock()     async def restore_from_store(self) -> int:        """        При старте daemon: загрузить неподтверждённые сообщения из диска в RAM.        Вызывать один раз в daemon.py после инициализации.        """        total = 0        health = await self.store.health_check()        for fname, stat in health.items():            if stat["corrupt"] > 0:                logger.error("CORRUPT records in %s: %d", fname, stat["corrupt"])            # парсим chat_id и topic_id из имени файла inbox_{chat_id}_{topic_id}.jsonl            parts = fname.replace("inbox_", "").replace(".jsonl", "").split("_")            if len(parts) != 2:                continue            chat_id, topic_id = int(parts[0]), int(parts[1])            msgs = await self.store.read_all(chat_id, topic_id)            if msgs:                key = (chat_id, topic_id)                async with self._lock:                    for msg in msgs:                        self._buffers[key].append(msg)                    # сигнализировать — есть непрочитанные                    self._events[key].set()                total += len(msgs)                logger.info(                    "Restored %d messages for chat=%d topic=%d",                    len(msgs), chat_id, topic_id                )        return total     async def handle(self, event) -> None:        chat_id  = event.chat_id        topic_id = (            getattr(event.message.reply_to, "reply_to_top_id", None)            or getattr(event.message.reply_to, "reply_to_msg_id", None)            or 0        )        msg = {            "id":   event.message.id,            "text": event.message.text or "",            "from": str(event.message.sender_id),            "ts":   int(event.message.date.timestamp()),        }        key = (chat_id, topic_id)         # 1. Сначала персистировать на диск        await self.store.append(chat_id, topic_id, msg)         # 2. Потом в RAM буфер        async with self._lock:            buf = self._buffers[key]            if len(buf) == self.maxlen:                logger.warning(                    "inbox overflow chat=%d topic=%d", chat_id, topic_id                )            buf.append(msg)            # 3. Сигнал только тому топику у которого есть сообщение            self._events[key].set()         logger.info(            "inbox: new message chat=%d topic=%d id=%d",            chat_id, topic_id, msg["id"]        )     async def wait(        self,        chat_id: int,        topic_id: int,        timeout: float = 25.0    ) -> list[dict]:        """        Блокирующий wait. Возвращает сообщения как только они появятся.        Правильный порядок: clear → check → wait исключает race condition.        """        key = (chat_id, topic_id)        ev  = self._events[key]         # clear ПЕРЕД peek — исключает race window        ev.clear()         # проверить буфер — вдруг уже есть непрочитанные        async with self._lock:            existing = list(self._buffers[key])        if existing:            ev.clear()            return existing         # ждать сигнала        try:            await asyncio.wait_for(ev.wait(), timeout=timeout)        except asyncio.TimeoutError:            return []         ev.clear()        async with self._lock:            return list(self._buffers[key])     async def peek(self, chat_id: int, topic_id: int) -> list[dict]:        async with self._lock:            return list(self._buffers.get((chat_id, topic_id), []))     async def ack(        self,        chat_id: int,        topic_id: int,        last_id: int    ) -> int:        key = (chat_id, topic_id)        # удалить из диска (атомарно)        dropped = await self.store.ack(chat_id, topic_id, last_id)        # синхронизировать RAM буфер        async with self._lock:            buf = self._buffers.get(key)            if buf:                remaining = deque(                    (m for m in buf if m["id"] > last_id),                    maxlen=self.maxlen                )                self._buffers[key] = remaining        return dropped`

---

## proxy.py — inbox_subscribe с конвертом приоритетов

python

`PRIORITY_RULES = """ ⚡ INBOX ALERT — ПРИОРИТЕТ №1 ⚡ Ты получил новые сообщения. Немедленно: 1. ЗАМОРОЗЬ текущую задачу (запомни состояние) 2. ПРОЧИТАЙ все сообщения ниже 3. ОПРЕДЕЛИ тип каждого:    - ОТМЕНА ("стоп"/"отмена"/"не надо") → остановить задачу, подтвердить   - УТОЧНЕНИЕ/КОНТЕКСТ → применить к текущей задаче, продолжить   - НОВОЕ ЗАДАНИЕ → завершить/отложить текущее, взять новое 4. ОТВЕТЬ в топик подтверждение что получил 5. ДЕЙСТВУЙ согласно типу Игнорирование этих правил недопустимо. ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ """ @server.call_tool() async def call_tool(name: str, arguments: dict) -> list[TextContent]:     ipc = await get_ipc()     if name == "inbox_subscribe":        # Блокирующий wait — висит до сигнала или timeout        result = await ipc.call("inbox_wait", {            "chat_id":  TG_CHAT_ID,            "topic_id": TG_TOPIC_ID,            "timeout":  25.0        })        msgs = result.get("messages", [])         if not msgs:            # timeout — агент переспрашивает            return [TextContent(type="text", text="__INBOX_TIMEOUT__")]         # ack после получения        last_id = msgs[-1]["id"]        await ipc.call("inbox_ack", {            "chat_id":  TG_CHAT_ID,            "topic_id": TG_TOPIC_ID,            "last_id":  last_id        })         # Конверт: правила приоритетности + сообщения        envelope = {            "priority_rules": PRIORITY_RULES,            "message_count":  len(msgs),            "messages":       msgs        }        return [TextContent(            type="text",            text=json.dumps(envelope, ensure_ascii=False, indent=2)        )]     # ... остальные tools`

---

## daemon.py — обновлённый старт с restore

python

`async def main() -> None:     cfg      = TelegramSettings()    sock_path = get_sock_path()     await _check_stale_socket(sock_path)     # Инициализировать store    store = InboxStore(store_dir=cfg.store_dir)  # /home/gg/tgmcpd/inbox_store     # Инициализировать inbox    inbox = InboxEngine(store=store)     # Восстановить непрочитанные сообщения с диска    restored = await inbox.restore_from_store()    if restored:        logger.warning("Restored %d unread messages from disk after restart", restored)     client = TelegramClient(str(cfg.session_path), cfg.api_id, cfg.api_hash)    await client.start()     client.add_event_handler(inbox.handle, events.NewMessage)     ipc = IPCServer(inbox, client)     try:        await asyncio.gather(            ipc.start(sock_path),            client.run_until_disconnected(),        )    finally:        Path(sock_path).unlink(missing_ok=True)`

---

## Health check endpoint

Добавить в IPC dispatch:

python

`elif method == "health":     store_health = await self.inbox.store.health_check()    buffers = {        f"{k[0]}_{k[1]}": len(v)        for k, v in self.inbox._buffers.items()    }    return {        "status":       "ok",        "store":        store_health,        "ram_buffers":  buffers,        "telegram":     self.client.is_connected()    }`

Вызывать из systemd ExecStartPost:

text

`ExecStartPost=/usr/local/bin/tgmcpd-healthcheck`

---

## Полная схема надёжности

text

`СООБЩЕНИЕ ПРИШЛО       │      ▼ store.append()          ← диск, атомарно       │      ▼ buffer.append()         ← RAM       │      ▼ event[(chat,topic)].set() ← только нужный агент       │      ▼ proxy: inbox_wait() разблокируется       │      ▼ конверт с правилами → агент       │      ▼ агент обработал → inbox_ack(last_id)       │      ├── store.ack()   ← удалить с диска      └── buffer.ack()  ← синхронизировать RAM РЕСТАРТ СИСТЕМЫ:       │      ▼ daemon.restore_from_store()       │      ▼ все неподтверждённые сообщения возвращаются event.set() для каждого топика с непрочитанными       │      ▼ агент при reconnect получает missed сообщения`

---

## Что изменилось от 

| Компонент |  | v0.9.2  |
| --------- | ---- | --- |

| Компонент                   |                      | v0.9.2                            |
| --------------------------- | ------------------------ | ----------------------------- |
| Персистентность             | ❌ RAM only               | ✅ JSONL на диске              |
| Восстановление при рестарте | ❌                        | ✅ `restore_from_store()`      |
| Сигнализация                | asyncio.Event (было)     | ✅ то же, правильный порядок   |
| Изоляция агентов            | ✅ по (chat_id, topic_id) | ✅ без изменений               |
| Правила приоритетности      | ❌                        | ✅ конверт с каждым сообщением |
| Health check                | ❌                        | ✅ IPC + ExecStartPost         |
| Атомарность ack             | частично                 | ✅ tmp→replace                 |
| Corrupt recovery            | ❌                        | ✅ пропуск битых строк + лог   |

Хороший вопрос — это действительно не было явно описано. Разберём по слоям:

## Где живёт механизм реактивности

text

`УРОВНИ: ───────────────────────────────────────────────── L1  Telegram → daemon     MTProto event (Telethon) L2  daemon → proxy        asyncio.Event (Unix socket) L3  proxy → агент         MCP tool return (stdio) L4  агент реагирует       system prompt правила ─────────────────────────────────────────────────`

Каждый уровень — свой инструмент. Разберём каждый:

---

## L1: Telegram → daemon

python

`# Telethon внутри asyncio event loop client.add_event_handler(inbox.handle, events.NewMessage)`

Это **push от Telegram** через постоянное MTProto соединение. Telethon держит TCP сокет к серверам Telegram, получает updates в реальном времени. Никакого polling — чистый push.

---

## L2: daemon → proxy (ключевой уровень)

Инструмент: **`asyncio.Event` + блокирующий IPC вызов `inbox_wait`**

text

`proxy вызвал inbox_wait         │        │  asyncio.Event.wait() — корутина висит        │  НЕ блокирует event loop daemon        │  НЕ жрёт CPU — просто suspended корутина        │ Telegram прислал сообщение         │        ▼ inbox.handle() → event[(chat_id, topic_id)].set()         │        ▼ ТОЛЬКО suspended корутина этого топика пробуждается         │        ▼ inbox_wait возвращает сообщения → IPC ответ → proxy`

**Это и есть механизм реактивности.** `asyncio.Event` — примитив синхронизации внутри одного event loop. Не сигнал ОС, не файл, не сокет — чистая asyncio координация.

---

## L3: proxy → агент

Инструмент: **блокирующий MCP tool call**

text

`агент вызвал inbox_subscribe         │        │  MCP stdio ждёт ответа        │  proxy держит открытый IPC вызов inbox_wait        │  оба висят — никто не polling        │ daemon получил сообщение → event.set()         │        ▼ IPC inbox_wait возвращает результат         │        ▼ proxy формирует конверт с правилами         │        ▼ MCP tool возвращает ответ агенту по stdio         │        ▼ opencode/IDE получает результат tool call`

---

## L4: агент реагирует

Инструмент: **system prompt + структура конверта**

Агент не может "получить сигнал" пока он не ждёт. Поэтому правило в system prompt:

text

`После КАЖДОГО завершённого шага — вызвать inbox_subscribe. Это единственная точка входа для внешних команд.`

---

## Полная цепочка одной строкой

text

`Telegram push → Telethon event → asyncio.Event.set() → inbox_wait корутина пробуждается → IPC ответ → MCP tool return → opencode получает конверт → агент реагирует по правилам`

---

## Почему не нужен отдельный сервис

| Вариант                | Проблема                                     |
| ---------------------- | -------------------------------------------- |
| Отдельный notify-демон | лишний процесс, IPC между двумя демонами     |
| inotify на файл        | race condition, файловые операции            |
| UNIX signal (SIGUSR1)  | нельзя передать данные, сложно в asyncio     |
| **asyncio.Event**      | ✅ внутри одного loop, zero overhead, надёжно |

`asyncio.Event` — это именно то что нужно: координация между двумя корутинами (`inbox.handle` и `inbox_wait`) внутри одного процесса без внешних зависимостей.

## Workflow реализации tg-mcpd v0.9.2

---

## Фаза 0 — Подготовка (без кода)

text

`0.1  Убедиться что  тесты все зелёные (31/31) 0.2  Создать ветку: git checkout -b v0.9.2-reactive-inbox 0.3  Зафиксировать структуру папок:      mkdir -p src/mcp_telegram     mkdir -p tests/unit tests/integration     mkdir -p /home/gg/tgmcpd/inbox_store`

---

## Фаза 1 — InboxStore (персистентность)

**Файл:** `src/mcp_telegram/inbox_store.py`

text

`1.1  Написать InboxStore:      - append(chat_id, topic_id, msg)     - read_all(chat_id, topic_id)     - ack(chat_id, topic_id, last_id)  ← атомарно через tmp→replace     - health_check()                   ← проверка битых строк 1.2  Тесты unit/test_inbox_store.py:      - test_append_persists_to_disk     - test_read_all_returns_all     - test_ack_removes_up_to_last_id     - test_ack_atomic_no_data_loss      ← tmp→replace     - test_corrupt_line_skipped     - test_health_check_detects_corrupt     - test_multiple_topics_separate_files 1.3  Все тесты зелёные → коммит:      git commit -m "feat: InboxStore persistent JSONL"`

---

## Фаза 2 — InboxEngine v0.9.2

**Файл:** `src/mcp_telegram/inbox.py`

text

`2.1  Обновить InboxEngine:      - добавить InboxStore в __init__     - handle(): store.append() ПЕРЕД buffer.append()     - wait(): правильный порядок clear→peek→wait     - ack(): store.ack() + buffer sync атомарно     - restore_from_store(): загрузка при старте 2.2  Тесты unit/test_inbox.py — добавить к существующим:      - test_handle_persists_to_store     - test_restore_loads_unread_on_start     - test_restore_fires_event_if_unread     - test_wait_wakes_on_event            ← asyncio.Event.set()     - test_wait_returns_existing_immediately     - test_wait_no_race_message_before_wait     - test_wait_no_race_message_during_clear     - test_ack_syncs_store_and_buffer     - test_wait_timeout_returns_empty 2.3  Все тесты зелёные → коммит:      git commit -m "feat: InboxEngine v0.9.2 + Event + restore"`

---

## Фаза 3 — IPC Server: inbox_wait dispatch

**Файл:** `src/mcp_telegram/ipc_server.py`

text

`3.1  Добавить в _dispatch():      - метод "inbox_wait" с отдельным WAIT_TIMEOUT=25s     - метод "health" → store.health_check() + buffer stats     - DISPATCH_TIMEOUT остаётся 30s (не режет wait) 3.2  Исправить таймаут для wait:      inbox_wait → asyncio.wait_for(inbox.wait(), timeout=27s)     (27 < 30 = DISPATCH_TIMEOUT, есть буфер для IPC overhead) 3.3  Тесты unit/test_ipc_server.py — добавить:      - test_dispatch_inbox_wait_returns_on_message     - test_dispatch_inbox_wait_timeout_returns_empty     - test_dispatch_health_returns_status     - test_dispatch_inbox_wait_does_not_block_ping  ← C-01 3.4  Все тесты зелёные → коммит:      git commit -m "feat: IPC inbox_wait + health dispatch"`

---

## Фаза 4 — Proxy: inbox_subscribe + конверт

**Файл:** `src/mcp_telegram/proxy.py`

text

`4.1  Добавить PRIORITY_RULES константу (текст правил) 4.2  Добавить tool inbox_subscribe:      - вызывает IPC inbox_wait (timeout=25s)     - при пустом ответе → возвращает __INBOX_TIMEOUT__     - при сообщениях → ack + формирует конверт:       {         "priority_rules": PRIORITY_RULES,         "message_count": N,         "messages": [...]       } 4.3  Обновить list_tools() — добавить inbox_subscribe 4.4  Тесты unit/test_proxy.py — добавить:      - test_inbox_subscribe_returns_envelope     - test_inbox_subscribe_timeout_returns_sentinel     - test_inbox_subscribe_acks_after_receive     - test_envelope_contains_priority_rules     - test_envelope_contains_all_messages 4.5  Все тесты зелёные → коммит:      git commit -m "feat: proxy inbox_subscribe + priority envelope"`

---

## Фаза 5 — Daemon: restore + store init

**Файл:** `src/mcp_telegram/daemon.py`

text

`5.1  Обновить main():      - инициализировать InboxStore(store_dir=cfg.store_dir)     - передать store в InboxEngine     - вызвать await inbox.restore_from_store()     - логировать restored count 5.2  Добавить в TelegramSettings:      - store_dir: str = "/home/gg/tgmcpd/inbox_store" 5.3  Тесты unit/test_daemon.py — добавить:      - test_restore_called_on_start     - test_store_dir_created_if_missing 5.4  Коммит:      git commit -m "feat: daemon restore_from_store on start"`

---

## Фаза 6 — Интеграционные тесты

**Файл:** `tests/integration/test_reactive_flow.py`

text

`6.1  Написать полный цикл реактивности:      test_message_triggers_inbox_subscribe:       1. Запустить IPC server с реальным сокетом       2. Запустить proxy подключённый к серверу       3. proxy вызывает inbox_subscribe (висит)       4. Симулировать inbox.handle() с сообщением       5. Проверить что inbox_subscribe вернул конверт       6. Проверить что конверт содержит правила + сообщение      test_message_survives_daemon_restart:       1. handle() → store.append() + buffer       2. Симулировать рестарт (новый InboxEngine)       3. restore_from_store()       4. wait() → возвращает сообщение      test_concurrent_topics_isolated:       1. Два proxy: topic=205, topic=310       2. Сообщение в topic=310       3. Только proxy 310 получает → proxy 205 продолжает ждать      test_ack_clears_store_and_buffer:       1. handle() → persist + buffer       2. ack(last_id)       3. read_all() == []       4. peek() == [] 6.2  Все тесты зелёные → коммит:      git commit -m "test: integration reactive flow v0.9.2"`

---

## Фаза 7 — systemd + deploy

text

`7.1  Обновить tgmcpd.service:      - добавить ReadWritePaths для store_dir     - ExecStartPost healthcheck скрипт 7.2  Написать tgmcpd-healthcheck:      #!/bin/bash     result=$(echo '{"method":"health","params":{},"id":1}' \       | socat - UNIX:/run/tgmcpd/tgmcpd.sock)     echo "$result" | python3 -c "     import sys, json     r = json.load(sys.stdin)     if r.get('result', {}).get('status') != 'ok':         sys.exit(1)     " 7.3  Установить:      sudo cp scripts/tgmcpd.service /etc/systemd/system/     sudo systemctl daemon-reload     sudo systemctl enable tgmcpd     sudo systemctl start tgmcpd 7.4  Проверить:      systemctl status tgmcpd     systemd-analyze security tgmcpd.service     journalctl -u tgmcpd -f`

---

## Фаза 8 — End-to-end ручной тест

text

`8.1  Запустить daemon под systemd 8.2  Запустить один агент (opencode) с proxy topic=205 8.3  Агент вызывает inbox_subscribe → висит 8.4  Отправить сообщение с телефона в топик 205 8.5  Проверить:      ✅ daemon лог: "MTProto event received"     ✅ daemon лог: "inbox: new message chat=X topic=205"     ✅ store файл: inbox_X_205.jsonl содержит сообщение     ✅ proxy: inbox_subscribe вернул конверт     ✅ агент: прочитал правила + сообщение     ✅ после ack: store файл пустой 8.6  Тест рестарта:      Отправить сообщение → НЕ читать     sudo systemctl restart tgmcpd     Агент переподключается     ✅ inbox_subscribe возвращает сообщение (из store)`

---

## Итоговые цифры

| Фаза          | Новых файлов | Новых тестов | Коммитов |
| ------------- | ------------ | ------------ | -------- |
| 1 InboxStore  | 1            | 7            | 1        |
| 2 InboxEngine | 0 (update)   | 9            | 1        |
| 3 IPC Server  | 0 (update)   | 4            | 1        |
| 4 Proxy       | 0 (update)   | 5            | 1        |
| 5 Daemon      | 0 (update)   | 2            | 1        |
| 6 Integration | 1            | 4            | 1        |
| 7 Deploy      | 2            | —            | 1        |
| **Итого**     | **4**        | **31**       | **7**    |

**Финальный счёт тестов: 31 () + 31 (v0.9.2) = 62 теста.**

Понял задачу. Deepseek V4 Flash — модель с хорошим кодом, но слабой инициативой в неоднозначных местах. Нужна спека, которая закрывает все серые зоны до начала кода.

---

# tg-mcpd v0.9.2 — Спека узких мест для агента-кодера

**Целевой агент:** Deepseek V4 Flash  
**Принцип:** каждое узкое место = точная инструкция без вариантов интерпретации

---

## УМ-1: `asyncio.Lock` в `InboxStore` — один на весь класс

**Проблема:** агент может создать lock внутри метода или отдельный на каждый файл.

**Требование:** один `asyncio.Lock` на весь экземпляр `InboxStore`. Все методы (`append`, `read_all`, `ack`, `health_check`) используют `async with self._lock`. Гранулярность per-topic не нужна — write-contention минимален, зато нет deadlock от вложенных вызовов.

python

`# ПРАВИЛЬНО class InboxStore:     def __init__(self, store_dir: str):        self._lock = asyncio.Lock()  # один на всё # НЕПРАВИЛЬНО self._locks: dict = defaultdict(asyncio.Lock)  # НЕ ДЕЛАТЬ`

---

## УМ-2: `ack()` — атомарность через `tmp → replace`

**Проблема:** агент может написать `path.write_text(...)` напрямую — при сбое в середине запись частично повреждена.

**Требование:** строго `tmp → replace`. Путь к tmp: `path.with_suffix(".tmp")`. После записи — `tmp.replace(path)`. `Path.replace()` на Linux атомарен (syscall `rename`).

python

`tmp = path.with_suffix(".tmp") tmp.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8") tmp.replace(path)  # атомарно — НЕ os.rename, НЕ shutil.move`

Если `kept` пуст — записать пустую строку `""`, не удалять файл. Файл удалять не нужно — `read_all` вернёт `[]` для пустого файла.

---

## УМ-3: `wait()` — порядок `clear → peek → wait` нарушать нельзя

**Проблема:** агент интуитивно пишет `peek → clear → wait` или `wait → clear`. Оба варианта создают race condition.

**Требование:** строго этот порядок:

python

`async def wait(self, chat_id, topic_id, timeout=25.0):     key = (chat_id, topic_id)    ev = self._events[key]     ev.clear()                          # 1. СНАЧАЛА clear     async with self._lock:              # 2. ПОТОМ peek        existing = list(self._buffers[key])    if existing:        return existing                 # 3. вернуть если есть (ev НЕ трогать)     try:        await asyncio.wait_for(ev.wait(), timeout=timeout)  # 4. иначе ждать    except asyncio.TimeoutError:        return []     ev.clear()    async with self._lock:        return list(self._buffers[key])`

**Почему:** если `handle()` вызовется между `peek` (пустой) и `ev.clear()` — сигнал потеряется навсегда. `clear` до `peek` исключает это окно.

---

## УМ-4: `restore_from_store()` — парсинг имени файла

**Проблема:** формат имени `inbox_{chat_id}_{topic_id}.jsonl` — `chat_id` и `topic_id` сами по себе числа, но `chat_id` может быть отрицательным (группы в Telegram имеют отрицательный `chat_id`, например `-1001234567890`).

**Требование:**

python

`# НЕПРАВИЛЬНО для отрицательных chat_id: parts = fname.replace("inbox_", "").replace(".jsonl", "").split("_") # → ["", "1001234567890", "205"] — сломается # ПРАВИЛЬНО — использовать регулярку или rsplit: name = fname.replace(".jsonl", "")  # inbox_-1001234567890_205 # Формат: inbox_{chat_id}_{topic_id} # topic_id всегда последний, chat_id — всё между "inbox_" и последним "_" prefix = "inbox_" body = name[len(prefix):]           # "-1001234567890_205" last_sep = body.rfind("_") chat_id = int(body[:last_sep])      # -1001234567890 topic_id = int(body[last_sep+1:])   # 205`

Имена файлов писать через `f"inbox_{chat_id}_{topic_id}.jsonl"` — отрицательный `chat_id` даст `inbox_-1001234567890_205.jsonl`, парсинг через `rfind("_")` это корректно обработает.

---

## УМ-5: Таймауты в `ipc_server.py` — три уровня, не путать

**Проблема:** агент может применить `DISPATCH_TIMEOUT` к `inbox_wait` или убрать один из уровней.

**Требование — три константы, три уровня:**

text

`WAIT_TIMEOUT     = 25.0  # inbox.wait() — внутренний asyncio.Event.wait IPC_WAIT_TIMEOUT = 27.0  # asyncio.wait_for(inbox.wait(...)) в dispatch DISPATCH_TIMEOUT = 30.0  # asyncio.wait_for(dispatch()) — общий потолок`

python

`# В dispatch(): elif method == "inbox_wait":     timeout = params.get("timeout", WAIT_TIMEOUT)    msgs = await asyncio.wait_for(        self.inbox.wait(chat_id, topic_id, timeout=timeout),        timeout=IPC_WAIT_TIMEOUT   # ← 27, не 30, не 25    )    return {"messages": msgs} # Обёртка вокруг dispatch(): result = await asyncio.wait_for(self._dispatch(req), timeout=DISPATCH_TIMEOUT)`

`IPC_WAIT_TIMEOUT > WAIT_TIMEOUT` — inbox успевает вернуть `[]` по таймауту до того, как IPC режет соединение.

---

## УМ-6: `handle()` — `topic_id` из `reply_to`

**Проблема:** агент может написать просто `event.message.reply_to.reply_to_top_id` и получить `AttributeError` для сообщений вне топика.

**Требование:** безопасное извлечение с fallback на `0`:

python

`topic_id = (     getattr(event.message.reply_to, "reply_to_top_id", None)    or getattr(event.message.reply_to, "reply_to_msg_id", None)    or 0 )`

Сообщения с `topic_id=0` — это общий чат группы. Daemon их обрабатывает, агент на них не подписан — просто осядут в store без читателя. Это нормально.

---

## УМ-7: `InboxEngine.__init__` — `defaultdict` с `asyncio.Event`

**Проблема:** `defaultdict(asyncio.Event)` — агент может написать `defaultdict(asyncio.Event())` (с вызовом) — создаст **один** Event для всех ключей.

**Требование:**

python

`# ПРАВИЛЬНО — фабрика (callable без скобок): self._events: dict[tuple, asyncio.Event] = defaultdict(asyncio.Event) # НЕПРАВИЛЬНО — один объект для всех: self._events = defaultdict(asyncio.Event())  # БАГ`

---

## УМ-8: `proxy.py` — `ack` происходит ДО возврата конверта агенту

**Проблема:** агент может поставить `ack` после `return` или пропустить вовсе.

**Требование:** порядок в `inbox_subscribe`:

text

`1. ipc.call("inbox_wait", ...)    → получить msgs 2. если пусто → return __INBOX_TIMEOUT__ 3. ipc.call("inbox_ack", last_id) → СНАЧАЛА ack 4. сформировать envelope 5. return envelope                 → ПОТОМ вернуть агенту`

Обоснование: если агент получил конверт, но упал до `ack` — при рестарте он снова получит те же сообщения (персистентность). Дубликаты лучше потери. Ack до return — намеренное решение.

---

## УМ-9: `TelegramSettings` — новое поле `store_dir`

**Проблема:** агент может захардкодить путь в `daemon.py` вместо добавления в `Settings`.

**Требование:** добавить поле в `TelegramSettings` (Pydantic или dataclass — в зависимости от текущей реализации):

python

`store_dir: str = "/home/gg/tgmcpd/inbox_store"`

`InboxStore` создаётся в `daemon.py` как `InboxStore(store_dir=cfg.store_dir)`. Путь не фигурирует нигде кроме `Settings` и вызова в `daemon.py`.

---

## УМ-10: Тест `test_dispatch_inbox_wait_does_not_block_ping` (C-01)

**Проблема:** это самый сложный тест — агент может написать его неправильно или пропустить.

**Требование — точная структура теста:**

python

`async def test_dispatch_inbox_wait_does_not_block_ping():     """inbox_wait в одном топике НЕ блокирует ping в другом соединении."""    # 1. Запустить IPC server    # 2. Соединение A: отправить inbox_wait (зависнет — нет сообщений)    # 3. Соединение B: отправить ping    # 4. Проверить что ping вернул {pong: true} ПОКА A висит    # 5. Отменить A     # Ключевое: asyncio.gather([wait_task, ping_task], return_when=FIRST_COMPLETED)    # ping должен вернуться быстро (<1s), wait — через timeout (~25s)`

Этот тест проверяет что `asyncio.wait_for` в dispatch не блокирует весь IPC server — т.е. что server реально конкурентен.

---

## Сводка узких мест

| #   | Где | Что может сломать агент | Критичность |
| --- | --- | ----------------------- | ----------- |

| #     | Где                    | Что может сломать агент           | Критичность |
| ----- | ---------------------- | --------------------------------- | ----------- |
| УМ-1  | `InboxStore`           | lock per-topic вместо глобального | 🟡 средняя  |
| УМ-2  | `InboxStore.ack()`     | `write_text` вместо `tmp→replace` | 🔴 высокая  |
| УМ-3  | `InboxEngine.wait()`   | неправильный порядок `clear/peek` | 🔴 высокая  |
| УМ-4  | `restore_from_store()` | парсинг отрицательного chat_id    | 🔴 высокая  |
| УМ-5  | `ipc_server.py`        | смешать три таймаута              | 🔴 высокая  |
| УМ-6  | `handle()`             | `AttributeError` на `reply_to`    | 🟡 средняя  |
| УМ-7  | `InboxEngine.__init__` | `defaultdict(Event())` с вызовом  | 🔴 высокая  |
| УМ-8  | `proxy.py`             | ack после return                  | 🟡 средняя  |
| УМ-9  | `TelegramSettings`     | захардкоженный путь               | 🟢 низкая   |
| УМ-10 | тест C-01              | пропуск или неверная структура    | 🟡 средняя  |

Подавать агенту эту спеку вместе с архитектурным документом перед каждой фазой — и указывать, какие УМ актуальны для текущей фазы (например, для Фазы 1 — только УМ-1, УМ-2, УМ-4).
