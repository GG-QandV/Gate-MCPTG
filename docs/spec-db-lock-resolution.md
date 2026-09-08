# Спецификация изменений: Устранение блокировок сессии Telegram (database is locked)

**Статус:** Согласовано, в процессе выполнения.
**Дата:** 2026-06-15

---

## 1. Архитектурный контекст (Architectural Context)

В версии проекта **v0.9.2 (Reactive Inbox)** внедрена клиент-серверная топология:
*   **Сервер (Daemon):** Процесс `tgmcpd` (`daemon.py`) — единственный монопольный владелец соединения Telegram (MTProto). Он инициализирует `TelegramClient`, держит блокировку SQLite-базы сессии (`mcp_telegram_session.session`) и слушает входящие сообщения.
*   **Клиент (Proxy):** Процесс `proxy.py` (или `tgmcp-proxy-wrapper.py`) — легковесный мост, который запускается IDE/агентом. Он не создает соединение с Telegram напрямую, а общается с демоном по IPC через Unix-сокет (`tgmcpd.sock`).

**Архитектурный сбой (Gap):** Логика запуска дублирующих процессов (через systemd и `run_server.sh`) нарушает этот принцип, приводя к одновременной попытке нескольких процессов открыть SQLite сессию, что вызывает ошибку `database is locked`.

---

## 2. Предлагаемые изменения по компонентам

### Компонент А: Юнит службы `tgmcpd.service`
*   **Файл:** `/home/gg/.config/systemd/user/tgmcpd.service`
*   **Архитектурное изменение:** 
    1. Удалить `ExecStartPre=/bin/rm -f %t/tgmcpd.sock`. Принудительное удаление сокета системой ломает логику `_check_stale_socket` в Python (демон не может обнаружить уже запущенный ручной процесс, так как его сокет удален).
    2. Добавить `RestartPreventExitStatus=69`. Если демон завершается с кодом 69 (указывающим на то, что другой процесс уже занял сокет), systemd должен прекратить попытки перезапуска, чтобы не спамить в лог.
*   **Diff:**
    ```diff
     [Service]
     Type=simple
     WorkingDirectory=/home/gg/projects/MCP-TG
    -ExecStartPre=/bin/rm -f %t/tgmcpd.sock
     ExecStart=/home/gg/.mnemostroma/venv/bin/python -m src.mcp_telegram.daemon
     Restart=always
     RestartSec=5
    +RestartPreventExitStatus=69
    ```

### Компонент Б: Логика инициализации демона (`daemon.py`)
*   **Файл:** `/home/gg/projects/MCP-TG/src/mcp_telegram/daemon.py`
*   **Архитектурное изменение:**
    При обнаружении активного сокета (запущенного другого демона) завершать работу с кодом `69` (EX_UNAVAILABLE в BSD-стиле) вместо общего кода `1`. Это позволит разделить системные сбои (требующие перезапуска) и штатный выход при конфликте портов/сокетов.
*   **Diff:**
    ```diff
     async def _check_stale_socket(sock_path: str) -> None:
         p = Path(sock_path)
         if not p.exists():
             return
         try:
             _, writer = await asyncio.wait_for(
                 asyncio.open_unix_connection(sock_path), timeout=1.0
             )
             writer.close()
             logger.error("Another tgmcpd instance is already running")
    -        sys.exit(1)
    +        sys.exit(69)
         except (ConnectionRefusedError, OSError):
    ```

### Компонент В: Логика реактивного ожидания (`inbox.py`)
*   **Файл:** `/home/gg/projects/MCP-TG/src/mcp_telegram/inbox.py`
*   **Архитектурное изменение:**
    Привести метод `wait()` в строгое соответствие с требованием **УМ-3** (порядок `clear -> peek -> wait`). Текущая версия (`peek -> wait -> clear`) оставляет событие `ev` взведенным после успешного деплоя, что заставляет прокси-клиент делать холостой цикл (лишний пустой запрос) на следующем шаге.
*   **Diff:**
    ```diff
         key = (chat_id, topic_id)
         ev = self._events[key]
+        ev.clear()  # 1. Сначала сбрасываем событие
         async with self._lock:
             existing = list(self._buffers.get(key, []))
             if existing:
                 return existing
         await ev.wait()  # blocks indefinitely — no polling
         ev.clear()
    ```

### Компонент Г: Скрипт запуска разработчика (`run_server.sh`)
*   **Файл:** `/home/gg/projects/MCP-TG/run_server.sh`
*   **Архитектурное изменение:**
    Переключить скрипт с запуска полноценного сервера (`mcp-tg` v1) на запуск прокси-клиента (`mcp-tg proxy`). Это защитит базу данных от блокировок, если разработчик случайно запустит скрипт параллельно с демоном.
*   **Diff:**
    ```diff
     while true; do
    -    mcp-tg
    +    mcp-tg proxy
         sleep 1
     done
    ```

---

## 3. План тестирования (Testing & Verification)

### Автоматические тесты (Unit Tests)

1.  **Тест `test_daemon.py`:**
    Обновить `test_live_socket_causes_exit`, чтобы убедиться, что `sys.exit` генерирует именно код `69` при живом сокете.
    ```python
    with pytest.raises(SystemExit) as excinfo:
        await _check_stale_socket(tmp_sock_path)
    assert excinfo.value.code == 69
    ```

2.  **Тест `test_inbox.py` (Соответствие УМ-3):**
    Добавить `test_wait_clears_event_on_entry` для проверки того, что `wait` сбрасывает событие перед проверкой буфера, предотвращая холостые циклы:
    ```python
    @pytest.mark.asyncio
    async def test_wait_clears_event_on_entry(inbox):
        key = (999, 999)
        ev = inbox._events[key]
        ev.set()  # Взводим событие вручную
        task = asyncio.create_task(inbox.wait(999, 999))
        await asyncio.sleep(0.05)
        assert not task.done()  # Должен заблокироваться (так как событие сброшено), а не завершиться
        task.cancel()
    ```

### Ручное тестирование

1.  Запуск ручного демона и проверка блокировки перезапуска службы systemd (код `69` не вызывает бесконечный перезапуск).
2.  Отправка контрольного сообщения через сокет и мониторинг логов (`journalctl`).
