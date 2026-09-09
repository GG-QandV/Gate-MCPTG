# Spec: systemd Socket Activation для tgmcpd (п.2)

**Status:** Draft → реализация после ревью
**Date:** 2026-09-09
**Version target:** 0.9.3
**Baseline:** v0.9.2 + flock single-instance (коммит b756045, 95 тестов зелёных)
**Supersedes:** раздел «Lifecycle» в `docs/architecture-legacy.md`; уточняет `spec-v0.9.2-finalization.md` §2 (IPC Server)
**Related:** `spec-db-lock-resolution.md`, `developer-guide.md`, `AGENTS.md`

---

## 1. Проблема (почему вообще п.2)

История отказа за 2026-09-08/09: сокет `/run/user/1000/tgmcpd.sock` периодически
исчезал из файловой системы при живом листенере (orphaned inode), мониторы
криали `CRITICAL: socket not found`, агенты теряли IPC.

Корень: **файл сокета принадлежал процессу демона**, а жизненным циклом файла
управляли несколько акторов (демон, второй инстанс, stale-check) — классическая
гонка «кто удалит файл». П.1 (flock) закрыл гонку инстансов, но:

- файл по-прежнему создаёт/удаляет демон (`ipc.start` → bind, `finally` → unlink);
- «socket not found» принципиально возможно (файла нет — клиенты падают мгновенно);
- монитор проверяет наличие файла, а не здоровье сервиса;
- при рестарте демона есть окно, когда файла нет вообще.

**Решение:** передать владение сокетом systemd (socket activation). Демон больше
не создаёт и не удаляет файл — он получает готовый listening fd. Файл существует
с момента старта `tgmcpd.socket` и не исчезает никогда (пока unit включён).

## 2. Целевая архитектура

```
systemd (user)
  ├─ tgmcpd.socket  ← ЕДИНСТВЕННЫЙ владелец файла /run/user/1000/tgmcpd.sock
  │     ListenStream, SocketMode=0600, RemoveOnStop=true
  │     держит listening fd ВСЕГДА (независимо от состояния сервиса)
  │     первый коннект → стартует tgmcpd.service
  └─ tgmcpd.service
        Requires=tgmcpd.socket
        получает fd через LISTEN_FDS/LISTEN_PID (sd_listen_fds семантика)
        НЕ bind-ит, НЕ unlink-ит, _check_stale_socket не выполняет
        flock остаётся (защита от ручного запуска)

Клиенты (proxy/Hermes/monitor): connect по пути — путь есть всегда;
при мёртвом сервисе → ECONNREFUSED (а не FileNotFoundError), клиент
ретраит (CONNECT_RETRIES=10 × 0.5s = 5s окно рестарта).
```

### Контракт владения файлом (главный инвариант спеки)

| Режим | Кто bind-ит | Кто unlink-ит | «not found» возможен? |
|---|---|---|---|
| **activation** (systemd) | systemd | systemd (`RemoveOnStop=true`) | **нет** |
| **fallback** (ручной запуск) | демон | демон (только владелец лока, только свой файл) | да (как в 0.9.2) |

Инвариант: в activation-режиме демон **не выполняет ни одной файловой операции
над путём сокета** (grep-проверка: unlink/chmod/touch отсутствуют в ветке activation).

## 3. Изменения по компонентам

### 3.1. `scripts/tgmcpd.socket` (новый unit)

```ini
[Unit]
Description=tgmcpd IPC socket (owner of the socket file)

[Socket]
ListenStream=%t/tgmcpd.sock
SocketMode=0600
RemoveOnStop=true
Service=tgmcpd.service

[Install]
WantedBy=sockets.target
```

- `SocketMode=0600` — как текущий `os.chmod(0o600)`; проверка `SO_PEERCRED` в
  `IPCServer._handle_client` остаётся (второй слой).
- `RemoveOnStop=true` — systemd сам чистит файл при `systemctl --user stop tgmcpd.socket`.
- `Service=tgmcpd.service` — явная привязка (иначе systemd выведет имя из имени unit).

### 3.2. `scripts/tgmcpd.user.service`

```diff
 [Unit]
 Description=Telegram MCP Daemon (tgmcpd)
-After=network.target
-Wants=network.target
+After=network.target
+Wants=network.target
+Requires=tgmcpd.socket
```

- `After` на socket не нужен: `Requires` уже сериализует (systemd сам ставит ordering).
- `Restart=always`, `RestartSec=5`, `RestartPreventExitStatus=69`, `EnvironmentFile` — без изменений.

### 3.3. `src/mcp_telegram/daemon.py`

Новый хелпер (модульный уровень, покрывается тестами напрямую):

```python
SD_LISTEN_FDS_START = 3

def sd_listen_fds() -> list[socket.socket]:
    """Parse LISTEN_PID/LISTEN_FDS per sd_listen_fds(3). FDs are consumed
    exactly once (env vars cleared) and wrapped with FD_CLOEXEC."""
```

Логика `main()`:

```
fds = sd_listen_fds()
acquire_instance_lock(sock_path)          # ВСЕГДА, до всего (см. §5)
if fds:
    validate_activation_socket(fds[0], sock_path)   # exit 69 при несовпадении
    server = await ipc.bind(sock=fds[0])            # bind уже сделан systemd
    unlink/chmod/stale-check — НЕ ВЫПОЛНЯТЬ
else:
    await _check_stale_socket(...)                  # 0.9.2: probe под локом
    server = await ipc.bind(sock_path=sock_path)    # OSError EADDRINUSE → exit 69
try:
    gather(ipc.serve(server), client.run_until_disconnected(), bridge.start())
finally:
    if not from_activation: unlink                  # только владелец лока, только свой файл
```

`IPCServer` разделяется на `bind()` (создаёт server, возвращает `AbstractServer`)
и `serve(server)` (`async with server: serve_forever`). `start()` остаётся тонкой
обёрткой `bind+serve` для обратной совместимости. Ровно один из аргументов
`sock_path` / `sock` задан — иначе `ValueError`.

EADDRINUSE в fallback отсекается probe'ом `_check_stale_socket` **до** bind
(под локом любой слушающий на пути — это tgmcpd.socket → exit 69, файл не
тронут); если всё же случился — `SystemExit(69)` возникает в `bind`, т.е. **до**
`try/finally`, unlink невозможен по построению.

Краевые случаи `sd_listen_fds()`:

| Случай | Поведение |
|---|---|
| `LISTEN_PID` ≠ наш pid | вернуть `[]` (fallback) |
| `LISTEN_FDS` = 0 / отсутствует | `[]` (fallback) |
| `LISTEN_FDS` ≥ 1 | взять fd 3, остальным `os.close()` + warning |
| fd не является listening unix socket ИЛИ `getsockname()` ≠ ожидаемый путь | `validate_activation_socket()` → **exit 69** (запуск с кривым unit хуже честного падения) |

Env (`LISTEN_PID`/`LISTEN_FDS`/`LISTEN_FDNAMES`) потребляется ровно один раз
(очищается при вызове — повторный вызов вернёт `[]`, семантика sd_listen_fds(3)).

### 3.4. `_check_stale_socket` — остаётся только в fallback

В activation-режиме файл живой всегда (его держит systemd) — любые операции
над ним из демона запрещены. В fallback функционально без изменений (0.9.2:
unlink под локом).

### 3.5. `flock` (п.1) — без изменений, зачем он теперь

- systemd гарантирует один сервис на socket unit, **но не запрещает** запуск
  `python -m src.mcp_telegram.daemon` руками — наш реальный источник инцидента.
- Ручной запуск при активном сервисе: лок занят → exit 69 (как сейчас).
- Ручной запуск при мёртвом сервисе: лок свободен, но bind на существующий
  файл (systemd листенит) → `EADDRINUSE` → **exit 69 с сообщением** «socket is
  owned by tgmcpd.socket — use systemctl --user start tgmcpd». Unlink запрещён.

### 3.6. `~/.local/bin/tgmcpd-monitor` — новая семантика проверок

Старая логика `test -S $SOCK` **удаляется** — «socket not found» исчезает как класс.

| # | Проверка | Метод | CRITICAL если |
|---|---|---|---|
| 1 | socket unit активен | `systemctl --user is-active tgmcpd.socket` ≠ active | unit выключен (misconfig) |
| 2 | файл есть и слушает | `socat UNIX-CONNECT` | connection refused (сервис мёртв и не стартует) |
| 3 | health | JSON `status`/`telegram`/store (как сейчас) | деградация |

Порядок: 1 → 2 → 3. Сообщения:
- `CRITICAL: tgmcpd.service down (socket alive)` — сервис не поднялся после activation trigger;
- `CRITICAL: IPC connect refused on $SOCK` — окно рестарта > 5s;
- текст «socket not found» не должен встречаться нигде (grep-проверка).

Self-heal НЕ добавляем (решено в обсуждении п.1): монитор — наблюдатель;
восстановление — `Restart=always` сервиса + activation trigger от коннекта.

### 3.7. Клиенты (`ipc_client.py`, proxy, Hermes, watchdog)

Без изменений кода. Ретраи `CONNECT_RETRIES=10, CONNECT_DELAY=0.5` покрывают
окно рестарта сервиса ~5s. Поведение при мёртвом сервисе меняется:
`FileNotFoundError` → `ConnectionRefusedError` (обоего обработаны в
`IPCClient.connect`; проверяется тестом).

### 3.8. Миграция (deploy) — ПОРЯДОК КРИТИЧЕН

```bash
cp scripts/tgmcpd.socket ~/.config/systemd/user/
cp scripts/tgmcpd.user.service ~/.config/systemd/user/tgmcpd.service
systemctl --user daemon-reload

# 1) СНАЧАЛА остановить старый (fallback) демон — иначе socket unit
#    стартует с 'Socket service already active, refusing' → 'Failed to
#    listen' → зомби-unit: файл сокета удалён, демон держит orphaned inode
systemctl --user stop tgmcpd
# 2) затем сокет (он создаёт файл)
systemctl --user enable --now tgmcpd.socket
# 3) затем сервис
systemctl --user restart tgmcpd

systemctl --user status tgmcpd.socket tgmcpd.service
```

**Симптом гонки деплоя:** файл сокета отсутствует при `tgmcpd.socket = active`,
в journal пара «refusing / Failed to listen» + «Listening on» в одну секунду,
в `ss -x` слушатель есть, но только у демона (orphaned inode), у systemd fd нет.
Лечение: `systemctl --user stop tgmcpd tgmcpd.socket` → `start tgmcpd.socket` →
`start tgmcpd`. Защита в мониторе — zombie guard (§3.6, шаг 1b).

Проверка корректности владения: `ss -x -lpn | grep tgmcpd.sock` должен показывать
**двух** держателей LISTEN — `systemd` и `python` (fd демона).

Откат: `systemctl --user disable --now tgmcpd.socket` + убрать `Requires` +
рестарт сервиса (fallback-режим полностью совместим с 0.9.2-инфраструктурой).

## 4. Новые тесты (обязательные к реализации)

Все — unit, pytest+pytest-asyncio, без сети и реального Telegram.
Итог: **95 существующих + 9 новых + 1 интеграционный (test_daemon, activation main-path) = 105**.

### `tests/unit/test_socket_activation.py` (новый файл, 9 тестов)

1. **`test_sd_listen_fds_parses_own_pid`** — Given `LISTEN_PID=os.getpid()`,
   `LISTEN_FDS=1`, реальный listening unix socket, продублированный fd которого
   выставлен как `SD_LISTEN_FDS_START` (monkeypatch — pytest держит fd 3).
   When `sd_listen_fds()`. Then вернёт 1 socket, `getsockname()` == путь,
   env-переменные очищены (повторный вызов → `[]`).

2. **`test_sd_listen_fds_pid_mismatch_fallback`** — Given `LISTEN_PID=999999`,
   `LISTEN_FDS=1`. Then → `[]`, env очищен.

3. **`test_sd_listen_fds_absent_fallback`** — Given env без LISTEN_*. Then → `[]`.

4. **`test_sd_listen_fds_multiple_uses_first`** — Given `LISTEN_FDS=2`, два
   валидных fd. Then возвращён только первый, второй закрыт (`fileno() == -1`).

5. **`test_activation_fd_invalid_exit69`** — Given socket **без** `listen()`
   и/или с `getsockname()` ≠ ожидаемый путь. When `validate_activation_socket()`.
   Then `SystemExit` code 69.

6. **`test_ipc_server_bind_requires_exactly_one_arg`** — `ipc.bind()` без
   аргументов и с обоими сразу → `ValueError` (оба варианта).

7. **`test_activation_mode_does_not_unlink_or_chmod`** — Given listening socket
   (fd-режим), `Path.unlink` и `os.chmod` под monkeypatch. When `bind(sock=...)`
   + `serve` + реальный клиентский `ping` (roundtrip OK) + graceful stop.
   Then unlink/chmod **не вызваны**, файл существует.

8. **`test_fallback_probe_live_socket_exit69`** — Given файл с живым листенером
   (симуляция «systemd держит сокет», лок свободен). When `_check_stale_socket()`.
   Then `SystemExit` code 69, файл **не удалён**.

9. **`test_ipc_client_refused_when_service_dead`** — Given путь с существующим
   файлом, но без листенера (bind+close, файл остался — состояние «сервис
   мёртв при живом socket unit»), `CONNECT_DELAY=0`. When `IPCClient.connect()`.
   Then `ConnectionRefusedError` (НЕ FileNotFoundError), после CONNECT_RETRIES.

### Правки существующих

7. **`tests/unit/test_daemon.py`** — оба теста `main()` (`test_restore_called_on_start`,
   `test_bridge_created_and_started`) получают `patch("src.mcp_telegram.daemon.sd_listen_fds", return_value=[])`
   → код идёт по fallback-ветке; плюс по одному варианту с `return_value=[mock_sock]`
   asserting: `ipc.start` вызван с `sock=...` и `Path.unlink` не вызван.
   (`+2 ассерта` внутри существующих тестов, без новых файлов).

8. **`test_instance_lock.py`** — без изменений (flock ортогонален; гонка
   «лок ↔ activation» покрыта №5).

### E2E чек-лист (ручной, в acceptance)

- [ ] `systemctl --user start tgmcpd.socket` → файл `srw-------` существует **до** старта сервиса
- [ ] `systemctl --user stop tgmcpd.service` → `socat ... UNIX-CONNECT` проходит (systemd слушает), health-JSON недоступен → монитор даёт CRITICAL `service down`, НЕ «not found»
- [ ] первый коннект агента стартует сервис (activation trigger)
- [ ] `kill -9 <pid>` → systemd рестарт ≤5s → ретраи клиента пробивают окно без ошибки наружу
- [ ] ручной `python -m src.mcp_telegram.daemon` при живом сервисе → exit 69 (flock)
- [ ] ручной запуск при мёртвом сервисе → exit 69 (EADDRINUSE), файл на месте
- [ ] `journalctl --user -u tgmcpd` не содержит «Removing stale socket» в activation-режиме
- [ ] `grep -rn unlink src/mcp_telegram/` — unlink только в fallback-ветке daemon.py

## 5. Риски и решения

| Риск | Вероятность | Митигация |
|---|---|---|
| Регрессия ручного dev-запуска | средняя | fallback-ветка сохранена, тестируется (№3, №5) |
| fd от systemd окажется не тем | низкая | строгая проверка `getsockname()` == ожидаемый путь, иначе exit 69 |
| Monitor ложно CRITICAL в окне рестарта | низкая | проверка №2 до health; окно ретраев клиента 5s |
| Двойное владение (кто-то забыл RemoveOnStop) | низкая | инвариант §2 + grep-проверка в acceptance |
| user manager без поддержи %t | нет (Ubuntu 22+ проверено) | — |

## 6. Acceptance criteria

1. `pytest tests/ -q` → 105 passed, 0 failed, 0 warnings (Pydantic уже починен).
2. Все 8 пунктов E2E чек-листа §4 выполнены на машине.
3. Инвариант §2 подтверждён grep'ом (нет unlink/chmod в activation-ветке).
4. В `journalctl` за 24h после деплоя — ноль `socket not found`.
5. Версия bump 0.9.2 → 0.9.3 (`setup.py`, `_version.py`, README), amend в
   initial-коммит по установленному процессу (без версий в истории гита).

## 7. План работ

1. `daemon.py`: `sd_listen_fds()` + режим activation/fallback в `main()` и `IPCServer.start` (§3.3).
2. Units: `tgmcpd.socket`, правка `tgmcpd.service` (§3.1–3.2).
3. Тесты: `test_socket_activation.py` (6) + правки `test_daemon.py` (§4).
4. Монитор: новая семантика §3.6.
5. E2E чек-лист на машине, деплой по §3.8, версия 0.9.3.

Оценка: ~150 строк прод-кода + ~120 строк тестов, 4 файла прод + 2 тестовых + монитор.
