#!/usr/bin/env python3
"""tg-mcp-watchdog — supervisor для tg-mcp-proxy-wrapper.

Проблема: `tg-mcp-opencode` прокси иногда зависает на IPC-запросе (чтение из
UDS-сокета tgmcpd), особенно при больших burst'ах сообщений. tmux-сессия
agent:opencode ждёт ответа, ответ не приходит, чат выглядит зависшим.

Решение: watchdog запускает прокси и периодически пингует UDS по короткому
healthcheck-протоколу (inbox_peek для актуального топика). Если timeout
превышен, SIGTERM → grace → SIGKILL → перезапуск.

Важно: watchdog осмысленен ТОЛЬКО когда у прокси есть живой клиент на stdin
(MCP-клиент: opencode/claude в tmux и т.п.). Без клиента stdio-сервер
получает EOF и мгновенно выходит с rc=0 — рестартить его в цикле
бессмысленно: такой случай определяется по короткому uptime + rc==0,
watchdog завершается с кодом 0, никого не крутя.

Использование:
  python3 tgmcp-proxy-watchdog.py --sock /run/user/1000/tgmcpd.sock --topic 205
"""
from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
import threading

PING_INTERVAL_S = 30.0
PING_TIMEOUT_S = 15.0
TERM_GRACE_S = 3.0
MAX_RESTARTS = 5
RESTART_BACKOFF_S = 5.0
# Ребёнок, проживший меньше этого времени и вышедший с rc==0, не висел,
# а получил EOF без клиента — рестартить бессмысленно, выходим.
MIN_UPTIME_S = 10.0


def _ping(sock_path: str, topic_id: int) -> bool:
    """Отправить JSON-RPC `inbox_peek` через UDS, ждать ответ <PING_TIMEOUT_S."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(PING_TIMEOUT_S)
        s.connect(sock_path)
        req = (
            '{"jsonrpc":"2.0","id":1,"method":"inbox_peek",'
            '"params":{"chat_id":-1003998609906,"topic_id":'
            + str(topic_id)
            + ',"limit":1}}\n'
        ).encode()
        s.sendall(req)
        data = s.recv(65536)
        s.close()
        return b'"ok"' in data or b'"result"' in data
    except Exception:
        return False


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=TERM_GRACE_S)
    except (OSError, subprocess.TimeoutExpired):
        try:
            proc.kill()
        except OSError:
            pass


def _watch(proc, sock_path, topic_id, stop_evt, ping_interval: float) -> None:
    """Фон-поток: пингует UDS, при зависании убивает процесс.

    Перезапуск НЕ здесь: главный цикл считает ВСЕ рестарты (и зависания,
    и падения), иначе лимит MAX_RESTARTS не срабатывает.
    """
    while not stop_evt.is_set():
        if stop_evt.wait(ping_interval):
            return
        if stop_evt.is_set() or proc.poll() is not None:
            return
        if _ping(sock_path, topic_id):
            continue
        # Завис: убить; перезапуск произойдёт в main-цикле
        print(f"[watchdog] proxy не отвечает на ping, kill+restart", file=sys.stderr)
        _terminate(proc)
        return


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Watchdog для tg-mcp-proxy")
    p.add_argument("--sock", default="/run/user/1000/tgmcpd.sock")
    p.add_argument("--topic", type=int, default=205)
    p.add_argument("--proxy", default="/home/gg/projects/SERVICES/MCP-TG/scripts/tgmcp-proxy-wrapper.py")
    p.add_argument("--ping-interval", type=float, default=PING_INTERVAL_S)
    p.add_argument("--min-uptime", type=float, default=MIN_UPTIME_S)
    p.add_argument("--max-restarts", type=int, default=MAX_RESTARTS)
    p.add_argument("--backoff", type=float, default=RESTART_BACKOFF_S)
    args = p.parse_args(argv)

    if not os.path.exists(args.sock):
        print(f"[watchdog] UDS {args.sock} не найден", file=sys.stderr)
    if not os.path.exists(args.proxy):
        print(f"[watchdog] proxy {args.proxy} не найден", file=sys.stderr)
        return 2

    stop_evt = threading.Event()
    restarts = [0]

    # Прокидываем SIGTERM/SIGINT для чистого завершения
    def _shutdown(signum, frame):
        stop_evt.set()
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    while not stop_evt.is_set():
        start = time.monotonic()
        proc = subprocess.Popen(
            [sys.executable, args.proxy],
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        # watchdog в фоне
        t = threading.Thread(
            target=_watch, args=(proc, args.sock, args.topic, stop_evt, args.ping_interval), daemon=True
        )
        t.start()
        rc = proc.wait()
        uptime = time.monotonic() - start
        if stop_evt.is_set():
            break
        # ЛЮБОЙ перезапуск считается: и зависание, и падение, и чистый выход.
        restarts[0] += 1
        if restarts[0] > args.max_restarts:
            print(f"[watchdog] лимит рестартов ({args.max_restarts}), выхожу", file=sys.stderr)
            return 1
        if uptime < args.min_uptime and rc == 0:
            # Чистый мгновенный выход = EOF без клиента на stdin.
            # Супервизить нечего: зависания тут быть не может.
            print(
                f"[watchdog] proxy вышел сам (uptime {uptime:.1f}s, rc=0) — "
                f"клиента нет, выхожу без рестарта",
                file=sys.stderr,
            )
            return 0
        print(
            f"[watchdog] рестарт {restarts[0]}/{args.max_restarts} "
            f"через {args.backoff}s (uptime {uptime:.1f}s, rc={rc})",
            file=sys.stderr,
        )
        stop_evt.wait(args.backoff)
    return 0


if __name__ == "__main__":
    sys.exit(main())
