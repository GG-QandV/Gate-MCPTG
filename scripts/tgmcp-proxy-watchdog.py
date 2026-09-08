#!/usr/bin/env python3
"""tg-mcp-watchdog — supervisor для tg-mcp-proxy-wrapper.

Проблема: `tg-mcp-opencode` прокси иногда зависает на IPC-запросе (чтение из
UDS-сокета tgmcpd), особенно при больших burst'ах сообщений. tmux-сессия
agent:opencode ждёт ответа, ответ не приходит, чат выглядит зависшим.

Решение: watchdog запускает прокси и периодически пингует UDS по короткому
healthcheck-протоколу (inbox_peek для актуального топика). Если timeout
превышен, SIGTERM → grace → SIGKILL → перезапуск.

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


def _watch(proc, sock_path, topic_id, stop_evt, restarts):
    """Фон-поток: пингует UDS каждые PING_INTERVAL_S, рестартит при зависании."""
    while not stop_evt.is_set():
        time.sleep(PING_INTERVAL_S)
        if stop_evt.is_set() or proc.poll() is not None:
            return
        ok = _ping(sock_path, topic_id)
        if ok:
            continue
        # Завис: убьём и перезапустим процесс
        restarts[0] += 1
        if restarts[0] > MAX_RESTARTS:
            print(f"[watchdog] превышен лимит рестартов ({MAX_RESTARTS})", file=sys.stderr)
            return
        print(f"[watchdog] proxy завис (рестарт {restarts[0]}/{MAX_RESTARTS}), kill+restart", file=sys.stderr)
        _terminate(proc)
        # Перезапуск происходит в main-цикле


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Watchdog для tg-mcp-proxy")
    p.add_argument("--sock", default="/run/user/1000/tgmcpd.sock")
    p.add_argument("--topic", type=int, default=205)
    p.add_argument("--proxy", default="/home/gg/projects/SERVICES/MCP-TG/scripts/tgmcp-proxy-wrapper.py")
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
        proc = subprocess.Popen(
            [sys.executable, args.proxy],
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        # watchdog в фоне
        t = threading.Thread(
            target=_watch, args=(proc, args.sock, args.topic, stop_evt, restarts), daemon=True
        )
        t.start()
        rc = proc.wait()
        if stop_evt.is_set():
            break
        if restarts[0] >= MAX_RESTARTS:
            print(f"[watchdog] лимит рестартов, выхожу", file=sys.stderr)
            return 1
        time.sleep(RESTART_BACKOFF_S)
    return 0


if __name__ == "__main__":
    sys.exit(main())
