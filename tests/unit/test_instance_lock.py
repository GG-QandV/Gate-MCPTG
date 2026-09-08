"""Single-instance flock: second tgmcpd must exit 69, never unlink live socket."""
from __future__ import annotations

import pytest

from mcp_telegram.daemon import acquire_instance_lock


def test_second_instance_exits_69(tmp_path):
    sock = str(tmp_path / "tgmcpd.sock")
    acquire_instance_lock(sock)  # first instance holds the lock
    with pytest.raises(SystemExit) as exc:
        acquire_instance_lock(sock)  # second instance must not touch the socket
    assert exc.value.code == 69


def test_lock_file_contains_pid(tmp_path):
    sock = str(tmp_path / "tgmcpd.sock")
    import os

    acquire_instance_lock(sock)
    lock_file = tmp_path / "tgmcpd.lock"
    assert lock_file.read_text().strip() == str(os.getpid())


def test_lock_released_after_close(tmp_path):
    sock = str(tmp_path / "tgmcpd.sock")
    acquire_instance_lock(sock)
    import mcp_telegram.daemon as d

    d._instance_lock_fp.close()
    d._instance_lock_fp = None
    acquire_instance_lock(sock)  # must succeed now
