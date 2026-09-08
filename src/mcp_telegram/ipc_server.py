from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import struct
from pathlib import Path

logger = logging.getLogger(__name__)


def get_sock_path() -> str:
    explicit = os.environ.get("TGMCPD_SOCK")
    if explicit:
        return explicit
    runtime = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    return f"{runtime}/tgmcpd.sock"


WAIT_TIMEOUT = 25.0


class IPCServer:
    DISPATCH_TIMEOUT = 30.0

    def __init__(self, inbox: "InboxEngine", client: "TelegramClient"):
        self.inbox = inbox
        self.client = client

    async def start(self, sock_path: str) -> None:
        server = await asyncio.start_unix_server(
            self._handle_client, path=sock_path
        )
        os.chmod(sock_path, 0o600)
        logger.info("IPC server listening on %s", sock_path)
        async with server:
            await server.serve_forever()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        raw = writer.get_extra_info("socket").getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED,
            struct.calcsize("3i")
        )
        _, uid, _ = struct.unpack("3i", raw)
        if uid != os.getuid():
            logger.warning("IPC rejected connection from uid=%d", uid)
            writer.close()
            return

        peer_fd = writer.get_extra_info("socket").fileno()
        logger.debug("IPC client connected fd=%d", peer_fd)

        try:
            while True:
                try:
                    line = await asyncio.wait_for(
                        reader.readline(), timeout=self.DISPATCH_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.warning("IPC client fd=%d read timeout", peer_fd)
                    break

                if not line:
                    break

                try:
                    req = json.loads(line)
                except json.JSONDecodeError as e:
                    resp = {
                        "error": {"code": -32700, "message": f"parse error: {e}"},
                        "id": None,
                    }
                    writer.write(json.dumps(resp).encode() + b"\n")
                    await writer.drain()
                    continue

                req_id = req.get("id")

                task = asyncio.create_task(self._dispatch(req))
                try:
                    result = await asyncio.wait_for(
                        asyncio.shield(task), timeout=self.DISPATCH_TIMEOUT
                    )
                    resp = {"result": result, "id": req_id}
                except asyncio.TimeoutError:
                    task.cancel()
                    resp = {
                        "error": {"code": -32000, "message": "timeout"},
                        "id": req_id,
                    }
                except Exception as e:
                    resp = {
                        "error": {"code": -32603, "message": str(e)},
                        "id": req_id,
                    }

                writer.write(json.dumps(resp, ensure_ascii=False).encode() + b"\n")
                await writer.drain()

        except (ConnectionResetError, BrokenPipeError):
            logger.debug("IPC client fd=%d disconnected", peer_fd)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch(self, req: dict) -> dict:
        method = req.get("method")
        params = req.get("params", {})

        if method == "ping":
            return {"pong": True}

        elif method == "send_message":
            chat_id = params["chat_id"]
            topic_id = params.get("topic_id")
            text = params["text"]
            msg = await self.client.send_message(
                chat_id, text,
                reply_to=topic_id if topic_id else None,
            )
            return {"message_id": msg.id}

        elif method == "inbox_peek":
            chat_id = params["chat_id"]
            topic_id = params.get("topic_id", 0)
            msgs = await self.inbox.peek(chat_id, topic_id)
            return {"messages": msgs}

        elif method == "inbox_ack":
            chat_id = params["chat_id"]
            topic_id = params.get("topic_id", 0)
            last_id = params["last_id"]
            dropped = await self.inbox.ack(chat_id, topic_id, last_id)
            return {"dropped": dropped}

        elif method == "send_file":
            chat_id = params["chat_id"]
            topic_id = params.get("topic_id")
            file_path = params["file_path"]
            caption = params.get("caption", "")
            msg = await self.client.send_file(
                chat_id, file_path,
                caption=caption or None,
                reply_to=topic_id if topic_id else None,
                force_document=True,
            )
            return {"message_id": msg.id}

        elif method == "download_file":
            chat_id = params["chat_id"]
            message_id = params["message_id"]
            output_path = params.get("output_path")
            msg = await self.client.get_messages(chat_id, ids=message_id)
            if not msg or not msg.file:
                raise ValueError("No file found in message")
            if output_path:
                dest = output_path
            else:
                import tempfile
                safe_name = Path(msg.file.name or f"file_{message_id}").name
                dest = str(Path(tempfile.gettempdir()) / f"tg_dl_{chat_id}_{message_id}_{safe_name}")
            dest_path = await msg.download_media(file=dest)
            return {"path": str(dest_path), "name": msg.file.name, "size": msg.file.size}

        elif method == "health":
            store_health = await self.inbox.store.health_check()
            buffers = {
                f"{k[0]}_{k[1]}": len(v)
                for k, v in self.inbox._buffers.items()
            }
            return {
                "status": "ok",
                "store": store_health,
                "ram_buffers": buffers,
                "telegram": self.client.is_connected(),
            }

        else:
            raise ValueError(f"Unknown method: {method}")
