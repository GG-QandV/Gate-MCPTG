from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from .ipc_client import IPCClient, get_sock_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.exit(f"ERROR: {name} env var is required")
    return val


TG_TOPIC_ID = int(_require_env("TG_TOPIC_ID"))
TG_CHAT_ID = int(_require_env("TG_CHAT_ID"))
TG_AGENT_NAME = os.environ.get("TG_AGENT_NAME", "Agent")

server = Server("tg-mcp-proxy")
_ipc: IPCClient | None = None


async def get_ipc() -> IPCClient:
    global _ipc
    if _ipc is None:
        _ipc = IPCClient(get_sock_path())
        await _ipc.connect()
    return _ipc


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="send_message",
            description="Send message to Telegram topic",
            inputSchema={
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
            },
        ),
        Tool(
            name="inbox_read",
            description="Read and acknowledge new messages from topic",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="send_file",
            description="Send a file from local filesystem to Telegram topic",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": "Absolute path to file on local filesystem"},
                    "caption": {"type": "string", "description": "Optional caption text"},
                },
                "required": ["file_path"],
            },
        ),
        Tool(
            name="download_file",
            description="Download a file from Telegram message to local filesystem",
            inputSchema={
                "type": "object",
                "properties": {
                    "chat_id": {"type": "integer", "description": "Chat ID where the file message is"},
                    "message_id": {"type": "integer", "description": "Message ID containing the file"},
                    "output_path": {"type": "string", "description": "Absolute path to save the file (optional, auto-generated if omitted)"},
                },
                "required": ["chat_id", "message_id"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    ipc = await get_ipc()

    if name == "send_message":
        from datetime import datetime
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        signed = f"**{TG_AGENT_NAME}** · {now}\n\n{arguments['text']}"
        result = await ipc.call("send_message", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
            "text": signed,
        })
        return [TextContent(
            type="text",
            text=f"sent message_id={result['message_id']}"
        )]

    elif name == "inbox_read":
        peek_result = await ipc.call("inbox_peek", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
        })
        msgs = peek_result.get("messages", [])

        if msgs:
            last_id = msgs[-1]["id"]
            await ipc.call("inbox_ack", {
                "chat_id": TG_CHAT_ID,
                "topic_id": TG_TOPIC_ID,
                "last_id": last_id,
            })

        return [TextContent(
            type="text",
            text=json.dumps(msgs, ensure_ascii=False, indent=2) if msgs else "[]",
        )]

    elif name == "send_file":
        file_path = arguments["file_path"]
        if not os.path.exists(file_path):
            return [TextContent(type="text", text=f"Error: file not found: {file_path}")]
        result = await ipc.call("send_file", {
            "chat_id": TG_CHAT_ID,
            "topic_id": TG_TOPIC_ID,
            "file_path": file_path,
            "caption": arguments.get("caption", ""),
        })
        return [TextContent(type="text", text=f"File sent. message_id={result['message_id']}")]

    elif name == "download_file":
        result = await ipc.call("download_file", {
            "chat_id": arguments["chat_id"],
            "message_id": arguments["message_id"],
            "output_path": arguments.get("output_path"),
        })
        return [TextContent(type="text", text=f"File saved to: {result['path']}")]

    raise ValueError(f"Unknown tool: {name}")


async def run() -> None:
    async with stdio_server() as (r, w):
        await server.run(r, w, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(run())
