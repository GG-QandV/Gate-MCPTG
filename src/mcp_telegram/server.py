from __future__ import annotations

import asyncio
import inspect
import logging
import typing as t
from collections.abc import Sequence
from functools import cache
from pathlib import Path

from mcp.server import Server
from mcp.types import (
    EmbeddedResource,
    ImageContent,
    Prompt,
    Resource,
    ResourceTemplate,
    TextContent,
    Tool,
)

from . import inbox, tools
from .telegram import TelegramSettings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Server("mcp-telegram")


@cache
def enumerate_available_tools() -> t.Generator[tuple[str, Tool], t.Any, None]:
    for _, tool_args in inspect.getmembers(tools, inspect.isclass):
        if issubclass(tool_args, tools.ToolArgs) and tool_args != tools.ToolArgs:
            logger.debug("Found tool: %s", tool_args)
            description = tools.tool_description(tool_args)
            yield description.name, description


mapping: dict[str, Tool] = dict(enumerate_available_tools())


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    return []


@app.list_resources()
async def list_resources() -> list[Resource]:
    return []


@app.list_tools()
async def list_tools() -> list[Tool]:
    return list(mapping.values())


@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    return []


@app.progress_notification()
async def progress_notification(pogress: str | int, p: float, s: float | None) -> None:
    pass


@app.call_tool()
async def call_tool(name: str, arguments: t.Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
    if arguments is None:
        arguments = {}
    elif not isinstance(arguments, dict):
        raise TypeError(f"arguments must be dictionary, got {type(arguments).__name__}")

    tool = mapping.get(name)
    if not tool:
        raise ValueError(f"Unknown tool: {name}")

    try:
        args = tools.tool_args(tool, **arguments)
        return await tools.tool_runner(args)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("Error running tool: %s", name)
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def _run_inbox() -> None:
    import os
    from telethon import TelegramClient, events
    from xdg_base_dirs import xdg_state_home

    config = TelegramSettings()
    session_path = str(xdg_state_home() / "mcp-telegram" / "mcp_telegram_session")

    for f in Path(xdg_state_home() / "mcp-telegram").glob("*.session-journal"):
        try:
            f.unlink()
        except OSError:
            pass

    client = TelegramClient(session_path, config.api_id, config.api_hash, base_logger="telethon")
    await client.start()
    logger.info("inbox: connected")

    chat_id = int(os.environ.get("TG_TARGET_CHAT", "0"))
    thread_id = int(os.environ.get("TG_TARGET_THREAD", "0"))

    @client.on(events.NewMessage)
    async def handler(event):
        msg = event.message
        if not msg or not msg.text:
            return
        if chat_id and msg.chat_id != chat_id:
            return
        t_id = 0
        r = getattr(msg, 'reply_to', None)
        if r is not None:
            t_id = getattr(r, 'reply_to_top_id', 0) or getattr(r, 'reply_to_msg_id', 0)
        if thread_id and t_id != thread_id:
            return
        inbox.push(msg)
        logger.info("inbox: msg %d from %d: %.60s", msg.id, msg.chat_id, msg.text or "")

    await client.run_until_disconnected()


async def _run_inbox_loop() -> None:
    retry = 0
    while True:
        try:
            await _run_inbox()
        except asyncio.CancelledError:
            break
        except Exception as e:
            retry += 1
            wait = min(retry * 5, 60)
            logger.error("inbox: crashed (retry %d in %ds): %s", retry, wait, e)
            await asyncio.sleep(wait)
            continue
        break


async def run_mcp_server() -> None:
    from mcp.server.stdio import stdio_server

    inbox_task = asyncio.create_task(_run_inbox_loop())

    async with stdio_server() as (read_stream, write_stream):
        logger.info("MCP server started")
        try:
            await app.run(read_stream, write_stream, app.create_initialization_options())
        finally:
            inbox_task.cancel()
            try:
                await inbox_task
            except asyncio.CancelledError:
                pass
            logger.info("inbox: stopped")


def main() -> None:
    import sys
    asyncio.run(run_mcp_server())
