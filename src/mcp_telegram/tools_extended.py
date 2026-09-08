# Extended tools for sparfenyuk/mcp-telegram
# Пример добавления новых методов

from __future__ import annotations
import typing as t
from mcp.types import TextContent
from telethon import TelegramClient
from .tools import ToolArgs, tool_runner
from .telegram import create_client


# 1. SEND MESSAGE
class SendMessage(ToolArgs):
    """Send a message to a specific chat or channel."""
    dialog_id: int
    message: str


@tool_runner.register
async def send_message(args: SendMessage) -> t.Sequence[TextContent]:
    async with create_client() as client:
        await client.send_message(args.dialog_id, args.message)
        return [TextContent(type="text", text=f"Message sent to {args.dialog_id}")]


# 2. GET ME
class GetMe(ToolArgs):
    """Get information about the current user."""
    pass


@tool_runner.register
async def get_me(args: GetMe) -> t.Sequence[TextContent]:
    async with create_client() as client:
        me = await client.get_me()
        info = f"ID: {me.id}\nUsername: @{me.username}\nName: {me.first_name} {me.last_name or ''}"
        return [TextContent(type="text", text=info)]


# 3. GET CHAT INFO
class GetChatInfo(ToolArgs):
    """Get detailed information about a chat."""
    dialog_id: int


@tool_runner.register  
async def get_chat_info(args: GetChatInfo) -> t.Sequence[TextContent]:
    async with create_client() as client:
        entity = await client.get_entity(args.dialog_id)
        info = f"Title: {getattr(entity, 'title', 'N/A')}\n"
        info += f"ID: {entity.id}\n"
        info += f"Type: {type(entity).__name__}"
        return [TextContent(type="text", text=info)]


# 4. JOIN CHAT
class JoinChat(ToolArgs):
    """Join a chat by invite link."""
    invite_link: str


@tool_runner.register
async def join_chat(args: JoinChat) -> t.Sequence[TextContent]:
    async with create_client() as client:
        try:
            result = await client(functions.messages.ImportChatInviteRequest(
                hash=args.invite_link.split('/')[-1].lstrip('+')
            ))
            return [TextContent(type="text", text="Successfully joined the chat")]
        except Exception as e:
            return [TextContent(type="text", text=f"Failed to join: {str(e)}")]


# 5. SEARCH MESSAGES
class SearchMessages(ToolArgs):
    """Search for messages in a specific chat."""
    dialog_id: int
    query: str
    limit: int = 10


@tool_runner.register
async def search_messages(args: SearchMessages) -> t.Sequence[TextContent]:
    async with create_client() as client:
        messages = []
        async for message in client.iter_messages(
            args.dialog_id, 
            search=args.query, 
            limit=args.limit
        ):
            if message.text:
                messages.append(f"[{message.date}] {message.text[:100]}...")
        
        if messages:
            return [TextContent(type="text", text="\n---\n".join(messages))]
        else:
            return [TextContent(type="text", text="No messages found")]