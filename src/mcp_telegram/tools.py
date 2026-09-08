from __future__ import annotations

import asyncio
import logging
import sys
import typing as t
from functools import singledispatch, wraps

from mcp.types import (
    EmbeddedResource,
    ImageContent,
    TextContent,
    Tool,
)
from pydantic import BaseModel, ConfigDict, Field, validator
from telethon import TelegramClient, custom, functions, types  # type: ignore[import-untyped]
from telethon.tl.types import (
    InputMessagesFilterPhotos, InputMessagesFilterVideo, InputMessagesFilterDocument,
    InputMessagesFilterUrl, InputMessagesFilterVoice, InputMessagesFilterMusic,
    InputMessagesFilterPhotoVideo, InputMessagesFilterPinned, InputMessagesFilterGif,
    InputPeerUser, InputPeerChat, InputPeerChannel
)
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, UserDeactivatedError,
    UserNotMutualContactError, UserPrivacyRestrictedError, ChatAdminRequiredError,
    MessageNotModifiedError, MessageDeleteForbiddenError, PeerIdInvalidError
)

from .telegram import create_client
from .rate_limiter import get_rate_limiter

logger = logging.getLogger(__name__)


def handle_flood_wait(max_retries: int = 3, use_rate_limiter: bool = True):
    """
    Декоратор для автоматической обработки FloodWaitError с интеграцией Rate Limiter.
    
    Args:
        max_retries: Максимальное количество попыток повтора
        use_rate_limiter: Использовать ли Rate Limiter для предотвращения FloodWait
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Извлекаем аргументы для rate limiter
            method_name = func.__name__
            chat_id = None
            priority = 1
            
            # Пытаемся извлечь chat_id из аргументов (улучшенная логика)
            if args and hasattr(args[0], 'chat_id'):
                chat_id = getattr(args[0], 'chat_id', None)
            elif args and hasattr(args[0], 'from_chat_id'):
                chat_id = getattr(args[0], 'from_chat_id', None)
            elif args and hasattr(args[0], 'peer_id'):
                chat_id = getattr(args[0], 'peer_id', None)
            elif args and hasattr(args[0], 'channel_id'):
                chat_id = getattr(args[0], 'channel_id', None)
            elif args and hasattr(args[0], 'user_id'):
                chat_id = getattr(args[0], 'user_id', None)
            
            # Определяем приоритет по типу операции (расширенный список)
            high_priority_methods = {
                'send_message', 'reply_to_message', 'get_me', 'get_user_info', 
                'edit_message', 'delete_message', 'forward_message'
            }
            medium_priority_methods = {
                'send_file', 'get_chat_info', 'get_chat_history', 'get_chat_members',
                'search_messages', 'download_file'
            }
            low_priority_methods = {
                'get_all_chats', 'get_folders', 'search_global', 'get_entity_info'
            }
            
            if method_name in high_priority_methods:
                priority = 5  # Высший приоритет
            elif method_name in medium_priority_methods:
                priority = 3  # Средний приоритет  
            elif method_name in low_priority_methods:
                priority = 1  # Низкий приоритет
            else:
                priority = 2  # Обычный приоритет
            
            # Особые лимиты для resolve операций (расширенный список)
            resolve_methods = {
                'resolve_username', 'search_global', 'search_chats', 'search_users',
                'get_entity_info', 'resolve_phone', 'check_username'
            }
            is_resolve_operation = method_name in resolve_methods
            
            if use_rate_limiter:
                rate_limiter = get_rate_limiter()
                
                # Проверяем дневной лимит для resolve операций
                if is_resolve_operation and not rate_limiter.check_resolve_limit():
                    logger.error(f"Daily resolve limit exceeded for {method_name}")
                    return [TextContent(
                        type="text", 
                        text="Error: Daily limit for resolve operations exceeded. Please try tomorrow."
                    )]
                
                # Используем rate limiter
                async with rate_limiter.acquire(method_name, chat_id, priority) as acquired:
                    if not acquired:
                        logger.warning(f"Rate limiter denied access for {method_name}")
                        return [TextContent(
                            type="text", 
                            text="Error: Rate limit exceeded. Please try again later."
                        )]
                    
                    # Выполняем основную логику с retry
                    return await _execute_with_retry(func, args, kwargs, max_retries, rate_limiter)
            else:
                # Выполняем без rate limiter (backward compatibility)
                return await _execute_with_retry(func, args, kwargs, max_retries, None)
                
        return wrapper
    return decorator


async def _execute_with_retry(func, args, kwargs, max_retries: int, rate_limiter=None):
    """Выполнение функции с retry логикой"""
    retries = 0
    
    while retries <= max_retries:
        try:
            return await func(*args, **kwargs)
            
        except FloodWaitError as e:
            if rate_limiter:
                rate_limiter.stats.record_flood_error()
            
            if e.seconds > 60:  # Слишком долгое ожидание
                logger.error(f"FloodWaitError: {e.seconds}s too long, aborting {func.__name__}")
                raise
            
            retries += 1
            if retries > max_retries:
                logger.error(f"FloodWaitError: Max retries exceeded for {func.__name__}")
                raise
            
            logger.warning(f"FloodWaitError: waiting {e.seconds}s, retry {retries}/{max_retries} for {func.__name__}")
            await asyncio.sleep(e.seconds)
            
        except Exception as e:
            # Логируем все остальные ошибки для отладки
            logger.error(f"Error in {func.__name__}: {type(e).__name__}: {e}")
            raise
    
    return None


class ToolArgs(BaseModel):
    model_config = ConfigDict()


@singledispatch
async def tool_runner(
    args,  # noqa: ANN001
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    raise NotImplementedError(f"Unsupported type: {type(args)}")


def tool_description(args: type[ToolArgs]) -> Tool:
    return Tool(
        name=args.__name__,
        description=args.__doc__,
        inputSchema=args.model_json_schema(),
    )


def tool_args(tool: Tool, *args, **kwargs) -> ToolArgs:  # noqa: ANN002, ANN003
    return sys.modules[__name__].__dict__[tool.name](*args, **kwargs)


# ========================================
# CHATS MODULE (15 methods + forum topics)
# ========================================


class GetFolders(ToolArgs):
    """Get Telegram chat folders (dialog filters)."""
    pass


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_folders(
    args: GetFolders,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetFolders] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Получаем папки (dialog filters)
            result = await client(functions.messages.GetDialogFiltersRequest())
            
            if hasattr(result, 'filters'):
                response.append(TextContent(type="text", text=f"Found {len(result.filters)} folders:"))
                
                for i, folder in enumerate(result.filters, 1):
                    if hasattr(folder, 'title'):
                        folder_info = f"\n{i}. 📁 {folder.title}"
                        folder_info += f"\n   ID: {folder.id}"
                        
                        # Количество чатов
                        if hasattr(folder, 'include_peers'):
                            folder_info += f"\n   Chats: {len(folder.include_peers)}"
                        
                        # Эмодзи
                        if hasattr(folder, 'emoticon'):
                            folder_info += f"\n   Emoji: {folder.emoticon}"
                            
                        # Настройки
                        settings = []
                        if getattr(folder, 'contacts', False):
                            settings.append("Contacts")
                        if getattr(folder, 'non_contacts', False):
                            settings.append("Non-contacts")
                        if getattr(folder, 'groups', False):
                            settings.append("Groups")
                        if getattr(folder, 'broadcasts', False):
                            settings.append("Channels")
                        if getattr(folder, 'bots', False):
                            settings.append("Bots")
                        if getattr(folder, 'exclude_muted', False):
                            settings.append("Exclude muted")
                        if getattr(folder, 'exclude_read', False):
                            settings.append("Exclude read")
                        if getattr(folder, 'exclude_archived', False):
                            settings.append("Exclude archived")
                            
                        if settings:
                            folder_info += f"\n   Settings: {', '.join(settings)}"
                            
                        response.append(TextContent(type="text", text=folder_info))
                else:
                    response.append(TextContent(type="text", text="No folders found"))
            else:
                response.append(TextContent(type="text", text="No folders configured"))
                
        except Exception as e:
            logger.error(f"Error in get_folders: {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    return response


class GetChatsFromFolder(ToolArgs):
    """Get chats from a specific folder."""
    folder_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chats_from_folder(
    args: GetChatsFromFolder,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatsFromFolder] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Получаем информацию о папке
            result = await client(functions.messages.GetDialogFiltersRequest())
            
            target_folder = None
            for folder in result.filters:
                if hasattr(folder, 'id') and folder.id == args.folder_id:
                    target_folder = folder
                    break
            
            if not target_folder:
                return [TextContent(type="text", text=f"Folder with ID {args.folder_id} not found")]
            
            response.append(TextContent(type="text", text=f"📁 Folder: {target_folder.title}"))
            response.append(TextContent(type="text", text=f"Total chats: {len(target_folder.include_peers)}"))
            response.append(TextContent(type="text", text="\nChats in folder:"))
            
            # Получаем информацию о каждом чате
            for i, peer in enumerate(target_folder.include_peers[:50], 1):  # Ограничиваем 50 чатами
                try:
                    if hasattr(peer, 'user_id'):
                        entity = await client.get_entity(peer.user_id)
                        chat_type = "👤 User"
                    elif hasattr(peer, 'chat_id'):
                        entity = await client.get_entity(peer.chat_id)
                        chat_type = "👥 Group"
                    elif hasattr(peer, 'channel_id'):
                        entity = await client.get_entity(peer.channel_id)
                        chat_type = "📢 Channel"
                    else:
                        continue
                    
                    name = getattr(entity, 'title', None) or getattr(entity, 'first_name', 'Unknown')
                    username = getattr(entity, 'username', None)
                    
                    chat_info = f"\n{i}. {chat_type} {name}"
                    if username:
                        chat_info += f" (@{username})"
                    chat_info += f"\n   ID: {entity.id}"
                    
                    response.append(TextContent(type="text", text=chat_info))
                    
                except Exception as e:
                    logger.error(f"Error getting entity: {e}")
                    continue
            
            if len(target_folder.include_peers) > 50:
                response.append(TextContent(
                    type="text", 
                    text=f"\n... and {len(target_folder.include_peers) - 50} more chats"
                ))
                
        except Exception as e:
            logger.error(f"Error in get_chats_from_folder: {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    return response


class GetForumTopics(ToolArgs):
    """Get forum topics from a chat."""
    chat_id: int
    limit: int = 20


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_forum_topics(
    args: GetForumTopics,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetForumTopics] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            chat = await client.get_entity(args.chat_id)
            
            result = await client(functions.channels.GetForumTopicsRequest(
                channel=chat,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=args.limit,
                q=None
            ))
            
            if hasattr(result, 'topics'):
                for topic in result.topics:
                    topic_info = f"Topic: {topic.title} (ID: {topic.id})"
                    if hasattr(topic, 'unread_count'):
                        topic_info += f", Unread: {topic.unread_count}"
                    topic_info += f", Closed: {'Yes' if topic.closed else 'No'}"
                    response.append(TextContent(type="text", text=topic_info))
            else:
                response.append(TextContent(type="text", text="No forum topics found or chat is not a forum"))
                
        except Exception as e:
            logger.error(f"Error in get_forum_topics: {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    return response


# ========================================
# CHATS MODULE (15 methods)
# ========================================


class GetChats(ToolArgs):
    """Get list of all chats (dialogs)."""
    limit: int = 100
    archived: bool = False
    folder_id: int | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chats(
    args: GetChats,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChats] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            dialogs = await client.get_dialogs(
                limit=args.limit,
                archived=args.archived
            )
            
            for dialog in dialogs:
                chat_info = {
                    "id": dialog.id,
                    "name": dialog.name,
                    "type": "user" if dialog.is_user else ("channel" if dialog.is_channel else "group"),
                    "unread_count": dialog.unread_count,
                    "last_message_date": dialog.date.isoformat() if dialog.date else None,
                    "pinned": dialog.pinned,
                    "archived": dialog.archived
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Chat: {chat_info['name']} (ID: {chat_info['id']}, Type: {chat_info['type']}, Unread: {chat_info['unread_count']})"
                ))
                
        except Exception as e:
            logger.error(f"Error in get_chats: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetChatInfo(ToolArgs):
    """Get detailed information about a specific chat."""
    chat_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chat_info(
    args: GetChatInfo,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatInfo] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if hasattr(entity, 'first_name'):
                # User
                info = {
                    "id": entity.id,
                    "type": "user",
                    "name": entity.first_name + (f" {entity.last_name}" if entity.last_name else ""),
                    "username": getattr(entity, 'username', None),
                    "phone": getattr(entity, 'phone', None),
                    "bio": getattr(entity, 'about', None),
                    "is_contact": getattr(entity, 'contact', False),
                    "is_mutual_contact": getattr(entity, 'mutual_contact', False)
                }
            else:
                # Chat/Channel
                full_chat = await client(functions.channels.GetFullChannelRequest(entity))
                info = {
                    "id": entity.id,
                    "type": "channel" if entity.broadcast else "group",
                    "title": entity.title,
                    "username": getattr(entity, 'username', None),
                    "participants_count": getattr(full_chat.full_chat, 'participants_count', 0),
                    "about": getattr(full_chat.full_chat, 'about', None),
                    "invite_link": getattr(full_chat.full_chat, 'exported_invite', None)
                }
            
            response.append(TextContent(type="text", text=str(info)))
            
        except Exception as e:
            logger.error(f"Error in get_chat_info: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetChatMembers(ToolArgs):
    """Get members of a chat or channel."""
    chat_id: int
    limit: int = 100
    search: str | None = None
    filter: str = "recent"  # recent, admins, kicked, bots


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chat_members(
    args: GetChatMembers,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatMembers] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            filter_map = {
                "recent": types.ChannelParticipantsRecent(),
                "admins": types.ChannelParticipantsAdmins(),
                "kicked": types.ChannelParticipantsKicked(q=""),
                "bots": types.ChannelParticipantsBots()
            }
            
            participants = await client.get_participants(
                entity,
                limit=args.limit,
                search=args.search,
                filter=filter_map.get(args.filter, types.ChannelParticipantsRecent())
            )
            
            for participant in participants:
                member_info = {
                    "id": participant.id,
                    "name": participant.first_name + (f" {participant.last_name}" if participant.last_name else ""),
                    "username": getattr(participant, 'username', None),
                    "is_bot": getattr(participant, 'bot', False)
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Member: {member_info['name']} (@{member_info['username']}) ID: {member_info['id']}"
                ))
                
        except Exception as e:
            logger.error(f"Error in get_chat_members: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetChatAdmins(ToolArgs):
    """Get administrators of a chat or channel."""
    chat_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chat_admins(
    args: GetChatAdmins,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatAdmins] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            admins = await client.get_participants(
                entity,
                filter=types.ChannelParticipantsAdmins()
            )
            
            for admin in admins:
                admin_info = {
                    "id": admin.id,
                    "name": admin.first_name + (f" {admin.last_name}" if admin.last_name else ""),
                    "username": getattr(admin, 'username', None),
                    "is_creator": hasattr(admin.participant, 'creator') and admin.participant.creator
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Admin: {admin_info['name']} (@{admin_info['username']}) ID: {admin_info['id']} {'(Creator)' if admin_info['is_creator'] else ''}"
                ))
                
        except Exception as e:
            logger.error(f"Error in get_chat_admins: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetChatOnlineCount(ToolArgs):
    """Get online members count in a chat."""
    chat_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chat_online_count(
    args: GetChatOnlineCount,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatOnlineCount] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if hasattr(entity, 'megagroup') and entity.megagroup:
                full_chat = await client(functions.channels.GetFullChannelRequest(entity))
                online_count = getattr(full_chat.full_chat, 'online_count', 0)
                response.append(TextContent(type="text", text=f"Online members: {online_count}"))
            else:
                response.append(TextContent(type="text", text="Online count not available for this chat type"))
                
        except Exception as e:
            logger.error(f"Error in get_chat_online_count: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class SearchChats(ToolArgs):
    """Search for chats by name or username."""
    query: str
    limit: int = 10


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def search_chats(
    args: SearchChats,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SearchChats] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Search in contacts and dialogs
            contacts = await client(functions.contacts.SearchRequest(
                q=args.query,
                limit=args.limit
            ))
            
            for chat in contacts.chats:
                chat_info = {
                    "id": chat.id,
                    "title": getattr(chat, 'title', getattr(chat, 'first_name', 'Unknown')),
                    "username": getattr(chat, 'username', None),
                    "type": "channel" if getattr(chat, 'broadcast', False) else ("group" if hasattr(chat, 'title') else "user")
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Found: {chat_info['title']} (@{chat_info['username']}) ID: {chat_info['id']} Type: {chat_info['type']}"
                ))
                
        except Exception as e:
            logger.error(f"Error in search_chats: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class JoinChatByInvite(ToolArgs):
    """Join a chat using an invite link."""
    invite_link: str


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def join_chat_by_invite(
    args: JoinChatByInvite,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[JoinChatByInvite] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Extract hash from invite link
            invite_hash = args.invite_link.split('/')[-1]
            if '+' in invite_hash:
                invite_hash = invite_hash.split('+')[1]
            
            result = await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
            
            if hasattr(result, 'chats') and result.chats:
                chat = result.chats[0]
                response.append(TextContent(
                    type="text", 
                    text=f"Successfully joined chat: {chat.title} (ID: {chat.id})"
                ))
            else:
                response.append(TextContent(type="text", text="Successfully joined the chat"))
                
        except Exception as e:
            logger.error(f"Error in join_chat_by_invite: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class LeaveChat(ToolArgs):
    """Leave a chat or channel."""
    chat_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def leave_chat(
    args: LeaveChat,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[LeaveChat] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
                # Channel or supergroup
                await client(functions.channels.LeaveChannelRequest(channel=entity))
            else:
                # Regular group
                await client(functions.messages.DeleteChatUserRequest(
                    chat_id=entity.id,
                    user_id="me"
                ))
            
            response.append(TextContent(type="text", text=f"Successfully left chat: {entity.title if hasattr(entity, 'title') else 'Chat'}"))
                
        except Exception as e:
            logger.error(f"Error in leave_chat: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetChatInviteLink(ToolArgs):
    """Get invite link for a chat."""
    chat_id: int
    revoke_previous: bool = False


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chat_invite_link(
    args: GetChatInviteLink,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatInviteLink] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if args.revoke_previous:
                result = await client(functions.messages.ExportChatInviteRequest(
                    peer=entity,
                    revoke_previous_invites=True
                ))
            else:
                result = await client(functions.messages.ExportChatInviteRequest(peer=entity))
            
            if hasattr(result, 'link'):
                response.append(TextContent(type="text", text=f"Invite link: {result.link}"))
            else:
                response.append(TextContent(type="text", text="Could not generate invite link"))
                
        except Exception as e:
            logger.error(f"Error in get_chat_invite_link: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class CheckChatInvite(ToolArgs):
    """Check information about a chat invite link."""
    invite_link: str


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def check_chat_invite(
    args: CheckChatInvite,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[CheckChatInvite] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Extract hash from invite link
            invite_hash = args.invite_link.split('/')[-1]
            if '+' in invite_hash:
                invite_hash = invite_hash.split('+')[1]
            
            result = await client(functions.messages.CheckChatInviteRequest(hash=invite_hash))
            
            if hasattr(result, 'chat'):
                chat = result.chat
                info = {
                    "title": chat.title,
                    "participants_count": getattr(result, 'participants_count', 0),
                    "already_participant": getattr(result, 'already_participant', False)
                }
                response.append(TextContent(type="text", text=str(info)))
            else:
                response.append(TextContent(type="text", text="Invalid invite link"))
                
        except Exception as e:
            logger.error(f"Error in check_chat_invite: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class BanChatMember(ToolArgs):
    """Ban a member from chat."""
    chat_id: int
    user_id: int
    until_date: int | None = None
    revoke_history: bool = False


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def ban_chat_member(
    args: BanChatMember,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[BanChatMember] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            user = await client.get_entity(args.user_id)
            
            if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
                # Channel or supergroup
                await client.edit_permissions(
                    entity,
                    user,
                    until_date=args.until_date,
                    view_messages=False
                )
                
                if args.revoke_history:
                    await client(functions.channels.DeleteParticipantHistoryRequest(
                        channel=entity,
                        participant=user
                    ))
                    
            response.append(TextContent(type="text", text=f"Successfully banned user {user.first_name} from {entity.title}"))
                
        except Exception as e:
            logger.error(f"Error in ban_chat_member: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class UnbanChatMember(ToolArgs):
    """Unban a member from chat."""
    chat_id: int
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def unban_chat_member(
    args: UnbanChatMember,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[UnbanChatMember] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            user = await client.get_entity(args.user_id)
            
            await client.edit_permissions(entity, user)
            
            response.append(TextContent(type="text", text=f"Successfully unbanned user {user.first_name} from {entity.title}"))
                
        except Exception as e:
            logger.error(f"Error in unban_chat_member: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class KickChatMember(ToolArgs):
    """Kick a member from chat."""
    chat_id: int
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def kick_chat_member(
    args: KickChatMember,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[KickChatMember] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            user = await client.get_entity(args.user_id)
            
            await client.kick_participant(entity, user)
            
            response.append(TextContent(type="text", text=f"Successfully kicked user {user.first_name} from {entity.title}"))
                
        except Exception as e:
            logger.error(f"Error in kick_chat_member: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class PromoteToAdmin(ToolArgs):
    """Promote a member to admin."""
    chat_id: int
    user_id: int
    title: str | None = None
    can_manage_chat: bool = True
    can_delete_messages: bool = True
    can_manage_video_chats: bool = True
    can_restrict_members: bool = True
    can_promote_members: bool = False
    can_change_info: bool = True
    can_invite_users: bool = True
    can_pin_messages: bool = True


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def promote_to_admin(
    args: PromoteToAdmin,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[PromoteToAdmin] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            user = await client.get_entity(args.user_id)
            
            admin_rights = types.ChatAdminRights(
                change_info=args.can_change_info,
                delete_messages=args.can_delete_messages,
                ban_users=args.can_restrict_members,
                invite_users=args.can_invite_users,
                pin_messages=args.can_pin_messages,
                add_admins=args.can_promote_members,
                manage_call=args.can_manage_video_chats
            )
            
            await client(functions.channels.EditAdminRequest(
                channel=entity,
                user_id=user,
                admin_rights=admin_rights,
                rank=args.title or ""
            ))
            
            response.append(TextContent(type="text", text=f"Successfully promoted {user.first_name} to admin in {entity.title}"))
                
        except Exception as e:
            logger.error(f"Error in promote_to_admin: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class AddChatMember(ToolArgs):
    """Add a member to chat."""
    chat_id: int
    user_id: int
    forward_limit: int = 100


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def add_chat_member(
    args: AddChatMember,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[AddChatMember] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            user = await client.get_entity(args.user_id)
            
            if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
                # Channel or supergroup
                await client(functions.channels.InviteToChannelRequest(
                    channel=entity,
                    users=[user]
                ))
            else:
                # Regular group
                await client(functions.messages.AddChatUserRequest(
                    chat_id=entity.id,
                    user_id=user,
                    fwd_limit=args.forward_limit
                ))
            
            response.append(TextContent(type="text", text=f"Successfully added {user.first_name} to {entity.title}"))
                
        except Exception as e:
            logger.error(f"Error in add_chat_member: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


# ========================================
# MESSAGES MODULE (12 methods)  
# ========================================


class SendMessage(ToolArgs):
    """Send a text message to a chat."""
    chat_id: int
    text: str
    reply_to_message_id: int | None = None
    parse_mode: str | None = None  # "markdown" or "html"
    silent: bool = False
    schedule_date: int | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def send_message(
    args: SendMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SendMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            message = await client.send_message(
                entity,
                args.text,
                reply_to=args.reply_to_message_id,
                parse_mode=args.parse_mode,
                silent=args.silent,
                schedule=args.schedule_date
            )
            
            response.append(TextContent(type="text", text=f"Message sent successfully. Message ID: {message.id}"))
                
        except Exception as e:
            logger.error(f"Error in send_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class ReplyToMessage(ToolArgs):
    """Reply to a specific message."""
    chat_id: int
    message_id: int
    text: str
    parse_mode: str | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def reply_to_message(
    args: ReplyToMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[ReplyToMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            message = await client.send_message(
                entity,
                args.text,
                reply_to=args.message_id,
                parse_mode=args.parse_mode
            )
            
            response.append(TextContent(type="text", text=f"Reply sent successfully. Message ID: {message.id}"))
                
        except Exception as e:
            logger.error(f"Error in reply_to_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class EditMessage(ToolArgs):
    """Edit a message."""
    chat_id: int
    message_id: int
    text: str
    parse_mode: str | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def edit_message(
    args: EditMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[EditMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            await client.edit_message(
                entity,
                args.message_id,
                args.text,
                parse_mode=args.parse_mode
            )
            
            response.append(TextContent(type="text", text=f"Message {args.message_id} edited successfully"))
                
        except Exception as e:
            logger.error(f"Error in edit_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class DeleteMessage(ToolArgs):
    """Delete messages."""
    chat_id: int
    message_ids: list[int]
    revoke: bool = True


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def delete_message(
    args: DeleteMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[DeleteMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            await client.delete_messages(entity, args.message_ids, revoke=args.revoke)
            
            response.append(TextContent(type="text", text=f"Successfully deleted {len(args.message_ids)} messages"))
                
        except Exception as e:
            logger.error(f"Error in delete_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class ForwardMessage(ToolArgs):
    """Forward messages from one chat to another."""
    from_chat_id: int
    to_chat_id: int
    message_ids: list[int]
    silent: bool = False
    as_album: bool = False


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def forward_message(
    args: ForwardMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[ForwardMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            from_entity = await client.get_entity(args.from_chat_id)
            to_entity = await client.get_entity(args.to_chat_id)
            
            messages = await client.forward_messages(
                to_entity,
                args.message_ids,
                from_entity,
                silent=args.silent,
                as_album=args.as_album
            )
            
            response.append(TextContent(type="text", text=f"Successfully forwarded {len(messages)} messages"))
                
        except Exception as e:
            logger.error(f"Error in forward_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetChatHistory(ToolArgs):
    """Get chat history (messages)."""
    chat_id: int
    limit: int = 100
    offset_id: int = 0
    min_id: int = 0
    max_id: int = 0


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_chat_history(
    args: GetChatHistory,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetChatHistory] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            messages = await client.get_messages(
                entity,
                limit=args.limit,
                offset_id=args.offset_id,
                min_id=args.min_id,
                max_id=args.max_id
            )
            
            for message in messages:
                if message.text:
                    sender_name = "Unknown"
                    if message.sender:
                        sender_name = message.sender.first_name if hasattr(message.sender, 'first_name') else str(message.sender_id)
                    
                    response.append(TextContent(
                        type="text", 
                        text=f"[{message.date}] {sender_name}: {message.text}"
                    ))
                
        except Exception as e:
            logger.error(f"Error in get_chat_history: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class SearchMessages(ToolArgs):
    """Search messages in a chat or globally."""
    query: str
    chat_id: int = Field(default=0, description="Chat ID to search in (0 = global search)")
    limit: int = 100
    offset_id: int = 0
    from_user_id: int = Field(default=0, description="User ID to search from (0 = any user)")
    filter: str = Field(default="", description="Filter type: photo, video, document, url, voice, music, gif (empty = no filter)")
    
    @validator('chat_id', 'from_user_id', 'limit', 'offset_id', pre=True, always=True)
    def fix_empty_int(cls, v):
        """Исправляем если Perplexity отправляет {} или None вместо числа"""
        if v == {} or v is None or v == "":
            return 0
        return v
    
    @validator('filter', pre=True, always=True)
    def fix_empty_string(cls, v):
        """Исправляем если Perplexity отправляет {} вместо строки"""
        if v == {} or v is None:
            return ""
        return v


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def search_messages(
    args: SearchMessages,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SearchMessages] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = None
            if args.chat_id > 0:
                entity = await client.get_entity(args.chat_id)
            
            from_user = None
            if args.from_user_id > 0:
                from_user = await client.get_entity(args.from_user_id)
            
            # Define filter mapping
            filter_map = {
                "photo": InputMessagesFilterPhotos(),
                "video": InputMessagesFilterVideo(), 
                "document": InputMessagesFilterDocument(),
                "url": InputMessagesFilterUrl(),
                "voice": InputMessagesFilterVoice(),
                "music": InputMessagesFilterMusic(),
                "gif": InputMessagesFilterGif()
            }
            
            search_filter = filter_map.get(args.filter) if args.filter else None
            
            messages = await client(functions.messages.SearchRequest(
                peer=entity or types.InputPeerEmpty(),
                q=args.query,
                filter=search_filter or types.InputMessagesFilterEmpty(),
                min_date=None,
                max_date=None,
                offset_id=args.offset_id,
                add_offset=0,
                limit=args.limit,
                max_id=0,
                min_id=0,
                hash=0,
                from_id=from_user
            ))
            
            for message in messages.messages:
                if hasattr(message, 'message') and message.message:
                    response.append(TextContent(
                        type="text", 
                        text=f"[{message.date}] ID:{message.id}: {message.message}"
                    ))
                
        except Exception as e:
            logger.error(f"Error in search_messages: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class MarkAsRead(ToolArgs):
    """Mark messages as read."""
    chat_id: int
    max_id: int | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def mark_as_read(
    args: MarkAsRead,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[MarkAsRead] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if args.max_id:
                await client(functions.messages.ReadHistoryRequest(
                    peer=entity,
                    max_id=args.max_id
                ))
            else:
                await client(functions.messages.ReadHistoryRequest(
                    peer=entity,
                    max_id=0
                ))
            
            response.append(TextContent(type="text", text="Messages marked as read"))
                
        except Exception as e:
            logger.error(f"Error in mark_as_read: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class PinMessage(ToolArgs):
    """Pin a message in chat."""
    chat_id: int
    message_id: int
    silent: bool = False


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def pin_message(
    args: PinMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[PinMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            await client(functions.messages.UpdatePinnedMessageRequest(
                peer=entity,
                id=args.message_id,
                silent=args.silent
            ))
            
            response.append(TextContent(type="text", text=f"Message {args.message_id} pinned successfully"))
                
        except Exception as e:
            logger.error(f"Error in pin_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class UnpinMessage(ToolArgs):
    """Unpin a message in chat."""
    chat_id: int
    message_id: int | None = None  # If None, unpins all messages


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def unpin_message(
    args: UnpinMessage,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[UnpinMessage] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if args.message_id:
                await client(functions.messages.UpdatePinnedMessageRequest(
                    peer=entity,
                    id=args.message_id,
                    unpin=True
                ))
                response.append(TextContent(type="text", text=f"Message {args.message_id} unpinned successfully"))
            else:
                await client(functions.messages.UnpinAllMessagesRequest(peer=entity))
                response.append(TextContent(type="text", text="All messages unpinned successfully"))
                
        except Exception as e:
            logger.error(f"Error in unpin_message: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class SendFile(ToolArgs):
    """Send a file to chat."""
    chat_id: int
    file_path: str
    caption: str | None = None
    reply_to_message_id: int | None = None
    silent: bool = False


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def send_file(
    args: SendFile,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SendFile] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            message = await client.send_file(
                entity,
                args.file_path,
                caption=args.caption,
                reply_to=args.reply_to_message_id,
                silent=args.silent
            )
            
            response.append(TextContent(type="text", text=f"File sent successfully. Message ID: {message.id}"))
                
        except Exception as e:
            logger.error(f"Error in send_file: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class DownloadMedia(ToolArgs):
    """Download media from a message."""
    chat_id: int
    message_id: int
    download_path: str | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def download_media(
    args: DownloadMedia,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[DownloadMedia] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            messages = await client.get_messages(entity, ids=[args.message_id])
            if not messages:
                response.append(TextContent(type="text", text="Message not found"))
                return response
                
            message = messages[0]
            if not message.media:
                response.append(TextContent(type="text", text="Message has no media"))
                return response
            
            file_path = await client.download_media(message, file=args.download_path)
            
            response.append(TextContent(type="text", text=f"Media downloaded to: {file_path}"))
                
        except Exception as e:
            logger.error(f"Error in download_media: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


# ========================================
# CONTACTS MODULE (8 methods)
# ========================================


class GetContacts(ToolArgs):
    """Get user's contacts list."""
    limit: int | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_contacts(
    args: GetContacts,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetContacts] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            contacts = await client(functions.contacts.GetContactsRequest(hash=0))
            
            count = 0
            for user in contacts.users:
                if args.limit and count >= args.limit:
                    break
                    
                contact_info = {
                    "id": user.id,
                    "name": user.first_name + (f" {user.last_name}" if user.last_name else ""),
                    "username": getattr(user, 'username', None),
                    "phone": getattr(user, 'phone', None),
                    "is_mutual_contact": user.mutual_contact
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Contact: {contact_info['name']} (@{contact_info['username']}) ID: {contact_info['id']} Phone: {contact_info['phone']}"
                ))
                count += 1
                
        except Exception as e:
            logger.error(f"Error in get_contacts: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class AddContact(ToolArgs):
    """Add a new contact."""
    phone: str
    first_name: str
    last_name: str | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def add_contact(
    args: AddContact,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[AddContact] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            contact = types.InputPhoneContact(
                client_id=0,
                phone=args.phone,
                first_name=args.first_name,
                last_name=args.last_name or ""
            )
            
            result = await client(functions.contacts.ImportContactsRequest(
                contacts=[contact]
            ))
            
            if result.imported:
                response.append(TextContent(type="text", text=f"Successfully added contact: {args.first_name}"))
            else:
                response.append(TextContent(type="text", text="Failed to add contact"))
                
        except Exception as e:
            logger.error(f"Error in add_contact: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class DeleteContact(ToolArgs):
    """Delete a contact."""
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def delete_contact(
    args: DeleteContact,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[DeleteContact] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            user = await client.get_entity(args.user_id)
            
            await client(functions.contacts.DeleteContactsRequest(
                id=[user]
            ))
            
            response.append(TextContent(type="text", text=f"Successfully deleted contact {user.first_name}"))
                
        except Exception as e:
            logger.error(f"Error in delete_contact: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class SearchContacts(ToolArgs):
    """Search contacts by name or username."""
    query: str
    limit: int = 10


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def search_contacts(
    args: SearchContacts,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SearchContacts] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            result = await client(functions.contacts.SearchRequest(
                q=args.query,
                limit=args.limit
            ))
            
            for user in result.users:
                if user.contact:  # Only show actual contacts
                    contact_info = {
                        "id": user.id,
                        "name": user.first_name + (f" {user.last_name}" if user.last_name else ""),
                        "username": getattr(user, 'username', None),
                        "phone": getattr(user, 'phone', None)
                    }
                    
                    response.append(TextContent(
                        type="text", 
                        text=f"Contact: {contact_info['name']} (@{contact_info['username']}) ID: {contact_info['id']}"
                    ))
                
        except Exception as e:
            logger.error(f"Error in search_contacts: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class BlockUser(ToolArgs):
    """Block a user."""
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def block_user(
    args: BlockUser,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[BlockUser] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            user = await client.get_entity(args.user_id)
            
            await client(functions.contacts.BlockRequest(id=user))
            
            response.append(TextContent(type="text", text=f"Successfully blocked user {user.first_name}"))
                
        except Exception as e:
            logger.error(f"Error in block_user: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class UnblockUser(ToolArgs):
    """Unblock a user."""
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def unblock_user(
    args: UnblockUser,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[UnblockUser] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            user = await client.get_entity(args.user_id)
            
            await client(functions.contacts.UnblockRequest(id=user))
            
            response.append(TextContent(type="text", text=f"Successfully unblocked user {user.first_name}"))
                
        except Exception as e:
            logger.error(f"Error in unblock_user: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetBlockedUsers(ToolArgs):
    """Get list of blocked users."""
    limit: int = 100
    offset: int = 0


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_blocked_users(
    args: GetBlockedUsers,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetBlockedUsers] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            blocked = await client(functions.contacts.GetBlockedRequest(
                offset=args.offset,
                limit=args.limit
            ))
            
            for user in blocked.users:
                user_info = {
                    "id": user.id,
                    "name": user.first_name + (f" {user.last_name}" if user.last_name else ""),
                    "username": getattr(user, 'username', None)
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Blocked: {user_info['name']} (@{user_info['username']}) ID: {user_info['id']}"
                ))
                
        except Exception as e:
            logger.error(f"Error in get_blocked_users: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class SearchGlobal(ToolArgs):
    """Search for users and chats globally."""
    query: str
    limit: int = 10


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def search_global(
    args: SearchGlobal,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SearchGlobal] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            result = await client(functions.contacts.SearchRequest(
                q=args.query,
                limit=args.limit
            ))
            
            # Users
            for user in result.users:
                user_info = {
                    "id": user.id,
                    "type": "user",
                    "name": user.first_name + (f" {user.last_name}" if user.last_name else ""),
                    "username": getattr(user, 'username', None)
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"User: {user_info['name']} (@{user_info['username']}) ID: {user_info['id']}"
                ))
            
            # Chats
            for chat in result.chats:
                chat_info = {
                    "id": chat.id,
                    "type": "channel" if getattr(chat, 'broadcast', False) else "group",
                    "title": chat.title,
                    "username": getattr(chat, 'username', None)
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"Chat: {chat_info['title']} (@{chat_info['username']}) ID: {chat_info['id']} Type: {chat_info['type']}"
                ))
                
        except Exception as e:
            logger.error(f"Error in search_global: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


# ========================================
# USERS MODULE (6 methods)
# ========================================


class GetMe(ToolArgs):
    """Get information about current user."""


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_me(
    args: GetMe,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetMe] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            me = await client.get_me()
            
            user_info = {
                "id": me.id,
                "first_name": me.first_name,
                "last_name": me.last_name,
                "username": getattr(me, 'username', None),
                "phone": getattr(me, 'phone', None),
                "is_verified": getattr(me, 'verified', False),
                "is_premium": getattr(me, 'premium', False),
                "is_bot": getattr(me, 'bot', False)
            }
            
            response.append(TextContent(type="text", text=str(user_info)))
                
        except Exception as e:
            logger.error(f"Error in get_me: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetUserInfo(ToolArgs):
    """Get information about a user."""
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_user_info(
    args: GetUserInfo,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetUserInfo] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            user = await client.get_entity(args.user_id)
            
            # Get full user info
            full_user = await client(functions.users.GetFullUserRequest(id=user))
            
            user_info = {
                "id": user.id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "username": getattr(user, 'username', None),
                "phone": getattr(user, 'phone', None),
                "is_verified": getattr(user, 'verified', False),
                "is_premium": getattr(user, 'premium', False),
                "is_bot": getattr(user, 'bot', False),
                "about": getattr(full_user.full_user, 'about', None),
                "common_chats_count": getattr(full_user.full_user, 'common_chats_count', 0),
                "is_blocked": getattr(full_user.full_user, 'blocked', False),
                "is_contact": user.contact if hasattr(user, 'contact') else False
            }
            
            response.append(TextContent(type="text", text=str(user_info)))
                
        except Exception as e:
            logger.error(f"Error in get_user_info: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class UpdateProfile(ToolArgs):
    """Update user profile information."""
    first_name: str | None = None
    last_name: str | None = None
    about: str | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def update_profile(
    args: UpdateProfile,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[UpdateProfile] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Update name if provided
            if args.first_name is not None or args.last_name is not None:
                await client(functions.account.UpdateProfileRequest(
                    first_name=args.first_name,
                    last_name=args.last_name
                ))
            
            # Update about if provided
            if args.about is not None:
                await client(functions.account.UpdateProfileRequest(
                    about=args.about
                ))
            
            response.append(TextContent(type="text", text="Profile updated successfully"))
                
        except Exception as e:
            logger.error(f"Error in update_profile: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class ResolveUsername(ToolArgs):
    """Resolve username to get user/chat information."""
    username: str


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def resolve_username(
    args: ResolveUsername,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[ResolveUsername] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Remove @ if present
            username = args.username.lstrip('@')
            
            result = await client(functions.contacts.ResolveUsernameRequest(username=username))
            
            if result.users:
                user = result.users[0]
                info = {
                    "id": user.id,
                    "type": "user",
                    "name": user.first_name + (f" {user.last_name}" if user.last_name else ""),
                    "username": user.username,
                    "is_verified": getattr(user, 'verified', False),
                    "is_bot": getattr(user, 'bot', False)
                }
            elif result.chats:
                chat = result.chats[0]
                info = {
                    "id": chat.id,
                    "type": "channel" if getattr(chat, 'broadcast', False) else "group",
                    "title": chat.title,
                    "username": getattr(chat, 'username', None),
                    "is_verified": getattr(chat, 'verified', False)
                }
            else:
                response.append(TextContent(type="text", text="Username not found"))
                return response
            
            response.append(TextContent(type="text", text=str(info)))
                
        except Exception as e:
            logger.error(f"Error in resolve_username: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class SearchUsers(ToolArgs):
    """Search for users by name."""
    query: str
    limit: int = 10


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def search_users(
    args: SearchUsers,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[SearchUsers] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            result = await client(functions.contacts.SearchRequest(
                q=args.query,
                limit=args.limit
            ))
            
            for user in result.users:
                user_info = {
                    "id": user.id,
                    "name": user.first_name + (f" {user.last_name}" if user.last_name else ""),
                    "username": getattr(user, 'username', None),
                    "is_verified": getattr(user, 'verified', False),
                    "is_bot": getattr(user, 'bot', False)
                }
                
                response.append(TextContent(
                    type="text", 
                    text=f"User: {user_info['name']} (@{user_info['username']}) ID: {user_info['id']}"
                ))
                
        except Exception as e:
            logger.error(f"Error in search_users: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetUserStatus(ToolArgs):
    """Get user's online status."""
    user_id: int


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def get_user_status(
    args: GetUserStatus,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[GetUserStatus] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            user = await client.get_entity(args.user_id)
            
            if hasattr(user, 'status'):
                status = user.status
                if isinstance(status, types.UserStatusOnline):
                    status_text = f"Online (expires: {status.expires})"
                elif isinstance(status, types.UserStatusOffline):
                    status_text = f"Last seen: {status.was_online}"
                elif isinstance(status, types.UserStatusRecently):
                    status_text = "Last seen recently"
                elif isinstance(status, types.UserStatusLastWeek):
                    status_text = "Last seen within a week"
                elif isinstance(status, types.UserStatusLastMonth):
                    status_text = "Last seen within a month"
                else:
                    status_text = "Status hidden"
            else:
                status_text = "Status unavailable"
            
            response.append(TextContent(type="text", text=f"User {user.first_name} status: {status_text}"))
                
        except Exception as e:
            logger.error(f"Error in get_user_status: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


# ========================================
# GROUPS MODULE (4 methods)
# ========================================


class CreateGroup(ToolArgs):
    """Create a new group."""
    title: str
    users: list[int]
    about: str | None = None


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def create_group(
    args: CreateGroup,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[CreateGroup] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            # Get user entities
            user_entities = []
            for user_id in args.users:
                user_entities.append(await client.get_entity(user_id))
            
            # Create the group
            result = await client(functions.messages.CreateChatRequest(
                users=user_entities,
                title=args.title
            ))
            
            if hasattr(result, 'chats') and result.chats:
                chat = result.chats[0]
                
                # Set description if provided
                if args.about:
                    await client(functions.messages.EditChatAboutRequest(
                        peer=chat,
                        about=args.about
                    ))
                
                response.append(TextContent(type="text", text=f"Group '{args.title}' created successfully. ID: {chat.id}"))
            else:
                response.append(TextContent(type="text", text="Failed to create group"))
                
        except Exception as e:
            logger.error(f"Error in create_group: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class CreateChannel(ToolArgs):
    """Create a new channel."""
    title: str
    about: str | None = None
    megagroup: bool = False


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def create_channel(
    args: CreateChannel,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[CreateChannel] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            result = await client(functions.channels.CreateChannelRequest(
                title=args.title,
                about=args.about or "",
                megagroup=args.megagroup
            ))
            
            if hasattr(result, 'chats') and result.chats:
                channel = result.chats[0]
                channel_type = "supergroup" if args.megagroup else "channel"
                response.append(TextContent(type="text", text=f"{channel_type.title()} '{args.title}' created successfully. ID: {channel.id}"))
            else:
                response.append(TextContent(type="text", text="Failed to create channel"))
                
        except Exception as e:
            logger.error(f"Error in create_channel: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class EditChatTitle(ToolArgs):
    """Edit chat title."""
    chat_id: int
    title: str


@tool_runner.register
@handle_flood_wait(max_retries=3)
async def edit_chat_title(
    args: EditChatTitle,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[EditChatTitle] args[%s]", args)
    
    response: list[TextContent] = []
    async with create_client() as client:
        try:
            entity = await client.get_entity(args.chat_id)
            
            if hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup'):
                # Channel or supergroup
                await client(functions.channels.EditTitleRequest(
                    channel=entity,
                    title=args.title
                ))
            else:
                # Regular group
                await client(functions.messages.EditChatTitleRequest(
                    chat_id=entity.id,
                    title=args.title
                ))
            
            response.append(TextContent(type="text", text=f"Chat title changed to: {args.title}"))
                
        except Exception as e:
            logger.error(f"Error in edit_chat_title: {e}")
            response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


class GetAllChats(ToolArgs):
    """Get all chats (groups and channels) where user is a member."""
    except_ids: list[int] = Field(default=[], description="List of chat IDs to exclude from results")
    
    @validator('except_ids', pre=True, always=True)
    def fix_empty_object(cls, v):
        """Исправляем проблему когда Perplexity отправляет {} вместо []"""
        if v == {} or v is None:
            return []
        return v


@tool_runner.register
async def get_all_chats(
    args: GetAllChats,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    # Debug for Perplexity
    from datetime import datetime
    from pathlib import Path
    debug_file = Path.home() / "Desktop" / "mcp-tg_calls.log"
    with open(debug_file, "a") as f:
        f.write(f"\n[{datetime.now()}] get_all_chats called with: {vars(args)}\n")
    
    logger.info("method[GetAllChats] args[%s]", args)
    
    response: list[TextContent] = []
    
    try:
        # Use context manager for proper cleanup
        async with create_client() as client:
            # Get dialogs with limit to avoid FloodWait and timeouts
            # Perplexity has short timeout, so we limit to 30 chats
            dialogs = await client.get_dialogs(limit=30)
            
            # Filter for groups and channels only
            chats = []
            for dialog in dialogs:
                if not dialog.is_user:  # Skip private chats
                    if args.except_ids and dialog.id in args.except_ids:
                        continue
                    
                    chat_info = {
                        "id": dialog.id,
                        "title": dialog.name,
                        "type": "channel" if dialog.is_channel else "group",
                        "unread_count": dialog.unread_count,
                        "is_creator": getattr(dialog.entity, 'creator', False) if hasattr(dialog, 'entity') else False,
                        "is_admin": getattr(dialog.entity, 'admin_rights', None) is not None if hasattr(dialog, 'entity') else False
                    }
                    
                    chats.append(chat_info)
                    response.append(TextContent(
                        type="text", 
                        text=f"Chat: {chat_info['title']} (ID: {chat_info['id']}, Type: {chat_info['type']}, Unread: {chat_info['unread_count']})"
                    ))
            
            response.insert(0, TextContent(type="text", text=f"Found {len(chats)} chats (showing first 30)"))
    
    except asyncio.CancelledError:
        logger.info("get_all_chats was cancelled by client")
        # Don't re-raise, just return what we have
        if not response:
            response.append(TextContent(type="text", text="Operation cancelled"))
    except Exception as e:
        logger.error(f"Error in get_all_chats: {e}")
        response.append(TextContent(type="text", text=f"Error: {str(e)}"))
    
    return response


# ========================================
# INBOX TOOLS
# ========================================


class InboxRead(ToolArgs):
    """Read (and clear) pending inbox messages from the persistent Telegram listener."""
    pass


@tool_runner.register
async def inbox_read(
    args: InboxRead,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[InboxRead] args[%s]", args)

    from . import inbox as inbox_module

    messages = inbox_module.get_pending()
    if not messages:
        return [TextContent(type="text", text="No new messages in inbox.")]

    result_lines = [f"Inbox ({len(messages)} messages):"]
    for m in messages:
        result_lines.append(
            f"  [{m['date']}] from={m['sender_id']} chat={m['chat_id']} "
            f"thread={m['thread_id']}: {m['text']}"
        )

    return [TextContent(type="text", text="\n".join(result_lines))]


class InboxPeek(ToolArgs):
    """Peek at inbox message count without consuming messages."""
    pass


@tool_runner.register
async def inbox_peek(
    args: InboxPeek,
) -> t.Sequence[TextContent | ImageContent | EmbeddedResource]:
    logger.info("method[InboxPeek] args[%s]", args)

    from . import inbox as inbox_module

    count = inbox_module.peek_pending()
    return [TextContent(type="text", text=f"Inbox has {count} pending message(s).")]
