"""
Rate Limiter для MCP-TG MCP сервера
Адаптировано из telethon_rate_limiter.py для интеграции с MCP архитектурой

Особенности:
- Глобальные и per-chat лимиты
- Автоматическое управление очередью запросов
- Интеграция с существующим логированием
- Неблокирующая работа (asyncio)
- Простой API для использования в MCP tools
"""
from __future__ import annotations

import asyncio
import time
import logging
import random
from collections import deque, defaultdict
from datetime import datetime
from typing import Dict, Deque, Optional, Any
from dataclasses import dataclass
from contextlib import asynccontextmanager

# Получаем логгер из существующей системы
logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Конфигурация Rate Limiter"""
    # Основные лимиты (запросов в секунду/минуту)
    global_limit: int = 30          # запросов в минуту глобально
    chat_limit: int = 1             # запросов в секунду на чат
    group_limit: int = 20           # запросов в минуту для групп
    resolve_daily_limit: int = 200  # resolve операций в день
    
    # Настройки повторов
    max_retries: int = 3            # максимум повторов при ошибках
    max_flood_wait: int = 300       # максимальное время ожидания FloodWait (5 мин)
    
    # Настройки конкурентности
    max_concurrent: int = 5         # максимум одновременных запросов
    
    # Человекоподобное поведение
    min_delay: float = 0.1          # минимальная задержка между запросами
    max_delay: float = 0.5          # максимальная задержка между запросами
    
    @classmethod
    def from_env(cls, 
                 global_limit: Optional[int] = None,
                 chat_limit: Optional[int] = None,
                 group_limit: Optional[int] = None,
                 resolve_daily_limit: Optional[int] = None,
                 enabled: bool = True) -> 'RateLimitConfig':
        """Создание конфигурации из переменных окружения"""
        return cls(
            global_limit=global_limit or 30,
            chat_limit=chat_limit or 1,
            group_limit=group_limit or 20,
            resolve_daily_limit=resolve_daily_limit or 200
        ) if enabled else cls(
            global_limit=1000,  # Отключаем лимиты
            chat_limit=100,
            group_limit=1000,
            resolve_daily_limit=10000
        )


@dataclass
class RateLimitStats:
    """Статистика работы Rate Limiter"""
    total_requests: int = 0
    successful_requests: int = 0
    rate_limited_requests: int = 0
    flood_errors: int = 0
    last_request_time: Optional[datetime] = None
    last_flood_time: Optional[datetime] = None
    resolve_requests_today: int = 0
    resolve_last_reset: Optional[datetime] = None
    
    def record_request(self):
        """Записать новый запрос"""
        self.total_requests += 1
        self.last_request_time = datetime.now()
    
    def record_success(self):
        """Записать успешный запрос"""
        self.successful_requests += 1
    
    def record_rate_limited(self):
        """Записать запрос, ограниченный rate limiter"""
        self.rate_limited_requests += 1
    
    def record_flood_error(self):
        """Записать FloodWaitError"""
        self.flood_errors += 1
        self.last_flood_time = datetime.now()
    
    def record_resolve_request(self):
        """Записать resolve запрос"""
        now = datetime.now()
        
        # Сброс счетчика если прошел день
        if (self.resolve_last_reset is None or 
            (now - self.resolve_last_reset).days >= 1):
            self.resolve_requests_today = 0
            self.resolve_last_reset = now
        
        self.resolve_requests_today += 1
    
    @property
    def success_rate(self) -> float:
        """Коэффициент успешности запросов"""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
    
    @property
    def flood_rate(self) -> float:
        """Коэффициент FloodWait ошибок"""
        if self.total_requests == 0:
            return 0.0
        return self.flood_errors / self.total_requests


class RateLimiter:
    """
    Rate Limiter для MCP Telegram сервера
    
    Основные возможности:
    - Соблюдение глобальных лимитов Telegram API
    - Per-chat rate limiting
    - Автоматические паузы для имитации человеческого поведения
    - Отслеживание resolve операций
    - Интеграция с логированием MCP
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self.stats = RateLimitStats()
        
        # Очереди для контроля частоты запросов
        self.global_queue: Deque[float] = deque()
        self.chat_queues: Dict[str, Deque[float]] = defaultdict(deque)
        
        # Семафор для ограничения конкурентности
        self.semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        # Блокировки для thread-safe операций
        self._global_lock = asyncio.Lock()
        self._chat_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        
        logger.info(
            f"RateLimiter initialized: {self.config.global_limit}/min global, "
            f"{self.config.chat_limit}/sec per chat, {self.config.group_limit}/min groups"
        )
    
    async def _cleanup_old_requests(self, queue: Deque[float], window_seconds: int) -> None:
        """Очистка старых записей из очереди"""
        now = time.time()
        while queue and queue[0] <= now - window_seconds:
            queue.popleft()
    
    async def _wait_for_global_limit(self) -> None:
        """Ожидание глобального лимита"""
        async with self._global_lock:
            now = time.time()
            
            # Очищаем записи старше 1 минуты
            await self._cleanup_old_requests(self.global_queue, 60)
            
            # Проверяем лимит
            if len(self.global_queue) >= self.config.global_limit:
                wait_time = 60 - (now - self.global_queue[0])
                if wait_time > 0:
                    logger.debug(f"Global rate limit: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    self.stats.record_rate_limited()
            
            # Добавляем текущий запрос
            self.global_queue.append(now)
    
    async def _wait_for_chat_limit(self, chat_id: str, is_group: bool = False) -> None:
        """Ожидание per-chat лимита"""
        async with self._chat_locks[chat_id]:
            now = time.time()
            chat_queue = self.chat_queues[chat_id]
            
            if is_group:
                # Групповой чат: лимит в минуту
                window_seconds = 60
                max_requests = self.config.group_limit
            else:
                # Личный чат: лимит в секунду
                window_seconds = 1
                max_requests = self.config.chat_limit
            
            # Очищаем старые записи
            await self._cleanup_old_requests(chat_queue, window_seconds)
            
            # Проверяем лимит
            if len(chat_queue) >= max_requests:
                wait_time = window_seconds - (now - chat_queue[0])
                if wait_time > 0:
                    logger.debug(f"Chat {chat_id} rate limit: waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)
                    self.stats.record_rate_limited()
            
            # Добавляем текущий запрос
            chat_queue.append(now)
    
    async def _add_human_delay(self, priority: int = 1) -> None:
        """Добавление случайной задержки для имитации человеческого поведения"""
        base_delay = random.uniform(self.config.min_delay, self.config.max_delay)
        # Меньше задержка для высокого приоритета
        delay = base_delay / priority
        await asyncio.sleep(delay)
    
    def _determine_chat_type(self, chat_id: Optional[str], method_name: str) -> bool:
        """Определение типа чата для выбора лимитов"""
        # Эвристика для определения групповых операций
        group_methods = {
            'get_chat_members', 'get_chat_admins', 'ban_chat_member', 
            'unban_chat_member', 'kick_chat_member', 'promote_to_admin',
            'add_chat_member', 'get_chat_online_count'
        }
        
        # Если метод связан с групповыми операциями, считаем чат группой
        return method_name.lower() in group_methods
    
    def check_resolve_limit(self) -> bool:
        """Проверка лимита resolve операций"""
        # Обновляем счетчик
        now = datetime.now()
        if (self.stats.resolve_last_reset is None or 
            (now - self.stats.resolve_last_reset).days >= 1):
            self.stats.resolve_requests_today = 0
            self.stats.resolve_last_reset = now
        
        return self.stats.resolve_requests_today < self.config.resolve_daily_limit
    
    @asynccontextmanager
    async def acquire(self, method_name: str, chat_id: Optional[str] = None, priority: int = 1):
        """
        Основной API для получения разрешения на выполнение запроса
        
        Args:
            method_name: Имя метода (для статистики и определения типа)
            chat_id: ID чата (опционально, для per-chat лимитов)
            priority: Приоритет запроса (1-5, где 5 = высший)
        
        Usage:
            async with rate_limiter.acquire('send_message', chat_id='123') as acquired:
                if acquired:
                    # выполнить операцию
                    result = await telegram_operation()
        """
        acquired = False
        
        try:
            # Проверка специальных лимитов
            if 'resolve' in method_name.lower():
                if not self.check_resolve_limit():
                    logger.warning(f"Daily resolve limit exceeded: {self.stats.resolve_requests_today}")
                    yield False
                    return
                self.stats.record_resolve_request()
            
            # Ограничение конкурентности
            async with self.semaphore:
                self.stats.record_request()
                
                # Ожидание глобального лимита
                await self._wait_for_global_limit()
                
                # Ожидание per-chat лимита если указан chat_id
                if chat_id:
                    is_group = self._determine_chat_type(chat_id, method_name)
                    await self._wait_for_chat_limit(str(chat_id), is_group)
                
                # Человекоподобная задержка
                await self._add_human_delay(priority)
                
                acquired = True
                logger.debug(f"Rate limiter acquired for {method_name} (chat: {chat_id})")
                
                yield True
                
        except Exception as e:
            logger.error(f"Rate limiter error for {method_name}: {e}")
            yield False
        finally:
            if acquired:
                self.stats.record_success()
                logger.debug(f"Rate limiter released for {method_name}")
    
    def get_stats(self) -> RateLimitStats:
        """Получение статистики работы"""
        return self.stats
    
    def log_stats(self) -> None:
        """Вывод статистики в лог"""
        stats = self.stats
        logger.info("=== Rate Limiter Statistics ===")
        logger.info(f"Total requests: {stats.total_requests}")
        logger.info(f"Successful: {stats.successful_requests} ({stats.success_rate:.1%})")
        logger.info(f"Rate limited: {stats.rate_limited_requests}")
        logger.info(f"Flood errors: {stats.flood_errors} ({stats.flood_rate:.1%})")
        logger.info(f"Resolve requests today: {stats.resolve_requests_today}/{self.config.resolve_daily_limit}")
        logger.info(f"Last request: {stats.last_request_time}")
        if stats.last_flood_time:
            logger.info(f"Last flood error: {stats.last_flood_time}")
    
    def is_healthy(self) -> bool:
        """Проверка здоровья Rate Limiter"""
        # Считаем здоровым если:
        # 1. Коэффициент успешности > 80%
        # 2. Коэффициент flood ошибок < 10%
        # 3. Не превышен лимит resolve запросов
        
        if self.stats.total_requests == 0:
            return True  # Еще не использовался
        
        success_ok = self.stats.success_rate > 0.8
        flood_ok = self.stats.flood_rate < 0.1
        resolve_ok = self.stats.resolve_requests_today < self.config.resolve_daily_limit
        
        is_healthy = success_ok and flood_ok and resolve_ok
        
        if not is_healthy:
            logger.warning(
                f"Rate Limiter unhealthy: success={self.stats.success_rate:.1%}, "
                f"flood={self.stats.flood_rate:.1%}, resolve={self.stats.resolve_requests_today}"
            )
        
        return is_healthy


# Глобальный экземпляр для использования в MCP tools
_global_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter(config: Optional[RateLimitConfig] = None) -> RateLimiter:
    """Получение глобального экземпляра Rate Limiter"""
    global _global_rate_limiter
    
    if _global_rate_limiter is None:
        _global_rate_limiter = RateLimiter(config)
    
    return _global_rate_limiter


def reset_rate_limiter() -> None:
    """Сброс глобального экземпляра (для тестирования)"""
    global _global_rate_limiter
    _global_rate_limiter = None


# Удобная функция для создания конфигурации из настроек
def create_rate_limiter_from_settings(
    global_limit: Optional[int] = None,
    chat_limit: Optional[int] = None,
    group_limit: Optional[int] = None,
    resolve_daily_limit: Optional[int] = None,
    enabled: bool = True
) -> RateLimiter:
    """
    Создание Rate Limiter из настроек окружения
    
    Args:
        global_limit: Глобальный лимит (запросов в минуту)
        chat_limit: Лимит на чат (запросов в секунду)
        group_limit: Лимит на группы (запросов в минуту)
        resolve_daily_limit: Лимит resolve операций в день
        enabled: Включен ли rate limiting
    """
    config = RateLimitConfig.from_env(
        global_limit=global_limit,
        chat_limit=chat_limit,
        group_limit=group_limit,
        resolve_daily_limit=resolve_daily_limit,
        enabled=enabled
    )
    
    return RateLimiter(config)