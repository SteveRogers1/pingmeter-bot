import os
from typing import Optional
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Конфигурация базы данных"""
    url: str
    min_connections: int = 2
    max_connections: int = 10
    command_timeout: int = 60
    pool_timeout: int = 30

@dataclass
class BotConfig:
    """Конфигурация бота"""
    token: str
    main_admin_id: Optional[int] = None
    rate_limit_per_second: int = 30
    polling_timeout: int = 30

@dataclass
class SecurityConfig:
    """Конфигурация безопасности"""
    activation_code_length: int = 8
    activation_code_expiry_hours: int = 24
    max_activation_codes_per_admin: int = 10
    max_chats_per_admin: int = 50

@dataclass
class CacheConfig:
    """Конфигурация кэширования"""
    admin_cache_ttl: int = 300  # 5 минут
    rate_limit_window: float = 1.0  # секунды
    username_cache_size: int = 1000
    commands_cache_size: int = 100

@dataclass
class LoggingConfig:
    """Конфигурация логирования"""
    level: str = "INFO"
    format: str = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    mask_sensitive_data: bool = True

class Config:
    """Основная конфигурация приложения"""
    
    def __init__(self):
        self.database = DatabaseConfig(
            url=os.getenv("DATABASE_URL", ""),
            min_connections=int(os.getenv("DB_MIN_CONNECTIONS", "2")),
            max_connections=int(os.getenv("DB_MAX_CONNECTIONS", "10")),
            command_timeout=int(os.getenv("DB_COMMAND_TIMEOUT", "60")),
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30"))
        )
        
        self.bot = BotConfig(
            token=os.getenv("BOT_TOKEN", ""),
            main_admin_id=int(os.getenv("MAIN_ADMIN_ID")) if os.getenv("MAIN_ADMIN_ID") else None,
            rate_limit_per_second=int(os.getenv("BOT_RATE_LIMIT", "30")),
            polling_timeout=int(os.getenv("BOT_POLLING_TIMEOUT", "30"))
        )
        
        self.security = SecurityConfig(
            activation_code_length=int(os.getenv("ACTIVATION_CODE_LENGTH", "8")),
            activation_code_expiry_hours=int(os.getenv("ACTIVATION_CODE_EXPIRY", "24")),
            max_activation_codes_per_admin=int(os.getenv("MAX_ACTIVATION_CODES", "10")),
            max_chats_per_admin=int(os.getenv("MAX_CHATS_PER_ADMIN", "50"))
        )
        
        self.cache = CacheConfig(
            admin_cache_ttl=int(os.getenv("ADMIN_CACHE_TTL", "300")),
            rate_limit_window=float(os.getenv("RATE_LIMIT_WINDOW", "1.0")),
            username_cache_size=int(os.getenv("USERNAME_CACHE_SIZE", "1000")),
            commands_cache_size=int(os.getenv("COMMANDS_CACHE_SIZE", "100"))
        )
        
        self.logging = LoggingConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format=os.getenv("LOG_FORMAT", "%(asctime)s %(levelname)s %(name)s: %(message)s"),
            mask_sensitive_data=os.getenv("MASK_SENSITIVE_DATA", "true").lower() == "true"
        )
    
    def validate(self) -> bool:
        """Валидация конфигурации"""
        if not self.database.url:
            raise ValueError("DATABASE_URL не установлен")
        
        if not self.bot.token:
            raise ValueError("BOT_TOKEN не установлен")
        
        if not self.bot.main_admin_id:
            raise ValueError("MAIN_ADMIN_ID не установлен")
        
        return True

# Глобальный экземпляр конфигурации
config = Config()
