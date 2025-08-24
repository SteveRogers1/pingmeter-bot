import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramConflictError

from app.handlers import router
from app.db import Database


def validate_bot_token(token: str) -> bool:
    """Валидация токена бота"""
    if not token:
        return False
    # Telegram bot token format: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz
    pattern = r'^\d{8,10}:[A-Za-z0-9_-]{35}$'
    return bool(re.match(pattern, token))


def setup_logging(log_level: str = "INFO") -> None:
    """Настройка логирования с улучшенной безопасностью"""
    # Убираем чувствительные данные из логов
    class SensitiveFormatter(logging.Formatter):
        def format(self, record):
            # Маскируем токены и пароли в логах
            if hasattr(record, 'msg'):
                record.msg = re.sub(r'BOT_TOKEN=[^,\s]+', 'BOT_TOKEN=***', str(record.msg))
                record.msg = re.sub(r'DATABASE_URL=[^,\s]+', 'DATABASE_URL=***', str(record.msg))
            return super().format(record)
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Применяем форматтер ко всем логгерам
    for handler in logging.root.handlers:
        handler.setFormatter(SensitiveFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))


@asynccontextmanager
async def app_lifespan(db: Database):
    """Контекстный менеджер для жизненного цикла приложения"""
    try:
        await db.initialize()
        yield
    except Exception as e:
        logging.error(f"Ошибка инициализации базы данных: {e}")
        raise
    finally:
        try:
            await db.close()
        except Exception as e:
            logging.error(f"Ошибка закрытия базы данных: {e}")


async def run() -> None:
    """Основная функция запуска бота с улучшенной обработкой ошибок"""
    try:
        # Загружаем переменные окружения
        load_dotenv()
        
        # Настраиваем логирование
        log_level = os.getenv("LOG_LEVEL", "INFO")
        setup_logging(log_level)
        
        # Валидируем токен
        token = os.getenv("BOT_TOKEN")
        if not token:
            raise RuntimeError("BOT_TOKEN не установлен в переменных окружения")
        
        if not validate_bot_token(token):
            raise RuntimeError("Неверный формат BOT_TOKEN")
        
        # Валидируем DATABASE_URL
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL не установлен в переменных окружения")
        
        # Проверяем MAIN_ADMIN_ID
        main_admin_id = os.getenv("MAIN_ADMIN_ID")
        if not main_admin_id or not main_admin_id.isdigit():
            logging.warning("MAIN_ADMIN_ID не установлен или неверный формат")
        
        logging.info("🚀 Запуск бота...")
        
        # Создаем экземпляр базы данных
        db = Database()
        
        async with app_lifespan(db):
            logging.info("🤖 Создание бота...")
            
            # Создаем бота с улучшенными настройками
            bot = Bot(
                token=token, 
                default=DefaultBotProperties(parse_mode=ParseMode.HTML)
            )
            
            # Привязываем базу данных к боту
            setattr(bot, "db", db)
            
            # Получаем информацию о боте
            try:
                logging.info("📡 Получение информации о боте...")
                me = await bot.get_me()
                setattr(bot, "bot_id", me.id)
                logging.info(f"✅ Бот @{me.username} (ID: {me.id}) готов к работе")
            except TelegramBadRequest as e:
                logging.error(f"Ошибка получения информации о боте: {e}")
                raise
            
            # Создаем диспетчер
            dp = Dispatcher()
            dp.include_router(router)
            
            logging.info("🔄 Запуск polling...")
            
            # Запускаем бота с обработкой ошибок
            try:
                await dp.start_polling(bot)
            except TelegramConflictError:
                logging.error("Обнаружен конфликт с другим экземпляром бота")
                raise
            except Exception as e:
                logging.error(f"Ошибка polling: {e}")
                raise
                
    except Exception as e:
        logging.error(f"❌ Критическая ошибка при запуске: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        exit(1)