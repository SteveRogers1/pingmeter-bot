import asyncio
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode

from app.handlers import router
from app.db import Database


@asynccontextmanager
async def app_lifespan(db: Database):
    try:
        await db.initialize()
        yield
    finally:
        await db.close()


async def run() -> None:
    try:
        load_dotenv()
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

        token = os.getenv("BOT_TOKEN")
        if not token:
            raise RuntimeError("BOT_TOKEN is not set in environment")

        print("🚀 Запуск бота...")
        db = Database()  # Не передаём db_path, пусть берёт из DATABASE_URL

        async with app_lifespan(db):
            print("🤖 Создание бота...")
            bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            setattr(bot, "db", db)
            
            print("📡 Получение информации о боте...")
            me = await bot.get_me()
            setattr(bot, "bot_id", me.id)
            print(f"✅ Бот @{me.username} (ID: {me.id}) готов к работе")
            
            dp = Dispatcher()
            dp.include_router(router)
            print("🔄 Запуск polling...")
            await dp.start_polling(bot)
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске: {e}")
        logging.error(f"Критическая ошибка при запуске: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())