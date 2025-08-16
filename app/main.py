import asyncio
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode

from handlers import router
from db import Database


@asynccontextmanager
async def app_lifespan(db: Database):
    try:
        await db.initialize()
        yield
    finally:
        await db.close()


async def run() -> None:
    load_dotenv()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set in environment")

    db_path = os.getenv("DB_PATH", "data.sqlite3")
    db = Database(db_path)

    async with app_lifespan(db):
        bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        # Share db via bot context
        setattr(bot, "db", db)

        dp = Dispatcher()
        dp.include_router(router)

        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(run())