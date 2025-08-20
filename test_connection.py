#!/usr/bin/env python3
"""
Тестовый скрипт для проверки подключения к базе данных и Telegram API
"""
import asyncio
import os
import sys
from dotenv import load_dotenv
import asyncpg
from aiogram import Bot

async def test_database():
    """Тестирует подключение к базе данных"""
    print("🔌 Тестирование подключения к базе данных...")
    
    try:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            print("❌ DATABASE_URL не установлен")
            return False
            
        print(f"📋 DSN: {dsn[:50]}...")
        
        # Тестируем подключение
        conn = await asyncpg.connect(dsn)
        print("✅ Подключение к базе данных успешно")
        
        # Проверяем таблицы
        tables = await conn.fetch("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        print(f"📊 Найдено таблиц: {len(tables)}")
        for table in tables:
            print(f"  - {table['table_name']}")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        return False

async def test_telegram():
    """Тестирует подключение к Telegram API"""
    print("\n🤖 Тестирование подключения к Telegram API...")
    
    try:
        token = os.getenv("BOT_TOKEN")
        if not token:
            print("❌ BOT_TOKEN не установлен")
            return False
            
        print(f"📋 Token: {token[:20]}...")
        
        bot = Bot(token=token)
        me = await bot.get_me()
        print(f"✅ Бот @{me.username} (ID: {me.id}) доступен")
        
        await bot.session.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка подключения к Telegram API: {e}")
        return False

async def main():
    """Основная функция тестирования"""
    load_dotenv()
    
    print("🧪 Запуск тестов подключения...")
    print("=" * 50)
    
    db_ok = await test_database()
    telegram_ok = await test_telegram()
    
    print("=" * 50)
    print("📊 Результаты тестирования:")
    print(f"  База данных: {'✅ OK' if db_ok else '❌ FAIL'}")
    print(f"  Telegram API: {'✅ OK' if telegram_ok else '❌ FAIL'}")
    
    if db_ok and telegram_ok:
        print("\n🎉 Все тесты пройдены! Система готова к работе.")
        return 0
    else:
        print("\n⚠️  Некоторые тесты не пройдены. Проверьте настройки.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
