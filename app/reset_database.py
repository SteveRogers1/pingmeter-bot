import asyncio
import os
from db import Database

async def reset_database():
    """Полностью очищает базу данных и пересоздает таблицы"""
    db = Database()
    await db.initialize()
    
    print("🗑️ Начинаем полную очистку базы данных...")
    
    async with db.pool.acquire() as conn:
        # Удаляем все данные из таблиц
        print("📊 Очищаем таблицу pings...")
        await conn.execute("DELETE FROM pings")
        
        print("👥 Очищаем таблицу users...")
        await conn.execute("DELETE FROM users")
        
        print("🔑 Очищаем таблицу activation_codes...")
        await conn.execute("DELETE FROM activation_codes")
        
        print("💬 Очищаем таблицу activated_chats...")
        await conn.execute("DELETE FROM activated_chats")
        
        # Сбрасываем автоинкрементные счетчики
        print("🔄 Сбрасываем автоинкрементные счетчики...")
        await conn.execute("ALTER SEQUENCE pings_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE activation_codes_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE activated_chats_id_seq RESTART WITH 1")
        
        print("✅ База данных полностью очищена!")
        
        # Проверяем что таблицы пустые
        pings_count = await conn.fetchval("SELECT COUNT(*) FROM pings")
        users_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        codes_count = await conn.fetchval("SELECT COUNT(*) FROM activation_codes")
        chats_count = await conn.fetchval("SELECT COUNT(*) FROM activated_chats")
        
        print(f"📋 Проверка очистки:")
        print(f"  - pings: {pings_count}")
        print(f"  - users: {users_count}")
        print(f"  - activation_codes: {codes_count}")
        print(f"  - activated_chats: {chats_count}")

if __name__ == "__main__":
    asyncio.run(reset_database())
