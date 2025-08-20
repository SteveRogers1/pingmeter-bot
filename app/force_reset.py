import asyncio
import os
from db import Database

async def force_reset():
    """Принудительная очистка базы данных"""
    db = Database()
    await db.initialize()
    
    print("🗑️ ПРИНУДИТЕЛЬНАЯ ОЧИСТКА БАЗЫ ДАННЫХ...")
    
    async with db.pool.acquire() as conn:
        try:
            # Удаляем ВСЕ данные
            print("📊 Удаляем ВСЕ пинги...")
            await conn.execute("DELETE FROM pings")
            
            print("👥 Удаляем ВСЕХ пользователей...")
            await conn.execute("DELETE FROM users")
            
            print("🔑 Удаляем ВСЕ коды активации...")
            await conn.execute("DELETE FROM activation_codes")
            
            print("💬 Удаляем ВСЕ активированные чаты...")
            await conn.execute("DELETE FROM activated_chats")
            
            # Сбрасываем счетчики
            print("🔄 Сбрасываем счетчики...")
            await conn.execute("ALTER SEQUENCE IF EXISTS pings_id_seq RESTART WITH 1")
            await conn.execute("ALTER SEQUENCE IF EXISTS activation_codes_id_seq RESTART WITH 1")
            await conn.execute("ALTER SEQUENCE IF EXISTS activated_chats_id_seq RESTART WITH 1")
            
            print("✅ БАЗА ДАННЫХ ПОЛНОСТЬЮ ОЧИЩЕНА!")
            
            # Проверка
            pings = await conn.fetchval("SELECT COUNT(*) FROM pings")
            users = await conn.fetchval("SELECT COUNT(*) FROM users")
            codes = await conn.fetchval("SELECT COUNT(*) FROM activation_codes")
            chats = await conn.fetchval("SELECT COUNT(*) FROM activated_chats")
            
            print(f"📋 Проверка: pings={pings}, users={users}, codes={codes}, chats={chats}")
            
        except Exception as e:
            print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(force_reset())
