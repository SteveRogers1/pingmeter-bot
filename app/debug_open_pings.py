import asyncio
import os
from db import Database

async def debug_open_pings():
    db = Database()
    await db.initialize()
    
    # Проверим все чаты
    async with db.pool.acquire() as conn:
        # Получим все чаты с пингами
        chats = await conn.fetch("SELECT DISTINCT chat_id FROM pings")
        print(f"Найдено чатов с пингами: {len(chats)}")
        
        for chat_row in chats:
            chat_id = chat_row['chat_id']
            print(f"\n=== Чат {chat_id} ===")
            
            # Все пинги в чате
            all_pings = await conn.fetch("""
                SELECT target_user_id, ping_ts, close_ts, source_message_id 
                FROM pings 
                WHERE chat_id = $1 
                ORDER BY ping_ts DESC 
                LIMIT 10
            """, chat_id)
            
            print(f"Всего пингов: {len(all_pings)}")
            
            # Открытые пинги
            open_pings = await conn.fetch("""
                SELECT target_user_id, ping_ts, source_message_id 
                FROM pings 
                WHERE chat_id = $1 AND close_ts IS NULL
                ORDER BY ping_ts DESC
            """, chat_id)
            
            print(f"Открытых пингов: {len(open_pings)}")
            
            for ping in open_pings:
                print(f"  - User {ping['target_user_id']}, ping_ts: {ping['ping_ts']}, msg_id: {ping['source_message_id']}")

if __name__ == "__main__":
    asyncio.run(debug_open_pings())
