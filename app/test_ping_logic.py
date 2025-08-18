import pytest
from app.db import Database
import asyncio
import datetime

@pytest.mark.asyncio
async def test_ping_creation_and_open_pings(tmp_path):
    db = Database(dsn=None)
    db._dsn = f"sqlite:///{tmp_path}/test.db"  # Для теста можно использовать SQLite через aiosqlite, если поддерживается
    await db.initialize()
    chat_id = 123
    user_id = 1
    source_user_id = 2
    now = int(datetime.datetime.utcnow().timestamp())
    # Создать пинг через тег
    await db.record_ping(chat_id, 100, source_user_id, user_id, "mention", now)
    open_pings = await db.get_open_pings(chat_id)
    assert len(open_pings) == 1
    assert open_pings[0][0] == user_id
    # Не создавать дублирующий пинг
    await db.record_ping(chat_id, 101, source_user_id, user_id, "mention", now+10)
    open_pings = await db.get_open_pings(chat_id)
    assert len(open_pings) == 1
    # Закрыть пинг
    await db.close_oldest_open_ping_by_message(chat_id, user_id, 200, now+20)
    open_pings = await db.get_open_pings(chat_id)
    assert len(open_pings) == 0
    # Проверить, что пользователь с n=0 появляется в open_pings
    await db.record_ping(chat_id, 102, source_user_id, 3, "mention", now+30)
    open_pings = await db.get_open_pings(chat_id)
    assert open_pings[0][0] == 3
