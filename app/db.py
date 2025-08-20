import os
import asyncpg
import datetime
from typing import Optional, List, Tuple

class Database:
    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or os.getenv("DATABASE_URL")
        print("DATABASE_URL:", self._dsn)  # DEBUG: print DSN for troubleshooting
        self.pool: Optional[asyncpg.Pool] = None
        if not self._dsn:
            raise RuntimeError("DATABASE_URL not set")

    async def initialize(self):
        self.pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)
        async with self.pool.acquire() as conn:
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen_ts BIGINT
            );
            CREATE TABLE IF NOT EXISTS pings (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                source_message_id BIGINT NOT NULL,
                source_user_id BIGINT NOT NULL,
                target_user_id BIGINT NOT NULL,
                ping_reason TEXT NOT NULL,
                ping_ts BIGINT NOT NULL,
                closed_ts BIGINT,
                close_type TEXT,
                close_message_id BIGINT,
                reaction_emoji TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pings_open ON pings(chat_id, target_user_id, closed_ts);
            CREATE INDEX IF NOT EXISTS idx_pings_time ON pings(chat_id, ping_ts);
            """)

    async def upsert_user(self, user_id: int, username: Optional[str], first_name: Optional[str], last_name: Optional[str]):
        now = int(datetime.datetime.utcnow().timestamp())
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users(user_id, username, first_name, last_name, last_seen_ts)
                VALUES($1, $2, $3, $4, $5)
                ON CONFLICT (user_id) DO UPDATE SET
                    username=EXCLUDED.username,
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    last_seen_ts=EXCLUDED.last_seen_ts
                """,
                user_id, username, first_name, last_name, now
            )

    async def record_ping(self, chat_id: int, source_message_id: int, source_user_id: int, target_user_id: int, ping_reason: str, ping_ts: int):
        async with self.pool.acquire() as conn:
            # Проверяем, есть ли уже открытый пинг для этого пользователя в этом чате
            row = await conn.fetchrow(
                """
                SELECT id FROM pings
                WHERE chat_id=$1 AND target_user_id=$2 AND closed_ts IS NULL
                LIMIT 1
                """,
                chat_id, target_user_id
            )
            if row:
                return  # Уже есть открытый пинг, не создаём новый
            await conn.execute(
                """
                INSERT INTO pings(chat_id, source_message_id, source_user_id, target_user_id, ping_reason, ping_ts)
                VALUES($1, $2, $3, $4, $5, $6)
                """,
                chat_id, source_message_id, source_user_id, target_user_id, ping_reason, ping_ts
            )

    async def close_oldest_open_ping_by_message(self, chat_id: int, target_user_id: int, close_message_id: int, close_ts: int) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id FROM pings
                WHERE chat_id=$1 AND target_user_id=$2 AND closed_ts IS NULL
                ORDER BY ping_ts ASC
                LIMIT 1
                """,
                chat_id, target_user_id
            )
            if not row:
                return None
            ping_id = row["id"]
            await conn.execute(
                """
                UPDATE pings SET closed_ts=$1, close_type='message', close_message_id=$2 WHERE id=$3
                """,
                close_ts, close_message_id, ping_id
            )
            return ping_id

    async def close_ping_by_reaction(self, chat_id: int, target_user_id: int, reacted_message_id: int, emoji: Optional[str], close_ts: int) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id FROM pings
                WHERE chat_id=$1 AND target_user_id=$2 AND source_message_id=$3 AND closed_ts IS NULL
                ORDER BY ping_ts ASC
                LIMIT 1
                """,
                chat_id, target_user_id, reacted_message_id
            )
            if not row:
                return None
            ping_id = row["id"]
            await conn.execute(
                """
                UPDATE pings SET closed_ts=$1, close_type='reaction', reaction_emoji=$2 WHERE id=$3
                """,
                close_ts, emoji, ping_id
            )
            return ping_id

    async def close_all_open_pings_by_message(self, chat_id: int, target_user_id: int, close_message_id: int, close_ts: int) -> int:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id FROM pings
                WHERE chat_id=$1 AND target_user_id=$2 AND closed_ts IS NULL
                """,
                chat_id, target_user_id
            )
            ping_ids = [row["id"] for row in rows]
            if not ping_ids:
                return 0
            await conn.executemany(
                """
                UPDATE pings SET closed_ts=$1, close_type='message', close_message_id=$2 WHERE id=$3
                """,
                [(close_ts, close_message_id, pid) for pid in ping_ids]
            )
            return len(ping_ids)

    async def resolve_username(self, username: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id FROM users WHERE lower(username)=lower($1) ORDER BY last_seen_ts DESC LIMIT 1
                """,
                username
            )
            return row["user_id"] if row else None

    async def get_user_info(self, user_id: int):
        """Получить информацию о пользователе"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT username, first_name, last_name
                FROM users
                WHERE user_id = $1
                """,
                user_id
            )
            if row:
                return {
                    'username': row['username'],
                    'first_name': row['first_name'],
                    'last_name': row['last_name']
                }
            return None

    async def get_top(self, chat_id: int, limit: int = 10):
        """Получить топ пользователей по времени ответа"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    p.target_user_id,
                    COUNT(*) as n,
                    AVG(p.close_ts - p.ping_ts) as avg_sec,
                    u.username
                FROM pings p
                LEFT JOIN users u ON p.target_user_id = u.user_id
                WHERE p.chat_id = $1 
                AND p.close_ts IS NOT NULL
                GROUP BY p.target_user_id, u.username
                HAVING COUNT(*) >= 1
                ORDER BY avg_sec ASC
                LIMIT $2
                """,
                chat_id, limit
            )
            return [(row['target_user_id'], row['n'], row['avg_sec'], row['username'] or f'user_{row["target_user_id"]}') for row in rows]

    async def get_user_stats(self, chat_id: int, user_id: int, since_ts: Optional[int]) -> Optional[Tuple[int, float]]:
        params: List = [chat_id, user_id]
        where = "close_ts IS NOT NULL"
        if since_ts is not None:
            where += " AND ping_ts >= $3"
            params.append(since_ts)
        query = f"""
        SELECT COUNT(*) as cnt, AVG(close_ts - ping_ts) AS avg_response
        FROM pings
        WHERE chat_id=$1 AND target_user_id=$2 AND {where}
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            if row and row[0] > 0:
                return (int(row[0]), float(row[1]) if row[1] is not None else None)
            return None

    async def get_open_pings(self, chat_id: int) -> List[Tuple[int, int, int]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT target_user_id, ping_ts, source_message_id FROM pings
                WHERE chat_id=$1 AND closed_ts IS NULL
                """,
                chat_id
            )
            return [(r[0], r[1], r[2] if r[2] is not None else None) for r in rows]

    async def close_oldest_open_ping_by_reaction(self, chat_id: int, target_user_id: int, close_message_id: int, close_ts: int):
        """Закрыть самый старый открытый пинг по реакции"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pings
                SET close_ts = $3, close_message_id = $4
                WHERE id = (
                    SELECT id FROM pings
                    WHERE chat_id = $1 AND target_user_id = $2 AND close_ts IS NULL
                    ORDER BY ping_ts ASC
                    LIMIT 1
                )
                """,
                chat_id, target_user_id, close_ts, close_message_id
            )

    async def close(self):
        if self.pool is not None:
            await self.pool.close()


