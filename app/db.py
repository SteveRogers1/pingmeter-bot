import os
import asyncpg
from typing import Optional, List, Tuple

class Database:
    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or os.getenv("DATABASE_URL")
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
        now = int(asyncpg.types.datetime.datetime.utcnow().timestamp())
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

    async def resolve_username(self, username: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id FROM users WHERE lower(username)=lower($1) ORDER BY last_seen_ts DESC LIMIT 1
                """,
                username
            )
            return row["user_id"] if row else None

    async def get_top(self, chat_id: int, since_ts: Optional[int], limit: int = 10) -> List[Tuple[int, float, int, Optional[str], Optional[str], Optional[str]]]:
        params: List = [chat_id]
        where = "closed_ts IS NOT NULL"
        if since_ts is not None:
            where += " AND ping_ts >= $2"
            params.append(since_ts)
        query = f"""
        SELECT p.target_user_id,
               AVG(p.closed_ts - p.ping_ts) AS avg_response,
               COUNT(*) as cnt,
               u.username,
               u.first_name,
               u.last_name
        FROM pings p
        LEFT JOIN users u ON u.user_id = p.target_user_id
        WHERE p.chat_id=$1 AND {where}
        GROUP BY p.target_user_id, u.username, u.first_name, u.last_name
        ORDER BY avg_response ASC
        LIMIT {limit}
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [
                (
                    int(r[0]),
                    float(r[1]),
                    int(r[2]),
                    r[3],
                    r[4],
                    r[5],
                )
                for r in rows
            ]

    async def get_user_stats(self, chat_id: int, user_id: int, since_ts: Optional[int]) -> Optional[Tuple[float, int]]:
        params: List = [chat_id, user_id]
        where = "closed_ts IS NOT NULL"
        if since_ts is not None:
            where += " AND ping_ts >= $3"
            params.append(since_ts)
        query = f"""
        SELECT AVG(closed_ts - ping_ts) AS avg_response, COUNT(*) as cnt
        FROM pings
        WHERE chat_id=$1 AND target_user_id=$2 AND {where}
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            if row and row[0] is not None:
                return float(row[0]), int(row[1])
            return None

    async def close(self):
        if self.pool is not None:
            await self.pool.close()


