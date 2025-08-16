import aiosqlite
import datetime as dt
from typing import Optional, List, Tuple


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not initialized")
        return self._conn

    async def initialize(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        await self._create_schema()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _create_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen_ts INTEGER
            );

            CREATE TABLE IF NOT EXISTS pings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                source_user_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                ping_reason TEXT NOT NULL,
                ping_ts INTEGER NOT NULL,
                closed_ts INTEGER,
                close_type TEXT,
                close_message_id INTEGER,
                reaction_emoji TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pings_open ON pings(chat_id, target_user_id, closed_ts);
            CREATE INDEX IF NOT EXISTS idx_pings_time ON pings(chat_id, ping_ts);
            """
        )
        await self.conn.commit()

    async def upsert_user(
        self,
        user_id: int,
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
    ) -> None:
        now = int(dt.datetime.utcnow().timestamp())
        await self.conn.execute(
            """
            INSERT INTO users(user_id, username, first_name, last_name, last_seen_ts)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_seen_ts=excluded.last_seen_ts
            """,
            (user_id, username, first_name, last_name, now),
        )
        await self.conn.commit()

    async def record_ping(
        self,
        chat_id: int,
        source_message_id: int,
        source_user_id: int,
        target_user_id: int,
        ping_reason: str,
        ping_ts: int,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO pings(chat_id, source_message_id, source_user_id, target_user_id, ping_reason, ping_ts)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (chat_id, source_message_id, source_user_id, target_user_id, ping_reason, ping_ts),
        )
        await self.conn.commit()

    async def close_oldest_open_ping_by_message(
        self,
        chat_id: int,
        target_user_id: int,
        close_message_id: int,
        close_ts: int,
    ) -> Optional[int]:
        async with self.conn.execute(
            """
            SELECT id FROM pings
            WHERE chat_id=? AND target_user_id=? AND closed_ts IS NULL
            ORDER BY ping_ts ASC
            LIMIT 1
            """,
            (chat_id, target_user_id),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            ping_id = row[0]
        await self.conn.execute(
            """
            UPDATE pings SET closed_ts=?, close_type='message', close_message_id=? WHERE id=?
            """,
            (close_ts, close_message_id, ping_id),
        )
        await self.conn.commit()
        return ping_id

    async def close_ping_by_reaction(
        self,
        chat_id: int,
        target_user_id: int,
        reacted_message_id: int,
        emoji: Optional[str],
        close_ts: int,
    ) -> Optional[int]:
        async with self.conn.execute(
            """
            SELECT id FROM pings
            WHERE chat_id=? AND target_user_id=? AND source_message_id=? AND closed_ts IS NULL
            ORDER BY ping_ts ASC
            LIMIT 1
            """,
            (chat_id, target_user_id, reacted_message_id),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            ping_id = row[0]
        await self.conn.execute(
            """
            UPDATE pings SET closed_ts=?, close_type='reaction', reaction_emoji=? WHERE id=?
            """,
            (close_ts, emoji, ping_id),
        )
        await self.conn.commit()
        return ping_id

    async def resolve_username(self, username: str) -> Optional[int]:
        async with self.conn.execute(
            "SELECT user_id FROM users WHERE lower(username)=lower(?) ORDER BY last_seen_ts DESC LIMIT 1",
            (username,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def get_top(
        self, chat_id: int, since_ts: Optional[int], limit: int = 10
    ) -> List[Tuple[int, float, int, Optional[str], Optional[str], Optional[str]]]:
        params: List[object] = [chat_id]
        where = "closed_ts IS NOT NULL"
        if since_ts is not None:
            where += " AND ping_ts >= ?"
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
        WHERE p.chat_id=? AND {where}
        GROUP BY p.target_user_id
        ORDER BY avg_response ASC
        LIMIT {limit}
        """
        async with self.conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
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

    async def get_user_stats(
        self, chat_id: int, user_id: int, since_ts: Optional[int]
    ) -> Optional[Tuple[float, int]]:
        params: List[object] = [chat_id, user_id]
        where = "closed_ts IS NOT NULL"
        if since_ts is not None:
            where += " AND ping_ts >= ?"
            params.append(since_ts)
        query = f"""
        SELECT AVG(closed_ts - ping_ts) AS avg_response, COUNT(*) as cnt
        FROM pings
        WHERE chat_id=? AND target_user_id=? AND {where}
        """
        async with self.conn.execute(query, params) as cursor:
            row = await cursor.fetchone()
            if row and row[0] is not None:
                return float(row[0]), int(row[1])
            return None


