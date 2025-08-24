import os
import asyncpg
from datetime import datetime
from typing import Optional, List, Tuple

class Database:
    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or os.getenv("DATABASE_URL")
        print("DATABASE_URL:", self._dsn)  # DEBUG: print DSN for troubleshooting
        self.pool: Optional[asyncpg.Pool] = None
        if not self._dsn:
            raise RuntimeError("DATABASE_URL not set")

    async def initialize(self):
        try:
            print(f"🔌 Подключаемся к базе данных...")
            self.pool = await asyncpg.create_pool(
                self._dsn, 
                min_size=1, 
                max_size=5,
                command_timeout=30,
                server_settings={'application_name': 'pingmeter_bot'}
            )
            print(f"✅ Подключение к базе данных установлено")
        except Exception as e:
            print(f"❌ Ошибка подключения к базе данных: {e}")
            raise
        async with self.pool.acquire() as conn:
            # Проверяем существующие таблицы
            existing_tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name IN ('users', 'pings', 'activation_codes', 'activated_chats')
            """)
            existing_table_names = [row['table_name'] for row in existing_tables]
            
            # Создаём таблицы если их нет
            if 'users' not in existing_table_names:
                await conn.execute("""
                CREATE TABLE users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    last_seen_ts BIGINT
                );
                """)
            
            if 'pings' not in existing_table_names:
                await conn.execute("""
                CREATE TABLE pings (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    source_message_id BIGINT NOT NULL,
                    source_user_id BIGINT NOT NULL,
                    target_user_id BIGINT NOT NULL,
                    ping_reason TEXT NOT NULL,
                    ping_ts BIGINT NOT NULL,
                    close_ts BIGINT,
                    close_type TEXT,
                    close_message_id BIGINT,
                    reaction_emoji TEXT
                );
                """)
            else:
                # Миграция существующей таблицы pings
                columns = await conn.fetch("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'pings' AND table_schema = 'public'
                """)
                column_names = [row['column_name'] for row in columns]
                
                # Если есть старая колонка closed_ts, переименовываем её
                if 'closed_ts' in column_names and 'close_ts' not in column_names:
                    await conn.execute("ALTER TABLE pings RENAME COLUMN closed_ts TO close_ts;")
                
                # Добавляем недостающие колонки если их нет
                if 'close_type' not in column_names:
                    await conn.execute("ALTER TABLE pings ADD COLUMN close_type TEXT;")
                if 'close_message_id' not in column_names:
                    await conn.execute("ALTER TABLE pings ADD COLUMN close_message_id BIGINT;")
                if 'reaction_emoji' not in column_names:
                    await conn.execute("ALTER TABLE pings ADD COLUMN reaction_emoji TEXT;")
            
            if 'activation_codes' not in existing_table_names:
                await conn.execute("""
                CREATE TABLE activation_codes (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    expires_at BIGINT NOT NULL,
                    created_by BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                );
                """)
            
            if 'activated_chats' not in existing_table_names:
                await conn.execute("""
                CREATE TABLE activated_chats (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT UNIQUE NOT NULL,
                    chat_name TEXT NOT NULL,
                    activated_by BIGINT NOT NULL,
                    activated_at BIGINT NOT NULL,
                    activation_code TEXT NOT NULL
                );
                """)
            
            # Создаём индексы если их нет
            await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pings_open ON pings(chat_id, target_user_id, close_ts);
            CREATE INDEX IF NOT EXISTS idx_pings_time ON pings(chat_id, ping_ts);
            CREATE INDEX IF NOT EXISTS idx_activation_codes_expires ON activation_codes(expires_at);
            CREATE INDEX IF NOT EXISTS idx_activated_chats_chat_id ON activated_chats(chat_id);
            """)
            
            # Миграция старых чатов из whitelist (если есть переменная ALLOWED_CHATS)
            allowed_chats = os.getenv("ALLOWED_CHATS", "")
            if allowed_chats:
                allowed_ids = [int(x.strip()) for x in allowed_chats.split(",") if x.strip()]
                for chat_id in allowed_ids:
                    # Проверяем, не активирован ли уже чат
                    existing = await conn.fetchrow(
                        "SELECT chat_id FROM activated_chats WHERE chat_id = $1",
                        chat_id
                    )
                    if not existing:
                        # Активируем старый чат автоматически
                        await conn.execute("""
                        INSERT INTO activated_chats(chat_id, chat_name, activated_by, activated_at, activation_code)
                        VALUES($1, $2, $3, $4, $5)
                        ON CONFLICT (chat_id) DO NOTHING
                        """,
                        chat_id, f"Legacy Chat {chat_id}", 0, int(datetime.utcnow().timestamp()), "LEGACY_MIGRATION"
                        )


    async def upsert_user(self, user_id: int, username: Optional[str], first_name: Optional[str], last_name: Optional[str]):
        now = int(datetime.utcnow().timestamp())
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
                WHERE chat_id=$1 AND target_user_id=$2 AND close_ts IS NULL
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
                WHERE chat_id=$1 AND target_user_id=$2 AND close_ts IS NULL
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
                UPDATE pings SET close_ts=$1, close_type='message', close_message_id=$2 WHERE id=$3
                """,
                close_ts, close_message_id, ping_id
            )
            return ping_id





    async def resolve_username(self, username: str) -> Optional[int]:
        async with self.pool.acquire() as conn:
            print(f"🔍 Ищем username='{username}' в базе данных")
            row = await conn.fetchrow(
                """
                SELECT user_id FROM users WHERE lower(username)=lower($1) ORDER BY last_seen_ts DESC LIMIT 1
                """,
                username
            )
            result = row["user_id"] if row else None
            print(f"📋 Результат поиска username='{username}': user_id={result}")
            return result

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

    async def get_top(self, chat_id: int, limit: int = 10, order: str = "ASC"):
        """Получить топ пользователей по времени ответа
        
        Args:
            chat_id: ID чата
            limit: Количество записей
            order: "ASC" для быстрых (по возрастанию), "DESC" для медленных (по убыванию)
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
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
                ORDER BY avg_sec {order}
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
                WHERE chat_id=$1 AND close_ts IS NULL
                """,
                chat_id
            )
            return [(r[0], r[1], r[2] if r[2] is not None else None) for r in rows]



    async def save_activation_code(self, code: str, expires_at: int, created_by: int):
        """Сохраняет код активации"""
        now = int(datetime.utcnow().timestamp())
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO activation_codes(code, expires_at, created_by, created_at)
                VALUES($1, $2, $3, $4)
                """,
                code, expires_at, created_by, now
            )

    async def get_activation_code(self, code: str):
        """Получает информацию о коде активации"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT code, expires_at, created_by, created_at
                FROM activation_codes
                WHERE code = $1 AND expires_at > $2
                """,
                code, int(datetime.utcnow().timestamp())
            )
            if row:
                return {
                    'code': row['code'],
                    'expires_at': row['expires_at'],
                    'created_by': row['created_by'],
                    'created_at': row['created_at']
                }
            return None

    async def delete_activation_code(self, code: str):
        """Удаляет использованный код активации"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM activation_codes
                WHERE code = $1
                """,
                code
            )

    async def activate_chat(self, chat_id: int, chat_name: str, activation_code: str, activated_by: int):
        """Активирует чат"""
        now = int(datetime.utcnow().timestamp())
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO activated_chats(chat_id, chat_name, activated_by, activated_at, activation_code)
                VALUES($1, $2, $3, $4, $5)
                ON CONFLICT (chat_id) DO UPDATE SET
                    chat_name = EXCLUDED.chat_name,
                    activated_by = EXCLUDED.activated_by,
                    activated_at = EXCLUDED.activated_at,
                    activation_code = EXCLUDED.activation_code
                """,
                chat_id, chat_name, activated_by, now, activation_code
            )

    async def is_chat_activated(self, chat_id: int) -> bool:
        """Проверяет, активирован ли чат"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT chat_id FROM activated_chats
                WHERE chat_id = $1
                """,
                chat_id
            )
            return row is not None

    async def get_activated_chats(self):
        """Получает список всех активированных чатов"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT chat_id, chat_name, activated_by, activated_at
                FROM activated_chats
                ORDER BY activated_at DESC
                """
            )
            return [(row['chat_id'], row['chat_name'], row['activated_by'], row['activated_at']) for row in rows]

    async def deactivate_chat(self, chat_id: int) -> bool:
        """Деактивирует чат"""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM activated_chats
                WHERE chat_id = $1
                """,
                chat_id
            )
            return result != "DELETE 0"

    async def close(self):
        if self.pool is not None:
            await self.pool.close()

    async def bulk_add_chat_members(self, chat_id: int, members_data: List[Tuple[int, str, str, str]]):
        """Массово добавляет участников чата в базу данных"""
        if not members_data:
            return
        
        now = int(datetime.utcnow().timestamp())
        async with self.pool.acquire() as conn:
            # Подготавливаем данные для batch insert
            values = []
            for user_id, username, first_name, last_name in members_data:
                values.append(f"({user_id}, '{username or ''}', '{first_name or ''}', '{last_name or ''}', {now})")
            
            if values:
                query = f"""
                INSERT INTO users(user_id, username, first_name, last_name, last_seen_ts)
                VALUES {','.join(values)}
                ON CONFLICT (user_id) DO UPDATE SET
                    username=EXCLUDED.username,
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    last_seen_ts=EXCLUDED.last_seen_ts
                """
                await conn.execute(query)


