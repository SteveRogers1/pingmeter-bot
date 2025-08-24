import os
import asyncpg
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any
import logging
import hashlib
import time
from functools import lru_cache

class Database:
    def __init__(self, dsn: Optional[str] = None):
        self._dsn = dsn or os.getenv("DATABASE_URL")
        self.pool: Optional[asyncpg.Pool] = None
        self._prepared_statements: Dict[str, str] = {}
        
        if not self._dsn:
            raise RuntimeError("DATABASE_URL не установлен")
        
        # Валидация DSN
        if not self._dsn.startswith(('postgresql://', 'postgres://')):
            raise RuntimeError("Неверный формат DATABASE_URL")

    async def initialize(self):
        """Инициализация базы данных с оптимизациями"""
        try:
            logging.info("🔌 Подключение к базе данных...")
            
            # Оптимизированные настройки пула соединений
            self.pool = await asyncpg.create_pool(
                self._dsn, 
                min_size=2,  # Увеличиваем минимальный размер пула
                max_size=10,  # Увеличиваем максимальный размер пула
                command_timeout=60,  # Увеличиваем таймаут
                server_settings={
                    'application_name': 'pingmeter_bot',
                    'jit': 'off',  # Отключаем JIT для простых запросов
                    'random_page_cost': '1.1',  # Оптимизация для SSD
                    'effective_cache_size': '256MB'  # Размер кэша
                }
            )
            
            logging.info("✅ Подключение к базе данных установлено")
            
            # Создаем таблицы и индексы
            await self._create_tables()
            await self._create_indexes()
            await self._prepare_statements()
            
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к базе данных: {e}")
            raise

    async def _create_tables(self):
        """Создание таблиц с оптимизированной структурой"""
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
                    last_seen_ts BIGINT NOT NULL,
                    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
                );
                """)
            else:
                # Миграция существующей таблицы users
                await self._migrate_users_table(conn)
            
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
                    reaction_emoji TEXT,
                    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
                );
                """)
            else:
                # Миграция существующей таблицы pings
                await self._migrate_pings_table(conn)
            
            if 'activation_codes' not in existing_table_names:
                await conn.execute("""
                CREATE TABLE activation_codes (
                    id SERIAL PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    expires_at BIGINT NOT NULL,
                    created_by BIGINT NOT NULL,
                    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
                    used_at BIGINT,
                    used_by BIGINT
                );
                """)
            else:
                # Миграция существующей таблицы activation_codes
                await self._migrate_activation_codes_table(conn)
            
            if 'activated_chats' not in existing_table_names:
                await conn.execute("""
                CREATE TABLE activated_chats (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT UNIQUE NOT NULL,
                    chat_name TEXT NOT NULL,
                    activated_by BIGINT NOT NULL,
                    activated_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
                    last_activity BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
                );
                """)
            else:
                # Миграция существующей таблицы activated_chats
                await self._migrate_activated_chats_table(conn)

    async def _create_indexes(self):
        """Создание оптимизированных индексов"""
        async with self.pool.acquire() as conn:
            # Индексы для таблицы users
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_username ON users(username) WHERE username IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen_ts DESC);
            """)
            
            # Индексы для таблицы pings
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pings_chat_target ON pings(chat_id, target_user_id);
                CREATE INDEX IF NOT EXISTS idx_pings_target_open ON pings(target_user_id) WHERE close_ts IS NULL;
                CREATE INDEX IF NOT EXISTS idx_pings_chat_open ON pings(chat_id) WHERE close_ts IS NULL;
                CREATE INDEX IF NOT EXISTS idx_pings_ping_ts ON pings(ping_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_pings_close_ts ON pings(close_ts DESC) WHERE close_ts IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_pings_chat_ping_ts ON pings(chat_id, ping_ts DESC);
            """)
            
            # Индексы для таблицы activation_codes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_activation_codes_expires ON activation_codes(expires_at);
                CREATE INDEX IF NOT EXISTS idx_activation_codes_used ON activation_codes(used_at) WHERE used_at IS NOT NULL;
            """)
            
            # Индексы для таблицы activated_chats
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_activated_chats_last_activity ON activated_chats(last_activity DESC);
            """)

    async def _prepare_statements(self):
        """Подготовка часто используемых запросов"""
        async with self.pool.acquire() as conn:
            # Подготавливаем часто используемые запросы
            self._prepared_statements = {
                'get_user': await conn.prepare("""
                    SELECT user_id, username, first_name, last_name, last_seen_ts 
                    FROM users WHERE user_id = $1
                """),
                'get_user_by_username': await conn.prepare("""
                    SELECT user_id FROM users WHERE lower(username) = lower($1) 
                    ORDER BY last_seen_ts DESC LIMIT 1
                """),
                'get_open_pings': await conn.prepare("""
                    SELECT target_user_id, ping_ts, source_message_id 
                    FROM pings 
                    WHERE chat_id = $1 AND close_ts IS NULL 
                    ORDER BY ping_ts ASC
                """),
                'is_chat_activated': await conn.prepare("""
                    SELECT chat_id FROM activated_chats WHERE chat_id = $1
                """),
                'get_activation_code': await conn.prepare("""
                    SELECT code, expires_at, created_by, created_at 
                    FROM activation_codes 
                    WHERE code = $1 AND expires_at > $2 AND used_at IS NULL
                """)
            }

    async def _migrate_pings_table(self, conn):
        """Миграция таблицы pings"""
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
        if 'created_at' not in column_names:
            await conn.execute("ALTER TABLE pings ADD COLUMN created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW());")

    async def _migrate_activation_codes_table(self, conn):
        """Миграция таблицы activation_codes"""
        columns = await conn.fetch("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'activation_codes' AND table_schema = 'public'
        """)
        column_names = [row['column_name'] for row in columns]
        
        # Добавляем недостающие колонки если их нет
        if 'used_at' not in column_names:
            await conn.execute("ALTER TABLE activation_codes ADD COLUMN used_at BIGINT;")
        if 'used_by' not in column_names:
            await conn.execute("ALTER TABLE activation_codes ADD COLUMN used_by BIGINT;")
        if 'created_at' not in column_names:
            await conn.execute("ALTER TABLE activation_codes ADD COLUMN created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW());")

    async def _migrate_activated_chats_table(self, conn):
        """Миграция таблицы activated_chats"""
        columns = await conn.fetch("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'activated_chats' AND table_schema = 'public'
        """)
        column_names = [row['column_name'] for row in columns]
        
        # Добавляем недостающие колонки если их нет
        if 'last_activity' not in column_names:
            await conn.execute("ALTER TABLE activated_chats ADD COLUMN last_activity BIGINT DEFAULT EXTRACT(EPOCH FROM NOW());")
        
        # Если есть старая колонка activation_code, удаляем её
        if 'activation_code' in column_names:
            await conn.execute("ALTER TABLE activated_chats DROP COLUMN activation_code;")

    async def _migrate_users_table(self, conn):
        """Миграция таблицы users"""
        columns = await conn.fetch("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = 'users' AND table_schema = 'public'
        """)
        column_names = [row['column_name'] for row in columns]
        
        # Добавляем недостающие колонки если их нет
        if 'created_at' not in column_names:
            await conn.execute("ALTER TABLE users ADD COLUMN created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW());")

    @lru_cache(maxsize=1000)
    def _hash_username(self, username: str) -> int:
        """Хеширование username для временных пользователей"""
        return int(hashlib.md5(username.encode()).hexdigest()[:8], 16) % (2**31)

    async def create_temp_user_by_username(self, username: str) -> int:
        """Создает временного пользователя по username для пингов"""
        now = int(time.time())
        # Генерируем временный user_id (отрицательный, чтобы не конфликтовать с реальными)
        temp_user_id = -self._hash_username(username)
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users(user_id, username, first_name, last_name, last_seen_ts)
                VALUES($1, $2, $3, $4, $5)
                ON CONFLICT (user_id) DO UPDATE SET
                    username=EXCLUDED.username,
                    last_seen_ts=EXCLUDED.last_seen_ts
                """,
                temp_user_id, username, None, None, now
            )
        return temp_user_id





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
            print(f"📝 Создаём пинг: chat_id={chat_id}, target_user_id={target_user_id}, reason={ping_reason}")
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
                print(f"⚠️ Уже есть открытый пинг для target_user_id={target_user_id}, не создаём новый")
                return  # Уже есть открытый пинг, не создаём новый
            print(f"✅ Создаём новый пинг для target_user_id={target_user_id}")
            await conn.execute(
                """
                INSERT INTO pings(chat_id, source_message_id, source_user_id, target_user_id, ping_reason, ping_ts)
                VALUES($1, $2, $3, $4, $5, $6)
                """,
                chat_id, source_message_id, source_user_id, target_user_id, ping_reason, ping_ts
            )

    async def close_oldest_open_ping_by_message(self, chat_id: int, target_user_id: int, close_message_id: int, close_ts: int) -> Optional[int]:
        async with self.pool.acquire() as conn:
            print(f"🔍 Ищем открытый пинг для закрытия: chat_id={chat_id}, target_user_id={target_user_id}")
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
                print(f"❌ Не найден открытый пинг для закрытия: chat_id={chat_id}, target_user_id={target_user_id}")
                return None
            ping_id = row["id"]
            print(f"✅ Закрываем пинг: ping_id={ping_id}, close_ts={close_ts}, close_message_id={close_message_id}")
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
                WHERE code = $1 AND expires_at > $2 AND used_at IS NULL
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
            # Активируем чат
            await conn.execute(
                """
                INSERT INTO activated_chats(chat_id, chat_name, activated_by, activated_at)
                VALUES($1, $2, $3, $4)
                ON CONFLICT (chat_id) DO UPDATE SET
                    chat_name = EXCLUDED.chat_name,
                    activated_by = EXCLUDED.activated_by,
                    activated_at = EXCLUDED.activated_at
                """,
                chat_id, chat_name, activated_by, now
            )
            
            # Отмечаем код как использованный
            await conn.execute(
                """
                UPDATE activation_codes 
                SET used_at = $1, used_by = $2 
                WHERE code = $3
                """,
                now, activated_by, activation_code
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
        """Деактивирует чат и очищает все связанные данные"""
        async with self.pool.acquire() as conn:
            # Проверяем, существует ли чат
            chat_exists = await conn.fetchrow(
                "SELECT chat_id FROM activated_chats WHERE chat_id = $1",
                chat_id
            )
            
            if not chat_exists:
                return False
            
            # Начинаем транзакцию для атомарности операций
            async with conn.transaction():
                # 1. Удаляем все пинги этого чата
                pings_deleted = await conn.execute(
                    "DELETE FROM pings WHERE chat_id = $1",
                    chat_id
                )
                print(f"🗑️ Удалено пингов для чата {chat_id}: {pings_deleted}")
                
                # 2. Находим пользователей, которые участвовали только в этом чате
                users_to_delete = await conn.fetch(
                    """
                    SELECT DISTINCT u.user_id 
                    FROM users u
                    LEFT JOIN pings p ON u.user_id = p.source_user_id OR u.user_id = p.target_user_id
                    WHERE u.user_id IN (
                        SELECT DISTINCT source_user_id FROM pings WHERE chat_id = $1
                        UNION
                        SELECT DISTINCT target_user_id FROM pings WHERE chat_id = $1
                    )
                    AND p.id IS NULL
                    """,
                    chat_id
                )
                
                # 3. Удаляем пользователей, которые больше не участвуют ни в каких пингах
                if users_to_delete:
                    user_ids = [row['user_id'] for row in users_to_delete]
                    users_deleted = await conn.execute(
                        "DELETE FROM users WHERE user_id = ANY($1)",
                        user_ids
                    )
                    print(f"🗑️ Удалено пользователей для чата {chat_id}: {users_deleted}")
                
                # 4. Удаляем запись об активации чата
                result = await conn.execute(
                    "DELETE FROM activated_chats WHERE chat_id = $1",
                    chat_id
                )
                
                print(f"✅ Чат {chat_id} деактивирован и все данные очищены")
                return result != "DELETE 0"

    async def close(self):
        if self.pool is not None:
            await self.pool.close()



    async def update_temp_user(self, username: str, real_user_id: int, first_name: str = None, last_name: str = None):
        """Обновляет временного пользователя реальными данными"""
        now = int(datetime.utcnow().timestamp())
        async with self.pool.acquire() as conn:
            # Находим временного пользователя по username
            temp_user = await conn.fetchrow(
                "SELECT user_id FROM users WHERE username = $1 AND user_id < 0",
                username
            )
            
            if temp_user:
                # Обновляем временного пользователя реальными данными
                await conn.execute(
                    """
                    UPDATE users 
                    SET user_id = $1, first_name = $2, last_name = $3, last_seen_ts = $4
                    WHERE user_id = $5
                    """,
                    real_user_id, first_name, last_name, now, temp_user['user_id']
                )
                
                # Обновляем все пинги с временным user_id на реальный
                await conn.execute(
                    """
                    UPDATE pings 
                    SET target_user_id = $1 
                    WHERE target_user_id = $2
                    """,
                    real_user_id, temp_user['user_id']
                )
                
                logging.info(f"Обновлен временный пользователь @{username}: {temp_user['user_id']} -> {real_user_id}")


