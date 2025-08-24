import os
import logging
import secrets
import string
import re
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Set
from functools import lru_cache

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest


from app.db import Database

router = Router()

# Кэш для rate limiting
_rate_limit_cache: Dict[int, float] = {}
_rate_limit_window = 1.0  # секунды между запросами

# Кэш для проверки прав администратора
_admin_cache: Dict[int, Dict[int, float]] = {}  # chat_id -> {user_id: timestamp}
_admin_cache_ttl = 300  # 5 минут

# Состояния для FSM
class ChatActivation(StatesGroup):
    waiting_for_chat_name = State()

def format_duration(seconds: int) -> str:
    """Форматирует время в человекочитаемый вид"""
    if seconds < 60:
        return f"{seconds}с"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}м {secs}с"
    elif seconds < 86400:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}ч {minutes}м"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}д {hours}ч"

def escape_username(username: Optional[str], user_id: int) -> str:
    """Экранирует специальные символы в username для Markdown"""
    if not username:
        return f'user_{user_id}'
    # Экранируем специальные символы Markdown
    escaped = re.sub(r'([_*[\]()~`>#+=|{}.!-])', r'\\\1', username)
    return escaped

def format_user_display(username: Optional[str], user_id: int) -> str:
    """Форматирует отображение пользователя с правильным префиксом @"""
    if not username:
        return f'user_{user_id}'  # Без @ для user_id
    return f'@{escape_username(username, user_id)}'  # С @ для username

def create_message_link(chat_id: int, chat_username: Optional[str], message_id: int) -> str:
    """Создает ссылку на сообщение для публичных и приватных чатов"""
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"
    else:
        # Для приватных чатов
        chat_id_str = str(chat_id)
        if chat_id_str.startswith('-100'):
            chat_id_short = chat_id_str[4:]
        else:
            chat_id_short = chat_id_str
        return f"https://t.me/c/{chat_id_short}/{message_id}"

def is_main_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь главным администратором"""
    main_admin_id = os.getenv("MAIN_ADMIN_ID")
    if not main_admin_id:
        return False
    return user_id == int(main_admin_id)

def generate_activation_code() -> str:
    """Генерирует одноразовый код активации"""
    return ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(8))

def rate_limit(user_id: int) -> bool:
    """Rate limiting для пользователей"""
    now = time.time()
    if user_id in _rate_limit_cache:
        if now - _rate_limit_cache[user_id] < _rate_limit_window:
            return False
    _rate_limit_cache[user_id] = now
    return True

def clean_rate_limit_cache():
    """Очистка устаревших записей в кэше rate limiting"""
    now = time.time()
    expired = [user_id for user_id, timestamp in _rate_limit_cache.items() 
               if now - timestamp > _rate_limit_window * 2]
    for user_id in expired:
        del _rate_limit_cache[user_id]

def clean_admin_cache():
    """Очистка устаревших записей в кэше администраторов"""
    now = time.time()
    for chat_id in list(_admin_cache.keys()):
        expired_users = [user_id for user_id, timestamp in _admin_cache[chat_id].items() 
                        if now - timestamp > _admin_cache_ttl]
        for user_id in expired_users:
            del _admin_cache[chat_id][user_id]
        if not _admin_cache[chat_id]:
            del _admin_cache[chat_id]

def validate_username(username: str) -> bool:
    """Валидация username"""
    if not username:
        return False
    # Telegram username format: 5-32 characters, letters, digits, underscores
    pattern = r'^[a-zA-Z0-9_]{5,32}$'
    return bool(re.match(pattern, username))

def validate_chat_name(chat_name: str) -> bool:
    """Валидация названия чата"""
    if not chat_name or len(chat_name) < 2 or len(chat_name) > 50:
        return False
    # Проверяем на недопустимые символы
    invalid_chars = ['<', '>', '&', '"', "'", '\\', '/']
    return not any(char in chat_name for char in invalid_chars)

@lru_cache(maxsize=1000)
def escape_username(username: Optional[str], user_id: int) -> str:
    """Экранирует username для Markdown с кэшированием"""
    if not username:
        return f"user_{user_id}"
    
    # Экранируем специальные символы Markdown
    escaped = re.sub(r'([_*[\]()~`>#+=|{}.!-])', r'\\\1', username)
    return escaped

def format_user_display(username: Optional[str], user_id: int) -> str:
    """Форматирует отображение пользователя"""
    if not username:
        return f"user_{user_id}"
    return f"@{username}"

def format_duration(seconds: int) -> str:
    """Форматирует длительность в читаемый вид"""
    if seconds < 60:
        return f"{seconds}с"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}м {secs}с"
    elif seconds < 86400:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}ч {minutes}м"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}д {hours}ч"

def create_message_link(chat_id: int, chat_username: Optional[str], message_id: int) -> str:
    """Создает ссылку на сообщение"""
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"
    else:
        # Для приватных чатов используем специальный формат
        return f"https://t.me/c/{str(chat_id)[4:]}/{message_id}"

@lru_cache(maxsize=100)
def get_bot_commands(bot_username: str = "pingmeter_bot") -> Dict[str, str]:
    """Возвращает полные команды с username бота с кэшированием"""
    bot_mention = f"@{bot_username}"
    return {
        "start": f"/start{bot_mention}",
        "generate_code": f"/generate_code{bot_mention}",
        "activate": f"/activate{bot_mention}",
        "name": f"/name{bot_mention}",
        "top_fast": f"/top_fast{bot_mention}",
        "top_slow": f"/top_slow{bot_mention}",
        "me": f"/me{bot_mention}",
        "help": f"/help{bot_mention}",
        "clear_db": f"/clear_db{bot_mention}",
        "test": f"/test{bot_mention}",
        "list_activated": f"/list_activated{bot_mention}",
        "deactivate_chat": f"/deactivate_chat{bot_mention}",
        "debug_chat_id": f"/debug_chat_id{bot_mention}",
        "debug_open_pings": f"/debug_open_pings{bot_mention}"
    }

async def check_admin_rights(message: Message) -> bool:
    """Проверяет права администратора с кэшированием"""
    if not message.from_user:
        return False
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем кэш
    now = time.time()
    if chat_id in _admin_cache and user_id in _admin_cache[chat_id]:
        if now - _admin_cache[chat_id][user_id] < _admin_cache_ttl:
            return True
    
    try:
        # Получаем информацию о чате
        chat_member = await message.bot.get_chat_member(chat_id, user_id)
        is_admin = chat_member.status in ['creator', 'administrator']
        
        # Кэшируем результат
        if chat_id not in _admin_cache:
            _admin_cache[chat_id] = {}
        _admin_cache[chat_id][user_id] = now
        
        return is_admin
    except Exception as e:
        logging.warning(f"Ошибка проверки прав администратора: {e}")
        return False



@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Обработчик команды /start"""
    # Получаем команды с username бота
    commands = get_bot_commands()
    
    if message.chat.type == "private":
        # ЛС с ботом
        if is_main_admin(message.from_user.id):
            await message.reply(
                f"👋 Привет! Я главный администратор бота.\n\n"
                f"📋 Доступные команды:\n"
                f"• {commands['generate_code']} - Создать код активации для чата\n"
                f"• {commands['list_activated']} - Список активированных чатов\n"
                f"• {commands['deactivate_chat']} chat_id - Деактивировать чат\n"
                f"• {commands['clear_db']} - Очистить базу данных\n\n"
                f"🔧 Для активации чата:\n"
                f"1. Используйте {commands['generate_code']}\n"
                f"2. Передайте код администратору чата\n"
                f"3. В чате выполните {commands['activate']} код"
            )
        else:
            await message.reply(
                f"👋 Привет! Я бот для отслеживания времени ответа на пинги.\n\n"
                f"🔧 Для активации в чате:\n"
                f"1. Попросите главного администратора создать код\n"
                f"2. В чате выполните {commands['activate']} код\n\n"
                f"📋 Команды в активированном чате:\n"
                f"• {commands['top_fast']} - Топ быстрых ответов\n"
                f"• {commands['top_slow']} - Топ медленных ответов\n"
                f"• {commands['me']} - Ваша статистика\n"
                f"• {commands['help']} - Справка"
            )
    else:
        # Групповой чат
        bot = message.bot
        db: Database = getattr(bot, "db")
        
        # Проверяем, активирован ли чат
        is_activated = await db.is_chat_activated(message.chat.id)
        if not is_activated:
            await message.reply(
                f"❌ Этот чат не активирован для использования бота.\n\n"
                f"🔧 Для активации:\n"
                f"1. Попросите главного администратора создать код\n"
                f"2. Выполните команду {commands['activate']} код\n\n"
                f"💡 Код можно получить в личных сообщениях с ботом."
            )
            return
        
        if not await check_admin_rights(message):
            await message.reply("❌ Только администраторы могут использовать команды бота.")
            return
        
        await message.reply(
            f"👋 Привет! Я бот для отслеживания времени ответа на пинги.\n\n"
            f"📋 Доступные команды:\n"
            f"• {commands['top_fast']} - Топ быстрых ответов\n"
            f"• {commands['top_slow']} - Топ медленных ответов\n"
            f"• {commands['me']} - Моя статистика\n"
            f"• {commands['help']} - Справка\n"
            f"• {commands['debug_chat_id']} - Показать ID чата\n"
            f"• {commands['debug_open_pings']} - Открытые пинги\n\n"
            f"🔒 Только администраторы могут использовать команды."
        )

@router.message(Command("generate_code"))
async def cmd_generate_code(message: Message) -> None:
    """Генерирует код активации для чата (только в ЛС)"""
    if message.chat.type != "private":
        await message.reply("❌ Эта команда доступна только в личных сообщениях с ботом.")
        return
    
    if not is_main_admin(message.from_user.id):
        await message.reply("❌ Только главный администратор может создавать коды активации.")
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    # Генерируем код
    activation_code = generate_activation_code()
    expires_at = int((datetime.now() + timedelta(hours=24)).timestamp())
    
    # Сохраняем код в базе
    await db.save_activation_code(activation_code, expires_at, message.from_user.id)
    
    await message.reply(
        f"🔑 **Код активации создан!**\n\n"
        f"**Код:** `{activation_code}`\n"
        f"**Действует до:** {datetime.fromtimestamp(expires_at).strftime('%d.%m.%Y %H:%M')}\n\n"
        f"📋 **Инструкция для активации:**\n"
        f"1. Добавьте бота в чат\n"
        f"2. Сделайте бота администратором\n"
        f"3. Выполните команду: /activate {activation_code}\n"
        f"4. Укажите название чата\n\n"
        f"⚠️ **Код одноразовый и действителен 24 часа!**",
        parse_mode="Markdown"
    )

@router.message(Command("activate"))
async def cmd_activate(message: Message, state: FSMContext) -> None:
    """Активирует чат по коду"""
    if message.chat.type == "private":
        await message.reply("❌ Эта команда должна выполняться в чате, который нужно активировать.")
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут активировать бота в чате.")
        return
    
    # Проверяем, не активирован ли уже чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if is_activated:
        await message.reply("✅ Этот чат уже активирован!")
        return
    
    # Получаем код из команды
    args = message.text.split()
    if len(args) != 2:
        await message.reply(
            "❌ Неверный формат команды.\n\n"
            "📋 Использование:\n"
            "/activate <код>\n\n"
            "💡 Пример: /activate ABC12345"
        )
        return
    
    activation_code = args[1].upper()
    
    # Проверяем код
    code_info = await db.get_activation_code(activation_code)
    if not code_info:
        await message.reply("❌ Неверный код активации или код истёк.")
        return
    
    # Сохраняем информацию о чате для активации
    await state.update_data(
        activation_code=activation_code,
        chat_id=message.chat.id,
        chat_title=message.chat.title or "Личные сообщения"
    )
    
    # Запрашиваем название чата
    await state.set_state(ChatActivation.waiting_for_chat_name)
    await message.reply(
        f"✅ Код `{activation_code}` действителен!\n\n"
        f"📝 Теперь используйте команду `/name название` для установки названия чата.\n\n"
        f"💡 Примеры:\n"
        f"• `/name Рабочий чат`\n"
        f"• `/name Команда разработки`\n"
        f"• `/name Общий чат`"
    )

@router.message(Command("name"))
async def cmd_name(message: Message, state: FSMContext) -> None:
    """Устанавливает название чата при активации"""
    if message.chat.type == "private":
        await message.reply("❌ Эта команда должна выполняться в чате.")
        return
    
    # Проверяем, что мы в состоянии ожидания названия
    current_state = await state.get_state()
    if current_state != ChatActivation.waiting_for_chat_name.state:
        await message.reply("❌ Сначала используйте команду /activate код")
        return
    
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.reply("❌ Используйте: /name название_чата")
        return
    
    chat_name = args[1].strip()
    if not validate_chat_name(chat_name):
        await message.reply("❌ Название чата должно быть от 2 до 50 символов и не содержать недопустимые символы.")
        return
    
    data = await state.get_data()
    activation_code = data.get("activation_code")
    chat_id = data.get("chat_id")
    
    if not activation_code or not chat_id:
        await message.reply("❌ Ошибка активации. Попробуйте снова.")
        await state.clear()
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    # Активируем чат
    await db.activate_chat(chat_id, chat_name, activation_code, message.from_user.id)
    
    await state.clear()
    
    # Экранируем специальные символы для Markdown
    escaped_chat_name = chat_name.replace('*', '\\*').replace('_', '\\_').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
    
    # Получаем команды для отображения
    commands = get_bot_commands()
    
    await message.reply(
        f"🎉 **Чат успешно активирован!**\n\n"
        f"📋 **Информация:**\n"
        f"• Название: {escaped_chat_name}\n"
        f"• Chat ID: `{chat_id}`\n"
        f"• Активировал: @{message.from_user.username or message.from_user.first_name}\n\n"
        f"✅ Теперь бот готов к работе!\n\n"
        f"📋 **Доступные команды:**\n"
        f"• {commands['top_fast']} - Топ быстрых ответов\n"
        f"• {commands['top_slow']} - Топ медленных ответов\n"
        f"• {commands['me']} - Ваша статистика\n"
        f"• {commands['help']} - Справка\n\n"
        f"🔒 **Только администраторы могут использовать команды.**"
    )

@router.message(ChatActivation.waiting_for_chat_name)
async def process_chat_name(message: Message, state: FSMContext) -> None:
    """Обрабатывает любые сообщения в состоянии ожидания названия (кроме команды /name)"""
    # Если это не команда /name, напоминаем о правильном использовании
    if not message.text or not message.text.startswith('/name'):
        # Получаем команды для отображения
        commands = get_bot_commands()
        
        await message.reply(
            f"❌ Используйте команду {commands['name']} название_чата для установки названия.\n\n"
            f"💡 Примеры:\n"
            f"• {commands['name']} Рабочий чат\n"
            f"• {commands['name']} Команда разработки\n"
            f"• {commands['name']} Общий чат"
        )
        return

@router.message(Command("list_activated"))
async def cmd_list_activated(message: Message) -> None:
    """Показывает список активированных чатов (только в ЛС)"""
    if message.chat.type != "private":
        await message.reply("❌ Эта команда доступна только в личных сообщениях с ботом.")
        return
    
    if not is_main_admin(message.from_user.id):
        await message.reply("❌ Только главный администратор может просматривать список чатов.")
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    activated_chats = await db.get_activated_chats()
    
    if not activated_chats:
        await message.reply("📋 Нет активированных чатов.")
        return
    
    result = "📋 **Активированные чаты:**\n\n"
    
    for chat_id, chat_name, activated_by, activated_at in activated_chats:
        activated_date = datetime.fromtimestamp(activated_at).strftime('%d.%m.%Y %H:%M')
        # Экранируем специальные символы для Markdown
        escaped_chat_name = chat_name.replace('*', '\\*').replace('_', '\\_').replace('[', '\\[').replace(']', '\\]').replace('(', '\\(').replace(')', '\\)').replace('~', '\\~').replace('`', '\\`').replace('>', '\\>').replace('#', '\\#').replace('+', '\\+').replace('-', '\\-').replace('=', '\\=').replace('|', '\\|').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.').replace('!', '\\!')
        result += f"• **{escaped_chat_name}**\n"
        result += f"  ID: `{chat_id}`\n"
        result += f"  Активировал: {activated_by}\n"
        result += f"  Дата: {activated_date}\n\n"
    
    await message.reply(result, parse_mode="Markdown")

@router.message(Command("deactivate_chat"))
async def cmd_deactivate_chat(message: Message) -> None:
    """Деактивирует чат (только в ЛС)"""
    if message.chat.type != "private":
        await message.reply("❌ Эта команда доступна только в личных сообщениях с ботом.")
        return
    
    if not is_main_admin(message.from_user.id):
        await message.reply("❌ Только главный администратор может деактивировать чаты.")
        return
    
    args = message.text.split()
    if len(args) != 2:
        await message.reply(
            "❌ Неверный формат команды.\n\n"
            "📋 Использование:\n"
            "/deactivate_chat chat_id\n\n"
            "💡 Chat ID можно получить командой /list_activated"
        )
        return
    
    try:
        chat_id = int(args[1])
        # Если пользователь ввел положительный ID, конвертируем в отрицательный
        if chat_id > 0:
            chat_id = -chat_id
    except ValueError:
        await message.reply("❌ Неверный формат Chat ID. Должно быть число.")
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    # Деактивируем чат
    success = await db.deactivate_chat(chat_id)
    
    if success:
        await message.reply(
            f"✅ Чат `{chat_id}` успешно деактивирован!\n\n"
            f"🗑️ **Все данные чата очищены:**\n"
            f"• Удалены все пинги\n"
            f"• Удалены пользователи, участвовавшие только в этом чате\n"
            f"• Удалена запись об активации"
        )
    else:
        await message.reply(f"❌ Чат `{chat_id}` не найден или уже деактивирован.\n\n💡 Убедитесь, что используете правильный Chat ID из списка активированных чатов.")

@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Обработчик команды /help"""
    # Получаем команды с username бота
    commands = get_bot_commands()
    
    if message.chat.type == "private":
        if is_main_admin(message.from_user.id):
            help_text = f"""
📋 **Справка для главного администратора:**

**Команды в ЛС:**
• {commands['generate_code']} - Создать код активации для чата
• {commands['list_activated']} - Список активированных чатов
• {commands['deactivate_chat']} chat_id - Деактивировать чат

**Процесс активации:**
1. Создайте код через {commands['generate_code']}
2. Передайте код администратору чата
3. В чате выполните {commands['activate']} код
4. Укажите название чата через {commands['name']} название

**Команды в активированных чатах:**
• {commands['top_fast']} - Топ быстрых ответов
• {commands['top_slow']} - Топ медленных ответов
• {commands['me']} - Ваша личная статистика
• {commands['help']} - Показать эту справку
• {commands['debug_chat_id']} - Показать ID чата
• {commands['debug_open_pings']} - Показать открытые пинги
"""
        else:
            help_text = f"""
📋 **Справка по использованию бота:**

**Для активации в чате:**
1. Попросите главного администратора создать код
2. В чате выполните {commands['activate']} код
3. Укажите название чата через {commands['name']} название

**Команды в активированном чате:**
• {commands['top_fast']} - Топ быстрых ответов
• {commands['top_slow']} - Топ медленных ответов
• {commands['me']} - Ваша личная статистика
• {commands['help']} - Показать эту справку

**Как работает бот:**
• Отслеживает пинги через @username или text_mention
• Закрывает пинг при любом сообщении или реакции
• Показывает статистику по времени ответа
• Работает только в активированных чатах

**💡 Как работают пинги:**
• При упоминании @username создается временный пользователь
• Когда пользователь напишет сообщение, временный профиль обновляется реальными данными
• Пинги работают сразу для всех упомянутых пользователей
"""
    else:
        # Проверяем, активирован ли чат
        bot = message.bot
        db: Database = getattr(bot, "db")
        
        is_activated = await db.is_chat_activated(message.chat.id)
        if not is_activated:
            await message.reply(f"❌ Этот чат не активирован. Используйте {commands['activate']} код для активации.")
            return
        
        if not await check_admin_rights(message):
            await message.reply("❌ Только администраторы могут использовать команды бота.")
            return
        
        help_text = f"""
📋 **Справка по командам:**

**Основные команды:**
• {commands['top_fast']} - Топ быстрых ответов
• {commands['top_slow']} - Топ медленных ответов
• {commands['me']} - Показать вашу личную статистику
• {commands['help']} - Показать эту справку

**Отладочные команды:**
• {commands['debug_chat_id']} - Показать ID текущего чата
• {commands['debug_open_pings']} - Показать все открытые пинги

**Как работает бот:**
• Отслеживает пинги через @username или text_mention
• Закрывает пинг при любом сообщении или реакции от пользователя
• Показывает статистику по времени ответа
• Работает только в активированных чатах

**💡 Как работают пинги:**
• При упоминании @username создается временный пользователь
• Когда пользователь напишет сообщение, временный профиль обновляется реальными данными
• Пинги работают сразу для всех упомянутых пользователей

**Требования:**
• Чат должен быть активирован
• Пользователь должен быть администратором
• Бот должен быть администратором чата
"""
    
    await message.reply(help_text, parse_mode="HTML")

@router.message(Command("debug_chat_id"))
async def cmd_debug_chat_id(message: Message) -> None:
    """Показать ID чата для добавления в whitelist"""
    chat_type = message.chat.type
    chat_id = message.chat.id
    chat_title = message.chat.title or "Личные сообщения"
    
    debug_info = f"""
🔍 **Информация о чате:**

**Тип чата:** {chat_type}
**Название:** {chat_title}
**Chat ID:** `{chat_id}`

**Для деактивации:**
Используйте команду /deactivate_chat {chat_id} в ЛС с ботом
"""
    
    await message.reply(debug_info, parse_mode="Markdown")

@router.message(Command("test"))
async def cmd_test(message: Message) -> None:
    """Тестовая команда"""
    await message.reply("✅ Тестовая команда работает! Новый код загрузился.")

@router.message(Command("clear_db"))
async def cmd_clear_db(message: Message) -> None:
    """Принудительная очистка базы данных (только главный админ)"""
    if not is_main_admin(message.from_user.id):
        await message.reply("❌ Только главный администратор может очищать базу данных.")
        return
    
    await message.reply("🗑️ Начинаю очистку базы данных...")
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    try:
        async with db.pool.acquire() as conn:
            # Удаляем ВСЕ данные
            await conn.execute("DELETE FROM pings")
            await conn.execute("DELETE FROM users")
            await conn.execute("DELETE FROM activation_codes")
            await conn.execute("DELETE FROM activated_chats")
            
            # Сбрасываем счетчики
            await conn.execute("ALTER SEQUENCE IF EXISTS pings_id_seq RESTART WITH 1")
            await conn.execute("ALTER SEQUENCE IF EXISTS activation_codes_id_seq RESTART WITH 1")
            await conn.execute("ALTER SEQUENCE IF EXISTS activated_chats_id_seq RESTART WITH 1")
        
        await message.reply("✅ База данных полностью очищена! Все данные удалены.")
    except Exception as e:
        await message.reply(f"❌ Ошибка при очистке: {e}")

@router.message(Command("reset_db"))
async def cmd_reset_db(message: Message) -> None:
    """Принудительная очистка базы данных (только главный админ)"""
    if not is_main_admin(message.from_user.id):
        await message.reply("❌ Только главный администратор может очищать базу данных.")
        return
    
    await message.reply("🗑️ Начинаю очистку базы данных...")
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    try:
        async with db.pool.acquire() as conn:
            # Удаляем ВСЕ данные
            await conn.execute("DELETE FROM pings")
            await conn.execute("DELETE FROM users")
            await conn.execute("DELETE FROM activation_codes")
            await conn.execute("DELETE FROM activated_chats")
            
            # Сбрасываем счетчики
            await conn.execute("ALTER SEQUENCE IF EXISTS pings_id_seq RESTART WITH 1")
            await conn.execute("ALTER SEQUENCE IF EXISTS activation_codes_id_seq RESTART WITH 1")
            await conn.execute("ALTER SEQUENCE IF EXISTS activated_chats_id_seq RESTART WITH 1")
        
        await message.reply("✅ База данных полностью очищена! Все данные удалены.")
    except Exception as e:
        await message.reply(f"❌ Ошибка при очистке: {e}")

@router.message(Command("debug_open_pings"))
async def cmd_debug_open_pings(message: Message) -> None:
    """Показать все открытые пинги в чате"""
    # Проверяем, активирован ли чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if not is_activated:
        await message.reply("❌ Этот чат не активирован. Используйте /activate код для активации.")
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    open_pings = await db.get_open_pings(message.chat.id)
    
    if not open_pings:
        await message.reply("✅ Нет открытых пингов в этом чате.")
        return
    
    result = "⏰ **Открытые пинги:**\n\n"
    
    for user_id, ping_ts, source_message_id in open_pings:
        elapsed = int(datetime.now().timestamp()) - ping_ts
        elapsed_str = format_duration(elapsed)
        
        # Получаем информацию о пользователе
        user_info = await db.get_user_info(user_id)
        username = user_info.get('username', f'user_{user_id}') if user_info else f'user_{user_id}'
        
        # Создаём ссылку на исходное сообщение
        if source_message_id:
            message_link = create_message_link(message.chat.id, message.chat.username, source_message_id)
            link_text = f"[сообщение]({message_link})"
        else:
            link_text = "ID неизвестен"
        
        result += f"👤 **{format_user_display(username, user_id)}** - {elapsed_str} ({link_text})\n"
    
    await message.reply(result, parse_mode="Markdown", disable_web_page_preview=True)

@router.message(Command("top_fast"))
async def cmd_top_fast(message: Message) -> None:
    """Показать топ быстрых пользователей по времени ответа"""
    # Проверяем, активирован ли чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if not is_activated:
        await message.reply("❌ Этот чат не активирован. Используйте /activate код для активации.")
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    bot_id = getattr(bot, "bot_id", None)
    
    # Получаем топ быстрых пользователей
    top_users = await db.get_top(message.chat.id, limit=10, order="ASC")
    
    result = "⚡ **Топ 10 быстрых ответов:**\n\n"
    
    if not top_users:
        result += "📊 Пока нет статистики в этом чате.\n\n"
    else:
        for i, (user_id, n, avg_sec, username) in enumerate(top_users, 1):
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            if avg_sec is not None:
                avg_str = format_duration(int(avg_sec))
            else:
                avg_str = "N/A"
            
            # Экранируем специальные символы в username
            escaped_username = escape_username(username, user_id)
            
            result += f"{i}. **{format_user_display(username, user_id)}** - {avg_str} (n={n})\n"
    
    # Получаем открытые пинги
    open_pings = await db.get_open_pings(message.chat.id)
    if open_pings:
        result += "\n⏰ **Открытые пинги:**\n"
        for user_id, ping_ts, source_message_id in open_pings:
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            elapsed = int(datetime.now().timestamp()) - ping_ts
            elapsed_str = format_duration(elapsed)
            
            user_info = await db.get_user_info(user_id)
            username = user_info.get('username', f'user_{user_id}') if user_info else f'user_{user_id}'
            
            # Создаём ссылку на исходное сообщение
            if source_message_id:
                message_link = create_message_link(message.chat.id, message.chat.username, source_message_id)
                link_text = f"[вопрос]({message_link})"
            else:
                link_text = "ID неизвестен"
            
            result += f"👤 **{format_user_display(username, user_id)}** - {elapsed_str} ({link_text})\n"
    
    # Добавляем кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать всех (до 1000)", callback_data="top_all")],
        [InlineKeyboardButton(text="🐌 Топ медленных", callback_data="top_slow")]
    ])
    
    await message.reply(result, parse_mode="Markdown", reply_markup=keyboard, disable_web_page_preview=True)

@router.message(Command("top_slow"))
async def cmd_top_slow(message: Message) -> None:
    """Показать топ медленных пользователей по времени ответа"""
    # Проверяем, активирован ли чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if not is_activated:
        await message.reply("❌ Этот чат не активирован. Используйте /activate код для активации.")
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    bot_id = getattr(bot, "bot_id", None)
    
    # Получаем топ медленных пользователей
    top_users = await db.get_top(message.chat.id, limit=10, order="DESC")
    
    result = "🐌 **Топ 10 медленных ответов:**\n\n"
    
    if not top_users:
        result += "📊 Пока нет статистики в этом чате.\n\n"
    else:
        for i, (user_id, n, avg_sec, username) in enumerate(top_users, 1):
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            if avg_sec is not None:
                avg_str = format_duration(int(avg_sec))
            else:
                avg_str = "N/A"
            
            # Экранируем специальные символы в username
            escaped_username = escape_username(username, user_id)
            
            result += f"{i}. **{format_user_display(username, user_id)}** - {avg_str} (n={n})\n"
    
    # Получаем открытые пинги
    open_pings = await db.get_open_pings(message.chat.id)
    if open_pings:
        result += "\n⏰ **Открытые пинги:**\n"
        for user_id, ping_ts, source_message_id in open_pings:
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            elapsed = int(datetime.now().timestamp()) - ping_ts
            elapsed_str = format_duration(elapsed)
            
            user_info = await db.get_user_info(user_id)
            username = user_info.get('username', f'user_{user_id}') if user_info else f'user_{user_id}'
            
            # Создаём ссылку на исходное сообщение
            if source_message_id:
                message_link = create_message_link(message.chat.id, message.chat.username, source_message_id)
                link_text = f"[вопрос]({message_link})"
            else:
                link_text = "ID неизвестен"
            
            result += f"👤 **{format_user_display(username, user_id)}** - {elapsed_str} ({link_text})\n"
    
    # Добавляем кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать всех (до 1000)", callback_data="top_all")],
        [InlineKeyboardButton(text="⚡ Топ быстрых", callback_data="top_fast")]
    ])
    
    await message.reply(result, parse_mode="Markdown", reply_markup=keyboard, disable_web_page_preview=True)

@router.callback_query(F.data == "top_all")
async def on_top_all(callback: CallbackQuery) -> None:
    """Показать всех пользователей (до 1000)"""
    # Проверяем, активирован ли чат
    bot = callback.message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(callback.message.chat.id)
    if not is_activated:
        await callback.answer("❌ Чат не активирован", show_alert=True)
        return
    
    if not await check_admin_rights(callback.message):
        await callback.answer("❌ Только администраторы", show_alert=True)
        return
    
    bot_id = getattr(bot, "bot_id", None)
    
    # Получаем всех пользователей
    all_users = await db.get_top(callback.message.chat.id, limit=1000)
    
    if not all_users:
        await callback.answer("📊 Нет данных", show_alert=True)
        return
    
    result = "📊 **Все пользователи (до 1000):**\n\n"
    
    for i, (user_id, n, avg_sec, username) in enumerate(all_users, 1):
        if bot_id and user_id == bot_id:
            continue  # Пропускаем бота
        
        if avg_sec is not None:
            avg_str = format_duration(int(avg_sec))
        else:
            avg_str = "N/A"
        
        # Экранируем специальные символы в username
        escaped_username = escape_username(username, user_id)
        
        result += f"{i}. **{format_user_display(username, user_id)}** - {avg_str} (n={n})\n"
    
    # Разбиваем на части, если сообщение слишком длинное
    if len(result) > 4096:
        parts = [result[i:i+4096] for i in range(0, len(result), 4096)]
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part, parse_mode="Markdown")
            else:
                await callback.message.answer(part, parse_mode="Markdown")
    else:
        await callback.message.edit_text(result, parse_mode="Markdown")
    
    await callback.answer()

@router.callback_query(F.data == "top_fast")
async def on_top_fast(callback: CallbackQuery) -> None:
    """Показать топ быстрых пользователей"""
    # Проверяем, активирован ли чат
    bot = callback.message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(callback.message.chat.id)
    if not is_activated:
        await callback.answer("❌ Чат не активирован", show_alert=True)
        return
    
    if not await check_admin_rights(callback.message):
        await callback.answer("❌ Только администраторы", show_alert=True)
        return
    
    bot_id = getattr(bot, "bot_id", None)
    
    # Получаем топ быстрых пользователей
    top_users = await db.get_top(callback.message.chat.id, limit=10, order="ASC")
    
    result = "⚡ **Топ 10 быстрых ответов:**\n\n"
    
    if not top_users:
        result += "📊 Пока нет статистики в этом чате.\n\n"
    else:
        for i, (user_id, n, avg_sec, username) in enumerate(top_users, 1):
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            if avg_sec is not None:
                avg_str = format_duration(int(avg_sec))
            else:
                avg_str = "N/A"
            
            # Экранируем специальные символы в username
            escaped_username = escape_username(username, user_id)
            
            result += f"{i}. **{format_user_display(username, user_id)}** - {avg_str} (n={n})\n"
    
    # Получаем открытые пинги
    open_pings = await db.get_open_pings(callback.message.chat.id)
    if open_pings:
        result += "\n⏰ **Открытые пинги:**\n"
        for user_id, ping_ts, source_message_id in open_pings:
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            elapsed = int(datetime.now().timestamp()) - ping_ts
            elapsed_str = format_duration(elapsed)
            
            user_info = await db.get_user_info(user_id)
            username = user_info.get('username', f'user_{user_id}') if user_info else f'user_{user_id}'
            
            # Создаём ссылку на исходное сообщение
            if source_message_id:
                message_link = create_message_link(callback.message.chat.id, callback.message.chat.username, source_message_id)
                link_text = f"[вопрос]({message_link})"
            else:
                link_text = "ID неизвестен"

            result += f"👤 **{format_user_display(username, user_id)}** - {elapsed_str} ({link_text})\n"
    
    # Добавляем кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать всех (до 1000)", callback_data="top_all")],
        [InlineKeyboardButton(text="🐌 Топ медленных", callback_data="top_slow")]
    ])
    
    await callback.message.edit_text(result, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data == "top_slow")
async def on_top_slow(callback: CallbackQuery) -> None:
    """Показать топ медленных пользователей"""
    # Проверяем, активирован ли чат
    bot = callback.message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(callback.message.chat.id)
    if not is_activated:
        await callback.answer("❌ Чат не активирован", show_alert=True)
        return
    
    if not await check_admin_rights(callback.message):
        await callback.answer("❌ Только администраторы", show_alert=True)
        return
    
    bot_id = getattr(bot, "bot_id", None)
    
    # Получаем топ медленных пользователей
    top_users = await db.get_top(callback.message.chat.id, limit=10, order="DESC")
    
    result = "🐌 **Топ 10 медленных ответов:**\n\n"
    
    if not top_users:
        result += "📊 Пока нет статистики в этом чате.\n\n"
    else:
        for i, (user_id, n, avg_sec, username) in enumerate(top_users, 1):
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            if avg_sec is not None:
                avg_str = format_duration(int(avg_sec))
            else:
                avg_str = "N/A"
            
            # Экранируем специальные символы в username
            escaped_username = escape_username(username, user_id)
            
            result += f"{i}. **{format_user_display(username, user_id)}** - {avg_str} (n={n})\n"
    
    # Получаем открытые пинги
    open_pings = await db.get_open_pings(callback.message.chat.id)
    if open_pings:
        result += "\n⏰ **Открытые пинги:**\n"
        for user_id, ping_ts, source_message_id in open_pings:
            if bot_id and user_id == bot_id:
                continue  # Пропускаем бота
            
            elapsed = int(datetime.now().timestamp()) - ping_ts
            elapsed_str = format_duration(elapsed)
            
            user_info = await db.get_user_info(user_id)
            username = user_info.get('username', f'user_{user_id}') if user_info else f'user_{user_id}'
            
            # Создаём ссылку на исходное сообщение
            if source_message_id:
                message_link = create_message_link(callback.message.chat.id, callback.message.chat.username, source_message_id)
                link_text = f"[вопрос]({message_link})"
            else:
                link_text = "ID неизвестен"
            
            result += f"👤 **{format_user_display(username, user_id)}** - {elapsed_str} ({link_text})\n"
    
    # Добавляем кнопки
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать всех (до 1000)", callback_data="top_all")],
        [InlineKeyboardButton(text="⚡ Топ быстрых", callback_data="top_fast")]
    ])
    
    await callback.message.edit_text(result, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

@router.message(Command("me"))
async def cmd_me(message: Message) -> None:
    """Показать личную статистику пользователя"""
    # Проверяем, активирован ли чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if not is_activated:
        await message.reply("❌ Этот чат не активирован. Используйте /activate код для активации.")
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    if not message.from_user:
        return
    
    # Получаем статистику за последние 30 дней
    since_ts = int((datetime.now() - timedelta(days=30)).timestamp())
    stats = await db.get_user_stats(message.chat.id, message.from_user.id, since_ts)
    
    if not stats:
        await message.reply("📊 У вас пока нет статистики в этом чате.")
        return
    
    n, avg_sec = stats
    
    if avg_sec is not None:
        avg_str = format_duration(int(avg_sec))
    else:
        avg_str = "N/A"
    
    # Экранируем специальные символы в username
    user_display_name = message.from_user.username or message.from_user.first_name
    escaped_username = escape_username(user_display_name, message.from_user.id)
    
    result = f"""
📊 **Ваша статистика за 30 дней:**

👤 **Пользователь:** {format_user_display(user_display_name, message.from_user.id)}
📈 **Количество пингов:** {n}
⏱️ **Среднее время ответа:** {avg_str}
"""
    
    await message.reply(result, parse_mode="Markdown")



@router.message(F.text | F.caption)
async def on_message(message: Message) -> None:
    """Обработчик всех текстовых сообщений (кроме команд)"""
    # Пропускаем команды - они обрабатываются отдельными хендлерами
    if message.text and message.text.startswith('/'):
        return
    # Проверяем, активирован ли чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if not is_activated:
        return  # Игнорируем сообщения в неактивированных чатах
    
    bot_id = getattr(bot, "bot_id", None)

    # Регистрируем пользователя
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        # Проверяем, есть ли временный пользователь с таким username
        if message.from_user.username:
            await db.update_temp_user(
                message.from_user.username,
                message.from_user.id,
                message.from_user.first_name,
                message.from_user.last_name
            )
        
        # Обычная регистрация/обновление
        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

    # Обрабатываем пинги
    entities = message.entities or []
    text = message.text or message.caption or ""
    
    logging.info(f"Обрабатываем сообщение: entities={len(entities)}, text='{text[:50]}...'")
    
    for ent in entities:
        logging.info(f"Проверяем entity: type={ent.type}, user={ent.user.id if ent.user else None}")
        logging.info(f"Проверяем условия: ent.type='{ent.type}', ent.user={ent.user}, bot_id={bot_id}")
        # Проверяем условия для создания пинга
        is_text_mention = ent.type == "text_mention" and ent.user and not ent.user.is_bot
        is_mention = ent.type == "mention"
        is_not_bot = not bot_id or (ent.user and ent.user.id != bot_id) if ent.user else True
        
        logging.info(f"Условия: is_text_mention={is_text_mention}, is_mention={is_mention}, is_not_bot={is_not_bot}")
        
        if (is_text_mention or is_mention) and is_not_bot:
            logging.info(f"✅ Условия выполнены, обрабатываем entity типа '{ent.type}'")
            target_user_id = None
            
            if ent.type == "text_mention" and ent.user:
                target_user_id = ent.user.id
                logging.info(f"text_mention: target_user_id={target_user_id}")
            elif ent.type == "mention":
                mention_text = text[ent.offset : ent.offset + ent.length]
                username = mention_text.lstrip("@")
                logging.info(f"Ищем пользователя: mention_text='{mention_text}', username='{username}'")
                
                # Ищем в базе данных
                target_user_id = await db.resolve_username(username)
                logging.info(f"Поиск в БД: username='{username}', resolved user_id={target_user_id}")
                
                # Если не нашли, создаем временного пользователя
                if not target_user_id:
                    logging.info(f"Создаем временного пользователя для @{username}")
                    target_user_id = await db.create_temp_user_by_username(username)
                    logging.info(f"Создан временный пользователь: username='{username}', temp_user_id={target_user_id}")
            
            if target_user_id and target_user_id != message.from_user.id:
                logging.info(f"Создаём пинг: {ent.type} для user_id={target_user_id}")
                await db.record_ping(
                    chat_id=message.chat.id,
                    source_message_id=message.message_id,
                    source_user_id=message.from_user.id,
                    target_user_id=target_user_id,
                    ping_reason=ent.type,
                    ping_ts=int(message.date.timestamp()),
                )
            else:
                logging.info(f"Пропускаем пинг: target_user_id={target_user_id}, from_user.id={message.from_user.id}")
        else:
            logging.info(f"❌ Условия НЕ выполнены для entity типа '{ent.type}'")
            continue
    
    # Закрываем самый старый открытый пинг для этого автора
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        logging.info(f"🔍 Пытаемся закрыть пинг для пользователя {message.from_user.id} (username: {message.from_user.username})")
        result = await db.close_oldest_open_ping_by_message(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(message.date.timestamp()),
        )
        if result:
            logging.info(f"✅ Пинг закрыт: ping_id={result}")
        else:
            logging.info(f"❌ Не найден открытый пинг для закрытия")

@router.message(F.reply_to_message)
async def on_reply(message: Message) -> None:
    """Обработчик ответов на сообщения"""
    # Проверяем, активирован ли чат
    bot = message.bot
    db: Database = getattr(bot, "db")
    
    is_activated = await db.is_chat_activated(message.chat.id)
    if not is_activated:
        return  # Игнорируем сообщения в неактивированных чатах
    
    bot_id = getattr(bot, "bot_id", None)
    
    # Закрываем самый старый открытый пинг для этого автора
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        logging.info(f"🔍 [REPLY] Пытаемся закрыть пинг для пользователя {message.from_user.id} (username: {message.from_user.username})")
        result = await db.close_oldest_open_ping_by_message(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(message.date.timestamp()),
        )
        if result:
            logging.info(f"✅ [REPLY] Пинг закрыт: ping_id={result}")
        else:
            logging.info(f"❌ [REPLY] Не найден открытый пинг для закрытия")






