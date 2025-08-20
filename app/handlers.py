import os
import logging
from datetime import datetime, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from .db import Database

router = Router()

# Состояния для FSM
class ChatManagement(StatesGroup):
    waiting_for_chat_id = State()

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

def is_chat_allowed(chat_id: int) -> bool:
    """Проверяет, разрешён ли чат в whitelist"""
    allowed_chats = os.getenv("ALLOWED_CHATS", "")
    if not allowed_chats:
        return False
    
    allowed_ids = [int(x.strip()) for x in allowed_chats.split(",") if x.strip()]
    return chat_id in allowed_ids

def is_main_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь главным администратором"""
    main_admin_id = os.getenv("MAIN_ADMIN_ID")
    if not main_admin_id:
        return False
    return user_id == int(main_admin_id)

async def check_admin_rights(message: Message) -> bool:
    """Проверяет права администратора в чате"""
    try:
        chat_member = await message.bot.get_chat_member(message.chat.id, message.from_user.id)
        return chat_member.status in ["creator", "administrator"]
    except Exception:
        return False

async def check_bot_admin_rights(message: Message) -> bool:
    """Проверяет, является ли бот администратором чата"""
    try:
        bot_member = await message.bot.get_chat_member(message.chat.id, message.bot.id)
        return bot_member.status in ["creator", "administrator"]
    except Exception:
        return False

@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Обработчик команды /start"""
    if not is_chat_allowed(message.chat.id):
        await message.reply("❌ Этот чат не авторизован для использования бота.")
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    await message.reply(
        "👋 Привет! Я бот для отслеживания времени ответа на пинги.\n\n"
        "📋 Доступные команды:\n"
        "/top - Топ пользователей по времени ответа\n"
        "/me - Моя статистика\n"
        "/help - Справка\n"
        "/debug_chat_id - Показать ID чата\n"
        "/debug_open_pings - Открытые пинги\n\n"
        "🔒 Только администраторы могут использовать команды."
    )

@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Обработчик команды /help"""
    if not is_chat_allowed(message.chat.id):
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    help_text = """
📋 **Справка по командам:**

**Основные команды:**
• `/top` - Показать топ пользователей по времени ответа
• `/me` - Показать вашу личную статистику
• `/help` - Показать эту справку

**Отладочные команды:**
• `/debug_chat_id` - Показать ID текущего чата
• `/debug_open_pings` - Показать все открытые пинги

**Как работает бот:**
• Отслеживает пинги через @username или text_mention
• Закрывает пинг при любом сообщении или реакции от пользователя
• Показывает статистику по времени ответа
• Работает только в авторизованных чатах

**Требования:**
• Чат должен быть в whitelist
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

**Для добавления в whitelist:**
Добавьте `{chat_id}` в переменную окружения `ALLOWED_CHATS`

**Пример:**
`ALLOWED_CHATS=123456789,-987654321,{chat_id}`
"""
    
    await message.reply(debug_info, parse_mode="HTML")

@router.message(Command("debug_open_pings"))
async def cmd_debug_open_pings(message: Message) -> None:
    """Показать все открытые пинги в чате"""
    if not is_chat_allowed(message.chat.id):
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
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
            chat_username = message.chat.username
            if chat_username:
                message_link = f"https://t.me/{chat_username}/{source_message_id}"
                link_text = f"[сообщение]({message_link})"
            else:
                link_text = f"ID: {source_message_id}"
        else:
            link_text = "ID неизвестен"
        
        result += f"👤 **@{username}** - {elapsed_str} ({link_text})\n"
    
    await message.reply(result, parse_mode="HTML", disable_web_page_preview=True)

@router.message(Command("top"))
async def cmd_top(message: Message) -> None:
    """Показать топ пользователей по времени ответа"""
    if not is_chat_allowed(message.chat.id):
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    bot_id = getattr(bot, "bot_id", None)
    
    # Получаем топ пользователей
    top_users = await db.get_top(message.chat.id, limit=10)
    
    if not top_users:
        await message.reply("📊 Пока нет статистики в этом чате.")
        return
    
    result = "🏆 **Топ 10 по времени ответа:**\n\n"
    
    for i, (user_id, n, avg_sec, username) in enumerate(top_users, 1):
        if bot_id and user_id == bot_id:
            continue  # Пропускаем бота
        
        if avg_sec is not None:
            avg_str = format_duration(int(avg_sec))
        else:
            avg_str = "N/A"
        
        result += f"{i}. **@{username}** - {avg_str} (n={n})\n"
    
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
                chat_username = message.chat.username
                if chat_username:
                    message_link = f"https://t.me/{chat_username}/{source_message_id}"
                    link_text = f"[сообщение]({message_link})"
                else:
                    link_text = f"ID: {source_message_id}"
            else:
                link_text = "ID неизвестен"
            
            result += f"👤 **@{username}** - {elapsed_str} ({link_text})\n"
    
    # Добавляем кнопку "Показать всех"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Показать всех (до 1000)", callback_data="top_all")]
    ])
    
    await message.reply(result, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)

@router.callback_query(F.data == "top_all")
async def on_top_all(callback: CallbackQuery) -> None:
    """Показать всех пользователей (до 1000)"""
    if not is_chat_allowed(callback.message.chat.id):
        await callback.answer("❌ Чат не авторизован", show_alert=True)
        return
    
    if not await check_admin_rights(callback.message):
        await callback.answer("❌ Только администраторы", show_alert=True)
        return
    
    bot = callback.message.bot
    db: Database = getattr(bot, "db")
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
        
        result += f"{i}. **@{username}** - {avg_str} (n={n})\n"
    
    # Разбиваем на части, если сообщение слишком длинное
    if len(result) > 4096:
        parts = [result[i:i+4096] for i in range(0, len(result), 4096)]
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part, parse_mode="HTML")
            else:
                await callback.message.answer(part, parse_mode="HTML")
    else:
        await callback.message.edit_text(result, parse_mode="HTML")
    
    await callback.answer()

@router.message(Command("me"))
async def cmd_me(message: Message) -> None:
    """Показать личную статистику пользователя"""
    if not is_chat_allowed(message.chat.id):
        return
    
    if not await check_admin_rights(message):
        await message.reply("❌ Только администраторы могут использовать команды бота.")
        return
    
    if not message.from_user:
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    
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
    
    result = f"""
📊 **Ваша статистика за 30 дней:**

👤 **Пользователь:** @{message.from_user.username or message.from_user.first_name}
📈 **Количество пингов:** {n}
⏱️ **Среднее время ответа:** {avg_str}
"""
    
    await message.reply(result, parse_mode="HTML")

@router.message(F.text | F.caption)
async def on_message(message: Message) -> None:
    """Обработчик всех текстовых сообщений"""
    if not is_chat_allowed(message.chat.id):
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    bot_id = getattr(bot, "bot_id", None)

    # Регистрируем пользователя
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

    # Обрабатываем пинги
    entities = message.entities or []
    text = message.text or message.caption or ""
    
    for ent in entities:
        if (
            (ent.type == "text_mention" and ent.user and not ent.user.is_bot)
            or (ent.type == "mention")
        ) and (not bot_id or (ent.user and ent.user.id != bot_id)):
            target_user_id = None
            if ent.type == "text_mention" and ent.user:
                target_user_id = ent.user.id
            elif ent.type == "mention":
                mention_text = text[ent.offset : ent.offset + ent.length]
                username = mention_text.lstrip("@")
                target_user_id = await db.resolve_username(username)
                logging.info(f"mention_text: {mention_text}, username: {username}, resolved user_id: {target_user_id}")
                if not target_user_id:
                    await message.reply(f"Не удалось найти пользователя @{username}. Попросите его написать любое сообщение в чат.")
                    logging.warning(f"Не удалось найти user_id для @{username}, пользователь должен написать сообщение в чат.")
                    continue
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
    
    # Закрываем самый старый открытый пинг для этого автора
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        await db.close_oldest_open_ping_by_message(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(message.date.timestamp()),
        )

@router.message(F.reply_to_message)
async def on_reply(message: Message) -> None:
    """Обработчик ответов на сообщения"""
    if not is_chat_allowed(message.chat.id):
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    bot_id = getattr(bot, "bot_id", None)
    
    # Закрываем самый старый открытый пинг для этого автора
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        await db.close_oldest_open_ping_by_message(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(message.date.timestamp()),
        )

@router.message(F.reaction)
async def on_reaction(message: Message) -> None:
    """Обработчик реакций на сообщения"""
    if not is_chat_allowed(message.chat.id):
        return
    
    bot = message.bot
    db: Database = getattr(bot, "db")
    bot_id = getattr(bot, "bot_id", None)
    
    # Закрываем самый старый открытый пинг для этого автора
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        await db.close_oldest_open_ping_by_reaction(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(message.date.timestamp()),
        )


