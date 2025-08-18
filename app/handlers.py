import datetime as dt
from typing import Optional, List
import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message

try:
    from aiogram.types import MessageReactionUpdated  # type: ignore
    HAVE_REACTIONS = True
except Exception:
    HAVE_REACTIONS = False

from app.db import Database


router = Router()


def _display_name(user) -> str:
    parts: List[str] = []
    if getattr(user, "first_name", None):
        parts.append(user.first_name)
    if getattr(user, "last_name", None):
        parts.append(user.last_name)
    name = " ".join(parts) if parts else (getattr(user, "username", None) or str(user.id))
    return name


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} сек"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} мин {sec} сек"
    hours, min_ = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} ч {min_} мин"
    days, hr = divmod(hours, 24)
    return f"{days} д {hr} ч"


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.reply(
        "Привет! Я замеряю скорость ответа. Отмечайте коллег через ответ на их сообщение или упоминание. Команды: /top, /me"
    )


@router.message(Command("top"))
async def cmd_top(message: Message) -> None:
    bot = message.bot
    db: Database = getattr(bot, "db")
    chat_id = message.chat.id
    bot_id = getattr(bot, "bot_id", None)
    text = message.text or ""
    limit = 10
    if "all" in text.lower():
        limit = 1000
    rows = await db.get_top(chat_id=chat_id, since_ts=None, limit=limit)
    open_pings = await db.get_open_pings(chat_id=chat_id)
    open_pings_map = {int(user_id): (ping_ts, source_message_id) for user_id, ping_ts, source_message_id in open_pings}
    now = int(dt.datetime.utcnow().timestamp())
    logging.info(f"open_pings_map: {open_pings_map}")
    if not rows and not open_pings:
        await message.reply("Пока нет данных для топа.")
        return
    lines = [f"Топ по среднему времени ответа за всё время ({'все' if limit > 10 else 'топ 10'}):"]
    for idx, (user_id, avg_sec, cnt, username, first_name, last_name) in enumerate(rows, start=1):
        user_id = int(user_id)
        if bot_id and user_id == bot_id:
            continue
        name_parts: List[str] = []
        if first_name:
            name_parts.append(first_name)
        if last_name:
            name_parts.append(last_name)
        text = " ".join(name_parts) if name_parts else (f"@{username}" if username else str(user_id))
        open_timer = ""
        if user_id in open_pings_map:
            ping_ts, source_message_id = open_pings_map[user_id]
            wait_sec = now - ping_ts
            if source_message_id:
                open_timer = f" | ⏳ {format_duration(wait_sec)} ждём <a href=\"https://t.me/c/{str(chat_id)[4:]}/{source_message_id}\">ответа</a>"
            else:
                open_timer = f" | ⏳ {format_duration(wait_sec)} ждём ответа"
        lines.append(f"{idx}. <a href=\"tg://user?id={user_id}\">{text}</a> — {format_duration(avg_sec)} (n={cnt}){open_timer}")
    # Добавить пользователей с открытыми пингами, которых нет в rows
    for user_id, (ping_ts, source_message_id) in open_pings_map.items():
        if any(int(user_id) == int(r[0]) for r in rows):
            continue
        if bot_id and user_id == bot_id:
            continue
        wait_sec = now - ping_ts
        if source_message_id:
            lines.append(f"— <a href=\"tg://user?id={user_id}\">{user_id}</a> — ⏳ {format_duration(wait_sec)} ждём <a href=\"https://t.me/c/{str(chat_id)[4:]}/{source_message_id}\">ответа</a>")
        else:
            lines.append(f"— <a href=\"tg://user?id={user_id}\">{user_id}</a> — ⏳ {format_duration(wait_sec)} ждём ответа")
    reply_markup = None
    if limit == 10 and len(rows) >= 10:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        reply_markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Топ all", callback_data="top_all")]]
        )
    await message.reply("\n".join(lines), reply_markup=reply_markup)


@router.callback_query(lambda c: c.data == "top_all")
async def on_top_all(callback_query):
    bot = callback_query.bot
    db: Database = getattr(bot, "db")
    chat_id = callback_query.message.chat.id
    rows = await db.get_top(chat_id=chat_id, since_ts=None, limit=1000)
    if not rows:
        await callback_query.message.edit_text("Пока нет данных для топа.")
        return
    lines = ["Топ по среднему времени ответа за всё время (все):"]
    for idx, (user_id, avg_sec, cnt, username, first_name, last_name) in enumerate(rows, start=1):
        name_parts: List[str] = []
        if first_name:
            name_parts.append(first_name)
        if last_name:
            name_parts.append(last_name)
        text = " ".join(name_parts) if name_parts else (f"@{username}" if username else str(user_id))
        lines.append(f"{idx}. <a href=\"tg://user?id={user_id}\">{text}</a> — {format_duration(avg_sec)} (n={cnt})")
    await callback_query.message.edit_text("\n".join(lines))


@router.message(Command("me"))
async def cmd_me(message: Message) -> None:
    bot = message.bot
    db: Database = getattr(bot, "db")
    chat_id = message.chat.id
    user_id = message.from_user.id
    since_ts = None
    stats = await db.get_user_stats(chat_id=chat_id, user_id=user_id, since_ts=since_ts)
    if not stats:
        await message.reply("Нет закрытых пингов по вам за всё время.")
        return
    avg_sec, cnt = stats
    await message.reply(f"Ваше среднее время ответа: {format_duration(avg_sec)} за всё время (n={cnt}).")


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.reply(
        """
<b>Доступные команды:</b>
/start — приветствие и краткая инструкция
/top — топ по среднему времени ответа
/me — ваша личная статистика
/help — список всех команд
""",
        parse_mode="HTML"
    )


@router.message(F.text | F.caption)
async def on_message(message: Message) -> None:
    import json
    bot = message.bot
    db: Database = getattr(bot, "db")
    bot_id = getattr(bot, "bot_id", None)
    # Логируем все entities для отладки
    entities = message.entities or []
    text = message.text or message.caption or ""
    import logging
    logging.info(f"entities: {[ent.__dict__ for ent in entities]} | text: {text}")

    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

    # Create pings only by mentions (только явные теги через @username или text_mention)
    for ent in entities:
        if (
            ent.type == "text_mention"
            and ent.user
            and not ent.user.is_bot
            and (not bot_id or ent.user.id != bot_id)
            and ent.user.id != message.from_user.id
        ):
            logging.info(f"Создаём пинг: text_mention для user_id={ent.user.id}")
            await db.record_ping(
                chat_id=message.chat.id,
                source_message_id=message.message_id,  # ссылка на исходное сообщение
                source_user_id=message.from_user.id,
                target_user_id=ent.user.id,
                ping_reason="mention",
                ping_ts=int(message.date.timestamp()),
            )
        elif ent.type == "mention":
            mention_text = text[ent.offset : ent.offset + ent.length]
            username = mention_text.lstrip("@")
            user_id = await db.resolve_username(username)
            logging.info(f"mention_text: {mention_text}, username: {username}, resolved user_id: {user_id}")
            if user_id and user_id != message.from_user.id and (not bot_id or user_id != bot_id):
                logging.info(f"Создаём пинг: mention для user_id={user_id}")
                await db.record_ping(
                    chat_id=message.chat.id,
                    source_message_id=message.message_id,  # ссылка на исходное сообщение
                    source_user_id=message.from_user.id,
                    target_user_id=user_id,
                    ping_reason="mention",
                    ping_ts=int(message.date.timestamp()),
                )

    # Close the oldest open ping for this author (if any)
    if message.from_user and not message.from_user.is_bot and (not bot_id or message.from_user.id != bot_id):
        await db.close_oldest_open_ping_by_message(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(dt.datetime.utcnow().timestamp()),
        )


if HAVE_REACTIONS:
    @router.message_reaction()
    async def on_reaction(event: "MessageReactionUpdated") -> None:  # type: ignore
        bot = event.bot
        db: Database = getattr(bot, "db")
        user = getattr(event, "user", None)
        if not user or getattr(user, "is_bot", False):
            return
        emojis: Optional[str] = None
        try:
            new_reaction = getattr(event, "new_reaction", None)
            if new_reaction:
                emojis = "".join([getattr(r, "emoji", "") for r in new_reaction]) or None
        except Exception:
            pass
        await db.upsert_user(
            user.id,
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
        )
        await db.close_ping_by_reaction(
            chat_id=event.chat.id,
            target_user_id=user.id,
            reacted_message_id=event.message_id,
            emoji=emojis,
            close_ts=int(dt.datetime.utcnow().timestamp()),
        )


@router.message(Command("debug_open_pings"))
async def cmd_debug_open_pings(message: Message) -> None:
    bot = message.bot
    db: Database = getattr(bot, "db")
    chat_id = message.chat.id
    open_pings = await db.get_open_pings(chat_id=chat_id)
    if not open_pings:
        await message.reply("Нет открытых пингов.")
        return
    now = int(dt.datetime.utcnow().timestamp())
    lines = ["<b>Открытые пинги:</b>"]
    for user_id, ping_ts, source_message_id in open_pings:
        user = await bot.get_chat_member(chat_id, user_id)
        username = user.user.username or user.user.first_name or str(user_id)
        wait_sec = now - ping_ts
        if source_message_id:
            lines.append(f"<a href=\"tg://user?id={user_id}\">{username}</a> — ⏳ {format_duration(wait_sec)} ждём <a href=\"https://t.me/c/{str(chat_id)[4:]}/{source_message_id}\">ответа</a>")
        else:
            lines.append(f"<a href=\"tg://user?id={user_id}\">{username}</a> — ⏳ {format_duration(wait_sec)} ждём ответа")
    await message.reply("\n".join(lines), parse_mode="HTML")


