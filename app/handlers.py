import datetime as dt
from typing import Optional, List

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message

try:
    from aiogram.types import MessageReactionUpdated  # type: ignore
    HAVE_REACTIONS = True
except Exception:
    HAVE_REACTIONS = False

from db import Database


router = Router()


def _display_name(user) -> str:
    parts: List[str] = []
    if getattr(user, "first_name", None):
        parts.append(user.first_name)
    if getattr(user, "last_name", None):
        parts.append(user.last_name)
    name = " ".join(parts) if parts else (getattr(user, "username", None) or str(user.id))
    return name


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
    since_ts = None
    rows = await db.get_top(chat_id=chat_id, since_ts=since_ts, limit=10)
    if not rows:
        await message.reply("Пока нет данных для топа.")
        return
    lines = ["Топ по среднему времени ответа за всё время:"]
    for idx, (user_id, avg_sec, cnt, username, first_name, last_name) in enumerate(rows, start=1):
        name_parts: List[str] = []
        if first_name:
            name_parts.append(first_name)
        if last_name:
            name_parts.append(last_name)
        text = " ".join(name_parts) if name_parts else (f"@{username}" if username else str(user_id))
        lines.append(f"{idx}. <a href=\"tg://user?id={user_id}\">{text}</a> — {int(avg_sec)} сек (n={cnt})")
    await message.reply("\n".join(lines))


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
    await message.reply(f"Ваше среднее время ответа: {int(avg_sec)} сек за всё время (n={cnt}).")


@router.message(F.text | F.caption)
async def on_message(message: Message) -> None:
    bot = message.bot
    db: Database = getattr(bot, "db")

    if message.from_user and not message.from_user.is_bot:
        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

    # Create pings by reply
    if (
        message.reply_to_message
        and message.reply_to_message.from_user
        and not message.reply_to_message.from_user.is_bot
    ):
        target = message.reply_to_message.from_user
        if target.id != message.from_user.id:
            await db.record_ping(
                chat_id=message.chat.id,
                source_message_id=message.message_id,
                source_user_id=message.from_user.id,
                target_user_id=target.id,
                ping_reason="reply",
                ping_ts=int(message.date.timestamp()),
            )

    # Create pings by mentions
    entities = message.entities or []
    text = message.text or message.caption or ""
    for ent in entities:
        if (
            ent.type == "text_mention"
            and ent.user
            and not ent.user.is_bot
            and ent.user.id != message.from_user.id
        ):
            await db.record_ping(
                chat_id=message.chat.id,
                source_message_id=message.message_id,
                source_user_id=message.from_user.id,
                target_user_id=ent.user.id,
                ping_reason="mention",
                ping_ts=int(message.date.timestamp()),
            )
        elif ent.type == "mention":
            mention_text = text[ent.offset : ent.offset + ent.length]
            username = mention_text.lstrip("@")
            user_id = await db.resolve_username(username)
            if user_id and user_id != message.from_user.id:
                await db.record_ping(
                    chat_id=message.chat.id,
                    source_message_id=message.message_id,
                    source_user_id=message.from_user.id,
                    target_user_id=user_id,
                    ping_reason="mention",
                    ping_ts=int(message.date.timestamp()),
                )

    # Close the oldest open ping for this author (if any)
    if message.from_user and not message.from_user.is_bot:
        await db.close_oldest_open_ping_by_message(
            chat_id=message.chat.id,
            target_user_id=message.from_user.id,
            close_message_id=message.message_id,
            close_ts=int(message.date.timestamp()),
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


