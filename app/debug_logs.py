# Добавить в функцию on_message (строка ~910):

is_activated = await db.is_chat_activated(message.chat.id)
logging.info(f"Чат {message.chat.id} активирован: {is_activated}")
if not is_activated:
    logging.info(f"Игнорируем сообщение в неактивированном чате {message.chat.id}")
    return  # Игнорируем сообщения в неактивированных чатах

# Логи уже добавлены:
# - logging.info(f"Обрабатываем сообщение: entities={len(entities)}, text='{text[:50]}...'")
# - logging.info(f"Проверяем entity: type={ent.type}, user={ent.user.id if ent.user else None}")

# После пуша проверить логи в Railway/Render при упоминании кого-то в чате
