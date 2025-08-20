# Исправления для handlers.py

# 1. Добавить функцию create_message_link после format_user_display:

def create_message_link(chat_id: int, chat_username: str, message_id: int) -> str:
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

# 2. Заменить все блоки создания ссылок на:

# Создаём ссылку на исходное сообщение
if source_message_id:
    message_link = create_message_link(message.chat.id, message.chat.username, source_message_id)
    link_text = f"[вопрос]({message_link})"
else:
    link_text = "ID неизвестен"

# 3. Места для замены:
# - cmd_top_fast (строка ~556)
# - cmd_top_slow (строка ~632) 
# - on_top_fast (строка ~762)
# - on_top_slow (строка ~842)
