# -*- coding: utf-8 -*-
"""
Відправка файлів користувачу через Telegram Bot API.
Використовується замість прямого скачування, оскільки в мобільному застосунку воно не працює.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def send_file_via_telegram(
    chat_id: int,
    file_bytes: bytes,
    filename: str,
    bot_token: str,
    caption: Optional[str] = None,
) -> bool:
    """
    Відправляє файл користувачу в чат через Telegram Bot API.

    Args:
        chat_id: ID чату (user_id для приватних чатів)
        file_bytes: Вміст файлу
        filename: Ім'я файлу
        bot_token: Токен бота
        caption: Підпис до файлу (опціонально)

    Returns:
        True якщо успішно, False при помилці
    """
    if not bot_token or not file_bytes:
        return False
    try:
        import requests
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
        files = {"document": (filename, file_bytes)}
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption[:1024]
        resp = requests.post(url, files=files, data=data, timeout=60)
        if resp.status_code != 200:
            logger.warning("Telegram sendDocument failed: %s %s", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception as e:
        logger.exception("Помилка відправки файлу через бота: %s", e)
        return False
