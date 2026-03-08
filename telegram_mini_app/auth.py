# -*- coding: utf-8 -*-
"""
Валідація initData від Telegram Web App (HMAC-SHA256).
Дані з initData слід використовувати лише після валідації на сервері.
"""

import hmac
import hashlib
from urllib.parse import unquote
from typing import Optional, Dict, Any


def validate_telegram_init_data(init_data: str, bot_token: str) -> Optional[Dict[str, Any]]:
    """
    Перевіряє підпис initData від Telegram Mini App.

    Алгоритм: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
    - Витягнути hash з параметрів
    - Секретний ключ = HMAC-SHA256("WebAppData", bot_token)
    - data_check_string = параметри (крім hash), відсортовані за ключем, key=value через \\n
    - Обчислити HMAC-SHA256(secret_key, data_check_string) і порівняти з hash

    Args:
        init_data: Рядок initData (формат query string)
        bot_token: Токен бота Telegram

    Returns:
        Словник з розпарсеними параметрами (user, auth_date, hash тощо) або None при помилці
    """
    if not init_data or not bot_token:
        return None

    parsed: Dict[str, str] = {}
    received_hash = ""

    for part in init_data.split("&"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key == "hash":
            received_hash = value
            continue
        parsed[key] = unquote(value)

    if not received_hash:
        return None

    data_check_string = "\n".join(
        f"{k}={parsed[k]}" for k in sorted(parsed.keys())
    )
    secret_key = hmac.new(
        b"WebAppData",
        bot_token.encode(),
        hashlib.sha256
    ).digest()
    computed_hash = hmac.new(
        secret_key,
        data_check_string.encode(),
        hashlib.sha256
    ).hexdigest()

    if computed_hash != received_hash:
        return None

    # Парсимо user з JSON, якщо є
    user_json = parsed.get("user")
    if user_json:
        import json
        try:
            parsed["user"] = json.loads(user_json)
        except (json.JSONDecodeError, TypeError):
            pass

    return parsed
