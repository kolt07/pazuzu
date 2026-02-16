# -*- coding: utf-8 -*-
"""
Залежності FastAPI: отримання поточного користувача з initData.
"""

from typing import Optional, Annotated
from fastapi import Header, HTTPException, Depends

from telegram_mini_app.auth import validate_telegram_init_data


def get_init_data_header(
    x_telegram_init_data: Annotated[Optional[str], Header(alias="X-Telegram-Init-Data")] = None,
) -> str:
    """Повертає заголовок initData або викликає 403."""
    if not x_telegram_init_data:
        raise HTTPException(status_code=403, detail="X-Telegram-Init-Data required")
    return x_telegram_init_data


def get_validated_user(
    init_data: str,
    bot_token: str,
):
    """
    Валідує initData і повертає словник з user та user_id.
    Використовується через залежність з переданим bot_token (замикання або app.state).
    """
    validated = validate_telegram_init_data(init_data, bot_token)
    if not validated:
        raise HTTPException(status_code=403, detail="Invalid init data")
    user_obj = validated.get("user")
    if not user_obj or not isinstance(user_obj, dict):
        raise HTTPException(status_code=403, detail="User data missing")
    user_id = user_obj.get("id")
    if user_id is None:
        raise HTTPException(status_code=403, detail="User id missing")
    return {
        "user_id": int(user_id),
        "user": user_obj,
        "init_data": validated,
    }
