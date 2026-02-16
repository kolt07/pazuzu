# -*- coding: utf-8 -*-
"""
API: поточний користувач (профіль, авторизація, чи адмін).
"""

from fastapi import APIRouter, Request, HTTPException, Depends
from telegram_mini_app.auth import validate_telegram_init_data

router = APIRouter(prefix="/api/me", tags=["me"])


def _get_validated_user(request: Request):
    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        raise HTTPException(status_code=403, detail="X-Telegram-Init-Data required")
    token = getattr(request.app.state, "bot_token", None)
    if not token:
        raise HTTPException(status_code=503, detail="Mini app not configured")
    validated = validate_telegram_init_data(init_data, token)
    if not validated:
        raise HTTPException(status_code=403, detail="Invalid init data")
    user_obj = validated.get("user")
    if not user_obj or not isinstance(user_obj, dict):
        raise HTTPException(status_code=403, detail="User data missing")
    user_id = user_obj.get("id")
    if user_id is None:
        raise HTTPException(status_code=403, detail="User id missing")
    return int(user_id), user_obj, request.app.state.user_service


@router.get("")
def me(request: Request):
    """
    Повертає профіль поточного користувача та чи він авторизований/адмін.
    """
    user_id, user_obj, user_service = _get_validated_user(request)
    authorized = user_service.is_user_authorized(user_id)
    is_admin = user_service.is_admin(user_id)
    nickname = user_service.get_user_nickname(user_id)
    return {
        "user_id": user_id,
        "username": user_obj.get("username"),
        "first_name": user_obj.get("first_name"),
        "last_name": user_obj.get("last_name"),
        "authorized": authorized,
        "is_admin": is_admin,
        "nickname": nickname,
    }
