# -*- coding: utf-8 -*-
"""
Синхронізація списку чатів Mini App між пристроями (Mongo chat_sessions).
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from telegram_mini_app.auth import validate_telegram_init_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/mini-app/chats", tags=["mini_app_chats"])


def _get_user_id(request: Request) -> int:
    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        raise HTTPException(status_code=403, detail="X-Telegram-Init-Data required")
    token = getattr(request.app.state, "bot_token", None)
    if not token:
        raise HTTPException(status_code=503, detail="Mini app not configured")
    validated = validate_telegram_init_data(init_data, token)
    if not validated:
        raise HTTPException(status_code=403, detail="Invalid init data")
    user_obj = validated.get("user") or {}
    user_id = user_obj.get("id")
    if user_id is None:
        raise HTTPException(status_code=403, detail="User id missing")
    user_service = request.app.state.user_service
    if not user_service.is_user_authorized(int(user_id)):
        raise HTTPException(status_code=403, detail="User not authorized")
    return int(user_id)


def _get_chat_repo(request: Request):
    from data.repositories.chat_session_repository import ChatSessionRepository

    repo = getattr(request.app.state, "chat_session_repository", None)
    if repo is None:
        repo = ChatSessionRepository()
        request.app.state.chat_session_repository = repo
    return repo


def _serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "chat_id": doc.get("chat_id"),
        "title": doc.get("ui_title") or "",
        "kind": doc.get("ui_kind") or "assistant",
        "flx": doc.get("ui_flx") or {},
        "messages": doc.get("ui_messages") or [],
        "listing_context": doc.get("ui_listing_context"),
        "updated_at": doc.get("updated_at").isoformat() if doc.get("updated_at") else None,
    }
    return out


@router.get("")
def list_chats(request: Request, limit: int = 200):
    user_id = _get_user_id(request)
    repo = _get_chat_repo(request)
    docs = repo.list_sidebar_for_user(str(user_id), limit=int(limit))
    return {"chats": [_serialize_doc(d) for d in docs]}


class ChatSyncItem(BaseModel):
    chat_id: str = Field(..., min_length=1, max_length=200)
    title: str = ""
    kind: str = "assistant"
    messages: Optional[List[Dict[str, Any]]] = None
    flx: Optional[Dict[str, Any]] = None
    listing_context: Optional[Dict[str, Any]] = None
    updated_at: Optional[int] = None


class ChatSyncBody(BaseModel):
    chats: List[ChatSyncItem] = Field(default_factory=list)


@router.post("/sync")
def sync_chats(request: Request, body: ChatSyncBody):
    user_id = _get_user_id(request)
    repo = _get_chat_repo(request)
    n = 0
    for item in (body.chats or [])[:80]:
        try:
            repo.upsert_sidebar_state(
                user_id=str(user_id),
                chat_id=str(item.chat_id).strip(),
                title=item.title or "",
                kind=item.kind or "assistant",
                flx=item.flx,
                ui_messages=item.messages,
                listing_context=item.listing_context,
            )
            n += 1
        except Exception as e:
            logger.warning("sync chat failed chat_id=%s: %s", item.chat_id, e)
    return {"ok": True, "upserted": n}


@router.delete("/{chat_id}")
def delete_chat(request: Request, chat_id: str):
    user_id = _get_user_id(request)
    repo = _get_chat_repo(request)
    deleted = repo.delete_sidebar(str(user_id), str(chat_id))
    return {"ok": True, "deleted": int(deleted)}
