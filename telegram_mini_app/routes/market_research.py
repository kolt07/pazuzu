# -*- coding: utf-8 -*-
"""API дослідження ринку (точковий пошук у джерелах + статистична довідка)."""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from telegram_mini_app.auth import validate_telegram_init_data

router = APIRouter(prefix="/api/market-research", tags=["market-research"])


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


def _get_service(request: Request):
    svc = getattr(request.app.state, "market_research_service", None)
    if svc is None:
        from business.services.market_research_service import MarketResearchService

        svc = MarketResearchService(request.app.state.settings)
        request.app.state.market_research_service = svc
    return svc


class StartResearchRequest(BaseModel):
    filter: Optional[Dict[str, Any]] = None
    deal_types: Optional[List[str]] = Field(default=None)
    depth_days: Optional[int] = None
    confirm_broad: bool = False


def _http_from_service(result: Dict[str, Any]) -> Dict[str, Any]:
    from business.services.market_research_service import (
        ERROR_ACTIVE_RUN,
        ERROR_CONFIRM_BROAD,
        ERROR_EMPTY_DEAL_TYPES,
        ERROR_FORBIDDEN,
        ERROR_NOT_ACTIVE,
        ERROR_NOT_FOUND,
    )

    if result.get("ok"):
        return result
    err = result.get("error")
    msg = result.get("message") or err or "error"
    if err == ERROR_CONFIRM_BROAD:
        raise HTTPException(status_code=400, detail={"error": err, "message": msg})
    if err == ERROR_EMPTY_DEAL_TYPES:
        raise HTTPException(status_code=400, detail=msg)
    if err == ERROR_ACTIVE_RUN:
        raise HTTPException(
            status_code=409,
            detail={"error": err, "message": msg, "research_id": result.get("research_id")},
        )
    if err == ERROR_NOT_FOUND:
        raise HTTPException(status_code=404, detail=msg)
    if err == ERROR_FORBIDDEN:
        raise HTTPException(status_code=403, detail=msg)
    if err == ERROR_NOT_ACTIVE:
        raise HTTPException(status_code=400, detail=msg)
    raise HTTPException(status_code=400, detail=msg)


@router.post("")
def start_research(request: Request, body: StartResearchRequest):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    result = svc.start(
        user_id=str(user_id),
        filter_spec=body.filter,
        deal_types=body.deal_types,
        depth_days=body.depth_days,
        confirm_broad=body.confirm_broad,
    )
    return _http_from_service(result)


@router.get("")
def list_research(request: Request, limit: int = Query(30, ge=1, le=100)):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    return {"items": svc.list_for_user(str(user_id), limit=limit)}


@router.get("/{research_id}")
def get_research(
    request: Request,
    research_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    result = svc.get(research_id, str(user_id), skip=skip, limit=limit)
    if not result.get("ok"):
        return _http_from_service(result)
    from telegram_mini_app.routes.search import _normalize_unified_doc, _sanitize_json_floats

    items = [_normalize_unified_doc(d) for d in (result.get("items") or [])]
    try:
        from business.services.price_analytics_service import PriceAnalyticsService
        analytics = PriceAnalyticsService()
        indicators = analytics.get_price_indicators_for_items(items)
        for item in items:
            cid = f"{item.get('source', '')}:{item.get('source_id', '')}"
            if cid in indicators:
                item["price_indicator"] = indicators[cid]["indicator"]
                item["price_indicator_source"] = indicators[cid].get("source", "region")
    except Exception:
        pass
    result["items"] = items
    return _sanitize_json_floats(result)


@router.post("/{research_id}/cancel")
def cancel_research(request: Request, research_id: str):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    return _http_from_service(svc.cancel(research_id, str(user_id)))
