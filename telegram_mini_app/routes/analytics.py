# -*- coding: utf-8 -*-
"""
API: зведена аналітика цін.
"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from bson import ObjectId

from telegram_mini_app.auth import validate_telegram_init_data
from business.services.price_analytics_service import PriceAnalyticsService


def _sanitize_for_json(obj: Any) -> Any:
    """Конвертує ObjectId та інші BSON-типи для JSON-серіалізації."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    return obj


router = APIRouter(prefix="/api/analytics", tags=["analytics"])


def _get_validated_user(request: Request):
    """Валідує користувача з initData."""
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
    return int(user_id), request.app.state.user_service


@router.get("/price-indicator")
def get_price_indicator(
    request: Request,
    city: str = Query(..., description="Місто"),
    metric: str = Query("price_uah", description="Метрика: price_uah, price_per_m2_uah, price_per_ha_uah"),
    region: Optional[str] = Query(None, description="Область (для уточнення)"),
    value: Optional[float] = Query(None, description="Значення ціни для перевірки"),
):
    """
    Отримує індикатор ціни (квартилі) для міста та метрики.
    Якщо value передано — повертає індикатор для цього значення: вигідна | середня | дорога.
    """
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    analytics = PriceAnalyticsService()
    if value is not None:
        indicator = analytics.get_price_indicator(value, city, metric, region)
        return {"indicator": indicator, "value": value, "city": city, "metric": metric}
    ind = analytics.repo.get_indicator(city, metric, region)
    if not ind:
        return {"indicator": None, "quartiles": None, "city": city, "metric": metric}
    return _sanitize_for_json({
        "indicator": None,
        "quartiles": {"q1": ind.get("q1"), "q2": ind.get("q2"), "q3": ind.get("q3"), "q4": ind.get("q4")},
        "count": ind.get("count"),
        "city": city,
        "metric": metric,
    })


@router.get("/aggregates")
def get_aggregates(
    request: Request,
    period_type: str = Query("month", description="Тип періоду: day, week, month"),
    period_key: Optional[str] = Query(None, description="Ключ періоду (напр. 2026-02)"),
    source: Optional[str] = Query(None, description="Фільтр за джерелом (olx/prozorro)"),
    property_type: Optional[str] = Query(None, description="Тип оголошення"),
    region: Optional[str] = Query(None, description="Область"),
    city: Optional[str] = Query(None, description="Місто"),
    limit: int = Query(100, ge=1, le=500),
):
    """Отримує агреговану аналітику з фільтрами."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    if period_type not in ("day", "week", "month"):
        raise HTTPException(status_code=400, detail="period_type must be day, week, or month")

    analytics = PriceAnalyticsService()
    rows = analytics.get_aggregated_analytics(
        period_type=period_type,
        period_key=period_key,
        source=source,
        property_type=property_type,
        region=region,
        city=city,
    )
    items = [_sanitize_for_json(r) for r in rows[:limit]]
    return {"items": items, "total": len(rows)}


@router.post("/rebuild")
def rebuild_analytics(request: Request):
    """Перераховує аналітику (тільки для адміністраторів)."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_admin(user_id):
        raise HTTPException(status_code=403, detail="Admin only")
    analytics = PriceAnalyticsService()
    counts = analytics.rebuild_all()
    return {"success": True, "counts": counts}
