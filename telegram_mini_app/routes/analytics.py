# -*- coding: utf-8 -*-
"""
API: зведена аналітика цін.
"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from bson import ObjectId
from pydantic import BaseModel

from telegram_mini_app.auth import validate_telegram_init_data
from business.services.price_analytics_service import PriceAnalyticsService
from data.repositories.price_analytics_repository import (
    LISTING_TYPE_GENERAL,
    LISTING_TYPE_LAND,
    LISTING_TYPE_MIXED,
    LISTING_TYPE_REAL_ESTATE,
)


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
    city: str = Query(..., description="Населений пункт (settlement)"),
    metric: str = Query("price_uah", description="Метрика: price_uah, price_per_m2_uah, price_per_ha_uah"),
    region: Optional[str] = Query(None, description="Область (для уточнення)"),
    value: Optional[float] = Query(None, description="Значення ціни для перевірки"),
    listing_type: str = Query(
        "real_estate",
        description="Тип: land, real_estate, mixed, general",
    ),
):
    """
    Отримує індикатор ціни (квартилі) для населеного пункту та метрики.
    Якщо value передано — повертає індикатор для цього значення: вигідна | середня | дорога.
    Логіка: settlement 5+ → локальний розподіл, інакше — область.
    """
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    lt_map = {"land": LISTING_TYPE_LAND, "real_estate": LISTING_TYPE_REAL_ESTATE, "mixed": LISTING_TYPE_MIXED, "general": LISTING_TYPE_GENERAL}
    lt = lt_map.get(listing_type, LISTING_TYPE_REAL_ESTATE)

    analytics = PriceAnalyticsService()
    if value is not None:
        res = analytics.get_price_indicator(value, city, metric, region, lt)
        if res:
            ind_val, ind_source = res
            return {"indicator": ind_val, "source": ind_source, "value": value, "city": city, "metric": metric}
        return {"indicator": None, "source": None, "value": value, "city": city, "metric": metric}
    ind = analytics.repo.get_indicator(city, metric, region, lt)
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


@router.get("/listing")
def get_listing_analytics(
    request: Request,
    source: str = Query(..., description="Джерело: olx або prozorro"),
    source_id: str = Query(..., description="ID в джерелі (URL для OLX, auction_id для ProZorro)"),
):
    """Отримує збережену LLM-аналітику оголошення (ціна, місцезнаходження, оточення)."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    if source.lower() not in ("olx", "prozorro"):
        raise HTTPException(status_code=400, detail="source must be olx or prozorro")

    from business.services.listing_analytics_service import ListingAnalyticsService
    svc = ListingAnalyticsService()
    doc = svc.get_analytics(source.lower(), source_id)
    if not doc:
        return {"analysis_text": None, "analysis_at": None}
    return _sanitize_for_json({
        "analysis_text": doc.get("analysis_text"),
        "analysis_at": doc.get("analysis_at"),
        "metadata": doc.get("metadata", {}),
    })


class GenerateListingAnalyticsBody(BaseModel):
    source: str
    source_id: str
    force: bool = False


@router.post("/listing/generate")
def generate_listing_analytics(request: Request, body: GenerateListingAnalyticsBody):
    """Генерує LLM-аналітику оголошення та зберігає. При запиті через помічника або кнопку «Сформувати аналітику»."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    if body.source.lower() not in ("olx", "prozorro"):
        raise HTTPException(status_code=400, detail="source must be olx or prozorro")

    from business.services.listing_analytics_service import ListingAnalyticsService
    svc = ListingAnalyticsService()
    result = svc.generate_and_save(body.source.lower(), body.source_id, force_refresh=body.force)
    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])
    return _sanitize_for_json(result)


@router.post("/rebuild")
def rebuild_analytics(request: Request):
    """Перераховує аналітику (тільки для адміністраторів)."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_admin(user_id):
        raise HTTPException(status_code=403, detail="Admin only")
    analytics = PriceAnalyticsService()
    counts = analytics.rebuild_all()
    return {"success": True, "counts": counts}
