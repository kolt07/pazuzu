# -*- coding: utf-8 -*-
"""
API: пошук оголошень за зведеною таблицею (unified_listings).
Об'єднує OLX та ProZorro в єдиний пошук з фільтрами.
"""

import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Request, HTTPException, Query, Depends
from fastapi.responses import Response
from pydantic import BaseModel

from telegram_mini_app.auth import validate_telegram_init_data
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository, _olx_url_variants
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository


router = APIRouter(prefix="/api/search", tags=["search"])


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


def _build_olx_filters(
    region: Optional[str] = None,
    city: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    price_eq: Optional[float] = None,
) -> Dict[str, Any]:
    """Будує MongoDB фільтри для OLX."""
    filters = {}
    
    # Фільтри за регіоном та містом
    if region or city:
        or_conditions = []
        
        # Спочатку пробуємо знайти ID топонімів через GeographyService
        region_id = None
        city_id = None
        try:
            from business.services.geography_service import GeographyService
            geography_service = GeographyService()
            
            if region:
                region_obj = geography_service.regions_repo.find_by_name(region)
                if region_obj:
                    region_id = str(region_obj["_id"])
            
            if city:
                if region_id:
                    city_obj = geography_service.cities_repo.find_by_name_and_region(city, region_id)
                    if city_obj:
                        city_id = str(city_obj["_id"])
                else:
                    # Шукаємо місто без області (менш точно)
                    all_regions = geography_service.get_all_regions()
                    for r in all_regions:
                        city_obj = geography_service.cities_repo.find_by_name_and_region(city, str(r["_id"]))
                        if city_obj:
                            city_id = str(city_obj["_id"])
                            break
        except Exception:
            pass
        
        # Фільтри з використанням address_refs (пріоритет). Підтримується тільки область або область+місто.
        if region_id or city_id:
            refs_match = {}
            if region_id:
                refs_match["region._id"] = region_id
            if city_id:
                refs_match["city._id"] = city_id
            if refs_match:
                or_conditions.append({
                    "detail.address_refs": {
                        "$elemMatch": refs_match
                    }
                })
        
        # Fallback: фільтри з текстового пошуку
        if region:
            or_conditions.append({
                "detail.resolved_locations": {
                    "$elemMatch": {"address_structured.region": {"$regex": str(region).strip(), "$options": "i"}}
                }
            })
            or_conditions.append({"search_data.location": {"$regex": str(region).strip(), "$options": "i"}})
        if city:
            or_conditions.append({
                "detail.resolved_locations": {
                    "$elemMatch": {"address_structured.city": {"$regex": str(city).strip(), "$options": "i"}}
                }
            })
            or_conditions.append({"search_data.location": {"$regex": str(city).strip(), "$options": "i"}})
        
        if or_conditions:
            filters["$or"] = or_conditions
    
    # Фільтри за ціною
    price_filters = {}
    if price_eq is not None:
        price_filters["search_data.price_value"] = price_eq
    else:
        if price_min is not None:
            price_filters["$gte"] = price_min
        if price_max is not None:
            price_filters["$lte"] = price_max
        if price_filters:
            filters["search_data.price_value"] = price_filters
    
    return filters


def _build_prozorro_filters(
    region: Optional[str] = None,
    city: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    price_eq: Optional[float] = None,
) -> Dict[str, Any]:
    """Будує MongoDB фільтри для Prozorro."""
    filters = {}
    
    # Фільтри за регіоном та містом
    if region or city:
        or_conditions = []
        
        # Спочатку пробуємо знайти ID топонімів через GeographyService
        region_id = None
        city_id = None
        try:
            from business.services.geography_service import GeographyService
            geography_service = GeographyService()
            
            if region:
                region_obj = geography_service.regions_repo.find_by_name(region)
                if region_obj:
                    region_id = str(region_obj["_id"])
            
            if city:
                if region_id:
                    city_obj = geography_service.cities_repo.find_by_name_and_region(city, region_id)
                    if city_obj:
                        city_id = str(city_obj["_id"])
                else:
                    # Шукаємо місто без області (менш точно)
                    all_regions = geography_service.get_all_regions()
                    for r in all_regions:
                        city_obj = geography_service.cities_repo.find_by_name_and_region(city, str(r["_id"]))
                        if city_obj:
                            city_id = str(city_obj["_id"])
                            break
        except Exception:
            pass
        
        # Фільтри з використанням address_refs (пріоритет). Підтримується тільки область або область+місто.
        if region_id or city_id:
            refs_match = {}
            if region_id:
                refs_match["region._id"] = region_id
            if city_id:
                refs_match["city._id"] = city_id
            if refs_match:
                or_conditions.append({
                    "auction_data.address_refs": {
                        "$elemMatch": refs_match
                    }
                })
        
        # Fallback: фільтри з текстового пошуку
        if region:
            or_conditions.append({
                "auction_data.items": {
                    "$elemMatch": {
                        "address.region.uk_UA": {"$regex": str(region).strip(), "$options": "i"}
                    }
                }
            })
        if city:
            or_conditions.append({
                "auction_data.items": {
                    "$elemMatch": {
                        "address.locality.uk_UA": {"$regex": str(city).strip(), "$options": "i"}
                    }
                }
            })
        
        if or_conditions:
            filters["$or"] = or_conditions
    
    # Фільтри за ціною (auction_data.value.amount)
    price_filters = {}
    if price_eq is not None:
        price_filters["auction_data.value.amount"] = price_eq
    else:
        if price_min is not None:
            price_filters["$gte"] = price_min
        if price_max is not None:
            price_filters["$lte"] = price_max
        if price_filters:
            filters["auction_data.value.amount"] = price_filters
    
    return filters


# Маппінг UI-значень типу оголошення на значення в БД (unified_listings.property_type)
_PROPERTY_TYPE_MAP = {
    "neruhomist": ["Нерухомість", "Комерційна нерухомість"],
    "zemelna_dilyanka": ["Земельна ділянка"],
    "zemelna_dilyanka_z_neruhomistyu": ["Земельна ділянка з нерухомістю"],
    "inshe": ["інше"],
}


def _build_unified_filters(
    region: Optional[str] = None,
    city: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    price_eq: Optional[float] = None,
    source: Optional[str] = None,
    property_type: Optional[str] = None,
    building_area_sqm_op: Optional[str] = None,
    building_area_sqm_value: Optional[float] = None,
    land_area_ha_op: Optional[str] = None,
    land_area_ha_value: Optional[float] = None,
    date_filter_days: Optional[int] = None,
    price_per_m2_min: Optional[float] = None,
    price_per_m2_max: Optional[float] = None,
    price_per_m2_currency: Optional[str] = None,
    price_per_ha_min: Optional[float] = None,
    price_per_ha_max: Optional[float] = None,
    price_per_ha_currency: Optional[str] = None,
) -> Dict[str, Any]:
    """Будує MongoDB фільтри для зведеної таблиці unified_listings."""
    filters: Dict[str, Any] = {}

    # Фільтр за джерелом (olx/prozorro)
    if source and source.lower() in ("olx", "prozorro"):
        filters["source"] = source.lower()

    # Геофільтри: addresses.region, addresses.settlement
    if region or city:
        elem_match: Dict[str, Any] = {}
        if region:
            r = str(region).strip()
            if r in _CITIES_WITH_SPECIAL_STATUS:
                # Київ, Севастополь — міста зі спеціальним статусом; фільтруємо за settlement
                escaped = re.escape(r)
                elem_match["settlement"] = {"$regex": f"^(м\\.\\s*)?{escaped}", "$options": "i"}
            else:
                elem_match["region"] = {"$regex": r, "$options": "i"}
        if city:
            c = str(city).strip()
            escaped = re.escape(c)
            # Підтримка "м. Київ" та "Київ"
            elem_match["settlement"] = {"$regex": f"^(м\\.\\s*)?{escaped}", "$options": "i"}
        if elem_match:
            filters["addresses"] = {"$elemMatch": elem_match}

    # Фільтри за ціною (price_uah)
    if price_eq is not None:
        filters["price_uah"] = price_eq
    else:
        if price_min is not None or price_max is not None:
            price_cond: Dict[str, Any] = {}
            if price_min is not None:
                price_cond["$gte"] = price_min
            if price_max is not None:
                price_cond["$lte"] = price_max
            if price_cond:
                filters["price_uah"] = price_cond

    # Фільтр за типом оголошення
    if property_type and property_type in _PROPERTY_TYPE_MAP:
        types = _PROPERTY_TYPE_MAP[property_type]
        if len(types) == 1:
            filters["property_type"] = types[0]
        else:
            filters["property_type"] = {"$in": types}

    # Фільтр за площею нерухомості (кв. м.)
    if building_area_sqm_op and building_area_sqm_value is not None:
        op_map = {"eq": "$eq", "gte": "$gte", "lte": "$lte"}
        op = op_map.get(building_area_sqm_op)
        if op:
            filters["building_area_sqm"] = {op: float(building_area_sqm_value)}

    # Фільтр за площею земельної ділянки (га)
    if land_area_ha_op and land_area_ha_value is not None:
        op_map = {"eq": "$eq", "gte": "$gte", "lte": "$lte"}
        op = op_map.get(land_area_ha_op)
        if op:
            filters["land_area_ha"] = {op: float(land_area_ha_value)}

    # Фільтр за датою (source_updated_at за останні N днів)
    if date_filter_days is not None and date_filter_days > 0:
        from utils.date_utils import KYIV_TZ
        now = datetime.now(KYIV_TZ)
        date_from = now - timedelta(days=date_filter_days)
        filters["source_updated_at"] = {"$gte": date_from}

    # Фільтр за ціною за м²
    price_m2_field = "price_per_m2_uah" if (price_per_m2_currency or "uah") == "uah" else "price_per_m2_usd"
    if price_per_m2_min is not None or price_per_m2_max is not None:
        price_m2_cond: Dict[str, Any] = {}
        if price_per_m2_min is not None:
            price_m2_cond["$gte"] = price_per_m2_min
        if price_per_m2_max is not None:
            price_m2_cond["$lte"] = price_per_m2_max
        if price_m2_cond:
            filters[price_m2_field] = price_m2_cond

    # Фільтр за ціною за га
    price_ha_field = "price_per_ha_uah" if (price_per_ha_currency or "uah") == "uah" else "price_per_ha_usd"
    if price_per_ha_min is not None or price_per_ha_max is not None:
        price_ha_cond: Dict[str, Any] = {}
        if price_per_ha_min is not None:
            price_ha_cond["$gte"] = price_per_ha_min
        if price_per_ha_max is not None:
            price_ha_cond["$lte"] = price_per_ha_max
        if price_ha_cond:
            filters[price_ha_field] = price_ha_cond

    return filters


def _normalize_unified_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Нормалізує документ unified_listings для відображення."""
    source = doc.get("source", "")
    region = None
    city = None

    addresses = doc.get("addresses", [])
    if addresses and isinstance(addresses, list):
        for addr in addresses:
            if isinstance(addr, dict):
                if not region and addr.get("region"):
                    region = addr["region"]
                if not city and addr.get("settlement"):
                    city = addr["settlement"]
            if region and city:
                break

    source_updated = doc.get("source_updated_at")
    date_str = None
    if source_updated:
        if hasattr(source_updated, "isoformat"):
            date_str = source_updated.isoformat()
        else:
            date_str = str(source_updated)

    return {
        "id": doc.get("_id"),
        "source": source,
        "source_id": doc.get("source_id"),
        "title": doc.get("title", ""),
        "price": doc.get("price_uah"),
        "price_usd": doc.get("price_usd"),
        "price_per_m2_uah": doc.get("price_per_m2_uah"),
        "price_per_m2_usd": doc.get("price_per_m2_usd"),
        "price_per_ha_uah": doc.get("price_per_ha_uah"),
        "price_per_ha_usd": doc.get("price_per_ha_usd"),
        "region": region,
        "city": city,
        "status": doc.get("status", ""),
        "property_type": doc.get("property_type", ""),
        "building_area_sqm": doc.get("building_area_sqm"),
        "land_area_ha": doc.get("land_area_ha"),
        "floor": doc.get("floor"),
        "tags": doc.get("tags") or [],
        "page_url": doc.get("page_url"),
        "updated_at": date_str,
    }


def _normalize_olx_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Нормалізує документ OLX для відображення."""
    # _id вже нормалізований методом find_many
    
    # Витягуємо основні поля для списку
    search_data = doc.get("search_data", {})
    detail = doc.get("detail", {})
    
    # Визначаємо область та місто
    region = None
    city = None
    
    # Спочатку з address_refs (нова структура)
    address_refs = detail.get("address_refs", [])
    if address_refs and isinstance(address_refs, list) and len(address_refs) > 0:
        first_refs = address_refs[0]
        if isinstance(first_refs, dict):
            if first_refs.get("region"):
                region = first_refs["region"].get("name")
            if first_refs.get("city"):
                city = first_refs["city"].get("name")
    
    # Якщо не знайшли в address_refs, пробуємо resolved_locations
    if not region and not city:
        resolved_locations = detail.get("resolved_locations", [])
        if resolved_locations and isinstance(resolved_locations, list):
            for loc in resolved_locations:
                if isinstance(loc, dict):
                    addr_struct = loc.get("address_structured", {})
                    if isinstance(addr_struct, dict):
                        if not region and addr_struct.get("region"):
                            region = addr_struct.get("region")
                        if not city and addr_struct.get("city"):
                            city = addr_struct.get("city")
    
    # Якщо не знайшли, пробуємо з search_data.location
    if not region and not city:
        location_text = search_data.get("location", "")
        if location_text:
            # Простий парсинг (можна покращити)
            parts = location_text.split(" - ")
            if len(parts) > 1:
                city = parts[0].strip()
    
    price_metrics = {}
    detail = doc.get("detail", {})
    if isinstance(detail, dict):
        price_metrics = detail.get("price_metrics") or {}

    llm = detail.get("llm", {}) if isinstance(detail, dict) else {}
    floor = llm.get("floor") if llm else None
    tags = llm.get("tags") if llm and isinstance(llm.get("tags"), list) else []

    return {
        "id": doc.get("_id"),
        "url": doc.get("url"),
        "title": search_data.get("title", ""),
        "price": search_data.get("price_value"),
        "price_text": search_data.get("price_text", ""),
        "price_usd": price_metrics.get("total_price_usd"),
        "price_per_m2_uah": price_metrics.get("price_per_m2_uah"),
        "price_per_m2_usd": price_metrics.get("price_per_m2_usd"),
        "price_per_ha_uah": price_metrics.get("price_per_ha_uah"),
        "price_per_ha_usd": price_metrics.get("price_per_ha_usd"),
        "location": search_data.get("location", ""),
        "region": region,
        "city": city,
        "area_m2": search_data.get("area_m2"),
        "floor": floor if floor else None,
        "tags": tags or [],
        "date_text": search_data.get("date_text", ""),
        "updated_at": doc.get("updated_at").isoformat() if doc.get("updated_at") and hasattr(doc.get("updated_at"), "isoformat") else str(doc.get("updated_at")) if doc.get("updated_at") else None,
    }


def _normalize_prozorro_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Нормалізує документ Prozorro для відображення."""
    # _id вже нормалізований методом find_many
    
    auction_data = doc.get("auction_data", {})
    
    # Визначаємо область та місто
    region = None
    city = None
    
    # Спочатку з address_refs (нова структура)
    address_refs = auction_data.get("address_refs", [])
    if address_refs and isinstance(address_refs, list) and len(address_refs) > 0:
        first_refs = address_refs[0]
        if isinstance(first_refs, dict):
            if first_refs.get("region"):
                region = first_refs["region"].get("name")
            if first_refs.get("city"):
                city = first_refs["city"].get("name")
    
    # Якщо не знайшли в address_refs, пробуємо items.address
    if not region and not city:
        items = auction_data.get("items", [])
        if items and isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    address = item.get("address", {})
                    if isinstance(address, dict):
                        if not region:
                            region_obj = address.get("region", {})
                            if isinstance(region_obj, dict):
                                region_ua = region_obj.get("uk_UA", "")
                                if region_ua:
                                    region = region_ua.replace(" область", "").replace(" обл.", "").strip()
                        if not city:
                            locality_obj = address.get("locality", {})
                            if isinstance(locality_obj, dict):
                                city = locality_obj.get("uk_UA", "")
    
    # Визначаємо ціну
    value = auction_data.get("value", {})
    price = None
    if isinstance(value, dict):
        price = value.get("amount")

    price_metrics = auction_data.get("price_metrics") or {}
    
    # Визначаємо заголовок
    title = auction_data.get("title", {})
    title_ua = ""
    if isinstance(title, dict):
        title_ua = title.get("uk_UA", "")
    if not title_ua:
        title_ua = auction_data.get("id", "")
    
    floor = auction_data.get("floor")
    tags = auction_data.get("tags")
    if not isinstance(tags, list):
        tags = []

    return {
        "id": doc.get("_id"),
        "auction_id": doc.get("auction_id"),
        "title": title_ua,
        "price": price,
        "price_usd": price_metrics.get("total_price_usd"),
        "price_per_m2_uah": price_metrics.get("price_per_m2_uah"),
        "price_per_m2_usd": price_metrics.get("price_per_m2_usd"),
        "price_per_ha_uah": price_metrics.get("price_per_ha_uah"),
        "price_per_ha_usd": price_metrics.get("price_per_ha_usd"),
        "region": region,
        "city": city,
        "floor": floor,
        "tags": tags,
        "status": auction_data.get("status", ""),
        "date_created": auction_data.get("dateCreated"),
        "date_modified": auction_data.get("dateModified"),
        "last_updated": doc.get("last_updated").isoformat() if doc.get("last_updated") and hasattr(doc.get("last_updated"), "isoformat") else str(doc.get("last_updated")) if doc.get("last_updated") else None,
    }


@router.get("/olx")
def search_olx(
    request: Request,
    region: Optional[str] = Query(None, description="Фільтр за областю"),
    city: Optional[str] = Query(None, description="Фільтр за містом"),
    price_min: Optional[float] = Query(None, description="Мінімальна ціна"),
    price_max: Optional[float] = Query(None, description="Максимальна ціна"),
    price_eq: Optional[float] = Query(None, description="Точна ціна"),
    sort_field: str = Query("updated_at", description="Поле для сортування"),
    sort_order: str = Query("desc", description="Напрямок сортування (asc/desc)"),
    limit: int = Query(50, ge=1, le=200, description="Кількість результатів"),
    skip: int = Query(0, ge=0, description="Пропустити результатів"),
):
    """Пошук оголошень OLX з фільтрами."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    
    repo = OlxListingsRepository()
    filters = _build_olx_filters(region, city, price_min, price_max, price_eq)
    
    # Визначаємо поле для сортування
    if sort_field == "updated_at":
        actual_sort_field = "updated_at"
        sort_direction = -1 if sort_order == "desc" else 1
        docs = repo.find_many(
            filter=filters,
            sort=[(actual_sort_field, sort_direction)],
            skip=skip,
            limit=limit
        )
    elif sort_field == "price":
        # Для сортування за ціною отримуємо всі документи та сортуємо в пам'яті
        all_docs = repo.find_many(filter=filters, sort=None, skip=None, limit=None)
        all_docs.sort(
            key=lambda x: x.get("search_data", {}).get("price_value", 0) or 0,
            reverse=(sort_order == "desc")
        )
        docs = all_docs[skip:skip + limit]
    elif sort_field == "title":
        # Для сортування за назвою також сортуємо в пам'яті
        all_docs = repo.find_many(filter=filters, sort=None, skip=None, limit=None)
        all_docs.sort(
            key=lambda x: (x.get("search_data", {}).get("title", "") or "").lower(),
            reverse=(sort_order == "desc")
        )
        docs = all_docs[skip:skip + limit]
    else:
        actual_sort_field = "updated_at"
        sort_direction = -1 if sort_order == "desc" else 1
        docs = repo.find_many(
            filter=filters,
            sort=[(actual_sort_field, sort_direction)],
            skip=skip,
            limit=limit
        )
    
    total = repo.collection.count_documents(filters)
    
    return {
        "items": [_normalize_olx_doc(doc) for doc in docs],
        "total": total,
        "limit": limit,
        "skip": skip,
    }


@router.get("/olx/filters/regions")
def get_olx_regions(request: Request):
    """Отримує список унікальних областей з OLX."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
        regions_list = geography_service.get_all_regions()
        if regions_list:
            return {"regions": [r["name"] for r in regions_list]}
    except Exception:
        pass
    repo = OlxListingsRepository()
    regions = set()
    try:
        pipeline = [
            {"$match": {"detail.address_refs": {"$exists": True, "$ne": []}}},
            {"$unwind": "$detail.address_refs"},
            {"$match": {"detail.address_refs.region.name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$detail.address_refs.region.name"}},
            {"$sort": {"_id": 1}},
        ]
        for item in repo.collection.aggregate(pipeline):
            if item.get("_id"):
                regions.add(item["_id"])
    except Exception:
        pass
    if not regions:
        try:
            pipeline = [
                {"$match": {"detail.resolved_locations": {"$exists": True, "$ne": []}}},
                {"$unwind": "$detail.resolved_locations"},
                {"$unwind": "$detail.resolved_locations.results"},
                {"$match": {"detail.resolved_locations.results.address_structured.region": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$detail.resolved_locations.results.address_structured.region"}},
                {"$sort": {"_id": 1}},
            ]
            for item in repo.collection.aggregate(pipeline):
                if item.get("_id"):
                    regions.add(item["_id"])
        except Exception:
            pass
    if not regions:
        try:
            pipeline = [
                {"$match": {"search_data.location": {"$exists": True, "$ne": None, "$ne": ""}}},
                {"$group": {"_id": "$search_data.location"}},
                {"$sort": {"_id": 1}},
            ]
            for item in repo.collection.aggregate(pipeline):
                loc = item.get("_id") or ""
                if not isinstance(loc, str):
                    continue
                part = loc.split(",")[-1].strip() if "," in loc else loc.strip()
                if part:
                    regions.add(part)
        except Exception:
            pass
    return {"regions": sorted(list(regions)) if regions else []}


@router.get("/olx/filters/cities")
def get_olx_cities(request: Request, region: Optional[str] = Query(None)):
    """Отримує список унікальних міст з OLX, опціонально фільтрованих за областю."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
        if region:
            region_obj = geography_service.regions_repo.find_by_name(region)
            if region_obj:
                cities_list = geography_service.get_cities_by_region(str(region_obj["_id"]))
                if cities_list:
                    return {"cities": [c["name"] for c in cities_list]}
        else:
            all_regions = geography_service.get_all_regions()
            all_cities = []
            for r in all_regions:
                cities_list = geography_service.get_cities_by_region(str(r["_id"]))
                all_cities.extend([c["name"] for c in cities_list])
            if all_cities:
                return {"cities": sorted(set(all_cities))}
    except Exception:
        pass
    repo = OlxListingsRepository()
    cities = set()
    try:
        match_stage = {"detail.address_refs": {"$exists": True, "$ne": []}}
        if region:
            match_stage["detail.address_refs.region.name"] = {"$regex": region, "$options": "i"}
        pipeline = [
            {"$match": match_stage},
            {"$unwind": "$detail.address_refs"},
            {"$match": {"detail.address_refs.city.name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$detail.address_refs.city.name"}},
            {"$sort": {"_id": 1}},
        ]
        for item in repo.collection.aggregate(pipeline):
            if item.get("_id"):
                cities.add(item["_id"])
    except Exception:
        pass
    if not cities:
        try:
            match_stage = {"detail.resolved_locations": {"$exists": True, "$ne": []}}
            pipeline = [
                {"$match": match_stage},
                {"$unwind": "$detail.resolved_locations"},
                {"$unwind": "$detail.resolved_locations.results"},
                {"$match": {"detail.resolved_locations.results.address_structured.city": {"$exists": True, "$ne": None}}},
            ]
            if region:
                pipeline.append({"$match": {"detail.resolved_locations.results.address_structured.region": {"$regex": region, "$options": "i"}}})
            pipeline.extend([{"$group": {"_id": "$detail.resolved_locations.results.address_structured.city"}}, {"$sort": {"_id": 1}}])
            for item in repo.collection.aggregate(pipeline):
                if item.get("_id"):
                    cities.add(item["_id"])
        except Exception:
            pass
    if not cities:
        try:
            pipeline = [
                {"$match": {"search_data.location": {"$exists": True, "$ne": None, "$ne": ""}}},
                {"$group": {"_id": "$search_data.location"}},
            ]
            for item in repo.collection.aggregate(pipeline):
                loc = item.get("_id") or ""
                if not isinstance(loc, str):
                    continue
                parts = [p.strip() for p in loc.split(",", 1) if p.strip()]
                city_part = parts[0] if parts else ""
                region_part = parts[1] if len(parts) > 1 else ""
                if not city_part:
                    continue
                if region:
                    if not region_part or region.lower() not in region_part.lower():
                        continue
                cities.add(city_part)
        except Exception:
            pass
    return {"cities": sorted(list(cities)) if cities else []}


@router.get("/prozorro")
def search_prozorro(
    request: Request,
    region: Optional[str] = Query(None, description="Фільтр за областю"),
    city: Optional[str] = Query(None, description="Фільтр за містом"),
    price_min: Optional[float] = Query(None, description="Мінімальна ціна"),
    price_max: Optional[float] = Query(None, description="Максимальна ціна"),
    price_eq: Optional[float] = Query(None, description="Точна ціна"),
    sort_field: str = Query("updated_at", description="Поле для сортування"),
    sort_order: str = Query("desc", description="Напрямок сортування (asc/desc)"),
    limit: int = Query(50, ge=1, le=200, description="Кількість результатів"),
    skip: int = Query(0, ge=0, description="Пропустити результатів"),
):
    """Пошук аукціонів Prozorro з фільтрами."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    
    repo = ProZorroAuctionsRepository()
    filters = _build_prozorro_filters(region, city, price_min, price_max, price_eq)
    
    # Визначаємо поле для сортування
    # Для Prozorro використовуємо last_updated як основне поле
    if sort_field == "updated_at":
        actual_sort_field = "last_updated"
    elif sort_field == "price":
        # Для сортування за ціною потрібно використовувати aggregation або інший підхід
        # Поки що сортуємо за last_updated
        actual_sort_field = "last_updated"
    elif sort_field == "title":
        # Для сортування за назвою також потрібен aggregation
        actual_sort_field = "last_updated"
    else:
        actual_sort_field = "last_updated"
    
    sort_direction = -1 if sort_order == "desc" else 1
    
    docs = repo.find_many(
        filter=filters,
        sort=[(actual_sort_field, sort_direction)],
        skip=skip,
        limit=limit
    )
    
    # Якщо потрібно сортувати за ціною або назвою, робимо це в пам'яті після отримання даних
    if sort_field == "price":
        docs.sort(key=lambda x: x.get("auction_data", {}).get("value", {}).get("amount", 0), reverse=(sort_order == "desc"))
    elif sort_field == "title":
        def get_title(doc):
            auction_data = doc.get("auction_data", {})
            title = auction_data.get("title", {})
            if isinstance(title, dict):
                return title.get("uk_UA", "") or auction_data.get("id", "")
            return str(title) or auction_data.get("id", "")
        docs.sort(key=get_title, reverse=(sort_order == "desc"))
    
    total = repo.collection.count_documents(filters)
    
    return {
        "items": [_normalize_prozorro_doc(doc) for doc in docs],
        "total": total,
        "limit": limit,
        "skip": skip,
    }


@router.get("/unified")
def search_unified(
    request: Request,
    region: Optional[str] = Query(None, description="Фільтр за областю"),
    city: Optional[str] = Query(None, description="Фільтр за містом"),
    price_min: Optional[float] = Query(None, description="Мінімальна ціна"),
    price_max: Optional[float] = Query(None, description="Максимальна ціна"),
    price_eq: Optional[float] = Query(None, description="Точна ціна"),
    source: Optional[str] = Query(None, description="Фільтр за джерелом (olx/prozorro)"),
    property_type: Optional[str] = Query(None, description="Тип оголошення (neruhomist/zemelna_dilyanka/...)"),
    building_area_sqm_op: Optional[str] = Query(None, description="Оператор площі нерухомості (eq/gte/lte)"),
    building_area_sqm_value: Optional[float] = Query(None, description="Значення площі нерухомості (кв. м.)"),
    land_area_ha_op: Optional[str] = Query(None, description="Оператор площі землі (eq/gte/lte)"),
    land_area_ha_value: Optional[float] = Query(None, description="Значення площі землі (га)"),
    date_filter_days: Optional[int] = Query(None, description="Фільтр за датою: 1, 7 або 30 днів"),
    price_per_m2_min: Optional[float] = Query(None),
    price_per_m2_max: Optional[float] = Query(None),
    price_per_m2_currency: Optional[str] = Query("uah"),
    price_per_ha_min: Optional[float] = Query(None),
    price_per_ha_max: Optional[float] = Query(None),
    price_per_ha_currency: Optional[str] = Query("uah"),
    sort_field: str = Query("source_updated_at", description="Поле для сортування"),
    sort_order: str = Query("desc", description="Напрямок сортування (asc/desc)"),
    limit: int = Query(50, ge=1, le=200, description="Кількість результатів"),
    skip: int = Query(0, ge=0, description="Пропустити результатів"),
):
    """Пошук за зведеною таблицею (OLX + ProZorro) з фільтрами."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    repo = UnifiedListingsRepository()
    filters = _build_unified_filters(
        region=region,
        city=city,
        price_min=price_min,
        price_max=price_max,
        price_eq=price_eq,
        source=source,
        property_type=property_type,
        building_area_sqm_op=building_area_sqm_op,
        building_area_sqm_value=building_area_sqm_value,
        land_area_ha_op=land_area_ha_op,
        land_area_ha_value=land_area_ha_value,
        date_filter_days=date_filter_days,
        price_per_m2_min=price_per_m2_min,
        price_per_m2_max=price_per_m2_max,
        price_per_m2_currency=price_per_m2_currency,
        price_per_ha_min=price_per_ha_min,
        price_per_ha_max=price_per_ha_max,
        price_per_ha_currency=price_per_ha_currency,
    )

    # Маппінг sort_field на MongoDB поле
    sort_field_map = {
        "source_updated_at": "source_updated_at",
        "updated_at": "source_updated_at",
        "price": "price_uah",
        "title": "title",
    }
    actual_sort = sort_field_map.get(sort_field, "source_updated_at")
    sort_direction = -1 if sort_order == "desc" else 1

    docs = repo.find_many(
        filter=filters,
        sort=[(actual_sort, sort_direction)],
        skip=skip,
        limit=limit,
    )

    if sort_field == "title":
        docs.sort(
            key=lambda x: (x.get("title", "") or "").lower(),
            reverse=(sort_order == "desc"),
        )

    total = repo.collection.count_documents(filters)

    items = [_normalize_unified_doc(doc) for doc in docs]
    try:
        from business.services.price_analytics_service import PriceAnalyticsService
        analytics = PriceAnalyticsService()
        indicators = analytics.get_price_indicators_for_items(items)
        for item in items:
            cid = f"{item.get('source', '')}:{item.get('source_id', '')}"
            if cid in indicators:
                item["price_indicator"] = indicators[cid]
    except Exception:
        pass

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "skip": skip,
    }


class ExportSearchRequest(BaseModel):
    """Параметри експорту результатів пошуку."""
    source: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    price_eq: Optional[float] = None
    property_type: Optional[str] = None
    building_area_sqm_op: Optional[str] = None
    building_area_sqm_value: Optional[float] = None
    land_area_ha_op: Optional[str] = None
    land_area_ha_value: Optional[float] = None
    date_filter_days: Optional[int] = None
    price_per_m2_min: Optional[float] = None
    price_per_m2_max: Optional[float] = None
    price_per_m2_currency: Optional[str] = "uah"
    price_per_ha_min: Optional[float] = None
    price_per_ha_max: Optional[float] = None
    price_per_ha_currency: Optional[str] = "uah"
    sort_field: str = "source_updated_at"
    sort_order: str = "desc"


@router.post("/export")
def export_search_results(request: Request, body: ExportSearchRequest):
    """
    Експортує результати пошуку у форматі зведеної таблиці (Excel).
    """
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    filters = _build_unified_filters(
        region=body.region,
        city=body.city,
        price_min=body.price_min,
        price_max=body.price_max,
        price_eq=body.price_eq,
        source=body.source,
        property_type=body.property_type,
        building_area_sqm_op=body.building_area_sqm_op,
        building_area_sqm_value=body.building_area_sqm_value,
        land_area_ha_op=body.land_area_ha_op,
        land_area_ha_value=body.land_area_ha_value,
        date_filter_days=body.date_filter_days,
        price_per_m2_min=body.price_per_m2_min,
        price_per_m2_max=body.price_per_m2_max,
        price_per_m2_currency=body.price_per_m2_currency,
        price_per_ha_min=body.price_per_ha_min,
        price_per_ha_max=body.price_per_ha_max,
        price_per_ha_currency=body.price_per_ha_currency,
    )

    repo = UnifiedListingsRepository()
    sort_field_map = {"source_updated_at": "source_updated_at", "price": "price_uah", "title": "title"}
    actual_sort = sort_field_map.get(body.sort_field, "source_updated_at")
    sort_direction = -1 if body.sort_order == "desc" else 1

    docs = repo.find_many(
        filter=filters,
        sort=[(actual_sort, sort_direction)],
        limit=10000,
        skip=0,
    )

    from domain.gateways.listing_gateway import ListingGateway
    from utils.file_utils import generate_excel_in_memory

    columns = [
        "source", "source_id", "status", "property_type", "building_area_sqm", "land_area_ha",
        "title", "description", "page_url", "price_uah", "price_usd", "addresses", "source_updated_at",
    ]
    headers = {
        "source": "Джерело", "source_id": "ID", "status": "Статус", "property_type": "Тип",
        "building_area_sqm": "Площа, м²", "land_area_ha": "Площа, га", "title": "Назва",
        "description": "Опис", "page_url": "Посилання", "price_uah": "Ціна, грн", "price_usd": "Ціна, $",
        "addresses": "Адреса", "source_updated_at": "Оновлено",
    }

    gateway = ListingGateway()
    coll = gateway.collection_from_raw_docs(docs, "unified_listings")
    rows = coll.to_export_rows(columns)
    if not rows:
        rows = [{"title": "Немає даних"}]
        columns = ["title"]
        headers = {"title": "Назва"}

    excel_bytes = generate_excel_in_memory(rows, columns, headers)
    content = excel_bytes.getvalue()

    from urllib.parse import quote
    filename = "Зведена_таблиця.xlsx"
    encoded = quote(filename, safe="")
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded}"},
    )


@router.post("/send-export-via-bot")
def send_export_via_bot(request: Request, body: ExportSearchRequest):
    """
    Експортує результати пошуку та надсилає Excel через бота.
    Для мобільних застосунків, де пряме скачування не працює.
    """
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    filters = _build_unified_filters(
        region=body.region,
        city=body.city,
        price_min=body.price_min,
        price_max=body.price_max,
        price_eq=body.price_eq,
        source=body.source,
        property_type=body.property_type,
        building_area_sqm_op=body.building_area_sqm_op,
        building_area_sqm_value=body.building_area_sqm_value,
        land_area_ha_op=body.land_area_ha_op,
        land_area_ha_value=body.land_area_ha_value,
        date_filter_days=body.date_filter_days,
        price_per_m2_min=body.price_per_m2_min,
        price_per_m2_max=body.price_per_m2_max,
        price_per_m2_currency=body.price_per_m2_currency,
        price_per_ha_min=body.price_per_ha_min,
        price_per_ha_max=body.price_per_ha_max,
        price_per_ha_currency=body.price_per_ha_currency,
    )

    repo = UnifiedListingsRepository()
    sort_field_map = {"source_updated_at": "source_updated_at", "price": "price_uah", "title": "title"}
    actual_sort = sort_field_map.get(body.sort_field, "source_updated_at")
    sort_direction = -1 if body.sort_order == "desc" else 1

    docs = repo.find_many(
        filter=filters,
        sort=[(actual_sort, sort_direction)],
        limit=10000,
        skip=0,
    )

    from domain.gateways.listing_gateway import ListingGateway
    from utils.file_utils import generate_excel_in_memory

    columns = [
        "source", "source_id", "status", "property_type", "building_area_sqm", "land_area_ha",
        "title", "description", "page_url", "price_uah", "price_usd", "addresses", "source_updated_at",
    ]
    headers = {
        "source": "Джерело", "source_id": "ID", "status": "Статус", "property_type": "Тип",
        "building_area_sqm": "Площа, м²", "land_area_ha": "Площа, га", "title": "Назва",
        "description": "Опис", "page_url": "Посилання", "price_uah": "Ціна, грн", "price_usd": "Ціна, $",
        "addresses": "Адреса", "source_updated_at": "Оновлено",
    }

    gateway = ListingGateway()
    coll = gateway.collection_from_raw_docs(docs, "unified_listings")
    rows = coll.to_export_rows(columns)
    if not rows:
        rows = [{"title": "Немає даних"}]
        columns = ["title"]
        headers = {"title": "Назва"}

    excel_bytes = generate_excel_in_memory(rows, columns, headers)
    content = excel_bytes.getvalue()

    filename = "Зведена_таблиця.xlsx"
    bot_token = getattr(request.app.state, "bot_token", None) or ""
    from telegram_mini_app.send_via_bot import send_file_via_telegram
    ok = send_file_via_telegram(user_id, content, filename, bot_token)
    if not ok:
        raise HTTPException(status_code=500, detail="Не вдалося надіслати файл через бота")
    return {"success": True, "message": "Файл надіслано в чат бота"}


@router.get("/unified/filters/regions")
def get_unified_regions(request: Request):
    """Отримує список унікальних областей з зведеної таблиці (через GeographyService)."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
        regions_list = geography_service.get_all_regions()
        if regions_list:
            return {"regions": [r["name"] for r in regions_list]}
    except Exception:
        pass

    # Fallback: агрегація з unified_listings
    repo = UnifiedListingsRepository()
    regions = set()
    try:
        pipeline = [
            {"$match": {"addresses": {"$exists": True, "$ne": []}}},
            {"$unwind": "$addresses"},
            {"$match": {"addresses.region": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$addresses.region"}},
            {"$sort": {"_id": 1}},
        ]
        for item in repo.collection.aggregate(pipeline):
            if item.get("_id"):
                regions.add(item["_id"])
    except Exception:
        pass

    return {"regions": sorted(list(regions)) if regions else []}


# Міста зі спеціальним статусом (не входять до складу областей)
_CITIES_WITH_SPECIAL_STATUS = ["Київ", "Севастополь"]


@router.get("/unified/filters/cities")
def get_unified_cities(request: Request, region: Optional[str] = Query(None)):
    """Отримує список унікальних міст з зведеної таблиці."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    # Київ та Севастополь — міста зі спеціальним статусом; при виборі регіону "Київ" повертаємо місто
    if region and region.strip() in _CITIES_WITH_SPECIAL_STATUS:
        return {"cities": [region.strip()]}

    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
        if region:
            region_obj = geography_service.regions_repo.find_by_name(region)
            if region_obj:
                cities_list = geography_service.get_cities_by_region(str(region_obj["_id"]))
                if cities_list:
                    return {"cities": [c["name"] for c in cities_list]}
        else:
            all_regions = geography_service.get_all_regions()
            all_cities = []
            for r in all_regions:
                cities_list = geography_service.get_cities_by_region(str(r["_id"]))
                all_cities.extend([c["name"] for c in cities_list])
            if all_cities:
                result = sorted(set(all_cities))
                # Додаємо міста зі спеціальним статусом, якщо їх ще немає
                for city in _CITIES_WITH_SPECIAL_STATUS:
                    if city not in result:
                        result.append(city)
                        result.sort()
                return {"cities": result}
    except Exception:
        pass

    # Fallback: агрегація з unified_listings
    repo = UnifiedListingsRepository()
    cities = set()
    try:
        match_stage: Dict[str, Any] = {"addresses.settlement": {"$exists": True, "$ne": None}}
        if region:
            match_stage["addresses.region"] = {"$regex": region, "$options": "i"}
        pipeline = [
            {"$match": match_stage},
            {"$unwind": "$addresses"},
            {"$match": {"addresses.settlement": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$addresses.settlement"}},
            {"$sort": {"_id": 1}},
        ]
        for item in repo.collection.aggregate(pipeline):
            if item.get("_id"):
                cities.add(item["_id"])
    except Exception:
        pass

    result = sorted(list(cities)) if cities else []
    # Додаємо міста зі спеціальним статусом при завантаженні без області
    if not region:
        for city in _CITIES_WITH_SPECIAL_STATUS:
            if city not in result:
                result.append(city)
                result.sort()
    return {"cities": result}


@router.get("/unified-detail")
def get_unified_detail(
    request: Request,
    source: str = Query(..., description="Джерело: olx або prozorro"),
    source_id: str = Query(..., description="ID в джерелі (URL для OLX, auction_id для ProZorro)"),
):
    """Отримує деталі оголошення з unified_listings з індикатором ціни."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    if source.lower() not in ("olx", "prozorro"):
        raise HTTPException(status_code=400, detail="source must be olx or prozorro")

    repo = UnifiedListingsRepository()
    doc = repo.find_by_source_id(source.lower(), source_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Item not found")

    item = _normalize_unified_doc(doc)
    try:
        from business.services.price_analytics_service import PriceAnalyticsService
        analytics = PriceAnalyticsService()
        indicators = analytics.get_price_indicators_for_items([item])
        cid = f"{item.get('source', '')}:{item.get('source_id', '')}"
        if cid in indicators:
            item["price_indicator"] = indicators[cid]
    except Exception:
        pass
    item["_detail_type"] = "unified"
    return item


@router.get("/usage-analysis")
def get_usage_analysis(
    request: Request,
    source: str = Query(..., description="Джерело: olx або prozorro"),
    source_id: str = Query(..., description="ID в джерелі (URL для OLX, auction_id для ProZorro)"),
):
    """Отримує попередній аналіз використання об'єкта."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    if source.lower() not in ("olx", "prozorro"):
        raise HTTPException(status_code=400, detail="source must be olx or prozorro")

    try:
        from business.services.property_usage_analysis_service import PropertyUsageAnalysisService
        svc = PropertyUsageAnalysisService()
        analysis = svc.get_or_create_analysis(source.lower(), source_id)
        if analysis.get("error"):
            return {"existing_usage": [], "usage_suggestions": [], "geo_analysis": {}}
        return {
            "existing_usage": analysis.get("existing_usage", []),
            "usage_suggestions": analysis.get("usage_suggestions", []),
            "geo_analysis": analysis.get("geo_analysis", {}),
        }
    except Exception as e:
        return {"existing_usage": [], "usage_suggestions": [], "geo_analysis": {}, "error": str(e)}


def _unified_olx_to_olx_like(unified_doc: Dict[str, Any], url: str) -> Dict[str, Any]:
    """Конвертує документ unified_listings (source=olx) у формат olx_listings для renderDetail."""
    addresses = unified_doc.get("addresses", [])
    location_parts = []
    if addresses and isinstance(addresses, list):
        for addr in addresses:
            if isinstance(addr, dict):
                settlement = addr.get("settlement")
                region = addr.get("region")
                if settlement:
                    location_parts.append(settlement)
                if region and region not in location_parts:
                    location_parts.append(region)
                if location_parts:
                    break
    location_str = " - ".join(location_parts) if location_parts else ""

    price_uah = unified_doc.get("price_uah")
    price_text = f"{int(price_uah):,} грн".replace(",", " ") if price_uah is not None else ""

    price_metrics = {}
    if unified_doc.get("price_usd") is not None:
        price_metrics["total_price_usd"] = unified_doc["price_usd"]
    if unified_doc.get("price_per_m2_uah") is not None:
        price_metrics["price_per_m2_uah"] = unified_doc["price_per_m2_uah"]
    if unified_doc.get("price_per_m2_usd") is not None:
        price_metrics["price_per_m2_usd"] = unified_doc["price_per_m2_usd"]
    if unified_doc.get("price_per_ha_uah") is not None:
        price_metrics["price_per_ha_uah"] = unified_doc["price_per_ha_uah"]
    if unified_doc.get("price_per_ha_usd") is not None:
        price_metrics["price_per_ha_usd"] = unified_doc["price_per_ha_usd"]

    floor = unified_doc.get("floor") or ""
    tags = unified_doc.get("tags")
    if not isinstance(tags, list):
        tags = []
    llm = {"floor": floor, "tags": tags} if (floor or tags) else {}

    return {
        "url": url,
        "search_data": {
            "title": unified_doc.get("title", ""),
            "location": location_str,
            "price_value": price_uah,
            "price_text": price_text,
            "area_m2": unified_doc.get("building_area_sqm"),
        },
        "detail": {
            "location": location_str,
            "description": unified_doc.get("description", ""),
            "price_metrics": price_metrics if price_metrics else None,
            "llm": llm if llm else None,
        },
    }


def _build_item_for_price_indicator(doc: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
    """Будує item для get_price_indicators_for_items з doc olx або prozorro."""
    if source == "olx":
        search_data = doc.get("search_data", {})
        detail = doc.get("detail", {})
        price_metrics = detail.get("price_metrics") or {}
        city, region = None, None
        loc = search_data.get("location", "")
        if isinstance(loc, str):
            parts = [p.strip() for p in loc.replace(" - ", ",").split(",")]
            if parts:
                city = parts[0]
            if len(parts) > 1:
                region = parts[1]
        return {
            "source": "olx",
            "source_id": doc.get("url", ""),
            "city": city,
            "region": region,
            "price_uah": search_data.get("price_value"),
            "price_per_m2_uah": price_metrics.get("price_per_m2_uah"),
            "price_per_ha_uah": price_metrics.get("price_per_ha_uah"),
        }
    if source == "prozorro":
        auction_data = doc.get("auction_data", {})
        city, region = None, None
        refs = auction_data.get("address_refs", [])
        if refs and isinstance(refs[0], dict):
            r = refs[0]
            if isinstance(r.get("city"), dict):
                city = r["city"].get("name")
            if isinstance(r.get("region"), dict):
                region = r["region"].get("name")
        if not city and not region:
            items = auction_data.get("items", [])
            if items and isinstance(items[0], dict):
                addr = items[0].get("address", {})
                if isinstance(addr, dict):
                    loc = addr.get("locality", {})
                    reg = addr.get("region", {})
                    if isinstance(loc, dict):
                        city = loc.get("uk_UA", "")
                    if isinstance(reg, dict):
                        region = (reg.get("uk_UA", "") or "").replace(" область", "").replace(" обл.", "").strip()
        value = auction_data.get("value", {})
        price = value.get("amount") if isinstance(value, dict) else None
        pm = auction_data.get("price_metrics") or {}
        return {
            "source": "prozorro",
            "source_id": doc.get("auction_id", ""),
            "city": city,
            "region": region,
            "price_uah": price,
            "price_per_m2_uah": pm.get("price_per_m2_uah"),
            "price_per_ha_uah": pm.get("price_per_ha_uah"),
        }
    return None


@router.get("/olx/{item_id:path}")
def get_olx_item(request: Request, item_id: str):
    """Отримує детальну інформацію про оголошення OLX. Якщо в olx_listings немає — fallback на unified_listings."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    olx_repo = OlxListingsRepository()
    doc = None

    if item_id.startswith("http://") or item_id.startswith("https://"):
        doc = olx_repo.find_by_url(item_id)
    else:
        try:
            doc = olx_repo.find_by_id(item_id)
        except Exception:
            pass

    if not doc and (item_id.startswith("http://") or item_id.startswith("https://")):
        unified_repo = UnifiedListingsRepository()
        for variant in _olx_url_variants(item_id):
            unified_doc = unified_repo.find_by_source_id("olx", variant)
            if unified_doc:
                url = unified_doc.get("source_id") or unified_doc.get("page_url") or item_id
                doc = _unified_olx_to_olx_like(unified_doc, url)
                break

    if not doc:
        raise HTTPException(status_code=404, detail="Item not found")

    try:
        item = _build_item_for_price_indicator(doc, "olx")
        if item:
            from business.services.price_analytics_service import PriceAnalyticsService
            analytics = PriceAnalyticsService()
            indicators = analytics.get_price_indicators_for_items([item])
            cid = f"olx:{item.get('source_id', '')}"
            if cid in indicators:
                doc["price_indicator"] = indicators[cid]
    except Exception:
        pass
    return doc


@router.get("/prozorro/{item_id}")
def get_prozorro_item(request: Request, item_id: str):
    """Отримує детальну інформацію про аукціон Prozorro."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    
    repo = ProZorroAuctionsRepository()
    
    # Спробуємо знайти за auction_id або _id
    doc = None
    
    doc = repo.find_by_auction_id(item_id)
    if not doc:
        try:
            doc = repo.find_by_id(item_id)
        except Exception:
            pass
    
    if not doc:
        raise HTTPException(status_code=404, detail="Item not found")

    try:
        item = _build_item_for_price_indicator(doc, "prozorro")
        if item:
            from business.services.price_analytics_service import PriceAnalyticsService
            analytics = PriceAnalyticsService()
            indicators = analytics.get_price_indicators_for_items([item])
            cid = f"prozorro:{item.get('source_id', '')}"
            if cid in indicators:
                doc["price_indicator"] = indicators[cid]
    except Exception:
        pass
    return doc


@router.get("/prozorro/filters/regions")
def get_prozorro_regions(request: Request):
    """Отримує список унікальних областей з Prozorro."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    
    # Спочатку пробуємо отримати з колекції regions
    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
        regions_list = geography_service.get_all_regions()
        if regions_list:
            return {"regions": [r["name"] for r in regions_list]}
    except Exception:
        pass
    
    # Fallback: отримуємо з prozorro_auctions
    repo = ProZorroAuctionsRepository()
    regions = set()
    
    try:
        # Спочатку з address_refs (якщо вони є в auction_data)
        pipeline = [
            {"$match": {"auction_data.address_refs": {"$exists": True, "$ne": []}}},
            {"$unwind": "$auction_data.address_refs"},
            {"$match": {"auction_data.address_refs.region.name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$auction_data.address_refs.region.name"}},
            {"$sort": {"_id": 1}},
        ]
        
        for item in repo.collection.aggregate(pipeline):
            if item.get("_id"):
                regions.add(item["_id"])
    except Exception:
        pass
    
    # Якщо немає address_refs, пробуємо items.address
    if not regions:
        try:
            pipeline = [
                {"$match": {"auction_data.items.address.region.uk_UA": {"$exists": True, "$ne": None}}},
                {"$unwind": "$auction_data.items"},
                {"$match": {"auction_data.items.address.region.uk_UA": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$auction_data.items.address.region.uk_UA"}},
                {"$sort": {"_id": 1}},
            ]
            
            regions_raw = [item["_id"] for item in repo.collection.aggregate(pipeline) if item.get("_id")]
            # Прибираємо " область" та " обл."
            for r in regions_raw:
                normalized = r.replace(" область", "").replace(" обл.", "").strip()
                if normalized:
                    regions.add(normalized)
        except Exception:
            pass
    
    return {"regions": sorted(list(regions)) if regions else []}


@router.get("/prozorro/filters/cities")
def get_prozorro_cities(request: Request, region: Optional[str] = Query(None)):
    """Отримує список унікальних міст з Prozorro, опціонально фільтрованих за областю."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    
    # Спочатку пробуємо отримати з колекції cities
    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
        
        if region:
            # Знаходимо область за назвою
            region_obj = geography_service.regions_repo.find_by_name(region)
            if region_obj:
                cities_list = geography_service.get_cities_by_region(str(region_obj["_id"]))
                if cities_list:
                    return {"cities": [c["name"] for c in cities_list]}
        else:
            # Якщо область не вказана, отримуємо всі міста
            all_regions = geography_service.get_all_regions()
            all_cities = []
            for r in all_regions:
                cities_list = geography_service.get_cities_by_region(str(r["_id"]))
                all_cities.extend([c["name"] for c in cities_list])
            if all_cities:
                return {"cities": sorted(set(all_cities))}
    except Exception:
        pass
    
    # Fallback: отримуємо з prozorro_auctions
    repo = ProZorroAuctionsRepository()
    cities = set()
    
    try:
        # Спочатку з address_refs
        match_stage = {"auction_data.address_refs": {"$exists": True, "$ne": []}}
        if region:
            match_stage["auction_data.address_refs.region.name"] = {"$regex": region, "$options": "i"}
        
        pipeline = [
            {"$match": match_stage},
            {"$unwind": "$auction_data.address_refs"},
            {"$match": {"auction_data.address_refs.city.name": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$auction_data.address_refs.city.name"}},
            {"$sort": {"_id": 1}},
        ]
        
        for item in repo.collection.aggregate(pipeline):
            if item.get("_id"):
                cities.add(item["_id"])
    except Exception:
        pass
    
    # Якщо немає address_refs, пробуємо items.address
    if not cities:
        try:
            match_stage = {"auction_data.items.address.locality.uk_UA": {"$exists": True, "$ne": None}}
            if region:
                # Шукаємо з " область" та без
                match_stage["$or"] = [
                    {"auction_data.items.address.region.uk_UA": {"$regex": region, "$options": "i"}},
                    {"auction_data.items.address.region.uk_UA": {"$regex": region + " область", "$options": "i"}},
                ]
            
            pipeline = [
                {"$match": match_stage},
                {"$unwind": "$auction_data.items"},
                {"$match": {"auction_data.items.address.locality.uk_UA": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$auction_data.items.address.locality.uk_UA"}},
                {"$sort": {"_id": 1}},
            ]
            
            for item in repo.collection.aggregate(pipeline):
                if item.get("_id"):
                    cities.add(item["_id"])
        except Exception:
            pass
    
    return {"cities": sorted(list(cities)) if cities else []}
