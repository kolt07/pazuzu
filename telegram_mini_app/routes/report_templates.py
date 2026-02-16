# -*- coding: utf-8 -*-
"""
API: шаблони звітів користувачів.
Список, створення, видалення, зміна порядку, генерація звіту, генерація назви через LLM.
"""

import base64
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Request, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from telegram_mini_app.auth import validate_telegram_init_data
from business.services.report_template_service import ReportTemplateService
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from domain.gateways.listing_gateway import ListingGateway

router = APIRouter(prefix="/api/report-templates", tags=["report-templates"])

# Імпортуємо _build_unified_filters з search
from telegram_mini_app.routes.search import _build_unified_filters


def _get_user_id(request: Request) -> int:
    """Валідує користувача та повертає user_id."""
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
    user_id = int(user_obj.get("id", 0))
    user_service = request.app.state.user_service
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")
    return user_id


def _get_report_template_service(request: Request) -> ReportTemplateService:
    """Повертає ReportTemplateService (леніва ініціалізація)."""
    if not hasattr(request.app.state, "report_template_service"):
        request.app.state.report_template_service = ReportTemplateService()
    return request.app.state.report_template_service


@router.get("/")
def list_templates(request: Request):
    """Список шаблонів звітів користувача."""
    user_id = _get_user_id(request)
    service = _get_report_template_service(request)
    return {"templates": service.list_templates(user_id)}


class CreateTemplateRequest(BaseModel):
    name: str
    params: Dict[str, Any]


@router.post("/")
def create_template(request: Request, body: CreateTemplateRequest):
    """Створює шаблон звіту."""
    user_id = _get_user_id(request)
    service = _get_report_template_service(request)
    template_id = service.create_template(user_id=user_id, name=body.name, params=body.params)
    return {"template_id": template_id, "name": body.name}


class GenerateNameRequest(BaseModel):
    params: Dict[str, Any]


@router.post("/generate-name")
def generate_template_name(request: Request, body: GenerateNameRequest):
    """Генерує назву шаблону через LLM на основі параметрів."""
    _get_user_id(request)
    service = _get_report_template_service(request)
    name = service.generate_template_name(body.params)
    return {"name": name}


@router.delete("/{template_id}")
def delete_template(request: Request, template_id: str):
    """Видаляє шаблон (не для системних)."""
    user_id = _get_user_id(request)
    service = _get_report_template_service(request)
    ok = service.delete_template(template_id, user_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Неможливо видалити системний шаблон або шаблон не знайдено")
    return {"deleted": True}


class ReorderRequest(BaseModel):
    template_ids: List[str]


@router.post("/reorder")
def reorder_templates(request: Request, body: ReorderRequest):
    """Змінює порядок шаблонів."""
    user_id = _get_user_id(request)
    service = _get_report_template_service(request)
    service.reorder_templates(user_id, body.template_ids or [])
    return {"ok": True}


def _params_to_search_filters(params: Dict[str, Any]) -> tuple:
    """
    Перетворює params шаблону на аргументи для _build_unified_filters.
    Повертає (filters_dict, sort_field, sort_order).
    """
    price = params.get("price") or {}
    price_per_m2 = params.get("price_per_m2") or {}
    price_per_ha = params.get("price_per_ha") or {}

    price_min = price.get("min") if isinstance(price, dict) else None
    price_max = price.get("max") if isinstance(price, dict) else None
    price_eq = price.get("value") if isinstance(price, dict) and price.get("op") == "eq" else None
    if isinstance(price, dict) and price.get("op") == "gte":
        price_min = price.get("value")
    elif isinstance(price, dict) and price.get("op") == "lte":
        price_max = price.get("value")

    filters = _build_unified_filters(
        region=params.get("region"),
        city=params.get("city"),
        price_min=price_min,
        price_max=price_max,
        price_eq=price_eq,
        source=params.get("source") or None,
        property_type=params.get("property_type") or None,
        building_area_sqm_op=params.get("building_area_sqm", {}).get("op") if isinstance(params.get("building_area_sqm"), dict) else None,
        building_area_sqm_value=params.get("building_area_sqm", {}).get("value") if isinstance(params.get("building_area_sqm"), dict) else None,
        land_area_ha_op=params.get("land_area_ha", {}).get("op") if isinstance(params.get("land_area_ha"), dict) else None,
        land_area_ha_value=params.get("land_area_ha", {}).get("value") if isinstance(params.get("land_area_ha"), dict) else None,
        date_filter_days=params.get("date_filter"),
        price_per_m2_min=price_per_m2.get("min") if isinstance(price_per_m2, dict) else None,
        price_per_m2_max=price_per_m2.get("max") if isinstance(price_per_m2, dict) else None,
        price_per_m2_currency=price_per_m2.get("currency", "uah") if isinstance(price_per_m2, dict) else "uah",
        price_per_ha_min=price_per_ha.get("min") if isinstance(price_per_ha, dict) else None,
        price_per_ha_max=price_per_ha.get("max") if isinstance(price_per_ha, dict) else None,
        price_per_ha_currency=price_per_ha.get("currency", "uah") if isinstance(price_per_ha, dict) else "uah",
    )
    sort_field = params.get("sort_field", "source_updated_at")
    sort_order = params.get("sort_order", "desc")
    return filters, sort_field, sort_order


class GenerateReportRequest(BaseModel):
    send_via_bot: bool = False


@router.post("/{template_id}/generate")
def generate_report_from_template(
    request: Request,
    template_id: str,
    body: GenerateReportRequest = Body(default_factory=GenerateReportRequest),
):
    """
    Генерує звіт за шаблоном. Повертає Excel у base64 або download_url для мобільних.
    Якщо send_via_bot=true — надсилає файл через бота замість повернення.
    """
    user_id = _get_user_id(request)
    service = _get_report_template_service(request)
    template = service.get_template(template_id, user_id)
    if not template:
        raise HTTPException(status_code=404, detail="Шаблон не знайдено")

    params = dict(template.get("params", {}))
    if template.get("is_default") and params.get("output_format") != "tabs_by_source":
        params["output_format"] = "tabs_by_source"
    filters, sort_field, sort_order = _params_to_search_filters(params)

    repo = UnifiedListingsRepository()
    sort_field_map = {
        "source_updated_at": "source_updated_at",
        "price": "price_uah",
        "title": "title",
    }
    actual_sort = sort_field_map.get(sort_field, "source_updated_at")
    sort_direction = -1 if sort_order == "desc" else 1

    docs = repo.find_many(
        filter=filters,
        sort=[(actual_sort, sort_direction)],
        limit=10000,
        skip=0,
    )

    output_format = params.get("output_format", "unified_table")
    columns = [
        "source", "source_id", "status", "property_type", "building_area_sqm", "land_area_ha",
        "title", "description", "page_url", "price_uah", "price_usd", "addresses", "source_updated_at",
    ]
    headers = {
        "source": "Джерело",
        "source_id": "ID",
        "status": "Статус",
        "property_type": "Тип",
        "building_area_sqm": "Площа, м²",
        "land_area_ha": "Площа, га",
        "title": "Назва",
        "description": "Опис",
        "page_url": "Посилання",
        "price_uah": "Ціна, грн",
        "price_usd": "Ціна, $",
        "addresses": "Адреса",
        "source_updated_at": "Оновлено",
    }

    if output_format == "tabs_by_source":
        # Окремі вкладки для OLX та ProZorro
        from utils.file_utils import generate_excel_with_sheets
        olx_docs = [d for d in docs if d.get("source") == "olx"]
        prozorro_docs = [d for d in docs if d.get("source") == "prozorro"]
        gateway = ListingGateway()
        sheets = []
        if olx_docs:
            coll = gateway.collection_from_raw_docs(olx_docs, "unified_listings")
            rows = coll.to_export_rows(columns)
            sheets.append(("OLX", rows, columns, headers))
        if prozorro_docs:
            coll = gateway.collection_from_raw_docs(prozorro_docs, "unified_listings")
            rows = coll.to_export_rows(columns)
            sheets.append(("ProZorro", rows, columns, headers))
        if not sheets:
            sheets.append(("Дані", [{"title": "Немає даних"}], ["title"], {"title": "Назва"}))
        excel_bytes = generate_excel_with_sheets(sheets)
    else:
        # Єдина таблиця
        gateway = ListingGateway()
        coll = gateway.collection_from_raw_docs(docs, "unified_listings")
        rows = coll.to_export_rows(columns)
        if not rows:
            rows = [{"title": "Немає даних"}]
            columns = ["title"]
            headers = {"title": "Назва"}
        from utils.file_utils import generate_excel_in_memory
        excel_bytes = generate_excel_in_memory(rows, columns, headers)

    content_bytes = excel_bytes.getvalue() if hasattr(excel_bytes, "getvalue") else excel_bytes.read()
    filename = f"Звіт_{template.get('name', 'report')}.xlsx"

    if body and body.send_via_bot:
        bot_token = getattr(request.app.state, "bot_token", None) or ""
        from telegram_mini_app.send_via_bot import send_file_via_telegram
        ok = send_file_via_telegram(user_id, content_bytes, filename, bot_token)
        if not ok:
            raise HTTPException(status_code=500, detail="Не вдалося надіслати файл через бота")
        return {"success": True, "message": "Файл надіслано в чат бота", "rows_count": len(docs)}

    file_b64 = base64.b64encode(content_bytes).decode("utf-8")
    download_token = str(uuid.uuid4())
    if not hasattr(request.app.state, "report_download_tokens"):
        request.app.state.report_download_tokens = {}
    request.app.state.report_download_tokens[download_token] = {
        "bytes": content_bytes,
        "user_id": user_id,
        "filename": filename,
    }

    return {
        "success": True,
        "format": "xlsx",
        "data": file_b64,
        "encoding": "base64",
        "download_url": f"/api/report-templates/download?token={download_token}",
        "rows_count": len(docs),
    }


@router.get("/download")
def download_generated_report(request: Request, token: str):
    """Завантажує згенерований звіт за одноразовим токеном (для мобільних)."""
    tokens = getattr(request.app.state, "report_download_tokens", {})
    if token not in tokens:
        raise HTTPException(status_code=404, detail="Посилання недійсне або прострочене")
    stored = tokens.pop(token)
    content = stored.get("bytes")
    filename = stored.get("filename", "report.xlsx")
    if not content:
        raise HTTPException(status_code=404, detail="Файл не знайдено")
    from urllib.parse import quote
    encoded = quote(filename, safe="")
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded}"},
    )
