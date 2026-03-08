# -*- coding: utf-8 -*-
"""
Міграція 015: Заповнення полів ціни та адреси в оголошеннях OLX.

Для існуючих записів у olx_listings заповнює:
- detail.price.value, detail.price.currency — з search_data (price_value, currency)
- detail.price_metrics — якщо відсутні: розрахунок з price_value, площі (м²/га), курс USD
- detail.llm.parsed_address.formatted_address — повна адреса з address_refs, llm.addresses або search_data.location

Запуск: py scripts/migrations/015_olx_price_and_address_fields.py
Або через run_migrations (викликає run_migration()).
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from bson import ObjectId
from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository
from business.services.geography_service import GeographyService
from utils.price_metrics import compute_price_metrics

# Курс USD для історичних даних (міграція без мережі)
DEFAULT_UAH_PER_USD = 41.0


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace(",", ".")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _formatted_address_from_refs(address_refs_list: List[Dict], geography_service: GeographyService) -> str:
    """Збирає повну адресу з масиву address_refs (кожен елемент — словник region, city, street, building)."""
    if not address_refs_list:
        return ""
    parts = []
    for refs in address_refs_list:
        if isinstance(refs, dict):
            addr = geography_service.format_address(refs)
            if addr and addr not in parts:
                parts.append(addr)
    return "; ".join(parts) if parts else ""


def _formatted_address_from_llm_addresses(addresses: List[Dict]) -> str:
    """Збирає адресу з detail.llm.addresses (об'єкти з region, settlement, street тощо)."""
    from scripts.olx_scraper.helpers import _address_line_from_llm_address
    if not addresses:
        return ""
    parts = []
    for addr in addresses:
        if isinstance(addr, dict):
            line = _address_line_from_llm_address(addr)
            if line and line not in parts:
                parts.append(line)
    return "; ".join(parts) if parts else ""


def migrate_olx_listings(
    repo: OlxListingsRepository,
    geography_service: GeographyService,
    uah_per_usd: float = DEFAULT_UAH_PER_USD,
    limit: Optional[int] = None,
) -> int:
    """
    Проходить по olx_listings, заповнює detail.price, detail.price_metrics (якщо потрібно),
    detail.llm.parsed_address.formatted_address.
    """
    filter_query = {}
    docs = repo.find_many(filter=filter_query, sort=[("updated_at", -1)], limit=limit, skip=None)
    total = len(docs)
    print(f"Обробка записів olx_listings: {total}")

    updated_count = 0
    for doc in docs:
        try:
            doc_id = doc.get("_id")
            if not doc_id:
                continue
            if isinstance(doc_id, str):
                try:
                    doc_id = ObjectId(doc_id)
                except Exception:
                    continue

            search_data = doc.get("search_data") or {}
            detail = doc.get("detail")
            if detail is None:
                detail = {}
            updates: Dict[str, Any] = {}

            # --- detail.price (value, currency) ---
            price_value = search_data.get("price_value")
            currency = (search_data.get("currency") or "UAH").strip().upper() or "UAH"
            if currency not in ("UAH", "USD", "EUR"):
                currency = "UAH"
            if _to_float(price_value) is not None:
                if "detail.price" not in updates:
                    updates["detail.price"] = {"value": _to_float(price_value), "currency": currency}

            # --- detail.price_metrics (якщо відсутні) ---
            existing_metrics = (detail or {}).get("price_metrics") if isinstance(detail, dict) else {}
            if not existing_metrics or not isinstance(existing_metrics, dict):
                total_uah = _to_float(price_value)
                if total_uah is not None and total_uah > 0:
                    llm = (detail or {}).get("llm") or {}
                    detail_structured = (detail or {}).get("detail_structured") or {}
                    total_area_m2 = _to_float(llm.get("total_area_m2")) or _to_float(search_data.get("area_m2")) or _to_float(detail_structured.get("total_area_m2"))
                    land_area_ha = _to_float(llm.get("land_area_ha"))
                    # Якщо ціна в USD/EUR — для метрик в UAH потрібен курс; для міграції вважаємо UAH
                    if currency != "UAH":
                        total_uah = None  # не рахуємо метрики в UAH без курсу
                    if total_uah is not None:
                        metrics = compute_price_metrics(
                            total_price_uah=total_uah,
                            building_area_sqm=total_area_m2,
                            land_area_ha=land_area_ha,
                            uah_per_usd=uah_per_usd,
                        )
                        updates["detail.price_metrics"] = metrics

            # --- detail.llm.parsed_address.formatted_address ---
            formatted = ""
            address_refs = (detail or {}).get("address_refs") if isinstance(detail, dict) else []
            if address_refs and isinstance(address_refs, list):
                formatted = _formatted_address_from_refs(address_refs, geography_service)
            if not formatted:
                llm = (detail or {}).get("llm") or {}
                addresses = llm.get("addresses") or []
                if addresses:
                    formatted = _formatted_address_from_llm_addresses(addresses)
            if not formatted:
                loc = (search_data.get("location") or "").strip()
                if loc:
                    formatted = loc

            if formatted:
                # Поле може бути detail.llm.parsed_address.formatted_address; якщо llm немає — створимо структуру
                updates["detail.llm.parsed_address"] = {"formatted_address": formatted}

            if updates:
                repo.collection.update_one(
                    {"_id": doc_id},
                    {"$set": updates},
                )
                updated_count += 1
        except Exception as e:
            print(f"Помилка обробки {doc.get('url', 'unknown')}: {e}")
            continue

    print(f"Оновлено записів: {updated_count}")
    return updated_count


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 015: OLX — поля ціни та адреси (detail.price, price_metrics, parsed_address)")
    print("=" * 60)
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = OlxListingsRepository()
        geography_service = GeographyService()
        uah = DEFAULT_UAH_PER_USD
        migrate_olx_listings(repo, geography_service, uah_per_usd=uah)
        print("Міграція 015 завершена успішно.")
        return True
    except Exception as e:
        print(f"Помилка міграції 015: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
