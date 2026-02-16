# -*- coding: utf-8 -*-
"""
Міграція 020: Додавання полів площі (building_area_sqm, land_area_ha) в unified_listings.

Додає обидва поля у ВСІ записи. Якщо є сирі дані — витягує площу; якщо немає — ставить null.
Логіка витягування збігається з UnifiedListingsService._extract_area_info.

Запуск: py scripts/migrations/020_unified_listings_area_fields.py
Або через run_migrations (викликає run_migration()).
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository


def _convert_to_sqm(value: float, unit_ua: str) -> float:
    """Конвертує площу в м²."""
    if not value:
        return 0.0
    u = (unit_ua or "").lower()
    if any(x in u for x in ["м²", "м2", "кв.м", "кв м", "квадратний метр"]):
        return float(value)
    if any(x in u for x in ["гектар", "hectare", "га"]):
        return float(value) * 10000.0
    if any(x in u for x in ["сотка", "соток", "ar"]):
        return float(value) * 100.0
    return float(value)


def _convert_to_hectares(value: float, unit_ua: str) -> float:
    """Конвертує площу в га."""
    if not value:
        return 0.0
    u = (unit_ua or "").lower()
    if any(x in u for x in ["гектар", "hectare", "га"]):
        return float(value)
    if any(x in u for x in ["м²", "м2", "кв.м", "кв м", "квадратний метр"]):
        return float(value) * 0.0001
    if any(x in u for x in ["сотка", "соток", "ar"]):
        return float(value) * 0.01
    return float(value) * 0.0001


def _extract_area_from_raw_doc(doc: Dict[str, Any], source: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Витягує площу (м² та га) з сирого документа.
    Повертає (building_area_sqm, land_area_ha).
    """
    building_area_sqm = None
    land_area_ha = None

    if source == "olx":
        detail = doc.get("detail", {})
        search_data = doc.get("search_data", {})
        llm = detail.get("llm", {})

        building_area = llm.get("building_area_sqm")
        land_area = llm.get("land_area_ha")

        if building_area is not None:
            try:
                building_area_sqm = float(building_area)
            except (ValueError, TypeError):
                pass

        if land_area is not None:
            try:
                land_area_ha = float(land_area)
            except (ValueError, TypeError):
                pass

        # Fallback: area_m2 як building_area — тільки якщо немає land_area_ha.
        # Для земельних ділянок area_m2 часто є площею землі в м².
        if (
            not building_area_sqm
            and not land_area_ha
            and search_data.get("area_m2") is not None
        ):
            try:
                building_area_sqm = float(search_data["area_m2"])
            except (ValueError, TypeError):
                pass

    elif source == "prozorro":
        auction_data = doc.get("auction_data", {})
        items = auction_data.get("items", [])

        total_building_sqm = 0.0
        total_land_ha = 0.0

        for item in items:
            if not isinstance(item, dict):
                continue

            # 1. Пріоритет: itemProps (ProZorro.Sale)
            item_props = item.get("itemProps") or {}
            if isinstance(item_props, dict):
                item_props_type = item_props.get("itemPropsType", "")
                classification = item.get("classification", {}) or {}
                class_id = (classification.get("id") or "") if isinstance(classification, dict) else ""

                unit_ua = ""
                unit = item.get("unit")
                if isinstance(unit, dict):
                    unit_name = unit.get("name")
                    if isinstance(unit_name, dict):
                        unit_ua = (unit_name.get("uk_UA") or "").lower()
                    elif isinstance(unit_name, str):
                        unit_ua = unit_name.lower()
                elif isinstance(unit, str):
                    unit_ua = unit.lower()

                if item_props_type == "land" or (class_id and class_id.startswith("06")):
                    land_area = item_props.get("landArea")
                    if land_area is not None:
                        try:
                            val = float(land_area)
                            if val > 0:
                                total_land_ha += _convert_to_hectares(val, unit_ua)
                        except (ValueError, TypeError):
                            pass
                    continue

                building_area = (
                    item_props.get("totalObjectArea")
                    or item_props.get("totalBuildingArea")
                    or item_props.get("usableArea")
                )
                if building_area is not None:
                    try:
                        val = float(building_area)
                        if val > 0:
                            total_building_sqm += _convert_to_sqm(val, unit_ua)
                    except (ValueError, TypeError):
                        pass
                    continue

            # 2. Fallback: quantity.value
            quantity = item.get("quantity")
            qty_value = None
            if isinstance(quantity, dict):
                qty_value = quantity.get("value")
            elif isinstance(quantity, (int, float)):
                qty_value = quantity

            unit = item.get("unit")
            unit_name = unit.get("name") if isinstance(unit, dict) else None
            unit_ua = ""
            if isinstance(unit_name, dict):
                unit_ua = (unit_name.get("uk_UA") or "").lower()
            elif isinstance(unit_name, str):
                unit_ua = unit_name.lower()

            if qty_value is not None:
                try:
                    qty_float = float(qty_value)
                    if qty_float > 0:
                        if unit_ua:
                            if "м²" in unit_ua or "кв.м" in unit_ua:
                                total_building_sqm += qty_float
                            elif "га" in unit_ua or "гектар" in unit_ua:
                                total_land_ha += qty_float
                        elif qty_float > 1000:
                            total_land_ha += qty_float / 10000.0
                        else:
                            total_building_sqm += qty_float
                except (ValueError, TypeError):
                    pass

        if total_building_sqm > 0:
            building_area_sqm = total_building_sqm
        if total_land_ha > 0:
            land_area_ha = total_land_ha

    return (building_area_sqm, land_area_ha)


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 020: Додавання полів площі в unified_listings")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)

        unified_repo = UnifiedListingsRepository()
        olx_repo = OlxListingsRepository()
        prozorro_repo = ProZorroAuctionsRepository()

        unified_repo._ensure_indexes()

        cursor = unified_repo.collection.find({})
        total = unified_repo.collection.count_documents({})
        print(f"Знайдено записів у unified_listings: {total}")

        updated_count = 0
        skipped_count = 0
        error_count = 0

        for doc in cursor:
            source = doc.get("source")
            source_id = doc.get("source_id")
            if not source or not source_id:
                skipped_count += 1
                continue

            raw_doc = None
            if source == "olx":
                raw_doc = olx_repo.find_by_url(source_id)
            elif source == "prozorro":
                raw_doc = prozorro_repo.find_by_auction_id(source_id)

            building_area_sqm = None
            land_area_ha = None
            if raw_doc:
                building_area_sqm, land_area_ha = _extract_area_from_raw_doc(raw_doc, source)

            try:
                update_data = {
                    "$set": {
                        "building_area_sqm": building_area_sqm,
                        "land_area_ha": land_area_ha,
                    }
                }
                result = unified_repo.collection.update_one(
                    {"_id": doc["_id"]},
                    update_data,
                )
                if result.modified_count > 0:
                    updated_count += 1

            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  Помилка обробки {source}:{source_id}: {e}")

        print(f"\nМіграція завершена:")
        print(f"  - Оновлено: {updated_count}")
        print(f"  - Пропущено (немає source/source_id): {skipped_count}")
        print(f"  - Помилок: {error_count}")

        return True
    except Exception as e:
        print(f"Помилка міграції 020: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
