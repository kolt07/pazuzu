# -*- coding: utf-8 -*-
"""
Постпроцесор для оголошень OLX у MongoDB.

- Проходить по колекції `olx_listings`.
- Читає `detail.parameters` та `detail.location`.
- Розкладає ключові параметри (площа, поверх, тип об'єкта, опалення тощо) у структуроване поле
  `detail_structured`, зручне для аналітики.

Запуск з кореня проекту:
  py scripts/olx_scraper/postprocess_parameters.py
"""

import sys
from pathlib import Path
from typing import Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository


def normalize_parameters(params: Any) -> Dict[str, Any]:
    """
    Преобразує список detail.parameters у структуровані поля.
    Очікує список словників {"label": "...", "value": "..."}.
    """
    structured: Dict[str, Any] = {
        # Геометрія
        "total_area_m2": None,
        "floor": None,
        "floors_total": None,
        # Тип / розташування
        "object_type": None,
        "building_location": None,
        "distance_to_city": None,
        # Інженерія / стан
        "year_built_range": None,
        "bathroom": None,
        "heating": None,
        # Флаги
        "is_business": False,
        "no_commission": False,
        "co_rent": False,
        # Сирові значення для довільного аналізу
        "raw_flags": [],
    }

    if not isinstance(params, list):
        return structured

    for item in params:
        label = (item.get("label") or "").strip()
        value = (item.get("value") or "").strip()
        label_low = label.lower()

        # Флаги без двокрапки (Бізнес, Без комісії, Для спільної оренди тощо)
        if not value and label:
            if "бізнес" in label_low:
                structured["is_business"] = True
            if "без коміс" in label_low:
                structured["no_commission"] = True
            if "спільн" in label_low or "совмес" in label_low:
                structured["co_rent"] = True
            structured["raw_flags"].append(label)
            continue

        # Загальна площа
        if "загальна площа" in label_low:
            # Очікуємо щось типу "80 м²"
            import re

            m = re.search(r"([\d.,]+)", value)
            if m:
                try:
                    structured["total_area_m2"] = float(m.group(1).replace(",", ".").replace(" ", ""))
                except ValueError:
                    pass
            continue

        # Поверх
        if label_low.startswith("поверх"):
            try:
                structured["floor"] = int(value.split()[0])
            except Exception:
                structured["floor"] = value or structured["floor"]
            continue

        # Поверховість
        if "поверхов" in label_low:
            try:
                structured["floors_total"] = int(value.split()[0])
            except Exception:
                structured["floors_total"] = value or structured["floors_total"]
            continue

        # Тип об'єкта
        if "тип об'єкта" in label_low or "тип объекта" in label_low:
            structured["object_type"] = value or structured["object_type"]
            continue

        # Розташування (окрема будівля, в бізнес-центрі, в житловому будинку тощо)
        if label_low.startswith("розташув") or "расположен" in label_low:
            structured["building_location"] = value or structured["building_location"]
            continue

        # Відстань до міста
        if "відстань до найближчого міста" in label_low or "расстояние до ближайшего города" in label_low:
            structured["distance_to_city"] = value or structured["distance_to_city"]
            continue

        # Рік побудови / здачі
        if "рік побудови" in label_low or "год постройки" in label_low:
            structured["year_built_range"] = value or structured["year_built_range"]
            continue

        # Санвузол
        if "санвузол" in label_low or "санузел" in label_low:
            structured["bathroom"] = value or structured["bathroom"]
            continue

        # Опалення
        if "опалення" in label_low or "отопление" in label_low:
            structured["heating"] = value or structured["heating"]
            continue

    return structured


def enrich_with_location(structured: Dict[str, Any], detail: Dict[str, Any]) -> None:
    """Додає місто/область із detail.location у структуровані поля."""
    loc = detail.get("location") or {}
    if isinstance(loc, dict):
        city = loc.get("city")
        region = loc.get("region")
        if city:
            structured["city"] = city
        if region:
            structured["region"] = region


def main() -> None:
    settings = Settings()
    MongoDBConnection.initialize(settings)
    repo = OlxListingsRepository()

    collection = repo.collection
    total = collection.count_documents({})
    print(f"[OLX postprocess] Документів у olx_listings: {total}", flush=True)

    processed = 0
    cursor = collection.find({})
    for doc in cursor:
        detail = doc.get("detail") or {}
        params = detail.get("parameters") or []

        structured = normalize_parameters(params)
        enrich_with_location(structured, detail)

        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"detail_structured": structured}},
        )

        processed += 1
        if processed % 50 == 0:
            print(f"[OLX postprocess] Оновлено {processed}/{total}", flush=True)

    print(f"[OLX postprocess] Готово. Оновлено {processed} документів.", flush=True)


if __name__ == "__main__":
    main()

