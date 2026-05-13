# -*- coding: utf-8 -*-
"""
Репозиторій земельних ділянок з кадастру (kadastrova-karta.com).
Ідентифікатор — cadastral_number.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_parcels"
SOURCE_KADASTROVA = "kadastrova-karta"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Повертає документ з _id у вигляді рядка."""
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class CadastralParcelsRepository(BaseRepository):
    """Робота з колекцією земельних ділянок з кадастру."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_cadastral_number(self, cadastral_number: str) -> Optional[Dict[str, Any]]:
        """Знаходить ділянку за кадастровим номером."""
        if not cadastral_number or not str(cadastral_number).strip():
            return None
        doc = self.collection.find_one({"cadastral_number": str(cadastral_number).strip()})
        return _normalize_doc(doc)

    def upsert_parcel(
        self,
        cadastral_number: str,
        parcel_data: Dict[str, Any],
        source_cell_id: Optional[str] = None,
    ) -> bool:
        """
        Створює або оновлює ділянку за кадастровим номером.
        parcel_data: bounds, purpose, purpose_label, category, area_sqm, ownership_form, тощо.
        """
        if not cadastral_number or not str(cadastral_number).strip():
            return False
        cadastral_number = str(cadastral_number).strip()
        now = datetime.now(timezone.utc)

        doc: Dict[str, Any] = {
            "cadastral_number": cadastral_number,
            "source": SOURCE_KADASTROVA,
            "fetched_at": now,
            **{k: v for k, v in parcel_data.items() if v is not None},
        }
        if source_cell_id:
            doc["source_cell_id"] = source_cell_id

        result = self.collection.update_one(
            {"cadastral_number": cadastral_number},
            {"$set": doc},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def upsert_many(
        self,
        parcels: List[Dict[str, Any]],
        source_cell_id: Optional[str] = None,
    ) -> int:
        """
        Масовий upsert ділянок. Повертає кількість успішно збережених.
        Кожен елемент parcels має містити cadastral_number та інші поля.
        """
        if not parcels:
            return 0
        now = datetime.now(timezone.utc)
        count = 0
        for p in parcels:
            cn = (p.get("cadastral_number") or "").strip()
            if not cn:
                continue
            doc = {
                "cadastral_number": cn,
                "source": SOURCE_KADASTROVA,
                "fetched_at": now,
                **{k: v for k, v in p.items() if k != "cadastral_number" and v is not None},
            }
            if source_cell_id:
                doc["source_cell_id"] = source_cell_id
            try:
                result = self.collection.update_one(
                    {"cadastral_number": cn},
                    {"$set": doc},
                    upsert=True,
                )
                if result.upserted_id or result.modified_count:
                    count += 1
            except Exception:
                pass  # Пропускаємо ділянки з невалідною геометрією (2dsphere)
        return count

    def ensure_index(self) -> None:
        """Створює індекси (міграція вже створює їх, але для явного виклику)."""
        self.collection.create_index("cadastral_number", unique=True)
        self.collection.create_index([("bounds", "2dsphere")])
        self.collection.create_index("source_cell_id")
        self.collection.create_index("source")

    def count_total(self) -> int:
        """Загальна кількість ділянок (точна, може бути повільною на великих колекціях)."""
        return self.collection.count_documents({})

    def count_total_estimated(self) -> int:
        """Орієнтовна кількість ділянок (O(1), для прогрес-бару)."""
        return self.collection.estimated_document_count()

    def find_within_radius(
        self,
        latitude: float,
        longitude: float,
        radius_meters: float,
        purpose: Optional[str] = None,
        purpose_label: Optional[str] = None,
        ownership_form: Optional[str] = None,
        category: Optional[str] = None,
        purpose_contains: Optional[str] = None,
        purpose_label_contains: Optional[str] = None,
        ownership_form_contains: Optional[str] = None,
        category_contains: Optional[str] = None,
        area_sqm_min: Optional[float] = None,
        area_sqm_max: Optional[float] = None,
        purpose_codes_in: Optional[List[str]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Знаходить кадастрові ділянки у радіусі від точки.

        Використовує `$geoWithin` + `$centerSphere` по полю `bounds`
        (2dsphere). radius переводиться з метрів у радіани (поділ на радіус
        Землі 6 378 100 м).

        `purpose`/`purpose_label`/`ownership_form`/`category` — точне співпадіння
        (рядки, case-sensitive). Для нечіткого пошуку (substring/regex,
        case-insensitive) використовуй парні `*_contains`. У реальних даних
        `purpose_label` зберігається як повна фраза («Для будівництва та
        обслуговування будівель торгівлі»), тому fuzz-варіанти набагато
        зручніші для агента.

        `purpose_codes_in` — список конкретних КВЦПЗ-кодів (напр.
        `["03.07", "12.04"]`). На рівні MongoDB перетворюється у `$in`, що
        дає ефективну server-side фільтрацію перед застосуванням
        `$geoWithin` limit (інакше ми б ризикували відсікти потрібні
        ділянки в щільних локаціях, де геопошук уже сам по собі повертає
        тисячі документів).
        """
        try:
            lat = float(latitude)
            lng = float(longitude)
            radius_m = max(0.0, float(radius_meters))
        except (TypeError, ValueError):
            return []
        if radius_m <= 0:
            return []
        radius_rad = radius_m / 6378100.0

        import re as _re

        criteria: Dict[str, Any] = {
            "bounds": {
                "$geoWithin": {"$centerSphere": [[lng, lat], radius_rad]},
            },
        }
        if purpose:
            criteria["purpose"] = str(purpose).strip()
        elif purpose_codes_in:
            codes = [str(c).strip() for c in purpose_codes_in if c and str(c).strip()]
            if codes:
                criteria["purpose"] = {"$in": codes}
        if purpose_label:
            criteria["purpose_label"] = str(purpose_label).strip()
        if ownership_form:
            criteria["ownership_form"] = str(ownership_form).strip()
        if category:
            criteria["category"] = str(category).strip()
        if purpose_contains:
            criteria["purpose"] = {"$regex": _re.escape(str(purpose_contains).strip()), "$options": "i"}
        if purpose_label_contains:
            criteria["purpose_label"] = {"$regex": _re.escape(str(purpose_label_contains).strip()), "$options": "i"}
        if ownership_form_contains:
            criteria["ownership_form"] = {"$regex": _re.escape(str(ownership_form_contains).strip()), "$options": "i"}
        if category_contains:
            criteria["category"] = {"$regex": _re.escape(str(category_contains).strip()), "$options": "i"}

        area_filter: Dict[str, Any] = {}
        if area_sqm_min is not None:
            try:
                area_filter["$gte"] = float(area_sqm_min)
            except (TypeError, ValueError):
                pass
        if area_sqm_max is not None:
            try:
                area_filter["$lte"] = float(area_sqm_max)
            except (TypeError, ValueError):
                pass
        if area_filter:
            criteria["area_sqm"] = area_filter

        cursor = self.collection.find(criteria).limit(max(1, int(limit or 50)))
        return [_normalize_doc(d) for d in cursor if d]

    def find_intersecting_polygon(
        self,
        polygon_geojson: Dict[str, Any],
        purpose: Optional[str] = None,
        purpose_label: Optional[str] = None,
        ownership_form: Optional[str] = None,
        category: Optional[str] = None,
        purpose_contains: Optional[str] = None,
        purpose_label_contains: Optional[str] = None,
        ownership_form_contains: Optional[str] = None,
        category_contains: Optional[str] = None,
        area_sqm_min: Optional[float] = None,
        area_sqm_max: Optional[float] = None,
        purpose_codes_in: Optional[List[str]] = None,
        skip: int = 0,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        Ділянки, чиї `bounds` перетинаються з заданим GeoJSON Polygon/MultiPolygon ($geoIntersects).
        """
        if not isinstance(polygon_geojson, dict) or not polygon_geojson.get("type"):
            return []
        geom_type = polygon_geojson.get("type")
        if geom_type not in ("Polygon", "MultiPolygon"):
            return []

        import re as _re

        criteria: Dict[str, Any] = {
            "bounds": {"$geoIntersects": {"$geometry": polygon_geojson}},
        }
        if purpose:
            criteria["purpose"] = str(purpose).strip()
        elif purpose_codes_in:
            codes = [str(c).strip() for c in purpose_codes_in if c and str(c).strip()]
            if codes:
                criteria["purpose"] = {"$in": codes}
        if purpose_label:
            criteria["purpose_label"] = str(purpose_label).strip()
        if ownership_form:
            criteria["ownership_form"] = str(ownership_form).strip()
        if category:
            criteria["category"] = str(category).strip()
        if purpose_contains:
            criteria["purpose"] = {"$regex": _re.escape(str(purpose_contains).strip()), "$options": "i"}
        if purpose_label_contains:
            criteria["purpose_label"] = {"$regex": _re.escape(str(purpose_label_contains).strip()), "$options": "i"}
        if ownership_form_contains:
            criteria["ownership_form"] = {"$regex": _re.escape(str(ownership_form_contains).strip()), "$options": "i"}
        if category_contains:
            criteria["category"] = {"$regex": _re.escape(str(category_contains).strip()), "$options": "i"}

        area_filter: Dict[str, Any] = {}
        if area_sqm_min is not None:
            try:
                area_filter["$gte"] = float(area_sqm_min)
            except (TypeError, ValueError):
                pass
        if area_sqm_max is not None:
            try:
                area_filter["$lte"] = float(area_sqm_max)
            except (TypeError, ValueError):
                pass
        if area_filter:
            criteria["area_sqm"] = area_filter

        sk = max(0, int(skip or 0))
        lim = max(1, int(limit or 500))
        cursor = self.collection.find(criteria).skip(sk).limit(lim)
        return [_normalize_doc(d) for d in cursor if d]

    def count_intersecting_polygon(
        self,
        polygon_geojson: Dict[str, Any],
        purpose_codes_in: Optional[List[str]] = None,
        purpose_label_contains: Optional[str] = None,
        ownership_form: Optional[str] = None,
        area_sqm_min: Optional[float] = None,
        area_sqm_max: Optional[float] = None,
    ) -> int:
        """Підрахунок ділянок у полігоні (без завантаження повних документів)."""
        if not isinstance(polygon_geojson, dict) or not polygon_geojson.get("type"):
            return 0
        if polygon_geojson.get("type") not in ("Polygon", "MultiPolygon"):
            return 0

        import re as _re

        criteria: Dict[str, Any] = {
            "bounds": {"$geoIntersects": {"$geometry": polygon_geojson}},
        }
        if purpose_codes_in:
            codes = [str(c).strip() for c in purpose_codes_in if c and str(c).strip()]
            if codes:
                criteria["purpose"] = {"$in": codes}
        if purpose_label_contains:
            criteria["purpose_label"] = {
                "$regex": _re.escape(str(purpose_label_contains).strip()),
                "$options": "i",
            }
        if ownership_form:
            criteria["ownership_form"] = str(ownership_form).strip()
        area_filter: Dict[str, Any] = {}
        if area_sqm_min is not None:
            try:
                area_filter["$gte"] = float(area_sqm_min)
            except (TypeError, ValueError):
                pass
        if area_sqm_max is not None:
            try:
                area_filter["$lte"] = float(area_sqm_max)
            except (TypeError, ValueError):
                pass
        if area_filter:
            criteria["area_sqm"] = area_filter
        try:
            return int(self.collection.count_documents(criteria))
        except Exception:
            return 0

    def find_many_by_cadastral_numbers(
        self,
        numbers: Sequence[str],
        projection: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Повертає ділянки за списком кадастрових номерів (bounds обов'язкові для кластеризації)."""
        if not numbers:
            return []
        uniq = sorted({str(n).strip() for n in numbers if n and str(n).strip()})
        if not uniq:
            return []
        proj = projection or {
            "cadastral_number": 1,
            "bounds": 1,
            "purpose": 1,
            "purpose_label": 1,
            "ownership_form": 1,
            "area_sqm": 1,
        }
        out: List[Dict[str, Any]] = []
        batch = 5000
        try:
            for i in range(0, len(uniq), batch):
                chunk = uniq[i : i + batch]
                cur = self.collection.find({"cadastral_number": {"$in": chunk}}, proj)
                out.extend(_normalize_doc(d) for d in cur if d)
            return out
        except Exception:
            return out
