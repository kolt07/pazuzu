# -*- coding: utf-8 -*-
"""
Репозиторії для роботи з географічними даними (області, міста, вулиці, будинки).
"""

import re
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timezone
from bson import ObjectId
from data.repositories.base_repository import BaseRepository
from utils.settlement_normalizer import (
    normalize_settlement_key,
    normalize_settlement_name,
    is_district_only_name,
)


def region_id_mongo_filter(region_id: str) -> Dict[str, Any]:
    """region_id у cities може бути str або ObjectId — шукаємо обидва варіанти."""
    values: List[Union[str, ObjectId]] = [region_id]
    try:
        oid = ObjectId(region_id)
        if oid not in values:
            values.append(oid)
    except Exception:
        pass
    if len(values) == 1:
        return {"region_id": values[0]}
    return {"region_id": {"$in": values}}


def active_cities_mongo_clause(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Фільтр активних НП у MongoDB.

    Враховує merged_into: null (поле є, значення null) та відсутність поля.
    """
    clause: Dict[str, Any] = {
        "$and": [
            {
                "$or": [
                    {"merged_into": {"$exists": False}},
                    {"merged_into": None},
                ]
            },
            {
                "$or": [
                    {"is_oblast_rayon_stub": {"$exists": False}},
                    {"is_oblast_rayon_stub": False},
                ],
            },
        ]
    }
    if extra:
        clause = {"$and": [clause, extra]}
    return clause


class RegionsRepository(BaseRepository):
    """Репозиторій для роботи з областями."""
    
    def __init__(self):
        super().__init__("regions")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index("name", unique=True)
            self.collection.create_index("name_normalized")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Знаходить область за назвою."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        return self.find_one({"name_normalized": normalized})
    
    def find_or_create(self, name: str) -> Dict[str, Any]:
        """Знаходить або створює область."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        
        existing = self.find_one({"name_normalized": normalized})
        if existing:
            return existing
        
        doc = {
            "name": name.strip(),
            "name_normalized": normalized,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Отримує всі області, відсортовані за назвою."""
        self._ensure_indexes()
        return self.find_many(sort=[("name", 1)])
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормалізує назву області для пошуку."""
        if not name:
            return ""
        # Прибираємо " область", " обл." та приводимо до нижнього регістру
        normalized = name.lower().strip()
        normalized = normalized.replace(" область", "").replace(" обл.", "").strip()
        return normalized


class CitiesRepository(BaseRepository):
    """Репозиторій для роботи з містами."""

    @staticmethod
    def _active_city_filter(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Документи, що представляють реальні НП (не stub після перенесення в oblast_rayons)."""
        return active_cities_mongo_clause(extra)

    def __init__(self):
        super().__init__("cities")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("region_id", 1), ("name_normalized", 1)], unique=True)
            self.collection.create_index("region_id")
            self.collection.create_index("name_normalized")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_name_and_region(self, name: str, region_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить місто за назвою та областю (exact + fuzzy)."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        if not normalized:
            return None

        existing = self.find_one({
            **self._active_city_filter(),
            "region_id": region_id,
            "name_normalized": normalized,
        })
        if existing:
            return existing

        by_alias = self.find_one({
            **self._active_city_filter(),
            "region_id": region_id,
            "search_aliases": normalized,
        })
        if by_alias:
            return by_alias

        return self.find_fuzzy_by_name_and_region(name, region_id)

    def find_fuzzy_by_name_and_region(
        self,
        name: str,
        region_id: str,
        *,
        fuzzy_threshold: float = 0.94,
    ) -> Optional[Dict[str, Any]]:
        """Fuzzy-пошук НП у межах області."""
        from business.services.settlement_matching_service import SettlementMatchingService

        normalized = self._normalize_name(name)
        if not normalized:
            return None

        candidates = self.find_many(
            filter=self._active_city_filter({"region_id": region_id}),
            sort=[("name", 1)],
        )
        match = SettlementMatchingService(fuzzy_threshold=fuzzy_threshold).find_best_match(
            name,
            region_id,
            candidates,
        )
        return match.city if match else None

    def find_or_create(self, name: str, region_id: str) -> Dict[str, Any]:
        """Знаходить або створює місто."""
        self._ensure_indexes()
        display_name = normalize_settlement_name(name)
        normalized = self._normalize_name(name)
        if not display_name or not normalized:
            raise ValueError(f"Некоректна назва населеного пункту: {name!r}")

        existing = self.find_by_name_and_region(display_name, region_id)
        if existing:
            return self._ensure_canonical_display(existing, display_name, normalized)

        doc = {
            "name": display_name,
            "name_normalized": normalized,
            "region_id": region_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc

    def _ensure_canonical_display(
        self,
        existing: Dict[str, Any],
        display_name: str,
        normalized: str,
    ) -> Dict[str, Any]:
        """Оновлює display/key, якщо знайдений дубль має кращу форму назви."""
        updates: Dict[str, Any] = {}
        if existing.get("name") != display_name:
            updates["name"] = display_name
        if existing.get("name_normalized") != normalized:
            updates["name_normalized"] = normalized
        if not updates:
            return existing

        updates["updated_at"] = datetime.now(timezone.utc)
        self.update_by_id(existing["_id"], {"$set": updates})
        existing.update(updates)
        return existing
    
    @staticmethod
    def _prefer_settlement_display(current: Optional[str], candidate: Optional[str]) -> bool:
        """True, якщо candidate кращий за current для показу (без префікса/ALL CAPS)."""
        if not candidate:
            return False
        if not current:
            return True
        cur, cand = current.strip(), candidate.strip()
        if cur == cand:
            return False
        prefix = re.compile(
            r"^(?:смт\.?|м\.|місто|с\.|село|селище|пгт\.?)\s+",
            re.IGNORECASE,
        )
        cur_has = bool(prefix.match(cur))
        cand_has = bool(prefix.match(cand))
        if cur_has and not cand_has:
            return True
        if not cur_has and cand_has:
            return False
        if cur == cur.upper() and cand != cand.upper():
            return True
        return False

    def get_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        """Отримує всі активні міста в області (один запис на ключ name_normalized)."""
        self._ensure_indexes()
        cities = self.find_many(
            filter=self._active_city_filter(region_id_mongo_filter(region_id)),
            sort=[("name", 1)],
        )
        by_key: Dict[str, Dict[str, Any]] = {}
        for doc in cities:
            raw_name = doc.get("name") or ""
            if is_district_only_name(raw_name):
                continue
            display = normalize_settlement_name(raw_name)
            key = normalize_settlement_key(display) if display else ""
            if not display or not key:
                continue
            doc["name"] = display
            doc["name_normalized"] = key
            if key not in by_key:
                by_key[key] = doc
            elif self._prefer_settlement_display(by_key[key].get("name"), display):
                by_key[key] = doc
        result = sorted(by_key.values(), key=lambda d: (d.get("name") or "").casefold())
        result.sort(
            key=lambda d: (d.get("population") is None, -(d.get("population") or 0)),
        )
        return result

    def find_matching_criteria(
        self,
        region_id: Optional[str] = None,
        *,
        population_gte: Optional[int] = None,
        population_lte: Optional[int] = None,
        population_gt: Optional[int] = None,
        population_lt: Optional[int] = None,
        area_gte: Optional[float] = None,
        area_lte: Optional[float] = None,
        area_gt: Optional[float] = None,
        area_lt: Optional[float] = None,
        settlement_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Повертає активні НП, що відповідають демографічним критеріям."""
        self._ensure_indexes()
        filt: Dict[str, Any] = dict(self._active_city_filter())
        if region_id:
            filt["region_id"] = region_id
        if settlement_name:
            key = self._normalize_name(settlement_name)
            if key:
                filt["$or"] = [
                    {"name_normalized": key},
                    {"search_aliases": key},
                ]
        pop_q: Dict[str, Any] = {}
        if population_gte is not None:
            pop_q["$gte"] = population_gte
        if population_lte is not None:
            pop_q["$lte"] = population_lte
        if population_gt is not None:
            pop_q["$gt"] = population_gt
        if population_lt is not None:
            pop_q["$lt"] = population_lt
        if pop_q:
            filt["population"] = pop_q
        area_q: Dict[str, Any] = {}
        if area_gte is not None:
            area_q["$gte"] = area_gte
        if area_lte is not None:
            area_q["$lte"] = area_lte
        if area_gt is not None:
            area_q["$gt"] = area_gt
        if area_lt is not None:
            area_q["$lt"] = area_lt
        if area_q:
            filt["area_sq_km"] = area_q
        return self.find_many(filter=filt, sort=[("population", -1)])

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормалізує назву міста для пошуку."""
        return normalize_settlement_key(name)


class StreetsRepository(BaseRepository):
    """Репозиторій для роботи з вулицями."""
    
    def __init__(self):
        super().__init__("streets")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("city_id", 1), ("name_normalized", 1)], unique=True)
            self.collection.create_index("city_id")
            self.collection.create_index("name_normalized")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_name_and_city(self, name: str, city_id: str, street_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Знаходить вулицю за назвою та містом."""
        self._ensure_indexes()
        normalized = self._normalize_name(name, street_type)
        return self.find_one({
            "city_id": city_id,
            "name_normalized": normalized
        })
    
    def find_or_create(self, name: str, city_id: str, street_type: Optional[str] = None) -> Dict[str, Any]:
        """Знаходить або створює вулицю."""
        self._ensure_indexes()
        normalized = self._normalize_name(name, street_type)
        
        existing = self.find_by_name_and_city(name, city_id, street_type)
        if existing:
            return existing
        
        doc = {
            "name": name.strip(),
            "name_normalized": normalized,
            "street_type": street_type.strip() if street_type else None,
            "city_id": city_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_by_city(self, city_id: str) -> List[Dict[str, Any]]:
        """Отримує всі вулиці в місті."""
        self._ensure_indexes()
        return self.find_many(
            filter={"city_id": city_id},
            sort=[("name", 1)]
        )
    
    @staticmethod
    def _normalize_name(name: str, street_type: Optional[str] = None) -> str:
        """Нормалізує назву вулиці для пошуку."""
        if not name:
            return ""
        normalized = name.lower().strip()
        # Якщо є тип вулиці, додаємо його до нормалізованої назви
        if street_type:
            normalized = street_type.lower().strip() + " " + normalized
        return normalized


class BuildingsRepository(BaseRepository):
    """Репозиторій для роботи з будинками."""
    
    def __init__(self):
        super().__init__("buildings")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("street_id", 1), ("number", 1)], unique=True)
            self.collection.create_index("street_id")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_number_and_street(self, number: str, street_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить будинок за номером та вулицею."""
        self._ensure_indexes()
        return self.find_one({
            "street_id": street_id,
            "number": number.strip()
        })
    
    def find_or_create(self, number: str, street_id: str, building_part: Optional[str] = None) -> Dict[str, Any]:
        """Знаходить або створює будинок."""
        self._ensure_indexes()
        
        existing = self.find_by_number_and_street(number, street_id)
        if existing:
            # Оновлюємо building_part якщо він не був встановлений
            if building_part and not existing.get("building_part"):
                self.update_by_id(existing["_id"], {
                    "$set": {
                        "building_part": building_part.strip(),
                        "updated_at": datetime.now(timezone.utc)
                    }
                })
                existing["building_part"] = building_part.strip()
            return existing
        
        doc = {
            "number": number.strip(),
            "building_part": building_part.strip() if building_part else None,
            "street_id": street_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_by_street(self, street_id: str) -> List[Dict[str, Any]]:
        """Отримує всі будинки на вулиці."""
        self._ensure_indexes()
        return self.find_many(
            filter={"street_id": street_id},
            sort=[("number", 1)]
        )


class OblastRayonsRepository(BaseRepository):
    """Адміністративні райони області (не місто, не громада в sense of circle)."""

    def __init__(self):
        super().__init__("oblast_rayons")
        self._indexes_created = False

    def _ensure_indexes(self):
        if self._indexes_created:
            return
        try:
            self.collection.create_index(
                [("region_id", 1), ("name_normalized", 1)],
                unique=True,
            )
            self.collection.create_index("region_id")
            self._indexes_created = True
        except Exception:
            pass

    def find_by_name_and_region(self, name: str, region_id: str) -> Optional[Dict[str, Any]]:
        from utils.rayon_normalizer import normalize_oblast_rayon_key

        self._ensure_indexes()
        key = normalize_oblast_rayon_key(name)
        if not key:
            return None
        return self.find_one({"region_id": region_id, "name_normalized": key})

    def find_or_create(self, name: str, region_id: str) -> Dict[str, Any]:
        from utils.rayon_normalizer import (
            format_oblast_rayon_display_name,
            normalize_oblast_rayon_key,
        )

        self._ensure_indexes()
        display = format_oblast_rayon_display_name(name)
        key = normalize_oblast_rayon_key(name)
        if not display or not key:
            raise ValueError(f"Некоректна назва району області: {name!r}")

        existing = self.find_by_name_and_region(display, region_id)
        if existing:
            return existing

        doc = {
            "name": display,
            "name_normalized": key,
            "region_id": region_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        doc_id = self.create(doc)
        created = self.find_by_id(doc_id)
        return created if created else doc

    def get_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        return self.find_many(filter={"region_id": region_id}, sort=[("name", 1)])


class GeoCirclesRepository(BaseRepository):
    """
    Округи / умовні групи топонімів: сільрада, «район» мегаполісу, рада тощо.
    Не область і не район області — окрема сутність для класифікації та зв'язків.
    """

    def __init__(self):
        super().__init__("geo_circles")
        self._indexes_created = False

    def _ensure_indexes(self):
        if self._indexes_created:
            return
        try:
            self.collection.create_index(
                [("scope_bucket", 1), ("kind", 1), ("name_normalized", 1)],
                unique=True,
            )
            self.collection.create_index("region_id")
            self.collection.create_index("parent_city_id")
            self.collection.create_index("kind")
            self._indexes_created = True
        except Exception:
            pass

    def find_by_scope_and_name(
        self,
        name: str,
        *,
        kind: str,
        region_id: Optional[str] = None,
        parent_city_id: Optional[str] = None,
        parent_rayon_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        from utils.rayon_normalizer import (
            build_geo_circle_scope_bucket,
            normalize_geo_circle_key,
        )

        self._ensure_indexes()
        key = normalize_geo_circle_key(name)
        if not key:
            return None
        bucket = build_geo_circle_scope_bucket(region_id, parent_city_id, parent_rayon_id)
        return self.find_one(
            {
                "scope_bucket": bucket,
                "kind": kind,
                "name_normalized": key,
            }
        )

    def find_or_create(
        self,
        name: str,
        *,
        kind: str = "other",
        region_id: Optional[str] = None,
        parent_city_id: Optional[str] = None,
        parent_rayon_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        from utils.rayon_normalizer import (
            build_geo_circle_scope_bucket,
            format_geo_circle_display_name,
            normalize_geo_circle_key,
        )

        self._ensure_indexes()
        display = format_geo_circle_display_name(name)
        key = normalize_geo_circle_key(name)
        if not display or not key:
            raise ValueError(f"Некоректна назва округу/групи: {name!r}")

        bucket = build_geo_circle_scope_bucket(region_id, parent_city_id, parent_rayon_id)
        existing = self.find_by_scope_and_name(
            display,
            kind=kind,
            region_id=region_id,
            parent_city_id=parent_city_id,
            parent_rayon_id=parent_rayon_id,
        )
        if existing:
            return existing

        doc = {
            "name": display,
            "name_normalized": key,
            "kind": kind,
            "region_id": region_id,
            "parent_city_id": parent_city_id,
            "parent_rayon_id": parent_rayon_id,
            "scope_bucket": bucket,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        doc_id = self.create(doc)
        created = self.find_by_id(doc_id)
        return created if created else doc

    def list_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        return self.find_many(filter={"region_id": region_id}, sort=[("kind", 1), ("name", 1)])
