# -*- coding: utf-8 -*-
"""Імпорт розпарсених НП з raw_mista_settlements у колекцію cities."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from business.services.settlement_matching_service import display_name_quality
from data.repositories.geography_repository import CitiesRepository, RegionsRepository
from data.repositories.raw_mista_settlements_repository import RawMistaSettlementsRepository
from utils.settlement_normalizer import (
    normalize_settlement_key,
    normalize_settlement_name,
    resolve_mista_canonical_and_aliases,
)
from utils.ukraine_regions import normalize_region_for_repository_lookup


class MistaSettlementImportService:
    """Злиття mista.ua → cities без автоматичного merge_into дублів."""

    def __init__(self):
        self.raw_repo = RawMistaSettlementsRepository()
        self.cities_repo = CitiesRepository()
        self.regions_repo = RegionsRepository()

    def _resolve_region_id(self, region_name: Optional[str]) -> Optional[str]:
        if not region_name:
            return None
        lookup = normalize_region_for_repository_lookup(region_name) or region_name
        if lookup in ("м. Київ", "Київ"):
            reg = self.regions_repo.find_by_name("м. Київ") or self.regions_repo.find_or_create("м. Київ")
            return str(reg["_id"])
        if lookup in ("м. Севастополь", "Севастополь"):
            reg = self.regions_repo.find_by_name("м. Севастополь") or self.regions_repo.find_or_create("м. Севастополь")
            return str(reg["_id"])
        if "крим" in lookup.lower():
            reg = self.regions_repo.find_by_name("АР Крим") or self.regions_repo.find_or_create("АР Крим")
            return str(reg["_id"])
        reg = self.regions_repo.find_by_name(lookup) or self.regions_repo.find_or_create(lookup)
        return str(reg["_id"]) if reg else None

    def _find_existing_city(
        self,
        name: str,
        region_id: str,
        search_aliases: Optional[List[str]] = None,
        mista_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        if mista_id is not None:
            found = self.cities_repo.find_one({
                **CitiesRepository._active_city_filter(),
                "mista_id": mista_id,
            })
            if found:
                return found

        existing = self.cities_repo.find_by_name_and_region(name, region_id)
        if existing:
            return existing

        key = normalize_settlement_key(name)
        if key:
            by_alias = self.cities_repo.find_one({
                **CitiesRepository._active_city_filter(),
                "region_id": region_id,
                "search_aliases": key,
            })
            if by_alias:
                return by_alias

        if search_aliases:
            for alias in search_aliases:
                by_alias = self.cities_repo.find_one({
                    **CitiesRepository._active_city_filter(),
                    "region_id": region_id,
                    "search_aliases": alias,
                })
                if by_alias:
                    return by_alias
        return None

    @staticmethod
    def _effective_parsed(raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """parsed з детальної сторінки + fallback на поля зі списку mista (population тощо)."""
        parsed = dict(raw_doc.get("parsed") or {})
        for key in (
            "population",
            "population_density",
            "area_sq_km",
            "elevation_m",
            "founded_year",
            "region_name",
            "oblast_rayon_name",
            "name",
            "mista_url",
            "mista_id",
            "settlement_status",
        ):
            if parsed.get(key) is None and raw_doc.get(key) is not None:
                parsed[key] = raw_doc[key]
        return parsed

    def _build_metadata(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        meta: Dict[str, Any] = {
            "population": parsed.get("population"),
            "population_density": parsed.get("population_density"),
            "area_sq_km": parsed.get("area_sq_km"),
            "postal_code": parsed.get("postal_code"),
            "phone_code": parsed.get("phone_code"),
            "coordinates": parsed.get("coordinates"),
            "elevation_m": parsed.get("elevation_m"),
            "water_body": parsed.get("water_body"),
            "founded_year": parsed.get("founded_year"),
            "city_day": parsed.get("city_day"),
            "former_names": parsed.get("former_names") or [],
            "search_aliases": parsed.get("search_aliases") or [],
            "settlement_status": parsed.get("settlement_status"),
            "mista_id": parsed.get("mista_id"),
            "mista_url": parsed.get("mista_url"),
            "oblast_rayon_name": parsed.get("oblast_rayon_name"),
            "source": "mista",
            "source_updated_at": now,
            "updated_at": now,
        }
        return {k: v for k, v in meta.items() if v is not None}

    def _resolve_import_name(self, raw_doc: Dict[str, Any], parsed: Dict[str, Any]) -> Optional[str]:
        mista_url = parsed.get("mista_url") or raw_doc.get("mista_url") or ""
        canonical, _aliases = resolve_mista_canonical_and_aliases(
            list_name=raw_doc.get("name"),
            detail_name=parsed.get("name"),
            mista_url=mista_url,
            former_names=parsed.get("former_names") or [],
        )
        return canonical

    def _resolve_import_aliases(
        self,
        raw_doc: Dict[str, Any],
        parsed: Dict[str, Any],
        canonical: Optional[str],
    ) -> List[str]:
        mista_url = parsed.get("mista_url") or raw_doc.get("mista_url") or ""
        _canon, aliases = resolve_mista_canonical_and_aliases(
            list_name=raw_doc.get("name"),
            detail_name=parsed.get("name"),
            mista_url=mista_url,
            former_names=parsed.get("former_names") or [],
        )
        merged = set(aliases) | set(parsed.get("search_aliases") or [])
        if canonical:
            key = normalize_settlement_key(canonical)
            if key:
                merged.add(key)
        return sorted(merged)

    def import_one(self, raw_doc: Dict[str, Any], *, dry_run: bool = False) -> Dict[str, Any]:
        parsed = self._effective_parsed(raw_doc)
        name = self._resolve_import_name(raw_doc, parsed)
        region_name = parsed.get("region_name") or raw_doc.get("region_name")
        region_id = self._resolve_region_id(region_name)
        if not name or not region_id:
            return {"status": "skipped", "reason": "missing_name_or_region", "name": name}

        aliases = self._resolve_import_aliases(raw_doc, parsed, name)
        mista_id = parsed.get("mista_id") or raw_doc.get("mista_id")
        existing = self._find_existing_city(name, region_id, aliases, mista_id)
        if not existing and raw_doc.get("mista_url"):
            existing = self.cities_repo.find_one({
                **CitiesRepository._active_city_filter(),
                "mista_url": raw_doc["mista_url"],
            })
        meta = self._build_metadata(parsed)
        meta["search_aliases"] = aliases

        if dry_run:
            return {
                "status": "dry_run",
                "name": name,
                "region_id": region_id,
                "existing_id": str(existing["_id"]) if existing else None,
            }

        if existing:
            updates = dict(meta)
            merged_aliases = sorted(
                set(existing.get("search_aliases") or []) | set(aliases)
            )
            if merged_aliases:
                updates["search_aliases"] = merged_aliases
            # Список mista дає актуальну назву; завжди піднімаємо канон, якщо він інший.
            if name and normalize_settlement_key(name) != normalize_settlement_key(
                existing.get("name") or ""
            ):
                updates["name"] = name
                updates["name_normalized"] = normalize_settlement_key(name)
            elif display_name_quality(name) > display_name_quality(existing.get("name")):
                updates["name"] = name
                updates["name_normalized"] = normalize_settlement_key(name)
            self.cities_repo.update_by_id(existing["_id"], {"$set": updates})
            return {
                "status": "updated",
                "city_id": str(existing["_id"]),
                "name": updates.get("name", existing.get("name")),
            }

        city = self.cities_repo.find_or_create(name, region_id)
        self.cities_repo.update_by_id(city["_id"], {"$set": meta})
        return {"status": "created", "city_id": str(city["_id"]), "name": name}

    def backfill_population_from_raw(self, *, dry_run: bool = False, limit: int = 0) -> Dict[str, Any]:
        """
        Заповнює cities.population / area_sq_km з raw_mista (top-level або parsed),
        зіставлення за mista_url, mista_id або name+region_id.
        """
        stats = {"scanned": 0, "updated": 0, "skipped": 0, "errors": 0}
        filt = {
            "$or": [
                {"population": {"$exists": True, "$ne": None}},
                {"parsed.population": {"$exists": True, "$ne": None}},
                {"area_sq_km": {"$exists": True, "$ne": None}},
                {"parsed.area_sq_km": {"$exists": True, "$ne": None}},
            ]
        }
        cursor = self.raw_repo.collection.find(filt)
        if limit > 0:
            cursor = cursor.limit(limit)
        for raw_doc in cursor:
            stats["scanned"] += 1
            try:
                parsed = self._effective_parsed(raw_doc)
                pop = parsed.get("population")
                area = parsed.get("area_sq_km")
                if pop is None and area is None:
                    stats["skipped"] += 1
                    continue
                name = self._resolve_import_name(raw_doc, parsed)
                region_id = self._resolve_region_id(parsed.get("region_name"))
                if not name or not region_id:
                    stats["skipped"] += 1
                    continue
                existing = self._find_existing_city(
                    name,
                    region_id,
                    parsed.get("search_aliases"),
                    parsed.get("mista_id"),
                )
                if not existing and raw_doc.get("mista_url"):
                    existing = self.cities_repo.find_one({
                        **CitiesRepository._active_city_filter(),
                        "mista_url": raw_doc["mista_url"],
                    })
                if not existing:
                    stats["skipped"] += 1
                    continue
                updates: Dict[str, Any] = {}
                if pop is not None and existing.get("population") is None:
                    updates["population"] = int(pop)
                if area is not None and existing.get("area_sq_km") is None:
                    updates["area_sq_km"] = float(area)
                if not updates:
                    stats["skipped"] += 1
                    continue
                if not dry_run:
                    updates["updated_at"] = datetime.now(timezone.utc)
                    self.cities_repo.update_by_id(existing["_id"], {"$set": updates})
                stats["updated"] += 1
            except Exception:
                stats["errors"] += 1
        return stats

    def run_import(self, *, limit: int = 0, dry_run: bool = False) -> Dict[str, Any]:
        docs = self.raw_repo.find_for_import(limit=limit)
        stats = {"total": len(docs), "created": 0, "updated": 0, "skipped": 0, "errors": 0}
        duplicates_report: List[Dict[str, Any]] = []

        for doc in docs:
            try:
                result = self.import_one(doc, dry_run=dry_run)
                st = result.get("status")
                if st == "created":
                    stats["created"] += 1
                elif st == "updated":
                    stats["updated"] += 1
                elif st == "skipped":
                    stats["skipped"] += 1
                parsed = doc.get("parsed") or {}
                name = parsed.get("name")
                region_id = self._resolve_region_id(parsed.get("region_name"))
                if name and region_id:
                    for alias in parsed.get("search_aliases") or []:
                        if alias == normalize_settlement_key(name):
                            continue
                        other = self.cities_repo.find_one({
                            **CitiesRepository._active_city_filter(),
                            "region_id": region_id,
                            "name_normalized": alias,
                            "name": {"$ne": normalize_settlement_name(name)},
                        })
                        if other:
                            duplicates_report.append({
                                "canonical": name,
                                "alias": alias,
                                "other_city_id": str(other["_id"]),
                                "other_name": other.get("name"),
                            })
            except Exception as e:
                stats["errors"] += 1
                print(f"[mista import] Помилка {doc.get('mista_url')}: {e}", flush=True)

        stats["duplicate_hints"] = len(duplicates_report)
        return stats
