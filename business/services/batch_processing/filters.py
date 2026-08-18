# -*- coding: utf-8 -*-
"""
Спільні фільтри та побудова Mongo-запитів для групових задач.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from business.services.llm_processing_regions_service import normalize_region_name


@dataclass
class BatchJobFilters:
    """Параметри відбору оголошень для групових задач."""

    source: str = "both"  # olx | prozorro | both
    regions: Optional[List[str]] = None
    status: Optional[str] = None  # активне | неактивне | None = всі
    days: Optional[int] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    load_new: bool = False  # лише source_reload
    limit: Optional[int] = None
    force: bool = False  # обробляти навіть якщо хеш не змінився

    def resolved_sources(self) -> List[str]:
        src = (self.source or "both").strip().lower()
        if src == "olx":
            return ["olx"]
        if src == "prozorro":
            return ["prozorro"]
        return ["olx", "prozorro"]

    def normalized_regions(self) -> Optional[List[str]]:
        if not self.regions:
            return None
        out: List[str] = []
        for r in self.regions:
            s = (r or "").strip()
            if not s:
                continue
            out.append(normalize_region_name(s) or s)
        return out or None

    def date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        if self.date_from or self.date_to:
            return self.date_from, self.date_to
        if self.days is not None and self.days > 0:
            now = datetime.now(timezone.utc)
            return now - timedelta(days=self.days), now
        return None, None


def build_unified_listing_query(filters: BatchJobFilters) -> Dict[str, Any]:
    """Mongo-фільтр для unified_listings (перезавантаження з джерел / адреси з кешу)."""
    from utils.ukraine_regions import build_region_search_regex

    criteria: Dict[str, Any] = {}
    sources = filters.resolved_sources()
    if len(sources) == 1:
        criteria["source"] = sources[0]

    regions = filters.normalized_regions()
    if regions:
        region_clauses = []
        for region in regions:
            pattern = build_region_search_regex(region) or region
            region_clauses.append({"region": {"$regex": pattern, "$options": "i"}})
        if len(region_clauses) == 1:
            criteria.update(region_clauses[0])
        else:
            criteria["$or"] = region_clauses

    if filters.status:
        criteria["status"] = str(filters.status).strip()

    date_from, date_to = filters.date_range()
    if date_from or date_to:
        date_crit: Dict[str, Any] = {}
        if date_from:
            date_crit["$gte"] = date_from
        if date_to:
            date_crit["$lte"] = date_to
        criteria["source_updated_at"] = date_crit

    return criteria


def build_raw_olx_query(filters: BatchJobFilters) -> Dict[str, Any]:
    """Mongo-фільтр для raw_olx_listings (повторне розпізнавання)."""
    criteria: Dict[str, Any] = {}
    regions = filters.normalized_regions()
    if regions:
        if len(regions) == 1:
            criteria["approximate_region"] = regions[0]
        else:
            criteria["approximate_region"] = {"$in": regions}

    date_from, date_to = filters.date_range()
    if date_from or date_to:
        date_crit: Dict[str, Any] = {}
        if date_from:
            date_crit["$gte"] = date_from
        if date_to:
            date_crit["$lte"] = date_to
        criteria["loaded_at"] = date_crit

    return criteria


def build_raw_prozorro_query(filters: BatchJobFilters) -> Dict[str, Any]:
    """Mongo-фільтр для raw_prozorro_auctions (повторне розпізнавання)."""
    return build_raw_olx_query(filters)


def get_unified_source_ids_by_status(
    status: str,
    sources: Optional[List[str]] = None,
) -> Set[Tuple[str, str]]:
    """Повертає множину (source, source_id) з unified_listings за статусом."""
    from data.repositories.unified_listings_repository import UnifiedListingsRepository

    repo = UnifiedListingsRepository()
    criteria: Dict[str, Any] = {"status": str(status).strip()}
    if sources:
        if len(sources) == 1:
            criteria["source"] = sources[0]
        else:
            criteria["source"] = {"$in": sources}

    out: Set[Tuple[str, str]] = set()
    cursor = repo.collection.find(criteria, {"source": 1, "source_id": 1})
    for doc in cursor:
        src = doc.get("source")
        sid = doc.get("source_id")
        if src and sid:
            out.add((str(src), str(sid)))
    return out
