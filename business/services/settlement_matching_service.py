# -*- coding: utf-8 -*-
"""
Fuzzy-пошук і дедуплікація населених пунктів у колекції cities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

from utils.settlement_normalizer import (
    format_settlement_display_name,
    is_district_only_name,
    normalize_settlement_key,
    normalize_settlement_name,
)

DEFAULT_FUZZY_THRESHOLD = 0.94


@dataclass
class SettlementMatch:
    """Результат fuzzy-пошуку населеного пункту."""

    city: Dict[str, Any]
    score: float
    match_type: str  # exact_key | fuzzy


@dataclass
class DuplicateGroup:
    """Група потенційних дублів у межах однієї області."""

    region_id: str
    region_name: str
    members: List[Dict[str, Any]] = field(default_factory=list)
    canonical_id: Optional[str] = None
    merge_pairs: List[Tuple[str, str]] = field(default_factory=list)


def similarity_score(left: Optional[str], right: Optional[str]) -> float:
    """Оцінка схожості двох назв (0..1) за нормалізованим ключем."""
    key_left = normalize_settlement_key(left)
    key_right = normalize_settlement_key(right)
    if not key_left or not key_right:
        return 0.0
    if key_left == key_right:
        return 1.0
    return SequenceMatcher(None, key_left, key_right).ratio()


def display_name_quality(name: Optional[str]) -> int:
    """
    Оцінка якості display-назви (більше = краще).
    Використовується для вибору канонічного запису серед дублів.
    """
    if not name:
        return 0
    score = 0
    if not is_district_only_name(name):
        score += 10
    if name == format_settlement_display_name(name):
        score += 5
    if not name.isupper():
        score += 3
    if not any(
        token in name.lower()
        for token in ("м.", "с.", "смт", "місто", "село", "район", "р-н", "/")
    ):
        score += 2
    return score


class SettlementMatchingService:
    """Fuzzy-пошук існуючих НП та формування груп дублів."""

    def __init__(self, fuzzy_threshold: float = DEFAULT_FUZZY_THRESHOLD):
        self.fuzzy_threshold = fuzzy_threshold

    def find_best_match(
        self,
        name: str,
        region_id: str,
        candidates: Iterable[Dict[str, Any]],
    ) -> Optional[SettlementMatch]:
        """
        Знаходить найкращий існуючий НП у межах області.

        1. exact match за normalize_settlement_key;
        2. fuzzy match >= threshold.
        """
        target_key = normalize_settlement_key(name)
        if not target_key:
            return None

        best: Optional[SettlementMatch] = None
        for city in candidates:
            if str(city.get("region_id")) != str(region_id):
                continue
            if city.get("merged_into"):
                continue

            city_key = city.get("name_normalized") or normalize_settlement_key(city.get("name"))
            if city_key == target_key:
                return SettlementMatch(city=city, score=1.0, match_type="exact_key")

            aliases = city.get("search_aliases") or []
            if target_key in aliases:
                return SettlementMatch(city=city, score=1.0, match_type="alias")

            ratio = similarity_score(name, city.get("name"))
            if ratio < self.fuzzy_threshold:
                continue

            candidate = SettlementMatch(city=city, score=ratio, match_type="fuzzy")
            if best is None or candidate.score > best.score:
                best = candidate
                if best.score >= 0.999:
                    break

        return best

    def pick_canonical(self, members: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обирає канонічний документ з групи дублів."""
        active = [m for m in members if not m.get("merged_into")]
        if not active:
            return members[0]

        def sort_key(city: Dict[str, Any]) -> Tuple[int, int, str]:
            ref_score = int(city.get("_reference_count") or 0)
            street_score = int(city.get("_street_count") or 0)
            quality = display_name_quality(city.get("name"))
            created = city.get("created_at")
            created_key = created.isoformat() if isinstance(created, datetime) else str(created or "")
            return (quality, ref_score + street_score, created_key)

        return sorted(active, key=sort_key, reverse=True)[0]

    def find_duplicate_groups(
        self,
        cities: List[Dict[str, Any]],
        region_names: Dict[str, str],
        reference_counts: Optional[Dict[str, int]] = None,
        street_counts: Optional[Dict[str, int]] = None,
    ) -> List[DuplicateGroup]:
        """
        Формує групи дублів через union-find у межах кожної області.
        """
        reference_counts = reference_counts or {}
        street_counts = street_counts or {}

        enriched: List[Dict[str, Any]] = []
        for city in cities:
            if city.get("merged_into"):
                continue
            item = dict(city)
            cid = str(city["_id"])
            item["_reference_count"] = reference_counts.get(cid, 0)
            item["_street_count"] = street_counts.get(cid, 0)
            enriched.append(item)

        by_region: Dict[str, List[Dict[str, Any]]] = {}
        for city in enriched:
            by_region.setdefault(str(city.get("region_id")), []).append(city)

        groups: List[DuplicateGroup] = []
        for region_id, region_cities in by_region.items():
            if len(region_cities) < 2:
                continue

            parent = {str(c["_id"]): str(c["_id"]) for c in region_cities}

            def find(node: str) -> str:
                while parent[node] != node:
                    parent[node] = parent[parent[node]]
                    node = parent[node]
                return node

            def union(a: str, b: str) -> None:
                ra, rb = find(a), find(b)
                if ra != rb:
                    parent[rb] = ra

            for i, left in enumerate(region_cities):
                for right in region_cities[i + 1 :]:
                    score = similarity_score(left.get("name"), right.get("name"))
                    if score >= self.fuzzy_threshold:
                        union(str(left["_id"]), str(right["_id"]))

            clusters: Dict[str, List[Dict[str, Any]]] = {}
            for city in region_cities:
                root = find(str(city["_id"]))
                clusters.setdefault(root, []).append(city)

            for cluster in clusters.values():
                if len(cluster) < 2:
                    continue
                canonical = self.pick_canonical(cluster)
                canonical_id = str(canonical["_id"])
                merge_pairs = [
                    (str(item["_id"]), canonical_id)
                    for item in cluster
                    if str(item["_id"]) != canonical_id
                ]
                groups.append(
                    DuplicateGroup(
                        region_id=region_id,
                        region_name=region_names.get(region_id, region_id),
                        members=cluster,
                        canonical_id=canonical_id,
                        merge_pairs=merge_pairs,
                    )
                )

        return groups

    @staticmethod
    def prepare_city_document(name: str, region_id: str) -> Optional[Dict[str, Any]]:
        """Готує канонічні поля name/name_normalized для нового або оновленого НП."""
        display = normalize_settlement_name(name)
        if not display:
            return None
        key = normalize_settlement_key(display)
        if not key:
            return None
        now = datetime.now(timezone.utc)
        return {
            "name": display,
            "name_normalized": key,
            "region_id": region_id,
            "updated_at": now,
        }
