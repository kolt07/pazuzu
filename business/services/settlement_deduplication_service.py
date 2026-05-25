# -*- coding: utf-8 -*-
"""
Дедуплікація населених пунктів: аудит, нормалізація, м'яке злиття посилань.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from business.services.settlement_matching_service import SettlementMatchingService
from data.database.connection import MongoDBConnection
from data.repositories.geography_repository import active_cities_mongo_clause
from utils.settlement_normalizer import (
    is_district_only_name,
    normalize_settlement_key,
    normalize_settlement_name,
)
from utils.ukraine_regions import (
    normalize_region_for_repository_lookup,
    normalize_region_to_canonical,
)


class SettlementDeduplicationService:
    """Аудит і м'яка заміна посилань на канонічні НП."""

    SOURCE_COLLECTIONS = (
        ("prozorro_auctions", "auction_data.address_refs"),
        ("olx_listings", "detail.address_refs"),
    )

    def __init__(self, fuzzy_threshold: float = 0.94):
        self.matcher = SettlementMatchingService(fuzzy_threshold=fuzzy_threshold)
        self.db = MongoDBConnection.get_database()

    def run_audit(self, output_path: Optional[Path] = None) -> Dict[str, Any]:
        """Повний аудит колекції cities та потенційних дублів."""
        cities = list(self.db.cities.find(active_cities_mongo_clause()))
        regions = {
            str(r["_id"]): r.get("name", "")
            for r in self.db.regions.find({}, {"name": 1})
        }

        ref_counts = self._count_city_references()
        street_counts = self._count_streets_by_city()

        issues = {
            "prefix_or_compound": [],
            "all_caps": [],
            "district_only": [],
            "wrong_region_id": [],
            "name_key_mismatch": [],
        }

        prefix_pattern = re.compile(
            r"(?:"
            r"^(?:м\.|с\.|смт\.?|місто|село|с-ще|селище|пгт\.?|тг\.?)\s+"
            r"|,\s*(?:м\.|с\.|смт\.?|місто|село|с-ще|селище|пгт\.?|тг\.?)\s+"
            r"|/(?:м\.|с\.|смт\.?|місто|село|с-ще|селище|пгт\.?|тг\.?)\.?\s*"
            r")",
            re.IGNORECASE,
        )

        for city in cities:
            name = city.get("name") or ""
            cid = str(city["_id"])
            region_name = regions.get(str(city.get("region_id")), "")
            entry = {
                "city_id": cid,
                "name": name,
                "name_normalized": city.get("name_normalized"),
                "region": region_name,
            }

            if prefix_pattern.search(name) or "/" in name:
                issues["prefix_or_compound"].append(entry)
            if name.isupper() and len(name) > 2:
                issues["all_caps"].append(entry)
            if is_district_only_name(name):
                issues["district_only"].append(entry)

            expected_key = normalize_settlement_key(name)
            if expected_key and city.get("name_normalized") != expected_key:
                issues["name_key_mismatch"].append(
                    {**entry, "expected_key": expected_key}
                )

        duplicate_groups = self.matcher.find_duplicate_groups(
            cities,
            regions,
            reference_counts=ref_counts,
            street_counts=street_counts,
        )

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_cities": len(cities),
            "total_regions": len(regions),
            "duplicate_groups": len(duplicate_groups),
            "duplicate_records": sum(len(g.merge_pairs) for g in duplicate_groups),
            "issues": {key: len(value) for key, value in issues.items()},
            "issue_samples": {
                key: value[:20] for key, value in issues.items() if value
            },
            "duplicate_samples": [
                {
                    "region": group.region_name,
                    "canonical_id": group.canonical_id,
                    "members": [
                        {
                            "id": str(item["_id"]),
                            "name": item.get("name"),
                            "name_normalized": item.get("name_normalized"),
                        }
                        for item in group.members
                    ],
                }
                for group in duplicate_groups[:50]
            ],
        }

        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        return report

    def normalize_all_cities(self, dry_run: bool = True) -> Dict[str, int]:
        """Нормалізує name та name_normalized для всіх активних cities."""
        stats = {"updated": 0, "skipped_district": 0, "skipped_invalid": 0, "unchanged": 0}

        for city in self.db.cities.find(active_cities_mongo_clause()):
            raw_name = city.get("name") or ""
            if is_district_only_name(raw_name):
                stats["skipped_district"] += 1
                continue

            display = normalize_settlement_name(raw_name)
            key = normalize_settlement_key(display) if display else ""
            if not display or not key:
                stats["skipped_invalid"] += 1
                continue

            update = {
                "name": display,
                "name_normalized": key,
                "updated_at": datetime.now(timezone.utc),
            }
            if city.get("name") == display and city.get("name_normalized") == key:
                stats["unchanged"] += 1
                continue

            stats["updated"] += 1
            if dry_run:
                continue

            conflict = self.db.cities.find_one(
                {
                    "region_id": city.get("region_id"),
                    "name_normalized": key,
                    "merged_into": {"$exists": False},
                    "_id": {"$ne": city["_id"]},
                }
            )
            if conflict:
                self._soft_merge_city(str(city["_id"]), str(conflict["_id"]))
                stats.setdefault("conflict_merged", 0)
                stats["conflict_merged"] += 1
                continue

            self.db.cities.update_one({"_id": city["_id"]}, {"$set": update})

        return stats

    def _soft_merge_city(self, duplicate_id: str, canonical_id: str) -> Dict[str, int]:
        """М'яко зливає один city у інший."""
        if duplicate_id == canonical_id:
            return {"relinked": 0, "merged": 0, "refs_updated": 0, "listings_updated": 0}

        dup_doc = self.db.cities.find_one({"_id": self._object_id(duplicate_id)}, {"name": 1})
        canonical_doc = self.db.cities.find_one({"_id": self._object_id(canonical_id)}, {"name": 1})
        if not dup_doc or not canonical_doc:
            return {"relinked": 0, "merged": 0, "refs_updated": 0, "listings_updated": 0}

        canonical_name = normalize_settlement_name(canonical_doc.get("name")) or canonical_doc.get("name")
        street_stats = self._merge_streets(duplicate_id, canonical_id)
        self.db.cities.update_one(
            {"_id": self._object_id(canonical_id)},
            {
                "$set": {
                    "name": canonical_name,
                    "name_normalized": normalize_settlement_key(canonical_name),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        self.db.cities.update_one(
            {"_id": self._object_id(duplicate_id)},
            {
                "$set": {
                    "merged_into": canonical_id,
                    "merged_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

        id_map = {duplicate_id: canonical_id}
        return {
            **street_stats,
            "refs_updated": self._update_address_refs(id_map),
            "listings_updated": self._update_unified_listings(id_map),
        }

    def merge_duplicates(self, dry_run: bool = True) -> Dict[str, int]:
        """М'яке злиття дублів і оновлення посилань."""
        cities = list(self.db.cities.find(active_cities_mongo_clause()))
        regions = {
            str(r["_id"]): r.get("name", "")
            for r in self.db.regions.find({}, {"name": 1})
        }
        ref_counts = self._count_city_references()
        street_counts = self._count_streets_by_city()

        groups = self.matcher.find_duplicate_groups(
            cities,
            regions,
            reference_counts=ref_counts,
            street_counts=street_counts,
        )

        stats = {
            "groups": len(groups),
            "cities_merged": 0,
            "streets_relinked": 0,
            "streets_merged": 0,
            "refs_updated": 0,
            "listings_updated": 0,
        }

        id_map: Dict[str, str] = {}
        for group in groups:
            canonical = next(
                (item for item in group.members if str(item["_id"]) == group.canonical_id),
                group.members[0],
            )
            for duplicate_id, canonical_id in group.merge_pairs:
                id_map[duplicate_id] = canonical_id
                stats["cities_merged"] += 1
                if dry_run:
                    continue

                merge_result = self._soft_merge_city(duplicate_id, canonical_id)
                stats["streets_relinked"] += merge_result["relinked"]
                stats["streets_merged"] += merge_result["merged"]
                stats["refs_updated"] += merge_result["refs_updated"]
                stats["listings_updated"] += merge_result["listings_updated"]

        return stats

    def _merge_streets(self, from_city_id: str, to_city_id: str) -> Dict[str, int]:
        stats = {"relinked": 0, "merged": 0}
        from_oid = self._object_id(from_city_id)
        to_oid = self._object_id(to_city_id)

        for street in self.db.streets.find({"city_id": from_city_id}):
            existing = self.db.streets.find_one(
                {
                    "city_id": to_city_id,
                    "name_normalized": street.get("name_normalized"),
                }
            )
            if existing:
                self.db.buildings.update_many(
                    {"street_id": str(street["_id"])},
                    {"$set": {"street_id": str(existing["_id"])}},
                )
                self.db.streets.update_one(
                    {"_id": street["_id"]},
                    {
                        "$set": {
                            "merged_into": str(existing["_id"]),
                            "merged_at": datetime.now(timezone.utc),
                        }
                    },
                )
                stats["merged"] += 1
            else:
                self.db.streets.update_one(
                    {"_id": street["_id"]},
                    {"$set": {"city_id": to_city_id, "updated_at": datetime.now(timezone.utc)}},
                )
                stats["relinked"] += 1

        return stats

    def _update_address_refs(self, id_map: Dict[str, str]) -> int:
        updated = 0
        id_to_name = self._build_id_to_name_map(id_map)

        for collection_name, path in self.SOURCE_COLLECTIONS:
            collection = self.db[collection_name]
            parent_field = path.rsplit(".", 1)[0]

            cursor = collection.find(
                {f"{path}.city._id": {"$in": list(id_map.keys())}},
                {"_id": 1, parent_field: 1},
            )
            for doc in cursor:
                parent_value = doc
                for part in parent_field.split("."):
                    parent_value = (parent_value or {}).get(part)

                refs_list = parent_value if isinstance(parent_value, list) else [parent_value]
                changed = False
                new_refs = []
                for ref in refs_list:
                    if not isinstance(ref, dict):
                        new_refs.append(ref)
                        continue
                    ref = dict(ref)
                    city = dict(ref.get("city") or {})
                    old_id = str(city.get("_id") or "")
                    if old_id in id_map:
                        canonical_id = self._resolve_canonical_id(old_id, id_map)
                        city["_id"] = canonical_id
                        city["name"] = id_to_name.get(canonical_id, city.get("name"))
                        ref["city"] = city
                        changed = True
                    new_refs.append(ref)

                if not changed:
                    continue

                update_value = new_refs if isinstance(parent_value, list) else new_refs[0]
                collection.update_one({"_id": doc["_id"]}, {"$set": {path: update_value}})
                updated += 1

        return updated

    def _update_unified_listings(self, id_map: Dict[str, str]) -> int:
        from business.services.unified_listings_service import _compute_root_geo_from_addresses

        replacement_map = self._build_settlement_replacement_map(id_map)
        if not replacement_map:
            return 0

        regions_by_name = {
            r.get("name"): str(r["_id"])
            for r in self.db.regions.find({}, {"name": 1})
        }

        updated = 0
        for doc in self.db.unified_listings.find({}, {"addresses": 1, "region": 1}):
            addresses = doc.get("addresses") or []
            if not isinstance(addresses, list):
                continue

            changed = False
            new_addresses = []
            for addr in addresses:
                if not isinstance(addr, dict):
                    new_addresses.append(addr)
                    continue
                addr = dict(addr)
                settlement = addr.get("settlement") or addr.get("city")
                addr_rid = regions_by_name.get(addr.get("region")) or regions_by_name.get(
                    doc.get("region")
                )
                canonical_name = self._lookup_replacement(
                    settlement, replacement_map, addr_rid
                )
                if canonical_name and canonical_name != settlement:
                    addr["settlement"] = canonical_name
                    changed = True
                new_addresses.append(addr)

            if not changed:
                continue

            root_geo = _compute_root_geo_from_addresses(new_addresses)
            self.db.unified_listings.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "addresses": new_addresses,
                        "city": root_geo.get("city"),
                        "region": root_geo.get("region"),
                        "oblast_raion": root_geo.get("oblast_raion"),
                        "city_district": root_geo.get("city_district"),
                    }
                },
            )
            updated += 1

        return updated

    def _build_settlement_replacement_map(self, id_map: Dict[str, str]) -> Dict[tuple, str]:
        """(region_id, variant_key) → канонічна назва в межах області."""
        replacement: Dict[tuple, str] = {}
        for duplicate_id in id_map:
            canonical_id = self._resolve_canonical_id(duplicate_id, id_map)
            dup_doc = self.db.cities.find_one(
                {"_id": self._object_id(duplicate_id)},
                {"name": 1, "name_normalized": 1, "region_id": 1},
            )
            canonical_doc = self.db.cities.find_one(
                {"_id": self._object_id(canonical_id)},
                {"name": 1, "region_id": 1},
            )
            if not dup_doc or not canonical_doc:
                continue
            rid = str(canonical_doc.get("region_id") or dup_doc.get("region_id") or "")
            if not rid:
                continue
            canonical_name = canonical_doc.get("name") or ""
            for variant in (dup_doc.get("name"), dup_doc.get("name_normalized")):
                if not variant:
                    continue
                replacement[(rid, variant.strip().lower())] = canonical_name
                key = normalize_settlement_key(variant)
                if key:
                    replacement[(rid, key)] = canonical_name
        return replacement

    @staticmethod
    def _lookup_replacement(
        settlement: Optional[str],
        replacement_map: Dict[tuple, str],
        region_id: Optional[str] = None,
    ) -> Optional[str]:
        if not settlement or not region_id:
            return None
        rid = str(region_id)
        direct = replacement_map.get((rid, settlement.strip().lower()))
        if direct:
            return direct
        key = normalize_settlement_key(settlement)
        if key:
            return replacement_map.get((rid, key))
        return None

    def repair_cities_by_computed_key(self, dry_run: bool = True) -> Dict[str, int]:
        """
        Зливає активні cities, у яких normalize_settlement_key(name) збігається,
        але name_normalized у БД різний (напр. «смт любешів» vs «любешів»).
        """
        from business.services.settlement_matching_service import SettlementMatchingService

        stats = {"groups": 0, "merged": 0}
        by_group: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)

        for city in self.db.cities.find(active_cities_mongo_clause()):
            if city.get("is_oblast_rayon_stub"):
                continue
            raw = city.get("name") or ""
            key = normalize_settlement_key(raw) or normalize_settlement_key(
                city.get("name_normalized") or ""
            )
            if not key:
                continue
            rid = str(city.get("region_id") or "")
            by_group[(rid, key)].append(city)

        matcher = SettlementMatchingService()
        for (rid, key), members in by_group.items():
            if len(members) <= 1:
                continue
            stats["groups"] += 1
            canonical = matcher.pick_canonical(members)
            cid = str(canonical["_id"])
            for m in members:
                mid = str(m["_id"])
                if mid == cid:
                    continue
                stats["merged"] += 1
                if dry_run:
                    continue
                self._soft_merge_city(mid, cid)

        return stats

    def audit_duplicate_city_groups(self) -> List[Dict[str, Any]]:
        """Групи активних cities з однаковим ключем у межах області (для звіту після міграцій)."""
        by_group: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for city in self.db.cities.find(active_cities_mongo_clause()):
            if city.get("is_oblast_rayon_stub"):
                continue
            raw = city.get("name") or ""
            key = normalize_settlement_key(raw) or normalize_settlement_key(
                city.get("name_normalized") or ""
            )
            if not key:
                continue
            rid = str(city.get("region_id") or "")
            by_group[(rid, key)].append(city)

        report: List[Dict[str, Any]] = []
        for (rid, key), members in sorted(by_group.items(), key=lambda x: x[0]):
            if len(members) <= 1:
                continue
            report.append(
                {
                    "region_id": rid,
                    "key": key,
                    "count": len(members),
                    "names": [m.get("name") for m in members],
                    "ids": [str(m["_id"]) for m in members],
                }
            )
        return report

    def normalize_denormalized_settlements(self, dry_run: bool = True) -> Dict[str, int]:
        """
        Приводить рядкові city/settlement у unified_listings та analytics_extracts
        до канонічної назви з cities за (region_id, name_normalized).
        """
        from business.services.unified_listings_service import _compute_root_geo_from_addresses

        regions_by_name = self._build_regions_by_name_lookup()
        canonical: Dict[tuple, str] = {}
        for city in self.db.cities.find(active_cities_mongo_clause()):
            if city.get("is_oblast_rayon_stub"):
                continue
            key = city.get("name_normalized") or normalize_settlement_key(city.get("name"))
            if not key:
                continue
            rid = str(city.get("region_id") or "")
            display = normalize_settlement_name(city.get("name")) or city.get("name")
            canonical[(rid, key)] = display

        stats = {"unified_listings": 0, "analytics_extracts": 0, "address_ref_names": 0}

        def canon_for(region_id: Optional[str], raw: Optional[str]) -> Optional[str]:
            """Лише в межах області; без cross-region fallback."""
            if not raw or not region_id:
                return None
            key = normalize_settlement_key(raw)
            if not key:
                return None
            if (region_id, key) in canonical:
                return canonical[(region_id, key)]
            return None

        for doc in self.db.unified_listings.find({}):
            rid = self._resolve_region_id(doc.get("region"), regions_by_name)
            changed = False
            new_city = doc.get("city")
            c = canon_for(rid, new_city)
            if c and c != new_city:
                new_city = c
                changed = True

            addresses = doc.get("addresses") or []
            new_addresses = []
            if isinstance(addresses, list):
                for addr in addresses:
                    if not isinstance(addr, dict):
                        new_addresses.append(addr)
                        continue
                    addr = dict(addr)
                    addr_rid = self._resolve_region_id(addr.get("region"), regions_by_name) or rid
                    for fld in ("settlement", "city"):
                        if fld in addr and addr[fld]:
                            fixed = canon_for(addr_rid, addr[fld])
                            if fixed and fixed != addr[fld]:
                                addr[fld] = fixed
                                changed = True
                    new_addresses.append(addr)
            else:
                new_addresses = addresses

            if not changed:
                continue
            stats["unified_listings"] += 1
            if dry_run:
                continue
            root = _compute_root_geo_from_addresses(new_addresses)
            self.db.unified_listings.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "city": new_city,
                        "addresses": new_addresses,
                        "region": root.get("region"),
                        "oblast_raion": root.get("oblast_raion"),
                        "city_district": root.get("city_district"),
                    }
                },
            )

        for doc in self.db.analytics_extracts.find({}):
            rid = self._resolve_region_id(doc.get("region"), regions_by_name)
            update: Dict[str, Any] = {}
            for fld in ("city", "settlement"):
                val = doc.get(fld)
                fixed = canon_for(rid, val)
                if fixed and fixed != val:
                    update[fld] = fixed
            if not update:
                continue
            stats["analytics_extracts"] += 1
            if not dry_run:
                self.db.analytics_extracts.update_one({"_id": doc["_id"]}, {"$set": update})

        if not dry_run:
            stats["address_ref_names"] = self._sync_address_refs_city_names(canonical)

        return stats

    def _build_regions_by_name_lookup(self) -> Dict[str, str]:
        """region label (повна/скорочена) → region_id."""
        lookup: Dict[str, str] = {}
        for r in self.db.regions.find({}, {"name": 1}):
            rid = str(r["_id"])
            name = (r.get("name") or "").strip()
            if not name:
                continue
            lookup[name] = rid
            short = normalize_region_for_repository_lookup(name)
            if short:
                lookup[short] = rid
                lookup[f"{short} область"] = rid
            canonical = normalize_region_to_canonical(name)
            if canonical:
                lookup[canonical] = rid
        return lookup

    def _resolve_region_id(
        self, region_label: Optional[str], regions_by_name: Dict[str, str]
    ) -> Optional[str]:
        if not region_label:
            return None
        raw = str(region_label).strip()
        if raw in regions_by_name:
            return regions_by_name[raw]
        canonical = normalize_region_to_canonical(raw)
        if canonical and canonical in regions_by_name:
            return regions_by_name[canonical]
        short = normalize_region_for_repository_lookup(raw)
        if short and short in regions_by_name:
            return regions_by_name[short]
        return None

    def _build_settlement_catalog(
        self,
    ) -> tuple[
        Dict[str, str],
        Dict[tuple, str],
        Dict[tuple, str],
        set[str],
        Dict[str, Dict[str, Any]],
    ]:
        """regions_by_name, canonical_name (rid,key), canonical_id, homonym_keys, cities_by_id."""
        regions_by_name = self._build_regions_by_name_lookup()
        canonical_name: Dict[tuple, str] = {}
        canonical_id: Dict[tuple, str] = {}
        cities_by_id: Dict[str, Dict[str, Any]] = {}
        by_key: Dict[str, set] = defaultdict(set)

        for city in self.db.cities.find(active_cities_mongo_clause()):
            cid = str(city["_id"])
            cities_by_id[cid] = city
            key = city.get("name_normalized") or normalize_settlement_key(city.get("name"))
            if not key:
                continue
            rid = str(city.get("region_id") or "")
            display = normalize_settlement_name(city.get("name")) or city.get("name")
            canonical_name[(rid, key)] = display
            canonical_id[(rid, key)] = cid
            by_key[key].add(rid)

        homonym_keys = {k for k, rids in by_key.items() if len(rids) > 1}
        return regions_by_name, canonical_name, canonical_id, homonym_keys, cities_by_id

    def audit_homonym_glued_listings(self, *, sample_limit: int = 50) -> Dict[str, Any]:
        """
        Аудит зліплених адрес: текст НП не збігається з каталогом своєї області,
        city_id вказує на НП іншої області, address_refs region/city роз'їхались.
        """
        (
            regions_by_name,
            canonical_name,
            canonical_id,
            homonym_keys,
            cities_by_id,
        ) = self._build_settlement_catalog()

        stats: Dict[str, int] = defaultdict(int)
        samples: List[Dict[str, Any]] = []

        def add_sample(kind: str, payload: Dict[str, Any]) -> None:
            stats[kind] += 1
            if len(samples) < sample_limit:
                samples.append({"kind": kind, **payload})

        for doc in self.db.unified_listings.find(
            {}, {"addresses": 1, "region": 1, "city": 1, "city_id": 1}
        ):
            doc_rid = self._resolve_region_id(doc.get("region"), regions_by_name)
            for addr in doc.get("addresses") or []:
                if not isinstance(addr, dict):
                    continue
                settlement = addr.get("settlement") or addr.get("city")
                if not settlement:
                    continue
                addr_region = addr.get("region") or doc.get("region")
                addr_rid = self._resolve_region_id(addr_region, regions_by_name) or doc_rid
                if not addr_rid:
                    if normalize_settlement_key(settlement) in homonym_keys:
                        add_sample(
                            "homonym_no_region",
                            {
                                "listing_id": str(doc["_id"]),
                                "settlement": settlement,
                                "addr_region": addr_region,
                            },
                        )
                    continue
                key = normalize_settlement_key(settlement)
                pair = (addr_rid, key)
                expected = canonical_name.get(pair)
                if expected and expected != settlement:
                    add_sample(
                        "settlement_text_mismatch",
                        {
                            "listing_id": str(doc["_id"]),
                            "region": addr_region,
                            "was": settlement,
                            "expected": expected,
                        },
                    )
                cid = str(addr.get("city_id") or "")
                if cid and cid in cities_by_id and str(cities_by_id[cid].get("region_id")) != addr_rid:
                    add_sample(
                        "city_id_wrong_region",
                        {
                            "listing_id": str(doc["_id"]),
                            "region": addr_region,
                            "city_id": cid,
                            "city_region_id": str(cities_by_id[cid].get("region_id")),
                        },
                    )
                elif pair in canonical_id and cid != canonical_id[pair]:
                    add_sample(
                        "city_id_missing_or_wrong",
                        {
                            "listing_id": str(doc["_id"]),
                            "region": addr_region,
                            "settlement": settlement,
                            "city_id": cid or None,
                            "expected_city_id": canonical_id[pair],
                        },
                    )

            if doc.get("city") and doc_rid:
                key = normalize_settlement_key(doc.get("city"))
                expected = canonical_name.get((doc_rid, key))
                if expected and expected != doc.get("city"):
                    add_sample(
                        "root_city_mismatch",
                        {
                            "listing_id": str(doc["_id"]),
                            "region": doc.get("region"),
                            "was": doc.get("city"),
                            "expected": expected,
                        },
                    )

        for collection_name, path in self.SOURCE_COLLECTIONS:
            parts = path.split(".")
            for doc in self.db[collection_name].find(
                {path: {"$exists": True, "$ne": []}}, {"_id": 1, path: 1}
            ):
                refs = doc
                for p in parts:
                    refs = (refs or {}).get(p)
                if not isinstance(refs, list):
                    continue
                for ref in refs:
                    if not isinstance(ref, dict):
                        continue
                    rid = str((ref.get("region") or {}).get("_id") or "")
                    city = ref.get("city") or {}
                    cid = str(city.get("_id") or "")
                    if not rid or not cid or cid not in cities_by_id:
                        continue
                    if str(cities_by_id[cid].get("region_id")) != rid:
                        add_sample(
                            "address_refs_region_city_mismatch",
                            {
                                "collection": collection_name,
                                "doc_id": str(doc["_id"]),
                                "region_id": rid,
                                "city_id": cid,
                                "city_region_id": str(cities_by_id[cid].get("region_id")),
                                "city_name": city.get("name"),
                            },
                        )

        return {
            "homonym_keys_in_catalog": len(homonym_keys),
            "issues": dict(stats),
            "issues_total": sum(stats.values()),
            "samples": samples,
        }

    def repair_homonym_glued_listings(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Виправляє зліплені адреси в unified_listings та address_refs (region-scoped).
        Потім normalize_denormalized_settlements для узгодження текстів.
        """
        from business.services.unified_listings_service import _compute_root_geo_from_addresses

        audit_before = self.audit_homonym_glued_listings(sample_limit=30)
        stats: Dict[str, int] = defaultdict(int)
        stats["audit_issues_before"] = audit_before["issues_total"]

        (
            regions_by_name,
            canonical_name,
            canonical_id,
            _homonym_keys,
            cities_by_id,
        ) = self._build_settlement_catalog()

        for doc in self.db.unified_listings.find(
            {}, {"addresses": 1, "region": 1, "city": 1, "city_id": 1}
        ):
            doc_rid = self._resolve_region_id(doc.get("region"), regions_by_name)
            changed = False
            new_addresses: List[Dict[str, Any]] = []
            new_city = doc.get("city")

            for addr in doc.get("addresses") or []:
                if not isinstance(addr, dict):
                    new_addresses.append(addr)
                    continue
                addr = dict(addr)
                settlement = addr.get("settlement") or addr.get("city")
                addr_region = addr.get("region") or doc.get("region")
                addr_rid = self._resolve_region_id(addr_region, regions_by_name) or doc_rid

                if settlement and addr_rid:
                    key = normalize_settlement_key(settlement)
                    pair = (addr_rid, key)
                    if pair in canonical_name:
                        canon = canonical_name[pair]
                        if addr.get("settlement") != canon:
                            addr["settlement"] = canon
                            changed = True
                            stats["address_settlement_renamed"] += 1
                        if addr.get("city") is not None and addr.get("city") != canon:
                            addr["city"] = canon
                            changed = True
                        cid = canonical_id.get(pair)
                        if cid and str(addr.get("city_id") or "") != cid:
                            addr["city_id"] = cid
                            changed = True
                            stats["address_city_id_set"] += 1
                    elif str(addr.get("city_id") or "") in cities_by_id:
                        cid = str(addr.get("city_id"))
                        if str(cities_by_id[cid].get("region_id")) != addr_rid:
                            addr.pop("city_id", None)
                            changed = True
                            stats["address_city_id_cleared"] += 1

                new_addresses.append(addr)

            if doc.get("city") and doc_rid:
                key = normalize_settlement_key(doc.get("city"))
                pair = (doc_rid, key)
                if pair in canonical_name:
                    canon = canonical_name[pair]
                    if new_city != canon:
                        new_city = canon
                        changed = True
                        stats["root_city_renamed"] += 1

            if not changed:
                continue
            stats["unified_listings_updated"] += 1
            if dry_run:
                continue

            root_geo = _compute_root_geo_from_addresses(new_addresses)
            update_fields: Dict[str, Any] = {
                "addresses": new_addresses,
                "region": root_geo.get("region") or doc.get("region"),
                "oblast_raion": root_geo.get("oblast_raion"),
                "city_district": root_geo.get("city_district"),
            }
            if new_city is not None:
                update_fields["city"] = new_city
            if doc_rid and new_city:
                key = normalize_settlement_key(new_city)
                cid = canonical_id.get((doc_rid, key))
                if cid:
                    update_fields["city_id"] = cid
            self.db.unified_listings.update_one({"_id": doc["_id"]}, {"$set": update_fields})

        stats["address_refs_fixed"] = self._repair_address_refs_region_city(
            cities_by_id, canonical_name, canonical_id, dry_run=dry_run
        )

        if not dry_run:
            null_unset = self.db.cities.update_many(
                {"merged_into": None},
                {"$unset": {"merged_into": ""}},
            )
            stats["cities_merged_into_null_unset"] = null_unset.modified_count
            denorm = self.normalize_denormalized_settlements(dry_run=False)
            stats["denorm"] = denorm

        audit_after = self.audit_homonym_glued_listings(sample_limit=30)
        stats["audit_issues_after"] = audit_after["issues_total"]
        return {
            "dry_run": dry_run,
            "stats": dict(stats),
            "audit_before": audit_before,
            "audit_after": audit_after,
        }

    def _repair_address_refs_region_city(
        self,
        cities_by_id: Dict[str, Dict[str, Any]],
        canonical_name: Dict[tuple, str],
        canonical_id: Dict[tuple, str],
        *,
        dry_run: bool = True,
    ) -> int:
        """Виправляє city._id у address_refs, якщо область ref не збігається з region_id міста."""
        updated_docs = 0
        for collection_name, path in self.SOURCE_COLLECTIONS:
            parent_field = path.rsplit(".", 1)[0]
            for doc in self.db[collection_name].find({path: {"$exists": True, "$ne": []}}):
                parent = doc
                for part in parent_field.split("."):
                    parent = (parent or {}).get(part)
                refs_list = parent if isinstance(parent, list) else [parent] if parent else []
                if not isinstance(refs_list, list):
                    continue
                changed = False
                new_refs = []
                for ref in refs_list:
                    if not isinstance(ref, dict):
                        new_refs.append(ref)
                        continue
                    ref = dict(ref)
                    region = dict(ref.get("region") or {})
                    city = dict(ref.get("city") or {})
                    rid = str(region.get("_id") or "")
                    cid = str(city.get("_id") or "")
                    cname = (city.get("name") or "").strip()

                    if rid and cname:
                        key = normalize_settlement_key(cname)
                        pair = (rid, key)
                        if pair in canonical_id:
                            new_cid = canonical_id[pair]
                            new_name = canonical_name[pair]
                            if cid != new_cid or city.get("name") != new_name:
                                city["_id"] = new_cid
                                city["name"] = new_name
                                ref["city"] = city
                                changed = True
                        elif cid and cid in cities_by_id and str(cities_by_id[cid].get("region_id")) != rid:
                            if pair in canonical_id:
                                city["_id"] = canonical_id[pair]
                                city["name"] = canonical_name[pair]
                                ref["city"] = city
                                changed = True
                    elif cid and cid in cities_by_id:
                        display = normalize_settlement_name(cities_by_id[cid].get("name")) or cities_by_id[cid].get("name")
                        if display and city.get("name") != display:
                            city["name"] = display
                            ref["city"] = city
                            changed = True

                    new_refs.append(ref)

                if not changed:
                    continue
                updated_docs += 1
                if dry_run:
                    continue
                val = new_refs if isinstance(parent, list) else new_refs[0]
                self.db[collection_name].update_one({"_id": doc["_id"]}, {"$set": {path: val}})
        return updated_docs

    def _sync_address_refs_city_names(self, canonical: Dict[tuple, str]) -> int:
        """Оновлює city.name у address_refs за канонічною назвою (за _id або ключем)."""
        id_to_name: Dict[str, str] = {}
        for city in self.db.cities.find(active_cities_mongo_clause()):
            cid = str(city["_id"])
            display = normalize_settlement_name(city.get("name")) or city.get("name")
            if display:
                id_to_name[cid] = display

        updated = 0
        for collection_name, path in self.SOURCE_COLLECTIONS:
            parent_field = path.rsplit(".", 1)[0]
            for doc in self.db[collection_name].find({}):
                parent = doc
                for part in parent_field.split("."):
                    parent = (parent or {}).get(part)
                refs_list = parent if isinstance(parent, list) else [parent] if parent else []
                changed = False
                new_refs = []
                for ref in refs_list:
                    if not isinstance(ref, dict):
                        new_refs.append(ref)
                        continue
                    ref = dict(ref)
                    city = dict(ref.get("city") or {})
                    cid = str(city.get("_id") or "")
                    if cid and cid in id_to_name and city.get("name") != id_to_name[cid]:
                        city["name"] = id_to_name[cid]
                        ref["city"] = city
                        changed = True
                    new_refs.append(ref)
                if not changed:
                    continue
                val = new_refs if isinstance(parent, list) else new_refs[0]
                self.db[collection_name].update_one({"_id": doc["_id"]}, {"$set": {path: val}})
                updated += 1
        return updated

    def _resolve_canonical_id(self, city_id: str, id_map: Dict[str, str]) -> str:
        seen = set()
        current = city_id
        while current in id_map and current not in seen:
            seen.add(current)
            current = id_map[current]
        return current

    def _build_id_to_name_map(self, id_map: Dict[str, str]) -> Dict[str, str]:
        canonical_ids = {self._resolve_canonical_id(old_id, id_map) for old_id in id_map}
        result: Dict[str, str] = {}
        for cid in canonical_ids:
            doc = self.db.cities.find_one({"_id": self._object_id(cid)}, {"name": 1})
            if doc:
                result[cid] = doc.get("name") or ""
        return result

    def _count_city_references(self) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for collection_name, path in self.SOURCE_COLLECTIONS:
            pipeline = [
                {"$match": {f"{path}.city._id": {"$exists": True, "$ne": None}}},
                {"$project": {"refs": f"${path}"}},
                {"$unwind": "$refs"},
                {"$group": {"_id": "$refs.city._id", "count": {"$sum": 1}}},
            ]
            for row in self.db[collection_name].aggregate(pipeline):
                cid = str(row.get("_id") or "")
                if cid:
                    counts[cid] += int(row.get("count") or 0)
        return dict(counts)

    def _count_streets_by_city(self) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for row in self.db.streets.aggregate(
            [
                {"$group": {"_id": "$city_id", "count": {"$sum": 1}}},
            ]
        ):
            cid = str(row.get("_id") or "")
            if cid:
                counts[cid] = int(row.get("count") or 0)
        return dict(counts)

    @staticmethod
    def _object_id(value: str):
        from bson import ObjectId

        return ObjectId(value)
