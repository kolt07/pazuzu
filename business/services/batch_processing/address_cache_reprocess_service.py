# -*- coding: utf-8 -*-
"""
Переобробка адрес з кешу LLM (без повторного виклику моделі).

Бере структуровані addresses[] з detail.llm / llm_cache, санітизує райони,
збагачує геоконтекст, перегеокодовує / будує address_refs, sync unified.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from config.settings import Settings
from business.services.batch_processing.filters import (
    BatchJobFilters,
    build_unified_listing_query,
)
from business.services.geocoding_service import GeocodingService
from business.services.llm_cache_service import LLMCacheService
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.unified_listings_service import UnifiedListingsService
from data.repositories.llm_cache_repository import LLMCacheRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from utils.address_geo_enrichment import build_listing_context, enrich_llm_geo_result
from utils.district_normalizer import sanitize_llm_address_districts
from utils.price_metrics import compute_price_metrics

logger = logging.getLogger(__name__)

ProgressFn = Optional[Callable[[Dict[str, Any]], None]]
LogFn = Optional[Callable[[str], None]]


def _llm_addresses_from_result(llm_result: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Нормалізує addresses[] з кешу LLM (включно зі старими плоскими полями)."""
    if not isinstance(llm_result, dict):
        return []
    addresses = llm_result.get("addresses") or []
    out: List[Dict[str, Any]] = [dict(a) for a in addresses if isinstance(a, dict)]
    if out:
        return out
    if llm_result.get("address_region") or llm_result.get("address_city"):
        return [
            {
                "region": llm_result.get("address_region") or "",
                "district": llm_result.get("address_district") or "",
                "settlement_type": llm_result.get("address_settlement_type") or "",
                "settlement": llm_result.get("address_city") or "",
                "settlement_district": llm_result.get("address_settlement_district") or "",
                "street_type": llm_result.get("address_street_type") or "",
                "street": llm_result.get("address_street") or "",
                "building": llm_result.get("address_building") or "",
                "building_part": llm_result.get("address_building_part") or "",
                "room": llm_result.get("address_room") or "",
            }
        ]
    return []


class AddressCacheReprocessService:
    """Масова переобробка адрес з llm_cache / detail.llm без виклику LLM."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.unified_repo = UnifiedListingsRepository()
        self.olx_repo = OlxListingsRepository()
        self.prozorro_repo = ProZorroAuctionsRepository()
        self.llm_cache = LLMCacheService()
        self.llm_cache_repo = LLMCacheRepository()
        self.geocoding_service = GeocodingService(self.settings)
        self.unified_service = UnifiedListingsService(self.settings)
        self.llm_extractor = OlxLLMExtractorService(self.settings)

    def collect_targets(self, filters: BatchJobFilters) -> List[Tuple[str, str]]:
        """Повертає список (source, source_id) з unified_listings."""
        query = build_unified_listing_query(filters)
        projection = {"source": 1, "source_id": 1}
        cursor = self.unified_repo.collection.find(query, projection).sort("source_updated_at", -1)
        limit = filters.limit if filters.limit and filters.limit > 0 else 0
        if limit:
            cursor = cursor.limit(limit)

        targets: List[Tuple[str, str]] = []
        allowed_sources = set(filters.resolved_sources())
        for doc in cursor:
            source = (doc.get("source") or "").strip().lower()
            source_id = doc.get("source_id")
            if not source or not source_id or source not in allowed_sources:
                continue
            targets.append((source, source_id))
        return list(dict.fromkeys(targets))

    def _load_olx_llm(
        self,
        search_data: Dict[str, Any],
        detail_data: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """Повертає (llm_result, source_tag): detail_llm | llm_cache | none."""
        existing = detail_data.get("llm")
        if isinstance(existing, dict) and _llm_addresses_from_result(existing):
            return dict(existing), "detail_llm"

        description_text = OlxLLMExtractorService._build_description_text(search_data, detail_data)
        cached = self.llm_cache.get_cached_result(description_text) if description_text else None
        if isinstance(cached, dict) and (
            _llm_addresses_from_result(cached) or cached.get("property_type") is not None
        ):
            return dict(cached), "llm_cache"
        if isinstance(existing, dict) and existing:
            return dict(existing), "detail_llm_empty_addresses"
        return None, "none"

    def reprocess_olx(
        self,
        listing_url: str,
        log_fn: LogFn = None,
    ) -> Dict[str, Any]:
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        doc = self.olx_repo.find_by_url(listing_url)
        if not doc:
            return {"success": False, "updated": False, "message": "OLX не знайдено", "source": "olx"}

        search_data = dict(doc.get("search_data") or {})
        detail_data = dict(doc.get("detail") or {})
        llm_raw, llm_source = self._load_olx_llm(search_data, detail_data)
        if not llm_raw:
            return {
                "success": True,
                "updated": False,
                "skipped": True,
                "reason": "no_llm_cache",
                "message": "Немає detail.llm / llm_cache",
                "source": "olx",
            }

        try:
            listing_context = build_listing_context(search_data, detail_data)
            llm_data = enrich_llm_geo_result(llm_raw, listing_context=listing_context)
            # Гарантуємо addresses навіть зі старого кешу
            addrs = _llm_addresses_from_result(llm_data)
            if addrs and not llm_data.get("addresses"):
                llm_data["addresses"] = [
                    sanitize_llm_address_districts(a) for a in addrs
                ]
            detail_data["llm"] = llm_data

            for key in ("resolved_locations", "geocode_query_hashes", "address_refs"):
                detail_data.pop(key, None)

            geography_service = None
            try:
                from business.services.geography_service import GeographyService

                geography_service = GeographyService()
            except ImportError:
                pass

            from scripts.olx_scraper.helpers import (
                _address_line_from_llm_address,
                _collect_and_geocode_locations,
            )

            result = _collect_and_geocode_locations(
                search_data,
                detail_data,
                self.geocoding_service,
                geography_service,
            )
            if len(result) == 3:
                geocode_hashes, resolved_locations, address_refs_list = result
            else:
                geocode_hashes, resolved_locations = result
                address_refs_list = []

            detail_data["geocode_query_hashes"] = geocode_hashes
            detail_data["resolved_locations"] = resolved_locations
            if address_refs_list:
                detail_data["address_refs"] = address_refs_list

            usd_rate = None
            try:
                from business.services.currency_rate_service import CurrencyRateService

                usd_rate = CurrencyRateService(self.settings).get_today_usd_rate(allow_fetch=True)
            except Exception:
                pass

            llm_struct = detail_data.get("llm") or {}
            total_area_m2 = llm_struct.get("total_area_m2") or search_data.get("area_m2")
            land_area_sqm = llm_struct.get("land_area_sqm")
            if land_area_sqm is None and llm_struct.get("land_area_ha") is not None:
                try:
                    land_area_sqm = float(llm_struct["land_area_ha"]) * 10000.0
                except (TypeError, ValueError):
                    land_area_sqm = None
            detail_data["price_metrics"] = compute_price_metrics(
                total_price_uah=search_data.get("price_value"),
                building_area_sqm=total_area_m2,
                land_area_sqm=land_area_sqm,
                uah_per_usd=usd_rate,
            )

            formatted_address = ""
            if address_refs_list and geography_service:
                parts = []
                for refs in address_refs_list:
                    if isinstance(refs, dict):
                        addr = geography_service.format_address(refs)
                        if addr and addr not in parts:
                            parts.append(addr)
                formatted_address = "; ".join(parts) if parts else ""
            if not formatted_address:
                for addr in (detail_data.get("llm") or {}).get("addresses") or []:
                    if isinstance(addr, dict):
                        line = _address_line_from_llm_address(addr)
                        if line:
                            formatted_address = line
                            break
            if not formatted_address:
                formatted_address = (search_data.get("location") or "").strip()
            if formatted_address:
                detail_data.setdefault("llm", {})["parsed_address"] = {
                    "formatted_address": formatted_address
                }

            self.olx_repo.upsert_listing(
                listing_url,
                search_data,
                detail=detail_data,
                is_active=doc.get("is_active", True),
            )
            self.unified_service.sync_olx_listing(listing_url, usd_rate_override=usd_rate)
            log(f"[AddressCache] OLX оновлено ({llm_source}): {listing_url[:70]}")
            return {
                "success": True,
                "updated": True,
                "skipped": False,
                "llm_source": llm_source,
                "addresses_count": len((detail_data.get("llm") or {}).get("addresses") or []),
                "address_refs_count": len(address_refs_list or []),
                "source": "olx",
            }
        except Exception as exc:
            logger.exception("Address cache reprocess OLX failed %s: %s", listing_url[:60], exc)
            return {"success": False, "updated": False, "message": str(exc), "source": "olx"}

    def _load_prozorro_llm(self, doc: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
        desc_hash = doc.get("description_hash")
        if desc_hash:
            entry = self.llm_cache_repo.find_by_description_hash(desc_hash)
            if entry and isinstance(entry.get("result"), dict):
                return dict(entry["result"]), "llm_cache"

        auction_data = doc.get("auction_data") or {}
        description = ""
        desc_obj = auction_data.get("description")
        if isinstance(desc_obj, dict):
            description = desc_obj.get("uk_UA") or desc_obj.get("en_US") or ""
        elif isinstance(desc_obj, str):
            description = desc_obj
        if description:
            cached = self.llm_cache.get_cached_result(description)
            if isinstance(cached, dict):
                return dict(cached), "llm_cache_by_text"
        return None, "none"

    def reprocess_prozorro(
        self,
        auction_id: str,
        log_fn: LogFn = None,
    ) -> Dict[str, Any]:
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        doc = self.prozorro_repo.find_by_auction_id(auction_id)
        if not doc or not doc.get("auction_data"):
            return {
                "success": False,
                "updated": False,
                "message": "ProZorro не знайдено",
                "source": "prozorro",
            }

        llm_raw, llm_source = self._load_prozorro_llm(doc)
        auction_data = deepcopy(doc["auction_data"])

        try:
            from business.services.geography_service import GeographyService
            from business.services.prozorro_service import ProZorroService

            pz = ProZorroService(self.settings)
            items_addresses = pz._extract_addresses_from_items(auction_data) or []
            llm_addresses = _llm_addresses_from_result(llm_raw) if llm_raw else []

            if not items_addresses and not llm_addresses:
                return {
                    "success": True,
                    "updated": False,
                    "skipped": True,
                    "reason": "no_addresses",
                    "message": "Немає адрес у items / llm_cache",
                    "source": "prozorro",
                    "llm_source": llm_source,
                }

            final_addresses: List[Dict[str, Any]] = []
            if items_addresses:
                final_addresses.extend(items_addresses)
            if llm_addresses:
                final_addresses.extend(llm_addresses)

            enriched = enrich_llm_geo_result(
                {"addresses": final_addresses},
                listing_context={},
            )
            final_addresses = [
                sanitize_llm_address_districts(a)
                for a in (enriched.get("addresses") or [])
                if isinstance(a, dict)
            ]

            geography_service = GeographyService()
            address_refs_list: List[Dict[str, Any]] = []
            for addr in final_addresses:
                try:
                    resolved = geography_service.resolve_address(addr)
                    if resolved.get("region_id") or resolved.get("city_id"):
                        address_refs_list.append(resolved["address_refs"])
                except Exception:
                    continue

            auction_data["address_refs"] = address_refs_list
            auction_data["llm_addresses"] = final_addresses

            self.prozorro_repo.upsert_auction(
                auction_id=auction_id,
                auction_data=auction_data,
                version_hash=doc.get("version_hash") or "",
                description_hash=doc.get("description_hash"),
                last_updated=datetime.now(timezone.utc),
            )
            self.unified_service.sync_prozorro_auction(auction_id)
            log(
                f"[AddressCache] ProZorro оновлено ({llm_source}): {auction_id} "
                f"refs={len(address_refs_list)}"
            )
            return {
                "success": True,
                "updated": True,
                "skipped": False,
                "llm_source": llm_source,
                "addresses_count": len(final_addresses),
                "address_refs_count": len(address_refs_list),
                "source": "prozorro",
            }
        except Exception as exc:
            logger.exception("Address cache reprocess ProZorro failed %s: %s", auction_id, exc)
            return {"success": False, "updated": False, "message": str(exc), "source": "prozorro"}

    def reprocess_one(
        self,
        source: str,
        source_id: str,
        log_fn: LogFn = None,
    ) -> Dict[str, Any]:
        src = (source or "").strip().lower()
        if src == "olx":
            return self.reprocess_olx(source_id, log_fn=log_fn)
        if src == "prozorro":
            return self.reprocess_prozorro(source_id, log_fn=log_fn)
        return {"success": False, "updated": False, "message": f"Невідоме джерело: {source}"}

    def run(
        self,
        filters: BatchJobFilters,
        progress_fn: ProgressFn = None,
        log_fn: LogFn = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        targets = self.collect_targets(filters)
        stats: Dict[str, Any] = {
            "selected": len(targets),
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "no_cache": 0,
            "dry_run": dry_run,
            "by_source": {"olx": 0, "prozorro": 0},
        }

        def progress(message: str, **extra: Any) -> None:
            payload = {
                "message": message,
                "processed": stats["processed"],
                "total": stats["selected"],
                "updated": stats["updated"],
                "skipped": stats["skipped"],
                "errors": stats["errors"],
                "no_cache": stats["no_cache"],
                **extra,
            }
            if progress_fn:
                progress_fn(payload)
            if log_fn:
                log_fn(message)

        if dry_run:
            progress(f"[AddressCache] Dry-run: відібрано {len(targets)} оголошень")
            stats["targets_preview"] = [
                {"source": s, "source_id": i} for s, i in targets[:50]
            ]
            return stats

        progress(f"[AddressCache] Старт: {len(targets)} оголошень (без LLM)")
        for index, (source, source_id) in enumerate(targets, start=1):
            if index <= 5 or index % 25 == 0:
                short_id = source_id if len(source_id) <= 80 else source_id[:77] + "..."
                progress(f"[AddressCache] Обробка {index}/{len(targets)} {source} {short_id}")
            result = self.reprocess_one(source, source_id, log_fn=None)
            stats["processed"] += 1
            stats["by_source"][source] = stats["by_source"].get(source, 0) + 1
            if result.get("updated"):
                stats["updated"] += 1
            elif result.get("skipped"):
                stats["skipped"] += 1
                if result.get("reason") in ("no_llm_cache", "no_addresses"):
                    stats["no_cache"] += 1
            if not result.get("success"):
                stats["errors"] += 1
                if index <= 10 and log_fn:
                    log_fn(f"[AddressCache] Помилка {source} {source_id[:60]}: {result.get('message')}")

            if index == 1 or index % 25 == 0 or index == len(targets):
                progress(
                    f"[AddressCache] {index}/{len(targets)} "
                    f"(оновлено {stats['updated']}, пропущено {stats['skipped']}, "
                    f"помилок {stats['errors']})"
                )
        progress(
            f"[AddressCache] Готово: оновлено {stats['updated']} з {stats['selected']}"
        )
        return stats
