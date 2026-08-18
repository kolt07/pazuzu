# -*- coding: utf-8 -*-
"""
Повторна обробка геоконтексту OLX-оголошень: збагачення адрес НП/областю та перегеокодування.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from config.settings import Settings
from business.services.geocoding_service import GeocodingService
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.unified_listings_service import UnifiedListingsService
from data.repositories.olx_listings_repository import OlxListingsRepository
from utils.address_geo_enrichment import (
    analyze_geo_reprocess_candidate,
    build_olx_geo_reprocess_mongo_query,
    enrich_llm_geo_result,
    listing_needs_geo_reprocess,
    normalize_olx_detail_location,
)
from utils.price_metrics import compute_price_metrics

logger = logging.getLogger(__name__)

LogFn = Optional[Callable[[str], None]]


class GeoContextReprocessService:
    """Масова переобробка оголошень з невірним геоконтекстом (street-only / склейка НП+район)."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.olx_repo = OlxListingsRepository()
        self.llm_extractor = OlxLLMExtractorService(self.settings)
        self.geocoding_service = GeocodingService(self.settings)
        self.unified_service = UnifiedListingsService(self.settings)

    def collect_candidate_urls(
        self,
        limit: int = 0,
        region: Optional[str] = None,
        include_empty_root: bool = True,
    ) -> List[str]:
        """Відбирає URL оголошень, що потребують переобробки геоконтексту."""
        query = build_olx_geo_reprocess_mongo_query(region=region)
        cursor = self.olx_repo.collection.find(
            query,
            {"url": 1, "search_data": 1, "detail": 1},
        ).sort("updated_at", -1)
        if limit and limit > 0:
            cursor = cursor.limit(max(limit * 5, limit))

        urls: List[str] = []
        for doc in cursor:
            url = doc.get("url")
            if not url:
                continue
            if not listing_needs_geo_reprocess(
                doc.get("search_data") or {},
                doc.get("detail") or {},
            ):
                continue
            urls.append(url)
            if limit and limit > 0 and len(urls) >= limit:
                break

        if include_empty_root and (not limit or len(urls) < limit):
            for url in self._collect_empty_root_olx_urls(
                limit=(limit - len(urls)) if limit and limit > 0 else 0,
                region=region,
                exclude=set(urls),
            ):
                urls.append(url)
                if limit and limit > 0 and len(urls) >= limit:
                    break
        return list(dict.fromkeys(urls))

    def _collect_empty_root_olx_urls(
        self,
        limit: int = 0,
        region: Optional[str] = None,
        exclude: Optional[set] = None,
    ) -> List[str]:
        """OLX у unified з порожнім root city — типовий miss фільтра НП."""
        from data.repositories.unified_listings_repository import UnifiedListingsRepository

        exclude = exclude or set()
        empty_city = {
            "source": "olx",
            "status": "активне",
            "$or": [
                {"city": {"$exists": False}},
                {"city": None},
                {"city": ""},
            ],
        }
        if region:
            from utils.address_geo_enrichment import normalize_region_name

            region_norm = normalize_region_name(region)
            empty_city = {
                "$and": [
                    empty_city,
                    {
                        "$or": [
                            {"region": {"$regex": region_norm, "$options": "i"}},
                            {"addresses.region": {"$regex": region_norm, "$options": "i"}},
                        ]
                    },
                ]
            }
        repo = UnifiedListingsRepository()
        cursor = repo.collection.find(
            empty_city,
            {"page_url": 1, "source_id": 1},
        ).sort("source_updated_at", -1)
        if limit and limit > 0:
            cursor = cursor.limit(limit * 2)

        urls: List[str] = []
        for doc in cursor:
            url = doc.get("page_url") or doc.get("source_id")
            if not url or url in exclude:
                continue
            urls.append(url)
            if limit and limit > 0 and len(urls) >= limit:
                break
        return urls

    def preview_candidates(
        self,
        limit: int = 20,
        region: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Повертає короткий опис кандидатів для dry-run."""
        urls = self.collect_candidate_urls(limit=limit, region=region)
        previews: List[Dict[str, Any]] = []
        for url in urls:
            doc = self.olx_repo.find_by_url(url)
            if not doc:
                continue
            analysis = analyze_geo_reprocess_candidate(
                doc.get("search_data") or {},
                doc.get("detail") or {},
            )
            previews.append(
                {
                    "url": url,
                    "reasons": analysis.get("reasons") or [],
                    "listing_context": analysis.get("listing_context") or {},
                    "current_query": analysis.get("current_query") or "",
                    "enriched_query": analysis.get("enriched_query") or "",
                }
            )
        return previews

    def _usd_rate(self):
        try:
            from business.services.currency_rate_service import CurrencyRateService

            return CurrencyRateService(self.settings).get_today_usd_rate(allow_fetch=True)
        except Exception:
            return None

    def reprocess_url(
        self,
        listing_url: str,
        force_llm: bool = False,
        resync_only: bool = False,
        log_fn: LogFn = None,
    ) -> Dict[str, Any]:
        """Переобробляє одне оголошення: збагачення адрес + геокодування + sync unified.

        resync_only=True: нормалізує detail.location, лишає resolved_locations,
        лише переобраховує addresses у unified (без Google Geocoding API).
        """
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        doc = self.olx_repo.find_by_url(listing_url)
        if not doc:
            return {"success": False, "message": "Оголошення не знайдено", "updated": False}

        search_data = dict(doc.get("search_data") or {})
        detail_data = dict(doc.get("detail") or {})
        analysis = analyze_geo_reprocess_candidate(search_data, detail_data)
        if not force_llm and not resync_only and not analysis.get("needs_reprocess"):
            return {
                "success": True,
                "message": "Переобробка не потрібна",
                "updated": False,
                "skipped": True,
                "reasons": analysis.get("reasons") or [],
            }

        try:
            # Завжди розклеюємо detail.location.city («Київ, Голосіївський»)
            if isinstance(detail_data.get("location"), dict):
                detail_data["location"] = normalize_olx_detail_location(
                    detail_data.get("location")
                )

            if resync_only and not force_llm:
                log(f"[GeoReprocess] Resync-only: {listing_url[:70]}...")
                llm_data = dict(detail_data.get("llm") or {})
                detail_data["llm"] = enrich_llm_geo_result(
                    llm_data,
                    listing_context=analysis.get("listing_context") or {},
                )
                usd_rate = self._usd_rate()
                self.olx_repo.upsert_listing(
                    listing_url,
                    search_data,
                    detail=detail_data,
                    is_active=doc.get("is_active", True),
                )
                self.unified_service.sync_olx_listing(
                    listing_url,
                    usd_rate_override=usd_rate,
                    side_effects=False,
                )
                return {
                    "success": True,
                    "message": "Картку нормалізовано, unified пересинхронізовано",
                    "updated": True,
                    "skipped": False,
                    "resync_only": True,
                    "reasons": analysis.get("reasons") or [],
                }

            if force_llm:
                log(f"[GeoReprocess] LLM + гео: {listing_url[:70]}...")
                llm_data = self.llm_extractor.extract_structured_data(
                    search_data,
                    detail_data,
                    force_refresh=True,
                )
                if llm_data:
                    detail_data["llm"] = llm_data
                new_hash = self.llm_extractor.calculate_listing_hash(search_data, detail_data)
                if new_hash:
                    detail_data["llm_content_hash"] = new_hash
            else:
                log(f"[GeoReprocess] Збагачення + гео: {listing_url[:70]}...")
                llm_data = dict(detail_data.get("llm") or {})
                detail_data["llm"] = enrich_llm_geo_result(
                    llm_data,
                    listing_context=analysis.get("listing_context") or {},
                )

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

            usd_rate = self._usd_rate()
            llm_struct = detail_data.get("llm") or {}
            total_area_m2 = llm_struct.get("total_area_m2") or search_data.get("area_m2")
            land_area_sqm = llm_struct.get("land_area_sqm")
            if land_area_sqm is None and llm_struct.get("land_area_ha") is not None:
                try:
                    land_area_sqm = float(llm_struct["land_area_ha"]) * 10000.0
                except (TypeError, ValueError):
                    land_area_sqm = None
            price_value = search_data.get("price_value")
            detail_data["price_metrics"] = compute_price_metrics(
                total_price_uah=price_value,
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
                if "llm" not in detail_data:
                    detail_data["llm"] = {}
                detail_data["llm"]["parsed_address"] = {"formatted_address": formatted_address}

            self.olx_repo.upsert_listing(
                listing_url,
                search_data,
                detail=detail_data,
                is_active=doc.get("is_active", True),
            )
            self.unified_service.sync_olx_listing(listing_url, usd_rate_override=usd_rate)

            return {
                "success": True,
                "message": "Геоконтекст оновлено",
                "updated": True,
                "skipped": False,
                "reasons": analysis.get("reasons") or [],
                "enriched_query": analysis.get("enriched_query") or "",
            }
        except Exception as exc:
            logger.exception("Помилка geo reprocess %s: %s", listing_url[:60], exc)
            return {"success": False, "message": str(exc), "updated": False}

    def run_batch(
        self,
        urls: Optional[List[str]] = None,
        limit: int = 0,
        region: Optional[str] = None,
        force_llm: bool = False,
        resync_only: bool = False,
        dry_run: bool = False,
        log_fn: LogFn = None,
    ) -> Dict[str, Any]:
        """Масова переобробка кандидатів."""
        selected = urls if urls is not None else self.collect_candidate_urls(limit=limit, region=region)
        stats = {
            "selected": len(selected),
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "dry_run": dry_run,
            "resync_only": resync_only,
        }
        if dry_run:
            previews = self.preview_candidates(limit=limit or 20, region=region)
            stats["previews"] = previews
            return stats

        for index, url in enumerate(selected, start=1):
            result = self.reprocess_url(
                url,
                force_llm=force_llm,
                resync_only=resync_only,
                log_fn=log_fn,
            )
            stats["processed"] += 1
            if result.get("updated"):
                stats["updated"] += 1
            elif result.get("skipped"):
                stats["skipped"] += 1
            if not result.get("success"):
                stats["errors"] += 1
            if log_fn and index % 25 == 0:
                log_fn(
                    f"[GeoReprocess] Прогрес: {index}/{len(selected)} "
                    f"(оновлено {stats['updated']}, пропущено {stats['skipped']}, помилок {stats['errors']})"
                )
        return stats
