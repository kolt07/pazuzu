# -*- coding: utf-8 -*-
"""
Точковий пошук процедур ProZorro.Sale через POST /api/search/procedures
(офіційний Search API), далі GET /procedures/{id} для деталей.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from business.services.prozorro_service import ProZorroService
from business.services.source_filter_mapper import SourceQueryPlan
from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository
from utils.deal_type import DEAL_RENT, DEAL_SALE, deal_type_from_prozorro_data
from utils.hash_utils import extract_auction_id
from utils.ukraine_regions import is_special_city_region
from business.services.llm_processing_regions_service import normalize_region_name

logger = logging.getLogger(__name__)

_METHODS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "prozorro_selling_methods.yaml"

SEARCH_LIMIT = 100
SEARCH_MAX_PAGES = 50


def _load_selling_methods() -> Dict[str, List[str]]:
    try:
        with open(_METHODS_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return {
            "rent": list(data.get("rent_procedure_types") or []),
            "sale": list(data.get("sale_procedure_types") or []),
            "rent_sub": [str(s).lower() for s in (data.get("rent_selling_method_substrings") or [])],
        }
    except Exception as e:
        logger.warning("Не вдалося прочитати prozorro_selling_methods.yaml: %s", e)
        return {
            "rent": ["legitimatePropertyLease", "landRental", "commercialPropertyLease"],
            "sale": ["smallPrivatization", "landSell", "commercialSell", "simpleSell"],
            "rent_sub": ["lease", "rental"],
        }


def _region_search_values(name: str) -> List[str]:
    raw = (name or "").strip()
    if not raw:
        return []
    short = normalize_region_name(raw) or raw
    values = [raw, short]
    if is_special_city_region(short) or short in ("Київ", "Севастополь"):
        values.extend([f"м. {short}", f"місто {short}"])
    else:
        if "област" not in short.lower():
            values.append(f"{short} область")
    out = []
    seen = set()
    for v in values:
        k = v.strip()
        if k and k.lower() not in seen:
            seen.add(k.lower())
            out.append(k)
    return out


def _is_property_auction(svc: ProZorroService, auction_data: Dict[str, Any]) -> bool:
    items = auction_data.get("items") or []
    if not isinstance(items, list) or not items:
        return False
    allowed = svc.get_allowed_classification_codes()
    for item in items:
        if not isinstance(item, dict):
            continue
        classification = item.get("classification") or {}
        scheme = classification.get("scheme", "")
        class_id = classification.get("id", "")
        if scheme == "CAV" and class_id:
            for allowed_code in allowed:
                if class_id.startswith(allowed_code) or class_id == allowed_code:
                    return True
    return False


def _want_deal(auction_data: Dict[str, Any], deal_types: List[str], svc: ProZorroService) -> bool:
    is_rent = svc._is_rental_auction(auction_data) or deal_type_from_prozorro_data(auction_data) == DEAL_RENT
    if is_rent:
        return DEAL_RENT in deal_types
    return DEAL_SALE in deal_types


class ProzorroFilteredSearch:
    """Пошук + завантаження деталей у raw_prozorro_auctions за SourceQueryPlan."""

    def __init__(self, prozorro: Optional[ProZorroService] = None):
        self.svc = prozorro or ProZorroService()
        self.methods = _load_selling_methods()

    def _search_url(self) -> str:
        return f"{self.svc.settings.prozorro_sale_search_api_base_url.rstrip('/')}/search/procedures"

    def build_filters(self, plan: SourceQueryPlan) -> List[Dict[str, Any]]:
        filters: List[Dict[str, Any]] = []
        dts = plan.deal_types or [DEAL_SALE]
        if DEAL_RENT in dts and DEAL_SALE not in dts:
            rent_types = self.methods.get("rent") or []
            if rent_types:
                filters.append({"field": "procedure_type", "operator": "in", "value": rent_types})
        elif DEAL_SALE in dts and DEAL_RENT not in dts:
            sale_types = self.methods.get("sale") or []
            if sale_types:
                filters.append({"field": "procedure_type", "operator": "in", "value": sale_types})

        region_vals: List[str] = []
        for r in plan.regions:
            region_vals.extend(_region_search_values(r))
        if region_vals:
            op = "eq" if len(region_vals) == 1 else "in"
            filters.append({
                "field": "extended_filters.items_address.region",
                "operator": op,
                "value": region_vals[0] if op == "eq" else region_vals,
            })
        if plan.settlements:
            loc = plan.settlements
            op = "eq" if len(loc) == 1 else "in"
            filters.append({
                "field": "extended_filters.items_address.locality",
                "operator": op,
                "value": loc[0] if op == "eq" else loc,
            })
        if plan.price_uah_min is not None:
            filters.append({"field": "value.amount", "operator": "gte", "value": plan.price_uah_min})
        if plan.price_uah_max is not None:
            filters.append({"field": "value.amount", "operator": "lte", "value": plan.price_uah_max})
        if plan.cutoff_utc is not None:
            iso = plan.cutoff_utc.strftime("%Y-%m-%dT%H:%M:%S")
            filters.append({"field": "datePublished", "operator": "gte", "value": iso})
        if plan.query_text:
            filters.append({"field": "full_text_search", "operator": "match", "value": plan.query_text})
        return filters

    def search_hits(
        self,
        plan: SourceQueryPlan,
        log_fn: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> List[Dict[str, Any]]:
        """Повертає [{proc_id, auction_id}, ...] з Search API."""
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        filters = self.build_filters(plan)
        hits: List[Dict[str, Any]] = []
        seen = set()
        url = self._search_url()
        for page in range(1, SEARCH_MAX_PAGES + 1):
            if cancel_check and cancel_check():
                break
            body = {
                "page": page,
                "limit": SEARCH_LIMIT,
                "filters": filters,
                "sort": [{"field": "dateModified", "direction": "desc"}],
            }
            try:
                response = self.svc.session.post(
                    url,
                    json=body,
                    timeout=self.svc.settings.prozorro_api_timeout,
                )
                response.raise_for_status()
                data = response.json() if response.content else {}
            except Exception as e:
                logger.warning("ProZorro search page %s failed: %s", page, e)
                log(f"[ProZorro search] помилка сторінки {page}: {e}")
                break
            payload = data.get("payload") or data.get("data") or []
            if not isinstance(payload, list) or not payload:
                break
            for item in payload:
                if not isinstance(item, dict):
                    continue
                proc_id = str(item.get("_id") or item.get("id") or "").strip()
                auction_id = (
                    item.get("auctionId")
                    or item.get("auction_id")
                    or extract_auction_id(item)
                    or proc_id
                )
                auction_id = str(auction_id or "").strip()
                key = auction_id or proc_id
                if not key or key in seen:
                    continue
                seen.add(key)
                hits.append({"proc_id": proc_id, "auction_id": auction_id})
            log(f"[ProZorro search] сторінка {page}: +{len(payload)}, разом {len(hits)}")
            max_page = int(data.get("max_page") or SEARCH_MAX_PAGES)
            if page >= max_page or len(payload) < SEARCH_LIMIT:
                break
        return hits

    def fetch_hits_to_raw(
        self,
        hits: List[Dict[str, Any]],
        plan: SourceQueryPlan,
        source_load_run_id: Optional[str] = None,
        log_fn: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> List[str]:
        """GET details + upsert raw. Повертає auction_id, що пройшли класифікацію та deal_type."""
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        raw_repo = RawProzorroAuctionsRepository()
        raw_repo.ensure_index()
        loaded: List[str] = []
        dts = plan.deal_types or [DEAL_SALE]
        ctx = {
            "trigger": "market_research",
            "deal_types": dts,
            "depth_days": plan.depth_days,
        }
        for i, hit in enumerate(hits, start=1):
            if cancel_check and cancel_check():
                break
            proc_id = hit.get("proc_id") or None
            auction_id = hit.get("auction_id") or ""
            try:
                details = self.svc.get_auction_details(auction_id or proc_id, proc_id=proc_id)
            except Exception as e:
                logger.debug("ProZorro detail %s: %s", auction_id or proc_id, e)
                continue
            auction_data = details.get("data") if isinstance(details.get("data"), dict) else details
            if not isinstance(auction_data, dict):
                continue
            aid = extract_auction_id(auction_data) or auction_id
            if not aid:
                continue
            if not _is_property_auction(self.svc, auction_data):
                continue
            if not _want_deal(auction_data, dts, self.svc):
                continue
            try:
                approx = self.svc._get_region_from_auction_data(auction_data)
                raw_repo.upsert_raw(
                    auction_id=aid,
                    auction_data=auction_data,
                    fetch_context=ctx,
                    approximate_region=approx,
                    source_load_run_id=source_load_run_id,
                )
                loaded.append(aid)
            except Exception as e:
                logger.warning("ProZorro raw upsert %s: %s", aid, e)
            if i % 20 == 0 or i == len(hits):
                log(f"[ProZorro search] деталі {i}/{len(hits)}, збережено {len(loaded)}")
        return loaded
