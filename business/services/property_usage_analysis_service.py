# -*- coding: utf-8 -*-
"""
Попередній аналіз використання об'єкта оголошення.
Етапи: існуюче використання, геоаналіз, можливі використання зі скорингом.
Результати кешуються в property_usage_analysis.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from data.repositories.property_usage_analysis_repository import PropertyUsageAnalysisRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.geocoding_service import GeocodingService
from business.services.places_service import PlacesService

logger = logging.getLogger(__name__)

# Ключові слова для виявлення існуючого використання
EXISTING_USAGE_PATTERNS = {
    "магазин": ["магазин", "крамниця", "торговий зал", "торгове приміщення"],
    "склад": ["склад", "складське", "логистика", "зберігання"],
    "виробництво": ["виробництво", "цех", "виробниче", "завод", "фабрика"],
    "офіс": ["офіс", "офісне приміщення", "бізнес-центр"],
    "аптека": ["аптека", "аптечний пункт"],
    "кафе": ["кафе", "ресторан", "бар", "їдальня", "кав'ярня"],
    "клініка": ["клініка", "медцентр", "медичний центр", "поліклініка"],
    "салон": ["салон краси", "перукарня", "косметологія"],
    "спортзал": ["спортзал", "фітнес", "тренажерний зал"],
    "автосервіс": ["автосервіс", "автомийка", "шиномонтаж", "СТО"],
}

# Типи POI для геоаналізу (універсальний набір)
GEO_PLACE_TYPES = [
    "pharmacy", "supermarket", "convenience_store", "shopping_mall",
    "bus_station", "transit_station", "apartment_building",
    "restaurant", "cafe", "hospital", "school", "bank",
]


def _extract_existing_usage(text: str) -> List[str]:
    """Витягує згадки про існуюче використання з тексту."""
    if not text or not isinstance(text, str):
        return []
    text_lower = text.lower()
    found = []
    for usage, keywords in EXISTING_USAGE_PATTERNS.items():
        for kw in keywords:
            if kw in text_lower:
                if usage not in found:
                    found.append(usage)
                break
    return found


def _build_address_from_unified(doc: Dict[str, Any]) -> Optional[str]:
    """Будує рядок адреси з unified_listings для геокодування."""
    addresses = doc.get("addresses", [])
    if not addresses or not isinstance(addresses, list):
        return None
    parts = []
    for addr in addresses[:1]:
        if isinstance(addr, dict):
            region = addr.get("region")
            settlement = addr.get("settlement")
            street = addr.get("street")
            building = addr.get("building")
            if region:
                parts.append(str(region))
            if settlement:
                parts.append(str(settlement))
            if street:
                parts.append(str(street))
            if building:
                parts.append(str(building))
    return ", ".join(parts) if parts else None


def _build_address_from_olx(doc: Dict[str, Any]) -> Optional[str]:
    """Будує адресу з olx_listings."""
    search_data = doc.get("search_data", {})
    detail = doc.get("detail", {})
    location = search_data.get("location")
    if location and isinstance(location, str):
        return location.strip()
    resolved = detail.get("resolved_locations", [])
    if resolved and isinstance(resolved, list):
        for loc in resolved:
            q = loc.get("query_text")
            if q:
                return str(q).strip()
    return None


def _build_address_from_prozorro(doc: Dict[str, Any]) -> Optional[str]:
    """Будує адресу з prozorro_auctions."""
    auction_data = doc.get("auction_data", {})
    address_refs = auction_data.get("address_refs", [])
    if address_refs and isinstance(address_refs, list):
        for refs in address_refs[:1]:
            if not isinstance(refs, dict):
                continue
            parts = []
            region = refs.get("region")
            city = refs.get("city")
            street = refs.get("street")
            building = refs.get("building")
            if isinstance(region, dict):
                parts.append(region.get("name", ""))
            elif isinstance(region, str):
                parts.append(region)
            if isinstance(city, dict):
                parts.append(city.get("name", ""))
            elif isinstance(city, str):
                parts.append(city)
            if isinstance(street, dict):
                parts.append(street.get("name", ""))
            elif isinstance(street, str):
                parts.append(street)
            if isinstance(building, dict):
                parts.append(str(building.get("number", "")))
            elif isinstance(building, str):
                parts.append(building)
            addr = ", ".join(p for p in parts if p)
            if addr:
                return addr
    items = auction_data.get("items", [])
    if items and isinstance(items, list):
        item = items[0]
        if not isinstance(item, dict):
            return None
        addr = item.get("address")
        if not isinstance(addr, dict):
            return None
        loc = addr.get("locality")
        reg = addr.get("region")
        loc_str = ""
        reg_str = ""
        if isinstance(loc, dict):
            loc_str = loc.get("uk_UA") or loc.get("en_US") or ""
        elif isinstance(loc, str):
            loc_str = loc
        if isinstance(reg, dict):
            reg_str = reg.get("uk_UA") or reg.get("en_US") or ""
        elif isinstance(reg, str):
            reg_str = reg
        if loc_str or reg_str:
            return ", ".join(p for p in [loc_str, reg_str] if p)
    return None


def _get_coordinates_from_addresses(addresses: List[Dict[str, Any]]) -> Optional[Tuple[float, float]]:
    """Витягує координати з масиву addresses (unified_listings)."""
    if not addresses or not isinstance(addresses, list):
        return None
    for addr in addresses:
        if isinstance(addr, dict):
            coords = addr.get("coordinates")
            if isinstance(coords, dict):
                lat = coords.get("latitude")
                lng = coords.get("longitude")
                if lat is not None and lng is not None:
                    try:
                        return (float(lat), float(lng))
                    except (TypeError, ValueError):
                        pass
    return None


def _get_coordinates_from_resolved(olx_doc: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """Витягує координати з resolved_locations (olx)."""
    detail = olx_doc.get("detail", {})
    resolved = detail.get("resolved_locations", [])
    if not resolved:
        return None
    for loc in resolved:
        results = loc.get("results", [])
        if results and isinstance(results, list):
            r = results[0]
            lat = r.get("latitude")
            lng = r.get("longitude")
            if lat is not None and lng is not None:
                try:
                    return (float(lat), float(lng))
                except (TypeError, ValueError):
                    pass
    return None


class PropertyUsageAnalysisService:
    """Сервіс попереднього аналізу використання об'єкта."""

    def __init__(self):
        self.repo = PropertyUsageAnalysisRepository()
        self.unified_repo = UnifiedListingsRepository()
        self.olx_repo = OlxListingsRepository()
        self.prozorro_repo = ProZorroAuctionsRepository()
        self.geocoding = GeocodingService()
        self.places = PlacesService()
        self.repo.ensure_index()

    def get_listing_doc(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Отримує повний документ оголошення за source та source_id."""
        if source.lower() == "olx":
            if source_id.startswith("http"):
                return self.olx_repo.find_by_url(source_id)
            return self.olx_repo.find_by_id(source_id)
        if source.lower() == "prozorro":
            return self.prozorro_repo.find_by_auction_id(source_id)
        return self.unified_repo.find_by_source_id(source, source_id)

    def get_or_create_analysis(
        self,
        source: str,
        source_id: str,
        listing_doc: Optional[Dict[str, Any]] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        Отримує аналіз з кешу або виконує новий.
        listing_doc — опційно, якщо не передано — завантажується за source/source_id.
        """
        if not force_refresh:
            cached = self.repo.find_by_source_id(source, source_id)
            if cached:
                return cached

        doc = listing_doc or self.get_listing_doc(source, source_id)
        if not doc:
            return {
                "source": source,
                "source_id": source_id,
                "existing_usage": [],
                "geo_analysis": {},
                "usage_suggestions": [],
                "error": "Оголошення не знайдено",
            }

        unified_doc = None
        if source.lower() in ("olx", "prozorro"):
            unified_doc = self.unified_repo.find_by_source_id(source, source_id)

        existing_usage = self._extract_existing_usage_from_doc(doc, unified_doc)
        address_str = self._get_address_string(doc, unified_doc, source)
        coordinates = self._get_coordinates(doc, unified_doc, address_str, source)
        geo_analysis = self._run_geo_analysis(coordinates) if coordinates else {}
        usage_suggestions = self._score_usages(
            doc, unified_doc, existing_usage, geo_analysis, source
        )

        self.repo.upsert(
            source=source,
            source_id=source_id,
            existing_usage=existing_usage,
            geo_analysis=geo_analysis,
            usage_suggestions=usage_suggestions,
            address_for_geocode=address_str,
        )

        return {
            "source": source,
            "source_id": source_id,
            "existing_usage": existing_usage,
            "geo_analysis": geo_analysis,
            "usage_suggestions": usage_suggestions,
        }

    def _extract_existing_usage_from_doc(
        self,
        doc: Dict[str, Any],
        unified_doc: Optional[Dict[str, Any]],
    ) -> List[str]:
        """Витягує існуюче використання з документу."""
        texts = []
        if unified_doc:
            title = unified_doc.get("title", "")
            description = unified_doc.get("description", "")
            tags = unified_doc.get("tags", [])
            if title:
                texts.append(str(title))
            if description:
                texts.append(str(description))
            if tags:
                texts.extend(str(t) for t in tags if t)
        if doc:
            if not unified_doc:
                if "title" in doc:
                    texts.append(str(doc.get("title", "")))
                if "description" in doc:
                    texts.append(str(doc.get("description", "")))
            search_data = doc.get("search_data")
            detail = doc.get("detail")
            auction_data = doc.get("auction_data")
            if isinstance(search_data, dict):
                texts.append(str(search_data.get("title", "")))
            if isinstance(detail, dict):
                texts.append(str(detail.get("description", "")))
                llm = detail.get("llm")
                if isinstance(llm, dict):
                    tags = llm.get("tags", [])
                    texts.extend(str(t) for t in tags if t)
            if isinstance(auction_data, dict):
                title_obj = auction_data.get("title", {})
                if isinstance(title_obj, dict):
                    texts.append(title_obj.get("uk_UA", "") or title_obj.get("en_US", ""))
                desc = auction_data.get("description", {})
                if isinstance(desc, dict):
                    texts.append(desc.get("uk_UA", "") or desc.get("en_US", ""))
        combined = " ".join(texts)
        return _extract_existing_usage(combined)

    def _get_address_string(
        self,
        doc: Dict[str, Any],
        unified_doc: Optional[Dict[str, Any]],
        source: str,
    ) -> Optional[str]:
        """Будує рядок адреси для геокодування."""
        if unified_doc:
            addr = _build_address_from_unified(unified_doc)
            if addr:
                return addr
        if source.lower() == "olx":
            return _build_address_from_olx(doc)
        if source.lower() == "prozorro":
            return _build_address_from_prozorro(doc)
        return _build_address_from_unified(doc) if doc else None

    def _get_coordinates(
        self,
        doc: Dict[str, Any],
        unified_doc: Optional[Dict[str, Any]],
        address_str: Optional[str],
        source: str,
    ) -> Optional[Tuple[float, float]]:
        """Отримує координати з документу або геокодування."""
        if unified_doc:
            coords = _get_coordinates_from_addresses(unified_doc.get("addresses", []))
            if coords:
                return coords
        if source.lower() == "olx":
            coords = _get_coordinates_from_resolved(doc)
            if coords:
                return coords
        if address_str:
            try:
                result = self.geocoding.geocode(
                    query=address_str, region="ua", caller="property_usage_analysis"
                )
                results = result.get("results", [])
                if results:
                    r = results[0]
                    lat, lng = r.get("latitude"), r.get("longitude")
                    if lat is not None and lng is not None:
                        return (float(lat), float(lng))
            except Exception as e:
                logger.warning("Геокодування не вдалося: %s", e)
        return None

    def _run_geo_analysis(self, coordinates: Tuple[float, float]) -> Dict[str, Any]:
        """Виконує пошук місць поблизу."""
        lat, lng = coordinates
        try:
            result = self.places.search_nearby(
                latitude=lat,
                longitude=lng,
                place_types=GEO_PLACE_TYPES,
                radius_meters=500,
                max_results=20,
            )
            if result.get("success"):
                places = result.get("places", [])
                by_type: Dict[str, List[Dict]] = {}
                for p in places:
                    for t in p.get("types", []):
                        if t not in by_type:
                            by_type[t] = []
                        by_type[t].append({"name": p.get("name"), "address": p.get("address")})
                return {
                    "coordinates": {"latitude": lat, "longitude": lng},
                    "nearby_by_type": {k: v[:5] for k, v in by_type.items()},
                    "total_places": len(places),
                }
        except Exception as e:
            logger.warning("Geo analysis failed: %s", e)
        return {"coordinates": {"latitude": lat, "longitude": lng}, "nearby_by_type": {}, "total_places": 0}

    def _score_usages(
        self,
        doc: Dict[str, Any],
        unified_doc: Optional[Dict[str, Any]],
        existing_usage: List[str],
        geo_analysis: Dict[str, Any],
        source: str,
    ) -> List[Dict[str, Any]]:
        """Визначає можливі використання та скоринг."""
        d = unified_doc or doc
        area = d.get("building_area_sqm") or d.get("land_area_ha")
        if area is None and doc:
            detail = doc.get("detail")
            if isinstance(detail, dict):
                llm = detail.get("llm")
                if isinstance(llm, dict):
                    area = llm.get("building_area_sqm") or llm.get("land_area_ha")
        if area is None and doc.get("auction_data"):
            items = doc["auction_data"].get("items", [])
            if items and isinstance(items, list):
                first_item = items[0]
                if isinstance(first_item, dict):
                    q = first_item.get("quantity")
                    if isinstance(q, dict):
                        area = q.get("value")
                    elif isinstance(q, (int, float)):
                        area = q

        nearby = geo_analysis.get("nearby_by_type", {})
        suggestions = []

        for usage in existing_usage:
            score = 85
            reasoning = [f"Об'єкт вже згадується як {usage}"]
            if area:
                reasoning.append(f"площа {area}")
            suggestions.append({
                "usage": usage,
                "score": min(100, score),
                "reasoning": reasoning,
            })

        usage_candidates = [
            ("аптека", ["pharmacy", "hospital"], 30, 80),
            ("кафе", ["cafe", "restaurant", "apartment_building"], 50, 75),
            ("магазин", ["supermarket", "shopping_mall", "apartment_building"], 30, 75),
            ("офіс", ["apartment_building", "bank", "local_government_office"], 50, 70),
            ("клініка", ["hospital", "pharmacy", "bus_station"], 80, 75),
            ("склад", ["apartment_building", "bus_station"], 100, 65),
            ("виробництво", ["apartment_building"], 150, 60),
        ]

        for usage, poi_types, min_area, base_score in usage_candidates:
            if usage in existing_usage:
                continue
            score = base_score
            reasoning = []
            if area is not None:
                try:
                    a = float(area)
                    if a >= min_area:
                        score += 5
                        reasoning.append(f"площа {a} підходить")
                    else:
                        score -= 10
                        reasoning.append(f"площа {a} менша за рекомендовану {min_area}")
                except (TypeError, ValueError):
                    pass
            poi_count = sum(len(nearby.get(t, [])) for t in poi_types)
            if poi_count > 0:
                score += min(15, poi_count * 2)
                reasoning.append(f"поруч: {poi_count} релевантних об'єктів")
            if score >= 50:
                suggestions.append({"usage": usage, "score": min(100, score), "reasoning": reasoning})

        suggestions.sort(key=lambda x: x["score"], reverse=True)
        return suggestions[:10]

    def format_analysis_for_llm(self, analysis: Dict[str, Any]) -> str:
        """Формує текст аналізу для вставки в контекст LLM."""
        parts = ["## Попередній аналіз використання об'єкта"]
        existing = analysis.get("existing_usage", [])
        if existing:
            parts.append(f"Існуюче використання (з опису): {', '.join(existing)}")
            parts.append("Базуючи пропозиції на тому, що вже є.")
        geo = analysis.get("geo_analysis", {})
        if geo.get("nearby_by_type"):
            parts.append("Геоаналіз (об'єкти поруч):")
            for k, v in geo["nearby_by_type"].items():
                parts.append(f"  - {k}: {len(v)} об'єктів")
        suggestions = analysis.get("usage_suggestions", [])
        if suggestions:
            parts.append("Можливі використання (зі скорингом):")
            for s in suggestions[:5]:
                parts.append(f"  - {s.get('usage', '')}: {s.get('score', 0)}/100")
        return "\n".join(parts)
