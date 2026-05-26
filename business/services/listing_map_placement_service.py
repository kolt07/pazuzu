# -*- coding: utf-8 -*-
"""
Визначення координат маркерів оголошень для вкладки «Мапа» mini-app.

Пріоритет: точна адреса → кадастрові ділянки → центр НП / геокодування вулиці.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from business.services.geocoding_service import GeocodingService
from config.settings import Settings
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
from data.repositories.geography_repository import CitiesRepository
from utils.geo_centroid import centroid_from_bounds, parse_lat_lng

logger = logging.getLogger(__name__)

PlacementType = str  # address | cadastral | settlement | street


def _source_label(source: str) -> str:
    s = (source or "").strip().lower()
    return "OLX" if s == "olx" else "Przr"


def _marker(
    lat: float,
    lng: float,
    source: str,
    placement: PlacementType,
    cadastral_number: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "lat": lat,
        "lng": lng,
        "label": _source_label(source),
        "source": (source or "").strip().lower(),
        "placement": placement,
        "cadastral_number": cadastral_number,
    }


def _in_bbox(lat: float, lng: float, bbox: Dict[str, float]) -> bool:
    sw_lat = bbox.get("sw_lat")
    sw_lng = bbox.get("sw_lng")
    ne_lat = bbox.get("ne_lat")
    ne_lng = bbox.get("ne_lng")
    if None in (sw_lat, sw_lng, ne_lat, ne_lng):
        return True
    if lat < sw_lat or lat > ne_lat:
        return False
    if sw_lng <= ne_lng:
        return sw_lng <= lng <= ne_lng
    return lng >= sw_lng or lng <= ne_lng


class ListingMapPlacementService:
    """Розміщення маркерів для оголошень на карті."""

    def __init__(self, settings: Optional[Settings] = None):
        self._settings = settings or Settings()
        self._cadastral_repo = CadastralParcelsRepository()
        self._cities_repo = CitiesRepository()
        self._geocoding: Optional[GeocodingService] = None
        self._cadastral_cache: Dict[str, Optional[Tuple[float, float]]] = {}

    @property
    def geocoding(self) -> GeocodingService:
        if self._geocoding is None:
            self._geocoding = GeocodingService(self._settings)
        return self._geocoding

    def resolve_markers(
        self,
        doc: Dict[str, Any],
        *,
        allow_geocode: bool = True,
    ) -> List[Dict[str, Any]]:
        """Повертає список маркерів для одного документа unified_listings."""
        source = (doc.get("source") or "").strip().lower()
        if not source:
            return []

        markers = self._markers_from_complete_addresses(doc, source)
        if markers:
            return markers

        markers = self._markers_from_cadastral(doc, source)
        if markers:
            return markers

        if not allow_geocode:
            return self._markers_from_toponym_fast(doc, source)

        return self._markers_from_toponym(doc, source)

    def resolve_markers_in_bbox(
        self,
        doc: Dict[str, Any],
        bbox: Dict[str, float],
        *,
        allow_geocode: bool = True,
    ) -> List[Dict[str, Any]]:
        return [
            m for m in self.resolve_markers(doc, allow_geocode=allow_geocode)
            if _in_bbox(m["lat"], m["lng"], bbox)
        ]

    def _markers_from_complete_addresses(
        self,
        doc: Dict[str, Any],
        source: str,
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        addresses = doc.get("addresses") or []
        if not isinstance(addresses, list):
            return out
        for addr in addresses:
            if not isinstance(addr, dict) or not addr.get("is_complete"):
                continue
            pair = parse_lat_lng(addr.get("coordinates"))
            if pair:
                out.append(_marker(pair[0], pair[1], source, "address"))
        return out

    def _markers_from_cadastral(
        self,
        doc: Dict[str, Any],
        source: str,
    ) -> List[Dict[str, Any]]:
        numbers = doc.get("cadastral_numbers") or []
        if not isinstance(numbers, list) or not numbers:
            return []
        uniq = sorted({str(n).strip() for n in numbers if n and str(n).strip()})
        if not uniq:
            return []

        to_fetch = [n for n in uniq if n not in self._cadastral_cache]
        if to_fetch:
            parcels = self._cadastral_repo.find_many_by_cadastral_numbers(
                to_fetch,
                projection={"cadastral_number": 1, "bounds": 1},
            )
            found = {p.get("cadastral_number"): p for p in parcels if p.get("cadastral_number")}
            for cn in to_fetch:
                parcel = found.get(cn)
                if not parcel:
                    self._cadastral_cache[cn] = None
                    continue
                cent = centroid_from_bounds(parcel.get("bounds"))
                self._cadastral_cache[cn] = cent

        out: List[Dict[str, Any]] = []
        for cn in uniq:
            cent = self._cadastral_cache.get(cn)
            if cent:
                out.append(_marker(cent[0], cent[1], source, "cadastral", cadastral_number=cn))
        return out

    def _markers_from_toponym_fast(
        self,
        doc: Dict[str, Any],
        source: str,
    ) -> List[Dict[str, Any]]:
        """Центр НП з cities без виклику Geocoding API (швидкий режим viewport)."""
        region, settlement, _street, _building = self._extract_toponym_fields(doc)
        if settlement and region:
            coords = self._cities_repo.find_coordinates(settlement, region)
            if coords:
                return [_marker(coords["lat"], coords["lon"], source, "settlement")]
        return []

    def _markers_from_toponym(
        self,
        doc: Dict[str, Any],
        source: str,
    ) -> List[Dict[str, Any]]:
        region, settlement, street, building = self._extract_toponym_fields(doc)
        if not settlement and not region:
            return []

        if street and not building:
            query_parts = [p for p in (street, settlement, region) if p]
            query = ", ".join(query_parts)
            pair = self._geocode_query(query)
            if pair:
                return [_marker(pair[0], pair[1], source, "street")]

        fast = self._markers_from_toponym_fast(doc, source)
        if fast:
            return fast

        if settlement or region:
            query = ", ".join(p for p in (settlement, region) if p)
            pair = self._geocode_query(query)
            if pair:
                return [_marker(pair[0], pair[1], source, "settlement")]

        return []

    @staticmethod
    def _extract_toponym_fields(
        doc: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        region = doc.get("region")
        settlement = doc.get("city")
        street = None
        building = None

        addresses = doc.get("addresses") or []
        if isinstance(addresses, list):
            for addr in addresses:
                if not isinstance(addr, dict):
                    continue
                if not region and addr.get("region"):
                    region = addr.get("region")
                if not settlement and addr.get("settlement"):
                    settlement = addr.get("settlement")
                if not street and addr.get("street"):
                    street = addr.get("street")
                if not building and addr.get("building"):
                    building = addr.get("building")
                if region and settlement:
                    break

        def _s(v: Any) -> Optional[str]:
            if v is None or not isinstance(v, str):
                return None
            t = v.strip()
            return t if t else None

        return _s(region), _s(settlement), _s(street), _s(building)

    def _geocode_query(self, query: str) -> Optional[Tuple[float, float]]:
        if not query or not query.strip():
            return None
        try:
            result = self.geocoding.geocode(
                query=query.strip(),
                region="ua",
                caller="listing_map_placement",
            )
            results = result.get("results") or []
            if not results:
                return None
            r = results[0]
            lat, lng = r.get("latitude"), r.get("longitude")
            if lat is None or lng is None:
                return None
            return (float(lat), float(lng))
        except Exception as e:
            logger.warning("listing_map_placement geocode failed for %r: %s", query, e)
            return None
