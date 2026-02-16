# -*- coding: utf-8 -*-
"""
Сервіс геокодування: перетворення текстового опису адреси/топоніма на координати.
Використовує Google Maps Geocoding API з кешуванням результатів.
Адреси зберігаються в деталізованому вигляді (область, місто, вулиця тощо) через address_structured.
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
import requests

logger = logging.getLogger(__name__)

from config.settings import Settings
from data.repositories.geocode_cache_repository import GeocodeCacheRepository
from utils.hash_utils import calculate_geocode_query_hash

GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
DEFAULT_REGION = "ua"
DEFAULT_LANGUAGE = "uk"  # Відповідь українською (кирилиця), не латиницею
DEFAULT_TIMEOUT = 15

# Типи address_components Google API -> ключі в address_structured
ADDRESS_COMPONENT_MAP = [
    ("country", ["country"]),
    ("region", ["administrative_area_level_1"]),
    ("administrative_area_level_2", ["administrative_area_level_2"]),
    ("city", ["locality", "administrative_area_level_2"]),
    ("sublocality", ["sublocality", "sublocality_level_1"]),
    ("street", ["route"]),
    ("street_number", ["street_number"]),
    ("postal_code", ["postal_code"]),
]


def _parse_address_components(components: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Парсить address_components відповіді Google Geocoding API у структуру:
    region (область), city (місто), street (вулиця), street_number тощо.
    """
    result: Dict[str, str] = {}
    if not components or not isinstance(components, list):
        return result
    for comp in components:
        if not isinstance(comp, dict):
            continue
        types = comp.get("types") or []
        long_name = (comp.get("long_name") or "").strip()
        if not long_name:
            continue
        for key, type_list in ADDRESS_COMPONENT_MAP:
            if key in result:
                continue
            for t in type_list:
                if t in types:
                    result[key] = long_name
                    break
    return result


def _normalize_place(result: Dict[str, Any]) -> Dict[str, Any]:
    """Нормалізує один результат Geocoding API до нашого формату; додає address_structured."""
    geometry = result.get("geometry") or {}
    location = geometry.get("location") or {}
    components = result.get("address_components") or []
    address_structured = _parse_address_components(components)
    return {
        "latitude": location.get("lat"),
        "longitude": location.get("lng"),
        "formatted_address": result.get("formatted_address") or "",
        "place_id": result.get("place_id") or "",
        "types": result.get("types") or [],
        "location_type": geometry.get("location_type"),
        "address_structured": address_structured,
    }


def _call_google_geocode(
    address: str, api_key: str, region: str = DEFAULT_REGION
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Виконує запит до Google Geocoding API.
    Повертає (нормалізовані результати з address_structured, чи варто кешувати).
    """
    if not api_key or not address or not address.strip():
        return [], False
    params = {
        "address": address.strip(),
        "key": api_key,
        "region": region,
        "language": DEFAULT_LANGUAGE,
    }
    try:
        resp = requests.get(GOOGLE_GEOCODE_URL, params=params, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(
            "Google Geocoding API request failed: %s (address=%r)",
            e, address[:80] if address else "",
            exc_info=False,
        )
        return [], False
    status = data.get("status")
    if status != "OK" and status != "ZERO_RESULTS":
        error_message = data.get("error_message", "")
        logger.warning(
            "Google Geocoding API returned status=%r error_message=%r (address=%r)",
            status, error_message, address[:80] if address else "",
        )
        return [], False
    results = data.get("results") or []
    normalized = [_normalize_place(r) for r in results]
    return normalized, True


class GeocodingService:
    """
    Геокодування з підтримкою кешу: спочатку перевірка кешу, при відсутності — запит до Google API.
    Результати містять address_structured (область, місто, вулиця тощо) для деталізованого збереження.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self._settings = settings or Settings()
        self._cache = GeocodeCacheRepository()

    def geocode(
        self,
        query: str,
        region: str = DEFAULT_REGION,
    ) -> Dict[str, Any]:
        """
        Повертає координати та метадані для текстового запиту (адреса, назва ЖК, тощо).
        Кожен результат у results містить address_structured: { region, city, street, street_number, ... }.
        """
        query_text = (query or "").strip()
        if not query_text:
            return {
                "query_hash": "",
                "query_text": query_text,
                "results": [],
                "from_cache": False,
            }
        query_hash = calculate_geocode_query_hash(query_text)
        cached = self._cache.find_by_query_hash(query_hash)
        if cached is not None:
            return {
                "query_hash": query_hash,
                "query_text": cached.get("query_text") or query_text,
                "results": cached.get("result") or [],
                "from_cache": True,
            }
        api_key = (self._settings.google_maps_api_key or "").strip()
        if not api_key:
            return {
                "query_hash": query_hash,
                "query_text": query_text,
                "results": [],
                "from_cache": False,
            }
        results, should_cache = _call_google_geocode(query_text, api_key, region=region)
        if should_cache:
            self._cache.save_result(query_hash, query_text, results)
        return {
            "query_hash": query_hash,
            "query_text": query_text,
            "results": results,
            "from_cache": False,
        }
