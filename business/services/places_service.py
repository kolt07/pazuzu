# -*- coding: utf-8 -*-
"""
Сервіс Google Places API (New): пошук місць поблизу координат.
Використовується для гео-аналізу придатності приміщення (аптека, кафе тощо).
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from config.settings import Settings

logger = logging.getLogger(__name__)

PLACES_NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"
DEFAULT_TIMEOUT = 15
DEFAULT_LANGUAGE = "uk"

# Поля для FieldMask (мінімальний набір для аналізу)
DEFAULT_FIELD_MASK = "places.displayName,places.formattedAddress,places.types,places.location"

# Дозволені типи місць (Table A з Places API)
ALLOWED_PLACE_TYPES = frozenset({
    "pharmacy", "hospital", "doctor", "dentist", "veterinary_care",
    "bus_station", "transit_station", "subway_station", "train_station",
    "restaurant", "cafe", "bar", "meal_takeaway", "meal_delivery",
    "supermarket", "grocery_store", "convenience_store", "shopping_mall",
    "school", "university", "primary_school", "secondary_school",
    "park", "gym", "stadium", "movie_theater",
    "apartment_building", "residential_building", "local_government_office",
    "bank", "atm", "post_office", "police",
    "parking", "gas_station", "lawyer", "accounting", "real_estate_agency",
})


class PlacesService:
    """
    Пошук місць поблизу координат через Google Places API (New).
    """

    def __init__(self, settings: Optional[Settings] = None):
        self._settings = settings or Settings()

    def search_nearby(
        self,
        latitude: float,
        longitude: float,
        place_types: List[str],
        radius_meters: int = 500,
        max_results: int = 20,
        language: str = DEFAULT_LANGUAGE,
    ) -> Dict[str, Any]:
        """
        Пошук місць поблизу координат.

        Args:
            latitude: Широта центру пошуку
            longitude: Довгота центру пошуку
            place_types: Список типів (pharmacy, hospital, bus_station тощо)
            radius_meters: Радіус пошуку в метрах (макс. 50000)
            max_results: Максимум результатів (1–20)
            language: Код мови для назв (uk, en)

        Returns:
            {
                "success": bool,
                "places": [{"name": str, "address": str, "types": list, "location": {lat, lng}}],
                "count": int,
                "error": str | None
            }
        """
        api_key = (self._settings.google_maps_api_key or "").strip()
        if not api_key:
            return {
                "success": False,
                "places": [],
                "count": 0,
                "error": "GOOGLE_MAPS_API_KEY не налаштовано",
            }

        # Фільтруємо типи
        types_ok = [t for t in place_types if t in ALLOWED_PLACE_TYPES]
        if not types_ok:
            return {
                "success": False,
                "places": [],
                "count": 0,
                "error": f"Немає дозволених типів. Дозволені: {sorted(ALLOWED_PLACE_TYPES)[:20]}...",
            }

        radius = max(50, min(radius_meters, 50000))
        max_res = max(1, min(max_results, 20))

        body = {
            "includedTypes": types_ok,
            "maxResultCount": max_res,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": latitude, "longitude": longitude},
                    "radius": float(radius),
                }
            },
        }

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": DEFAULT_FIELD_MASK,
        }

        try:
            resp = requests.post(
                PLACES_NEARBY_URL,
                json=body,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Places API request failed: %s (lat=%.4f, lng=%.4f, types=%s)",
                e, latitude, longitude, types_ok[:3],
                exc_info=False,
            )
            return {
                "success": False,
                "places": [],
                "count": 0,
                "error": str(e),
            }

        places_raw = data.get("places") or []
        places = []
        for p in places_raw:
            display = p.get("displayName") or {}
            name = display.get("text", "") if isinstance(display, dict) else str(display)
            addr = p.get("formattedAddress") or ""
            loc = p.get("location") or {}
            places.append({
                "name": name,
                "address": addr,
                "types": p.get("types") or [],
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
            })

        return {
            "success": True,
            "places": places,
            "count": len(places),
            "error": None,
        }
