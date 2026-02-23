# -*- coding: utf-8 -*-
"""
GeoFilterService (business): повний сервіс геофільтрації.
Створює геофільтри, обробляє фільтрацію колекцій за географічними ознаками,
фільтрацію за координатами з радіусом та побудову географічних метрик (відстань до точки).
"""

import logging
import math
from typing import Any, Dict, List, Optional, Union

from config.settings import Settings
from domain.models.filter_models import (
    GeoFilter,
    GeoFilterElement,
    GeoFilterGroup,
    GeoFilterOperator,
    FilterGroupType,
)
from domain.services.geo_filter_service import GeoFilterService as DomainGeoFilterService

logger = logging.getLogger(__name__)

# Радіус Землі в км (приблизно)
EARTH_RADIUS_KM = 6371.0


def haversine_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Обчислює відстань між двома точками на земній кулі (формула Haversine).
    Returns: відстань у кілометрах.
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return EARTH_RADIUS_KM * c


def point_in_radius_km(
    point_lat: float,
    point_lon: float,
    center_lat: float,
    center_lon: float,
    radius_km: float,
) -> bool:
    """
    Перевіряє, чи точка (point_lat, point_lon) знаходиться в радіусі radius_km
    від центру (center_lat, center_lon).
    """
    dist = haversine_distance_km(point_lat, point_lon, center_lat, center_lon)
    return dist <= radius_km


class GeoFilterService:
    """
    Сервіс геофільтрації на рівні бізнес-логіки.
    
    Функції:
    - Створення геофільтрів з різних входів (місто, область, «не в місті», радіус)
    - Розв'язання адрес/топонімів у координати через GeocodingService
    - Фільтрація колекцій за географічними ознаками
    - Фільтрація за координатами з радіусом
    - Побудова географічних метрик (відстань до центру міста)
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings
        self._domain_service = DomainGeoFilterService()
        self._geocoding_service = None

    @property
    def geocoding_service(self):
        """Лінива ініціалізація GeocodingService."""
        if self._geocoding_service is None and self.settings:
            try:
                from business.services.geocoding_service import GeocodingService
                self._geocoding_service = GeocodingService(self.settings)
            except Exception as e:
                logger.warning("GeoFilterService: GeocodingService недоступний: %s", e)
        return self._geocoding_service

    def create_geo_filter(
        self,
        city: Optional[str] = None,
        region: Optional[str] = None,
        exclude_city: Optional[str] = None,
        exclude_region: Optional[str] = None,
        center_address: Optional[str] = None,
        center_lat: Optional[float] = None,
        center_lon: Optional[float] = None,
        radius_km: Optional[float] = None,
        use_or: bool = True,
    ) -> Optional[GeoFilter]:
        """
        Створює GeoFilter з заданих умов.
        
        Args:
            city: місто (включити)
            region: область (включити)
            exclude_city: місто (виключити, «не в Києві»)
            exclude_region: область (виключити)
            center_address: адреса/топонім для центру (геокодується)
            center_lat, center_lon: координати центру (якщо вже відомі)
            radius_km: радіус у км від центру
            use_or: якщо city і region задані — об'єднати через OR
        
        Returns:
            GeoFilter або None
        """
        elements: List[Union[GeoFilterElement, GeoFilterGroup]] = []
        
        # Нормалізація топонімів до формату в БД
        city, region, exclude_city, exclude_region = self._normalize_toponyms(
            city, region, exclude_city, exclude_region
        )
        
        # Включення: місто та/або область
        if city or region:
            gf = self._domain_service.from_city_region(city=city, region=region, use_or=use_or)
            if gf:
                elements.append(gf.root)
        
        # Виключення: не в місті / не в області
        if exclude_city:
            elements.append(GeoFilterElement(
                operator=GeoFilterOperator.NE,
                geo_type="settlement",
                value=exclude_city,
            ))
        if exclude_region:
            elements.append(GeoFilterElement(
                operator=GeoFilterOperator.NE,
                geo_type="region",
                value=exclude_region,
            ))
        
        # Радіус від центру
        if radius_km is not None and radius_km > 0:
            lat, lon = center_lat, center_lon
            if (lat is None or lon is None) and center_address and self.geocoding_service:
                coords = self.resolve_address_to_coordinates(center_address)
                if coords:
                    lat, lon = coords["latitude"], coords["longitude"]
            if lat is not None and lon is not None:
                elements.append(GeoFilterElement(
                    operator=GeoFilterOperator.IN_RADIUS,
                    geo_type="coordinates",
                    value={"latitude": lat, "longitude": lon},
                    radius_km=radius_km,
                ))
        
        if not elements:
            return None
        if len(elements) == 1:
            return GeoFilter(root=elements[0])
        
        return GeoFilter(root=GeoFilterGroup(group_type=FilterGroupType.AND, items=elements))

    def from_dict(self, data: Dict[str, Any]) -> Optional[GeoFilter]:
        """
        Створює GeoFilter з словника (сумісний з filter_metrics).
        Підтримує: city, region, exclude_city, exclude_region, center_address, radius_km.
        """
        city = data.get("city") if isinstance(data.get("city"), str) else None
        region = data.get("region") if isinstance(data.get("region"), str) else None
        exclude_city = data.get("exclude_city") if isinstance(data.get("exclude_city"), str) else None
        exclude_region = data.get("exclude_region") if isinstance(data.get("exclude_region"), str) else None
        center_address = data.get("center_address") if isinstance(data.get("center_address"), str) else None
        radius_km = data.get("radius_km")
        if isinstance(radius_km, (int, float)):
            radius_km = float(radius_km)
        else:
            radius_km = None
        center_lat = data.get("center_lat")
        center_lon = data.get("center_lon")
        if center_lat is not None:
            center_lat = float(center_lat)
        if center_lon is not None:
            center_lon = float(center_lon)
        
        return self.create_geo_filter(
            city=city,
            region=region,
            exclude_city=exclude_city,
            exclude_region=exclude_region,
            center_address=center_address,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_km=radius_km,
            use_or=True,
        )

    def _normalize_toponyms(
        self,
        city: Optional[str],
        region: Optional[str],
        exclude_city: Optional[str],
        exclude_region: Optional[str],
    ) -> tuple:
        """Нормалізує топоніми до формату в БД."""
        from utils.toponym_normalizer import normalize_region, normalize_settlement

        n_city = normalize_settlement(city) if city else None
        n_region = normalize_region(region) if region else None
        n_exclude_city = normalize_settlement(exclude_city) if exclude_city else None
        n_exclude_region = normalize_region(exclude_region) if exclude_region else None
        return n_city, n_region, n_exclude_city, n_exclude_region

    def resolve_address_to_coordinates(self, address: str) -> Optional[Dict[str, float]]:
        """
        Геокодує адресу/топонім у координати.
        Returns: {"latitude": float, "longitude": float} або None
        """
        if not self.geocoding_service:
            return None
        try:
            result = self.geocoding_service.geocode(
                query=address, region="ua", caller="geo_filter_service"
            )
            results_list = result.get("results", [])
            if not results_list:
                return None
            first = results_list[0]
            if isinstance(first, dict):
                lat = first.get("latitude")
                lon = first.get("longitude")
            else:
                geometry = getattr(first, "geometry", None) or {}
                loc = geometry.get("location", {})
                lat = loc.get("lat")
                lon = loc.get("lng")
            if lat is not None and lon is not None:
                return {"latitude": float(lat), "longitude": float(lon)}
        except Exception as e:
            logger.warning("GeoFilterService: геокодування %r: %s", address[:50], e)
        return None

    def add_distance_metric(
        self,
        records: List[Dict[str, Any]],
        reference_point: str,
        output_field: str = "distance_km",
        coord_fields: tuple = ("addresses", "coordinates"),
    ) -> List[Dict[str, Any]]:
        """
        Додає до кожного запису метрику відстані до референсної точки.
        
        Args:
            records: список документів
            reference_point: адреса/топонім (напр. «центр Львова») — геокодується
            output_field: назва поля для відстані
            coord_fields: кортеж (шлях_до_масиву_адрес, ключ_координат)
        
        Returns:
            Той самий список з доданим полем output_field у кожному записі
        """
        coords_ref = self.resolve_address_to_coordinates(reference_point)
        if not coords_ref:
            logger.warning("GeoFilterService: не вдалося геокодувати %r", reference_point)
            return records
        
        ref_lat = coords_ref["latitude"]
        ref_lon = coords_ref["longitude"]
        addr_path, coord_key = coord_fields
        
        for rec in records:
            rec[output_field] = None
            addrs = rec.get(addr_path)
            if not isinstance(addrs, list):
                addrs = [addrs] if addrs else []
            for addr in addrs:
                if not isinstance(addr, dict):
                    continue
                coords = addr.get(coord_key)
                if isinstance(coords, dict):
                    lat = coords.get("latitude")
                    lon = coords.get("longitude")
                else:
                    lat = lon = None
                if lat is not None and lon is not None:
                    dist = haversine_distance_km(
                        float(lat), float(lon), ref_lat, ref_lon
                    )
                    if rec[output_field] is None or dist < rec[output_field]:
                        rec[output_field] = round(dist, 2)
        
        return records

    def filter_by_radius(
        self,
        records: List[Dict[str, Any]],
        center_address: Optional[str] = None,
        center_lat: Optional[float] = None,
        center_lon: Optional[float] = None,
        radius_km: float = 10.0,
        coord_fields: tuple = ("addresses", "coordinates"),
    ) -> List[Dict[str, Any]]:
        """
        Фільтрує записи, залишаючи тільки ті, що в радіусі від центру.
        """
        lat, lon = center_lat, center_lon
        if (lat is None or lon is None) and center_address and self.geocoding_service:
            coords = self.resolve_address_to_coordinates(center_address)
            if coords:
                lat, lon = coords["latitude"], coords["longitude"]
        if lat is None or lon is None:
            return []
        
        addr_path, coord_key = coord_fields
        result = []
        for rec in records:
            addrs = rec.get(addr_path) or []
            if not isinstance(addrs, list):
                addrs = [addrs] if addrs else []
            for addr in addrs:
                if not isinstance(addr, dict):
                    continue
                coords = addr.get(coord_key)
                if isinstance(coords, dict):
                    plat = coords.get("latitude")
                    plon = coords.get("longitude")
                else:
                    plat = plon = None
                if plat is not None and plon is not None:
                    if point_in_radius_km(float(plat), float(plon), lat, lon, radius_km):
                        result.append(rec)
                        break
        return result
