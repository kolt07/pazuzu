# -*- coding: utf-8 -*-
"""
GeoFilterService: формування GeoFilter об'єктів для пошуку за географією.
Генерує доменні об'єкти GeoFilter з простих входів (населений пункт, область, координати).
"""

from typing import Any, Dict, List, Optional, Union

from domain.models.filter_models import (
    GeoFilter,
    GeoFilterElement,
    GeoFilterGroup,
    GeoFilterOperator,
    FilterGroupType,
)


class GeoFilterService:
    """
    Сервіс формування геофільтрів.
    Перетворює логічні умови (місто, область, координати) на GeoFilter.
    """

    def from_city_region(
        self,
        city: Optional[str] = None,
        region: Optional[str] = None,
        use_or: bool = True
    ) -> Optional[GeoFilter]:
        """
        Створює GeoFilter з міста та/або області.
        Якщо обидва задані і use_or=True — логіка АБО (місто або область).
        
        Args:
            city: Назва населеного пункту (напр. "Київ")
            region: Назва області (напр. "Київська")
            use_or: Якщо city і region задані — об'єднати через OR (місто АБО область)
        
        Returns:
            GeoFilter або None якщо нічого не задано
        """
        elements: List[GeoFilterElement] = []
        if city:
            elements.append(GeoFilterElement(
                operator=GeoFilterOperator.EQ,
                geo_type="settlement",
                value=city
            ))
        if region:
            elements.append(GeoFilterElement(
                operator=GeoFilterOperator.EQ,
                geo_type="region",
                value=region
            ))
        
        if not elements:
            return None
        if len(elements) == 1:
            return GeoFilter(root=elements[0])
        
        group_type = FilterGroupType.OR if use_or else FilterGroupType.AND
        return GeoFilter(root=GeoFilterGroup(group_type=group_type, items=elements))

    def from_coordinates_with_radius(
        self,
        latitude: float,
        longitude: float,
        radius_km: float
    ) -> GeoFilter:
        """Створює GeoFilter «в радіусі» від координат."""
        return GeoFilter(root=GeoFilterElement(
            operator=GeoFilterOperator.IN_RADIUS,
            geo_type="coordinates",
            value={"latitude": latitude, "longitude": longitude},
            radius_km=radius_km
        ))

    def from_dict(self, data: Dict[str, Any]) -> Optional[GeoFilter]:
        """
        Створює GeoFilter з словника.
        Підтримує: city, region, exclude_city, exclude_region, center_lat, center_lon, radius_km.
        center_address — має бути розв'язаний до center_lat/center_lon на рівні бізнес-шару.
        Топоніми (city, region) нормалізуються до формату в БД перед використанням.
        """
        city = data.get("city") if isinstance(data.get("city"), str) else None
        region = data.get("region") if isinstance(data.get("region"), str) else None
        exclude_city = data.get("exclude_city") if isinstance(data.get("exclude_city"), str) else None
        exclude_region = data.get("exclude_region") if isinstance(data.get("exclude_region"), str) else None

        # Нормалізація топонімів до формату в БД (як при завантаженні з джерел)
        city, region, exclude_city, exclude_region = self._normalize_toponyms(
            city, region, exclude_city, exclude_region
        )
        center_lat = data.get("center_lat")
        center_lon = data.get("center_lon")
        radius_km = data.get("radius_km")
        if center_lat is not None:
            center_lat = float(center_lat)
        if center_lon is not None:
            center_lon = float(center_lon)
        if isinstance(radius_km, (int, float)):
            radius_km = float(radius_km)
        else:
            radius_km = None
        
        and_items: List[Union[GeoFilterElement, GeoFilterGroup]] = []

        if city or region:
            city_region_gf = self.from_city_region(city=city, region=region, use_or=True)
            if city_region_gf:
                and_items.append(city_region_gf.root)
        if exclude_city:
            and_items.append(GeoFilterElement(
                operator=GeoFilterOperator.NE,
                geo_type="settlement",
                value=exclude_city,
            ))
        if exclude_region:
            and_items.append(GeoFilterElement(
                operator=GeoFilterOperator.NE,
                geo_type="region",
                value=exclude_region,
            ))
        if radius_km and center_lat is not None and center_lon is not None:
            and_items.append(GeoFilterElement(
                operator=GeoFilterOperator.IN_RADIUS,
                geo_type="coordinates",
                value={"latitude": center_lat, "longitude": center_lon},
                radius_km=radius_km,
            ))

        if not and_items:
            return None
        if len(and_items) == 1:
            return GeoFilter(root=and_items[0])
        return GeoFilter(root=GeoFilterGroup(group_type=FilterGroupType.AND, items=and_items))

    def _normalize_toponyms(
        self,
        city: Optional[str],
        region: Optional[str],
        exclude_city: Optional[str],
        exclude_region: Optional[str],
    ) -> tuple:
        """
        Нормалізує топоніми до формату в БД (як при завантаженні з джерел).
        Волинській області → Волинська, у Києві → Київ.
        """
        from utils.toponym_normalizer import normalize_region, normalize_settlement

        n_city = normalize_settlement(city) if city else None
        n_region = normalize_region(region) if region else None
        n_exclude_city = normalize_settlement(exclude_city) if exclude_city else None
        n_exclude_region = normalize_region(exclude_region) if exclude_region else None
        return n_city, n_region, n_exclude_city, n_exclude_region
