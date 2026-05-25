# -*- coding: utf-8 -*-
"""
Сервіс для роботи з географічними даними (області, райони області, округи, міста, вулиці, будинки).
Забезпечує нормалізацію та збереження топонімів з посиланнями.
"""

from typing import Optional, Dict, Any, List
from data.repositories.geography_repository import (
    RegionsRepository,
    CitiesRepository,
    StreetsRepository,
    BuildingsRepository,
    OblastRayonsRepository,
    GeoCirclesRepository,
)


class GeographyService:
    """Сервіс для роботи з географічними даними."""

    def __init__(self):
        self.regions_repo = RegionsRepository()
        self.oblast_rayons_repo = OblastRayonsRepository()
        self.geo_circles_repo = GeoCirclesRepository()
        self.cities_repo = CitiesRepository()
        self.streets_repo = StreetsRepository()
        self.buildings_repo = BuildingsRepository()

    def resolve_address(self, address_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Розв'язує адресу, створюючи або знаходячи посилання на топоніми.

        Args:
            address_data: Словник з полями:
                - region: назва області
                - oblast_raion / oblast_rayon / raion: район області (не плутати з районом міста)
                - settlement/city: НП
                - geo_circle: назва умовної групи (рядок) або
                  {name, kind?, parent_city_id?, parent_rayon_id?} — сільрада, округ Києва тощо
                - street_type, street, building, building_part
        """
        result = {
            "region_id": None,
            "oblast_rayon_id": None,
            "geo_circle_id": None,
            "city_id": None,
            "street_id": None,
            "building_id": None,
            "address_refs": {},
        }

        region_name = address_data.get("region") or address_data.get("address_region")
        if region_name:
            region = self.regions_repo.find_or_create(region_name)
            result["region_id"] = str(region["_id"])
            result["address_refs"]["region"] = {
                "_id": str(region["_id"]),
                "name": region["name"],
            }

        raion_name = (
            address_data.get("oblast_raion")
            or address_data.get("oblast_rayon")
            or address_data.get("raion")
            or address_data.get("rayon_oblast")
        )
        if raion_name and result["region_id"]:
            try:
                rz = self.oblast_rayons_repo.find_or_create(raion_name, result["region_id"])
                result["oblast_rayon_id"] = str(rz["_id"])
                result["address_refs"]["oblast_rayon"] = {
                    "_id": str(rz["_id"]),
                    "name": rz["name"],
                }
            except ValueError:
                pass

        city_name = (
            address_data.get("settlement")
            or address_data.get("city")
            or address_data.get("address_city")
        )
        if city_name and result["region_id"]:
            from utils.settlement_normalizer import is_district_only_name

            if is_district_only_name(city_name):
                try:
                    rz = self.oblast_rayons_repo.find_or_create(city_name, result["region_id"])
                    result["oblast_rayon_id"] = str(rz["_id"])
                    result["address_refs"]["oblast_rayon"] = {
                        "_id": str(rz["_id"]),
                        "name": rz["name"],
                    }
                except ValueError:
                    pass
            else:
                try:
                    city = self.cities_repo.find_or_create(city_name, result["region_id"])
                except ValueError:
                    city = None
                if city:
                    result["city_id"] = str(city["_id"])
                    result["address_refs"]["city"] = {
                        "_id": str(city["_id"]),
                        "name": city["name"],
                    }

        gc_raw = address_data.get("geo_circle")
        if gc_raw:
            try:
                circle_doc = self._resolve_geo_circle(address_data, result)
                if circle_doc:
                    result["geo_circle_id"] = str(circle_doc["_id"])
                    ref: Dict[str, Any] = {
                        "_id": str(circle_doc["_id"]),
                        "name": circle_doc["name"],
                        "kind": circle_doc.get("kind", "other"),
                    }
                    result["address_refs"]["geo_circle"] = ref
            except ValueError:
                pass

        street_name = address_data.get("street")
        street_type = address_data.get("street_type")
        if street_name and result["city_id"]:
            street = self.streets_repo.find_or_create(
                street_name,
                result["city_id"],
                street_type,
            )
            result["street_id"] = str(street["_id"])
            result["address_refs"]["street"] = {
                "_id": str(street["_id"]),
                "name": street["name"],
                "street_type": street.get("street_type"),
            }

        building_number = address_data.get("building") or address_data.get("building_number")
        building_part = address_data.get("building_part")
        if building_number and result["street_id"]:
            building = self.buildings_repo.find_or_create(
                building_number,
                result["street_id"],
                building_part,
            )
            result["building_id"] = str(building["_id"])
            result["address_refs"]["building"] = {
                "_id": str(building["_id"]),
                "number": building["number"],
                "building_part": building.get("building_part"),
            }

        return result

    def _resolve_geo_circle(
        self,
        address_data: Dict[str, Any],
        result: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        gc_raw = address_data.get("geo_circle")
        region_id = result.get("region_id")
        parent_city_id = result.get("city_id")
        parent_rayon_id = result.get("oblast_rayon_id")

        if isinstance(gc_raw, str):
            return self.geo_circles_repo.find_or_create(
                gc_raw,
                kind="other",
                region_id=region_id,
                parent_city_id=parent_city_id,
                parent_rayon_id=parent_rayon_id,
            )

        if isinstance(gc_raw, dict):
            name = gc_raw.get("name")
            if not name:
                return None
            kind = gc_raw.get("kind") or "other"
            pc = gc_raw.get("parent_city_id") or parent_city_id
            pr = gc_raw.get("parent_rayon_id") or parent_rayon_id
            reg = gc_raw.get("region_id") or region_id
            return self.geo_circles_repo.find_or_create(
                name,
                kind=kind,
                region_id=reg,
                parent_city_id=pc,
                parent_rayon_id=pr,
            )
        return None

    def get_region_by_id(self, region_id: str) -> Optional[Dict[str, Any]]:
        """Отримує область за ID."""
        return self.regions_repo.find_by_id(region_id)

    def get_oblast_rayon_by_id(self, rayon_id: str) -> Optional[Dict[str, Any]]:
        return self.oblast_rayons_repo.find_by_id(rayon_id)

    def get_geo_circle_by_id(self, circle_id: str) -> Optional[Dict[str, Any]]:
        return self.geo_circles_repo.find_by_id(circle_id)

    def get_city_by_id(self, city_id: str) -> Optional[Dict[str, Any]]:
        """Отримує місто за ID."""
        return self.cities_repo.find_by_id(city_id)

    def get_street_by_id(self, street_id: str) -> Optional[Dict[str, Any]]:
        """Отримує вулицю за ID."""
        return self.streets_repo.find_by_id(street_id)

    def get_building_by_id(self, building_id: str) -> Optional[Dict[str, Any]]:
        """Отримує будинок за ID."""
        return self.buildings_repo.find_by_id(building_id)

    def get_all_regions(self) -> List[Dict[str, Any]]:
        """Отримує всі області."""
        return self.regions_repo.get_all()

    def get_cities_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        """Отримує всі міста в області."""
        return self.cities_repo.get_by_region(region_id)

    def get_oblast_rayons_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        return self.oblast_rayons_repo.get_by_region(region_id)

    def get_streets_by_city(self, city_id: str) -> List[Dict[str, Any]]:
        """Отримує всі вулиці в місті."""
        return self.streets_repo.get_by_city(city_id)

    def format_address(self, address_refs: Dict[str, Any]) -> str:
        """
        Форматує адресу з посилань у читабельний рядок.
        """
        parts = []

        if address_refs.get("region"):
            parts.append(address_refs["region"]["name"])

        if address_refs.get("oblast_rayon"):
            parts.append(address_refs["oblast_rayon"]["name"])

        if address_refs.get("geo_circle"):
            gc = address_refs["geo_circle"]
            label = gc["name"]
            if gc.get("kind") and gc["kind"] != "other":
                label = f"{label} ({gc['kind']})"
            parts.append(label)

        if address_refs.get("city"):
            city_name = address_refs["city"]["name"]
            parts.append(city_name)

        if address_refs.get("street"):
            street = address_refs["street"]
            street_name = street["name"]
            if street.get("street_type"):
                street_name = street["street_type"] + " " + street_name
            parts.append(street_name)

        if address_refs.get("building"):
            building = address_refs["building"]
            building_str = building["number"]
            if building.get("building_part"):
                building_str += "/" + building["building_part"]
            parts.append(building_str)

        return ", ".join(parts)
