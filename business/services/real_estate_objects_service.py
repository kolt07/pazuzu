# -*- coding: utf-8 -*-
"""
Сервіс обробки об'єктів нерухомого майна (ОНМ).

Витягує ОНМ з оголошень, створює/оновлює записи, зберігає посилання в unified_listings.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from data.repositories.real_estate_objects_repository import RealEstateObjectsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
from business.services.real_estate_objects_llm_extractor_service import RealEstateObjectsLLMExtractorService
from business.services.geocoding_service import GeocodingService
from config.settings import Settings
from utils.address_parser import parse_prozorro_item_address

logger = logging.getLogger(__name__)

# Поріг суттєвої різниці площі (м²): якщо площа в оголошенні та кадастрі відрізняються більше — додаємо area_by_cadastre_sqm
AREA_DIFF_THRESHOLD_SQM = 50.0

BUILDING_TYPES = (
    "житловий будинок",
    "торгівельний комплекс",
    "МАФ",
    "промислове",
    "складське",
    "не визначено",
)
PREMISES_TYPES = ("житлове", "комерційне", "технічне")


class RealEstateObjectsService:
    """Сервіс для створення та оновлення об'єктів нерухомого майна з оголошень."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.repo = RealEstateObjectsRepository()
        self.unified_repo = UnifiedListingsRepository()
        self.cadastral_repo = CadastralParcelsRepository()
        self.llm_extractor = RealEstateObjectsLLMExtractorService(self.settings)
        self.geocoding = GeocodingService(self.settings)

    def _build_description_from_olx(self, olx_doc: Dict[str, Any]) -> str:
        """Формує текст для LLM з документа OLX."""
        parts = []
        search_data = olx_doc.get("search_data", {})
        detail = olx_doc.get("detail", {})
        title = (search_data.get("title") or "").strip()
        if title:
            parts.append(f"Заголовок: {title}")
        loc = (search_data.get("location") or "").strip()
        if loc:
            parts.append(f"Локація: {loc}")
        params = detail.get("parameters") or []
        if params:
            parts.append("Параметри:")
            for p in params:
                if isinstance(p, dict):
                    lv = (p.get("label") or "").strip()
                    vv = (p.get("value") or "").strip()
                    if lv or vv:
                        parts.append(f"- {lv}: {vv}")
        desc = (detail.get("description") or "").strip()
        if desc:
            parts.append(f"Опис: {desc}")
        llm = detail.get("llm", {})
        if isinstance(llm, dict):
            addrs = llm.get("addresses") or []
            for a in addrs:
                if isinstance(a, dict):
                    line = ", ".join(str(v) for v in a.values() if v)
                    if line:
                        parts.append(f"Адреса: {line}")
        return "\n".join(parts)

    def _extract_objects_from_prozorro_items(
        self, prozorro_doc: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Витягує ОНМ безпосередньо з auction_data.items (структуровані дані ProZorro).
        Повертає список об'єктів у форматі, сумісному з LLM-виводом.
        """
        objects: List[Dict[str, Any]] = []
        auction_data = prozorro_doc.get("auction_data", {})
        items = auction_data.get("items", [])
        if not items:
            return []

        def _get_ua(obj: Any, key: str = "uk_UA") -> str:
            if isinstance(obj, dict):
                return (obj.get("uk_UA") or obj.get("en_US") or "").strip()
            return str(obj).strip() if obj else ""

        def _to_sqm(val: Any, unit_ua: str) -> Optional[float]:
            if val is None:
                return None
            try:
                v = float(val)
                if v <= 0:
                    return None
                u = (unit_ua or "").lower()
                if "га" in u or "гектар" in u:
                    return v * 10000
                return v
            except (ValueError, TypeError):
                return None

        for i, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            item_props = item.get("itemProps") or {}
            classification = item.get("classification", {}) or {}
            class_id = (classification.get("id") or "") if isinstance(classification, dict) else ""
            item_props_type = (item_props.get("itemPropsType") or "").lower()
            unit_ua = ""
            unit = item.get("unit")
            if isinstance(unit, dict):
                unit_name = unit.get("name")
                if isinstance(unit_name, dict):
                    unit_ua = (unit_name.get("uk_UA") or unit_name.get("en_US") or "").lower()
                elif isinstance(unit_name, str):
                    unit_ua = unit_name.lower()

            addr = item.get("address")
            address_dict: Dict[str, Any] = {}
            if isinstance(addr, dict):
                parsed = parse_prozorro_item_address(addr)
                if parsed.get("region"):
                    address_dict["region"] = parsed["region"]
                if parsed.get("settlement"):
                    address_dict["settlement"] = parsed["settlement"]
                if parsed.get("street"):
                    address_dict["street"] = parsed["street"]
                if parsed.get("building"):
                    address_dict["building"] = parsed["building"]
                if parsed.get("formatted_address"):
                    address_dict["formatted_address"] = parsed["formatted_address"]
            formatted_addr = address_dict.get("formatted_address") or ""

            cadastral = None
            if isinstance(item_props, dict):
                cad = item_props.get("cadastralNumber")
                if cad:
                    cadastral = cad if isinstance(cad, str) else (str(cad[0]) if isinstance(cad, list) and cad else None)

            is_land = (
                item_props_type == "land"
                or (class_id and class_id.startswith("06"))
                or (isinstance(item_props, dict) and item_props.get("landArea"))
            )
            building_area = (
                (item_props.get("totalObjectArea") or item_props.get("totalBuildingArea") or item_props.get("usableArea"))
                if isinstance(item_props, dict)
                else None
            )
            land_area = item_props.get("landArea") if isinstance(item_props, dict) else None

            if cadastral and is_land:
                area_sqm = None
                if land_area is not None:
                    try:
                        v = float(land_area)
                        if v > 0:
                            area_sqm = v * 10000 if ("га" in unit_ua or "гектар" in unit_ua) else v
                    except (ValueError, TypeError):
                        pass
                desc = _get_ua(item.get("description")) or f"Земельна ділянка {i + 1}"
                objects.append({
                    "type": "land_plot",
                    "description": desc[:200] if desc else "Земельна ділянка",
                    "area_sqm": area_sqm,
                    "cadastral_number": cadastral,
                })

            has_building = bool(building_area or (address_dict and not is_land))
            if has_building:
                area_sqm = _to_sqm(building_area, unit_ua)
                if area_sqm is None:
                    qty = item.get("quantity")
                    qty_val = qty.get("value") if isinstance(qty, dict) else (qty if isinstance(qty, (int, float)) else None)
                    if qty_val is not None:
                        area_sqm = _to_sqm(qty_val, unit_ua)
                add_cls = item.get("additionalClassifications") or []
                desc = ""
                if isinstance(add_cls, list) and add_cls:
                    ac = add_cls[0] if isinstance(add_cls[0], dict) else {}
                    desc = _get_ua(ac.get("description")) if isinstance(ac, dict) else ""
                if not desc:
                    desc = _get_ua(item.get("description")) or f"Об'єкт {i + 1}"
                obj_type = "building" if address_dict or formatted_addr else "premises"
                obj: Dict[str, Any] = {
                    "type": obj_type,
                    "description": (desc or "Будівля/приміщення")[:200],
                    "area_sqm": area_sqm,
                    "address": address_dict if address_dict else None,
                }
                if obj["address"] and formatted_addr:
                    obj["address"]["formatted_address"] = formatted_addr
                if obj_type == "building" and cadastral:
                    obj["cadastral_number"] = cadastral
                objects.append(obj)

        return objects

    def _build_description_from_prozorro(self, prozorro_doc: Dict[str, Any]) -> str:
        """Формує текст для LLM з документа ProZorro."""
        parts = []
        auction_data = prozorro_doc.get("auction_data", {})
        title_obj = auction_data.get("title", {})
        if isinstance(title_obj, dict):
            title = title_obj.get("uk_UA") or title_obj.get("en_US") or ""
        else:
            title = str(title_obj) if title_obj else ""
        if title:
            parts.append(f"Заголовок: {title}")
        desc_obj = auction_data.get("description", {})
        if isinstance(desc_obj, dict):
            desc = desc_obj.get("uk_UA") or desc_obj.get("en_US") or ""
        else:
            desc = str(desc_obj) if desc_obj else ""
        if desc:
            parts.append(f"Опис: {desc}")
        items = auction_data.get("items", [])
        for i, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            item_parts = []
            addr = item.get("address")
            if isinstance(addr, dict):
                loc = addr.get("locality", {})
                if isinstance(loc, dict):
                    loc_ua = loc.get("uk_UA") or loc.get("en_US")
                    if loc_ua:
                        item_parts.append(str(loc_ua))
                street = addr.get("streetAddress", {})
                if isinstance(street, dict):
                    street_ua = street.get("uk_UA") or street.get("en_US")
                    if street_ua:
                        item_parts.append(str(street_ua))
            if item_parts:
                parts.append(f"Предмет {i + 1} адреса: {', '.join(item_parts)}")
            props = item.get("itemProps", {})
            if isinstance(props, dict):
                area = props.get("totalObjectArea") or props.get("totalBuildingArea") or props.get("usableArea")
                land = props.get("landArea")
                cad = props.get("cadastralNumber")
                if area:
                    parts.append(f"Предмет {i + 1} площа: {area} м²")
                if land:
                    parts.append(f"Предмет {i + 1} земля: {land} га")
                if cad:
                    parts.append(f"Предмет {i + 1} кадастр: {cad}")
        return "\n".join(parts)

    def _merge_cadastral_info(
        self,
        from_listing: Dict[str, Any],
        cadastral_number: str,
        listing_area_sqm: Optional[float] = None,
    ) -> tuple[Dict[str, Any], Optional[float], Optional[float]]:
        """
        Об'єднує кадастрову інформацію з оголошення та cadastral_parcels.
        Заповнює в ОНМ те, чого немає в оголошенні.

        Returns:
            (cadastral_info, area_sqm_for_doc, area_by_cadastre_sqm)
            area_sqm_for_doc — площа для ОНМ: з оголошення, або з кадастру якщо в оголошенні немає
            area_by_cadastre_sqm — площа за кадастром, якщо суттєво відрізняється від оголошення (±50 м²)
        """
        info: Dict[str, Any] = {"cadastral_number": cadastral_number}
        area_sqm_for_doc: Optional[float] = listing_area_sqm
        area_by_cadastre: Optional[float] = None
        parcel = self.cadastral_repo.find_by_cadastral_number(cadastral_number)
        if parcel:
            for k in ("purpose", "purpose_label", "category", "ownership_form", "address"):
                if parcel.get(k) is not None and from_listing.get(k) is None:
                    info[k] = parcel[k]
            cadastral_area = parcel.get("area_sqm")
            if cadastral_area is not None:
                try:
                    cadastral_area = float(cadastral_area)
                except (ValueError, TypeError):
                    cadastral_area = None
            if cadastral_area is not None:
                info["area_sqm"] = cadastral_area
                listing_area = listing_area_sqm
                if listing_area is not None:
                    try:
                        listing_area = float(listing_area)
                    except (ValueError, TypeError):
                        listing_area = None
                if listing_area is not None:
                    if abs(listing_area - cadastral_area) > AREA_DIFF_THRESHOLD_SQM:
                        area_by_cadastre = cadastral_area
                else:
                    area_sqm_for_doc = cadastral_area
        for k, v in from_listing.items():
            if v is not None and k != "cadastral_number":
                info[k] = v
        return info, area_sqm_for_doc, area_by_cadastre

    def _backfill_land_plot_from_cadastre(
        self,
        object_id: str,
        cadastral_number: str,
        listing_area_sqm: Optional[float] = None,
    ) -> bool:
        """
        Перезаповнює земельну ділянку даними з кадастру.
        Використовується для існуючих записів — додає те, чого немає в оголошенні.

        Returns:
            True якщо оновлено
        """
        parcel = self.cadastral_repo.find_by_cadastral_number(cadastral_number)
        if not parcel:
            return False
        existing = self.repo.find_by_id(object_id)
        if not existing or existing.get("type") != "land_plot":
            return False

        updates: Dict[str, Any] = {}
        cad_info = dict(existing.get("cadastral_info") or {})
        cad_info["cadastral_number"] = cadastral_number

        for k in ("purpose", "purpose_label", "category", "ownership_form", "address"):
            if parcel.get(k) is not None and cad_info.get(k) is None:
                cad_info[k] = parcel[k]

        cadastral_area = parcel.get("area_sqm")
        if cadastral_area is not None:
            try:
                cadastral_area = float(cadastral_area)
            except (ValueError, TypeError):
                cadastral_area = None

        if cadastral_area is not None:
            cad_info["area_sqm"] = cadastral_area
            existing_area = existing.get("area_sqm")
            if existing_area is not None:
                try:
                    existing_area = float(existing_area)
                except (ValueError, TypeError):
                    existing_area = None
            listing_area = listing_area_sqm if listing_area_sqm is not None else existing_area
            if listing_area is not None and abs(listing_area - cadastral_area) > AREA_DIFF_THRESHOLD_SQM:
                updates["area_by_cadastre_sqm"] = cadastral_area
            elif existing_area is None:
                updates["area_sqm"] = cadastral_area

        updates["cadastral_info"] = cad_info
        return self.repo.update_object(object_id, **updates)

    def _geocode_address(self, address_str: str) -> Optional[Dict[str, Any]]:
        """
        Геокодує адресу, повертає нормалізовану структуру.
        is_complete=True тільки якщо є вулиця та номер будинку (точність до будинку).
        """
        if not address_str or not address_str.strip():
            return None
        try:
            result = self.geocoding.geocode(
                query=address_str.strip(), region="ua", caller="real_estate_objects"
            )
            results = result.get("results", [])
            if not results:
                return None
            r = results[0]
            addr_struct = r.get("address_structured", {}) or {}
            street = addr_struct.get("street")
            street_number = addr_struct.get("street_number")
            is_complete = bool(street and street_number)
            location_type = r.get("location_type", "")
            if location_type == "APPROXIMATE" and not is_complete:
                is_complete = False
            return {
                "region": addr_struct.get("region"),
                "settlement": addr_struct.get("city") or addr_struct.get("sublocality"),
                "street": street,
                "building": street_number,
                "formatted_address": r.get("formatted_address", ""),
                "coordinates": {
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude"),
                },
                "is_complete": is_complete,
            }
        except Exception as e:
            logger.warning("Геокодування адреси %s: %s", address_str[:50], e)
            return None

    def _is_address_precise_enough(self, geocoded: Optional[Dict[str, Any]]) -> bool:
        """
        Перевіряє, чи адреса достатньо точна (вулиця + номер будинку).
        Тільки при точній адресі групуємо будівлі з різних оголошень.
        """
        if not geocoded:
            return False
        return bool(geocoded.get("is_complete"))

    def _format_address_from_llm(self, addr: Dict[str, Any]) -> str:
        """Формує рядок адреси з LLM-об'єкта для геокодування."""
        parts = []
        for k in ("region", "district", "settlement", "street", "building"):
            v = addr.get(k)
            if v:
                parts.append(str(v))
        return ", ".join(parts) if parts else addr.get("formatted_address") or ""

    def _create_building_doc(
        self,
        obj: Dict[str, Any],
        addr: Optional[Dict[str, Any]],
        geocoded: Optional[Dict[str, Any]],
        formatted: Optional[str],
        desc_short: str,
        area_sqm: Optional[float],
        source: str,
        source_id: str,
        land_plot_id: Optional[str],
    ) -> str:
        """Створює документ будівлі (з адресою чи без, з прив'язкою до ділянки чи без)."""
        addr_doc: Dict[str, Any] = {}
        if geocoded:
            addr_doc = {k: v for k, v in geocoded.items() if k != "is_complete"}
            if not addr_doc.get("building") and isinstance(addr, dict) and addr.get("building"):
                addr_doc["building"] = addr["building"]
        elif isinstance(addr, dict):
            addr_doc = {
                "region": addr.get("region"),
                "settlement": addr.get("settlement"),
                "street": addr.get("street"),
                "building": addr.get("building"),
                "formatted_address": formatted or "",
            }
        doc: Dict[str, Any] = {
            "type": "building",
            "description": desc_short,
            "area_sqm": area_sqm,
            "address": addr_doc if addr_doc else None,
            "building_type": obj.get("building_type") if obj.get("building_type") in BUILDING_TYPES else "не визначено",
            "source_listing_ids": [{"source": source, "source_id": source_id}],
        }
        if land_plot_id:
            doc["land_plot_ids"] = [ObjectId(land_plot_id) if isinstance(land_plot_id, str) else land_plot_id]
        if obj.get("communications"):
            doc["building_info"] = {"communications": obj["communications"]}
        return self.repo.create_doc(doc)

    def process_listing(
        self,
        source: str,
        source_id: str,
        olx_doc: Optional[Dict[str, Any]] = None,
        prozorro_doc: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> List[str]:
        """
        Обробляє оголошення: витягує ОНМ, створює/оновлює, зберігає посилання.

        Returns:
            Список object_id створених/оновлених об'єктів
        """
        if source not in ("olx", "prozorro"):
            return []
        objects_raw: List[Dict[str, Any]] = []

        if prozorro_doc:
            objects_raw = self._extract_objects_from_prozorro_items(prozorro_doc)
            if not objects_raw:
                description = self._build_description_from_prozorro(prozorro_doc)
                if description.strip():
                    objects_raw = self.llm_extractor.extract_objects(description, use_cache=use_cache) or []
        elif olx_doc:
            description = self._build_description_from_olx(olx_doc)
            if description.strip():
                objects_raw = self.llm_extractor.extract_objects(description, use_cache=use_cache) or []
        else:
            unified = self.unified_repo.find_by_source_id(source, source_id)
            if not unified:
                return []
            description = f"{unified.get('title') or ''}\n{unified.get('description') or ''}"
            if description.strip():
                objects_raw = self.llm_extractor.extract_objects(description, use_cache=use_cache) or []

        if not objects_raw:
            return []
        refs: List[Dict[str, Any]] = []
        created_ids: List[str] = []
        building_by_address: Dict[str, str] = {}  # тільки для точних адрес (групування)
        building_by_cadastral: Dict[str, str] = {}  # кадастр -> building_id (групування)
        land_plot_by_cadastral: Dict[str, str] = {}  # кадастр -> land_plot_id

        def _parse_obj(obj: Dict[str, Any]) -> tuple:
            obj_type = (obj.get("type") or "").strip().lower()
            desc_short = (obj.get("description") or "").strip() or "Без опису"
            area_sqm = obj.get("area_sqm")
            if area_sqm is not None:
                try:
                    area_sqm = float(area_sqm)
                except (ValueError, TypeError):
                    area_sqm = None
            return obj_type, desc_short, area_sqm

        # Перший прохід: тільки land_plot, щоб land_plot_by_cadastral був заповнений
        for obj in objects_raw:
            if not isinstance(obj, dict):
                continue
            obj_type, desc_short, area_sqm = _parse_obj(obj)
            if obj_type == "land_plot":
                cad = (obj.get("cadastral_number") or "").strip()
                if not cad:
                    continue
                existing = self.repo.find_by_cadastral_number(cad)
                if existing:
                    oid = existing["_id"]
                    self._backfill_land_plot_from_cadastre(oid, cad, listing_area_sqm=area_sqm)
                    self.repo.add_source_listing(oid, source, source_id)
                else:
                    cad_info, area_sqm_for_doc, area_by_cadastre_sqm = self._merge_cadastral_info(
                        {k: obj.get(k) for k in ("area_sqm", "purpose", "purpose_label") if obj.get(k) is not None},
                        cad,
                        listing_area_sqm=area_sqm,
                    )
                    create_kwargs: Dict[str, Any] = {
                        "description": desc_short,
                        "area_sqm": area_sqm_for_doc or area_sqm,
                        "cadastral_info": cad_info,
                        "source_listing_ids": [{"source": source, "source_id": source_id}],
                    }
                    if area_by_cadastre_sqm is not None:
                        create_kwargs["area_by_cadastre_sqm"] = area_by_cadastre_sqm
                    oid = self.repo.create("land_plot", **create_kwargs)
                refs.append({"object_id": oid, "role": "primary"})
                created_ids.append(oid)
                land_plot_by_cadastral[cad] = oid

        # Другий прохід: building та premises
        for obj in objects_raw:
            if not isinstance(obj, dict):
                continue
            obj_type, desc_short, area_sqm = _parse_obj(obj)
            if obj_type == "land_plot":
                continue
            if obj_type == "building":
                addr = obj.get("address")
                if isinstance(addr, dict):
                    addr_str = self._format_address_from_llm(addr) or addr.get("formatted_address") or ""
                else:
                    addr_str = ""
                cad = (obj.get("cadastral_number") or "").strip()
                geocoded = self._geocode_address(addr_str) if addr_str else None
                address_precise = self._is_address_precise_enough(geocoded)
                land_plot_id = land_plot_by_cadastral.get(cad) if cad else None

                oid = None
                if address_precise:
                    formatted = (geocoded.get("formatted_address") if geocoded else None) or addr_str
                    if formatted in building_by_address:
                        oid = building_by_address[formatted]
                        self.repo.add_source_listing(oid, source, source_id)
                    else:
                        existing = self.repo.find_building_by_address(
                            formatted,
                            region=geocoded.get("region") if geocoded else None,
                            settlement=geocoded.get("settlement") if geocoded else None,
                        )
                        if existing:
                            oid = existing["_id"]
                            self.repo.add_source_listing(oid, source, source_id)
                        else:
                            oid = self._create_building_doc(
                                obj, addr, geocoded, formatted, desc_short, area_sqm,
                                source, source_id, land_plot_id,
                            )
                        building_by_address[formatted] = oid
                elif land_plot_id:
                    if cad in building_by_cadastral:
                        oid = building_by_cadastral[cad]
                        self.repo.add_source_listing(oid, source, source_id)
                    else:
                        existing = self.repo.find_building_by_land_plot_id(land_plot_id)
                        if existing:
                            oid = existing["_id"]
                            self.repo.add_source_listing(oid, source, source_id)
                        else:
                            oid = self._create_building_doc(
                                obj, addr, geocoded, addr_str or None, desc_short, area_sqm,
                                source, source_id, land_plot_id,
                            )
                        building_by_cadastral[cad] = oid
                else:
                    oid = self._create_building_doc(
                        obj, addr, geocoded, addr_str or None, desc_short, area_sqm,
                        source, source_id, None,
                    )
                if oid:
                    refs.append({"object_id": oid, "role": "primary"})
                    created_ids.append(oid)
            elif obj_type == "premises":
                addr = obj.get("address")
                addr_str = ""
                if isinstance(addr, dict):
                    addr_str = self._format_address_from_llm(addr) or addr.get("formatted_address") or ""
                cad = (obj.get("cadastral_number") or "").strip()
                geocoded = self._geocode_address(addr_str) if addr_str else None
                address_precise = self._is_address_precise_enough(geocoded)
                land_plot_id = land_plot_by_cadastral.get(cad) if cad else None

                building_id = None
                if address_precise:
                    formatted = geocoded.get("formatted_address") or addr_str
                    if formatted in building_by_address:
                        building_id = building_by_address[formatted]
                    else:
                        building = self.repo.find_building_by_address(
                            formatted,
                            region=geocoded.get("region"),
                            settlement=geocoded.get("settlement"),
                        )
                        if building:
                            building_id = building["_id"]
                            building_by_address[formatted] = building_id
                        else:
                            building_id = self._create_building_doc(
                                obj, addr, geocoded, formatted, desc_short or "Будівля", area_sqm,
                                source, source_id, None,
                            )
                            building_by_address[formatted] = building_id
                            refs.append({"object_id": building_id, "role": "secondary"})
                elif land_plot_id:
                    if cad in building_by_cadastral:
                        building_id = building_by_cadastral[cad]
                    else:
                        building = self.repo.find_building_by_land_plot_id(land_plot_id)
                        if building:
                            building_id = building["_id"]
                            building_by_cadastral[cad] = building_id
                        else:
                            building_id = self._create_building_doc(
                                obj, addr, geocoded, addr_str or None, desc_short or "Будівля", area_sqm,
                                source, source_id, land_plot_id,
                            )
                            building_by_cadastral[cad] = building_id
                            refs.append({"object_id": building_id, "role": "secondary"})
                elif addr_str or addr:
                    building_id = self._create_building_doc(
                        obj, addr, geocoded, addr_str or None, desc_short or "Будівля", area_sqm,
                        source, source_id, None,
                    )
                    refs.append({"object_id": building_id, "role": "secondary"})
                if not building_id:
                    continue
                floor = obj.get("floor")
                if floor is not None:
                    try:
                        floor = int(floor)
                    except (ValueError, TypeError):
                        floor = None
                prem_type = obj.get("premises_type") if obj.get("premises_type") in PREMISES_TYPES else "комерційне"
                doc = {
                    "type": "premises",
                    "description": desc_short,
                    "area_sqm": area_sqm,
                    "building_id": ObjectId(building_id) if isinstance(building_id, str) else building_id,
                    "floor": floor,
                    "premises_type": prem_type,
                    "communications": obj.get("communications") if obj.get("communications") else [],
                    "description_text": (obj.get("description_text") or "").strip(),
                    "source_listing_ids": [{"source": source, "source_id": source_id}],
                }
                oid = self.repo.create_doc(doc)
                try:
                    bid = ObjectId(building_id) if isinstance(building_id, str) else building_id
                    self.repo.collection.update_one(
                        {"_id": bid},
                        {
                            "$addToSet": {"premises_ids": ObjectId(oid)},
                            "$set": {"updated_at": datetime.now(timezone.utc)},
                        },
                    )
                except Exception as e:
                    logger.warning("Оновлення premises_ids будівлі %s: %s", building_id, e)
                refs.append({"object_id": oid, "role": "primary"})
                created_ids.append(oid)
        if refs:
            seen = set()
            unique_refs = []
            for r in refs:
                oid = r.get("object_id")
                if oid is None:
                    continue
                oid_str = str(oid) if not isinstance(oid, str) else oid
                if oid_str in seen:
                    continue
                seen.add(oid_str)
                unique_refs.append({"object_id": oid_str, "role": r.get("role", "primary")})
            self.unified_repo.update_real_estate_object_refs(source, source_id, unique_refs)
        return created_ids
