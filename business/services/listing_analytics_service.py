# -*- coding: utf-8 -*-
"""
LLM-згенерована детальна аналітика оголошення: ціна за одиницю площі, місцезнаходження, оточення.
Зберігається в listing_analytics за source + source_id.
"""

import logging
from typing import Any, Dict, Optional

from config.settings import Settings
from data.repositories.listing_analytics_repository import ListingAnalyticsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository

logger = logging.getLogger(__name__)


def _build_context_from_unified(doc: Dict[str, Any]) -> str:
    """Будує текстовий контекст з unified_listings для LLM."""
    parts = []
    if doc.get("title"):
        parts.append(f"Назва: {doc['title']}")
    if doc.get("price_uah") is not None:
        parts.append(f"Ціна: {doc['price_uah']} грн")
    if doc.get("price_usd") is not None:
        parts.append(f"Ціна (USD): {doc['price_usd']}")
    if doc.get("price_per_m2_uah") is not None:
        parts.append(f"Ціна за м²: {doc['price_per_m2_uah']} грн/м²")
    if doc.get("price_per_ha_uah") is not None:
        parts.append(f"Ціна за сотку: {doc['price_per_ha_uah'] / 100} грн/с")
    if doc.get("building_area_sqm") is not None:
        parts.append(f"Площа будівлі: {doc['building_area_sqm']} м²")
    if doc.get("land_area_sqm") is not None:
        sotky = doc["land_area_sqm"] / 100.0
        parts.append(f"Площа землі: {sotky:.1f} с")
    addresses = doc.get("addresses", [])
    if addresses and isinstance(addresses, list):
        addr_parts = []
        for a in addresses[:2]:
            if isinstance(a, dict):
                p = []
                if a.get("region"):
                    p.append(str(a["region"]))
                if a.get("settlement"):
                    p.append(str(a["settlement"]))
                if a.get("street"):
                    p.append(str(a["street"]))
                if a.get("building"):
                    p.append(str(a["building"]))
                if p:
                    addr_parts.append(", ".join(p))
        if addr_parts:
            parts.append("Адреса: " + "; ".join(addr_parts))
    if doc.get("region"):
        parts.append(f"Область: {doc['region']}")
    if doc.get("city"):
        parts.append(f"Місто: {doc['city']}")
    if doc.get("description"):
        desc = str(doc["description"])[:1500]
        if len(str(doc.get("description", ""))) > 1500:
            desc += "..."
        parts.append(f"Опис: {desc}")
    if doc.get("property_type"):
        parts.append(f"Тип: {doc['property_type']}")
    return "\n".join(parts) if parts else "Немає даних"


def _build_context_from_olx(doc: Dict[str, Any]) -> str:
    """Будує контекст з olx_listings."""
    parts = []
    search = doc.get("search_data", {}) or {}
    detail = doc.get("detail", {}) or {}
    if search.get("title"):
        parts.append(f"Назва: {search['title']}")
    if search.get("price_value") is not None:
        parts.append(f"Ціна: {search['price_value']} грн")
    if search.get("location"):
        parts.append(f"Локація: {search['location']}")
    pm = detail.get("price_metrics") or {}
    if pm.get("price_per_m2_uah") is not None:
        parts.append(f"Ціна за м²: {pm['price_per_m2_uah']} грн/м²")
    if pm.get("price_per_ha_uah") is not None:
        parts.append(f"Ціна за сотку: {pm['price_per_ha_uah'] / 100} грн/с")
    llm = detail.get("llm") or {}
    if llm.get("building_area_sqm") is not None:
        parts.append(f"Площа будівлі: {llm['building_area_sqm']} м²")
    land_sqm = llm.get("land_area_sqm")
    if land_sqm is None and llm.get("land_area_ha") is not None:
        try:
            land_sqm = float(llm["land_area_ha"]) * 10000.0
        except (TypeError, ValueError):
            land_sqm = None
    if land_sqm is not None:
        parts.append(f"Площа землі: {land_sqm / 100:.1f} с")
    if detail.get("description"):
        desc = str(detail["description"])[:1500]
        if len(str(detail.get("description", ""))) > 1500:
            desc += "..."
        parts.append(f"Опис: {desc}")
    return "\n".join(parts) if parts else "Немає даних"


def _build_context_from_prozorro(doc: Dict[str, Any]) -> str:
    """Будує контекст з prozorro_auctions."""
    parts = []
    ad = doc.get("auction_data", {}) or {}
    title = ad.get("title") or {}
    if isinstance(title, dict):
        title = title.get("uk_UA") or title.get("en_US") or ""
    if title:
        parts.append(f"Назва: {title}")
    val = ad.get("value") or {}
    if isinstance(val, dict) and val.get("amount") is not None:
        parts.append(f"Стартова ціна: {val['amount']} грн")
    pm = ad.get("price_metrics") or {}
    if pm.get("price_per_m2_uah") is not None:
        parts.append(f"Ціна за м²: {pm['price_per_m2_uah']} грн/м²")
    if pm.get("price_per_ha_uah") is not None:
        parts.append(f"Ціна за га: {pm['price_per_ha_uah']} грн/га")
    refs = ad.get("address_refs", [])
    if refs and isinstance(refs[0], dict):
        r = refs[0]
        loc_parts = []
        if isinstance(r.get("region"), dict):
            loc_parts.append(r["region"].get("name", ""))
        if isinstance(r.get("city"), dict):
            loc_parts.append(r["city"].get("name", ""))
        if loc_parts:
            parts.append("Адреса: " + ", ".join(loc_parts))
    items = ad.get("items", [])
    if items and isinstance(items[0], dict):
        ip = items[0].get("itemProps") or {}
        if ip.get("totalBuildingArea"):
            parts.append(f"Площа будівлі: {ip['totalBuildingArea']} м²")
        if ip.get("landArea"):
            parts.append(f"Площа землі: {ip['landArea']} га")
    desc = ad.get("description") or {}
    if isinstance(desc, dict):
        desc = desc.get("uk_UA") or desc.get("en_US") or ""
    if desc:
        desc = str(desc)[:1500]
        if len(str(ad.get("description", {}).get("uk_UA", ""))) > 1500:
            desc += "..."
        parts.append(f"Опис: {desc}")
    return "\n".join(parts) if parts else "Немає даних"


class ListingAnalyticsService:
    """Сервіс LLM-аналітики оголошення."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.repo = ListingAnalyticsRepository()
        self.unified_repo = UnifiedListingsRepository()
        self.olx_repo = OlxListingsRepository()
        self.prozorro_repo = ProZorroAuctionsRepository()
        self.repo.ensure_index()
        self._llm = None

    @property
    def llm_service(self):
        if self._llm is None:
            from business.services.llm_service import LLMService
            self._llm = LLMService(self.settings)
        return self._llm

    def get_listing_doc(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Отримує документ оголошення."""
        src = source.lower()
        if src == "olx":
            if source_id.startswith("http"):
                return self.olx_repo.find_by_url(source_id)
            return self.olx_repo.find_by_id(source_id)
        if src == "prozorro":
            return self.prozorro_repo.find_by_auction_id(source_id)
        return self.unified_repo.find_by_source_id(src, source_id)

    def get_analytics(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Отримує збережену аналітику."""
        return self.repo.find_by_source_id(source.lower(), source_id)

    def _get_analytics_context(
        self,
        unified_doc: Optional[Dict[str, Any]],
        doc: Optional[Dict[str, Any]],
        source: str,
        source_id: str,
    ) -> str:
        """Отримує ринковий контекст (індикатор, агрегати) для LLM."""
        try:
            from business.services.price_analytics_service import PriceAnalyticsService
            analytics = PriceAnalyticsService()

            item = None
            if unified_doc:
                addrs = unified_doc.get("addresses", [])
                city = None
                region = None
                if addrs and isinstance(addrs[0], dict):
                    city = addrs[0].get("settlement")
                    region = addrs[0].get("region")
                item = {
                    "source": source,
                    "source_id": source_id,
                    "city": city or unified_doc.get("city"),
                    "region": region or unified_doc.get("region"),
                    "price_uah": unified_doc.get("price_uah"),
                    "price_per_m2_uah": unified_doc.get("price_per_m2_uah"),
                    "price_per_ha_uah": unified_doc.get("price_per_ha_uah"),
                    "building_area_sqm": unified_doc.get("building_area_sqm"),
                    "land_area_sqm": unified_doc.get("land_area_sqm"),
                }
            elif doc and source == "olx":
                search = doc.get("search_data", {}) or {}
                detail = doc.get("detail", {}) or {}
                pm = detail.get("price_metrics") or {}
                llm = detail.get("llm") or {}
                loc = search.get("location", "")
                parts = [p.strip() for p in str(loc).replace(" - ", ",").split(",")]
                item = {
                    "source": "olx",
                    "source_id": source_id,
                    "city": parts[0] if parts else None,
                    "region": parts[1] if len(parts) > 1 else None,
                    "price_uah": search.get("price_value"),
                    "price_per_m2_uah": pm.get("price_per_m2_uah"),
                    "price_per_ha_uah": pm.get("price_per_ha_uah"),
                    "building_area_sqm": llm.get("building_area_sqm"),
                    "land_area_sqm": llm.get("land_area_sqm") or (float(llm["land_area_ha"]) * 10000.0 if llm.get("land_area_ha") not in (None, "") else None),
                }
            elif doc and source == "prozorro":
                ad = doc.get("auction_data", {}) or {}
                refs = ad.get("address_refs", [])
                city = region = None
                if refs and isinstance(refs[0], dict):
                    r = refs[0]
                    if isinstance(r.get("city"), dict):
                        city = r["city"].get("name")
                    if isinstance(r.get("region"), dict):
                        region = r["region"].get("name")
                pm = ad.get("price_metrics") or {}
                items = ad.get("items", [])
                b_sqm = l_sqm = None
                if items and isinstance(items[0], dict):
                    ip = items[0].get("itemProps") or {}
                    b_sqm = ip.get("totalBuildingArea") or ip.get("totalObjectArea") or ip.get("usableArea")
                    raw = ip.get("landArea")
                    if raw is not None:
                        try:
                            v = float(raw)
                            l_sqm = v * 10000.0 if v < 10000 else v
                        except (TypeError, ValueError):
                            l_sqm = None
                item = {
                    "source": "prozorro",
                    "source_id": source_id,
                    "city": city,
                    "region": region,
                    "price_uah": (ad.get("value") or {}).get("amount") if isinstance(ad.get("value"), dict) else None,
                    "price_per_m2_uah": pm.get("price_per_m2_uah"),
                    "price_per_ha_uah": pm.get("price_per_ha_uah"),
                    "building_area_sqm": b_sqm,
                    "land_area_sqm": l_sqm,
                }

            if not item:
                return ""

            indicators = analytics.get_price_indicators_for_items([item])
            cid = f"{source}:{source_id}"
            ind_data = indicators.get(cid) if indicators else None
            indicator = ind_data.get("indicator") if ind_data else None
            ind_source = ind_data.get("source", "region") if ind_data else None

            city = item.get("city") or ""
            region = item.get("region") or ""
            from datetime import datetime, timezone
            month_key = datetime.now(timezone.utc).strftime("%Y-%m")
            rows = analytics.get_aggregated_analytics(
                period_type="month",
                period_key=month_key,
                region=region or None,
                city=city or None,
            )
            locality = [city, region] if (city and region) else [region or city or "локації"]
            locality_str = ", ".join(p for p in locality if p)

            parts = [f"Ринковий контекст ({locality_str}):"]
            total_count = 0
            avg_m2 = None
            avg_ha = None
            def _norm(s: str) -> str:
                return (s or "").replace(" область", "").strip().lower()

            for r in rows[:30]:
                m = r.get("metrics", {}) or {}
                gr = r.get("group_region") or (r.get("group_by") or {}).get("region", "")
                gc = r.get("group_city") or (r.get("group_by") or {}).get("city", "")
                if region and gr and _norm(gr) != _norm(region):
                    continue
                if city and gc and (gc or "").strip().lower() != (city or "").strip().lower():
                    continue
                total_count += r.get("count", 0)
                pm2 = m.get("price_per_m2_uah") or {}
                if isinstance(pm2, dict) and pm2.get("avg") and avg_m2 is None:
                    avg_m2 = pm2["avg"]
                pha = m.get("price_per_ha_uah") or {}
                if isinstance(pha, dict) and pha.get("avg") and avg_ha is None:
                    avg_ha = pha["avg"]

            if total_count > 0:
                parts.append(f"За останній місяць: {total_count} схожих оголошень.")
                if avg_m2:
                    parts.append(f"Середня ціна за м²: {round(avg_m2):,} грн/м².".replace(",", " "))
                if avg_ha:
                    parts.append(f"Середня ціна за га: {round(avg_ha):,} грн/га.".replace(",", " "))

            city_context_note = None
            ppm2 = item.get("price_per_m2_uah")
            ppha = item.get("price_per_ha_uah")
            if ind_source == "region" and city and total_count > 0 and total_count < 5:
                if avg_m2 and ppm2 is not None and ppm2 > 0:
                    if ppm2 < avg_m2 and indicator in ("аномально висока", "дорога", "вище середньої"):
                        city_context_note = (
                            f"Ми маємо небагато ({total_count}) даних із міста, але якщо орієнтуватись по них — "
                            "ціна по цій місцевості нижче середньої. Це може вказувати на те, що дана локація "
                            "має переваги в межах області. З накопиченням масиву оголошень аналітика буде більш точна."
                        )
                    elif ppm2 > avg_m2 * 1.3 and indicator in ("аномально низька", "вигідна"):
                        city_context_note = (
                            f"Ми маємо небагато ({total_count}) даних із міста, але якщо орієнтуватись по них — "
                            "ціна по цій місцевості вища за середню. З накопиченням масиву оголошень аналітика буде більш точна."
                        )
                elif avg_ha and ppha is not None and ppha > 0:
                    if ppha < avg_ha and indicator in ("аномально висока", "дорога", "вище середньої"):
                        city_context_note = (
                            f"Ми маємо небагато ({total_count}) даних із міста, але якщо орієнтуватись по них — "
                            "ціна по цій місцевості нижче середньої. Це може вказувати на те, що дана локація "
                            "має переваги в межах області. З накопиченням масиву оголошень аналітика буде більш точна."
                        )
                    elif ppha > avg_ha * 1.3 and indicator in ("аномально низька", "вигідна"):
                        city_context_note = (
                            f"Ми маємо небагато ({total_count}) даних із міста, але якщо орієнтуватись по них — "
                            "ціна по цій місцевості вища за середню. З накопиченням масиву оголошень аналітика буде більш точна."
                        )

            if indicator:
                scope = "у місті" if ind_source == "city" else "в області"
                ind_labels = {
                    "вигідна": "нижча за 25% оголошень",
                    "нижче середньої": "у діапазоні 25–62% (нижча за медіану)",
                    "середня": "у середині діапазону (62–87%)",
                    "вище середньої": "у верхній частині (87–100%)",
                    "дорога": "вища за 75% оголошень",
                    "аномально низька": "значно нижча за типовий діапазон",
                    "аномально висока": "значно вища за типовий діапазон",
                }
                ind_desc = ind_labels.get(indicator, indicator)
                parts.append(f"Ціна цього об'єкта {ind_desc} {scope}.")
                if city_context_note:
                    parts.append(city_context_note)

            return "\n".join(parts) if len(parts) > 1 else ""
        except Exception as e:
            logger.debug("Analytics context for LLM: %s", e)
            return ""

    def generate_and_save(
        self,
        source: str,
        source_id: str,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """
        Генерує аналітику через LLM та зберігає.
        Повертає { analysis_text, analysis_at, error? }.
        """
        src = source.lower()
        if not force_refresh:
            cached = self.repo.find_by_source_id(src, source_id)
            if cached and cached.get("analysis_text"):
                return {
                    "analysis_text": cached["analysis_text"],
                    "analysis_at": cached.get("analysis_at"),
                    "metadata": cached.get("metadata", {}),
                }

        unified_doc = self.unified_repo.find_by_source_id(src, source_id)
        doc = self.get_listing_doc(src, source_id) if not unified_doc else None
        if unified_doc:
            context = _build_context_from_unified(unified_doc)
        elif doc:
            if src == "olx":
                context = _build_context_from_olx(doc)
            elif src == "prozorro":
                context = _build_context_from_prozorro(doc)
            else:
                context = "Немає даних"
        else:
            return {"error": "Оголошення не знайдено"}

        analytics_context = self._get_analytics_context(unified_doc, doc, src, source_id)

        try:
            from config.config_loader import get_config_loader
            loader = get_config_loader()
            template = loader.get_prompt("listing_analytics", fallback=None)
            if template:
                prompt = template.format(
                    context=context,
                    analytics_context=analytics_context or "Ринковий контекст відсутній.",
                )
            else:
                prompt = f"""Produce a short analysis of the real-estate listing. The analysis must be written in Ukrainian.

Listing data:
{context}

Market context:
{analytics_context or "None."}

Write the analysis in 3 parts: 1) Price (comparison per unit area — UAH/m² or UAH/ha); 2) Location; 3) Surroundings. Be concise. Use the market context for comparison. Output in Ukrainian only."""

            raw = self.llm_service.generate_text(prompt, temperature=0.3)
            if not raw or not raw.strip():
                return {"error": "LLM не повернув відповідь"}

            analysis_text = raw.strip()
            self.repo.upsert(
                source=src,
                source_id=source_id,
                analysis_text=analysis_text,
                metadata={"source": src, "source_id": source_id},
            )
            cached = self.repo.find_by_source_id(src, source_id)
            return {
                "analysis_text": analysis_text,
                "analysis_at": cached.get("analysis_at") if cached else None,
                "metadata": cached.get("metadata", {}) if cached else {},
            }
        except Exception as e:
            logger.exception("Listing analytics generation failed: %s", e)
            return {"error": str(e)}
