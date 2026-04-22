# -*- coding: utf-8 -*-
"""
LLM-парсер сторінок оголошень OLX.

Завдання:
- Отримати з detail/search_data максимум структурованої інформації про об'єкт.
- Передавати в LLM вже очищений від банерів/сміття текст (не сирий HTML).
- Використовувати LLM кеш: однаковий текст → один виклик LLM, далі — відповідь з кешу.

Поточна реалізація:
- Використовує існуючий LLMService.parse_auction_description(), який вже заточений під
  нерухомість (адреси, площі, поверхи тощо).
- Будує "description" як конкатенацію:
  заголовка, локації, параметрів, опису та контактних даних (витягнутих із detail).
"""

import logging
import re
from typing import Dict, Any

from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService
from utils.hash_utils import calculate_object_version_hash, calculate_description_hash


logger = logging.getLogger(__name__)


class OlxLLMExtractorService:
    """Сервіс, що застосовує LLM до сторінки оголошення OLX з кешуванням результатів."""
    _CADASTRAL_PATTERN = re.compile(r"\b\d{10,12}(?::\d{1,4}){2,3}\b")
    _PRICE_RECOVERY_CACHE_PREFIX = "olx_price_recovery_"
    _SOTOK_PATTERN = re.compile(r"(\d[\d\s]*[.,]?\d*)\s*сот(?:к(?:а|и|у|ою)?|ок)?\b", re.IGNORECASE)

    def __init__(self, settings: Settings):
        self.settings = settings
        # LLMService всередині вже використовує rate limiting
        self.llm_service = None
        try:
            self.llm_service = LLMService(settings)
        except Exception as e:
            # Якщо LLM недоступний / не налаштований — сервіс працює в режимі no-op
            print(f"[OlxLLMExtractor] Попередження: LLM недоступний: {e}")
            self.llm_service = None

        self.cache_service = LLMCacheService()

    @staticmethod
    def _build_description_text(search_data: Dict[str, Any], detail: Dict[str, Any]) -> str:
        """Будує текстовий опис для LLM на основі полів пошуку + detail."""
        parts = []

        title = (search_data.get("title") or "").strip()
        if title:
            parts.append(f"Заголовок оголошення: {title}")

        # Локація: з detail.location та/або search_data.location
        loc_block = []
        loc_struct = detail.get("location") or {}
        if isinstance(loc_struct, dict):
            city = loc_struct.get("city")
            region = loc_struct.get("region")
            raw = loc_struct.get("raw")
            if city:
                loc_block.append(f"Місто: {city}")
            if region:
                loc_block.append(f"Область: {region}")
            if raw:
                loc_block.append(f"Сирий рядок локації: {raw}")

        loc_text_search = (search_data.get("location") or "").strip()
        if loc_text_search:
            loc_block.append(f"Локація зі сторінки пошуку: {loc_text_search}")

        if loc_block:
            parts.append("Локація об'єкта:")
            parts.extend(loc_block)

        # Параметри (теги: площа, поверх, тип об'єкта тощо) — сортуємо для стабільного кешу
        params = detail.get("parameters") or []
        if isinstance(params, list) and params:
            parts.append("Параметри об'єкта (ключ=значення):")
            for p in sorted(
                params,
                key=lambda x: (
                    (x.get("label") or "").strip(),
                    (x.get("value") or "").strip(),
                ),
            ):
                label = (p.get("label") or "").strip()
                value = (p.get("value") or "").strip()
                if not label and not value:
                    continue
                if value:
                    parts.append(f"- {label}: {value}")
                else:
                    parts.append(f"- {label}")

        # Опис
        description = (detail.get("description") or "").strip()
        if not description:
            # fallback: raw_snippet зі сторінки списку
            snippet = (search_data.get("raw_snippet") or "").strip()
            description = snippet
        if description:
            parts.append("Повний опис оголошення:")
            parts.append(description)

        # Контактні дані
        contact = detail.get("contact") or {}
        contact_lines = []
        if isinstance(contact, dict):
            name = (contact.get("name") or "").strip()
            if name:
                contact_lines.append(f"Ім'я продавця: {name}")
            profile_url = (contact.get("profile_url") or "").strip()
            if profile_url:
                contact_lines.append(f"URL профілю продавця: {profile_url}")
            phone_preview = (contact.get("phone_preview") or "").strip()
            if phone_preview:
                contact_lines.append(f"Фрагмент телефону (як видно на сторінці): {phone_preview}")

        if contact_lines:
            parts.append("Контактні дані продавця:")
            parts.extend(contact_lines)

        # Ціна
        price_text = (search_data.get("price_text") or "").strip()
        if price_text:
            parts.append(f"Ціна (зі сторінки): {price_text}")

        return "\n".join(parts)

    @staticmethod
    def _build_key_payload(search_data: Dict[str, Any], detail: Dict[str, Any]) -> Dict[str, Any]:
        """
        Формує payload лише з ключової інформації оголошення для хешування.

        Ігноруємо технічні/волатильні поля (дати, phone_preview, профіль продавця тощо),
        щоб незмінене «по суті» оголошення давало той самий хеш.
        """
        # Текстовий опис, який дійсно впливає на зміст об'єкта
        description = (detail.get("description") or "").strip() or (search_data.get("raw_snippet") or "").strip()

        # Локація: тільки стабільні структуровані поля
        loc_struct = detail.get("location") or {}
        location_payload: Dict[str, Any] = {}
        if isinstance(loc_struct, dict):
            city = (loc_struct.get("city") or "").strip()
            region = (loc_struct.get("region") or "").strip()
            if city:
                location_payload["city"] = city
            if region:
                location_payload["region"] = region

        # Параметри — сортовані, лише label/value
        params = detail.get("parameters") or []
        params_payload = []
        if isinstance(params, list):
            for p in sorted(
                params,
                key=lambda x: (
                    (x.get("label") or "").strip(),
                    (x.get("value") or "").strip(),
                ),
            ):
                label = (p.get("label") or "").strip()
                value = (p.get("value") or "").strip()
                if not label and not value:
                    continue
                params_payload.append({"label": label, "value": value})

        return {
            "title": (search_data.get("title") or "").strip(),
            # Локація з деталі + зі сторінки пошуку (як текстовий топонім)
            "location": location_payload,
            "search_location": (search_data.get("location") or "").strip(),
            # Геометрія об'єкта
            "area_m2": search_data.get("area_m2"),
            # Ключовий текст оголошення
            "description": description,
            "parameters": params_payload,
        }

    def calculate_listing_hash(self, search_data: Dict[str, Any], detail: Dict[str, Any]) -> str:
        """
        Обчислює хеш оголошення OLX лише по ключових полях, релевантних для LLM.

        Використовується для визначення, чи потрібно повторно викликати LLM:
        якщо хеш не змінився, старий результат LLM можна безпечно перевикористати.
        """
        payload = self._build_key_payload(search_data, detail)
        return calculate_object_version_hash(payload)

    def extract_structured_data(
        self,
        search_data: Dict[str, Any],
        detail: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Застосовує LLM до оголошення OLX, повертає структуровані дані.
        Якщо LLM недоступний — повертає порожній dict.
        """
        if self.llm_service is None:
            return {}

        description_text = self._build_description_text(search_data, detail)
        if not description_text.strip():
            return {}

        # Кеш: якщо такий самий текст ми вже парсили — повертаємо з кешу
        cached = self.cache_service.get_cached_result(description_text)
        if cached is not None:
            return cached

        # Виклик LLM: використовуємо наявний parse_auction_description (нерухомість)
        try:
            result = self.llm_service.parse_auction_description(description_text)
        except Exception as e:
            print(f"[OlxLLMExtractor] Помилка при виклику LLM: {e}")
            return {}

        # Захист від типового зсуву масштабу площі землі (x10) при значеннях у "сотках".
        # Напр.: "38 соток" -> має бути 3800 м², а не 380 або 38000.
        if isinstance(result, dict):
            self._fix_land_area_scale_from_sotok(result, description_text)

        # Зберігаємо в кеш
        try:
            self.cache_service.save_result(description_text, result)
        except Exception as e:
            print(f"[OlxLLMExtractor] Помилка при збереженні в кеш: {e}")

        return result or {}

    @classmethod
    def _extract_sotok_area_sqm(cls, text: str) -> float:
        """Повертає сумарну площу у м² з фрагментів виду 'N соток'."""
        if not text:
            return 0.0
        total_sqm = 0.0
        for m in cls._SOTOK_PATTERN.finditer(text):
            raw = (m.group(1) or "").replace(" ", "").replace(",", ".").strip()
            if not raw:
                continue
            try:
                sotok_value = float(raw)
            except ValueError:
                continue
            # Відсікаємо шумові/нереалістичні значення.
            if sotok_value <= 0 or sotok_value > 100000:
                continue
            total_sqm += sotok_value * 100.0
        return total_sqm

    @staticmethod
    def _to_float(value: Any) -> float:
        if value in (None, ""):
            return 0.0
        try:
            return float(str(value).replace(" ", "").replace(",", "."))
        except (TypeError, ValueError):
            return 0.0

    def _fix_land_area_scale_from_sotok(self, llm_result: Dict[str, Any], description_text: str) -> None:
        expected_sqm = self._extract_sotok_area_sqm(description_text)
        if expected_sqm <= 0:
            return
        raw_land_sqm = self._to_float(llm_result.get("land_area_sqm"))
        if raw_land_sqm <= 0:
            raw_land_ha = self._to_float(llm_result.get("land_area_ha"))
            raw_land_sqm = raw_land_ha * 10000.0 if raw_land_ha > 0 else 0.0
        if raw_land_sqm <= 0:
            llm_result["land_area_sqm"] = expected_sqm
            llm_result["land_area_ha"] = expected_sqm / 10000.0
            return
        ratio = raw_land_sqm / expected_sqm if expected_sqm else 1.0
        # Типові промахи LLM по масштабу: /10 або *10 від значення з соток.
        if 0.09 <= ratio <= 0.11 or 9.0 <= ratio <= 11.0:
            logger.warning(
                "[OLX LLM] Скориговано площу землі за сотками: llm=%.2f sqm -> expected=%.2f sqm",
                raw_land_sqm,
                expected_sqm,
            )
            llm_result["land_area_sqm"] = expected_sqm
            llm_result["land_area_ha"] = expected_sqm / 10000.0

    @classmethod
    def has_cadastral_in_price_text(cls, search_data: Dict[str, Any]) -> bool:
        """Повертає True, якщо в price_text/price є формат кадастрового номера."""
        if not isinstance(search_data, dict):
            return False
        for key in ("price_text", "price"):
            raw = search_data.get(key)
            if not raw:
                continue
            text = str(raw)
            if cls._CADASTRAL_PATTERN.search(text):
                return True
        return False

    def recover_price_ignoring_cadastral(
        self,
        search_data: Dict[str, Any],
        detail: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Повторно витягує ціну з опису, якщо базове поле ціни схоже на кадастровий номер.
        """
        if self.llm_service is None:
            return {}

        description_text = self._build_description_text(search_data, detail)
        if not description_text.strip():
            return {}

        cache_key = self._PRICE_RECOVERY_CACHE_PREFIX + calculate_description_hash(description_text)
        cached = self.cache_service.repository.find_by_cache_key(cache_key)
        if cached and isinstance(cached.get("result"), dict):
            return cached.get("result") or {}

        try:
            result = self.llm_service.parse_olx_price_recovery(description_text) or {}
        except Exception as e:
            logger.warning("OLX price recovery LLM error: %s", e)
            return {}

        try:
            self.cache_service.repository.save_result_by_key(cache_key, result)
        except Exception as e:
            logger.warning("OLX price recovery cache save error: %s", e)

        return result

