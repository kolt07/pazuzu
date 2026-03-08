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

from typing import Dict, Any

from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService
from utils.hash_utils import calculate_object_version_hash


class OlxLLMExtractorService:
    """Сервіс, що застосовує LLM до сторінки оголошення OLX з кешуванням результатів."""

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

        # Зберігаємо в кеш
        try:
            self.cache_service.save_result(description_text, result)
        except Exception as e:
            print(f"[OlxLLMExtractor] Помилка при збереженні в кеш: {e}")

        return result or {}

