# -*- coding: utf-8 -*-
"""
Сервіс для роботи з шаблонами звітів користувачів.
Генерація назви через LLM, забезпечення дефолтних шаблонів.
"""

import logging
from typing import Any, Dict, List, Optional

from config.settings import Settings
from data.repositories.report_template_repository import ReportTemplateRepository

logger = logging.getLogger(__name__)

# Ключі параметрів шаблону
PARAM_SOURCE = "source"
PARAM_DATE_FILTER = "date_filter"
PARAM_REGION = "region"
PARAM_CITY = "city"
PARAM_PROPERTY_TYPE = "property_type"
PARAM_PRICE = "price"
PARAM_PRICE_PER_HA = "price_per_ha"
PARAM_PRICE_PER_M2 = "price_per_m2"
PARAM_SORT_FIELD = "sort_field"
PARAM_SORT_ORDER = "sort_order"
PARAM_OUTPUT_FORMAT = "output_format"

# Валідні значення
SOURCES = ("", "olx", "prozorro")
DATE_FILTERS = (1, 7, 30)
PROPERTY_TYPES = ("", "neruhomist", "zemelna_dilyanka", "zemelna_dilyanka_z_neruhomistyu", "inshe")
OUTPUT_FORMATS = ("unified_table", "tabs_by_source")
CURRENCIES = ("uah", "usd")


def _default_params() -> Dict[str, Any]:
    """Параметри за замовчуванням для нового шаблону."""
    return {
        PARAM_SOURCE: "",
        PARAM_DATE_FILTER: 7,
        PARAM_REGION: None,
        PARAM_CITY: None,
        PARAM_PROPERTY_TYPE: "",
        PARAM_PRICE: None,
        PARAM_PRICE_PER_HA: None,
        PARAM_PRICE_PER_M2: None,
        PARAM_SORT_FIELD: "source_updated_at",
        PARAM_SORT_ORDER: "desc",
        PARAM_OUTPUT_FORMAT: "unified_table",
    }


def _ensure_default_templates(user_id: int, repo: ReportTemplateRepository) -> None:
    """
    Перевіряє наявність дефолтних шаблонів для користувача.
    Якщо їх немає — створює.
    """
    templates = repo.list_by_user(user_id)
    default_names = {"Звіт за добу", "Звіт за тиждень"}
    existing_names = {t.get("name") for t in templates if t.get("is_default")}
    missing = default_names - existing_names

    if "Звіт за добу" in missing:
        repo.create(
            user_id=user_id,
            name="Звіт за добу",
            params={
                **_default_params(),
                PARAM_SOURCE: "",
                PARAM_DATE_FILTER: 1,
                PARAM_OUTPUT_FORMAT: "tabs_by_source",
            },
            is_default=True,
        )
    if "Звіт за тиждень" in missing:
        repo.create(
            user_id=user_id,
            name="Звіт за тиждень",
            params={
                **_default_params(),
                PARAM_SOURCE: "",
                PARAM_DATE_FILTER: 7,
                PARAM_OUTPUT_FORMAT: "tabs_by_source",
            },
            is_default=True,
        )


class ReportTemplateService:
    """Сервіс для управління шаблонами звітів."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.repo = ReportTemplateRepository()
        self._llm_service = None

    @property
    def llm_service(self):
        """Леніва ініціалізація LLM для генерації назв."""
        if self._llm_service is None:
            try:
                from business.services.llm_service import LLMService
                self._llm_service = LLMService(self.settings)
            except Exception as e:
                logger.warning("LLMService недоступний для генерації назв: %s", e)
        return self._llm_service

    def list_templates(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Повертає список шаблонів користувача.
        Переконається, що є дефолтні шаблони.
        Дефолтні шаблони завжди використовують формат з вкладками по джерелах.
        """
        _ensure_default_templates(user_id, self.repo)
        templates = self.repo.list_by_user(user_id)
        for t in templates:
            if t.get("is_default") and t.get("params", {}).get(PARAM_OUTPUT_FORMAT) != "tabs_by_source":
                if "params" not in t:
                    t["params"] = {}
                t["params"][PARAM_OUTPUT_FORMAT] = "tabs_by_source"
        return templates

    def generate_template_name(self, params: Dict[str, Any]) -> str:
        """
        Генерує назву шаблону через LLM на основі параметрів.
        Якщо LLM недоступний — повертає просту назву.
        """
        if not self.llm_service:
            return self._fallback_template_name(params)

        prompt = self._build_name_prompt(params)
        try:
            raw = self.llm_service.generate_text(
                prompt,
                system_prompt="You help generate short report template names. Return only the name, no quotes, 6-8 words. The name must be in Ukrainian.",
                temperature=0.3,
            )
            if raw and raw.strip():
                name = raw.strip().strip('"\'')
                if len(name) > 100:
                    name = name[:97] + "..."
                return name
        except Exception as e:
            logger.warning("Помилка генерації назви через LLM: %s", e)
        return self._fallback_template_name(params)

    def _build_name_prompt(self, params: Dict[str, Any]) -> str:
        """Формує промпт для LLM."""
        parts = []
        source = params.get(PARAM_SOURCE) or "всі джерела"
        if source == "olx":
            parts.append("OLX")
        elif source == "prozorro":
            parts.append("ProZorro")
        else:
            parts.append("OLX + ProZorro")

        days = params.get(PARAM_DATE_FILTER, 7)
        if days == 1:
            parts.append("за добу")
        elif days == 7:
            parts.append("за тиждень")
        elif days == 30:
            parts.append("за 30 днів")

        region = params.get(PARAM_REGION)
        city = params.get(PARAM_CITY)
        if city:
            parts.append(f"м. {city}")
        elif region:
            parts.append(region)

        prop_type = params.get(PARAM_PROPERTY_TYPE)
        if prop_type == "neruhomist":
            parts.append("нерухомість")
        elif prop_type == "zemelna_dilyanka":
            parts.append("ЗД")
        elif prop_type == "zemelna_dilyanka_z_neruhomistyu":
            parts.append("ЗД з нерухомістю")

        return f"Generate a short report template name (max 6-8 words). Report parameters: {', '.join(parts)}. Return only the name, no quotes. The name must be in Ukrainian."

    def _fallback_template_name(self, params: Dict[str, Any]) -> str:
        """Формує просту назву без LLM."""
        days = params.get(PARAM_DATE_FILTER, 7)
        source = params.get(PARAM_SOURCE) or "всі"
        region = params.get(PARAM_REGION) or ""
        city = params.get(PARAM_CITY) or ""
        if days == 1:
            period = "Звіт за добу"
        elif days == 7:
            period = "Звіт за тиждень"
        else:
            period = f"Звіт за {days} днів"
        loc = city or region
        if loc:
            return f"{period} ({loc})"
        return f"{period} ({source})"

    def create_template(
        self,
        user_id: int,
        name: str,
        params: Dict[str, Any],
    ) -> str:
        """Створює шаблон звіту."""
        return self.repo.create(user_id=user_id, name=name, params=params, is_default=False)

    def delete_template(self, template_id: str, user_id: int) -> bool:
        """Видаляє шаблон (не для системних)."""
        return self.repo.delete(template_id, user_id)

    def reorder_templates(self, user_id: int, template_ids: List[str]) -> bool:
        """Змінює порядок шаблонів."""
        return self.repo.reorder(user_id, template_ids)

    def get_template(self, template_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """Отримує шаблон за ID."""
        return self.repo.get_by_id(template_id, user_id)
