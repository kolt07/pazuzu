# -*- coding: utf-8 -*-
"""
Агент визначення структурних елементів запиту.
Аналізує запит користувача та визначає джерела, метрики, фільтри, сортування тощо.
"""

import logging
import json
from typing import Dict, Any, Optional, List
from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.app_metadata_service import AppMetadataService

logger = logging.getLogger(__name__)


class QueryStructureAgent:
    """
    Агент для визначення структурних елементів запиту.
    Аналізує запит користувача та визначає:
    - Джерела даних (колекції)
    - Метрики, що цікавлять у відповіді
    - Метрики для фільтрації
    - Метрики для сортування
    - Метрики для поєднання даних з джерел
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm_service: Optional[LLMService] = None
        self.metadata_service = AppMetadataService(settings)
    
    @property
    def llm_service(self) -> Optional[LLMService]:
        """Лінива ініціалізація LLM сервісу."""
        if self._llm_service is None:
            try:
                self._llm_service = LLMService(self.settings)
            except Exception as e:
                logger.warning("QueryStructureAgent: LLM недоступний: %s", e)
        return self._llm_service
    
    def analyze_query_structure(
        self,
        user_query: str,
        intent_info: Dict[str, Any],
        context: Optional[str] = None,
        listing_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Аналізує запит та визначає структурні елементи.
        
        Args:
            user_query: Запит користувача
            intent_info: Результат від IntentDetectorAgent (intent, response_format, тощо)
            context: Додатковий контекст (опціонально)
        
        Returns:
            Словник з полями:
            - sources: список колекцій (["prozorro_auctions", "olx_listings"])
            - response_metrics: метрики для відповіді (список рядків або об'єктів)
            - filter_metrics: метрики для фільтрації (словник з умовами)
            - sort_metrics: метрики для сортування (список об'єктів {field, order})
            - join_metrics: метрики для поєднання даних (опціонально)
            - aggregation_needed: чи потрібна агрегація (bool)
            - limit: обмеження кількості результатів (int, опціонально)
        """
        if not self.llm_service:
            return self._fallback_structure(user_query, intent_info)
        
        try:
            # Формуємо промпт для аналізу структури
            prompt = self._build_structure_analysis_prompt(
                user_query, intent_info, context, listing_context=listing_context
            )
            
            # Викликаємо LLM
            response_text = self.llm_service.generate_text(
                prompt=prompt,
                system_prompt=None,
                temperature=0.2  # Низька температура для більш детермінованих результатів
            )
            
            # Парсимо відповідь
            result = self._parse_llm_response(response_text)
            
            # Валідуємо та нормалізуємо результат
            result = self._validate_and_normalize(
                result, user_query, intent_info, listing_context=listing_context
            )
            
            return result
            
        except Exception as e:
            logger.exception("Помилка аналізу структури запиту: %s", e)
            return self._fallback_structure(user_query, intent_info)
    
    def _build_structure_analysis_prompt(
        self,
        user_query: str,
        intent_info: Dict[str, Any],
        context: Optional[str] = None,
        listing_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Формує промпт для аналізу структури запиту."""
        metadata_summary = self.metadata_service.get_metadata_for_llm(max_length=2000)
        intent_info_str = json.dumps(intent_info, ensure_ascii=False, indent=2)
        context_block = ("\n\n## Додатковий контекст:\n" + context) if context else ""
        listing_context_block = ""
        if listing_context and isinstance(listing_context, dict):
            listing_context_block = "\n\n## ВАЖЛИВО — аналіз конкретного оголошення:\nКористувач запитує про КОНКРЕТНЕ оголошення (є посилання в контексті).\nНЕ додавай filter_metrics.property_type — оголошення може мати тип 'інше' або інший.\nВикористовуй лише region/city з контексту для порівняння з іншими оголошеннями в цьому регіоні."
        try:
            from config.config_loader import get_config_loader
            template = get_config_loader().get_prompt("query_structure")
            if template:
                return template.format(
                    metadata_summary=metadata_summary,
                    user_query=user_query,
                    intent_info=intent_info_str,
                    context_block=context_block,
                    listing_context_block=listing_context_block,
                )
        except Exception:
            pass
        # Fallback — збираємо з частин (legacy)
        prompt_parts = [
            "Проаналізуй запит користувача та визнач структурні елементи для вибірки даних.",
            "",
            "## Контекст застосунку:",
            metadata_summary,
            "",
            "## Запит користувача:",
            user_query,
            "",
            "## Визначений намір:",
            intent_info_str,
        ]
        if context:
            prompt_parts.extend([
                "",
                "## Додатковий контекст:",
                context
            ])
        if listing_context and isinstance(listing_context, dict):
            prompt_parts.extend([
                "",
                "## ВАЖЛИВО — аналіз конкретного оголошення:",
                "Користувач запитує про КОНКРЕТНЕ оголошення (є посилання в контексті).",
                "НЕ додавай filter_metrics.property_type — оголошення може мати тип 'інше' або інший.",
                "Використовуй лише region/city з контексту для порівняння з іншими оголошеннями в цьому регіоні.",
            ])
        prompt_parts.extend([
            "",
            "## Завдання:",
            "Визнач структурні елементи запиту:",
            "",
            "1. **Джерела даних (sources)**: ОБОВ'ЯЗКОВО unified_listings.",
            "   Пайплайн підтримує ТІЛЬКИ unified_listings (зведена таблиця з OLX та ProZorro).",
            "   olx_listings та prozorro_auctions як sources — ЗАБОРОНЕНО для пайплайну.",
            "   Щоб отримати лише OLX: sources=['unified_listings'], filter_metrics: {source: 'olx'}.",
            "   Щоб отримати лише ProZorro: filter_metrics: {source: 'prozorro'}.",
            "",
            "2. **Метрики для відповіді (response_metrics)**: які поля/метрики потрібні у відповіді?",
            "   Приклади: price, area, region, city, date, property_type, bids_count тощо",
            "",
            "3. **Метрики для фільтрації (filter_metrics)**: які умови фільтрації?",
            "   Формат: {field: {operator: value}}. ТІЛЬКИ логічні поля: city, region, price, date, status, source, property_type, area.",
            "   Заборонено: address_refs, addresses.settlement, будь-які шляхи з крапкою.",
            "   Для ВІДНОСНИХ періодів (останній тиждень, за 7 днів, за місяць) — використовуй date_range:",
            "   {period: 'last_7_days'} або {type: 'relative', value: 'last_week'}.",
            "   Заборонено шаблони: {{LAST_WEEK_START_DATE}}, {{TODAY}} тощо.",
            "   Для абсолютних дат: {date: {gte: '2026-02-01'}}. Приклади: {region: 'Київська'}, {price: {lt: 1000000}}",
            "   ВАЖЛИВО для географії:",
            "   - 'Київської області', 'Київська область' — тільки region: 'Київська', БЕЗ city.",
            "   - 'Київ та область', 'в Києві та області' — OR: додай {region: 'Київська', city: 'Київ'}.",
            "   Система обробить city+region як OR (місто або область).",
            "   ВАЖЛИВО для property_type: якщо користувач каже 'нерухомість', 'найдорожча нерухомість', 'оголошення про нерухомість' —",
            "   це стосується лише ОБ'ЄКТІВ НЕРУХОМОСТІ (будівлі, приміщення), а НЕ земельних ділянок.",
            "   Додай filter_metrics: {property_type: ['Комерційна нерухомість', 'Земельна ділянка з нерухомістю']},",
            "   щоб виключити тип 'Земельна ділянка' (лише земля без будівель).",
            "   Якщо користувач явно каже 'земля', 'ділянка', 'земельна ділянка' — НЕ додавай цей фільтр.",
            "",
            "4. **Метрики для сортування (sort_metrics)**: як сортувати результати?",
            "   Формат: [{field: 'price', order: 'desc'}, {field: 'date', order: 'asc'}]",
            "",
            "5. **Метрики для поєднання (join_metrics)**: чи потрібно об'єднати дані з різних джерел?",
            "   Опціонально, якщо потрібно порівняти ProZorro та OLX",
            "",
            "6. **Агрегація (aggregation_needed, aggregation_group_by, aggregation_metrics)**:",
            "   Якщо запит про «середня ціна по областях», «найдорожчий регіон», «ціна за кв.м. по регіонах» —",
            "   aggregation_needed: true, aggregation_group_by: ['region'],",
            "   aggregation_metrics: [{field: 'price_per_m2_uah', aggregation: 'avg'}].",
            "   aggregation: avg|sum|min|max|count.",
            "",
            "7. **Обмеження (limit)**: максимальна кількість результатів (якщо вказано топ-N, то N)",
            "",
            "Поверни результат у форматі JSON:",
            "{",
            '  "sources": ["unified_listings"],',
            '  "response_metrics": ["price", "area", "region"],',
            '  "filter_metrics": {"region": "Київська"},',
            '  "date_range": {"period": "last_7_days"},',
            '  "sort_metrics": [{"field": "price", "order": "desc"}],',
            '  "join_metrics": null,',
            '  "aggregation_needed": false,',
            '  "aggregation_group_by": [],',
            '  "aggregation_metrics": [],',
            '  "limit": 10',
            "}"
        ])
        
        return "\n".join(prompt_parts)
    
    def _parse_llm_response(self, response_text: str) -> Dict[str, Any]:
        """Парсить відповідь LLM у структурований формат."""
        response_text = response_text.strip()
        
        # Шукаємо JSON блок
        start_idx = response_text.find("{")
        end_idx = response_text.rfind("}")
        
        if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
            logger.warning("Не знайдено JSON у відповіді LLM: %s", response_text[:200])
            return {}
        
        json_text = response_text[start_idx:end_idx + 1]
        
        try:
            result = json.loads(json_text)
        except json.JSONDecodeError as e:
            logger.warning("Помилка парсингу JSON: %s. Текст: %s", e, json_text[:200])
            return {}
        
        return result
    
    def _validate_and_normalize(
        self,
        result: Dict[str, Any],
        user_query: str,
        intent_info: Dict[str, Any],
        listing_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Валідує та нормалізує результат."""
        # Джерела даних — пайплайн підтримує тільки unified_listings
        if "sources" not in result:
            result["sources"] = ["unified_listings"]
        else:
            valid_sources = ["unified_listings", "prozorro_auctions", "olx_listings"]
            result["sources"] = [s for s in result["sources"] if s in valid_sources]
            if not result["sources"]:
                result["sources"] = ["unified_listings"]
            if result["sources"] != ["unified_listings"]:
                result["sources"] = ["unified_listings"]
        
        # Метрики для відповіді
        if "response_metrics" not in result:
            result["response_metrics"] = []
        
        # Метрики для фільтрації
        if "filter_metrics" not in result:
            result["filter_metrics"] = {}
        result["filter_metrics"] = self._normalize_filters(
            result["filter_metrics"], user_query, listing_context=listing_context
        )
        
        # Метрики для сортування
        if "sort_metrics" not in result:
            result["sort_metrics"] = []
        result["sort_metrics"] = self._normalize_sorts(result["sort_metrics"], user_query)
        
        # Метрики для поєднання
        if "join_metrics" not in result:
            result["join_metrics"] = None
        
        # Агрегація
        if "aggregation_needed" not in result:
            result["aggregation_needed"] = False
        if "aggregation_group_by" not in result:
            result["aggregation_group_by"] = []
        if "aggregation_metrics" not in result:
            result["aggregation_metrics"] = []
        
        # Обмеження
        if "limit" not in result:
            result["limit"] = self._infer_limit(user_query)
        else:
            try:
                limit = int(result["limit"])
                result["limit"] = max(1, min(limit, 5000)) if limit > 0 else None
            except (ValueError, TypeError):
                result["limit"] = self._infer_limit(user_query)
        
        return result
    
    def _infer_sources(self, user_query: str) -> List[str]:
        """Визначає джерела даних. Пайплайн підтримує тільки unified_listings."""
        return ["unified_listings"]
    
    # Типи оголошення: нерухомість (будівлі, приміщення) — без чистих земельних ділянок
    PROPERTY_TYPE_NERUKHOMIST = ["Комерційна нерухомість", "Земельна ділянка з нерухомістю"]

    def _contains_template(self, value: Any) -> bool:
        """Перевіряє, чи значення містить непідставлений шаблон {{...}}."""
        if isinstance(value, str):
            return "{{" in value and "}}" in value
        if isinstance(value, dict):
            return any(self._contains_template(v) for v in value.values())
        return False

    def _normalize_filters(
        self,
        filters: Dict[str, Any],
        user_query: str,
        listing_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Нормалізує фільтри."""
        normalized = {}
        query_lower = user_query.lower()
        has_listing_context = bool(listing_context and isinstance(listing_context, dict))
        
        # Регіон — з запиту або з контексту оголошення
        if "region" not in filters:
            region = self._extract_region(user_query)
            if not region and has_listing_context and listing_context.get("summary"):
                region = self._extract_region(str(listing_context["summary"]))
            if region:
                normalized["region"] = region
        
        # Місто — з запиту або з контексту оголошення
        if "city" not in filters:
            city = self._extract_city(user_query)
            if not city and has_listing_context and listing_context.get("summary"):
                city = self._extract_city(str(listing_context["summary"]))
            if city:
                normalized["city"] = city
        
        # Дата / date_range: якщо LLM повернув шаблон {{...}} — ігноруємо й використовуємо rule-based
        filters_copy = dict(filters)
        date_val = filters_copy.pop("date", None)
        date_range_val = filters_copy.pop("date_range", None)
        effective_date = date_val or date_range_val
        if effective_date and self._contains_template(effective_date):
            effective_date = self._extract_date_range(user_query)
        if effective_date is None:
            effective_date = self._extract_date_range(user_query)
        if effective_date:
            normalized["date"] = self._normalize_date_range(effective_date)

        # Тип оголошення: якщо "нерухомість" без "земля"/"ділянка" — лише об'єкти нерухомості.
        # При аналізі конкретного оголошення (listing_context) — НЕ додаємо property_type,
        # щоб не виключити оголошення з типом "інше" або іншим.
        if "property_type" not in filters_copy and not has_listing_context:
            property_type = self._extract_property_type(user_query)
            if property_type:
                normalized["property_type"] = property_type
        
        # Додаємо явні фільтри з результату LLM (без date, вже оброблено)
        normalized.update(filters_copy)
        # При аналізі конкретного оголошення — прибираємо property_type з LLM,
        # щоб не виключити оголошення з типом "інше"
        if has_listing_context and "property_type" in normalized:
            normalized.pop("property_type", None)
        
        # Київської області / Київська область — тільки region, не city
        if "city" in normalized and "region" in normalized:
            r = str(normalized.get("region", "")).lower()
            q = query_lower
            if ("київськ" in q or "київської" in q) and ("област" in q or "обл" in q):
                if r in ("київська", "київська область"):
                    normalized.pop("city", None)

        # Нормалізація топонімів до формату в БД (Волинській області → Волинська)
        if "region" in normalized and normalized["region"]:
            from utils.toponym_normalizer import normalize_region
            n = normalize_region(normalized["region"])
            if n:
                normalized["region"] = n
        if "city" in normalized and normalized["city"]:
            from utils.toponym_normalizer import normalize_settlement
            n = normalize_settlement(normalized["city"])
            if n:
                normalized["city"] = n
        
        # Валідація: тільки логічні поля, заборонено фізичні (address_refs.city, addresses.settlement)
        from domain.validators import validate_logical_filters
        validate_logical_filters(normalized, context="QueryStructureAgent._normalize_filters")
        
        return normalized
    
    def _normalize_sorts(self, sorts: List[Dict[str, Any]], user_query: str) -> List[Dict[str, Any]]:
        """Нормалізує сортування."""
        if sorts is None:
            sorts = []
        query_lower = user_query.lower()
        
        # Якщо немає сортування, але є топ-N або найдорожче/найдешевше
        if not sorts:
            if any(word in query_lower for word in ["найдорожч", "найвищ", "топ"]):
                sorts.append({"field": "price", "order": "desc"})
            elif any(word in query_lower for word in ["найдешевш", "найнижч"]):
                sorts.append({"field": "price", "order": "asc"})
        
        # Валідуємо кожне сортування
        validated_sorts = []
        for sort in sorts:
            if isinstance(sort, dict) and "field" in sort:
                order = sort.get("order", "asc")
                if order not in ["asc", "desc"]:
                    order = "asc"
                validated_sorts.append({"field": sort["field"], "order": order})
        
        return validated_sorts
    
    def _extract_property_type(self, user_query: str) -> Optional[List[str]]:
        """
        Витягує фільтр за типом оголошення.
        Якщо в запиті є 'нерухомість' без 'земля'/'ділянка' — лише об'єкти нерухомості
        (Комерційна нерухомість, Земельна ділянка з нерухомістю), виключаючи чисту землю.
        """
        query_lower = user_query.lower()
        has_nerukhomist = "нерухомість" in query_lower
        has_land = any(w in query_lower for w in ["земл", "ділянк", "земельн"])
        if has_nerukhomist and not has_land:
            return self.PROPERTY_TYPE_NERUKHOMIST
        return None

    def _extract_region(self, user_query: str) -> Optional[str]:
        """Витягує регіон з запиту."""
        # Проста логіка - можна покращити
        query_lower = user_query.lower()
        if "київськ" in query_lower:
            return "Київська"
        elif "львівськ" in query_lower:
            return "Львівська"
        # Додати інші регіони за потреби
        return None
    
    def _extract_city(self, user_query: str) -> Optional[str]:
        """Витягує місто з запиту."""
        query_lower = user_query.lower()
        if "києв" in query_lower or "київ" in query_lower:
            return "Київ"
        elif "львів" in query_lower:
            return "Львів"
        # Додати інші міста за потреби
        return None
    
    def _extract_date_range(self, user_query: str) -> Optional[Dict[str, Any]]:
        """Витягує діапазон дат з запиту."""
        query_lower = user_query.lower()
        # Проста логіка - можна покращити
        if "за останню добу" in query_lower or "за день" in query_lower:
            return {"period": "last_1_day"}
        elif "за тиждень" in query_lower or "за 7 днів" in query_lower or "останній тиждень" in query_lower:
            return {"period": "last_7_days"}
        elif "за місяць" in query_lower or "за 30 днів" in query_lower or "останній місяць" in query_lower:
            return {"period": "last_30_days"}
        return None

    def _normalize_date_range(self, date_val: Dict[str, Any]) -> Dict[str, str]:
        """Нормалізує date_range до єдиного формату {period: "last_7_days"}."""
        if "period" in date_val:
            return {"period": date_val["period"]}
        if date_val.get("type") == "relative" and "value" in date_val:
            value = date_val["value"]
            period_map = {"last_week": "last_7_days", "last_month": "last_30_days", "last_1_day": "last_1_day"}
            return {"period": period_map.get(value, "last_7_days")}
        return {"period": "last_7_days"}
    
    def _infer_limit(self, user_query: str) -> Optional[int]:
        """Визначає обмеження кількості результатів."""
        import re
        query_lower = user_query.lower()
        
        # Шукаємо топ-N
        top_match = re.search(r'топ[-\s]?(\d+)', query_lower)
        if top_match:
            return int(top_match.group(1))
        
        # Шукаємо "перші N" або "N най..."
        first_match = re.search(r'(?:перші|перших|най)[\s-]?(\d+)', query_lower)
        if first_match:
            return int(first_match.group(1))
        
        return None
    
    def _fallback_structure(
        self,
        user_query: str,
        intent_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fallback визначення структури без LLM."""
        return {
            "sources": self._infer_sources(user_query),  # За замовчуванням unified_listings
            "response_metrics": ["price", "region", "date"],
            "filter_metrics": self._normalize_filters({}, user_query),
            "sort_metrics": self._normalize_sorts([], user_query),
            "join_metrics": None,
            "aggregation_needed": False,
            "limit": self._infer_limit(user_query)
        }
