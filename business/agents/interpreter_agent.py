# -*- coding: utf-8 -*-
"""
Агент-інтерпретатор: розбір запитів, пояснення метрик, витягування структурованої
інформації з неструктурованого тексту (описи аукціонів/оголошень).

Двоступенева маршрутизація:
- Крок 1 (rule-based, без LLM): явні патерни (звіт/експорт за період, slash-команди,
  кнопки, Mini App) → мультиагентний пайплайн. Стабільна поведінка, легко дебажити.
- Крок 2 (LLM): лише коли не очевидно або користувач формулює «людською мовою» → один
  LangChain-агент з інструментами.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple

from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService

logger = logging.getLogger(__name__)

# Патерни для rule-based fast path (крок 1). Якщо збіг — йдемо в пайплайн без LLM.
EXPLICIT_REPORT_DAY_PATTERNS = [
    "звіт за добу", "звіт за день", "експорт за добу", "оголошення за добу",
    "за останню добу", "звіт за 1 день", "звіт за одну добу",
]
EXPLICIT_REPORT_WEEK_PATTERNS = [
    "звіт за тиждень", "експорт за тиждень", "звіт за 7 днів",
]
EXPLICIT_EXPORT_PATTERNS = [
    "експорт за", "виведи оголошення за", "виведи аукціони за",
    "оголошення за тиждень", "оголошення за добу", "аукціони за тиждень",
    "аукціони за добу", "експорт оголошень", "експорт аукціонів",
]
# Slash-команди та кнопки (точний збіг або початок)
SLASH_REPORT_DAY = ("/report_day", "/reportday", "/звіт_день", "/звітдень", "/report_1d")
SLASH_REPORT_WEEK = ("/report_week", "/reportweek", "/звіт_тиждень", "/звіттиждень", "/report_7d")
SLASH_EXPORT = ("/export", "/експорт", "/export_data")

# Mini App / кнопки можуть передавати явний intent через explicit_intent (див. interpret_user_query).


def _normalized_query(q: str) -> str:
    """Повертає рядок у нижньому регістрі, без зайвих пробілів."""
    return (q or "").strip().lower()


def _matches_any(q: str, patterns: List[str]) -> bool:
    return any(p in q for p in patterns)


def _matches_slash(q: str, commands: Tuple[str, ...]) -> bool:
    """Перевіряє, чи рядок є однією з slash-команд (точно або з пробілами після)."""
    q = _normalized_query(q)
    for cmd in commands:
        if q == cmd or q.startswith(cmd + " ") or q.startswith(cmd + "\n"):
            return True
    return False


def _infer_collections(q: str, intent: str) -> List[str]:
    """Визначає колекції за текстом для report/export."""
    if "олх" in q or "olx" in q:
        return ["olx_listings"]
    if "аукціон" in q or "prozorro" in q or "прозорро" in q:
        return ["prozorro_auctions"]
    return ["prozorro_auctions", "olx_listings"]


def _infer_period_days(q: str) -> Optional[int]:
    if "добу" in q or "день" in q or "1 день" in q or "1 днів" in q:
        return 1
    if "тиждень" in q or "7 днів" in q or "7 день" in q:
        return 7
    if "місяць" in q or "30 днів" in q:
        return 30
    return None


def _infer_region_filter(q: str) -> Optional[Dict[str, str]]:
    if any(x in q for x in ("по всім джерелам", "по всіх джерелах", "всі джерела", "всі регіони", "по всіх регіонах")):
        return None
    if any(x in q for x in ("києву", "києвській області", "київ", "київська", "київська область", "київ та область")):
        return {"region": "Київська", "city": "Київ"}
    return None


def _split_complex_query(original: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Грубо ділить складний запит на:
    - основну частину (що саме потрібно зробити)
    - умову/складну логіку (менше/більше за середню, тощо)
    
    Наприклад:
    "Створи звіт по Києву та області, вибравши нерухомість, ціна за квадратний метр якої менша за середню по цій локації"
    → ("Створи звіт по Києву та області, вибравши нерухомість", "ціна за квадратний метр якої менша за середню по цій локації")
    """
    if not original:
        return None, None
    q = _normalized_query(original)
    # Маркери, після яких зазвичай йде складна умова
    tokens = ["вибравши", "де", "для яких", "для котрих"]
    split_idx = -1
    for t in tokens:
        idx = q.find(t)
        if idx != -1:
            split_idx = idx
            break
    if split_idx == -1:
        return None, None
    # Визначаємо позицію в оригінальному рядку через довжину префікса
    prefix = q[:split_idx]
    orig_prefix = original[:len(prefix)]
    main_part = orig_prefix.rstrip(" ,")
    condition_part = original[len(orig_prefix):].lstrip(" ,")
    if not main_part or not condition_part:
        return None, None
    return main_part, condition_part


def _infer_property_type_filter(q: str) -> Optional[str]:
    """
    Визначає тип об'єкта (нерухомість / земля) з формулювання запиту.
    Повертає узагальнене значення, яке відповідає полю property_type у llm_cache.result.
    """
    # Нежитлова / комерційна нерухомість → загальний тип "Нерухомість"
    if "нежитлов" in q or "нежитлова" in q or "нежитлової" in q or "комерційн" in q:
        return "Нерухомість"
    # Земельні ділянки / земля під будівництво
    if "земельн" in q or "земля" in q or "ділянк" in q:
        return "Земля під будівництво"
    # Якщо просто згадується нерухомість без уточнень — це все ще може бути змішаним кейсом,
    # тому не звужуємо фільтр за замовчуванням.
    return None


def _build_structured(
    intent: str,
    raw_short: str,
    period_days: Optional[int],
    collections: List[str],
    region_filter: Optional[Dict[str, str]],
    need_update_first: bool,
    property_type_filter: Optional[str] = None,
    confidence: float = 1.0,
) -> Dict[str, Any]:
    """Збирає єдиний словник результату інтерпретатора. confidence: 0–1 (rule-based=1.0, llm/fallback — з моделі або дефолт)."""
    needs_data = intent in ("report_last_day", "report_last_week", "export_data")
    response_format = "files" if needs_data else None
    filters: Dict[str, Any] = {}
    if property_type_filter:
        # Узагальнений фільтр: планувальник уже знає, як мапити його на конкретні поля (наприклад llm_result.result.property_type)
        filters["property_type"] = property_type_filter
    # Валідація: тільки логічні поля, заборонено фізичні (address_refs.city, addresses.settlement)
    from domain.validators import validate_logical_filters
    if filters:
        validate_logical_filters(filters, context="InterpreterAgent._build_structured filters")
    if region_filter:
        validate_logical_filters(region_filter, context="InterpreterAgent._build_structured region_filter")
    return {
        "intent": intent,
        "raw": raw_short,
        "period_days": period_days,
        "collections": collections or ["prozorro_auctions", "olx_listings"],
        "filters": filters,
        "region_filter": region_filter,
        "property_type_filter": property_type_filter,
        "export_format": "xlsx",
        "need_update_first": need_update_first,
        "needs_data": needs_data,
        "response_format": response_format,
        "confidence": confidence,
    }


class InterpreterAgent:
    """
    Інтерпретатор у мультиагентному пайплайні: перетворює вільний запит у структурований намір.
    Двоступенева маршрутизація: спочатку rule-based fast path (без LLM), при невідповідності —
    крок 2 (LLM або fallback intent=query). Кнопки, slash-команди та Mini App можуть передавати
    explicit_intent для гарантованого пайплайну без інтерпретації тексту.
    Гайдбук формату даних: docs/interpreter_agent_handbook.md.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm_service: Optional[LLMService] = None
        self.cache_service = LLMCacheService()

    @property
    def llm_service(self) -> Optional[LLMService]:
        if self._llm_service is None:
            try:
                self._llm_service = LLMService(self.settings)
            except Exception as e:
                logger.warning("InterpreterAgent: LLM недоступний: %s", e)
        return self._llm_service

    def try_rule_based_routing(
        self,
        user_query: str,
        explicit_intent: Optional[str] = None,
        explicit_params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Крок 1 маршрутизації: rule-based fast path без LLM.
        Повертає структурований намір лише коли є явний збіг (звіт/експорт за період,
        slash-команди, кнопки, explicit_intent). Інакше — None (потрібен крок 2, LLM).

        Returns:
            Словник з полями intent, period_days, collections, ... або None.
        """
        q = _normalized_query(user_query)
        raw_short = (user_query or "")[:500]
        explicit_params = explicit_params or {}
        # Єдиний блок для витягування базових сутностей (період, регіон, тип об'єкта),
        # щоб однаково працювати як для текстових запитів, так і для explicit_intent.
        inferred_period = _infer_period_days(q)
        inferred_region = _infer_region_filter(q)
        inferred_property_type = _infer_property_type_filter(q)

        # Явний intent з клієнта (кнопка, slash, Mini App)
        if explicit_intent in ("report_last_day", "report_last_week", "export_data"):
            intent = explicit_intent
            period_days = explicit_params.get("period_days")
            if period_days is None and intent == "report_last_day":
                period_days = 1
            elif period_days is None and intent == "report_last_week":
                period_days = 7
            elif period_days is None and intent == "export_data":
                period_days = explicit_params.get("period_days", inferred_period or 7)
            collections = explicit_params.get("collections") or ["prozorro_auctions", "olx_listings"]
            region_filter = explicit_params.get("region_filter") or inferred_region
            property_type_filter = explicit_params.get("property_type") or inferred_property_type
            need_update_first = explicit_params.get("need_update_first", False)
            return _build_structured(
                intent,
                raw_short,
                period_days,
                collections,
                region_filter,
                need_update_first,
                property_type_filter=property_type_filter,
            )

        # Slash-команди
        if _matches_slash(user_query, SLASH_REPORT_DAY):
            collections = _infer_collections(q, "report_last_day")
            return _build_structured(
                "report_last_day",
                raw_short,
                1,
                collections,
                _infer_region_filter(q),
                "спочатку онови" in q or "онов дані" in q,
                property_type_filter=_infer_property_type_filter(q),
            )
        if _matches_slash(user_query, SLASH_REPORT_WEEK):
            collections = _infer_collections(q, "report_last_week")
            return _build_structured(
                "report_last_week",
                raw_short,
                7,
                collections,
                _infer_region_filter(q),
                "спочатку онови" in q or "онов дані" in q,
                property_type_filter=_infer_property_type_filter(q),
            )
        if _matches_slash(user_query, SLASH_EXPORT):
            collections = _infer_collections(q, "export_data")
            period = inferred_period or 7
            return _build_structured(
                "export_data",
                raw_short,
                period,
                collections,
                _infer_region_filter(q),
                "спочатку онови" in q or "онов дані" in q,
                property_type_filter=_infer_property_type_filter(q),
            )

        # Текстові патерни: звіт за добу / тиждень
        if _matches_any(q, EXPLICIT_REPORT_DAY_PATTERNS):
            collections = _infer_collections(q, "report_last_day")
            region_filter = inferred_region
            property_type_filter = inferred_property_type
            need_update_first = any(x in q for x in ("спочатку онови", "онов дані", "перезавантаж дані", "оновлення даних"))
            return _build_structured(
                "report_last_day",
                raw_short,
                1,
                collections,
                region_filter,
                need_update_first,
                property_type_filter=property_type_filter,
            )
        if _matches_any(q, EXPLICIT_REPORT_WEEK_PATTERNS):
            collections = _infer_collections(q, "report_last_week")
            region_filter = inferred_region
            property_type_filter = inferred_property_type
            need_update_first = any(x in q for x in ("спочатку онови", "онов дані", "перезавантаж дані", "оновлення даних"))
            return _build_structured(
                "report_last_week",
                raw_short,
                7,
                collections,
                region_filter,
                need_update_first,
                property_type_filter=property_type_filter,
            )

        # Експорт за період
        if _matches_any(q, EXPLICIT_EXPORT_PATTERNS):
            collections = _infer_collections(q, "export_data")
            period_days = inferred_period or 7
            region_filter = inferred_region
            property_type_filter = inferred_property_type
            need_update_first = any(x in q for x in ("спочатку онови", "онов дані", "перезавантаж дані", "оновлення даних"))
            return _build_structured(
                "export_data",
                raw_short,
                period_days,
                collections,
                region_filter,
                need_update_first,
                property_type_filter=property_type_filter,
            )

        # Немає явного патерну → крок 2 (LLM)
        return None

    def explain_metric(self, metric_id: str) -> str:
        """
        Повертає коротке пояснення метрики аналітики за ідентифікатором.
        """
        from utils.analytics_metrics import AnalyticsMetrics
        metrics = AnalyticsMetrics.list_metrics()
        for m in metrics:
            if isinstance(m, dict) and m.get("id") == metric_id:
                return m.get("description", m.get("name", metric_id))
            if isinstance(m, str) and m == metric_id:
                return f"Метрика: {metric_id}"
        return f"Метрика «{metric_id}»: агрегований показник з аналітики ProZorro/OLX."

    def extract_structured_from_text(self, text: str) -> Dict[str, Any]:
        """
        Витягує структуровані дані з неструктурованого опису (аукціон або оголошення).
        Використовує LLM + кеш (те саме, що parse_auction_description для ProZorro/OLX).
        """
        if not text or not text.strip():
            return {}
        cached = self.cache_service.get_cached_result(text)
        if cached is not None:
            return cached
        if not self.llm_service:
            return {}
        try:
            result = self.llm_service.parse_auction_description(text)
            self.cache_service.save_result(text, result)
            return result
        except Exception as e:
            logger.exception("extract_structured_from_text: %s", e)
            return {}


def _fix_region_for_explicit_result(q: str, result: Dict[str, Any]) -> None:
    """Доповнює region_filter з тексту, якщо прийшло з explicit_intent без region_filter."""
    if result.get("intent") not in ("report_last_day", "report_last_week", "export_data"):
        return
    if result.get("region_filter") is None:
        result["region_filter"] = _infer_region_filter(q)


def interpret_user_query(
    self,
    user_query: str,
    context: Optional[str] = None,
    explicit_intent: Optional[str] = None,
    explicit_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Двоступенева маршрутизація: спочатку rule-based (без LLM), при відсутності збігу — LLM/fallback.
    Повертає структурований намір з полем routing_path: "rule_based" | "llm".

    explicit_intent: заданий клієнтом (кнопка, Mini App) — report_last_day | report_last_week | export_data.
    explicit_params: опційні period_days, collections, region_filter, need_update_first.
    """
    # Крок 1: rule-based fast path
    fast = self.try_rule_based_routing(user_query, explicit_intent, explicit_params)
    if fast is not None:
        q = _normalized_query(user_query)
        _fix_region_for_explicit_result(q, fast)
        fast["routing_path"] = "rule_based"
        return fast

    # Крок 2a: складна умова («менша/більша за середню» тощо) → рекурсивний розбір основної частини
    q = _normalized_query(user_query)
    has_complex_comparison = (
        "менш" in q and "середн" in q
    ) or (
        "нижче" in q and "середн" in q
    ) or (
        "вище" in q and "середн" in q
    )
    if has_complex_comparison:
        main_part, condition_part = _split_complex_query(user_query)
        if main_part and condition_part and main_part.strip().lower() != q:
            # Рекурсивно інтерпретуємо «базовий» запит (без складної умови)
            base_structured = self.interpret_user_query(
                main_part,
                context=context,
                explicit_intent=explicit_intent,
                explicit_params=explicit_params,
            )
            # Позначаємо, що до базового наміру додається складна умова, яку оброблятиме Planner/LLM
            base_structured["has_complex_condition"] = True
            base_structured["complex_condition_raw"] = condition_part
            # Для таких кейсів усе одно використовуємо шлях "llm" (складна аналітика/порівняння)
            base_structured["routing_path"] = base_structured.get("routing_path", "llm")
            return base_structured

    # Крок 2b: LLM Intent Extractor (без tools, тільки JSON) або fallback
    result = self._interpret_fallback(user_query, context)
    if self.llm_service and hasattr(self.llm_service, "extract_intent_for_routing"):
        try:
            llm_out = self.llm_service.extract_intent_for_routing(user_query, context)
            result["intent"] = llm_out.get("intent", "query")
            result["confidence"] = llm_out.get("confidence", 0.5)
            if llm_out.get("analysis_intent"):
                result["analysis_intent"] = llm_out["analysis_intent"]
        except Exception as e:
            logger.debug("LLM Intent Extractor: %s", e)
    result["routing_path"] = "llm"
    return result


def _interpret_fallback(self, user_query: str, context: Optional[str]) -> Dict[str, Any]:
    """Крок 2: коли rule-based не збігся. Fallback intent=query, confidence=0.5; далі може бути перезаписано LLM Intent Extractor."""
    raw_short = (user_query or "")[:500]
    return _build_structured(
        "query", raw_short, None, ["prozorro_auctions", "olx_listings"],
        None,
        False,
        property_type_filter=None,
        confidence=0.5,
    )


# Прив'язка методів до класу (вони визначені як функції з self для читання)
InterpreterAgent.interpret_user_query = interpret_user_query
InterpreterAgent._interpret_fallback = _interpret_fallback
