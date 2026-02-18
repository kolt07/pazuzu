# -*- coding: utf-8 -*-
"""
Агент визначення наміру та формату відповіді.
Визначає загальний намір користувача та попередній формат відповіді.
"""

import logging
import json
from typing import Dict, Any, Optional
from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.app_metadata_service import AppMetadataService

logger = logging.getLogger(__name__)

# Формати відповіді
RESPONSE_FORMAT_TEXT_ANSWER = "text_answer"
RESPONSE_FORMAT_DATA_EXPORT = "data_export"
RESPONSE_FORMAT_ANALYTICAL_TEXT = "analytical_text"
RESPONSE_FORMAT_GEO_ASSESSMENT = "geo_assessment"
RESPONSE_FORMAT_OUT_OF_SCOPE = "out_of_scope"


class IntentDetectorAgent:
    """
    Агент для визначення наміру користувача та формату відповіді.
    Використовує LLM для аналізу запиту та визначення:
    - Загального наміру користувача
    - Попереднього формату відповіді
    - Шаблону відповіді
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
                logger.warning("IntentDetectorAgent: LLM недоступний: %s", e)
        return self._llm_service
    
    def detect_intent_and_format(
        self,
        user_query: str,
        context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Визначає намір користувача та формат відповіді.
        
        Args:
            user_query: Запит користувача
            context: Додатковий контекст (опціонально)
        
        Returns:
            Словник з полями:
            - intent: загальний намір (string)
            - response_format: формат відповіді (text_answer | data_export | analytical_text | out_of_scope)
            - response_template: шаблон відповіді (string, опціонально)
            - confidence: впевненість (float, 0-1)
            - reasoning: пояснення вибору (string, опціонально)
        """
        if not self.llm_service:
            # Fallback без LLM
            return self._fallback_detection(user_query)
        
        try:
            # Формуємо промпт для визначення наміру
            prompt = self._build_intent_detection_prompt(user_query, context)
            
            # Викликаємо LLM
            response_text = self.llm_service.generate_text(
                prompt=prompt,
                system_prompt=None,
                temperature=0.3  # Низька температура для більш детермінованих результатів
            )
            
            # Парсимо відповідь
            result = self._parse_llm_response(response_text)
            
            # Валідуємо результат
            result = self._validate_result(result, user_query)
            
            return result
            
        except Exception as e:
            logger.exception("Помилка визначення наміру: %s", e)
            return self._fallback_detection(user_query)
    
    def _build_intent_detection_prompt(self, user_query: str, context: Optional[str] = None) -> str:
        """Формує промпт для визначення наміру."""
        metadata_summary = self.metadata_service.get_metadata_for_llm(max_length=1500)
        context_block = ""
        if context:
            context_block = "\n\n## Додатковий контекст (для розуміння контексту розмови):\nВАЖЛИВО: Якщо в контексті є відповіді асистента типу 'неможливо визначити', 'даних недостатньо' — це НЕ підстава для out_of_scope. Такі запити стосуються даних системи.\n" + context
        try:
            from config.config_loader import get_config_loader
            template = get_config_loader().get_prompt("intent_detection")
            if template:
                return template.format(
                    metadata_summary=metadata_summary,
                    user_query=user_query,
                    context_block=context_block,
                )
        except Exception:
            pass
        # Fallback — збираємо з частин (legacy)
        prompt_parts = [
            "Проаналізуй запит користувача та визнач його намір та очікуваний формат відповіді.",
            "",
            "## Контекст застосунку:",
            metadata_summary,
            "",
            "## Запит користувача:",
            user_query,
        ]
        if context:
            prompt_parts.extend([
                "",
                "## Додатковий контекст (для розуміння контексту розмови):",
                "ВАЖЛИВО: Якщо в контексті є відповіді асистента типу 'неможливо визначити', 'даних недостатньо' — це НЕ підстава для out_of_scope. Такі запити стосуються даних системи.",
                context
            ])
        prompt_parts.extend([
            "",
            "## Завдання:",
            "Визнач:",
            "1. Загальний намір користувача (що він хоче отримати)",
            "2. Формат відповіді з наступних варіантів:",
            f"   - {RESPONSE_FORMAT_TEXT_ANSWER}: текстова відповідь на конкретне питання з посиланнями та цифрами",
            f"   - {RESPONSE_FORMAT_DATA_EXPORT}: вибірка даних у файл (Excel) - для великих вибірок, звітів за період",
            f"   - {RESPONSE_FORMAT_ANALYTICAL_TEXT}: аналітичний текст з висновками та аргументацією",
            f"   - {RESPONSE_FORMAT_GEO_ASSESSMENT}: оцінка придатності приміщення для виду діяльності (аптека, кафе, клініка) — гео-аналіз оточення",
            f"   - {RESPONSE_FORMAT_OUT_OF_SCOPE}: запит не стосується функціональності системи",
            "",
            "## Правила вибору формату:",
            f"- {RESPONSE_FORMAT_TEXT_ANSWER}: питання типу 'знайди найдорожчу нерухомість', 'коли востанне оновлено дані', 'топ-10 оголошень'",
            f"- {RESPONSE_FORMAT_DATA_EXPORT}: великі вибірки, звіти за період, пошукові запити з багатьма результатами (ВИКЛЮЧЕННЯ: топ-N, найбільше/найменше)",
            f"- {RESPONSE_FORMAT_ANALYTICAL_TEXT}: запити на аналітику, кореляцію, порівняння даних між джерелами, середня ціна по областях/регіонах",
            f"- {RESPONSE_FORMAT_GEO_ASSESSMENT}: «чи підходить для аптеки/кафе/клініки», «оціни для відкриття магазину», «проаналізуй оточення для бізнесу» — оцінка приміщення з урахуванням розташування та POI навколо",
            f"- {RESPONSE_FORMAT_OUT_OF_SCOPE}: ТІЛЬКИ запити, що НЕ стосуються нерухомості та даних (погода, рецепти, загальні знання). Запити про ціни, регіони, оголошення — ЗАВЖДИ в межах системи. Ігноруй попередні відповіді асистента при визначенні формату.",
            "",
            "Поверни результат у форматі JSON:",
            "{",
            '  "intent": "короткий опис наміру",',
            f'  "response_format": "{RESPONSE_FORMAT_TEXT_ANSWER}|{RESPONSE_FORMAT_DATA_EXPORT}|{RESPONSE_FORMAT_ANALYTICAL_TEXT}|{RESPONSE_FORMAT_GEO_ASSESSMENT}|{RESPONSE_FORMAT_OUT_OF_SCOPE}",',
            '  "response_template": "короткий опис шаблону відповіді (опціонально)",',
            '  "confidence": 0.0-1.0,',
            '  "reasoning": "коротке пояснення вибору (опціонально)"',
            "}"
        ])
        return "\n".join(prompt_parts)
    
    def _parse_llm_response(self, response_text: str) -> Dict[str, Any]:
        """Парсить відповідь LLM у структурований формат."""
        # Спробуємо знайти JSON у відповіді
        response_text = response_text.strip()
        
        # Шукаємо JSON блок
        start_idx = response_text.find("{")
        end_idx = response_text.rfind("}")
        
        if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
            logger.warning("Не знайдено JSON у відповіді LLM: %s", response_text[:200])
            return self._create_default_result()
        
        json_text = response_text[start_idx:end_idx + 1]
        
        try:
            result = json.loads(json_text)
        except json.JSONDecodeError as e:
            logger.warning("Помилка парсингу JSON: %s. Текст: %s", e, json_text[:200])
            return self._create_default_result()
        
        return result
    
    def _validate_result(self, result: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """Валідує та нормалізує результат."""
        # Перевіряємо обов'язкові поля
        if "intent" not in result:
            result["intent"] = "query"
        
        if "response_format" not in result:
            result["response_format"] = RESPONSE_FORMAT_TEXT_ANSWER
        
        # Валідуємо response_format
        valid_formats = [
            RESPONSE_FORMAT_TEXT_ANSWER,
            RESPONSE_FORMAT_DATA_EXPORT,
            RESPONSE_FORMAT_ANALYTICAL_TEXT,
            RESPONSE_FORMAT_GEO_ASSESSMENT,
            RESPONSE_FORMAT_OUT_OF_SCOPE
        ]
        if result["response_format"] not in valid_formats:
            logger.warning("Невірний формат відповіді: %s", result["response_format"])
            result["response_format"] = RESPONSE_FORMAT_TEXT_ANSWER
        
        # Валідуємо confidence
        if "confidence" not in result:
            result["confidence"] = 0.7
        else:
            try:
                confidence = float(result["confidence"])
                result["confidence"] = max(0.0, min(1.0, confidence))
            except (ValueError, TypeError):
                result["confidence"] = 0.7
        
        # Додаємо response_template якщо відсутній
        if "response_template" not in result:
            result["response_template"] = None
        
        # Додаємо reasoning якщо відсутній
        if "reasoning" not in result:
            result["reasoning"] = None
        
        # Додаткова логіка для визначення формату на основі запиту
        result = self._refine_format_with_rules(result, user_query)
        
        return result
    
    def _refine_format_with_rules(self, result: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """Уточнює формат відповіді на основі правил."""
        query_lower = user_query.lower()
        
        # Перевірка на топ-N запити (завжди текстова відповідь)
        top_patterns = ["топ-", "топ ", "найбільше", "найменше", "найдорожч", "найдешевш"]
        if any(pattern in query_lower for pattern in top_patterns):
            if result["response_format"] == RESPONSE_FORMAT_DATA_EXPORT:
                result["response_format"] = RESPONSE_FORMAT_TEXT_ANSWER
                result["reasoning"] = (result.get("reasoning") or "") + " (змінено на text_answer через топ-N запит)"
        
        # Перевірка на явні запити файлу
        export_patterns = ["експорт", "виведи у файл", "звіт за", "excel", "файл"]
        if any(pattern in query_lower for pattern in export_patterns):
            if result["response_format"] == RESPONSE_FORMAT_TEXT_ANSWER:
                result["response_format"] = RESPONSE_FORMAT_DATA_EXPORT
                result["reasoning"] = (result.get("reasoning") or "") + " (змінено на data_export через явний запит файлу)"
        
        # Перевірка на аналітичні запити
        analytical_patterns = ["проаналізуй", "кореляція", "порівняй", "висновки", "тренд"]
        if any(pattern in query_lower for pattern in analytical_patterns):
            if result["response_format"] == RESPONSE_FORMAT_TEXT_ANSWER:
                result["response_format"] = RESPONSE_FORMAT_ANALYTICAL_TEXT
                result["reasoning"] = (result.get("reasoning") or "") + " (змінено на analytical_text через аналітичний запит)"
        
        return result
    
    def _create_default_result(self) -> Dict[str, Any]:
        """Створює результат за замовчуванням."""
        return {
            "intent": "query",
            "response_format": RESPONSE_FORMAT_TEXT_ANSWER,
            "response_template": None,
            "confidence": 0.5,
            "reasoning": "Використано fallback через помилку парсингу"
        }
    
    def _fallback_detection(self, user_query: str) -> Dict[str, Any]:
        """Fallback визначення наміру без LLM (на основі простих правил)."""
        query_lower = user_query.lower()
        
        # Перевірка на out_of_scope
        if len(query_lower) < 10 or not any(word in query_lower for word in ["нерухомість", "аукціон", "олх", "prozorro", "оголошення", "земля", "ділянка"]):
            return {
                "intent": "out_of_scope",
                "response_format": RESPONSE_FORMAT_OUT_OF_SCOPE,
                "response_template": None,
                "confidence": 0.6,
                "reasoning": "Запит не містить ключових слів про нерухомість"
            }
        
        # Перевірка на експорт
        if any(word in query_lower for word in ["експорт", "виведи у файл", "звіт за", "excel"]):
            return {
                "intent": "export_data",
                "response_format": RESPONSE_FORMAT_DATA_EXPORT,
                "response_template": None,
                "confidence": 0.7,
                "reasoning": "Виявлено запит на експорт/звіт"
            }
        
        # Перевірка на аналітику
        if any(word in query_lower for word in ["проаналізуй", "кореляція", "порівняй", "висновки"]):
            return {
                "intent": "analytical_query",
                "response_format": RESPONSE_FORMAT_ANALYTICAL_TEXT,
                "response_template": None,
                "confidence": 0.7,
                "reasoning": "Виявлено аналітичний запит"
            }
        
        # За замовчуванням - текстова відповідь
        return {
            "intent": "query",
            "response_format": RESPONSE_FORMAT_TEXT_ANSWER,
            "response_template": None,
            "confidence": 0.5,
            "reasoning": "Fallback: текстова відповідь за замовчуванням"
        }
