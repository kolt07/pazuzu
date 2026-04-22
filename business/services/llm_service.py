# -*- coding: utf-8 -*-
"""
Сервіс для роботи з LLM API (Gemini, ChatGPT, Claude).
"""

import os
import time
import json
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import logging

from config.settings import Settings

logger = logging.getLogger(__name__)

def _get_vllm_orchestrator():
    from business.services.vllm_runtime_orchestrator import get_shared_vllm_runtime_orchestrator

    return get_shared_vllm_runtime_orchestrator()


class RateLimiter:
    """Клас для обмеження швидкості викликів API. При calls_per_minute <= 0 — без обмежень."""

    def __init__(self, calls_per_minute: int = 0):
        """
        Ініціалізація rate limiter.

        Args:
            calls_per_minute: Максимальна кількість викликів за хвилину. 0 = без обмежень.
        """
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute if calls_per_minute > 0 else 0.0
        self.last_call_time: Optional[float] = None

    def wait_if_needed(self) -> None:
        """Чекає, якщо потрібно, щоб не перевищити ліміт викликів. При вимкненому ліміті — нічого не робить."""
        if self.calls_per_minute <= 0:
            return
        if self.last_call_time is not None:
            elapsed = time.time() - self.last_call_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
        self.last_call_time = time.time()


class BaseLLMProvider(ABC):
    """Базовий клас для LLM провайдерів."""
    
    def __init__(self, api_key: str, rate_limiter: RateLimiter):
        """
        Ініціалізація провайдера.
        
        Args:
            api_key: API ключ для доступу до сервісу
            rate_limiter: Об'єкт для обмеження швидкості викликів
        """
        self.api_key = api_key
        self.rate_limiter = rate_limiter
    
    @abstractmethod
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """
        Парсить опис аукціону та повертає структуровані дані.
        
        Args:
            description: Текст опису аукціону
            
        Returns:
            Dict з полями: cadastral_number, area, area_unit, address_region,
            address_city, address_street, address_street_type, address_building,
            floor, property_type, utilities
        """
        pass

    def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
    ) -> str:
        """
        Генерує текст за промптом (для intent extraction тощо). За замовчуванням не реалізовано.
        """
        raise NotImplementedError("generate_text не реалізовано для цього провайдера")
    
    def _create_parsing_prompt(self, description: str) -> str:
        """
        Створює промпт для парсингу опису аукціону.
        
        Args:
            description: Текст опису аукціону
            
        Returns:
            Текст промпту
        """
        # Визначаємо, чи це, ймовірно, опис OLX (наш OlxLLMExtractorService додає префікси українською)
        is_olx_like = "Заголовок оголошення:" in (description or "") or "Локація об'єкта:" in (description or "")
        try:
            from config.config_loader import get_config_loader
            loader = get_config_loader()
            if is_olx_like:
                tmpl = loader.get_prompt("olx_parsing", fallback=None)
                if tmpl:
                    return tmpl.format(description=description)
            template = loader.get_parsing_template()
            if template:
                return template.format(description=description)
        except Exception:
            pass
        # Fallback — спрощений хардкод (англійські інструкції, усі текстові значення українською).
        base = (
            "Extract structured information from the real-estate description and return ONLY JSON. "
            "If information is missing or unclear, use null or an empty value. "
            "All extracted text values (addresses, tags, descriptions, labels) must be in Ukrainian. "
            "Use square meters as the main unit for all areas; use the formula 1 hectare = 100 sotok = 10 000 m²."
        )
        fields = (
            "- cadastral_number: cadastral number (string, if present).\n"
            "- building_area_sqm: building/premises area in square meters (number; not land).\n"
            "- land_area_sqm: land plot area in square meters (number; not buildings).\n"
            "- addresses: array of address objects (region, district, settlement_type, settlement, settlement_district, "
            "street_type, street, building, building_part, room) with Ukrainian text.\n"
            "- floor: floor number if present.\n"
            "- property_type: one of \"Земля під будівництво\", \"Землі с/г призначення\", \"Нерухомість\", \"інше\".\n"
            "- utilities: short Ukrainian string with communications (e.g. \"електрика, вода, газ\") or \"відсутні\".\n"
            "- tags: array of lowercase Ukrainian tags (purpose + utilities).\n"
            "- arrests_info: text about encumbrances in Ukrainian or \"відсутні\" if none; for OLX-like listings you may simply return \"відсутні\"."
        )
        return (
            f"{base}\n\n"
            f"## Description:\n{description}\n\n"
            "## Fields (field names in English, values in Ukrainian):\n"
            f"{fields}\n\n"
            "Return ONLY valid JSON without any extra text."
        )


class GeminiLLMProvider(BaseLLMProvider):
    """Провайдер для Google Gemini API."""

    def __init__(self, api_key: str, rate_limiter: RateLimiter, model_name: str = 'gemini-2.5-flash'):
        super().__init__(api_key, rate_limiter)
        self._last_usage: Optional[Dict[str, int]] = None  # input_tokens, output_tokens після останнього виклику
        self._last_request_text: Optional[str] = None  # повний текст запиту для логування обміну
        self._last_response_text: Optional[str] = None  # повний текст відповіді для логування обміну
        try:
            from google import genai
            self.client = genai.Client(api_key=self.api_key)
            # Список моделей для спроби (в порядку пріоритету)
            # Актуальні моделі: gemini-2.5-flash, gemini-2.5-pro, gemini-2.5-flash-lite
            self.model_name = model_name
            self._validate_model(model_name)
        except ImportError:
            raise ImportError("Для використання Gemini потрібно встановити google-genai: pip install google-genai")
    
    def _validate_model(self, preferred_model: str):
        """Валідує модель, спробувавши кілька варіантів. Тільки Flash-моделі (Pro не використовується)."""
        # Список моделей для спроби (в порядку пріоритету). Лише Flash — gemini-pro застаріла (404)
        models_to_try = [
            preferred_model,
            'gemini-2.5-flash',
            'gemini-2.5-flash-lite',
        ]
        
        # Видаляємо дублікати, зберігаючи порядок
        seen = set()
        self._available_models = [m for m in models_to_try if not (m in seen or seen.add(m))]
        
        # Зберігаємо першу модель як активну (перевірка доступності буде при першому запиті)
        self.model_name = self._available_models[0]

    def _usage_from_response(self, response: Any) -> Optional[Dict[str, int]]:
        """Витягує input_tokens та output_tokens з відповіді Gemini (usage_metadata)."""
        try:
            um = getattr(response, "usage_metadata", None)
            if um is None:
                return None
            inp = getattr(um, "prompt_token_count", None) or getattr(um, "input_token_count", None)
            out = getattr(um, "candidates_token_count", None) or getattr(um, "output_token_count", None)
            if inp is not None or out is not None:
                return {"input_tokens": int(inp or 0), "output_tokens": int(out or 0)}
        except (TypeError, ValueError):
            pass
        return None
    
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """Парсить опис аукціону через Gemini API."""
        if not description or not description.strip():
            return self._empty_result()
        
        self.rate_limiter.wait_if_needed()
        
        max_retries = 3
        retry_delay = 5  # Початкова затримка в секундах
        
        # Список моделей для спроби (якщо перша не спрацює)
        models_to_try = getattr(self, '_available_models', [self.model_name])
        last_error = None
        success = False
        
        for model_name in models_to_try:
            for attempt in range(max_retries):
                try:
                    prompt = self._create_parsing_prompt(description)
                    
                    # Виконуємо запит через новий API
                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=prompt
                    )

                    # Зберігаємо використання токенів та тексти для логування обміну
                    self._last_usage = self._usage_from_response(response)
                    self._last_request_text = prompt
                    if not hasattr(response, 'text') or response.text is None:
                        raise ValueError("Відповідь від Gemini не містить тексту")

                    response_text = str(response.text).strip()
                    self._last_response_text = response_text

                    if not response_text:
                        raise ValueError("Відповідь від Gemini порожня")
                    
                    # Спробуємо знайти JSON у відповіді
                    json_text = self._extract_json_from_response(response_text)
                    
                    if not json_text:
                        raise ValueError("Не вдалося витягти JSON з відповіді")
                    
                    result = json.loads(json_text)
                    
                    # Перевіряємо тип результату
                    if isinstance(result, list):
                        # Якщо це список, беремо перший елемент
                        if len(result) > 0 and isinstance(result[0], dict):
                            result = result[0]
                        else:
                            # Якщо список порожній або не містить словників, повертаємо порожній результат
                            return self._empty_result()
                    elif not isinstance(result, dict):
                        # Якщо це не словник і не список, повертаємо порожній результат
                        return self._empty_result()
                    
                    # Оновлюємо активну модель, якщо використали іншу
                    if model_name != self.model_name:
                        self.model_name = model_name
                    
                    success = True
                    return self._normalize_result(result)
                except KeyboardInterrupt:
                    # Переривання користувача - не обробляємо, просто пробрасуємо далі
                    raise
                except Exception as e:
                    error_str = str(e)
                    last_error = e
                    
                    # Перевіряємо, чи це помилка квоти (429)
                    if '429' in error_str or 'quota' in error_str.lower() or 'exceeded' in error_str.lower():
                        # Спробуємо витягти час очікування з помилки
                        import re
                        retry_match = re.search(r'retry in (\d+(?:\.\d+)?)s', error_str, re.IGNORECASE)
                        if retry_match:
                            retry_delay = float(retry_match.group(1)) + 1  # Додаємо 1 секунду для безпеки
                        else:
                            retry_delay = min(retry_delay * 2, 60)  # Подвоюємо затримку, але не більше 60 секунд
                        
                        if attempt < max_retries - 1:
                            print(f"Перевищено квоту Gemini. Очікування {retry_delay:.1f} секунд перед повторною спробою...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            # Досягнуто максимум спроб для цієї моделі — чекаємо перед переходом на наступну
                            if model_name != models_to_try[-1]:
                                print(f"Перевищено квоту Gemini ({model_name}). Очікування {retry_delay:.1f} с перед спробою іншої моделі...")
                                time.sleep(retry_delay)
                            break
                    else:
                        # Інша помилка - пробуємо наступну модель
                        break

            # Якщо успішно виконали запит, виходимо з циклу по моделях
            if success:
                break
        
        # Якщо всі моделі не спрацювали
        if last_error and not success:
            print(f"Помилка при парсингу через Gemini (спробовано всі моделі): {last_error}")
        
        return self._empty_result()
    
    def _extract_json_from_response(self, text: str) -> str:
        """Витягує JSON з текстової відповіді."""
        # Шукаємо JSON у тексті (може бути обгорнутий в markdown код блоки)
        text = text.strip()
        
        # Якщо текст починається з ```json або ```
        if text.startswith('```'):
            lines = text.split('\n')
            # Пропускаємо перший рядок з ```
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith('```'):
                    if not in_json:
                        in_json = True
                    else:
                        break
                    continue
                if in_json:
                    json_lines.append(line)
            return '\n'.join(json_lines)
        
        # Якщо текст починається з {, спробуємо знайти перший { і останній }
        if '{' in text:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                return text[start:end+1]
        
        return text
    
    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Нормалізує результат парсингу."""
        # Обробляємо адреси - підтримуємо як старий формат (для сумісності), так і новий (масив)
        addresses = result.get('addresses', [])
        if not addresses:
            # Якщо немає масиву адрес, але є старі поля - створюємо адресу з них
            if result.get('address_region') or result.get('address_city'):
                addresses = [{
                    'region': result.get('address_region', ''),
                    'district': result.get('address_district', ''),
                    'settlement_type': result.get('address_settlement_type', ''),
                    'settlement': result.get('address_city', ''),
                    'settlement_district': result.get('address_settlement_district', ''),
                    'street_type': result.get('address_street_type', ''),
                    'street': result.get('address_street', ''),
                    'building': result.get('address_building', ''),
                    'building_part': result.get('address_building_part', ''),
                    'room': result.get('address_room', '')
                }]
        
        # Обробляємо площі - конвертуємо в стандартні одиниці
        building_area_sqm = result.get('building_area_sqm', '')
        land_area_ha = result.get('land_area_ha', '')
        
        # Якщо є старі поля area та area_unit - конвертуємо їх
        if not building_area_sqm and not land_area_ha:
            old_area = result.get('area', '')
            old_unit = result.get('area_unit', '')
            if old_area:
                try:
                    area_value = float(str(old_area).replace(',', '.').replace(' ', ''))
                    if old_unit:
                        unit_lower = old_unit.lower()
                        # Визначаємо тип площі за одиницею
                        if any(x in unit_lower for x in ['гектар', 'hectare', 'га']):
                            land_area_ha = area_value
                        elif any(x in unit_lower for x in ['м²', 'м2', 'кв.м', 'квадратний метр']):
                            building_area_sqm = area_value
                        elif 'сот' in unit_lower:
                            land_area_ha = area_value * 0.01
                    else:
                        # Якщо одиниця не вказана - визначаємо за значенням
                        if area_value > 1000:
                            building_area_sqm = area_value
                        elif area_value < 10:
                            land_area_ha = area_value
                except (ValueError, AttributeError):
                    pass
        
        tags_raw = result.get('tags', [])
        tags = [str(t).strip().lower() for t in (tags_raw if isinstance(tags_raw, list) else []) if t and str(t).strip()]
        tags = list(dict.fromkeys(tags))
        # Площа землі зберігається в м² (land_area_sqm). Якщо LLM повернув land_area_ha — конвертуємо.
        land_area_sqm = result.get('land_area_sqm', '')
        if not land_area_sqm and land_area_ha:
            try:
                land_area_sqm = float(land_area_ha) * 10000.0
            except (TypeError, ValueError):
                land_area_sqm = ''
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'building_area_sqm': building_area_sqm if building_area_sqm else '',
            'land_area_ha': land_area_ha if land_area_ha else '',
            'land_area_sqm': land_area_sqm if land_area_sqm else '',
            'addresses': addresses,  # Масив адрес
            'floor': result.get('floor', ''),
            'property_type': result.get('property_type', ''),
            'utilities': result.get('utilities', ''),
            'tags': tags,
            'arrests_info': result.get('arrests_info', '')
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            'cadastral_number': '',
            'building_area_sqm': '',
            'land_area_ha': '',
            'land_area_sqm': '',
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
            'tags': [],
            'arrests_info': ''
        }

    def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
    ) -> str:
        """Генерує текст за промптом (Gemini API)."""
        self.rate_limiter.wait_if_needed()
        full_content = prompt
        if system_prompt:
            full_content = f"{system_prompt}\n\n{prompt}"
        self._last_request_text = full_content
        self._last_response_text = None
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=full_content,
                config={"temperature": temperature},
            )
            self._last_usage = self._usage_from_response(response)
            if not hasattr(response, 'text') or response.text is None:
                self._last_response_text = ""
                return ""
            out = str(response.text).strip()
            self._last_response_text = out
            return out
        except Exception as e:
            self._last_response_text = f"[error] {e!s}"
            return ""


class OpenAILLMProvider(BaseLLMProvider):
    """Провайдер для OpenAI (ChatGPT) API."""
    
    def __init__(self, api_key: str, rate_limiter: RateLimiter):
        super().__init__(api_key, rate_limiter)
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=self.api_key)
        except ImportError:
            raise ImportError("Для використання OpenAI потрібно встановити openai: pip install openai")
    
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """Парсить опис аукціону через OpenAI API."""
        if not description or not description.strip():
            return self._empty_result()
        
        self.rate_limiter.wait_if_needed()
        
        try:
            prompt = self._create_parsing_prompt(description)
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Ти експерт з аналізу описів нерухомості. Повертай тільки валідний JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            response_text = response.choices[0].message.content.strip()
            json_text = self._extract_json_from_response(response_text)
            result = json.loads(json_text)
            return self._normalize_result(result)
        except Exception as e:
            print(f"Помилка при парсингу через OpenAI: {e}")
            return self._empty_result()
    
    def _extract_json_from_response(self, text: str) -> str:
        """Витягує JSON з текстової відповіді."""
        text = text.strip()
        
        if text.startswith('```'):
            lines = text.split('\n')
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith('```'):
                    if not in_json:
                        in_json = True
                    else:
                        break
                    continue
                if in_json:
                    json_lines.append(line)
            return '\n'.join(json_lines)
        
        if '{' in text:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                return text[start:end+1]
        
        return text
    
    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Нормалізує результат парсингу."""
        # Обробляємо адреси - підтримуємо як старий формат (для сумісності), так і новий (масив)
        addresses = result.get('addresses', [])
        if not addresses:
            # Якщо немає масиву адрес, але є старі поля - створюємо адресу з них
            if result.get('address_region') or result.get('address_city'):
                addresses = [{
                    'region': result.get('address_region', ''),
                    'district': result.get('address_district', ''),
                    'settlement_type': result.get('address_settlement_type', ''),
                    'settlement': result.get('address_city', ''),
                    'settlement_district': result.get('address_settlement_district', ''),
                    'street_type': result.get('address_street_type', ''),
                    'street': result.get('address_street', ''),
                    'building': result.get('address_building', ''),
                    'building_part': result.get('address_building_part', ''),
                    'room': result.get('address_room', '')
                }]
        
        # Обробляємо площі - конвертуємо в стандартні одиниці
        building_area_sqm = result.get('building_area_sqm', '')
        land_area_ha = result.get('land_area_ha', '')
        
        # Якщо є старі поля area та area_unit - конвертуємо їх
        if not building_area_sqm and not land_area_ha:
            old_area = result.get('area', '')
            old_unit = result.get('area_unit', '')
            if old_area:
                try:
                    area_value = float(str(old_area).replace(',', '.').replace(' ', ''))
                    if old_unit:
                        unit_lower = old_unit.lower()
                        # Визначаємо тип площі за одиницею
                        if any(x in unit_lower for x in ['гектар', 'hectare', 'га']):
                            land_area_ha = area_value
                        elif any(x in unit_lower for x in ['м²', 'м2', 'кв.м', 'квадратний метр']):
                            building_area_sqm = area_value
                        elif 'сот' in unit_lower:
                            land_area_ha = area_value * 0.01
                    else:
                        # Якщо одиниця не вказана - визначаємо за значенням
                        if area_value > 1000:
                            building_area_sqm = area_value
                        elif area_value < 10:
                            land_area_ha = area_value
                except (ValueError, AttributeError):
                    pass
        
        tags_raw = result.get('tags', [])
        tags = [str(t).strip().lower() for t in (tags_raw if isinstance(tags_raw, list) else []) if t and str(t).strip()]
        tags = list(dict.fromkeys(tags))
        land_area_sqm = result.get('land_area_sqm', '')
        if not land_area_sqm and land_area_ha:
            try:
                land_area_sqm = float(land_area_ha) * 10000.0
            except (TypeError, ValueError):
                land_area_sqm = ''
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'building_area_sqm': building_area_sqm if building_area_sqm else '',
            'land_area_ha': land_area_ha if land_area_ha else '',
            'land_area_sqm': land_area_sqm if land_area_sqm else '',
            'addresses': addresses,  # Масив адрес
            'floor': result.get('floor', ''),
            'property_type': result.get('property_type', ''),
            'utilities': result.get('utilities', ''),
            'tags': tags,
            'arrests_info': result.get('arrests_info', '')
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            'cadastral_number': '',
            'building_area_sqm': '',
            'land_area_ha': '',
            'land_area_sqm': '',
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
            'tags': [],
            'arrests_info': ''
        }


class AnthropicLLMProvider(BaseLLMProvider):
    """Провайдер для Anthropic (Claude) API."""
    
    def __init__(self, api_key: str, rate_limiter: RateLimiter):
        super().__init__(api_key, rate_limiter)
        try:
            from anthropic import Anthropic
            self.client = Anthropic(api_key=self.api_key)
        except ImportError:
            raise ImportError("Для використання Anthropic потрібно встановити anthropic: pip install anthropic")
    
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """Парсить опис аукціону через Anthropic API."""
        if not description or not description.strip():
            return self._empty_result()
        
        self.rate_limiter.wait_if_needed()
        
        try:
            prompt = self._create_parsing_prompt(description)
            message = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1024,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            response_text = message.content[0].text.strip()
            json_text = self._extract_json_from_response(response_text)
            result = json.loads(json_text)
            return self._normalize_result(result)
        except Exception as e:
            print(f"Помилка при парсингу через Anthropic: {e}")
            return self._empty_result()
    
    def _extract_json_from_response(self, text: str) -> str:
        """Витягує JSON з текстової відповіді."""
        text = text.strip()
        
        if text.startswith('```'):
            lines = text.split('\n')
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith('```'):
                    if not in_json:
                        in_json = True
                    else:
                        break
                    continue
                if in_json:
                    json_lines.append(line)
            return '\n'.join(json_lines)
        
        if '{' in text:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                return text[start:end+1]
        
        return text
    
    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Нормалізує результат парсингу."""
        # Обробляємо адреси - підтримуємо як старий формат (для сумісності), так і новий (масив)
        addresses = result.get('addresses', [])
        if not addresses:
            # Якщо немає масиву адрес, але є старі поля - створюємо адресу з них
            if result.get('address_region') or result.get('address_city'):
                addresses = [{
                    'region': result.get('address_region', ''),
                    'district': result.get('address_district', ''),
                    'settlement_type': result.get('address_settlement_type', ''),
                    'settlement': result.get('address_city', ''),
                    'settlement_district': result.get('address_settlement_district', ''),
                    'street_type': result.get('address_street_type', ''),
                    'street': result.get('address_street', ''),
                    'building': result.get('address_building', ''),
                    'building_part': result.get('address_building_part', ''),
                    'room': result.get('address_room', '')
                }]
        
        # Обробляємо площі - конвертуємо в стандартні одиниці
        building_area_sqm = result.get('building_area_sqm', '')
        land_area_ha = result.get('land_area_ha', '')
        
        # Якщо є старі поля area та area_unit - конвертуємо їх
        if not building_area_sqm and not land_area_ha:
            old_area = result.get('area', '')
            old_unit = result.get('area_unit', '')
            if old_area:
                try:
                    area_value = float(str(old_area).replace(',', '.').replace(' ', ''))
                    if old_unit:
                        unit_lower = old_unit.lower()
                        # Визначаємо тип площі за одиницею
                        if any(x in unit_lower for x in ['гектар', 'hectare', 'га']):
                            land_area_ha = area_value
                        elif any(x in unit_lower for x in ['м²', 'м2', 'кв.м', 'квадратний метр']):
                            building_area_sqm = area_value
                        elif 'сот' in unit_lower:
                            land_area_ha = area_value * 0.01
                    else:
                        # Якщо одиниця не вказана - визначаємо за значенням
                        if area_value > 1000:
                            building_area_sqm = area_value
                        elif area_value < 10:
                            land_area_ha = area_value
                except (ValueError, AttributeError):
                    pass
        
        tags_raw = result.get('tags', [])
        tags = [str(t).strip().lower() for t in (tags_raw if isinstance(tags_raw, list) else []) if t and str(t).strip()]
        tags = list(dict.fromkeys(tags))
        land_area_sqm = result.get('land_area_sqm', '')
        if not land_area_sqm and land_area_ha:
            try:
                land_area_sqm = float(land_area_ha) * 10000.0
            except (TypeError, ValueError):
                land_area_sqm = ''
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'building_area_sqm': building_area_sqm if building_area_sqm else '',
            'land_area_ha': land_area_ha if land_area_ha else '',
            'land_area_sqm': land_area_sqm if land_area_sqm else '',
            'addresses': addresses,  # Масив адрес
            'floor': result.get('floor', ''),
            'property_type': result.get('property_type', ''),
            'utilities': result.get('utilities', ''),
            'tags': tags,
            'arrests_info': result.get('arrests_info', '')
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            'cadastral_number': '',
            'building_area_sqm': '',
            'land_area_ha': '',
            'land_area_sqm': '',
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
            'tags': [],
            'arrests_info': ''
        }


class OllamaLLMProvider(BaseLLMProvider):
    """Провайдер для локальної LLM через Ollama (gemma3:27b тощо)."""

    def _ollama_host_for_log(self) -> str:
        """Куди ходить клієнт Ollama (локально або Vast через OLLAMA_HOST)."""
        for attr in ("host", "_host"):
            h = getattr(self.client, attr, None)
            if h:
                return str(h)
        return str(os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434"))

    def __init__(self, api_key: str, rate_limiter: RateLimiter, model_name: str = 'gemma3:27b'):
        super().__init__(api_key or '', rate_limiter)
        self._last_usage: Optional[Dict[str, int]] = None
        self._last_request_text: Optional[str] = None
        self._last_response_text: Optional[str] = None
        try:
            from ollama import Client
            self.client = Client()
            self.model_name = model_name
        except ImportError:
            raise ImportError("Для використання Ollama потрібно встановити ollama: pip install ollama")

    def _usage_from_ollama_response(self, response: Dict[str, Any]) -> Dict[str, int]:
        """Витягує кількість токенів з відповіді Ollama (prompt_eval_count, eval_count)."""
        inp = response.get("prompt_eval_count")
        out = response.get("eval_count")
        return {"input_tokens": int(inp or 0), "output_tokens": int(out or 0)}

    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """Парсить опис аукціону через Ollama API."""
        if not description or not description.strip():
            return self._empty_result()

        self.rate_limiter.wait_if_needed()
        prompt = self._create_parsing_prompt(description)
        self._last_request_text = prompt
        self._last_response_text = None

        t0 = time.perf_counter()
        try:
            response = self.client.generate(
                model=self.model_name,
                prompt=prompt,
                options={"temperature": 0.0},
            )
            self._last_usage = self._usage_from_ollama_response(response)
            response_text = (response.get("response") or "").strip()
            self._last_request_text = prompt
            self._last_response_text = response_text
            if not response_text:
                return self._empty_result()

            json_text = self._extract_json_from_response(response_text)
            if not json_text:
                return self._empty_result()

            result = json.loads(json_text)
            if isinstance(result, list):
                if len(result) > 0 and isinstance(result[0], dict):
                    result = result[0]
                else:
                    return self._empty_result()
            elif not isinstance(result, dict):
                return self._empty_result()

            dur_ms = int((time.perf_counter() - t0) * 1000)
            u = self._last_usage or {}
            logger.info(
                "[llm-ollama] парсинг опису: model=%s тривалість_ms=%s вхід_ток=%s вихід_ток=%s host=%s",
                self.model_name,
                dur_ms,
                u.get("input_tokens", 0),
                u.get("output_tokens", 0),
                self._ollama_host_for_log(),
            )
            return self._normalize_result(result)
        except Exception as e:
            dur_ms = int((time.perf_counter() - t0) * 1000)
            logger.warning(
                "[llm-ollama] парсинг опису помилка після %sms host=%s: %s",
                dur_ms,
                self._ollama_host_for_log(),
                e,
            )
            self._last_response_text = f"[error] {e!s}"
            return self._empty_result()

    def _extract_json_from_response(self, text: str) -> str:
        """Витягує JSON з відповіді."""
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith("```"):
                    if not in_json:
                        in_json = True
                    else:
                        break
                    continue
                if in_json:
                    json_lines.append(line)
            return "\n".join(json_lines)
        if "{" in text:
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return text[start : end + 1]
        return text

    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Нормалізує результат парсингу (аналог GeminiLLMProvider)."""
        addresses = result.get("addresses", [])
        if not addresses and (result.get("address_region") or result.get("address_city")):
            addresses = [{
                "region": result.get("address_region", ""),
                "district": result.get("address_district", ""),
                "settlement_type": result.get("address_settlement_type", ""),
                "settlement": result.get("address_city", ""),
                "settlement_district": result.get("address_settlement_district", ""),
                "street_type": result.get("address_street_type", ""),
                "street": result.get("address_street", ""),
                "building": result.get("address_building", ""),
                "building_part": result.get("address_building_part", ""),
                "room": result.get("address_room", ""),
            }]

        building_area_sqm = result.get("building_area_sqm", "")
        land_area_ha = result.get("land_area_ha", "")
        if not building_area_sqm and not land_area_ha:
            old_area = result.get("area", "")
            old_unit = result.get("area_unit", "")
            if old_area:
                try:
                    area_value = float(str(old_area).replace(",", ".").replace(" ", ""))
                    if old_unit:
                        unit_lower = str(old_unit).lower()
                        if any(x in unit_lower for x in ["гектар", "hectare", "га"]):
                            land_area_ha = area_value
                        elif any(x in unit_lower for x in ["м²", "м2", "кв.м", "квадратний метр"]):
                            building_area_sqm = area_value
                        elif "сот" in unit_lower:
                            land_area_ha = area_value * 0.01
                    else:
                        building_area_sqm = area_value if area_value > 1000 else ""
                        land_area_ha = area_value if area_value < 10 else ""
                except (ValueError, AttributeError):
                    pass

        tags_raw = result.get("tags", [])
        tags = [str(t).strip().lower() for t in (tags_raw if isinstance(tags_raw, list) else []) if t and str(t).strip()]
        tags = list(dict.fromkeys(tags))
        land_area_sqm = result.get("land_area_sqm", "")
        if not land_area_sqm and land_area_ha:
            try:
                land_area_sqm = float(land_area_ha) * 10000.0
            except (TypeError, ValueError):
                land_area_sqm = ""
        return {
            "cadastral_number": result.get("cadastral_number", ""),
            "building_area_sqm": building_area_sqm if building_area_sqm else "",
            "land_area_ha": land_area_ha if land_area_ha else "",
            "land_area_sqm": land_area_sqm if land_area_sqm else "",
            "addresses": addresses,
            "floor": result.get("floor", ""),
            "property_type": result.get("property_type", ""),
            "utilities": result.get("utilities", ""),
            "tags": tags,
            "arrests_info": result.get("arrests_info", ""),
        }

    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            "cadastral_number": "",
            "building_area_sqm": "",
            "land_area_ha": "",
            "land_area_sqm": "",
            "addresses": [],
            "floor": "",
            "property_type": "",
            "utilities": "",
            "tags": [],
            "arrests_info": "",
        }

    def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
    ) -> str:
        """Генерує текст за промптом через Ollama."""
        self.rate_limiter.wait_if_needed()
        full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        self._last_request_text = full_prompt
        self._last_response_text = None
        t0 = time.perf_counter()
        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            response = self.client.chat(
                model=self.model_name,
                messages=messages,
                options={"temperature": temperature},
            )
            self._last_usage = self._usage_from_ollama_response(response)
            out = (response.get("message", {}).get("content") or "").strip()
            self._last_response_text = out
            dur_ms = int((time.perf_counter() - t0) * 1000)
            u = self._last_usage or {}
            logger.info(
                "[llm-ollama] chat: model=%s тривалість_ms=%s вхід_ток=%s вихід_ток=%s host=%s",
                self.model_name,
                dur_ms,
                u.get("input_tokens", 0),
                u.get("output_tokens", 0),
                self._ollama_host_for_log(),
            )
            return out
        except Exception as e:
            dur_ms = int((time.perf_counter() - t0) * 1000)
            logger.warning(
                "[llm-ollama] chat помилка після %sms host=%s: %s",
                dur_ms,
                self._ollama_host_for_log(),
                e,
            )
            self._last_response_text = f"[error] {e!s}"
            return ""


class VllmRemoteLLMProvider(BaseLLMProvider):
    """Провайдер remote vLLM (OpenAI-compatible) з автоматичним стартом Vast.ai."""

    @staticmethod
    def _api_host_label(base_url: str) -> str:
        """Короткий host для логів (Vast/Ollama за SSH — той самий OpenAI /v1)."""
        if not base_url:
            return ""
        try:
            netloc = urlparse(base_url).netloc
            return netloc or base_url[:96]
        except Exception:
            return base_url[:96]

    def __init__(
        self,
        api_key: str,
        rate_limiter: RateLimiter,
        model_name: str = "google/gemma-2-9b-it",
    ):
        super().__init__(api_key=api_key or "", rate_limiter=rate_limiter)
        self._runtime = _get_vllm_orchestrator()
        self.model_name = model_name
        self._client = None
        self._base_url = ""
        self._vllm_api_key = api_key or "pazuzu-vllm"
        self._last_usage: Optional[Dict[str, int]] = None
        self._last_request_text: Optional[str] = None
        self._last_response_text: Optional[str] = None
        self._last_runtime_meta: Dict[str, Any] = {}

    def _ensure_client(self) -> bool:
        runtime_cfg = self._runtime._settings_svc.get_settings()
        wait_timeout_sec = int(runtime_cfg.get("llm_cached_endpoint_wait_sec") or 5)
        endpoint = self._runtime.get_cached_runtime_endpoint(wait_timeout_sec=wait_timeout_sec)
        if not endpoint:
            self._runtime.schedule_forced_healthcheck("llm_cached_endpoint_miss")
            return False
        base_url = endpoint.rstrip("/") + "/v1"
        if self._client is None or self._base_url != base_url:
            from openai import OpenAI

            self._client = OpenAI(base_url=base_url, api_key=self._vllm_api_key)
            self._base_url = base_url
        return True

    @staticmethod
    def _usage_from_openai_response(response: Any) -> Dict[str, int]:
        usage = getattr(response, "usage", None)
        if not usage:
            return {"input_tokens": 0, "output_tokens": 0}
        return {
            "input_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
            "output_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
        }

    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        if not description or not description.strip():
            return self._empty_result()
        self.rate_limiter.wait_if_needed()
        if not self._ensure_client():
            return self._empty_result()

        prompt = self._create_parsing_prompt(description)
        self._last_request_text = prompt
        self._last_response_text = None
        t0 = time.perf_counter()
        try:
            response = self._client.chat.completions.create(
                model=self.model_name,
                temperature=0.0,
                messages=[
                    {
                        "role": "system",
                        "content": "Return only valid JSON. Keep textual values in Ukrainian.",
                    },
                    {"role": "user", "content": prompt},
                ],
            )
            self._last_usage = self._usage_from_openai_response(response)
            response_text = (response.choices[0].message.content or "").strip()
            self._last_response_text = response_text
            self._last_runtime_meta = {
                "provider": "vllm_remote",
                "endpoint": self._base_url,
                "duration_ms": int((time.perf_counter() - t0) * 1000),
            }
            self._runtime.report_inference_success(self._last_runtime_meta)
            if not response_text:
                return self._empty_result()
            json_text = self._extract_json_from_response(response_text)
            if not json_text:
                return self._empty_result()
            result = json.loads(json_text)
            if isinstance(result, list):
                if len(result) > 0 and isinstance(result[0], dict):
                    result = result[0]
                else:
                    return self._empty_result()
            elif not isinstance(result, dict):
                return self._empty_result()
            dur_ms = int((time.perf_counter() - t0) * 1000)
            u = self._last_usage or {}
            logger.info(
                "[llm-remote] парсинг опису: model=%s тривалість_ms=%s вхід_ток=%s вихід_ток=%s api_host=%s",
                self.model_name,
                dur_ms,
                u.get("input_tokens", 0),
                u.get("output_tokens", 0),
                self._api_host_label(self._base_url),
            )
            return self._normalize_result(result)
        except Exception as e:
            dur_ms = int((time.perf_counter() - t0) * 1000)
            logger.warning(
                "[llm-remote] парсинг опису помилка після %sms (host=%s): %s",
                dur_ms,
                self._api_host_label(self._base_url),
                e,
            )
            self._last_response_text = f"[error] {e!s}"
            self._runtime.report_inference_failure(
                str(e),
                {
                    "provider": "vllm_remote",
                    "endpoint": self._base_url,
                    "duration_ms": dur_ms,
                },
            )
            return self._empty_result()

    def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
    ) -> str:
        self.rate_limiter.wait_if_needed()
        if not self._ensure_client():
            return ""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        self._last_request_text = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
        self._last_response_text = None
        t0 = time.perf_counter()
        try:
            response = self._client.chat.completions.create(
                model=self.model_name,
                temperature=temperature,
                messages=messages,
            )
            self._last_usage = self._usage_from_openai_response(response)
            out = (response.choices[0].message.content or "").strip()
            self._last_response_text = out
            dur_ms = int((time.perf_counter() - t0) * 1000)
            self._last_runtime_meta = {
                "provider": "vllm_remote",
                "endpoint": self._base_url,
                "duration_ms": dur_ms,
            }
            self._runtime.report_inference_success(self._last_runtime_meta)
            u = self._last_usage or {}
            logger.info(
                "[llm-remote] generate_text: model=%s тривалість_ms=%s вхід_ток=%s вихід_ток=%s api_host=%s",
                self.model_name,
                dur_ms,
                u.get("input_tokens", 0),
                u.get("output_tokens", 0),
                self._api_host_label(self._base_url),
            )
            return out
        except Exception as e:
            dur_ms = int((time.perf_counter() - t0) * 1000)
            logger.warning(
                "[llm-remote] generate_text помилка після %sms (host=%s): %s",
                dur_ms,
                self._api_host_label(self._base_url),
                e,
            )
            self._last_response_text = f"[error] {e!s}"
            self._runtime.report_inference_failure(
                str(e),
                {
                    "provider": "vllm_remote",
                    "endpoint": self._base_url,
                    "duration_ms": dur_ms,
                },
            )
            return ""

    # Методи нормалізації/витягу JSON уніфіковані з Ollama-провайдером.
    def _extract_json_from_response(self, text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith("```"):
                    if not in_json:
                        in_json = True
                    else:
                        break
                    continue
                if in_json:
                    json_lines.append(line)
            return "\n".join(json_lines)
        if "{" in text:
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return text[start : end + 1]
        return text

    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        addresses = result.get("addresses", [])
        if not addresses and (result.get("address_region") or result.get("address_city")):
            addresses = [{
                "region": result.get("address_region", ""),
                "district": result.get("address_district", ""),
                "settlement_type": result.get("address_settlement_type", ""),
                "settlement": result.get("address_city", ""),
                "settlement_district": result.get("address_settlement_district", ""),
                "street_type": result.get("address_street_type", ""),
                "street": result.get("address_street", ""),
                "building": result.get("address_building", ""),
                "building_part": result.get("address_building_part", ""),
                "room": result.get("address_room", ""),
            }]

        building_area_sqm = result.get("building_area_sqm", "")
        land_area_ha = result.get("land_area_ha", "")
        if not building_area_sqm and not land_area_ha:
            old_area = result.get("area", "")
            old_unit = result.get("area_unit", "")
            if old_area:
                try:
                    area_value = float(str(old_area).replace(",", ".").replace(" ", ""))
                    if old_unit:
                        unit_lower = str(old_unit).lower()
                        if any(x in unit_lower for x in ["гектар", "hectare", "га"]):
                            land_area_ha = area_value
                        elif any(x in unit_lower for x in ["м²", "м2", "кв.м", "квадратний метр"]):
                            building_area_sqm = area_value
                        elif "сот" in unit_lower:
                            land_area_ha = area_value * 0.01
                    else:
                        building_area_sqm = area_value if area_value > 1000 else ""
                        land_area_ha = area_value if area_value < 10 else ""
                except (ValueError, AttributeError):
                    pass

        tags_raw = result.get("tags", [])
        tags = [str(t).strip().lower() for t in (tags_raw if isinstance(tags_raw, list) else []) if t and str(t).strip()]
        tags = list(dict.fromkeys(tags))
        land_area_sqm = result.get("land_area_sqm", "")
        if not land_area_sqm and land_area_ha:
            try:
                land_area_sqm = float(land_area_ha) * 10000.0
            except (TypeError, ValueError):
                land_area_sqm = ""
        return {
            "cadastral_number": result.get("cadastral_number", ""),
            "building_area_sqm": building_area_sqm if building_area_sqm else "",
            "land_area_ha": land_area_ha if land_area_ha else "",
            "land_area_sqm": land_area_sqm if land_area_sqm else "",
            "addresses": addresses,
            "floor": result.get("floor", ""),
            "property_type": result.get("property_type", ""),
            "utilities": result.get("utilities", ""),
            "tags": tags,
            "arrests_info": result.get("arrests_info", ""),
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "cadastral_number": "",
            "building_area_sqm": "",
            "land_area_ha": "",
            "land_area_sqm": "",
            "addresses": [],
            "floor": "",
            "property_type": "",
            "utilities": "",
            "tags": [],
            "arrests_info": "",
        }


class LLMService:
    """Сервіс для роботи з LLM провайдерами."""

    def __init__(self, settings: Optional[Settings] = None):
        """
        Ініціалізація сервісу.

        Args:
            settings: Налаштування застосунку
        """
        self.settings = settings or Settings()
        self.rate_limiter = RateLimiter(self.settings.llm_rate_limit_calls_per_minute)
        self.provider = self._create_provider()  # парсинг описів (Ollama за замовчуванням)
        self._assistant_provider = self._create_assistant_provider()  # intent, generate_text (Gemini за замовчуванням)
        self._logging = None

    def _get_logging(self):
        if self._logging is None:
            try:
                from business.services.logging_service import LoggingService
                self._logging = LoggingService()
            except Exception:
                pass
        return self._logging
    
    def _create_provider(self) -> BaseLLMProvider:
        """Створює провайдера для парсингу описів (на основі llm_parsing_*)."""
        provider_name = self.settings.llm_parsing_provider.lower()
        api_key = self.settings.llm_api_keys.get(provider_name, '')

        if provider_name == 'vllm_remote':
            model_name = getattr(self.settings, 'llm_parsing_model_name', 'google/gemma-2-9b-it')
            runtime_cfg = None
            try:
                from business.services.vast_ai_runtime_settings_service import VastRuntimeSettingsService

                runtime_cfg = VastRuntimeSettingsService().get_settings()
            except Exception:
                runtime_cfg = None
            vllm_key = (runtime_cfg or {}).get("vllm_api_key", "") or api_key
            try:
                return VllmRemoteLLMProvider(vllm_key, self.rate_limiter, model_name)
            except Exception as e:
                fallback = ((runtime_cfg or {}).get("fallback_provider") or "ollama").lower()
                if fallback == "ollama":
                    logger.warning("vllm_remote недоступний, fallback на ollama: %s", e)
                    return OllamaLLMProvider('', self.rate_limiter, model_name=model_name)
                raise

        if provider_name == 'ollama':
            model_name = getattr(self.settings, 'llm_parsing_model_name', 'gemma3:27b')
            return OllamaLLMProvider(api_key, self.rate_limiter, model_name)
        if not api_key:
            raise ValueError(f"API ключ для провайдера {provider_name} не вказано в конфігурації")

        model_name = getattr(self.settings, 'llm_parsing_model_name', 'gemini-2.5-flash')
        if provider_name == 'gemini':
            return GeminiLLMProvider(api_key, self.rate_limiter, model_name)
        elif provider_name == 'openai':
            return OpenAILLMProvider(api_key, self.rate_limiter)
        elif provider_name == 'anthropic':
            return AnthropicLLMProvider(api_key, self.rate_limiter)
        else:
            raise ValueError(f"Невідомий провайдер LLM: {provider_name}")

    def _create_assistant_provider(self) -> BaseLLMProvider:
        """Створює провайдера для асистента (intent, generate_text) на основі llm_assistant_*."""
        provider_name = self.settings.llm_assistant_provider.lower()
        api_key = self.settings.llm_api_keys.get(provider_name, '')

        if provider_name == 'ollama':
            model_name = getattr(self.settings, 'llm_assistant_model_name', 'gemma3:27b')
            return OllamaLLMProvider(api_key, self.rate_limiter, model_name)
        if not api_key:
            raise ValueError(f"API ключ для провайдера асистента {provider_name} не вказано в конфігурації")

        model_name = getattr(self.settings, 'llm_assistant_model_name', 'gemini-2.5-flash')
        if provider_name == 'gemini':
            return GeminiLLMProvider(api_key, self.rate_limiter, model_name)
        elif provider_name == 'openai':
            return OpenAILLMProvider(api_key, self.rate_limiter)
        elif provider_name == 'anthropic':
            return AnthropicLLMProvider(api_key, self.rate_limiter)
        else:
            raise ValueError(f"Невідомий провайдер асистента: {provider_name}")
    
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """
        Парсить опис аукціону та повертає структуровані дані.

        Args:
            description: Текст опису аукціону

        Returns:
            Dict з структурованою інформацією
        """
        log_svc = self._get_logging()

        result = self.provider.parse_auction_description(description)

        if log_svc:
            try:
                usage = getattr(self.provider, "_last_usage", None)
                meta = {"desc_preview": (description or "")[:80] + ("..." if len(description or "") > 80 else "")}
                if isinstance(usage, dict):
                    meta["input_tokens"] = usage.get("input_tokens", 0)
                    meta["output_tokens"] = usage.get("output_tokens", 0)
                runtime_meta = getattr(self.provider, "_last_runtime_meta", None)
                if isinstance(runtime_meta, dict):
                    meta.update(runtime_meta)
                log_svc.log_api_usage(
                    service="llm",
                    source="llm_service.parse_auction_description",
                    from_cache=False,
                    metadata=meta,
                )
                req_text = getattr(self.provider, "_last_request_text", None) or ""
                resp_text = getattr(self.provider, "_last_response_text", None) or ""
                log_svc.log_llm_exchange(
                    request_text=req_text,
                    response_text=resp_text,
                    input_tokens=meta.get("input_tokens", 0),
                    output_tokens=meta.get("output_tokens", 0),
                    source="llm_service.parse_auction_description",
                    provider=(getattr(self.settings, "llm_parsing_provider", None) or "ollama"),
                    duration_ms=meta.get("duration_ms"),
                )
            except Exception as e:
                logger.warning("Не вдалося записати llm_exchange (parse_auction): %s", e)
        return result

    def parse_real_estate_objects(self, description: str) -> Dict[str, Any]:
        """
        Витягує об'єкти нерухомого майна (ОНМ) з опису оголошення.
        Використовує провайдер парсингу (Ollama), а не асистента (Gemini).

        Args:
            description: Текст опису оголошення

        Returns:
            Dict з ключем "objects" — масив об'єктів (land_plot, building, premises)
        """
        if not description or not description.strip():
            return {"objects": []}
        try:
            from config.config_loader import get_config_loader
            loader = get_config_loader()
            template = loader.get_prompt("real_estate_objects_parsing")
            if not template:
                return {"objects": []}
            prompt = template.format(description=description)
        except Exception:
            return {"objects": []}
        # ОНМ та парсинг — через провайдер парсингу (Ollama), не асистента
        parsing_provider = getattr(self, "provider", None)
        if parsing_provider and hasattr(parsing_provider, "generate_text"):
            raw = parsing_provider.generate_text(prompt, system_prompt=None, temperature=0.0)
            log_svc = self._get_logging()
            if log_svc and raw:
                try:
                    usage = getattr(parsing_provider, "_last_usage", None)
                    meta = {"desc_preview": (description or "")[:80] + ("..." if len(description or "") > 80 else "")}
                    if isinstance(usage, dict):
                        meta["input_tokens"] = usage.get("input_tokens", 0)
                        meta["output_tokens"] = usage.get("output_tokens", 0)
                    runtime_meta = getattr(parsing_provider, "_last_runtime_meta", None)
                    if isinstance(runtime_meta, dict):
                        meta.update(runtime_meta)
                    log_svc.log_api_usage(
                        service="llm",
                        source="llm_service.parse_real_estate_objects",
                        from_cache=False,
                        metadata=meta,
                    )
                    req_text = getattr(parsing_provider, "_last_request_text", None) or ""
                    resp_text = getattr(parsing_provider, "_last_response_text", None) or ""
                    log_svc.log_llm_exchange(
                        request_text=req_text,
                        response_text=resp_text,
                        input_tokens=meta.get("input_tokens", 0),
                        output_tokens=meta.get("output_tokens", 0),
                        source="llm_service.parse_real_estate_objects",
                        provider=(getattr(self.settings, "llm_parsing_provider", None) or "ollama"),
                        duration_ms=meta.get("duration_ms"),
                    )
                except Exception as e:
                    logger.warning("Не вдалося записати llm_exchange (parse_real_estate_objects): %s", e)
        else:
            raw = self.generate_text(prompt, temperature=0.0)
        if not raw or not raw.strip():
            return {"objects": []}
        json_text = self._extract_json_from_intent_response(raw)
        if not json_text:
            return {"objects": []}
        try:
            data = json.loads(json_text)
            objects = data.get("objects")
            if not isinstance(objects, list):
                return {"objects": []}
            return {"objects": objects}
        except json.JSONDecodeError:
            return {"objects": []}

    def parse_olx_price_recovery(self, description: str) -> Dict[str, Any]:
        """
        Повторно витягує ціну OLX-оголошення спеціальним промптом.
        Використовується коли базовий парсер/regex міг сплутати ціну з кадастровим номером.
        """
        empty = {"price_value": None, "currency": "", "price_text": "", "cadastral_number": ""}
        if not description or not description.strip():
            return empty

        try:
            from config.config_loader import get_config_loader
            loader = get_config_loader()
            template = loader.get_prompt("olx_price_recovery")
            if not template:
                return empty
            prompt = template.format(description=description)
        except Exception:
            return empty

        parsing_provider = getattr(self, "provider", None)
        if parsing_provider and hasattr(parsing_provider, "generate_text"):
            raw = parsing_provider.generate_text(prompt, system_prompt=None, temperature=0.0)
            log_svc = self._get_logging()
            if log_svc and raw:
                try:
                    usage = getattr(parsing_provider, "_last_usage", None)
                    meta = {"desc_preview": (description or "")[:80] + ("..." if len(description or "") > 80 else "")}
                    if isinstance(usage, dict):
                        meta["input_tokens"] = usage.get("input_tokens", 0)
                        meta["output_tokens"] = usage.get("output_tokens", 0)
                    runtime_meta = getattr(parsing_provider, "_last_runtime_meta", None)
                    if isinstance(runtime_meta, dict):
                        meta.update(runtime_meta)
                    log_svc.log_api_usage(
                        service="llm",
                        source="llm_service.parse_olx_price_recovery",
                        from_cache=False,
                        metadata=meta,
                    )
                    req_text = getattr(parsing_provider, "_last_request_text", None) or ""
                    resp_text = getattr(parsing_provider, "_last_response_text", None) or ""
                    log_svc.log_llm_exchange(
                        request_text=req_text,
                        response_text=resp_text,
                        input_tokens=meta.get("input_tokens", 0),
                        output_tokens=meta.get("output_tokens", 0),
                        source="llm_service.parse_olx_price_recovery",
                        provider=(getattr(self.settings, "llm_parsing_provider", None) or "ollama"),
                        duration_ms=meta.get("duration_ms"),
                    )
                except Exception as e:
                    logger.warning("Не вдалося записати llm_exchange (parse_olx_price_recovery): %s", e)
        else:
            raw = self.generate_text(prompt, temperature=0.0, _caller="llm_service.parse_olx_price_recovery")

        if not raw or not raw.strip():
            return empty
        json_text = self._extract_json_from_intent_response(raw)
        if not json_text:
            return empty

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            return empty
        if not isinstance(data, dict):
            return empty

        price_value = data.get("price_value")
        if isinstance(price_value, str):
            cleaned = (
                price_value.replace("\u00a0", "")
                .replace("\u202f", "")
                .replace(" ", "")
                .replace(",", ".")
            )
            try:
                price_value = float(cleaned)
            except (TypeError, ValueError):
                price_value = None
        elif isinstance(price_value, (int, float)):
            price_value = float(price_value)
        else:
            price_value = None
        if isinstance(price_value, float) and price_value <= 0:
            price_value = None

        currency = str(data.get("currency") or "").strip().upper()
        if currency not in ("UAH", "USD", "EUR"):
            currency = ""

        price_text = str(data.get("price_text") or "").strip()
        cadastral_number = str(data.get("cadastral_number") or "").strip()
        return {
            "price_value": price_value,
            "currency": currency,
            "price_text": price_text,
            "cadastral_number": cadastral_number,
        }

    def extract_intent_for_routing(
        self,
        user_query: str,
        context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Визначає намір користувача для маршрутизації (без tools, тільки JSON).
        Повертає словник: intent, confidence, опційно analysis_intent.
        """
        if not user_query or not user_query.strip():
            return {"intent": "query", "confidence": 0.0}

        prompt = self._create_intent_extraction_prompt(user_query, context)
        if not hasattr(self._assistant_provider, "generate_text"):
            return {"intent": "query", "confidence": 0.5}

        try:
            log_svc = self._get_logging()
            raw = self._assistant_provider.generate_text(
                prompt,
                system_prompt="You are an intent classifier. Return only valid JSON, no explanations. All string values in the JSON (e.g. reasoning, templates) must be in Ukrainian.",
                temperature=0.0,
            )
            if log_svc:
                try:
                    usage = getattr(self._assistant_provider, "_last_usage", None)
                    meta = {"query_preview": (user_query or "")[:80] + ("..." if len(user_query or "") > 80 else "")}
                    if isinstance(usage, dict):
                        meta["input_tokens"] = usage.get("input_tokens", 0)
                        meta["output_tokens"] = usage.get("output_tokens", 0)
                    runtime_meta = getattr(self._assistant_provider, "_last_runtime_meta", None)
                    if isinstance(runtime_meta, dict):
                        meta.update(runtime_meta)
                    log_svc.log_api_usage(
                        service="llm",
                        source="llm_service.extract_intent_for_routing",
                        from_cache=False,
                        metadata=meta,
                    )
                    req_text = getattr(self._assistant_provider, "_last_request_text", None) or ""
                    resp_text = getattr(self._assistant_provider, "_last_response_text", None) or ""
                    log_svc.log_llm_exchange(
                        request_text=req_text,
                        response_text=resp_text,
                        input_tokens=meta.get("input_tokens", 0),
                        output_tokens=meta.get("output_tokens", 0),
                        source="llm_service.extract_intent_for_routing",
                        provider=(getattr(self.settings, "llm_assistant_provider", None) or "gemini"),
                        duration_ms=meta.get("duration_ms"),
                    )
                except Exception as e:
                    logger.warning("Не вдалося записати llm_exchange (extract_intent): %s", e)
        except Exception:
            return {"intent": "query", "confidence": 0.5}

        if not raw:
            return {"intent": "query", "confidence": 0.5}

        json_text = self._extract_json_from_intent_response(raw)
        if not json_text:
            return {"intent": "query", "confidence": 0.5}

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            return {"intent": "query", "confidence": 0.5}

        return self._normalize_intent_response(data, user_query)

    def generate_text(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
        _caller: Optional[str] = None,
    ) -> str:
        """
        Генерує текст за промптом (для плану аналітики тощо). Повертає порожній рядок, якщо провайдер не підтримує.
        """
        if not hasattr(self._assistant_provider, "generate_text"):
            return ""
        log_svc = self._get_logging()
        try:
            raw = self._assistant_provider.generate_text(
                prompt,
                system_prompt=system_prompt,
                temperature=temperature,
            )
            if log_svc:
                try:
                    usage = getattr(self._assistant_provider, "_last_usage", None)
                    meta = {"prompt_preview": (prompt or "")[:80] + ("..." if len(prompt or "") > 80 else "")}
                    if _caller:
                        meta["caller"] = _caller
                    if isinstance(usage, dict):
                        meta["input_tokens"] = usage.get("input_tokens", 0)
                        meta["output_tokens"] = usage.get("output_tokens", 0)
                    runtime_meta = getattr(self._assistant_provider, "_last_runtime_meta", None)
                    if isinstance(runtime_meta, dict):
                        meta.update(runtime_meta)
                    log_svc.log_api_usage(
                        service="llm",
                        source=_caller or "llm_service.generate_text",
                        from_cache=False,
                        metadata=meta,
                    )
                    req_text = getattr(self._assistant_provider, "_last_request_text", None) or ""
                    resp_text = getattr(self._assistant_provider, "_last_response_text", None) or ""
                    log_svc.log_llm_exchange(
                        request_text=req_text,
                        response_text=resp_text,
                        input_tokens=meta.get("input_tokens", 0),
                        output_tokens=meta.get("output_tokens", 0),
                        source=_caller or "llm_service.generate_text",
                        provider=(getattr(self.settings, "llm_assistant_provider", None) or "gemini"),
                        duration_ms=meta.get("duration_ms"),
                    )
                except Exception as e:
                    logger.warning("Не вдалося записати llm_exchange (generate_text): %s", e)
            return raw
        except Exception:
            return ""

    def _create_intent_extraction_prompt(self, user_query: str, context: Optional[str]) -> str:
        """Prompt for LLM Intent Extractor. Instructions in English; output JSON string values in Ukrainian."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        date_ctx = f"Current date/time (UTC): {now.isoformat()}. Periods: last 1 day = last_1_day, last 7 days = last_7_days, last 30 days = last_30_days."
        ctx_block = f"\nConversation context (brief):\n{context[:800]}" if context else ""
        return f"""Determine the user's intent from the query. {date_ctx}{ctx_block}

## User query:
{user_query[:2000]}

## Task:
Return ONLY one JSON object without comments. All text values in the JSON (e.g. in reasoning or analysis_intent filters) must be in Ukrainian.
Fields:
- intent: one of "report_last_day", "report_last_week", "export_data", "query", "analytical_query"
- confidence: number 0–1 (classification confidence)

If intent = "analytical_query", add object analysis_intent with:
- entity: "olx_listings" or "prozorro_auctions"
- time_range: "last_1_day" | "last_7_days" | "last_30_days" or null
- dimensions: array ["location"] | ["region"] | ["city"] | ["date"] | ["property_type"] or []
- filters: object with optional city (string or array), region (string or array), property_type (string or array) — use Ukrainian place names where applicable
- metrics: array of {{ "field": "price"|"count"|..., "aggregation": "top"|"count"|"avg"|"sum"|"distribution"|"trend", "order": "asc"|"desc", "limit": number or null }}
- presentation: "list" | "table" | "chart" or null
- multi_step: true only if the query requires comparing two sources (OLX and ProZorro) or multi-step analytics; otherwise false or omit

Examples:
- "звіт за добу по Києву" -> intent: "report_last_day", confidence: 0.95
- "топ-10 найдорожчих оголошень OLX за тиждень по Києву" -> intent: "analytical_query", confidence: 0.85, analysis_intent: {{ entity: "olx_listings", time_range: "last_7_days", dimensions: ["location"], filters: {{ city: ["Київ"] }}, metrics: [{{ field: "price", aggregation: "top", order: "desc", limit: 10 }}], presentation: "list" }}
- "скільки аукціонів за місяць" -> intent: "analytical_query", confidence: 0.8, analysis_intent: {{ entity: "prozorro_auctions", time_range: "last_30_days", metrics: [{{ field: "count", aggregation: "count" }}], presentation: "list" }}
- "привіт" or unclear -> intent: "query", confidence: 0.3"""

    def _extract_json_from_intent_response(self, text: str) -> str:
        """Витягує JSON з відповіді (аналог _extract_json_from_response у провайдерів)."""
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith("```"):
                    if not in_json:
                        in_json = True
                    else:
                        break
                    continue
                if in_json:
                    json_lines.append(line)
            return "\n".join(json_lines)
        if "{" in text:
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return text[start : end + 1]
        return text

    def _normalize_intent_response(self, data: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """Нормалізує відповідь LLM до формату інтерпретатора."""
        intent = data.get("intent")
        if intent not in ("report_last_day", "report_last_week", "export_data", "query", "analytical_query"):
            intent = "query"
        confidence = float(data.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))
        out = {"intent": intent, "confidence": confidence}
        if intent == "analytical_query" and isinstance(data.get("analysis_intent"), dict):
            out["analysis_intent"] = data["analysis_intent"]
        return out
