# -*- coding: utf-8 -*-
"""
Сервіс для роботи з LLM API (Gemini, ChatGPT, Claude).
"""

import time
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from config.settings import Settings


class RateLimiter:
    """Клас для обмеження швидкості викликів API."""
    
    def __init__(self, calls_per_minute: int = 15):
        """
        Ініціалізація rate limiter.
        
        Args:
            calls_per_minute: Максимальна кількість викликів за хвилину
        """
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # Мінімальний інтервал між викликами в секундах
        self.last_call_time: Optional[float] = None
    
    def wait_if_needed(self) -> None:
        """Чекає, якщо потрібно, щоб не перевищити ліміт викликів."""
        if self.last_call_time is not None:
            elapsed = time.time() - self.last_call_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
        
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
    
    def _create_parsing_prompt(self, description: str) -> str:
        """
        Створює промпт для парсингу опису аукціону.
        
        Args:
            description: Текст опису аукціону
            
        Returns:
            Текст промпту
        """
        return f"""Проаналізуй наступний опис аукціону нерухомості та витягни структуровану інформацію.
Якщо інформація відсутня або невизначена, поверни порожнє значення або null.

Опис:
{description}

Витягни та поверни JSON з наступними полями:
- cadastral_number: кадастровий номер, номер земельного кадастру (строка, якщо є). 
  Шукай формати типу "6320685503:03:000:0202" або подібні. Якщо є кілька номерів - використовуй основний.
- area: площа (число, якщо є). Шукай числа з одиницями виміру (га, м², сотка). 
  Якщо площа вказана в гектарах - конвертуй в число (наприклад, "0,5296 га" -> 0.5296).
- area_unit: одиниця вимірювання площі (га, м², сотка тощо). Якщо в тексті є "га" - пиши "гектар", якщо "м²" - пиши "м²".
- addresses: масив адрес (якщо в тексті є кілька адрес - витягни всі). Кожна адреса - об'єкт з полями:
  * region: область у форматі "Волинська", "Тернопільська", "Харківська" тощо (без скорочень, без додаткових слів). 
    м. Київ та м. Симферополь НЕ входять в склад областей - для них залишай порожнє значення.
    Крим пиши як "АР Крим".
    Якщо в тексті є "Харківська область" - пиши "Харківська", якщо "Лозівський район" - це Харківська область.
    Якщо в тексті не зустрічається назва області, але за назвою міста або іншого топоніма можна виявити, в якій він області знаходиться - доповни інформацію про область.
  * district: район (якщо є, наприклад "Лозівський", "Бориспільський" тощо)
  * settlement_type: скорочено тип населеного пункту (м., с., смт., с-ще тощо) - якщо є в тексті
  * settlement: населений пункт/місто/село БЕЗ приставок м., с. тощо, з великої літери (наприклад, "Верхньоводяне", "Київ", "Львів").
    Якщо є "с. Верхньоводяне" - пиши "Верхньоводяне", якщо "м. Київ" - пиши "Київ".
    Якщо є "на території с. Верхньоводяне" - витягни "Верхньоводяне".
    Може бути також топонім типу "сільрада", "міськрада", "районна рада" тощо - витягни як є.
  * settlement_district: район населеного пункту (якщо є, наприклад "Шевченківський район м. Києва")
  * street_type: тип вулиці (вул., просп., бул., пров., пл. тощо). Якщо в тексті є "вул." - пиши "вул."
  * street: назва вулиці (без типу, наприклад "Незалежності" замість "вул. Незалежності")
  * building: номер будинку/будівлі (тільки номер, наприклад "39" або "27")
  * building_part: номер блоку/корпусу (якщо є, наприклад "корпус А", "блок 1" тощо)
  * room: номер приміщення (офіс, квартира, тощо, якщо є, наприклад "кв. 5", "офіс 12")
- floor: поверх (якщо є, тільки для будівель)
- property_type: тип нерухомості зі списку: "Земля під будівництво", "Землі с/г призначення", "Нерухомість", "інше"
  Якщо в описі є "земельна ділянка" або "землі житлової забудови" - це "Земля під будівництво".
- utilities: підведені комунікації (через кому, наприклад: електрика, вода, газ, опалення).
  Якщо в тексті є "електропостачання" - пиши "електрика", якщо "водопостачання" - пиши "вода".
  Якщо вказано "відсутні" або "не підведені" - пиши "відсутні".
- arrests_info: інформація про обтяження майна (арешти) у форматі: "Арешт 1: Видав ХХХХХ, Дата: УУУУ, Можливе зняття так/ні" 
  (якщо є декілька арештів - кожен на окремому рядку, наприклад: "Арешт 1: ...\\nАрешт 2: ...")
  Якщо в тексті є "не зареєстровано" або "відсутні" - пиши "відсутні".

ВАЖЛИВО:
- Уважно читай весь текст, включаючи деталі про адресу, площу, кадастровий номер.
- Якщо інформація вказана в різних форматах (наприклад, "0,5296 га" і "0.5296 га") - використовуй той, що згадується першим.
- Для адреси: якщо є "Харківська область, Лозівський район, на території с. Верхньоводяне, вул. Центральна, 15" - 
  витягни як: {{"region": "Харківська", "district": "Лозівський", "settlement_type": "с.", "settlement": "Верхньоводяне", "street_type": "вул.", "street": "Центральна", "building": "15"}}
- Якщо в тексті є кілька адрес - витягни всі в масив addresses.
- Якщо адреса неповна, спробуй доповнити її на основі доступної інформації.
- Використовуй виключно ту інформацію, яка є в описі аукціону, або таку, яку можна отримати на базі інформації з опису аукціону. Не вигадуй іншу інформацію.
- Поверни ТІЛЬКИ валідний JSON без додаткових пояснень."""


class GeminiLLMProvider(BaseLLMProvider):
    """Провайдер для Google Gemini API."""
    
    def __init__(self, api_key: str, rate_limiter: RateLimiter, model_name: str = 'gemini-2.5-flash'):
        super().__init__(api_key, rate_limiter)
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
        """Валідує модель, спробувавши кілька варіантів."""
        # Список моделей для спроби (в порядку пріоритету)
        models_to_try = [
            preferred_model,
            'gemini-2.5-flash',
            'gemini-2.5-pro',
            'gemini-2.5-flash-lite',
            'gemini-pro'  # Стара модель на випадок, якщо нові недоступні
        ]
        
        # Видаляємо дублікати, зберігаючи порядок
        seen = set()
        self._available_models = [m for m in models_to_try if not (m in seen or seen.add(m))]
        
        # Зберігаємо першу модель як активну (перевірка доступності буде при першому запиті)
        self.model_name = self._available_models[0]
    
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """Парсить опис аукціону через Gemini API."""
        if not description or not description.strip():
            return self._empty_result()
        
        self.rate_limiter.wait_if_needed()
        
        max_retries = 3
        retry_delay = 5  # Початкова затримка в секундах
        
        for attempt in range(max_retries):
            try:
                prompt = self._create_parsing_prompt(description)
                
                # Виконуємо запит через новий API
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt
                )
                
                # Витягуємо JSON з відповіді
                # Перевіряємо, чи response.text існує та є рядком
                if not hasattr(response, 'text') or response.text is None:
                    raise ValueError("Відповідь від Gemini не містить тексту")
                
                response_text = str(response.text).strip()
                
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
                
                return self._normalize_result(result)
            except KeyboardInterrupt:
                # Переривання користувача - не обробляємо, просто пробрасуємо далі
                raise
            except Exception as e:
                error_str = str(e)
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
                        print(f"Помилка при парсингу через Gemini (досягнуто максимум спроб): {e}")
                        return self._empty_result()
                else:
                    # Інша помилка - не повторюємо
                    print(f"Помилка при парсингу через Gemini: {e}")
                    return self._empty_result()
        
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
        
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'area': result.get('area', ''),
            'area_unit': result.get('area_unit', ''),
            'addresses': addresses,  # Масив адрес
            'floor': result.get('floor', ''),
            'property_type': result.get('property_type', ''),
            'utilities': result.get('utilities', ''),
            'arrests_info': result.get('arrests_info', '')
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            'cadastral_number': '',
            'area': '',
            'area_unit': '',
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
            'arrests_info': ''
        }


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
        
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'area': result.get('area', ''),
            'area_unit': result.get('area_unit', ''),
            'addresses': addresses,  # Масив адрес
            'floor': result.get('floor', ''),
            'property_type': result.get('property_type', ''),
            'utilities': result.get('utilities', ''),
            'arrests_info': result.get('arrests_info', '')
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            'cadastral_number': '',
            'area': '',
            'area_unit': '',
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
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
        
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'area': result.get('area', ''),
            'area_unit': result.get('area_unit', ''),
            'addresses': addresses,  # Масив адрес
            'floor': result.get('floor', ''),
            'property_type': result.get('property_type', ''),
            'utilities': result.get('utilities', ''),
            'arrests_info': result.get('arrests_info', '')
        }
    
    def _empty_result(self) -> Dict[str, Any]:
        """Повертає порожній результат."""
        return {
            'cadastral_number': '',
            'area': '',
            'area_unit': '',
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
            'arrests_info': ''
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
        self.provider = self._create_provider()
    
    def _create_provider(self) -> BaseLLMProvider:
        """Створює провайдера на основі налаштувань."""
        provider_name = self.settings.llm_provider.lower()
        api_key = self.settings.llm_api_keys.get(provider_name, '')
        
        if not api_key:
            raise ValueError(f"API ключ для провайдера {provider_name} не вказано в конфігурації")
        
        if provider_name == 'gemini':
            model_name = getattr(self.settings, 'llm_model_name', 'gemini-1.5-flash')
            return GeminiLLMProvider(api_key, self.rate_limiter, model_name)
        elif provider_name == 'openai':
            return OpenAILLMProvider(api_key, self.rate_limiter)
        elif provider_name == 'anthropic':
            return AnthropicLLMProvider(api_key, self.rate_limiter)
        else:
            raise ValueError(f"Невідомий провайдер LLM: {provider_name}")
    
    def parse_auction_description(self, description: str) -> Dict[str, Any]:
        """
        Парсить опис аукціону та повертає структуровані дані.
        
        Args:
            description: Текст опису аукціону
            
        Returns:
            Dict з структурованою інформацією
        """
        return self.provider.parse_auction_description(description)
