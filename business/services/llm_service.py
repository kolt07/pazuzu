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
        try:
            from config.config_loader import get_config_loader
            loader = get_config_loader()
            template = loader.get_parsing_template()
            if template:
                return template.format(description=description)
        except Exception:
            pass
        # Fallback — захардкодений промпт (legacy)
        return f"""Проаналізуй наступний опис аукціону нерухомості та витягни структуровану інформацію.
Якщо інформація відсутня або невизначена, поверни порожнє значення або null.

Опис:
{description}

Витягни та поверни JSON з наступними полями:
- cadastral_number: кадастровий номер, номер земельного кадастру (строка, якщо є). 
  Шукай формати типу "6320685503:03:000:0202" або подібні. Якщо є кілька номерів - використовуй основний.
- building_area_sqm: площа нерухомості (будівель, споруд, приміщень, квартир) в квадратних метрах (число, якщо є).
  Це НЕ площа земельної ділянки — тільки площа будівель/приміщень. Шукай у ВСЬОМУ тексті: описі, параметрах, заголовку.
  Фрази для пошуку: "площа", "площею", "загальна площа", "житлова площа", "корисна площа", "площа будівлі", "площа об'єкта",
  "площа приміщень", "площа квартири", "площа будинку", "площа приміщення", "кв.м", "м²", "м2", "кв м", "квадратних метрів".
  У параметрах (наприклад "Площа: 65 м²") — обов'язково витягуй. У заголовку ("3-к.кв. 65 м²") — також.
  Формати чисел: "956,7 м²" -> 956.7; "25 659,90 кв.м" -> 25659.90 (пробіли як роздільник тисяч — видаляй);
  "65м²", "65 м2", "65 кв.м" -> 65; "1 234.5" (крапка/кома як десяткова) -> 1234.5.
  Якщо в гектарах (площа будівлі) — конвертуй: 1 га = 10000 м². Якщо в сотках — 1 сотка = 100 м².
  Кілька значень площі нерухомості — СУМУЙ (наприклад "2,2 кв.м" і "19,8 кв.м" -> 22.0).
  Якщо вказана тільки площа землі без будівель — залишай порожнім. Якщо є і будівля, і ділянка — витягуй обидві окремо.
- land_area_ha: площа земельної ділянки в гектарах (число, якщо є).
  Це площа ЗЕМЛІ, не будівель. Шукай у всьому тексті.
  Фрази: "земельна ділянка", "площа ділянки", "площа землі", "на земельній ділянці", "га", "гектар", "соток", "соток землі".
  Гектари — як є ("5,1545 га" -> 5.1545). Квадратні метри землі — конвертуй: 10000 м² = 1 га. Сотки — 100 соток = 1 га.
  Кілька значень — СУМУЙ. Якщо тільки площа будівель — залишай порожнім.
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
- tags: масив тегів (рядків) для фільтрації оголошень. Кожен тег — короткий ідентифікатор у нижньому регістрі. Витягуй усі релевантні:
  * Призначення/тип об'єкта: крамниця, аптека, офіс, склад, кафе, ресторан, виробництво, складське приміщення, коворкінг, логістика, автосервіс, СТО, паркінг, гараж, готель, склад-холодильник тощо — якщо в тексті явно або з контексту зрозуміло.
  * Комунікації як окремі теги (якщо згадані): газ, вода, електрика, світло, каналізація, опалення, інтернет, вентиляція. Якщо "відсутні" — не додавай теги комунікацій.
  * Інше: ремонт (якщо згадано стан ремонту), паркінг (якщо є), під\'їзд (якщо згадано).
  Приклад: приміщення під крамницю, газ, вода, світло, каналізація → tags: ["крамниця", "газ", "вода", "електрика", "каналізація"].
  Не вигадуй теги — лише з того, що є в тексті. Якщо нічого не підходить — порожній масив [].
- arrests_info: інформація про обтяження майна (арешти) у форматі: "Арешт 1: Видав ХХХХХ, Дата: УУУУ, Можливе зняття так/ні" 
  (якщо є декілька арештів - кожен на окремому рядку, наприклад: "Арешт 1: ...\\nАрешт 2: ...")
  Якщо в тексті є "не зареєстровано" або "відсутні" - пиши "відсутні".

ВАЖЛИВО:
- Уважно читай весь текст, включаючи деталі про адресу, площу, кадастровий номер.
- Площа нерухомості (building_area_sqm) та площа землі (land_area_ha) — різні речі. Не плутай: будівля/приміщення -> building_area_sqm; ділянка/земля -> land_area_ha.
- Шукай площу в усіх блоках: "Параметри об'єкта", "Повний опис", "Заголовок", "Локація". Часто площа є в параметрах типу "Площа: 65" або "Загальна площа: 120 м²".
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
                            # Якщо досягнуто максимум спроб для цієї моделі, пробуємо наступну
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
                        elif any(x in unit_lower for x in ['сотка', 'соток']):
                            # Сотки можуть бути і для землі, і для нерухомості - за значенням визначаємо
                            if area_value < 100:  # Швидше за все це гектари (менше 1 га)
                                land_area_ha = area_value * 0.01
                            else:  # Швидше за все це м²
                                building_area_sqm = area_value * 100
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
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'building_area_sqm': building_area_sqm if building_area_sqm else '',
            'land_area_ha': land_area_ha if land_area_ha else '',
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
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=full_content,
                config={"temperature": temperature},
            )
            if not hasattr(response, 'text') or response.text is None:
                return ""
            return str(response.text).strip()
        except Exception:
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
                        elif any(x in unit_lower for x in ['сотка', 'соток']):
                            # Сотки можуть бути і для землі, і для нерухомості - за значенням визначаємо
                            if area_value < 100:  # Швидше за все це гектари (менше 1 га)
                                land_area_ha = area_value * 0.01
                            else:  # Швидше за все це м²
                                building_area_sqm = area_value * 100
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
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'building_area_sqm': building_area_sqm if building_area_sqm else '',
            'land_area_ha': land_area_ha if land_area_ha else '',
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
                        elif any(x in unit_lower for x in ['сотка', 'соток']):
                            # Сотки можуть бути і для землі, і для нерухомості - за значенням визначаємо
                            if area_value < 100:  # Швидше за все це гектари (менше 1 га)
                                land_area_ha = area_value * 0.01
                            else:  # Швидше за все це м²
                                building_area_sqm = area_value * 100
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
        return {
            'cadastral_number': result.get('cadastral_number', ''),
            'building_area_sqm': building_area_sqm if building_area_sqm else '',
            'land_area_ha': land_area_ha if land_area_ha else '',
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
            'addresses': [],  # Порожній масив адрес
            'floor': '',
            'property_type': '',
            'utilities': '',
            'tags': [],
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
        if not hasattr(self.provider, "generate_text"):
            return {"intent": "query", "confidence": 0.5}

        try:
            raw = self.provider.generate_text(
                prompt,
                system_prompt="Ти класифікатор намірів. Повертай тільки валідний JSON без пояснень.",
                temperature=0.0,
            )
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
    ) -> str:
        """
        Генерує текст за промптом (для плану аналітики тощо). Повертає порожній рядок, якщо провайдер не підтримує.
        """
        if not hasattr(self.provider, "generate_text"):
            return ""
        try:
            return self.provider.generate_text(
                prompt,
                system_prompt=system_prompt,
                temperature=temperature,
            )
        except Exception:
            return ""

    def _create_intent_extraction_prompt(self, user_query: str, context: Optional[str]) -> str:
        """Промпт для LLM Intent Extractor."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        date_ctx = f"Поточна дата/час (UTC): {now.isoformat()}. Періоди: остання добу = last_1_day, останні 7 днів = last_7_days, останні 30 днів = last_30_days."
        ctx_block = f"\nКонтекст розмови (коротко):\n{context[:800]}" if context else ""
        return f"""Визнач намір користувача за запитом. {date_ctx}{ctx_block}

Запит користувача:
{user_query[:2000]}

Поверни ТІЛЬКИ один JSON-об'єкт без коментарів з полями:
- intent: один з "report_last_day", "report_last_week", "export_data", "query", "analytical_query"
- confidence: число від 0 до 1 (впевненість у класифікації)

Якщо intent = "analytical_query", додай об'єкт analysis_intent з полями:
- entity: "olx_listings" або "prozorro_auctions"
- time_range: "last_1_day" | "last_7_days" | "last_30_days" або null
- dimensions: масив ["location"] | ["region"] | ["city"] | ["date"] | ["property_type"] або []
- filters: об'єкт з опційними ключами city (рядок або масив), region (рядок або масив), property_type (рядок або масив)
- metrics: масив об'єктів, кожен: {{ "field": "price"|"count"|..., "aggregation": "top"|"count"|"avg"|"sum"|"distribution"|"trend", "order": "asc"|"desc", "limit": число або null }}
- presentation: "list" | "table" | "chart" або null
- multi_step: true лише якщо запит вимагає порівняння двох джерел (OLX і ProZorro) або багатокрокової аналітики (наприклад "порівняй ціни OLX і ProZorro по регіонах"); інакше false або не вказуй

Приклади намірів:
- "звіт за добу по Києву" -> intent: "report_last_day", confidence: 0.95
- "топ-10 найдорожчих оголошень OLX за тиждень по Києву" -> intent: "analytical_query", confidence: 0.85, analysis_intent: {{ entity: "olx_listings", time_range: "last_7_days", dimensions: ["location"], filters: {{ city: ["Київ"] }}, metrics: [{{ field: "price", aggregation: "top", order: "desc", limit: 10 }}], presentation: "list" }}
- "скільки аукціонів за місяць" -> intent: "analytical_query", confidence: 0.8, analysis_intent: {{ entity: "prozorro_auctions", time_range: "last_30_days", metrics: [{{ field: "count", aggregation: "count" }}], presentation: "list" }}
- "привіт" або незрозуміло -> intent: "query", confidence: 0.3"""

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
