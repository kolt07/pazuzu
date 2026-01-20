# -*- coding: utf-8 -*-
"""
Сервіс для роботи з API ProZorro.
"""

import requests
import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
import sys
import time

from config.settings import Settings
from transport.dto.prozorro_dto import AuctionDTO, AuctionsResponseDTO
from utils.date_utils import get_date_range, format_datetime_for_api, format_datetime_for_byDateModified
from utils.file_utils import save_json_to_file, save_csv_to_file, save_excel_to_file, generate_json_filename, generate_auction_filename, ensure_directory_exists, merge_excel_files
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService
import yaml


class ProZorroService:
    """Сервіс для отримання оголошень з ProZorro."""

    def __init__(self, settings: Optional[Settings] = None):
        """
        Ініціалізація сервісу.

        Args:
            settings: Налаштування застосунку. Якщо не вказано, створюється новий екземпляр.
        """
        self.settings = settings or Settings()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.settings.user_agent,
            'Accept': 'application/json'
        })
        # Ініціалізація LLM сервісу (може викликати помилку, якщо API ключ не вказано)
        self.llm_service = None
        try:
            self.llm_service = LLMService(self.settings)
        except (ValueError, ImportError) as e:
            print(f"Попередження: LLM сервіс недоступний: {e}")
            print("Парсинг описів через LLM буде пропущено")
        
        # Ініціалізація кешу LLM
        self.llm_cache_service = LLMCacheService()
        
        # Шлях до файлу конфігурації кодів класифікації
        config_dir = Path(__file__).parent.parent.parent / 'config'
        self.classification_codes_config_path = config_dir / 'ProZorro_clasification_codes.yaml'

    def get_real_estate_auctions_by_date_range(self, date_from: datetime, date_to: datetime) -> List[AuctionDTO]:
        """
        Отримує активні аукціони, змінені в конкретному діапазоні дат.
        Використовує ендпоінт /api/search/byDateModified/{date} для ефективного отримання даних.

        Args:
            date_from: Початкова дата діапазону
            date_to: Кінцева дата діапазону

        Returns:
            List[AuctionDTO]: Список активних аукціонів

        Raises:
            requests.RequestException: При помилках HTTP запитів
        """
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=timezone.utc)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=timezone.utc)
        
        return self._get_auctions_internal(date_from, date_to)
    
    def get_real_estate_auctions(self, days: int = 1) -> List[AuctionDTO]:
        """
        Отримує активні аукціони, створені або змінені протягом останніх N днів.
        Використовує ендпоінт /api/search/byDateModified/{date} для отримання даних.
        Фільтрує за dateCreated АБО dateModified в межах заданого діапазону.

        Args:
            days: Кількість днів для виборки (за замовчуванням 1)

        Returns:
            List[AuctionDTO]: Список активних аукціонів

        Raises:
            requests.RequestException: При помилках HTTP запитів
        """
        date_from, date_to = get_date_range(days)
        return self._get_auctions_internal(date_from, date_to)
    
    def _get_auctions_by_date_modified(self, date_from: datetime, date_to: datetime) -> List[AuctionDTO]:
        """
        Отримує аукціони за датою модифікації.
        
        Args:
            date_from: Початкова дата діапазону
            date_to: Кінцева дата діапазону
            
        Returns:
            List[AuctionDTO]: Список аукціонів
        """
        # Починаємо з date_from як початкової дати для пошуку
        current_date = date_from
        params = {
            'limit': 100,  # Максимальний ліміт для ендпоінта byDateModified
        }

        # Використовуємо ендпоінт /api/search/byDateModified/{date} з нової ЦБД
        base_url = f'{self.settings.prozorro_sale_search_api_base_url}/search/byDateModified'
        
        # Обробка відповіді API з підтримкою пагінації
        auctions = []
        errors_count = 0
        filtered_by_date_count = 0
        filtered_by_status_count = 0
        filtered_by_start_date_count = 0
        filtered_by_property_type_count = 0
        all_auctions_processed = 0
        page_count = 0
        
        try:
            while current_date <= date_to:
                page_count += 1
                
                # Форматуємо поточну дату для URL
                date_str = format_datetime_for_byDateModified(current_date)
                url = f'{base_url}/{date_str}'
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.settings.prozorro_api_timeout
                )
                response.raise_for_status()
                
                # Обробка статусу 204 (No Content) - немає даних для повернення
                if response.status_code == 204:
                    print("Немає даних (статус 204 - No Content)")
                    break
                
                # Перевірка, чи є вміст для парсингу
                if not response.text or not response.text.strip():
                    print("Порожня відповідь від API")
                    break
                
                response_data = response.json()
                
                # Отримуємо список аукціонів з відповіді
                auctions_data = []
                if isinstance(response_data, dict):
                    auctions_data = response_data.get('data', []) or response_data.get('procedures', [])
                elif isinstance(response_data, list):
                    auctions_data = response_data
                
                if not auctions_data:
                    print("Немає більше аукціонів для обробки")
                    break
                
                # Обробляємо аукціони на поточній сторінці
                last_auction_on_page = None
                max_date_modified = None
                
                for idx, auction_data in enumerate(auctions_data):
                    all_auctions_processed += 1
                    try:
                        auction_id = auction_data.get('id', '')
                        
                        auction = AuctionDTO.from_dict(auction_data)
                        
                        # Зберігаємо останнє оголошення на сторінці
                        last_auction_on_page = auction
                        
                        # Оновлюємо максимальну дату модифікації
                        if max_date_modified is None or auction.date_modified > max_date_modified:
                            max_date_modified = auction.date_modified
                        
                        # ---- Фільтрація за датою створення АБО датою модифікації ----
                        # Фільтруємо по dateCreated АБО dateModified в межах заданого діапазону
                        date_created = auction.date_created
                        date_modified = auction.date_modified
                        
                        # Перевіряємо, чи хоча б одна з дат входить у діапазон
                        created_in_range = date_from <= date_created <= date_to
                        modified_in_range = date_from <= date_modified <= date_to
                        in_date_range = created_in_range or modified_in_range
                        
                        if in_date_range:
                            # Додаємо до списку без фільтрації (фільтрацію застосуємо пізніше в _get_auctions_internal)
                            auctions.append(auction)
                        else:
                            filtered_by_date_count += 1
                    except (KeyError, ValueError) as e:
                        errors_count += 1
                        if errors_count <= 3:  # Логуємо перші 3 помилки
                            print(f"Помилка обробки аукціону #{idx}: {e}")
                            print(f"  Дані: {auction_data.get('id', 'N/A')}")
                        continue
                
                # Перевіряємо, чи потрібно продовжувати
                if max_date_modified is None:
                    # Якщо не знайшли жодної дати, зупиняємось
                    break
                
                if max_date_modified >= date_to:
                    # Якщо максимальна дата досягнула або перевищила date_to, зупиняємось
                    break
                
                # Оновлюємо current_date для наступної ітерації: додаємо 1 мілісекунду до максимальної дати
                current_date = max_date_modified + timedelta(milliseconds=1)
                
                # Невелика пауза для уникнення перевантаження API
                time.sleep(0.1)
            
            print(f"\nСтатистика обробки:")
            print(f"  Оброблено сторінок: {page_count}")
            print(f"  Всього аукціонів оброблено: {all_auctions_processed}")
            print(f"  Відфільтровано за датою (created/modified): {filtered_by_date_count}")
            print(f"  Відфільтровано за статусом: {filtered_by_status_count}")
            print(f"  Відфільтровано за датою старту торгів: {filtered_by_start_date_count}")
            print(f"  Відфільтровано за типом (не продаж нерухомості): {filtered_by_property_type_count}")
            print(f"  Успішно оброблено: {len(auctions)}")
            print(f"  Помилок обробки: {errors_count}")

            return auctions

        except requests.exceptions.RequestException as e:
            print(f"Помилка при запиті до API ProZorro.Sale (byDateModified): {e}")
            raise
    
    def _get_auctions_internal(self, date_from: datetime, date_to: datetime) -> List[AuctionDTO]:
        """
        Внутрішній метод для отримання аукціонів у діапазоні дат.
        Отримує аукціони за dateModified через ендпоінт /api/search/byDateModified/{date}.
        Фільтрує за dateCreated АБО dateModified в межах заданого діапазону.
        
        Args:
            date_from: Початкова дата діапазону
            date_to: Кінцева дата діапазону
            
        Returns:
            List[AuctionDTO]: Список активних аукціонів
        """
        
        print(f"Діапазон дат для фільтрації:")
        print(f"  Від: {date_from} ({format_datetime_for_api(date_from)})")
        print(f"  До: {date_to} ({format_datetime_for_api(date_to)})")
        
        # Отримуємо аукціони за dateModified
        auctions_modified = self._get_auctions_by_date_modified(date_from, date_to)
        
        # Об'єднуємо результати, видаляючи дублікати за ID
        all_auctions_dict = {}
        empty_id_count = 0
        
        # Додаємо аукціони, видаляючи дублікати за ID
        for auction in auctions_modified:
            if not auction.id or auction.id.strip() == '':
                empty_id_count += 1
                # Якщо ID порожній, використовуємо унікальний ключ
                all_auctions_dict[f'_empty_{len(all_auctions_dict)}'] = auction
            else:
                # Якщо аукціон з таким ID вже є, залишаємо перший
                if auction.id not in all_auctions_dict:
                    all_auctions_dict[auction.id] = auction
        
        # Конвертуємо назад в список
        all_auctions = list(all_auctions_dict.values())
        
        print(f"\nОб'єднано результати:")
        print(f"  За dateModified: {len(auctions_modified)}")
        print(f"  Аукціонів з порожнім ID: {empty_id_count}")
        print(f"  Всього унікальних (до фільтрації): {len(all_auctions)}")
        
        # Застосовуємо фільтрацію до об'єднаних результатів
        filtered_auctions = []
        filtered_by_date_count = 0
        filtered_by_status_count = 0
        filtered_by_start_date_count = 0
        filtered_by_property_type_count = 0
        
        for auction in all_auctions:
            # Перевірка дати (created OR modified)
            date_created = auction.date_created
            date_modified = auction.date_modified
            created_in_range = date_from <= date_created <= date_to
            modified_in_range = date_from <= date_modified <= date_to
            in_date_range = created_in_range or modified_in_range
            
            if not in_date_range:
                filtered_by_date_count += 1
                continue
            
            # Застосовуємо решту фільтрації (дату вже перевірили вище)
            if self._should_include_auction(auction, date_from, date_to, skip_date_check=True):
                filtered_auctions.append(auction)
            else:
                # Підраховуємо причини відсіву для статистики
                if not auction.data:
                    filtered_by_status_count += 1
                else:
                    status = auction.data.get('status', '')
                    active_statuses = ['active', 'active.tendering', 'active.auction', 'active.qualification', 
                                      'active_rectification', 'active_tendering', 'active_auction', 'active_qualification']
                    is_active = any(
                        status.startswith(active_status.replace('_', '.')) or 
                        status == active_status or
                        status.startswith(active_status.replace('.', '_'))
                        for active_status in active_statuses
                    )
                    if not is_active:
                        filtered_by_status_count += 1
                    else:
                        # Перевіряємо інші причини
                        auction_period = auction.data.get('auctionPeriod', {})
                        auction_start_date_str = auction_period.get('startDate')
                        if auction_start_date_str:
                            try:
                                if auction_start_date_str.endswith('Z'):
                                    auction_start_date_str = auction_start_date_str.replace('Z', '+00:00')
                                auction_start_date = datetime.fromisoformat(auction_start_date_str)
                                if auction_start_date.tzinfo:
                                    auction_start_date = auction_start_date.astimezone(timezone.utc)
                                else:
                                    auction_start_date = auction_start_date.replace(tzinfo=timezone.utc)
                                now = datetime.now(timezone.utc)
                                days_since_start = (now - auction_start_date).days
                                if auction_start_date <= now and days_since_start > 7:
                                    filtered_by_start_date_count += 1
                                else:
                                    filtered_by_property_type_count += 1
                            except:
                                filtered_by_start_date_count += 1
                        else:
                            filtered_by_property_type_count += 1
        
        print(f"Після фільтрації:")
        print(f"  Відфільтровано за датою: {filtered_by_date_count}")
        print(f"  Відфільтровано за статусом: {filtered_by_status_count}")
        print(f"  Відфільтровано за датою старту торгів: {filtered_by_start_date_count}")
        print(f"  Відфільтровано за типом: {filtered_by_property_type_count}")
        print(f"  Успішно оброблено: {len(filtered_auctions)}")
        
        return filtered_auctions
    
    def _should_include_auction(self, auction: AuctionDTO, date_from: datetime, date_to: datetime, skip_date_check: bool = False) -> bool:
        """
        Перевіряє, чи потрібно включити аукціон у результат на основі всіх критеріїв фільтрації.
        
        Args:
            auction: Аукціон для перевірки
            date_from: Початкова дата діапазону
            date_to: Кінцева дата діапазону
            skip_date_check: Якщо True, пропускає перевірку дати (вже виконана)
            
        Returns:
            bool: True якщо аукціон має бути включений
        """
        # Перевірка дати (created OR modified) - тільки якщо не пропущено
        if not skip_date_check:
            date_created = auction.date_created
            date_modified = auction.date_modified
            created_in_range = date_from <= date_created <= date_to
            modified_in_range = date_from <= date_modified <= date_to
            in_date_range = created_in_range or modified_in_range
            
            if not in_date_range:
                return False
        
        if not auction.data:
            return False
        
        # Перевірка статусу
        status = auction.data.get('status', '')
        active_statuses = ['active', 'active.tendering', 'active.auction', 'active.qualification', 
                          'active_rectification', 'active_tendering', 'active_auction', 'active_qualification']
        
        is_active = any(
            status.startswith(active_status.replace('_', '.')) or 
            status == active_status or
            status.startswith(active_status.replace('.', '_'))
            for active_status in active_statuses
        )
        
        if not is_active:
            return False
        
        # Перевірка дати старту торгів
        auction_period = auction.data.get('auctionPeriod', {})
        auction_start_date_str = auction_period.get('startDate')
        
        if auction_start_date_str:
            try:
                if auction_start_date_str.endswith('Z'):
                    auction_start_date_str = auction_start_date_str.replace('Z', '+00:00')
                auction_start_date = datetime.fromisoformat(auction_start_date_str)
                if auction_start_date.tzinfo:
                    auction_start_date = auction_start_date.astimezone(timezone.utc)
                else:
                    auction_start_date = auction_start_date.replace(tzinfo=timezone.utc)
                
                now = datetime.now(timezone.utc)
                days_since_start = (now - auction_start_date).days
                
                if auction_start_date > now:
                    pass  # Ще не почався - дозволяємо
                elif days_since_start <= 7:
                    pass  # Завершився недавно - дозволяємо
                else:
                    return False  # Завершився давно - відсіваємо
            except (ValueError, AttributeError):
                return False
        # Якщо немає дати старту, але статус активний - дозволяємо
        
        # Перевірка на PA01-7 в додаткових класифікаторах (відсікаємо такі аукціони)
        items = auction.data.get('items', [])
        if isinstance(items, list):
            for item in items:
                additional_classifications = item.get('additionalClassifications', [])
                if isinstance(additional_classifications, list):
                    for add_class in additional_classifications:
                        add_class_id = add_class.get('id', '')
                        # Якщо знайдено PA01-7 - відсікаємо аукціон
                        if add_class_id == 'PA01-7':
                            return False
        
        # Перевірка класифікатора
        allowed_classification_codes = self.get_allowed_classification_codes()
        
        is_property_sale = False
        if isinstance(items, list) and len(items) > 0:
            for item in items:
                classification = item.get('classification', {})
                if classification:
                    scheme = classification.get('scheme', '')
                    class_id = classification.get('id', '')
                    
                    if scheme == 'CAV' and class_id:
                        for allowed_code in allowed_classification_codes:
                            if class_id.startswith(allowed_code) or class_id == allowed_code:
                                is_property_sale = True
                                break
                        
                        if is_property_sale:
                            break
        
        if not is_property_sale:
            return False
        
        return True
    
    def _is_rental_auction(self, data: Dict[str, Any]) -> bool:
        """
        Перевіряє, чи аукціон стосується аренди.
        
        Args:
            data: Дані аукціону з auction.data
            
        Returns:
            bool: True якщо це аренда, False якщо продаж
        """
        # Перевірка через leaseType
        lease_type = data.get('leaseType') or data.get('lease_type')
        if lease_type:
            return True
        
        # Перевірка через saleType (якщо є saleType і немає leaseType - це продаж)
        sale_type = data.get('saleType') or data.get('sale_type')
        if sale_type and not lease_type:
            return False
        
        # Перевірка через procedureType
        procedure_type = data.get('procedureType') or data.get('procedure_type', '')
        if procedure_type:
            procedure_type_lower = procedure_type.lower()
            # Перевіряємо на наявність слів, що вказують на аренду
            rental_keywords = ['lease', 'rent', 'оренд', 'rental', 'аренд']
            if any(keyword in procedure_type_lower for keyword in rental_keywords):
                return True
        
        # Перевірка через items.additionalClassifications
        items = data.get('items', [])
        if isinstance(items, list):
            for item in items:
                additional_classifications = item.get('additionalClassifications', [])
                if isinstance(additional_classifications, list):
                    for add_class in additional_classifications:
                        add_class_id = add_class.get('id', '')
                        add_class_scheme = add_class.get('scheme', '')
                        
                        # Перевірка на код PA01-7 (відсікаємо такі аукціони)
                        if add_class_id == 'PA01-7':
                            return True
                        
                        # Перевіряємо на коди аренди
                        if add_class_scheme == 'CAV':
                            # Коди аренди зазвичай починаються з 70... або мають інші префікси
                            # Але для точності перевіряємо наявність ключових слів
                            add_class_desc = add_class.get('description', '').lower()
                            if any(keyword in add_class_desc for keyword in ['оренд', 'lease', 'rent']):
                                return True
        
        # Перевірка через title/description (якщо інші методи не спрацювали)
        title = data.get('title', '') or data.get('title_ua', '')
        description = data.get('description', '')
        text_to_check = f"{title} {description}".lower()
        rental_keywords_in_text = ['оренд', 'lease', 'rent', 'оренда', 'оренду']
        if any(keyword in text_to_check for keyword in rental_keywords_in_text):
            # Але перевіряємо, чи не є це продаж (продаж має пріоритет)
            sale_keywords = ['продаж', 'sale', 'продажі', 'продажу']
            if any(keyword in text_to_check for keyword in sale_keywords):
                return False
            return True
        
        # За замовчуванням вважаємо, що це не аренда (продаж)
        return False

    def save_auctions_to_csv(self, auctions: List[AuctionDTO], days: int = 1, output_dir: Optional[str] = None, user_id: Optional[int] = None) -> str:
        """
        Зберігає список аукціонів у Excel файл з вибраними полями.
        Зберігає в каталог "archives".

        Args:
            auctions: Список аукціонів для збереження
            days: Кількість днів виборки (для метаданих)
            output_dir: Директорія для збереження. Якщо не вказано, використовується archives/
            user_id: Ідентифікатор користувача, який сформував файл (опціонально)

        Returns:
            str: Шлях до збереженого файлу
        """
        def ensure_string(value):
            """Конвертує значення в рядок, обробляючи None та інші типи."""
            if value is None:
                return ''
            return str(value)
        
        def format_address(address_street_type: str, address_street: str, address_building: str) -> str:
            """Формує повну адресу з компонентів."""
            parts = []
            if address_street_type:
                parts.append(address_street_type)
            if address_street:
                parts.append(address_street)
            if address_building:
                parts.append(address_building)
            return ', '.join(parts) if parts else ''
        
        def format_date(date_str: str) -> str:
            """Форматує дату у форматі дд.ММ.рррр ГГ:ХХ."""
            if not date_str:
                return ''
            try:
                if date_str.endswith('Z'):
                    date_str = date_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(date_str)
                # Конвертуємо в локальний час, якщо потрібно
                if dt.tzinfo:
                    dt = dt.astimezone(timezone.utc)
                return dt.strftime('%d.%m.%Y %H:%M')
            except (ValueError, AttributeError):
                return str(date_str)
        
        def format_arrests(arrests_data: List[Dict[str, Any]], parsed_arrests_info: str) -> str:
            """Формує інформацію про арешти у правильному форматі."""
            # Якщо є інформація з парсингу, використовуємо її
            if parsed_arrests_info:
                return parsed_arrests_info
            
            # Інакше формуємо з даних API
            if not arrests_data or not isinstance(arrests_data, list):
                return ''
            
            arrests_parts = []
            for idx, arrest in enumerate(arrests_data, 1):
                if isinstance(arrest, dict):
                    # Витягуємо ключові поля
                    restriction_org = arrest.get('restrictionOrganization', '')
                    restriction_date = arrest.get('restrictionDate', '')
                    is_removable = arrest.get('isRemovable', False)
                    
                    # Форматуємо дату
                    date_str = ''
                    if restriction_date:
                        date_str = format_date(restriction_date)
                    
                    # Форматуємо організацію
                    org_str = ''
                    if restriction_org:
                        # Можливо, це об'єкт з полем "Видавник" або просто рядок
                        if isinstance(restriction_org, dict):
                            org_str = restriction_org.get('Видавник', restriction_org.get('name', str(restriction_org)))
                        else:
                            org_str = str(restriction_org)
                    
                    # Формуємо рядок
                    arrest_parts = []
                    if org_str:
                        arrest_parts.append(f"Видав {org_str}")
                    if date_str:
                        arrest_parts.append(f"Дата: {date_str}")
                    arrest_parts.append(f"Можливе зняття {'так' if is_removable else 'ні'}")
                    
                    arrests_parts.append(f"Арешт {idx}: {', '.join(arrest_parts)}")
            
            return '\n'.join(arrests_parts) if arrests_parts else ''
        
        # Використовуємо каталог "archives" за замовчуванням
        if output_dir is None:
            output_dir = 'archives'

        ensure_directory_exists(output_dir)

        filename = generate_auction_filename(
            prefix='prozorro_real_estate_auctions',
            extension='xlsx',
            user_id=user_id,
            days=days
        )
        file_path = f'{output_dir}/{filename}'

        # Порядок колонок: Область, Населений пункт, Адреса, Тип нерухомості, Кадастровий номер,
        # Площа, Стартова ціна, Розмір взносу, Дата торгів, Дата фінальної подачі документів,
        # Мінімальна кількість учасників, Кількість зареєстрованих учасників, Арешти, Опис, Посилання, Код класифікатора
        fieldnames = [
            'address_region',                    # Область
            'address_city',                      # Населений пункт
            'address',                           # Адреса
            'property_type',                     # Тип нерухомості
            'cadastral_number',                  # Кадастровий номер
            'area',                              # Площа
            'base_price',                        # Стартова ціна
            'deposit_amount',                    # Розмір взносу
            'auction_start_date',                # Дата торгів
            'document_submission_deadline',      # Дата фінальної подачі документів
            'min_participants_count',            # Мінімальна кількість учасників
            'participants_count',                # Кількість зареєстрованих учасників
            'arrests_info',                      # Арешти
            'description',                       # Опис
            'auction_url',                       # Посилання
            'classification_code'                # Код класифікатора
        ]
        
        # Українські назви колонок
        column_headers = {
            'address_region': 'Область',
            'address_city': 'Населений пункт',
            'address': 'Адреса',
            'property_type': 'Тип нерухомості',
            'cadastral_number': 'Кадастровий номер',
            'area': 'Площа',
            'base_price': 'Стартова ціна',
            'deposit_amount': 'Розмір взносу',
            'auction_start_date': 'Дата торгів',
            'document_submission_deadline': 'Дата фінальної подачі документів',
            'min_participants_count': 'Мінімальна кількість учасників',
            'participants_count': 'Кількість зареєстрованих учасників',
            'arrests_info': 'Арешти',
            'description': 'Опис',
            'auction_url': 'Посилання',
            'classification_code': 'Код класифікатора'
        }
        
        from tqdm import tqdm
        
        auctions_data = []
        total_auctions = len(auctions)
        print(f"Початок обробки {total_auctions} аукціонів для збереження в Excel...")
        
        # Прогрес-бар для обробки аукціонів
        for auction in tqdm(auctions, desc="Обробка аукціонів", unit="аукціон", ncols=100):
            if not auction.data:
                continue
            
            data = auction.data
            
            # Посилання на аукціон
            auction_id = data.get('auctionId') or data.get('_id') or auction.id
            auction_url = f"https://prozorro.sale/auction/{auction_id}"
            
            # Опис
            description = ''
            if 'description' in data:
                desc_obj = data['description']
                if isinstance(desc_obj, dict):
                    description = desc_obj.get('uk_UA', desc_obj.get('en_US', ''))
                elif isinstance(desc_obj, str):
                    description = desc_obj
            
            # Дата старту торгів
            auction_start_date = ''
            auction_period = data.get('auctionPeriod', {})
            if auction_period and 'startDate' in auction_period:
                auction_start_date = format_date(auction_period['startDate'])
            
            # Дата фінальної подачі документів (enquiryPeriod.endDate або qualificationPeriod.endDate)
            document_submission_deadline = ''
            enquiry_period = data.get('enquiryPeriod', {})
            if enquiry_period and 'endDate' in enquiry_period:
                document_submission_deadline = format_date(enquiry_period['endDate'])
            else:
                qualification_period = data.get('qualificationPeriod', {})
                if qualification_period and 'endDate' in qualification_period:
                    document_submission_deadline = format_date(qualification_period['endDate'])
            
            # Кількість учасників
            bids = data.get('bids', [])
            participants_count = len(bids) if isinstance(bids, list) else 0
            
            # Мінімальна кількість учасників
            min_participants_count = data.get('minNumberOfQualifiedBids', '')
            if min_participants_count:
                min_participants_count = str(min_participants_count)
            
            # Базова ставка (стартова ціна)
            base_price = ''
            value = data.get('value', {})
            if value and 'amount' in value:
                base_price = str(value['amount'])
                currency = value.get('currency', '')
                if currency:
                    base_price += f' {currency}'
            
            # Розмір взносу (guarantee.amount)
            deposit_amount = ''
            guarantee = data.get('guarantee', {})
            if guarantee and 'amount' in guarantee:
                deposit_amount = str(guarantee['amount'])
                currency = guarantee.get('currency', '')
                if currency:
                    deposit_amount += f' {currency}'
            
            # Арешти
            arrests_data = data.get('arrests', [])
            
            # Перевірка, чи це аренда (якщо так - пропускаємо обробку через LLM)
            is_rental = self._is_rental_auction(data)
            if is_rental:
                # Пропускаємо обробку через LLM для аренди
                parsed_info = {
                    'cadastral_number': '',
                    'area': '',
                    'area_unit': '',
                    'address_region': '',
                    'address_city': '',
                    'address_street': '',
                    'address_street_type': '',
                    'address_building': '',
                    'floor': '',
                    'property_type': '',
                    'utilities': '',
                    'arrests_info': ''
                }
            else:
                # Парсинг опису через LLM для отримання структурованої інформації
                parsed_info = {
                    'cadastral_number': '',
                    'area': '',
                    'area_unit': '',
                    'address_region': '',
                    'address_city': '',
                    'address_street': '',
                    'address_street_type': '',
                    'address_building': '',
                    'floor': '',
                    'property_type': '',
                    'utilities': '',
                    'arrests_info': ''
                }
            
            if description and self.llm_service and not is_rental:
                try:
                    # Спочатку перевіряємо кеш
                    cached_result = self.llm_cache_service.get_cached_result(description)
                    
                    if cached_result is not None:
                        # Використовуємо результат з кешу
                        parsed_info = cached_result
                    else:
                        # Парсинг опису через LLM (прогрес-бар вже показує прогрес обробки)
                        parsed_info = self.llm_service.parse_auction_description(description)
                        # Зберігаємо результат в кеш
                        self.llm_cache_service.save_result(description, parsed_info)
                except KeyboardInterrupt:
                    # Переривання користувача - пробрасуємо далі
                    raise
                except Exception as e:
                    # Інші помилки - просто пропускаємо парсинг для цього аукціону
                    pass
            
            # Формуємо адресу
            address = format_address(
                parsed_info.get('address_street_type', ''),
                parsed_info.get('address_street', ''),
                parsed_info.get('address_building', '')
            )
            
            # Формуємо площу з одиницею
            area = ''
            area_value = parsed_info.get('area', '')
            area_unit = parsed_info.get('area_unit', '')
            if area_value:
                area = str(area_value)
                if area_unit:
                    area += f' {area_unit}'
            
            # Формуємо арешти
            arrests_info = format_arrests(arrests_data, parsed_info.get('arrests_info', ''))
            
            # Витягуємо код класифікатора з items.classification.id
            classification_code = ''
            has_additional_classification_03_07 = False
            items = data.get('items', [])
            if isinstance(items, list) and len(items) > 0:
                for item in items:
                    classification = item.get('classification', {})
                    if classification:
                        scheme = classification.get('scheme', '')
                        class_id = classification.get('id', '')
                        if scheme == 'CAV' and class_id:
                            classification_code = class_id
                            break  # Беремо перший знайдений код CAV
                    
                    # Перевіряємо додатковий класифікатор 03.07
                    additional_classifications = item.get('additionalClassifications', [])
                    if isinstance(additional_classifications, list):
                        for add_class in additional_classifications:
                            if isinstance(add_class, dict):
                                add_class_id = add_class.get('id', '')
                                if add_class_id == '03.07':
                                    has_additional_classification_03_07 = True
                                    break
                    if has_additional_classification_03_07:
                        break
            
            auction_row = {
                'address_region': ensure_string(parsed_info.get('address_region', '')),
                'address_city': ensure_string(parsed_info.get('address_city', '')),
                'address': ensure_string(address),
                'property_type': ensure_string(parsed_info.get('property_type', '')),
                'cadastral_number': ensure_string(parsed_info.get('cadastral_number', '')),
                'area': ensure_string(area),
                'base_price': ensure_string(base_price),
                'deposit_amount': ensure_string(deposit_amount),
                'auction_start_date': ensure_string(auction_start_date),
                'document_submission_deadline': ensure_string(document_submission_deadline),
                'min_participants_count': ensure_string(min_participants_count),
                'participants_count': ensure_string(participants_count),
                'arrests_info': ensure_string(arrests_info),
                'description': ensure_string(description),
                'auction_url': ensure_string(auction_url),
                'classification_code': ensure_string(classification_code),
                '_has_additional_classification_03_07': has_additional_classification_03_07  # Службове поле для форматування
            }
            
            auctions_data.append(auction_row)

        print(f"Підготовлено {len(auctions_data)} рядків для збереження в Excel")
        save_excel_to_file(auctions_data, file_path, fieldnames, column_headers)
        print(f"Файл успішно збережено: {file_path}")
        return file_path

    def _fetch_and_save_single_day(
        self,
        date_from: datetime,
        date_to: datetime,
        day_number: int,
        temp_dir: str,
        user_id: Optional[int] = None
    ) -> Optional[str]:
        """
        Обробляє один день та зберігає результат у тимчасовий файл.
        
        Args:
            date_from: Початкова дата дня
            date_to: Кінцева дата дня
            day_number: Номер дня (для назви файлу)
            temp_dir: Тимчасова директорія для збереження
            user_id: Ідентифікатор користувача
            
        Returns:
            Шлях до збереженого файлу або None
        """
        try:
            auctions = self.get_real_estate_auctions_by_date_range(date_from, date_to)
            if not auctions:
                print(f"День {day_number}: аукціони не знайдено")
                return None
            
            print(f"День {day_number}: знайдено {len(auctions)} аукціонів")
            
            # Зберігаємо в тимчасовий файл
            temp_file_path = os.path.join(temp_dir, f"day_{day_number}.xlsx")
            self.save_auctions_to_csv(auctions, 1, temp_dir, user_id)
            
            # Знаходимо останній створений файл в temp_dir
            import glob
            pattern = os.path.join(temp_dir, 'prozorro_real_estate_auctions_*.xlsx')
            files = glob.glob(pattern)
            if files:
                latest_file = max(files, key=os.path.getmtime)
                # Перейменовуємо в стандартну назву
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)
                os.rename(latest_file, temp_file_path)
                print(f"День {day_number}: файл збережено - {temp_file_path}")
                return temp_file_path
            return None
        except Exception as e:
            print(f"Помилка обробки дня {day_number}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def fetch_and_save_real_estate_auctions(
        self,
        days: Optional[int] = None,
        output_dir: Optional[str] = None,
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Отримує та зберігає список аукціонів про нерухомість за останні N днів.
        Для тижня (7 днів) використовує паралельну обробку по днях.

        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
            output_dir: Директорія для збереження. Якщо не вказано, використовується temp/
            user_id: Ідентифікатор користувача, який сформував файл (опціонально)

        Returns:
            Dict[str, Any]: Результат операції з інформацією про кількість знайдених аукціонів та шлях до файлу
        """
        if days is None:
            days = self.settings.default_days_range
        
        # Для тижня використовуємо оптимізовану паралельну обробку
        if days == 7:
            return self._fetch_and_save_week_optimized(output_dir, user_id)
        
        # Для інших періодів використовуємо стандартну обробку
        try:
            print(
                f"Отримання аукціонів з ProZorro.Sale за останні {days} днів"
                f" (dateModified в діапазоні)..."
            )
            auctions = self.get_real_estate_auctions(days)
            
            days_text = "день" if days == 1 else ("дні" if days < 5 else "днів")
            print(f"Знайдено {len(auctions)} аукціонів про нерухомість за останні {days} {days_text}")
            
            if not auctions:
                print("Аукціони не знайдено")
                return {
                    'success': True,
                    'count': 0,
                    'file_path': None,
                    'message': 'Аукціони не знайдено'
                }

            file_path = self.save_auctions_to_csv(auctions, days, output_dir, user_id)
            print(f"Дані збережено у файл: {file_path}")
            
            return {
                'success': True,
                'count': len(auctions),
                'file_path': file_path,
                'message': f'Успішно збережено {len(auctions)} аукціонів'
            }

        except Exception as e:
            error_message = f"Помилка при отриманні та збереженні даних: {e}"
            print(error_message)
            return {
                'success': False,
                'count': 0,
                'file_path': None,
                'message': error_message,
                'error': str(e)
            }
    
    def _fetch_and_save_week_optimized(
        self,
        output_dir: Optional[str] = None,
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Оптимізована обробка тижня: паралельна обробка по днях з подальшим об'єднанням.
        
        Args:
            output_dir: Директорія для збереження
            user_id: Ідентифікатор користувача
            
        Returns:
            Dict[str, Any]: Результат операції
        """
        import concurrent.futures
        import tempfile
        
        if output_dir is None:
            output_dir = 'archives'
        
        ensure_directory_exists(output_dir)
        
        # Створюємо тимчасову директорію для файлів по днях
        temp_dir = tempfile.mkdtemp(prefix='prozorro_week_')
        
        try:
            print("Оптимізована обробка тижня: паралельна обробка по днях...")
            
            # Розбиваємо тиждень на 7 днів
            now = datetime.now(timezone.utc)
            day_ranges = []
            for i in range(7):
                day_end = now - timedelta(days=i)
                day_start = day_end - timedelta(days=1)
                day_ranges.append((day_start, day_end, 7 - i))
            
            # Обробляємо дні паралельно
            file_paths = []
            completed_days = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
                futures = {}
                for day_start, day_end, day_num in day_ranges:
                    future = executor.submit(
                        self._fetch_and_save_single_day,
                        day_start,
                        day_end,
                        day_num,
                        temp_dir,
                        user_id
                    )
                    futures[future] = day_num
                
                # Збираємо результати
                for future in concurrent.futures.as_completed(futures):
                    day_num = futures[future]
                    completed_days += 1
                    file_path = future.result()
                    if file_path:
                        file_paths.append(file_path)
                    print(f"Завершено обробку дня {day_num} ({completed_days}/7)")
            
            if not file_paths:
                return {
                    'success': True,
                    'count': 0,
                    'file_path': None,
                    'message': 'Аукціони не знайдено'
                }
            
            # Об'єднуємо всі файли в один
            print(f"Об'єднуємо {len(file_paths)} файлів...")
            
            # Визначаємо fieldnames та column_headers (беремо з першого файлу або використовуємо стандартні)
            fieldnames = [
                'address_region', 'address_city', 'address', 'property_type',
                'cadastral_number', 'area', 'base_price', 'deposit_amount',
                'auction_start_date', 'document_submission_deadline',
                'min_participants_count', 'participants_count', 'arrests_info',
                'description', 'auction_url', 'classification_code'
            ]
            
            column_headers = {
                'address_region': 'Область',
                'address_city': 'Населений пункт',
                'address': 'Адреса',
                'property_type': 'Тип нерухомості',
                'cadastral_number': 'Кадастровий номер',
                'area': 'Площа',
                'base_price': 'Стартова ціна',
                'deposit_amount': 'Розмір взносу',
                'auction_start_date': 'Дата торгів',
                'document_submission_deadline': 'Дата фінальної подачі документів',
                'min_participants_count': 'Мінімальна кількість учасників',
                'participants_count': 'Кількість зареєстрованих учасників',
                'arrests_info': 'Арешти',
                'description': 'Опис',
                'auction_url': 'Посилання',
                'classification_code': 'Код класифікатора'
            }
            
            # Створюємо фінальний файл
            final_filename = generate_auction_filename(
                prefix='prozorro_real_estate_auctions',
                extension='xlsx',
                user_id=user_id,
                days=7
            )
            final_file_path = os.path.join(output_dir, final_filename)
            
            merge_excel_files(file_paths, final_file_path, fieldnames, column_headers)
            
            # Підраховуємо загальну кількість аукціонів
            total_count = 0
            for file_path in file_paths:
                try:
                    import pandas as pd
                    df = pd.read_excel(file_path, engine='openpyxl')
                    total_count += len(df)
                except:
                    pass
            
            # Видаляємо тимчасові файли
            import shutil
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
            
            print(f"Об'єднано {len(file_paths)} файлів в один. Всього аукціонів: {total_count}")
            
            return {
                'success': True,
                'count': total_count,
                'file_path': final_file_path,
                'message': f'Успішно збережено {total_count} аукціонів за тиждень'
            }
            
        except Exception as e:
            error_message = f"Помилка при оптимізованій обробці тижня: {e}"
            print(error_message)
            # Видаляємо тимчасову директорію при помилці
            import shutil
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
            return {
                'success': False,
                'count': 0,
                'file_path': None,
                'message': error_message,
                'error': str(e)
            }

    def get_auction_details(self, auction_id: str) -> Dict[str, Any]:
        """
        Отримує детальну інформацію по конкретному аукціону.

        Endpoint (ProZorro.Sale): GET {base}/auctions/{id}

        Args:
            auction_id: Ідентифікатор аукціону

        Returns:
            Dict[str, Any]: Повна відповідь API у вигляді JSON (dict)
        """
        if not auction_id:
            raise ValueError("auction_id is required")

        url = f'{self.settings.prozorro_sale_api_base_url}/auctions/{auction_id}'
        response = self.session.get(
            url,
            timeout=self.settings.prozorro_api_timeout
        )
        response.raise_for_status()
        return response.json()

    def fetch_and_save_real_estate_auction_details(
        self,
        days: Optional[int] = None,
        output_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Отримує список релевантних аукціонів і для кожного `id` завантажує детальну інформацію.
        Результат зберігається в один JSON файл.

        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
            output_dir: Директорія для збереження. Якщо не вказано, використовується temp/

        Returns:
            Dict[str, Any]: Результат операції з інформацією про кількість знайдених/збережених аукціонів та шлях до файлу
        """
        if days is None:
            days = self.settings.default_days_range

        if output_dir is None:
            output_dir = self.settings.temp_directory

        try:
            print(
                f"Отримання аукціонів з ProZorro.Sale за останні {days} днів"
                f" (dateModified в діапазоні) + завантаження деталей по id..."
            )
            auctions = self.get_real_estate_auctions(days)

            days_text = "день" if days == 1 else ("дні" if days < 5 else "днів")
            print(f"Знайдено {len(auctions)} аукціонів про нерухомість за останні {days} {days_text}")

            if not auctions:
                return {
                    'success': True,
                    'count': 0,
                    'file_path': None,
                    'message': 'Аукціони не знайдено'
                }

            ensure_directory_exists(output_dir)
            filename = generate_json_filename(prefix='prozorro_real_estate_auctions_details')
            file_path = f'{output_dir}/{filename}'

            detailed_auctions: List[Dict[str, Any]] = []
            errors: List[Dict[str, Any]] = []
            total = len(auctions)
            bar_width = 30

            def _render_progress(current: int) -> None:
                if total <= 0:
                    return
                ratio = current / total
                filled = int(ratio * bar_width)
                bar = ('#' * filled) + ('-' * (bar_width - filled))
                percent = int(ratio * 100)
                sys.stdout.write(f"\rDetails: [{bar}] {percent:3d}% ({current}/{total})")
                sys.stdout.flush()

            for idx, auction in enumerate(auctions, start=1):
                auction_id = auction.id
                try:
                    _render_progress(idx)
                    details = self.get_auction_details(auction_id)
                    detailed_auctions.append(details)
                except requests.exceptions.RequestException as e:
                    errors.append({
                        'id': auction_id,
                        'error': str(e)
                    })
                    print(f"Помилка при отриманні деталей аукціону {auction_id}: {e}")

            if total > 0:
                sys.stdout.write("\n")

            result_data = {
                'metadata': {
                    'total_count': len(detailed_auctions),
                    'saved_at': format_datetime_for_api(datetime.now()),
                    'days_range': days,
                    'source': 'prozorro_sale_auctions',
                    'requested_ids_count': len(auctions),
                    'errors_count': len(errors),
                    'errors': errors
                },
                'data': detailed_auctions
            }

            save_json_to_file(result_data, file_path)

            message = f'Успішно збережено {len(detailed_auctions)} детальних аукціонів'
            if errors:
                message += f' (з помилками: {len(errors)})'

            return {
                'success': True,
                'count': len(detailed_auctions),
                'file_path': file_path,
                'message': message,
                'errors_count': len(errors),
            }

        except Exception as e:
            error_message = f"Помилка при отриманні та збереженні детальних даних аукціонів: {e}"
            print(error_message)
            return {
                'success': False,
                'count': 0,
                'file_path': None,
                'message': error_message,
                'error': str(e)
            }
    
    def get_allowed_classification_codes(self) -> List[str]:
        """
        Отримує список дозволених кодів класифікації з файлу конфігурації.
        
        Returns:
            List[str]: Список кодів класифікації
        """
        try:
            if not self.classification_codes_config_path.exists():
                # Якщо файл не існує, повертаємо значення за замовчуванням
                return self._get_default_classification_codes()
            
            with open(self.classification_codes_config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if config and 'classification_codes' in config:
                codes = []
                for item in config['classification_codes']:
                    if isinstance(item, dict) and 'code' in item:
                        codes.append(item['code'])
                return codes if codes else self._get_default_classification_codes()
            
            return self._get_default_classification_codes()
        except Exception as e:
            print(f"Помилка читання файлу конфігурації кодів класифікації: {e}")
            return self._get_default_classification_codes()
    
    def _get_default_classification_codes(self) -> List[str]:
        """Повертає список кодів класифікації за замовчуванням."""
        return [
            '04000000-8',
            '04233000-0',
            '04230000-9',
            '04130000-8',
            '04232000-3',
            '70123000-9',
            '70123200-1',
            '04222000-0',
            '06000000-2',
            '04211000-0',
            '06111000-3',
            '06112000-0',
            '05000000-5',
            '06110000-6',
            '06128000-5'
        ]
    
    def get_classification_codes_config(self) -> Dict[str, Any]:
        """
        Отримує повну конфігурацію кодів класифікації.
        
        Returns:
            Dict[str, Any]: Конфігурація з кодами та описами
        """
        try:
            if not self.classification_codes_config_path.exists():
                return {
                    'classification_codes': [
                        {'code': code, 'description': ''} 
                        for code in self._get_default_classification_codes()
                    ]
                }
            
            with open(self.classification_codes_config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            return config if config else {'classification_codes': []}
        except Exception as e:
            print(f"Помилка читання файлу конфігурації: {e}")
            return {'classification_codes': []}
    
    def save_classification_codes_config(self, config: Dict[str, Any]) -> bool:
        """
        Зберігає конфігурацію кодів класифікації у файл.
        
        Args:
            config: Словник з конфігурацією (має містити 'classification_codes')
            
        Returns:
            bool: True якщо успішно, False якщо помилка
        """
        try:
            # Валідація формату
            if not isinstance(config, dict) or 'classification_codes' not in config:
                return False
            
            if not isinstance(config['classification_codes'], list):
                return False
            
            # Валідація кожного елемента
            for item in config['classification_codes']:
                if not isinstance(item, dict) or 'code' not in item:
                    return False
                if not isinstance(item['code'], str) or not item['code'].strip():
                    return False
            
            # Створюємо директорію, якщо не існує
            self.classification_codes_config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Зберігаємо файл
            with open(self.classification_codes_config_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
            
            return True
        except Exception as e:
            print(f"Помилка збереження файлу конфігурації: {e}")
            return False
