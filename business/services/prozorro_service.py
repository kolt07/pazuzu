# -*- coding: utf-8 -*-
"""
Сервіс для роботи з API ProZorro.
"""

import requests
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
import sys
import time

from config.settings import Settings
from transport.dto.prozorro_dto import AuctionDTO, AuctionsResponseDTO
from utils.date_utils import get_date_range, format_datetime_for_api, format_datetime_for_byDateModified
from utils.file_utils import save_json_to_file, save_csv_to_file, save_excel_to_file, generate_json_filename, ensure_directory_exists
from business.services.llm_service import LLMService


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

    def get_real_estate_auctions(self, days: int = 1) -> List[AuctionDTO]:
        """
        Отримує активні аукціони, змінені протягом останніх N днів.
        Використовує ендпоінт /api/search/byDateModified/{date} для ефективного отримання даних.

        Args:
            days: Кількість днів для виборки (за замовчуванням 1)

        Returns:
            List[AuctionDTO]: Список активних аукціонів

        Raises:
            requests.RequestException: При помилках HTTP запитів
        """
        date_from, date_to = get_date_range(days)
        
        print(f"Діапазон дат для фільтрації:")
        print(f"  Від: {date_from} ({format_datetime_for_api(date_from)})")
        print(f"  До: {date_to} ({format_datetime_for_api(date_to)})")
        
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
                
                print(f"Обробка сторінки {page_count}...")
                print(f"Виконується запит до API: {url}")
                print(f"Параметри запиту: {params}")
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.settings.prozorro_api_timeout
                )
                response.raise_for_status()
                
                print(f"Статус відповіді: {response.status_code}")
                
                # Обробка статусу 204 (No Content) - немає даних для повернення
                if response.status_code == 204:
                    print("Немає даних (статус 204 - No Content)")
                    break
                
                # Перевірка, чи є вміст для парсингу
                if not response.text or not response.text.strip():
                    print("Порожня відповідь від API")
                    break
                
                response_data = response.json()
                
                # Діагностичне логування структури відповіді
                if isinstance(response_data, dict):
                    print(f"Тип відповіді: dict, ключі: {list(response_data.keys())}")
                    if 'data' in response_data:
                        print(f"Кількість аукціонів у відповіді: {len(response_data.get('data', []))}")
                elif isinstance(response_data, list):
                    print(f"Тип відповіді: list, кількість елементів: {len(response_data)}")
                else:
                    print(f"Невідомий тип відповіді: {type(response_data)}")
                
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
                        # Логування першого аукціону для діагностики
                        if page_count == 1 and idx == 0:
                            print(f"\nПриклад даних першого аукціону:")
                            print(f"  ID: {auction_data.get('id', 'N/A')}")
                            print(f"  dateCreated: {auction_data.get('dateCreated', 'N/A')}")
                            print(f"  dateModified: {auction_data.get('dateModified', 'N/A')}")
                            print(f"  status: {auction_data.get('status', 'N/A')}")
                        
                        auction = AuctionDTO.from_dict(auction_data)
                        
                        # Зберігаємо останнє оголошення на сторінці
                        last_auction_on_page = auction
                        
                        # Оновлюємо максимальну дату модифікації
                        if max_date_modified is None or auction.date_modified > max_date_modified:
                            max_date_modified = auction.date_modified
                        
                        # ---- Фільтрація за датою модифікації ----
                        # Фільтруємо по dateModified в межах заданого діапазону
                        date_to_check = auction.date_modified
                        
                        if date_from <= date_to_check <= date_to:
                            # Додаткова фільтрація за активним статусом та майбутньою датою старту торгів
                            if auction.data:
                                # Перевірка статусу (активні статуси)
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
                                    continue
                                
                                # Перевірка дати старту торгів (має бути в майбутньому)
                                auction_period = auction.data.get('auctionPeriod', {})
                                auction_start_date_str = auction_period.get('startDate')
                                
                                if auction_start_date_str:
                                    try:
                                        # Парсимо дату старту торгів
                                        if auction_start_date_str.endswith('Z'):
                                            auction_start_date_str = auction_start_date_str.replace('Z', '+00:00')
                                        auction_start_date = datetime.fromisoformat(auction_start_date_str)
                                        if auction_start_date.tzinfo:
                                            auction_start_date = auction_start_date.astimezone(timezone.utc)
                                        else:
                                            auction_start_date = auction_start_date.replace(tzinfo=timezone.utc)
                                        
                                        # Перевіряємо, чи дата в майбутньому
                                        now = datetime.now(timezone.utc)
                                        if auction_start_date <= now:
                                            filtered_by_start_date_count += 1
                                            continue
                                    except (ValueError, AttributeError):
                                        # Якщо не вдалося розпарсити дату, пропускаємо
                                        filtered_by_start_date_count += 1
                                        continue
                                else:
                                    # Якщо немає дати старту торгів, пропускаємо
                                    filtered_by_start_date_count += 1
                                    continue
                                
                                # Перевірка типу продажу (тільки продаж нерухомості)
                                # Фільтрація ТІЛЬКИ за items.classification.id з дозволеними кодами CAV для нерухомості
                                items = auction.data.get('items', [])
                                
                                is_property_sale = False
                                
                                # Дозволені коди CAV класифікації для нерухомості та земельних ділянок:
                                # 0612xxx - землі (земельні ділянки)
                                # 0613xxx - будівлі
                                # 0614xxx - споруди
                                # 70000000-1 - нерухомість (загальний код)
                                allowed_classification_codes = [
                                    '0612',  # Землі
                                    '0613',  # Будівлі
                                    '0614',  # Споруди
                                    '70000000-1'  # Нерухомість
                                ]
                                
                                # Перевірка через items.classification.id
                                if isinstance(items, list) and len(items) > 0:
                                    for item in items:
                                        classification = item.get('classification', {})
                                        if classification:
                                            scheme = classification.get('scheme', '')
                                            class_id = classification.get('id', '')
                                            
                                            # Перевірка: схема має бути CAV, а код має починатися з дозволеного префіксу
                                            if scheme == 'CAV' and class_id:
                                                # Перевірка, чи код починається з дозволеного префіксу
                                                for allowed_code in allowed_classification_codes:
                                                    if class_id.startswith(allowed_code):
                                                        is_property_sale = True
                                                        break
                                                
                                                if is_property_sale:
                                                    break
                                
                                if not is_property_sale:
                                    filtered_by_property_type_count += 1
                                    continue
                            else:
                                # Якщо немає повних даних, пропускаємо
                                filtered_by_status_count += 1
                                continue
                            
                            auctions.append(auction)
                        else:
                            filtered_by_date_count += 1
                            if page_count == 1 and idx < 3:
                                print(
                                    f"Аукціон {auction.id} відфільтровано за датою: "
                                    f"dateToCheck={date_to_check}, діапазон={days} днів"
                                )
                    except (KeyError, ValueError) as e:
                        errors_count += 1
                        if errors_count <= 3:  # Логуємо перші 3 помилки
                            print(f"Помилка обробки аукціону #{idx}: {e}")
                            print(f"  Дані: {auction_data.get('id', 'N/A')}")
                        continue
                
                # Виводимо дані останнього оголошення на сторінці
                if last_auction_on_page:
                    print(f"Останнє оголошення на сторінці {page_count}:")
                    print(f"  ID: {last_auction_on_page.id}")
                    print(f"  dateModified: {format_datetime_for_api(last_auction_on_page.date_modified)}")
                    print(f"  status: {last_auction_on_page.status}")
                
                # Перевіряємо, чи потрібно продовжувати
                if max_date_modified is None:
                    # Якщо не знайшли жодної дати, зупиняємось
                    break
                
                if max_date_modified >= date_to:
                    # Якщо максимальна дата досягнула або перевищила date_to, зупиняємось
                    print(f"Досягнуто кінець діапазону: max_dateModified={max_date_modified} >= date_to={date_to}")
                    break
                
                # Оновлюємо current_date для наступної ітерації: додаємо 1 мілісекунду до максимальної дати
                current_date = max_date_modified + timedelta(milliseconds=1)
                
                # Невелика пауза для уникнення перевантаження API
                time.sleep(0.1)
            
            print(f"\nСтатистика обробки:")
            print(f"  Оброблено сторінок: {page_count}")
            print(f"  Всього аукціонів оброблено: {all_auctions_processed}")
            print(f"  Відфільтровано за датою модифікації: {filtered_by_date_count}")
            print(f"  Відфільтровано за статусом: {filtered_by_status_count}")
            print(f"  Відфільтровано за датою старту торгів: {filtered_by_start_date_count}")
            print(f"  Відфільтровано за типом (не продаж нерухомості): {filtered_by_property_type_count}")
            print(f"  Успішно оброблено: {len(auctions)}")
            print(f"  Помилок обробки: {errors_count}")

            return auctions

        except requests.exceptions.RequestException as e:
            print(f"Помилка при запиті до API ProZorro.Sale: {e}")
            raise

    def save_auctions_to_csv(self, auctions: List[AuctionDTO], days: int = 1, output_dir: Optional[str] = None) -> str:
        """
        Зберігає список аукціонів у Excel файл з вибраними полями.
        Фільтрує лише продажі (без оренди) та зберігає в каталог "archives".

        Args:
            auctions: Список аукціонів для збереження
            days: Кількість днів виборки (для метаданих)
            output_dir: Директорія для збереження. Якщо не вказано, використовується archives/

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
                        try:
                            if restriction_date.endswith('Z'):
                                restriction_date = restriction_date.replace('Z', '+00:00')
                            dt = datetime.fromisoformat(restriction_date)
                            date_str = dt.strftime('%Y-%m-%d')
                        except (ValueError, AttributeError):
                            date_str = str(restriction_date)
                    
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

        filename = generate_json_filename(prefix='prozorro_real_estate_auctions', extension='xlsx')
        file_path = f'{output_dir}/{filename}'

        # Порядок колонок: область, населений пункт, адрес, площа, стартова ціна, розмір взноса, 
        # дата финальной подачи докуметов, дата торгов, кадастровий номер, арешти, посилання
        fieldnames = [
            'address_region',           # Область
            'address_city',              # Населений пункт
            'address',                   # Адрес (повна адреса)
            'area',                      # Площа
            'base_price',                # Стартова ціна
            'deposit_amount',            # Розмір взносу
            'document_submission_deadline',  # Дата фінальної подачі документів
            'auction_start_date',        # Дата торгів
            'cadastral_number',          # Кадастровий номер
            'arrests_info',              # Арешти
            'auction_url'                # Посилання
        ]
        
        from tqdm import tqdm
        
        auctions_data = []
        total_auctions = len(auctions)
        print(f"Початок обробки {total_auctions} аукціонів для збереження в Excel...")
        
        # Прогрес-бар для обробки аукціонів
        for auction in tqdm(auctions, desc="Обробка аукціонів", unit="аукціон", ncols=100):
            if not auction.data:
                continue
            
            data = auction.data
            
            # Тип аукціону (продаж, аренда) - фільтруємо лише продажі
            sale_type = data.get('saleType', '')
            lease_type = data.get('leaseType', '')
            if 'lease' in sale_type.lower() or lease_type:
                # Пропускаємо оренду
                continue
            elif 'sale' not in sale_type.lower() and not sale_type:
                # Якщо не вказано тип, пропускаємо
                continue
            
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
                auction_start_date = auction_period['startDate']
            
            # Дата фінальної подачі документів (enquiryPeriod.endDate або qualificationPeriod.endDate)
            document_submission_deadline = ''
            enquiry_period = data.get('enquiryPeriod', {})
            if enquiry_period and 'endDate' in enquiry_period:
                document_submission_deadline = enquiry_period['endDate']
            else:
                qualification_period = data.get('qualificationPeriod', {})
                if qualification_period and 'endDate' in qualification_period:
                    document_submission_deadline = qualification_period['endDate']
            
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
            
            if description and self.llm_service:
                try:
                    # Парсинг опису через LLM (прогрес-бар вже показує прогрес обробки)
                    parsed_info = self.llm_service.parse_auction_description(description)
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
            
            auction_row = {
                'address_region': ensure_string(parsed_info.get('address_region', '')),
                'address_city': ensure_string(parsed_info.get('address_city', '')),
                'address': ensure_string(address),
                'area': ensure_string(area),
                'base_price': ensure_string(base_price),
                'deposit_amount': ensure_string(deposit_amount),
                'document_submission_deadline': ensure_string(document_submission_deadline),
                'auction_start_date': ensure_string(auction_start_date),
                'cadastral_number': ensure_string(parsed_info.get('cadastral_number', '')),
                'arrests_info': ensure_string(arrests_info),
                'auction_url': ensure_string(auction_url)
            }
            
            auctions_data.append(auction_row)

        print(f"Підготовлено {len(auctions_data)} рядків для збереження в Excel (тільки продажі)")
        save_excel_to_file(auctions_data, file_path, fieldnames)
        print(f"Файл успішно збережено: {file_path}")
        return file_path

    def fetch_and_save_real_estate_auctions(
        self,
        days: Optional[int] = None,
        output_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Отримує та зберігає список аукціонів про нерухомість за останні N днів.

        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
            output_dir: Директорія для збереження. Якщо не вказано, використовується temp/

        Returns:
            Dict[str, Any]: Результат операції з інформацією про кількість знайдених аукціонів та шлях до файлу
        """
        if days is None:
            days = self.settings.default_days_range
        
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

            file_path = self.save_auctions_to_csv(auctions, days, output_dir)
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
