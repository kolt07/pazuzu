# -*- coding: utf-8 -*-
"""
Сервіс для роботи з API ProZorro.
"""

import requests
import json
import os
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timezone, timedelta
import sys
import time

from config.settings import Settings
from transport.dto.prozorro_dto import AuctionDTO, AuctionsResponseDTO
from utils.date_utils import get_date_range, format_datetime_for_api, format_datetime_for_byDateModified, format_date_display, format_datetime_display
from utils.file_utils import save_json_to_file, save_csv_to_file, save_excel_to_file, generate_json_filename, generate_auction_filename, ensure_directory_exists, merge_excel_files, generate_excel_in_memory, generate_excel_with_sheets
from utils.hash_utils import calculate_object_version_hash, calculate_description_hash, extract_auction_id
from utils.price_metrics import compute_price_metrics
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService
from business.services.llm_processing_regions_service import is_region_enabled_for_llm, normalize_region_name
from business.services.logging_service import LoggingService
from business.services.currency_rate_service import CurrencyRateService
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository
from data.repositories.app_data_repository import AppDataRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.database.connection import MongoDBConnection
import logging
import yaml

logger = logging.getLogger(__name__)


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
        
        # Ініціалізація MongoDB підключення
        try:
            MongoDBConnection.initialize(self.settings)
        except Exception as e:
            print(f"Попередження: не вдалося ініціалізувати MongoDB: {e}")
            print("Робота з базою даних буде недоступна")
        
        # Ініціалізація репозиторіїв та сервісів
        self.auctions_repository = ProZorroAuctionsRepository()
        self.app_data_repository = AppDataRepository()
        self.logging_service = LoggingService()
        # Курс продажу USD (може бути None, тоді USD-метрики не обчислюються)
        self._usd_rate = None
        try:
            self._usd_rate = CurrencyRateService(self.settings).get_today_usd_rate(allow_fetch=True)
        except Exception:
            self._usd_rate = None
        
        # Ініціалізація LLM сервісу (може викликати помилку, якщо API ключ не вказано)
        self.llm_service = None
        try:
            self.llm_service = LLMService(self.settings)
        except (ValueError, ImportError) as e:
            print(f"Попередження: LLM сервіс недоступний: {e}")
            print("Парсинг описів через LLM буде пропущено")
        
        # Ініціалізація кешу LLM
        self.llm_cache_service = LLMCacheService()
        # Репозиторій оголошень OLX (для експорту в той самий Excel)
        self.olx_listings_repository = OlxListingsRepository()
        # Репозиторій зведеної таблиці оголошень
        self.unified_listings_repository = UnifiedListingsRepository()
        self._unified_listings_service = None

        # Шлях до файлу конфігурації кодів класифікації
        config_dir = Path(__file__).parent.parent.parent / 'config'
        self.classification_codes_config_path = config_dir / 'ProZorro_clasification_codes.yaml'

    # ------------------------------------------------------------------
    # Внутрішні допоміжні методи
    # ------------------------------------------------------------------

    def _attach_price_metrics_to_auction_data(self, auction_data: Dict[str, Any]) -> None:
        """
        Додає до auction_data поле price_metrics з розрахованими метриками:
        - total_price_uah / total_price_usd
        - price_per_m2_uah / price_per_m2_usd
        - price_per_ha_uah / price_per_ha_usd (якщо є площа ділянки в га)
        """
        try:
            value = auction_data.get('value') or {}
            total_price_uah = value.get('amount')

            items = auction_data.get('items') or []
            building_area_sqm = None
            land_area_ha = None
            if items and isinstance(items, list) and isinstance(items[0], dict):
                first = items[0]
                quantity = first.get('quantity')
                unit = first.get('unit') or {}
                unit_name = ''
                if isinstance(unit, dict):
                    unit_name = str(unit.get('name', '')).lower()
                elif isinstance(unit, str):
                    unit_name = unit.lower()

                if 'га' in unit_name or 'hectar' in unit_name or 'hectare' in unit_name:
                    land_area_ha = quantity
                else:
                    building_area_sqm = quantity

            metrics = compute_price_metrics(
                total_price_uah=total_price_uah,
                building_area_sqm=building_area_sqm,
                land_area_ha=land_area_ha,
                uah_per_usd=self._usd_rate,
            )
            auction_data['price_metrics'] = metrics
        except Exception:
            # Не перериваємо основний процес збереження аукціону
            pass

    def _sync_auction_to_unified(self, auction_id: str) -> None:
        """Синхронізує аукціон у зведену таблицю unified_listings."""
        try:
            if self._unified_listings_service is None:
                from business.services.unified_listings_service import UnifiedListingsService
                self._unified_listings_service = UnifiedListingsService(self.settings)
            self._unified_listings_service.sync_prozorro_auction(auction_id)
        except Exception as e:
            logger.warning("Помилка синхронізації аукціону %s в unified_listings: %s", auction_id, e)

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
        
        return self._get_auctions_by_date_modified(date_from, date_to)
    
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
        return self._get_auctions_by_date_modified(date_from, date_to)
    
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
        
        # Логуємо початок обміну з API
        try:
            self.logging_service.log_api_exchange(
                message=f"Почато обмін з ProZorro API (діапазон дат: {date_from.date()} - {date_to.date()})",
                url=base_url,
                method='GET',
                metadata={'date_from': date_from.isoformat(), 'date_to': date_to.isoformat()}
            )
        except:
            pass
        
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
                
                try:
                    response = self.session.get(
                        url,
                        params=params,
                        timeout=self.settings.prozorro_api_timeout
                    )
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    # Логуємо помилку запиту
                    try:
                        self.logging_service.log_api_exchange(
                            message=f"Помилка запиту до API ProZorro",
                            url=url,
                            method='GET',
                            error=str(e)
                        )
                    except:
                        pass
                    raise
                
                # Обробка статусу 204 (No Content) - немає даних для цієї конкретної дати/часу
                if response.status_code == 204:
                    print(f"Немає даних для дати {date_str} (статус 204 - No Content)")
                    # Якщо отримано 204, це означає, що для цієї конкретної дати/часу немає даних
                    # Оновлюємо current_date для продовження пагінації
                    # Додаємо 1 день до поточної дати для переходу до наступного дня
                    next_date = current_date + timedelta(days=1)
                    # Встановлюємо час на початок дня для наступної ітерації
                    next_date = next_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    # Перевіряємо, чи не вийшли за межі діапазону
                    if next_date > date_to:
                        # Якщо наступна дата виходить за межі діапазону - завершуємо обробку
                        break
                    
                    # Оновлюємо current_date для наступної ітерації
                    current_date = next_date
                    continue
                
                # Перевірка, чи є вміст для парсингу
                if not response.text or not response.text.strip():
                    print(f"Порожня відповідь від API для дати {date_str}")
                    # Якщо відповідь порожня, але статус не 204 - це може бути помилка
                    # Але для безпеки також переходимо до наступної дати
                    next_date = current_date + timedelta(days=1)
                    next_date = next_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    if next_date > date_to:
                        break
                    
                    current_date = next_date
                    continue
                
                # Парсимо JSON відповідь
                response_data = response.json()
                
                # Отримуємо список аукціонів з відповіді
                auctions_data = []
                if isinstance(response_data, dict):
                    auctions_data = response_data.get('data', []) or response_data.get('procedures', [])
                elif isinstance(response_data, list):
                    auctions_data = response_data
                
                if not auctions_data:
                    print(f"Немає більше аукціонів для обробки для дати {date_str}")
                    # Якщо отримано порожній список аукціонів, переходимо до наступного дня
                    next_date = current_date + timedelta(days=1)
                    next_date = next_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    if next_date > date_to:
                        break
                    
                    current_date = next_date
                    continue
                
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
                            # Застосовуємо фільтрацію
                            if self._should_include_auction(auction, date_from, date_to, skip_date_check=True):
                                auctions.append(auction)
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
                    # Якщо не знайшли жодної дати модифікації в отриманих даних
                    # (наприклад, порожній список або дані без dateModified)
                    # Переходимо до наступного дня
                    next_date = current_date + timedelta(days=1)
                    next_date = next_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    if next_date > date_to:
                        break
                    
                    current_date = next_date
                    continue
                
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

            # Логуємо отримання даних
            try:
                self.logging_service.log_api_exchange(
                    message=f"Отримано дані з ProZorro API",
                    url=base_url,
                    method='GET',
                    status_code=200,
                    metadata={
                        'pages_processed': page_count,
                        'total_processed': all_auctions_processed,
                        'auctions_found': len(auctions),
                        'filtered_by_date': filtered_by_date_count,
                        'filtered_by_status': filtered_by_status_count,
                        'filtered_by_start_date': filtered_by_start_date_count,
                        'filtered_by_type': filtered_by_property_type_count,
                        'errors': errors_count
                    }
                )
            except:
                pass

            # Зберігаємо аукціони в MongoDB
            llm_requests_count = 0
            if auctions:
                save_result = self._save_auctions_to_database(auctions)
                llm_requests_count = save_result['llm_requests_count']
            
            # Логуємо завершення обміну з API
            try:
                self.logging_service.log_api_exchange(
                    message=f"Завершено обмін з ProZorro API",
                    url=base_url,
                    method='GET',
                    status_code=200,
                    metadata={
                        'pages_processed': page_count,
                        'auctions_found': len(auctions),
                        'date_from': date_from.isoformat(),
                        'date_to': date_to.isoformat()
                    }
                )
            except:
                pass

            return auctions

        except requests.exceptions.RequestException as e:
            error_msg = f"Помилка при запиті до API ProZorro.Sale (byDateModified): {e}"
            print(error_msg)
            try:
                self.logging_service.log_api_exchange(
                    message=error_msg,
                    url=url if 'url' in locals() else None,
                    method='GET',
                    error=str(e)
                )
            except:
                pass  # Якщо логування не працює, просто продовжуємо
            raise
    
    def _is_active_auction(self, auction_data: Dict[str, Any]) -> bool:
        """Перевіряє, чи аукціон активний (status починається з 'active')."""
        status = (auction_data.get("status") or "").strip().lower()
        return status.startswith("active")

    def _save_auctions_to_database(
        self,
        auctions: List[AuctionDTO],
        llm_only_for_active: bool = False,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Зберігає аукціони в MongoDB з перевіркою версій.
        
        Після отримання інформації з API проводить пошук отриманих аукціонів у базі.
        Перевіряє, чи змінились дані в порівнянні з тим, що збережено у базі.
        Якщо дані ті ж самі - нічого не міняємо. Якщо версія змінилася - записуємо нові дані.
        Записує ті, яких там ще немає.
        Обробляє описи через LLM для нових/оновлених аукціонів (або тільки для активних, якщо llm_only_for_active).

        Args:
            auctions: Список аукціонів для збереження
            llm_only_for_active: Якщо True — LLM тільки для активних, інші парсяться з полів
            progress_callback: Опціональний callback(progress_dict) для звіту прогресу (phase, current, total, llm_processed, message)

        Returns:
            Dict[str, Any]: Словник зі статистикою: llm_requests_count, saved_count (кількість збережених без аренди)
        """
        if not auctions:
            return {'llm_requests_count': 0, 'saved_count': 0}
        
        updated_count = 0
        created_count = 0
        unchanged_count = 0
        errors_count = 0
        llm_requests_count = 0  # Кількість викликів LLM без кешу
        deleted_rental_count = 0  # Кількість видалених аукціонів-аренди
        total_auctions = len(auctions)
        
        # Прогрес-бар для консолі (тільки якщо немає callback для UI)
        from tqdm import tqdm
        llm_progress = None
        if progress_callback is None:
            llm_progress = tqdm(
                total=total_auctions,
                desc="Обробка через LLM",
                unit="аукціон",
                ncols=100,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
            )
        
        # Обробляємо аукціони
        for idx, auction in enumerate(auctions):
            try:
                if not auction.data:
                    continue
                
                auction_data = auction.data
                auction_id = extract_auction_id(auction_data)
                
                if not auction_id:
                    continue
                
                # Обчислюємо версію об'єкта (хеш повного тексту оголошення)
                version_hash = calculate_object_version_hash(auction_data)
                
                # Витягуємо опис для обчислення хешу опису
                description = ''
                if 'description' in auction_data:
                    desc_obj = auction_data['description']
                    if isinstance(desc_obj, dict):
                        description = desc_obj.get('uk_UA', desc_obj.get('en_US', ''))
                    elif isinstance(desc_obj, str):
                        description = desc_obj
                
                description_hash = None
                if description:
                    description_hash = calculate_description_hash(description)
                
                # Перевіряємо, чи це аренда - аренду не зберігаємо в БД
                is_rental = self._is_rental_auction(auction_data)
                
                # Шукаємо аукціон у базі
                existing_auction = self.auctions_repository.find_by_auction_id(auction_id)
                
                # Якщо це аренда - не зберігаємо в БД
                if is_rental:
                    # Якщо аренда вже є в БД - видаляємо її
                    if existing_auction:
                        self.auctions_repository.delete_by_id(existing_auction['_id'])
                        deleted_rental_count += 1
                    continue
                
                if existing_auction:
                    existing_description_hash = existing_auction.get('description_hash')
                    # Перевіряємо, чи змінилася версія
                    if existing_auction.get('version_hash') == version_hash:
                        # Версія не змінилася - перевіряємо, чи є результат LLM в кеші
                        # (на випадок, якщо промпт змінився і кеш очистили)
                        if existing_description_hash and description:
                            use_llm = not (llm_only_for_active and not self._is_active_auction(auction_data))
                            if use_llm:
                                cached_entry = self.llm_cache_service.repository.find_by_description_hash(existing_description_hash)
                                if not cached_entry:
                                    if self.llm_service:
                                        region = self._get_region_from_auction_data(auction_data)
                                        if region is None or is_region_enabled_for_llm(region):
                                            llm_requests_count += self._process_auction_with_llm(auction_data)
                                            if llm_progress:
                                                llm_progress.update(1)
                        unchanged_count += 1
                    else:
                        # Версія змінилася - перед оновленням додаємо цінові метрики
                        self._attach_price_metrics_to_auction_data(auction_data)
                        # Оновлюємо
                        self.auctions_repository.upsert_auction(
                            auction_id=auction_id,
                            auction_data=auction_data,
                            version_hash=version_hash,
                            description_hash=description_hash,
                            last_updated=datetime.now(timezone.utc)
                        )
                        self._sync_auction_to_unified(auction_id)
                        updated_count += 1
                        # Обробляємо через LLM для оновлених аукціонів (якщо не llm_only_for_active для неактивних)
                        use_llm = not (llm_only_for_active and not self._is_active_auction(auction_data))
                        if use_llm and description_hash and self.llm_service:
                            cached_entry = self.llm_cache_service.repository.find_by_description_hash(description_hash)
                            if not cached_entry:
                                region = self._get_region_from_auction_data(auction_data)
                                if region is None or is_region_enabled_for_llm(region):
                                    llm_requests_count += self._process_auction_with_llm(auction_data)
                                    if llm_progress:
                                        llm_progress.update(1)
                else:
                    # Аукціону немає в базі - додаємо (попередньо рахуємо цінові метрики)
                    self._attach_price_metrics_to_auction_data(auction_data)
                    self.auctions_repository.upsert_auction(
                        auction_id=auction_id,
                        auction_data=auction_data,
                        version_hash=version_hash,
                        description_hash=description_hash,
                        last_updated=datetime.now(timezone.utc)
                    )
                    self._sync_auction_to_unified(auction_id)
                    created_count += 1
                    # Обробляємо через LLM для нових аукціонів (якщо не llm_only_for_active для неактивних)
                    use_llm = not (llm_only_for_active and not self._is_active_auction(auction_data))
                    if use_llm and description_hash and self.llm_service:
                        cached_entry = self.llm_cache_service.repository.find_by_description_hash(description_hash)
                        if not cached_entry:
                            region = self._get_region_from_auction_data(auction_data)
                            if region is None or is_region_enabled_for_llm(region):
                                llm_requests_count += self._process_auction_with_llm(auction_data)
                                if llm_progress:
                                    llm_progress.update(1)
            
            except Exception as e:
                errors_count += 1
                print(f"Помилка збереження аукціону в базу: {e}")
                try:
                    self.logging_service.log_app_event(
                        message=f"Помилка збереження аукціону в базу",
                        event_type='error',
                        metadata={'auction_id': auction.id if hasattr(auction, 'id') else 'unknown'},
                        error=str(e)
                    )
                except:
                    pass
            finally:
                # Звіт прогресу для UI (після кожної обробки аукціону)
                if progress_callback:
                    progress_callback({
                        "phase": "llm",
                        "current": idx + 1,
                        "total": total_auctions,
                        "llm_processed": llm_requests_count,
                        "message": f"Обробка через LLM: {idx + 1}/{total_auctions} (LLM: {llm_requests_count})",
                    })
                elif llm_progress:
                    llm_progress.n = idx + 1
                    llm_progress.refresh()
        
        # Закриваємо прогрес-бар (тільки якщо використовували консольний tqdm)
        if llm_progress:
            llm_progress.close()
        
        print(f"\nСтатистика збереження в MongoDB:")
        print(f"  Створено нових: {created_count}")
        print(f"  Оновлено: {updated_count}")
        print(f"  Без змін: {unchanged_count}")
        print(f"  Видалено аренди: {deleted_rental_count}")
        print(f"  Помилок: {errors_count}")
        print(f"  Викликів LLM: {llm_requests_count}")
        
        # Логуємо збереження в MongoDB
        try:
            self.logging_service.log_app_event(
                message=f"Збережено аукціони в MongoDB",
                event_type='database_save',
                metadata={
                    'auctions_count': len(auctions),
                    'created': created_count,
                    'updated': updated_count,
                    'unchanged': unchanged_count,
                    'deleted_rental': deleted_rental_count,
                    'errors': errors_count,
                    'llm_requests': llm_requests_count
                }
            )
        except:
            pass
        
        # Рахуємо кількість збережених аукціонів (без аренди)
        saved_count = created_count + updated_count + unchanged_count
        
        return {
            'llm_requests_count': llm_requests_count,
            'saved_count': saved_count
        }
    
    def _analyze_auctions_before_save(self, auctions: List[AuctionDTO]) -> Dict[str, int]:
        """
        Аналізує аукціони перед збереженням для отримання статистики.
        
        Args:
            auctions: Список аукціонів для аналізу
            
        Returns:
            Словник зі статистикою: total, unchanged, changed, llm_planned
        """
        if not auctions:
            return {'total': 0, 'unchanged': 0, 'changed': 0, 'llm_planned': 0}
        
        total = 0
        unchanged = 0
        changed = 0
        llm_planned = 0
        
        for auction in auctions:
            try:
                if not auction.data:
                    continue
                
                auction_data = auction.data
                auction_id = extract_auction_id(auction_data)
                
                if not auction_id:
                    continue
                
                # Перевіряємо, чи це аренда - аренду не враховуємо
                if self._is_rental_auction(auction_data):
                    continue
                
                total += 1
                
                # Обчислюємо версію об'єкта
                version_hash = calculate_object_version_hash(auction_data)
                
                # Витягуємо опис для обчислення хешу опису
                description = ''
                if 'description' in auction_data:
                    desc_obj = auction_data['description']
                    if isinstance(desc_obj, dict):
                        description = desc_obj.get('uk_UA', desc_obj.get('en_US', ''))
                    elif isinstance(desc_obj, str):
                        description = desc_obj
                
                description_hash = None
                if description:
                    description_hash = calculate_description_hash(description)
                
                # Шукаємо аукціон у базі
                existing_auction = self.auctions_repository.find_by_auction_id(auction_id)
                
                if existing_auction:
                    # Перевіряємо, чи змінилася версія
                    if existing_auction.get('version_hash') == version_hash:
                        unchanged += 1
                        # Перевіряємо, чи потрібен виклик LLM (немає в кеші)
                        existing_description_hash = existing_auction.get('description_hash')
                        if existing_description_hash and description:
                            cached_entry = self.llm_cache_service.repository.find_by_description_hash(existing_description_hash)
                            if not cached_entry and self.llm_service:
                                llm_planned += 1
                    else:
                        changed += 1
                        # Перевіряємо, чи потрібен виклик LLM (немає в кеші)
                        if description_hash and self.llm_service:
                            cached_entry = self.llm_cache_service.repository.find_by_description_hash(description_hash)
                            if not cached_entry:
                                llm_planned += 1
                else:
                    # Новий аукціон
                    changed += 1
                    # Перевіряємо, чи потрібен виклик LLM (немає в кеші)
                    if description_hash and self.llm_service:
                        cached_entry = self.llm_cache_service.repository.find_by_description_hash(description_hash)
                        if not cached_entry:
                            llm_planned += 1
            except Exception:
                # Помилка аналізу - пропускаємо
                pass
        
        return {
            'total': total,
            'unchanged': unchanged,
            'changed': changed,
            'llm_planned': llm_planned
        }
    
    def _process_auction_with_llm(self, auction_data: Dict[str, Any]) -> int:
        """
        Обробляє аукціон через LLM для парсингу опису.
        
        Викликається тільки якщо:
        - Це не аренда
        - Є опис
        - Є LLM сервіс
        - Немає результату в кеші по description_hash
        
        Args:
            auction_data: Дані аукціону
            
        Returns:
            int: 1 якщо був успішний виклик LLM, 0 якщо виникла помилка
        """
        # Витягуємо опис
        description = ''
        if 'description' in auction_data:
            desc_obj = auction_data['description']
            if isinstance(desc_obj, dict):
                description = desc_obj.get('uk_UA', desc_obj.get('en_US', ''))
            elif isinstance(desc_obj, str):
                description = desc_obj
        
        if not description or not self.llm_service:
            return 0
        
        try:
            # Викликаємо LLM
            llm_result = self.llm_service.parse_auction_description(description)
            # Зберігаємо результат в кеш
            self.llm_cache_service.save_result(description, llm_result)
            # Повертаємо 1, оскільки був реальний виклик LLM
            return 1
        except KeyboardInterrupt:
            # Переривання користувача - пробрасуємо далі
            raise
        except Exception as e:
            # Помилка обробки - не рахуємо
            return 0
    
    def _get_region_from_auction_data(self, auction_data: Dict[str, Any]) -> Optional[str]:
        """
        Витягує назву області/регіону з auction_data для перевірки «області для LLM».
        Пріоритет: address_refs[].region.name → items[].address.region.uk_UA.
        Повертає нормалізовану назву (без « область»/« обл.») або None.
        """
        if not auction_data:
            return None
        # address_refs (нормалізовані топоніми)
        refs = auction_data.get("address_refs") or auction_data.get("auction_data", {}).get("address_refs")
        if refs and isinstance(refs, list):
            for r in refs[:1]:
                if not isinstance(r, dict):
                    continue
                region = r.get("region")
                if isinstance(region, dict):
                    name = region.get("name") or ""
                elif isinstance(region, str):
                    name = region
                else:
                    continue
                if name:
                    return normalize_region_name(name)
        # items[].address.region.uk_UA
        items = auction_data.get("items") or auction_data.get("auction_data", {}).get("items")
        if items and isinstance(items, list):
            for it in items[:1]:
                if not isinstance(it, dict):
                    continue
                addr = it.get("address") or {}
                region = addr.get("region") or {}
                if isinstance(region, dict):
                    name = region.get("uk_UA") or region.get("uk") or ""
                else:
                    name = str(region) if region else ""
                if name:
                    return normalize_region_name(name)
        return None

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
    
    def _extract_structured_info_from_items(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Витягує структуровані дані з items аукціону перед парсингом через LLM.
        
        Args:
            data: Дані аукціону з auction.data
            
        Returns:
            Dict з структурованою інформацією
        """
        result = {
            'cadastral_number': '',
            'building_area_sqm': 0.0,  # Площа нерухомості в м²
            'land_area_ha': 0.0,       # Площа земельної ділянки в га
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
        
        # Витягуємо площі з усіх items
        areas = self._extract_areas_from_items(data)
        result['building_area_sqm'] = areas['building_area_sqm']
        result['land_area_ha'] = areas['land_area_ha']
        
        items = data.get('items', [])
        if not isinstance(items, list) or len(items) == 0:
            return result
        
        # Беремо перший item (зазвичай є один)
        item = items[0]
        
        # Кадастровий номер
        item_props = item.get('itemProps', {})
        if item_props:
            cadastral = item_props.get('cadastralNumber', '')
            if cadastral:
                result['cadastral_number'] = str(cadastral)
            
            # Площі вже витягнуті через _extract_areas_from_items на початку методу
            
            # Комунікації
            has_utilities = item_props.get('hasUtilitiesAvailability', False)
            has_encumbrances = item_props.get('hasEncumbrances', False)
            
            if has_utilities:
                result['utilities'] = 'електрика, вода, газ, опалення'  # Загальна інформація
            elif has_utilities is False:
                result['utilities'] = 'відсутні'
            
            if has_encumbrances:
                result['arrests_info'] = 'Є обтяження'
        
        # Адреса
        address = item.get('address', {})
        if address:
            region = address.get('region', {})
            if isinstance(region, dict):
                region_ua = region.get('uk_UA', '')
                if region_ua:
                    # Прибираємо слово "область" для консистентності
                    region_ua = region_ua.replace(' область', '').replace(' обл.', '').strip()
                    result['address_region'] = region_ua
            
            locality = address.get('locality', {})
            if isinstance(locality, dict):
                locality_ua = locality.get('uk_UA', '')
                if locality_ua:
                    result['address_city'] = locality_ua
            
            street_address = address.get('streetAddress', {})
            if isinstance(street_address, dict):
                street_ua = street_address.get('uk_UA', '')
                if street_ua:
                    # Спробуємо розділити тип вулиці та назву
                    street_parts = street_ua.split(' ', 1)
                    if len(street_parts) > 1:
                        result['address_street_type'] = street_parts[0]
                        result['address_street'] = street_parts[1]
                    else:
                        result['address_street'] = street_ua
            
            # Додаткова інформація про населений пункт
            address_id = address.get('addressID', {})
            if address_id:
                address_name = address_id.get('name', {})
                if isinstance(address_name, dict):
                    name_ua = address_name.get('uk_UA', '')
                    if name_ua and not result['address_city']:
                        result['address_city'] = name_ua
        
        # Тип нерухомості
        classification = item.get('classification', {})
        if classification:
            class_id = classification.get('id', '')
            if class_id:
                if class_id.startswith('06'):
                    result['property_type'] = 'Земля під будівництво'
                elif class_id.startswith('04'):
                    result['property_type'] = 'Нерухомість'
                else:
                    result['property_type'] = 'інше'
        
        return result

    def _extract_address_from_item_address(self, address: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Конвертує адресу з формату items.address у формат системи.
        
        Args:
            address: Адреса з item.address
            
        Returns:
            Dict з адресою у форматі системи або None, якщо адреса відсутня
        """
        if not address:
            return None
        
        # Створюємо об'єкт адреси у форматі системи
        address_obj = {
            'region': '',
            'district': '',
            'settlement_type': '',
            'settlement': '',
            'settlement_district': '',
            'street_type': '',
            'street': '',
            'building': '',
            'building_part': '',
            'room': ''
        }
        
        # Область
        region = address.get('region', {})
        if isinstance(region, dict):
            region_ua = region.get('uk_UA', '')
            if region_ua:
                # Прибираємо слово "область" для консистентності
                region_ua = region_ua.replace(' область', '').replace(' обл.', '').strip()
                address_obj['region'] = region_ua
        
        # Населений пункт (locality)
        locality = address.get('locality', {})
        if isinstance(locality, dict):
            locality_ua = locality.get('uk_UA', '')
            if locality_ua:
                # Парсимо тип населеного пункту (м., с., смт. тощо)
                locality_ua = locality_ua.strip()
                # Перевіряємо, чи починається з типу населеного пункту
                # Спочатку перевіряємо довші патерни, потім коротші
                settlement_type_patterns = [
                    'смт.', 'смт ', 'с-ще', 'с-ще ',
                    'м.', 'м ', 'с.', 'с '
                ]
                settlement_type = ''
                settlement = locality_ua
                
                for pattern in settlement_type_patterns:
                    if locality_ua.startswith(pattern):
                        settlement_type = pattern.rstrip(' ')
                        settlement = locality_ua[len(pattern):].strip()
                        break
                
                address_obj['settlement_type'] = settlement_type
                address_obj['settlement'] = settlement
        
        # Вулиця (streetAddress)
        street_address = address.get('streetAddress', {})
        if isinstance(street_address, dict):
            street_ua = street_address.get('uk_UA', '')
            if street_ua:
                # Парсимо вулицю (може бути "вулиця Івана Богуна" або "вул. Соборності, 7")
                street_ua = street_ua.strip()
                
                # Спробуємо розділити тип вулиці, назву та номер будинку
                # Формати можуть бути: "вул. Соборності, 7", "вул. Соборності 7", "вул. Соборності", "вулиця Івана Богуна"
                street_parts = street_ua.split(',', 1)
                street_part = street_parts[0].strip()
                building_part = street_parts[1].strip() if len(street_parts) > 1 else ''
                
                # Розділяємо тип вулиці та назву
                street_type_patterns = [
                    'вулиця', 'вул.', 'просп.', 'бул.', 'пров.', 'пл.',
                    'вулиця ', 'вул ', 'просп ', 'бул ', 'пров ', 'пл '
                ]
                street_type = ''
                street = street_part
                
                for pattern in street_type_patterns:
                    if street_part.startswith(pattern):
                        # Конвертуємо "вулиця" в "вул."
                        if pattern.startswith('вулиця'):
                            street_type = 'вул.'
                        else:
                            street_type = pattern.rstrip(' ')
                        street = street_part[len(pattern):].strip()
                        break
                
                address_obj['street_type'] = street_type
                address_obj['street'] = street
                
                # Номер будинку
                if building_part:
                    # Можуть бути формати: "7", "7 корпус А", "7, корпус А"
                    building_part = building_part.replace(',', ' ').strip()
                    building_parts = building_part.split(' ', 1)
                    address_obj['building'] = building_parts[0]
                    if len(building_parts) > 1:
                        address_obj['building_part'] = building_parts[1]
                else:
                    # Спробуємо знайти номер будинку в кінці назви вулиці
                    street_words = street.split()
                    if street_words and street_words[-1].isdigit():
                        address_obj['building'] = street_words[-1]
                        address_obj['street'] = ' '.join(street_words[:-1])
        
        # Перевіряємо, чи є хоча б одна заповнена частина адреси
        has_address = any([
            address_obj['region'],
            address_obj['settlement'],
            address_obj['street'],
            address_obj['building']
        ])
        
        return address_obj if has_address else None

    def _extract_addresses_from_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Витягує адреси з масиву items та конвертує їх у формат системи.
        
        Args:
            data: Дані аукціону з auction.data
            
        Returns:
            Список адрес у форматі системи (може бути порожнім)
        """
        items = data.get('items', [])
        if not isinstance(items, list) or len(items) == 0:
            return []
        
        addresses = []
        for item in items:
            address = item.get('address', {})
            if address:
                address_obj = self._extract_address_from_item_address(address)
                if address_obj:
                    addresses.append(address_obj)
        
        return addresses

    def _is_area_unit(self, unit: str) -> bool:
        """
        Перевіряє, чи одиниця виміру є одиницею площі.
        
        Args:
            unit: Одиниця виміру
            
        Returns:
            True, якщо одиниця є одиницею площі
        """
        if not unit:
            return False
        
        unit_lower = unit.lower()
        # Одиниці площі
        area_units = [
            'гектар', 'hectare', 'га',
            'квадратний метр', 'square metre', 'м²', 'м2', 'кв.м', 'кв м', 'квадратних метрів',
            'сотка', 'соток', 'ar'
        ]
        
        return any(area_unit in unit_lower for area_unit in area_units)
    
    def _normalize_area_unit(self, unit: str) -> str:
        """
        Нормалізує одиницю виміру площі до стандартного формату.
        
        Args:
            unit: Одиниця виміру
            
        Returns:
            Нормалізована одиниця виміру: 'гектар', 'м²', 'сотка' або порожній рядок
        """
        if not unit:
            return ''
        
        unit_lower = unit.lower()
        
        # Гектари
        if any(x in unit_lower for x in ['гектар', 'hectare', 'га']):
            return 'гектар'
        
        # Квадратні метри
        if any(x in unit_lower for x in ['квадратний метр', 'square metre', 'м²', 'м2', 'кв.м', 'кв м', 'квадратних метрів']):
            return 'м²'
        
        # Сотки
        if any(x in unit_lower for x in ['сотка', 'соток', 'ar']):
            return 'сотка'
        
        return ''
    
    def _convert_area_to_sqm(self, value: float, unit: str) -> float:
        """
        Конвертує площу в квадратні метри.
        
        Args:
            value: Значення площі
            unit: Одиниця виміру ('гектар', 'м²', 'сотка', 'акр' тощо)
            
        Returns:
            Площа в квадратних метрах
        """
        if not value or not unit:
            return 0.0
        
        unit_lower = unit.lower()
        
        # Вже в квадратних метрах
        if any(x in unit_lower for x in ['м²', 'м2', 'кв.м', 'кв м', 'квадратний метр', 'square metre']):
            return float(value)
        
        # Гектари → м² (1 га = 10000 м²)
        if any(x in unit_lower for x in ['гектар', 'hectare', 'га']):
            return float(value) * 10000.0
        
        # Сотки → м² (1 сотка = 100 м²)
        if any(x in unit_lower for x in ['сотка', 'соток', 'ar']):
            return float(value) * 100.0
        
        # Акре → м² (1 акр ≈ 4046.86 м²)
        if any(x in unit_lower for x in ['акр', 'acre', 'ac']):
            return float(value) * 4046.86
        
        # Якщо одиниця невідома, повертаємо як є (припускаємо, що це вже м²)
        return float(value)
    
    def _convert_area_to_hectares(self, value: float, unit: str) -> float:
        """
        Конвертує площу в гектари.
        
        Args:
            value: Значення площі
            unit: Одиниця виміру ('гектар', 'м²', 'сотка', 'акр' тощо)
            
        Returns:
            Площа в гектарах
        """
        if not value or not unit:
            return 0.0
        
        unit_lower = unit.lower()
        
        # Вже в гектарах
        if any(x in unit_lower for x in ['гектар', 'hectare', 'га']):
            return float(value)
        
        # м² → га (1 м² = 0.0001 га)
        if any(x in unit_lower for x in ['м²', 'м2', 'кв.м', 'кв м', 'квадратний метр', 'square metre']):
            return float(value) * 0.0001
        
        # Сотки → га (1 сотка = 0.01 га)
        if any(x in unit_lower for x in ['сотка', 'соток', 'ar']):
            return float(value) * 0.01
        
        # Акре → га (1 акр ≈ 0.404686 га)
        if any(x in unit_lower for x in ['акр', 'acre', 'ac']):
            return float(value) * 0.404686
        
        # Якщо одиниця невідома, припускаємо що це м²
        return float(value) * 0.0001
    
    def _extract_areas_from_items(self, data: Dict[str, Any]) -> Dict[str, float]:
        """
        Витягує та сумує площі нерухомості та земельних ділянок з усіх items.
        Аналізує кількість об'єктів, щоб уникнути подвійного підрахунку.
        
        Args:
            data: Дані аукціону з auction.data
            
        Returns:
            Dict з ключами 'building_area_sqm' (площа нерухомості в м²) та 'land_area_ha' (площа землі в га)
        """
        result = {
            'building_area_sqm': 0.0,
            'land_area_ha': 0.0
        }
        
        items = data.get('items', [])
        if not isinstance(items, list) or len(items) == 0:
            return result
        
        # Аналізуємо items для визначення унікальних об'єктів
        # Групуємо items за ID, кадастровим номером або комбінацією полів, щоб уникнути подвійного підрахунку
        processed_items = set()  # Множина оброблених унікальних ідентифікаторів
        
        for item in items:
            # Формуємо унікальний ідентифікатор об'єкта
            item_id = item.get('id', '')
            item_props = item.get('itemProps', {})
            cadastral_number = item_props.get('cadastralNumber', '') if item_props else ''
            
            # Використовуємо item_id якщо є, інакше кадастровий номер, інакше індекс
            unique_id = item_id if item_id else (cadastral_number if cadastral_number else f"item_{items.index(item)}")
            
            if unique_id in processed_items:
                continue  # Пропускаємо дублікати
            
            # Визначаємо тип об'єкта
            item_props_type = item_props.get('itemPropsType', '')
            classification = item.get('classification', {})
            class_id = classification.get('id', '') if classification else ''
            
            # Визначаємо одиницю виміру
            unit = item.get('unit', {})
            unit_ua = ''
            if unit:
                unit_name = unit.get('name', {})
                if isinstance(unit_name, dict):
                    unit_ua = unit_name.get('uk_UA', '')
            
            # Перевіряємо, чи одиниця виміру є одиницею площі
            is_area_unit = self._is_area_unit(unit_ua) if unit_ua else False
            
            if not is_area_unit:
                continue  # Пропускаємо, якщо одиниця не є одиницею площі
            
            normalized_unit = self._normalize_area_unit(unit_ua)
            
            # Для землі використовуємо landArea
            if item_props_type == 'land' or (class_id and class_id.startswith('06')):
                land_area = item_props.get('landArea')
                if land_area is not None:
                    # Конвертуємо в гектари
                    if normalized_unit == 'гектар':
                        area_ha = float(land_area)
                    elif normalized_unit == 'м²':
                        area_ha = float(land_area) * 0.0001
                    elif normalized_unit == 'сотка':
                        area_ha = float(land_area) * 0.01
                    else:
                        # Якщо одиниця невідома, припускаємо гектари
                        area_ha = float(land_area)
                    
                    result['land_area_ha'] += area_ha
                    processed_items.add(unique_id)
            else:
                # Для будівель/нерухомості використовуємо totalObjectArea або totalBuildingArea
                total_object_area = item_props.get('totalObjectArea')
                total_building_area = item_props.get('totalBuildingArea')
                usable_area = item_props.get('usableArea')
                
                # Пріоритет: totalObjectArea > totalBuildingArea > usableArea
                building_area = None
                if total_object_area is not None:
                    building_area = total_object_area
                elif total_building_area is not None:
                    building_area = total_building_area
                elif usable_area is not None:
                    building_area = usable_area
                
                if building_area is not None:
                    # Конвертуємо в квадратні метри
                    if normalized_unit == 'м²':
                        area_sqm = float(building_area)
                    elif normalized_unit == 'гектар':
                        area_sqm = float(building_area) * 10000.0
                    elif normalized_unit == 'сотка':
                        area_sqm = float(building_area) * 100.0
                    else:
                        # Якщо одиниця невідома, припускаємо м²
                        area_sqm = float(building_area)
                    
                    result['building_area_sqm'] += area_sqm
                    processed_items.add(unique_id)
        
        return result

    def save_auctions_to_csv(self, auctions: List[AuctionDTO], days: int = 1, output_dir: Optional[str] = None, user_id: Optional[int] = None) -> tuple:
        """
        Зберігає список аукціонів у Excel файл з вибраними полями.
        Зберігає в каталог "archives".

        Args:
            auctions: Список аукціонів для збереження
            days: Кількість днів виборки (для метаданих)
            output_dir: Директорія для збереження. Якщо не вказано, використовується archives/
            user_id: Ідентифікатор користувача, який сформував файл (опціонально)

        Returns:
            tuple: (Шлях до збереженого файлу, кількість запитів до LLM без кешу)
        """
        def ensure_string(value):
            """Конвертує значення в рядок, обробляючи None та інші типи."""
            if value is None:
                return ''
            return str(value)
        
        def format_full_address(address_obj: Dict[str, Any]) -> str:
            """
            Формує повну адресу з об'єкта адреси у форматі:
            Область, Район (за наявності), скорочено тип н.п. (за наявності) Населений пункт, 
            або інший топонім (сільрада, міськрада, тощо), район нп. (за наявності), 
            вулиця (за наявності), номер будинку, номер блоку/корпусу (за наявності), 
            номер приміщення (офіс, квартира, тощо, за наявності).
            """
            parts = []
            
            # Область
            if address_obj.get('region'):
                parts.append(address_obj['region'])
            
            # Район
            if address_obj.get('district'):
                parts.append(address_obj['district'])
            
            # Тип населеного пункту + Населений пункт
            settlement_type = address_obj.get('settlement_type', '')
            settlement = address_obj.get('settlement', '')
            if settlement:
                if settlement_type:
                    parts.append(f"{settlement_type} {settlement}")
                else:
                    parts.append(settlement)
            
            # Район населеного пункту
            if address_obj.get('settlement_district'):
                parts.append(address_obj['settlement_district'])
            
            # Вулиця
            street_type = address_obj.get('street_type', '')
            street = address_obj.get('street', '')
            if street:
                if street_type:
                    parts.append(f"{street_type} {street}")
                else:
                    parts.append(street)
            
            # Номер будинку
            if address_obj.get('building'):
                parts.append(address_obj['building'])
            
            # Блок/корпус
            if address_obj.get('building_part'):
                parts.append(address_obj['building_part'])
            
            # Приміщення
            if address_obj.get('room'):
                parts.append(address_obj['room'])
            
            return ', '.join(parts) if parts else ''
        
        def format_address(address_street_type: str, address_street: str, address_building: str) -> str:
            """Формує адресу з компонентів (для сумісності зі старим форматом)."""
            parts = []
            if address_street_type:
                parts.append(address_street_type)
            if address_street:
                parts.append(address_street)
            if address_building:
                parts.append(address_building)
            return ', '.join(parts) if parts else ''
        
        def format_date(date_str: str) -> str:
            """Форматує дату у форматі дд.ММ.рррр ГГ:ХХ (київський час)."""
            return format_date_display(date_str, '%d.%m.%Y %H:%M')
        
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
        # Площа нерухомості (кв. м.), Площа земельної ділянки (га), Стартова ціна, Розмір взносу, Дата торгів, Дата фінальної подачі документів,
        # Мінімальна кількість учасників, Кількість зареєстрованих учасників, Арешти, Опис, Посилання, Код класифікатора,
        # Повторний аукціон, Посилання на минулі аукціони
        fieldnames = [
            'date_updated',                      # Дата оновлення
            'address_region',                    # Область
            'address_city',                      # Населений пункт
            'address',                           # Адреса
            'property_type',                     # Тип нерухомості
            'cadastral_number',                  # Кадастровий номер
            'building_area_sqm',                 # Площа нерухомості (кв. м.)
            'land_area_ha',                      # Площа земельної ділянки (га)
            'base_price',                        # Стартова ціна
            'deposit_amount',                    # Розмір взносу
            'auction_start_date',                # Дата торгів
            'document_submission_deadline',      # Дата фінальної подачі документів
            'min_participants_count',            # Мінімальна кількість учасників
            'participants_count',                # Кількість зареєстрованих учасників
            'arrests_info',                      # Арешти
            'description',                       # Опис
            'auction_url',                       # Посилання
            'classification_code',               # Код класифікатора
            'is_repeat_auction',                 # Повторний аукціон
            'previous_auctions_links'            # Посилання на минулі аукціони
        ]
        
        # Українські назви колонок
        column_headers = {
            'date_updated': 'Дата оновлення',
            'address_region': 'Область',
            'address_city': 'Населений пункт',
            'address': 'Адреса',
            'property_type': 'Тип нерухомості',
            'cadastral_number': 'Кадастровий номер',
            'building_area_sqm': 'Площа нерухомості (кв. м.)',
            'land_area_ha': 'Площа земельної ділянки (га)',
            'base_price': 'Стартова ціна',
            'deposit_amount': 'Розмір взносу',
            'auction_start_date': 'Дата торгів',
            'document_submission_deadline': 'Дата фінальної подачі документів',
            'min_participants_count': 'Мінімальна кількість учасників',
            'participants_count': 'Кількість зареєстрованих учасників',
            'arrests_info': 'Арешти',
            'description': 'Опис',
            'auction_url': 'Посилання',
            'classification_code': 'Код класифікатора',
            'is_repeat_auction': 'Повторний аукціон',
            'previous_auctions_links': 'Посилання на минулі аукціони'
        }
        
        from tqdm import tqdm
        
        auctions_data = []
        total_auctions = len(auctions)
        llm_requests_count = 0  # Кількість запитів до LLM (без кешу)
        
        print(f"Початок обробки {total_auctions} аукціонів для збереження в Excel...")
        
        # Логуємо початок обробки даних
        try:
            self.logging_service.log_app_event(
                message=f"Почато обробку {total_auctions} аукціонів для збереження в Excel",
                event_type='processing_start',
                metadata={'auctions_count': total_auctions}
            )
        except:
            pass
        
        # Прогрес-бар для обробки аукціонів
        for auction in tqdm(auctions, desc="Обробка аукціонів", unit="аукціон", ncols=100):
            if not auction.data:
                continue
            
            data = auction.data
            
            # Посилання на аукціон
            # Використовуємо extract_auction_id для консистентності з іншими частинами коду
            auction_id = extract_auction_id(data) or auction.id
            if not auction_id:
                # Якщо не вдалося витягти ID, спробуємо альтернативні методи
                auction_id = data.get('auctionId') or data.get('_id') or auction.id
            auction_url = f"https://prozorro.sale/auction/{auction_id}" if auction_id else ""
            
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
            
            # Спочатку витягуємо структуровані дані з items (якщо є) - fallback
            structured_info = self._extract_structured_info_from_items(data)
            
            # Перевірка, чи це аренда (якщо так - пропускаємо обробку через LLM)
            is_rental = self._is_rental_auction(data)
            if is_rental:
                # Пропускаємо обробку через LLM для аренди, використовуємо тільки структуровані дані
                parsed_info = structured_info
            else:
                # Парсинг опису через LLM для отримання структурованої інформації
                parsed_info = structured_info.copy()  # Починаємо зі структурованих даних
            
            if description and self.llm_service and not is_rental:
                import traceback
                try:
                    # Спочатку перевіряємо кеш
                    cached_result = self.llm_cache_service.get_cached_result(description)
                    
                    if cached_result is not None:
                        # Використовуємо результат з кешу
                        llm_result = cached_result
                    else:
                        # Парсинг опису через LLM (прогрес-бар вже показує прогрес обробки)
                        desc_hash = calculate_description_hash(description)
                        llm_result = self.llm_service.parse_auction_description(description)
                        # Зберігаємо результат в кеш
                        self.llm_cache_service.save_result(description, llm_result)
                        # Підраховуємо запити до LLM (без кешу)
                        llm_requests_count += 1
                    
                    # Об'єднуємо результати: структуровані дані мають пріоритет, але LLM може доповнити порожні поля
                    # Спочатку витягуємо адреси з items (якщо є)
                    items_addresses = self._extract_addresses_from_items(data)
                    
                    # Обробляємо адреси з масиву LLM
                    llm_addresses = llm_result.get('addresses', [])
                    if not llm_addresses:
                        # Якщо немає масиву адрес, але є старі поля - створюємо адресу
                        if llm_result.get('address_region') or llm_result.get('address_city'):
                            llm_addresses = [{
                                'region': llm_result.get('address_region', ''),
                                'district': llm_result.get('address_district', ''),
                                'settlement_type': llm_result.get('address_settlement_type', ''),
                                'settlement': llm_result.get('address_city', ''),
                                'settlement_district': llm_result.get('address_settlement_district', ''),
                                'street_type': llm_result.get('address_street_type', ''),
                                'street': llm_result.get('address_street', ''),
                                'building': llm_result.get('address_building', ''),
                                'building_part': llm_result.get('address_building_part', ''),
                                'room': llm_result.get('address_room', '')
                            }]
                    
                    # Формуємо фінальний масив адрес:
                    # - Якщо є адреси з items - вони стають основними (першими)
                    # - Адреси з LLM додаються як додаткові (після адрес з items)
                    final_addresses = []
                    if items_addresses:
                        # Адреси з items - основні
                        final_addresses.extend(items_addresses)
                        # Додаємо адреси з LLM як додаткові
                        final_addresses.extend(llm_addresses)
                    else:
                        # Якщо немає адрес з items - використовуємо адреси з LLM
                        final_addresses = llm_addresses
                    
                    parsed_info['addresses'] = final_addresses
                    
                    # Об'єднуємо інші поля: структуровані дані мають пріоритет, LLM доповнює порожні
                    for key in ['cadastral_number', 'floor', 'property_type', 'utilities', 'arrests_info']:
                        if not parsed_info.get(key) and llm_result.get(key):
                            parsed_info[key] = llm_result[key]
                    
                    # Окремо обробляємо площі з LLM - конвертуємо в стандартні одиниці та сумуємо
                    # Площа нерухомості (building_area_sqm)
                    if llm_result.get('building_area_sqm'):
                        try:
                            llm_building_area = float(str(llm_result.get('building_area_sqm', '')).replace(',', '.').replace(' ', ''))
                            # Додаємо до існуючої площі (якщо є з items)
                            current_building_area = parsed_info.get('building_area_sqm', 0.0)
                            if isinstance(current_building_area, (int, float)):
                                parsed_info['building_area_sqm'] = current_building_area + llm_building_area
                            else:
                                parsed_info['building_area_sqm'] = llm_building_area
                        except (ValueError, AttributeError):
                            pass
                    
                    # Площа землі (land_area_ha)
                    if llm_result.get('land_area_ha'):
                        try:
                            llm_land_area = float(str(llm_result.get('land_area_ha', '')).replace(',', '.').replace(' ', ''))
                            # Додаємо до існуючої площі (якщо є з items)
                            current_land_area = parsed_info.get('land_area_ha', 0.0)
                            if isinstance(current_land_area, (int, float)):
                                parsed_info['land_area_ha'] = current_land_area + llm_land_area
                            else:
                                parsed_info['land_area_ha'] = llm_land_area
                        except (ValueError, AttributeError):
                            pass
                    
                    # Якщо є старі поля area та area_unit - конвертуємо їх (для сумісності зі старими кешами)
                    if llm_result.get('area') and not llm_result.get('building_area_sqm') and not llm_result.get('land_area_ha'):
                        try:
                            old_area = float(str(llm_result.get('area', '')).replace(',', '.').replace(' ', ''))
                            old_unit = llm_result.get('area_unit', '').lower()
                            
                            if any(x in old_unit for x in ['гектар', 'hectare', 'га']):
                                # Додаємо до існуючої площі землі
                                current_land_area = parsed_info.get('land_area_ha', 0.0)
                                if isinstance(current_land_area, (int, float)):
                                    parsed_info['land_area_ha'] = current_land_area + old_area
                                else:
                                    parsed_info['land_area_ha'] = old_area
                            elif any(x in old_unit for x in ['м²', 'м2', 'кв.м', 'квадратний метр']):
                                # Додаємо до існуючої площі нерухомості
                                current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                if isinstance(current_building_area, (int, float)):
                                    parsed_info['building_area_sqm'] = current_building_area + old_area
                                else:
                                    parsed_info['building_area_sqm'] = old_area
                            elif any(x in old_unit for x in ['сотка', 'соток']):
                                # Сотки - визначаємо за значенням
                                if old_area < 100:
                                    # Швидше за все це гектари (менше 1 га)
                                    area_ha = old_area * 0.01
                                    current_land_area = parsed_info.get('land_area_ha', 0.0)
                                    if isinstance(current_land_area, (int, float)):
                                        parsed_info['land_area_ha'] = current_land_area + area_ha
                                    else:
                                        parsed_info['land_area_ha'] = area_ha
                                else:
                                    # Швидше за все це м²
                                    area_sqm = old_area * 100
                                    current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                    if isinstance(current_building_area, (int, float)):
                                        parsed_info['building_area_sqm'] = current_building_area + area_sqm
                                    else:
                                        parsed_info['building_area_sqm'] = area_sqm
                            else:
                                # Якщо одиниця невідома - визначаємо за значенням
                                if old_area > 1000:
                                    current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                    if isinstance(current_building_area, (int, float)):
                                        parsed_info['building_area_sqm'] = current_building_area + old_area
                                    else:
                                        parsed_info['building_area_sqm'] = old_area
                                elif old_area < 10:
                                    current_land_area = parsed_info.get('land_area_ha', 0.0)
                                    if isinstance(current_land_area, (int, float)):
                                        parsed_info['land_area_ha'] = current_land_area + old_area
                                    else:
                                        parsed_info['land_area_ha'] = old_area
                        except (ValueError, AttributeError):
                            pass
                except KeyboardInterrupt:
                    # Переривання користувача - пробрасуємо далі
                    raise
                except Exception as e:
                    # Детальне логування помилки
                    error_traceback = traceback.format_exc()
                    desc_hash = calculate_description_hash(description) if description else None
                    description_preview = description[:200] + "..." if len(description) > 200 else description
                    print(f"\n[LLM ПОМИЛКА] Помилка обробки через LLM для аукціону {auction_id}")
                    print(f"[LLM ПОМИЛКА] Хеш опису: {desc_hash[:16] if desc_hash else 'N/A'}...")
                    print(f"[LLM ПОМИЛКА] Тип помилки: {type(e).__name__}")
                    print(f"[LLM ПОМИЛКА] Повідомлення: {str(e)}")
                    print(f"[LLM ПОМИЛКА] Опис (перші 200 символів): {description_preview}")
                    print(f"[LLM ПОМИЛКА] Повний traceback:")
                    print(error_traceback)
                    
                    # Логуємо в сервіс логування
                    try:
                        self.logging_service.log_app_event(
                            message=f"Помилка обробки через LLM для аукціону {auction_id}",
                            event_type='llm_error',
                            metadata={
                                'auction_id': auction_id,
                                'description_hash': desc_hash[:16] if desc_hash else None,
                                'description_preview': description_preview,
                                'error_type': type(e).__name__,
                                'error_message': str(e)
                            },
                            error=error_traceback
                        )
                    except:
                        pass
                    
                    # Інші помилки - просто пропускаємо парсинг для цього аукціону
                    pass
            
            # Обробляємо адреси: перевіряємо, чи є адреси з items (якщо LLM не використовувався)
            addresses = parsed_info.get('addresses', [])
            if not addresses:
                # Якщо немає масиву адрес, спочатку перевіряємо items
                items_addresses = self._extract_addresses_from_items(data)
                if items_addresses:
                    addresses = items_addresses
                else:
                    # Якщо немає адрес з items, але є старі поля - створюємо адресу
                    if parsed_info.get('address_region') or parsed_info.get('address_city'):
                        addresses = [{
                            'region': parsed_info.get('address_region', ''),
                            'district': parsed_info.get('address_district', ''),
                            'settlement_type': parsed_info.get('address_settlement_type', ''),
                            'settlement': parsed_info.get('address_city', ''),
                            'settlement_district': parsed_info.get('address_settlement_district', ''),
                            'street_type': parsed_info.get('address_street_type', ''),
                            'street': parsed_info.get('address_street', ''),
                            'building': parsed_info.get('address_building', ''),
                            'building_part': parsed_info.get('address_building_part', ''),
                            'room': parsed_info.get('address_room', '')
                        }]
            
            # Оновлюємо parsed_info з фінальним масивом адрес
            parsed_info['addresses'] = addresses
            
            # Створюємо посилання на топоніми через GeographyService
            address_refs_list = []
            try:
                from business.services.geography_service import GeographyService
                geography_service = GeographyService()
                for addr in addresses:
                    if isinstance(addr, dict):
                        address_refs = geography_service.resolve_address(addr)
                        if address_refs.get("region_id") or address_refs.get("city_id"):
                            address_refs_list.append(address_refs["address_refs"])
            except Exception:
                # Якщо GeographyService недоступний, продовжуємо без посилань
                pass
            
            if address_refs_list:
                parsed_info['address_refs'] = address_refs_list
            
            # Беремо першу адресу для полів область та місто
            first_address = addresses[0] if addresses else {}
            address_region = first_address.get('region', '')
            address_city = first_address.get('settlement', '')
            
            # Формуємо повну адресу з усіх адрес
            if addresses:
                formatted_addresses = []
                for idx, addr in enumerate(addresses, 1):
                    formatted_addr = format_full_address(addr)
                    if formatted_addr:
                        if len(addresses) > 1:
                            formatted_addresses.append(f"адреса {idx}: {formatted_addr}")
                        else:
                            formatted_addresses.append(formatted_addr)
                address = ', '.join(formatted_addresses) if formatted_addresses else ''
            else:
                # Fallback на старий формат
                address = format_address(
                    parsed_info.get('address_street_type', ''),
                    parsed_info.get('address_street', ''),
                    parsed_info.get('address_building', '')
                )
            
            # Формуємо площі нерухомості та землі окремо
            # Беремо площі зі структурованих даних (items) та з LLM, об'єднуємо їх
            building_area_sqm = parsed_info.get('building_area_sqm', 0.0)
            land_area_ha = parsed_info.get('land_area_ha', 0.0)
            
            # Конвертуємо в числа
            try:
                if building_area_sqm:
                    if isinstance(building_area_sqm, str):
                        building_area_sqm = float(str(building_area_sqm).replace(',', '.').replace(' ', ''))
                    else:
                        building_area_sqm = float(building_area_sqm)
                else:
                    building_area_sqm = 0.0
            except (ValueError, AttributeError, TypeError):
                building_area_sqm = 0.0
            
            try:
                if land_area_ha:
                    if isinstance(land_area_ha, str):
                        land_area_ha = float(str(land_area_ha).replace(',', '.').replace(' ', ''))
                    else:
                        land_area_ha = float(land_area_ha)
                else:
                    land_area_ha = 0.0
            except (ValueError, AttributeError, TypeError):
                land_area_ha = 0.0
            
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
            
            # Визначаємо повторний аукціон та посилання на минулі аукціони
            is_repeat_auction = 'ні'
            previous_auctions_links = ''
            
            # Перевіряємо, чи є auction_id та description для пошуку попередніх аукціонів
            if description and auction_id:
                # Обчислюємо хеш опису
                description_hash = calculate_description_hash(description)
                
                if description_hash:
                    # Знаходимо інші аукціони з тим самим описом
                    previous_auctions = self.auctions_repository.get_auctions_by_description_hash(
                        description_hash,
                        exclude_auction_id=auction_id
                    )
                    
                    if previous_auctions:
                        # Отримуємо дату створення поточного аукціону
                        current_date_created = None
                        date_created_str = data.get('dateCreated', '')
                        if date_created_str:
                            try:
                                if date_created_str.endswith('Z'):
                                    date_created_str = date_created_str.replace('Z', '+00:00')
                                current_date_created = datetime.fromisoformat(date_created_str)
                                if current_date_created.tzinfo:
                                    current_date_created = current_date_created.astimezone(timezone.utc)
                                else:
                                    current_date_created = current_date_created.replace(tzinfo=timezone.utc)
                            except (ValueError, AttributeError):
                                pass
                        
                        # Фільтруємо попередні аукціони за датою створення
                        previous_links = []
                        has_earlier_auction = False
                        
                        for prev_auction in previous_auctions:
                            prev_auction_data = prev_auction.get('auction_data', {})
                            if not prev_auction_data:
                                continue
                            
                            prev_auction_id = prev_auction.get('auction_id')
                            if not prev_auction_id:
                                continue
                            
                            # Отримуємо дату створення попереднього аукціону
                            prev_date_created_str = prev_auction_data.get('dateCreated', '')
                            prev_date_created = None
                            
                            if prev_date_created_str:
                                try:
                                    if prev_date_created_str.endswith('Z'):
                                        prev_date_created_str = prev_date_created_str.replace('Z', '+00:00')
                                    prev_date_created = datetime.fromisoformat(prev_date_created_str)
                                    if prev_date_created.tzinfo:
                                        prev_date_created = prev_date_created.astimezone(timezone.utc)
                                    else:
                                        prev_date_created = prev_date_created.replace(tzinfo=timezone.utc)
                                except (ValueError, AttributeError):
                                    pass
                            
                            # Якщо є дата створення поточного аукціону, порівнюємо
                            if current_date_created and prev_date_created:
                                if prev_date_created < current_date_created:
                                    has_earlier_auction = True
                                    prev_link = f"https://prozorro.sale/auction/{prev_auction_id}"
                                    previous_links.append(prev_link)
                            else:
                                # Якщо немає дати створення, додаємо всі попередні аукціони
                                has_earlier_auction = True
                                prev_link = f"https://prozorro.sale/auction/{prev_auction_id}"
                                previous_links.append(prev_link)
                        
                        # Визначаємо, чи є це повторний аукціон
                        if has_earlier_auction:
                            is_repeat_auction = 'так'
                        
                        # Формуємо посилання на минулі аукціони через символ повернення каретки і переносу строки
                        if previous_links:
                            previous_auctions_links = '\r\n'.join(previous_links)
            
            # Дата оновлення: dateModified якщо аукціон оновлювався, інакше dateCreated
            date_modified_str = data.get('dateModified', '')
            date_created_str = data.get('dateCreated', '')
            date_updated_str = date_modified_str if (date_modified_str and date_modified_str != date_created_str) else (date_created_str or date_modified_str)
            date_updated = format_date(date_updated_str) if date_updated_str else ''
            try:
                if date_updated_str:
                    s = date_updated_str.replace('Z', '+00:00') if date_updated_str.endswith('Z') else date_updated_str
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo:
                        dt = dt.astimezone(timezone.utc)
                    else:
                        dt = dt.replace(tzinfo=timezone.utc)
                    date_updated_ts = dt
                else:
                    date_updated_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                date_updated_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
            
            auction_row = {
                'address_region': ensure_string(address_region),
                'address_city': ensure_string(address_city),
                'address': ensure_string(address),
                'property_type': ensure_string(parsed_info.get('property_type', '')),
                'cadastral_number': ensure_string(parsed_info.get('cadastral_number', '')),
                'building_area_sqm': ensure_string(building_area_sqm) if building_area_sqm > 0 else '',
                'land_area_ha': ensure_string(land_area_ha) if land_area_ha > 0 else '',
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
                'is_repeat_auction': ensure_string(is_repeat_auction),
                'previous_auctions_links': ensure_string(previous_auctions_links),
                'date_updated': ensure_string(date_updated),
                'date_updated_ts': date_updated_ts,
                '_has_additional_classification_03_07': has_additional_classification_03_07  # Службове поле для форматування
            }
            
            auctions_data.append(auction_row)

        print(f"Підготовлено {len(auctions_data)} рядків для збереження в Excel")
        
        # Перевіряємо, чи всі колонки присутні в даних
        if auctions_data:
            sample_row = auctions_data[0]
            missing_columns = [col for col in fieldnames if col not in sample_row]
            if missing_columns:
                print(f"Попередження: відсутні колонки в даних: {missing_columns}")
            # Перевіряємо нові колонки
            if 'is_repeat_auction' in sample_row:
                repeat_count = sum(1 for row in auctions_data if row.get('is_repeat_auction') == 'так')
                links_count = sum(1 for row in auctions_data if row.get('previous_auctions_links', '').strip())
                print(f"Знайдено {repeat_count} повторних аукціонів")
                print(f"Знайдено {links_count} аукціонів з посиланнями на минулі")
                # Діагностика: показуємо приклад значень
                if repeat_count > 0:
                    example = next((row for row in auctions_data if row.get('is_repeat_auction') == 'так'), None)
                    if example:
                        print(f"Приклад повторного аукціону: is_repeat_auction='{example.get('is_repeat_auction')}', previous_auctions_links='{example.get('previous_auctions_links', '')[:100]}...'")
            else:
                print("ПОМИЛКА: колонка 'is_repeat_auction' відсутня в даних!")
                print(f"Доступні колонки: {list(sample_row.keys())}")
        
        # Сортуємо за датою оновлення від найсвіжішого до найдавнішого
        _min_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        auctions_data.sort(key=lambda r: r.get('date_updated_ts') or _min_ts, reverse=True)
        
        save_excel_to_file(auctions_data, file_path, fieldnames, column_headers)
        print(f"Файл успішно збережено: {file_path}")
        
        # Логуємо завершення обробки з детальною інформацією
        try:
            self.logging_service.log_app_event(
                message=f"Завершено обробку аукціонів для збереження в Excel",
                event_type='processing_complete',
                metadata={
                    'total_auctions': total_auctions,
                    'processed_rows': len(auctions_data),
                    'llm_requests': llm_requests_count,
                    'llm_cached': total_auctions - llm_requests_count,
                    'file_path': file_path
                }
            )
        except:
            pass
        
        return file_path, llm_requests_count

    def get_auctions_from_db_by_period(self, days: int) -> List[Dict[str, Any]]:
        """
        Отримує аукціони з БД за період до збереженої дати оновлення.
        
        Args:
            days: Кількість днів періоду (1 або 7)
            
        Returns:
            Список аукціонів з БД
        """
        # Отримуємо дату останнього оновлення
        update_date = self.app_data_repository.get_update_date(days)
        
        if not update_date:
            # Якщо дати оновлення немає, повертаємо порожній список
            return []
        
        # Визначаємо діапазон дат
        date_to = update_date
        date_from = date_to - timedelta(days=days)
        
        # Отримуємо аукціони з БД за цей діапазон
        auctions = self.auctions_repository.get_auctions_by_date_range(date_from, date_to)
        
        return auctions
    
    def generate_excel_from_db(self, days: int) -> Optional[BytesIO]:
        """
        Генерує Excel файл в пам'яті з даних БД за період до збереженої дати оновлення.
        Викликає LLM для аукціонів, у яких description_hash є в БД, але немає в кеші LLM.
        
        Args:
            days: Кількість днів періоду (1 або 7)
            
        Returns:
            BytesIO з Excel файлом або None, якщо дані не знайдено
        """
        from transport.dto.prozorro_dto import AuctionDTO
        
        # Отримуємо аукціони з БД
        auctions_docs = self.get_auctions_from_db_by_period(days)
        
        if not auctions_docs:
            return None
        
        # Конвертуємо документи БД в AuctionDTO та перевіряємо кеш LLM
        # Навіть якщо версія не змінилась, перевіряємо кеш на випадок зміни промпту
        auctions = []
        for doc in auctions_docs:
            auction_data = doc.get('auction_data', {})
            description_hash = doc.get('description_hash')
            
            if auction_data:
                try:
                    auction = AuctionDTO.from_dict(auction_data)
                    # Перевіряємо, чи є description_hash і чи є він в кеші
                    # Навіть якщо версія не змінилась, перевіряємо кеш на випадок зміни промпту
                    if description_hash:
                        cached_entry = self.llm_cache_service.repository.find_by_description_hash(description_hash)
                        if not cached_entry:
                            # Якщо хеш є в БД, але немає в кеші - потрібно викликати LLM
                            # Передаємо опис для обробки
                            description = ''
                            if 'description' in auction_data:
                                desc_obj = auction_data['description']
                                if isinstance(desc_obj, dict):
                                    description = desc_obj.get('uk_UA', desc_obj.get('en_US', ''))
                                elif isinstance(desc_obj, str):
                                    description = desc_obj
                            
                            if description and self.llm_service and not self._is_rental_auction(auction_data):
                                import traceback
                                try:
                                    # Викликаємо LLM для парсингу
                                    auction_id_from_doc = doc.get('auction_id', 'unknown')
                                    desc_hash = calculate_description_hash(description)
                                    llm_result = self.llm_service.parse_auction_description(description)
                                    # Зберігаємо результат в кеш
                                    self.llm_cache_service.save_result(description, llm_result)
                                except Exception as e:
                                    # Детальне логування помилки
                                    error_traceback = traceback.format_exc()
                                    auction_id_from_doc = doc.get('auction_id', 'unknown')
                                    desc_hash = calculate_description_hash(description) if description else None
                                    description_preview = description[:200] + "..." if len(description) > 200 else description
                                    print(f"\n[LLM ПОМИЛКА] Помилка обробки через LLM для аукціону {auction_id_from_doc}")
                                    print(f"[LLM ПОМИЛКА] Хеш опису: {desc_hash[:16] if desc_hash else 'N/A'}...")
                                    print(f"[LLM ПОМИЛКА] Тип помилки: {type(e).__name__}")
                                    print(f"[LLM ПОМИЛКА] Повідомлення: {str(e)}")
                                    print(f"[LLM ПОМИЛКА] Опис (перші 200 символів): {description_preview}")
                                    print(f"[LLM ПОМИЛКА] Повний traceback:")
                                    print(error_traceback)
                                    
                                    # Логуємо в сервіс логування
                                    try:
                                        self.logging_service.log_app_event(
                                            message=f"Помилка обробки через LLM для аукціону {auction_id_from_doc}",
                                            event_type='llm_error',
                                            metadata={
                                                'auction_id': auction_id_from_doc,
                                                'description_hash': desc_hash[:16] if desc_hash else None,
                                                'description_preview': description_preview,
                                                'error_type': type(e).__name__,
                                                'error_message': str(e)
                                            },
                                            error=error_traceback
                                        )
                                    except:
                                        pass
                    
                    auctions.append(auction)
                except Exception as e:
                    print(f"Помилка конвертації аукціону з БД: {e}")
                    continue
        
        if not auctions:
            return None
        
        # Формуємо дані для Excel з обробкою через LLM (якщо потрібно)
        # Використовуємо стандартний метод з fallback на структуровані дані
        excel_data = self._prepare_auctions_data_for_excel(auctions, skip_llm=False)
        
        # Визначаємо fieldnames та column_headers
        fieldnames = [
            'date_updated',
            'address_region', 'address_city', 'address', 'property_type',
            'cadastral_number', 'building_area_sqm', 'land_area_ha', 'base_price', 'deposit_amount',
            'auction_start_date', 'document_submission_deadline',
            'min_participants_count', 'participants_count', 'arrests_info',
            'description', 'auction_url', 'classification_code',
            'is_repeat_auction', 'previous_auctions_links'
        ]
        
        column_headers = {
            'date_updated': 'Дата оновлення',
            'address_region': 'Область',
            'address_city': 'Населений пункт',
            'address': 'Адреса',
            'property_type': 'Тип нерухомості',
            'cadastral_number': 'Кадастровий номер',
            'building_area_sqm': 'Площа нерухомості (кв. м.)',
            'land_area_ha': 'Площа земельної ділянки (га)',
            'base_price': 'Стартова ціна',
            'deposit_amount': 'Розмір взносу',
            'auction_start_date': 'Дата торгів',
            'document_submission_deadline': 'Дата фінальної подачі документів',
            'min_participants_count': 'Мінімальна кількість учасників',
            'participants_count': 'Кількість зареєстрованих учасників',
            'arrests_info': 'Арешти',
            'description': 'Опис',
            'auction_url': 'Посилання',
            'classification_code': 'Код класифікатора',
            'is_repeat_auction': 'Повторний аукціон',
            'previous_auctions_links': 'Посилання на минулі аукціони'
        }
        
        # Сортуємо за датою оновлення від найсвіжішого до найдавнішого
        _min_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        excel_data.sort(key=lambda r: r.get('date_updated_ts') or _min_ts, reverse=True)
        
        # Друга вкладка — OLX: оголошення з сайту OLX (структура аналогічна ProZorro)
        olx_docs = self.olx_listings_repository.get_all_for_export()
        olx_data = self._prepare_olx_data_for_excel(olx_docs) if olx_docs else []
        olx_fieldnames = [
            'date_updated', 'address_region', 'address_city', 'address', 'property_type',
            'building_area_sqm', 'land_area_ha', 'base_price', 'description', 'listing_url', 'category'
        ]
        olx_headers = {
            'date_updated': 'Дата оновлення',
            'address_region': 'Область',
            'address_city': 'Населений пункт',
            'address': 'Адреса',
            'property_type': 'Тип нерухомості',
            'building_area_sqm': 'Площа нерухомості (кв. м.)',
            'land_area_ha': 'Площа земельної ділянки (га)',
            'base_price': 'Ціна',
            'description': 'Опис',
            'listing_url': 'Посилання',
            'category': 'Категорія OLX',
        }
        sheets = [
            ('ProZorro', excel_data, fieldnames, column_headers),
            ('OLX', olx_data, olx_fieldnames, olx_headers),
        ]
        return generate_excel_with_sheets(sheets)

    # Стандартні fieldnames та заголовки для експорту (як у «файл за день/тиждень»)
    STANDARD_EXPORT_FIELDNAMES_PROZORRO = [
        'date_updated', 'address_region', 'address_city', 'address', 'property_type',
        'cadastral_number', 'building_area_sqm', 'land_area_ha', 'base_price', 'deposit_amount',
        'auction_start_date', 'document_submission_deadline',
        'min_participants_count', 'participants_count', 'arrests_info',
        'description', 'auction_url', 'classification_code',
        'is_repeat_auction', 'previous_auctions_links'
    ]
    STANDARD_EXPORT_HEADERS_PROZORRO = {
        'date_updated': 'Дата оновлення', 'address_region': 'Область', 'address_city': 'Населений пункт',
        'address': 'Адреса', 'property_type': 'Тип нерухомості', 'cadastral_number': 'Кадастровий номер',
        'building_area_sqm': 'Площа нерухомості (кв. м.)', 'land_area_ha': 'Площа земельної ділянки (га)',
        'base_price': 'Стартова ціна', 'deposit_amount': 'Розмір взносу', 'auction_start_date': 'Дата торгів',
        'document_submission_deadline': 'Дата фінальної подачі документів',
        'min_participants_count': 'Мінімальна кількість учасників', 'participants_count': 'Кількість зареєстрованих учасників',
        'arrests_info': 'Арешти', 'description': 'Опис', 'auction_url': 'Посилання',
        'classification_code': 'Код класифікатора', 'is_repeat_auction': 'Повторний аукціон',
        'previous_auctions_links': 'Посилання на минулі аукціони'
    }
    STANDARD_EXPORT_FIELDNAMES_OLX = [
        'date_updated',
        'address_region',
        'address_city',
        'address',
        'property_type',
        'building_area_sqm',
        'land_area_ha',
        'base_price',
        'total_price_uah',
        'total_price_usd',
        'price_per_m2_uah',
        'price_per_m2_usd',
        'price_per_ha_uah',
        'price_per_ha_usd',
        'description',
        'listing_url',
        'category',
    ]
    STANDARD_EXPORT_HEADERS_OLX = {
        'date_updated': 'Дата оновлення',
        'address_region': 'Область',
        'address_city': 'Населений пункт',
        'address': 'Адреса',
        'property_type': 'Тип нерухомості',
        'building_area_sqm': 'Площа нерухомості (кв. м.)',
        'land_area_ha': 'Площа земельної ділянки (га)',
        'base_price': 'Ціна (текст)',
        'total_price_uah': 'Ціна (грн)',
        'total_price_usd': 'Ціна (USD)',
        'price_per_m2_uah': 'Ціна за м² (грн)',
        'price_per_m2_usd': 'Ціна за м² (USD)',
        'price_per_ha_uah': 'Ціна за га (грн)',
        'price_per_ha_usd': 'Ціна за га (USD)',
        'description': 'Опис',
        'listing_url': 'Посилання',
        'category': 'Категорія OLX',
    }
    STANDARD_EXPORT_FIELDNAMES_UNIFIED = [
        'date_updated', 'source', 'source_id', 'status', 'property_type',
        'building_area_sqm', 'land_area_ha',
        'address_region', 'address_city', 'address', 'cadastral_numbers',
        'price_uah', 'price_usd', 'price_per_m2_uah', 'price_per_ha_uah',
        'title', 'description', 'page_url',
    ]
    STANDARD_EXPORT_HEADERS_UNIFIED = {
        'date_updated': 'Дата оновлення', 'source': 'Джерело', 'source_id': 'ID в джерелі',
        'status': 'Статус', 'property_type': 'Тип нерухомості',
        'building_area_sqm': 'Площа (м²)', 'land_area_ha': 'Площа (га)',
        'address_region': 'Область', 'address_city': 'Населений пункт', 'address': 'Адреса',
        'cadastral_numbers': 'Кадастрові номери', 'price_uah': 'Ціна (грн)', 'price_usd': 'Ціна (USD)',
        'price_per_m2_uah': 'Ціна за м² (грн)', 'price_per_ha_uah': 'Ціна за га (грн)',
        'title': 'Заголовок', 'description': 'Опис', 'page_url': 'Посилання',
    }

    def get_standard_sheet_data_for_export(
        self, ids: List[str], collection: str
    ) -> Optional[tuple]:
        """
        Повертає дані для одного аркуша Excel у стандартному форматі (як «файл за день/тиждень»).
        collection: 'prozorro_auctions' або 'olx_listings'.
        Returns: (rows, fieldnames, column_headers) або None.
        """
        if not ids:
            rows_prozorro = [{self.STANDARD_EXPORT_FIELDNAMES_PROZORRO[0]: 'Немає даних за вказаний період'}]
            rows_olx = [{self.STANDARD_EXPORT_FIELDNAMES_OLX[0]: 'Немає даних за вказаний період'}]
            rows_unified = [{self.STANDARD_EXPORT_FIELDNAMES_UNIFIED[0]: 'Немає даних за вказаний період'}]
            if collection == 'prozorro_auctions':
                return (rows_prozorro, self.STANDARD_EXPORT_FIELDNAMES_PROZORRO, self.STANDARD_EXPORT_HEADERS_PROZORRO)
            if collection == 'olx_listings':
                return (rows_olx, self.STANDARD_EXPORT_FIELDNAMES_OLX, self.STANDARD_EXPORT_HEADERS_OLX)
            if collection == 'unified_listings':
                return (rows_unified, self.STANDARD_EXPORT_FIELDNAMES_UNIFIED, self.STANDARD_EXPORT_HEADERS_UNIFIED)
            return None
        if collection == 'prozorro_auctions':
            docs = self.auctions_repository.get_by_ids(ids)
            if not docs:
                return (
                    [{self.STANDARD_EXPORT_FIELDNAMES_PROZORRO[0]: 'Немає даних за вказаний період'}],
                    self.STANDARD_EXPORT_FIELDNAMES_PROZORRO,
                    self.STANDARD_EXPORT_HEADERS_PROZORRO,
                )
            auctions = []
            for d in docs:
                ad = d.get('auction_data')
                if ad:
                    try:
                        auctions.append(AuctionDTO.from_dict(ad))
                    except Exception:
                        continue
            if not auctions:
                return (
                    [{self.STANDARD_EXPORT_FIELDNAMES_PROZORRO[0]: 'Немає даних'}],
                    self.STANDARD_EXPORT_FIELDNAMES_PROZORRO,
                    self.STANDARD_EXPORT_HEADERS_PROZORRO,
                )
            data = self._prepare_auctions_data_for_excel(auctions, skip_llm=False)
            _min_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
            data.sort(key=lambda r: r.get('date_updated_ts') or _min_ts, reverse=True)
            rows = [{k: r.get(k, '') for k in self.STANDARD_EXPORT_FIELDNAMES_PROZORRO} for r in data]
            return (rows, self.STANDARD_EXPORT_FIELDNAMES_PROZORRO, self.STANDARD_EXPORT_HEADERS_PROZORRO)
        if collection == 'olx_listings':
            docs = self.olx_listings_repository.get_by_ids(ids)
            if not docs:
                return (
                    [{self.STANDARD_EXPORT_FIELDNAMES_OLX[0]: 'Немає даних за вказаний період'}],
                    self.STANDARD_EXPORT_FIELDNAMES_OLX,
                    self.STANDARD_EXPORT_HEADERS_OLX,
                )
            _min_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            docs.sort(key=lambda d: d.get('updated_at') or _min_dt, reverse=True)
            data = self._prepare_olx_data_for_excel(docs)
            rows = [{k: r.get(k, '') for k in self.STANDARD_EXPORT_FIELDNAMES_OLX} for r in data]
            return (rows, self.STANDARD_EXPORT_FIELDNAMES_OLX, self.STANDARD_EXPORT_HEADERS_OLX)
        if collection == 'unified_listings':
            docs = self.unified_listings_repository.get_by_ids(ids)
            if not docs:
                return (
                    [{self.STANDARD_EXPORT_FIELDNAMES_UNIFIED[0]: 'Немає даних за вказаний період'}],
                    self.STANDARD_EXPORT_FIELDNAMES_UNIFIED,
                    self.STANDARD_EXPORT_HEADERS_UNIFIED,
                )
            _min_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            docs.sort(key=lambda d: d.get('source_updated_at') or d.get('system_updated_at') or _min_dt, reverse=True)
            data = self._prepare_unified_data_for_excel(docs)
            rows = [{k: r.get(k, '') for k in self.STANDARD_EXPORT_FIELDNAMES_UNIFIED} for r in data]
            return (rows, self.STANDARD_EXPORT_FIELDNAMES_UNIFIED, self.STANDARD_EXPORT_HEADERS_UNIFIED)
        return None

    def get_standard_sheet_data_for_export_from_docs(
        self, docs: List[Dict[str, Any]], source_collection: str
    ) -> Optional[tuple]:
        """
        Повертає дані для одного аркуша Excel у стандартному форматі з уже отриманих документів
        (наприклад із тимчасової вибірки агента).
        source_collection: 'prozorro_auctions', 'olx_listings' або 'unified_listings'.
        Якщо порожній або невідомий — використовується unified_listings (зведена таблиця).
        Returns: (rows, fieldnames, column_headers) або None.
        """
        coll = (source_collection or "").strip() or "unified_listings"
        if not docs:
            if coll == 'prozorro_auctions':
                rows = [{self.STANDARD_EXPORT_FIELDNAMES_PROZORRO[0]: 'Немає даних за вказаний період'}]
                return (rows, self.STANDARD_EXPORT_FIELDNAMES_PROZORRO, self.STANDARD_EXPORT_HEADERS_PROZORRO)
            if coll == 'olx_listings':
                rows = [{self.STANDARD_EXPORT_FIELDNAMES_OLX[0]: 'Немає даних за вказаний період'}]
                return (rows, self.STANDARD_EXPORT_FIELDNAMES_OLX, self.STANDARD_EXPORT_HEADERS_OLX)
            if coll == 'unified_listings':
                rows = [{self.STANDARD_EXPORT_FIELDNAMES_UNIFIED[0]: 'Немає даних за вказаний період'}]
                return (rows, self.STANDARD_EXPORT_FIELDNAMES_UNIFIED, self.STANDARD_EXPORT_HEADERS_UNIFIED)
            return None
        if coll == 'prozorro_auctions':
            auctions = []
            for d in docs:
                ad = d.get('auction_data')
                if ad:
                    try:
                        auctions.append(AuctionDTO.from_dict(ad))
                    except Exception:
                        continue
            if not auctions:
                return (
                    [{self.STANDARD_EXPORT_FIELDNAMES_PROZORRO[0]: 'Немає даних'}],
                    self.STANDARD_EXPORT_FIELDNAMES_PROZORRO,
                    self.STANDARD_EXPORT_HEADERS_PROZORRO,
                )
            data = self._prepare_auctions_data_for_excel(auctions, skip_llm=False)
            _min_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
            data.sort(key=lambda r: r.get('date_updated_ts') or _min_ts, reverse=True)
            rows = [{k: r.get(k, '') for k in self.STANDARD_EXPORT_FIELDNAMES_PROZORRO} for r in data]
            return (rows, self.STANDARD_EXPORT_FIELDNAMES_PROZORRO, self.STANDARD_EXPORT_HEADERS_PROZORRO)
        if coll == 'olx_listings':
            _min_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            docs_sorted = sorted(docs, key=lambda d: d.get('updated_at') or _min_dt, reverse=True)
            data = self._prepare_olx_data_for_excel(docs_sorted)
            rows = [{k: r.get(k, '') for k in self.STANDARD_EXPORT_FIELDNAMES_OLX} for r in data]
            return (rows, self.STANDARD_EXPORT_FIELDNAMES_OLX, self.STANDARD_EXPORT_HEADERS_OLX)
        if coll == 'unified_listings':
            _min_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            docs_sorted = sorted(
                docs,
                key=lambda d: d.get('source_updated_at') or d.get('system_updated_at') or _min_dt,
                reverse=True,
            )
            data = self._prepare_unified_data_for_excel(docs_sorted)
            rows = [{k: r.get(k, '') for k in self.STANDARD_EXPORT_FIELDNAMES_UNIFIED} for r in data]
            return (rows, self.STANDARD_EXPORT_FIELDNAMES_UNIFIED, self.STANDARD_EXPORT_HEADERS_UNIFIED)
        return None

    def generate_excel_bytes_for_export(
        self, ids: List[str], collection: str, filename_prefix: str = 'export'
    ) -> Optional[BytesIO]:
        """
        Генерує Excel у стандартному форматі (як «файл за день/тиждень») для заданих ids та collection.
        Повертає BytesIO або None.
        """
        sheet = self.get_standard_sheet_data_for_export(ids, collection)
        if not sheet:
            return None
        rows, fieldnames, column_headers = sheet
        return generate_excel_in_memory(rows, fieldnames, column_headers)
    
    def _prepare_olx_data_for_excel(self, olx_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Підготовлює оголошення OLX для Excel у структурі, аналогічній ProZorro.
        Адреса беруться з detail.resolved_locations (деталізовано: область, місто, вулиця).
        """
        def ensure_str(v: Any) -> str:
            if v is None:
                return ''
            return str(v).strip()

        rows = []
        for doc in olx_docs:
            url = doc.get('url') or ''
            search_data = doc.get('search_data') or {}
            detail = doc.get('detail') or {}
            updated_at = doc.get('updated_at')

            date_updated = ''
            if updated_at:
                try:
                    if hasattr(updated_at, 'strftime'):
                        date_updated = format_datetime_display(updated_at, '%d.%m.%Y %H:%M')
                    else:
                        date_updated = ensure_str(updated_at)
                except Exception:
                    date_updated = ensure_str(updated_at)

            address_region = ''
            address_city = ''
            address = ''
            resolved = detail.get('resolved_locations') or []
            if resolved:
                first = resolved[0]
                results = first.get('results') or []
                if results:
                    r0 = results[0]
                    addr_struct = r0.get('address_structured') or {}
                    address_region = ensure_str(addr_struct.get('region', ''))
                    address_city = ensure_str(addr_struct.get('city', ''))
                    street = ensure_str(addr_struct.get('street', ''))
                    street_number = ensure_str(addr_struct.get('street_number', ''))
                    if address_region or address_city or street:
                        address = ', '.join(p for p in [address_region, address_city, street, street_number] if p)
                    if not address:
                        address = ensure_str(r0.get('formatted_address', ''))

            if not address:
                address = ensure_str(search_data.get('location', ''))

            llm = detail.get('llm') or {}
            property_type = ensure_str(llm.get('property_type', ''))
            if not property_type:
                params = detail.get('parameters') or []
                for p in params:
                    if isinstance(p, dict):
                        lb = (p.get('label') or '').lower()
                        if 'тип' in lb or 'призначення' in lb:
                            property_type = ensure_str(p.get('value', ''))
                            break

            building_area_sqm = ''
            land_area_ha = ''
            if llm.get('building_area_sqm') is not None:
                try:
                    building_area_sqm = str(float(str(llm.get('building_area_sqm', '')).replace(',', '.')))
                except (ValueError, TypeError):
                    pass
            if llm.get('land_area_ha') is not None:
                try:
                    land_area_ha = str(float(str(llm.get('land_area_ha', '')).replace(',', '.')))
                except (ValueError, TypeError):
                    pass
            if not building_area_sqm and not land_area_ha:
                area_m2 = search_data.get('area_m2')
                if area_m2 is not None:
                    try:
                        building_area_sqm = str(float(str(area_m2).replace(',', '.')))
                    except (ValueError, TypeError):
                        pass

            price_text = ensure_str(search_data.get('price_text', ''))
            price_value = search_data.get('price_value')
            base_price = price_text or (str(price_value) if price_value is not None else '')

            description = ensure_str(detail.get('description', ''))
            if not description:
                description = ensure_str(search_data.get('title', ''))

            # Цінові метрики, розраховані при завантаженні OLX (detail.price_metrics)
            price_metrics = detail.get('price_metrics') or {}

            rows.append({
                'date_updated': date_updated,
                'address_region': address_region,
                'address_city': address_city,
                'address': address,
                'property_type': property_type,
                'building_area_sqm': building_area_sqm,
                'land_area_ha': land_area_ha,
                'base_price': base_price,
                'total_price_uah': price_metrics.get('total_price_uah'),
                'total_price_usd': price_metrics.get('total_price_usd'),
                'price_per_m2_uah': price_metrics.get('price_per_m2_uah'),
                'price_per_m2_usd': price_metrics.get('price_per_m2_usd'),
                'price_per_ha_uah': price_metrics.get('price_per_ha_uah'),
                'price_per_ha_usd': price_metrics.get('price_per_ha_usd'),
                'description': description,
                'listing_url': url,
                'category': ensure_str(doc.get('category_label', '')),
            })
        return rows

    def _prepare_unified_data_for_excel(self, unified_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Підготовлює зведені оголошення (unified_listings) для Excel.
        """
        def ensure_str(v: Any) -> str:
            if v is None:
                return ''
            if isinstance(v, list):
                return ', '.join(str(x) for x in v if x)
            return str(v).strip()

        rows = []
        for doc in unified_docs:
            updated_at = doc.get('source_updated_at') or doc.get('system_updated_at')
            date_updated = ''
            if updated_at:
                try:
                    if hasattr(updated_at, 'strftime'):
                        date_updated = format_datetime_display(updated_at, '%d.%m.%Y %H:%M')
                    else:
                        date_updated = ensure_str(updated_at)
                except Exception:
                    date_updated = ensure_str(updated_at)

            addresses = doc.get('addresses') or []
            if isinstance(addresses, dict):
                addresses = [addresses] if addresses else []
            elif not isinstance(addresses, list):
                addresses = []
            addr_region = ''
            addr_city = ''
            addr = ''
            if addresses and isinstance(addresses[0], dict):
                first = addresses[0]
                addr_region = ensure_str(first.get('region', ''))
                addr_city = ensure_str(first.get('settlement', ''))
                addr = ensure_str(first.get('formatted_address', ''))
                if not addr and (addr_region or addr_city):
                    addr = ', '.join(p for p in [addr_region, addr_city] if p)

            cadastral = doc.get('cadastral_numbers') or []
            cadastral_str = ', '.join(str(c) for c in cadastral) if isinstance(cadastral, list) else ''

            rows.append({
                'date_updated': date_updated,
                'source': ensure_str(doc.get('source', '')),
                'source_id': ensure_str(doc.get('source_id', '')),
                'status': ensure_str(doc.get('status', '')),
                'property_type': ensure_str(doc.get('property_type', '')),
                'building_area_sqm': doc.get('building_area_sqm'),
                'land_area_ha': doc.get('land_area_ha'),
                'address_region': addr_region,
                'address_city': addr_city,
                'address': addr,
                'cadastral_numbers': cadastral_str,
                'price_uah': doc.get('price_uah'),
                'price_usd': doc.get('price_usd'),
                'price_per_m2_uah': doc.get('price_per_m2_uah'),
                'price_per_ha_uah': doc.get('price_per_ha_uah'),
                'title': ensure_str(doc.get('title', '')),
                'description': ensure_str(doc.get('description', '')),
                'page_url': ensure_str(doc.get('page_url', '')),
            })
        return rows

    def _prepare_auctions_data_for_excel_with_hashes(
        self,
        auctions_with_hashes: List[tuple]
    ) -> List[Dict[str, Any]]:
        """
        Підготовлює дані аукціонів для збереження в Excel з урахуванням description_hash.
        Використовує тільки результати LLM, без fallback на структуровані дані.
        
        Args:
            auctions_with_hashes: Список кортежів (AuctionDTO, description_hash)
            
        Returns:
            Список словників з даними для Excel
        """
        def ensure_string(value):
            """Конвертує значення в рядок, обробляючи None та інші типи."""
            if value is None:
                return ''
            return str(value)
        
        def format_full_address(address_obj: Dict[str, Any]) -> str:
            """Формує повну адресу з об'єкта адреси."""
            parts = []
            if address_obj.get('region'):
                parts.append(address_obj['region'])
            if address_obj.get('district'):
                parts.append(address_obj['district'])
            settlement_type = address_obj.get('settlement_type', '')
            settlement = address_obj.get('settlement', '')
            if settlement:
                if settlement_type:
                    parts.append(f"{settlement_type} {settlement}")
                else:
                    parts.append(settlement)
            if address_obj.get('settlement_district'):
                parts.append(address_obj['settlement_district'])
            street_type = address_obj.get('street_type', '')
            street = address_obj.get('street', '')
            if street:
                if street_type:
                    parts.append(f"{street_type} {street}")
                else:
                    parts.append(street)
            if address_obj.get('building'):
                parts.append(address_obj['building'])
            if address_obj.get('building_part'):
                parts.append(address_obj['building_part'])
            if address_obj.get('room'):
                parts.append(address_obj['room'])
            return ', '.join(parts) if parts else ''
        
        def format_date(date_str: str) -> str:
            """Форматує дату у форматі дд.ММ.рррр ГГ:ХХ (київський час)."""
            return format_date_display(date_str, '%d.%m.%Y %H:%M')
        
        excel_data = []
        
        for auction, description_hash in auctions_with_hashes:
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
            
            # Дата фінальної подачі документів
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
            
            # Розмір взносу
            deposit_amount = ''
            guarantee = data.get('guarantee', {})
            if guarantee and 'amount' in guarantee:
                deposit_amount = str(guarantee['amount'])
                currency = guarantee.get('currency', '')
                if currency:
                    deposit_amount += f' {currency}'
            
            # Арешти
            arrests_data = data.get('arrests', [])
            
            # Спочатку витягуємо структуровані дані з items (якщо є) - fallback
            structured_info = self._extract_structured_info_from_items(data)
            
            # Перевірка, чи це аренда
            is_rental = self._is_rental_auction(data)
            if is_rental:
                # Пропускаємо обробку через LLM для аренди, використовуємо тільки структуровані дані
                parsed_info = structured_info
            else:
                # Парсинг опису через LLM для отримання структурованої інформації
                parsed_info = structured_info.copy()  # Починаємо зі структурованих даних
            
            if description and self.llm_service and not is_rental:
                import traceback
                try:
                    cached_result = self.llm_cache_service.get_cached_result(description)
                    if cached_result is not None:
                        llm_result = cached_result
                    else:
                        # Якщо немає в кеші, але є description_hash - викликаємо LLM
                        # (description_hash вказує, що раніше був виклик LLM для цього опису)
                        desc_hash = calculate_description_hash(description)
                        if description_hash:
                            llm_result = self.llm_service.parse_auction_description(description)
                            self.llm_cache_service.save_result(description, llm_result)
                        else:
                            # Якщо немає description_hash - це новий опис, також викликаємо LLM
                            llm_result = self.llm_service.parse_auction_description(description)
                            self.llm_cache_service.save_result(description, llm_result)
                    
                    if llm_result:
                        # Об'єднуємо результати: структуровані дані мають пріоритет, але LLM може доповнити порожні поля
                        llm_addresses = llm_result.get('addresses', [])
                        if llm_addresses:
                            parsed_info['addresses'] = llm_addresses
                        elif llm_result.get('address_region') or llm_result.get('address_city'):
                            parsed_info['addresses'] = [{
                                'region': llm_result.get('address_region', ''),
                                'district': llm_result.get('address_district', ''),
                                'settlement_type': llm_result.get('address_settlement_type', ''),
                                'settlement': llm_result.get('address_city', ''),
                                'settlement_district': llm_result.get('address_settlement_district', ''),
                                'street_type': llm_result.get('address_street_type', ''),
                                'street': llm_result.get('address_street', ''),
                                'building': llm_result.get('address_building', ''),
                                'building_part': llm_result.get('address_building_part', ''),
                                'room': llm_result.get('address_room', '')
                            }]
                        
                        # Об'єднуємо інші поля: структуровані дані мають пріоритет, LLM доповнює порожні
                        for key in ['cadastral_number', 'floor', 'property_type', 'utilities', 'arrests_info']:
                            if not parsed_info.get(key) and llm_result.get(key):
                                parsed_info[key] = llm_result[key]
                        
                        # Окремо обробляємо площі з LLM - конвертуємо в стандартні одиниці
                        # Площа нерухомості (building_area_sqm)
                        if llm_result.get('building_area_sqm'):
                            try:
                                llm_building_area = float(str(llm_result.get('building_area_sqm', '')).replace(',', '.').replace(' ', ''))
                                # Додаємо до існуючої площі (якщо є з items)
                                current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                if isinstance(current_building_area, (int, float)):
                                    parsed_info['building_area_sqm'] = current_building_area + llm_building_area
                                else:
                                    parsed_info['building_area_sqm'] = llm_building_area
                            except (ValueError, AttributeError):
                                pass
                        
                        # Площа землі (land_area_ha)
                        if llm_result.get('land_area_ha'):
                            try:
                                llm_land_area = float(str(llm_result.get('land_area_ha', '')).replace(',', '.').replace(' ', ''))
                                # Додаємо до існуючої площі (якщо є з items)
                                current_land_area = parsed_info.get('land_area_ha', 0.0)
                                if isinstance(current_land_area, (int, float)):
                                    parsed_info['land_area_ha'] = current_land_area + llm_land_area
                                else:
                                    parsed_info['land_area_ha'] = llm_land_area
                            except (ValueError, AttributeError):
                                pass
                except Exception as e:
                    # Детальне логування помилки
                    error_traceback = traceback.format_exc()
                    desc_hash = calculate_description_hash(description) if description else None
                    description_preview = description[:200] + "..." if len(description) > 200 else description
                    print(f"\n[LLM ПОМИЛКА] Помилка обробки через LLM для аукціону {auction_id}")
                    print(f"[LLM ПОМИЛКА] Хеш опису: {desc_hash[:16] if desc_hash else 'N/A'}...")
                    print(f"[LLM ПОМИЛКА] Тип помилки: {type(e).__name__}")
                    print(f"[LLM ПОМИЛКА] Повідомлення: {str(e)}")
                    print(f"[LLM ПОМИЛКА] Опис (перші 200 символів): {description_preview}")
                    print(f"[LLM ПОМИЛКА] Повний traceback:")
                    print(error_traceback)
                    
                    # Логуємо в сервіс логування
                    try:
                        self.logging_service.log_app_event(
                            message=f"Помилка обробки через LLM для аукціону {auction_id}",
                            event_type='llm_error',
                            metadata={
                                'auction_id': auction_id,
                                'description_hash': desc_hash[:16] if desc_hash else None,
                                'description_preview': description_preview,
                                'error_type': type(e).__name__,
                                'error_message': str(e)
                            },
                            error=error_traceback
                        )
                    except:
                        pass
            
            # Обробляємо адреси: перевіряємо, чи є адреси з items (якщо LLM не використовувався)
            addresses = parsed_info.get('addresses', [])
            if not addresses:
                # Якщо немає масиву адрес, спочатку перевіряємо items
                items_addresses = self._extract_addresses_from_items(data)
                if items_addresses:
                    addresses = items_addresses
                else:
                    # Якщо немає адрес з items, але є старі поля - створюємо адресу
                    if parsed_info.get('address_region') or parsed_info.get('address_city'):
                        addresses = [{
                            'region': parsed_info.get('address_region', ''),
                            'district': parsed_info.get('address_district', ''),
                            'settlement_type': parsed_info.get('address_settlement_type', ''),
                            'settlement': parsed_info.get('address_city', ''),
                            'settlement_district': parsed_info.get('address_settlement_district', ''),
                            'street_type': parsed_info.get('address_street_type', ''),
                            'street': parsed_info.get('address_street', ''),
                            'building': parsed_info.get('address_building', ''),
                            'building_part': parsed_info.get('address_building_part', ''),
                            'room': parsed_info.get('address_room', '')
                        }]
            
            # Оновлюємо parsed_info з фінальним масивом адрес
            parsed_info['addresses'] = addresses
            
            first_address = addresses[0] if addresses else {}
            address_region = first_address.get('region', '')
            address_city = first_address.get('settlement', '')
            
            if addresses:
                formatted_addresses = []
                for idx, addr in enumerate(addresses, 1):
                    formatted_addr = format_full_address(addr)
                    if formatted_addr:
                        if len(addresses) > 1:
                            formatted_addresses.append(f"адреса {idx}: {formatted_addr}")
                        else:
                            formatted_addresses.append(formatted_addr)
                address = ', '.join(formatted_addresses) if formatted_addresses else ''
            else:
                address = ''
            
            # Формуємо площі нерухомості та землі окремо
            # Беремо площі зі структурованих даних (items) та з LLM, об'єднуємо їх
            building_area_sqm = parsed_info.get('building_area_sqm', 0.0)
            land_area_ha = parsed_info.get('land_area_ha', 0.0)
            
            # Конвертуємо в числа
            try:
                if building_area_sqm:
                    if isinstance(building_area_sqm, str):
                        building_area_sqm = float(str(building_area_sqm).replace(',', '.').replace(' ', ''))
                    else:
                        building_area_sqm = float(building_area_sqm)
                else:
                    building_area_sqm = 0.0
            except (ValueError, AttributeError, TypeError):
                building_area_sqm = 0.0
            
            try:
                if land_area_ha:
                    if isinstance(land_area_ha, str):
                        land_area_ha = float(str(land_area_ha).replace(',', '.').replace(' ', ''))
                    else:
                        land_area_ha = float(land_area_ha)
                else:
                    land_area_ha = 0.0
            except (ValueError, AttributeError, TypeError):
                land_area_ha = 0.0
            
            # Формуємо арешти
            arrests_info = ''
            if arrests_data and isinstance(arrests_data, list):
                arrests_parts = []
                for idx, arrest in enumerate(arrests_data, 1):
                    if isinstance(arrest, dict):
                        restriction_org = arrest.get('restrictionOrganization', '')
                        restriction_date = arrest.get('restrictionDate', '')
                        is_removable = arrest.get('isRemovable', False)
                        
                        date_str = format_date(restriction_date) if restriction_date else ''
                        org_str = ''
                        if restriction_org:
                            if isinstance(restriction_org, dict):
                                org_str = restriction_org.get('Видавник', restriction_org.get('name', str(restriction_org)))
                            else:
                                org_str = str(restriction_org)
                        
                        arrest_parts = []
                        if org_str:
                            arrest_parts.append(f"Видав {org_str}")
                        if date_str:
                            arrest_parts.append(f"Дата: {date_str}")
                        arrest_parts.append(f"Можливе зняття {'так' if is_removable else 'ні'}")
                        arrests_parts.append(f"Арешт {idx}: {', '.join(arrest_parts)}")
                arrests_info = '\n'.join(arrests_parts) if arrests_parts else ''
            elif parsed_info.get('arrests_info'):
                arrests_info = parsed_info.get('arrests_info', '')
            
            # Код класифікатора
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
                            break
                    
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
                'address_region': ensure_string(address_region),
                'address_city': ensure_string(address_city),
                'address': ensure_string(address),
                'property_type': ensure_string(parsed_info.get('property_type', '')),
                'cadastral_number': ensure_string(parsed_info.get('cadastral_number', '')),
                'building_area_sqm': ensure_string(building_area_sqm) if building_area_sqm > 0 else '',
                'land_area_ha': ensure_string(land_area_ha) if land_area_ha > 0 else '',
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
                '_has_additional_classification_03_07': has_additional_classification_03_07
            }
            
            excel_data.append(auction_row)
        
        return excel_data
    
    def _prepare_auctions_data_for_excel(
        self,
        auctions: List[AuctionDTO],
        skip_llm: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Підготовлює дані аукціонів для збереження в Excel.
        
        Args:
            auctions: Список аукціонів
            skip_llm: Якщо True, пропускає обробку через LLM (використовує тільки структуровані дані)
            
        Returns:
            Список словників з даними для Excel
        """
        def ensure_string(value):
            """Конвертує значення в рядок, обробляючи None та інші типи."""
            if value is None:
                return ''
            return str(value)
        
        def format_full_address(address_obj: Dict[str, Any]) -> str:
            """Формує повну адресу з об'єкта адреси."""
            parts = []
            if address_obj.get('region'):
                parts.append(address_obj['region'])
            if address_obj.get('district'):
                parts.append(address_obj['district'])
            settlement_type = address_obj.get('settlement_type', '')
            settlement = address_obj.get('settlement', '')
            if settlement:
                if settlement_type:
                    parts.append(f"{settlement_type} {settlement}")
                else:
                    parts.append(settlement)
            if address_obj.get('settlement_district'):
                parts.append(address_obj['settlement_district'])
            street_type = address_obj.get('street_type', '')
            street = address_obj.get('street', '')
            if street:
                if street_type:
                    parts.append(f"{street_type} {street}")
                else:
                    parts.append(street)
            if address_obj.get('building'):
                parts.append(address_obj['building'])
            if address_obj.get('building_part'):
                parts.append(address_obj['building_part'])
            if address_obj.get('room'):
                parts.append(address_obj['room'])
            return ', '.join(parts) if parts else ''
        
        def format_date(date_str: str) -> str:
            """Форматує дату у форматі дд.ММ.рррр ГГ:ХХ (київський час)."""
            return format_date_display(date_str, '%d.%m.%Y %H:%M')
        
        excel_data = []
        
        for auction in auctions:
            if not auction.data:
                continue
            
            data = auction.data
            
            # Посилання на аукціон
            # Використовуємо extract_auction_id для консистентності з іншими частинами коду
            auction_id = extract_auction_id(data) or auction.id
            if not auction_id:
                # Якщо не вдалося витягти ID, спробуємо альтернативні методи
                auction_id = data.get('auctionId') or data.get('_id') or auction.id
            auction_url = f"https://prozorro.sale/auction/{auction_id}" if auction_id else ""
            
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
            
            # Дата фінальної подачі документів
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
            
            # Розмір взносу
            deposit_amount = ''
            guarantee = data.get('guarantee', {})
            if guarantee and 'amount' in guarantee:
                deposit_amount = str(guarantee['amount'])
                currency = guarantee.get('currency', '')
                if currency:
                    deposit_amount += f' {currency}'
            
            # Арешти
            arrests_data = data.get('arrests', [])
            
            # Спочатку витягуємо структуровані дані з items (якщо є) - fallback
            structured_info = self._extract_structured_info_from_items(data)
            
            # Перевірка, чи це аренда
            is_rental = self._is_rental_auction(data)
            if is_rental:
                # Пропускаємо обробку через LLM для аренди, використовуємо тільки структуровані дані
                parsed_info = structured_info
            else:
                # Парсинг опису через LLM для отримання структурованої інформації
                parsed_info = structured_info.copy()  # Починаємо зі структурованих даних
            
            # Якщо не пропускаємо LLM і це не аренда, обробляємо через LLM
            if not skip_llm and description and self.llm_service and not is_rental:
                import traceback
                try:
                    cached_result = self.llm_cache_service.get_cached_result(description)
                    if cached_result is not None:
                        llm_result = cached_result
                    else:
                        desc_hash = calculate_description_hash(description)
                        llm_result = self.llm_service.parse_auction_description(description)
                        self.llm_cache_service.save_result(description, llm_result)
                    
                    # Об'єднуємо результати: структуровані дані мають пріоритет, але LLM може доповнити порожні поля
                    # Спочатку витягуємо адреси з items (якщо є)
                    items_addresses = self._extract_addresses_from_items(data)
                    
                    # Обробляємо адреси з масиву LLM
                    llm_addresses = llm_result.get('addresses', [])
                    if not llm_addresses:
                        # Якщо немає масиву адрес, але є старі поля - створюємо адресу
                        if llm_result.get('address_region') or llm_result.get('address_city'):
                            llm_addresses = [{
                                'region': llm_result.get('address_region', ''),
                                'district': llm_result.get('address_district', ''),
                                'settlement_type': llm_result.get('address_settlement_type', ''),
                                'settlement': llm_result.get('address_city', ''),
                                'settlement_district': llm_result.get('address_settlement_district', ''),
                                'street_type': llm_result.get('address_street_type', ''),
                                'street': llm_result.get('address_street', ''),
                                'building': llm_result.get('address_building', ''),
                                'building_part': llm_result.get('address_building_part', ''),
                                'room': llm_result.get('address_room', '')
                            }]
                    
                    # Формуємо фінальний масив адрес:
                    # - Якщо є адреси з items - вони стають основними (першими)
                    # - Адреси з LLM додаються як додаткові (після адрес з items)
                    final_addresses = []
                    if items_addresses:
                        # Адреси з items - основні
                        final_addresses.extend(items_addresses)
                        # Додаємо адреси з LLM як додаткові
                        final_addresses.extend(llm_addresses)
                    else:
                        # Якщо немає адрес з items - використовуємо адреси з LLM
                        final_addresses = llm_addresses
                    
                    parsed_info['addresses'] = final_addresses
                    
                    # Об'єднуємо інші поля: структуровані дані мають пріоритет, LLM доповнює порожні
                    for key in ['cadastral_number', 'floor', 'property_type', 'utilities', 'arrests_info']:
                        if not parsed_info.get(key) and llm_result.get(key):
                            parsed_info[key] = llm_result[key]
                    
                    # Окремо обробляємо площі з LLM - конвертуємо в стандартні одиниці та сумуємо
                    # Площа нерухомості (building_area_sqm)
                    if llm_result.get('building_area_sqm'):
                        try:
                            llm_building_area = float(str(llm_result.get('building_area_sqm', '')).replace(',', '.').replace(' ', ''))
                            # Додаємо до існуючої площі (якщо є з items)
                            current_building_area = parsed_info.get('building_area_sqm', 0.0)
                            if isinstance(current_building_area, (int, float)):
                                parsed_info['building_area_sqm'] = current_building_area + llm_building_area
                            else:
                                parsed_info['building_area_sqm'] = llm_building_area
                        except (ValueError, AttributeError):
                            pass
                    
                    # Площа землі (land_area_ha)
                    if llm_result.get('land_area_ha'):
                        try:
                            llm_land_area = float(str(llm_result.get('land_area_ha', '')).replace(',', '.').replace(' ', ''))
                            # Додаємо до існуючої площі (якщо є з items)
                            current_land_area = parsed_info.get('land_area_ha', 0.0)
                            if isinstance(current_land_area, (int, float)):
                                parsed_info['land_area_ha'] = current_land_area + llm_land_area
                            else:
                                parsed_info['land_area_ha'] = llm_land_area
                        except (ValueError, AttributeError):
                            pass
                    
                    # Якщо є старі поля area та area_unit - конвертуємо їх (для сумісності зі старими кешами)
                    if llm_result.get('area') and not llm_result.get('building_area_sqm') and not llm_result.get('land_area_ha'):
                        try:
                            old_area = float(str(llm_result.get('area', '')).replace(',', '.').replace(' ', ''))
                            old_unit = llm_result.get('area_unit', '').lower()
                            
                            if any(x in old_unit for x in ['гектар', 'hectare', 'га']):
                                # Додаємо до існуючої площі землі
                                current_land_area = parsed_info.get('land_area_ha', 0.0)
                                if isinstance(current_land_area, (int, float)):
                                    parsed_info['land_area_ha'] = current_land_area + old_area
                                else:
                                    parsed_info['land_area_ha'] = old_area
                            elif any(x in old_unit for x in ['м²', 'м2', 'кв.м', 'квадратний метр']):
                                # Додаємо до існуючої площі нерухомості
                                current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                if isinstance(current_building_area, (int, float)):
                                    parsed_info['building_area_sqm'] = current_building_area + old_area
                                else:
                                    parsed_info['building_area_sqm'] = old_area
                            elif any(x in old_unit for x in ['сотка', 'соток']):
                                # Сотки - визначаємо за значенням
                                if old_area < 100:
                                    # Швидше за все це гектари (менше 1 га)
                                    area_ha = old_area * 0.01
                                    current_land_area = parsed_info.get('land_area_ha', 0.0)
                                    if isinstance(current_land_area, (int, float)):
                                        parsed_info['land_area_ha'] = current_land_area + area_ha
                                    else:
                                        parsed_info['land_area_ha'] = area_ha
                                else:
                                    # Швидше за все це м²
                                    area_sqm = old_area * 100
                                    current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                    if isinstance(current_building_area, (int, float)):
                                        parsed_info['building_area_sqm'] = current_building_area + area_sqm
                                    else:
                                        parsed_info['building_area_sqm'] = area_sqm
                            else:
                                # Якщо одиниця невідома - визначаємо за значенням
                                if old_area > 1000:
                                    current_building_area = parsed_info.get('building_area_sqm', 0.0)
                                    if isinstance(current_building_area, (int, float)):
                                        parsed_info['building_area_sqm'] = current_building_area + old_area
                                    else:
                                        parsed_info['building_area_sqm'] = old_area
                                elif old_area < 10:
                                    current_land_area = parsed_info.get('land_area_ha', 0.0)
                                    if isinstance(current_land_area, (int, float)):
                                        parsed_info['land_area_ha'] = current_land_area + old_area
                                    else:
                                        parsed_info['land_area_ha'] = old_area
                        except (ValueError, AttributeError):
                            pass
                except Exception as e:
                    # Детальне логування помилки
                    error_traceback = traceback.format_exc()
                    desc_hash = calculate_description_hash(description) if description else None
                    description_preview = description[:200] + "..." if len(description) > 200 else description
                    print(f"\n[LLM ПОМИЛКА] Помилка обробки через LLM для аукціону {auction_id}")
                    print(f"[LLM ПОМИЛКА] Хеш опису: {desc_hash[:16] if desc_hash else 'N/A'}...")
                    print(f"[LLM ПОМИЛКА] Тип помилки: {type(e).__name__}")
                    print(f"[LLM ПОМИЛКА] Повідомлення: {str(e)}")
                    print(f"[LLM ПОМИЛКА] Опис (перші 200 символів): {description_preview}")
                    print(f"[LLM ПОМИЛКА] Повний traceback:")
                    print(error_traceback)
                    
                    # Логуємо в сервіс логування
                    try:
                        self.logging_service.log_app_event(
                            message=f"Помилка обробки через LLM для аукціону {auction_id}",
                            event_type='llm_error',
                            metadata={
                                'auction_id': auction_id,
                                'description_hash': desc_hash[:16] if desc_hash else None,
                                'description_preview': description_preview,
                                'error_type': type(e).__name__,
                                'error_message': str(e)
                            },
                            error=error_traceback
                        )
                    except:
                        pass
            
            # Обробляємо адреси: перевіряємо, чи є адреси з items (якщо LLM не використовувався)
            addresses = parsed_info.get('addresses', [])
            if not addresses:
                # Якщо немає масиву адрес, спочатку перевіряємо items
                items_addresses = self._extract_addresses_from_items(data)
                if items_addresses:
                    addresses = items_addresses
                else:
                    # Якщо немає адрес з items, але є старі поля - створюємо адресу
                    if parsed_info.get('address_region') or parsed_info.get('address_city'):
                        addresses = [{
                            'region': parsed_info.get('address_region', ''),
                            'district': parsed_info.get('address_district', ''),
                            'settlement_type': parsed_info.get('address_settlement_type', ''),
                            'settlement': parsed_info.get('address_city', ''),
                            'settlement_district': parsed_info.get('address_settlement_district', ''),
                            'street_type': parsed_info.get('address_street_type', ''),
                            'street': parsed_info.get('address_street', ''),
                            'building': parsed_info.get('address_building', ''),
                            'building_part': parsed_info.get('address_building_part', ''),
                            'room': parsed_info.get('address_room', '')
                        }]
            
            # Оновлюємо parsed_info з фінальним масивом адрес
            parsed_info['addresses'] = addresses
            
            first_address = addresses[0] if addresses else {}
            address_region = first_address.get('region', '')
            address_city = first_address.get('settlement', '')
            
            if addresses:
                formatted_addresses = []
                for idx, addr in enumerate(addresses, 1):
                    formatted_addr = format_full_address(addr)
                    if formatted_addr:
                        if len(addresses) > 1:
                            formatted_addresses.append(f"адреса {idx}: {formatted_addr}")
                        else:
                            formatted_addresses.append(formatted_addr)
                address = ', '.join(formatted_addresses) if formatted_addresses else ''
            else:
                address = ''
            
            # Формуємо площі нерухомості та землі окремо
            # Беремо площі зі структурованих даних (items) та з LLM, об'єднуємо їх
            building_area_sqm = parsed_info.get('building_area_sqm', 0.0)
            land_area_ha = parsed_info.get('land_area_ha', 0.0)
            
            # Конвертуємо в числа
            try:
                if building_area_sqm:
                    if isinstance(building_area_sqm, str):
                        building_area_sqm = float(str(building_area_sqm).replace(',', '.').replace(' ', ''))
                    else:
                        building_area_sqm = float(building_area_sqm)
                else:
                    building_area_sqm = 0.0
            except (ValueError, AttributeError, TypeError):
                building_area_sqm = 0.0
            
            try:
                if land_area_ha:
                    if isinstance(land_area_ha, str):
                        land_area_ha = float(str(land_area_ha).replace(',', '.').replace(' ', ''))
                    else:
                        land_area_ha = float(land_area_ha)
                else:
                    land_area_ha = 0.0
            except (ValueError, AttributeError, TypeError):
                land_area_ha = 0.0
            
            # Формуємо арешти
            arrests_info = ''
            if arrests_data and isinstance(arrests_data, list):
                arrests_parts = []
                for idx, arrest in enumerate(arrests_data, 1):
                    if isinstance(arrest, dict):
                        restriction_org = arrest.get('restrictionOrganization', '')
                        restriction_date = arrest.get('restrictionDate', '')
                        is_removable = arrest.get('isRemovable', False)
                        
                        date_str = format_date(restriction_date) if restriction_date else ''
                        org_str = ''
                        if restriction_org:
                            if isinstance(restriction_org, dict):
                                org_str = restriction_org.get('Видавник', restriction_org.get('name', str(restriction_org)))
                            else:
                                org_str = str(restriction_org)
                        
                        arrest_parts = []
                        if org_str:
                            arrest_parts.append(f"Видав {org_str}")
                        if date_str:
                            arrest_parts.append(f"Дата: {date_str}")
                        arrest_parts.append(f"Можливе зняття {'так' if is_removable else 'ні'}")
                        arrests_parts.append(f"Арешт {idx}: {', '.join(arrest_parts)}")
                arrests_info = '\n'.join(arrests_parts) if arrests_parts else ''
            elif parsed_info.get('arrests_info'):
                arrests_info = parsed_info.get('arrests_info', '')
            
            # Код класифікатора
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
                            break
                    
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
            
            # Визначаємо повторний аукціон та посилання на минулі аукціони
            is_repeat_auction = 'ні'
            previous_auctions_links = ''
            
            # Перевіряємо, чи є auction_id та description для пошуку попередніх аукціонів
            if description and auction_id:
                # Обчислюємо хеш опису
                description_hash = calculate_description_hash(description)
                
                if description_hash:
                    # Знаходимо інші аукціони з тим самим описом
                    previous_auctions = self.auctions_repository.get_auctions_by_description_hash(
                        description_hash,
                        exclude_auction_id=auction_id
                    )
                    
                    if previous_auctions:
                        # Отримуємо дату створення поточного аукціону
                        current_date_created = None
                        date_created_str = data.get('dateCreated', '')
                        if date_created_str:
                            try:
                                if date_created_str.endswith('Z'):
                                    date_created_str = date_created_str.replace('Z', '+00:00')
                                current_date_created = datetime.fromisoformat(date_created_str)
                                if current_date_created.tzinfo:
                                    current_date_created = current_date_created.astimezone(timezone.utc)
                                else:
                                    current_date_created = current_date_created.replace(tzinfo=timezone.utc)
                            except (ValueError, AttributeError):
                                pass
                        
                        # Фільтруємо попередні аукціони за датою створення
                        previous_links = []
                        has_earlier_auction = False
                        
                        for prev_auction in previous_auctions:
                            prev_auction_data = prev_auction.get('auction_data', {})
                            if not prev_auction_data:
                                continue
                            
                            prev_auction_id = prev_auction.get('auction_id')
                            if not prev_auction_id:
                                continue
                            
                            # Отримуємо дату створення попереднього аукціону
                            prev_date_created_str = prev_auction_data.get('dateCreated', '')
                            prev_date_created = None
                            
                            if prev_date_created_str:
                                try:
                                    if prev_date_created_str.endswith('Z'):
                                        prev_date_created_str = prev_date_created_str.replace('Z', '+00:00')
                                    prev_date_created = datetime.fromisoformat(prev_date_created_str)
                                    if prev_date_created.tzinfo:
                                        prev_date_created = prev_date_created.astimezone(timezone.utc)
                                    else:
                                        prev_date_created = prev_date_created.replace(tzinfo=timezone.utc)
                                except (ValueError, AttributeError):
                                    pass
                            
                            # Якщо є дата створення поточного аукціону, порівнюємо
                            if current_date_created and prev_date_created:
                                if prev_date_created < current_date_created:
                                    has_earlier_auction = True
                                    prev_link = f"https://prozorro.sale/auction/{prev_auction_id}"
                                    previous_links.append(prev_link)
                            else:
                                # Якщо немає дати створення, додаємо всі попередні аукціони
                                has_earlier_auction = True
                                prev_link = f"https://prozorro.sale/auction/{prev_auction_id}"
                                previous_links.append(prev_link)
                        
                        # Визначаємо, чи є це повторний аукціон
                        if has_earlier_auction:
                            is_repeat_auction = 'так'
                        
                        # Формуємо посилання на минулі аукціони через символ повернення каретки і переносу строки
                        if previous_links:
                            previous_auctions_links = '\r\n'.join(previous_links)
            
            # Дата оновлення: dateModified якщо аукціон оновлювався, інакше dateCreated
            date_modified_str = data.get('dateModified', '')
            date_created_str = data.get('dateCreated', '')
            date_updated_str = date_modified_str if (date_modified_str and date_modified_str != date_created_str) else (date_created_str or date_modified_str)
            date_updated = format_date(date_updated_str) if date_updated_str else ''
            try:
                if date_updated_str:
                    s = date_updated_str.replace('Z', '+00:00') if date_updated_str.endswith('Z') else date_updated_str
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo:
                        dt = dt.astimezone(timezone.utc)
                    else:
                        dt = dt.replace(tzinfo=timezone.utc)
                    date_updated_ts = dt
                else:
                    date_updated_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                date_updated_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
            
            auction_row = {
                'address_region': ensure_string(address_region),
                'address_city': ensure_string(address_city),
                'address': ensure_string(address),
                'property_type': ensure_string(parsed_info.get('property_type', '')),
                'cadastral_number': ensure_string(parsed_info.get('cadastral_number', '')),
                'building_area_sqm': ensure_string(building_area_sqm) if building_area_sqm > 0 else '',
                'land_area_ha': ensure_string(land_area_ha) if land_area_ha > 0 else '',
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
                'is_repeat_auction': ensure_string(is_repeat_auction),
                'previous_auctions_links': ensure_string(previous_auctions_links),
                'date_updated': ensure_string(date_updated),
                'date_updated_ts': date_updated_ts,
                '_has_additional_classification_03_07': has_additional_classification_03_07
            }
            
            excel_data.append(auction_row)
        
        return excel_data
    
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
            file_path, _ = self.save_auctions_to_csv(auctions, 1, temp_dir, user_id)
            
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
        user_id: Optional[int] = None,
        auctions: Optional[List[AuctionDTO]] = None,
        full: bool = False,
        llm_only_for_active: Optional[bool] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Отримує та зберігає список аукціонів про нерухомість за останні N днів.
        Зберігає дату оновлення в БД замість файлів.
        Для тижня (7 днів) використовує паралельну обробку по днях.

        Args:
            days: Кількість днів для виборки. Якщо не вказано, використовується значення з налаштувань
            output_dir: Директорія для збереження (не використовується, залишено для сумісності)
            user_id: Ідентифікатор користувача, який сформував файл (опціонально)
            auctions: Вже отримані аукціони (опціонально, якщо вказано - не викликається API)
            full: Якщо True — завантажує всю історію (роки), LLM тільки для активних
            llm_only_for_active: Якщо задано — перевизначає правило LLM тільки для активних (None = використати full)

        Returns:
            Dict[str, Any]: Результат операції з інформацією про кількість знайдених аукціонів
        """
        if days is None and not full:
            days = self.settings.default_days_range
        if full:
            return self._fetch_and_save_full_optimized(output_dir, user_id, progress_callback=progress_callback)

        # Для тижня використовуємо оптимізовану паралельну обробку
        if days == 7:
            use_llm_only_active = llm_only_for_active if llm_only_for_active is not None else full
            return self._fetch_and_save_week_optimized(
                output_dir, user_id, llm_only_for_active=use_llm_only_active, progress_callback=progress_callback
            )
        
        # Для інших періодів використовуємо стандартну обробку
        try:
            # Перед завантаженням нових даних з API - швидке оновлення активних аукціонів
            fast_update_result = self._fast_update_active_auctions(progress_callback=progress_callback)
            
            # Якщо аукціони вже передані - використовуємо їх, інакше отримуємо з API
            if auctions is None:
                print(
                    f"Отримання аукціонів з ProZorro.Sale за останні {days} днів"
                    f" (dateModified в діапазоні)..."
                )
                auctions = self.get_real_estate_auctions(days)
            else:
                print(f"Використовуються передані аукціони ({len(auctions)} шт.)")
            
            days_text = "день" if days == 1 else ("дні" if days < 5 else "днів")
            print(f"Знайдено {len(auctions)} аукціонів про нерухомість за останні {days} {days_text}")
            
            if not auctions:
                print("Аукціони не знайдено")
                return {
                    'success': True,
                    'count': 0,
                    'file_path': None,
                    'message': 'Аукціони не знайдено',
                }

            # Зберігаємо аукціони в MongoDB та обробляємо через LLM
            use_llm_only_active = llm_only_for_active if llm_only_for_active is not None else full
            if progress_callback:
                progress_callback({"phase": "llm", "current": 0, "total": len(auctions), "llm_processed": 0, "message": f"Початок обробки через LLM: 0/{len(auctions)}"})
            save_result = self._save_auctions_to_database(
                auctions,
                llm_only_for_active=use_llm_only_active,
                progress_callback=progress_callback,
            )
            llm_requests_count = save_result['llm_requests_count']
            saved_count = save_result['saved_count']
            
            # Зберігаємо дату оновлення в БД
            update_date = datetime.now(timezone.utc)
            self.app_data_repository.set_update_date(days, update_date)
            
            # Якщо оновлюємо за тиждень, також оновлюємо дату за добу
            if days == 7:
                self.app_data_repository.set_update_date(1, update_date)
            
            print(f"Дата оновлення збережена: {format_datetime_display(update_date, '%d.%m.%Y %H:%M:%S')}")
            print(f"Викликів LLM: {llm_requests_count}")
            
            return {
                'success': True,
                'count': saved_count,  # Кількість збережених аукціонів (без аренди)
                'file_path': None,  # Файли більше не зберігаються
                'update_date': update_date,
                'llm_requests_count': llm_requests_count + fast_update_result.get('llm_requests_count', 0),
                'message': f'Успішно оновлено {saved_count} аукціонів',
                'fast_update_result': fast_update_result
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

    def fetch_and_save_to_raw_only(
        self,
        days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Phase 1 pipeline: отримання аукціонів з API та запис лише в raw_prozorro_auctions (без LLM, без prozorro_auctions).
        Повертає success, count, loaded_auction_ids.
        """
        if days is None:
            days = self.settings.default_days_range
        date_from, date_to = get_date_range(days)
        fetch_context = {
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "days": days,
        }
        raw_repo = RawProzorroAuctionsRepository()
        raw_repo.ensure_index()

        if days == 7:
            now = datetime.now(timezone.utc)
            day_ranges = []
            for i in range(7):
                day_end = now - timedelta(days=i)
                day_start = day_end - timedelta(days=1)
                day_ranges.append((day_start, day_end))
            all_auctions = []
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
                futures = [
                    executor.submit(self.get_real_estate_auctions_by_date_range, ds, de)
                    for ds, de in day_ranges
                ]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        all_auctions.extend(f.result())
                    except Exception as e:
                        logger.warning("ProZorro raw: помилка дня: %s", e)
            unique_auctions = {}
            for a in all_auctions:
                aid = getattr(a, "id", None) or (extract_auction_id(a.data) if getattr(a, "data", None) else None)
                if aid and aid not in unique_auctions:
                    unique_auctions[aid] = a
            auctions = list(unique_auctions.values())
        else:
            auctions = self.get_real_estate_auctions(days)

        loaded_auction_ids: List[str] = []
        for auction in auctions:
            try:
                if not getattr(auction, "data", None):
                    continue
                auction_data = auction.data
                auction_id = extract_auction_id(auction_data)
                if not auction_id:
                    continue
                if self._is_rental_auction(auction_data):
                    continue
                approximate_region = self._get_region_from_auction_data(auction_data)
                raw_repo.upsert_raw(
                    auction_id=auction_id,
                    auction_data=auction_data,
                    fetch_context=fetch_context,
                    approximate_region=approximate_region,
                )
                loaded_auction_ids.append(auction_id)
            except Exception as e:
                logger.warning("ProZorro raw: помилка запису аукціону: %s", e)
        return {
            "success": True,
            "count": len(loaded_auction_ids),
            "loaded_auction_ids": loaded_auction_ids,
        }

    def _fetch_and_save_full_optimized(
        self,
        output_dir: Optional[str] = None,
        user_id: Optional[int] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Повне оновлення історії: пакетна обробка по місяцях з паралелізмом.
        Розбиває ~6 років на місяці, обробляє кілька місяців паралельно.
        LLM тільки для активних аукціонів.
        """
        import concurrent.futures

        def month_ranges(years: int = 6):
            """Генерує (month_start, month_end) для кожного місяця за останні years років."""
            now = datetime.now(timezone.utc)
            end = now
            start_year = now.year - years
            start_month = now.month
            current = datetime(start_year, start_month, 1, 0, 0, 0, tzinfo=timezone.utc)
            while current < end:
                if current.month == 12:
                    next_month = datetime(current.year + 1, 1, 1, tzinfo=timezone.utc)
                else:
                    next_month = datetime(current.year, current.month + 1, 1, tzinfo=timezone.utc)
                month_end = next_month - timedelta(seconds=1)
                if month_end > end:
                    month_end = end
                yield current, month_end
                current = next_month

        months = list(month_ranges())
        total_months = len(months)
        print(f"Повне оновлення ProZorro: {total_months} місяців (пакетна обробка)...")

        all_auctions = []
        max_workers = min(6, total_months)
        completed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.get_real_estate_auctions_by_date_range,
                    month_start,
                    month_end,
                ): (month_start, month_end)
                for month_start, month_end in months
            }
            for future in concurrent.futures.as_completed(futures):
                month_start, month_end = futures[future]
                completed += 1
                try:
                    month_auctions = future.result()
                    all_auctions.extend(month_auctions)
                    label = f"{month_start.strftime('%Y-%m')}"
                    print(f"Місяць {completed}/{total_months} ({label}): {len(month_auctions)} аукціонів")
                except Exception as e:
                    print(f"Помилка місяця {month_start.strftime('%Y-%m')}: {e}")

        unique_auctions = {}
        for auction in all_auctions:
            aid = getattr(auction, "id", None) or (auction.data.get("id") if auction.data else None)
            if aid and aid not in unique_auctions:
                unique_auctions[aid] = auction
            elif not aid:
                unique_auctions[f"_empty_{len(unique_auctions)}"] = auction

        final_auctions = list(unique_auctions.values())
        print(f"Всього унікальних аукціонів: {len(final_auctions)}")

        if not final_auctions:
            return {
                "success": True,
                "count": 0,
                "file_path": None,
                "message": "Аукціони не знайдено",
            }

        fast_update_result = self._fast_update_active_auctions(progress_callback=progress_callback)
        if progress_callback:
            progress_callback({"phase": "llm", "current": 0, "total": len(final_auctions), "llm_processed": 0, "message": f"Обробка через LLM: 0/{len(final_auctions)}"})
        save_result = self._save_auctions_to_database(
            final_auctions,
            llm_only_for_active=True,
            progress_callback=progress_callback,
        )
        llm_requests_count = save_result["llm_requests_count"]
        saved_count = save_result["saved_count"]

        update_date = datetime.now(timezone.utc)
        self.app_data_repository.set_update_date(7, update_date)
        self.app_data_repository.set_update_date(1, update_date)

        print(f"Дата оновлення збережена: {format_datetime_display(update_date, '%d.%m.%Y %H:%M:%S')}")
        print(f"Викликів LLM: {llm_requests_count}")

        return {
            "success": True,
            "count": saved_count,
            "file_path": None,
            "update_date": update_date,
            "llm_requests_count": llm_requests_count + fast_update_result.get("llm_requests_count", 0),
            "message": f"Успішно оновлено {saved_count} аукціонів (повна історія)",
            "fast_update_result": fast_update_result,
        }

    def _fetch_and_save_week_optimized(
        self,
        output_dir: Optional[str] = None,
        user_id: Optional[int] = None,
        llm_only_for_active: bool = True,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Оптимізована обробка тижня: паралельна обробка по днях в пам'яті.
        
        Args:
            output_dir: Директорія для збереження (не використовується, залишено для сумісності)
            user_id: Ідентифікатор користувача
            
        Returns:
            Dict[str, Any]: Результат операції
        """
        import concurrent.futures
        
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
            all_auctions = []
            completed_days = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
                futures = {}
                for day_start, day_end, day_num in day_ranges:
                    future = executor.submit(
                        self.get_real_estate_auctions_by_date_range,
                        day_start,
                        day_end
                    )
                    futures[future] = day_num
                
                # Збираємо результати
                for future in concurrent.futures.as_completed(futures):
                    day_num = futures[future]
                    completed_days += 1
                    try:
                        day_auctions = future.result()
                        all_auctions.extend(day_auctions)
                        print(f"Завершено обробку дня {day_num} ({completed_days}/7): знайдено {len(day_auctions)} аукціонів")
                    except Exception as e:
                        print(f"Помилка обробки дня {day_num}: {e}")
            
            # Видаляємо дублікати за auction.id
            unique_auctions = {}
            for auction in all_auctions:
                auction_id = auction.id if hasattr(auction, 'id') and auction.id else None
                if auction_id:
                    if auction_id not in unique_auctions:
                        unique_auctions[auction_id] = auction
                else:
                    # Якщо немає ID, додаємо всі
                    unique_auctions[f"_empty_{len(unique_auctions)}"] = auction
            
            final_auctions = list(unique_auctions.values())
            total_count = len(final_auctions)
            
            if total_count == 0:
                return {
                    'success': True,
                    'count': 0,
                    'file_path': None,
                    'message': 'Аукціони не знайдено'
                }
            
            print(f"Об'єднано дані за 7 днів. Всього унікальних аукціонів: {total_count}")
            
            # Перед завантаженням нових даних з API - швидке оновлення активних аукціонів
            fast_update_result = self._fast_update_active_auctions(progress_callback=progress_callback)
            
            # Зберігаємо аукціони в MongoDB та обробляємо через LLM
            if progress_callback:
                progress_callback({"phase": "llm", "current": 0, "total": total_count, "llm_processed": 0, "message": f"Обробка через LLM: 0/{total_count}"})
            save_result = self._save_auctions_to_database(
                final_auctions,
                llm_only_for_active=llm_only_for_active,
                progress_callback=progress_callback,
            )
            llm_requests_count = save_result['llm_requests_count']
            saved_count = save_result['saved_count']
            
            # Зберігаємо дату оновлення в БД
            update_date = datetime.now(timezone.utc)
            self.app_data_repository.set_update_date(7, update_date)
            # Оновлюємо також дату за добу (оскільки оновлення за тиждень включає останню добу)
            self.app_data_repository.set_update_date(1, update_date)
            
            print(f"Дата оновлення збережена: {format_datetime_display(update_date, '%d.%m.%Y %H:%M:%S')}")
            print(f"Викликів LLM: {llm_requests_count}")
            
            return {
                'success': True,
                'count': saved_count,  # Кількість збережених аукціонів (без аренди)
                'file_path': None,  # Файли більше не зберігаються
                'update_date': update_date,
                'llm_requests_count': llm_requests_count + fast_update_result.get('llm_requests_count', 0),
                'message': f'Успішно оновлено {saved_count} аукціонів за тиждень',
                'fast_update_result': fast_update_result
            }
            
        except Exception as e:
            error_message = f"Помилка при оптимізованій обробці тижня: {e}"
            print(error_message)
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'count': 0,
                'file_path': None,
                'message': error_message,
                'error': str(e)
            }

    def get_auction_details(self, auction_id: str, proc_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Отримує детальну інформацію по конкретному аукціону.

        Endpoint (ProZorro.Sale): GET {base}/procedures/{id}
        API procedure.prozorro.sale очікує _id (MongoDB ObjectId), а НЕ auctionId (LSE001-UA-...).
        Якщо є proc_id з auction_data._id — використовуємо його.

        Args:
            auction_id: Ідентифікатор аукціону (auctionId, для fallback)
            proc_id: Ідентифікатор процедури в API (_id з auction_data), має пріоритет

        Returns:
            Dict[str, Any]: Повна відповідь API у вигляді JSON (dict)
        """
        # API очікує _id (24-символьний hex), не auctionId (LSE001-UA-...)
        effective_id = proc_id if proc_id else auction_id
        if not effective_id:
            raise ValueError("auction_id or proc_id is required")

        url = f'{self.settings.prozorro_sale_search_api_base_url}/procedures/{effective_id}'
        response = self.session.get(
            url,
            timeout=self.settings.prozorro_api_timeout
        )
        # Перевіряємо статус вручну: 404 — процедура не знайдена; 422 — недоступна (архів тощо)
        if response.status_code == 404:
            raise requests.exceptions.HTTPError(f"404 Client Error: Not Found for url: {url}", response=response)
        if response.status_code == 422:
            raise requests.exceptions.HTTPError(f"422 Client Error: Unprocessable Entity for url: {url}", response=response)
        response.raise_for_status()
        return response.json()
    
    def _is_active_status(self, status: str) -> bool:
        """
        Перевіряє, чи статус аукціону є активним.
        
        Args:
            status: Статус аукціону
            
        Returns:
            bool: True якщо статус активний
        """
        active_statuses = ['active', 'active.tendering', 'active.auction', 'active.qualification', 
                          'active_rectification', 'active_tendering', 'active_auction', 'active_qualification']
        
        return any(
            status.startswith(active_status.replace('_', '.')) or 
            status == active_status or
            status.startswith(active_status.replace('.', '_'))
            for active_status in active_statuses
        )
    
    def _fast_update_active_auctions(
        self,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Швидке оновлення активних аукціонів з бази даних.
        
        Логіка:
        1. Отримуємо активні аукціони з бази
        2. Для кожного отримуємо актуальні дані з API
        3. Перевіряємо хеш версії - якщо не змінився, пропускаємо
        4. Якщо хеш змінився:
           - Перевіряємо статус
           - Якщо статус став неактивним - просто оновлюємо дані
           - Якщо статус ще активний:
             - Перевіряємо хеш опису
             - Якщо немає LLM кешу для хешу опису - викликаємо LLM
        
        Returns:
            Dict[str, Any]: Статистика оновлення
        """
        print("Швидке оновлення активних аукціонів...")

        # Отримуємо активні аукціони з бази
        active_auctions = self.auctions_repository.get_active_auctions()
        total_count = len(active_auctions)

        if total_count == 0:
            print("В базі немає активних аукціонів для оновлення")
            return {
                'updated_count': 0,
                'skipped_count': 0,
                'errors_count': 0,
                'not_found_count': 0,
                'llm_requests_count': 0,
                'total_count': 0
            }

        print(f"Знайдено {total_count} активних аукціонів для перевірки оновлень")

        updated_count = 0
        skipped_count = 0
        errors_count = 0
        not_found_count = 0
        unprocessable_count = 0  # 422 — процедура недоступна в API
        llm_requests_count = 0

        # Прогрес-бар для оновлення (лише для CLI, якщо немає callback)
        from tqdm import tqdm
        update_progress = None
        if progress_callback is None:
            update_progress = tqdm(
                total=total_count,
                desc="Оновлення активних аукціонів",
                unit="аукціон",
                ncols=100,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
            )

        def _report_step(idx: int) -> None:
            """Оновлення прогресу для fast_update."""
            if progress_callback:
                progress_callback({
                    "phase": "fast_update",
                    "current": idx,
                    "total": total_count,
                    "llm_processed": llm_requests_count,
                    "message": f"Швидке оновлення активних аукціонів: {idx}/{total_count} (LLM: {llm_requests_count})",
                })
            elif update_progress:
                update_progress.update(1)

        # Оновлюємо кожен активний аукціон
        for idx, auction_doc in enumerate(active_auctions, start=1):
            auction_id = auction_doc.get('auction_id')
            if not auction_id:
                _report_step(idx)
                continue
            # API очікує _id (ObjectId), не auctionId
            proc_id = None
            ad = auction_doc.get('auction_data') or {}
            if isinstance(ad.get('_id'), str):
                proc_id = ad['_id']
            elif ad.get('_id') is not None:
                proc_id = str(ad['_id'])
            
            try:
                auction_data = self.get_auction_details(auction_id, proc_id=proc_id)
                
                # Конвертуємо в AuctionDTO для зручності
                auction_dto = AuctionDTO.from_dict(auction_data)
                current_auction_data = auction_dto.data
                
                if not current_auction_data:
                    _report_step(idx)
                    continue

                # Обчислюємо новий хеш версії
                new_version_hash = calculate_object_version_hash(current_auction_data)
                existing_version_hash = auction_doc.get('version_hash')
                
                # Якщо хеш не змінився - пропускаємо
                if existing_version_hash == new_version_hash:
                    skipped_count += 1
                    _report_step(idx)
                    continue

                # Хеш змінився - обробляємо
                # Витягуємо опис для обчислення хешу опису
                description = ''
                if 'description' in current_auction_data:
                    desc_obj = current_auction_data['description']
                    if isinstance(desc_obj, dict):
                        description = desc_obj.get('uk_UA', desc_obj.get('en_US', ''))
                    elif isinstance(desc_obj, str):
                        description = desc_obj
                
                new_description_hash = None
                if description:
                    new_description_hash = calculate_description_hash(description)
                
                # Перевіряємо статус
                current_status = current_auction_data.get('status', '')
                is_still_active = self._is_active_status(current_status)
                
                # Оновлюємо дані в базі
                self.auctions_repository.upsert_auction(
                    auction_id=auction_id,
                    auction_data=current_auction_data,
                    version_hash=new_version_hash,
                    description_hash=new_description_hash,
                    last_updated=datetime.now(timezone.utc)
                )
                self._sync_auction_to_unified(auction_id)

                # Якщо статус став неактивним - просто оновлюємо дані, подальшу обробку опускаємо
                # ГАРАНТОВАНО не викликаємо LLM для неактивних аукціонів
                if not is_still_active:
                    updated_count += 1
                    _report_step(idx)
                    continue

                # Якщо статус ще активний - перевіряємо хеш опису та LLM кеш
                # Додаткова перевірка is_still_active для гарантії (хоча continue вище вже гарантує це)
                if is_still_active and new_description_hash and self.llm_service:
                    # Перевіряємо, чи є LLM кеш для цього хешу опису
                    cached_entry = self.llm_cache_service.repository.find_by_description_hash(new_description_hash)
                    if not cached_entry:
                        # Немає кешу — викликаємо LLM лише якщо область увімкнена для LLM
                        region = self._get_region_from_auction_data(current_auction_data)
                        if region is None or is_region_enabled_for_llm(region):
                            llm_requests_count += self._process_auction_with_llm(current_auction_data)

                updated_count += 1
                _report_step(idx)
                
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 404:
                    not_found_count += 1
                    _report_step(idx)
                elif e.response and e.response.status_code == 422:
                    # 422 Unprocessable Entity — процедура недоступна (архівована, змінена схема тощо)
                    unprocessable_count += 1
                    _report_step(idx)
                else:
                    errors_count += 1
                    _report_step(idx)
                    if update_progress:
                        update_progress.write(f"Помилка при отриманні аукціону {auction_id}: {e}")
            except Exception as e:
                errors_count += 1
                _report_step(idx)
                if update_progress:
                    update_progress.write(f"Помилка при оновленні аукціону {auction_id}: {e}")

        if update_progress:
            update_progress.close()
        
        print(f"\nСтатистика швидкого оновлення активних аукціонів:")
        print(f"  Всього перевірено: {total_count}")
        print(f"  Оновлено: {updated_count}")
        print(f"  Пропущено (без змін): {skipped_count}")
        print(f"  Не знайдено в API (404): {not_found_count}")
        print(f"  Недоступні в API (422): {unprocessable_count}")
        print(f"  Помилок: {errors_count}")
        print(f"  Викликів LLM: {llm_requests_count}")
        
        return {
            'updated_count': updated_count,
            'skipped_count': skipped_count,
            'errors_count': errors_count,
            'not_found_count': not_found_count,
            'unprocessable_count': unprocessable_count,
            'llm_requests_count': llm_requests_count,
            'total_count': total_count
        }
    
    def _update_existing_auctions_in_database(self) -> Dict[str, Any]:
        """
        Оновлює існуючі аукціони в базі даних, отримуючи актуальні дані з API.
        Це дозволяє фіксувати зміни статусів аукціонів (наприклад, перехід з активного в інші стадії).
        
        Returns:
            Dict[str, Any]: Статистика оновлення: updated_count, errors_count, not_found_count
        """
        print("Оновлення існуючих аукціонів в базі даних...")
        
        # Отримуємо всі auction_id з бази
        all_auctions = self.auctions_repository.find_many()
        total_count = len(all_auctions)
        
        if total_count == 0:
            print("В базі немає аукціонів для оновлення")
            return {
                'updated_count': 0,
                'errors_count': 0,
                'not_found_count': 0,
                'total_count': 0
            }
        
        print(f"Знайдено {total_count} аукціонів в базі для перевірки оновлень")
        
        errors_count = 0
        not_found_count = 0
        unprocessable_count = 0
        
        # Прогрес-бар для оновлення
        from tqdm import tqdm
        update_progress = tqdm(
            total=total_count,
            desc="Оновлення аукціонів",
            unit="аукціон",
            ncols=100,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
        )
        
        # Оновлюємо кожен аукціон
        updated_auctions = []
        for auction_doc in all_auctions:
            auction_id = auction_doc.get('auction_id')
            if not auction_id:
                update_progress.update(1)
                continue
            proc_id = None
            ad = auction_doc.get('auction_data') or {}
            if isinstance(ad.get('_id'), str):
                proc_id = ad['_id']
            elif ad.get('_id') is not None:
                proc_id = str(ad['_id'])
            
            try:
                auction_data = self.get_auction_details(auction_id, proc_id=proc_id)
                
                # Конвертуємо в AuctionDTO
                auction_dto = AuctionDTO.from_dict(auction_data)
                
                # Додаємо до списку для збереження
                updated_auctions.append(auction_dto)
                update_progress.update(1)
                
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 404:
                    not_found_count += 1
                    update_progress.update(1)
                elif e.response and e.response.status_code == 422:
                    unprocessable_count += 1
                    update_progress.update(1)
                else:
                    errors_count += 1
                    update_progress.update(1)
                    update_progress.write(f"Помилка при отриманні аукціону {auction_id}: {e}")
            except Exception as e:
                errors_count += 1
                update_progress.update(1)
                update_progress.write(f"Помилка при оновленні аукціону {auction_id}: {e}")
        
        update_progress.close()
        
        # Зберігаємо оновлені аукціони через існуючий метод
        if updated_auctions:
            print(f"\nЗбереження {len(updated_auctions)} оновлених аукціонів...")
            save_result = self._save_auctions_to_database(updated_auctions)
            # Оновлюємо статистику на основі результатів збереження
            # (метод _save_auctions_to_database вже логує детальну статистику)
        
        print(f"\nСтатистика оновлення існуючих аукціонів:")
        print(f"  Всього перевірено: {total_count}")
        print(f"  Успішно оновлено: {len(updated_auctions)}")
        print(f"  Не знайдено в API (404): {not_found_count}")
        print(f"  Недоступні в API (422): {unprocessable_count}")
        print(f"  Помилок: {errors_count}")
        
        return {
            'updated_count': len(updated_auctions),
            'errors_count': errors_count,
            'not_found_count': not_found_count,
            'total_count': total_count
        }

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
                proc_id = None
                if auction.data:
                    pid = auction.data.get('_id')
                    proc_id = pid if isinstance(pid, str) else (str(pid) if pid is not None else None)
                try:
                    _render_progress(idx)
                    details = self.get_auction_details(auction_id, proc_id=proc_id)
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
