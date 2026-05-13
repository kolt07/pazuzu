# Pipeline завантаження даних з джерел (OLX, ProZorro)

## Мета

Розділити завантаження на два етапи: спочатку отримання сирих даних без LLM, потім — обробка обраних оголошень через LLM і синхронізація в основні колекції та unified. Деталізація географії щонайменше до області фіксується вже на етапі сирих даних.

## Етапи

### Phase 1: Завантаження сирих даних (без LLM)

- **Джерела:** OLX, ProZorro.
- **Дія:** отримання сирих даних з джерела, запис **без додаткової обробки** в колекцію сирих даних джерела.
- **Паралелізм:** кілька потоків (по типу оголошення, по області, по даті тощо). Ліміту на кількість потоків не вводимо — максимум швидкість збору.
- **Правило запису:** якщо запис уже є в колекції сирих даних — оновлюємо; паралельно формуємо масив ідентифікаторів завантажених/оновлених.
- **OLX: хеш даних зі сторінки пошуку:** у raw_olx_listings зберігається `search_data_hash` (від полів title, price, location, area тощо). Якщо для URL вже є запис з тим самим хешем — не оновлюємо сирі дані і не додаємо URL до loaded_urls, тобто подальша LLM-обробка для нього не запускається.
- **Географія:** визначаємо приблизну географічну приналежність (мінімум до області):
  - **ProZorro:** з полів адреси в API (address_refs, items[].address).
  - **OLX:** з фільтрів пошуку, за якими отримали оголошення (наприклад, область у URL).
- **У сирих даних зберігаємо:** за якими фільтрами в джерелі отримали оголошення (fetch_filters / fetch_context), approximate_region.
- **LLM на цьому етапі не використовується.**

Колекції сирих даних:

- `raw_olx_listings` — url, search_data, search_data_hash, detail (сирі з парсера), fetch_filters, approximate_region, loaded_at.
- `raw_prozorro_auctions` — auction_id, auction_data (сирі з API), fetch_context, approximate_region, loaded_at.

### Phase 2: Визначення кандидатів на LLM і обробка

- **Старт:** після завершення всіх потоків завантаження з джерела.
- **Вхід:** масив ідентифікаторів, завантажених/оновлених на Phase 1 (для OLX — лише URL зі зміною search_data_hash).
- **Визначення:** згідно з налаштуваннями (наприклад, llm_processing_regions) визначаємо, які з них мають бути опрацьовані через LLM.
- **Обробка:** обрані оголошення проходять через LLM (парсинг деталей, об’єкти нерухомості тощо) і потрапляють у колекцію джерела (olx_listings / prozorro_auctions) та в unified_listings.
- **OLX:** дані в olx_listings та unified_listings потрапляють лише після LLM-обробки; сирі записи не піднімаються в olx_listings до LLM. **ProZorro:** сирі записи піднімаються в prozorro_auctions; обрана підмножина проходить LLM і sync в unified.

### Phase 2.5: Vector-index sync (Qdrant + bge-m3)

- **Тригер:** одразу після успішного `unified_repo.upsert_listing(unified_doc)` у `UnifiedListingsService.sync_olx_listing` / `sync_prozorro_auction`.
- **Дія:** `VectorIndexService.upsert_listings([unified_doc])` — композує текст із `title/description/property_type/region/oblast/city/tags/price/area/cadastral_numbers`, дістає ембединг через `EmbeddingService` (TEI bge-m3) та upsertить точку у Qdrant-колекцію `unified_listings_vec`.
- **Кеш:** ембединги кешуються у Mongo `embedding_cache` за SHA-256 хешем тексту + ім'ям моделі (TTL `embeddings_cache_ttl_days` днів). Повторні sync (наприклад при reformat-listing) не витрачають ресурс TEI.
- **Best-effort:** помилки (Qdrant недоступний, TEI ще не прокинувся, тощо) лише логуються warning-ом і не блокують основний sync — векторний індекс це secondary store.
- **Позначка:** після успішного upsert `unified_listings.vector_indexed_at` оновлюється.
- **Backfill:** `py -m scripts.embeddings.build_vector_index --limit ...` для існуючих документів без `vector_indexed_at`. Окремо `py -m scripts.embeddings.build_cadastral_vector_index` — для `cadastral_parcels` та `cadastral_parcel_clusters`.

### Phase 3: Після оновлення джерела

- Після завершення оновлення та обробки з джерела:
  - запускаємо перерахунок аналітики (price analytics, listing analytics за потреби);
  - запускаємо процес побудови географічного індексу (якщо реалізовано).

## Компоненти

- **Raw-репозиторії:** `RawOlxListingsRepository`, `RawProzorroAuctionsRepository` (колекції raw_olx_listings, raw_prozorro_auctions).
- **Оркестратор завантаження:** сервіс, який запускає Phase 1 (паралельні воркери по джерелах/фільтрах), збирає ID, потім Phase 2 (вибір за налаштуваннями → LLM → запис у основні колекції + unified), потім Phase 3 (аналітика + гео-індекс).
- **Налаштування:** існуючий `llm_processing_regions.yaml` визначає, для яких областей виконувати LLM; approximate_region у сирих даних використовується для цього вибору.
