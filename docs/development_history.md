# Історія розробки

## 2026-03-11 — Перевірка структури БД та кандидати на очищення

- **Запит**: Почистити проект: прибрати зайве в БД (колекції, що не використовуються і не наповнюються), прибрати зайві механізми; тимчасові або замінені рішення з історії. При сумнівах — не видаляти, а скласти перелік сумнівних механік.

- **Дії**:
  - **scripts/check_db_structure.py**: Скрипт перевірки стану БД — список очікуваних колекцій (з репозиторіїв і міграцій), порівняння з фактичними. Показує відсутні очікувані колекції та колекції в БД поза списком (кандидати на ручне видалення). Запуск: `py scripts/check_db_structure.py`.
  - **docs/cleanup_candidates.md**: Документ з підсумком по колекціях (усі поточні використовуються і наповнюються), інструкцією перевірки БД та переліком **сумнівних механік**: регламентне оновлення даних (тимчасово вимкнено), відображення аналітики (тимчасово приховано), OLX скрапер-клікер (експеримент), pipeline_templates. Рекомендації щодо видалення «зайвих» колекцій та роботи з сумнівними механіками.

## 2026-03-06 — Скрипт очищення колекцій OLX та кешу LLM

- **Запит**: Очистити всі колекції, повʼязані з OLX, та повʼязані дані з кешів LLM.

- **Дії**:
  - **scripts/clear_olx_data.py**: Скрипт видаляє: raw_olx_listings (усі документи), olx_listings (усі), unified_listings (де source="olx"), listing_analytics (де source="olx"), у real_estate_objects прибирає посилання на OLX з source_listing_ids і видаляє документи з порожнім масивом. Опція --clear-llm-cache очищає колекцію llm_cache (спільна з ProZorro; окремо по OLX не зберігається). Запуск: `py scripts/clear_olx_data.py`, `--dry-run`, `--clear-llm-cache`.

## 2026-03-06 — OLX: прибрано логіку оновлення актуальності оголошення

- **Запит**: Прибрати логіку оновлення актуальності оголошення OLX; помічати за замовчуванням усі як активні.

- **Дії**:
  - **_process_category**: Видалено гілку для неактивних оголошень (is_active=False, continue без LLM). Якщо parse_detail_page повертає _inactive — лише прибираємо прапорець з detail, далі оголошення обробляється як звичайне та зберігається з is_active=True.
  - **_process_llm_pending**: Прибрано повторну перевірку активності (recheck fetch + is_active=False). Усі оголошення зберігаються з is_active=True. Параметр skip_activity_check залишено в сигнатурі для сумісності.
  - У unified_listings статус OLX як і раніше береться з olx_listings.is_active; оскільки ми більше не виставляємо is_active=False, усі будуть «активне». Джерело: scripts/olx_scraper/run_update.py.

## 2026-03-06 — OLX Phase 1: список через requests, деталі через клікер

- **Запит**: Обробка сторінок пошуку — старим способом (прямі запити); деталі оголошень — одним потоком через клікер.

- **Дії**:
  - **Сторінки пошуку**: у _process_category_raw_only завжди використовується fetch_page(session=session) — браузер для списку більше не використовується.
  - **Деталі**: якщо передано browser_fetcher — завантаження сторінки оголошення лише через browser_fetcher.get_detail_page(); інакше — _fetch_detail_page(session).
  - **_process_region_raw_only**: session = get_session() завжди (для запитів списку); browser_fetcher передається лише для етапу деталей.
  - При use_browser області обробляються послідовно, один екземпляр BrowserPageFetcher для всіх деталей. Джерело: scripts/olx_scraper/run_update.py.

## 2026-03-06 — Повний опис оголошення для LLM: очікування блоку опису в браузері та діагностика

- **Запит**: Повні тексти оголошень не потрапляють у запит до LLM на витягування ОНМ навіть при використанні клікера; провести дослідження етапів завантаження та порівняти один URL старим способом і клікером.

- **Дії**:
  - **BrowserPageFetcher.get_detail_page**: Після `page.goto(..., wait_until="domcontentloaded")` додано очікування появи одного з селекторів опису (`[data-cy="ad_description"]`, `ad_description_content`, `[data-testid="ad-description"]`, `ad_description`) з таймаутом 15 с перед викликом `page.content()`. Так контент сторінки знімається після JS-рендеру блоку опису OLX, і парсер отримує заповнений HTML. Джерело: scripts/olx_scraper/browser_fetcher.py.
  - **Діагностичний скрипт**: scripts/diagnose_olx_detail_description.py — приймає URL сторінки оголошення OLX, завантажує її двома способами (requests та Playwright), викликає parse_detail_page для обох HTML, виводить довжину опису, параметри та фрагмент тексту для LLM; опція `--save-html` зберігає HTML у scripts/temp/olx_detail_diagnostic/ для ручного огляду. Запуск: `py scripts/diagnose_olx_detail_description.py "URL" [--save-html] [--no-browser]`.

## 2026-03-06 — Підключення клікера до основного скрапера OLX (опційно)

- **Запит**: Замінити в основному скрапері отримання сторінки пошуку та сторінки оголошення на клікер, не видаляючи стару логіку.

- **Дії**:
  - **BrowserPageFetcher**: scripts/olx_scraper/browser_fetcher.py — клас-контекстний менеджер з get_list_page(url) та get_detail_page(url). Повертає PageResult(.text, .status_code), сумісний з використанням response.text у run_update. Затримки та антибот-повтор як у старому fetcher.
  - **Конфіг**: OLX_SCRAPER_USE_BROWSER (env 1/true/yes) — у config.py змінна USE_BROWSER. При True Phase 1 використовує браузер; області обробляються послідовно (один браузер на run).
  - **run_update**: _process_category_raw_only приймає browser_fetcher=None. Якщо browser_fetcher задано — list/detail HTML беруться з browser_fetcher.get_list_page / get_detail_page; інакше — fetch_page та _fetch_detail_page (стара логіка). _process_region_raw_only приймає browser_fetcher, передає його в _process_category_raw_only; session не створюється при browser_fetcher. run_olx_update_raw_only: при USE_BROWSER — цикл по областях у контексті BrowserPageFetcher; інакше — як раніше ThreadPoolExecutor з get_session у кожному потоці. Старий код не видалено, лише додано гілки.

## 2026-03-06 — Експериментальний OLX-скрапер на основі клікера (Playwright)

- **Запит**: Зробити скрапер OLX на основі клікера — замість введення в оману сервера запитами працювати під виглядом фізичного користувача. Запуск через міні-апп з панелі адміністрування; не змінювати основні флоу.

- **Дії**:
  - **Playwright**: Додано залежність playwright>=1.40.0 у requirements.txt. Після встановлення потрібно виконати `playwright install chromium`.
  - **Скрапер-клікер**: Новий модуль scripts/olx_scraper/clicker_scraper.py — run_olx_clicker_scraper(settings, max_pages, max_listings, headless, log_fn, progress_callback). Відкриває Chromium, переходить на сторінку пошуку нежитлової нерухомості OLX, отримує HTML (page.content()), парсить parse_listings_page; для кожного оголошення (до max_listings) переходить на сторінку деталей, парсить parse_detail_page, зберігає через RawOlxListingsRepository.upsert_raw з fetch_filters={"source": "clicker"}. Використовує існуючі парсери та helpers (search_data_from_listing).
  - **Admin API**: POST /api/admin/olx-clicker-scraper/start (max_pages, max_listings), GET /api/admin/olx-clicker-scraper/status?task_id=... — запуск у фоновому потоці, збереження статусу в _olx_clicker_tasks. Джерело: telegram_mini_app/routes/admin.py.
  - **Mini App**: У вкладці «Дані та аналітика» додано блок «OLX скрапер (клікер) — експеримент»: поля «Сторінок пошуку» (1–10) та «Макс. оголошень для збереження» (5–100), кнопка «Запустити клікер», відображення статусу з опитуванням. Джерело: telegram_mini_app/static/index.html, app.js.
  - **Глосарій**: Термін «OLX скрапер (клікер)» у docs/developer_glossary.md.

## 2026-03-06 — Точкове оновлення даних: по джерелах, областях та типах оголошень

- **Запит**: Окрім ручного запуску за 1/7 днів — додати можливість оновлювати окремо по джерелам, а в рамках джерела — по областях і типах оголошень.

- **Дії**:
  - **OLX Phase 1**: У `run_olx_update_raw_only` додано параметри `regions` (список областей) та `listing_types` (типи оголошень); допоміжна `_filter_regions_with_categories` фільтрує список (region, categories) перед паралельною обробкою. Джерело: scripts/olx_scraper/run_update.py.
  - **Pipeline**: `run_full_pipeline` приймає `regions` та `listing_types`; передає їх у `run_olx_update_raw_only`; для ProZorro після Phase 1 список `loaded_auction_ids` фільтрується по `approximate_region` (normalize_region_name) — у Phase 2 promote/LLM/sync лише для обраних областей. Джерело: business/services/source_data_load_service.py.
  - **Опції для UI**: `get_targeted_update_options()` повертає `regions` (з olx_region_slugs / get_all_region_names) та `olx_listing_types` (з _get_base_categories у run_update).
  - **Admin API**: GET /api/admin/data-update/options — опції для точкового оновлення. POST /api/admin/data-update — додано query-параметри `source` (olx|prozorro|both), `regions` (рядок через кому), `listing_types` (рядок через кому). При наявності regions або listing_types використовується єдиний run_full_pipeline з фільтрами (без режиму full_olx/full_prozorro). Джерело: telegram_mini_app/routes/admin.py.
  - **Планувальник**: У SchedulerService._execute_data_update payload підтримує `regions` та `listing_types` (список або рядок через кому); вони передаються в run_full_pipeline.
  - **MCP**: trigger_olx_update приймає `regions` та `listing_types`; додано інструмент get_targeted_update_options. Джерело: mcp_servers/data_update_mcp_server.py.
  - **LangChain**: _trigger_data_update та tool trigger_data_update приймають `regions` та `listing_types` і передають їх у run_full_pipeline.
  - **Mini-app**: Секція «Точкове оновлення» в адмінці: вибір джерела (OLX / ProZorro / обидва), період (1/7/30 днів), чекбокси областей та типів оголошень OLX (опції з GET /api/admin/data-update/options), кнопка «Запустити точкове оновлення». Джерело: telegram_mini_app/static/index.html, app.js, styles.css.

## 2026-03-03 — Ollama для парсингу/ОНМ, Gemini для асистента; переобробка недооброблених OLX; діагностика пошуку

- **Запит**: Використовувати Ollama для парсингу даних та отримання сутностей (ОНМ); Gemini лишати для елементів, пов’язаних із LLM-агентами та помічником. Перезапустити обробку даних з фінальних колекцій, де немає опису та нормальних атрибутів (повний пайплайн: оновлення з джерела → LLM → перенос у вищі колекції). З’ясувати, чому після оновлення даних зміни не відображаються в пошуку.

- **Дії**:
  - **Розділення провайдерів**: У config (config.example.yaml) та Settings за замовчуванням: `llm.parsing` — Ollama (парсинг описів OLX/ProZorro, витягування ОНМ); `llm.assistant` — Gemini (діалог, intent, tools). LLMService.parse_real_estate_objects тепер використовує провайдер парсингу (Ollama), а не асистента: виклик `provider.generate_text()` з логуванням у llm_exchange з provider=llm_parsing_provider.
  - **Переобробка OLX без/з порожнім detail.llm**: OlxListingsRepository.find_urls_needing_llm_reprocess(limit) — повертає URL, де detail.llm відсутній, null або {}. Скрипт scripts/reprocess_olx_underprocessed.py: вибирає ці URL, викликає _process_llm_pending(..., skip_activity_check=True) (без повторного fetch сторінки OLX), оновлює olx_listings та sync у unified_listings. Запуск: `py scripts/reprocess_olx_underprocessed.py`, `--limit N`, `--dry-run`.
  - **_process_llm_pending**: Додано параметр skip_activity_check=False; при True перевірка активності оголошення (fetch_page) не виконується — для переобробки вже збережених даних без навантаження на джерело.
  - **Діагностика «оновлення не в пошуку»**: Пошук читає з unified_listings; записи потрапляють туди лише після Phase 2 (LLM) та sync_olx_listing. Якщо Phase 2 OLX обробив 0 оголошень (наприклад, urls_for_llm порожній через llm_processing_regions або approximate_region у raw), у unified нічого не з’явиться. Покращено лог при пропуску Phase 2: виводиться кількість завантажених URL та підказка перевірити llm_processing_regions.yaml та approximate_region у raw. Для вже збережених оголошень з порожнім detail.llm — запустити scripts/reprocess_olx_underprocessed.py.

## 2026-03-02 — OLX: колекції джерела та unified лише після LLM; хеш даних пошуку в raw

- **Запит**: Для OLX дані не повинні потрапляти в колекцію джерела (olx_listings) та unified_listings до обробки LLM. У сирих даних перевіряти хеш даних зі сторінки пошуку — якщо не змінились, не оновлювати сирі дані і не запускати LLM.

- **Дії**:
  - **Хеш search_data**: utils/hash_utils.py — calculate_search_data_hash(search_data) за полями title, price_text, price_value, currency, location, area_m2. raw_olx_listings зберігає search_data_hash при upsert_raw.
  - **Пропуск без змін**: У _process_category_raw_only перед завантаженням деталей — existing_raw = raw_repo.find_by_url(url); якщо existing_raw.search_data_hash == hash(search_data), пропускаємо (не fetch detail, не upsert, не додаємо в loaded_urls). Тому Phase 2 отримує лише URL з реально змінившимися даними.
  - **OLX без promote до LLM**: Прибрано _promote_raw_olx_to_main та масовий sync усіх olx_loaded_urls у unified. Phase 2 OLX: лише _process_llm_pending(pending_list, raw_olx, main_olx, ...). _process_llm_pending читає дані з raw_repo, після LLM пише в main_repo (olx_listings) і викликає sync_olx_listing у unified. У olx_listings та unified потрапляють тільки оголошення, опрацьовані LLM.
  - **Сигнатура _process_llm_pending**: (pending_urls, raw_repo, main_repo, ...); для run_full_pipeline — raw_olx, main_olx; для run_olx_update — repo, repo (дані вже в olx_listings).
  - **Документація**: source_data_load_pipeline.md — опис search_data_hash, OLX без підняття в main до LLM.

## 2026-03-02 — Окрема колекція llm_exchange_logs; логування Gemini та Ollama

- **Запит**: Логувати запити до LLM (і Gemini, і Ollama) в окрему колекцію.

- **Дії**:
  - **Нова колекція**: llm_exchange_logs — повні запити/відповіді, хеші, токени, поле provider (gemini, ollama).
  - **LLMExchangeLogsRepository**: data/repositories/llm_exchange_logs_repository.py — метод add(), індекси timestamp, source, provider.
  - **LoggingService**: використовує llm_exchange_repo замість logs; log_llm_exchange() приймає опційний параметр provider.
  - **LogsRepository**: видалено create_llm_exchange_log та індекс llm_exchange (логи обміну більше не пишуться в logs).
  - **OllamaLLMProvider**: додано _last_usage, _last_request_text, _last_response_text; заповнення в parse_auction_description та generate_text; _usage_from_ollama_response() з prompt_eval_count/eval_count.
  - **LLMService / LangChainAgentService**: при виклику log_llm_exchange передається provider (llm_parsing_provider для parse, llm_assistant_provider для intent/generate/langchain).
  - **Міграція 043**: створення колекції llm_exchange_logs та індексів.

## 2026-03-02 — Логування повних запитів/відповідей LLM з хешами та токенами

- **Запит**: Логувати повні запити та відповіді від LLM із збереженням хешів та повних текстів вхідних/вихідних (формат запит–відповідь), плюс кількість вхідних та вихідних токенів на кожен запит.

- **Дії**:
  - **LogsRepository**: додано `create_llm_exchange_log(request_text, response_text, input_tokens, output_tokens, source, request_id, initiator)` — зберігає документ з event_type=llm_exchange, полями request_text, response_text, metadata (request_hash, response_hash — SHA256, input_tokens, output_tokens, source, request_id). Індекс для event_type+timestamp.
  - **LoggingService**: додано `log_llm_exchange(...)` — викликає репозиторій для запису обміну.
  - **LLMService**: у провайдері Gemini збереження _last_request_text та _last_response_text у parse_auction_description та generate_text; після parse_auction_description, extract_intent_for_routing та generate_text — виклик log_llm_exchange з текстами та токенами з провайдера.
  - **LangChainAgentService**: допоміжні функції _message_content_to_str, _messages_to_request_text, _aimessage_to_response_text для серіалізації повідомлень у текст; після кожного invoke (summarize, main loop, fallback) — log_llm_exchange з request_text (серіалізована історія) та response_text (вміст AIMessage).
  - **docs/developer_glossary.md**: термін «лог обміну LLM (llm_exchange)».

## 2026-03-02 — Pipeline завантаження даних: raw → main → LLM → аналітика

- **Запит**: Завантаження з джерел має йти в три етапи: (1) отримання сирих даних без обробки в колекції сирих даних джерела, паралельно по потоках (тип/область), з фіксацією fetch_filters та приблизної географії (мінімум область); (2) після завершення завантаження — визначення за налаштуваннями, які оголошення опрацювати через LLM, їх парсинг і запис у колекцію джерела + unified; (3) перерахунок аналітики та побудова гео-індексу.

- **Дії**:
  - **Документація**: docs/source_data_load_pipeline.md — опис етапів (Phase 1 raw, Phase 2 LLM + promote, Phase 3 аналітика + гео).
  - **Колекції сирих даних**: Міграція 042 — raw_olx_listings, raw_prozorro_auctions з індексами (url/auction_id, loaded_at, approximate_region, fetch_filters/fetch_context). Репозиторії RawOlxListingsRepository, RawProzorroAuctionsRepository (upsert_raw, get_by_urls/get_by_auction_ids).
  - **OLX Phase 1**: run_olx_update_raw_only() — паралельна обробка по областях, запис лише в raw_olx_listings (fetch_filters: region_filter, category_label; approximate_region з фільтра пошуку), повертає loaded_urls. Функції _process_category_raw_only, _process_region_raw_only.
  - **ProZorro Phase 1**: ProZorroService.fetch_and_save_to_raw_only(days) — отримання аукціонів з API (в т.ч. тиждень паралельно по днях), запис у raw_prozorro_auctions (fetch_context: date_from/date_to; approximate_region з _get_region_from_auction_data), повертає loaded_auction_ids.
  - **Оркестратор**: business/services/source_data_load_service.py — run_full_pipeline(sources, days): Phase 1 — run_olx_update_raw_only + fetch_and_save_to_raw_only; Phase 2 — promote raw → olx_listings/prozorro_auctions, sync усіх у unified, вибір за llm_processing_regions → _process_llm_pending (OLX) та _process_auction_with_llm + _sync_auction_to_unified (ProZorro); Phase 3 — refresh_knowledge_after_sources, price_analytics.rebuild_all(), rebuild_analytics_extracts(), опційно CadastralLocationIndexService.build_index_from_parcels.
  - **Планувальник**: У scheduler_service подія data_update при payload.use_raw_pipeline=True викликає run_full_pipeline замість окремих fetch_and_save та run_olx_update.

## 2026-03-01 — Geo-фільтр з екранованою лапкою; рядок фільтра на всю площу

- **Запит**: Рядок з geo('Район міста' INSIDE 'Солом\'янський район') не працював; розгорнути вью формування рядка фільтра на всю робочу площу.

- **Дії**:
  - **filter_string_service.py**: У _parse_geo_element підтримка екранованих лапок у гео-значеннях: патерн для quoted-рядків змінено на '((?:[^'\\]|\\.)*)' (дозволено \' та \\\\ всередині), додано _unescape_quoted() для розекрановування значень. Райони типу Солом'янський тепер коректно парсяться.
  - **styles.css**: Екран пошуку (#screen-search) — flex-column; вкладка «Фільтри та сортування» (#search-tab-settings.active) займає flex:1, блок #search-filters і .filter-string-group — flex:1, textarea .filter-string-input — flex:1 і min-height: 12rem для розгортання на всю доступну площу.
  - **index.html**: textarea #filter-string — rows="10" замість 3.

## 2026-03-01 — Пошук лише за рядком фільтрів; конструктор фільтрів

- **Запит**: Прибрати окремі поля фільтрів (Джерело, Період, Регіон, Місто, Тип, Ціна, Площі, Заголовок/Опис) і кнопки «Застосувати»/«Очистити». Залишити виключно механізм «Рядок фільтрів»: можна вставити рядок відборів вручну або натиснути «Створити фільтри» і зібрати умови в конструкторі, після чого вони перетворюються на рядок.

- **Дії**:
  - **index.html**: Блок фільтрів пошуку замінено на один блок «Рядок фільтрів»: textarea, кнопки «Шукати за рядком», «Створити фільтри», «Очистити»; додано модалку конструктора (умови за полями + гео).
  - **Backend**: GET /api/search/filter-fields — конфіг полів та гео для конструктора (get_builder_config). POST /api/search/filter-string-from-structure — приймає filters + geo, повертає filter_string (filter_string_service: structure_to_filter_models, filter_string_from_structure). ExportSearchRequest та експорт/send-export-via-bot підтримують filter_string: при наявності використовується find_by_filter_string.
  - **app.js**: performSearch завжди викликає POST /api/search/query з filter_string з textarea та sort/limit/skip. Видалено обробники та DOM для окремих фільтрів (region, city, source, apply, clear). Додано openFilterBuilderModal() (fetch filter-fields, рендер рядків з полем/оператором/значенням, гео), обробники «Створити фільтри», «Вставити рядок», «Скасувати». buildSearchExportBody та openReportConstructorFromSearch переходять на filter_string + sort.
  - **filter_string_service**: get_builder_config(), structure_to_filter_models(), filter_string_from_structure() для UI конструктора.
  - **styles.css**: .filter-string-actions, .filter-builder-row, .modal-filter-builder, .filter-builder-geo.

## 2026-03-01 — Окремо Gemini для асистента та Ollama для парсингу

- **Запит**: Використовувати для LLM-асистента Gemini; для парсингу описів — локальну модель через Ollama.

- **Дії**:
  - **config/settings.py**: додано `llm_assistant_provider`, `llm_assistant_model_name` (за замовчуванням gemini, gemini-2.5-flash) та `llm_parsing_provider`, `llm_parsing_model_name` (за замовчуванням ollama, gemma3:27b). У `_load_config` читаються секції `llm.assistant` та `llm.parsing`; при їх відсутності асистент береться з `llm.provider`/`llm.model_name`, парсинг лишається за замовчуванням (ollama).
  - **config/config.example.yaml**: додано блоки `llm.assistant` (provider: gemini, model_name: gemini-2.5-flash) та `llm.parsing` (provider: ollama, model_name: gemma3:27b).
  - **business/services/llm_service.py**: `_create_provider()` використовує `llm_parsing_provider` та `llm_parsing_model_name` для парсингу описів (parse_auction_description, parse_real_estate_objects тощо). Додано `_create_assistant_provider()` та `_assistant_provider` для extract_intent_for_routing та generate_text (Gemini за замовчуванням).
  - **business/services/langchain_agent_service.py**: `_create_llm_internal`, умова для `llm_assistant` (thinking) та embeddings використовують `llm_assistant_provider` та `llm_assistant_model_name`.
  - **business/services/llm_agent_service.py**: модель асистента — `llm_assistant_model_name`. **telegram_mini_app/routes/admin.py**: відображення моделі асистента — `llm_assistant_model_name`.
  - **scripts/benchmark_parsing_methods.py**: переключення на `llm_parsing_provider` / `llm_parsing_model_name` при бенчмарку ollama/gemini.
  - **docs/developer_glossary.md**: термін «LLM асистент vs парсинг».

## 2026-03-01 — Інтеграція фільтрів на сторінку пошуку, звіти та LLM-генерація рядка пошуку

- **Запит**: Фільтри мають бути інтегровані і на сторінку пошуку, і в звіти. LLM-помічник має обробляти запити користувача та вміти генерувати рядок пошуку відповідно до запиту.

- **Дії**:
  - **Сторінка пошуку та звіти**: Рядок фільтрів (filter_string) та flat-параметри (title_contains, description_contains, region, city, price_min/max тощо) вже підключені в routes/search.py та report_templates.py (unified_search). Звіти використовують find_by_filter_string або build_query_from_flat_params + find(); у шаблоні можна передавати filter_string.
  - **LLM-інструменти**: У langchain_agent_service додано реалізації _generate_search_filter_string та _search_unified_listings. generate_search_filter_string приймає flat-параметри (region, city, source, property_type, price_min, price_max, date_filter_days, title_contains, description_contains) і повертає filter_string через filter_string_from_flat_params. search_unified_listings приймає або filter_string (пошук за рядком), або ті самі flat-параметри; повертає items, total, при flat-параметрах також filter_string.
  - **Маршрути та run_tool**: Інструменти generate_search_filter_string та search_unified_listings додані в TOOL_ROUTES для query_export та analytics; у run_tool додано обробники для обох назв.
  - **Системний промпт**: Додано блок «Зведений пошук оголошень (unified_listings)» — коли використовувати generate_search_filter_string та search_unified_listings, формат рядка фільтрів, приклади запитів («підбери фільтри», «згенеруй рядок пошуку за Києвом», «зроби пошук за …»).

## 2026-03-01 — Менше вихідних токенів LLM, аналітика по токенах та вартість Gemini

- **Запит**: Прибрати політику «запит–контекст–запит» у промптах; скоротити вихідні токени без втрати якості. В аналітиці зберігати вхідні/вихідні токени замість кількості запитів. У адмін-панелі показувати токени та приблизні витрати на LLM (прайсинг Gemini).

- **Дії**:
  - **.cursor/rules/prompt-design.mdc**: прибрано обов'язкову тріаду «завдання–контекст–закріплення»; рекомендовано стислі промпти, одна чітка інструкція, опційне закріплення; додано практики зменшення вихідних токенів.
  - **config/prompts.yaml**: скорочено intent_detection, query_structure, real_estate_objects_parsing, listing_analytics — менше повторень, без зайвих блоків «Закріплення завдання».
  - **data/repositories/logs_repository.py**: sum_llm_tokens_by_day, sum_llm_tokens_total, sum_llm_tokens_last_month — агрегація metadata.input_tokens, metadata.output_tokens для api_usage (service=llm).
  - **business/services/llm_service.py**: GeminiLLMProvider — _last_usage після generate_content, _usage_from_response(); логування api_usage після виклику з input_tokens, output_tokens (parse_auction_description, extract_intent_for_routing, generate_text).
  - **business/services/langchain_agent_service.py**: _extract_usage_from_aimessage() для AIMessage; лог api_usage після invoke з токенами (main loop, fallback, summarize).
  - **config/llm_pricing.py**: прайсинг Gemini (за 1M токенів): Flash 0.30/2.50, Pro 1.25/10, Flash-Lite 0.10/0.40; estimate_gemini_cost_usd().
  - **telegram_mini_app/routes/admin.py**: get_usage_stats повертає для LLM токени (by_day, total, last_month), estimated_cost_usd, estimated_cost_usd_last_month, model; _fill_days_with_zeros_tokens().
  - **telegram_mini_app/static/app.js**: адмін-панель — відображення вхідних/вихідних токенів, орієнтовної вартості USD, графік «Токени LLM (вх+вих)».
  - **docs/llm_output_tokens_research.md**: дослідження та практики зменшення вихідних токенів; посилання на джерела.
  - **docs/developer_glossary.md**: термін «токени (аналітика LLM)».

## 2026-03-01 — Налаштування областей для LLM-обробки (OLX + ProZorro)

- **Запит**: Додати в панель адміністрування налаштування: які області проганяти через LLM при обробці. Сирі дані з джерел зберігати в колекцію, максимально використовуючи парсинг без LLM. Якщо область або м. Київ не в переліку увімкнених — лишати сирі дані без LLM в колекції джерела та unified. Користувач може вмикати/вимикати області; при включенні пропонувати оновлення на глибину 1/7/30 днів та чи оброблювати неактивні. Перед LLM додатково перевіряти оголошення в джерелах (OLX: пуста сторінка або повідомлення про неактивність; ProZorro — за статусом).

- **Дії**:
  - **config/llm_processing_regions.yaml**: конфіг списку увімкнених областей (enabled_regions). Порожній = усі області увімкнені (backward compatibility).
  - **business/services/llm_processing_regions_service.py**: get_all_region_names() з olx_region_slugs; get_enabled_regions() / set_enabled_regions(); is_region_enabled_for_llm(); normalize_region_name() для збігу з ProZorro.
  - **OLX**: У run_update зберігається search_data.region_filter (область пошуку). Якщо область не в увімкнених — сирі дані зберігаються, sync в unified без LLM; pending_llm_urls лише для увімкнених областей. Парсер parser.is_detail_page_inactive(html) — визначення неактивної сторінки (типові фрази OLX); при _inactive зберігаємо з is_active=False. Перед Phase 2 LLM — повторне завантаження сторінки деталей і перевірка неактивності.
  - **ProZorro**: _get_region_from_auction_data() з address_refs / items[].address; перед кожним _process_auction_with_llm перевірка is_region_enabled_for_llm(region). fetch_and_save_real_estate_auctions та _fetch_and_save_week_optimized приймають llm_only_for_active для опції «обробляти неактивні».
  - **UnifiedListingsService**: статус OLX з olx_doc.is_active (активне/неактивне).
  - **Admin API**: GET/PUT /api/admin/llm-processing-regions; POST /api/admin/llm-processing-regions/backfill (days, process_inactive) з опитуванням статусу через data-update/status.
  - **Admin UI**: блок «Області для LLM-обробки» — чекбокси областей, кнопка Зберегти; модалка «Оновити вже завантажені дані?» з глибиною 1/7/30 днів та опцією «Обробляти неактивні».

## 2026-03-01 — Площа землі в м², відображення в сотках та ціна за сотку

- **Запит**: Змінити використання одиниць площі для земельних ділянок: зберігати всюди в м², для зручності відображати землю в сотках (с) та ціну за сотку замість гектара. Нерухомість — без змін (м², грн/м²).

- **Дії**:
  - **Зберігання**: У колекціях (unified_listings, olx_listings detail.llm, llm_cache result, analytics_extracts) площа землі тепер **land_area_sqm** (м²). Поле land_area_ha видалено з unified_listings після міграції.
  - **utils/price_metrics.py**: compute_price_metrics приймає land_area_sqm; price_per_ha рахується як price / (land_sqm/10000). Константи SQM_PER_HECTARE, SQM_PER_SOTKA.
  - **UnifiedListingsService**: _extract_area_info повертає land_area_sqm; для OLX читає land_area_sqm або конвертує land_area_ha*10000; для ProZorro земля накопичується в м².
  - **LLM (llm_service)**: _normalize_result додає land_area_sqm = land_area_ha*10000; olx_scraper/run_update та listing_reformat використовують land_area_sqm для compute_price_metrics.
  - **Міграція 041_land_area_sqm.py**: unified_listings land_area_ha→land_area_sqm; olx_listings detail.llm.land_area_sqm; llm_cache result.land_area_sqm; перезаповнення analytics_extracts.
  - **API/UI**: Фільтр площі землі приймає значення в сотках (land_area_ha_value), перетворюється на land_area_sqm = value*100. Відповідь містить land_area_sqm та land_area_sotky (для відображення). Ціна за сотку в UI: price_per_ha_uah/100 (грн/с).
  - **telegram_mini_app/static/app.js**: Відображення землі в сотках; ціна за сотку (₴/с); таблиці та деталі — лейбли «Ціна за сотку», «Земля: X с».
  - **data_dictionary.yaml, price-metrics-rules.mdc, developer_glossary.md**: оновлено описи полів та правила; додано термін «площа землі (одиниці)».

## 2026-03-01 — Єдиний механізм фільтрів пошуку (дерево фільтрів, рядок, гео)

- **Запит**: Змінити підхід до фільтрів на сторінці пошуку: елементи (поле, тип порівняння, значення), групи І/АБО, геопошук окремо (в межах, не в межах, в радіусі). Додати пошук за заголовком і описом. Рядок фільтрів для редагування; парсинг з рядка. Централізувати пошук для сторінки пошуку та звітів.

- **Дії**:
  - **domain/models/filter_models.py**: додано FilterOperator.NOT_CONTAINS; GeoFilterOperator — INSIDE, NOT_INSIDE, IN_RADIUS (разом з існуючими EQ, NE).
  - **domain/managers/collection_manager.py**: _filter_element_to_mongo — обробка NOT_CONTAINS; _geo_filter_to_mongo — INSIDE/NOT_INSIDE для region, settlement, city_district; винесено _query_to_mongo_filter, get_count(query).
  - **config/search_fields.yaml**: конфіг полів пошуку для unified_listings (лейбли, value_type, allowed_operators), секція geo (toponym_precision, operators).
  - **domain/services/filter_string_service.py**: серіалізація FilterGroup + GeoFilter у рядок; парсинг рядка → FilterGroup + GeoFilter (filter_group_to_string, filter_string_to_models); маппінг лейблів з конфігу.
  - **domain/services/unified_search_service.py**: find(filter_group, geo_filter, sort, limit, skip), find_by_filter_string(filter_string, ...), build_query_from_flat_params(...), get_search_fields_config; за замовчуванням status=активне.
  - **telegram_mini_app/routes/search.py**: у _build_unified_filters додано title_contains, description_contains; у GET /unified — query-параметри title_contains, description_contains; POST /api/search/query — пошук за filter_string (тіло SearchByFilterRequest).
  - **docs/geo_distance_index_design.md**: опис дизайну індексу відстаней до топонімів (поки не реалізований; IN_RADIUS від центру bounds).
  - **docs/developer_glossary.md**: терміни «дерево фільтрів», «рядок фільтрів», «UnifiedSearchService».

## 2026-02-26 — Відновлення позиції скролу на сторінці пошуку при поверненні з оголошення

- **Запит**: При поверненні на сторінку пошуку після перегляду оголошення повертати користувача на те саме місце у списку, звідки відкривали картку.

- **Дії**:
  - **telegram_mini_app/static/app.js**: У `searchState` додано поле `searchScrollTop`. При кліку по картці оголошення перед викликом `showDetail()` зберігається `scrollTop` контейнера `#screen-search`. Після повернення викликається `performSearch()`, який перемальовує список; в кінці `renderSearchResults()` при наявності збереженої позиції виконується відновлення скролу через `requestAnimationFrame` і скидання `searchScrollTop`.

## 2026-02-26 — Відповідь на запит «середня вартість за кв. м. у Соломянському районі Києва»

- **Запит**: ЛЛМ помічник не міг сформувати відповідь на «Розрахуй середню вартість за кв. м. нерухомості у Соломянському районі Києва». Потрібно перевірити доступ агентів до аналітичних даних та заповнення районів міст (зокрема Солом'янського).
- **Причина**: (1) У _try_analytics_extracts місто визначалося лише за «київ»/«києві»; у формулюванні «районі Києва» є родові відмінки «києва», тому city не встановлювався. (2) Запити «розрахуй середню вартість» не завжди класифікувалися як analytical_text.
- **Дії**: MultiAgentService._try_analytics_extracts — додано родові відмінки міст (києва, львова, харкова, одеси, дніпра). IntentDetectorAgent._refine_format_with_rules — патерни «розрахуй»/«порахуй»/«підрахуй» та фрази «середня/середню вартість/ціна», «ціна за кв/м²» для analytical_text. Якщо по району немає записів у analytics_extracts — shortcut повертає середнє по місту з приміткою «дані по району відсутні». Заповнення city_district: rebuild_analytics_extracts з unified_listings; опційно enrich_analytics_extracts_city_districts.py (геокодування за вулицями).

## 2026-02-26 — Опис стеку технологій та оцінка складності для резюме

- **Запит**: Створити опис стека технологій проекту для додавання в резюме; приблизно оцінити складність реалізованих фіч та рівень проектування.

- **Дії**:
  - **docs/tech_stack_and_complexity.md**: додано документ з розділами: (1) стек технологій (backend, LLM/MCP, джерела даних, клієнт, міграції); (2) архітектура та рівень проектування (шари, мультиагентна система, MCP як domain API); (3) таблиця складності фіч по категоріях; (4) підсумкова оцінка та готові формулювання для резюме (короткий та розширений опис).

## 2026-02-26 — Контекст оголошення та аналіз місцезнаходження з листингу

- **Запит**: 1) Асистент не розумів, що питання стосується саме оголошення, з якого він викликаний — потрібен повноцінний контекст при першому запиті. 2) По опрацюванню конкретного оголошення помічник має проводити вивчення даних листингу та сирих даних, розбір тексту, виклик геоінструментів для спроби визначення місцеположення та інтерпретацію результатів.

- **Дії**:
  - **Контекст оголошення**: У `_get_context_summary`, при формуванні `effective_context` у `_process_query_new_flow` та у SystemMessage для LangChain додано явні формулювання: «Користувач пише в контексті ОДНОГО оголошення. Фрази «оголошення», «в оголошенні», «місцезнаходження», «розташування», «адреса» стосуються САМЕ цього оголошення. Посилання вже надано — НЕ проси його.»
  - **Маршрут listing_detail**: Якщо є `listing_context` і запит містить ключові слова про локацію (місцезнаходження, розташування, адреса ділянки, координати тощо), запит направляється в LangChain з `route=listing_detail` замість пайплайну.
  - **Інструмент get_listing_details**: У LangChain додано інструмент, який повертає повні дані оголошення (зведені + сирі): опис, location.raw, llm.addresses, регіон/місто. Викликається без аргументів для поточного оголошення (з `_current_listing_context`) або з `source` та `source_id`. Агент може витягувати топоніми з опису та викликати `geocode_address` для визначення координат.
  - **TOOL_ROUTES["listing_detail"]**: набір інструментів `get_listing_details`, `geocode_address`, `get_collection_info`. Для маршруту додано підказку: спочатку get_listing_details, потім витягнути топоніми, geocode_address, інтерпретувати результати.

## 2026-02-26 — OLX: фільтр «усі оголошення крім бізнес-центрів»

- **Запит**: Додати для нерухомості фільтр на OLX — всі оголошення крім бізнес-центрів (Тип об'єкта: виключити «Бізнес центр»).

- **Дії**:
  - **config/olx_commercial_object_type_slugs.yaml**: список slug типів об'єкта для включення (other, shopping_and_office_center, administrative_building, non_residential_in_residential_fund, residential_fund, hotel); бізнес-центр (business_center) виключено.
  - **scripts/olx_scraper/config.py**: шлях _OLX_COMM_OBJECT_TYPE_SLUGS_PATH, функція get_olx_comm_re_object_type_slugs_include(); у get_commercial_real_estate_list_url додано пари search[filter_enum_comm_re_object_type]=<slug> для кожного slug з конфігу.

## 2026-02-26 — OLX: додаткові фільтри на ресурсі (площа, поверх, земля)

- **Запит**: Додати при завантаженні оголошень з OLX фільтри на самому сайті. Для нерухомості: площа більше 200 м², поверх 1–2. Для земельних ділянок: більше 15 соток.

- **Дії**:
  - **scripts/olx_scraper/config.py**: константи фільтрів (FILTER_REAL_ESTATE_TOTAL_AREA_FROM_M2=200, FILTER_REAL_ESTATE_FLOOR_FROM/TO=1/2, FILTER_LAND_AREA_FROM_SOTOK=15), опційно змінні середовища OLX_FILTER_*; `_build_category_url` приймає `extra_query_pairs` і збирає query через `urlencode`; `get_commercial_real_estate_list_url` додає `search[filter_float_total_area:from]`, `search[filter_float_floor:from]`, `search[filter_float_floor:to]`; `get_land_list_url` додає `search[filter_float_land_area:from]`.

## 2026-02-25 — Бенчмарк парсингу OLX без LLM

- **Запит**: Перевірити можливість отримання структурованої інформації зі сторінки OLX без LLM; порівняти інструменти парсингу; зосередитись на нежитловій нерухомості (площа, поверх, адреси, призначення) та земельних ділянках (кадастровий номер, площа, місцезнаходження, призначення, будинок на ділянці).

- **Дії**:
  - **scripts/olx_html_parsing_benchmark.py**: бенчмарк — завантаження HTML за URL, парсинг через BeautifulSoup, lxml, Selectolax, Parsel; екстракція з параметрів (ad_parameters) + regex (listing_regex_extractor) + блок «Місцезнаходження»; порівняння з detail.llm.
  - **requirements.txt**: додано selectolax, parsel.
  - **docs/olx_parsing_benchmark_report.md**: звіт — огляд інструментів, методологія, результати по полях, аналіз для нежитлової нерухомості та землі, висновки.
  - **Висновки**: Парсери дають однакову якість; lxml/Parsel найшвидші. Поверх і кадастровий номер — 100% за параметрами; площі — 40–60%; адреси та призначення — обмежено без LLM.

## 2026-02-25 — OLX: Phase 1 (сирі дані) + Phase 2 (LLM одним потоком)

- **Запит**: Спочатку завантажити сирі дані по всім областям і типам, потім обробити LLM одним потоком з прогрес-баром.

- **Дії**:
  - **run_update.py**: Phase 1 — паралельна обробка по областях: завантаження сторінок пошуку та деталей, збереження в БД без LLM. Оголошення з новим/зміненим контентом позначаються `llm_pending=True`, URL додаються в `pending_llm_urls`.
  - **run_update.py**: Phase 2 — після завершення Phase 1: обробка `pending_llm_urls` одним потоком через `_process_llm_pending` з прогрес-баром (tqdm). LLM, геокодування, price_metrics, sync у unified.
  - **llm_service.py**: прибрано `_llm_api_lock` — LLM тепер викликається лише з одного потоку в Phase 2.

## 2026-02-25 — Серіалізація LLM та retry при 429 (замінено на Phase 1+2)

- **Дії**:
  - **llm_service.py**: при 429 після вичерпання retry для моделі — очікування `retry_delay` перед переходом на наступну модель.

## 2026-02-25 — Виправлення: 404 при парсингу через Gemini (gemini-pro застаріла)

- **Запит**: Помилка при парсингу — `models/gemini-pro is not found for API version v1beta`. Використовується лише Gemini Flash, Pro не потрібен.

- **Дії**:
  - **llm_service.py**: видалено `gemini-pro` та `gemini-2.5-pro` з fallback-списку в `GeminiLLMProvider._validate_model`. Залишено лише Flash-моделі: preferred_model, gemini-2.5-flash, gemini-2.5-flash-lite.
  - **llm_service.py**: дефолт для Gemini при `_create_provider` змінено з `gemini-1.5-flash` на `gemini-2.5-flash`.

## 2026-02-24 — Shortcut analytics_extracts для середньої ціни за м² з районом

- **Запит**: Агент не міг обробити «середня вартість за кв. м. комерційної нерухомості в м. Київ в Соломянському районі» — хоча дані є.

- **Дії**:
  - **utils/district_normalizer.py**: нормалізація назв районів Києва (Соломянський/Солом'янський), extract_district_from_query, get_district_filter_value ($regex з опціональним апострофом).
  - **MultiAgentService._try_analytics_extracts**: shortcut для запитів «середня вартість/ціна за кв. м.» + місто (Київ, Львів, тощо) + опціонально район. Викликає analytics_extracts_aggregate, формує відповідь. Викликається перед _try_analytics_aggregation_by_city.
  - **AnalystAgent**: додано analytics_extracts_aggregate до _action_to_tool.
  - **Промпти**: приклад для «в Соломянському районі» з city_district regex; список районів Києва в prompts.yaml та langchain_agent_service.

## 2026-02-24 — Колекція analytics_extracts та доменний шар для агрегацій

- **Запит**: Створити додаткову колекцію з доменним шаром над нею, доступну для MCP агентів. Реалізувати методи агрегації по метриках, пошуку з логічними умовами, створення метрик на базі існуючих. Запит «Знайди район міста Києва, де середня ціна за квадратний метр нерухомості найвища» має виконуватись чисто за рахунок агрегації з цієї колекції.

- **Дії**:
  - **Міграція 040**: колекція `analytics_extracts` з індексами (source+source_id, region, city, city_district, source_date).
  - **AnalyticsExtractsRepository**: upsert_extract, upsert_many, clear_all.
  - **AnalyticsExtractsService** (domain/services): aggregate_by_metric (avg/sum/min/max), search з логічними умовами ($and, $or, $not, eq, gt, gte, lt, lte, in, regex), get_distinct_values, get_available_metrics, get_group_by_fields.
  - **analytics_extracts_populator**: rebuild_analytics_extracts() — витягує плоскі поля з unified_listings (listing_url, property_type, source, source_date, region, oblast_raion, settlement_type, settlement, city, city_district, street_type, street, building, floor, land_area_sqm, price_per_ha_uah/usd, building_area_sqm, price_per_m2_uah/usd).
  - **PriceAnalyticsService.rebuild_all()**: викликає rebuild_analytics_extracts() перед compute_indicators/aggregates.
  - **MCP tools** (LangChain agent): analytics_extracts_aggregate, analytics_extracts_search, analytics_extracts_list_metrics, analytics_extracts_list_dimensions, analytics_extracts_get_distinct. Опис інструментів містить логічні умови.
  - **data_dictionary.yaml**: analytics_extracts з полями та індексами.
  - **SourceFieldMapper**: analytics_extracts.
  - **Промпт агента**: пріоритет analytics_extracts для гео-агрегацій («район міста з найвищою ціною за м²»).

## 2026-02-24 — Доповнення analytics_extracts районами міста через Google Maps

- **Запит**: З analytics_extracts вибрати унікальні комбінації Область/місто/вулиця для Києва, Одеси, Львова, Миколаєва, Харкова; для кожної виконати геокодування в Google Maps і доповнити city_district.

- **Дії**:
  - **scripts/enrich_analytics_extracts_city_districts.py**: скрипт — агрегація унікальних (region, city, street), геокодування через GeocodingService, оновлення city_district, oblast_raion, geocode_formatted_address, geocode_latitude, geocode_longitude. Опції --dry-run, --limit.
  - **data_dictionary.yaml**: поля geocode_formatted_address, geocode_latitude, geocode_longitude для analytics_extracts.

## 2026-02-24 — Геопошук: root geo та райони міста в unified_listings

- **Запит**: (1) Додавати до адрес у великих містах (обласні, районні центри) додаткові геометрики — райони міста (city_district), визначаються при наявності вулиці; (2) В корені unified_listings — базові географічні поля: region, oblast_raion, city, city_district; заповнювати з addresses — якщо не суперечать, інакше більшість або точна адреса.

- **Дії**:
  - **unified_listings_service**: `_normalize_address_from_geocode` — додано city_district з sublocality (Google Maps); `_compute_root_geo_from_addresses` — обчислення root geo з addresses (majority vote, пріоритет точній адресі).
  - **unified_listings**: root поля region, oblast_raion, city, city_district; заповнюються при sync з OLX/ProZorro.
  - **data_dictionary.yaml**: root geo поля, city_district в addresses.nested_fields.
  - **SourceFieldMapper**: unified_listings — city, region, oblast_raion, city_district на root; get_geo_match_keys → (region, city); uses_root_geo().
  - **GeoFilterBuilder**: для unified_listings — root geo без $unwind.
  - **CollectionManager**: _geo_filter_to_mongo — root city/region з fallback на addresses; без $unwind.
  - **UnifiedListing entity**: get_region, get_settlement — root з fallback; get_oblast_raion, get_city_district; _format_addresses_for_export — city_district.
  - **Міграція 039**: backfill root geo з addresses для існуючих записів.
  - **UnifiedListingsRepository**: індекси region, city, oblast_raion, city_district.

## 2026-02-24 — Фільтрація землі OLX за типами (без с/г, без LLM)

- **Запит**: При завантаженні земельних ділянок з OLX завантажувати всі, окрім сільськогосподарського призначення — через фільтри OLX (як області й типи оголошень), без використання LLM.

- **Дії**:
  - **config/olx_land_type_slugs.yaml**: словник типів землі (житлова, оздоровча, рекреаційна, лісова, водна, промислова, запасу) — с/г виключено.
  - **scripts/olx_scraper/config.py**: `LAND_SALE_PATH`, `get_olx_land_type_slugs()`, `get_land_list_url(land_type_slug=...)` — URL типу `.../prodazha-zemli/{land_type_slug}/{region_slug}/`.
  - **scripts/olx_scraper/run_update.py**: `_get_base_categories()` — замість однієї «Земельні ділянки» 7 категорій за типами з olx_land_type_slugs.
  - Прибрано LLM-фільтрацію с/г з UnifiedListingsService та config (olx.exclude_agricultural_land).

## 2026-02-24 — Подальші кроки ревʼю: метрики recovery, retry, LangGraph

- **Запит**: Реалізувати всі три подальші кроки з ревʼю AI-помічника.

- **Дії**:
  - **Метрики recovery**: `tool_failures_count`, `tool_recovery_attempted` у `_last_request_metrics` та `agent_activity_log`; `_build_request_metrics()` для формування словника.
  - **Retry при порожніх результатах**: розширено `_agent_hint` для execute_query/execute_aggregation з 0 результатів — підказки про альтернативні колекції (prozorro↔olx, unified→both).
  - **LangGraph**: додано `langgraph>=0.2.0`; `business/services/langgraph_agent_runner.py` — StateGraph (agent→tools→agent), checkpointer MemorySaver; `llm_agent_use_langgraph` у config (default false); інтеграція в `_process_query_impl` з fallback на while-цикл при помилці.

## 2026-02-24 — Ревʼю AI-помічника: ReAct, error recovery, тестування флоу

- **Запит**: Провести ревʼю ШІ-помічника, вивчити Reddit та тематичні ресурси для концепцій (MCP, LangChain), запропонувати способи покращення, протестувати та порівняти з існуючим флоу.

- **Дії**:
  - **docs/ai_assistant_review_2026.md**: документ ревʼю з концепціями з ReAct, PALADIN, Reflexion, Reddit (hybrid workflow), LangChain MCP docs.
  - **Покращення 1 — Error recovery hint**: при `success=false` у результаті tool додається `[ПОМИЛКА ІНСТРУМЕНТУ]` з інструкцією само-корекції (проаналізуй причину, спробуй інший підхід). `langchain_agent_service.py`.
  - **Покращення 2 — ReAct/антиципаторне міркування**: у `config/prompts.yaml` (langchain_system) додано блок «МІРКУВАННЯ ТА ПЕРЕВІРКИ» — перед викликом інструменту формулювати, що шукаєш і чому; перевіряти параметри; мати на увазі альтернативи (ProZorro→OLX); при помилці — аналізувати і пробувати інший підхід.
  - **scripts/review_ai_assistant_flow.py**: скрипт для прогону тестових запитів через LangChainAgentService, збір метрик (ітерації, тривалість), порівняння флоу. Запуск: `py scripts/review_ai_assistant_flow.py` (опції: --filter, --limit).

## 2026-02-27 — Перехід парсингу оголошень на локальну Gemma3 12B через Ollama

- **Запит**: Переключити обробку парсингу оголошень на локальну модель gemma3 12B.
- **Дії**:
  - **config/config.yaml**: змінено LLM-провайдера на `ollama` та модель на `gemma3:12b`, додано секцію `api_keys.ollama`. Це переводить парсинг оголошень (OLX, ProZorro) та інші виклики `LLMService` на локальну модель через Ollama (за наявності моделі `gemma3:12b` у середовищі).
  - **config/config.example.yaml**: оновлено коментарі щодо підтримки `ollama` та переліку моделей (gemma3:12b, gemma3:27b).

## 2026-02-24 — Фільтр області в пошуку: Київська замість Житомирської

- **Запит**: При фільтрі «Житомирська область» в результатах з’являлись оголошення з «Київська область».

- **Причина**: (1) Документи з кількома адресами (напр. лот з Київської + лот з Житомирської) потрапляли через $elemMatch, але відображалась перша адреса (Київська); (2) regex без re.escape міг давати неочікувані збіги.

- **Дії**:
  - **telegram_mini_app/routes/search.py**: `_build_unified_filters` — re.escape для region, нормалізація варіантів («Житомирська область» → «Житомирська» для substring-збігу).
  - **telegram_mini_app/routes/search.py**: `_filter_addresses_by_region` — при наявності фільтра області залишає тільки адреси, що збігаються; `_normalize_unified_doc` отримує вже відфільтрований doc, тому показує правильну область.
  - Застосовано до search_unified, export, send-export-via-bot.

## 2026-02-23 — Київська область без Києва та відображення списків у відповіді

- **Запит**: (1) Запит «10 найвигідніших земельних ділянок в Київській області» повертав оголошення з м. Києва замість області; (2) АІ помічник створював відповідь «знайшов 10» без фактичного списку — користувач не бачив дані.

- **Дії**:
  - **InterpreterAgent._infer_region_filter**: «Київська область» / «Київської області» (БЕЗ «та») → region: «Київська», exclude_city: «Київ». Київ не входить до Київської області.
  - **prompts.yaml, QueryStructureAgent**: для «Київська область» — filter_metrics з region + exclude_city.
  - **domain/validators.py**: додано exclude_city до ALLOWED_LOGICAL_FILTER_KEYS.
  - **PipelineExecutor**: підтримка exclude_city у GEO_KEYS, передача в geo_filters_dict для GeoFilterService.from_dict.
  - **FinalAnswerRefinementService**: для запитів типу «топ-N», «найвигідніші» — інструкція: відповідь ОБОВ'ЯЗКОВО має містити список з описом кожного об'єкта, не замінювати фразою «готові до ознайомлення».

## 2026-02-23 — Аналіз використання Google Maps та прайсинг

- **Запит**: Створити аналогічний аналіз та прайсинг по використанню Google Maps та інших сервісів Google.

- **Дії**:
  - **scripts/google_maps_usage_analysis.py**: аналіз Geocoding API та Places API (Nearby Search); дані з logs (api_usage service=geocoding), geocode_cache; екстраполяція на місяць; прайсинг за Google Maps Platform (березень 2025) — Geocoding $5/1000 після 10k безкоштовних, Places Nearby $32/1000 після 5k безкоштовних.
  - Запуск: `py scripts/google_maps_usage_analysis.py --days 60`.

## 2026-02-23 — Тестування та порівняння методів парсингу з існуючими потоками

- **Запит**: Провести тестування та порівняння із існуючими потоками обробки інформації.

- **Дії**:
  - Розширено benchmark: порівняння з існуючим кешем (результати поточного потоку Gemini), підрахунок заповнених полів.
  - Запуск: 16 зразків (8 ProZorro + 8 OLX). Regex: 0.001 с, 2.8 полів/8; LLM: 4.7 полів/8; узгодженість regex з LLM 0–67%.
  - Ollama (gemma3:27b): 2 зразки, ~68 с/опис (перший запуск), 100% узгодженість з Gemini.
  - **docs/benchmark_parsing_report.md**: звіт з результатами, висновками та рекомендаціями.

## 2026-02-23 — План зменшення токенів, regex-екстрактор, Ollama (gemma3:27b), benchmark

- **Запит**: План зменшення токенів і запитів до LLM; альтернативні методи розпізнавання (regex, BERT); тестування локальної LLM gemma3:27b через Ollama; використання кешу для порівняння результатів. Фокус на парсингу оголошень та аукціонів.

- **Дії**:
  - **docs/plan_token_reduction.md**: план — каскадний підхід (regex → LLM), скорочення промпту, локальна LLM; альтернативи (regex, BERT, Ollama).
  - **utils/listing_regex_extractor.py**: regex-екстрактор — cadastral_number, building_area_sqm, land_area_ha, addresses, floor, property_type, utilities, tags; `extract_from_description()`, `can_skip_llm()`.
  - **OllamaLLMProvider**: провайдер для Ollama (gemma3:27b); `parse_auction_description`, `generate_text`; підтримка в LLMService._create_provider.
  - **config/settings.py, config.example.yaml**: підтримка `ollama` у provider, model_name, api_keys.
  - **requirements.txt**: додано `ollama>=0.3.0`.
  - **scripts/benchmark_parsing_methods.py**: benchmark — зразки з prozorro_auctions та olx_listings; методи regex, gemini, ollama; порівняння результатів та часу. Запуск: `py scripts/benchmark_parsing_methods.py --limit 5 --methods regex,gemini,ollama`.

## 2026-02-23 — Аналіз використання LLM: токени на оголошення, екстраполяція, прайсинг

- **Запит**: Глибока аналітика та прогнозування застосування LLM: (1) середня кількість вхідних-вихідних токенів на обробку одного оголошення OLX + пов’язаного ОНМ; (2) екстраполяція на місяць за середньою кількістю оголошень/аукціонів на добу; (3) приблизні місячні витрати за Gemini 3 Flash.

- **Дії**:
  - **LogsRepository**: метод `count_api_usage_by_source(service, days, from_cache_only)` — агрегація api_usage по metadata.source.
  - **scripts/llm_usage_analysis.py**: скрипт аналізу — оцінки токенів з коду (parsing template ~3200 символів, real_estate_objects ~1200), середня кількість оголошень/аукціонів на добу з БД, екстраполяція на 30 днів, вартість за Gemini 3 Flash ($0.50/1M input, $3.00/1M output) та Gemini 2.5 Flash ($0.30/1M, $2.50/1M).
  - **Результати (приклад)**: ~2113 токенів на 1 оголошення OLX+ОНМ; ~1428 на аукціон ProZorro; при ~1926 OLX/добу та ~79 ProZorro/добу — ~125M токенів/міс, ~$105 (Gemini 3 Flash) або ~$75 (Gemini 2.5 Flash). Запуск: `py scripts/llm_usage_analysis.py --days 60`.

## 2026-02-23 — Контекст архітектури даних для маршрутизації (порівняння цін по містах)

- **Запит**: LLM не бачило оголошення Дніпра при порівнянні цін Київ vs Дніпро; неправильний вибір розрахунку — лізло в raw listings замість агрегованих даних. Потрібен кращий контекст для частини процесу, що відповідає за маршрут отримання даних.

- **Дії**:
  - **app_metadata.yaml**: додано секцію `data_flow_architecture` — шари (primary → unified → aggregated), правила маршрутизації (порівняння по містах → execute_analytics, НЕ execute_query).
  - **AppMetadataService.get_metadata_for_llm**: включено data_flow_architecture та routing_rules у контекст для LLM.
  - **prompts.yaml (langchain_system)**: блок «КРИТИЧНО — Пріоритет інструментів» — для порівняння по містах використовувати execute_analytics з groupBy city, не raw listings.
  - **_try_analytics_aggregation_by_city**: фільтр city з $in для settlement-варіантів (Київ, м. Київ, Дніпро, м. Дніпро) — збіг з addresses.settlement у БД.

## 2026-02-23 — Domain-шар для listing_analytics та real_estate_objects, контекст LLM

- **Запит**: LLM-помічник має мати актуальний контекст структури доступних даних доменного рівня. Domain-шар не тільки над таблицями з джерел і зведеною таблицею, але й над аналітикою та об'єктами нерухомого майна. Ці сутності мають бути включати в пайплайни обробки інформації.

- **Дії**:
  - **data_dictionary.yaml**: додано колекцію `listing_analytics` (source, source_id, analysis_text, analysis_at).
  - **app_metadata.yaml**: додано описи `unified_listings`, `listing_analytics`, `real_estate_objects`, `price_analytics` для контексту LLM.
  - **Domain entities**: `ListingAnalytics`, `RealEstateObject`; колекції `ListingAnalyticsCollection`, `RealEstateObjectCollection`.
  - **CollectionManagers**: `ListingAnalyticsCollectionManager`, `RealEstateObjectsCollectionManager` — find(), get_field_structure(), get_available_field_values().
  - **PipelineExecutor, PipelineService**: підтримка listing_analytics та real_estate_objects як колекцій пайплайну; `PIPELINE_COLLECTIONS`.
  - **QueryBuilder, schema_mcp_server**: додано listing_analytics, real_estate_objects, price_analytics до ALLOWED_COLLECTIONS.
  - **SourceFieldMapper**: маппінг логічних полів для listing_analytics та real_estate_objects.
  - **prompts.yaml (query_structure)**: джерела — unified_listings (за замовч.), listing_analytics, real_estate_objects для відповідних запитів.
  - **get_allowed_collections**: описи нових колекцій для LangChain та MCP.

## 2026-02-23 — Перезаповнення земельних ділянок з кадастру в процедурі оновлення ОНМ

- **Запит**: У процедуру оновлення колекції об'єктів нерухомого майна додати перезаповнення даних земельних ділянок на базі кадастру. Для існуючих записів — якщо знаходимо земельну ділянку по номеру в кадастрі, завантажувати в ОНМ те, чого немає в оголошенні. Якщо площа в кадастрі та в оголошенні суттєво різна (±50 м²), створювати окремий показник «площа за кадастром» і підсвічувати різницю в інтерфейсі.

- **Дії**:
  - **RealEstateObjectsService**: `_merge_cadastral_info` — повертає `(cadastral_info, area_sqm_for_doc, area_by_cadastre_sqm)`; заповнює з кадастру purpose, purpose_label, category, ownership_form, address; при різниці площ >50 м² додає `area_by_cadastre_sqm`.
  - **RealEstateObjectsService**: `_backfill_land_plot_from_cadastre` — перезаповнення існуючих land_plot даними з cadastral_parcels.
  - **process_listing**: для існуючих land_plot викликається `_backfill_land_plot_from_cadastre` перед add_source_listing; для нових — `area_by_cadastre_sqm` передається в create при різниці площ.
  - **backfill_real_estate_objects.py**: після обробки оголошень — перезаповнення всіх land_plot з cadastral_info.cadastral_number з кадастру (опція `--no-cadastral-backfill` для пропуску).
  - **data_dictionary.yaml**: поле `area_by_cadastre_sqm` для real_estate_objects.
  - **Mini App UI**: відображення «за кадастром: X м²» для land_plot з area_by_cadastre_sqm; CSS-підсвітка `.detail-reo-card-area-cadastre-diff`, `.detail-reo-cadastre-diff` (жовтий фон).

## 2026-02-22 — Виправлення: порівняння цін Київ vs Львів (продовження)

- **Запит**: Після попередніх змін відповідь все ще була поганою: «11 записів», «неможливо сформувати окремі середні для Києва та Львова», хоча даних по містах багато.

- **Причина**: (1) unified_listings fallback використовував ISO-рядок для source_updated_at замість datetime (BSON Date); (2) addresses.settlement має варіанти «Київ» / «м. Київ» — exact match пропускав частину даних; (3) не було фільтра property_type для «комерційна нерухомість»; (4) price_analytics fallback не фільтрував за типом оголошення.

- **Дії**:
  - **MultiAgentService._try_analytics_aggregation_by_city**: фільтр property_type="Комерційна нерухомість" при наявності «комерційн» у запиті — для ProZorro, OLX, price_analytics та unified_listings fallback.
  - **unified_listings fallback**: source_updated_at як datetime.fromisoformat(); settlement_values включають «Київ» та «м. Київ»; base_match з status="активне" та property_type при want_commercial.
  - **AnalyticsBuilder._build_olx_price_per_sqm_pipeline**: додано $match за detail.llm.property_type при наявності filters.property_type.
  - **price_analytics fallback**: get_aggregated_analytics з property_type="Комерційна нерухомість" при want_commercial.

## 2026-02-22 — Виправлення: порівняння цін Київ vs Львів

- **Запит**: Відповідь асистента на «Порівняй середню ціну за м² на комерційну нерухомість у Києві та Львові» була поганою: «1007 оголошень в Києві», «даних по Львову немає», «немає агрегованих показників». При цьому дані є по обох містах.

- **Причина**: (1) _try_analytics_aggregation_by_region спрацьовував лише на «по областям/регіонах», не на «порівняй Київ і Львів»; (2) execute_analytics для olx_listings підтримував лише groupBy: ["date"], не city/region; (3) не було явного шляху для порівняння по містах.

- **Дії**:
  - **MultiAgentService**: додано _try_analytics_aggregation_by_city — перехоплює запити «порівняй ціни в містах X та Y» (Київ, Львів, Харків, Одеса, Дніпро). Викликає execute_analytics з groupBy: ["city"], filters: $or для міст, діапазон last_30_days. ProZorro, потім OLX як fallback.
  - **AnalyticsBuilder**: для olx_listings дозволено groupBy: ["city"], ["region"] (крім ["date"]). _build_olx_price_per_sqm_pipeline — динамічне групування за groupBy; витягування city/region з detail.resolved_locations[0].address_structured.
  - **prompts.yaml**: додано інструкцію «Порівняння по містах (Київ vs Львів)» — groupBy: ["city"], filters: $or.
  - **Fallback на price_analytics**: якщо execute_analytics (ProZorro + OLX) повертає 0 — використовуємо PriceAnalyticsService.get_aggregated_analytics (передобчислені агрегати з unified_listings). Отримуємо monthly aggregates, фільтруємо по cities, для кожного міста беремо рядок з max count.

## 2026-02-22 — Виправлення: Tool use unsupported (Gemini + Google Search)

- **Запит**: Помилка `Tool use with function calling is unsupported by the model` при запиті до geo_assessment (приміщення під аптеку в Луцьку).

- **Причина**: langchain-google не підтримує сумісність `{"google_search": {}}` з function tools (StructuredTool). При їх сумісному використанні Gemini повертає INVALID_ARGUMENT. Див. https://github.com/langchain-ai/langchain-google/issues/1116

- **Дії**:
  - **LangChainAgentService**: Google Search grounding додається лише коли `len(tools_for_request) == 0`; на практиці агент завжди має tools, тому grounding не застосовується.
  - **Settings**: дефолт `llm_agent_google_search_grounding` змінено на `false`.
  - **config.example.yaml**: приклад `google_search_grounding: false` з поясненням несумісності.

## 2026-02-22 — Thinking mode та Google Search grounding для AI-асистента

- **Запит**: Увімкнути Thinking mode, Google Search grounding та відображення ходу думок агента в AI-асистенті. Пріоритет — інформація з бази. У інших випадках (парсинг тощо) Thinking mode не вмикати.

- **Дії**:
  - **Settings**: `llm_agent_thinking_budget` (8192 за замовч., 0 = вимкнено), `llm_agent_google_search_grounding`, `llm_agent_include_thoughts`.
  - **LangChainAgentService**: окремий `llm_assistant` з thinking_budget та include_thoughts для Gemini; базовий `llm` без thinking для summarize/fallback. Google Search — через `bind_tools([{"google_search": {}}])` при grounding.
  - **System prompt** (prompts.yaml): блок «ПРІОРИТЕТ ДЖЕРЕЛ ІНФОРМАЦІЇ» — пріоритет даних з БД над інтернетом.
  - **process_query**: параметр `thinking_callback`; витягування thinking-блоків з AIMessage та виклик callback.
  - **MultiAgentService**: проброс `thinking_callback` до LangChain (основний шлях та geo_assessment).
  - **chat-stream API**: `thinking_callback` накопичує thinking; подія `done` містить `thinking`.
  - **Mini App UI**: блок «Хід думок агента» (details/summary) під відповіддю бота; стилі `.chat-msg-thinking`.
  - **config.example.yaml**: приклади `thinking_budget`, `google_search_grounding`, `include_thoughts` в llm.agent.

## 2026-02-21 — Екранування regex у RealEstateObjectsRepository

- **Запит**: Помилка backfill ОНМ: `Regular expression is invalid: missing closing parenthesis` при обробці оголошень OLX (напр. з адресами типу «м. Київ (обласний центр)»).

- **Дії**:
  - **RealEstateObjectsRepository.find_building_by_address**: екранування `formatted_address`, `region`, `settlement` через `re.escape()` перед підстановкою в `$regex` — дужки та інші спецсимволи regex більше не ламають запит.

## 2026-02-21 — OLX: фільтр сміття, дефолти пошуку, очищення

- **Запит**: У завантаженні з OLX з’явились оголошення про кепки та губну помаду. Потрібно: 1) на сторінці пошуку лише комерційна нерухомість та земельні ділянки; 2) вичистити сміття з БД.

- **Дії**:
  - **UnifiedListingsService.sync_olx_listing**: не синхронізує оголошення з property_type="інше" у unified_listings; видаляє їх із unified, якщо вже є.
  - **_determine_property_type**: fallback за заголовком/описом, якщо LLM не повернув тип — інферує "Земельна ділянка" / "Комерційна нерухомість" за ключовими словами.
  - **Сторінка пошуку**: дефолтні фільтри — Джерело: OLX, Тип: «Комерційна нерухомість та ЗД»; при відкритті екрану пошуку фільтри синхронізуються з DOM і застосовуються.
  - **_PROPERTY_TYPE_MAP**: додано `kommercheska_ta_zd` — Нерухомість, Комерційна нерухомість, Земельна ділянка, Земельна ділянка з нерухомістю.
  - **UnifiedListingsRepository.delete_by_source_id**: метод для видалення за source+source_id; для OLX — fallback на канонічний URL.
  - **OlxListingsRepository.delete_by_url**: метод для видалення за URL.
  - **scripts/cleanup_olx_junk_listings.py**: скрипт видалення сміття за ключовими словами (кепк, помад, шапк, губна помада, бейсболк). Видалено 2 оголошення (кепка, губна помада).
  - **run_update.py**: зупинка при 0 результатів — (1) «знайшли 0 оголошень» / «ми знайшли 0 оголошень»; (2) «ми нічого не знайшли, тому підібрали рекламні» + regex N=0. Фраза «рекламні» є й на сторінках з результатами (Донецька): «знайшли 0 оголошень», «ми нічого не знайшли, тому підібрали рекламні». Окрема «ми нічого не знайшли» прибрано — вона з’являється й на сторінках з результатами (напр. у фільтрах).

## 2026-02-21 — Детальне логування викликів LLM та геосервісів

- **Запит**: Щось мало запитів до LLM. Почати детально логувати кожен виклик LLM та геосервісів.

- **Дії**:
  - **LoggingService**: `log_api_usage(service, source, from_cache, metadata)` — новий тип події `api_usage` для збереження кожного виклику.
  - **LogsRepository**: `count_api_usage_by_day`, `count_api_usage_total`, `count_api_usage_last_month` — агрегація по `event_type=api_usage`, `metadata.service`.
  - **GeocodingService**: логування кожного виклику `geocode()` — cache hit (from_cache=True) та API call (from_cache=False). Опційний параметр `caller` для джерела.
  - **LangChainAgentService**: логування перед кожним `llm.invoke` (summarize, main loop, fallback).
  - **LLMService**: логування в `parse_auction_description`, `generate_text`, `extract_intent_for_routing`.
  - **usage-stats**: переведено на `api_usage` — LLM та Geocoding рахуються з логів; додано user_queries, cache_hits.
  - **caller** у geocode: langchain_agent, unified_listings_olx, unified_listings_prozorro, olx_scraper.

## 2026-02-21 — Графіки використання LLM та Geocoding в адмін-панелі

- **Запит**: Створити на сторінці адміністрування графіки з використанням запитів LLM та геосервісів Google по днях, плюс загальна кількість, плюс кількість за останній місяць.

- **Дії**:
  - **LogsRepository**: `count_llm_queries_by_day(days)`, `count_llm_queries_total()`, `count_llm_queries_last_month()` — агрегація по `event_type=user_action`, `metadata.action=llm_query`.
  - **GeocodeCacheRepository**: `count_api_calls_by_day(days)`, `count_total()`, `count_last_month()` — кожен запис у кеші = один виклик Google Geocoding API (cache miss).
  - **Admin API**: GET `/api/admin/usage-stats?days=60` — повертає `llm` та `geocoding` з `by_day`, `total`, `last_month`.
  - **index.html**: блок «Використання API» з підсумком (всього / за місяць) та двома bar-графіками (Chart.js).
  - **app.js**: `loadAdminUsageStats()` — завантаження даних та `renderAdminUsageCharts()` для відображення.
  - **styles.css**: `.admin-usage-summary`, `.admin-usage-charts`, `.admin-chart-wrap` для оформлення.

## 2026-02-21 — OLX: паралельна обробка по областях

- **Запит**: Обробка скрапера OLX в окремих потоках — по одному на область. Використовувати connection повторно в рамках потоку.

- **Дії**:
  - **fetcher.py**: `fetch_page(session=None)` — опційний параметр для повторного використання HTTP-сеансу (keep-alive).
  - **config.py**: `MAX_PARALLEL_REGIONS` (за замовчуванням 25) — кількість потоків.
  - **run_update.py**: `ThreadPoolExecutor` — один потік на область; `_process_region` обробляє 2 категорії (нежитлова + земля) з одним `requests.Session`; thread-safe лог через `threading.Lock`. При явній передачі `categories` — legacy послідовний режим.

## 2026-02-21 — OLX: об'єднання дублікатів за канонічним URL

- **Запит**: Оголошення з search_reason=promoted та search_reason=organic — одне й те саме. Такі оголошення в базі слід поєднати і розглядати як одне.

- **Дії**:
  - **utils/olx_url.py**: `normalize_olx_listing_url(url)` — повертає URL без query-параметрів.
  - **olx_listings_repository**: зберігання та пошук за канонічним URL; `upsert_listing` нормалізує вхідний url; `find_by_url` приймає будь-який варіант (з query чи без).
  - **unified_listings_repository**: `find_by_source_id` для OLX — fallback на канонічний URL, якщо з query не знайдено.
  - **Міграція 038**: об'єднання дублікатів в olx_listings, оновлення source_id у unified_listings, listing_analytics, real_estate_objects.

## 2026-02-21 — OLX: пошук по областях, обмеження 25 сторінок

- **Запит**: Завантаження даних з OLX обмежуємо 25 сторінками пошуку (OLX перенаправляє на 25-ту при спробі більше). Для більшого обсягу — пошук послідовно по всім областям, max 25 сторінок на область, з зупинкою по даті (cutoff).

- **Дії**:
  - **config.py**: MAX_SEARCH_PAGES = 25 (за замовчуванням); додано `get_olx_region_slugs()` — завантаження з `config/olx_region_slugs.yaml`; `get_commercial_real_estate_list_url` та `get_land_list_url` приймають `region_slug` для фільтрації по області (URL: `.../{slug}/` — path-суфікс з короткими слагами з sitemap).
  - **config/olx_region_slugs.yaml**: маппінг областей → короткі OLX slugs з sitemap (Вінницька→vin, Київська→ko, Львівська→lv тощо); АР Крим та Севастополь пропущено.
  - **run_update.py**: `_build_region_categories()` — будує категорії як (тип × область): «Нежитлова нерухомість — Вінницька», «Земельні ділянки — Київська» тощо; кожен пошук max 25 сторінок; зупинка по cutoff_utc при days; при full — 25 сторінок без зупинки по даті.
  - Fallback: якщо olx_region_slugs.yaml відсутній — два пошуки без фільтра (як раніше).

## 2026-02-21 — LLM-аналітика оголошення: ціна за одиницю площі, місцезнаходження, оточення

- **Запит**: Текст «У Рівне за останній місяць зареєстровано 50 схожих оголошень. Середня ціна становить близько 6 919 930 грн. Для оцінки відносності ціни потрібно більше даних» — не інформативний. Порівнюємо ціни за одиницю площі (грн/м², грн/га), а не абсолютні. Потрібен детальний LLM-розбір: ціна, місцезнаходження, оточення. Аналітику формуємо при запиті через LLM-помічника або натисканні кнопки «Сформувати аналітику». Зберігаємо в прив'язці до оголошення.

- **Дії**:
  - **Текст аналітики**: Підкреслено порівняння за одиницю площі (грн/м², грн/га), а не абсолютні суми. Додано заклик «Сформувати аналітику» для детального розбору.
  - **ListingAnalyticsRepository** + колекція **listing_analytics**: збереження LLM-згенерованої аналітики за source+source_id.
  - **ListingAnalyticsService**: генерація через LLM (промпт listing_analytics у config/prompts.yaml) — 3 блоки: ціна за одиницю площі, місцезнаходження, оточення.
  - **API**: GET /api/analytics/listing, POST /api/analytics/listing/generate (body: source, source_id, force).
  - **Міграція 037**: колекція listing_analytics.
  - **UI**: кнопка «Сформувати аналітику» / «Оновити аналітику» у вкладці Аналітика; відображення збереженої LLM-аналітики в блоці «Детальний розбір».
  - **Інтеграція з LLM-помічником**: поки що тільки кнопка; збереження при запиті через чат — окремо.
  - **2026-02-21 (поправка)**: LLM тепер отримує ринковий контекст з бази (індикатор, агрегати, середня ціна за м²/га) — блок «Ринковий контекст» у промпті. Перша частина аналітики: пріоритет price_per_m2_uah над price_uah; прибрано надлишкові фрази; при price_uah — «середня абсолютна ціна (для об'єктів без площі)».

## 2026-02-21 — OLX: прибрано фільтр «дата не змінюється 5 сторінок поспіль»

- **Запит**: Прибрати фільтр, що відсікає пошук OLX, якщо дата не змінюється 5 сторінок поспіль. Шукати допоки не отримаємо потрібну дату (cutoff) або не закінчаться сторінки пошуку.

- **Дії**:
  - **run_update.py**: видалено логіку `last_date_stale_count` / `prev_last_date_key` — зупинка лише при cutoff (більше половини оголошень старші за граничну дату) або при порожній сторінці / max_pages.
  - **config.py**: видалено `STALE_DATE_CONSECUTIVE_PAGES` та змінну середовища `OLX_SCRAPER_STALE_DATE_PAGES`.

## 2026-02-21 — OLX: зупинка по даті — більше половини сторінки; пропуск sync при незмінних даних; нормалізація region для агрегатів; узгодження індикатора з агрегатами міста

- **Запит**: На сторінках пошуку OLX є «топові» (платні) оголошення, вставлені без урахування сортування. Потрібно поліпшити визначення граничної дати. Також — не запускати sync_olx_listing і process_listing (ОНМ), якщо дані з пошуку не змінились. Питання: чому по Одесі плашка показує «Нижче середньої (місто)», а вкладка Аналітика — «Немає достатніх даних»? Протиріччя в Смілі: плашка «Аномально висока (область)», а LLM-розбір правильно каже «нижча за середню в місті».

- **Причина розбіжності**: Плашка використовує get_indicator, який має нормалізацію варіантів області («Одеська область» / «Одеська»). Агрегати (вкладка Аналітика) використовували точний збіг group_region — при різному форматі запит не знаходив записів. Протиріччя плашка vs агрегати: при < 5 оголошень у місті індикатор береться з області, а середня ціна — з міста; розподіл по області може відрізнятись (напр. 8438 грн/м² нижче середньої в Смілі 12492, але «аномально висока» для області).

- **Дії**:
  - **run_update.py**: змінено логіку зупинки по cutoff_utc — зупиняємось лише якщо більше половини оголошень на сторінці старші за граничну дату (count_older_than_cutoff > len(listings) / 2). Раніше зупинка відбувалась при першому ж старому оголошенні, що призводило до передчасної зупинки через топові оголошення.
  - **run_update.py**: при пропуску завантаження деталей (search_data не змінився) більше не викликаємо sync_olx_listing — unified_listings і ОНМ не потребують оновлення, контент той самий.
  - **PriceAnalyticsRepository**: додано _region_variants() та нормалізацію region у get_aggregates — пошук за $in варіантів («Одеська», «Одеська область»), як у get_indicator.
  - **app.js**, **ListingAnalyticsService**: індикатор області — основа аналітики. Коли є дані міста (totalCount < 5) і вони суперечать індикатору області — додаємо пояснення: «Ми маємо небагато (N) даних із міста, але якщо орієнтуватись по них — ціна по цій місцевості нижче/вища за середню. Це може вказувати на переваги локації в межах області. З накопиченням масиву оголошень аналітика буде більш точна.» Без перевизначення індикатора.

## 2026-02-20 — Плашка ціни: settlement 5+, 3 розподіли, фільтрація викидів

- **Запит**: Плашка має дивитись на аналітику по найнижчому топоніму (місто, громада). Якщо 5+ оголошень схожого типу — розподіл по локації; інакше — область. Рахувати при відображенні. Виключати з розрахунку значення за межами Q1-2*IQR та Q4+2*IQR по країні. Три окремі розподіли: земля, нерухомість, змішані.

- **Дії**:
  - **PriceAnalyticsRepository**: додано `listing_type` (land, real_estate, mixed) до індикаторів; `get_indicator`, `get_region_indicator`, `upsert_indicator`, `upsert_region_indicator` приймають listing_type.
  - **PriceAnalyticsService**: `_compute_indicators` — три окремі пайплайни (land, real_estate, mixed); фільтрація викидів за країнськими квартилями (Q1-2*IQR, Q4+2*IQR); settlement — мінімум 5 оголошень, fallback — область; `_get_country_quartiles`, `_listing_type_match`, `_listing_type_from_item`.
  - **get_price_indicator**: settlement 5+ → локальний розподіл, інакше — область; приймає listing_type.
  - **get_price_indicators_for_items**: визначає listing_type з building_area_sqm/land_area_ha; не вимагає city (може бути порожнім для region-only).
  - **PriceAnomalyService**: `_get_local_distribution` приймає listing_type; `_get_listing_type` для визначення типу з doc.
  - **search.py**: `_build_item_for_price_indicator` — додано building_area_sqm, land_area_ha для OLX (detail.llm, price_metrics) та ProZorro (itemProps); `_normalize_unified_doc` — додано addresses для fallback.
  - **analytics API**: GET /api/analytics/price-indicator — параметр listing_type (land, real_estate, mixed, general).
  - **2026-02-20 (поправка)**: Додано LISTING_TYPE_GENERAL (земля+нерухомість) як fallback коли по типу замало даних; нормалізація назви області (з/без « область»).
  - **2026-02-20 (діагностика)**: Якщо є тільки mixed-індикатори — fallback на mixed для land/real_estate. scripts/diagnose_price_badges.py для діагностики.
  - **2026-02-20 (джерело плашки)**: get_price_indicator повертає (indicator, source); price_indicator_source = "city"|"region". Плашка показує «(місто)» або «(область)». Секція аналітики — оновлено опис (у місті/в області).

## 2026-02-20 — Номер будинку в адресах (ProZorro, OLX, ОНМ)

- **Запит**: У жодному об'єкті НМ не заповнена повна адреса до номера будинку, хоча часто є деталізація до вулиці.

- **Причини**:
  1. ProZorro: streetAddress містить вулицю та номер в одному полі ("вул. Соборності, 7") — не парсили окремо.
  2. Real estate objects: _extract_objects_from_prozorro_items не витягував building з streetAddress.
  3. Geocode fallback: коли Google не повертає street_number, втрачали building з LLM/джерела.
  4. GeocodingService: додано premise як fallback для street_number (Google іноді повертає premise).

- **Дії**:
  - **utils/address_parser.py**: parse_street_address, parse_prozorro_item_address — парсинг streetAddress на street + building (формати "вул. Хрещатик, 15", "вул. Соборності 7").
  - **RealEstateObjectsService**: використання parse_prozorro_item_address для items; fallback addr.building коли geocode не повертає street_number.
  - **UnifiedListingsService**: items fallback — parse_prozorro_item_address замість ручного парсингу; збереження building з парсера.
  - **GeocodingService**: ADDRESS_COMPONENT_MAP — premise як fallback для street_number.
  - **prompts.yaml**: посилення real_estate_objects_parsing — обов'язково витягувати building, якщо є в тексті.

## 2026-02-20 — Відображення зв'язків ОНМ: будівля↔ділянка, приміщення

- **Запит**: У об'єктах нерухомого майна не видно прямої прив'язки будівлі до земельної ділянки та приміщень.

- **Дії**:
  - **API** GET /api/search/real-estate-objects: розкриття зв'язків — для будівель додано `land_plots` (з land_plot_ids) та `premises` (з premises_ids); для приміщень додано `building` (з building_id).
  - **UI** вкладка «Об'єкти нерухомості»: для будівлі показуються блоки «Земельні ділянки» та «Приміщення»; для приміщення — блок «Будівля».
  - **Стилі**: detail-reo-card-related, detail-reo-related-list.

## 2026-02-20 — Прискорення обробки порожніх кадастрових тайлів

- **Запит**: Порожній тайл обробляється ~10 секунд — потрібно прискорити.

- **Причини**:
  1. Нова HTTP-сесія на кожен запит — немає повторного використання TCP/TLS з'єднання.
  2. При workers > 1 — затримка 1–3 с перед кожним запитом (навіть порожнім).
  3. Порожні відповіді (Content-Length: 0) — повне читання тіла не потрібне.

- **Дії**:
  - **Reuse HTTP session**: одна сесія при workers=1, окрема на воркер при workers>1. Зменшення накладних витрат на з'єднання.
  - **Early exit для Content-Length: 0**: stream=True, перевірка заголовка, повернення b"" без читання тіла.
  - **Менша затримка при workers>1**: для запитів після першого — 0.2–0.5 с замість 1–3 с (CADASTRAL_DELAY_SUBSEQUENT_MIN/MAX).
  - **CADASTRAL_TIMING=1**: діагностика — лог fetch_ms, total_ms, parcels для кожного тайлу.
  - **count_total_estimated()**: замість count_documents({}) на 16M+ документів — estimated_document_count() (O(1)) для прогрес-бару та _report_progress. Усунуто вузьке місце ~2–15 с на тайл.

## 2026-02-20 — Об'єкти нерухомого майна (ОНМ)

- **Запит**: Упорядкувати та уніфікувати обробку оголошень з кількома об'єктами (ділянки, будівлі, приміщення). Створити сутності ОНМ, витягувати їх з оголошень, зберігати посилання.

- **Уточнення (точність адреси)**: Будівлю без адреси або з неповною адресою створювати можна. Але по неповній адресі (напр. «м. Київ») не поєднуємо будівлі з різних оголошень. Групування тільки за: (1) точною адресою (вулиця + номер будинку), (2) кадастровим номером земельної ділянки — одна ділянка = одна будівля або комплекс.

- **Дії**:
  - **Колекція real_estate_objects**: єдина колекція з type (land_plot, building, premises). Поля: description, area_sqm, cadastral_info (для land_plot), address, building_info (для building), building_id, floor, premises_type (для premises).
  - **Міграції 035, 036**: колекція real_estate_objects з індексами; поле real_estate_object_refs у unified_listings.
  - **RealEstateObjectsRepository**: find_by_cadastral_number, find_building_by_address, find_by_ids, find_by_listing.
  - **RealEstateObjectsLLMExtractorService**: LLM-екстракція з промптом real_estate_objects_parsing, кеш reo_*.
  - **RealEstateObjectsService**: process_listing — витягує ОНМ, геокодує адреси, створює/оновлює записи, зливає кадастрову інформацію з cadastral_parcels. Групування: тільки за точною адресою (вулиця+номер) або за кадастровим номером земельної ділянки. find_building_by_land_plot_id для групування по ділянці.
  - **ProZorro — структурована екстракція**: _extract_objects_from_prozorro_items — витягує ОНМ безпосередньо з auction_data.items (кадастр, itemProps, address). LLM — fallback, якщо items не дають результатів.
  - **Інтеграція**: sync_olx_listing та sync_prozorro_auction викликають process_listing після upsert.
  - **Backfill**: scripts/backfill_real_estate_objects.py з опцією --clear-llm-cache.
  - **API** GET /api/search/real-estate-objects (source, source_id).
  - **UI**: related_objects_count у картках пошуку; вкладка «Об'єкти нерухомості» в деталях оголошення.

## 2026-02-19 — Прибрати логіку помітки неактивності OLX

- **Запит**: Прибери існуючу логіку помітки неактивності оголошень OLX, позначь все активним.

- **Дії**:
  - **run_update.py**: видалено блок позначення неактивних (оголошення в БД за період, але відсутні в пошуку).
  - **search.py**: прибрано фільтр `is_active: {$ne: False}` з _build_olx_filters — показуються всі оголошення OLX.
  - **unified_listings_service**: для OLX завжди status = "активне" (ігнорується is_active).
  - **Міграція 034**: olx_listings — is_active=True для всіх; unified_listings (source=olx) — status="активне".

## 2026-02-19 — Уніфікація відображення: сторінка оголошення та пошук

- **Запит**: По-різному виглядає на сторінці оголошення і на сторінці пошуку.

- **Дії**:
  - **Заголовок**: fixTitlePriceDisplay застосовано і на сторінці деталей (renderUnifiedDetail, renderDetail для OLX) — корекція "| $2" на коректну ціну в USD.
  - **Бейдж ціни**: однакова логіка: `badgeToShow = price_indicator || price_notes` на сторінці пошуку, сторінці unified-деталей та сторінці olx/prozorro-деталей. Пріоритет: price_indicator (якщо є аналітика), інакше price_notes.
  - **Відображення ціни на пошуку**: addPriceRow — бейдж показується біля основної метрики (price/m2/ha) залежно від типу оголошення.

## 2026-02-19 — Курс валют, аномалії за одиницю, змішані оголошення

- **Запит**: Коректна конвертація USD→UAH; аномалії тільки за ціною за одиницю (га/м²); змішані оголошення — окрема аналітика з ширшими межами.

- **Дії**:
  - **Курс**: sync_olx_listing приймає usd_rate_override; reformat передає курс. _ensure_usd_rate() для оновлення при потребі.
  - **Аномалії**: find_anomalous_listings використовує ТІЛЬКИ price_per_m2_uah або price_per_ha_uah (не price_uah).
  - **Змішані**: _is_mixed_listing (building_area_sqm>0 і land_area_ha>0). Виключені з загальної статистики (_compute_indicators, _compute_aggregates). Окремий розподіл _get_global_distribution_mixed з IQR_MULTIPLIER_MIXED=2.5.

## 2026-02-19 — Парсер ціни: 4.68 замість 2 000 000 (OLX)

- **Проблема**: Оголошення https://www.olx.ua/.../prodazh-promislovo-bazi-4-68-ga-volin-2-000-000-IDZUDDD.html показувало 4.68 $ замість 2 000 000 $.

- **Причини**:
  1. `_extract_price_value` повертав перше число — "4.68" (площа) замість "2 000 000" (ціна).
  2. `parse_detail_page` знаходив price у `<title>`, де є і площа, і ціна.
  3. Відсутній fallback з URL slug.

- **Дії**:
  - **`_extract_price_value`**: якщо кілька чисел — повертає найбільше з тих, що >= 100 (ціна зазвичай більша за площу).
  - **`parse_detail_page`**: спочатку шукає h2/h3/h4 з коротким текстом ціни; не бере price з `title`/`head`.
  - **`_extract_price_from_olx_url`**: fallback — витягує ціну з slug (патерн X-XXX-XXX перед -ID).
  - **`_reparse_price_from_text`**: приймає olx_url для fallback з URL.

## 2026-02-19 — Переформатування: re-fetch з OLX

- **Проблема**: Перерахунок не вирішував проблему — ціна залишалась некоректною.

- **Дії**:
  - **Re-fetch**: перед обробкою ListingReformatService завантажує сторінку оголошення з OLX, парсить її через parse_detail_page. Якщо отримано price_text/price_value — оновлює search_data.
  - **parse_detail_page**: додано витягування ціни з HTML (data-cy="ad-price", fallback на текст з грн/$/€). Повертає price_text, price_value, currency.

## 2026-02-19 — Переформатування: ціна, аномалії, price_notes

- **Проблема**: Після переформатування оголошення все одно показувало 2$ замість 2 млн; аномальна ціна позначалась як «Вигідна».

- **Дії**:
  - **ListingReformatService**: повторний парсинг ціни з `price_text`; fallback — витягування з опису/заголовка (патерни $X XXX XXX). Конвертація USD/ EUR в UAH перед compute_price_metrics.
  - **price_notes**: при виявленні аномалії після переформатування — `set_price_notes("Ціна потребує перевірки")`; якщо аномалії немає — очищення.
  - **API**: `price_notes` у `_normalize_unified_doc`; для olx/prozorro detail — додаємо price_notes з unified_listings.
  - **UI**: якщо `price_notes` встановлено — показуємо його замість індикатора «Вигідна»; бейдж «Ціна потребує перевірки» з класом `search-item-price-anomalous`.

## 2026-02-19 — Переформатування оголошень та обробка аномальних цін

- **Запит**: Кнопка «Переформатувати дані» для адмінів, повторна обробка через LLM/гео, контрольна перевірка. Обробка аномальних цін: глобально → по місцевості, LLM-верифікація, позначка «Ціна потребує перевірки».

- **Дії**:
  - **ListingReformatService**: повторний LLM-парсинг, геокодування, перерахунок метрик для OLX; для ProZorro — повторна синхронізація. Після переформатування — контрольна перевірка аномалій.
  - **API** POST /api/admin/reformat-listing (source, source_id).
  - **Кнопка** «Переформатувати дані» на формі оголошення/аукціону (тільки для адмінів).
  - **PriceAnomalyService**: виявлення аномалій — спочатку глобально (країна), потім по місцевості. Метод set_price_notes для позначки «Ціна потребує перевірки».
  - **API** POST /api/admin/process-anomalous-prices — ручний запуск пошуку аномальних цін.
  - **Адмін-меню**: блок «Обробка аномальних цін» з кнопкою «Знайти аномальні ціни».
  - **data_dictionary**: поле price_notes для unified_listings.
  - LLM-верифікація (ціна за одиницю, оренда, договірна) — залишена як TODO для подальшої реалізації.

## 2026-02-19 — OLX парсер ціни: роздільник тисяч (·)

- **Проблема**: Оголошення «2 000 000 $» зберігалось як 2 долари.

- **Причина**: OLX використовує middle dot (·) як роздільник тисяч. Regex `[\d\s.]+` давав окремі фрагменти ["2", "000", "000"], поверталось перше значення — 2.

- **Виправлення**: У `_extract_price_value` додано нормалізацію роздільників тисяч: `\u00a0` (nbsp), `\u202f` (narrow nbsp), `\u00b7` (·) → пробіл перед парсингом.

## 2026-02-19 — OLX скрапер: зупинка при однаковій даті, діагностика фільтрів

- **Проблема**: Скрапер заходив на дуже глибокі сторінки (155+), хоча дата останнього оголошення не змінювалась («18 лютого 2026 р.»).

- **Дії**:
  - **Евристика зупинки**: якщо дата останнього оголошення на сторінці однакова N сторінок поспіль (за замовчуванням 5) — зупиняємо. Конфіг `OLX_SCRAPER_STALE_DATE_PAGES`.
  - **Лог URL**: на першій сторінці виводимо повний URL пошуку для перевірки наявності `search[order]=created_at:desc`.
  - **Діагностика парсингу**: при cutoff (days) на першій сторінці логуємо, скільки оголошень мають `listed_at_iso` — для перевірки коректності парсингу дати.

## 2026-02-19 — Сортування за датою: джерело + БД

- **Запит**: Сортування за датою означає дату оновлення в джерелі (source_updated_at), а не в нашій базі. Якщо дата в джерелі однакова — сортуємо за датою оновлення в нашій базі (system_updated_at).

- **Дії**:
  - **search.py**: search_unified, export endpoints — при сортуванні за source_updated_at додано вторинне сортування за system_updated_at.
  - **report_templates.py**: той самий патерн для експорту звітів.
  - **pipeline_executor**: для unified_listings при сортуванні за source_updated_at (за замовчуванням або явно) додано вторинне сортування за system_updated_at.

## 2026-02-19 — OLX скрапер: Accept-Encoding br, сортування землі

- **Проблема**: Скрапер отримував 0 оголошень — HTML приходив як бінарний мусор.

- **Причина**: Заголовок Accept-Encoding: gzip, deflate, br. OLX повертав br (brotli). Без пакета brotli requests не декодує br, і response.text містить стиснуті байти.

- **Виправлення**: fetcher.py — прибрано "br" з Accept-Encoding. Сервер віддає gzip/deflate, requests коректно декодує. Парсер знову знаходить оголошення.

- **Інше**: config.py — get_land_list_url додано sort_newest (search[order]=created_at:desc) для узгодженості з комерційною нерухомістю.

## 2026-02-19 — OLX скрапер: затримка після завантаження, повне оновлення, неактивні оголошення

- **Запит**: OLX додав таймаут — спочатку 0 оголошень, потім підвантаження. Додати опції «Оновити OLX повністю», «Оновити ProZorro повністю», «Оновити дані за місяць». При оновленні за період — позначати неактивними оголошення, що є в БД, але відсутні в пошуку.

- **Дії**:
  - **OLX fetcher**: затримка після отримання сторінки (DELAY_AFTER_PAGE_LOAD), повторні спроби при 0 оголошень (RETRY_EMPTY_PAGE_COUNT). config.py: DELAY_AFTER_PAGE_LOAD, RETRY_EMPTY_PAGE_COUNT.
  - **OLX run_update**: параметр `full` — завантаження всіх сторінок пошуку без обмеження; `days=30` для місяця.
  - **Позначення неактивних**: при оновленні за період (days) — оголошення в БД за період, але відсутні в результатах пошуку, позначаються is_active=False. OlxListingsRepository: mark_inactive_by_urls, find_urls_in_period; upsert_listing приймає is_active.
  - **UnifiedListingsService**: статус OLX з is_active (активне/неактивне).
  - **Admin API**: POST /api/admin/data-update з mode=full_olx|full_prozorro, days=1|7|30.
  - **ProZorro full**: fetch_and_save_real_estate_auctions(full=True) — завантаження ~6 років історії, LLM тільки для активних (llm_only_for_active). _save_auctions_to_database: llm_only_for_active, _is_active_auction. Пакетна обробка по місяцях з паралелізмом (ThreadPoolExecutor, до 6 місяців одночасно).
  - **UI**: кнопки «Оновити за місяць», «Оновити OLX повністю», «Оновити ProZorro повністю».
  - **data_dictionary**: поле is_active для olx_listings.

## 2026-02-19 — Кадастрова мапа: індекс місцезнаходження та кластеризація

- **Запит**: Покращити роботу з кадастровою мапою: дослідити структуру кадастрового номера, створити індекс для пошуку за місцезнаходженням, кластеризувати ділянки за спільними кордонами та однаковим призначенням/формою власності.

- **Дії**:
  - **Парсер кадастрового номера** (`utils/cadastral_code_parser.py`): розбір формату НКЗ:НКК:НЗД, витягування КОАТУУ (перші 10 цифр), коду області (2 цифри), району, міста, зони, кварталу, ділянки.
  - **Маппінг КОАТУУ** (`config/koatuu_oblast_codes.yaml`): відповідність кодів областей (01–85) до назв (АР Крим, Вінницька, Київ тощо).
  - **Індекс місцезнаходження** (`cadastral_parcel_location_index`): колекція для пошуку ділянок за областю, районом, префіксом КОАТУУ. Міграція 032, репозиторій, CadastralLocationIndexService.
  - **Кластеризація** (`cadastral_parcel_clusters`): групування ділянок зі спільними кордонами (або в межах ~5 м), однаковим призначенням та формою власності. Shapely STRtree для пошуку сусідів, union-find для об'єднання. Міграція 033, CadastralClusteringService.
  - **Інтеграція**: run_scraper при збереженні ділянок індексує їх у location index.
  - **Скрипти**: `cadastral_build_location_index.py`, `cadastral_build_clusters.py`, `test_cadastral_mechanisms.py`.
  - **data_dictionary**: опис cadastral_parcel_location_index, cadastral_parcel_clusters.

## 2026-02-19 — Дизайн промптів: завдання → контекст → закріплення

- **Запит**: Внести зміну в усі промпти до LLM: на початку — завдання, потім контекст, далі закріплення завдання. Створити правило для дизайну промптів. Прогнати тести основних пайплайнів.

- **Дії**:
  - **Правило** `.cursor/rules/prompt-design.mdc`: трьохчастинна структура (завдання → контекст → закріплення), приклади, заборонені/дозволені патерни.
  - **prompts.yaml**: intent_detection, query_structure, parsing — перебудовано за новою структурою.
  - **Агенти**: IntentDetectorAgent, QueryStructureAgent, PipelineBuilderAgent — fallback-промпти оновлено.
  - **Сервіси**: ResultValidatorService, FinalAnswerRefinementService, LLMService (parsing, intent extraction), ReportTemplateService, AnalyticalReasoningService, AgentTestRunnerService — промпти оновлено.
  - Прогнано тести: `run_agent_test_queries.py --quick` та повний прогон для порівняння результатів.

## 2026-02-19 — Скрапер кадастру: підтримка MultiPolygon у MVT

- **Проблема**: Тайли з raw>0, features=1 повертали parsed=0. Діагностика: geometry type = MultiPolygon.

- **Причина**: Парсер очікував лише Polygon; MultiPolygon має іншу структуру coordinates (масив полігонів).

- **Дії**: `parser.py` — `_decode_geometry` тепер обробляє MultiPolygon: бере перший полігон, його кільця, конвертує в WGS84.

## 2026-02-19 — Скрапер кадастру: усунення сповільнення та rate limiting

- **Проблема**: Скрапер сильно сповільнився, зросла кількість "0 ділянок (тайл порожній або помилка завантаження)".

- **Причини**:
  1. **index_parcel у циклі** — для кожної ділянки (500–3000/тайл) викликався location_index_service.index_parcel (парсинг + DB upsert). Це створювало велике навантаження.
  2. **Rate limiting** — 10 workers без затримки між запитами могли отримувати 429 або порожні відповіді.

- **Дії**:
  - Видалено виклики index_parcel з run_scraper. Індекс будується окремо: `py scripts/cadastral_build_location_index.py`.
  - При workers > 1 — завжди delay_before перед кожним запитом (зменшення rate limiting).

## 2026-02-19 — Скрапер кадастру: збереження адреси ділянки

- **Запит**: Зберігати адресу ділянки (якщо вказана в кадастрі). Очистити завантажені дані для перезавантаження у новому форматі.

- **Дії**:
  - **parser.py**: екстракція `address` з MVT properties (address, full_address, addr, адреса).
  - **data_dictionary.yaml**: поле `address` для cadastral_parcels.
  - **scripts/clear_cadastral_for_reload.py**: скрипт очищення — видаляє cadastral_parcels, скидає cadastral_scraper_cells на pending.
  - **cadastral_scraper_research.md**: додано address до таблиці властивостей.

## 2026-02-18 — Скрапер кадастру: паралельна обробка в 5 потоків

- **Запит**: Запуск обробки в 5 потоків.

- **Дії**:
  - `run_scraper.py`: параметр `workers` (default 1), ThreadPoolExecutor при workers > 1.
  - Кожен воркер атомарно бере комірку через `get_next_pending_cell()`, обробляє, оновлює shared counters під lock.
  - `run_cadastral_from_project_root.py`, `main()`: `--workers 5` (default 5 для CLI).
  - Адмін-панель залишається з workers=1 (не передає параметр).

## 2026-02-18 — Скрапер кадастру: прогрес-бар з ETA

- **Запит**: Додати прогрес-бар з приблизним часом, що лишився.

- **Дії**:
  - `run_scraper.py`: tqdm-прогрес-бар (тільки для CLI, без progress_callback).
  - Формат: `{l_bar}{bar}| {n_fmt}/{total_fmt} [elapsed<remaining, rate]`, postfix: ділянок, за_сесію.
  - `_log_loop()` — лог через pbar.write щоб не ламати відображення бару.
  - total = max_cells або (cells_total - cells_done) при запуску без ліміту.

## 2026-02-18 — Скрапер кадастру: center-first порядок та zoom 12

- **Проблема**: Скрапер повертав 0 ділянок — перші тайли (NW кут України) порожні.

- **Причина**: Ітерація по cell_id (asc) починала з тайлів над морем/кордоном; тайли з даними (Київ) були в кінці черги.

- **Дії**:
  - **config.py**: DEFAULT_ZOOM 14→12 (менше тайлів, більша площа; Київський тайл z=12 містить дані), UKRAINE_CENTER_LAT/LON.
  - **grid_iterator.py**: sort_priority — відстань від центру України (Київ); менше = вищий пріоритет.
  - **CadastralScraperProgressRepository**: get_next_pending_cell сортує за sort_priority, cell_id; ensure_cells_exist зберігає sort_priority; індекс (status, sort_priority, cell_id).
  - **Міграція 031**: backfill sort_priority для існуючих комірок (з bbox).
  - **Admin API**: POST /api/admin/cadastral-scraper/reset-cells — очищення комірок для перезапуску з новою сіткою.
  - Перша комірка тепер 12_2395_1381 (Київ) — ~1MB даних.
  - **Діагностика 0 ділянок**: 609 комірок були оброблені раніше (порожні NW тайли), 4 зависли в processing. Зменшено поріг reset_stale до 5 хв, додано POST /cadastral-scraper/reset-stale та кнопку «Скинути завислі» в адмінці.
  - **Тест зберігає, робочий — ні**: MongoDBConnection — fork-safety: перевірка _init_pid; після fork (uvicorn workers) створюється нове підключення. Явна ініціалізація MongoDB у admin route перед запуском потоку скрапера.
  - **Ділянки не створюються**: MongoDB 2dsphere відхиляв полігони з самоперетином («Loop is not valid: Edges cross»). parser.py — Shapely make_valid() для виправлення невалідних полігонів; cadastral_parcels_repository — try/except на кожен upsert (пропуск при помилці). requirements.txt — shapely>=2.0.0.
  - **Скрипт/інтерфейс не збільшують кількість**: run_scraper — os.chdir(project_root) для однакового контексту; діагностика БД у логах та stats (db_info); scripts/run_cadastral_from_project_root.py — обгортка; GET /cadastral-scraper/db-info.

## 2026-02-18 — Дослідження kadastrova-karta.com: vector tiles

- **Запит**: Самостійно дослідити kadastrova-karta.com для визначення джерела даних.

- **Результати дослідження**:
  - Джерело: Mapbox Vector Tiles (MVT/PBF) — `https://kadastrova-karta.com/tiles/maps/kadastr/land_polygons/{z}/{x}/{y}.pbf`
  - Zoom 11–16 для детальних ділянок (land_polygons)
  - Властивості: cadnum, purpose_code, purpose, category, area (га), ownership
  - Playwright використано для перехоплення мережевих запитів

- **Оновлення скрапера**:
  - `grid_iterator.py` — перехід на тайлову сітку (z/x/y) замість bbox
  - `fetcher.py` — завантаження PBF тайлів
  - `parser.py` — парсинг MVT через `mapbox-vector-tile`, конвертація координат у WGS84
  - `config.py` — TILE_BASE_URL, LAND_POLYGONS_ZOOM
  - `requirements.txt` — додано mapbox-vector-tile
  - `docs/cadastral_scraper_research.md` — оновлено з результатами

## 2026-02-18 — Скрапер кадастрової карти kadastrova-karta.com

- **Запит**: Створити скрапер, що поступово викачує мапу кадастрового реєстру з kadastrova-karta.com по сітці, зберігає дані в БД, керується з адмін-панелі, запам'ятовує оброблені комірки та продовжує при перезапуску.

- **Дії**:
  - **Дослідження**: `docs/cadastral_scraper_research.md` — інструкція для ручного аналізу мережевих запитів сайту.
  - **Міграція 030**: колекції `cadastral_parcels`, `cadastral_scraper_cells` з індексами.
  - **Репозиторії**: `CadastralParcelsRepository`, `CadastralScraperProgressRepository` (bulk seed для сітки).
  - **Скрапер**: `scripts/cadastral_scraper/` — config, grid_iterator, fetcher, parser, run_scraper. Підтримка HTTP API (після дослідження) та GeoJSON.
  - **Адмін-панель**: `POST /api/admin/cadastral-scraper/start`, `GET status`, `GET stats`.
  - **data_dictionary**: опис колекцій cadastral_parcels, cadastral_scraper_cells.
  - Для роботи з реальними даними потрібно налаштувати `CADASTRAL_API_URL` після дослідження мережевих запитів kadastrova-karta.com.

## 2026-02-18 — Опис системи для клієнта

- **Запит**: Створити опис системи для клієнта без складних технічних термінів: що може система, переваги порівняно з OLX/ProZorro, потенційні напрямки розвитку.

- **Дії**: Створено `docs/client_system_description.md` — опис можливостей (пошук, чат з асистентом, звіти, експорт, аналітика), порівняльна таблиця з OLX/ProZorro, окремий блок перспективних напрямків (карта, додаткові джерела, поглиблене використання AI, збережені пошуки, розширена аналітика).

## 2026-02-17 — Модульна архітектура: платформа, конфігурація та дані

- **Запит**: Зробити конфігурацію зовнішньою (промпти, налаштування в bundle), експорт/імпорт конфігу та даних, версіонування, відстеження міграцій, перевірка цілісності.

- **Дії**:
  - **Фаза 1**: `config/prompts.yaml`, `config/bundle_metadata.yaml`, `config/config_loader.py` — ConfigLoader з `get_prompt()`, `get_parsing_template()`, `get_glossary()`, `get_bundle_metadata()`. Сервіси (langchain_agent_service, llm_service, intent_detector, query_structure, llm_agent_service) читають промпти з YAML з fallback.
  - **Фаза 2**: Колекція `_migration_history`, міграція 029 (створення + backfill 001–028). `run_migrations.py` — перевірка виконаних міграцій перед запуском, запис після успіху.
  - **Фаза 3**: `config/config_export_service.py` — `build_config_zip()`, `build_data_zip()`, `build_full_zip()`, `import_config_from_zip()`. API: `GET /api/admin/export/config`, `export/data`, `export/full`, `POST /api/admin/import/config`.
  - **Фаза 4**: `DataIntegrityService` — перевірка колекцій vs data_dictionary. `config/config_migration_runner.py` — міграції конфігу. `scripts/check_data_integrity.py` — CLI. Інтеграція в `main.py`.
  - **Фаза 5**: Розділ «Конфігурація та дані» в адмінці Mini App — кнопки експорту (конфіг, дані, усе), форма імпорту конфігу (ZIP), відображення версії конфігу, платформи та результату перевірки цілісності.

## 2026-02-17 — Аналітика текстом замість графіків

- **Запит**: Зробити викладки на вкладці аналітики текстом. Загальний опис положення ціни серед схожих об'єктів у місцевості, пояснення тегу відносності ціни (особливо аномальної). Прибрати графіки.

- **Дії**:
  - **search.py**: _build_item_for_price_indicator() — побудова item з olx/prozorro doc для get_price_indicators_for_items. Додано price_indicator до відповідей get_olx_item та get_prozorro_item.
  - **app.js**: renderAnalyticsTab переписано — текстовий опис (кількість оголошень, середня ціна, положення ціни за індикатором), окрема секція з поясненням тегу (вигідна, середня, дорога, аномально низька/висока). Видалено Chart.js та графіки.
  - **index.html**: видалено підключення chart.js та chartjs-plugin-datalabels.
  - **styles.css**: додано .detail-analytics-text, .detail-analytics-desc, .detail-analytics-tag-section, .detail-analytics-tag-title, .detail-analytics-tag-desc.

## 2026-02-17 — Усунення [object Object] у блоці «Параметри»

- **Проблема**: У секції «Параметри» на сторінці деталей оголошення відображався текст [object Object] замість читабельних значень.

- **Причина**: label або value в елементах detail.parameters могли бути об'єктами (наприклад, з полями uk_UA, en_US), при конкатенації з рядком давали [object Object].

- **Дії**: app.js — додано formatParameterItem() для безпечного перетворення label/value у рядок (підтримка примітивів та об'єктів з uk_UA, en_US, name, raw). У renderDetailField — фільтрація елементів масиву, що дають порожній або [object Object] результат; не додаємо порожні li.

## 2026-02-17 — Пошук як стартова вкладка застосунку

- **Запит**: Зробити вкладку «Пошук» стартовою замість AI-помічника.

- **Дії**: app.js — при авторизації викликається showSearch() замість show("screen-home"); початкове значення currentScreen змінено на "screen-search".

## 2026-02-17 — Прибрано аналітику використання з пошуку та сторінок оголошень

- **Запит**: Прибрати з пошуку і сторінок інформацію про аналітику використання.

- **Дії**: Видалено відображення usage_analysis з карток пошуку, сторінки деталей (renderDetail) та renderUnifiedDetail. Видалено batch lookup та додавання usage_analysis до API-відповідей get_olx_item, get_prozorro_item, get_unified_detail, пошуку. Залишено ендпоінт `/api/search/usage-analysis` для інших потреб.

## 2026-02-17 — Попередній аналіз використання об'єкта в обробнику LLM

- **Запит**: Додати етап попереднього аналізу використання об'єкта: існуюче використання, геоаналіз, можливі використання зі скорингом. Кешувати геоаналіз. Batch-скрипт для всіх оголошень.

- **Дії**:
  - **PropertyUsageAnalysisService**: виявлення існуючого використання (магазин, склад, виробництво, аптека тощо) з опису/тегів; геоаналіз (pharmacy, supermarket, bus_station, mall поруч); скоринг можливих використань за площею та POI.
  - **PropertyUsageAnalysisRepository** + колекція **property_usage_analysis**: збереження аналізу за source+source_id.
  - **MultiAgentService**: при listing_context з detail_source+detail_id — виклик get_or_create_analysis(), додавання до effective_context та listing_context._usage_analysis.
  - **LangChainAgentService**: при listing_context — додавання format_analysis_for_llm() до listing_hint.
  - **Міграція 028**: колекція property_usage_analysis.
  - **scripts/run_property_usage_analysis_batch.py**: batch-аналіз для unified_listings (--limit, --source olx|prozorro|all, --force).

## 2026-02-17 — Помічник не видав відповідь (geo_assessment, max iterations)

- **Проблема**: Запит «5 посилань на приміщення в Луцьку під аптеку» — помічник досяг 10 ітерацій без відповіді.

- **Причини**: 1) geocode_address повертав результат без поля success — LLM вважав виклик невдалим; 2) при max iterations фінальний виклик з bind_tools давав tool_calls замість тексту; 3) execute_query з projection як dict замість list — помилка валідації.

- **Дії**:
  - **langchain_agent_service.py**: `_geocode_address` — додано success=True коли results не порожні. При max iterations — виклик `self.llm.invoke()` БЕЗ tools + HumanMessage-підказка «дай фінальну відповідь текстом». Нормалізація projection: dict → list полів (ключі з value=1).

## 2026-02-17 — Фідбек: посилання не працюють, задубльовані (user_id=390636278)

- **Фідбек**: Частина посилань у відповіді не працює. Посилання задубльовані.

- **Аналіз**: LLM повертав markdown `[Відкрити в застосунку](https://...)`. link_formatter замінював лише URL, залишаючи `[текст](<a>...</a>)` — з’являлися два посилання. URL з query-параметрами (`?search_reason=...`) могли не знаходитися в БД.

- **Дії**:
  - **link_formatter.py**: `_collapse_markdown_links()` — заміна `[текст](url)` на сам URL перед форматуванням; `_MARKDOWN_LINK_PATTERN` для розпізнавання. Застосовано в `format_message_links_for_mini_app` та `format_message_links_for_telegram`.
  - **langchain_agent_service.py**: правила в system prompt — не використовувати markdown для посилань; не дублювати URL.
  - **olx_listings_repository.py**: `_olx_url_variants` — додано варіант без query-параметрів (`?`).
  - **scripts/check_feedback.py**: підтримка пошуку за часом (date_str з пробілом).

## 2026-02-17 — Усунути 404 при пошуку в нашій базі (OLX)

- **Запит**: Якщо шукаємо серед своєї бази — оголошення не повинно бути відсутнім навіть теоретично.

- **Дії**:
  - **olx_listings_repository.py**: `find_by_url` — пошук за варіантами URL (http/https, www.olx.ua/olx.ua, trailing slash) через `_olx_url_variants`. `get_by_ids` — для URL-ів додано варіанти до пошуку.
  - **search.py**: `get_olx_item` — fallback на `unified_listings` при відсутності в `olx_listings`; `_unified_olx_to_olx_like` — конвертація unified doc у olx-подібний формат для renderDetail.

## 2026-02-17 — AI-помічник: команда «що ти вмієш», кнопки швидких дій, внутрішні посилання

- **Запит**: Обробка «Розкажи, що ти вмієш» з кнопками тест-кейсів; кнопки при уточненні/варіантах; покращення вигляду відповідей з оголошеннями (внутрішні посилання, охайне оформлення).

- **Дії**:
  - **app.js**: Команда «що ти вмієш» — локальна відповідь з описом можливостей та кнопками (пошук приміщення, звіт у файл, ціни по областях, оголошення OLX). `sendChatMessageWithText()` — відправка запиту з коду. Підтримка `quick_actions` у повідомленнях — кнопки під відповіддю.
  - **llm.py**: `_infer_quick_actions_from_response()` — визначення кнопок за текстом відповіді (показати тут, вивантажити в Excel). У події `done` додано `quick_actions`.
  - **link_formatter.py**: ProZorro/OLX URL → внутрішні посилання з `data-source`, `data-source-id`; клік відкриває оголошення в застосунку (`showDetail`).
  - **styles.css**: Стилі для `.chat-quick-actions`, `.chat-quick-action-btn`, `.chat-link-internal`.

## 2026-02-17 — Відображення тегів та поверху на сторінках пошуку та деталей

- **Запит**: Вивести на сторінку пошуку та сторінки оголошень теги, що отримуються при парсингу. В першу чергу — поверх.

- **Дії**:
  - **search.py**: `_normalize_unified_doc` — додано `floor`, `tags` у відповідь з unified_listings. `_normalize_olx_doc` — додано `floor`, `tags` з `detail.llm`. `_normalize_prozorro_doc` — додано `floor`, `tags` з `auction_data`.
  - **UnifiedListingsService**: при конвертації OLX → unified додано `floor`, `tags` з `detail.llm`; при конвертації ProZorro → unified додано `floor`, `tags` з `auction_data`.
  - **app.js**: на сторінці пошуку — відображення «Поверх» та «Теги» після площі/землі. На сторінці деталей — поверх у «швидких фактах» (key info) першим; у вкладці «Характеристики» — додано поля «Поверх» та «Теги» для OLX і ProZorro.
  - **styles.css**: додано класи `.search-item-floor`, `.search-item-tags` до стилів карток пошуку.

## 2026-02-16 — Парсинг блоку ad-parameters-container (OLX)

- **Запит**: В парсері даних OLX необхідно отримувати також блок характеристик з `data-testid="ad-parameters-container"` — там багато корисних даних (відстань до міста, тип нерухомості, кадастровий номер, площа ділянки, комунікації, інфраструктура).
- **Дії**: У `scripts/olx_scraper/parser.py` в `parse_detail_page` додано парсинг контейнера `[data-testid="ad-parameters-container"]`. Текст з дочірніх `<p>` елементів розбирається як пари «лейбл: значення» та об'єднується з існуючими параметрами з `data-cy="ad_parameters"`. При дублікаті лейбла зберігається значення з більшою довжиною (більш повне).

## 2026-02-15 — Покращення якості відповідей (посилання, переліки)

- **Фідбек**: user_id=171554829, 20:24 — відповідь «2 оголошення» з одним посиланням; друге — плейсхолдер замість URL.
- **Дії**: Додано в system prompt правила для посилань та переліків: надавати лише те, що є в даних; не обіцяти N, якщо їх менше; не писати плейсхолдери типу «[Посилання на друге...]»; формат: номер, опис, URL.

## 2026-02-15 — Виправлення формату відповіді LLM та LogicalLayerViolation для floor

- **Проблема 1**: Помічник повертав відповідь у сирому форматі `{'type': 'text', 'text': "...", 'extras': {...}}` замість тексту.
- **Проблема 2**: LogicalLayerViolation — Unknown logical field: 'floor' при запитах на приміщення для аптеки.
- **Дії**:
  - **LangChainAgentService**: при content у вигляді списку блоків (Gemini) — витягування тексту з елементів `{type:'text', text:'...'}` замість str(item).
  - **domain/validators.py**: додано `floor` до ALLOWED_LOGICAL_FILTER_KEYS.
  - **SourceFieldMapper**: маппінг floor → detail.llm.floor (olx_listings), floor (unified_listings).

## 2026-02-15 — Логування бесіди при дизлайку та перегляд в адмін-панелі

- **Запит**: При «пальці вниз» на відповідь LLM — логувати бесіду повністю та дати можливість продивитися її в панелі адміністратора.
- **Дії**:
  - **FeedbackRequest**: додано chat_id для завантаження бесіди з ChatSessionRepository.
  - **feedback.py**: при dislike і наявності chat_id — завантаження повної бесіди (messages) з ChatSessionRepository та збереження в feedback.
  - **FeedbackRepository.save_feedback**: додано параметр conversation (список {role, content}).
  - **Frontend submitFeedback**: передача chat_id з getCurrentChat().id.
  - **Admin API**: GET /api/admin/feedback/dislikes?limit=50&days=14 — список дизлайків з conversation.
  - **Admin UI**: блок «Фідбек (дизлайки)» з розгортанням — список з можливістю переглянути запит, відповідь, діагностику та повну бесіду.

## 2026-02-15 — Пам'ять контексту оголошення та прікріплене посилання в чаті

- **Проблема**: LLM-помічник втрачав контекст — відповідав про інше оголошення; не було видно, про який об'єкт йде розмова.
- **Дії**:
  - **ChatSessionRepository**: `get_listing_context`, `set_listing_context` — збереження контексту оголошення в service_data чату.
  - **MultiAgentService**: при отриманні listing_context — збереження в сесію; при відсутності в запиті — відновлення з сесії; `_get_context_summary` додає блок «ВАЖЛИВО: Розмова ведеться про КОНКРЕТНЕ оголошення» для IntentDetector та інших агентів.
  - **LangChainAgentService**: посилений SystemMessage при listing_context — «Усі відповіді мають стосуватися САМЕ цього об'єкта».
  - **Frontend**: `buildDetailContext` повертає `detail_source`, `detail_id` для навігації в застосунку; `openChatWithListingContext` зберігає їх у listingContext.
  - **UI**: прікріплена панель «Обговорюємо: [посилання]» над повідомленнями чату; клік відкриває оголошення в застосунку (showDetail) або в браузері.

## 2026-02-15 — Реалізація geo_assessment (гео-аналіз придатності приміщення)

- **Запит**: Реалізувати оцінку придатності приміщення для виду діяльності (аптека, кафе, клініка) з використанням Google Places API.
- **Дії**:
  - **PlacesService**: пошук місць поблизу координат (search_nearby) через Places API (New).
  - **LangChainAgentService**: tools geocode_address, search_nearby_places; маршрут geo_assessment з обмеженим набором інструментів; завантаження business_profiles.yaml у контекст при route=geo_assessment.
  - **IntentDetectorAgent**: response_format geo_assessment для запитів «чи підходить для аптеки/кафе», «оціни для відкриття магазину»; додано geo_assessment до JSON-прикладу в промпті.
  - **MultiAgentService**: маршрутизація при response_format=geo_assessment → LangChain process_query(route="geo_assessment"); stream_callback передається в _process_query_new_flow.
  - **config/business_profiles.yaml**: профілі pharmacy, cafe, clinic з poi_criteria (типи POI, радіуси, ролі).

## 2026-02-15 — Пропозиція: агент-осінтер для гео-аналізу

- **Запит**: Надати LLM інструменти для розширеного гео-аналізу — оцінка придатності приміщення для виду діяльності (аптека, кафе тощо) на основі оголошення, розташування, POI навколо (Google Places API).
- **Дії**: Створено пропозицію `docs/feature_proposal_geo_assessor.md` — архітектура AssessorAgent, PlacesService, MCP tools, профілі видів діяльності, етапи реалізації. Додано посилання в `feature_proposals_ux.md`, терміни в глосарій.

## 2026-02-15 — Виправлення URL для API procedure.prozorro.sale

- **Проблема**: Масові 422 при оновленні аукціонів. API procedure.prozorro.sale/api/procedures/{id} очікує **_id** (MongoDB ObjectId, 24 hex), а не **auctionId** (LSE001-UA-...).
- **Дії**:
  - **get_auction_details(auction_id, proc_id=None)**: додано параметр proc_id; при наявності proc_id використовується він для запиту.
  - **_fast_update_active_auctions**, **_update_existing_auctions_in_database**: витягування proc_id з auction_data._id; передача в get_auction_details.

## 2026-02-15 — Обробка 422 при оновленні з ProZorro

- **Проблема**: Масові помилки «422 Unprocessable Entity» при оновленні аукціонів. Кожна помилка виводилась окремо.
- **Дії**:
  - **get_auction_details**: явний raise HTTPError для 422 (як для 404).
  - **_fast_update_active_auctions**, **_update_existing_auctions_in_database**: обробка 422 окремо — лічильник unprocessable_count, без виводу кожної помилки.
  - **Статистика**: додано рядок «Недоступні в API (422): N».

## 2026-02-15 — Якісніша обробка запитів про конкретне оголошення

- **Проблема**: Запит «Проаналізуй це оголошення» повертав 0 результатів, хоча дані в базі є. QueryStructureAgent додавав property_type: ['Комерційна нерухомість', 'Земельна ділянка з нерухомістю'], що виключало оголошення з типом «інше». PipelineExecutor падав на iloc[0] при порожньому результаті агрегації.
- **Дії**:
  - **PipelineExecutor**: перевірка `not result.empty` перед `iloc[0]` у _apply_aggregate_step.
  - **QueryStructureAgent**: приймає listing_context; при аналізі конкретного оголошення не додає й не зберігає filter_metrics.property_type (щоб не виключити оголошення з типом «інше»).
  - **MultiAgentService**: передає effective_context та listing_context у QueryStructureAgent.analyze_query_structure.
  - **Промпт**: інструкція «НЕ додавай filter_metrics.property_type» при listing_context.
  - **Регіон/місто**: витягування з listing_context.summary, якщо немає в user_query.

## 2026-02-15 — Предзапит з контекстом оголошення замість заголовка в LLM

- **Запит**: Замість передачі повного заголовка оголошення в текст запиту LLM — формувати предзапит з контекстом (посилання + короткий опис без заголовка).
- **Дії**:
  - **API**: ChatRequest.listing_context — { page_url, summary }; передається з Mini App при чаті, відкритому з деталей оголошення.
  - **Frontend**: `buildDetailContext` замість `buildDetailContextSummary` — повертає { page_url, summary } без заголовка; `openChatWithListingContext` зберігає listingContext у чаті; поле вводу — «Проаналізуй це оголошення (див. посилання вище).»; `sendChatMessage` передає listing_context у body.
  - **MultiAgentService**: listing_context додається до effective_context для IntentDetectorAgent (посилання + короткий опис).
  - **LangChainAgentService**: SystemMessage з контекстом оголошення (посилання, summary) при listing_context.

## 2026-02-15 — Підтримка оператора eq у PipelineExecutor

- **Проблема**: Запит «Проаналізуй це оголошення» з area та price у filter_metrics — ResultValidatorService: «Втрата фільтрів: очікувалось 6, застосовано 5». Пайплайн виконувався успішно (1 результат), але валідатор блокував відповідь.
- **Причина**: `_build_filter_group` у pipeline_executor не обробляв формат `{eq: value}` — фільтри area та price ігнорувались.
- **Дії**: Додано гілку `elif "eq" in value` у domain/services/pipeline_executor.py для створення FilterElement з оператором EQ.

## 2026-02-15 — Контекст діалогу та службові дані для LangChain

- **Запит**: LangChain має адекватно працювати з контекстом кожного діалогу; кожен діалог — підтримувана бесіда, а не відповідь на одне питання; зберігати службові дані (пайплайни, вибірки).
- **Дії**:
  - **ChatSessionRepository**: нова колекція `chat_sessions` для збереження історії повідомлень та службових даних (temp_collection_ids) на пару (user_id, chat_id).
  - **Міграція 027**: індекси (user_id, chat_id) unique, TTL 30 днів на updated_at.
  - **API**: ChatRequest.chat_id — Mini App передає ідентифікатор чату з кожним запитом.
  - **MultiAgentService**: process_query приймає chat_id; _get_context_summary завантажує контекст з ChatSessionRepository (останні вибірки для «експортуй це»); _process_query_new_flow зберігає відповідь у чат-сесію.
  - **LangChainAgentService**: memory key = (user_id, chat_id) при chat_id; завантаження історії з ChatSessionRepository; збереження повідомлень після відповіді; append_temp_collection при save_query_to_temp_collection.
  - **Frontend**: відправка chat_id у body запиту до /api/llm/chat-stream.

## 2026-02-15 — Кнопка «Спитати у AI-помічника» на сторінці деталей

- **Запит**: На сторінці деталей оголошення/аукціону додати кнопку для переходу в чат з AI-помічником, щоб користувач міг запитати аналітику цін, оцінку розташування тощо.
- **Дії**:
  - **Кнопка**: «Спитати у AI-помічника» на сторінці деталей (renderDetail для olx/prozorro, renderUnifiedDetail для unified).
  - **Поведінка**: при натисканні створюється новий чат, відкривається екран AI-асистента, поле вводу попередньо заповнюється контекстом оголошення (назва, ціна, локація, джерело).
  - **app.js**: `openChatWithListingContext()`, `buildDetailContextSummary()`, `currentDetailContext`; кнопка в detail-cta-wrap.
  - **styles.css**: `.detail-ai-btn`, `.detail-cta-wrap` з flex-column та gap.

## 2026-02-15 — Приховування аналітики та індикатора ціни

- **Запит**: Сховати аналітику зі сторінок оголошень/аукціонів та позначення вигідності ціни в пошуку — недостатньо даних. Аналітика має рахуватися як рахувалася, просто тимчасово прибираємо з інтерфейсу.
- **Дії**:
  - **SHOW_ANALYTICS_UI = false**: флаг у app.js — при true показує вкладку Аналітика та індикатор ціни. Бекенд не змінювався — аналітика рахується, API повертає price_indicator.
  - **Вкладка Аналітика**: додається до TAB_FIELD_CONFIG лише при SHOW_ANALYTICS_UI.
  - **Індикатор ціни**: бейджі в картках пошуку та unified detail — лише при SHOW_ANALYTICS_UI.

## 2026-02-15 — Переробка сторінок деталей (ProZorro, OLX)

- **Запит**: Максимально user-friendly інтерфейс з корисною інформацією.
- **Дії**:
  - **Hero-блок**: фото (edge-to-edge), назва, ціна (великим шрифтом), статус (бейдж для ProZorro), локація.
  - **Швидкі факти**: сітка 2×3 карток з ключовою інформацією — ціна за м²/га, дати торгів, організатор (ProZorro); площа, ціна за м²/га, дата публікації (OLX); кількість заявок (ProZorro).
  - **CTA-кнопка**: «Відкрити на ProZorro.Sale» / «Відкрити на OLX» — помітна, на весь ряд.
  - **Вкладки**: «Опис» (текст, контакти, посилання), «Характеристики», «Учасники»/«Предмети» (ProZorro), «Аналітика».
  - **Стилі**: detail-hero, detail-key-info, detail-key-card, detail-cta-btn; статуси з кольорами (активний — зелений, завершений — зелений, скасований — червоний).

## 2026-02-15 — Покращення графіків аналітики (Chart.js)

- **Запит**: Аналітика виглядає убого; потрібні гарні графіки.
- **Дії**:
  - **Chart.js 4.4**: підключено CDN для інтерактивних графіків.
  - **renderAnalyticsTab**: замість CSS-барів — горизонтальний bar chart з Chart.js; градієнтна інтенсивність за значенням; підтримка світлої/темної теми; tooltip з кількістю оголошень.
  - **Метрика**: автоматичний вибір (price_per_m2_uah / price_per_ha_uah / price_uah) за кількістю рядків з даними; одиниця в заголовку (грн/м², грн/га, грн).
  - **Очищення**: destroy() при переході з деталі, щоб уникнути витоку пам'яті.

## 2026-02-15 — Вкладки на формах деталей аукціону та оголошення OLX

- **Запит**: Додати вкладки на форму аукціону ProZorro та покращити сторінку OLX аналогічно.
- **Дії**:
  - **ProZorro**: вкладки Основне (опис, ціна, дати, організатор), Характеристики (площі, ціни за одиницю), Учасники (заявки, мін. кількість), Предмети аукціону, Аналітика.
  - **OLX**: вкладки Основне (назва, ціна, локація, опис, контакти), Характеристики (площа, ціни за м²/га, параметри), Аналітика.
  - **Аналітика**: графік розподілу цін по місту/області (бар-чарт на CSS), дані з /api/analytics/aggregates.
  - **app.js**: TAB_FIELD_CONFIG, renderDetail з табами, renderAnalyticsTab, renderDetailField.
  - **styles.css**: .detail-tabs, .detail-tab-btn, .detail-tab-panel, .detail-analytics-bar-wrap.

## 2026-02-15 — Виправлення посилань ProZorro.Sale (невалідні URL)

- **Запит**: ЛЛМ-помічник видає невалідні посилання для топ-10 аукціонів за кількістю учасників.
- **Причина**: API procedure.prozorro.sale повертає `_id` (MongoDB ObjectId) та `auctionId` (формат LSE001-UA-...). Сайт prozorro.sale використовує auctionId. extract_auction_id повертав _id, тому зберігався невалідний для URL ідентифікатор.
- **Дії**:
  - **utils/hash_utils.py**: extract_auction_id — пріоритет auctionId > id > _id (раніше _id був перед auctionId).
  - **AnswerComposerService._extract_url_from_row**: прибрано fallback на _id; додано перевірку auction_data.auctionId.
  - **Міграція 026**: scripts/migrations/026_prozorro_auction_id_fix.py — оновлення auction_id в prozorro_auctions та source_id в unified_listings для існуючих записів.

## 2026-02-15 — Покращення промптів для витягування площі нерухомості (OLX, ProZorro)

- **Запит**: Площа нерухомості погано вичисляється в деяких випадках, хоча вона є в описі.
- **Дії**:
  - **LLMService._create_parsing_prompt**: розширено інструкції для building_area_sqm та land_area_ha.
  - building_area_sqm: пошук у всьому тексті (опис, параметри, заголовок); додано фрази "житлова площа", "корисна площа", "площа квартири", "площа будинку", варіанти написання (м², м2, кв.м, кв м); явне розрізнення з площею землі.
  - land_area_ha: фрази "площа ділянки", "площа землі", "соток землі"; чітке розділення building vs land.
  - У блоці ВАЖЛИВО: правило не плутати building_area_sqm з land_area_ha; інструкція шукати площу в параметрах ("Площа: 65") та заголовку ("3-к.кв. 65 м²").

## 2026-02-15 — Київ у фільтрах пошуку та обрізання dropdown

- **Запит**: На вкладці пошуку неможливо вибрати Київ (не є частиною області); випадаючий список обрізається рамкою фільтрів.
- **Дії**:
  - **Київ та Севастополь**: міста зі спеціальним статусом — додано обробку в API `/unified/filters/cities`: при region="Київ" повертається ["Київ"]; при завантаженні всіх міст додаються до списку.
  - **Фільтр пошуку**: при region="Київ" фільтруємо за `addresses.settlement`, а не за `region` (щоб не збігатися з "Київська").
  - **UI**: дозволено вибір міста без області (завантаження всіх міст); лейбл "Область" → "Регіон", placeholder "Область або м. Київ...".
  - **Dropdown**: `overflow: visible` для `.search-filters:not(.collapsed)`; `max-height: 280px`, `z-index: 1000` для `.filter-dropdown`.

## 2026-02-15 — Зведена аналітика цін

- **Запит**: Зведена аналітика з груповими метриками (джерело, тип, область, місто), ресурсними метриками (ціна за м²/га, середня ціна UAH/USD), агрегацією за днем/тижнем/місяцем, нормальним розподілом (квартилі), індикатором ціни на пошуку та сторінці оголошення (вигідна/середня/дорога по місту за останній місяць).
- **Дії**:
  - **Міграція 025**: колекція `price_analytics` для агрегованих метрик та індикаторів.
  - **PriceAnalyticsRepository**: зберігання агрегатів (period_type, period_key, group_by, metrics з avg/std/q1–q4) та індикаторів (квартилі по місту за 30 днів).
  - **PriceAnalyticsService**: `rebuild_all()` — перерахунок після оновлення даних; `_compute_indicators()` — квартилі по містах; `_compute_aggregates()` — агрегація за днем/тижнем/місяцем; `get_price_indicator()`, `get_price_indicators_for_items()`.
  - **Інтеграція**: Scheduler та admin data-update викликають `rebuild_all()` після оновлення ProZorro/OLX.
  - **API**: GET `/api/analytics/price-indicator`, GET `/api/analytics/aggregates`, POST `/api/analytics/rebuild` (admin); GET `/api/search/unified-detail` для деталей з індикатором.
  - **UI**: індикатор ціни (бейдж вигідна/середня/дорога/аномально низька/аномально висока) на картках пошуку та сторінці оголошення; кнопка «Перерахувати аналітику» в адмінці.
  - **Аномальні ціни**: індикація за межами типових меж (IQR×1.5): аномально низька — синій бейдж, аномально висока — помаранчевий.
  - **Fallback на область**: якщо по місту < 3 оголошень за період — використовується метрика по області для вирахування вигідності.

## 2026-02-15 — План пропозицій нових фішок для UX

- **Запит**: Створити план з пропозиціями щодо нових цікавих фішок, що покращать користувацький досвід.
- **Дії**:
  - Створено документ `docs/feature_proposals_ux.md` з пропозиціями, згрупованими за пріоритетом.
  - Високий пріоритет: візуалізація графіків, обране (watchlist), розширений фідбек, збережені пошуки.
  - Середній: алерти на нові оголошення, порівняння оголошень, експорт у PDF, inline-кнопки посилань.
  - Довгострокові: інтерактивна карта, рекомендації, спільний доступ, голосовий ввід.
  - Додатково: таблиця з рекомендованим порядком впровадження.

## 2026-02-13 — Виправлення property_type для ProZorro в unified_listings

- **Запит**: У зведеній таблиці записи з ProZorro мали тип оголошення «Інше» замість коректного (нерухомість, земля тощо).
- **Причина**: `_determine_property_type` шукав рядки "land"/"building" у `classification.id`, але CPV-коди мають формат "06100000-6", "04200000-0" — без цих слів.
- **Дії**:
  - **UnifiedListingsService._determine_property_type**: для prozorro використовувати префікси CPV (06=земля, 04=нерухомість, 05=комплекс), itemProps.itemPropsType та одиниці виміру (га, м²).
  - **scripts/migrations/024_prozorro_property_type_fix.py**: міграція для оновлення існуючих записів ProZorro у unified_listings.

## 2026-02-13 — Телефони продавця OLX (без Playwright)

- **Запит**: На сторінці оголошення OLX в секції контактів є кнопка «показати» для отримання телефонів. Спочатку використовувався Playwright, але телефони вже присутні в HTML відповіді.
- **Дії**:
  - **parser.py**: витягування телефонів з HTML — посилання `a[href^="tel:"]`, regex-патерни (0XX XXX XX XX, 0XXXXXXXXX, +38...). Функція `_format_phone()` для нормалізації. Телефони в `detail.contact.phones`.
  - **run_update.py**: звичайний `fetch_page` + `parse_detail_page` без Playwright.
  - Видалено **phone_reveal.py**, `USE_PLAYWRIGHT_FOR_PHONES`, залежність `playwright` з requirements.txt.

## 2026-02-13 — Виправлення роботи з адресами (OLX)

- **Запит**: Погана робота з адресами — Іванівка Житомирська відображалась як Тернопільська; оголошення з Рівненської віднесено до Житомирської.
- **Причина**: Короткі неоднозначні топоніми (напр. «Іванівка») геокодувались окремо і Google повертав перший результат (інша область). Пріоритет був невірний: location.city перед LLM-адресами.
- **Дії**:
  - **scripts/olx_scraper/helpers.py**: `_collect_and_geocode_locations` — змінено порядок: LLM-адреси (з регіоном) → raw → city → search location. Додано `_should_skip_ambiguous_short_location`: не геокодувати короткі топоніми, якщо вже є повніша адреса з регіоном.
  - **UnifiedListingsService**: `_extract_region_from_query` — витягує область з query_text. `_normalize_address_from_geocode` — при кількох результатах обирає той, чий регіон збігається з query_text.
  - **Існуючі записи**: для оновлення адрес потрібно перезапустити оновлення OLX (`run_olx_update`), щоб перегеокодувати з новою логікою.

## 2026-02-13 — Кількість результатів та пейджинг у пошуку

- **Запит**: У результатах пошуку додати напис кількості результатів та пейджинг (20 оголошень на сторінку).
- **Дії**:
  - **index.html**: додано блок `search-results-count` для відображення «Знайдено X оголошень».
  - **app.js**: функція `pluralizeListings(n)` для коректної множини (оголошення/оголошень); `renderSearchResults` показує кількість результатів; при пагінації — «(показано 1–20)»; пейджинг (pageSize: 20, limit/skip у API); виправлено очищення пагінації при зміні фільтрів; приховування напису під час завантаження та при помилці.
  - **styles.css**: стилі для `search-results-count`.

## 2026-02-13 — Прибрано LLM-агента з бота

- **Запит**: Бот використовується лише як канал отримання файлів і додаткового адміністрування. Повідомлення логуються, але не відповідаємо.
- **Дії**:
  - **handle_message**: замість _handle_text_with_llm — логування через log_user_action (action: bot_message_received), без відповіді.
  - **Швидкі дії**: report_last_day, report_last_week → handle_get_file (файл з БД без LLM); export_data → підказка відкрити застосунок.
  - Видалено _handle_text_with_llm та _handle_quick_action_llm.
  - Оновлено текст меню: «Оберіть швидку дію або відкрийте застосунок».
  - MultiAgentService залишено лише для адмін-функції «Тестування агента».

## 2026-02-13 — Файли через бота замість прямого скачування (мобільні)

- **Запит**: У мобільному застосунку скачування файлів не працює. Надсилати файли через бота в чат.
- **Дії**:
  - **send_via_bot.py**: `send_file_via_telegram(chat_id, file_bytes, filename, bot_token)` — виклик Telegram API sendDocument.
  - **API files**: POST /send-report-via-bot (days: 1|7), POST /send-generated-via-bot (task_id, token), POST /send-artifact-via-bot (artifact_id, token).
  - **API search**: POST /send-export-via-bot — той самий body що й export, надсилає Excel через бота.
  - **API report-templates**: POST /{id}/generate — body.send_via_bot=true надсилає файл через бота замість base64/download_url.
  - **app.js**: downloadReport → send-report-via-bot; generateReportFromTemplate → send_via_bot: true; search «Зберегти у файл» → send-export-via-bot; артефакти в чаті → кнопка «Отримати в чаті» викликає send-artifact-via-bot; startGenerateSeven при done → кнопка «Надіслати в чат» викликає send-generated-via-bot.

## 2026-02-13 — Конструктор шаблонів звітів та оновлення вкладки «Звіти»

- **Запит**: Можливість користувачу створювати власні шаблони звітів через конструктор; перелік шаблонів з кнопками згенерувати/видалити; drag-and-drop для зміни порядку. На вкладці пошуку — «Зберегти у файл» та «Зберегти як шаблон».
- **Дії**:
  - **Міграція 023**: колекція `report_templates` (user_id, name, params, is_default, order).
  - **ReportTemplateRepository**: create, list_by_user, get_by_id, delete, reorder.
  - **ReportTemplateService**: list_templates (з дефолтними «Звіт за добу», «Звіт за тиждень»), generate_template_name (LLM), create_template, delete_template, reorder_templates.
  - **API report_templates**: GET / (список), POST / (створити), POST /generate-name (LLM-назва), DELETE /{id}, POST /reorder, POST /{id}/generate (Excel), GET /download?token=.
  - **API search**: POST /export — експорт результатів пошуку у Excel за фільтрами; додано date_filter_days, price_per_m2, price_per_ha до _build_unified_filters.
  - **index.html**: нова структура screen-files (список шаблонів, кнопка «Новий шаблон»), модальний конструктор з полями (джерело, період, область, місто, тип, ціна/ціна за м²/за га з валютою, сортування, формат виводу, назва + «Згенерувати»). На пошуку — фільтр «Період», кнопки «Зберегти у файл», «Зберегти як шаблон».
  - **app.js**: showFiles, loadReportTemplates, renderReportTemplates (Sortable.js для drag-drop), generateReportFromTemplate, openReportConstructor, getConstructorParams, buildSearchExportBody, openReportConstructorFromSearch.
  - **styles.css**: report-templates-list, report-template-item, modal, report-constructor, search-actions-row.

## 2026-02-13 — Площа нерухомості з ProZorro в unified_listings

- **Запит**: Перевірити заповнення площі нерухомості з ProZorro в узагальнену таблицю — вона завжди пуста, хоча в оголошенні присутня.
- **Причина**: ProZorro.Sale зберігає площу в **items[].itemProps** (totalObjectArea, totalBuildingArea, usableArea, landArea), а не в quantity.value. UnifiedListingsService використовував лише quantity.value.
- **Дії**:
  - **UnifiedListingsService._extract_area_info**: для prozorro додано пріоритетне витягування з itemProps (totalObjectArea/totalBuildingArea/usableArea для нерухомості, landArea для землі); fallback — quantity.value. Додано _convert_to_sqm та _convert_to_hectares для конвертації одиниць.
  - **data_dictionary.yaml**: оновлено опис building_area_sqm та land_area_ha для unified_listings.
  - **prozorro-data-structures.mdc**: уточнено правило про площі — itemProps як пріоритет.

## 2026-02-13 — Фільтри пошуку: тип оголошення, площа нерухомості, площа землі
- **Запит**: Додати у вкладку пошук фільтри: тип оголошення (Нерухомість, Земельні ділянки, Земельні ділянки з нерухомістю, інше, все), площа нерухомості (кв. м.), площа земельної ділянки (га).
- **Дії**:
  - **index.html**: додано filter-group для типу оголошення (select), площі нерухомості (оператор + значення), площі землі (оператор + значення).
  - **search.py**: _build_unified_filters — додано property_type (маппінг UI → БД), building_area_sqm (eq/gte/lte), land_area_ha (eq/gte/lte). API /unified — нові query-параметри.
  - **app.js**: searchState.filters — propertyType, buildingAreaOp/Value, landAreaOp/Value; performSearch — передача параметрів; bindSearchEvents — читання та скидання нових полів.

## 2026-02-12 — Агрегація в пайплайні: group_by + metrics (avg, sum, min, max, count)
- **Запит**: Побудовник не мав можливості зробити агрегацію, коли одні поля групуються, а інші — агрегуються (середнє, сума). Потрібно отримати датафрейм з домен-шару й додати етапи групування.
- **Дії**:
  - **PipelineExecutor**: крок `aggregate` з `group_by` та `metrics: [{field, aggregation}]`. aggregation: avg|sum|min|max|count. Маппінг логічних полів через AGGREGATE_FIELD_MAP. Витягування region/settlement з addresses для unified_listings. Сортування та limit після агрегації.
  - **PipelineBuilderAgent**: оновлено _validate_aggregate_step для metrics; додано aggregate step у _build_pipeline_from_context при aggregation_needed/aggregation_group_by.
  - **QueryStructureAgent**: додано aggregation_group_by, aggregation_metrics до промпту та валідації.
  - **CanonicalQuery**: додано aggregation_group_by, aggregation_metrics.
  - **SourceFieldMapper**: додано building_area_sqm, land_area_ha, price_per_m2 для unified_listings.
  - **app_metadata.yaml**: оновлено опис analytical_text та available_operations для агентів.

## 2026-02-12 — Виправлення аналітичних запитів (середня ціна по областях, найдорожчий регіон)
- **Запит**: Обидва запити («Який регіон найдорожчий за середньою ціною за кв.м.?», «Яка середня ціна за кв. м. по областям?») були валідними, але не виконувались.
- **Причини**: (1) IntentDetector визначав out_of_scope для другого запиту через контекст попередньої відповіді «неможливо визначити»; (2) pipeline не мав execute_analytics для агрегації по областях; (3) SORT_FIELD_MAP не містив average_price_per_sqm.
- **Дії**:
  - **IntentDetectorAgent**: уточнено правила out_of_scope — тільки запити поза нерухомістю; додано застереження щодо ігнорування попередніх відповідей асистента.
  - **SORT_FIELD_MAP**: додано average_price_per_sqm → price_per_m2_uah для unified_listings.
  - **MultiAgentService._try_analytics_aggregation_by_region**: для analytical_text і text_answer з запитами «середня ціна по областям» або «найдорожчий/найдешевший регіон» — виклик execute_analytics з groupBy: ["region"].
  - **AnswerComposerService._extract_price_from_row**: додано підтримку поля "value" для аналітичних рядків.

## 2026-02-12 — Посилання на Excel у чаті: збереження та TTL
- **Запит**: Після оновлення сторінки бота пропадають посилання на завантаження файлів. Зберігати їх і файли. Видаляти файли, якщо закривається чат із файлами або якщо файли старші 10 днів.
- **Дії**:
  - **llm.py**: Excel зберігаються в артефакти через `ArtifactService.register_with_token()` (TTL 10 днів); у відповіді повертаються `artifact_id`, `download_token`, `filename` замість `file_base64`.
  - **files.py**: `GET /api/files/artifact/{id}?token=...` — завантаження по токену (для мобільних); `POST /api/files/artifacts/delete` — видалення артефактів при видаленні чату (з перевіркою user_id).
  - **app.js**: повідомлення зберігають `excelFiles` (artifact_id, download_token, filename); при рендері історії відображаються посилання; при видаленні чату — виклик API для видалення артефактів.
  - **scheduler_service**: щоденна задача о 3:00 — `delete_expired()` для артефактів з простроченим TTL.

## 2026-02-12 — Синхронізація unified_listings при оновленні джерел
- **Запит**: Після оновлення даних з ProZorro та OLX зведена таблиця не оновлювалась. Запис з джерела має одразу потрапляти в unified_listings.
- **Дії**:
  - **prozorro_service**: додано `_sync_auction_to_unified(auction_id)` — виклик після кожного `upsert_auction` у `_save_auctions_to_database` та `fetch_and_save_real_estate_auction_details`.
  - **scripts/olx_scraper/run_update.py**: додано `UnifiedListingsService`; після кожного `repo.upsert_listing` викликається `unified_service.sync_olx_listing(listing_url)`.
  - Помилки синхронізації логуються, але не переривають основним процес оновлення.

## 2026-02-12 — Кнопка Open у пошуку: інструкція BotFather
- **Запит**: Перезапускав застосунок і Telegram — кнопки нема.
- **Пояснення**: Кнопка «Open» у результатах пошуку (як у BotFather) — це **Main Mini App**; вона налаштовується **тільки через BotFather**, не через код. `setChatMenuButton` дає лише кнопку меню (☰) в чаті.
- **Дії**:
  - **docs/mini_app_setup.md**: інструкція — /mybots → Bot Settings → Configure Mini App → Enable Mini App, вказати URL.
  - **config.example.yaml**: уточнено коментар про обидві кнопки (Open у пошуку vs меню в чаті).
  - **development_history**: фіксація відповіді.

## 2026-02-12 — Згортання sidebar і кнопка меню в чаті
- **Запит**: Обробник згортання sidebar при нестачі місця; на мобільному — згорнуто за замовчуванням. Кнопка відкриття застосунку доступна зі сторінки чату та пошуку (як у BotFather).
- **Дії**:
  - **Sidebar**: на viewport < 768px або platform android/ios — згорнуто за замовчуванням. Кнопка ☰ в header відкриває overlay. Кнопка «‹» в sidebar згортає. Клік по backdrop або вибір чату закриває overlay. Resize viewport — авто-оновлення стану.
  - **Кнопка меню (☰) в чаті**: вже реалізована через `set_chat_menu_button` при налаштованому `mini_app_base_url` (HTTPS).

## 2026-02-12 — UI LLM-помічника: full viewport та історія чатів
- **Запит**: Сторінка ллм-помічника виглядала не дуже: розтягнути на все вікно; додати зліва меню з історією запитів для перемикання між чатами (як у ChatGPT).
- **Дії**:
  - **index.html**: додано layout з `.app-body`, sidebar (`#chat-sidebar`) з кнопкою «Новий чат» та списком чатів.
  - **styles.css**: full viewport (html/body height 100%, #root flex column 100vh); `.app-body` flex з sidebar (260px); стилі для `.chat-history-list`, `.chat-history-item`; chat-section та chat-messages на flex для заповнення висоти.
  - **app.js**: chat sessions у localStorage (`pazuzu_chat_sessions`); `createNewChat()`, `switchChat(id)`, `addMessageToCurrentChat()`; sidebar показується лише на екрані AI-асистента; перший запит створює чат, назва — перші 50 символів запиту.

## 2026-02-12 — Правила заповнення price_per_m2 та price_per_ha
- **Запит**: Земельна ділянка без нерухомості — price_per_m2 має бути NULL; земля з нерухомістю — обидва поля; чисто нерухомість — тільки price_per_m2.
- **Дії**:
  - **utils/price_metrics.py**: оновлено docstring з правилами заповнення.
  - **UnifiedListingsService._extract_area_info**: для OLX не використовувати `search_data.area_m2` як fallback для building_area, якщо вже є `land_area_ha` (для земельних ділянок area_m2 часто є площею землі в м²).
  - **config/data_dictionary.yaml**: оновлено описи полів price_per_m2_*, price_per_ha_*.
  - **.cursor/rules/price-metrics-rules.mdc**: нове правило з таблицею заповнення.
  - **scripts/migrations/020_unified_listings_area_fields.py**: приведено логіку area_m2 fallback до єдиних правил.
  - **scripts/migrations/022_price_metrics_land_rules.py**: нова міграція — повторне витягування площ з сирих документів (з виправленою логікою OLX) та перерахунок метрик.

## 2026-02-12 — Покращення експорту: ім'я файлу, сортування, активні аукціони
- **Запит**: Найменування файлу українсько і зрозуміло; сортування за замовчуванням від нового до старого; лише активні аукціони, якщо користувач не вказав інакше.
- **Дії**:
  - **MultiAgentService._query_to_filename_prefix**: «Оголошення_Нововолинськ_Ковель_Луцьк», «Оголошення_область_Київська»; fallback «Експорт».
  - **pipeline_executor**: DEFAULT_SORT_FIELD (source_updated_at, dateModified, updated_at) — якщо sort не вказано, сортуємо за датою desc (від нового до старого).
  - **pipeline_executor**: DEFAULT_STATUS_ACTIVE = "активне" — для unified_listings додаємо status=активне, якщо користувач не вказав статус.

## 2026-02-12 — Релевантне ім'я файлу замість export
- **Запит**: Якщо повертаємо результат у файлі — називати його релевантно запиту.
- **Дії**:
  - **MultiAgentService**: метод `_query_to_filename_prefix(user_query, query_structure)` — з фільтрів (city, region) або з запиту формує префікс (напр. mista_novovolynsk_kovel, oblast_kyivska).
  - **MultiAgentService**: при експорті для data_export передається filename_prefix з `_query_to_filename_prefix`.
  - **LangChain prompt**: вказівка — filename_prefix має бути релевантним запиту.

## 2026-02-12 — NEW_FLOW: експорт у уніфікованому форматі
- **Проблема**: Результат пайплайну NEW_FLOW зберігався через save_query_results_to_excel з колонками response_metrics (price, area, region, city...) — не у новому уніфікованому форматі.
- **Дії**:
  - **LangChainAgentService**: метод `export_results_to_excel_unified(results, source_collection, filename_prefix)`; винесено логіку експорту в `_export_docs_to_excel(docs, ...)`.
  - **MultiAgentService**: для data_export використовується `export_results_to_excel_unified` замість `save_query_results_to_excel` — Excel у форматі з датою, джерелом, адресою, ціною, посиланням (клікабельним).

## 2026-02-12 — Втрата фільтра city/region з $in
- **Проблема**: Запит «оголошення по містам Нововолинськ, Ковель, Луцьк» — Pipeline не може бути порожнім; Втрата фільтрів: очікувалось 1, застосовано 0.
- **Причина**: city/region зі значенням `$in` або list передавались у geo_filters_dict; GeoFilterService.from_dict приймає лише string; geo_filters_clean відкидав dict/list; city не потрапляв у all_conditions.
- **Дії**:
  - **domain/services/pipeline_executor.py**: коли city/region має `$in` або list — додавати в all_conditions (физичний шлях addresses.settlement/region); лише одиночний string — geo_filters_dict. Додано обробку `$in` у _build_filter_group.
  - **Повтор**: QueryStructureAgent іноді виводить `in` замість `$in`. Додано підтримку обох варіантів у pipeline_executor та _query_to_filename_prefix.

## 2026-02-12 — Уніфікований формат експорту Excel
- **Запит**: Результат «знайди оголошення в місті Нововолинськ» був неінформативним через структуру файлу. Потрібен єдиний формат вивантаження зі зведеної таблиці, посилання — клікабельними; без явного джерела — використовувати unified_listings.
- **Дії**:
  - **utils/file_utils.py**: `DEFAULT_URL_FIELDNAMES` = [auction_url, listing_url, page_url, previous_auctions_links]; `generate_excel_in_memory` переведено на `_apply_sheet_style` з підтримкою page_url; `_apply_sheet_style` та `save_excel_to_file` роблять колонку page_url клікабельною гіперпосиланням.
  - **prozorro_service.get_standard_sheet_data_for_export_from_docs**: при порожньому source_collection використовує unified_listings (coll = ... or "unified_listings").
  - **langchain_agent_service**: опис save_query_to_temp_collection — «Якщо користувач НЕ вказав явно ProZorro чи OLX — використовуй unified_listings».

## 2026-02-12 — Mini App: кнопка fullscreen, оновлення даних, навігація
- **Запит**: Перемістити кнопку fullscreen (щоб не перекривала кнопку закриття); додати в адмін кнопки оновлення даних з прогресом; перейменувати «Головна» на «AI-асистент» та візуально показувати активну вкладку.
- **Дії**:
  - **index.html**: видалено кнопку expand з header-row; додано блок «Оновлення даних» в адмін (кнопки «Оновити за 1 день» / «7 днів», статус).
  - **admin.py**: POST /api/admin/data-update?days=1|7, GET /api/admin/data-update/status?task_id=...; запуск в фоновому потоці ProZorro + OLX.
  - **app.js**: кнопка ⛶ перенесена в nav (останній елемент); renderNav — «AI-асистент» замість «Головна»; setNavActive() для підсвітки активної вкладки; startAdminDataUpdate() з polling статусу.
  - **styles.css**: .nav a.active, admin-data-update-status, admin-hint; видалено header-row, btn-expand-viewport з header.

## 2026-02-12 — Пошук у Mini App: зведена таблиця та fullscreen для веб/десктоп
- **Запит**: Замінити окремі вкладки OLX/ProZorro на єдиний пошук за зведеною таблицею; збільшити вікно міні-апп на веб/десктоп версії.
- **Дії**:
  - **telegram_mini_app/routes/search.py**: додано API `/api/search/unified` — пошук за зведеною таблицею (UnifiedListingsRepository) з фільтрами region, city, price, source (olx/prozorro). Endpoints `/unified/filters/regions`, `/unified/filters/cities` для геофільтрів (через GeographyService).
  - **index.html**: змінено вкладки Пошук — прибрано OLX/ProZorro tabs, додано фільтр «Джерело» (Всі/OLX/ProZorro), інтро «Зведений пошук».
  - **app.js**: пошук за зведеною таблицею; елементи списку з бейджем джерела (OLX/ProZorro); при кліку — showDetail(source, source_id). Кнопка «Розгорнути на весь екран» для веб/десктоп.
  - **app.js**: виклик `Tg.requestFullscreen()` при завантаженні на веб/десктоп (platform: web, tdesktop, macos, windows тощо).
  - **styles.css**: стилі для search-item-source-badge, intro-search, header-row, btn-expand-viewport.

## 2026-02-12 — Фінальна відповідь: count template та FinalAnswerRefinementService
- **Запит**: Запит «Яка кількість оголошень за останній тиждень по Волинській області» повертав список посилань замість числа. Потрібен фінальний етап, де агент повторно аналізує запит з контекстом результатів.
- **Дії**:
  - **AnswerComposer**: для запитів на кількість (response_template з «X», «кількість» в intent) — відповідь через _format_count_answer з підстановкою числа в шаблон.
  - **FinalAnswerRefinementService**: новий сервіс — LLM аналізує user_query, intent_info, execution_result, draft_summary та формує фінальну людяну відповідь.
  - **MultiAgentService**: передача intent_info, user_query в execution_result_for_composer; виклик FinalAnswerRefinementService після AnswerComposer.

## 2026-02-12 — RelativeDateResolver та відновлення date-фільтра
- **Запит**: date-фільтр губився в пайплайні; LLM генерував {{LAST_WEEK_START_DATE}}; потрібен TimeResolutionLayer.
- **Дії**:
  - **RelativeDateResolver** (`business/services/relative_date_resolver.py`): детермінований сервіс — перетворює {period: "last_7_days"} / {type: "relative", value: "last_week"} у {gte, lte} з конкретними датами.
  - **CanonicalQuery**: додано date_range; витягування з filter_metrics.date або date_range.
  - **QueryStructureAgent**: промпт — заборона шаблонів {{...}}; date_range: {period: "last_7_days"}; _normalize_filters — ігнорування date з шаблоном, використання _extract_date_range; _normalize_date_range.
  - **MultiAgentService**: після QueryStructureAgent виклик RelativeDateResolver; merge resolved date у filter_metrics.
  - **pipeline_executor._build_filter_group**: підтримка gte/lte/gt/lt у dict; пропуск полів з шаблонами {{...}}; _contains_template.
  - **ResultValidatorService**: filter consistency — expected_filters vs applied_filters_count; filter_loss → should_retry.
  - **PipelineExecutor**: diagnostic_info.applied_filters_count.
  - **UnifiedListingsCollectionManager.find()**: логування Final Mongo $match.

## 2026-02-12 — Механізм створення пайплайнів та GeoFilterService
- **Запит**: Змінити механізм створення пайплайнів: інтент + extracted data → PipelineService формує контекст (структура полів, кеші) → PipelineBuilderAgent створює flowchart-пайплайн (unified_listings як основна; джерела лише за потреби). Геофільтри — через окремий GeoFilterService.
- **Дії**:
  - **PipelineService** (`business/services/pipeline_service.py`): приймає intent_info + extracted_data, формує контекст (field_structure, collection_value_caches, collection_manager_methods).
  - **PipelineBuilderAgent**: новий метод `_build_pipeline_from_context()` — flowchart-пайплайн; unified_listings як основна колекція; кроки з `use_geo_filter` для вибірок. Типи кроків: select, filter, add_metric, sort, limit.
  - **GeoFilterService** (`business/services/geo_filter_service.py`): повний сервіс геофільтрації — створення фільтрів (місто, область, «не в місті», радіус від координат), геокодування адрес, метрика відстані до точки.
  - **domain GeoFilterService**: розширено `from_dict` — exclude_city, exclude_region, center_lat, center_lon, radius_km.
  - **GeoFilterOperator.NE**: новий оператор для «не в місті» / «не в області».
  - **UnifiedListingsCollectionManager._geo_filter_to_mongo**: підтримка NE через $not $elemMatch.
  - **PipelineInterpreterService**: _apply_add_metric_steps — після виконання застосовує add_metric (distance_km) через GeoFilterService.add_distance_metric.
  - **MultiAgentService**: інтеграція PipelineService; pipeline_context передається в PipelineBuilderAgent.build_pipeline().

## 2026-02-12 — Виправлення GeoFilterService: CollectionManager.find() та QueryBuilder
- **Запит**: Тестування GeoFilterService — вибірка по Львів, Київ, Вінниця та відповідним областям.
- **Дії**:
  - **UnifiedListingsCollectionManager.find()**: змінено `result.get("data", [])` на `result.get("results", result.get("data", []))` — QueryBuilder.execute_aggregation повертає `results`, а не `data`.
  - **scripts/test_geo_filter_service.py**: новий скрипт для тестування геофільтрів (міста Львів, Київ, Вінниця та області Львівська, Київська, Вінницька).

## 2026-02-12 — Фільтр за типом оголошення (нерухомість без земельних ділянок)
- **Запит**: «Найдорожча нерухомість Києва» має враховувати лише об'єкти нерухомості (будівлі, приміщення), а не земельні ділянки.
- **Дії**:
  - **QueryStructureAgent**: додано інструкції в промпт — при «нерухомість» без «земля»/«ділянка» додавати property_type: ['Комерційна нерухомість', 'Земельна ділянка з нерухомістю'].
  - **QueryStructureAgent._extract_property_type()**: rule-based витягування — якщо «нерухомість» в запиті і немає «земл»/«ділянк»/«земельн», додає фільтр за типом оголошення.

## 2026-02-12 — Нормалізація топонімів у геофільтрах
- **Запит**: Запит «Волинській області» має перетворюватися на геофільтр Область = «Волинська» (форма в БД). Нормалізація за тими ж правилами, як при завантаженні з джерел.
- **Дії**:
  - **utils/toponym_normalizer.py**: normalize_region (Волинській області → Волинська), normalize_settlement (у Києві → Київ). Правила як у UnifiedListingsService (прибрати «область»/«обл.»/«області»; прикметник -ій/-ої/-у/-ою → -а).
  - **domain GeoFilterService.from_dict**, **business GeoFilterService.create_geo_filter**: крок нормалізації топонімів перед створенням GeoFilter.
  - **QueryStructureAgent._normalize_filters**: нормалізація city і region у filter_metrics.

## 2026-02-12 — Маршрут теоретичного опрацювання запиту
- **Запит**: Зробити маршрут теоретичного опрацювання запиту (приклад: «Яка найдорожча нерухомість в Києві?»).
- **Дії**:
  - **MultiAgentService.trace_query_processing()**: новий метод — виконує кроки 1–3 (IntentDetector, QueryStructureAgent, PipelineBuilder) без виконання пайплайну. Повертає зведення для дебагу.
  - **POST /api/admin/trace**: новий маршрут в Mini App — приймає текст запиту, повертає JSON з intent_info, query_structure, pipeline. Достіп тільки для адмінів.
  - **scripts/trace_query.py**: CLI-скрипт для прогану теоретичного опрацювання (за замовчуванням: «Яка найдорожча нерухомість в Києві?»).

## 2026-02-12 — Геофільтр для city/region та price→price_uah
- **Запит**: Місто має опрацьовувати геофільтром (не звичайними відборами). Під ціною в unified_listings — price_uah.
- **Дії**:
  - **pipeline_executor**: city, region, addresses.settlement, addresses.region — завжди через геофільтр (geo_filters_dict). Підтримка conditions, filter_metrics, filters у кроках filter.
  - **SourceFieldMapper**: коментар — price у unified_listings завжди означає price_uah.
  - **Глосарій**: термін «price (unified_listings)».

## 2026-02-11 — Пайплайни тільки через домен-шар (без MongoDB)
- **Запит**: Пайплайни не повинні бути пов’язані з MongoDB. Всі пайплайни мають працювати з методами домен-шару.
- **Дії**:
  - **domain/services/pipeline_executor.py**: новий модуль — виконує пайплайни виключно через `CollectionManager.find()`. Підтримка filter, sort, limit, calculate, aggregate (базова). Маппінг логічних полів (price→price_uah) через SourceFieldMapper.
  - **PipelineInterpreterService**: повністю рефакторовано — делегує на `execute_pipeline()`. Видалено `_build_mongo_pipeline`, `_transform_conditions_to_mongo`, `_collect_diagnostic_info`, `_get_total_documents_count`, `GeoFilterBuilder`, `QueryBuilder`. Жодних прямих викликів MongoDB.
  - **BaseCollectionManager**: додано `get_total_count()` для діагностики.
  - **UnifiedListingsCollectionManager**: реалізація `get_total_count()`; regex для settlement — підтримка варіанту «м. Київ».
  - Для prozorro_auctions та olx_listings повертається помилка «не підтримується через домен-шар» (поки що тільки unified_listings).

## 2026-02-11 — Logical vs Physical Layer Separation
- **Запит**: Розділити логічний рівень і фізичну схему БД, усунути змішування address_refs.city, addresses.settlement у одному потоці, гарантувати детерміновану побудову Mongo pipeline.
- **Дії**:
  - **LogicalLayerViolation** (`domain/exceptions.py`): виняток для фізичних полів на логічному рівні.
  - **validate_logical_filters** (`domain/validators.py`): валідація дозволених ключів (city, region, price, date тощо), заборона ключів з крапкою.
  - **CanonicalQuery** (`domain/models/canonical_query.py`): канонічний опис запиту, ізольований від фізики БД.
  - **FieldMappingService** (`business/services/field_mapping_service.py`): єдиний mapping layer (map_logical_to_physical, normalize_physical_filters).
  - **QueryStructureAgent**: валідація filter_metrics після _normalize_filters; промпт — тільки логічні поля.
  - **InterpreterAgent**: валідація filters та region_filter у _build_structured.
  - **analysis_intent_schema**: валідація filters у analysis_intent.
  - **MultiAgentService**: перетворення query_structure на CanonicalQuery після QueryStructureAgent.
  - **PipelineInterpreterService**: прибрано обробку фізичних ключів у _transform_conditions_to_mongo; FieldMappingService для _get_field_path; normalize_physical_filters перед $match; логування Final Mongo $match.
  - **ResultValidatorService**: перевірка potential_filter_conflict (geo_filter_applied + total_docs > 0 + count == 0) → should_retry.
  - **.cursor/rules/llm-agent-architecture.mdc**: новий розділ 9 — Logical vs Physical Layer, заборона Mongo-полів у агентів.

## 2026-02-11 — Домен-шар: CollectionManager, ObjectManager, GeoFilterService
- **Запит**: Сервіси-менеджери колекцій та об'єктів; кеш довідкових даних; гео-пошук; пайплайни через методи менеджерів; очистити кеш пайплайнів; рул при зміні структури.
- **Дії**:
  - **domain/models/filter_models.py**: FilterElement, FilterGroup (AND/OR/NOT), FilterOperator, GeoFilterElement, GeoFilterGroup, GeoFilterOperator, GeoFilter, FindQuery.
  - **domain/services/geo_filter_service.py**: GeoFilterService — from_city_region, from_coordinates_with_radius, from_dict.
  - **domain/managers/collection_manager.py**: BaseCollectionManager, UnifiedListingsCollectionManager — add(), delete(), get_available_field_values (кеш), get_field_structure (кеш), update_cache(), find(FindQuery) → DataFrame, get_object().
  - **domain/managers/object_manager.py**: ObjectManager — get(object_id, collection).
  - **PipelineInterpreterService**: _try_execute_via_collection_manager — для unified_listings виконує через CollectionManager.find() замість прямого MongoDB.
  - **PipelineRepository.clear_cache()**: видаляє всі записи з pipeline_templates.
  - **domain_cache_service.invalidate_domain_caches(sources)**: оновлює кеш CollectionManager, очищає кеш пайплайнів — викликається після оновлення даних у main.py.
  - **.cursor/rules/domain-layer-structure.mdc**: рул — при зміні data_dictionary, міграцій, репозиторіїв обов'язково оновлювати домен-шар.

## 2026-02-11 — Усунення address_refs з промптів агента
- **Запит**: Прибрати використання поля address_refs агентом — перейти на addresses (unified_listings).
- **Дії**:
  - **LangChainAgentService**: промпти за адресами переписано на unified_listings + addresses ($elemMatch по addresses.region, addresses.settlement); видалено інструкції для OLX/ProZorro з address_refs; опис execute_query оновлено.
  - **PipelineBuilderAgent**: _get_field_path_for_filter повертає лише для unified_listings (addresses.region, addresses.settlement); prozorro/olx не підтримуються агентом.
  - **address_refs_available** перейменовано на **addresses_available**; повідомлення «Поле address_refs відсутнє» → «Поле адрес відсутнє» (PipelineInterpreterService, MultiAgentService, AnswerComposerService).
  - **PipelineInterpreterService**: гео-ключі перевіряють addresses і settlement; refs_path для addresses вважається [:-1].
  - **CollectionKnowledgeService**: коментар оновлено.

## 2026-02-11 — address_refs_available для unified_listings (поле addresses)
- **Запит**: _collect_diagnostic_info визначав address_refs_available лише для prozorro_auctions (auction_data.address_refs) та olx_listings (detail.address_refs). У unified_listings поле називається addresses, тому умова не спрацьовувала.
- **Дії**: Додано гілку для collection == "unified_listings" у PipelineInterpreterService._collect_diagnostic_info: перевірка на наявність `addresses` ({"$exists": True, "$ne": []}).

## 2026-02-11 — Переорієнтація агентів та інструментів на unified_listings
- **Запит**: Робота агентів та інструментів має бути переорієнтована на об'єднану таблицю та домен-левел.
- **Дії**:
  - **QueryStructureAgent**: за замовчуванням sources = ["unified_listings"]; валідація додає unified_listings; промпт рекомендує unified_listings як основне джерело.
  - **PipelineBuilderAgent**: підтримка unified_listings у _get_field_path_for_filter (addresses.region, addresses.settlement); промпт оновлено.
  - **PipelineInterpreterService**: fallback-колекція змінена з prozorro_auctions на unified_listings; _transform_conditions_to_mongo використовує SourceFieldMapper.get_addresses_array_path та get_geo_match_keys для unified_listings.
  - **GeoFilterBuilder**: додано unified_listings (_get_default_collection_info, _analyze_address_refs_structure); _build_regex_filter використовує region/settlement замість region.name/city.name; $unwind тепер з префіксом $ (MongoDB вимагає `$addresses`); ігнорування непідставлених параметрів ($region, {{city}}); порядок stages: $unwind → $match (прямий шлях addresses.settlement після unwind).
  - **SourceFieldMapper**: додано unified_listings (city→addresses.settlement, region→addresses.region, price→price_uah); get_addresses_array_path, get_geo_match_keys.
  - **schema_filter_resolver**: _resolve_geo_filter_unified для addresses з $elemMatch.
  - **MultiAgentService**: _get_sort_value (price_uah), _belongs_to_source (unified_listings), діагностика addresses.
  - **LangChainAgentService**: save_query_to_temp_collection, export_listings_to_file, _normalize_export_ids для unified_listings.
  - **CollectionKnowledgeService**: unified_listings у PROFILE_COLLECTIONS та FIELD_PROFILE_CONFIG; refresh mapping "unified".
  - **AgentTestRunnerService**: unified_listings у COLLECTION_DATE_FIELDS, expected_collections, fallback тест-кейс.

## 2026-02-11 — Поля площі в unified_listings (building_area_sqm, land_area_ha)
- **Запит**: Додати до зведеної таблиці дані по площі (м² та га), заповнити існуючі записи з сирих даних.
- **Дії**:
  - **UnifiedListingsService**: додано `building_area_sqm` та `land_area_ha` до unified_doc у `_convert_olx_to_unified` та `_convert_prozorro_to_unified`. Для OLX додано fallback на `search_data.area_m2`, якщо detail.llm не містить площі.
  - **Міграція 020** (`020_unified_listings_area_fields.py`): проходить по всіх unified_listings, отримує сирі документи з olx_listings/prozorro_auctions, витягує площу за тією ж логікою, оновлює записи.
  - **data_dictionary.yaml**: додано опис полів `building_area_sqm` та `land_area_ha` для unified_listings.
  - **ProzorroService**: `_prepare_unified_data_for_excel` та `STANDARD_EXPORT_FIELDNAMES_UNIFIED` включають площі.
  - **UnifiedListing** (domain entity): властивості `building_area_sqm`, `land_area_ha`.

## 2026-02-11 — Domain-шар логіки над даними з БД
- **Запит**: Створити шар логіки, що замість роботи з БД огортає дані в об'єкти. Основа — зведена таблиця (unified_listings). Об'єкти мають методи отримання властивостей та пов'язаних сутностей. Окремі сутності для колекцій об'єктів. Всі інструменти та сервіси агентів переналаштовані на роботу з цими механізмами.
- **Дії**:
  - **Domain entities** (`domain/entities/`): `BaseEntity`, `UnifiedListing`, `ProzorroAuction`, `OlxListing` — обгортають сирі документи, надають властивості та методи (get_property, get_addresses, get_raw_data тощо).
  - **Domain collections** (`domain/entities/listing_collection.py`): `UnifiedListingCollection`, `ProzorroAuctionCollection`, `OlxListingCollection` — ізольована робота з масивами (filter, sort_by, limit, to_export_rows, to_raw_list).
  - **ListingGateway** (`domain/gateways/listing_gateway.py`): отримання сутностей з репозиторіїв за ids, `collection_from_raw_docs()` для результатів запитів, `get_raw_source_document_for_listing()` для доступу до сирих даних джерела.
  - **ExportDataService**: використовує `ListingGateway` замість репозиторіїв; підтримка `unified_listings`; експорт через domain-колекції.
  - **ProzorroService**: додано `_prepare_unified_data_for_excel`, `STANDARD_EXPORT_FIELDNAMES_UNIFIED`, підтримка `unified_listings` у `get_standard_sheet_data_for_export` та `get_standard_sheet_data_for_export_from_docs`.
  - **MCP**: query-builder — `save_query_to_temp_collection` підтримує `unified_listings`; export — `export_listings_to_file` та `get_export_collections` включають `unified_listings`.
  - Глосарій: термін «domain-шар».

## 2026-02-10 — Автоматичне дослідження даних та контекст «знання про колекції» для агента
- **Запит**: Система має автоматично досліджувати дані з джерел, зберігати згуртовані масиви повторюваних значень, середні по полях (для чисел); агент при отриманні запиту має дивитися на нього в контексті загальних знань про інформацію в колекціях.
- **Дії**:
  - Додано колекцію **collection_knowledge** (міграція 016): збереження профілів колекцій (total_documents, sample_size, field_stats з numeric/categorical статистикою).
  - **CollectionKnowledgeRepository**: save, get_latest(collection_name), get_all_latest(collection_names).
  - **CollectionKnowledgeService**: профілювання за вибіркою документів — для кожного поля з конфігу (FIELD_PROFILE_CONFIG) збираються числові (min, max, avg, count) або категоріальні (топ значень, кардинальність); підтримка вкладених шляхів та масивів (наприклад auction_data.address_refs.region.name, detail.llm.tags). Метод get_knowledge_for_agent() формує текстовий блок для контексту агента.
  - Планувальник: новий тип події **data_profile** (EVENT_TYPE_DATA_PROFILE); payload: collection_names, sample_size. Після виконання події профілі зберігаються в collection_knowledge.
  - Контекст агента: у **LangChainAgentService** після блоку дати/часу додано SystemMessage із змістом get_knowledge_for_agent(max_length=3500). У **MultiAgentService** контекст для інтерпретатора (_get_context_summary) доповнено тим самим блоком знань (max_length=2000), щоб інтерпретатор і планувальник бачили загальні знання про дані.
  - MCP (schema-mcp): ресурс **mongodb://collection-knowledge**, інструмент **get_collection_knowledge(collection_name?)** для читання збережених профілів.
  - Глосарій: терміни «знання про колекції», «профілювання даних».

## 2026-02-10 — Оновлення знань про колекції після кожного завантаження з джерел
- **Запит**: Оновлювати знання про колекції (профілювання) після кожного завантаження даних із джерел.
- **Дії**:
  - Додано `refresh_knowledge_after_sources(sources, sample_size)` у `collection_knowledge_service`: приймає список джерел (`["prozorro", "olx"]`), запускає профілювання для відповідних колекцій (prozorro_auctions, olx_listings).
  - **Планувальник** (`_execute_data_update`): після оновлення ProZorro та/або OLX викликається `refresh_knowledge_after_sources(sources)`.
  - **Telegram** (`_run_data_update_sync`): після синхронного оновлення обох джерел викликається оновлення знань для prozorro та olx.
  - **main.py**: після успішного `fetch_real_estate_auctions` — оновлення знань для prozorro; після успішного `run_olx_data_update` — для olx; у циклі фонового оновлення — для обох джерел.
  - **data_update_mcp_server** (`trigger_olx_update`): після `run_olx_update` викликається профілювання для olx_listings.

## 2026-02-10 — Красиве форматування посилань у відповідях (Telegram, Mini App)
- **Запит**: У відповідях користувачу (міні-апп та Telegram-бот) робити клікабельні посилання без виведення сирого URL, з коротким представленням (наприклад «Посилання»).
- **Дії**:
  - Додано `utils/link_formatter.py`: `format_message_links_for_telegram()` — заміна URL на HTML `<a href="...">Посилання</a>` з екрануванням решти тексту для Telegram (parse_mode=HTML); `format_message_links_for_mini_app()` — аналогічно для веб-відповіді з класом `chat-link` та target="_blank".
  - У `TelegramBotService`: перед відправкою відповіді LLM (edit_text/reply_text) текст форматується через `format_message_links_for_telegram`, відправка з `parse_mode="HTML"`. Застосовано в `_handle_text_with_llm` та `_handle_quick_action_llm` (включно з частинами при розбитті довгих повідомлень).
  - У Mini App: API `/api/llm/chat` повертає відповідь, відформатовану через `format_message_links_for_mini_app`. У `app.js` повідомлення бота відображаються через `innerHTML` для коректного рендеру посилань; додано стилі `.chat-msg.bot a.chat-link` у `styles.css`.

## 2026-02-10 — Аналітичний пайплайн без написання Mongo в LLM (Intent → Planner → Execution → Composer)
- **Запит**: Перехід від «User → LLM → Mongo Aggregation → Retry» до «User → Intent (LLM) → Deterministic Planner → Validated Execution → Answer Composer». LLM не пише Mongo, а описує намір; агрегації будує детермінований планувальник.
- **Дії**:
  - **Контракт та фільтри:** Додано `business/agents/analysis_intent_schema.py` (Analysis Intent Model, валідація). Додано `utils/schema_filter_resolver.py` (resolve_geo_filter для olx_listings/prozorro_auctions з address_refs та fallback). PlannerAgent переведено на використання schema_filter_resolver для OLX.
  - **Фаза 1 — Розширений Interpreter:** LLM Intent Extractor у `LLMService.extract_intent_for_routing` (JSON, temp=0, без tools). У InterpreterAgent при відсутності rule-based викликається extract_intent_for_routing; підтримка intent=analytical_query та analysis_intent. У MultiAgentService тристадійний роутинг: rule_based → analytical_pipeline (при analytical_query та analysis_intent) → LangChain.
  - **Фаза 2 — Analysis Planner:** Додано `utils/aggregation_patterns.py` (патерни top, count, avg, sum, distribution, trend). Додано `business/agents/analysis_planner_agent.py` (без LLM: нормалізація фільтрів, вибір патерну, побудова pipeline, валідація). Аналітичний пайплайн у MultiAgentService: planner.plan → query_builder.execute_aggregation → Answer Composer.
  - **Фаза 4 — Answer Composer:** Додано `business/services/answer_composer_service.py` (ExecutionResult → контракт type, title, items, attachments, summary). Додано `docs/answer_contract.md` та запис у developer_glossary.
  - **Фаза 3 — Analytical Reasoning (складні запити):** Додано `business/services/analytical_reasoning_service.py`: LLM будує план кроків (build_plan), guardrails (validate_plan: max_steps, allowed_operations), детерміноване виконання (execute_plan). При analysis_intent.multi_step=true використовується цей сервіс.
  - **Фаза 5 — LangChain тільки чат:** У free_form виключено інструменти execute_aggregation та execute_query (FREE_FORM_EXCLUDED_TOOLS). Оновлено системний промпт: агрегації та складні аналітичні запити обробляються окремим пайплайном; агент — для діалогу, уточнення та пояснень.
  - Додано `LLMService.generate_text` для плану аналітики та інших сценаріїв. GeminiLLMProvider реалізує generate_text.

## 2026-02-10 — Міграція OLX: ціна та адреса (detail.price, price_metrics, parsed_address)
- **Запит**: У повернутих даних OLX відсутні ціна (detail.price.value, detail.price.currency), ціна за м² та повна адреса (detail.llm.parsed_address.formatted_address). Потрібна міграція існуючих записів та заповнення цих полів.
- **Дії**:
  - Додано міграцію **015_olx_price_and_address_fields.py**: проходить по всіх olx_listings, заповнює detail.price (value, currency) з search_data; при відсутності detail.price_metrics рахує метрики (total_price_uah/usd, price_per_m2_uah/usd, price_per_ha) через compute_price_metrics; заповнює detail.llm.parsed_address.formatted_address з address_refs (GeographyService.format_address), або з detail.llm.addresses, або з search_data.location.
  - Оновлено **scripts/olx_scraper/run_update.py**: при збереженні деталей оголошення тепер також встановлюються detail.price (value, currency), detail.llm.parsed_address.formatted_address (з address_refs або llm.addresses або location), щоб нові записи одразу мали ці поля.
  - Запуск міграції: `py scripts/migrations/015_olx_price_and_address_fields.py` або через `run_migrations.py`.

## 2026-02-09 — Реорганізація структури географічних даних
- **Запит**: Реорганізувати структуру географічних даних. Області, міста, вулиці та будинки мають зберігатися в окремих ієрархічних колекціях. При завантаженні оголошень і обробці адрес - замість текстового представлення назви топоніму підставляємо посилання. Якщо це новий топонім - доповнюємо базу.
- **Дії**:
  - Створено репозиторії для географічних даних: `RegionsRepository`, `CitiesRepository`, `StreetsRepository`, `BuildingsRepository` з нормалізацією назв та унікальними індексами.
  - Створено `GeographyService` для роботи з географічними даними: метод `resolve_address()` створює або знаходить топоніми та повертає посилання.
  - Оновлено обробку адрес в OLX скрапері: функція `_collect_and_geocode_locations()` тепер також створює `address_refs` з посиланнями на топоніми.
  - Оновлено `ProzorroService`: при збереженні аукціонів створюються посилання на топоніми через `GeographyService`.
  - Створено міграцію `012_geography_collections.py` для створення колекцій та індексів.
  - Створено міграцію `013_migrate_addresses_to_references.py` для перетворення існуючих текстових адрес у посилання.
  - Оновлено API endpoints для фільтрів (`/api/search/*/filters/regions`, `/api/search/*/filters/cities`): спочатку використовують колекції географічних даних, fallback на текстові поля.
  - Оновлено фільтри пошуку: використовують `address_refs` для точного пошуку за ID топонімів, fallback на текстові поля.
  - Оновлено нормалізацію документів для відображення: спочатку використовують `address_refs`, потім `resolved_locations`, потім текстові поля.
  - Оновлено `data_dictionary.yaml`: додано описи колекцій `regions`, `cities`, `streets`, `buildings` та поля `address_refs` в `olx_listings` та `prozorro_auctions`.
  - Переваги: чітка структурована інформація про адреси, швидкий пошук областей/міст, унікальність топонімів, можливість швидкої фільтрації за посиланнями.

## 2026-02-09 — Файл з OLX за замовчуванням відсортований за датою оновлення
- **Запит**: За замовчуванням файл з OLX надавати відсортований за зменшенням дати оновлення.
- **Дії**: У `utils/query_builder.py` для колекції `olx_listings` додано сортування за замовчуванням: якщо в запиті не передано `sort`, у pipeline додається `$sort: { updated_at: -1 }`. У `business/services/prozorro_service.py` для OLX: у `get_standard_sheet_data_for_export` та `get_standard_sheet_data_for_export_from_docs` документи перед формуванням аркуша сортуються за `updated_at` за зменшенням (щоб експорт за ids або з тимчасової вибірки також був у порядку «найновіші спочатку»).

## 2026-02-08 — План покращень архітектури Pazuzu (імплементація)
- **Запит**: Реалізувати план покращень архітектури (прикріплений plan file): маршрутизація, confidence, схема кроків плану, MCP deny-rules, request_id, tool groups, артефакти, ліміти експорту, кнопки/explicit_intent, rate limit, session state, структуровані логи, replay, підтвердження великого експорту, кешування intent, state machine, контракти.
- **Дії** (стисло): (1) Intent confidence + fallback у InterpreterAgent та MultiAgentService, конфіг routing.confidence_threshold / ask_on_low_confidence. (2) Єдина схема кроку плану (plan_step_schema), валідація плану перед виконанням. (3) Explicit deny-rules для MCP у QueryBuilder та llm_agent_audit. (4) Повний проброс request_id у LangChainAgentService та run_tool. (5) Tool groups за маршрутом, time budget для агента. (6) Artifact service, export limits, TTL. (7) Кнопки «Звіт за день/тиждень», «Експорт» у Telegram; Mini App — intent/params у chat API. (8) Rate limit + complexity (Security Layer), session state (репозиторій, merge у interpret). (9) Структуровані логи кроків, скрипт replay_request. (10) Підтвердження великого експорту (confirm_token, pending_export). (11) Кеш intent (нормалізований текст + контекст), логування станів запиту (SECURITY_CHECKED, DELIVERED), розділ «Контракти» та таблиця компонент→тип у architecture_and_llm_agents.md, глосарій Security Layer. Оновлено development_history.

## 2026-02-08 — Двоступенева маршрутизація InterpreterAgent (rule-based + LLM)
- **Запит**: Зробити двоступеневу маршрутизацію: спочатку rule-based fast path (без LLM) для явних патернів (звіт, експорт, за сьогодні/тиждень, кнопки, slash-команди, Mini App), лише при незбігу — LLM InterpreterAgent. Це зменшить навантаження на LLM, стабілізує поведінку та полегшить дебаг.
- **Дії**:
  - **InterpreterAgent**: Додано `try_rule_based_routing(user_query, explicit_intent, explicit_params)` — повертає структурований намір лише при явному збігу (текстові патерни, slash: /report_day, /report_week, /export, або explicit_intent з клієнта). Інакше None → крок 2. `interpret_user_query` спочатку викликає rule-based; при збігу повертає результат з `routing_path=rule_based`, інакше — `_interpret_fallback` (поки intent=query без LLM) з `routing_path=llm`. Підтримка `explicit_intent` та `explicit_params` для кнопок/Mini App.
  - **MultiAgentService.process_query**: Додано опційні параметри `explicit_intent`, `explicit_params`; передаються в `interpret_user_query`. У лог відповіді (agent_activity_log) додано `routing_path`. При виборі пайплайну в `assistant.run()` передається `precomputed_structured`, щоб не викликати інтерпретатора двічі.
  - **AssistantAgent.run**: Опційний параметр `precomputed_structured`; якщо передано — інтерпретатор не викликається повторно.
  - Документація: оновлено `architecture_and_llm_agents.md` (двоступенева маршрутизація, routing_path), `developer_glossary.md` (терміни «двоступенева маршрутизація», «rule-based fast path»).

## 2026-02-08 — Короткий опис архітектури та LLM-агентів
- **Запит**: Зробити короткий опис архітектури застосунку та використання та функції LLM-агентів.
- **Дії**: Додано документ `docs/architecture_and_llm_agents.md`: шари застосунку (вхідна точка, клієнти, MultiAgentService, бізнес-логіка/MCP/БД, LLM), таблиця агентів (Security, Interpreter, Planner, Analyst, Assistant, LangChain-агент) з призначенням і функціями, спрощений потік запиту. Посилання на llm_agent_audit та developer_glossary.

## 2026-02-08 — LLM-інтерпретація успішності відповіді в тест-агенті
- **Запит**: Застосовувати LLM для інтерпретації, чи була відповідь помічника успішною.
- **Дії**: У `AgentTestRunnerService` додано метод `_llm_interpret_response_success`: після правил і перевірок тест-агент надсилає LLM запит із текстом запиту користувача, фрагментом відповіді помічника, фактами з БД (кількість записів за період, тип верифікації, кількість файлів) і просить один JSON `{success: true|false, reason: "..."}`. Якщо LLM повертає `success: false`, причина додається в issues і кейс вважається не пройденим. Крок «LLM оцінка успішності» додано до звіту (steps).

## 2026-02-08 — Розпізнавання невдалої відповіді тест-агентом (дані є в БД)
- **Проблема**: Тест-агент вважав успішним кейс, коли помічник відповідав «не вдалося знайти даних» за період, хоча в базі дані були.
- **Дії**: У `AgentTestRunnerService` додано список **FAILURE_PHRASES** (наприклад «не вдалося знайти даних», «не знайдено даних», «відсутність даних») та перевірку `_response_indicates_no_data_found`. Якщо за період у БД є записи (`total_expected > 0`), а відповідь містить одну з фраз невдачі — до issues додається помилка і кейс вважається не пройденим. Для типів `aggregation_or_text` без явних колекцій очікувана кількість тепер підраховується по обох колекціях (prozorro_auctions, olx_listings) за `expected_period_days`.

## 2026-02-08 — Механізм тестів за допомогою LLM тест-агента
- **Запит**: Створити механізм тестування за допомогою LLM-агента: агент генерує 5 тест-кейсів різної складності (від простої виборки до складних агрегацій), має прямий доступ до БД для перевірки результатів, запуск з меню Адміністрування; виводить «хід думок» та короткий звіт по кожному кейсу.
- **Дії**:
  - **AgentTestRunnerService** (`business/services/agent_test_runner_service.py`): тест-агент генерує 5 кейсів через LLM (або резервний набір FALLBACK_TEST_CASES), викликає MultiAgentService.process_query, перевіряє через прямий доступ до MongoDB (prozorro_auctions, olx_listings). Типи перевірки: count_in_text, report_with_files, aggregation_or_text, no_export. Результат: steps, summary, passed, issues.
  - **Меню адміністратора**: кнопка «🧪 Тестування агента»; _run_agent_test запускає run_all у ThreadPoolExecutor, надсилає звіт частинами у Telegram.
  - Використання agent_activity_log за request_id; agent_temp_exports при верифікації.

## 2026-02-08 — Контекст агентів, м’які принципи замість жорстких правил
- **Запит**: Не жорстко задавати перелік термінів і правил для агентів. Агент має отримати запит, за потреби уточнити, проаналізувати інструменти та дані, творчо обробити й надати відповідь. Додати більше контексту агентам; вони мають розуміти, коли можуть викликати та що роблять інші агенти.
- **Дії**:
  - **LangChain системний промпт**: Заміна жорсткої «ЛОГІКИ ОБРОБКИ» та «АЛГОРИТМУ» на блок **«РОЛЬ ТА КОНТЕКСТ АРХІТЕКТУРИ»** — хто обробляє запит (SecurityAgent, мультиагентний пайплайн для звітів/експортів, LangChain-агент для вільних запитів), твоя роль (викликаєш лише інструменти; планувальник і аналітик викликаються системою), підхід (отримай → уточни → проаналізуй інструменти та дані → творчо оброби). Пом’якшено формулювання про експорт (не «ОБОВ’ЯЗКОВО», а коли явно просять файл — використай інструменти; для аналітики/підсумку — достатньо тексту з числами).
  - **InterpreterAgent**: Мінімальний роутинг — needs_data і response_format встановлюються лише для intent report_last_day / report_last_week / export_data. Intent=export_data звужено до явних фраз («експорт за», «виведи оголошення за», «оголошення за тиждень» тощо), без широких «покажи», «оголошення». Для intent=query агент сам визначає потребу в даних і формат.
  - **AssistantAgent**: Числа в відповідь додаються за наявності результатів у кроках (не залежить від needs_data). Оновлено docstring з контекстом пайплайну та передачі керування LangChain-агенту.
  - **PlannerAgent, AnalystAgent, InterpreterAgent**: Розширені docstring — коли викликаються, що роблять інші агенти, хто виконує кроки.
  - **Глосарій**: оновлено термін «логіка обробки запиту» (принципи, мінімальний роутинг, агент визначає сам).

## 2026-02-08 — Фільтрація звітів за датою та регіоном (звіт за добу по джерелах / по Києву)
- **Запит**: «Сформуй звіт за добу по всім джерелам» та «Сформуй звіт за добу по Києву та Києвській області» — в обох випадках поверталась повна колекція без фільтрації.
- **Дії**:
  - **InterpreterAgent**: Додано витягування `region_filter` з тексту запиту. Фрази «по всім джерелам», «всі джерела», «всі регіони» → без обмеження; «Києву», «Києвській області», «Київ», «Київська» → `region_filter = {"region": "Київська", "city": "Київ"}`.
  - **PlannerAgent**: Для звітів/експортів `period_days` за замовчуванням 1, якщо не вказано. Новий метод `_build_save_query_params(collection, period_days, region_filter)` формує параметри для `save_query_to_temp_collection`: завжди додаються фільтри за датою (`_date_filters`); при наявності `region_filter`: для **olx_listings** — `aggregation_pipeline` з `$match` за датою (BSON) та за регіоном/містом (helper `_olx_region_city_match`); для **prozorro_auctions** — запит з `join` на `llm_cache` та фільтром по `llm_result.result.addresses` ($elemMatch за region/settlement).
  - **LangChainAgentService._save_query_to_temp_collection**: При передачі запиту з полем `join` тепер прокидується у `query_for_builder`, щоб QueryBuilder виконував join перед фільтрацією за регіоном.

## 2026-02-08 — Приховування повідомлення «AFC is enabled with max remote calls: 10»
- **Запит**: Якщо це лише інформаційне повідомлення — прибрати; якщо функціонал корисний — інтегрувати. Повідомлення виводиться клієнтом Google Gemini (Automatic Function Calling, ліміт 10 викликів за запит).
- **Дії**: AFC — це вже використовуваний механізм (агент викликає tools); повідомлення лише інформаційне. Прибрано з виводу: (1) фільтр логування `_SuppressAfcInfoFilter` на root handler — приховує записи, що містять «AFC» та «max remote calls»; (2) рівень логів для `google`, `google.genai`, `langchain_google_genai` встановлено в WARNING. Функціонал AFC залишається увімкненим.

## 2026-02-08 — Гайдбук для агента-інтерпретатора
- **Запит**: Створити гайдбук для агента-інтерпретатора з описом формату інформації для збереження в БД: формати дат, мова, формат опису, збереження та вивід топонімів тощо.
- **Дії**: Додано `docs/interpreter_agent_handbook.md`: мова (українська), формати дат (ISO 8601 для ProZorro, BSON Date для OLX, вивід локалізований), структурований вивід парсингу (поля, addresses, tags), топоніми та адреси (збереження й геокодування), числа та одиниці, посилання на data_dictionary та llm_service. У InterpreterAgent у docstring та в глосарій додано посилання на гайдбук.

## 2026-02-08 — Тестові кейси-запити для відладки агента
- **Запит**: Агент без запиту вивантажив вибірку даних; потрібні тестові кейси для відладки всіх змін.
- **Дії**:
  - **Документ** `docs/agent_test_queries.md`: описані кейси — (1) аналітика лише текстом без експорту, (2) звіт/експорт за період (очікуємо файли), (3) запити без вибірки, (4) відповідь з числовими даними, (5) перевірка інтерпретатора, (6) регресія «не експортувати без явного запиту».
  - **Скрипт** `scripts/run_agent_test_queries.py`: прогон кейсів через MultiAgentService. Опції: `--quick` (лише інтерпретатор, без LLM), `--filter <substring>` (фільтр по id або тексту). Перевірки: expect_no_files (FAIL якщо з’явились файли), expect_numbers_in_text (WARN). Запуск з кореня проекту: `py scripts/run_agent_test_queries.py`.

## 2026-02-08 — Завершення циклу LangChain-агента при порожній відповіді після execute_analytics
- **Проблема**: Запит «аналітика зміни цін за кв. м. в Києві та області за тиждень по дням» — агент викликав execute_analytics (ProZorro 0, OLX 3 записи), після чого 8 ітерацій повертав 0 tool_calls і порожній контент, досягнувши max ітерацій без фінальної відповіді.
- **Дії**:
  - Якщо немає викликів tools і відповідь порожня — один раз додається підказка (HumanMessage): просити фінальну відповідь українською з числовими даними, без виклику інструментів; наступна ітерація має повернути текст.
  - execute_analytics: лог-повідомлення змінено з WARNING на INFO («якщо користувач просив файл — викликай export; інакше підсумуй текстом»). Підказку агенту (_agent_hint) виведено на початок контенту ToolMessage («ІНСТРУКЦІЯ: … Наступним повідомленням дай лише фінальну текстову відповідь»), щоб модель бачила її першою.

## 2026-02-08 — Логіка обробки запиту агентом-помічником
- **Запит**: Запит користувача повинен оброблятися за логікою: який запит; чи потребує отримання інформації; у якому вигляді відповідь (файли, текст повний/загальний); навіть коротка текстова відповідь має бути підкріплена числовими даними при вибірці.
- **Дії**:
  - **InterpreterAgent**: У результат `interpret_user_query` додано поля `needs_data` (чи потребує запит вибірки/звіту) та `response_format` (files | full_text | general_text) за ключовими словами.
  - **AssistantAgent**: При наявності Excel-файлів у відповідь додається підсумок «Усього записів у файлах: N». У `_format_response_from_results` для запитів з `needs_data` додаються числові підсумки з результатів кроків (count, rows_count).
  - **LangChainAgentService**: У системний промпт додано блок «ЛОГІКА ОБРОБКИ ЗАПИТУ КОРИСТУВАЧА» — чотири кроки (запит, потреба в інформації, форма відповіді, обов'язкові числові дані у відповіді при вибірці).
  - **Глосарій**: додано термін «логіка обробки запиту (агента-помічника)».

## 2026-02-08 — Аудит LLM-агентів та best practices
- **Запит**: Провести загальний аудит застосунку та застосувати best practice по роботі з LLM-агентами.
- **Дії**:
  - **Документація аудиту** (`docs/llm_agent_audit.md`): опис поточної архітектури (MultiAgentService, LangChainAgentService, LLMAgentService, агенти), звірка з best practices (tool use, пам'ять, observability, безпека, обмеження ресурсів, обробка помилок, простота архітектури). Виявлені прогалини та виконані зміни задокументовані; рекомендації на майбутнє (request_id, retry в agent loop, метрики).
  - **Mini App — єдина точка входу**: Запити з Telegram Mini App тепер обробляються через **MultiAgentService** замість прямого виклику LangChainAgentService. Застосовується перевірка безпеки (SecurityAgent); при порушенні подія логується через `log_app_event` (event_type=security_incident), оскільки в контексті Mini App немає Telegram-бота для сповіщень адмінам. Excel-файли повертаються через `get_last_excel_files()`.
  - **Конфігурація агента**: У `config/settings.py` та `config.example.yaml` додано опційні параметри `llm.agent.max_iterations`, `llm.agent.max_output_tokens`, `llm.agent.temperature` (змінні оточення: `LLM_AGENT_MAX_ITERATIONS`, `LLM_AGENT_MAX_OUTPUT_TOKENS`, `LLM_AGENT_TEMPERATURE`). **LangChainAgentService** та **LLMAgentService** використовують ці значення замість захардкоджених констант.
  - **Глосарій**: додано терміни «agent loop (цикл агента)», «tool call (виклик інструменту)», «iteration limit (ліміт ітерацій)», «guardrails (обмеження безпеки)».
- **Подальше виконання рекомендацій аудиту (той самий день)**:
  - **request_id**: Опційний параметр `request_id` у `MultiAgentService.process_query` та `LangChainAgentService.process_query`; якщо не передано — генерується. Telegram-бот і Mini App генерують UUID і передають у `process_query`. У логах LangChain — префікс `[request_id=...]` для трасування; у `agent_activity_log` request_id вже використовувався.
  - **Retry в agent loop**: У `LangChainAgentService` додано retry при тимчасових помилках LLM: константи `AGENT_LLM_RETRY_ATTEMPTS` (2), `AGENT_LLM_RETRY_BACKOFF_SECONDS` (1.0, 2.0), функція `_is_transient_llm_error()` (503, 429, quota, timeout, connection, unavailable). Retry застосовується до `llm_with_tools.invoke()` у циклі та до виклику фінальної відповіді після max ітерацій.
  - **Метрики**: У MultiAgentService після обробки записується крок STEP_RESPONSE з `duration_ms`, `path` (multi_agent | langchain), для шляху langchain — також `iterations` з `LangChainAgentService._last_request_metrics`. У LangChainAgentService перед кожним return встановлюється `_last_request_metrics` (iterations, duration_seconds); у finally process_query логується завершення з duration_sec. Реалізація винесена в `_process_query_impl` для єдиного finally.

## 2026-02-08 — Кастомні метрики аналітики (формули від агента)
- **Запит**: Не жорстко задавати метрики; агент, відповідальний за збір аналітики, має мати можливість самостійно створювати метрики з формулами розрахунків. Створені метрики можна хешувати; дати агенту більше творчої свободи для комплексних метрик.
- **Дії**:
  - **Парсер формул** (`utils/analytics_formula.py`): безпечний вираз над полями документів. Дозволені шляхи — лише `auction_data.*` та `llm_result.result.*`; оператори +, -, *, /; дужки та числа. Парсер будує AST і перетворює його на MongoDB aggregation expression ($divide, $multiply, $cond для безпечного ділення). Функції: parse_formula, formula_to_mongo_expr, formula_references_llm, formula_hash.
  - **Підтримка кастомної метрики в запиті**: у execute_analytics поле `metric` може бути рядком (назва вбудованої метрики) або об'єктом `{ name?, formula, aggregation?, description?, unit? }`. formula — рядок виразу; aggregation: avg (за замовч.) | sum | min | max. Валідація формули при прийнятті запиту; при побудові pipeline для кастомної метрики: needs_llm_lookup з formula_references_llm(formula), _metric_value = formula_to_mongo_expr(formula).
  - **AnalyticsBuilder**: _is_custom_metric, _normalize_metric_spec; validate_analytics_query приймає кастомний metric і перевіряє формулу; build_pipeline гілка is_custom; execute_analytics_query відповідь з metric_display, metric_desc, metric_unit для кастомних.
  - **MCP та агент**: оновлено опис execute_analytics (analytics_mcp_server, langchain_agent_service) та системний промпт — агент може вигадувати метрики з формулами над auction_data та llm_result.
- **Глосарій**: додано «кастомна метрика (аналитика)».

## 2026-02-08 — Аналітика ціни за м² по OLX (Київ та область)
- **Запит**: За «аналітика зміни цін за кв. м. в Києві та області за останній тиждень по днях» результатів не знайдено (execute_analytics по ProZorro повертав 0).
- **Дії**:
  - **execute_analytics для OLX**: Підтримка колекції `olx_listings`. Параметр `collection: "olx_listings"` — аналітика середньої ціни за м² (search_data.price / detail.llm.building_area_sqm), groupBy: ["date"]. Фільтри: діапазон дат (auction_data.dateModified або updated_at), регіон/місто через $or (region/city) — зіставлення по detail.resolved_locations та search_data.location. У analytics_builder: валідація для olx_listings, _build_olx_price_per_sqm_pipeline, _execute_olx_analytics, _parse_iso_to_datetime, _build_olx_region_city_match.
  - **Агент**: У промпті та описі execute_analytics — при 0 по ProZorro для ціни за м² викликати той самий запит з collection: "olx_listings". У _execute_analytics при count=0 додається _agent_hint з підказкою спробувати olx_listings.
  - **MCP analytics_mcp_server**: Оновлено опис execute_analytics (параметр collection, приклади ProZorro/OLX).

## 2026-02-08 — Сервіс-планувальник подій
- **Запит**: Сервіс-планувальник для планування подій на майбутнє: оновлення даних з джерел (тільки для адмінів), регламентне формування звітів (файл або текст за розкладом), нагадування (разові/постійні).
- **Дії**:
  - **Репозиторій та міграція**: `ScheduledEventsRepository`, колекція `scheduled_events` (поля: event_type, scope, user_id, created_by, schedule, payload, is_active, last_run_at, next_run_at). Міграція 008.
  - **SchedulerService**: використовує APScheduler (BackgroundScheduler, timezone=Europe/Kyiv). Типи подій: `data_update` (ProZorro + OLX за payload.sources та days), `scheduled_report` (ReportGenerator + доставка файлом або текстом через notifier), `reminder` (текст у Telegram; при разовому — деактивація). При створенні події типу data_update перевіряється, що created_by — адміністратор.
  - **Розклад**: cron (minute, hour, day_of_week, day, month) або once (run_at). Після виконання оновлюються last_run_at та next_run_at.
  - **SchedulerNotifier / TelegramSchedulerNotifier**: інтерфейс для відправки повідомлень і файлів з потоку планувальника. У TelegramBotService додано збереження event loop у post_init та методи `send_message_to_chat_sync`, `send_document_to_chat_sync` (run_coroutine_threadsafe у потоку бота).
  - **ReportGenerator.get_report_data**: отримання даних звіту без генерації файлу для текстової доставки регламентних звітів.
  - **Інтеграція**: у main.py при запуску Telegram бота створюються SchedulerService з TelegramSchedulerNotifier, задається UserService для перевірки is_admin, планувальник стартує після бота; при stop() — shutdown планувальника.
- **Глосарій**: додано «сервіс-планувальник», «регламентне формування звітів», «запланована подія».

## 2026-02-08 — Аналітика «по днях»: один виклик замість восьми; виправлення фільтрів дат
- **Проблема (лог)**: Запит «аналітика зміни цін за кв.м в Києві та області за останній тиждень по дням» — агент викликав execute_analytics 8 разів (по одному на день), ліміт 5 обрізав до 5 викликів за ітерацію; усі виклики повертали total_count=0; далі 6 ітерацій без викликів tools до вичерпання ліміту.
- **Дії**:
  - **groupBy date**: Додано групування за днем у аналітиці. У `analytics_metrics.ALLOWED_GROUP_BY_FIELDS` додано **date**; у `analytics_builder.build_pipeline` для `field == 'date'` додано `_group_date` через `$substr` від `auction_data.dateModified` (YYYY-MM-DD). Для звітів «по днях» агент має використовувати один виклик execute_analytics з **groupBy: ["date"]** та фільтрами за діапазоном дат і регіоном/містом.
  - **Фільтри дат**: У `_build_match_filters` фільтр з ключем **auction_data.dateModified** або **auction_data.dateCreated** тепер зберігається як є (без подвійного префікса auction_data.). Для ключів, що вже починаються з `auction_data.`, префікс не дублюється. Це усувало причину total_count=0 при коректних даних.
  - **Ліміт викликів**: `MAX_TOOL_CALLS_PER_ITERATION` збільшено з 5 до 10.
  - **Системний промпт та опис інструмента**: Зазначено використання groupBy: ["date"] для звітів по днях; у описі execute_analytics додано groupBy (date, region, city тощо) та фільтр auction_data.dateModified.
  - **Логування**: Для execute_analytics у лог підставляється `count` (кількість рядків результатів), якщо немає `total_count`; для підрахунку записів використовується ключ `results`, а не `data`.

## 2026-02-08 — Команди оновлення даних у меню адміністратора; вимкнено регламентне оновлення
- **Запит**: Додати в меню адміністрування команди оновлення даних за добу і за тиждень; тимчасово відключити регламентне оновлення даних.
- **Дії**:
  - У меню адміністратора (Telegram бот) додано кнопки «🔄 Оновити дані за добу» та «🔄 Оновити дані за тиждень». Обробники запускають оновлення ProZorro та OLX у фоновому потоці; після завершення адміну надсилається підсумок (успіх/помилка, для OLX — кількість оголошень та деталей).
  - Регламентне фонове оновлення даних у `main.run_telegram_bot()` тимчасово вимкнено (закоментовано запуск потоку `_background_data_update_loop`). Оновлення виконується лише вручну через меню адміністратора.

## 2026-02-08 — Теги оголошень (агент-інтерпретатор та фільтрація для аналітика)
- **Запит**: Покращити парсинг описів — додати набір тегів (призначення: крамниця, аптека; комунікації: газ, вода тощо) для подальшої фільтрації агентом-аналітиком (наприклад «усі аптеки», «об'єкти з газом»).
- **Дії**:
  - **Парсинг**: У `BaseLLMProvider._create_parsing_prompt()` додано поле **tags** — масив тегів (нижній регістр): призначення (крамниця, аптека, офіс, склад, кафе, ресторан тощо), комунікації (газ, вода, електрика, каналізація, опалення, інтернет), інше (ремонт, паркінг). У всіх провайдерах (Gemini, OpenAI, Anthropic) у `_empty_result()` та `_normalize_result()` додано `tags` (нормалізація: список рядків, lower, без дублікатів).
  - **Зберігання**: Теги потрапляють у результат парсингу (OLX — `detail.llm.tags`, ProZorro llm_cache — `result.tags`).
  - **Фільтрація для аналітика**: У `QueryBuilder.get_distinct_values()` додано параметр **unwrap_array**: при `True` виконується `$unwind` по полю, потім `$group` — для отримання унікальних елементів масиву (наприклад унікальні теги). Інструмент та MCP `get_distinct_values` приймають `unwrap_array`; для фільтра за тегами: `get_distinct_values(olx_listings, detail.llm.tags, unwrap_array=True)`, потім `$match` з `"detail.llm.tags": {"$in": ["крамниця", "газ"]}`.
  - **Довідка**: У системному промпті агента та описі інструмента зазначено фільтрацію за тегами та використання unwrap_array для тегів. У `config/data_dictionary.yaml` додано поле **detail_llm_tags** (масив тегів, джерело detail.llm.tags).
- **Глосарій**: додано термін «теги (оголошень)».

## 2026-02-08 — Геокодування українською; аналіз поля перед фільтрацією
- **Геокодування**: Відповідь Google Geocoding API приходила латиницею. У geocoding_service додано параметр запиту **language=uk** (DEFAULT_LANGUAGE), щоб formatted_address та address_components поверталися українською (кирилиця).
- **Аналіз поля перед фільтром**: Щоб агент не ставив лише один варіант фільтра (напр. «Київська область») і не втрачав записи з іншими написаннями (Київська обл., латиницею тощо), додано інструмент **get_distinct_values(collection_name, field_path, limit)**. Він повертає список унікальних значень поля (наприклад search_data.location в olx_listings). У системному промпті та описі інструмента зазначено: перед фільтрацією за регіоном/локацією обов'язково викликати get_distinct_values, побачити які значення є в даних, і побудувати $match з $in або $or з усіма варіантами, що логічно відповідають запиту. Реалізація: QueryBuilder.get_distinct_values (aggregation $group за полем), tool у langchain_agent_service та query_builder_mcp_server.

## 2026-02-08 — save_query_to_temp_collection ігнорував aggregation_pipeline
- **Проблема**: Запит «оголошення OLX по Київській області, відсортовані за ціною за кв.м» — агент передавав save_query_to_temp_collection з aggregation_pipeline ($match за регіоном, $addFields price_per_sqm, $sort), але в результаті користувач отримував усі оголошення без фільтрації та сортування.
- **Причина**: У _save_query_to_temp_collection враховувалися лише collection, filters та limit; параметр aggregation_pipeline ігнорувався. Бралися filters з query (їх не було), виконувався execute_query з порожніми filters → поверталися всі документи.
- **Дії**: У langchain_agent_service._save_query_to_temp_collection додано гілку: якщо в запиті є aggregation_pipeline (непорожній список), викликається execute_aggregation(collection, pipeline, limit), результати зберігаються в тимчасову вибірку. Інакше — як раніше execute_query за filters та limit. Опис інструмента оновлено: зазначено підтримку aggregation_pipeline для фільтрації та сортування.

## 2026-02-08 — Кешування парсингу оголошень OLX
- **Запит користувача**: Оголошення OLX кожен раз проганяються через LLM; кеш не працює адекватно.
- **Дії**:
  - **Причина 1**: У `search_data_changed` враховувалися `date_text` та `listed_at_iso`. Вони змінюються з часом («Вчора» → «Сьогодні»), тому кожен запуск вважав дані змінились і робив refetch деталей + виклик LLM. З порівняння прибрано `date_text` та `listed_at_iso` — тепер refetch лише при зміні title, price, location, area_m2.
  - **Причина 2**: Хеш для кешу обчислювався лише через `strip()`; різне форматування (пробіли, порожні рядки) давало різні хеші. У `utils/hash_utils.py` додано `_normalize_description_for_hash`: обрізка рядків, прибирання порожніх — однаковий контент дає один хеш.
  - **Детермінованість тексту**: У `OlxLLMExtractorService._build_description_text` параметри об'єкта тепер сортуються за (label, value), щоб один і той самий набір параметрів завжди давав однаковий текст і потрапляв у кеш.

## 2026-02-08 — Мультиагентна архітектура (помічник, планувальник, аналітик, інтерпретатор, безпека)
- **Запит користувача**: Перейти на мультиагентну архітектуру: агент-помічник (спілкування, контекст, виклик інших агентів), агент-аналітик даних (запити до БД, тимчасові вибірки, ітеративна обробка), агент-інтерпретатор (розбір запитів, пояснення метрик, структурована інформація з тексту описів), агент безпеки (верифікація шкідливих/експлойтних запитів, сповіщення адмінів у TG), агент-планувальник (план кроків при складних пайплайнах). Інструменти — як MCP-сервери. Логування наміру помічника та дій під-агентів у БД.
- **Дії**:
  - **Репозиторій логів**: `AgentActivityLogRepository` та колекція `agent_activity_log` (request_id, user_id, agent_name, step: intent|action|response, payload, created_at). Міграція 007.
  - **Агенти**: Пакет `business/agents/` — `SecurityAgent` (патерни заборонених запитів, callback сповіщення адмінів), `InterpreterAgent` (interpret_user_query за ключовими словами, extract_structured_from_text через LLM+кеш, explain_metric), `PlannerAgent` (план кроків: save_query_to_temp_collection, export_from_temp_collection тощо), `AnalystAgent` (виконання кроків через run_tool), `AssistantAgent` (оркестрація: інтерпретатор → лог наміру → планувальник → виконання → відповідь).
  - **Інструменти**: У `LangChainAgentService` додано `run_tool(tool_name, tool_args)` для виклику інструментів з мультиагентної системи (ті самі MCP-backed операції).
  - **MultiAgentService**: Вхідна точка обробки запиту: перевірка безпеки → формування контексту з пам'яті → інтерпретація → для простих намірів (звіт за добу/тиждень, експорт) — пайплайн через планувальника та аналітика; для складних — fallback на одного LangChain-агента. Логування в agent_activity_log. Метод `get_last_excel_files()` для відправки файлів у Telegram.
  - **Telegram**: Бот використовує `MultiAgentService` замість лише `LangChainAgentService`. При ініціалізації передається `notify_admins_fn`, що викликає `_notify_admins_async` (відправка повідомлення усім адмінам при спрацьовуванні агента безпеки). `UserService.get_admin_user_ids()` для списку адмінів.
  - **Парсинг описів**: Агент-інтерпретатор має `extract_structured_from_text(text)` (LLM + llm_cache); існуючий механізм ProZorro/OLX залишається на LLMService.parse_auction_description — концептуально відповідальність за структуру з тексту на інтерпретаторі, при потребі джерела можна перевести на виклик інтерпретатора.

## 2026-02-07 — Telegram Mini App (дубль функціональності бота)
- **Запит користувача**: Перевести телеграм бота в режим міні-застосунка Telegram Mini App; поки що створити застосунок та дублювати існуючу функціональність.
- **Дії**:
  - **Бекенд**: Додано пакет `telegram_mini_app/`: валідація `initData` від Telegram (HMAC-SHA256) у `auth.py`; FastAPI-сервер у `server.py` з маршрутами `/api/me`, `/api/llm/chat`, `/api/admin/*`, `/api/files/*`. Користувач визначається з валідованого initData; перевірка авторизації та ролі через існуючий `UserService`. LLM-чат викликає `LangChainAgentService.process_query`; адмін-дії — додати/заблокувати користувача, отримати/завантажити конфіг ProZorro; файли — скачати звіт з БД (1/7 днів), запустити формування за 7 днів (асинхронна задача з опитуванням статусу та завантаженням ZIP).
  - **Фронтенд**: У `telegram_mini_app/static/` додано односторінковий застосунок (index.html, app.js, styles.css): підключення Telegram Web App SDK, передача initData в заголовку `X-Telegram-Init-Data`; головний екран з чатом LLM; розділи «Звіти» (завантажити звіт за 1/7 днів, сформувати за 7 днів); для адмінів — «Адміністрування» (додати користувача/адміна, заблокувати, ProZorro config).
  - **Інтеграція**: У `config/settings.py` та `config.example.yaml` додано `mini_app_port` (за замовчуванням 8000) та `mini_app_base_url` (HTTPS для BotFather). При запуску Telegram бота (`main.run_telegram_bot`) додатково запускається uvicorn на `mini_app_port`; у боті в `post_init` встановлюється кнопка меню «Відкрити застосунок» (MenuButtonWebApp) за наявності `mini_app_base_url`. Залежності: fastapi, uvicorn.
  - **Глосарій**: додано терміни «Telegram Mini App», «initData».

## 2026-02-07 — Пусті рядки в Excel та ліміт за замовчуванням для звіту за період
- **Запит користувача**: Результати звіту за добу пусті; не задавати ліміти, якщо їх не вказав користувач («звіт за добу» — не «100 оголошень за добу»).
- **Дії**:
  - **Пусті рядки**: У тимчасову вибірку потрапляло 7 документів ProZorro, а в Excel — 1 рядок. Причина: LLM передавав `projection: []`; у query_builder при будь-якій наявності `projection` додавався $project, і порожній список давав лише `_id` — без `auction_data` експорт не міг сформувати рядки. У `utils/query_builder.py` projection stage додається лише якщо `query['projection']` непорожній; порожній або відсутній projection = повні документи. У `langchain_agent_service._save_query_to_temp_collection` у `query_for_builder` projection передається тільки якщо користувач явно задав непорожній список полів; інакше для експорту використовуються повні документи.
  - **Ліміт**: Замість дефолтного limit=100 при відсутності в запиті — для звітів за період використовується верхня межа 5000 (користувач не вказував «100 за добу»). У агенті: якщо limit не заданий — `limit_val = 5000`; якщо заданий — `min(limit_val, 5000)`. У `utils/query_builder.py` `MAX_RESULTS` змінено з 100 на 5000.

## 2026-02-07 — Регламентне оновлення OLX: передача days=1
- **Запит користувача**: Регламентний обмін вибирає забагато; перевірити параметри.
- **Дії**: У регламентному циклі (_background_data_update_loop) викликався run_olx_data_update() без параметра days, тому run_olx_update працював з days=None: cutoff_utc не встановлювався, використовувалося max_pages=100 і скрапер обходив до 100 сторінок замість зупинки по даті «минула добу». Виправлено: run_olx_data_update(days=None) приймає опційний days; при виклику з циклу передається run_olx_data_update(days=1). При days=1 зупинка по cutoff_utc (оголошення старші за добу не збираються), max_pages не обмежує.

## 2026-02-07 — Зміна загального системного промпту: аналіз запиту, план, уточнення
- **Запит користувача**: Змінити загальний системний промпт. Спочатку агент має аналізувати запит. Якщо впевнений, що зрозумів — складає план з використанням інструментів, аналізує його та або втілює, або повідомляє чому неможливо. Якщо запит не зрозумілий — перепитати чи уточнити, використовуючи історію діалогу для контексту.
- **Дії**: У системний промпт (langchain_agent_service._get_system_prompt) додано секцію «АЛГОРИТМ РОБОТИ З ЗАПИТОМ» на початку: (1) Аналіз запиту з урахуванням історії діалогу; (2) Якщо зрозуміло — план інструментів, аналіз плану, виконання або пояснення чому неможливо; (3) Якщо не зрозуміло — перепитати/уточнити, використовуючи контекст бесіди. Підсумок: не пропускати етап аналізу.

## 2026-02-07 — Порожні файли вибірки: нормалізація дат для OLX
- **Запит користувача**: Файли вибірки знов пусті.
- **Дії**: У olx_listings поле updated_at зберігається як BSON Date; при фільтрі з рядками ISO ($gte/$lte) MongoDB не завжди коректно порівнює. У utils/query_builder.py додано _parse_iso_to_utc та _normalize_date_filters: перед побудовою $match для колекції olx_listings значення фільтра updated_at (рядки ISO) перетворюються на datetime (UTC), щоб порівняння з BSON Date давало результати. Інші колекції (prozorro_auctions — dateModified як рядок) не змінюються.

## 2026-02-07 — Схема та опис полів OLX для агента (MCP та Data Dictionary)
- **Запит користувача**: Оновити MCP сервіси, щоб агент бачив адекватну структуру і опис полів для джерела OLX.
- **Дії**:
  - **config/data_dictionary.yaml**: Розширено секцію olx_listings — повний опис полів (url, search_data з title, price, location, price_text, price_value, area_m2; detail з description, llm.property_type, llm.building_area_sqm, llm.land_area_ha, resolved_locations, parameters; created_at, updated_at). Додано індекс updated_at; flattened_fields для підказок агенту (url, updated_at, search_data_*, detail_llm_*). У description колекції вказано використання updated_at (BSON Date) для фільтра за періодом та get_collection_info для повної схеми.
  - **query_builder_mcp_server**: get_allowed_collections повертає для кожної колекції об'єкт {id, description}; для olx_listings — опис полів (url, search_data, detail.llm.*, updated_at) та посилання на get_collection_info('olx_listings').
  - **langchain_agent_service**: _get_allowed_collections повертає той самий формат (collections як список {id, description}); опис tool get_allowed_collections доповнено підказкою про get_collection_info для повної схеми.

## 2026-02-07 — Експорт через тимчасову вибірку (агент не отримує результати запиту в контекст)
- **Запит користувача**: Змінити логіку: замість напряму отримувати результати запиту агент має створювати методами сервера тимчасову колекцію і давати команду на експорт у файл з неї.
- **Дії**:
  - **Тимчасові вибірки**: Додано колекцію `agent_temp_exports` та репозиторій `AgentTempExportsRepository` (insert_batch(batch_id, source_collection, docs), get_batch(batch_id), delete_batch(batch_id)). Міграція 006.
  - **Інструменти сервера**: У query-builder-mcp — `save_query_to_temp_collection(query)`: виконує запит, зберігає результати в тимчасову вибірку, повертає `temp_collection_id` та `count`. У export-mcp — `export_from_temp_collection(temp_collection_id, format, filename_prefix)`: експортує вибірку в Excel, повертає file_base64 для Telegram, після експорту видаляє тимчасові дані.
  - **ProZorroService**: метод `get_standard_sheet_data_for_export_from_docs(docs, source_collection)` для формування листа з уже отриманих документів (без get_by_ids).
  - **Агент**: інструменти `save_query_to_temp_collection` та `export_from_temp_collection`; системний промпт змінено: основний спосіб експорту результатів запиту — не execute_query + export_listings_to_file(ids), а save_query_to_temp_collection(query) → export_from_temp_collection(temp_collection_id). Результати запиту не потрапляють у контекст агента — лише ідентифікатор вибірки та кількість записів.
  - Глосарій: термін «тимчасова вибірка (агента)» — збережені на сервері результати запиту під batch_id для подальшого експорту в файл без передачі їх у контекст LLM.

## 2026-02-07 — У файл лише 10 оголошень з ProZorro; відсутність OLX в експорті
- **Запит користувача**: У файл виводиться лише 10 оголошень з ProZorro. Де інші аукціони? Де інфо з OLX?
- **Дії**:
  - **Ліміт 10**: У прикладі MCP execute_query було "limit": 10 — LLM копіював і повертав лише 10 записів. Крім того, якщо LLM передавав limit всередині об'єкта query, він потрапляв у filters і потім у $match як умова «поле limit = 10», що спотворювало вибірку. У langchain_agent_service при нормалізації execute_query: (1) з filters виключено ключ limit (щоб не потрапляв у $match); (2) limit передається окремо в query_for_builder, за замовчуванням 100; (3) якщо LLM передає limit, використовується min(limit, 100). У query_builder_mcp_server приклад змінено на limit: 100 і підказка «для експорту за період передавай limit: 100».
  - **Системний промпт та опис tool**: Для execute_query вказано, що за замовчуванням limit 100 і для експорту «усіх» за період потрібно передавати limit: 100. Додано правило: при запиті «оголошення за період» / «всі оголошення» / «експорт за добу» враховувати обидва джерела — виконати execute_query (з limit: 100) для prozorro_auctions та окремо для olx_listings, потім викликати export_listings_to_file двічі (два файли: ProZorro та OLX).

## 2026-02-07 — Обрізання великих результатів execute_query/execute_aggregation (ліміт токенів LLM)
- **Запит користувача**: Помилка в терміналі — після отримання 100 результатів execute_query контекст перевищував ліміт токенів Gemini (1048576), виклик LLM падав з INVALID_ARGUMENT.
- **Дії**:
  - У `langchain_agent_service.py` перед створенням ToolMessage для execute_query та execute_aggregation додано обрізання великих результатів: якщо записів більше 20, у контекст передається не повний список документів, а скорочена відповідь — success, count, ids_for_export (список auction_id/url), _agent_hint. Агент отримує підказку використовувати ids=ids_for_export для виклику export_listings_to_file, що уникнення перевантаження контексту та падіння через ліміт токенів.

## 2026-02-07 — Оптимізація промптів та механізмів агента (три категорії інструментів, шарова архітектура)
- **Запит користувача**: Оптимізувати промпти й механізми, щоб LLM-агент ефективно працював з інструментами. Інструменти: ініціація обмінів (з параметрами), вибірка/агрегація/інтелектуальна обробка, збереження у файл і відправка (структура від користувача або стандартна). Агент має зберігати інформацію про діалог, отримувати контекст попередніх бесід, використовувати та оновлювати глосарій. Шарова логіка: агент — найвищий шар, бізнес-логіка — нижчий.
- **Дії**:
  - Системний промпт переписано за трьома категоріями: (1) Ініціація обмінів — trigger_data_update; (2) Вибірка, агрегація та інтелектуальна обробка — схема, execute_analytics, execute_query, execute_aggregation, geocode_address, правила дат/безпеки; (3) Збереження у файл і відправка — export_listings_to_file, save_query_results_to_excel, generate_report. Додано секцію «Діалог, контекст та глосарій» (пам'ять через сервіси, контекст попередніх бесід, термінологія; при наявності інструментів оновлення глосарію — використовувати).
  - Описи інструментів скорочено та уніфіковано з префіксами [Схема], [Вибірка], [Метрики], [Контекст], [Оновлення], [Експорт], [Звіт].
  - У docstring модуля langchain_agent_service та в глосарій додано: шарова архітектура (агент — найвищий шар, бізнес-логіка — нижчий; нові сервіси додаються через інструменти), три категорії інструментів агента.

## 2026-02-07 — Пошук даних агентом та стандартний формат експорту
- **Запит користувача**: (1) Оновлював дані, але бот їх не знаходить. (2) Формат виведення не той, який зазвичай отримую.
- **Дії**:
  - **Пошук даних**: У колекції prozorro_auctions поля auction_data.dateModified та auction_data.dateCreated зберігаються як рядки ISO 8601, а не BSON Date. Агент формував $match з {"$date": "..."}, через що MongoDB не знаходив записів. У контекст дати/часу в process_query додано явну вказівку: для prozorro_auctions використовувати порівняння рядків, наприклад "auction_data.dateModified": {"$gte": "2026-02-06T00:00:00.000Z", "$lte": "..."}, не використовувати {"$date": "..."}.
  - **Стандартний формат**: Користувач очікує той самий формат Excel, що й при «формуванні файлу за день/тиждень» (колонки: Дата оновлення, Область, Населений пункт, Адреса, Тип нерухомості, площі, ціна, посилання тощо). У ProZorroService додано get_standard_sheet_data_for_export(ids, collection) та generate_excel_bytes_for_export(ids, collection, filename_prefix); константи STANDARD_EXPORT_FIELDNAMES_* та STANDARD_EXPORT_HEADERS_* для ProZorro та OLX. У LangchainAgentService при виклику export_listings_to_file без параметра columns використовується цей стандартний формат (через prozorro_service), інакше — ExportDataService.export_to_file. Оновлено опис tool: за замовчуванням — стандартний формат, columns не передавати.

## 2026-02-07 — Відправка файлу користувачу після export_listings_to_file
- **Запит користувача**: Виправити механізм збереження даних у файли LLM-агентом. Очікуваний потік: агент через MCP отримує перелік оголошень → викликає MCP з переліком для збереження у файл (отримує розташування) → щось відправляє сформований файл користувачу.
- **Дії**:
  - У `business/services/langchain_agent_service.py`: після успішного виклику `export_to_file` у `_export_listings_to_file` додано читання створеного файлу з диску та додавання до результату поля `file_base64`. Результат tool потрапляє в ToolMessage; існуючий метод `_extract_excel_files_from_history` шукає в історії повідомлення з `success`, `file_base64` та `filename` — тепер знаходить їх і Telegram-обробник відправляє файл через `send_document`. Оновлено опис tool та системний промпт: після виклику export_listings_to_file файл автоматично відправляється користувачу, агент лише повідомляє про це.
  - У глосарій додано термін «відправка файлу агентом».

## 2026-02-07 — Регламентне фонове оновлення даних (кожні 10 хв)
- **Запит користувача**: Додати механізм регламентного, фонового оновлення інформації із джерел. Кожні 10 хвилин роботи застосунку оновлювати у фоні дані за минулу добу.
- **Дії**:
  - У `config/settings.py`: додано `background_update_interval_minutes` (env `BACKGROUND_UPDATE_INTERVAL_MINUTES`, за замовчуванням 10); підтримка секції `background_update.interval_minutes` у config.yaml. Значення 0 вимикає фоновий оновлення.
  - У `main.py`: додано потік `_background_data_update_loop` — кожні N хвилин викликає `fetch_real_estate_auctions(days=1)` та `run_olx_data_update()`. Потік запускається при старті Telegram бота (якщо інтервал > 0); перед цим викликається `initialize()` для наявності ProZorro-сервісу.
  - У `config/config.example.yaml` додано приклад секції `background_update`. У глосарій додано термін «регламентне оновлення даних».

## 2026-02-07 — Агент: робота з таблицею OLX та сервер оновлення даних
- **Запит користувача**: Додати агенту можливість по аналогії з ProZorro працювати з таблицею оголошень OLX; додати сервер, який агент викликає для ініціювання оновлення даних у базу.
- **Дії**:
  - У `config/data_dictionary.yaml` додано колекцію `olx_listings` (url, search_data, detail, created_at, updated_at) для схеми та MCP.
  - У `utils/query_builder.py` додано `olx_listings` до `ALLOWED_COLLECTIONS` — агент може виконувати execute_query та execute_aggregation по OLX.
  - У `mcp_servers/schema_mcp_server.py` додано `olx_listings` до allowed_collections.
  - У `business/services/langchain_agent_service.py`: оновлено системний промпт (колекції prozorro_auctions, llm_cache, olx_listings; опис OLX та export по url); додано tool `trigger_data_update(source, days)` — для source="olx" викликає `run_olx_update`, для source="prozorro" повертає підказку про пайплайн/Telegram. Реалізовано `_trigger_data_update`.
  - Створено MCP сервер `mcp_servers/data_update_mcp_server.py`: інструменти `trigger_olx_update(days)` та `get_data_update_sources()`. Реєстрація в `main.py` та `scripts/start_mcp_servers.py`. Документація в `docs/mcp_servers.md`.

## 2026-02-07 — Telegram: лише адміністрування в меню, весь текст через LLM, пам'ять агента
- **Запит користувача**: Прибрати з інтерфейсу Telegram усі пункти меню крім адміністрування. Текстове спілкування з ботом завжди опрацьовувати LLM-асистентом. Реалізувати елементи VectorStoreRetrieverMemory, ConversationSummaryMemory та ConversationBufferMemory для орієнтування агента в історії; агент має опрацьовувати загальні відповіді та відповіді на конкретні повідомлення (наприклад додатковий аналіз файлу, зміна запиту).
- **Дії**:
  - У `business/services/telegram_bot_service.py`: головне меню показує лише кнопку «Адміністрування» (для адмінів); видалено пункти скачування/формування файлів та «Спитати LLM». Будь-який текст (крім кнопок адмін-меню) спрямовується в `_handle_text_with_llm`; підтримка `reply_to_message` — текст цитованого повідомлення передається агенту як контекст відповіді на конкретне повідомлення. Видалено окремий ConversationHandler для LLM.
  - У `business/services/langchain_agent_service.py`: введено `UserConversationMemory` (буфер останніх повідомлень, саммарі старішої частини, опційно векторний пошук за embeddings). `process_query` приймає `user_id`, `reply_to_text`; будує контекст з саммарі, релевантними фрагментами (vector), буфером та підказкою reply_to; після успішної відповіді зберігає обмін у пам'ять (buffer, при переповненні — саммарізація через LLM, додавання в векторний store при наявності Gemini embeddings). У глосарій додано термін «пам'ять розмови (агента)».

## 2026-02-07 — MCP сервер експорту та авто-відповідь файлом при >10 оголошень
- **Запит користувача**: Створити MCP сервер, який отримує від агента перелік ідентифікаторів оголошень/аукціонів та опціонально структуру даних, викликає механізм отримання з БД та збереження у файл, повертає агенту посилання на файл. Агент має розуміти, чи очікує користувач відповідь у вигляді Excel; автоматично надавати відповідь файлом, якщо результат перевищує 10 оголошень.
- **Дії**:
  - MCP сервер `mcp_servers/export_mcp_server.py`: інструменти `export_listings_to_file(ids, collection, format, columns, column_headers, filename_prefix)` — виклик `ExportDataService.export_to_file`, повертає url; `get_export_collections()` — список колекцій для експорту.
  - У `business/services/langchain_agent_service.py`: tool `export_listings_to_file` (виклик `report_generator.export_data_service.export_to_file`), обробка виклику в циклі агента; у результат execute_query/execute_aggregation при кількості записів >10 додається `_agent_hint` з інструкцією викликати export_listings_to_file та повернути користувачу url.
  - Оновлено системний промпт агента: при явному запиті Excel/файлу — використовувати export_listings_to_file або save_query_results_to_excel; при більше ніж 10 оголошень — обов'язково викликати export_listings_to_file та відповісти посиланням на файл.
  - Реєстрація export-mcp у `main.py`, `scripts/start_mcp_servers.py`; опис у `docs/mcp_servers.md`.

## 2026-02-07 — Уніфікований механізм збереження даних у файл за ідентифікаторами
- **Запит користувача**: Уніфікувати механізм збереження даних у файл. Сервіс на вхід отримує ідентифікатори оголошень у БД та опціонально структуру полів; генерує файли у тимчасовій папці та повертає посилання на файл. Вбудувати у існуючий механізм формування звітів.
- **Дії**:
  - У `data/repositories/olx_listings_repository.py`: метод `get_by_ids(ids)` — пошук за URL або `_id` (ObjectId).
  - У `data/repositories/prozorro_auctions_repository.py`: метод `get_by_ids(ids)` — пошук за `auction_id` або `_id`.
  - Сервіс `business/services/export_data_service.py`: `ExportDataService.export_to_file(ids, collection, file_format, fields, column_headers, filename_prefix)` — підтримка колекцій `prozorro_auctions`, `olx_listings`; витягування полів з документів за dot-notation; збереження у `temp/exports/`; повертає `url`, `filename`, `size`, `rows_count`.
  - У `utils/report_generator.py`: валідація запиту приймає альтернативний варіант — `ids` + `collection` (замість `dataSource` + `columns`); при такому запиті виклик `ExportDataService.export_to_file` та повернення результату (url або base64 за параметром `return_base64`).
  - Оновлено опис інструмента `generate_report` у `mcp_servers/report_mcp_server.py` та `docs/mcp_servers.md`. У глосарій додано термін «експорт за ідентифікаторами».

## 2026-02-05 — Файл друку: вкладки ProZorro + OLX, структуровані адреси геокодування
- **Запит користувача**: При виведенні результатів у файли створити в файлі друку дві вкладки: перша «ProZorro» (без змін), друга «OLX» з оголошеннями OLX; підігнати структуру даних OLX під аналогічну ProZorro; адреси з Google Maps API зберігати в деталізованому вигляді (область, місто, вулиця тощо).
- **Дії**:
  - У `utils/file_utils.py`: додано `_apply_sheet_style` та `generate_excel_with_sheets(sheets)` для Excel з кількома вкладками; гіперпосилання для auction_url, listing_url.
  - У `data/repositories/olx_listings_repository.py`: метод `get_all_for_export(limit)`.
  - У `business/services/prozorro_service.py`: дві вкладки в `generate_excel_from_db` — «ProZorro» та «OLX»; `_prepare_olx_data_for_excel(olx_docs)` — маппінг OLX на колонки як у ProZorro; адреса з `resolved_locations[].results[].address_structured` або `formatted_address`.
  - Сервіс `business/services/geocoding_service.py`: парсинг `address_components` у `address_structured` (region, city, street, street_number тощо).

## 2026-02-05 — OLX скрапер: тільки продаж, сортування «найновіші», дата оголошення
- **Запит користувача**: У скрапері додати фільтр тільки продаж (не вся комерційна нерухомість), сортування «Найновіші спочатку», зберігати дату оголошення зі списку.
- **Дії**:
  - У `scripts/olx_scraper/config.py`: додано `COMMERCIAL_REAL_ESTATE_SALE_PATH` (підкатегорія «Продаж комерційної нерухомості»), `OLX_SORT_NEWEST` (`search[order]=created_at:desc`). `get_commercial_real_estate_list_url(page, sale_only=True, sort_newest=True)` тепер за замовчуванням веде на продаж із сортуванням «найновіші».
  - У `scripts/olx_scraper/parser.py`: розділення блоку «локація — дата» (`_split_location_and_date`: "Петро-Михайлівка - 04 лютого 2026 р." → location + date_text); парсинг української дати (`_parse_listed_date_ua`: "04 лютого 2026 р.", "Сьогодні о 07:28", "Вчора о 12:00") у ISO для сортування; у картці оголошення зберігаються `location`, `date_text`, `listed_at_iso`.
  - У `scripts/olx_scraper/helpers.py`: у `search_data` та порівняння змін додано поле `listed_at_iso`.

## 2026-02-05 — OLX у процедурах оновлення даних (нежитлова + земельні ділянки)
- **Запит користувача**: Додати завантаження з OLX у процедури оновлення даних, де зараз оновлюються дані ProZorro. Цікавлять оголошення по нежитловій нерухомості та по земельних ділянках для забудови.
- **Дії**:
  - У `scripts/olx_scraper/config.py`: додано `LAND_PATH` та `get_land_list_url(page)` для категорії «Земельні ділянки» (olx.ua/uk/nedvizhimost/zemlya/).
  - Створено `scripts/olx_scraper/helpers.py`: спільні функції `search_data_from_listing`, `search_data_changed`, `_address_line_from_llm_address`, `_collect_and_geocode_locations` для уникнення циклічних імпортів.
  - Створено `scripts/olx_scraper/run_update.py`: `run_olx_update(settings, categories, log_fn)` — обробка кількох категорій; за замовчуванням категорії «Нежитлова нерухомість» та «Земельні ділянки» (по MAX_SEARCH_PAGES сторінок кожна). Повертає success, total_listings, total_detail_fetches, by_category.
  - У `main.py`: після `fetch_real_estate_auctions` викликається `run_olx_data_update()` (ProZorro + OLX у одному запуску).
  - У `business/services/telegram_bot_service.py`: після успішного оновлення ProZorro в _generate_file_async виконується оновлення OLX у executor та користувачу відправляється повідомлення з підсумком (кількість оголошень та завантажених деталей).
  - `run_prototype.py` спрощено: використовує `run_olx_update` з однією категорією (нежитлова нерухомість); спільна логіка в helpers та run_update.

## 2026-02-05 — Інтеграція Google Maps (геокодування)
- **Запит користувача**: Інтегрувати сервіс Google Maps для геокодування адрес і топонімів; кешувати запити; в оголошеннях зберігати хеш кеша замість текстового представлення; налаштування з окремим ключем Google Maps; MCP-сервіс для LLM-агента; LLM повинна розпізнавати адреси в запитах і даних та відправляти їх на геокодування; адреси з полів (наприклад ProZorro) теж проганяти через розпізнавання. Мета — перетворювати всі топоніми на координати.
- **Дії**:
  - У `config/settings.py` та `config/config.example.yaml`: додано поле `google_maps_api_key` (з конфігу `google_maps.api_key`).
  - У `utils/hash_utils.py`: функція `calculate_geocode_query_hash(query)` для ключа кешу геокодування.
  - Репозиторій `data/repositories/geocode_cache_repository.py`: колекція `geocode_cache`, унікальний індекс по `query_hash`; методи `find_by_query_hash`, `save_result(query_hash, query_text, result)`.
  - Міграція `scripts/migrations/005_geocode_cache_collection.py`: створення колекції та індексу.
  - Сервіс `business/services/geocoding_service.py`: перевірка кешу, при відсутності — запит до Google Geocoding API (region=ua), нормалізація результатів (latitude, longitude, formatted_address, place_id, types), збереження в кеш; повертає `query_hash`, `query_text`, `results`, `from_cache`.
  - MCP-сервер `mcp_servers/geocoding_mcp_server.py`: інструмент `geocode_address(address_or_place, region)`; зареєстровано в `scripts/start_mcp_servers.py` та `main.py`.
  - У `business/services/langchain_agent_service.py`: додано GeocodingService, tool `geocode_address` та інструкції в системному промпті про розпізнавання адрес/топонімів і використання геокодування.
  - У `scripts/olx_scraper/run_prototype.py`: після LLM-екстракції збираються адреси з `detail.location`, `search_data.location` та `llm.addresses`; кожен унікальний рядок геокодується через GeocodingService; в оголошення зберігаються `detail.geocode_query_hashes` та `detail.resolved_locations`.
  - Документація: оновлено `docs/mcp_servers.md` (запуск та приклад конфігу Cursor для geocoding-mcp).

## 2026-02-05 — Прототип скрапера OLX: нежитлова нерухомість → MongoDB
- **Запит користувача**: Зробити на базі тестового скрапера робочий прототип із збереженням у базу. Проходити по всьому масиву активних оголошень у розділі нежитлової нерухомості та зберігати в БД. Якщо оголошення нове (нема в базі за URL) або нема блоку detail або змінилась інформація з пошуку — відкривати сторінку оголошення та зберігати detail. Затримка між запитами 2–10 с. Для тестів обмежитися першими 5 сторінками.
- **Дії**:
  - У `scripts/olx_scraper/config.py`: додано `COMMERCIAL_REAL_ESTATE_PATH`, `get_commercial_real_estate_list_url()`, `DELAY_DETAIL_MIN/MAX` (2–10 с), `get_delay_detail_seconds()`, `MAX_SEARCH_PAGES` (5).
  - Репозиторій `data/repositories/olx_listings_repository.py`: колекція `olx_listings`, ідентифікатор — `url`; методи `find_by_url`, `upsert_listing(url, search_data, detail)`, `ensure_index()` (унікальний індекс по `url`).
  - У `scripts/olx_scraper/parser.py`: функція `parse_detail_page(html)` — опис, параметри (label/value), `fetched_at`.
  - Скрипт `scripts/olx_scraper/run_prototype.py`: цикл по сторінках 1..MAX_SEARCH_PAGES, завантаження списку нежитлової нерухомості, для кожного оголошення перевірка в БД; якщо потрібен detail — затримка 2–10 с, запит сторінки оголошення, парсинг, збереження. Порівняння змін по полях search_data (title, price_*, location, date_text, area_m2).
  - Міграція `scripts/migrations/003_olx_listings_collection.py`: створення/перевірка колекції та індексу. Оновлено `scripts/olx_scraper/README.md` та історію розробки.

## 2026-02-05 — Тестовий скрапер OLX (нерухомість → JSON)
- **Запит користувача**: Побудувати тестовий скрапер сайту OLX: структурувати корисну інформацію з сторінок та складати в БД/файл. Тест: перша сторінка пошуку в розділі «нерухомість» → зберегти в JSON. Використовувати LLM і типові бібліотеки для обробки неструктурованих даних; одразу пропрацювати заходи обходу антиботового захисту.
- **Дії**:
  - Додано залежності `beautifulsoup4`, `lxml` до `requirements.txt`.
  - Створено модуль `scripts/olx_scraper/`: `config.py` (URL, затримки, User-Agent), `fetcher.py` (один запит за раз, затримка 2–5 с перед запитом, заголовки як у браузера), `parser.py` (BeautifulSoup, селектори `[data-cy="l-card"]`, витягування title/price/location/area/url, fallback заголовка з raw_snippet), `run_test.py` (fetch → parse → save JSON у UTF-8), `README.md`.
  - Антибот: без паралельних запитів, випадкова пауза перед запитом, реалістичний User-Agent та Accept-Language, таймаут 25 с.
  - Результат тесту зберігається в `scripts/olx_scraper/output/olx_nedvizhimost_page1.json` (source, total_count, listings з полями title, price_text, price_value, currency, location, date_text, area_m2, url, raw_snippet). Каталог `output/` додано до `.gitignore`.
  - LLM: зараз використовується лише парсинг через BeautifulSoup; поле `raw_snippet` залишено для майбутньої нормалізації через LLM (локація/дата).

## 2026-02-05 — Огляд OLX Partner API
- **Запит користувача**: Вивчити документацію API для інтеграції з OLX та зробити короткий огляд можливостей і варіантів використання в застосунку як додаткового до ProZorro джерела даних.
- **Дії**:
  - Проаналізовано `docs/partner_api.yaml` (OpenAPI 3.0.2): аутентифікація OAuth 2.0, ендпоінти (користувачі, регіони/міста, категорії, оголошення, threads/messages, пакети/білінг).
  - Визначено обмеження: GET /adverts повертає лише оголошення поточного користувача; публічного пошуку оголошень API не надає.
  - Додано `docs/olx_partner_api_overview.md` — короткий огляд можливостей API та сценаріїв інтеграції з Pazuzu (публікація на OLX після ProZorro, довідники, керування власними оголошеннями).
  - У `docs/developer_glossary.md` додано термін «OLX Partner API».

## 2026-02-05 — Пошук можливостей отримання оголошень OLX за фільтрами
- **Запит користувача**: Вивчити загальнодоступну документацію OLX API, зосередившись на пошуку та отриманні актуальних оголошень за фільтрами.
- **Дії**:
  - Переглянуто офіційні джерела: Partner API (partner_api.yaml), developer.olx.ua, developer.olxgroup.com, api-documentation.olx.ba (Listings), olx-group.readme.io (RE API).
  - Підтверджено: публічного ендпоінту пошуку оголошень за категорією/регіоном/ціною/датою немає; усі API — для керування власними оголошеннями.
  - Єдиний варіант «фільтрації» — GET /adverts (offset, limit, external_id, category_ids) лише для оголошень авторизованого користувача.
  - У `docs/olx_partner_api_overview.md` додано розділ «Пошук і отримання оголошень за фільтрами» з висновками та посиланнями на джерела.

## 2026-02-03 — Вивід дати й часу за київським часом
- **Запит користувача**: Дату й час виводити за київським часом (у тексті меню ТГ бота, у файлі, тощо).
- **Дії**:
  - У `utils/date_utils.py`: додано `KYIV_TZ` (Europe/Kyiv), `to_kyiv()`, `format_datetime_display()`, `format_date_display()` для форматування дат у київському часі; імпорт `zoneinfo` (з fallback на `backports.zoneinfo` для Python &lt; 3.9).
  - Усі користувацькі виводи переведено на київський час: меню Telegram (дати оновлення), назви архівів/файлів у ТГ та при збереженні, усі дати в Excel (дата оновлення, дата торгів, дедлайни тощо), консольні повідомлення про збережену дату оновлення, контекст поточної дати/часу в LLM/LangChain промптах, timestamps у назвах файлів (generate_auction_filename, generate_json_filename, report_*, query_results_*).
  - Логіка порівнянь, API та БД лишається в UTC.

## 2026-02-03 — Колонка «Дата оновлення» в Excel та сортування
- **Запит користувача**: Додати у вивід Excel колонку з датою оновлення (або створення, якщо аукціон не оновлювався після створення). Сортувати список перед виводом у Excel за цим полем від найсвіжішого до найдавнішого.
- **Дії**:
  - У `prozorro_service.py`: додано обчислення `date_updated` (dateModified якщо є і відрізняється від dateCreated, інакше dateCreated) та поле `date_updated_ts` для сортування у `save_auctions_to_csv` та `_prepare_auctions_data_for_excel`; колонка «Дата оновлення» додана до fieldnames/column_headers; перед збереженням/генерацією Excel список сортується за `date_updated_ts` за спаданням.
  - У `langchain_agent_service.py`: додано «Дата оновлення» до стандартного формату Excel (`_get_standard_excel_format`) та маппінг полів для витягування з результатів агрегації (`date_updated`, `auction_data.dateModified`, `auction_data.dateCreated`) з форматуванням дати.

## 2026-01-23 — Інтегровано LangChain у проєкт з безпечною архітектурою
- **Запит користувача**: Інтегрувати LangChain у проєкт БЕЗПЕЧНО, КОНТРОЛЬОВАНО та ПРИДАТНО ДЛЯ ПРОДАКШЕНУ з дотриманням архітектурних правил.
- **Дії**:
  - Додано залежності LangChain до `requirements.txt`:
    - `langchain>=0.1.0`
    - `langchain-google-genai>=1.0.0`
    - `langchain-openai>=0.1.0`
    - `langchain-anthropic>=0.1.0`
  - Створено сервіс `business/services/langchain_agent_service.py`:
    - Безпечна інтеграція LangChain з MCP-серверами
    - Явний цикл агента (plan → act → observe) з максимальною кількістю ітерацій
    - Всі операції з даними виконуються ВИКЛЮЧНО через MCP tools
    - LLM НІКОЛИ не має прямого доступу до БД, API або файлової системи
    - Детальне логування всіх дій агента
    - Валідація запитів на рівні сервісу (перевірка на заборонені операції)
    - Безпека на рівні tools (перевірка на $regex та інші заборонені оператори)
  - Реалізовано LangChain Tools для всіх MCP-серверів:
    - Schema MCP: `get_database_schema`, `get_collection_info`, `get_data_dictionary`
    - Query Builder MCP: `execute_query`, `get_allowed_collections`
    - Analytics MCP: `execute_analytics`, `list_metrics`
    - Report MCP: `generate_report`, `list_templates`
  - Архітектурні принципи:
    - Композиція замість наслідування
    - Явний контроль замість автономії
    - Максимальна спостережуваність та логування
    - Детермінована поведінка агента
  - Підтримка провайдерів LLM:
    - Google Gemini (через `langchain-google-genai`)
    - OpenAI (через `langchain-openai`)
    - Anthropic Claude (через `langchain-anthropic`)
  - Системний промпт з архітектурними правилами:
    - Заборона прямого доступу до даних
    - Обов'язкове використання MCP tools
    - Пояснення використання кожного tool
    - Відповіді українською мовою

## 2026-01-23 — Оновлено описи полів даних та підтримку фільтрації за регіоном
- **Запит користувача**: Перевірити та уточнити описи полів даних в базі, щоб LLM розуміла, які поля дивитися. Орієнтуватися на те, як формуються поля для файлів.
- **Дії**:
  - Додано розділ `flattened_fields` в `data_dictionary.yaml` з детальними описами полів, що використовуються для експорту та аналітики
  - Оновлено `DataDictionary` для включення `flattened_fields` в схему, яку бачить LLM
  - Виправлено `analytics_builder.py` для правильної обробки фільтрів за регіоном/містом через join з llm_cache
  - Оновлено системний промпт LLM агента з детальними інструкціями про використання вирівняних полів
  - Додано інструкції про те, як фільтрувати за регіоном через analytics-mcp
  - Вирівняні поля тепер відображаються в схемі, яку повертає schema-mcp

## 2026-01-23 — Інтегровано LLM агента з MCP серверами та Telegram ботом
- **Запит користувача**: Поєднати MCP сервери з LLM, створити системний промпт та tool descriptions, додати кнопку "Спитати LLM" в Telegram боті для адміністраторів з трансляцією відповіді.
- **Дії**: 
  - Створено сервіс `business/services/llm_agent_service.py` для роботи з LLM агентом:
    - Інтеграція з Gemini API з підтримкою function calling
    - Системний промпт, який наказує LLM використовувати інструменти для дослідження структури БД
    - Створено tool descriptions для всіх MCP інструментів (schema-mcp, query-builder-mcp, analytics-mcp, report-mcp)
    - Реалізовано виклик інструментів та обробку результатів
    - Підтримка stream_callback для трансляції проміжних результатів
  - Оновлено Telegram бота:
    - Додано кнопку "🤖 Спитати LLM" в головне меню для адміністраторів
    - Створено ConversationHandler для обробки LLM запитів
    - Реалізовано трансляцію проміжних результатів (ходу "думок") користувачу
    - Обробка довгих відповідей (розбиття на частини)
    - Логування запитів до LLM
  - LLM агент має доступ до всіх MCP інструментів:
    - Дослідження схеми БД через schema-mcp
    - Виконання запитів через query-builder-mcp
    - Аналітика через analytics-mcp
    - Генерація звітів через report-mcp

## 2026-01-23 — Створено Data Dictionary як єдине джерело правди про структуру даних
- **Запит користувача**: Створити Data Dictionary у форматі YAML як єдине джерело правди, яке використовується для MCP серверів, MongoDB валідації та генерації документації.
- **Дії**: 
  - Створено YAML файл `config/data_dictionary.yaml` з описом колекцій та полів
  - Створено модуль `utils/data_dictionary.py` для читання та парсингу Data Dictionary
  - Реалізовано підтримку вкладених полів, enum значень, типів, одиниць виміру
  - Створено модуль `utils/mongodb_validator.py` для валідації MongoDB на основі Data Dictionary:
    - Генерація MongoDB validation schema
    - Застосування validation schema до колекцій
    - Валідація документів
    - Валідація всієї колекції з підсумком помилок
  - Оновлено `mcp_servers/schema_mcp_server.py` для використання Data Dictionary:
    - Схема генерується на основі Data Dictionary, доповнюючись реальною статистикою
    - Додано інструменти: `get_data_dictionary`, `apply_validation_schema`, `validate_collection`
  - Створено модуль `utils/doc_generator.py` для генерації документації:
    - Генерація Markdown документації
    - Генерація JSON Schema
  - Створено скрипт `scripts/generate_documentation.py` для генерації документації
  - Оновлено документацію з описом Data Dictionary

## 2026-01-23 — Додано MCP сервер для генерації звітів у різних форматах
- **Запит користувача**: Додати MCP сервер report-mcp для генерації звітів. LLM лише описує що хоче (формат, шаблон, джерело даних, колонки), а сервер сам бере шаблон, генерує файл та віддає URL або base64.
- **Дії**: 
  - Створено модуль `utils/report_templates.py` для визначення шаблонів звітів
  - Реалізовано шаблони: `auction_summary`, `price_analysis`, `property_types`, `time_series`, `simple_list`
  - Створено модуль `utils/report_generator.py` для генерації звітів:
    - Підтримка форматів: xlsx, csv, json
    - Інтеграція з analytics-mcp та query-builder-mcp для отримання даних
    - Генерація файлів у пам'яті (base64) або збереження на диск (URL)
    - Автоматичне форматування з використанням шаблонів
  - Створено MCP сервер `mcp_servers/report_mcp_server.py` з інструментами:
    - `generate_report` - генерація звіту у вказаному форматі
    - `validate_report_request` - валідація запиту без виконання
    - `list_templates` - список доступних шаблонів
    - `get_template_info` - інформація про конкретний шаблон
    - `get_supported_formats` - список підтримуваних форматів
  - Додано report-mcp до списку серверів для запуску в `main.py` та `scripts/start_mcp_servers.py`
  - Оновлено документацію з описом report-mcp сервера

## 2026-01-23 — Додано MCP сервер для аналітики з метриками та агрегаціями
- **Запит користувача**: Додати MCP сервер analytics-mcp для виконання аналітичних запитів. LLM не повинна вигадувати агрегації, а використовувати готові метрики, які сервер знає як обчислювати.
- **Дії**: 
  - Створено модуль `utils/analytics_metrics.py` для визначення метрик та їх формул
  - Реалізовано метрики: `average_price_per_m2`, `total_price`, `base_price`, `area`, `building_area`, `land_area`, `count`
  - Створено модуль `utils/analytics_builder.py` для побудови aggregation pipeline на основі метрик
  - Реалізовано автоматичну побудову MongoDB aggregation pipeline з підтримкою:
    - Обчислення метрик за формулами (наприклад, average_price_per_m2 = priceFinal / area)
    - Групування за полями (region, city, property_type, status, year, month, quarter)
    - Фільтрація за статусом та діапазонами дат
    - Lookup до llm_cache для отримання додаткових даних
  - Створено MCP сервер `mcp_servers/analytics_mcp_server.py` з інструментами:
    - `execute_analytics` - виконання аналітичного запиту
    - `validate_analytics_query` - валідація запиту без виконання
    - `list_metrics` - список доступних метрик
    - `get_metric_info` - інформація про конкретну метрику
    - `get_allowed_group_by_fields` - список дозволених полів для групування
  - Додано analytics-mcp до списку серверів для запуску в `main.py` та `scripts/start_mcp_servers.py`
  - Оновлено документацію з описом analytics-mcp сервера

## 2026-01-23 — Додано MCP сервер для безпечного виконання запитів до MongoDB
- **Запит користувача**: Додати MCP сервер query-builder-mcp для безпечного виконання запитів до MongoDB через абстрактний API. LLM не має прямого доступу до MongoDB, а формує абстрактний запит, який валідується, трансформується у MongoDB запит та виконується безпечно.
- **Дії**: 
  - Створено модуль `utils/query_builder.py` для валідації та трансформації абстрактних запитів
  - Реалізовано валідацію запитів з перевіркою дозволених колекцій та операторів
  - Заборонено небезпечні оператори: `$where`, `$eval`, `$function`, `$expr`, `$regex`, `$text`
  - Реалізовано трансформацію абстрактних запитів у MongoDB aggregation pipeline з підтримкою `$lookup` для join
  - Додано обмеження: максимум 100 результатів, максимальна глибина вкладеності - 5 рівнів
  - Створено MCP сервер `mcp_servers/query_builder_mcp_server.py` з інструментами:
    - `execute_query` - виконання безпечного запиту
    - `validate_query` - валідація запиту без виконання
    - `get_allowed_collections` - список дозволених колекцій
    - `get_allowed_operators` - список дозволених та заборонених операторів
  - Оновлено `main.py` для запуску MCP серверів при старті застосунку (опція `--start-mcp`)
  - Створено скрипт `scripts/start_mcp_servers.py` для окремого запуску всіх MCP серверів
  - Оновлено документацію з описом обох MCP серверів

## 2026-01-23 — Додано MCP сервер для надання схеми метаданих колекцій БД
- **Запит користувача**: Додати MCP сервер schema-mcp, який повертає схему метаданих колекцій бази даних на основі реальних даних.
- **Дії**: 
  - Додано бібліотеку `mcp` до requirements.txt
  - Створено модуль `utils/schema_analyzer.py` для глибокого аналізу структури даних у колекціях MongoDB
  - Реалізовано аналіз структури з урахуванням того, що не всі записи мають однакову структуру
  - Створено MCP сервер `mcp_servers/schema_mcp_server.py` з ресурсом для надання схеми метаданих
  - MCP сервер надає доступ тільки до колекцій `prozorro_auctions` та `llm_cache`, виключаючи колекції з налаштуваннями користувачів та системи
  - Реалізовано виявлення зв'язків між колекціями (через `description_hash`)
  - Додано інструменти для оновлення кешу схеми та отримання інформації про конкретну колекцію

## 2026-01-21 — Прибрано вивід діагностики LLM з коду
- **Запит користувача**: Прибрати з коду вивід діагностики LLM.
- **Дії**: 
  - Видалено всі `print(f"[LLM ДІАГНОСТИКА] ...")` з файлу `prozorro_service.py`
  - Видалено 8 рядків діагностичного виводу в 4 місцях коду
  - Код тепер працює без діагностичних повідомлень про виклики LLM

## 2026-01-21 — Прибрано повідомлення про статистику при відсутності викликів LLM
- **Запит користувача**: Прибрати повідомлення про статистику, коли планується 0 викликів LLM.
- **Дії**: 
  - Додано умову в Telegram боті: повідомлення про статистику виводиться тільки якщо `llm_planned > 0`
  - Якщо викликів LLM не планується, повідомлення не виводиться

## 2026-01-21 — Повернення прогрес-бару обробки LLM у термінальний вивід
- **Запит користувача**: Повернути прогрес-бар обробки даних (запити до LLM) у термінальний вивід.
- **Дії**: 
  - Додано прогрес-бар для обробки LLM в методі `_save_auctions_to_database()`
  - Прогрес-бар показує поточну кількість викликів LLM та час обробки
  - Прогрес-бар оновлюється при кожному виклику `_process_auction_with_llm()`
  - Використовується бібліотека `tqdm` для відображення прогрес-бару

## 2026-01-21 — Виправлення невідповідності кількості аукціонів у повідомленнях
- **Проблема**: Різні цифри "знайдено" і "відібрано" - `_analyze_auctions_before_save()` фільтрує аренду (66), а `fetch_and_save_real_estate_auctions()` повертає всі аукціони включно з арендою (122).
- **Дії**: 
  - Змінено `_save_auctions_to_database()` щоб повертати словник зі статистикою: `llm_requests_count` та `saved_count` (кількість збережених без аренди)
  - Оновлено `fetch_and_save_real_estate_auctions()` щоб повертати `saved_count` замість `len(auctions)`
  - Оновлено всі місця виклику `_save_auctions_to_database()` для роботи зі словником
  - Тепер кількість в повідомленні "Знайдено X аукціонів" відповідає кількості попередньо відібраних (без аренди)

## 2026-01-21 — Оновлення повідомлень в Telegram боті про обробку аукціонів
- **Запит користувача**: Прибрати повідомлення "Всі аукціони мають кешовані результати LLM - обробка буде швидкою." (воно видається після обробки LLM, тому не має сенсу). Додати інформативне повідомлення після отримання даних з API з кількістю попередньо відібраних аукціонів, з них: без змін, змінено, планується викликів LLM.
- **Дії**: 
  - Прибрано повідомлення "Всі аукціони мають кешовані результати LLM - обробка буде швидкою."
  - Додано метод `_analyze_auctions_before_save()` для аналізу аукціонів перед збереженням
  - Оновлено повідомлення в боті: після отримання даних з API виводиться статистика з кількістю попередньо відібраних аукціонів, без змін, змінено, планується викликів LLM
  - Додано приблизний час обробки, якщо планується викликів LLM

## 2026-01-21 — Виключення аукціонів-аренди з збереження в БД
- **Запит користувача**: Аукціони-аренда взагалі не мають зберігатися в БД.
- **Дії**: 
  - Додано перевірку на аренду перед збереженням аукціону в БД - якщо це аренда, аукціон не зберігається
  - Додано видалення існуючих аукціонів-аренди з БД, якщо вони там є
  - Додано лічильник видалених аукціонів-аренди в статистику збереження
  - Прибрано перевірки на аренду в умовах виклику LLM, оскільки аренда тепер не зберігається в БД

## 2026-01-21 — Виправлення логіки перевірки кешу LLM перед викликом
- **Запит користувача**: Виправити логіку так, щоб перевірка кешу по `description_hash` відбувалася перед викликом LLM, а не всередині `_process_auction_with_llm`.
- **Проблема**: Кількість записів в LLM кеші була менша за кількість оброблених аукціонів, оскільки перевірка кешу відбувалася всередині `_process_auction_with_llm` по опису, а не по `description_hash` перед викликом.
- **Дії**: 
  - Переписано логіку в `_save_auctions_to_database`: перевірка кешу по `description_hash` тепер відбувається ПЕРЕД викликом `_process_auction_with_llm` для нових та оновлених аукціонів
  - Спрощено метод `_process_auction_with_llm`: тепер він просто викликає LLM і зберігає результат, без перевірки кешу (перевірка відбувається на рівні виклику)
  - Прибрано всі діагностичні повідомлення про LLM
  - Логіка тепер відповідає вимогам: обчислюємо `description_hash`, перевіряємо кеш по `description_hash`, якщо немає - викликаємо LLM і зберігаємо результат

## 2026-01-21 — Додано детальне діагностичне логування помилок LLM
- **Запит користувача**: Додати діагностичні повідомлення про помилки від LLM для виявлення причин, чому деякі описи не обробляються.
- **Дії**: 
  - Додано детальне логування помилок LLM у всіх місцях викликів:
    - `_process_auction_with_llm()` - додано виведення ID аукціону, хешу опису, типу помилки, повідомлення, превью опису та повного traceback
    - `save_auctions_to_csv()` - додано діагностичне логування при виклику LLM
    - `_prepare_auctions_data_for_excel()` - додано діагностичне логування при виклику LLM
    - `_prepare_auctions_data_for_excel_with_hashes()` - додано діагностичне логування при виклику LLM
    - `generate_excel_from_db()` - додано діагностичне логування при виклику LLM
  - Додано інформативні повідомлення про успішні виклики LLM з ID аукціону та хешем опису
  - Помилки логуються в консоль з префіксом `[LLM ПОМИЛКА]` та в сервіс логування з типом події `llm_error`
  - Тепер можна легко виявити, які описи не обробляються через LLM та чому

## 2026-01-21 — Виправлення обробки статусу 204 (No Content) в API ProZorro
- **Запит користувача**: Дослідити причини виникнення помилки "Немає даних (статус 204 - No Content)".
- **Дії**: 
  - Виявлено проблему: при отриманні статусу 204 код виходив з циклу обробки (`break`), що призводило до зупинки обробки всіх наступних дат
  - Оновлено обробку статусу 204: замість виходу з циклу код тепер переходить до наступного дня в діапазоні
  - Оновлено обробку порожніх відповідей: також перехід до наступного дня замість виходу з циклу
  - Оновлено обробку порожніх списків аукціонів: перехід до наступного дня замість виходу з циклу
  - Оновлено обробку випадку, коли `max_date_modified` є `None`: перехід до наступного дня замість виходу з циклу
  - Тепер система коректно обробляє ситуації, коли для деяких дат немає даних, і продовжує обробку наступних дат у діапазоні

## 2026-01-21 — Повернення fallback методів та додавання перевірки хешу LLM навіть при незмінній версії
- **Запит користувача**: Повернути fallback методи парсингу без LLM. При обробці перевіряти версію хешу LLM навіть якщо версія не змінилась - на випадок, якщо ми змінили щось в промптингу і очистили кеш LLM.
- **Дії**: 
  - Повернуто використання `_extract_structured_info_from_items()` як fallback методу парсингу
  - Оновлено `_save_auctions_to_database()`: навіть якщо версія аукціону не змінилась, перевіряється наявність результату LLM в кеші за `description_hash`; якщо хеш є в БД, але немає в кеші - викликається LLM
  - Оновлено `save_auctions_to_csv()`: повернуто fallback на структуровані дані, структуровані дані мають пріоритет, LLM доповнює порожні поля
  - Оновлено `_prepare_auctions_data_for_excel()`: повернуто fallback на структуровані дані
  - Оновлено `_prepare_auctions_data_for_excel_with_hashes()`: повернуто fallback на структуровані дані
  - Тепер система використовує структуровані дані з `items` як базові, а LLM доповнює порожні поля; якщо `description_hash` є в БД, але немає в кеші - автоматично викликається LLM

## 2026-01-21 — Повне видалення файлового кешу LLM
- **Запит користувача**: Прибрати використання кешу LLM через файл, використовувати виключно кеш у БД.
- **Дії**: 
  - Перевірено, що весь код використовує тільки БД кеш через `LLMCacheService` та `LLMCacheRepository`
  - Оновлено документацію - прибрано застарілі згадки про файловий кеш `data/cache/llm_cache.json`
  - Підтверджено, що міграція 002 вже перенесла всі дані з файлу в MongoDB колекцію `llm_cache`
  - Кеш LLM тепер використовує виключно MongoDB для збереження та отримання результатів парсингу

## 2026-01-21 — Додано статистику викликів LLM під час оновлення даних
- **Запит користувача**: У кінці оновлення даних виводити кількість викликів LLM.
- **Дії**: 
  - Додано метод `_process_auction_with_llm()` для обробки аукціонів через LLM під час збереження в БД
  - Метод повертає 1 якщо був реальний виклик LLM (без кешу), 0 якщо використано кеш або пропущено
  - Оновлено метод `_save_auctions_to_database()`:
    - Тепер повертає кількість викликів LLM
    - Обробляє нові та оновлені аукціони через LLM для поповнення кешу
    - Виводить статистику викликів LLM в консоль
  - Оновлено методи `fetch_and_save_real_estate_auctions()` та `_fetch_and_save_week_optimized()`:
    - Після збереження даних виводиться кількість викликів LLM
    - Статистика також повертається в результаті операції

## 2026-01-21 — Міграція з google.generativeai на google.genai
- **Запит користувача**: Виправити попередження про застарілий пакет `google.generativeai`.
- **Дії**: 
  - Оновлено `GeminiLLMProvider` для використання нового пакету `google.genai` замість застарілого `google.generativeai`
  - Замінено `import google.generativeai as genai` на `from google import genai`
  - Замінено `genai.configure(api_key=...)` на `self.client = genai.Client(api_key=...)`
  - Замінено `model.generate_content(prompt)` на `client.models.generate_content(model=..., contents=...)`
  - Оновлено `requirements.txt`: замінено `google-generativeai>=0.3.0` на `google-genai>=0.2.0`
  - Додано fallback логіку для спроб різних моделей, якщо перша не спрацює
  - Видалено метод `_initialize_model()`, замінено на `_validate_model()` з простішою логікою

## 2026-01-21 — Зміна підходу до скачування файлів: робота в пам'яті та збереження дат оновлень
- **Запит користувача**: Змінити підхід до скачування файлів. Файли Excel, що формуються для відправки, не зберігати. В БД створити колекцію для збереження дат і періодів останніх оновлень. Замість збереження файлів за день і за тиждень - зберігати періоди, коли було останнє відповідне оновлення інформації. Оновлення за тиждень автоматично означає оновлення і за останню добу. Команда скачування файлу за добу/тиждень вибирає з БД результати за період до збереженої дати оновлення і повертає її, без звернень до API і LLM.
- **Дії**: 
  - Створено репозиторій `AppDataRepository` для збереження дат оновлень в колекції `app_data`
  - Додано метод `generate_excel_in_memory()` в `utils/file_utils.py` для формування Excel файлів в пам'яті (BytesIO) замість збереження на диск
  - Додано метод `get_auctions_by_date_range()` в `ProZorroAuctionsRepository` для отримання аукціонів з БД за діапазон дат
  - Додано метод `get_auctions_from_db_by_period()` в `ProZorroService` для отримання аукціонів з БД за період до збереженої дати оновлення
  - Додано метод `generate_excel_from_db()` в `ProZorroService` для формування Excel в пам'яті з даних БД без звернень до API і LLM
  - Додано метод `_prepare_auctions_data_for_excel()` для підготовки даних аукціонів для Excel (з можливістю пропуску LLM)
  - Змінено логіку `fetch_and_save_real_estate_auctions()`: замість збереження файлів зберігаються дати оновлень в БД через `AppDataRepository`
  - Оновлення за тиждень автоматично оновлює також дату за добу
  - Переписано `_fetch_and_save_week_optimized()` для роботи в пам'яті без використання тимчасових файлів
  - Оновлено `TelegramBotService`:
    - Метод `handle_get_file()` тепер використовує `generate_excel_from_db()` замість пошуку файлів на диску
    - Метод `show_main_menu()` отримує дати оновлень з БД замість пошуку файлів
    - Метод `_generate_file_async()` оновлено для роботи з новою логікою (дати оновлень замість файлів)
  - Видалено невикористовувані імпорти `find_latest_auction_file` та `extract_date_range_from_filename` з `telegram_bot_service.py`

## 2026-01-20 — Міграція на MongoDB колекції
- **Запит користувача**: Міграція застосунку з файлового зберігання на MongoDB колекції. Створити колекції prozorro_auctions, logs, users, llm_cache. Перенести дані з файлів в MongoDB. Змінити механізм отримання даних з ProZorro для збереження в MongoDB з перевіркою версій.
- **Дії**: 
  - Створено репозиторії для колекцій:
    - `ProZorroAuctionsRepository` - для збереження аукціонів з полями: auction_id, auction_data, version_hash, description_hash, last_updated
    - `LogsRepository` - для логування подій (API обміни, дії користувачів, події застосунку)
    - `UsersRepository` - для управління користувачами бота
    - `LLMCacheRepository` - для кешування результатів парсингу LLM
  - Створено утиліту `utils/hash_utils.py` для обчислення хешів:
    - `calculate_object_version_hash()` - хеш повного тексту оголошення для визначення змін
    - `calculate_description_hash()` - хеш опису (як в механізмі LLM кешу)
    - `extract_auction_id()` - витягування ідентифікатора аукціону
  - Створено `LoggingService` для логування подій:
    - `log_api_exchange()` - обміни з API (помилки, факти обміну, ініціатор, параметри)
    - `log_user_action()` - дії користувачів через бот (скачування файлів, адміністрування, запити на оновлення)
    - `log_app_event()` - загальні події застосунку (старт/зупинка, внутрішні помилки)
  - Оновлено `LLMCacheService` для роботи з MongoDB замість файлу
  - Оновлено `UserService` для роботи з MongoDB замість YAML файлу
  - Оновлено `ProZorroService`:
    - Додано метод `_save_auctions_to_database()` для збереження аукціонів в MongoDB
    - Після отримання з API проводиться пошук аукціонів у базі за auction_id
    - Перевіряється версія об'єкта (version_hash) - якщо не змінилася, нічого не оновлюється
    - Якщо версія змінилася або аукціону немає - записується/оновлюється в базі
    - Додано логування API запитів та помилок
  - Створено міграційний скрипт `scripts/migrations/002_migrate_to_mongodb_collections.py`:
    - Переносить користувачів з `config/users.yaml` в колекцію users
    - Переносить LLM кеш з `data/cache/llm_cache.json` в колекцію llm_cache
    - Видаляє файли після успішного переносу (users.yaml, users.example.yaml, llm_cache.json)
  - Оновлено `main.py` та `telegram_bot_service.py` для використання логування
  - Після збереження в MongoDB обробка продовжується як раніше: перевірка кешу LLM, парсинг через LLM якщо потрібно, формування Excel файлів

## 2026-01-20 — Створення механізмів роботи з MongoDB
- **Запит користувача**: Створити механізми для збереження, отримання та пошуку об'єктів із бази даних MongoDB. Параметри підключення прописати в файлі конфіга (локальний хост). Створити механізм стартової міграції, що створює БД pazuzu.
- **Дії**: 
  - Додано `pymongo>=4.6.0` до `requirements.txt`
  - Створено модуль `data/database/connection.py` з класом `MongoDBConnection` для управління підключенням до MongoDB
  - Додано налаштування MongoDB до `config/settings.py` та `config/config.example.yaml`:
    - `mongodb_host` (за замовчуванням localhost)
    - `mongodb_port` (за замовчуванням 27017)
    - `mongodb_database_name` (за замовчуванням pazuzu)
    - `mongodb_username` та `mongodb_password` (опціонально)
    - `mongodb_auth_source` (за замовчуванням admin)
  - Створено базовий репозиторій `data/repositories/base_repository.py` з методами:
    - `create()`, `create_many()` - створення документів
    - `find_by_id()`, `find_one()`, `find_many()` - пошук документів
    - `update_by_id()`, `update_many()` - оновлення документів
    - `delete_by_id()`, `delete_many()` - видалення документів
    - `count()`, `exists()` - перевірка наявності
  - Створено систему міграцій:
    - `scripts/migrations/001_create_database.py` - міграція для створення БД pazuzu
    - `scripts/migrations/run_migrations.py` - скрипт для запуску всіх міграцій
  - Оновлено `__init__.py` файли для зручного імпорту класів

## 2026-01-20 — Покращення парсингу через LLM та використання структурованих даних з items
- **Запит користувача**: У результаті обробки оголошення купа пустих полів, хоча в тексті оголошення і даних є необхідна інформація. Покращити отримання інформації та парсинг через LLM.
- **Дії**: 
  - Додано метод `_extract_structured_info_from_items()` для витягування структурованих даних з `items` перед парсингом через LLM
  - Метод витягує: кадастровий номер, площу, одиницю виміру, адресу (область, місто, вулицю), тип нерухомості, комунікації, обтяження
  - Покращено промпт для LLM: додано детальні інструкції для кожного поля, приклади форматування, покращено розпізнавання адрес та площ
  - Змінено логіку обробки: спочатку витягуються структуровані дані з `items`, потім LLM доповнює порожні поля
  - Структуровані дані з `items` мають пріоритет над результатами LLM
  - Очищено кеш LLM для перегенерації результатів з покращеним промптом

## 2026-01-19 — Фільтрація аренди та PA01-7 перед обробкою через LLM та повне відсікання PA01-7
- **Запит користувача**: Перед відправкою на обробку в LLM додатково фільтрувати аукціони, прибрати всі аукціони, що стосуються аренди, лишивши все інше. Також відсікати результати, що в додаткових класифікаторах містять PA01-7.
- **Дії**: 
  - Додано метод `_is_rental_auction()` для визначення, чи аукціон стосується аренди
  - Метод перевіряє різні джерела інформації:
    - `leaseType` / `lease_type` - якщо присутній, то це аренда
    - `saleType` / `sale_type` - якщо присутній і немає leaseType, то це продаж
    - `procedureType` - перевіряє на ключові слова про аренду (lease, rent, оренд)
    - `items.additionalClassifications` - перевіряє описи класифікаторів на ключові слова про аренду
    - `title` / `description` - перевіряє текст на ключові слова (з пріоритетом продажу над арендою)
  - Перед викликом LLM додано перевірку: якщо `is_rental == True`, то пропускається обробка через LLM
  - **Додано повне відсікання аукціонів з кодом PA01-7**: в методі `_should_include_auction()` додано перевірку на наявність коду `PA01-7` в `items.additionalClassifications` - якщо знайдено, аукціон повністю виключається з результату (не потрапляє в Excel)
  - Аукціони з арендою (без PA01-7) все ще зберігаються в Excel, але без обробки опису через LLM (економія на API викликах)

## 2026-01-19 — Дослідження оптимального ендпоінта для активних аукціонів
- **Запит користувача**: Вивчити API ProZorro в пошуках оптимального ендпоінта для отримання активних аукціонів на теперішню дату.
- **Дії**: 
  - Вивчено документацію ProZorro.Sale API
  - Виявлено, що ендпоінт `/api/search/byDateCreated` не підтримується (404) - видалено метод `_get_auctions_by_date_created`
  - Виявлено, що ендпоінт `/api/procedures` повертає 405 (Method Not Allowed) для GET запитів без ID - він призначений тільки для отримання конкретного процедури за ID, а не для пошуку/списку
  - Видалено метод `_get_auctions_via_procedures_endpoint`, оскільки він не працює
  - Тепер використовується тільки `/api/search/byDateModified/{date}` для отримання даних
  - Фільтрація за `dateCreated` АБО `dateModified` виконується на стороні клієнта після отримання даних з API
  - Додано діагностичне логування для відстеження структури даних з API (ключі, `_id`, `data.id`)
  - Покращено парсинг ID в `AuctionDTO.from_dict` - тепер перевіряє `id`, `_id`, `data.id`

## 2026-01-19 — Видалено зайве діагностичне логування
- **Запит користувача**: Прибрати зайве діагностичне логування з сервісів.
- **Дії**: 
  - Видалено всі блоки діагностичного логування з `prozorro_service.py`:
    - Прибрано логування про цільовий аукціон `SPE001-UA-20260115-99514`
    - Прибрано діагностичні повідомлення з мітками `[ДІАГНОСТИКА ...]`
    - Прибрано логування структури відповіді API
    - Прибрано логування першого аукціону для діагностики
    - Прибрано логування останнього оголошення на сторінці
    - Прибрано зайве логування запитів до API
  - Видалено діагностичне повідомлення про використання моделі Gemini з `llm_service.py`
  - Залишено тільки корисне логування: статистику обробки, помилки обробки та основні повідомлення про помилки API

## 2026-02-10 — OLX: хеш ключових полів оголошення для контролю викликів LLM
- **Запит користувача**: Оголошення з OLX не повинні щоразу повторно проходити через LLM, якщо «по суті» не змінилися.
- **Дії**:
  - Додано формування хешу ключових полів оголошення (title, локація, площа, опис, параметри) в `OlxLLMExtractorService`.
  - В `run_update.py` перед викликом LLM обчислюється новий хеш і порівнюється з попереднім; якщо хеш не змінився, повторний виклик LLM пропускається, а попередній результат `detail.llm` перевикористовується.
  - Хеш зберігається в полі `detail.llm_content_hash` колекції `olx_listings`.

## 2026-01-19 — Додано кешування результатів парсингу LLM
- **Запит користувача**: Створити систему кешування результатів парсингу описів через LLM для зменшення кількості запитів до LLM API.
- **Дії**: 
  - Створено сервіс `business/services/llm_cache_service.py` для кешування результатів парсингу
  - Використовується MD5 хеш опису як ключ для збереження результатів
  - Результати зберігаються в MongoDB колекцію `llm_cache` (після міграції 2026-01-20)
  - Інтегровано кеш в `prozorro_service.py`: перед викликом LLM перевіряється кеш, якщо результат є - використовується з кешу, якщо немає - викликається LLM і результат зберігається
  - Додано методи для отримання статистики кешу та очищення кешу

## 2026-01-19 — Видалення фільтра продажів/оренди
- **Запит користувача**: Прибрати фільтр 4.1, який відсівав оренду і залишав тільки продажі.
- **Дії**: 
  - Видалено перевірку `saleType` та `leaseType` з методу `save_auctions_to_csv()`
  - Оновлено docstring методу та повідомлення про збереження
  - Оновлено документацію `docs/data_pipeline.md` - прибрано опис фільтра 4.1 та оновлено підсумкову таблицю фільтрів
  - Тепер в Excel зберігаються всі аукціони (продажі та оренда), що пройшли фільтрацію на попередніх етапах

## 2026-01-19 — Документація пайплайну отримання та фільтрації даних
- **Запит користувача**: Сформулювати існуючий пайплайн отримання та фільтрації даних для розуміння, на якому етапі які фільтри накладаються.
- **Дії**: Створено документ `docs/data_pipeline.md` з детальним описом:
  - Етапів отримання даних з API (за dateModified та dateCreated)
  - Процесу об'єднання та дедуплікації
  - Всіх фільтрів з умовами та кодом:
    - Фільтр за датою (dateCreated OR dateModified)
    - Фільтр наявності даних
    - Фільтр статусу (тільки активні)
    - Фільтр дати старту торгів (не більше 7 днів тому)
    - Фільтр класифікаційного коду (CAV схема)
    - Фільтр типу операції (тільки продажі, не оренда)
  - Підсумкової таблиці фільтрів з номерами етапів
  - Особливостей обробки тижня (паралельна обробка по днях)

## 2026-01-15 — Етап 5: покращення обробки даних та інтеграція LLM
- **Розбиття арештів на окремі колонки**: JSON поле з арештами тепер розбивається на колонки `arrests_count` (кількість арештів) та `arrests_info` (читабельна інформація про арешти).
- **Створено конфігураційний файл**: Додано `config/config.example.yaml` як приклад конфігурації. Файл `config/config.yaml` додано в `.gitignore` для безпечного зберігання API ключів.
- **Модулі для роботи з LLM**: Створено `business/services/llm_service.py` з підтримкою трьох провайдерів:
  - Google Gemini (за замовчуванням)
  - OpenAI (ChatGPT)
  - Anthropic (Claude)
- **Rate limiting**: Додано обмеження швидкості викликів LLM API (15 викликів за хвилину за замовчуванням) для уникнення перевищення лімітів.
- **Парсинг опису аукціону через LLM**: Додано автоматичний парсинг опису аукціону для витягування структурованої інформації:
  - Кадастровий номер
  - Площа та одиниця вимірювання
  - Адреса (розкладена по колонках: область, місто, вулиця, тип вулиці, будинок)
  - Поверх
  - Тип приміщення (житлове, комерційне)
  - Підведені комунікації
- **Оновлено CSV структуру**: Додано нові колонки для парсингу опису та арештів у файли CSV.
- **Оновлено залежності**: Додано `pyyaml`, `google-generativeai`, `openai`, `anthropic` в `requirements.txt`.

## 2026-01-15 — Етап 4: збереження списку аукціонів та видалення логіки тендерів
- Прибрано фільтр по статусу з методу `get_real_estate_auctions()` - тепер зберігаються всі аукціони без фільтрації по статусу.
- Додано метод `save_auctions_to_json()` для збереження списку аукціонів у файл.
- Додано метод `fetch_and_save_real_estate_auctions()` для отримання та збереження списку аукціонів.
- Видалено всі методи для тендерів:
  - `get_real_estate_tenders()`
  - `get_tender_details()`
  - `save_tenders_to_json()`
  - `fetch_and_save_real_estate_tenders()`
  - `fetch_and_save_real_estate_tender_details()`
- Прибрано імпорти `TenderDTO` та `TendersResponseDTO` з сервісу.
- Оновлено `main.py` для використання методу збереження списку аукціонів замість деталей.

## 2026-01-15 — Етап 3: перехід з тендерів на аукціони
- Змінено етап першочергової виборки з тендерів (`/tenders`) на аукціони (`/auctions`).
- Додано DTO для аукціонів (`AuctionDTO`, `AuctionsResponseDTO`) в `transport/dto/prozorro_dto.py`.
- Оновлено налаштування: додано `prozorro_sale_api_base_url` для ProZorro.Sale API (`https://public.api.ea.openprocurement.org/api/2`).
- Додано методи в `ProZorroService`:
  - `get_real_estate_auctions()` - отримання списку аукціонів
  - `get_auction_details()` - отримання деталей конкретного аукціону
  - `fetch_and_save_real_estate_auction_details()` - повний цикл отримання та збереження
- Оновлено `main.py` для використання методів аукціонів замість тендерів.
- Оновлено глосарій розробника з термінологією для аукціонів.

## 2026-01-15 — Етап 2: деталізація оголошень (тендерів) по `id`
- Прибрано збереження "короткого списку" тендерів у файл як фінального результату.
- Додано запит деталей `GET /tenders/{id}` для кожного `id` зі списку.
- Детальні дані зберігаються в **один** JSON файл у `temp/` з `metadata` та `data`.
- Додано паузу 0.5s між запитами деталей для уникнення блокувань/rate-limit.
