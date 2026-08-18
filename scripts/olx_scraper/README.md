# Скрапер OLX

Тестовий режим: перша сторінка «Нерухомість» → JSON.  
Прототип: нежитлова (комерційна) нерухомість — обхід перших N сторінок пошуку, збереження в MongoDB, за потреби завантаження сторінки оголошення (detail).

## Заходи антибот

- **List через HTTP, detail через спільний BrowserPool** — один Chromium, обмежена кількість page-слотів (low-RAM).
- **Серіалізація HTTP list** — між Phase1-потоками максимум `OLX_SCRAPER_LIST_HTTP_CONCURRENCY` паралельних list-запитів (дефолт 1).
- **HTTP 403** — довгий backoff (`OLX_SCRAPER_403_BACKOFF_MIN`/`MAX`, дефолт 15–45 с); після вичерпання спроб — **fallback list через браузер** (`BrowserPagePool.get_list_page`), без скіпу сторінок пагінації.
- **Затримка перед list-запитом** — 0.5–1.5 с (випадкова), `OLX_SCRAPER_DELAY_MIN` / `OLX_SCRAPER_DELAY_MAX`.
- **Затримка перед detail** — 0.5–2 с, `OLX_SCRAPER_DELAY_DETAIL_MIN` / `OLX_SCRAPER_DELAY_DETAIL_MAX`.
- **Заголовки як у браузера** — User-Agent (Chrome), Accept-Language: uk, Sec-Ch-Ua тощо.
- **Таймаут** — запит пошуку до 25 с (`OLX_SCRAPER_TIMEOUT`); сторінка оголошення — до 90 с (`OLX_SCRAPER_DETAIL_TIMEOUT`).
- **Підміна куків** — опційно куки з браузера через `OLX_SCRAPER_COOKIES` (JSON) або `OLX_SCRAPER_COOKIES_FILE`.
- **Виявлення антиботу** — на сторінці шукаються ознаки капчі/Cloudflare/перевірки; при виявленні — одна повторна спроба через 8 с.
- HTTP list **не** чекає `DELAY_AFTER_PAGE_LOAD` (повний HTML уже в відповіді). Browser list path може використовувати `OLX_SCRAPER_DELAY_AFTER_LOAD` (дефолт 0).

## Запуск

З кореня проекту:

```bash
py scripts/olx_scraper/run_test.py
```

Результат: `scripts/olx_scraper/output/olx_nedvizhimost_page1.json`.

### Прототип (нежитлова нерухомість → MongoDB)

```bash
py scripts/olx_scraper/run_prototype.py
```

- Категорія: комерційна нерухомість (`/uk/nedvizhimost/kommercheskaya-nedvizhimost/`).
- Обмеження для тестів: перші 5 сторінок пошуку (`OLX_SCRAPER_MAX_PAGES=5`).
- Кожне оголошення зберігається в колекції `olx_listings` (MongoDB): `url`, `search_data`, опційно `detail`, `created_at`, `updated_at`.
- Якщо оголошення нове, або в базі немає блоку `detail`, або змінилась інформація зі сторінки пошуку — скрапер відкриває сторінку оголошення, парсить опис/параметри, зберігає в `detail`.

## Структура даних

- **Парсинг** — BeautifulSoup (lxml), селектори `[data-cy="l-card"]`, резервні варіанти.
- Поля оголошення: `title`, `price_text`, `price_value`, `currency`, `location`, `date_text`, `area_m2`, `url`, `raw_snippet`.
- **LLM** — зараз не використовується; поле `raw_snippet` можна передавати в LLM окремо для нормалізації (локація/дата, ціна).

## Змінні середовища

| Змінна | Опис | За замовчуванням |
|--------|------|-------------------|
| `OLX_SCRAPER_BASE_URL` | Базовий URL сайту | `https://www.olx.ua` |
| `OLX_SCRAPER_DELAY_MIN` / `OLX_SCRAPER_DELAY_MAX` | Затримка перед list-запитом (с) | 0.5, 1.5 |
| `OLX_SCRAPER_403_BACKOFF_MIN` / `OLX_SCRAPER_403_BACKOFF_MAX` | Пауза перед повтором після HTTP 403 (с) | 15, 45 |
| `OLX_SCRAPER_LIST_HTTP_CONCURRENCY` | Макс. паралельних HTTP list між потоками | 1 |
| `OLX_SCRAPER_LIST_FALLBACK_BROWSER_ON_403` | Fallback list через BrowserPool після 403 | `1` |
| `OLX_SCRAPER_TIMEOUT` | Таймаут запиту (с) | 25 |
| `OLX_SCRAPER_USER_AGENT` | User-Agent | Chrome 120 |
| `OLX_SCRAPER_OUTPUT_DIR` | Каталог виводу | `output` |
| `OLX_SCRAPER_OUTPUT_FILE` | Ім’я файлу JSON | `olx_nedvizhimost_page1.json` |
| `OLX_SCRAPER_DELAY_DETAIL_MIN` / `OLX_SCRAPER_DELAY_DETAIL_MAX` | Затримка перед detail (с) | 0.5, 2 |
| `OLX_SCRAPER_DELAY_AFTER_LOAD` | Пауза після browser list load (с); HTTP ігнорує | 0 |
| `OLX_SCRAPER_DETAIL_POST_GOTO_SETTLE_MAX` | Макс. jitter після goto detail перед selector wait (с) | 0.3 |
| `OLX_SCRAPER_DETAIL_TIMEOUT` | Таймаут запиту сторінки оголошення (с) | 90 |
| `OLX_SCRAPER_BROWSER_DOCKER_SAFE_ARGS` | Додає docker-safe args для Chromium (`--disable-dev-shm-usage`, `--no-sandbox`) | `1` у Docker, інакше `0` |
| `OLX_SCRAPER_COOKIES` | JSON-рядок куків `[{"name":"...","value":"..."}]` (підміна з браузера) | — |
| `OLX_SCRAPER_COOKIES_FILE` | Шлях до файлу з JSON куків | — |
| `OLX_SCRAPER_MAX_PAGES` | Кількість сторінок пошуку (прототип) | 5 |
| `OLX_SCRAPER_PHASE1_MAX_THREADS` | Потоки Phase 1 (list/jobs); detail через спільний BrowserPool | `3` у Docker, інакше `5` |
| `OLX_SCRAPER_BROWSER_POOL_SIZE` | Розмір BrowserPool. `0` = low-RAM дефолт (`1` Docker / `2` host), **не** = числу потоків | `0` (ефективно 1/2) |
| `OLX_SCRAPER_BROWSER_POOL_FETCH_BUDGET_SEC` | Жорсткий бюджет одного detail-fetch у слоті (с); після — force-close page + recovery | `160` |
| `OLX_SCRAPER_BROWSER_POOL_CLIENT_TIMEOUT_SEC` | Таймаут очікування клієнта на слот (с); після — restart pool + 1 retry | `200` |
| `OLX_SCRAPER_BROWSER_POOL_RESTART_AFTER_HANGS` | Після N поспіль hang у слоті — повний restart Chromium | `2` |

**Deprecated:** legacy `run_olx_update` з окремим Chromium на регіон (високе RAM). Production шлях — `run_olx_update_raw_only` + `BrowserPagePool`.

## Телефони продавця

Телефони витягуються з HTML сторінки оголошення (посилання `tel:`, regex-патерни). Зберігаються в `detail.contact.phones`.

Тест: `py scripts/olx_scraper/test_phone_reveal.py [URL_оголошення]`
