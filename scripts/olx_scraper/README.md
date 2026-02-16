# Скрапер OLX

Тестовий режим: перша сторінка «Нерухомість» → JSON.  
Прототип: нежитлова (комерційна) нерухомість — обхід перших N сторінок пошуку, збереження в MongoDB, за потреби завантаження сторінки оголошення (detail) з затримкою 2–10 с.

## Заходи антибот

- **Один запит за раз** — без паралельних/конкурентних запитів.
- **Затримка перед запитом** — 2–5 с (випадкова), налаштовується через `OLX_SCRAPER_DELAY_MIN`, `OLX_SCRAPER_DELAY_MAX`.
- **Заголовки як у браузера** — User-Agent (Chrome), Accept-Language: uk, Accept тощо.
- **Таймаут** — один запит не довше 25 с (`OLX_SCRAPER_TIMEOUT`).

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
- Якщо оголошення нове, або в базі немає блоку `detail`, або змінилась інформація зі сторінки пошуку — скрапер відкриває сторінку оголошення, парсить опис/параметри, зберігає в `detail`. Між запитами сторінок оголошень — випадкова затримка 2–10 с (`OLX_SCRAPER_DELAY_DETAIL_MIN`, `OLX_SCRAPER_DELAY_DETAIL_MAX`).

## Структура даних

- **Парсинг** — BeautifulSoup (lxml), селектори `[data-cy="l-card"]`, резервні варіанти.
- Поля оголошення: `title`, `price_text`, `price_value`, `currency`, `location`, `date_text`, `area_m2`, `url`, `raw_snippet`.
- **LLM** — зараз не використовується; поле `raw_snippet` можна передавати в LLM окремо для нормалізації (локація/дата, ціна).

## Змінні середовища

| Змінна | Опис | За замовчуванням |
|--------|------|-------------------|
| `OLX_SCRAPER_BASE_URL` | Базовий URL сайту | `https://www.olx.ua` |
| `OLX_SCRAPER_DELAY_MIN` / `OLX_SCRAPER_DELAY_MAX` | Затримка перед запитом (с) | 2, 5 |
| `OLX_SCRAPER_TIMEOUT` | Таймаут запиту (с) | 25 |
| `OLX_SCRAPER_USER_AGENT` | User-Agent | Chrome 120 |
| `OLX_SCRAPER_OUTPUT_DIR` | Каталог виводу | `output` |
| `OLX_SCRAPER_OUTPUT_FILE` | Ім’я файлу JSON | `olx_nedvizhimost_page1.json` |
| `OLX_SCRAPER_DELAY_DETAIL_MIN` / `OLX_SCRAPER_DELAY_DETAIL_MAX` | Затримка перед запитом сторінки оголошення (с) | 2, 10 |
| `OLX_SCRAPER_MAX_PAGES` | Кількість сторінок пошуку (прототип) | 5 |

## Телефони продавця

Телефони витягуються з HTML сторінки оголошення (посилання `tel:`, regex-патерни). Зберігаються в `detail.contact.phones`.

Тест: `py scripts/olx_scraper/test_phone_reveal.py [URL_оголошення]`
