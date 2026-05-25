# Скрапер mista.ua (населені пункти)

Джерело: [Пошук населених пунктів](https://mista.ua/%D0%9F%D0%BE%D1%88%D1%83%D0%BA_%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%85_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%96%D0%B2) (~2751 НП).

## Стратегія збору списку

1. Завантажити сторінку пошуку → розпарсити фільтр **Область** (`?obl=<id>`).
2. **Послідовно для кожної області**: GET `/?obl=ID` (стор. 1), POST на той самий URL (стор. 2+).
3. У межах області — усі сторінки пагінації (`citySPG`), потім наступна область.

Без фільтра `obl` список змішує НП з усієї країни (69 стор. × 40 рядків) і не покриває повний каталог по регіонах.

## Змінні середовища

| Змінна | За замовчуванням |
|--------|------------------|
| `MISTA_SCRAPER_DELAY_MIN` | 2 |
| `MISTA_SCRAPER_DELAY_MAX` | 5 |
| `MISTA_SCRAPER_TIMEOUT` | 45 |

Пагінація в межах області: **POST** на `/Пошук_населених_пунктів/?obl=ID` з `reload=ajax`, `cscontent=1`, `citySPG=N`.
Query `?page=2` повертає **404** — не використовувати.

## Запуск

```bash
# Міграція індексів (один раз)
py scripts/migrations/057_mista_settlement_metadata.py

# Список + деталі (повний прогін ~2–3 год)
py scripts/mista_scraper/run_scrape.py

# Лише список (усі області)
py scripts/mista_scraper/run_scrape.py --list-only

# Одна область (тест)
py scripts/mista_scraper/run_scrape.py --list-only --region "Волинська"

# Деталі з resume (пропуск parsed)
py scripts/mista_scraper/run_scrape.py --details --limit 50

# Імпорт у cities
py scripts/mista_scraper/run_import.py
```

## Адмін-панель (Telegram Mini App)

Вкладка **Адміністрування → Кадастр** — блок «Скрапер mista.ua»:
- тест (3 стор. + 10 деталей), повний скрап, лише список/деталі, імпорт у `cities`;
- прогрес-бар і polling `GET /api/admin/mista-scraper/status`.

## Колекції

- `raw_mista_settlements` — сирі дані (`list_only` → `parsed`)
- `cities` — після `run_import.py`
