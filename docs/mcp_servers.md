# MCP Сервери

Проект містить MCP (Model Context Protocol) сервери для надання контексту про структуру даних у базі даних та безпечного виконання запитів.

## Запуск MCP серверів

### Через окремий скрипт

```bash
py scripts/start_mcp_servers.py
```

### При старті застосунку

```bash
py main.py --start-mcp
```

### Окремо кожен сервер

```bash
py -m mcp_servers.schema_mcp_server
py -m mcp_servers.query_builder_mcp_server
py -m mcp_servers.analytics_mcp_server
py -m mcp_servers.report_mcp_server
```

## Schema MCP Server

MCP сервер `schema-mcp` надає схему метаданих колекцій бази даних на основі реальних даних.

### Доступні колекції

Сервер надає доступ тільки до наступних колекцій:
- `prozorro_auctions` - колекція з даними про аукціони ProZorro
- `llm_cache` - колекція з кешованими результатами парсингу описів через LLM

Колекції з налаштуваннями користувачів та системи (`users`, `logs`, `app_data`) не доступні через MCP сервер.

### Ресурси

#### `mongodb://schema`

Ресурс, який повертає повну схему метаданих всіх доступних колекцій у форматі JSON.

Схема містить:
- `generated_at` - час генерації схеми
- `collections` - словник зі схемами кожної колекції:
  - `collection_name` - назва колекції
  - `total_documents` - загальна кількість документів
  - `analyzed_documents` - кількість проаналізованих документів
  - `schema` - детальна схема структури документів
  - `indexes` - список індексів колекції
  - `relationships` - зв'язки з іншими колекціями
- `global_relationships` - глобальні зв'язки між колекціями

### Інструменти (Tools)

#### `refresh_schema_cache`

Оновлює кеш схеми метаданих.

**Параметри**: немає

**Повертає**: 
```json
{
  "success": true,
  "message": "Кеш схеми оновлено успішно",
  "generated_at": "2026-01-23T12:00:00"
}
```

#### `get_collection_info`

Отримує детальну інформацію про конкретну колекцію.

**Параметри**:
- `collection_name` (string) - назва колекції (`prozorro_auctions` або `llm_cache`)

**Повертає**:
```json
{
  "success": true,
  "collection": {
    "collection_name": "prozorro_auctions",
    "description": "Колекція з даними про аукціони ProZorro.Sale",
    "from_data_dictionary": true,
    "total_documents": 1000,
    "analyzed_documents": 100,
    "schema": { ... },
    "indexes": [ ... ],
    "relationships": [ ... ]
  }
}
```

#### `get_data_dictionary`

Повертає повний Data Dictionary.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "data_dictionary": {
    "generated_from": "data_dictionary",
    "collections": { ... }
  },
  "metadata": {
    "version": "1.0.0",
    "last_updated": "2026-01-23",
    "description": "..."
  }
}
```

#### `apply_validation_schema`

Застосовує validation schema до колекції на основі Data Dictionary.

**Параметри**:
- `collection_name` (string) - назва колекції

**Повертає**:
```json
{
  "success": true,
  "message": "Validation schema успішно застосовано до колекції prozorro_auctions"
}
```

#### `validate_collection`

Валідує всі документи в колекції на основі Data Dictionary.

**Параметри**:
- `collection_name` (string) - назва колекції

**Повертає**:
```json
{
  "success": true,
  "collection": "prozorro_auctions",
  "total_documents": 1000,
  "valid_documents": 950,
  "invalid_documents": 50,
  "errors_summary": {
    "Поле 'status' має невірне значення": 10,
    ...
  }
}
```

### Запуск сервера

Для запуску MCP сервера використовуйте:

```bash
py -m mcp_servers.schema_mcp_server
```

Або через uvrun (якщо встановлено):

```bash
uvrun --with mcp mcp_servers/schema_mcp_server.py
```

### Налаштування в Cursor

Для використання MCP сервера в Cursor додайте до конфігурації (`.cursor/mcp.json` або налаштування Cursor):

```json
{
  "mcpServers": {
    "schema-mcp": {
      "command": "py",
      "args": [
        "-m",
        "mcp_servers.schema_mcp_server"
      ],
      "env": {
        "PYTHONPATH": "."
      }
    }
  }
}
```

### Структура схеми

Схема кожного поля містить:
- `type` - тип даних (string, integer, number, boolean, datetime, array, object, mixed)
- `nullable` - чи може поле бути null
- `occurrence_rate` - частота появи поля (0.0 - 1.0)
- `possible_types` - можливі типи (якщо type = 'mixed')
- `examples` - приклади значень (для простих типів)
- `nested_structure` - вкладені структури (для об'єктів)
- `item_structure` - структура елементів масиву (для масивів об'єктів)
- `item_type` - тип елементів масиву (для простих масивів)

### Зв'язки між колекціями

Сервер автоматично виявляє зв'язки між колекціями:
- `llm_cache.description_hash` ↔ `prozorro_auctions.description_hash` - зв'язок один-до-багатьох (один кешований результат може бути пов'язаний з кількома аукціонами з однаковим описом)

### Data Dictionary

Сервер використовує Data Dictionary (`config/data_dictionary.yaml`) як єдине джерело правди про структуру даних. Схема генерується на основі Data Dictionary, доповнюючись реальною статистикою з бази даних.

Для генерації документації з Data Dictionary використовуйте:
```bash
py scripts/generate_documentation.py
```

Це створить файли:
- `docs/data_dictionary.md` - Markdown документація
- `docs/data_dictionary.schema.json` - JSON Schema

---

## Query Builder MCP Server

MCP сервер `query-builder-mcp` надає безпечний API для виконання запитів до MongoDB через абстрактний інтерфейс.

### Безпека

Сервер забезпечує безпеку запитів:
- ❌ Заборонено: `$where`, `$eval`, `$function`, `$expr`, `$regex`, `$text`
- ✅ Дозволено: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$not`
- 🔒 Обмеження: максимум 100 результатів на запит
- 🔒 Обмеження: максимальна глибина вкладеності фільтрів - 5 рівнів

### Інструменти (Tools)

#### `execute_query`

Виконує безпечний запит до MongoDB.

**Параметри**:
```json
{
  "collection": "prozorro_auctions",
  "filters": {
    "status": "finished",
    "region": "Київська"
  },
  "join": [
    {
      "collection": "llm_cache",
      "on": ["description_hash", "description_hash"],
      "as": "llm_result",
      "unwrap": true
    }
  ],
  "projection": ["auction_id", "status", "llm_result"],
  "limit": 10
}
```

**Повертає**:
```json
{
  "success": true,
  "results": [...],
  "count": 10
}
```

#### `validate_query`

Валідує абстрактний запит без його виконання.

**Параметри**: такі самі, як у `execute_query`

**Повертає**:
```json
{
  "success": true,
  "valid": true,
  "message": "Запит валідний"
}
```

або

```json
{
  "success": true,
  "valid": false,
  "error": "Помилка валідації"
}
```

#### `get_allowed_collections`

Повертає список дозволених колекцій для запитів.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "collections": ["prozorro_auctions", "llm_cache"],
  "max_results": 100
}
```

#### `get_allowed_operators`

Повертає список дозволених та заборонених операторів.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "allowed_operators": ["$and", "$eq", "$exists", ...],
  "forbidden_operators": ["$eval", "$expr", "$function", ...]
}
```

### Формат абстрактного запиту

#### Базовий запит

```json
{
  "collection": "prozorro_auctions",
  "filters": {
    "status": "finished"
  },
  "limit": 10
}
```

#### Запит з join

```json
{
  "collection": "prozorro_auctions",
  "filters": {
    "status": "finished"
  },
  "join": [
    {
      "collection": "llm_cache",
      "on": ["description_hash", "description_hash"],
      "as": "llm_result",
      "unwrap": true
    }
  ],
  "projection": ["auction_id", "status", "llm_result"],
  "limit": 10
}
```

#### Запит зі складними фільтрами

```json
{
  "collection": "prozorro_auctions",
  "filters": {
    "$and": [
      {"status": "finished"},
      {"$or": [
        {"region": "Київська"},
        {"region": "Львівська"}
      ]}
    ]
  },
  "limit": 20
}
```

### Поля запиту

- `collection` (обов'язкове) - назва колекції (`prozorro_auctions` або `llm_cache`)
- `filters` (опціональне) - фільтри для пошуку
- `join` (опціональне) - список join операцій
  - `collection` - колекція для join
  - `on` - список з двох полів: `[localField, foreignField]`
  - `as` (опціональне) - назва поля для результату (за замовчуванням: `{collection}_joined`)
  - `unwrap` (опціональне) - чи розгортати масив результатів (за замовчуванням: `false`)
- `projection` (опціональне) - список полів для повернення
- `limit` (опціональне) - максимальна кількість результатів (максимум 100)

### Налаштування в Cursor

Для використання обох MCP серверів в Cursor додайте до конфігурації:

```json
{
  "mcpServers": {
    "schema-mcp": {
      "command": "py",
      "args": ["-m", "mcp_servers.schema_mcp_server"],
      "env": {
        "PYTHONPATH": "."
      }
    },
    "query-builder-mcp": {
      "command": "py",
      "args": ["-m", "mcp_servers.query_builder_mcp_server"],
      "env": {
        "PYTHONPATH": "."
      }
    },
    "analytics-mcp": {
      "command": "py",
      "args": ["-m", "mcp_servers.analytics_mcp_server"],
      "env": {
        "PYTHONPATH": "."
      }
    },
    "report-mcp": {
      "command": "py",
      "args": ["-m", "mcp_servers.report_mcp_server"],
      "env": {
        "PYTHONPATH": "."
      }
    }
  }
}
```

---

## Report MCP Server

MCP сервер `report-mcp` надає API для генерації звітів у різних форматах. LLM лише описує що хоче (формат, шаблон, джерело даних, колонки), а сервер сам бере шаблон, генерує файл та віддає URL або base64.

### Підтримувані формати

- `xlsx` - Microsoft Excel (OpenXML)
- `csv` - Comma-Separated Values
- `json` - JavaScript Object Notation

### Шаблони звітів

- `auction_summary` - Звіт з підсумками аукціонів
- `price_analysis` - Аналіз цін по регіонах
- `property_types` - Аналіз по типах нерухомості
- `time_series` - Динаміка по часу
- `simple_list` - Простий список даних

### Інструменти (Tools)

#### `generate_report`

Генерує звіт у вказаному форматі.

**Параметри**:
```json
{
  "format": "xlsx",
  "template": "auction_summary",
  "dataSource": "analytics-mcp:average_price_per_m2",
  "columns": ["region", "value"]
}
```

Або з повним запитом:
```json
{
  "format": "xlsx",
  "template": "auction_summary",
  "dataSource": "analytics-mcp:{\"metric\":\"average_price_per_m2\",\"groupBy\":[\"region\"]}",
  "columns": ["region", "value"]
}
```

**Параметри**:
- `request` (Dict) - Запит на генерацію звіту
- `return_base64` (bool, опціональне) - Чи повертати файл у base64 (за замовчуванням: true)

**Повертає** (base64):
```json
{
  "success": true,
  "format": "xlsx",
  "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "data": "UEsDBBQAAAAI...",
  "encoding": "base64",
  "size": 12345
}
```

**Повертає** (URL):
```json
{
  "success": true,
  "format": "xlsx",
  "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "url": "temp/reports/report_20240123_120000.xlsx",
  "filename": "report_20240123_120000.xlsx",
  "size": 12345
}
```

#### `validate_report_request`

Валідує запит на генерацію звіту без його виконання.

**Параметри**: такі самі, як у `generate_report`

**Повертає**:
```json
{
  "success": true,
  "valid": true,
  "message": "Запит валідний"
}
```

#### `list_templates`

Повертає список доступних шаблонів звітів.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "templates": [
    {
      "name": "auction_summary",
      "description": "Звіт з підсумками аукціонів",
      "format": "xlsx",
      "default_columns": ["region", "avg_price_m2", "auctions_count"],
      "column_headers": {
        "region": "Область",
        "avg_price_m2": "Середня ціна за м²",
        "auctions_count": "Кількість аукціонів"
      },
      "required_columns": ["region"]
    },
    ...
  ]
}
```

#### `get_template_info`

Отримує детальну інформацію про шаблон.

**Параметри**:
- `template_name` (string) - назва шаблону

**Повертає**:
```json
{
  "success": true,
  "template": {
    "name": "auction_summary",
    "description": "Звіт з підсумками аукціонів",
    "format": "xlsx",
    "default_columns": ["region", "avg_price_m2", "auctions_count"],
    "column_headers": {...},
    "required_columns": ["region"]
  }
}
```

#### `get_supported_formats`

Повертає список підтримуваних форматів файлів.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "formats": [
    {
      "name": "xlsx",
      "description": "Microsoft Excel (OpenXML)",
      "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    },
    {
      "name": "csv",
      "description": "Comma-Separated Values",
      "mime_type": "text/csv"
    },
    {
      "name": "json",
      "description": "JavaScript Object Notation",
      "mime_type": "application/json"
    }
  ]
}
```

### Формат запиту на генерацію звіту

#### Простий запит з назвою метрики

```json
{
  "format": "xlsx",
  "template": "auction_summary",
  "dataSource": "analytics-mcp:average_price_per_m2",
  "columns": ["region", "value"]
}
```

#### Запит з повним JSON запитом до analytics-mcp

```json
{
  "format": "xlsx",
  "template": "auction_summary",
  "dataSource": "analytics-mcp:{\"metric\":\"average_price_per_m2\",\"groupBy\":[\"region\"],\"filters\":{\"status\":\"finished\"}}",
  "columns": ["region", "value"]
}
```

#### Запит з query-builder-mcp

```json
{
  "format": "csv",
  "template": "simple_list",
  "dataSource": "query-builder-mcp:{\"collection\":\"prozorro_auctions\",\"filters\":{\"status\":\"finished\"},\"limit\":100}",
  "columns": ["auction_id", "status"]
}
```

### Поля запиту

- `format` (обов'язкове) - формат файлу (`xlsx`, `csv`, `json`)
- `template` (опціональне) - назва шаблону (використовується для default_columns та column_headers)
- `dataSource` (обов'язкове) - джерело даних у форматі `mcp-server:query`:
  - `analytics-mcp:metric_name` - простий формат з назвою метрики
  - `analytics-mcp:{"metric":"...","groupBy":[...]}` - повний JSON запит
  - `query-builder-mcp:{"collection":"...","filters":{...}}` - запит до query-builder
- `columns` (обов'язкове) - список колонок для включення в звіт

### Джерела даних

#### analytics-mcp

Підтримує два формати:
1. Простий: `analytics-mcp:average_price_per_m2` - автоматично створює запит з метрикою
2. Повний: `analytics-mcp:{"metric":"average_price_per_m2","groupBy":["region"],"filters":{...}}` - повний JSON запит

#### query-builder-mcp

Повний JSON запит: `query-builder-mcp:{"collection":"prozorro_auctions","filters":{...},"limit":100}`

---

## Analytics MCP Server

MCP сервер `analytics-mcp` надає API для виконання аналітичних запитів з метриками та агрегаціями. LLM не повинна вигадувати агрегації - вона використовує готові метрики, які сервер знає як обчислювати.

### Метрики

Сервер знає формули для наступних метрик:
- `average_price_per_m2` - Середня ціна за квадратний метр (priceFinal / area)
- `total_price` - Загальна ціна
- `base_price` - Стартова ціна
- `area` - Площа
- `building_area` - Площа будівлі
- `land_area` - Площа земельної ділянки
- `count` - Кількість записів

### Інструменти (Tools)

#### `execute_analytics`

Виконує аналітичний запит з метриками та агрегаціями.

**Параметри**:
```json
{
  "metric": "average_price_per_m2",
  "groupBy": ["region"],
  "filters": {
    "status": "finished",
    "dateEnd": {
      "from": "2024-01-01",
      "to": "2024-12-31"
    }
  }
}
```

**Повертає**:
```json
{
  "success": true,
  "metric": "average_price_per_m2",
  "metric_description": "Середня ціна за квадратний метр",
  "unit": "UAH/m²",
  "group_by": ["region"],
  "results": [
    {
      "region": "Київська",
      "value": 15000.50,
      "unit": "UAH/m²"
    },
    {
      "region": "Львівська",
      "value": 12000.75,
      "unit": "UAH/m²"
    }
  ],
  "count": 2
}
```

#### `validate_analytics_query`

Валідує аналітичний запит без його виконання.

**Параметри**: такі самі, як у `execute_analytics`

**Повертає**:
```json
{
  "success": true,
  "valid": true,
  "message": "Запит валідний"
}
```

#### `list_metrics`

Повертає список доступних метрик.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "metrics": [
    {
      "name": "average_price_per_m2",
      "description": "Середня ціна за квадратний метр",
      "unit": "UAH/m²",
      "required_fields": ["priceFinal", "area"]
    },
    ...
  ]
}
```

#### `get_metric_info`

Отримує детальну інформацію про конкретну метрику.

**Параметри**:
- `metric_name` (string) - назва метрики

**Повертає**:
```json
{
  "success": true,
  "metric": {
    "name": "average_price_per_m2",
    "description": "Середня ціна за квадратний метр",
    "unit": "UAH/m²",
    "required_fields": ["priceFinal", "area"]
  }
}
```

#### `get_allowed_group_by_fields`

Повертає список дозволених полів для групування.

**Параметри**: немає

**Повертає**:
```json
{
  "success": true,
  "group_by_fields": ["region", "city", "property_type", "status", "year", "month", "quarter"]
}
```

### Формат аналітичного запиту

#### Базовий запит з метрикою

```json
{
  "metric": "average_price_per_m2",
  "filters": {
    "status": "finished"
  }
}
```

#### Запит з групуванням

```json
{
  "metric": "average_price_per_m2",
  "groupBy": ["region"],
  "filters": {
    "status": "finished"
  }
}
```

#### Запит з діапазоном дат

```json
{
  "metric": "total_price",
  "groupBy": ["region", "year"],
  "filters": {
    "status": "finished",
    "dateEnd": {
      "from": "2024-01-01",
      "to": "2024-12-31"
    }
  }
}
```

#### Запит з кількома полями групування

```json
{
  "metric": "count",
  "groupBy": ["region", "property_type"],
  "filters": {
    "status": "finished"
  }
}
```

### Поля запиту

- `metric` (обов'язкове) - назва метрики
- `groupBy` (опціональне) - список полів для групування:
  - `region` - регіон
  - `city` - місто
  - `property_type` - тип нерухомості
  - `status` - статус аукціону
  - `year` - рік
  - `month` - місяць
  - `quarter` - квартал
- `filters` (опціональне) - фільтри для пошуку:
  - Прості фільтри: `{"status": "finished"}`
  - Діапазони дат: `{"dateEnd": {"from": "2024-01-01", "to": "2024-12-31"}}`
  - Доступні поля дат: `dateEnd`, `dateStart`, `dateModified`, `dateCreated`
