# Data Dictionary

Data Dictionary - це єдине джерело правди про структуру даних у базі даних проекту Pazuzu.

## Місцезнаходження

Data Dictionary зберігається у файлі `config/data_dictionary.yaml`.

## Використання

Data Dictionary використовується для:

1. **MCP сервери** - схема метаданих генерується на основі Data Dictionary
2. **MongoDB валідація** - автоматична генерація та застосування validation schema
3. **Генерація документації** - автоматична генерація Markdown та JSON Schema документації

## Структура

Data Dictionary містить:

- **collections** - визначення колекцій з їх полями
- **field_types** - допоміжні типи даних
- **metadata** - метадані про версію та дату оновлення

## Формат визначення колекції

```yaml
collections:
  collection_name:
    description: "Опис колекції"
    mongo_collection: "actual_collection_name"
    indexes:
      - field: "field_name"
        unique: true
        description: "Опис індексу"
    fields:
      field_name:
        type: "string"
        required: true
        description: "Опис поля"
        unit: "UAH"
        example: "Приклад значення"
        enum: ["value1", "value2"]
        nested_fields:
          nested_field:
            type: "string"
            description: "Вкладене поле"
    relationships:
      - type: "reference"
        field: "field_name"
        target_collection: "target_collection"
        target_field: "target_field"
        cardinality: "many-to-one"
```

## Генерація документації

Для генерації документації з Data Dictionary використовуйте:

```bash
py scripts/generate_documentation.py
```

Це створить:
- `docs/data_dictionary.md` - Markdown документація
- `docs/data_dictionary.schema.json` - JSON Schema

## MongoDB Валідація

Для застосування validation schema до колекції:

```python
from utils.mongodb_validator import MongoDBValidator

validator = MongoDBValidator()
success, error = validator.apply_validation_schema('prozorro_auctions')
```

Для валідації всіх документів у колекції:

```python
result = validator.validate_collection('prozorro_auctions')
```

## Оновлення Data Dictionary

При зміні структури даних:

1. Оновіть `config/data_dictionary.yaml`
2. Запустіть генерацію документації: `py scripts/generate_documentation.py`
3. Застосуйте validation schema до колекцій через MCP сервер або напряму

## Підтримувані типи полів

- `string` - Рядок тексту
- `number` - Число (integer або float)
- `boolean` - Булеве значення
- `datetime` - Дата та час
- `ObjectId` - MongoDB ObjectId
- `object` - Вкладений об'єкт
- `array` - Масив значень
