# 🛠 Опис виправлення: 0 результатів при фільтрації по місту (unified_listings)

**Придатний для Jira / технічної документації**

---

## 🔍 Проблема

При виконанні запиту:

> "Яка найдорожча нерухомість в Києві?"

система повертала 0 результатів, хоча:

- у колекції `unified_listings` є 982 документи
- поле з містом існує (`addresses.settlement`)
- `distinct` по цьому полю повертає 100+ унікальних значень
- "Київ" присутній у даних

Pipeline виконувався з `success=True`, але результатів 0.

---

## 🎯 Кореневі причини

### 1. Неправильний синтаксис `$unwind`

MongoDB вимагає, щоб шлях у `$unwind` мав префікс `$`:

```
path option to $unwind stage should be prefixed with a '$': addresses
```

**Було:** `{"$unwind": "addresses"}`  
**Має бути:** `{"$unwind": "$addresses"}`

Без префіксу aggregation падала з `OperationFailure`.

### 2. Архітектурна проблема: логічні vs фізичні поля

LLM працює з логічними полями (`city`, `price`, `region`), а Mongo — з фізичною схемою.

У `unified_listings` структура інша:
- `city` → фактично `addresses[].settlement` (масив об'єктів)
- `region` → фактично `addresses[].region`
- `price` → `price_uah`

Для масивів не можна використовувати прямий маппінг `city: "Київ"` — потрібен `$elemMatch` по масиву `addresses`.

### 3. Непідставлені параметри

У шаблоні пайплайну умови були параметризовані: `{region: "$region", city: "$city"}`.  
Якщо `region` не потрапляв у параметри, залишався рядок `"$region"`, який оброблявся як значення для regex — створювалися умови, що ніколи не збігалися.

---

## ✅ Виправлення

### 1. Field Mapping Layer (`SourceFieldMapper`)

Централізований маппінг логічних полів на фізичні шляхи:

```python
FIELD_MAP = {
    "unified_listings": {
        "city": "addresses.settlement",
        "region": "addresses.region",
        "price": "price_uah",
        ...
    },
    "olx_listings": { "city": "detail.address_refs.city.name", ... },
    "prozorro_auctions": { "city": "auction_data.address_refs.city.name", ... },
}
```

Додаткові методи:
- `get_addresses_array_path(collection)` — шлях до масиву для `$elemMatch` (`addresses`, `auction_data.address_refs`, тощо)
- `get_geo_match_keys(collection)` — ключі всередині елемента (`region`, `settlement` для unified_listings; `region.name`, `city.name` для prozorro/olx)

### 2. Порядок stages: `$unwind` → `$match`

Правильний порядок:
1. `$unwind $addresses` — розгортаємо масив
2. `$match` за `addresses.settlement` — фільтруємо після unwind
3. `$sort`, `$limit`

```javascript
[
  { "$unwind": "$addresses" },
  {
    "$match": {
      "$or": [
        { "addresses.settlement": { "$regex": "^Київ", "$options": "i" } },
        { "addresses.settlement": { "$regex": "^м\\. Київ", "$options": "i" } }
      ]
    }
  },
  { "$sort": { "price_uah": -1 } },
  { "$limit": 1 }
]
```

**Неправильно:** `$match` перед `$unwind` — не відповідає очікуваній поведінці.

### 3. Префікс `$` для `$unwind`

```python
unwind_path_mongo = unwind_path if unwind_path.startswith("$") else f"${unwind_path}"
pipeline.insert(0, {"$unwind": unwind_path_mongo})
```

### 4. Ігнорування непідставлених параметрів

У `GeoFilterBuilder.build_geo_filter()` значення, що починаються з `$` або `{{`, не використовуються як фільтри:

```python
if not _is_substituted(city_value):
    city_value = None
if not _is_substituted(region_value):
    region_value = None
```

### 5. Підтримка `unified_listings` у валідації

`PipelineBuilderAgent._validate_filter_step()` — додано `unified_listings` до списку дозволених колекцій.

---

## 📋 Змінені компоненти

| Компонент | Зміни |
|-----------|-------|
| `utils/source_field_mapper.py` | Маппінг для `unified_listings`, `get_addresses_array_path`, `get_geo_match_keys` |
| `utils/geo_filter_builder.py` | Префікс `$` для `$unwind`, ігнорування непідставлених параметрів, підтримка `region`/`settlement` |
| `business/agents/pipeline_builder_agent.py` | `unified_listings` у allowed collections |
| `business/services/pipeline_interpreter_service.py` | Використання SourceFieldMapper для геофільтрів |

---

## 🔗 Пов’язані документи

- `docs/development_history.md` — історія розробки
- `.cursor/rules/prozorro-data-structures.mdc` — правила для address_refs
