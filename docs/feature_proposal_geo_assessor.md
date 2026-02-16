# Пропозиція: Агент-осінтер для гео-аналізу придатності нерухомості

## 1. Концепція

**Агент-осінтер** — LLM-агент, що на основі відкритої інформації та запиту користувача проводить аналіз **придатності оголошення до певного виду діяльності** (аптека, кафе, клініка, магазин тощо).

### Приклад сценарію

Користувач на сторінці деталей оголошення натискає «Спитати у AI» і пише:
> «Чи підходить це приміщення під аптеку?»

Агент:
1. Аналізує оголошення: поверх, площа, тип нерухомості, адреса
2. Геокодує адресу (Google Geocoding — вже є)
3. Запитує Google Places API: аптеки навколо, багатоквартирні будинки, лікарні, зупинки транспорту
4. Зіставляє з профілем «аптека» (або профілем від користувача)
5. Формує висновок: придатність, ризики, рекомендації

---

## 2. Архітектура

### 2.1 Компоненти

| Компонент | Призначення |
|-----------|-------------|
| **AssessorAgent** | LLM-агент: інтерпретує запит, планує кроки, викликає MCP tools, формує звіт |
| **PlacesService** | Сервіс для Google Places API (Nearby Search, Text Search) |
| **Places MCP Server** | MCP tools: `search_nearby_places`, `get_place_details` |
| **BusinessProfile** | Профіль виду діяльності: критерії (площа, поверх, POI-типи, радіуси) |

### 2.2 Потік даних

```
User: "Чи підходить для аптеки?"
        │
        ▼
IntentDetector → intent: "geo_assessment", business_type: "аптека"
        │
        ▼
AssessorAgent
  1. Отримує listing_context (page_url, summary) або дані з unified_listings
  2. Витягує адресу → geocode_address (MCP)
  3. Отримує профіль "аптека" (predefined або від користувача)
  4. search_nearby_places(lat, lng, types=["pharmacy","hospital","bus_station","residential_building"], radius=500)
  5. Аналізує результати + дані оголошення
  6. Формує звіт
        │
        ▼
AnswerComposer → текстова відповідь з висновками
```

### 2.3 Відповідність правилам архітектури

- **LLM не виконує дії** — тільки планує та інтерпретує
- **Доступ через MCP** — geocode, search_nearby_places, get_listing_details
- **Мінімальний доступ** — AssessorAgent отримує тільки: geocoding, places, schema (для unified_listings)

---

## 3. Google Places API

### 3.1 Що потрібно

| API | Призначення | Статус |
|-----|-------------|--------|
| Geocoding API | Адреса → координати | ✅ Вже є (GeocodingService) |
| Places API (New) | Nearby Search, Text Search | ❌ Потрібно додати |

### 3.2 Places API (New) — Nearby Search

- **Endpoint:** `POST https://places.googleapis.com/v1/places:searchNearby`
- **Параметри:**
  - `includedTypes`: `["pharmacy"]`, `["hospital"]`, `["bus_station"]`, `["apartment_building"]` тощо
  - `locationRestriction.circle`: center (lat, lng), radius (метри)
  - `maxResultCount`: 20
- **Place types** (Table A): pharmacy, hospital, bus_station, transit_station, shopping_mall, supermarket, school, university, park, restaurant, cafe тощо

### 3.3 Ключ API

- Використовується той самий `GOOGLE_MAPS_API_KEY` (Geocoding та Places — один ключ з різними enabled APIs у Google Cloud Console)
- Потрібно увімкнути **Places API (New)** у консолі

---

## 4. Профілі видів діяльності

### 4.1 Predefined-профілі

Зберігаються в `config/business_profiles.yaml` або окремому модулі:

```yaml
pharmacy:
  name: "Аптека"
  listing_criteria:
    min_area_sqm: 30
    max_area_sqm: 150
    preferred_floor: [1]  # 1 поверх
    property_types: ["Комерційна нерухомість"]
  poi_criteria:
    - type: pharmacy
      radius_m: 500
      role: competitor  # конкуренція — небажано багато
      max_count: 3
    - type: hospital
      radius_m: 1000
      role: positive
    - type: bus_station
      radius_m: 300
      role: positive
    - type: apartment_building
      radius_m: 500
      role: positive  # потік людей

cafe:
  name: "Кафе"
  listing_criteria:
    min_area_sqm: 50
    max_area_sqm: 200
    preferred_floor: [1]
  poi_criteria:
    - type: transit_station
      radius_m: 200
      role: positive
    - type: university
      radius_m: 500
      role: positive
```

### 4.2 Профіль від користувача

Користувач може описати:
> «Шукаю приміщення під клініку: 80–120 м², 1 поверх, поруч багато будинків, лікарня в 10 хв пішки, аптеки не поруч»

LLM витягує структурований профіль і передає агенту.

---

## 5. MCP Tools

### 5.1 Новий MCP-сервер: `places_mcp_server`

```python
@mcp.tool()
def search_nearby_places(
    latitude: float,
    longitude: float,
    place_types: List[str],  # ["pharmacy", "hospital", "bus_station"]
    radius_meters: int = 500,
    max_results: int = 20
) -> dict:
    """
    Пошук місць поблизу координат.
    place_types: pharmacy, hospital, bus_station, transit_station, 
                 apartment_building, supermarket, school, restaurant, cafe, park
    """
```

### 5.2 Розширення geocoding (вже є)

- `geocode_address` — вже доступний через Geocoding MCP

### 5.3 Доступ до даних оголошення

- Якщо є `listing_context` з `page_url` — можна викликати `get_unified_listing_by_source_id` або аналог для отримання повних даних (площа, поверх, адреса)
- Або передавати `summary` з listing_context — LLM витягує площа, поверх з тексту

---

## 6. Маршрутизація

### 6.1 Intent

IntentDetectorAgent додає новий intent: `geo_assessment`

- Тригери: «підходить для», «чи підходить під», «оціни для», «аналіз для аптеки/кафе/клініки»
- `response_format`: `geo_assessment`

### 6.2 Роутинг

У MultiAgentService: якщо `intent == "geo_assessment"` → виклик AssessorAgent замість звичайного пайплайну.

---

## 7. Етапи реалізації

### Фаза 1: Інфраструктура (1–2 дні)

1. **PlacesService** — клас для Google Places API (Nearby Search)
2. **places_mcp_server** — MCP tools: `search_nearby_places`
3. Конфіг: перевірка `GOOGLE_MAPS_API_KEY`, увімкнення Places API у консолі

### Фаза 2: Профілі та агент (2–3 дні)

4. **config/business_profiles.yaml** — predefined профілі (аптека, кафе, клініка)
5. **AssessorAgent** — агент з промптом, виклики MCP (geocode, search_nearby)
6. Інтеграція з LangChain (або окремий шлях у MultiAgentService)

### Фаза 3: Маршрутизація та UX (1–2 дні)

7. IntentDetectorAgent — intent `geo_assessment`
8. MultiAgentService — роутинг на AssessorAgent
9. Frontend: підказка «Наприклад: чи підходить для аптеки?» при listing_context

### Фаза 4: Профіль від користувача (опційно)

10. LLM витягує профіль з вільного тексту
11. Схема BusinessProfile для валідації

---

## 8. Приклад відповіді агента

```
**Оцінка придатності для аптеки**

**Параметри приміщення:**
- Площа: 45 м² ✓ (рекомендовано 30–150 м²)
- Поверх: 1 ✓
- Тип: Комерційна нерухомість ✓

**Оточення (радіус 500 м):**
- Аптеки: 2 (помірна конкуренція)
- Лікарні: 1 (позитивно)
- Зупинки транспорту: 3 (добре)
- Багатоквартирні будинки: ~15 (високий потік людей)

**Висновок:** Приміщення підходить для аптеки. Переваги: 1 поверх, достатня площа, наявність лікарні та транспорту. Увага: 2 аптеки поруч — варто оцінити цільову аудиторію.
```

---

## 9. Ризики та обмеження

| Ризик | Мітигація |
|-------|-----------|
| Ліміти Google Places API (платний після безкоштовного tier) | Кешування результатів по (lat, lng, types, radius) |
| Неточність адреси в оголошенні | Fallback на місто/район, попередження в звіті |
| Профіль від користувача — неоднозначність | LLM структурує; при невпевненості — уточнення |

---

## 10. Глосарій

- **AssessorAgent** — агент-осінтер, що оцінює придатність нерухомості для виду діяльності
- **BusinessProfile** — профіль виду діяльності (критерії по приміщенню та POI)
- **POI** — Point of Interest (аптека, лікарня, зупинка тощо)
