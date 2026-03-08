# План зменшення токенів та альтернативних методів парсингу оголошень

## 1. Поточний стан

### 1.1 Де використовується LLM для парсингу

| Компонент | Метод | Джерело даних |
|-----------|-------|---------------|
| OlxLLMExtractorService | extract_structured_data → parse_auction_description | OLX (search_data + detail) |
| ProZorroService | _process_auction_with_llm → parse_auction_description | ProZorro (auction_data.description) |
| InterpreterAgent | extract_structured_from_text → parse_auction_description | Вільний текст |

### 1.2 Що витягується з опису

- **cadastral_number** — кадастровий номер
- **building_area_sqm** — площа нерухомості (м²)
- **land_area_ha** — площа землі (га)
- **addresses** — масив адрес (region, district, settlement, street, building…)
- **floor** — поверх
- **property_type** — тип нерухомості
- **utilities** — комунікації
- **tags** — теги (крамниця, газ, вода…)
- **arrests_info** — інформація про арешти

### 1.3 Кеш

- LLMCacheService: ключ = MD5(нормалізований опис)
- Однаковий текст → один виклик LLM, далі з кешу

---

## 2. Стратегії зменшення токенів і запитів

### 2.1 Каскадний підхід (Regex → LLM)

1. **Спочатку regex** — витягти кадастровий номер, площі (building_area_sqm, land_area_ha), базову адресу.
2. **LLM тільки якщо потрібно** — якщо regex вже дав достатньо (наприклад, є площа + адреса) — пропустити LLM.
3. **Гібрид** — regex заповнює поля, LLM доповнює лише порожні (tags, utilities, arrests_info).

**Оцінка економії:** 30–50% запитів, якщо regex покриває 30–50% оголошень.

### 2.2 Скорочення промпту

- Видалити дублікати інструкцій у промпті.
- Використати коротший формат (наприклад, bullet points замість повних речень).
- Обрізати опис до N символів (наприклад, 2000) — втрата якості на довгих описах.

**Оцінка економії:** 20–40% токенів на запит.

### 2.3 Локальна LLM (Ollama)

- Використання gemma3:27b через Ollama — без API-лімітів, без вартості.
- Порівняння швидкості та якості з Gemini.

---

## 3. Альтернативні методи розпізнавання

### 3.1 Регулярні вирази (Regex)

**Плюси:** швидко, без мережі, детерміновано, нуль токенів.  
**Мінуси:** слабко для складних адрес, tags, utilities, arrests_info.

**Покриття:**
- ✅ cadastral_number — формат НКЗ:НКК:НЗД
- ✅ building_area_sqm — "65 м²", "120 кв.м", "площа 65"
- ✅ land_area_ha — "0.5 га", "5 гектар", "10 соток"
- ⚠️ addresses — базові патерни (область, місто, вулиця)
- ❌ tags, utilities, arrests_info — складно без LLM

### 3.2 BERT / NER-моделі

**Ідея:** fine-tuned NER для української нерухомості (адреси, площі, типи).

**Плюси:** швидше за LLM, можна запускати локально.  
**Мінуси:** потрібен датасет для fine-tune, підтримка моделі.

**Статус:** пропозиція для майбутнього; поки що не реалізовано.

### 3.3 Локальна LLM (Ollama + Gemma)

**Плюси:** без API-лімітів, без вартості, повна заміна Gemini для парсингу.  
**Мінуси:** потрібні ресурси (GPU/CPU), можлива відмінність якості.

---

## 4. Реалізація

### 4.1 Regex-екстрактор (`utils/listing_regex_extractor.py`)

- Функції для витягування: cadastral_number, building_area_sqm, land_area_ha, базової адреси.
- Інтерфейс сумісний з результатом `parse_auction_description` (Dict з тими ж ключами).

### 4.2 Ollama-провайдер (`business/services/llm_service.py`)

- Клас `OllamaLLMProvider(BaseLLMProvider)`.
- Використання `ollama` Python-пакета.
- Модель за замовчуванням: `gemma3:27b`.

### 4.3 Конфігурація

- `config/config.example.yaml`: додати `ollama` у `llm.provider`, `llm.model_name`, `api_keys.ollama` (порожній для локального).
- `config/settings.py`: підтримка `ollama` у `_load_config` та `_create_provider`.

### 4.4 Benchmark-скрипт (`scripts/benchmark_parsing_methods.py`)

- Завантаження зразків описів з `prozorro_auctions` та `olx_listings`.
- Порівняння методів:
  - Regex
  - LLM Gemini (з кешем)
  - LLM Ollama (gemma3:27b)
- Метрики: час на опис, покриття полів, узгодженість з еталоном (кешований Gemini).

---

## 5. Рекомендації

1. **Коротко:** впровадити regex-екстрактор як перший крок; викликати LLM лише коли regex не покриває ключові поля.
2. **Ollama:** протестувати gemma3:27b на реальних описах; якщо якість прийнятна — використовувати для парсингу замість Gemini.
3. **BERT/NER:** залишити як можливий наступний крок після накопичення даних для fine-tune.
