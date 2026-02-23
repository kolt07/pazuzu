# Звіт тестування методів парсингу оголошень

**Дата:** 2026-02-23  
**Зразки:** prozorro_auctions + olx_listings  
**Кеш існуючого потоку:** 19 272 записів (результати Gemini з production)

---

## 1. Порівняння з існуючими потоками

### 1.1 Потоки обробки

| Потік | Компонент | Джерело | Кеш |
|-------|-----------|---------|-----|
| OLX | OlxLLMExtractorService.extract_structured_data | olx_listings (search_data + detail) | LLMCacheService |
| ProZorro | ProZorroService._process_auction_with_llm | prozorro_auctions (auction_data.description) | LLMCacheService |
| Interpreter | InterpreterAgent.extract_structured_from_text | Вільний текст | LLMCacheService |

Усі потоки використовують `LLMService.parse_auction_description()` → Gemini (за замовчуванням).

### 1.2 Результати benchmark (16 зразків: 8 ProZorro + 8 OLX)

| Метрика | Regex | LLM (існуючий/Gemini) |
|---------|-------|------------------------|
| Середній час на опис | **0.001 с** | 0.001 с (з кешу) / ~2–5 с (API) |
| Середнє заповнених полів | **2.8/8** | **4.7/8** |
| Узгодженість з існуючим | 0–67% по полях | 100% (еталон) |

### 1.3 Ollama (gemma3:27b) — 2 зразки ProZorro

| Метрика | Значення |
|---------|----------|
| Час на опис | **~68 с** (перший запуск, завантаження моделі) |
| Узгодженість з існуючим (Gemini) | **100%** (по порівняних полях) |
| Заповненість | Аналогічна Gemini |

---

## 2. Висновки

### 2.1 Regex

- **Швидкість:** практично миттєво (<1 мс)
- **Покриття:** 2.8 полів з 8 в середньому; краще на ProZorro (простіші описи), слабше на OLX (різноманітні формати)
- **Узгодженість з LLM:** 0–67% залежно від зразка
- **Рекомендація:** використовувати як перший крок у каскаді; для полів cadastral_number, building_area_sqm, land_area_ha — можливе покращення патернів

### 2.2 Існуючий потік (Gemini + кеш)

- **Швидкість:** 0.001 с при попаданні в кеш
- **Покриття:** 4.7 полів з 8
- **Стабільність:** висока завдяки кешу

### 2.3 Ollama (gemma3:27b)

- **Якість:** 100% узгодженість з Gemini по порівняних полях
- **Швидкість:** ~68 с на опис (перший запуск); очікувано 10–30 с при «прогрітій» моделі
- **Рекомендація:** підходить для офлайн-обробки або batch; для real-time краще Gemini з кешем

### 2.4 Каскадний підхід

- Regex як перший крок може зменшити 30–50% викликів LLM для оголошень з чіткими патернами (площа, кадастр, адреса)
- Потрібна доопрацювання regex для адрес (різні формати OLX)

---

## 3. Запуск тестів

```bash
# Regex vs існуючий кеш
py scripts/benchmark_parsing_methods.py --limit 8 --methods regex

# Regex + Gemini (з кешем)
py scripts/benchmark_parsing_methods.py --limit 5 --methods regex,gemini

# Regex + Ollama
py scripts/benchmark_parsing_methods.py --limit 2 --methods regex,ollama

# Всі методи
py scripts/benchmark_parsing_methods.py --limit 3 --methods regex,gemini,ollama
```
