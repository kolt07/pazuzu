# Аудит LLM-агентів та Best Practices

Дата: 2026-02-08

## 1. Поточна архітектура

- **MultiAgentService** — вхідна точка для Telegram-бота: перевірка безпеки → інтерпретація наміру → для простих намірів (звіт за добу/тиждень, експорт) пайплайн через Planner + Analyst; для складних — fallback на **LangChainAgentService** (один агент з інструментами та пам'яттю).
- **LangChainAgentService** — один агент на LangChain з MCP-інструментами, пам'ять розмови (буфер + саммарі + опційно векторний пошук), системний промпт з глосарієм та контекстом дати/часу.
- **LLMAgentService** — альтернативна реалізація на нативному Gemini API (function calling), використовується окремо (наприклад, для сценаріїв без LangChain).
- **Агенти**: SecurityAgent, InterpreterAgent, PlannerAgent, AnalystAgent, AssistantAgent — оркестрація та спеціалізовані кроки.

**Потік запиту (Telegram бот):**  
Користувач → MultiAgentService.process_query → SecurityAgent.check → AssistantAgent (інтерпретація → планування → виконання) або LangChainAgentService.process_query.

**Потік запиту (Mini App до змін):**  
Користувач → LangChainAgentService.process_query (без перевірки безпеки та без мультиагентного пайплайну).

---

## 2. Best Practices та відповідність

### 2.1 Tool use (інструменти)

| Практика | Статус | Коментар |
|----------|--------|----------|
| Чіткі описи інструментів | ✅ | Описи в `get_tools_descriptions()` та LangChain-еквіваленті деталізовані, з прикладами. |
| Обмежений набір інструментів (closed-world) | ✅ | Фіксований список MCP-інструментів, без динамічного коду. |
| Один оркестратор викликів | ✅ | Агент викликає tools через сервіс; виконання централізоване в `_call_tool` / LangChain bind. |

**Рекомендація:** Залишити як є; при додаванні нових інструментів — чіткі описи та обмеження в системному промпті.

### 2.2 Пам'ять та контекст

| Практика | Статус | Коментар |
|----------|--------|----------|
| Пам'ять розмови на користувача | ✅ | `UserConversationMemory` (буфер + саммарі + опційно векторний пошук) по `user_id`. |
| Обмеження довжини контексту | ✅ | `CONVERSATION_BUFFER_MAX_MESSAGES`, `CONVERSATION_SUMMARY_TRIM_SIZE`, обрізання старих обмінів. |
| Контекст дати/часу в промпті | ✅ | Поточна дата/час (Київ) та діапазони (доба, тиждень, місяць) передаються в LLM. |

**Рекомендація:** При збільшенні навантаження розглянути окремий ліміт на токени контексту в налаштуваннях.

### 2.3 Observability (спостережність)

#### Контракт структурованого логу (agent_activity_log)

- **STEP_INTENT:** payload = структурований намір (intent, collections, period_days, region_filter, routing_path, confidence).
- **STEP_ACTION (planner):** payload = steps_count, steps (список кроків плану).
- **STEP_ACTION (analyst):** payload = step_index, action, success, duration_ms, input_summary (action, params_keys), output_summary (success, rows_count, count).
- **STEP_RESPONSE:** payload = response_length, excel_count, duration_ms, path (multi_agent | langchain), routing_path, iterations (для langchain).

Replay: за request_id можна відтворити виконання плану без LLM (скрипт `scripts/replay_request.py`).

| Практика | Статус | Коментар |
|----------|--------|----------|
| Лог активності агентів | ✅ | `AgentActivityLogRepository` — намір, кроки плану, відповідь. |
| Логування викликів інструментів | ✅ | Logger у `_call_tool`, LangChain-агент, ітерації циклу. |
| request_id для трасування | ✅ | Пробрасовується в `process_query` (опційно); Telegram та Mini App передають; у логах LangChain — префікс `[request_id=...]`; у `agent_activity_log` використовується для всіх кроків. |

**Виконано:** Параметр `request_id` у `MultiAgentService` та `LangChainAgentService`; генерація в клієнтах; логування з request_id.

### 2.4 Безпека та guardrails

| Практика | Статус | Коментар |
|----------|--------|----------|
| Перевірка запиту перед обробкою | ✅ | SecurityAgent — патерни (DB, $regex, цикли, eval тощо), ліміт довжини запиту. |
| Сповіщення адмінів при порушенні | ✅ | У боті — `notify_admins_fn` (Telegram). У Mini App — не було; після змін використовується MultiAgentService з логуванням інцидентів. |
| Обмеження ітерацій агента | ✅ | LangChain: `MAX_ITERATIONS = 10`; LLMAgentService (Gemini): `max_iterations = 5`. |
| Заборона $regex у query-builder | ✅ | Документовано в промпті та валідації. |

**Рекомендація:** Єдина точка входу для всіх клієнтів (бот, Mini App) — MultiAgentService, щоб SecurityAgent завжди виконувався.

#### MCP deny-rules

MCP та QueryBuilder **не приймають** і відхиляють на валідації:

- **Верхньорівневі ключі запиту:** `raw_query`, `$where`, `$eval`, `$function`, `$expr` — будь-який такий ключ у запиті призводить до відмови з повідомленням про deny-rule.
- **Оператори у фільтрах:** `$where`, `$eval`, `$function`, `$expr`, `$text` — заборонені. `$regex` дозволений **тільки** для полів статусу (і лише з фіксованими патернами у внутрішній логіці); для фільтрації за регіоном/містом використовуйте analytics-mcp або попередньо визначені умови.
- **User input у небезпечних операторах:** якщо в майбутньому буде динамічний regex з вводом користувача — такий ввод у `$regex` блокується.

Див. `utils/query_builder.py`: `DENY_TOP_LEVEL_KEYS`, `FORBIDDEN_OPERATORS`, `_validate_filters`, `_transform_filters`.

### 2.5 Обмеження ресурсів

| Практика | Статус | Коментар |
|----------|--------|----------|
| Макс. ітерації циклу агента | ✅ | 10 (LangChain) / 5 (LLMAgentService). |
| Макс. викликів tools за ітерацію | ✅ | LangChain: `MAX_TOOL_CALLS_PER_ITERATION = 10`. |
| Rate limit викликів LLM | ✅ | `RateLimiter` у llm_service та llm_agent_service. |
| Таймаут відповіді (Mini App) | ✅ | `future.result(timeout=300)` у routes/llm.py. |
| Константи в конфігурації | ⚠️ | MAX_ITERATIONS, temperature, max_output_tokens захардкоджені в сервісах. |

**Рекомендація:** Винести max_iterations, temperature, max_output_tokens в `config` (наприклад, `llm.agent.*`), щоб керувати без зміни коду.

### 2.6 Обробка помилок

| Практика | Статус | Коментар |
|----------|--------|----------|
| Try/except на викликах інструментів | ✅ | `_call_tool` повертає `{success, error}`; винятки логуються. |
| Повідомлення користувачу без витоку деталей | ✅ | Загальні формулювання типу "Помилка обробки запиту", без stack trace. |
| Retry при помилках LLM (парсинг) | ✅ | У llm_service (Gemini) — retry при 429, зміна моделі. |
| Retry в agent loop | ✅ | У LangChainAgentService додано retry (до 2 повторів, backoff 1 с / 2 с) при тимчасових помилках (503, 429, timeout, connection тощо) для `invoke` у циклі та для фінальної відповіді. |

**Виконано:** Функція `_is_transient_llm_error()`, константи `AGENT_LLM_RETRY_ATTEMPTS` та `AGENT_LLM_RETRY_BACKOFF_SECONDS`.

### 2.7 Простота архітектури

| Практика | Статус | Коментар |
|----------|--------|----------|
| Розділення: простий пайплайн vs складний агент | ✅ | Чіткі наміри (report_last_day, report_last_week, export_data) → пайплайн; решта → один LangChain-агент. |
| Один основний агент для вільного діалогу | ✅ | Один LangChain-агент з інструментами, без надмірної кількості під-агентів у циклі. |

---

## 3. Виявлені прогалини та виконані зміни

1. **Mini App не використовував MultiAgentService**  
   - **Ризик:** запити з Mini App не проходили перевірку SecurityAgent.  
   - **Зміна:** у Mini App як точка входу використовується MultiAgentService; при порушенні безпеки адміни отримують подію через лог (у Mini App контексті немає Telegram-бота для сповіщень).

2. **Константи агента в коді**  
   - **Ризик:** зміна лімітів або температури вимагала правок у кількох сервісах.  
   - **Зміна:** у `config/settings.py` та `config.example.yaml` додано опційні параметри `llm.agent.max_iterations`, `llm.agent.max_output_tokens`, `llm.agent.temperature`; LangChainAgentService та LLMAgentService беруть їх із налаштувань.

3. **Документація**  
   - Додано цей файл `docs/llm_agent_audit.md`.  
   - У глосарій додано терміни: agent loop, tool call, iteration limit, guardrails.  
   - Оновлено `docs/development_history.md`.

---

## 4. Рекомендації на майбутнє (виконано 2026-02-08)

- **request_id:** ✅ Пробрасовується у всі виклики `process_query` (опційний параметр; якщо не передано — генерується у MultiAgentService). Telegram-бот та Mini App генерують і передають `request_id`. Додано до логів LangChain-агента (префікс `[request_id=...]`) та до записів `agent_activity_log` (усі кроки вже використовували request_id).
- **Retry в agent loop:** ✅ У LangChainAgentService додано 1–2 повтори з експоненційною затримкою (1 с, 2 с) при тимчасових помилках LLM (503, 429, timeout, connection, quota). Функція `_is_transient_llm_error()` визначає, чи варто повторювати; retry застосовується і до основного `invoke`, і до виклику для фінальної відповіді після досягнення max ітерацій.
- **Метрики:** ✅ У `agent_activity_log` у кроці STEP_RESPONSE додано `duration_ms`, `path` (multi_agent | langchain), для шляху langchain — також `iterations` (з `LangChainAgentService._last_request_metrics`). У логах LangChain — завершальний рядок із duration_sec; метрики останнього запиту зберігаються в `_last_request_metrics` для зчитування MultiAgentService.
- **Єдиний LLM-шар:** залишити LangChainAgentService основним агентом для діалогу; LLMAgentService (Gemini native) використовувати лише там, де потрібна відсутність залежності від LangChain.
