# Ревʼю AI-помічника: концепції з Reddit, досліджень та рекомендації

**Дата:** 2026-02-24

## 1. Поточна архітектура (коротко)

- **MultiAgentService** — точка входу: SecurityAgent → InterpreterAgent → Planner/AssistantAgent або **LangChainAgentService**
- **LangChainAgentService** — один агент з MCP tools, памʼять (buffer + summary + vector retrieval), явний цикл plan → act → observe
- **Потік:** LLM отримує запит → вирішує: tool_calls чи текст → tools виконуються → результат додається в історію → повтор до max_iterations або фінальної відповіді

---

## 2. Концепції з Reddit, LangChain, досліджень

### 2.1 ReAct (Reason + Act)

**Джерело:** [ReAct: Synergizing Reasoning and Acting in Language Models](https://react-lm.github.io/), [LangGraph ReAct Agent](https://www.emergentmind.com/topics/langgraph-react-agent)

**Суть:** Ітеративний цикл **Thought → Action → Observation**:
- **Thought** — LLM міркує про проблему, визначає прогалини, планує кроки
- **Action** — виклик інструментів (пошук, запити, API)
- **Observation** — результати повертаються в reasoning

**Відмінність від поточного флоу:** У Pazuzu агент використовує function calling без явного **Thought** — LLM одразу викликає tools. LangGraph не використовує окремий текстовий «Thought» step; замість цього модель може «думати» в контексті (Gemini thinking mode).

**Рекомендація:** Залишити як є — Gemini thinking_budget вже дає внутрішній reasoning. Підсилити в системному промпті інструкцію: «Перед викликом інструменту коротко сформулюй: що саме потрібно знайти і чому це допоможе відповісти».

---

### 2.2 Self-correction / Error Recovery

**Джерело:** [PALADIN: Self-Correcting Language Model Agents](https://arxiv.org/html/2509.25238v1), [Hell or High Water: Evaluating Agentic Recovery](https://arxiv.org/html/2508.11027v1), [LangGraph retries](https://langchain-ai.github.io/langgraph/tutorials/extraction/retries/)

**Ключові практики:**
1. **Повертати помилки в контекст агента** — якщо tool повернув `success: false`, LLM має отримати чітке повідомлення та можливість спробувати інший підхід
2. **Validation + Re-prompting** — при невалідних аргументах форматувати помилку як нове повідомлення і дати LLM виправити
3. **Backup planning** — завдання мають залишатися вирішуваними через альтернативні інструменти (наприклад, execute_analytics замість execute_query якщо ProZorro порожній)

**Поточний стан:**  
- У Pazuzu: `_agent_hint` вже використовується для підказок (наприклад, «спробуй olx_listings якщо ProZorro порожній»)
- При помилці tool результат повертається як `{success: false, error: "..."}` — але LLM не отримує явної інструкції «проаналізуй помилку і спробуй інший підхід»

**Рекомендація:** Додати **error recovery hint** — коли tool повертає `success: false`, додавати до ToolMessage префікс:

```
[ПОМИЛКА ІНСТРУМЕНТУ] Результат невдалий. Проаналізуй причину (невірні параметри, порожні дані, заборонені операції) і спробуй інший підхід: інший інструмент, інші фільтри або зміна стратегії запиту.
```

---

### 2.3 Hybrid Workflow (Reddit r/LangChain)

**Джерело:** [Hybrid workflow with LLM calls + programmatic steps](https://www.reddit.com/r/LangChain/comments/1p5lchr/)

**Суть:** Варто використовувати LLM лише там, де потрібне судження. Детерміновані кроки (нормалізація, валідація, агрегація) краще виконувати кодом.

**Поточний стан:** Pazuzu вже дотримується цього — Planner + Analyst виконують детерміновані кроки; LangChain-агент лише для складних/вільних запитів.

**Рекомендація:** Залишити як є; при додаванні нових інструментів — максимум логіки в коді, мінімум «віддавати на LLM».

---

### 2.4 Anticipatory Reflection / Reflexion

**Джерело:** [Recursive Introspection (RISE)](https://arxiv.org/abs/2407.18219), [Reflexion](https://scispace.com/pdf/reflexion-language-agents-with-verbal-reinforcement-learning-242t789l.pdf)

**Суть:** Перед виконанням — коротке «антиципаторне» міркування про можливі помилки; після виконання — перевірка результату і відкат при невідповідності.

**Практична реалізація на рівні промпту:** Додати в системний промпт блок:

```
Перед викликом інструменту коротко перевір: чи правильно визначені параметри (колекція, фільтри, дати)? Чи є альтернативний інструмент, якщо цей може повернути порожній результат?
```

---

### 2.5 MCP Tool Best Practices (LangChain)

**Джерело:** [LangChain MCP docs](https://docs.langchain.com/oss/javascript/langchain/mcp), [langchain-mcp-adapters](https://github.com/langchain-ai/langchain-mcp-adapters)

**Ключові практики:**
- Чіткі описи інструментів з прикладами
- Closed-world: фіксований список tools, без динамічного коду
- Stateless sessions для масштабованості

**Поточний стан:** Pazuzu вже відповідає — `get_tools_descriptions()` деталізовані, TOOL_ROUTES для фільтрації.

---

### 2.6 Tool Result Truncation / Context Management

**Поточний стан:** Pazuzu обрізає великі результати: при >20 записів передає `ids_for_export` і `_agent_hint` на експорт.

**Рекомендація:** Залишити; при потребі можна додати підказку «якщо потрібні деталі — використовуй export_from_temp_collection».

---

## 3. Запропоновані покращення

| # | Покращення | Опис | Складність |
|---|------------|------|------------|
| 1 | **Error recovery hint** | При `success: false` додавати інструкцію само-корекції в ToolMessage | Низька |
| 2 | **ReAct-підказка в системному промпті** | «Перед викликом інструменту коротко сформулюй: що шукаєш і чому» | Низька |
| 3 | **Anticipatory reflection** | Блок «перевір параметри перед викликом» у системному промпті | Низька |
| 4 | **Retry при помилці tool з іншими параметрами** | Якщо execute_analytics повернув 0 — автоматично додати hint про olx_listings (вже є частково) | Середня |
| 5 | **Метрики recovery** | Логувати: кількість помилок tools, чи агент спробував інший підхід | Середня |

---

## 4. Реалізація покращень 1–3

### 4.1 Error recovery hint

У `_process_query_impl` при формуванні `ToolMessage` — якщо `tool_result.get('success') is False`:

```python
if isinstance(tool_result, dict) and tool_result.get('success') is False:
    error_hint = (
        "[ПОМИЛКА ІНСТРУМЕНТУ] Результат невдалий. Проаналізуй причину "
        "(невірні параметри, порожні дані, заборонені операції) і спробуй інший підхід: "
        "інший інструмент, інші фільтри або зміна стратегії запиту.\n\n"
    )
    if tool_result.get('_agent_hint'):
        tool_result['_agent_hint'] = error_hint + tool_result['_agent_hint']
    else:
        tool_result['_agent_hint'] = error_hint
```

### 4.2 ReAct + Anticipatory reflection у промпті

Додати в `config/prompts.yaml` (langchain_system.base) блок:

```yaml
## МІРКУВАННЯ ТА ПЕРЕВІРКИ (ReAct)

- Перед викликом інструменту коротко сформулюй: що саме потрібно знайти і чому це допоможе відповісти на запит.
- Перевір параметри: колекція, фільтри, діапазони дат — чи відповідають вони запиту користувача?
- Якщо інструмент може повернути порожній результат (наприклад, ProZorro за датою) — май на увазі альтернативу (наприклад, olx_listings).
```

---

## 5. Порівняння флоу (до/після)

| Аспект | До | Після |
|--------|-----|-------|
| Помилка tool | `{success: false, error: "..."}` — LLM бачить, але без явної інструкції | `[ПОМИЛКА ІНСТРУМЕНТУ] Результат невдалий. Проаналізуй причину...` — явна інструкція само-корекції |
| Планування перед викликом | Немає | Підказка «що шукаєш і чому» + «перевір параметри» |
| Альтернативні інструменти | `_agent_hint` лише для окремих кейсів (ProZorro→OLX) | Той самий механізм; додатково — загальна інструкція в промпті |

---

## 6. Реалізовані подальші кроки (2026-02-24)

### 6.1 Метрики recovery

- **tool_failures_count** — кількість викликів tools з `success=false` за запит
- **tool_recovery_attempted** — чи агент спробував інший підхід після помилки (успішний tool після failed)
- Логуються в `agent_activity_log` (STEP_RESPONSE payload) та `_last_request_metrics`

### 6.2 Retry при порожніх результатах

Розширено `_agent_hint` для `execute_query`/`execute_aggregation` з 0 результатів:
- **prozorro_auctions** → підказка спробувати olx_listings
- **olx_listings** → підказка спробувати prozorro_auctions
- **unified_listings** → підказка спробувати prozorro та olx окремо

### 6.3 LangGraph

- **llm_agent_use_langgraph** (config/env) — при `true` використовується StateGraph замість while-циклу
- **business/services/langgraph_agent_runner.py** — оркестрація: agent → tools → agent
- Checkpoints через MemorySaver (опційно)
- Fallback при досягненні max_iterations без текстової відповіді

---

## 7. Тестування

Скрипт `scripts/review_ai_assistant_flow.py` дозволяє:
- Запустити набір тестових запитів (успішні, з помилками, складні)
- Порівняти кількість ітерацій, викликів tools, наявність recovery при помилках
- Запуск: `py scripts/review_ai_assistant_flow.py` (потребує налаштованого LLM)

---

## 7. Джерела

- [ReAct: Synergizing Reasoning and Acting](https://react-lm.github.io/)
- [LangGraph ReAct Agent](https://www.emergentmind.com/topics/langgraph-react-agent)
- [PALADIN: Self-Correcting Language Model Agents](https://arxiv.org/html/2509.25238v1)
- [LangChain MCP docs](https://docs.langchain.com/oss/javascript/langchain/mcp)
- [Reddit: Hybrid workflow with LLM](https://www.reddit.com/r/LangChain/comments/1p5lchr/)
- [LangGraph extraction retries](https://langchain-ai.github.io/langgraph/tutorials/extraction/retries/)
