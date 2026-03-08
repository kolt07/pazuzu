# -*- coding: utf-8 -*-
"""
Тест швидкості Ollama на реальних промптах з колекції llm_exchange_logs.

Запуск (з кореня проекту):
  py scripts/ollama_prompt_benchmark.py

Скрипт:
  - бере останні промпти з llm_exchange_logs (provider='ollama')
  - міряє час відповіді через ollama.run для 2 найдовших промптів
  - один з промптів додатково ганяє в режимі "чату" (system + кілька user-повідомлень)
  - вмикає OLLAMA_DEBUG=1 для першого прогону, щоб у виводі було видно, чи використовується GPU
"""

import sys
import time
import os
from pathlib import Path
from typing import List, Tuple


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.llm_exchange_logs_repository import LLMExchangeLogsRepository


def _pick_latest_ollama_prompts(limit: int = 20, count: int = 2) -> List[str]:
    """
    Беремо останні промпти provider='ollama' і обираємо кілька найдовших (щоб прогнати «найгірший» кейс).
    """
    repo = LLMExchangeLogsRepository()
    docs = repo.find_recent(limit=limit, provider="ollama")
    texts = []
    seen_hashes = set()
    for d in docs:
        txt = (d.get("request_text") or "").strip()
        if not txt:
            continue
        h = hash(txt)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        texts.append(txt)
    if not texts:
        return []
    # обираємо кілька найдовших промптів
    texts.sort(key=len, reverse=True)
    return texts[:count]


def _run_ollama_cli(prompt: str, model: str, debug: bool = False) -> Tuple[float, int]:
    """
    Виконує один запит через ollama run, повертає (seconds, return_code).
    """
    import subprocess

    env = os.environ.copy()
    if debug:
        env["OLLAMA_DEBUG"] = "1"

    cmd = ["ollama", "run", model, "-p", prompt]
    print(f"\n=== ollama run {model} (довжина промпта: {len(prompt)} символів) ===")
    print("Команда:", " ".join(cmd))
    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - start
    print(f"Код завершення: {proc.returncode}, час: {elapsed:.2f} s")

    if debug:
        # Виводимо кілька рядків debug-виводу, щоб побачити, чи використовується GPU
        stderr = (proc.stderr or "").splitlines()
        print("\n--- OLLAMA_DEBUG (перші 30 рядків stderr) ---")
        for line in stderr[:30]:
            print(line)
    else:
        # Щоб не засмічувати консоль, показуємо тільки перші рядки відповіді
        out_lines = (proc.stdout or "").splitlines()
        print("\n--- Перші рядки відповіді ---")
        for line in out_lines[:10]:
            print(line)

    return elapsed, proc.returncode


def _split_instructions_and_description(prompt: str) -> Tuple[str, str]:
    """
    Намагається розділити промпт на «інструкції» та «текст оголошення».
    Пошук маркерів, як у шаблонах parsing / olx_parsing.
    """
    markers = [
        "Description (Ukrainian):",
        "Description:",
        "Опис:",
    ]
    for m in markers:
        idx = prompt.find(m)
        if idx != -1:
            head = prompt[: idx + len(m)]
            tail = prompt[idx + len(m) :].lstrip()
            return head, tail
    # fallback: всякий випадок – усе вважаємо інструкцією, опису немає
    return prompt, ""


def _chat_reuse_instructions(prompt: str, model: str, turns: int = 3) -> None:
    """
    Тестує режим «один раз задали інструкції, далі лише новий текст» через ollama.Client().chat.
    """
    try:
        from ollama import Client
    except ImportError:
        print("Модуль ollama для Python не встановлено, пропускаю chat-тест.")
        return

    instructions, description = _split_instructions_and_description(prompt)
    if not description:
        description = "Демонстраційний текст оголошення для тесту."

    print(f"\n=== Chat-тест з контекстом (модель {model}) ===")
    print(f"Довжина інструкцій: {len(instructions)} символів, довжина опису: {len(description)} символів")

    client = Client()
    messages = [
        {"role": "system", "content": instructions},
        {"role": "user", "content": f"Тепер текст такий:\n{description}"},
    ]

    for i in range(turns):
        start = time.perf_counter()
        resp = client.chat(model=model, messages=messages)
        elapsed = time.perf_counter() - start
        content = (resp.get("message", {}) or {}).get("content") or ""
        usage = {
            "prompt_eval_count": resp.get("prompt_eval_count"),
            "eval_count": resp.get("eval_count"),
        }
        print(f"\n--- Turn {i + 1}/{turns} ---")
        print(f"Час: {elapsed:.2f} s, usage: {usage}")
        print("Перші рядки відповіді:")
        for line in content.splitlines()[:10]:
            print(line)

        # Додаємо відповідь в історію та новий текст (імітуємо «Тепер текст такий: ...»)
        messages.append({"role": "assistant", "content": content})
        messages.append(
            {
                "role": "user",
                "content": f"Тепер текст такий (#{i + 2}):\n{description}",
            }
        )


def main() -> None:
    settings = Settings()
    MongoDBConnection.initialize(settings)

    model = getattr(settings, "llm_parsing_model_name", "gemma3:12b")
    print(f"Використовуємо модель Ollama для тестів: {model}")

    prompts = _pick_latest_ollama_prompts(limit=30, count=2)
    if not prompts:
        print("Не знайшов жодного промпта з provider='ollama' у llm_exchange_logs.")
        return

    print(f"Знайдено {len(prompts)} промптів для тестів.")

    for idx, p in enumerate(prompts, start=1):
        print(f"\n==============================")
        print(f"Промпт #{idx}: довжина {len(p)} символів (попередній перегляд перших 400):")
        preview = p[:400]
        print(preview)
        if len(p) > 400:
            print("... [обрізано]")

        # Для першого промпта вмикаємо детальний debug, щоб побачити GPU / CPU.
        debug = idx == 1
        _run_ollama_cli(p, model=model, debug=debug)

        if idx == 1:
            # На першому ж промпті додатково тестуємо режим chat з повторним використанням інструкцій.
            _chat_reuse_instructions(p, model=model, turns=3)


if __name__ == "__main__":
    main()

