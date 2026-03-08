# -*- coding: utf-8 -*-
"""
Прайсинг LLM API для оцінки витрат у адмін-панелі.
Джерело: https://ai.google.dev/gemini-api/docs/pricing (оновлювати при зміні тарифів).
"""

# Ціни за 1 млн токенів (USD). Gemini 2.5 Flash — типова модель у Pazuzu.
GEMINI_25_FLASH_INPUT_PER_1M = 0.30
GEMINI_25_FLASH_OUTPUT_PER_1M = 2.50

# Інші моделі (опційно для майбутнього вибору)
GEMINI_25_PRO_INPUT_PER_1M = 1.25
GEMINI_25_PRO_OUTPUT_PER_1M = 10.00
GEMINI_25_FLASH_LITE_INPUT_PER_1M = 0.10
GEMINI_25_FLASH_LITE_OUTPUT_PER_1M = 0.40


def estimate_gemini_cost_usd(
    input_tokens: int,
    output_tokens: int,
    model: str = "gemini-2.5-flash",
) -> float:
    """
    Оцінка вартості в USD за кількістю вхідних та вихідних токенів.

    Args:
        input_tokens: кількість вхідних токенів
        output_tokens: кількість вихідних токенів
        model: назва моделі (gemini-2.5-flash, gemini-2.5-pro, gemini-2.5-flash-lite)

    Returns:
        Орієнтовна вартість у USD
    """
    if "pro" in model.lower():
        in_rate = GEMINI_25_PRO_INPUT_PER_1M
        out_rate = GEMINI_25_PRO_OUTPUT_PER_1M
    elif "lite" in model.lower():
        in_rate = GEMINI_25_FLASH_LITE_INPUT_PER_1M
        out_rate = GEMINI_25_FLASH_LITE_OUTPUT_PER_1M
    else:
        in_rate = GEMINI_25_FLASH_INPUT_PER_1M
        out_rate = GEMINI_25_FLASH_OUTPUT_PER_1M
    return (input_tokens / 1_000_000 * in_rate) + (output_tokens / 1_000_000 * out_rate)
