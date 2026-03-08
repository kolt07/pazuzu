# -*- coding: utf-8 -*-
"""
Аналіз використання LLM: токени на оголошення, екстраполяція на місяць, прайсинг.

Запуск:
  py scripts/llm_usage_analysis.py
  py scripts/llm_usage_analysis.py --days 90
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.logs_repository import LogsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository


# --- Оцінки токенів на основі коду та промптів ---
# Джерела: config/prompts.yaml (parsing, real_estate_objects_parsing), llm_service._create_parsing_prompt
# Приблизно: 1 токен ≈ 3-4 символи для українського тексту (Cyrillic)

# 1. parse_auction_description (OLX structured extraction)
# Промпт: ~3200 символів (parsing template) + опис оголошення
# Типовий опис OLX: 400-2500 символів (заголовок, параметри, опис, локація)
PARSING_TEMPLATE_CHARS = 3200
PARSING_DESCRIPTION_AVG_CHARS = 1200
PARSING_OUTPUT_AVG_CHARS = 600
CHARS_PER_TOKEN = 3.5  # консервативна оцінка для української

TOKENS_PARSING_INPUT = int((PARSING_TEMPLATE_CHARS + PARSING_DESCRIPTION_AVG_CHARS) / CHARS_PER_TOKEN)
TOKENS_PARSING_OUTPUT = int(PARSING_OUTPUT_AVG_CHARS / CHARS_PER_TOKEN)

# 2. parse_real_estate_objects (ОНМ з опису)
# Промпт real_estate_objects_parsing: ~1200 символів + опис
REO_TEMPLATE_CHARS = 1200
REO_DESCRIPTION_AVG_CHARS = 800
REO_OUTPUT_AVG_CHARS = 400

TOKENS_REO_INPUT = int((REO_TEMPLATE_CHARS + REO_DESCRIPTION_AVG_CHARS) / CHARS_PER_TOKEN)
TOKENS_REO_OUTPUT = int(REO_OUTPUT_AVG_CHARS / CHARS_PER_TOKEN)

# На одне оголошення OLX + пов'язане ОНМ (без кешу):
# - 1 виклик parse_auction_description (при завантаженні/оновленні)
# - 1 виклик parse_real_estate_objects (при backfill ОНМ)
TOKENS_PER_OLX_LISTING_INPUT = TOKENS_PARSING_INPUT + TOKENS_REO_INPUT
TOKENS_PER_OLX_LISTING_OUTPUT = TOKENS_PARSING_OUTPUT + TOKENS_REO_OUTPUT
TOKENS_PER_OLX_LISTING_TOTAL = TOKENS_PER_OLX_LISTING_INPUT + TOKENS_PER_OLX_LISTING_OUTPUT

# ProZorro: LLM використовується рідше (items дають структуру). Якщо LLM — аналогічно parsing.
TOKENS_PER_PROZORRO_AUCTION_INPUT = TOKENS_PARSING_INPUT
TOKENS_PER_PROZORRO_AUCTION_OUTPUT = TOKENS_PARSING_OUTPUT
TOKENS_PER_PROZORRO_AUCTION_TOTAL = TOKENS_PER_PROZORRO_AUCTION_INPUT + TOKENS_PER_PROZORRO_AUCTION_OUTPUT


# --- Gemini 3 Flash прайсинг (грудень 2025) ---
# Джерело: https://aifreeapi.com/en/posts/gemini-3-flash-api-price
GEMINI_3_FLASH_INPUT_PER_1M = 0.50  # USD
GEMINI_3_FLASH_OUTPUT_PER_1M = 3.00  # USD

# Gemini 2.5 Flash (fallback, якщо 3 ще недоступний)
GEMINI_25_FLASH_INPUT_PER_1M = 0.30
GEMINI_25_FLASH_OUTPUT_PER_1M = 2.50


def run_analysis(days: int = 60) -> dict:
    Settings()
    MongoDBConnection.initialize(Settings())

    logs_repo = LogsRepository()
    unified_repo = UnifiedListingsRepository()
    olx_repo = OlxListingsRepository()
    prozorro_repo = ProZorroAuctionsRepository()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # --- 1. Середня кількість оголошень/аукціонів на добу ---
    olx_by_day = list(
        olx_repo.collection.aggregate([
            {"$match": {"updated_at": {"$gte": cutoff}}},
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$updated_at"}},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ])
    )
    prozorro_by_day = list(
        prozorro_repo.collection.aggregate([
            {"$match": {"last_updated": {"$gte": cutoff}}},
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$last_updated"}},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ])
    )

    olx_total = sum(r["count"] for r in olx_by_day)
    prozorro_total = sum(r["count"] for r in prozorro_by_day)
    olx_days_with_data = len(olx_by_day) or 1
    prozorro_days_with_data = len(prozorro_by_day) or 1

    olx_avg_per_day = olx_total / olx_days_with_data if olx_by_day else 0
    prozorro_avg_per_day = prozorro_total / prozorro_days_with_data if prozorro_by_day else 0

    # --- 2. LLM api_usage по source (реальні виклики, не з кешу) ---
    llm_by_source = logs_repo.count_api_usage_by_source(
        service="llm", days=days, from_cache_only=False
    )
    llm_total_api = sum(s.get("count", 0) for s in llm_by_source)
    llm_total_cached = logs_repo.count_api_usage_total(service="llm", from_cache_only=True)
    llm_total_all = logs_repo.count_api_usage_total(service="llm")

    # Доля parse_auction vs generate_text (real_estate_objects)
    parse_auction_count = sum(
        s.get("count", 0) for s in llm_by_source
        if "parse_auction" in str(s.get("source", ""))
    )
    generate_text_count = sum(
        s.get("count", 0) for s in llm_by_source
        if "generate_text" in str(s.get("source", ""))
    )
    langchain_count = sum(
        s.get("count", 0) for s in llm_by_source
        if "langchain" in str(s.get("source", "")).lower()
    )
    other_count = llm_total_api - parse_auction_count - generate_text_count - langchain_count

    # --- 3. Оцінка токенів на оголошення ---
    # Використовуємо оцінки з коду (промпти, типові довжини)
    tokens_per_listing_input = TOKENS_PER_OLX_LISTING_INPUT
    tokens_per_listing_output = TOKENS_PER_OLX_LISTING_OUTPUT

    # --- 4. Екстраполяція на місяць ---
    days_per_month = 30
    olx_per_month = olx_avg_per_day * days_per_month
    prozorro_per_month = prozorro_avg_per_day * days_per_month

    # Токени за місяць (тільки обробка оголошень, без агента/llm_query)
    # Припускаємо: 100% нових оголошень потребують LLM (без кешу при першому завантаженні)
    # Backfill ОНМ — один раз на оголошення, далі кеш
    tokens_month_input_listings = (
        olx_per_month * (TOKENS_PARSING_INPUT + TOKENS_REO_INPUT)
        + prozorro_per_month * TOKENS_PARSING_INPUT
    )
    tokens_month_output_listings = (
        olx_per_month * (TOKENS_PARSING_OUTPUT + TOKENS_REO_OUTPUT)
        + prozorro_per_month * TOKENS_PARSING_OUTPUT
    )

    # З урахуванням кешу: при повторному оновленні того ж оголошення — хеш не змінюється, LLM не викликається
    # api_usage з from_cache=True не рахується в llm_total_api, тому cache_hit_rate = cached / (api + cached)
    if llm_total_all > 0:
        cache_hit_rate = llm_total_cached / llm_total_all
    else:
        cache_hit_rate = 0.0
    effective_rate = 1.0 - cache_hit_rate  # частка реальних викликів при екстраполяції

    tokens_month_input_effective = tokens_month_input_listings * effective_rate
    tokens_month_output_effective = tokens_month_output_listings * effective_rate

    # --- 5. Вартість (Gemini 3 Flash) ---
    cost_input_3f = (tokens_month_input_effective / 1_000_000) * GEMINI_3_FLASH_INPUT_PER_1M
    cost_output_3f = (tokens_month_output_effective / 1_000_000) * GEMINI_3_FLASH_OUTPUT_PER_1M
    cost_total_3f = cost_input_3f + cost_output_3f

    cost_input_25f = (tokens_month_input_effective / 1_000_000) * GEMINI_25_FLASH_INPUT_PER_1M
    cost_output_25f = (tokens_month_output_effective / 1_000_000) * GEMINI_25_FLASH_OUTPUT_PER_1M
    cost_total_25f = cost_input_25f + cost_output_25f

    return {
        "days": days,
        "olx": {
            "total": olx_total,
            "avg_per_day": round(olx_avg_per_day, 1),
            "days_with_data": olx_days_with_data,
        },
        "prozorro": {
            "total": prozorro_total,
            "avg_per_day": round(prozorro_avg_per_day, 1),
            "days_with_data": prozorro_days_with_data,
        },
        "llm": {
            "api_calls_total": llm_total_api,
            "api_calls_cached": llm_total_cached,
            "api_calls_all": llm_total_all,
            "by_source": llm_by_source,
            "parse_auction_count": parse_auction_count,
            "generate_text_count": generate_text_count,
            "langchain_count": langchain_count,
            "other_count": other_count,
        },
        "tokens_per_listing": {
            "input": round(tokens_per_listing_input, 0),
            "output": round(tokens_per_listing_output, 0),
            "total": round(tokens_per_listing_input + tokens_per_listing_output, 0),
        },
        "tokens_estimates_from_code": {
            "parsing_input": TOKENS_PARSING_INPUT,
            "parsing_output": TOKENS_PARSING_OUTPUT,
            "reo_input": TOKENS_REO_INPUT,
            "reo_output": TOKENS_REO_OUTPUT,
            "per_olx_listing_total": TOKENS_PER_OLX_LISTING_TOTAL,
            "per_prozorro_auction_total": TOKENS_PER_PROZORRO_AUCTION_TOTAL,
        },
        "extrapolation_month": {
            "olx_listings": round(olx_per_month, 0),
            "prozorro_auctions": round(prozorro_per_month, 0),
            "tokens_input": round(tokens_month_input_effective, 0),
            "tokens_output": round(tokens_month_output_effective, 0),
            "tokens_total": round(tokens_month_input_effective + tokens_month_output_effective, 0),
            "cache_hit_rate": round(cache_hit_rate, 2),
        },
        "cost_usd_month": {
            "gemini_3_flash": {
                "input": round(cost_input_3f, 2),
                "output": round(cost_output_3f, 2),
                "total": round(cost_total_3f, 2),
            },
            "gemini_25_flash": {
                "input": round(cost_input_25f, 2),
                "output": round(cost_output_25f, 2),
                "total": round(cost_total_25f, 2),
            },
        },
    }


def print_report(data: dict) -> None:
    print("=" * 70)
    print("АНАЛІЗ ВИКОРИСТАННЯ LLM — ОГОЛОШЕННЯ ТА ОНМ")
    print("=" * 70)
    print(f"Період аналізу: останні {data['days']} днів")
    print()

    print("--- 1. СЕРЕДНЯ КІЛЬКІСТЬ ОГОЛОШЕНЬ/АУКЦІОНІВ НА ДОБУ ---")
    print(f"OLX:      всього {data['olx']['total']}, середнє на добу: {data['olx']['avg_per_day']:.1f}")
    print(f"ProZorro: всього {data['prozorro']['total']}, середнє на добу: {data['prozorro']['avg_per_day']:.1f}")
    print()

    print("--- 2. ВИКЛИКИ LLM API (api_usage, без кешу) ---")
    print(f"Всього викликів (реальних): {data['llm']['api_calls_total']}")
    print(f"З кешу: {data['llm']['api_calls_cached']}")
    print(f"Усі (реальні + кеш): {data['llm']['api_calls_all']}")
    if data["llm"]["by_source"]:
        print("По джерелах:")
        for s in data["llm"]["by_source"][:10]:
            print(f"  - {s.get('source', '?')}: {s.get('count', 0)}")
    print()

    print("--- 3. ОЦІНКА ТОКЕНІВ НА ОДНЕ ОГОЛОШЕННЯ OLX + ОНМ ---")
    est = data["tokens_estimates_from_code"]
    print(f"parse_auction_description:  input ~{est['parsing_input']}, output ~{est['parsing_output']} токенів")
    print(f"parse_real_estate_objects:   input ~{est['reo_input']}, output ~{est['reo_output']} токенів")
    print(f"Разом на 1 оголошення OLX: ~{est['per_olx_listing_total']} токенів")
    print(f"На 1 аукціон ProZorro (LLM): ~{est['per_prozorro_auction_total']} токенів")
    print()

    print("--- 4. ЕКСТРАПОЛЯЦІЯ НА МІСЯЦЬ (30 днів) ---")
    ext = data["extrapolation_month"]
    print(f"Оголошень OLX на місяць:     ~{ext['olx_listings']:.0f}")
    print(f"Аукціонів ProZorro на міс:  ~{ext['prozorro_auctions']:.0f}")
    print(f"Токенів (input):            ~{ext['tokens_input']:,.0f}")
    print(f"Токенів (output):           ~{ext['tokens_output']:,.0f}")
    print(f"Токенів (всього):           ~{ext['tokens_total']:,.0f}")
    print(f"Частка з кешу:              {ext['cache_hit_rate']*100:.0f}%")
    print()

    print("--- 5. ПРИБЛИЗНА ВАРТІСТЬ LLM ЗА МІСЯЦЬ (USD) ---")
    c3 = data["cost_usd_month"]["gemini_3_flash"]
    c25 = data["cost_usd_month"]["gemini_25_flash"]
    print("Gemini 3 Flash (input $0.50/1M, output $3.00/1M):")
    print(f"  Input:  ${c3['input']:.2f}")
    print(f"  Output: ${c3['output']:.2f}")
    print(f"  Всього: ${c3['total']:.2f}")
    print("Gemini 2.5 Flash (input $0.30/1M, output $2.50/1M):")
    print(f"  Input:  ${c25['input']:.2f}")
    print(f"  Output: ${c25['output']:.2f}")
    print(f"  Всього: ${c25['total']:.2f}")
    print()
    print("Примітка: вартість лише за обробку оголошень та ОНМ. Запити агента (llm_query)")
    print("та LangChain додають додаткові токени — перегляньте usage-stats в адмін-панелі.")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Аналіз використання LLM")
    parser.add_argument("--days", type=int, default=60, help="Кількість днів для аналізу")
    args = parser.parse_args()
    data = run_analysis(days=args.days)
    print_report(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
