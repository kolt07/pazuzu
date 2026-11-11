# -*- coding: utf-8 -*-
"""
Тестовий запуск скрапера OLX: перша сторінка розділу «Нерухомість» → JSON.

Production-шлях: list через BrowserPagePool (HTTP list вимкнено).

Запуск з кореня проекту:
  py scripts/olx_scraper/run_test.py
"""

import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from scripts.olx_scraper import config as scraper_config
from scripts.olx_scraper.browser_fetcher import BrowserPagePool
from scripts.olx_scraper.parser import detect_antibot_page, parse_listings_page


def main() -> None:
    url = scraper_config.get_real_estate_list_url(page=1)
    print(f"[OLX scraper] URL: {url}", flush=True)

    with BrowserPagePool(headless=True, pool_size=1, log_fn=lambda s: print(s, flush=True)) as pool:
        response = pool.get_list_page(url)
        html = response.text or ""
        print(f"[OLX scraper] Отримано байт: {len(html)}, status={getattr(response, 'status_code', None)}", flush=True)
        antibot = detect_antibot_page(html)
        print(f"[OLX scraper] antibot={antibot.get('is_antibot')} hints={antibot.get('hints')}", flush=True)

        listings = parse_listings_page(html, use_llm=False)
        print(f"[OLX scraper] Знайдено оголошень: {len(listings)}", flush=True)

    output_dir = Path(__file__).resolve().parent / scraper_config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / scraper_config.OUTPUT_FILENAME

    payload = {
        "source": url,
        "total_count": len(listings),
        "antibot": antibot,
        "listings": listings,
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[OLX scraper] Збережено: {output_file}", flush=True)
    if antibot.get("is_antibot") or len(listings) == 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
