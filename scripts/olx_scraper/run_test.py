# -*- coding: utf-8 -*-
"""
Тестовий запуск скрапера OLX: перша сторінка розділу «Нерухомість» → JSON.

Заходи антибот:
- один запит (без конкурентних);
- затримка 2–5 с перед запитом;
- User-Agent та заголовки як у звичайного браузера.

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
from scripts.olx_scraper.fetcher import fetch_page
from scripts.olx_scraper.parser import parse_listings_page


def main() -> None:
    url = scraper_config.get_real_estate_list_url(page=1)
    print(f"[OLX scraper] URL: {url}", flush=True)

    response = fetch_page(url, delay_before=True)
    html = response.text
    print(f"[OLX scraper] Отримано байт: {len(html)}", flush=True)

    listings = parse_listings_page(html, use_llm=False)
    print(f"[OLX scraper] Знайдено оголошень: {len(listings)}", flush=True)

    # Каталог для виводу
    output_dir = Path(__file__).resolve().parent / scraper_config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / scraper_config.OUTPUT_FILENAME

    payload = {
        "source": url,
        "total_count": len(listings),
        "listings": listings,
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[OLX scraper] Збережено: {output_file}", flush=True)


if __name__ == "__main__":
    main()
