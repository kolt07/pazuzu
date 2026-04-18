# -*- coding: utf-8 -*-
"""
Тестовий скрипт для виявлення телефонів продавця OLX.
Використовує fetch_page + parse_detail_page (телефони вже є в HTML).

Запуск з кореня проекту:
  py scripts/olx_scraper/test_phone_reveal.py [URL_оголошення]
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from scripts.olx_scraper.fetcher import fetch_page
from scripts.olx_scraper.parser import parse_detail_page


def main() -> None:
    if len(sys.argv) > 1:
        url = sys.argv[1]
    else:
        url = "https://www.olx.ua/d/uk/obyavlenie/prodazh-fermi-pdhodit-dlya-svinofermi-kurnika-korvnika-IDYkiYl.html"

    print(f"[test] Тестування: {url}", flush=True)
    print("[test] Завантаження сторінки через requests...", flush=True)

    try:
        response = fetch_page(url, delay_before=False)
        detail = parse_detail_page(response.text)
        phones = (detail.get("contact") or {}).get("phones") or []
        print(f"[test] HTML: {len(response.text)} символів", flush=True)
        print(f"[test] Телефони: {phones if phones else '(не знайдено)'}", flush=True)
    except Exception as e:
        print(f"[test] Помилка: {e}", flush=True)
        raise


if __name__ == "__main__":
    main()
